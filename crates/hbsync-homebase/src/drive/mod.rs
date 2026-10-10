//! The Filesync drive as a [`Remote`]: node schema v1 on the identity's V2 drive API.
//!
//! The drive is found by its slugs (`GET /api/v2/apps/filesync/drives/filesync`); file routes
//! then take its alias, as own-identity routes by slug don't exist yet (odin-core #1902).
//! Facts: `docs/spec/homebase-api-notes.md`.

use std::io::Write;
use std::sync::Mutex;

use hbsync_remote::{ChangesPage, NodeMeta, Remote, RemoteError, RemoteHeader, Result, Upload};
use hbsync_types::schema::PAYLOAD_KEY;
use hbsync_types::{RemoteFileId, RemoteRef, VersionTag};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::client::{Client, UploadMethod};
use crate::crypto::random;
use crate::multipart::{Multipart, Part};
use crate::{HomebaseError, app};

pub mod sealed;
pub mod wire;

/// Payload bytes per ranged read: odin-core reads each request whole into memory (#1892).
const READ_CHUNK: u64 = 8 * 1024 * 1024;

/// Ranges are 32-bit on the server (#1893): payloads this large can't be read back.
const MAX_PAYLOAD: u64 = (1 << 31) - 1;

/// Skipped feed rows kept for [`HomebaseRemote::take_skipped`] at most.
const MAX_SKIPPED: usize = 1000;

/// The sync drive on an identity, as its alias and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct TargetDrive {
    pub alias: Uuid,
    #[serde(rename = "type")]
    pub drive_type: Uuid,
}

/// The Filesync drive of one identity.
#[derive(Debug)]
pub struct HomebaseRemote {
    client: Client,
    drive: TargetDrive,
    /// Feed rows that aren't usable sync nodes, with the reason; for the caller to report.
    skipped: Mutex<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveAnswer {
    target_drive: TargetDrive,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriteResult {
    file_id: Uuid,
    new_version_tag: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteResult {
    #[serde(default)]
    local_file_not_found: bool,
}

impl HomebaseRemote {
    /// Finds the Filesync drive on the client's identity.
    pub fn connect(client: Client) -> Result<Self, HomebaseError> {
        let answer: DriveAnswer = client.get(
            &format!(
                "/api/v2/apps/{}/drives/{}",
                app::APP_SLUG,
                app::SYNC_DRIVE.slug
            ),
            &[],
        )?;
        Ok(HomebaseRemote {
            client,
            drive: answer.target_drive,
            skipped: Mutex::new(Vec::new()),
        })
    }

    pub fn drive(&self) -> TargetDrive {
        self.drive
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The feed rows skipped since the last call, with the reason (at most a thousand).
    pub fn take_skipped(&self) -> Vec<String> {
        std::mem::take(&mut *self.skipped.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn files(&self) -> String {
        format!("/api/v2/drives/{}/files", self.drive.alias.simple())
    }

    fn file(&self, file_id: RemoteFileId) -> String {
        format!("{}/{}", self.files(), file_id.0)
    }

    /// Creates a file, or with `target` updates that file at that version, in one multipart
    /// request.
    fn write(
        &self,
        target: Option<RemoteRef>,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef, HomebaseError> {
        if let Some(p) = &payload
            && p.size > MAX_PAYLOAD
        {
            return Err(HomebaseError::Protocol(format!(
                "{} bytes is too large for this server (odin-core #1893)",
                p.size
            )));
        }
        let transfer_iv = transfer_iv()?;
        let version = target.map(|t| t.version);
        let instructions = wire::instructions(
            &transfer_iv,
            payload.as_ref().map(|p| &p.hash),
            target.is_some(),
        );
        let metadata = wire::metadata(meta, version, self.client.key(), &transfer_iv);
        let mut parts = vec![
            Part::bytes(
                "instructions",
                "application/json",
                instructions.to_string().into_bytes(),
            ),
            Part::bytes("metadata", wire::CONTENT_TYPE, metadata),
        ];
        if let Some(p) = payload {
            parts.push(Part::reader(
                "payload",
                PAYLOAD_KEY,
                wire::CONTENT_TYPE,
                p.reader,
                p.size,
            ));
        }
        let (method, path) = match target {
            None => (UploadMethod::Post, self.files()),
            Some(at) => (UploadMethod::Patch, self.file(at.file_id)),
        };
        let boundary = Uuid::new_v4().simple().to_string();
        let result: WriteResult =
            self.client
                .upload(method, &path, Multipart::new(boundary, parts))?;
        Ok(RemoteRef {
            file_id: RemoteFileId(result.file_id),
            version: VersionTag(result.new_version_tag),
        })
    }
}

/// A fresh transfer IV; the server refuses an all-zero one.
fn transfer_iv() -> Result<[u8; 16], HomebaseError> {
    loop {
        let iv = random::<16>()?;
        if iv != [0; 16] {
            return Ok(iv);
        }
    }
}

impl Remote for HomebaseRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        let size = limit.max(1);
        let page: wire::QueryBatchResponse = self
            .client
            .post(
                &format!("{}/query-batch", self.files()),
                &wire::feed_query(cursor, size),
            )
            .map_err(remote_error)?;
        let mut entries = Vec::with_capacity(page.search_results.len());
        let mut skipped = Vec::new();
        for row in &page.search_results {
            match serde_json::from_str::<wire::FileHeader>(row.get())
                .map_err(|e| format!("unreadable header: {e}"))
                .and_then(wire::to_remote)
            {
                Ok(header) => entries.push(header),
                Err(why) => skipped.push(why),
            }
        }
        if !skipped.is_empty() {
            let mut kept = self.skipped.lock().unwrap_or_else(|e| e.into_inner());
            let room = MAX_SKIPPED.saturating_sub(kept.len());
            kept.extend(skipped.into_iter().take(room));
        }
        Ok(ChangesPage {
            entries,
            // An empty page of an empty drive has no cursor yet: start from the beginning.
            cursor: page
                .cursor_state
                .or_else(|| cursor.map(str::to_owned))
                .unwrap_or_default(),
            has_more: page.has_more_rows,
        })
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        let header: wire::FileHeader = self
            .client
            .get(&format!("{}/header", self.file(file_id)), &[])
            .map_err(remote_error)?;
        wire::to_remote(header).map_err(RemoteError::Fatal)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef> {
        self.write(None, node, payload).map_err(remote_error)
    }

    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        let at = RemoteRef {
            file_id,
            version: expected,
        };
        let error = match self.write(Some(at), meta, payload) {
            Ok(written) => return Ok(written),
            Err(e) => e,
        };
        // odin-core answers a content update of a deleted file with 500 (a null payload list in
        // `ProcessAppendOrOverwrite`, odin-core #1904) instead of 4002: look before retrying.
        if matches!(&error, HomebaseError::Api { error, .. } if error.status == 500) {
            match self.header(file_id) {
                Ok(h) if h.is_deleted() => return Err(RemoteError::NotFound),
                Ok(h) if h.version != expected => return Err(RemoteError::VersionMismatch),
                Err(e @ (RemoteError::Denied(_) | RemoteError::NotFound)) => return Err(e),
                _ => {}
            }
        }
        Err(remote_error(error))
    }

    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        // Check, then act: the server's delete takes no version (odin-core #1903).
        let current = self.header(file_id)?;
        if current.is_deleted() {
            return Ok(()); // a retry; deleting again would rewrite the tombstone
        }
        if current.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        let result: DeleteResult = self
            .client
            .post(
                &format!("{}/delete", self.file(file_id)),
                &json!({ "recipients": [] }),
            )
            .map_err(remote_error)?;
        if result.local_file_not_found {
            return Err(RemoteError::NotFound);
        }
        Ok(())
    }

    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        let range = match range {
            Some(r) => r,
            None => {
                // A range past the end is refused (416): learn the size first.
                let header = self.header(file_id)?;
                let size = match &header.body {
                    hbsync_remote::RemoteBody::Live {
                        payload: Some(p), ..
                    } => p.bytes,
                    _ => return Err(RemoteError::NotFound),
                };
                0..size
            }
        };
        let path = format!("{}/payload/{PAYLOAD_KEY}", self.file(file_id));
        let mut start = range.start;
        while start < range.end {
            let len = (range.end - start).min(READ_CHUNK);
            // The route takes 32-bit start and length; the Range header decides.
            let got = self
                .client
                .download(
                    &format!("{path}/{start}/{len}"),
                    start,
                    start + len - 1,
                    sink,
                )
                .map_err(remote_error)?;
            if got != len {
                return Err(RemoteError::Transient(format!(
                    "short read: {got} of {len} bytes at {start}"
                )));
            }
            start += len;
        }
        Ok(range.end.saturating_sub(range.start))
    }
}

/// How a Homebase answer maps onto the trait's errors (`homebase-api-notes.md`, Errors).
fn remote_error(e: HomebaseError) -> RemoteError {
    match e {
        HomebaseError::Network(why) => RemoteError::Transient(why),
        HomebaseError::Denied(why) => RemoteError::Denied(why),
        HomebaseError::Io(e) => RemoteError::Io(e),
        HomebaseError::Api { what, error } => {
            let by_code = [
                ("existingFileWithUniqueId", RemoteError::AlreadyExists),
                ("versionTagMismatch", RemoteError::VersionMismatch),
                ("fileNotFound", RemoteError::NotFound),
                ("cannotUpdateNonActiveFile", RemoteError::NotFound),
                ("payloadHashMismatch", RemoteError::HashMismatch),
                ("invalidPayloadHash", RemoteError::InvalidHash),
                ("payloadHashRequired", RemoteError::HashRequired),
            ]
            .into_iter()
            .find(|(code, _)| error.is(code));
            if let Some((_, mapped)) = by_code {
                return mapped;
            }
            let why = format!("{what}: {error}");
            match error.status {
                404 | 410 | 416 => RemoteError::NotFound,
                507 => RemoteError::Fatal(format!("out of storage quota: {why}")),
                408 | 429 | 499 | 500..=599 => RemoteError::Transient(why),
                _ => RemoteError::Fatal(why),
            }
        }
        e @ (HomebaseError::Protocol(_) | HomebaseError::Crypto(_) | HomebaseError::Json(_)) => {
            RemoteError::Fatal(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiError;

    fn api(status: u16, code: Option<&str>) -> HomebaseError {
        HomebaseError::Api {
            what: "test".into(),
            error: ApiError {
                status,
                code: code.map(str::to_owned),
                title: String::new(),
            },
        }
    }

    #[test]
    fn errors_map_onto_the_trait() {
        let cases = [
            (api(400, Some("existingFileWithUniqueId")), "AlreadyExists"),
            (api(400, Some("versionTagMismatch")), "VersionMismatch"),
            (api(400, Some("fileNotFound")), "NotFound"),
            (api(400, Some("cannotUpdateNonActiveFile")), "NotFound"),
            (api(400, Some("payloadHashMismatch")), "HashMismatch"),
            (api(400, Some("payloadHashRequired")), "HashRequired"),
            (api(404, None), "NotFound"),
            (api(503, None), "Transient"),
            (api(507, None), "Fatal"),
            (api(400, Some("invalidUpload")), "Fatal"),
            (HomebaseError::Network("reset".into()), "Transient"),
            (HomebaseError::Denied("401".into()), "Denied"),
        ];
        for (error, expected) in cases {
            let mapped = format!("{:?}", remote_error(error));
            assert!(mapped.starts_with(expected), "{mapped} isn't {expected}");
        }
    }
}
