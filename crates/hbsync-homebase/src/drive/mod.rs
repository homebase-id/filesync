//! The Filesync drive as a [`Remote`]: node schema v1 on the identity's V2 drive API.
//!
//! The drive is found by its slugs (`GET /api/v2/apps/filesync/drives/filesync`); file routes
//! then take its alias, as own-identity routes by slug don't exist yet (odin-core #1902).
//! Facts: `docs/spec/homebase-api-notes.md`.

use std::cell::RefCell;
use std::io::{Read, Write};
use std::ops::Range;
use std::rc::Rc;

use hbsync_fileio::{HashAlgorithm, Hasher};
use hbsync_remote::{
    ChangesPage, NodeMeta, Paced, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::{NodeContent, PAYLOAD_KEY};
use hbsync_types::{ContentHash, RemoteFileId, RemoteRef, VersionTag};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::client::{Client, Fetched, UploadMethod};
use crate::drive::sealed::{
    CipherRange, Decrypting, Encrypting, KeyHeader, SealedKeyHeader, sealed_len,
};
use crate::multipart::{Multipart, Part, SourceEnded};
use crate::{HomebaseError, app};

pub mod sealed;
pub mod wire;

/// The server counts the bytes of an upload in 32 bits: a larger one fails (#1893).
const MAX_PAYLOAD: u64 = u32::MAX as u64;

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
        })
    }

    pub fn drive(&self) -> TargetDrive {
        self.drive
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The route of the drive's files.
    pub fn files(&self) -> String {
        format!("/api/v2/drives/{}/files", self.drive.alias.simple())
    }

    /// The route of one file.
    pub fn file(&self, file_id: RemoteFileId) -> String {
        format!("{}/{}", self.files(), file_id.0)
    }

    /// A file's header as the server sends it.
    pub fn raw_header(&self, file_id: RemoteFileId) -> Result<wire::FileHeader> {
        self.client
            .get(&format!("{}/header", self.file(file_id)), &[])
            .map_err(remote_error)
    }

    /// Creates a file, or with `target` updates that file at that version, in one multipart
    /// request, encrypted. An update keeps the file's key, with a new header IV.
    fn write(
        &self,
        target: Option<RemoteRef>,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        let key = match target {
            None => KeyHeader::random().map_err(remote_error)?,
            Some(at) => self.update_key(at)?,
        };
        let transfer_iv = sealed::strong().map_err(remote_error)?;
        // The payload as sent: read once, hashed, encrypted under a new payload IV, hashed
        // again and paced. Its hashes follow it (odin-core #1935): the descriptor says only the
        // algorithm.
        let mut sending = match payload {
            None => None,
            Some(p) => {
                let size = sealed_len(p.size);
                if size > MAX_PAYLOAD {
                    return Err(RemoteError::Fatal(format!(
                        "{} bytes is too large for this server (odin-core #1893)",
                        p.size
                    )));
                }
                let iv = sealed::strong().map_err(remote_error)?;
                let (plain, sent) = (tally(p.hash.algorithm), tally(p.hash.algorithm));
                let reader = Paced {
                    inner: tap(Encrypting::new(tap(p.reader, &plain), &key.key, &iv), &sent),
                    pace: p.pace,
                };
                Some(Sending {
                    descriptor: wire::PayloadDescriptor {
                        algorithm: p.hash.algorithm,
                        iv,
                    },
                    reader,
                    planned: (p.hash, p.size),
                    plain,
                    sent,
                })
            }
        };
        let version = target.map(|t| t.version);
        let instructions = wire::instructions(
            &transfer_iv,
            sending.as_ref().map(|s| &s.descriptor),
            target.is_some(),
        );
        let metadata = wire::metadata(meta, version, self.client.key(), &transfer_iv, &key);
        let mut parts = vec![
            Part::bytes(
                "instructions",
                "application/json",
                instructions.to_string().into_bytes(),
            ),
            Part::bytes("metadata", wire::CONTENT_TYPE, metadata),
        ];
        if let Some(s) = &mut sending {
            let hash_part = s.hash_part(&key);
            parts.push(Part::reader(
                "payload",
                PAYLOAD_KEY,
                wire::CONTENT_TYPE,
                &mut s.reader,
                sealed_len(s.planned.1),
            ));
            parts.push(Part::later(
                "payloadHash",
                PAYLOAD_KEY,
                "application/json",
                wire::PAYLOAD_HASH_LEN as u64,
                hash_part,
            ));
        }
        let (method, path) = match target {
            None => (UploadMethod::Post, self.files()),
            Some(at) => (UploadMethod::Patch, self.file(at.file_id)),
        };
        let boundary = Uuid::new_v4().simple().to_string();
        match self
            .client
            .upload(method, &path, Multipart::new(boundary, parts))
        {
            Ok(WriteResult {
                file_id,
                new_version_tag,
            }) => Ok(RemoteRef {
                file_id: RemoteFileId(file_id),
                version: VersionTag(new_version_tag),
            }),
            // odin-core answers a content update of a file deleted since `update_key` looked
            // with 500 (a null payload list in `ProcessAppendOrOverwrite`, odin-core #1904)
            // instead of 4002: look again before retrying.
            Err(HomebaseError::Api { ref error, .. }) if error.status == 500 => {
                if let Some(at) = target {
                    self.current(at)?;
                }
                Err(RemoteError::Transient(format!("{path}: {error}")))
            }
            Err(e) => Err(remote_error(e)),
        }
    }

    /// The header of `at`, which must still be that version and not deleted.
    fn current(&self, at: RemoteRef) -> Result<wire::FileHeader> {
        let header = self.raw_header(at.file_id)?;
        if header.file_state == "deleted" {
            return Err(RemoteError::NotFound);
        }
        if header.file_metadata.version_tag != at.version.0 {
            return Err(RemoteError::VersionMismatch);
        }
        Ok(header)
    }

    /// The key an update of `at` sends: the file's own, with a new header IV. A file deleted
    /// or changed since is found here, before the upload.
    fn update_key(&self, at: RemoteRef) -> Result<KeyHeader> {
        self.current(at)?
            .key(self.client.key())
            .and_then(|key| key.with_new_iv())
            .map_err(remote_error)
    }

    /// The plaintext size of a payload, for a read without a range.
    fn payload_size(&self, file_id: RemoteFileId) -> Result<u64> {
        match self.header(file_id)?.body {
            RemoteBody::Live {
                payload: Some(_),
                content: NodeContent {
                    size: Some(size), ..
                },
                ..
            } => Ok(size),
            _ => Err(RemoteError::NotFound),
        }
    }

    /// Stored bytes `range` of the payload at `path`: one request.
    fn fetch(&self, path: &str, range: Range<u64>) -> Result<Fetched, HomebaseError> {
        // The route names the range as well; the Range header decides.
        let (start, len) = (range.start, range.end - range.start);
        self.client
            .fetch(&format!("{path}/{start}/{len}"), start, range.end - 1)
    }
}

/// A payload on its way: what `write` sends, and the hashes of what went.
struct Sending<'a> {
    descriptor: wire::PayloadDescriptor,
    reader: Paced<'a, Tap<Encrypting<Tap<&'a mut dyn Read>>>>,
    /// The plaintext's hash and size the upload was planned with.
    planned: (ContentHash, u64),
    plain: Tally,
    sent: Tally,
}

impl Sending<'_> {
    /// Makes the `payloadHash` part once the payload is sent. The server can't tell from
    /// ciphertext that the file changed since it was hashed: if the plaintext isn't what was
    /// planned, values no bytes hash to make it refuse the payload.
    fn hash_part<'k>(&self, key: &'k KeyHeader) -> impl FnOnce() -> Vec<u8> + 'k {
        let (plain, sent, planned, iv) = (
            self.plain.clone(),
            self.sent.clone(),
            self.planned,
            self.descriptor.iv,
        );
        move || {
            let (content, len) = finish(&plain);
            if (content, len) != planned {
                return wire::refused_payload_hash();
            }
            wire::payload_hash(key, &iv, &content, &finish(&sent).0)
        }
    }
}

/// The hash and length of what passed through a [`Tap`], shared: the body still holds the
/// reader when its hash part is made.
type Tally = Rc<RefCell<(Hasher, u64)>>;

fn tally(algorithm: HashAlgorithm) -> Tally {
    Rc::new(RefCell::new((Hasher::new(algorithm), 0)))
}

fn finish(tally: &Tally) -> (ContentHash, u64) {
    let (hasher, len) = &*tally.borrow();
    (hasher.clone().finalize(), *len)
}

/// `inner`, hashed and counted into `tally` as it is read.
fn tap<R>(inner: R, tally: &Tally) -> Tap<R> {
    Tap {
        inner,
        tally: Rc::clone(tally),
    }
}

struct Tap<R> {
    inner: R,
    tally: Tally,
}

impl<R: Read> Read for Tap<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        let (hasher, len) = &mut *self.tally.borrow_mut();
        hasher.update(&buf[..n]);
        *len += n as u64;
        Ok(n)
    }
}

impl Remote for HomebaseRemote {
    /// Rows that aren't usable sync nodes come back in [`ChangesPage::skipped`], with why.
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
                .and_then(|h| wire::to_remote(h, self.client.key()))
            {
                Ok(header) => entries.push(header),
                Err(why) => skipped.push(why),
            }
        }
        Ok(ChangesPage {
            entries,
            // An empty page of an empty drive has no cursor yet: start from the beginning.
            cursor: page
                .cursor_state
                .or_else(|| cursor.map(str::to_owned))
                .unwrap_or_default(),
            has_more: page.has_more_rows,
            skipped,
        })
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        wire::to_remote(self.raw_header(file_id)?, self.client.key()).map_err(RemoteError::Fatal)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef> {
        self.write(None, node, payload)
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
        self.write(Some(at), meta, payload)
    }

    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        // The server deletes only at `expected` (odin-core #1903), else 4160.
        let result: DeleteResult = match self
            .client
            .post(
                &format!("{}/delete", self.file(file_id)),
                &json!({ "recipients": [], "versionTag": expected.0 }),
            )
            .map_err(remote_error)
        {
            Ok(result) => result,
            // A retry: deleting gave the tombstone a version of its own. The raw header is
            // enough: a row that doesn't decrypt is still a tombstone. A header that can't be
            // read keeps the server's answer.
            Err(RemoteError::VersionMismatch)
                if self
                    .raw_header(file_id)
                    .is_ok_and(|h| h.file_state == "deleted") =>
            {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if result.local_file_not_found {
            return Err(RemoteError::NotFound);
        }
        Ok(())
    }

    /// Plaintext `range` of the payload, decrypted with the key that comes with the first
    /// response. It starts a block early (ciphertext needs the block before, plaintext drops
    /// it) and ends at the whole block encryption needs. A range that reaches the payload's
    /// last block has its padding removed, and comes back short if it runs past the plaintext.
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        let range = match range {
            Some(r) => r,
            None => 0..self.payload_size(file_id)?,
        };
        if range.is_empty() {
            return Ok(0);
        }
        let len = range.end - range.start;
        let path = format!("{}/payload/{PAYLOAD_KEY}", self.file(file_id));
        let at = CipherRange::of(&range);
        // One request, streamed: the client's idle timeout catches a stalled one.
        let mut response = self.fetch(&path, at.fetch.clone()).map_err(remote_error)?;
        let sealed = match (response.encrypted, response.key_header.as_deref()) {
            (true, Some(sealed)) => sealed,
            (true, None) => {
                return Err(RemoteError::Fatal(format!("{path}: encrypted, but no key")));
            }
            (false, _) => return Err(RemoteError::Fatal(format!("{path}: not encrypted"))),
        };
        // A payload's key header carries the payload's IV.
        let key = SealedKeyHeader::from_base64(sealed)
            .and_then(|s| KeyHeader::open(&s, self.client.key()))
            .map_err(remote_error)?;
        // A range past the stored payload is cut short at its end (odin-core #1921).
        let end = at.fetch.end.min(response.size);
        let first_iv = (!at.iv_first).then_some(&key.iv);
        let mut plain = Decrypting::new(&key.key, first_iv, at.skip, len, sink);
        let want = end.saturating_sub(at.fetch.start);
        let got = response.copy_to(&mut plain, want).map_err(remote_error)?;
        if got != want {
            return Err(RemoteError::Transient(format!(
                "{path}: short read, {got} of {want} bytes at {}",
                at.fetch.start
            )));
        }
        plain.finish(end == response.size)?;
        Ok(len - plain.left())
    }
}

/// How a Homebase answer maps onto the trait's errors (`homebase-api-notes.md`, Errors).
fn remote_error(e: HomebaseError) -> RemoteError {
    match e {
        HomebaseError::Network(why) => RemoteError::Transient(why),
        HomebaseError::Denied(why) => RemoteError::Denied(why),
        // A source that ended early changed since it was hashed.
        HomebaseError::Io(e) if e.get_ref().is_some_and(|e| e.is::<SourceEnded>()) => {
            RemoteError::HashMismatch
        }
        HomebaseError::Io(e) => RemoteError::Io(e),
        HomebaseError::Api { what, error } => {
            let by_code = [
                ("existingFileWithUniqueId", RemoteError::AlreadyExists),
                ("versionTagMismatch", RemoteError::VersionMismatch),
                ("fileNotFound", RemoteError::NotFound),
                ("cannotUpdateNonActiveFile", RemoteError::NotFound),
                ("payloadHashMismatch", RemoteError::HashMismatch),
                // An upload whose hash part didn't arrive: sent again like one refused.
                ("payloadHashMissing", RemoteError::HashMismatch),
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
                507 => RemoteError::Full(why),
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
            (api(400, Some("payloadHashMissing")), "HashMismatch"),
            (api(400, Some("payloadHashRequired")), "HashRequired"),
            (api(404, None), "NotFound"),
            (api(503, None), "Transient"),
            (api(507, None), "Full"),
            (api(400, Some("invalidUpload")), "Fatal"),
            (HomebaseError::Network("reset".into()), "Transient"),
            (HomebaseError::Denied("401".into()), "Denied"),
            // The upload source: one that ended early changed; one that can't be read is local.
            (
                HomebaseError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    SourceEnded { missing: 1 },
                )),
                "HashMismatch",
            ),
            (HomebaseError::Io(std::io::Error::other("locked")), "Io"),
        ];
        for (error, expected) in cases {
            let mapped = format!("{:?}", remote_error(error));
            assert!(mapped.starts_with(expected), "{mapped} isn't {expected}");
        }
    }
}
