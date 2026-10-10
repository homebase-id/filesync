//! The Filesync drive as a [`Remote`]: node schema v1 on the identity's V2 drive API.
//!
//! The drive is found by its slugs (`GET /api/v2/apps/filesync/drives/filesync`); file routes
//! then take its alias, as own-identity routes by slug don't exist yet (odin-core #1902).
//! Facts: `docs/spec/homebase-api-notes.md`.

use std::io::{Read, SeekFrom, Write};
use std::ops::Range;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use hbsync_fileio::HashingReader;
use hbsync_remote::{
    ChangesPage, NodeMeta, Paced, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::PAYLOAD_KEY;
use hbsync_types::{ContentHash, RemoteFileId, RemoteRef, VersionTag};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::client::{Client, Fetched, UploadMethod};
use crate::crypto::Key;
use crate::drive::sealed::{
    CipherRange, Decrypting, Encrypting, KeyHeader, SealedKeyHeader, sealed_len,
};
use crate::multipart::{Multipart, Part};
use crate::{HomebaseError, app};

pub mod sealed;
pub mod wire;

/// The server counts the bytes of an upload in 32 bits: a larger one fails (#1893).
const MAX_PAYLOAD: u64 = u32::MAX as u64;

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

    /// Creates a node unencrypted, as clients before encryption did: for tests of what they
    /// left on drives.
    #[doc(hidden)]
    pub fn create_unencrypted(
        &self,
        node: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        self.write(None, node, payload, false)
    }

    /// Creates a file (encrypted when `encrypt`), or with `target` updates that file at that
    /// version, in one multipart request. An update keeps the file's mode (the server can't
    /// change it) and its key, with a new header IV.
    fn write(
        &self,
        target: Option<RemoteRef>,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
        encrypt: bool,
    ) -> Result<RemoteRef> {
        let key = match target {
            None if encrypt => Some(KeyHeader::random().map_err(remote_error)?),
            None => None,
            Some(at) => self.update_key(at)?,
        };
        let transfer_iv = sealed::strong().map_err(remote_error)?;
        // The payload as sent: encrypted under a new payload IV, or as it is, and paced.
        let (descriptor, body) = match payload {
            None => (None, None),
            Some(mut p) => {
                let size = key.as_ref().map_or(p.size, |_| sealed_len(p.size));
                if size > MAX_PAYLOAD {
                    return Err(RemoteError::Fatal(format!(
                        "{} bytes is too large for this server (odin-core #1893)",
                        p.size
                    )));
                }
                let pace = p.pace;
                let (descriptor, reader): (_, Box<dyn Read + '_>) = match &key {
                    Some(key) => {
                        let iv = sealed::strong().map_err(remote_error)?;
                        let stored = sealed_hash(&mut p, &key.key, &iv)?;
                        let descriptor = wire::PayloadDescriptor::sealed(key, iv, &p.hash, &stored);
                        (
                            descriptor,
                            Box::new(Encrypting::new(p.reader, &key.key, &iv)),
                        )
                    }
                    None => (wire::PayloadDescriptor::plain(&p.hash), Box::new(p.reader)),
                };
                (
                    Some(descriptor),
                    Some((
                        Paced {
                            inner: reader,
                            pace,
                        },
                        size,
                    )),
                )
            }
        };
        let version = target.map(|t| t.version);
        let instructions = wire::instructions(&transfer_iv, descriptor.as_ref(), target.is_some());
        let metadata = wire::metadata(meta, version, self.client.key(), &transfer_iv, key.as_ref());
        let mut parts = vec![
            Part::bytes(
                "instructions",
                "application/json",
                instructions.to_string().into_bytes(),
            ),
            Part::bytes("metadata", wire::CONTENT_TYPE, metadata),
        ];
        let mut body = body;
        if let Some((reader, size)) = &mut body {
            parts.push(Part::reader(
                "payload",
                PAYLOAD_KEY,
                wire::CONTENT_TYPE,
                reader,
                *size,
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

    /// The key an update of `at` sends: the file's own, with a new header IV. `None` when the
    /// file isn't encrypted. A file deleted or changed since is found here, before the upload.
    fn update_key(&self, at: RemoteRef) -> Result<Option<KeyHeader>> {
        self.current(at)?
            .key(self.client.key())
            .and_then(|key| key.map(|k| k.with_new_iv()).transpose())
            .map_err(remote_error)
    }

    /// The plaintext size of a payload, for a read without a range.
    fn payload_size(&self, file_id: RemoteFileId) -> Result<u64> {
        let header = self.raw_header(file_id)?;
        let stored = header
            .file_metadata
            .sync_payload()
            .ok_or(RemoteError::NotFound)?
            .bytes_written;
        if !header.file_metadata.is_encrypted {
            return Ok(stored);
        }
        match wire::to_remote(header, self.client.key())
            .map_err(RemoteError::Fatal)?
            .body
        {
            RemoteBody::Live { content, .. } => content
                .size
                .ok_or_else(|| RemoteError::Fatal("an encrypted file without its size".into())),
            RemoteBody::Deleted => Err(RemoteError::NotFound),
        }
    }

    /// The bytes `pieces` covers into `sink`: `response` answers its first piece, then a request
    /// per piece. Bytes from `need` on are optional: a range past the payload's end is cut
    /// short there (RFC 9110), and an unencrypted payload ends before the last whole block.
    fn read_ranges(
        &self,
        path: &str,
        mut pieces: Pieces,
        need: u64,
        mut response: Fetched,
        sink: &mut dyn Write,
    ) -> Result<()> {
        loop {
            let asked = pieces.asked();
            let len = asked.end - asked.start;
            let got = response.copy_to(sink, len).map_err(remote_error)?;
            if asked.start + got >= need {
                return Ok(());
            }
            if got != len {
                return Err(RemoteError::Transient(format!(
                    "short read: {got} of {len} bytes at {}",
                    asked.start
                )));
            }
            pieces.done(got);
            response = self.fetch(path, pieces.next()).map_err(remote_error)?;
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

/// The hash of `upload` encrypted with `key` under `iv`: what the server checks the bytes
/// against. Reads the source once, then goes back to where it started. Its plaintext must
/// still hash to `upload.hash`: the server can't tell from ciphertext that the file changed
/// since it was hashed.
fn sealed_hash(upload: &mut Upload<'_>, key: &Key, iv: &[u8; 16]) -> Result<ContentHash> {
    let algorithm = upload.hash.algorithm;
    let reader = &mut *upload.reader;
    let start = reader.stream_position()?;
    let mut sealing = Encrypting::new(HashingReader::new(&mut *reader, algorithm), key, iv);
    let (stored, _) = hbsync_fileio::hash_reader(&mut sealing, algorithm)?;
    let (content, len) = sealing.into_inner().finish();
    if content != upload.hash || len != upload.size {
        return Err(RemoteError::HashMismatch);
    }
    reader.seek(SeekFrom::Start(start))?;
    Ok(stored)
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
                .and_then(|h| wire::to_remote(h, self.client.key()))
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
        wire::to_remote(self.raw_header(file_id)?, self.client.key()).map_err(RemoteError::Fatal)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef> {
        self.write(None, node, payload, true)
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
        self.write(Some(at), meta, payload, true)
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

    /// Plaintext `range` of the payload, decrypted when the file is encrypted. Whether it is,
    /// and its key, come with the first response. It starts a block early (ciphertext needs
    /// the block before, plaintext drops it) and ends at the whole block encryption needs; an
    /// unencrypted payload's answer ends at the payload's end.
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
        let mut pieces = Pieces::new(at.fetch.clone());
        let first = self.fetch(&path, pieces.next()).map_err(remote_error)?;
        let left = match first.key_header.as_deref() {
            Some(sealed) if first.encrypted => {
                // A payload's key header carries the payload's IV.
                let key = SealedKeyHeader::from_base64(sealed)
                    .and_then(|s| KeyHeader::open(&s, self.client.key()))
                    .map_err(remote_error)?;
                let first_iv = (!at.iv_first).then_some(&key.iv);
                let mut plain = Decrypting::new(&key.key, first_iv, at.skip, len, sink);
                self.read_ranges(&path, pieces, at.fetch.end, first, &mut plain)?;
                plain.left()
            }
            None if first.encrypted => {
                return Err(RemoteError::Fatal(format!("{path}: encrypted, but no key")));
            }
            _ => {
                let mut plain = Window {
                    sink,
                    skip: range.start - at.fetch.start,
                    left: len,
                };
                self.read_ranges(&path, pieces, range.end, first, &mut plain)?;
                plain.left
            }
        };
        if left > 0 {
            return Err(RemoteError::Transient(format!(
                "short read: {} of {len} bytes",
                len - left
            )));
        }
        Ok(len)
    }
}

/// The first ranged request of a read: small files fit in it whole.
const FIRST_PIECE: u64 = 8 << 20;
/// How long a later piece should take at the rate the one before came in: well inside the
/// client's body timeout, so a stalled connection is caught, and long enough that the round
/// trip between pieces doesn't matter.
const PIECE_TIME: f64 = 15.0;
const MIN_PIECE: u64 = 1 << 20;
const MAX_PIECE: u64 = 256 << 20;

/// The ranged requests a read is made of, each sized from the rate the one before came in at.
#[derive(Debug)]
struct Pieces {
    /// What is left to read.
    left: Range<u64>,
    size: u64,
    /// When the last piece was asked for.
    since: Instant,
}

impl Pieces {
    fn new(range: Range<u64>) -> Self {
        Pieces {
            left: range,
            size: FIRST_PIECE,
            since: Instant::now(),
        }
    }

    /// The next piece to ask for.
    fn next(&mut self) -> Range<u64> {
        self.since = Instant::now();
        self.asked()
    }

    /// The piece asked for last.
    fn asked(&self) -> Range<u64> {
        self.left.start..self.left.end.min(self.left.start + self.size)
    }

    /// The piece asked for came in whole.
    fn done(&mut self, bytes: u64) {
        self.left.start += bytes;
        let took = self.since.elapsed().max(Duration::from_millis(1));
        let fits = bytes as f64 / took.as_secs_f64() * PIECE_TIME;
        self.size = (fits as u64).clamp(MIN_PIECE, MAX_PIECE);
    }
}

/// Passes on `left` bytes after the first `skip` written to it.
struct Window<'a> {
    sink: &'a mut dyn Write,
    skip: u64,
    left: u64,
}

impl Write for Window<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let skip = self.skip.min(buf.len() as u64) as usize;
        self.skip -= skip as u64;
        let keep = self.left.min((buf.len() - skip) as u64) as usize;
        self.sink.write_all(&buf[skip..skip + keep])?;
        self.left -= keep as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
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

    #[test]
    fn pieces_follow_the_rate() {
        let mut pieces = Pieces::new(100..u64::MAX);
        assert_eq!(pieces.next(), 100..100 + FIRST_PIECE);
        // Came in at once: as large as a piece gets.
        pieces.done(FIRST_PIECE);
        assert_eq!(
            pieces.next(),
            100 + FIRST_PIECE..100 + FIRST_PIECE + MAX_PIECE
        );
        // A piece that took a while: smaller, but never below the floor.
        pieces.since -= Duration::from_secs(6000);
        pieces.done(MAX_PIECE);
        assert_eq!(pieces.size, MIN_PIECE);
        // About PIECE_TIME at the rate it came in.
        pieces.next();
        pieces.since -= Duration::from_secs(1);
        pieces.done(MIN_PIECE);
        let want = MIN_PIECE as f64 * PIECE_TIME;
        assert!(
            (pieces.size as f64 - want).abs() < want * 0.05,
            "{}",
            pieces.size
        );
        // The last piece ends with the range.
        let mut pieces = Pieces::new(0..10);
        assert_eq!(pieces.next(), 0..10);
    }
}
