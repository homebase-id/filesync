//! The Filesync drive as a [`Remote`]: node schema v1 on the identity's V2 drive API.
//!
//! The drive is found by its slugs (`GET /api/v2/apps/filesync/drives/filesync`); file routes
//! then take its alias, as own-identity routes by slug don't exist yet (odin-core #1902).
//! Facts: `docs/spec/homebase-api-notes.md`.

use std::cell::RefCell;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::ops::Range;
use std::rc::Rc;
use std::sync::Mutex;

use hbsync_fileio::{HashAlgorithm, Hasher};
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ErrorClass, ForGood, Holds, NodeMeta, Paced, Planned, RefsPage,
    Remote, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::{ARCHIVAL_REMOVED, NodeContent, PAYLOAD_KEY};
use hbsync_types::{
    ContentHash, Doomed, NodeId, RemoteFileId, RemoteRef, Target, VersionTag, Written,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::client::{Client, Fetched, UploadMethod};
use crate::crypto::{Key, zk_unlock};
use crate::drive::sealed::{
    CipherRange, Decrypting, Encrypting, KeyHeader, SealedKeyHeader, sealed_len,
};
use crate::multipart::{Multipart, Part, SourceEnded};
use crate::{ApiError, HomebaseError, app};

pub mod sealed;
pub mod wire;

use wire::Keys;

/// Why a private drive refuses this computer: it wasn't handed the app's zero-knowledge key
/// (a login from before it asked for one), or the key it was handed opens the drive's no more.
const PRIVATE: &str = "the Filesync drive on your Homebase cloud is private now, and this \
                       computer doesn't hold its key";

/// The largest file uploaded: 50 GiB. Not the server's limit (odin-core counts an upload's
/// bytes in 64 bits since PR #1932, #1893, **[verified]**), a sanity cap for one upload: it has no resume, so
/// one that fails is sent again from the start (resumable upload is a tabled odin-core ask). A
/// larger file is refused ([`check_size`]).
const MAX_FILE: u64 = 50 << 30;
/// Content updates at least this large read the file's header first, token or not: one small
/// request, against sending all of it for a refusal (the file changed or went meanwhile).
const LOOK_FIRST: u64 = 8 << 20;

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
    /// When the drive was made ([`HomebaseRemote::made`]).
    made: i64,
    /// The drive's key, once the drive is private ([`HomebaseRemote::drive_key`]).
    drive_key: DriveKey,
}

/// A private drive's key, while this computer holds it.
#[derive(Default)]
struct DriveKey(Mutex<Option<Key>>);

impl std::fmt::Debug for DriveKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DriveKey(..)")
    }
}

/// The security context (`GET /api/v2/auth/context`, odin-core `RedactedOdinContext`): the
/// drives the app is granted, and on a private drive its key locked with the app's
/// zero-knowledge key (`zkAppKeyEncryptedStorageKey`, `docs/zero-knowledge-drive-api-plan.md`,
/// 3.5).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Context {
    permission_context: Option<Permissions>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Permissions {
    #[serde(default)]
    permission_groups: Vec<Group>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Group {
    #[serde(default)]
    drive_grants: Vec<Grant>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Grant {
    permissioned_drive: PermissionedDrive,
    zk_app_key_encrypted_storage_key: Option<String>,
}

#[derive(Deserialize)]
struct PermissionedDrive {
    drive: TargetDrive,
}

/// The drive's key if it is private, opened with the app's zero-knowledge key (one request);
/// `None` if it isn't. A private drive this login can't open refuses this computer: log in
/// again.
fn drive_key_of(client: &Client, drive: &TargetDrive) -> Result<Option<Key>, HomebaseError> {
    let context: Context = client.get("/api/v2/auth/context", &[])?;
    let locked = context
        .permission_context
        .into_iter()
        .flat_map(|p| p.permission_groups)
        .flat_map(|g| g.drive_grants)
        .filter(|g| g.permissioned_drive.drive == *drive)
        .find_map(|g| g.zk_app_key_encrypted_storage_key);
    let Some(locked) = locked else {
        return Ok(None);
    };
    let denied = || HomebaseError::Denied(PRIVATE.into());
    let app_key = client.zk_app_key().ok_or_else(denied)?;
    let key = zk_unlock(app_key, &locked).map_err(|_| denied())?;
    key.try_into()
        .map(Some)
        .map_err(|_| HomebaseError::Protocol("the drive's key isn't 16 bytes".into()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveAnswer {
    target_drive: TargetDrive,
    /// When the drive was made, Unix ms (the identity's clock).
    created: i64,
}

/// A create's or update's answer (`CreateFileResult`, `UpdateFileResult`): the version written
/// and its time, the one the feed shows for it (`updated`, odin-core #1971; null only for a
/// peer-only update, which this client never makes).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriteResult {
    file_id: Uuid,
    new_version_tag: Uuid,
    #[serde(default)]
    updated: Option<i64>,
}

pub use hbsync_remote::BATCH_FILES;

/// The answer of a batch write with an answer per file, in request order (`soft-delete-batch`,
/// `hard-delete-batch`, `update-archival-status-batch`).
#[derive(Deserialize)]
struct BatchAnswer<T> {
    results: Vec<T>,
}

/// `SoftDeleteResultV2` (verified in odin-core); its `recipientStatus` is left out (this client
/// names no recipients).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SoftDeleted {
    file_id: Uuid,
    /// `None`: deleted, a tombstone already too. Else why it stayed, as `OdinClientErrorCode`'s
    /// camelCase name.
    error_code: Option<String>,
    /// The tombstone's time (Unix ms) as the feed shows it; `None` when it stayed.
    updated: Option<i64>,
}

/// One file's answer to a soft delete: its tombstone's time.
fn soft_deleted(answer: &SoftDeleted) -> Result<Option<i64>> {
    match &answer.error_code {
        None => Ok(answer.updated),
        Some(code) => Err(refused("soft delete", code)),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HardDeleted {
    file_id: Uuid,
    /// `None`: gone. Else why it stayed, as `OdinClientErrorCode`'s camelCase name.
    error_code: Option<String>,
}

/// One file's answer to a hard delete.
fn hard_deleted(answer: &HardDeleted) -> Result<()> {
    match &answer.error_code {
        None => Ok(()),
        Some(code) => Err(refused("hard delete", code)),
    }
}

/// `UpdateArchivalStatusResultV2` (verified in odin-core).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArchivalSet {
    file_id: Uuid,
    /// The version after it; the one asked for when the file was already so. `None` with an
    /// error.
    new_version_tag: Option<Uuid>,
    /// Its time after it (Unix ms), as the feed shows it.
    updated: Option<i64>,
    error_code: Option<String>,
}

/// One file's answer to a removal or restore of `asked`: the file as written, its token kept
/// (its key didn't change).
fn archival_set(answer: &ArchivalSet, asked: &Target) -> Result<Written> {
    if let Some(code) = &answer.error_code {
        return Err(refused("archival status", code));
    }
    let version = answer.new_version_tag.ok_or_else(|| {
        RemoteError::Fatal("update-archival-status-batch answered no version".into())
    })?;
    Ok(Written {
        target: Target {
            at: RemoteRef {
                file_id: asked.at.file_id,
                version: VersionTag(version),
            },
            token: asked.token.clone(),
        },
        changed_at: answer.updated,
        content: None,
    })
}

/// Why a batch write (`what`) left a file as it was, by its `OdinClientErrorCode` (camelCase):
/// written since, not there, a tombstone, or anything else.
fn refused(what: &str, code: &str) -> RemoteError {
    code_error(code).unwrap_or_else(|| RemoteError::Fatal(format!("{what} refused: {code}")))
}

impl HomebaseRemote {
    /// Finds the Filesync drive on the client's identity: [`HomebaseError::NoDrive`] when the
    /// identity says nothing answers at its address.
    pub fn connect(client: Client) -> Result<Self, HomebaseError> {
        let route = format!(
            "/api/v2/apps/{}/drives/{}",
            app::APP_SLUG,
            app::SYNC_DRIVE.slug
        );
        let answer: DriveAnswer = client.get(&route, &[]).map_err(|e| match e {
            HomebaseError::Api { error, .. } if no_drive(&error) => {
                HomebaseError::NoDrive(client.identity().to_owned())
            }
            e => e,
        })?;
        let drive_key = drive_key_of(&client, &answer.target_drive)?;
        Ok(HomebaseRemote {
            client,
            drive: answer.target_drive,
            made: answer.created,
            drive_key: DriveKey(Mutex::new(drive_key)),
        })
    }

    /// The private drive's key: the one held, or when `needed` and none is (the drive was made
    /// private since this connected), fetched. A private drive whose key this login can't open
    /// refuses this computer ([`RemoteError::Denied`]).
    fn drive_key(&self, needed: bool) -> Result<Option<Key>> {
        let mut held = self.drive_key.0.lock().unwrap_or_else(|e| e.into_inner());
        if held.is_none() && needed {
            let key = drive_key_of(&self.client, &self.drive).map_err(remote_error)?;
            *held = Some(key.ok_or_else(|| {
                RemoteError::Fatal(
                    "a file locked with the drive's key, on a drive that isn't private".into(),
                )
            })?);
        }
        Ok(*held)
    }

    /// `header` as a [`RemoteHeader`], opened with the drive's key when locked with it.
    fn to_remote(
        &self,
        header: wire::FileHeader,
    ) -> Result<std::result::Result<RemoteHeader, String>> {
        let drive = self.drive_key(header.locked_with_drive_key())?;
        Ok(wire::to_remote(header, self.keys(drive.as_ref())))
    }

    fn keys<'a>(&'a self, drive: Option<&'a Key>) -> Keys<'a> {
        Keys {
            shared: self.client.key(),
            drive,
        }
    }

    /// A write's error: one the drive refused for its key header's kind was made private since
    /// this connected; with its key fetched (or this computer refused), it is written again.
    fn write_error(&self, e: HomebaseError) -> RemoteError {
        if let HomebaseError::Api { error, .. } = &e
            && error
                .code
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case("driveIsZeroKnowledge"))
        {
            return match self.drive_key(true) {
                Ok(_) => RemoteError::Transient(format!("{e}: the drive was made private")),
                Err(e) => e,
            };
        }
        remote_error(e)
    }

    /// When the drive was made, as the identity said as it was found: Unix ms, its clock. A
    /// drive deleted and made again has the same alias and slugs, and another time.
    pub fn made(&self) -> i64 {
        self.made
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

    /// Creates a file, or with `target` updates that file at its version (the key from its
    /// token, if given), in one multipart request, encrypted. An update keeps the file's key,
    /// with a new header IV. Returns the file as written, with the token the server will give
    /// for this version.
    fn write(
        &self,
        target: Option<&Target>,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        // Before any request: a file too large costs nothing.
        if let Some(p) = &payload {
            check_size(p.size)?;
        }
        let at = target.map(|t| t.at);
        let look_first = payload.as_ref().is_some_and(|p| p.size >= LOOK_FIRST);
        let key = match target {
            None => KeyHeader::random().map_err(remote_error)?,
            Some(t) => self.update_key(t, look_first)?,
        };
        let transfer_iv = sealed::strong().map_err(remote_error)?;
        // The payload as sent: read once, hashed, encrypted under a new payload IV, hashed
        // again and paced. Its hashes follow it (odin-core #1935): the descriptor says only the
        // algorithm.
        let mut sending = match payload {
            None => None,
            Some(p) => {
                let iv = sealed::strong().map_err(remote_error)?;
                let (plain, sent) = (tally(p.hash.algorithm()), tally(p.hash.algorithm()));
                let reader = Paced {
                    inner: tap(Encrypting::new(tap(p.reader, &plain), &key.key, &iv), &sent),
                    pace: p.pace,
                };
                Some(Sending {
                    descriptor: wire::PayloadDescriptor {
                        algorithm: p.hash.algorithm(),
                        iv,
                    },
                    reader,
                    planned: (p.hash, p.size),
                    plain,
                    sent,
                })
            }
        };
        let version = at.map(|t| t.version);
        let instructions = wire::instructions(
            &transfer_iv,
            sending.as_ref().map(|s| &s.descriptor),
            target.is_some(),
        );
        let drive = self.drive_key(false)?;
        let metadata = wire::metadata(meta, version, self.keys(drive.as_ref()), &transfer_iv, &key);
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
        let (method, path) = match at {
            None => (UploadMethod::Post, self.files()),
            Some(at) => (UploadMethod::Patch, self.file(at.file_id)),
        };
        let boundary = Uuid::new_v4().simple().to_string();
        let WriteResult {
            file_id,
            new_version_tag,
            updated,
        } = self
            .client
            .upload(method, &path, Multipart::new(boundary, parts))
            .map_err(|e| self.write_error(e))?;
        Ok(Written {
            target: Target {
                at: RemoteRef {
                    file_id: RemoteFileId(file_id),
                    version: VersionTag(new_version_tag),
                },
                token: Some(key.token(self.client.key())),
            },
            changed_at: updated,
            // The plaintext's hash, as its hash part sent it (the server took it).
            content: sending.as_ref().map(|s| finish(&s.plain).0),
        })
    }

    /// One page of the change feed in `shape`,
    /// and the cursor after it.
    fn feed_page(
        &self,
        cursor: Option<&str>,
        limit: usize,
        shape: wire::Shape,
    ) -> Result<(wire::QueryBatchResponse, String)> {
        let mut page: wire::QueryBatchResponse = self
            .client
            .post(
                &format!("{}/query-batch", self.files()),
                &wire::feed_query(cursor, limit.max(1), shape),
            )
            .map_err(remote_error)?;
        // An empty page of an empty drive has no cursor yet: start from the beginning.
        let next = page
            .cursor_state
            .take()
            .or_else(|| cursor.map(str::to_owned))
            .unwrap_or_default();
        Ok((page, next))
    }

    /// The header of `at`, which must still be that version and not deleted.
    fn current(&self, at: RemoteRef) -> Result<wire::FileHeader> {
        let header = self.raw_header(at.file_id)?;
        if header.file_state == "deleted" {
            return Err(RemoteError::Deleted);
        }
        if header.file_metadata.version_tag != at.version.0 {
            return Err(RemoteError::VersionMismatch);
        }
        Ok(header)
    }

    /// The key an update of `target` sends: the file's own, with a new header IV. From its
    /// token when that opens: the server's version guard (4160) refuses the write if the file
    /// changed since. Else, or to `look_first`, from its header, read first: a file changed or
    /// deleted since is found then.
    fn update_key(&self, target: &Target, look_first: bool) -> Result<KeyHeader> {
        let shared = self.client.key();
        let opened = target
            .token
            .as_ref()
            .filter(|_| !look_first)
            .and_then(SealedKeyHeader::from_token)
            .and_then(|sealed| KeyHeader::open(&sealed, shared).ok());
        let key = match opened {
            Some(key) => key,
            None => {
                let header = self.current(target.at)?;
                let drive = self.drive_key(header.locked_with_drive_key())?;
                header
                    .key(self.keys(drive.as_ref()))
                    .map_err(remote_error)?
            }
        };
        key.with_new_iv().map_err(remote_error)
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

    /// A private drive's payload key: the file key from its header, with the payload's IV. A
    /// header read after the payload was written over has another IV: the bytes then don't
    /// match their hash, and the download is tried again.
    fn payload_key(&self, file_id: RemoteFileId) -> Result<KeyHeader> {
        let header = self.raw_header(file_id)?;
        let drive = self.drive_key(true)?;
        let key = header
            .key(self.keys(drive.as_ref()))
            .map_err(remote_error)?;
        let payload = header
            .file_metadata
            .sync_payload()
            .ok_or_else(|| RemoteError::Fatal(format!("file {}: no payload", file_id.0)))?;
        let iv = wire::payload_iv(payload).map_err(RemoteError::Fatal)?;
        Ok(KeyHeader { iv, key: key.key })
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
    /// The plaintext's hash (or hashed on send) and size the upload was planned with.
    planned: (Planned<'a>, u64),
    plain: Tally,
    sent: Tally,
}

impl<'a> Sending<'a> {
    /// Makes the `payloadHash` part once the payload is sent. The server can't tell from
    /// ciphertext that the file changed since it was hashed: if the plaintext isn't what was
    /// planned (hashed on send: if its source changed while it was read), values no bytes hash
    /// to make it refuse the payload.
    fn hash_part<'k>(&self, key: &'k KeyHeader) -> impl FnOnce() -> Vec<u8> + 'k
    where
        'a: 'k,
    {
        let (plain, sent, planned, iv) = (
            self.plain.clone(),
            self.sent.clone(),
            self.planned,
            self.descriptor.iv,
        );
        move || {
            let (content, len) = finish(&plain);
            if len != planned.1 || !planned.0.takes(content) {
                return wire::refused_payload_hash();
            }
            wire::payload_hash(key, &iv, &content, &finish(&sent).0)
        }
    }
}

/// Refuses a file of `size` bytes larger than [`MAX_FILE`]: [`RemoteError::Fatal`], so the
/// engine records it as refused and doesn't send it again while it is the same (sync semantics
/// §8a); the note names the file and says this.
fn check_size(size: u64) -> Result<()> {
    if size <= MAX_FILE {
        return Ok(());
    }
    // Rounded up to a tenth: a file just over the limit doesn't read as at it.
    let gib = (size as f64 / (1u64 << 30) as f64 * 10.0).ceil() / 10.0;
    Err(RemoteError::Fatal(format!(
        "too large to sync: {gib:.1} GiB, over the {} GiB limit for one file",
        MAX_FILE >> 30
    )))
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
        let (page, cursor) = self.feed_page(cursor, limit, wire::Shape::Full)?;
        let mut entries = Vec::with_capacity(page.search_results.len());
        let mut skipped = Vec::new();
        for row in &page.search_results {
            let read = match serde_json::from_str::<wire::FileHeader>(row.get()) {
                Ok(h) => self.to_remote(h)?,
                Err(e) => Err(format!("unreadable header: {e}")),
            };
            match read {
                Ok(header) => entries.push(header),
                Err(why) => skipped.push(wire::skipped_row(row.get(), why)),
            }
        }
        Ok(ChangesPage {
            entries,
            cursor,
            has_more: page.has_more_rows,
            skipped,
            server_time: page.query_time,
        })
    }

    /// A minimal page (odin-core #1967): what changed, without the headers.
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        let (page, cursor) = self.feed_page(cursor, limit, wire::Shape::Minimal)?;
        // A server without #1967 ignores the shape and sends headers: an empty page there
        // would skip what changed.
        let rows = page.file_refs.ok_or_else(|| {
            RemoteError::Fatal("the server doesn't page the feed slim (odin-core #1967)".into())
        })?;
        let mut refs = Vec::with_capacity(rows.len());
        let mut unread = 0;
        for row in &rows {
            match serde_json::from_str::<wire::FileRef>(row.get()) {
                Ok(r) => refs.push(r.to_ref()),
                Err(_) => unread += 1,
            }
        }
        Ok(RefsPage {
            refs,
            cursor,
            has_more: page.has_more_rows,
            unread,
            server_time: page.query_time,
        })
    }

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        wire::cursor_time(cursor)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.to_remote(self.raw_header(file_id)?)?
            .map_err(RemoteError::Fatal)
    }

    /// One `groupId` query a page at a time (any type, active files only): a removed file is
    /// told by its `archivalStatus`, a file's version by its `versionTag` (minimal rows carry
    /// both, **[verified]** odin-core #1967 `QueryBatchFileRef`: [`wire::FileRef`]).
    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        let mut holding = HashSet::new();
        for some in folders.chunks(CHILDREN_QUERY_FOLDERS) {
            let mut cursor: Option<String> = None;
            loop {
                let page: wire::QueryBatchResponse = self
                    .client
                    .post(
                        &format!("{}/query-batch", self.files()),
                        &wire::children_query(some, cursor.as_deref(), 500),
                    )
                    .map_err(remote_error)?;
                for row in page.file_refs.iter().flatten() {
                    // A row that doesn't read is something there all the same: its folder's.
                    let Ok(r) = serde_json::from_str::<wire::FileRef>(row.get()) else {
                        holding.extend(some.iter().copied());
                        continue;
                    };
                    let at = RemoteRef {
                        file_id: RemoteFileId(r.file_id),
                        version: VersionTag(r.version_tag),
                    };
                    if holds.counts(at, r.archival_status == ARCHIVAL_REMOVED)
                        && let Some(folder) = r.group_id.map(NodeId)
                    {
                        holding.insert(folder);
                    }
                }
                // Each folder known to hold one: the rest of the pages can't change that.
                if some.iter().all(|f| holding.contains(f)) {
                    break;
                }
                // A page may be empty with more to come (the query's hold-back).
                match page.cursor_state {
                    Some(next) if page.has_more_rows && Some(&next) != cursor.as_ref() => {
                        cursor = Some(next)
                    }
                    _ => break,
                }
            }
        }
        Ok(holding)
    }

    /// odin-core's query leaves out what changed in its last second (`HoldBackWindowMs`, #1923,
    /// homebase-api-notes), and a little more for the clocks.
    fn hold_back(&self) -> std::time::Duration {
        std::time::Duration::from_millis(1100)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Written> {
        self.write(None, node, payload)
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        self.write(Some(target), meta, payload)
    }

    /// One request per [`BATCH_FILES`] files (`soft-delete-batch`, odin-core #1977): each file
    /// goes only at its version (a tombstone isn't checked: it answers as deleted, with its own
    /// time), one after another in the order asked; one file's answer never stops the others.
    /// No recipients: nothing goes to peers.
    fn delete(&self, files: &[Doomed]) -> Result<Deletes> {
        Answers::in_batches(files, BATCH_FILES, |batch| {
            let answer: BatchAnswer<SoftDeleted> = self
                .client
                .post(
                    &format!("{}/soft-delete-batch", self.files()),
                    &versioned(batch.iter().map(|f| f.at())),
                )
                .map_err(remote_error)?;
            let asked = batch.iter().map(|f| f.at().file_id);
            same_files("soft-delete-batch", &answer.results, |a| a.file_id, asked)?;
            Ok(answer.results.iter().map(soft_deleted).collect())
        })
    }

    /// One request per [`BATCH_FILES`] files (`hard-delete-batch`, odin-core #1968): each
    /// file goes only at its version, and one file's answer never stops the others.
    fn delete_for_good(&self, files: &[Doomed]) -> Result<ForGood> {
        Answers::in_batches(files, BATCH_FILES, |batch| {
            let answer: BatchAnswer<HardDeleted> = self
                .client
                .post(
                    &format!("{}/hard-delete-batch", self.files()),
                    &versioned(batch.iter().map(|f| f.at())),
                )
                .map_err(remote_error)?;
            let asked = batch.iter().map(|f| f.at().file_id);
            same_files("hard-delete-batch", &answer.results, |a| a.file_id, asked)?;
            Ok(answer.results.iter().map(hard_deleted).collect())
        })
    }

    /// One request per [`BATCH_FILES`] files (`update-archival-status-batch`, odin-core
    /// #1964): only `archivalStatus` changes (2 removed, 0 back), each file only at its version,
    /// one after another in the order asked; one file's answer never stops the others.
    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Written>> {
        Answers::in_batches(files, BATCH_FILES, |batch| {
            let mut body = versioned(batch.iter().map(|t| t.at));
            body["archivalStatus"] = json!(if removed { ARCHIVAL_REMOVED } else { 0 });
            let answer: BatchAnswer<ArchivalSet> = self
                .client
                .post(
                    &format!("{}/update-archival-status-batch", self.files()),
                    &body,
                )
                .map_err(remote_error)?;
            let asked = batch.iter().map(|t| t.at.file_id);
            same_files(
                "update-archival-status-batch",
                &answer.results,
                |a| a.file_id,
                asked,
            )?;
            Ok(answer
                .results
                .iter()
                .zip(batch)
                .map(|(a, t)| archival_set(a, t))
                .collect())
        })
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
        let key = match (response.encrypted, response.key_header.as_deref()) {
            // A payload's key header carries the payload's IV.
            (true, Some(sealed)) => SealedKeyHeader::from_base64(sealed)
                .and_then(|s| KeyHeader::open(&s, self.client.key()))
                .map_err(remote_error)?,
            // A private drive's file comes without one: the file key is in its header, with
            // the payload's IV (one more request).
            (true, None) if self.drive_key(false)?.is_some() => self.payload_key(file_id)?,
            (true, None) => {
                return Err(RemoteError::Fatal(format!("{path}: encrypted, but no key")));
            }
            (false, _) => return Err(RemoteError::Fatal(format!("{path}: not encrypted"))),
        };
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

/// The body of a batch of files each at its version: `{files: [{fileId, versionTag}]}`.
fn versioned(files: impl Iterator<Item = RemoteRef>) -> serde_json::Value {
    json!({ "files": files.map(|f| json!({
        "fileId": f.file_id.0,
        "versionTag": f.version.0,
    })).collect::<Vec<_>>() })
}

/// Whether a batch answered the files asked (`asked`, their ids), in order.
fn same_files<A>(
    what: &str,
    answers: &[A],
    answered: impl Fn(&A) -> Uuid,
    asked: impl Iterator<Item = RemoteFileId>,
) -> Result<()> {
    if !answers.iter().map(answered).eq(asked.map(|f| f.0)) {
        return Err(RemoteError::Transient(format!(
            "{what} answered other files than asked"
        )));
    }
    Ok(())
}

/// Folders asked about in one query ([`HomebaseRemote::holding`]).
const CHILDREN_QUERY_FOLDERS: usize = 100;

/// The trait's error for an `OdinClientErrorCode` (its camelCase name), if it has one of its own.
fn code_error(code: &str) -> Option<RemoteError> {
    let mapped = match code.to_ascii_lowercase().as_str() {
        "existingfilewithuniqueid" => RemoteError::AlreadyExists,
        "versiontagmismatch" => RemoteError::VersionMismatch,
        "filenotfound" => RemoteError::NotFound,
        // Not active: a file is active or deleted (odin-core's `AssertActiveForUpdate`), so a
        // tombstone.
        "cannotupdatenonactivefile" => RemoteError::Deleted,
        // An upload whose hash part didn't arrive: sent again like one refused.
        "payloadhashmismatch" | "payloadhashmissing" => RemoteError::HashMismatch,
        "invalidpayloadhash" => RemoteError::InvalidHash,
        "payloadhashrequired" => RemoteError::HashRequired,
        _ => return None,
    };
    Some(mapped)
}

/// The identity's own answer that nothing is at a drive's address: no such drive on the app (or
/// no such app, which the identity doesn't tell apart; verified in odin-core's
/// `V2AppDriveController.GetAppDrive`).
fn no_drive(error: &ApiError) -> bool {
    error.from_identity
        && error
            .code
            .as_deref()
            .is_some_and(|c| c.eq_ignore_ascii_case("invalidDrive"))
}

/// How a Homebase answer maps onto the trait's errors (`homebase-api-notes.md`, Errors).
fn remote_error(e: HomebaseError) -> RemoteError {
    match e {
        HomebaseError::Network(why) => RemoteError::Transient(why),
        HomebaseError::Denied(why) => RemoteError::Denied(why),
        HomebaseError::Io(e) if source_ended(&e) => RemoteError::HashMismatch,
        HomebaseError::Io(e) => RemoteError::Io(e),
        HomebaseError::Api { what, error } => api_error(&what, &error),
        e @ (HomebaseError::NoDrive(_)
        | HomebaseError::Protocol(_)
        | HomebaseError::Crypto(_)
        | HomebaseError::Json(_)) => RemoteError::Fatal(e.to_string()),
    }
}

/// A source that ended early: it changed since it was hashed.
fn source_ended(e: &std::io::Error) -> bool {
    e.get_ref().is_some_and(|e| e.is::<SourceEnded>())
}

/// The trait's error for an error answer.
fn api_error(what: &str, error: &ApiError) -> RemoteError {
    let why = || format!("{what}: {error}");
    let mapped = match error.code.as_deref().and_then(code_error) {
        Some(mapped) => mapped,
        None => match error.status {
            404 | 410 => RemoteError::NotFound,
            416 => RemoteError::StaleRange(why()),
            507 => RemoteError::Full {
                why: why(),
                retry_after: error.retry_after,
            },
            408 | 429 | 499 | 500..=599 => match error.retry_after {
                Some(retry_after) => RemoteError::Later {
                    why: why(),
                    retry_after,
                },
                None => RemoteError::Transient(why()),
            },
            _ => RemoteError::Fatal(why()),
        },
    };
    // "Not there" and "a tombstone" delete here and are remembered for good: believed only from
    // the identity itself, never from a proxy or a page in front of it (`ApiError::read`).
    if matches!(mapped, RemoteError::NotFound | RemoteError::Deleted) && !error.from_identity {
        return RemoteError::Transient(format!("{} (not from the identity)", why()));
    }
    mapped
}

impl HomebaseError {
    /// When the server said to ask again (`Retry-After`).
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            HomebaseError::Api { error, .. } => error.retry_after,
            _ => None,
        }
    }

    /// What it means ([`ErrorClass`]): as the drive's error for it would say.
    pub fn class(&self) -> ErrorClass {
        match self {
            HomebaseError::Network(_) => ErrorClass::Transient,
            HomebaseError::Denied(_) => ErrorClass::Denied,
            HomebaseError::Io(e) if source_ended(e) => ErrorClass::Stale,
            HomebaseError::Io(e) => ErrorClass::of_io(e),
            HomebaseError::Api { what, error } => api_error(what, error).class(),
            HomebaseError::NoDrive(_)
            | HomebaseError::Protocol(_)
            | HomebaseError::Crypto(_)
            | HomebaseError::Json(_) => ErrorClass::Refused,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error answer of the identity itself.
    fn api(status: u16, code: Option<&str>) -> HomebaseError {
        HomebaseError::Api {
            what: "test".into(),
            error: ApiError {
                status,
                code: code.map(str::to_owned),
                title: String::new(),
                from_identity: true,
                retry_after: None,
            },
        }
    }

    /// Only the identity's own `invalidDrive` says the drive is gone: a page in front of it
    /// saying so, or another refusal, doesn't.
    #[test]
    fn only_the_identitys_invalid_drive_is_no_drive() {
        let error = |status, code: &str, from_identity| ApiError {
            status,
            code: Some(code.into()),
            title: String::new(),
            from_identity,
            retry_after: None,
        };
        assert!(no_drive(&error(400, "invalidDrive", true)));
        assert!(!no_drive(&error(400, "invalidDrive", false)));
        assert!(!no_drive(&error(400, "fileNotFound", true)));
        assert_eq!(
            HomebaseError::NoDrive("anna.example.com".into()).class(),
            ErrorClass::Refused
        );
    }

    /// An error's class is the class of the drive's error for it, so the account's connect
    /// and the folders' syncs judge an answer alike: a server in trouble, or a "not found"
    /// from a page in front of the identity, is offline.
    #[test]
    fn an_errors_class_is_its_drive_errors() {
        let mut proxy = api(404, None);
        if let HomebaseError::Api { error, .. } = &mut proxy {
            error.from_identity = false;
        }
        let errors = || {
            [
                HomebaseError::Network("cut".into()),
                HomebaseError::Denied("401".into()),
                api(503, None),
                api(429, None),
                api(404, None),
                api(400, Some("versionTagMismatch")),
                api(400, None),
                api(507, None),
                HomebaseError::Protocol("odd".into()),
                HomebaseError::Io(std::io::Error::other(SourceEnded { missing: 1 })),
                HomebaseError::Io(std::io::Error::other("disk")),
            ]
        };
        for e in errors() {
            let class = e.class();
            assert_eq!(class, remote_error(e).class());
        }
        assert_eq!(api(503, None).class(), ErrorClass::Transient);
        assert_eq!(proxy.class(), ErrorClass::Transient);
        assert_eq!(api(404, None).class(), ErrorClass::Gone);
    }

    /// A server that says when to ask again is waited for: a busy or paused one (429, 503)
    /// stops the run until then, a full one holds uploads until then.
    #[test]
    fn retry_after_is_kept() {
        let later = |status| {
            let mut e = api(status, None);
            if let HomebaseError::Api { error, .. } = &mut e {
                error.retry_after = Some(std::time::Duration::from_secs(600));
            }
            e
        };
        for status in [429, 503] {
            let e = later(status);
            assert_eq!(e.class(), ErrorClass::RateLimited);
            assert_eq!(e.retry_after(), Some(std::time::Duration::from_secs(600)));
            let mapped = remote_error(e);
            assert_eq!(
                mapped.retry_after(),
                Some(std::time::Duration::from_secs(600))
            );
            assert!(matches!(mapped, RemoteError::Later { .. }), "{mapped:?}");
        }
        let full = remote_error(later(507));
        assert!(
            matches!(
                full,
                RemoteError::Full {
                    retry_after: Some(_),
                    ..
                }
            ),
            "{full:?}"
        );
        assert_eq!(full.class(), ErrorClass::Full);
    }

    /// A batch that fails after others went keeps their answers and stops there: the files
    /// left have no answer, whatever the request's error (a not found too).
    #[test]
    fn a_failed_batch_keeps_the_answers_before_it() -> std::result::Result<(), RemoteError> {
        let files: Vec<RemoteRef> = (0..5)
            .map(|_| RemoteRef {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: VersionTag(Uuid::new_v4()),
            })
            .collect();
        let mut sent = 0;
        let done = Answers::in_batches(&files, 2, |batch| {
            sent += 1;
            match sent {
                1 => Ok(batch.iter().map(|_| Ok(())).collect()),
                _ => Err(RemoteError::NotFound),
            }
        })?;
        assert_eq!(sent, 2, "none sent after the failure");
        assert_eq!(done.answers.len(), 2, "{done:?}");
        assert!(done.answers.iter().all(Result::is_ok));
        assert!(matches!(done.stopped, Some(RemoteError::NotFound)));
        assert!(
            Answers::<()>::in_batches(&files, 2, |_| Err(RemoteError::Transient("cut".into())))
                .is_err(),
            "the first fails: the call's error"
        );
        Ok(())
    }

    /// `update-archival-status-batch`'s answer as odin-core writes it
    /// (`UpdateArchivalStatusBatchResultV2`), read per file.
    #[test]
    fn archival_answers_read_per_file() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let asked: Vec<Target> = (0..4)
            .map(|_| Target {
                at: RemoteRef {
                    file_id: RemoteFileId(Uuid::new_v4()),
                    version: VersionTag(Uuid::new_v4()),
                },
                token: Some(hbsync_types::FileToken(Box::new([7]))),
            })
            .collect();
        let new = Uuid::new_v4();
        let answer: BatchAnswer<ArchivalSet> = serde_json::from_value(json!({"results": [
            {"fileId": asked[0].at.file_id.0, "newVersionTag": new, "updated": 1_760_000_000_123i64,
             "errorCode": null},
            {"fileId": asked[1].at.file_id.0, "newVersionTag": null, "updated": null,
             "errorCode": "versionTagMismatch"},
            {"fileId": asked[2].at.file_id.0, "errorCode": "cannotUpdateNonActiveFile"},
            {"fileId": asked[3].at.file_id.0, "errorCode": "invalidFile"},
        ]}))?;
        let ids = |asked: &[Target]| asked.iter().map(|t| t.at.file_id).collect::<Vec<_>>();
        same_files(
            "batch",
            &answer.results,
            |a| a.file_id,
            ids(&asked).into_iter(),
        )?;
        let shifted = ids(&asked[..3]).into_iter();
        assert!(same_files("batch", &answer.results[1..], |a| a.file_id, shifted).is_err());
        let got: Vec<Result<Written>> = answer
            .results
            .iter()
            .zip(&asked)
            .map(|(a, t)| archival_set(a, t))
            .collect();
        let first = got[0].as_ref().map_err(|e| e.to_string())?;
        assert_eq!(first.at().version, VersionTag(new));
        assert_eq!(first.changed_at, Some(1_760_000_000_123));
        assert_eq!(first.target.token, asked[0].token, "the key stays");
        assert!(matches!(got[1], Err(RemoteError::VersionMismatch)));
        assert!(matches!(got[2], Err(RemoteError::Deleted)));
        assert!(matches!(got[3], Err(RemoteError::Fatal(_))));
        Ok(())
    }

    /// `soft-delete-batch`'s answer as odin-core writes it (`SoftDeleteBatchResultV2`), read per
    /// file: deleted with the tombstone's time (a tombstone already, too), or why it stayed.
    #[test]
    fn soft_delete_answers_read_per_file() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let answer: BatchAnswer<SoftDeleted> = serde_json::from_value(json!({"results": [
            {"fileId": ids[0], "errorCode": null, "updated": 1_760_000_000_123i64,
             "recipientStatus": {}},
            {"fileId": ids[1], "errorCode": "versionTagMismatch", "updated": null,
             "recipientStatus": {}},
            {"fileId": ids[2], "errorCode": "fileNotFound", "updated": null,
             "recipientStatus": {}},
            {"fileId": ids[3], "errorCode": "invalidFile", "updated": null,
             "recipientStatus": {}},
        ]}))?;
        let got: Vec<Result<Option<i64>>> = answer.results.iter().map(soft_deleted).collect();
        assert!(
            matches!(got[0], Ok(Some(1_760_000_000_123))),
            "{:?}",
            got[0]
        );
        assert!(matches!(got[1], Err(RemoteError::VersionMismatch)));
        assert!(matches!(got[2], Err(RemoteError::NotFound)));
        assert!(matches!(got[3], Err(RemoteError::Fatal(_))));
        Ok(())
    }

    #[test]
    fn errors_map_onto_the_trait() {
        let cases = [
            (api(400, Some("existingFileWithUniqueId")), "AlreadyExists"),
            (api(400, Some("versionTagMismatch")), "VersionMismatch"),
            (api(400, Some("fileNotFound")), "NotFound"),
            (api(400, Some("cannotUpdateNonActiveFile")), "Deleted"),
            (api(400, Some("payloadHashMismatch")), "HashMismatch"),
            (api(400, Some("payloadHashMissing")), "HashMismatch"),
            (api(400, Some("payloadHashRequired")), "HashRequired"),
            (api(404, None), "NotFound"),
            (api(416, None), "StaleRange"),
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

    /// A "not there" or "a tombstone" that the identity itself didn't send (a proxy's 404, a
    /// maintenance page, a route the server doesn't have) is passing, never a file gone: it
    /// would delete here.
    #[test]
    fn not_found_only_from_the_identity() {
        let read = |status: u16, body: &[u8], odin: bool, sealed: bool| {
            let error = ApiError::read(
                status,
                body,
                crate::problem::Sender {
                    odin,
                    sealed,
                    retry_after: None,
                },
            );
            format!(
                "{:?}",
                remote_error(HomebaseError::Api {
                    what: "test".into(),
                    error
                })
            )
        };
        let problem = br#"{"title":"Not Found","status":404,"traceId":"00-ab"}"#;
        let gone = br#"{"status":410,"traceId":"00-ab"}"#;
        let code = br#"{"title":"x","status":400,"errorCode":"fileNotFound"}"#;
        let envelope = br#"{"iv":"AAAA","data":"BBBB"}"#;
        let tombstone = br#"{"title":"x","status":400,"errorCode":"cannotUpdateNonActiveFile"}"#;
        let cases: [(String, &str); 12] = [
            (read(400, tombstone, true, false), "Deleted"),
            (read(400, tombstone, false, false), "Transient"),
            (read(404, envelope, true, true), "NotFound"),
            (read(404, problem, true, false), "NotFound"),
            (read(410, gone, true, false), "NotFound"),
            (read(400, code, true, false), "NotFound"),
            (read(404, b"", true, false), "Transient"),
            (read(404, problem, false, false), "Transient"),
            (read(410, gone, false, false), "Transient"),
            (read(400, code, false, false), "Transient"),
            (
                read(404, b"<html>Not Found</html>", false, false),
                "Transient",
            ),
            // Not a "not there": as before, whoever sent it.
            (
                read(400, br#"{"errorCode":"versionTagMismatch"}"#, false, false),
                "VersionMismatch",
            ),
        ];
        for (mapped, expected) in cases {
            assert!(mapped.starts_with(expected), "{mapped} isn't {expected}");
        }
    }

    /// Files up to 50 GiB go up, past 4 GiB too (odin-core counts their bytes in 64 bits);
    /// one byte more is refused for good, saying the limit.
    #[test]
    fn files_up_to_50_gib_go_up() {
        const GIB: u64 = 1 << 30;
        for size in [0, 4 * GIB - 1, 4 * GIB, 4 * GIB + 1, 50 * GIB] {
            assert!(check_size(size).is_ok(), "{size}");
        }
        for size in [50 * GIB + 1, 1 << 62, u64::MAX] {
            let Err(refused) = check_size(size) else {
                panic!("{size} bytes go up");
            };
            assert_eq!(refused.class(), ErrorClass::Refused);
            assert!(refused.to_string().contains("50 GiB limit"), "{refused}");
        }
        let Err(RemoteError::Fatal(why)) = check_size(50 * GIB + 1) else {
            panic!("not refused");
        };
        assert_eq!(
            why,
            "too large to sync: 50.1 GiB, over the 50 GiB limit for one file"
        );
    }

    /// A file over the limit is refused before any request, a create or an update: here the
    /// identity can't be reached, and nothing of the file is read.
    #[test]
    fn a_file_over_the_limit_costs_no_request() -> Result<(), HomebaseError> {
        struct Unread;
        impl Read for Unread {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                panic!("a refused file is read");
            }
        }
        let credentials = crate::youauth::Credentials {
            identity: "unreachable.invalid".into(),
            client_auth_token: "AAAA".into(),
            shared_secret: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            zk_app_key: None,
        };
        let remote = HomebaseRemote {
            client: Client::new(&credentials)?,
            drive: TargetDrive {
                alias: Uuid::nil(),
                drive_type: Uuid::nil(),
            },
            made: 0,
            drive_key: DriveKey::default(),
        };
        let size = MAX_FILE + 1;
        let meta = NodeMeta {
            node: hbsync_types::NodeId::new(),
            parent: hbsync_types::NodeId::ROOT,
            kind: hbsync_types::NodeKind::File,
            content: NodeContent::file("big.pst", 0, size, false),
            removed: false,
        };
        let target = Target {
            at: hbsync_types::RemoteRef {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: VersionTag(Uuid::new_v4()),
            },
            token: None,
        };
        for target in [None, Some(&target)] {
            let upload = Upload {
                hash: Planned::Known(hbsync_fileio::hash_bytes(b"", HashAlgorithm::Blake3)),
                size,
                reader: &mut Unread,
                pace: None,
            };
            let Err(refused) = remote.write(target, &meta, Some(upload)) else {
                panic!("a file over the limit went up");
            };
            assert!(refused.to_string().contains("50 GiB limit"), "{refused}");
        }
        Ok(())
    }
}
