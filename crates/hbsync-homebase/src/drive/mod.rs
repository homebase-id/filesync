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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use hbsync_fileio::{HashAlgorithm, Hasher};
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ErrorClass, ForGood, Holds, NodeMeta, Paced, Planned, RefsPage,
    Refusal, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::{ARCHIVAL_REMOVED, NodeContent, PAYLOAD_KEY};
use hbsync_types::{
    ContentHash, Doomed, NodeId, RemoteFileId, RemoteRef, Target, VersionTag, Written,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::client::{Client, Fetched, UploadMethod};
use crate::crypto::{Key, WriteOnlyKey, key_of, zk_unlock};
use crate::drive::sealed::{
    CipherRange, Decrypting, Encrypting, KeyHeader, SealedKeyHeader, sealed_len,
};
use crate::drive::share_keys::CircleKey;
use crate::multipart::{Multipart, Part, SourceEnded};
use crate::peer::{PeerToken, SealedKey};
use crate::{ApiError, HomebaseError, app};

pub mod markers;
pub mod sealed;
mod share_keys;
pub mod shared;
pub mod wire;

use wire::Keys;

/// Why a private drive refuses this computer: it wasn't handed the app's zero-knowledge key
/// (a login from before it asked for one), the key it was handed opens the drive's no more, or
/// the drive's key isn't locked for the app yet: a sign-in sets each right
/// ([`HomebaseError::NoDriveKey`]).
pub(crate) const PRIVATE: &str = "the Filesync drive on your Homebase cloud is private now, and this \
                       computer doesn't hold its key";

/// Why a private drive can't be opened by a login that holds the app's zero-knowledge key: the
/// security context lists no grant on it ([`drive_grant`]). Signing in again doesn't help;
/// tried again by itself.
const UNLISTED: &str = "your Homebase cloud doesn't hand this app its private drive's key: it \
                        needs an update of Homebase (odin-core #2047)";

/// Why nothing shared opens: the app was installed before it asked for the permission to read
/// what others share, and the owner hasn't given it since ([`app::update_url`]).
const NOT_PERMITTED: &str = "your Homebase cloud hasn't given Filesync the permission to read what \
                             others share with you yet";

/// Why nothing shared opens: this identity's drive has no write-only key that opens, which
/// sharers seal their circles' keys to.
const NO_WRITE_ONLY_KEY: &str =
    "your Filesync drive has no key that opens what others share with you";

/// How long a share waits on its sharer (a circle key not sealed to this identity yet, a
/// connection they haven't reviewed) before it is asked about again.
const SHARER_WAIT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// How long the app's want of the permission to send to other identities is taken as read,
/// before the security context is asked again ([`HomebaseRemote::may_send`]).
const SENDS_READ_AGAIN: std::time::Duration = std::time::Duration::from_secs(60);

/// The same for a share's folder, which is read-only while the app may not send
/// ([`HomebaseRemote::sends`]): asked as its syncs are due, so seldom. The owner's approval
/// is found sooner all the same: by the sync asked for as they give it
/// ([`SENDS_READ_ASKED`]), and as this identity's notifications connect again (the security
/// context is read anew then).
const SENDS_LOOK_AGAIN: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// The same where the user asked for a sync just now (they may have given the permission a
/// moment ago): long enough for every share's runner asked at once to go by one read.
const SENDS_READ_ASKED: std::time::Duration = std::time::Duration::from_secs(2);

/// How long a share waits before it asks again where this identity isn't connected to its
/// sharer: connecting is done by people, in their Homebase.
const NOT_CONNECTED_WAIT: std::time::Duration = std::time::Duration::from_secs(3600);

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
    /// What is known of the drive's privacy, and its key ([`HomebaseRemote::drive_key`]).
    drive_key: DriveKey,
    /// Held by whoever asks whether the app may send ([`HomebaseRemote::may_send`]): one
    /// asks, and the rest go by its answer.
    sends_asking: Mutex<()>,
}

/// How [`read_pages`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    /// At the last page, or where the reader had what it needed.
    Done,
    /// The server promised more rows without a new cursor.
    CutShort,
}

/// Reads the pages `fetch` gives for each cursor, each to `each`, until the last or `each` says
/// it has what it needs (false). A page promising more rows without a new cursor (a held-back
/// page of a change order may) ends it, [`Ended::CutShort`].
fn read_pages(
    mut fetch: impl FnMut(Option<&str>) -> Result<wire::QueryBatchResponse>,
    mut each: impl FnMut(&wire::QueryBatchResponse) -> Result<bool>,
) -> Result<Ended> {
    let mut cursor: Option<String> = None;
    loop {
        let page = fetch(cursor.as_deref())?;
        if !each(&page)? || !page.has_more_rows {
            return Ok(Ended::Done);
        }
        match page.cursor_state {
            Some(next) if Some(&next) != cursor.as_ref() => cursor = Some(next),
            _ => return Ok(Ended::CutShort),
        }
    }
}

/// The markers of `identity`'s drive, from the pages `fetch` gives ([`read_pages`]), opened
/// with `shared` or what `drive_key` gives (when a row needs the drive's key). A listing the
/// server cut short, or that came back to a cursor it gave, is an error, never fewer markers.
fn read_markers(
    identity: &str,
    shared: &Key,
    fetch: impl FnMut(Option<&str>) -> Result<wire::QueryBatchResponse>,
    mut drive_key: impl FnMut(bool) -> Result<Option<Key>>,
) -> Result<markers::Markers> {
    let mut found = markers::Markers::of(identity);
    let mut seen = HashSet::new();
    let mut asked: Option<String> = None;
    let ended = read_pages(fetch, |page| {
        for row in &page.search_results {
            found.read(row.get(), shared, &mut drive_key)?;
        }
        // A server coming back to an earlier cursor would page on for ever (the one just asked
        // from is read_pages' to end short).
        if page.has_more_rows
            && let Some(cursor) = &page.cursor_state
            && Some(cursor) != asked.as_ref()
            && !seen.insert(cursor.clone())
        {
            return Err(RemoteError::Transient(
                "the sync markers' listing came back to a cursor it gave".into(),
            ));
        }
        asked.clone_from(&page.cursor_state);
        Ok(true)
    })?;
    match ended {
        Ended::Done => Ok(found),
        Ended::CutShort => Err(RemoteError::Transient(
            "the sync markers' listing promised more rows without a new cursor".into(),
        )),
    }
}

/// What the security context says, as this computer knows it: the drive's privacy and keys, the
/// app's permissions, and when that was read: at how many of the notifications' reconnects
/// ([`Client::connects`]).
#[derive(Default)]
struct KeyState {
    read_at: u64,
    private: bool,
    key: Option<Key>,
    /// The security context listed no grant on the drive while this login holds the app's key
    /// ([`UNLISTED`]).
    unlisted: bool,
    /// The drive's write-only key, locked with the drive's key, as the grant gives it.
    write_only: Option<String>,
    /// That key opened ([`KeyState::write_only`]), the first time it is asked for.
    opened: OnceLock<Option<Arc<WriteOnlyKey>>>,
    /// The app holds `UseTransitWrite` ([`app::USE_TRANSIT_WRITE`]): it may send to other
    /// identities' drives, as a change to what they share with this one is.
    sends: bool,
    /// When the security context was last read for that.
    sends_read: Option<std::time::Instant>,
}

impl KeyState {
    /// Read from the security context (one request): a private drive is one the app's grant
    /// says is, or whose key is locked for the app; one known to be private stays so (there is
    /// no way back). Without the grant listed, the drive's own answer says: one more request
    /// per read on a drive not known to be private, while servers leave it out (#2047).
    fn read(
        client: &Client,
        drive: &TargetDrive,
        read_at: u64,
        known_private: bool,
    ) -> Result<Self, HomebaseError> {
        let context = security_context(client)?;
        let grant = drive_grant(&context, drive);
        let zero_knowledge = match &grant {
            Some(grant) => grant.zero_knowledge,
            None => {
                static WARNED: AtomicBool = AtomicBool::new(false);
                if !WARNED.swap(true, Ordering::Relaxed) {
                    tracing::warn!("the security context lists no grant on the Filesync drive");
                }
                // Not asked again once known.
                known_private || drive_answer(client)?.private()
            }
        };
        let unlisted = grant.is_none() && client.zk_key().is_some();
        let key = grant
            .as_ref()
            .and_then(|g| g.locked)
            .map(|locked| unlock_drive_key(client, locked))
            .transpose()?;
        Ok(KeyState {
            read_at,
            private: known_private || zero_knowledge || key.is_some(),
            key,
            unlisted,
            write_only: grant.and_then(|g| g.write_only).map(str::to_owned),
            opened: OnceLock::new(),
            sends: holds_key(&context, app::USE_TRANSIT_WRITE),
            sends_read: Some(std::time::Instant::now()),
        })
    }

    /// The drive's write-only key, opened with the drive's key from the copy the grant gives
    /// ([`open_write_only`]), once for as long as this state is held (it costs an AES-GCM
    /// unlock and a P-384 key check): none on a shared drive, nor from a grant without the
    /// copy, nor where the copy doesn't open; a private drive without its key refuses as
    /// [`KeyState::key`] does.
    fn write_only(&self) -> std::result::Result<Option<Arc<WriteOnlyKey>>, HomebaseError> {
        let Some(key) = self.key()? else {
            return Ok(None);
        };
        let open = || {
            let locked = self.write_only.as_deref()?;
            open_write_only(&key, locked).map(Arc::new)
        };
        Ok(self.opened.get_or_init(open).clone())
    }

    /// The key to open and lock with: none on a shared drive. A private drive without one (a
    /// login without the app's key, or the drive's key not locked for the app yet) refuses this
    /// computer: log in again, never read as shared; one whose grant the server left out stops
    /// until the server lists it.
    fn key(&self) -> Result<Option<Key>, HomebaseError> {
        match (self.private, self.key) {
            (true, None) if self.unlisted => Err(HomebaseError::Protocol(UNLISTED.into())),
            (true, None) => Err(HomebaseError::NoDriveKey),
            (_, key) => Ok(key),
        }
    }
}

/// Which of the permissions the app asks for it lacks now (to read what others share with
/// this identity, to change what they share for editing: an install from before it asked
/// for one lacks that one until the owner approves, [`app::update_url`]): one read of the
/// login's security context.
pub fn lacks(client: &Client) -> Result<app::Lacks, HomebaseError> {
    Ok(lacks_in(&security_context(client)?))
}

/// [`lacks`], of a security context read.
fn lacks_in(context: &serde_json::Value) -> app::Lacks {
    app::Lacks {
        reads: !holds_key(context, app::USE_TRANSIT_READ),
        sends: !holds_key(context, app::USE_TRANSIT_WRITE),
    }
}

/// Whether the app holds permission `key` now: one read of the login's security context.
fn holds(client: &Client, key: u32) -> Result<bool, HomebaseError> {
    Ok(holds_key(&security_context(client)?, key))
}

/// The login's security context: its grants and permissions.
fn security_context(client: &Client) -> Result<serde_json::Value, HomebaseError> {
    client.get("/api/v2/auth/context", &[])
}

/// The keys one header opens with ([`HomebaseRemote::held`]).
struct Held {
    drive: Option<Key>,
    write_only: Option<Arc<WriteOnlyKey>>,
}

impl Held {
    fn keys<'a>(&'a self, remote: &'a HomebaseRemote) -> Keys<'a> {
        Keys {
            write_only: self.write_only.as_deref(),
            ..remote.keys(self.drive.as_ref())
        }
    }
}

struct DriveKey(Mutex<KeyState>);

impl DriveKey {
    fn lock(&self) -> std::sync::MutexGuard<'_, KeyState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl std::fmt::Debug for DriveKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DriveKey(..)")
    }
}

/// The drive's key locked with the app's zero-knowledge key, opened. One this login can't open
/// refuses this computer.
fn unlock_drive_key(client: &Client, locked: &str) -> Result<Key, HomebaseError> {
    let app_key = client.zk_key().ok_or(HomebaseError::NoDriveKey)?;
    let key = zk_unlock(app_key, locked).map_err(|e| match e {
        // Locked with another key than this login's (another sign-in's won).
        HomebaseError::Crypto(_) => HomebaseError::NoDriveKey,
        e => e,
    })?;
    key_of(&key, "the drive's key")
}

/// The drive's write-only key `locked` with its `key` in the app's grant, opened as chat-kmp's
/// `ZkKeyStore` does: none where it doesn't open (locked with another key, or not a key), said
/// once for each reading of the key state ([`KeyState::write_only`]), and only when something
/// shared, or a file sealed to the drive, is read. Then nothing shared opens here, nor what
/// others added to this drive, and the drive's own folders sync on.
fn open_write_only(key: &Key, locked: &str) -> Option<WriteOnlyKey> {
    zk_unlock(key, locked)
        .and_then(|der| WriteOnlyKey::from_pkcs8(&der))
        .inspect_err(|e| tracing::warn!("the Filesync drive's write-only key doesn't open: {e}"))
        .ok()
}

/// The security context's permission groups (`permissionContext.permissionGroups`, odin-core
/// `RedactedPermissionContext`).
fn permission_groups(context: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    context["permissionContext"]["permissionGroups"]
        .as_array()
        .into_iter()
        .flatten()
}

/// Whether the security context gives the caller permission `key` (`permissionContext
/// .permissionGroups[].permissionSet.keys`, odin-core `RedactedPermissionSet`, **[verified]**).
fn holds_key(context: &serde_json::Value, key: u32) -> bool {
    permission_groups(context)
        .filter_map(|g| g["permissionSet"]["keys"].as_array())
        .flatten()
        .any(|k| k.as_u64() == Some(u64::from(key)))
}

/// The app's grant on a drive, as the security context gives it.
#[derive(Debug, PartialEq, Eq)]
struct Grant<'a> {
    /// The drive is private (`isZeroKnowledge`), whether its key is locked for the app or not.
    zero_knowledge: bool,
    /// Its key locked with the app's zero-knowledge key (`zkAppKeyEncryptedStorageKey`).
    locked: Option<&'a str>,
    /// Its write-only private key locked with its key
    /// (`zkStorageKeyEncryptedWriteOnlyPrivateKey`, **[verified]** odin-core `RedactedDriveGrant`).
    write_only: Option<&'a str>,
}

/// In the security context (`GET /api/v2/auth/context`, odin-core `RedactedOdinContext`), the
/// app's grant on `drive` (`RedactedDriveGrant`, `docs/zero-knowledge-drive-api-plan.md`, 3.5
/// and 4.2). Read leniently: no other grant's shape matters. None when it isn't listed, as for
/// any app without permission keys up to odin-core v0.1.974 (odin-core #2047,
/// `docs/spec/homebase-api-notes.md`).
fn drive_grant<'a>(context: &'a serde_json::Value, drive: &TargetDrive) -> Option<Grant<'a>> {
    let grant = permission_groups(context)
        .filter_map(|g| g["driveGrants"].as_array())
        .flatten()
        .find(|g| {
            serde_json::from_value::<TargetDrive>(g["permissionedDrive"]["drive"].clone())
                .is_ok_and(|d| d == *drive)
        })?;
    Some(Grant {
        zero_knowledge: grant["isZeroKnowledge"].as_bool().unwrap_or(false),
        locked: grant["zkAppKeyEncryptedStorageKey"].as_str(),
        write_only: grant["zkStorageKeyEncryptedWriteOnlyPrivateKey"].as_str(),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveAnswer {
    target_drive: TargetDrive,
    /// When the drive was made, Unix ms (the identity's clock).
    created: i64,
    /// `collaborative`, `private` or `selectiveShare`, from the drive itself (odin-core #2046
    /// `ClientDriveData.SharingModel`, **[verified]**): absent from a server before it.
    #[serde(default)]
    sharing_model: Option<String>,
}

impl DriveAnswer {
    /// The drive is private: its files locked with the drive's key.
    fn private(&self) -> bool {
        matches!(
            self.sharing_model.as_deref(),
            Some("private" | "selectiveShare")
        )
    }
}

/// The drive's answer (`GET /api/v2/apps/filesync/drives/filesync`): [`HomebaseError::NoDrive`]
/// when the identity says nothing answers at its address.
fn drive_answer(client: &Client) -> Result<DriveAnswer, HomebaseError> {
    let route = format!(
        "/api/v2/apps/{}/drives/{}",
        app::APP_SLUG,
        app::SYNC_DRIVE.slug
    );
    client.get(&route, &[]).map_err(|e| match e {
        HomebaseError::Api { error, .. } if no_drive(&error) => {
            HomebaseError::NoDrive(client.identity().to_owned())
        }
        e => e,
    })
}

/// A create's or update's answer (`CreateFileResult`, `UpdateFileResult`): the version written
/// and its time (`versionUpdated`, odin-core PR #2143: the header's, [`wire::Times`]; null only
/// for a peer-only update, which this client never makes). Its `updated` isn't read: the
/// file's place in the feed after the request, the same but for what the request changed
/// besides (**[verified]** `WriteTimes.Of`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriteResult {
    file_id: Uuid,
    new_version_tag: Uuid,
    #[serde(default)]
    version_updated: Option<i64>,
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
    /// The tombstone's time (Unix ms), as its header's `versionUpdated`; `None` when it stayed.
    #[serde(default)]
    version_updated: Option<i64>,
}

/// One file's answer to a soft delete: its tombstone's time.
fn soft_deleted(answer: &SoftDeleted) -> Result<Option<i64>> {
    match &answer.error_code {
        None => Ok(answer.version_updated),
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
    /// That version's time (Unix ms), as its header's `versionUpdated`.
    #[serde(default)]
    version_updated: Option<i64>,
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
        changed_at: answer.version_updated,
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
        let answer = drive_answer(&client)?;
        let state = KeyState::read(
            &client,
            &answer.target_drive,
            client.connects(),
            answer.private(),
        )?;
        state.key()?;
        Ok(HomebaseRemote {
            client,
            drive: answer.target_drive,
            made: answer.created,
            drive_key: DriveKey(Mutex::new(state)),
            sends_asking: Mutex::default(),
        })
    }

    /// The private drive's key; none on a shared drive. Read again (one request) once the
    /// notifications connected since (making a drive private closes them), or when `needed`
    /// (a file locked with it, or a write the drive refused for its key header) on a drive not
    /// known to be private: that it is stays known (there is no way back). A private drive
    /// whose key this computer doesn't hold refuses it ([`RemoteError::Denied`]): it stops,
    /// rather than read the files hidden from it as gone.
    fn drive_key(&self, needed: bool) -> Result<Option<Key>> {
        self.key_state(needed, KeyState::key)
    }

    /// What `take` reads of the drive's key state, read again first as [`Self::drive_key`]
    /// says.
    fn key_state<T>(
        &self,
        needed: bool,
        take: impl FnOnce(&KeyState) -> std::result::Result<T, HomebaseError>,
    ) -> Result<T> {
        let connects = self.client.connects();
        // Held while it is read: whoever needs it then waits for that one read.
        let mut state = self.drive_key.lock();
        if state.read_at != connects || (needed && !state.private) {
            let known = state.private || needed;
            *state =
                KeyState::read(&self.client, &self.drive, connects, known).map_err(remote_error)?;
        }
        take(&state).map_err(remote_error)
    }

    /// `header` as a [`RemoteHeader`], opened with the keys it needs ([`Self::held`]).
    fn to_remote(
        &self,
        header: wire::FileHeader,
    ) -> Result<std::result::Result<RemoteHeader, String>> {
        let held = self.held(&header)?;
        Ok(wire::to_remote(header, held.keys(self)))
    }

    /// The keys `header` opens with, besides the shared secret: the drive's when a live node's
    /// is locked with it, and the drive's write-only key when a live node's is sealed to that
    /// (a file someone added to a folder shared with them). A tombstone's or another app's
    /// file's isn't opened. A write-only key that isn't there, or doesn't open, leaves the file
    /// unread, said: on a drive not known to be private too, which a sealed file doesn't make
    /// one (a file locked with the drive's key does: that one only a private drive has).
    fn held(&self, header: &wire::FileHeader) -> Result<Held> {
        if !header.needs_write_only_key() {
            let drive = self.drive_key(header.needs_drive_key())?;
            return Ok(Held {
                drive,
                write_only: None,
            });
        }
        let write_only = self.key_state(false, KeyState::write_only)?;
        Ok(Held {
            drive: None,
            write_only,
        })
    }

    fn keys<'a>(&'a self, drive: Option<&'a Key>) -> Keys<'a> {
        Keys {
            shared: self.client.key(),
            drive,
            circle: None,
            write_only: None,
        }
    }

    /// A write's error: one the drive refused for its key header's kind was made private since
    /// this connected; with its key fetched (or this computer refused), it is written again.
    fn write_error(&self, e: HomebaseError) -> RemoteError {
        if let HomebaseError::Api { error, .. } = &e
            && error.is("driveIsZeroKnowledge")
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
        files_route(self.drive.alias)
    }

    /// The route of one file.
    pub fn file(&self, file_id: RemoteFileId) -> String {
        file_route(self.drive.alias, file_id)
    }

    /// A file's header as the server sends it.
    pub fn raw_header(&self, file_id: RemoteFileId) -> Result<wire::FileHeader> {
        raw_header(&self.client, self.drive.alias, file_id).map_err(remote_error)
    }

    /// The key of `sharer`'s `circle`, sealed to this identity's drive and opened with the
    /// drive's write-only key (its private half, locked with the drive's key in the app's grant):
    /// one request to this identity's server (which asks theirs).
    ///
    /// None sealed yet, or the connection not reviewed by them, is [`RemoteError::Waiting`]: they
    /// do it on their own time (a device of theirs seals it when next online). What only the
    /// owner lifts refuses, in this order: a drive without a write-only key that opens
    /// ([`Refusal::NoKey`]: a drive made before them, a grant without the copy, or a drive that
    /// isn't private), the app's want of the permission ([`Refusal::Permission`], an approval).
    /// A private drive without its key refuses this computer first, as [`Self::drive_key`]
    /// does.
    pub(crate) fn circle_key(&self, sharer: &markers::Sharer, circle: Uuid) -> Result<CircleKey> {
        let write_only = self.key_state(false, KeyState::write_only)?;
        let no_key = || RemoteError::Denied(NO_WRITE_ONLY_KEY.into(), Refusal::NoKey);
        // The write-only key first: without one that opens, the permission wouldn't help (and it
        // costs no request).
        let write_only = write_only.ok_or_else(no_key)?;
        let answer = self.ask_peer(&sharer.odin_id, || {
            SealedKey::ask(&self.client, &sharer.odin_id, circle)
        })?;
        let sealed = sealed_or_wait(answer, &sharer.odin_id)?;
        let member = markers::owner_of(self.client.identity());
        CircleKey::unseal(&sealed, &write_only, circle, sharer.drive_alias, &member)
            .map_err(remote_error)
    }

    /// A new token for `sharer`'s server ([`PeerToken::new`]: a row there that stays, so one
    /// is kept, [`shared::PeerRemote::open`]): one request to this identity's server, which
    /// asks theirs. The app's want of the permission refuses ([`Refusal::Permission`], an
    /// approval), told from their refusal as [`Self::circle_key`] tells it.
    pub(crate) fn peer_token(&self, sharer: &str) -> Result<PeerToken> {
        self.ask_peer(sharer, || PeerToken::new(&self.client, sharer))
    }

    /// What `ask` answers of `sharer`'s server through this one's, which the app needs a
    /// permission for. This identity's server says where the app lacks it
    /// ([`lacks_permission`]): refused ([`Refusal::Permission`], an approval: an install from
    /// before the app asked for it has to be given it by the owner, [`app::update_url`]).
    /// Whether it holds it is never looked up first: the ask itself tells, also once the owner
    /// has given it (which no reconnect says; **[inferred]**: odin-core's permission update
    /// closes no socket). Any other 403 is theirs (`Client::whose`).
    fn ask_peer<T>(
        &self,
        sharer: &str,
        ask: impl FnOnce() -> std::result::Result<T, HomebaseError>,
    ) -> Result<T> {
        match ask() {
            Ok(answer) => Ok(answer),
            // Not connected to them (any more): this identity's own answer, before theirs is
            // asked. Theirs to mend or the owner's, and nothing a retry in minutes changes.
            Err(e) if not_connected(&e) => Err(not_connected_to(sharer, "read")),
            Err(e) if lacks_permission(&e) => Err(RemoteError::Denied(
                NOT_PERMITTED.into(),
                Refusal::Permission,
            )),
            Err(e) => Err(remote_error(e)),
        }
    }

    /// A send was refused for want of the permission to send: not held, as of now.
    pub(crate) fn may_not_send(&self) {
        let mut state = self.drive_key.lock();
        state.sends = false;
        state.sends_read = Some(std::time::Instant::now());
    }

    /// Whether the app may send to other identities' drives ([`app::USE_TRANSIT_WRITE`]), as
    /// the security context said when last read; where it said no, read again (one request:
    /// the owner may have given the permission since) at most once in [`SENDS_READ_AGAIN`],
    /// and kept: every write refused for want of it asks.
    pub(crate) fn may_send(&self) -> Result<bool> {
        self.may_send_within(SENDS_READ_AGAIN)
    }

    /// [`HomebaseRemote::may_send`] for a share's folder, which is written to while it says
    /// yes: a no is read again where the user `asked` for the sync just now, once it is
    /// [`SENDS_READ_ASKED`] old.
    pub(crate) fn sends(&self, asked: bool) -> Result<bool> {
        self.may_send_within(match asked {
            true => SENDS_READ_ASKED,
            false => SENDS_LOOK_AGAIN,
        })
    }

    /// [`HomebaseRemote::may_send`], a no read again once it is `within` old. One asks at a
    /// time, and whoever waited for it goes by its answer (the shares' runners, all due at
    /// once after a Sync now: one request). A read that failed counts as read just now: the
    /// no stands, and the next asks only once it is `within` old again.
    fn may_send_within(&self, within: std::time::Duration) -> Result<bool> {
        let known = || {
            let (sends, read) = self.key_state(false, |s| Ok((s.sends, s.sends_read)))?;
            Ok::<_, RemoteError>(
                (sends || read.is_some_and(|at| at.elapsed() < within)).then_some(sends),
            )
        };
        if let Some(sends) = known()? {
            return Ok(sends);
        }
        let _asking = self.sends_asking.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sends) = known()? {
            return Ok(sends);
        }
        // (The key state isn't held while it is asked.)
        let asked = holds(&self.client, app::USE_TRANSIT_WRITE);
        let mut state = self.drive_key.lock();
        state.sends_read = Some(std::time::Instant::now());
        let sends = asked.map_err(remote_error)?;
        state.sends = sends;
        Ok(sends)
    }

    /// What this identity's own markers say: whose shares its computers sync, and which of its
    /// markers don't read (`docs/spec/node-schema-v1.md`, "Sync markers"). A page per 100
    /// markers; a listing the server cut short is an error, never fewer markers.
    pub fn markers(&self) -> Result<markers::Markers> {
        // A drive made private since the notifications last connected refuses a login without
        // its key here, before a page that leaves its files out (until they reconnect, a
        // listing can't tell, as the feed can't).
        let private = self.key_state(false, |s| s.key().map(|_| s.private))?;
        // A marker counts only on a private drive (node schema, "Sync markers", Trust): on one
        // that isn't, whoever may write to it could rewrite the owner's marker, whose sender
        // the server keeps. None is read there: nothing is turned on, and what is synced
        // stays as it is.
        if !private {
            return Ok(markers::Markers::untrusted(self.client.identity()));
        }
        read_markers(
            self.client.identity(),
            self.client.key(),
            |cursor| self.query_batch(&markers::markers_query(cursor)),
            |needed| self.drive_key(needed),
        )
    }

    /// One `query-batch` page.
    fn query_batch(&self, body: &serde_json::Value) -> Result<wire::QueryBatchResponse> {
        query_batch(&self.client, self.drive.alias, body).map_err(remote_error)
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
        let mut sending = Sending::of(payload, &key)?;
        let version = at.map(|t| t.version);
        let instructions = wire::instructions(
            &transfer_iv,
            sending.as_ref().map(|s| &s.descriptor),
            target.is_some(),
        );
        let drive = self.drive_key(false)?;
        let metadata = wire::metadata(meta, version, self.keys(drive.as_ref()), &transfer_iv, &key);
        let parts = parts(&instructions, metadata, sending.as_mut(), &key);
        let (method, path) = match at {
            None => (UploadMethod::Post, self.files()),
            Some(at) => (UploadMethod::Patch, self.file(at.file_id)),
        };
        let boundary = Uuid::new_v4().simple().to_string();
        let WriteResult {
            file_id,
            new_version_tag,
            version_updated,
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
            changed_at: version_updated,
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
        // A drive made private since the notifications last connected refuses a login without
        // its key here, before a page that leaves its files out.
        self.drive_key(false)?;
        feed_page(|body| self.query_batch(body), cursor, limit, shape)
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
                let held = self.held(&header)?;
                header.key(held.keys(self)).map_err(remote_error)?
            }
        };
        key.with_new_iv().map_err(remote_error)
    }

    /// A private drive's payload key ([`payload_key`]), from its header, read first.
    fn payload_key(&self, file_id: RemoteFileId) -> Result<KeyHeader> {
        let header = self.raw_header(file_id)?;
        let held = self.held(&header)?;
        payload_key(&header, held.keys(self))
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
    /// `payload` as it is sent: read once, hashed, encrypted with the file `key` under a new
    /// payload IV, hashed again and paced. Its hashes follow it (odin-core #1935): the
    /// descriptor says only the algorithm.
    fn of(payload: Option<Upload<'a>>, key: &KeyHeader) -> Result<Option<Self>> {
        let Some(p) = payload else {
            return Ok(None);
        };
        let iv = sealed::strong().map_err(remote_error)?;
        let (plain, sent) = (tally(p.hash.algorithm()), tally(p.hash.algorithm()));
        let reader = Paced {
            inner: tap(Encrypting::new(tap(p.reader, &plain), &key.key, &iv), &sent),
            pace: p.pace,
        };
        Ok(Some(Sending {
            descriptor: wire::PayloadDescriptor {
                algorithm: p.hash.algorithm(),
                iv,
            },
            reader,
            planned: (p.hash, p.size),
            plain,
            sent,
        }))
    }

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

/// The parts of a write's multipart form: its `instructions`, its `metadata` (the header,
/// sealed) and, with a payload, the bytes and then their hashes, made once the bytes went.
fn parts<'p>(
    instructions: &serde_json::Value,
    metadata: Vec<u8>,
    sending: Option<&'p mut Sending<'_>>,
    key: &'p KeyHeader,
) -> Vec<Part<'p>> {
    let mut parts = vec![
        Part::bytes(
            "instructions",
            "application/json",
            instructions.to_string().into_bytes(),
        ),
        Part::bytes("metadata", wire::CONTENT_TYPE, metadata),
    ];
    if let Some(s) = sending {
        let hash_part = s.hash_part(key);
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
    parts
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
        changes_page(page, cursor, |h| self.to_remote(h))
    }

    /// A minimal page (odin-core #1967): what changed, without the headers.
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        let (page, cursor) = self.feed_page(cursor, limit, wire::Shape::Minimal)?;
        refs_page(page, cursor)
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
        holding(|body| self.query_batch(body), folders, holds)
    }

    fn hold_back(&self) -> std::time::Duration {
        HOLD_BACK
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
            None => 0..plain_size(&self.header(file_id)?)?,
        };
        if range.is_empty() {
            return Ok(0);
        }
        // A private drive's payload comes without its key: the file's header is read first
        // (one request, before the payload's holds a connection).
        let private = match self.drive_key(false)? {
            Some(_) => Some(self.payload_key(file_id)?),
            None => None,
        };
        let len = range.end - range.start;
        let path = format!("{}/payload/{PAYLOAD_KEY}", self.file(file_id));
        let at = CipherRange::of(&range);
        // One request, streamed: the client's idle timeout catches a stalled one.
        let response = fetch(&self.client, &path, at.fetch.clone()).map_err(remote_error)?;
        let key = match (response.encrypted, response.key_header.as_deref()) {
            // A payload's key header carries the payload's IV.
            (true, Some(sealed)) => SealedKeyHeader::from_base64(sealed)
                .and_then(|s| KeyHeader::open(&s, self.client.key()))
                .map_err(remote_error)?,
            (true, None) => match private {
                Some(key) => key,
                // Made private since: its key is fetched with the header.
                None => self.payload_key(file_id)?,
            },
            (false, _) => return Err(RemoteError::Fatal(format!("{path}: not encrypted"))),
        };
        decrypt(response, &key, &at, len, &path, sink)
    }
}

/// This identity's server saying it isn't connected to the one asked of (400
/// `notAConnectedIdentity`, odin-core `CircleNetworkService`, **[verified]** live 2026-11-19).
fn not_connected(e: &HomebaseError) -> bool {
    matches!(e, HomebaseError::Api { error, .. } if error.is("notAConnectedIdentity"))
}

/// What isn't `done` (read, changed) to what `sharer` shares while this identity isn't
/// connected to them: asked again after [`NOT_CONNECTED_WAIT`].
fn not_connected_to(sharer: &str, done: &str) -> RemoteError {
    RemoteError::Waiting {
        why: format!(
            "not connected to {sharer}: what they share with you can't be {done} until you are"
        ),
        retry_after: NOT_CONNECTED_WAIT,
    }
}

/// This identity's own server's refusal for want of a permission the app would need: a 403
/// that says so by its code, `missingPermissionKey` (with `permissionKeys`, the keys any one
/// of which would have done; odin-core PR #2164, **[verified]** live at b79aefa99). Another
/// identity's own such refusal never comes as one: this server passes theirs on as a plain
/// refusal. So it is always this app's.
pub(crate) fn lacks_permission(e: &HomebaseError) -> bool {
    matches!(e, HomebaseError::Api { error, .. }
        if error.status == 403 && error.is("missingPermissionKey"))
}

/// The sealed circle key `sharer`'s server answered, or how long to wait for it
/// ([`HomebaseRemote::circle_key`]).
fn sealed_or_wait(answer: SealedKey, sharer: &str) -> Result<Vec<u8>> {
    let why = match answer {
        SealedKey::Sealed(sealed) => return Ok(sealed),
        SealedKey::NotYet => format!(
            "waiting for {sharer}: a device of theirs opens the share to you when next online"
        ),
        SealedKey::NotReviewed => format!("waiting for {sharer} to review your connection"),
    };
    Err(RemoteError::Waiting {
        why,
        retry_after: SHARER_WAIT,
    })
}

/// The route of the files of the drive `alias` names.
fn files_route(alias: Uuid) -> String {
    format!("/api/v2/drives/{}/files", alias.simple())
}

/// The route of one file of the drive `alias` names.
fn file_route(alias: Uuid, file_id: RemoteFileId) -> String {
    format!("{}/{}", files_route(alias), file_id.0)
}

/// One `query-batch` page of the drive `alias` names, through `client`.
fn query_batch(
    client: &Client,
    alias: Uuid,
    body: &serde_json::Value,
) -> Result<wire::QueryBatchResponse, HomebaseError> {
    client.post(&format!("{}/query-batch", files_route(alias)), body)
}

/// A file's header as the server sends it, through `client`.
fn raw_header(
    client: &Client,
    alias: Uuid,
    file_id: RemoteFileId,
) -> Result<wire::FileHeader, HomebaseError> {
    client.get(&format!("{}/header", file_route(alias, file_id)), &[])
}

/// Stored bytes `range` of the payload at `path`, through `client`: one request.
fn fetch(client: &Client, path: &str, range: Range<u64>) -> Result<Fetched, HomebaseError> {
    // The route names the range as well; the Range header decides.
    let (start, len) = (range.start, range.end - range.start);
    client.fetch(&format!("{path}/{start}/{len}"), start, range.end - 1)
}

/// odin-core's query leaves out what changed in its last second (`HoldBackWindowMs`, #1923,
/// homebase-api-notes), and a little more for the clocks.
const HOLD_BACK: std::time::Duration = std::time::Duration::from_millis(1100);

/// One page of the change feed in `shape` through `query_batch`, and the cursor after it: an
/// empty page of an empty drive has no cursor yet, so the next starts from the beginning.
fn feed_page(
    query_batch: impl Fn(&serde_json::Value) -> Result<wire::QueryBatchResponse>,
    cursor: Option<&str>,
    limit: usize,
    shape: wire::Shape,
) -> Result<(wire::QueryBatchResponse, String)> {
    let mut page = query_batch(&wire::feed_query(cursor, limit, shape))?;
    let next = page
        .cursor_state
        .take()
        .or_else(|| cursor.map(str::to_owned))
        .unwrap_or_default();
    Ok((page, next))
}

/// A payload's key, as its file's `header` gives it (opened with `keys`): the file key, with the
/// payload's IV. A header read after the payload was written over has another IV: the bytes
/// then don't match their hash, and the download is tried again.
fn payload_key(header: &wire::FileHeader, keys: Keys<'_>) -> Result<KeyHeader> {
    if header.file_state == "deleted" {
        return Err(RemoteError::Deleted);
    }
    let key = header.key(keys).map_err(remote_error)?;
    let iv = header
        .file_metadata
        .sync_payload()
        .ok_or_else(|| RemoteError::StaleRange(format!("file {}: no payload now", header.file_id)))
        .and_then(|p| wire::payload_iv(p).map_err(RemoteError::Fatal))?;
    Ok(KeyHeader { iv, key: key.key })
}

/// The plaintext size of a file's payload, for a read without a range: a tombstone's is
/// deleted, as the payload's own answer would say (`fileDeleted`).
fn plain_size(header: &RemoteHeader) -> Result<u64> {
    match &header.body {
        RemoteBody::Deleted => Err(RemoteError::Deleted),
        RemoteBody::Live {
            payload: Some(_),
            content: NodeContent {
                size: Some(size), ..
            },
            ..
        } => Ok(*size),
        _ => Err(RemoteError::NotFound),
    }
}

/// A full feed page, each row read with `read`: rows that aren't usable sync nodes come back in
/// [`ChangesPage::skipped`], with why.
fn changes_page(
    page: wire::QueryBatchResponse,
    cursor: String,
    mut read: impl FnMut(wire::FileHeader) -> Result<std::result::Result<RemoteHeader, String>>,
) -> Result<ChangesPage> {
    let mut entries = Vec::with_capacity(page.search_results.len());
    let mut skipped = Vec::new();
    for row in &page.search_results {
        let header = match serde_json::from_str::<wire::FileHeader>(row.get()) {
            Ok(h) => read(h)?,
            Err(e) => Err(format!("unreadable header: {e}")),
        };
        match header {
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

/// A minimal feed page (odin-core #1967): what changed, without the headers.
fn refs_page(page: wire::QueryBatchResponse, cursor: String) -> Result<RefsPage> {
    // A server without #1967 ignores the shape and sends headers: an empty page there would
    // skip what changed.
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

/// [`Remote::holding`] through `query_batch`: one `groupId` query a page at a time (any type,
/// active files only).
fn holding(
    query_batch: impl Fn(&serde_json::Value) -> Result<wire::QueryBatchResponse>,
    folders: &[NodeId],
    holds: Holds<'_>,
) -> Result<HashSet<NodeId>> {
    let mut holding = HashSet::new();
    for some in folders.chunks(CHILDREN_QUERY_FOLDERS) {
        let fetch = |cursor: Option<&str>| query_batch(&wire::children_query(some, cursor, 500));
        // Ended short, it is as before: what the pages read.
        read_pages(fetch, |page| {
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
            Ok(!some.iter().all(|f| holding.contains(f)))
        })?;
    }
    Ok(holding)
}

/// Plaintext `len` bytes of the payload at `path` from `response`, the stored bytes `at` asks
/// for, decrypted with `key` (the payload's): a range that reaches the payload's last block has
/// its padding removed, and comes back short if it runs past the plaintext.
fn decrypt(
    mut response: Fetched,
    key: &KeyHeader,
    at: &CipherRange,
    len: u64,
    path: &str,
    sink: &mut dyn Write,
) -> Result<u64> {
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
        // A deleted file's payload (a 404, odin-core PR #2141): a tombstone, said without a
        // header read.
        "filedeleted" => RemoteError::Deleted,
        // An upload whose hash part didn't arrive: sent again like one refused.
        "payloadhashmismatch" | "payloadhashmissing" => RemoteError::HashMismatch,
        "invalidpayloadhash" => RemoteError::InvalidHash,
        "payloadhashrequired" => RemoteError::HashRequired,
        // Through a peer route, the other identity's server not answering now (odin-core
        // `PeerCircleShareKeyService`, `PeerAppNotificationService`): it may again.
        "remoteserverreturnedunavailable" => {
            RemoteError::Transient("the other identity's server isn't answering".into())
        }
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

/// How a Homebase answer maps onto the trait's errors (`homebase-api-notes.md`, Errors):
/// [`mapped`], a local I/O error kept as it is.
fn remote_error(e: HomebaseError) -> RemoteError {
    match e {
        HomebaseError::Io(e) if !source_ended(&e) => RemoteError::Io(e),
        e => mapped(&e, true),
    }
}

/// The one mapping of a Homebase answer onto the trait's errors, with its words when `words`
/// (its class and refusal need none, [`HomebaseError::class`]); not for a local I/O error,
/// which stays its own (rebuilt, it would lose its OS error code).
fn mapped(e: &HomebaseError, words: bool) -> RemoteError {
    let say = |why: &dyn Fn() -> String| if words { why() } else { String::new() };
    match e {
        HomebaseError::Network(why) => RemoteError::Transient(say(&|| why.clone())),
        HomebaseError::Denied(why) => {
            RemoteError::Denied(say(&|| why.clone()), Refusal::Credentials)
        }
        HomebaseError::NoDriveKey => RemoteError::Denied(say(&|| PRIVATE.into()), Refusal::NoKey),
        // A source that ended early: the only I/O error that comes here.
        HomebaseError::Io(_) => RemoteError::HashMismatch,
        HomebaseError::Api { what, error } => {
            api_error(error, || say(&|| format!("{what}: {error}")))
        }
        e @ (HomebaseError::NoDrive(_)
        | HomebaseError::Protocol(_)
        | HomebaseError::Crypto(_)
        | HomebaseError::Json(_)) => RemoteError::Fatal(say(&|| e.to_string())),
    }
}

/// A source that ended early: it changed since it was hashed.
fn source_ended(e: &std::io::Error) -> bool {
    e.get_ref().is_some_and(|e| e.is::<SourceEnded>())
}

/// The trait's error for an error answer, `why` its words (the class needs none).
fn api_error(error: &ApiError, why: impl Fn() -> String) -> RemoteError {
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

    /// Why the identity refused this computer, if it did.
    pub fn refusal(&self) -> Option<Refusal> {
        self.classified().1
    }

    /// What it means ([`ErrorClass`]): as the drive's error for it would say.
    pub fn class(&self) -> ErrorClass {
        self.classified().0
    }

    /// Its class and refusal, as the drive's error for it ([`remote_error`]) has them, without
    /// its words.
    fn classified(&self) -> (ErrorClass, Option<Refusal>) {
        match self {
            HomebaseError::Io(e) if !source_ended(e) => (ErrorClass::of_io(e), None),
            e => {
                let mapped = mapped(e, false);
                (mapped.class(), mapped.refusal())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A private drive without its key refuses this computer; a shared one needs none.
    #[test]
    fn a_private_drive_without_its_key_asks_to_log_in_again() {
        let state = |private: bool, key: Option<Key>| KeyState {
            private,
            key,
            ..KeyState::default()
        };
        // Told apart from other refusals, with the same words and class.
        let lacking = state(true, None).key();
        assert!(
            matches!(lacking, Err(HomebaseError::NoDriveKey)),
            "{lacking:?}"
        );
        let lacking = lacking.expect_err("refused");
        assert_eq!(lacking.class(), ErrorClass::Denied);
        assert_eq!(lacking.to_string(), format!("denied: {PRIVATE}"));
        // The grant left out by the server, for a login holding the app's key: the server's
        // to fix, not a sign-in's, and never read as shared.
        let unlisted = KeyState {
            unlisted: true,
            ..state(true, None)
        };
        assert!(matches!(unlisted.key(), Err(HomebaseError::Protocol(_))));
        assert_eq!(state(false, None).key().ok(), Some(None));
        assert_eq!(state(true, Some([1; 16])).key().ok(), Some(Some([1; 16])));
    }

    /// The drive's answer says whether it is private (`sharingModel`, absent before odin-core
    /// #2046: shared), as the security context may leave its grant out.
    #[test]
    fn the_drive_answer_says_whether_it_is_private() {
        let answer = |model: serde_json::Value| {
            serde_json::from_value::<DriveAnswer>(json!({
                "targetDrive": { "alias": Uuid::from_u128(1), "type": Uuid::from_u128(2) },
                "created": 1,
                "sharingModel": model,
            }))
            .expect("a drive answer")
            .private()
        };
        assert!(answer(json!("selectiveShare")));
        assert!(answer(json!("private")));
        assert!(!answer(json!("collaborative")));
        assert!(!answer(json!(null)));
    }

    /// The app's grant on the drive is found whatever shape the others have: whether the
    /// drive is private, and its locked key.
    #[test]
    fn the_context_gives_the_drives_grant() {
        let drive = TargetDrive {
            alias: Uuid::from_u128(1),
            drive_type: Uuid::from_u128(2),
        };
        let grant = |alias: u128, zero_knowledge: bool, locked: Option<&str>| {
            json!({
                "permissionedDrive": {
                    "drive": { "alias": Uuid::from_u128(alias), "type": Uuid::from_u128(2) },
                    "permission": ["read", "write"],
                },
                "hasStorageKey": false,
                "isZeroKnowledge": zero_knowledge,
                "zkAppKeyEncryptedStorageKey": locked,
            })
        };
        let context = |grants: Vec<serde_json::Value>| {
            json!({ "caller": {}, "permissionContext": { "permissionGroups": [
                { "driveGrants": [ { "odd": true } ] },
                { "driveGrants": grants },
            ] } })
        };
        let found = |c: serde_json::Value| {
            drive_grant(&c, &drive).map(|g| (g.zero_knowledge, g.locked.map(str::to_owned)))
        };
        let write_only = |c: serde_json::Value| {
            drive_grant(&c, &drive).and_then(|g| g.write_only.map(str::to_owned))
        };
        let private = context(vec![
            grant(3, true, Some("other")),
            grant(1, true, Some("ours")),
        ]);
        assert_eq!(found(private.clone()), Some((true, Some("ours".into()))));
        assert_eq!(write_only(private), None);
        let mut with_key = grant(1, true, Some("ours"));
        with_key["zkStorageKeyEncryptedWriteOnlyPrivateKey"] = json!("wo");
        assert_eq!(write_only(context(vec![with_key])), Some("wo".into()));
        // Private, its key not locked for the app yet.
        let no_copy = context(vec![grant(1, true, None)]);
        assert_eq!(found(no_copy), Some((true, None)));
        let shared = context(vec![grant(1, false, None)]);
        assert_eq!(found(shared), Some((false, None)));
        // Not listed (odin-core's bug for an app without permission keys).
        assert_eq!(found(context(vec![grant(3, true, Some("other"))])), None);
        assert_eq!(found(json!({})), None);
    }

    /// The security context's permission keys, in any group.
    #[test]
    fn the_context_gives_the_apps_permission_keys() {
        let context = json!({ "permissionContext": { "permissionGroups": [
            { "permissionSet": { "keys": [10] } },
            { "permissionSet": null },
            { "permissionSet": { "keys": [305, 210] } },
        ] } });
        assert!(holds_key(&context, app::USE_TRANSIT_READ));
        assert!(holds_key(&context, app::USE_TRANSIT_WRITE));
        assert!(!holds_key(&context, 50));
        assert!(!holds_key(&json!({}), app::USE_TRANSIT_READ));
        // Which of the two the app asks for it lacks: none here; the one to send, of an
        // install from before it asked for that; both, of one from before either.
        assert!(lacks_in(&context).none());
        let reads = json!({ "permissionContext": { "permissionGroups": [
            { "permissionSet": { "keys": [305] } },
        ] } });
        let only_sends = app::Lacks {
            reads: false,
            sends: true,
        };
        assert_eq!(lacks_in(&reads), only_sends);
        assert_eq!(lacks_in(&json!({})), app::Lacks::ALL);
    }

    /// An error answer of the identity itself.
    pub(super) fn api(status: u16, code: Option<&str>) -> HomebaseError {
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
        #[cfg(windows)]
        const LOCKED: i32 = 32; // ERROR_SHARING_VIOLATION
        #[cfg(not(windows))]
        const LOCKED: i32 = 16; // EBUSY
        let mut proxy = api(404, None);
        if let HomebaseError::Api { error, .. } = &mut proxy {
            error.from_identity = false;
        }
        let errors = || {
            [
                HomebaseError::Network("cut".into()),
                HomebaseError::Denied("401".into()),
                HomebaseError::NoDriveKey,
                HomebaseError::NoDrive("a.example".into()),
                HomebaseError::Crypto("bad".into()),
                HomebaseError::Json(serde_json::from_str::<u8>("x").expect_err("not json")),
                api(503, None),
                api(429, None),
                api(404, None),
                api(400, Some("versionTagMismatch")),
                api(400, None),
                api(507, None),
                HomebaseError::Protocol("odd".into()),
                HomebaseError::Io(std::io::Error::other(SourceEnded { missing: 1 })),
                HomebaseError::Io(std::io::Error::other("disk")),
                // Locked by another program: told by its OS error code alone.
                HomebaseError::Io(std::io::Error::from_raw_os_error(LOCKED)),
            ]
        };
        // Every variant: its class and refusal are those of the drive's error for it.
        for e in errors() {
            let said = (e.class(), e.refusal());
            let remote = remote_error(e);
            assert_eq!(said, (remote.class(), remote.refusal()), "{remote}");
        }
        assert_eq!(api(503, None).class(), ErrorClass::Transient);
        assert_eq!(proxy.class(), ErrorClass::Transient);
        assert_eq!(api(404, None).class(), ErrorClass::Gone);
        // Why the identity refused this computer: typed.
        let refusal = |e: HomebaseError| (e.refusal(), remote_error(e).refusal());
        let key = Some(Refusal::NoKey);
        assert_eq!(refusal(HomebaseError::NoDriveKey), (key, key));
        let login = Some(Refusal::Credentials);
        assert_eq!(refusal(HomebaseError::Denied("401".into())), (login, login));
        assert_eq!(refusal(api(503, None)), (None, None));
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
            {"fileId": asked[0].at.file_id.0, "newVersionTag": new, "updated": 1_760_000_000_999i64,
             "versionUpdated": 1_760_000_000_123i64,
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
            {"fileId": ids[0], "errorCode": null, "updated": 1_760_000_000_999i64,
             "versionUpdated": 1_760_000_000_123i64,
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
            // A deleted file's payload.
            (api(404, Some("fileDeleted")), "Deleted"),
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
            (HomebaseError::NoDriveKey, "Denied"),
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
        let deleted = br#"{"title":"Not Found","status":404,"errorCode":"fileDeleted"}"#;
        let cases: [(String, &str); 14] = [
            (read(400, tombstone, true, false), "Deleted"),
            (read(400, tombstone, false, false), "Transient"),
            (read(404, deleted, true, false), "Deleted"),
            (read(404, deleted, false, false), "Transient"),
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

    /// A page of no rows, with `cursor` and `more` to come.
    fn page(cursor: Option<&str>, more: bool) -> wire::QueryBatchResponse {
        serde_json::from_value(json!({
            "cursorState": cursor, "hasMoreRows": more, "queryTime": 1,
        }))
        .expect("a page")
    }

    /// A listing of markers cut short, or coming back to a cursor it gave, is an error; a last
    /// page may give the cursor it was asked from.
    #[test]
    fn a_markers_listing_is_read_whole_or_not_at_all() {
        let read = |answers: Vec<wire::QueryBatchResponse>| {
            let mut answers = answers.into_iter();
            let fetch = |_: Option<&str>| answers.next().ok_or(RemoteError::NotFound);
            read_markers("frodo.test", &[3; 16], fetch, |_| Ok(None))
        };
        let last = vec![page(Some("a"), true), page(Some("a"), false)];
        assert!(read(last).is_ok());
        let cycle = vec![
            page(Some("a"), true),
            page(Some("b"), true),
            page(Some("a"), true),
        ];
        assert!(matches!(read(cycle), Err(RemoteError::Transient(_))));
        let cut = vec![page(None, true)];
        assert!(matches!(read(cut), Err(RemoteError::Transient(_))));
        let held = vec![page(Some("a"), true), page(Some("a"), true)];
        let got = read(held).expect_err("cut short");
        assert!(got.to_string().contains("without a new cursor"), "{got}");
    }

    /// Every page's markers are read, and a drive refusing this computer ends the listing.
    #[test]
    fn every_pages_markers_are_read() {
        let key = crate::drive::sealed::KeyHeader::random().expect("random");
        let marker = |who: &str| {
            json!({
                "fileId": Uuid::new_v4(),
                "fileState": "active",
                "sharedSecretEncryptedKeyHeader": key.seal(&[3; 16], &[7; 16]),
                "fileMetadata": {
                    "isEncrypted": true,
                    "versionTag": Uuid::new_v4(),
                    "appData": {
                        "uniqueId": markers::marker_id(who, "frodo.test"),
                        "fileType": hbsync_types::schema::FILE_TYPE_MARKER,
                        "content": key.seal_text(&format!(
                            r#"{{"odinId":"{who}","driveAlias":"{}","sync":true}}"#,
                            Uuid::nil()
                        )),
                    },
                },
            })
        };
        let pages = || {
            [("a", true, "sam.test"), ("b", false, "merry.test")].map(|(cursor, more, who)| {
                serde_json::from_value::<wire::QueryBatchResponse>(json!({
                    "cursorState": cursor, "hasMoreRows": more, "queryTime": 1,
                    "searchResults": [marker(who)],
                }))
                .expect("a page")
            })
        };
        let mut answers = pages().into_iter();
        let fetch = |_: Option<&str>| answers.next().ok_or(RemoteError::NotFound);
        let found = read_markers("frodo.test", &[3; 16], fetch, |_| Ok(None)).expect("read");
        let names: Vec<_> = found.synced.iter().map(|s| s.odin_id.as_str()).collect();
        assert_eq!(names, ["sam.test", "merry.test"]);
        let mut answers = pages().into_iter();
        let fetch = |_: Option<&str>| answers.next().ok_or(RemoteError::NotFound);
        let refuse = |_| Err(RemoteError::Denied("no key".into(), Refusal::NoKey));
        let got = read_markers("frodo.test", &[3; 16], fetch, refuse);
        assert!(matches!(got, Err(RemoteError::Denied(..))), "{got:?}");
    }

    /// Pages are read on while more come with a new cursor; one promising more without one ends
    /// the reading short, as does the reader having what it needs.
    #[test]
    fn pages_are_read_to_the_last() {
        let read = |answers: Vec<wire::QueryBatchResponse>, wanted: usize| {
            let mut answers = answers.into_iter();
            let mut asked = Vec::new();
            let mut seen = 0;
            let ended = read_pages(
                |cursor| {
                    asked.push(cursor.map(str::to_owned));
                    answers.next().ok_or(RemoteError::NotFound)
                },
                |_| {
                    seen += 1;
                    Ok(seen < wanted)
                },
            );
            (ended.map_err(|e| e.to_string()), asked)
        };
        let some = |c: &str| Some(c.to_owned());
        let all = vec![
            page(Some("a"), true),
            page(Some("b"), true),
            page(Some("c"), false),
        ];
        assert_eq!(
            read(all, 9),
            (Ok(Ended::Done), vec![None, some("a"), some("b")])
        );
        let held = vec![page(Some("a"), true), page(Some("a"), true)];
        assert_eq!(read(held, 9), (Ok(Ended::CutShort), vec![None, some("a")]));
        let lost = vec![page(None, true)];
        assert_eq!(read(lost, 9), (Ok(Ended::CutShort), vec![None]));
        let enough = vec![page(Some("a"), true), page(Some("b"), true)];
        assert_eq!(read(enough, 1), (Ok(Ended::Done), vec![None]));
    }

    /// A drive without a write-only key opens nothing shared: it refuses (the owner's to lift),
    /// before any request (the identity is unreachable here).
    #[test]
    fn no_write_only_key_asks_nothing() -> Result<(), HomebaseError> {
        // Nor the permission: the key is looked at first (the permission costs a request, and
        // alone it wouldn't help).
        let own = unreachable_remote(KeyState::default())?;
        let sharer = markers::Sharer {
            odin_id: "sam.example".into(),
            drive_alias: Uuid::new_v4(),
        };
        match own.circle_key(&sharer, Uuid::new_v4()) {
            Err(RemoteError::Denied(why, Refusal::NoKey)) => {
                assert!(why.contains("no key"), "{why}")
            }
            other => panic!("{other:?}"),
        }
        // One there that doesn't open: the same, before the permission is looked at.
        let own = unreachable_remote(KeyState {
            private: true,
            key: Some([9; 16]),
            write_only: Some("bm90IGEga2V5".into()),
            ..KeyState::default()
        })?;
        match own.circle_key(&sharer, Uuid::new_v4()) {
            Err(RemoteError::Denied(_, Refusal::NoKey)) => {}
            other => panic!("{other:?}"),
        }
        Ok(())
    }

    /// On a drive that isn't private the markers aren't read: nobody is turned on, nobody
    /// off, and nothing is asked (the identity is unreachable here). On a private one they
    /// are read, which fails here.
    #[test]
    fn markers_are_read_on_a_private_drive_only() -> Result<(), HomebaseError> {
        let open = unreachable_remote(KeyState::default())?;
        let markers = open.markers().expect("nothing asked");
        assert!(markers.synced.is_empty());
        assert!(markers.unsure("sam.example"));
        let private = unreachable_remote(KeyState {
            private: true,
            key: Some([9; 16]),
            ..KeyState::default()
        })?;
        assert!(private.markers().is_err());
        Ok(())
    }

    /// This identity's server saying it isn't connected to the sharer puts the share off an
    /// hour, in words that name them: nothing a retry in minutes changes, and no failure.
    #[test]
    fn not_connected_to_the_sharer_waits_an_hour_and_says_so() -> Result<(), HomebaseError> {
        let own = unreachable_remote(KeyState::default())?;
        let not_connected = || Err::<(), _>(api(400, Some("notAConnectedIdentity")));
        match own.ask_peer("sam.example", not_connected) {
            Err(RemoteError::Waiting { why, retry_after }) => {
                assert!(why.starts_with("not connected to sam.example"), "{why}");
                assert_eq!(retry_after, NOT_CONNECTED_WAIT);
            }
            other => panic!("{other:?}"),
        }
        // Another 400 of theirs stays the failure it is.
        let other = || Err::<(), _>(api(400, Some("invalidGlobalTransitId")));
        assert!(matches!(
            own.ask_peer("sam.example", other),
            Err(RemoteError::Fatal(_))
        ));
        Ok(())
    }

    /// This identity's server says by its code where the app lacks the permission to ask
    /// another's (as it answers it, seen live): refused as that, an approval lifts it, and
    /// nothing more is asked (the identity is unreachable here). A 403 without that code is
    /// the other identity's refusal, as before.
    #[test]
    fn the_apps_want_of_the_permission_is_told_by_its_code() -> Result<(), HomebaseError> {
        let sent = |body: &[u8]| HomebaseError::Api {
            what: "/api/v2/peer/notify/token".into(),
            error: ApiError::parse(403, body),
        };
        let lacking = br#"{"title":"Forbidden: Does not have permission","status":403,"errorCode":"missingPermissionKey","permissionKeys":[305]}"#;
        let by_number = br#"{"status":403,"errorCode":1006,"permissionKeys":[210,305]}"#;
        let own = unreachable_remote(KeyState::default())?;
        for body in [&lacking[..], &by_number[..]] {
            assert!(lacks_permission(&sent(body)));
            match own.ask_peer("sam.example", || Err::<(), _>(sent(body))) {
                Err(RemoteError::Denied(why, Refusal::Permission)) => {
                    assert_eq!(why, NOT_PERMITTED)
                }
                other => panic!("{other:?}"),
            }
        }
        // Theirs: no code, the server's "no code", or another one. And the code on another
        // status isn't it.
        let codeless = br#"{"title":"Forbidden","status":403,"errorCode":"noErrorCode"}"#;
        for theirs in [
            api(403, None),
            sent(codeless),
            api(403, Some("notReviewedByOwner")),
            api(400, Some("missingPermissionKey")),
            api(401, None),
            HomebaseError::Network("cut".into()),
        ] {
            assert!(!lacks_permission(&theirs), "{theirs:?}");
        }
        let refused = own.ask_peer("sam.example", || Err::<(), _>(api(403, None)));
        assert!(
            !matches!(refused, Err(RemoteError::Denied(_, Refusal::Permission))),
            "{refused:?}"
        );
        Ok(())
    }

    /// A circle key not sealed yet, or a connection not reviewed, waits on the sharer: asked
    /// again later, never a refusal.
    #[test]
    fn a_sharer_not_ready_is_waited_for() {
        assert_eq!(
            sealed_or_wait(SealedKey::Sealed(vec![1]), "sam.example").ok(),
            Some(vec![1])
        );
        for (answer, says) in [
            (SealedKey::NotYet, "when next online"),
            (SealedKey::NotReviewed, "review your connection"),
        ] {
            match sealed_or_wait(answer, "sam.example") {
                Err(RemoteError::Waiting { why, retry_after }) => {
                    assert!(why.contains("sam.example") && why.contains(says), "{why}");
                    assert_eq!(retry_after, SHARER_WAIT);
                }
                other => panic!("{other:?}"),
            }
        }
    }

    /// The drive of an identity that can't be reached, what is known of its key as `state`
    /// says (read at the client's start: not read again).
    pub(super) fn unreachable_remote(state: KeyState) -> Result<HomebaseRemote, HomebaseError> {
        let credentials = crate::youauth::Credentials {
            identity: "unreachable.invalid".into(),
            client_auth_token: "AAAA".into(),
            shared_secret: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            zk_app_key: None,
        };
        Ok(HomebaseRemote {
            client: Client::new(&credentials)?,
            drive: TargetDrive {
                alias: Uuid::nil(),
                drive_type: Uuid::nil(),
            },
            made: 0,
            drive_key: DriveKey(Mutex::new(state)),
            sends_asking: Mutex::default(),
        })
    }

    /// A private drive whose key this login doesn't hold refuses this computer for want of the
    /// key, typed so, as the folders' syncs see it; with the key there, it is read.
    #[test]
    fn a_missing_key_is_a_typed_refusal() -> Result<(), HomebaseError> {
        let state = |key| KeyState {
            private: true,
            key,
            ..KeyState::default()
        };
        let remote = unreachable_remote(state(None))?;
        let refused = remote.drive_key(false).expect_err("refused");
        assert_eq!(refused.refusal(), Some(Refusal::NoKey), "{refused}");
        assert_eq!(refused.to_string(), format!("access denied: {PRIVATE}"));
        *remote.drive_key.lock() = state(Some([1; 16]));
        assert_eq!(remote.drive_key(false).ok(), Some(Some([1; 16])));
        Ok(())
    }

    /// The vectors' write-only key as a grant carries it: locked with `drive`'s key.
    fn locked_write_only(drive: &Key) -> String {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        let pkcs8 = share_keys::tests::bytes(share_keys::tests::WRITE_ONLY_PKCS8);
        let iv = [4u8; 12];
        let sealed = crate::crypto::gcm_seal(drive, &iv, &pkcs8, &[]).expect("sealed");
        STANDARD.encode([&iv[..], &sealed].concat())
    }

    /// What is known of a private drive's keys: its `key`, and its write-only key as the grant
    /// has it.
    fn private_state(key: Option<Key>, write_only: Option<&str>) -> KeyState {
        KeyState {
            private: true,
            key,
            write_only: write_only.map(str::to_owned),
            ..KeyState::default()
        }
    }

    /// The drive's write-only key opens from the grant with the drive's key; a drive without
    /// one (a shared drive, or no copy in the grant) has none to give.
    #[test]
    fn the_write_only_key_opens_with_the_drives_key() {
        let drive: Key = [9; 16];
        let locked = locked_write_only(&drive);
        let key = open_write_only(&drive, &locked).expect("opens");
        assert!(share_keys::tests::unseal_with(&key, "sam.dotyou.cloud").is_ok());
        // Locked with another key: none here.
        assert!(open_write_only(&[1; 16], &locked).is_none());
        let state = private_state;
        let read = |s: KeyState| s.write_only().map(|key| key.is_some());
        assert_eq!(read(state(Some(drive), Some(&locked))).ok(), Some(true));
        assert_eq!(read(state(Some([1; 16]), Some(&locked))).ok(), Some(false));
        // Opened once for a state: asked again, it is the same key.
        let held = state(Some(drive), Some(&locked));
        let (first, again) = (
            held.write_only().expect("read"),
            held.write_only().expect("read"),
        );
        assert!(Arc::ptr_eq(&first.expect("opens"), &again.expect("opens")));
        // None in the grant, or a shared drive: none here.
        assert_eq!(read(state(Some(drive), None)).ok(), Some(false));
        let shared = KeyState {
            private: false,
            ..state(None, Some(&locked))
        };
        assert_eq!(read(shared).ok(), Some(false));
        // A private drive without its key refuses, as for everything else.
        assert!(matches!(
            read(state(None, Some(&locked))),
            Err(HomebaseError::NoDriveKey)
        ));
    }

    /// A file someone added to a folder shared with them is read on the owner's computer with
    /// the drive's write-only key, opened from the grant with the drive's key; where the grant
    /// has no copy, or one that doesn't open, the file is left unread and says why, and a
    /// private drive without its key refuses this computer, as for any file.
    #[test]
    fn a_file_a_member_added_is_read_with_the_grants_write_only_key()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let drive: Key = [9; 16];
        let locked = locked_write_only(&drive);
        let state = private_state;
        // A folder, as stored: its name under the file key and the header IV.
        let (kh, sent) = share_keys::tests::sealed_key_header();
        let header: wire::FileHeader = serde_json::from_value(json!({
            "fileId": Uuid::from_u128(7),
            "fileState": "active",
            "sharedSecretEncryptedKeyHeader": sent,
            "fileMetadata": {
                "isEncrypted": true,
                "versionTag": Uuid::from_u128(8),
                "appData": {
                    "uniqueId": Uuid::from_u128(9),
                    "groupId": NodeId::ROOT.0,
                    "fileType": hbsync_types::schema::FILE_TYPE_FOLDER,
                    "content": kh.seal_text(r#"{"v":1,"name":"From a member"}"#),
                },
            },
        }))?;

        let remote = unreachable_remote(state(Some(drive), Some(&locked)))?;
        let read = remote.to_remote(header.clone())??;
        match &read.body {
            RemoteBody::Live { content, .. } => assert_eq!(content.name, "From a member"),
            other => panic!("{other:?}"),
        }
        assert!(read.token.is_some());

        for no_key in [
            state(Some(drive), None),
            state(Some([1; 16]), Some(&locked)),
        ] {
            let remote = unreachable_remote(no_key)?;
            let why = remote.to_remote(header.clone())?.expect_err("opened");
            assert!(why.contains("write-only key"), "{why}");
        }
        let remote = unreachable_remote(state(None, Some(&locked)))?;
        let refused = remote.to_remote(header.clone()).expect_err("read");
        assert_eq!(refused.refusal(), Some(Refusal::NoKey), "{refused}");

        // On a drive not known to be private the file is left unread, and no more: the drive
        // isn't taken for private (which would refuse this computer), nor asked about (the
        // identity can't be reached here).
        let shared = KeyState {
            private: false,
            ..state(None, Some(&locked))
        };
        let remote = unreachable_remote(shared)?;
        let why = remote.to_remote(header)?.expect_err("opened");
        assert!(why.contains("write-only key"), "{why}");
        assert!(!remote.drive_key.lock().private);
        Ok(())
    }

    /// The app's want of the permission to send, read a moment ago, is not asked about again
    /// at once (the identity can't be reached here); holding it is never asked about.
    #[test]
    fn the_permission_to_send_is_not_asked_for_at_every_write() -> Result<(), HomebaseError> {
        let state = |sends, sends_read| KeyState {
            sends,
            sends_read,
            ..KeyState::default()
        };
        let just_read = Some(std::time::Instant::now());
        let remote = unreachable_remote(state(false, just_read))?;
        assert_eq!(remote.may_send().ok(), Some(false));
        let remote = unreachable_remote(state(true, None))?;
        assert_eq!(remote.may_send().ok(), Some(true));
        // A send refused for want of it: not held from then on, and not asked about at once.
        remote.may_not_send();
        assert_eq!(remote.may_send().ok(), Some(false));
        Ok(())
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
        let remote = unreachable_remote(KeyState::default())?;
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
