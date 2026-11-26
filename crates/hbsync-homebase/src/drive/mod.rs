//! The Filesync drive as a [`Remote`]: node schema v1 on the identity's V2 drive API.
//!
//! The drive is found by its slugs (`GET /api/v2/apps/filesync/drives/filesync`); file routes
//! then take its alias, as own-identity routes by slug don't exist yet (odin-core #1902).
//! Facts: `docs/spec/homebase-api-notes.md`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::ops::Range;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use hbsync_fileio::{HashAlgorithm, Hasher};
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ErrorClass, ForGood, Holds, NodeMeta, Paced, Planned, RefsPage,
    Refusal, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::{ARCHIVAL_REMOVED, FILE_TYPE_FOLDER, NodeContent, PAYLOAD_KEY};
use hbsync_types::{
    ContentHash, Doomed, NodeId, NodeKind, RemoteFileId, RemoteRef, Target, VersionTag, Written,
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
use crate::drive::shared_as::{Folders, Share, Shares, alike};
use crate::multipart::{Multipart, Part, SourceEnded};
use crate::notify::{Event, Events, Notifier};
use crate::peer::{PeerToken, SealedKey};
use crate::{ApiError, HomebaseError, app};

pub mod markers;
pub mod sealed;
mod share_keys;
pub mod shared;
mod shared_as;
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

/// The most shares one request takes, an upload's too (**[verified]** odin-core
/// `DriveShareService.MaxSharesPerRequest`): a file whose folder has more goes up with so
/// many of them.
const MOST_SHARES: usize = 100;

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
    /// How the folders this computer adds to are shared ([`HomebaseRemote::shared_as`]).
    folders: Folders,
    /// Held by whoever reads how a folder is shared, or a circle's key: of the uploads that
    /// go into one folder at once, one reads, and the rest go by what it learnt.
    shares_reading: Mutex<()>,
    /// Held for writing by a folder's move that changes shares ([`HomebaseRemote::moved`]);
    /// for reading by every other move, by whoever reads how a folder is shared, and by a
    /// new node held to its folder ([`HomebaseRemote::settle_new`]). So what is added to a
    /// folder, or moved out of it, while it moves is listed by the move or learns how the
    /// folder is shared once the move is done; and nothing gives or withdraws a share by
    /// what such a move is changing.
    moves: RwLock<()>,
    /// The keys of this identity's own circles, each read once ([`HomebaseRemote::own_circle`]):
    /// a circle's key is set once and stays (**[verified]** odin-core
    /// `CircleShareKeyAlreadySet`). None: one that isn't to be had in this run.
    circles: Mutex<HashMap<Uuid, Option<Arc<CircleKey>>>>,
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
            folders: Folders::default(),
            shares_reading: Mutex::default(),
            moves: RwLock::default(),
            circles: Mutex::default(),
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
        let size = payload.as_ref().map_or(0, |p| p.size);
        let look_first = size >= LOOK_FIRST;
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
        // A new node only: a write of one that is there keeps its shares as they are. And on
        // a private drive only: shares are a Selective Share drive's, which is one.
        let (folder, shared) = match (target, &drive) {
            (None, Some(_)) => self.shares_of_new(meta, &key, size)?,
            _ => (Shares::default(), None),
        };
        let keys = self.keys(drive.as_ref());
        let sent = shared.as_ref().map(|given| &given.sent);
        let metadata = wire::metadata(meta, version, keys, &transfer_iv, &key, sent);
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
            .map_err(|e| {
                let for_shares = may_be_for_shares(&e);
                let e = self.write_error(e);
                match &shared {
                    Some(given) if for_shares => self.sent_shared(e, meta, &folder, given),
                    // Refused without shares too: it wasn't for them the last time.
                    None if for_shares && target.is_none() => {
                        self.folders.not_for_its_shares(meta.parent);
                        e
                    }
                    _ => e,
                }
            })?;
        let at = RemoteRef {
            file_id: RemoteFileId(file_id),
            version: VersionTag(new_version_tag),
        };
        match (target, meta.kind) {
            (None, kind) if drive.is_some() => {
                let as_sent = self.settle_new(meta, at, &key, &folder, shared.as_ref());
                match (kind, as_sent) {
                    // What is added to a new folder is shared as the folder was made, or not
                    // at all: nothing is asked for it (a new tree costs no request per
                    // folder).
                    (NodeKind::Folder, true) => {
                        let shares = shared.as_ref().map(|given| Arc::clone(&given.shares));
                        (self.folders).made(meta.node, at, shares.unwrap_or_default());
                    }
                    // Shared otherwise by now, or not known how: asked when next added to.
                    (NodeKind::Folder, false) => self.folders.forget(meta.node),
                    (NodeKind::File, _) => {}
                }
            }
            // A folder written again is shared as it was: its new version is this write's.
            (Some(_), NodeKind::Folder) => self.folders.rewritten(meta.node, at),
            _ => {}
        }
        Ok(Written {
            target: Target {
                at,
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

    /// What becomes of the error `e` of an upload of the new node `meta` that went up
    /// shared as `given`, where the server's refusal may be for the shares
    /// ([`may_be_for_shares`]): a circle disabled or gone since, a share that ended on the
    /// way (the server looks at the shares only once the bytes are in). The file is to go
    /// up all the same, so it is tried again soon:
    /// - where a share that was sent has ended since, with the folder's shares read again
    ///   (the ones that last: the others weren't refused);
    /// - else without them, as is what is added to that folder for a while
    ///   ([`Folders::refused`]; where the folder changed meanwhile, with what it shares now).
    ///   Which share it was for, the server doesn't say: none is given. The circles' keys
    ///   are asked again by then, and the share of a circle that is gone is left out
    ///   ([`HomebaseRemote::own_circle`]).
    ///
    /// One refused for something else is refused again then, and held (or stops the sync)
    /// as any other. Told as an outage is (there is no class for one node sent again at
    /// once): what else was to go up waits one backoff.
    fn sent_shared(
        &self,
        e: RemoteError,
        meta: &NodeMeta,
        folder: &Shares,
        given: &Given,
    ) -> RemoteError {
        let now = hbsync_types::unix_now_ms();
        let mut circles = self.circles.lock().unwrap_or_else(|e| e.into_inner());
        circles.retain(|circle, _| !folder.iter().any(|share| share.circle == *circle));
        drop(circles);
        if given.shares.iter().any(|share| !share.lasts(now, 0)) {
            self.folders.forget(meta.parent);
        } else if self.folders.refused(meta.parent, folder) {
            tracing::warn!(
                "{:?} was refused with its folder's shares ({e}): what is added to that \
                 folder goes up unshared for now",
                meta.content.name
            );
        }
        RemoteError::Transient(format!("{e} (sent with its folder's shares)"))
    }

    /// The drive's notifications ([`Notifier::spawn`]), their events sent to `events`: what
    /// they say of a folder this computer adds to is taken here first ([`Folders::heard`]):
    /// a share of it given or withdrawn keeps its version, which the feed isn't read for.
    /// And whether they are connected ([`Folders::listening`]).
    pub fn notifier(self: &Arc<Self>, events: impl Into<Events>) -> std::io::Result<Notifier> {
        let (remote, events) = (Arc::clone(self), events.into());
        let heard = move |event: Event| {
            match &event {
                Event::Changed(Some(change)) => remote.folders.heard(change.file_id),
                Event::Connected => remote.folders.listening(true),
                Event::Lost(_) | Event::Refused(_) => remote.folders.listening(false),
                Event::Changed(None) | Event::Markers => {}
            }
            events.send(event)
        };
        let notifier = Notifier::spawn(self.client.clone(), self.drive, Events::to(heard))?;
        // Asked to connect again (this computer woke, the network changed) or stopped, the
        // connection it had hears nothing more, and says so only once the next is made.
        let remote = Arc::clone(self);
        Ok(notifier.before_it_drops_a_connection(move || remote.folders.listening(false)))
    }

    /// How `folder` of this drive, a private one, is shared: what a node added to it goes up
    /// with. From what is known of it ([`Folders`]), else read: one request, a `query-batch`
    /// for the folder alone with its shares (`includeShares`: for the app that owns the
    /// drive, **[verified]** live 2026-11-25 and odin-core
    /// `SelectiveShareDrives.CallerManagesShares`). Nothing is shared at the top: no request.
    ///
    /// A folder that isn't on the drive, or whose row doesn't read, shares nothing, as far
    /// as is known for a while; a share of it that doesn't read isn't given. Said in the log:
    /// what is added there goes up all the same.
    fn shared_as(&self, folder: NodeId) -> Result<Shares> {
        if folder.is_root() {
            return Ok(Shares::default());
        }
        if let Some(shares) = self.folders.known(folder) {
            return Ok(shares);
        }
        // Not while a folder's move changes shares: what it reads then may be half moved.
        let _no_move = self.moves.read().unwrap_or_else(|e| e.into_inner());
        self.shared_as_held(folder)
    }

    /// [`HomebaseRemote::shared_as`] for a caller that holds `moves` itself. One read for
    /// all that ask at once (a folder's files go up side by side).
    fn shared_as_held(&self, folder: NodeId) -> Result<Shares> {
        if folder.is_root() {
            return Ok(Shares::default());
        }
        if let Some(shares) = self.folders.known(folder) {
            return Ok(shares);
        }
        let _one_reads = (self.shares_reading.lock()).unwrap_or_else(|e| e.into_inner());
        if let Some(shares) = self.folders.known(folder) {
            return Ok(shares);
        }
        self.folders.reading();
        let read = self.read_shares(folder);
        let answer = (read.as_ref().ok()).map(|(at, shares)| (*at, Arc::clone(shares)));
        self.folders.read(folder, answer);
        Ok(read?.1)
    }

    /// `folder`'s row with its shares, asked of the server: the file it is, and its live
    /// shares that read. No file for a folder that isn't there or whose row doesn't read.
    fn read_shares(&self, folder: NodeId) -> Result<(Option<RemoteRef>, Shares)> {
        let page = self.query_batch(&wire::node_query(folder, &[FILE_TYPE_FOLDER]))?;
        let Some(row) = page.search_results.first() else {
            return Ok((None, Shares::default()));
        };
        let read = match wire::shared_folder(row.get()) {
            Ok(read) => read,
            Err(e) => {
                tracing::warn!("how a folder is shared can't be read ({e}): added to unshared");
                return Ok((None, Shares::default()));
            }
        };
        if read.unread > 0 {
            tracing::warn!(
                "{} of a folder's shares can't be read: what is added to it isn't given them",
                read.unread
            );
        }
        if read.shares.len() > MOST_SHARES {
            tracing::warn!(
                "a folder has more shares than a new file goes up with: what is added to \
                 it is given {MOST_SHARES} of them"
            );
        }
        // (A tombstone still lists the rows it had: nothing is shared by it.)
        let shares = match read.deleted {
            true => Shares::default(),
            false => Shares::from(read.shares),
        };
        Ok((Some(read.at), shares))
    }

    /// The key of `circle`, one of this identity's own, which the files shared with it are
    /// locked under: read once (one request, `GET /api/v2/connections/circles/{id}/share-key`)
    /// and opened with the app's key ([`CircleKey::own`]). None where it isn't to be had: no
    /// key set for the circle, one that doesn't open, a login without the app's key, or the
    /// server refusing it (a circle deleted leaves its rows on the files, and its key is
    /// refused with a 403: **[verified]** odin-core `CircleDefinitionService.DeleteAsync`,
    /// `GetCircleCallerMayManageAsync`). Said once, and not asked again in this run.
    fn own_circle(&self, circle: Uuid) -> Result<Option<Arc<CircleKey>>> {
        let circles = || self.circles.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = circles().get(&circle) {
            return Ok(key.clone());
        }
        // One read for all that ask at once, as of the folder's shares.
        let _one_reads = (self.shares_reading.lock()).unwrap_or_else(|e| e.into_inner());
        if let Some(key) = circles().get(&circle) {
            return Ok(key.clone());
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Locked {
            app_key_locked_share_key: Option<String>,
        }
        let opened = || -> Result<std::result::Result<CircleKey, &'static str>> {
            let Some(app_key) = self.client.zk_key() else {
                return Ok(Err("this login lacks the app's key (log in again)"));
            };
            let route = format!("/api/v2/connections/circles/{circle}/share-key");
            let answer: Locked = match self.client.get(&route, &[]).map_err(remote_error) {
                Ok(answer) => answer,
                // A refusal of this key isn't one of the login: the upload says if it is.
                Err(RemoteError::Denied(..) | RemoteError::Fatal(_) | RemoteError::NotFound) => {
                    return Ok(Err(
                        "the server refuses the circle's key (is the circle gone?)",
                    ));
                }
                Err(e) => return Err(e),
            };
            let locked =
                (answer.app_key_locked_share_key).and_then(|key| STANDARD.decode(key).ok());
            let Some(locked) = locked else {
                return Ok(Err("the circle has no key"));
            };
            Ok(CircleKey::own(&locked, app_key, circle, self.drive.alias)
                .map_err(|_| "the circle's key doesn't open"))
        };
        let key = match opened()? {
            Ok(key) => Some(Arc::new(key)),
            Err(why) => {
                tracing::warn!(
                    "what is added to a folder shared with circle {circle} isn't shared \
                     with it: {why}"
                );
                None
            }
        };
        circles().insert(circle, key.clone());
        Ok(key)
    }

    /// The shares a new node `meta` goes up with, its payload `size` bytes: those of the
    /// folder it is added to that last past the upload ([`Share::lasts`]), the most a
    /// request takes ([`MOST_SHARES`]), its `key` locked under each circle's key
    /// ([`new_shares`]). None where the folder isn't shared, or its shares were refused
    /// with an upload not long ago ([`Folders::is_refused`]). And how the folder is shared,
    /// as known now: what the node is held to once it is up ([`HomebaseRemote::settle_new`]).
    ///
    /// The node goes up whatever becomes of its shares: one that can't be given (a circle's
    /// key not to be had) is left out, and said. Only what would fail the upload too fails
    /// this (the server not reached).
    fn shares_of_new(
        &self,
        meta: &NodeMeta,
        key: &KeyHeader,
        size: u64,
    ) -> Result<(Shares, Option<Given>)> {
        let folder = self.shared_as(meta.parent)?;
        if folder.is_empty() || self.folders.is_refused(meta.parent, &folder) {
            return Ok((folder, None));
        }
        let now = hbsync_types::unix_now_ms();
        let lasting = folder.iter().filter(|share| share.lasts(now, size));
        // (Asked here, and by `new_shares` again: kept, whether it is to be had or not.)
        let mut shares = Vec::new();
        for share in lasting.take(MOST_SHARES) {
            if self.own_circle(share.circle)?.is_some() {
                shares.push(*share);
            }
        }
        let shares: Shares = shares.into();
        let given = (shares.iter()).map(|s| (s.circle, s.permissions, s.expires_at));
        let sent = new_shares(key, given, |circle| self.own_circle(circle))?;
        let given = (!sent.shares.is_empty()).then_some(Given { shares, sent });
        Ok((folder, given))
    }

    /// A new node `meta` is up, as the file `at`, shared as `given`: as its folder was known
    /// to be shared (`then`) when its upload began. The folder may have changed while the
    /// node was on its way (a share withdrawn during a long upload; the folder moved by
    /// this computer in the same sync, which was seen live to leave six new files shared
    /// in a folder moved out of a shared one). So the folder is asked about once more,
    /// which waits for a move of it to end ([`HomebaseRemote::moved`]), and where it is
    /// shared otherwise now, the node's shares are made what the folder's are: the ones it
    /// has no more withdrawn, the ones it lacks given.
    ///
    /// Whether the node is shared as it went up, for certain: not where its shares were
    /// changed here, nor where the folder wasn't read (the node is up: said, and left).
    fn settle_new(
        &self,
        meta: &NodeMeta,
        at: RemoteRef,
        key: &KeyHeader,
        then: &Shares,
        given: Option<&Given>,
    ) -> bool {
        if meta.parent.is_root() {
            return true;
        }
        // The usual: what was known of the folder still holds. Nothing is asked.
        let as_then = |now: &Shares| alike(now, then);
        if self
            .folders
            .known(meta.parent)
            .is_some_and(|now| as_then(&now))
        {
            return true;
        }
        // Held until the node's shares are the folder's: a move of the folder after this
        // lists the node as it is then.
        let _no_move = self.moves.read().unwrap_or_else(|e| e.into_inner());
        let now = match self.shared_as_held(meta.parent) {
            Ok(now) if as_then(&now) => return true,
            Ok(now) => now,
            Err(e) => {
                tracing::warn!(
                    "{:?} is up, shared as its folder was when its upload began; how the \
                     folder is shared now wasn't read: {e}",
                    meta.content.name
                );
                return false;
            }
        };
        let node = Moving {
            node: meta.node,
            at,
            folder: meta.kind == NodeKind::Folder,
            key: Some(key.key),
            shares: given.map_or_else(Vec::new, |given| given.shares.to_vec()),
        };
        self.shared_as_now(std::slice::from_ref(&node), &now);
        false
    }

    /// Makes the shares of `nodes` what their folder shares `now`: the ones of circles it
    /// isn't shared with withdrawn, the ones a node lacks (or has otherwise) given. Said
    /// where it fails: for nodes that are where they are.
    fn shared_as_now(&self, nodes: &[Moving], now: &[Share]) {
        let time = hbsync_types::unix_now_ms();
        let lasting: Vec<Share> = (now.iter().filter(|share| share.lasts(time, 0)))
            .copied()
            .collect();
        let gone = |circle: &Uuid| !lasting.iter().any(|share| share.circle == *circle);
        let has = nodes.iter().flat_map(|node| &node.shares);
        let leaving: HashSet<Uuid> = has.map(|share| share.circle).filter(gone).collect();
        let leaving: Vec<Uuid> = leaving.into_iter().collect();
        if let Err(e) = self.withdraw(nodes, &leaving) {
            tracing::warn!("a share to withdraw from what was added wasn't: {e}");
        }
        self.share(nodes, &lasting);
    }

    /// An update of `meta`, which the drive has in the folder `from`, into another folder:
    /// the node's shares, and those of what is in it, become what the folder it goes into
    /// shares ([`Remote::update_from`], [`HomebaseRemote::moved_as`]).
    ///
    /// A folder whose shares change moves alone (`moves`, for writing): what else is moved
    /// or added meanwhile waits, and learns how the folders are shared afterwards. A file,
    /// and a folder that moves between two folders shared alike (with no one, mostly),
    /// only waits for such a move and holds one off until it is done (for reading: any
    /// number at once). Seen live before this: of 30 files moved out of a folder as that
    /// moved into a shared one, 7 were left shared where they went.
    fn moved(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
        from: NodeId,
    ) -> Result<Written> {
        let shared = || -> Result<(Shares, Shares)> {
            Ok((
                self.shared_as_held(from)?,
                self.shared_as_held(meta.parent)?,
            ))
        };
        {
            let _no_folder_moves = self.moves.read().unwrap_or_else(|e| e.into_inner());
            let (left, entered) = shared()?;
            if meta.kind == NodeKind::File || alike(&left, &entered) {
                return self.moved_as(target, meta, payload, &left, &entered);
            }
        }
        // (Asked again: another move may have gone first.)
        let _alone = self.moves.write().unwrap_or_else(|e| e.into_inner());
        let (left, entered) = shared()?;
        self.moved_as(target, meta, payload, &left, &entered)
    }

    /// [`HomebaseRemote::moved`], `moves` held: from a folder shared as `left` into one
    /// shared as `entered`.
    /// - Shared alike, the move is the update alone: what is below is shared as it was.
    /// - The shares of the circles the folder it leaves is shared with, and the one it
    ///   enters isn't, are withdrawn **before** the move; the ones the folder it enters
    ///   shares, and a node lacks (or has with other rights, or another end), are given
    ///   **after** it. So whatever fails on the way, a node is shared with fewer circles,
    ///   never with more, than where it is. (A circle of both folders keeps its share
    ///   through the move: where the folder entered gives it fewer rights and giving those
    ///   fails, the node keeps the wider ones. Said, and left: a known limit.)
    /// - What is in the node is read first, with each node's shares and (to give one) its
    ///   key: one request for the node, one per page of 500 nodes per 100 folders below it.
    ///   Withdrawn: one request per share; given: one per 100 shares of a circle.
    /// - The move itself is the one write that is answered: shares that can't be given
    ///   after it are said in the log, and left (the node is where it is).
    fn moved_as(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
        left: &[Share],
        entered: &[Share],
    ) -> Result<Written> {
        if alike(left, entered) {
            return self.write(Some(target), meta, payload);
        }
        let now = hbsync_types::unix_now_ms();
        let entered: Vec<Share> = (entered.iter().filter(|share| share.lasts(now, 0)))
            .copied()
            .collect();
        let stays = |circle: &Uuid| entered.iter().any(|share| share.circle == *circle);
        let leaving: Vec<Uuid> = (left.iter().map(|share| share.circle))
            .filter(|circle| !stays(circle))
            .collect();
        // (A share that has ended, of the folder entered, is all the two differ by.)
        if leaving.is_empty() && entered.is_empty() {
            return self.write(Some(target), meta, payload);
        }
        let moving = self.moving(meta.node, target.at, !entered.is_empty())?;
        self.withdraw(&moving, &leaving)?;
        let written = self.write(Some(target), meta, payload)?;
        self.share(&moving, &entered);
        // A folder among them is shared as this left it, which nothing tells this login of
        // (its own writes aren't announced to it): read when next added to.
        for node in moving.iter().filter(|node| node.folder) {
            self.folders.forget(node.node);
        }
        Ok(written)
    }

    /// The node `top`, which is to be the file `at` still, and all that is in it, as a move
    /// reads them ([`Moving`]): each with its shares and, where `keys`, its key. A listing
    /// that is cut short is an error: a share withdrawn from part of a folder would leave
    /// the rest shared where it goes.
    fn moving(&self, top: NodeId, at: RemoteRef, keys: bool) -> Result<Vec<Moving>> {
        // Each node once: two folders can be in each other on the drive (sync semantics
        // §6), and the walk down would never end.
        let (mut moving, mut seen) = (Vec::new(), HashSet::new());
        let node = self.query_batch(&wire::node_query(top, wire::NODE_TYPES))?;
        self.read_moving(&node, keys, &mut moving, &mut seen)?;
        // Written since the caller last read it: its write would be refused, after the
        // shares it leaves were withdrawn. Refused here, before anything is.
        match moving.first() {
            Some(node) if node.at == at => {}
            Some(_) => return Err(RemoteError::VersionMismatch),
            None => return Err(RemoteError::NotFound),
        }
        // Folder by folder, from the top: the folders read last are the ones to read next.
        let mut read = 0;
        while read < moving.len() {
            let folders: Vec<NodeId> = (moving[read..].iter())
                .filter(|node| node.folder)
                .map(|node| node.node)
                .collect();
            read = moving.len();
            for some in folders.chunks(CHILDREN_QUERY_FOLDERS) {
                let fetch = |cursor: Option<&str>| {
                    self.query_batch(&wire::nodes_in_query(some, cursor, NODES_PAGE))
                };
                let each = |page: &wire::QueryBatchResponse| {
                    (self.read_moving(page, keys, &mut moving, &mut seen)).map(|()| true)
                };
                if read_pages(fetch, each)? == Ended::CutShort {
                    return Err(RemoteError::Transient(
                        "what is in a folder to move wasn't listed to its end".into(),
                    ));
                }
            }
        }
        Ok(moving)
    }

    /// A page's nodes, as a move reads them: the ones not `seen` yet added to `moving`,
    /// with their keys where `keys` (a share is to be given: it takes the node's key). A
    /// folder among them is known no more from here on: what is added to it meanwhile
    /// asks, and waits for the move.
    fn read_moving(
        &self,
        page: &wire::QueryBatchResponse,
        keys: bool,
        moving: &mut Vec<Moving>,
        seen: &mut HashSet<NodeId>,
    ) -> Result<()> {
        for row in &page.search_results {
            let header: wire::FileHeader = serde_json::from_str(row.get())
                .map_err(|e| RemoteError::Fatal(format!("a node to move doesn't read: {e}")))?;
            let Some(node) = header.file_metadata.app_data.unique_id.map(NodeId) else {
                continue;
            };
            if !seen.insert(node) {
                continue;
            }
            let folder = header.file_metadata.app_data.file_type == FILE_TYPE_FOLDER;
            if folder {
                self.folders.forget(node);
            }
            // The keys this computer holds are asked for whatever fails (the server not
            // reached fails the move, nothing written yet); a key that doesn't open with
            // them leaves its node unshared, and is said.
            let key = match keys {
                true => header
                    .key(self.held(&header)?.keys(self))
                    .ok()
                    .map(|key| key.key),
                false => None,
            };
            let share = |row: &wire::ShareRow| {
                Some(Share {
                    circle: row.circle_id?,
                    permissions: row.permissions,
                    expires_at: row.expires_at,
                })
            };
            moving.push(Moving {
                node,
                at: RemoteRef {
                    file_id: RemoteFileId(header.file_id),
                    version: VersionTag(header.file_metadata.version_tag),
                },
                folder,
                key,
                shares: (header.shares.iter().flatten()).filter_map(share).collect(),
            });
        }
        Ok(())
    }

    /// Withdraws the shares of the nodes `moving` with the circles `leaving`: one request
    /// each (`DELETE /drives/{d}/files/{fileId}/shares/{circleId}`; there is no route that
    /// takes several). One withdrawn already answers 200; a file with no row for the circle
    /// a 400, and a circle that is gone a 403 (**[verified]** odin-core
    /// `DriveShareService.WithdrawShareAsync`, `GetCircleCallerMayManageAsync`; the route
    /// live 2026-11-25 with the app's login). A refusal is said and passed by: the share of
    /// a circle that is gone reaches no one, and this login's own refusal is the move's
    /// too. A 403 is the circle's or the login's, the same for every node: the circle's
    /// other shares aren't asked for. The server not reached fails it, the move not made
    /// yet.
    fn withdraw(&self, moving: &[Moving], leaving: &[Uuid]) -> Result<()> {
        for circle in leaving {
            let shared = |node: &&Moving| node.shares.iter().any(|share| share.circle == *circle);
            let (mut refused, mut last) = (0, None);
            for node in moving.iter().filter(shared) {
                let route = format!(
                    "{}/shares/{circle}",
                    file_route(self.drive.alias, node.at.file_id)
                );
                match self.client.delete(&route).map_err(remote_error) {
                    Ok(()) => {}
                    Err(e @ RemoteError::Denied(..)) => {
                        (refused, last) = (refused + 1, Some(e));
                        break;
                    }
                    Err(e @ (RemoteError::Fatal(_) | RemoteError::NotFound)) => {
                        (refused, last) = (refused + 1, Some(e));
                    }
                    Err(e) => return Err(e),
                }
            }
            if let Some(e) = last {
                tracing::warn!(
                    "shares with circle {circle} weren't withdrawn ({refused} refused): {e}"
                );
            }
        }
        Ok(())
    }

    /// Gives the nodes `moving` the shares `entered` each lacks ([`HomebaseRemote::share_with`]).
    /// After the move: what can't be given is said, not failed.
    fn share(&self, moving: &[Moving], entered: &[Share]) {
        for share in entered {
            match self.share_with(moving, share) {
                Ok((0, 0)) => {}
                Ok((shared, 0)) => {
                    tracing::info!("shared {shared} nodes as the folder they are in now");
                }
                Ok((shared, not)) => tracing::warn!(
                    "{not} nodes in a shared folder aren't shared as it is (circle {}): they \
                     are gone, the server says ({shared} are shared)",
                    share.circle
                ),
                Err(e) => tracing::warn!(
                    "what is in a shared folder isn't all shared as that folder is (circle \
                     {}): {e}",
                    share.circle
                ),
            }
        }
    }

    /// Gives each of `moving` that lacks it the share `share` (or has it with other rights
    /// or another end: the server writes a share over the one a file has, **[verified]**
    /// odin-core `DriveShareService.WriteSharesAsync`), its key locked under the circle's
    /// key by its file id: a request per [`MOST_SHARES`] nodes (`PUT /drives/{d}/shares`,
    /// all or none: one circle's alone, so one the server refuses takes no other's along).
    /// Where the server refuses such a request for a file that is gone since it was read
    /// (`invalidFile`), its shares are given one by one; any other refusal is the
    /// circle's or the share's, the same for all of them, and ends it. How many were
    /// given, and how many were gone: none where the circle's key isn't to be had.
    fn share_with(&self, moving: &[Moving], share: &Share) -> Result<(usize, usize)> {
        let Some(circle_key) = self.own_circle(share.circle)? else {
            return Ok((0, 0));
        };
        let mut rows = Vec::new();
        for node in moving.iter().filter(|node| !node.shares.contains(share)) {
            let Some(key) = &node.key else {
                tracing::warn!("a node's key doesn't open: it isn't shared as its folder is");
                continue;
            };
            let file = node.at.file_id.0;
            let locked = circle_key.lock_key_of(key, file).map_err(remote_error)?;
            let (permissions, ends) = (share.permissions, share.expires_at);
            rows.push(wire::share_of(
                file,
                share.circle,
                permissions,
                ends,
                &locked,
            ));
        }
        let route = shares_route(self.drive.alias);
        let put = |rows: &[serde_json::Value]| self.client.put(&route, &json!({ "shares": rows }));
        let gone = |e: &HomebaseError| matches!(e, HomebaseError::Api { error, .. } if error.is("invalidFile"));
        let mut refused = 0;
        for some in rows.chunks(MOST_SHARES) {
            match put(some) {
                Ok(()) => {}
                Err(e) if gone(&e) && some.len() > 1 => {
                    for one in some {
                        match put(std::slice::from_ref(one)) {
                            Ok(()) => {}
                            Err(e) if gone(&e) => refused += 1,
                            Err(e) => return Err(remote_error(e)),
                        }
                    }
                }
                Err(e) if gone(&e) => refused += 1,
                Err(e) => return Err(remote_error(e)),
            }
        }
        Ok((rows.len() - refused, refused))
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
        let page = changes_page(page, cursor, |h| {
            if let Some(fed) = h.fed() {
                self.folders.fed(fed);
            }
            self.to_remote(h)
        })?;
        // A row that doesn't read says nothing of its folder's shares: read again, if known.
        for at in page.skipped.iter().filter_map(|skipped| skipped.at) {
            self.folders.heard(at.file_id);
        }
        Ok(page)
    }

    /// A minimal page (odin-core #1967): what changed, without the headers.
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        let (page, cursor) = self.feed_page(cursor, limit, wire::Shape::Minimal)?;
        refs_page(page, cursor, |row| match row {
            Ok(row) => {
                if let Some(fed) = row.fed() {
                    self.folders.fed(fed);
                }
            }
            // One that doesn't read says nothing of its folder's shares: read again, if known.
            Err(file) => self.folders.heard(file),
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

    /// A move on a private drive takes the node's shares along ([`HomebaseRemote::moved`]):
    /// nothing is asked for a write that leaves the node where it is.
    fn update_from(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
        from: NodeId,
    ) -> Result<Written> {
        match from != meta.parent && self.drive_key(false)?.is_some() {
            true => self.moved(target, meta, payload, from),
            false => self.write(Some(target), meta, payload),
        }
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

/// The route of the shares of the drive `alias` names (`PUT`: [`wire::share_of`]).
fn shares_route(alias: Uuid) -> String {
    format!("/api/v2/drives/{}/shares", alias.simple())
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

/// Nodes asked for per page of what is in folders ([`HomebaseRemote::holding`],
/// [`HomebaseRemote::moving`]).
const NODES_PAGE: usize = 500;

/// A node that moves, or is in a folder that does, as [`HomebaseRemote::moving`] reads it.
struct Moving {
    node: NodeId,
    at: RemoteRef,
    folder: bool,
    /// Its key, where it was asked for; none where it doesn't open (it can't be shared
    /// then).
    key: Option<Key>,
    /// Its shares, as the server lists them (the live ones) or as it went up with them: of
    /// a circle its folder isn't shared with, to withdraw; one that isn't as its folder's
    /// is, to give again.
    shares: Vec<Share>,
}

/// What a new node goes up shared as ([`HomebaseRemote::shares_of_new`]).
struct Given {
    /// The ones given to it (a circle whose key isn't to be had is left out): a new
    /// folder is shared so from then on.
    shares: Shares,
    sent: wire::NewShares,
}

/// Whether the server's refusal `e` of an upload may be for the shares it carried: the
/// codes it refuses a share with (**[verified]** odin-core `DriveShareService`: a 400
/// `argumentError` for a share that ended, a circle disabled or not granting what the share
/// gives, too many shares; `circleShareKeyNotSet`, `invalidShareKey`), and its 403 for a
/// circle that isn't the app's any more (`GetCircleCallerMayShareWithAsync`), which this
/// login's own refusal is too: the upload without shares tells them apart.
fn may_be_for_shares(e: &HomebaseError) -> bool {
    const CODES: [&str; 3] = ["argumentError", "circleShareKeyNotSet", "invalidShareKey"];
    match e {
        HomebaseError::Denied(_) => true,
        HomebaseError::Api { error, .. } => CODES.iter().any(|code| error.is(code)),
        _ => false,
    }
}

/// The shares a new file goes up with: its `key` locked under the key of each circle of
/// `given` (with what the circle may do with the file, and until when), as `circle_key`
/// gives it, bound to a global transit id picked for the file, which has no id yet
/// ([`CircleKey::lock_file_key`]). A circle whose key isn't to be had (`None`) is left out.
pub(crate) fn new_shares(
    key: &KeyHeader,
    given: impl Iterator<Item = (Uuid, u32, Option<i64>)>,
    circle_key: impl Fn(Uuid) -> Result<Option<Arc<CircleKey>>>,
) -> Result<wire::NewShares> {
    let global_transit_id = Uuid::new_v4();
    let mut shares = Vec::new();
    for (circle, permissions, ends) in given {
        let Some(circle_key) = circle_key(circle)? else {
            continue;
        };
        let locked = circle_key
            .lock_file_key(&key.key, global_transit_id)
            .map_err(remote_error)?;
        shares.push(wire::share_of_new(circle, permissions, ends, &locked));
    }
    Ok(wire::NewShares {
        global_transit_id,
        shares,
    })
}

/// A minimal feed page (odin-core #1967): what changed, without the headers. Each row is
/// shown to `each` as it is read: one that doesn't read, as the file it names (if it reads).
fn refs_page(
    page: wire::QueryBatchResponse,
    cursor: String,
    mut each: impl FnMut(std::result::Result<&wire::FileRef, RemoteFileId>),
) -> Result<RefsPage> {
    // A server without #1967 ignores the shape and sends headers: an empty page there would
    // skip what changed.
    let rows = page.file_refs.ok_or_else(|| {
        RemoteError::Fatal("the server doesn't page the feed slim (odin-core #1967)".into())
    })?;
    let mut refs = Vec::with_capacity(rows.len());
    let mut unread = 0;
    for row in &rows {
        match serde_json::from_str::<wire::FileRef>(row.get()) {
            Ok(r) => {
                each(Ok(&r));
                refs.push(r.to_ref());
            }
            Err(_) => {
                if let Some(file) = wire::file_id_of(row.get()) {
                    each(Err(RemoteFileId(file)));
                }
                unread += 1;
            }
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
        let fetch =
            |cursor: Option<&str>| query_batch(&wire::children_query(some, cursor, NODES_PAGE));
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
            folders: Folders::default(),
            shares_reading: Mutex::default(),
            moves: RwLock::default(),
            circles: Mutex::default(),
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

    /// A new node that is up is held to its folder without a request where what is known
    /// of the folder still holds: shared as it went up, for certain, at the top and in a
    /// folder known to be shared as it was (in whatever order). Not where the folder isn't
    /// known and can't be asked about: a new folder isn't taken to share what it went up
    /// with then.
    #[test]
    fn a_new_node_is_held_to_what_its_folder_is_known_to_share()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use crate::drive::shared_as::tests::{at, share};
        let remote = unreachable_remote(private_state(Some([1; 16]), None))?;
        let key = KeyHeader::random()?;
        let new = |parent: NodeId| NodeMeta {
            node: NodeId::new(),
            parent,
            kind: NodeKind::Folder,
            content: NodeContent::folder("added"),
            removed: false,
        };
        let settled = |parent: NodeId, then: &Shares| {
            remote.settle_new(&new(parent), at(300, 30), &key, then, None)
        };
        let unshared = Shares::default();
        assert!(settled(NodeId::ROOT, &unshared));
        let folder = NodeId::new();
        let (a, b) = (share(1, None), share(2, None));
        (remote.folders).made(folder, at(100, 10), Arc::from([a, b]));
        assert!(
            settled(folder, &Arc::from([b, a])),
            "as it was: nothing asked"
        );
        remote.folders.forget(folder);
        assert!(
            !settled(folder, &Arc::from([a, b])),
            "not known now, not read"
        );
        assert!(!settled(NodeId::new(), &unshared));
        Ok(())
    }

    /// What a new node goes up with, without a request where none is needed: nothing at the
    /// top; a known folder's shares that last past the upload, the node's key locked under
    /// each circle's key; none for a circle whose key isn't to be had, nor from a folder
    /// whose shares the server refused. A folder that isn't known is asked about (nothing
    /// answers here).
    #[test]
    fn a_new_node_is_given_what_its_folder_is_known_to_share()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use crate::drive::shared_as::{Share, tests::at};
        use share_keys::tests::{APP_KEY, CIRCLE, LOCKED_CIRCLE_KEY, bytes, id};
        let remote = unreachable_remote(private_state(Some([1; 16]), None))?;
        let key = KeyHeader::random()?;
        let given = |parent: NodeId, size: u64| {
            let meta = NodeMeta {
                node: NodeId::new(),
                parent,
                kind: NodeKind::File,
                content: NodeContent::file("added", 1, 1, false),
                removed: false,
            };
            (remote.shares_of_new(&meta, &key, size)).map(|(_, given)| given)
        };
        assert!(
            given(NodeId::ROOT, 0)?.is_none(),
            "nothing is shared at the top"
        );
        let folder = NodeId::new();
        assert!(given(folder, 0).is_err(), "not known: asked about");

        let app_key = key_of(&bytes(APP_KEY), "the vector's")?;
        let (opens, lacks) = (id(CIRCLE), Uuid::from_u128(9));
        let alias = remote.drive.alias;
        let circle_key = Arc::new(CircleKey::own(
            &bytes(LOCKED_CIRCLE_KEY),
            &app_key,
            opens,
            alias,
        )?);
        let circles = [(opens, Some(Arc::clone(&circle_key))), (lacks, None)];
        (remote.circles.lock().expect("unpoisoned")).extend(circles);
        let share = |circle: Uuid, expires_at: Option<i64>| Share {
            circle,
            permissions: app::READ | app::WRITE,
            expires_at,
        };
        let shares = [share(opens, None), share(lacks, None)];
        (remote.folders).made(folder, at(100, 10), Arc::from(shares));
        let went = given(folder, 0)?.ok_or("shared")?;
        assert_eq!(
            *went.shares,
            [share(opens, None)],
            "what a folder made here shares in turn: what went up"
        );
        assert_eq!(went.sent.shares.len(), 1, "{:?}", went.sent.shares);
        let row = &went.sent.shares[0];
        assert_eq!(
            (&row["circleId"], &row["permissions"]),
            (&json!(opens), &json!(3))
        );
        assert_eq!(
            (&row["fileId"], &row["expiresAt"]),
            (&json!(Uuid::nil()), &json!(null))
        );
        let locked = STANDARD.decode(row["lockedFileKey"].as_str().ok_or("a key")?)?;
        let bound_to = Some(went.sent.global_transit_id);
        let opened = circle_key.file_key(&locked, Uuid::new_v4(), bound_to)?;
        assert_eq!(opened, key.key, "bound to the transit id it goes up with");

        // A share that ends before the upload can be up isn't given: by the file's size.
        let ending = NodeId::new();
        let in_ten_minutes = hbsync_types::unix_now_ms() + 600_000;
        let ends = [share(opens, Some(in_ten_minutes))];
        (remote.folders).made(ending, at(200, 20), Arc::from(ends));
        assert!(given(ending, 0)?.is_some());
        assert!(given(ending, 1 << 30)?.is_none(), "a gigabyte takes longer");

        // Refused by the server with an upload that carried them: tried again soon, and
        // none are given for now; the keys of the circles sent are asked again by then.
        let meta = |parent: NodeId| NodeMeta {
            node: NodeId::new(),
            parent,
            kind: NodeKind::File,
            content: NodeContent::file("added", 1, 1, false),
            removed: false,
        };
        let refused = || RemoteError::Fatal("400".into());
        let keys = || remote.circles.lock().expect("unpoisoned").len();
        assert_eq!(keys(), 2);
        let folders: Shares = Arc::from(shares);
        let again = remote.sent_shared(refused(), &meta(folder), &folders, &went);
        assert!(matches!(again, RemoteError::Transient(_)), "{again:?}");
        assert!(given(folder, 0)?.is_none());
        assert_eq!(keys(), 0);

        // Refused as a share that was sent has ended since: not for the others. The folder
        // is read again (nothing answers here), and what lasts of it is given then.
        (remote.circles.lock().expect("unpoisoned")).insert(opens, Some(circle_key));
        let went = given(ending, 0)?.ok_or("shared")?;
        let ended = Given {
            shares: Arc::from([share(opens, Some(hbsync_types::unix_now_ms() - 1))]),
            ..went
        };
        let again = remote.sent_shared(refused(), &meta(ending), &Arc::from(ends), &ended);
        assert!(matches!(again, RemoteError::Transient(_)), "{again:?}");
        assert!(
            given(ending, 0).is_err(),
            "asked about again, not taken as unshared"
        );
        Ok(())
    }

    /// Which of the server's refusals of an upload may be for the shares it carried.
    #[test]
    fn a_refusal_may_be_for_the_shares_by_its_code() {
        let api = |code: &str| HomebaseError::Api {
            what: "upload".into(),
            error: ApiError::parse(
                400,
                json!({ "status": 400, "errorCode": code })
                    .to_string()
                    .as_bytes(),
            ),
        };
        for code in ["argumentError", "circleShareKeyNotSet", "invalidShareKey"] {
            assert!(may_be_for_shares(&api(code)), "{code}");
        }
        assert!(may_be_for_shares(&HomebaseError::Denied("403".into())));
        assert!(!may_be_for_shares(&api("existingFileWithUniqueId")));
        assert!(!may_be_for_shares(&HomebaseError::Network("down".into())));
        assert!(!may_be_for_shares(&HomebaseError::Protocol(
            "not json".into()
        )));
    }

    /// A slim page shows each row as it is read: one that reads, and the file of one that
    /// doesn't where its id does.
    #[test]
    fn a_slim_page_shows_each_of_its_rows() -> Result<()> {
        const FOLDER: &str = "11111111-1111-1111-1111-111111111111";
        const BROKEN: &str = "44444444-4444-4444-4444-444444444444";
        let page: wire::QueryBatchResponse = serde_json::from_value(json!({
            "cursorState": "c", "hasMoreRows": false, "queryTime": 1, "fileRefs": [
                { "fileId": FOLDER, "fileState": "active", "fileType": 7101,
                  "versionTag": "22222222-2222-2222-2222-222222222222",
                  "uniqueId": "33333333-3333-3333-3333-333333333333",
                  "updated": 12, "versionUpdated": 9 },
                { "fileId": BROKEN, "fileState": 7 },
                { "fileState": "active" },
            ],
        }))
        .expect("a page");
        let mut shown = Vec::new();
        let read = refs_page(page, "c".into(), |row| {
            shown.push(match row {
                Ok(row) => (row.file_id, row.fed().map(|fed| fed.as_written)),
                Err(file) => (file.0, None),
            });
        })?;
        assert_eq!((read.refs.len(), read.unread), (1, 2));
        let id = |s: &str| s.parse::<Uuid>().expect("a uuid");
        assert_eq!(shown, [(id(FOLDER), Some(false)), (id(BROKEN), None)]);
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
