//! What the daemon and its clients (the CLI, the tray) say to each other on the control socket
//! ([`crate::ipc`]): one [`Request`] per connection, one [`Reply`]; but [`Ask::Watch`], answered
//! with a [`Reply::Status`] now and another each time it changes.
//!
//! The daemon syncs folders, each with an account (an identity): its status lists both, and a
//! [`Control`] goes to the daemon (every folder), to an account (its Recently deleted), or to
//! one folder (its held deletes).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use hbsync_types::NodeId;
use hbsync_types::Role;
use hbsync_types::activity::{Deleted, Synced};
use serde::{Deserialize, Serialize};

use crate::Identity;

pub use hbsync_types::activity::Progress;

/// The protocol version this build speaks: a daemon refuses a request in another (after an
/// update, until it restarts).
pub(crate) const PROTOCOL: u32 = 9;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub protocol: u32,
    pub ask: Ask,
}

impl Request {
    pub fn new(ask: Ask) -> Request {
        Request {
            protocol: PROTOCOL,
            ask,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ask {
    Status,
    /// The status now, and again each time it changes (at most once a second while a folder
    /// syncs), on this connection until either side closes it: what the tray shows.
    Watch,
    /// Stop the daemon (between operations). The one request a daemon of any version takes,
    /// whatever protocol it speaks: an update stops the daemon of the build before it.
    Stop,
    Control(Control),
    /// The newest entries of the recently synced lists, every folder's.
    Recent {
        limit: usize,
    },
    /// The newest deletes of Recently deleted, every account's: each item deleted on its own,
    /// with how many items its delete took along (never those items themselves).
    Deleted {
        limit: usize,
    },
    /// The items of Recently deleted the user named (a node id, or its place as `hbsync
    /// deleted` shows it), any account's; refused naming the first one that isn't there.
    FindDeleted {
        items: Vec<String>,
    },
    /// Set the folder at `path` up for `identity` and sync it (`hbsync setup` has checked it).
    AddFolder {
        identity: Identity,
        path: PathBuf,
        role: Option<Role>,
    },
    /// Stop syncing the folder at `path`; its files and state stay.
    RemoveFolder {
        path: PathBuf,
    },
    /// The credentials of `identity` changed (logged in again): its account starts over with
    /// them.
    StartOver {
        identity: Identity,
    },
}

/// How a daemon refusing a request of another protocol starts its answer, its own protocol
/// after it.
pub const OTHER_PROTOCOL: &str = "this daemon speaks protocol ";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Status(Box<DaemonStatus>),
    /// Newest first.
    Recent(Vec<RecentItem>),
    /// The newest deletes, and each account's Recently deleted as a whole.
    Deleted(DeletedList),
    /// The items asked for, in the order asked.
    Found(Vec<DeletedItem>),
    Done,
    Refused(String),
}

/// A pause of syncing: until then (Unix seconds), or until resumed (`until: None`). The
/// config and the daemon keep it, which resumes every account when it runs out (decided here
/// alone); the accounts and folders hold it, without a timer, while paused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pause {
    pub until: Option<u64>,
}

impl Pause {
    /// Paused until resumed.
    pub const UNTIL_RESUMED: Pause = Pause { until: None };

    /// Paused until `unix` (Unix seconds).
    pub const fn ending_at(unix: u64) -> Pause {
        Pause { until: Some(unix) }
    }

    /// Whether the pause is over at `unix` (Unix seconds): a timed one whose time has come.
    pub fn ran_out(self, unix: u64) -> bool {
        self.until.is_some_and(|t| unix >= t)
    }

    /// When it runs out, `unix` being the Unix time at `now`; `None` for one until resumed (or
    /// one too far ahead for an instant).
    pub fn end_at(self, now: Instant, unix: u64) -> Option<Instant> {
        self.until
            .and_then(|t| now.checked_add(Duration::from_secs(t.saturating_sub(unix))))
    }
}

/// A request to change what the daemon does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    /// For every folder.
    Daemon(DaemonControl),
    /// For an account: its Recently deleted.
    Account {
        identity: Identity,
        control: AccountControl,
    },
    /// For one folder, by its path.
    Folder {
        path: PathBuf,
        control: FolderControl,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonControl {
    /// Sync now, without waiting out a backoff.
    SyncNow,
    /// Pause syncing.
    Pause(Pause),
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountControl {
    /// Restore these items of Recently deleted, each with what was deleted with it.
    Restore { nodes: Vec<NodeId> },
    /// Delete these items of Recently deleted for good, each with what was in it when it was
    /// listed ([`DeletedItem::db`], [`DeletedItem::seq`]): nothing recorded since (a removal
    /// that arrived in it later) and nothing live. A listing of another DB, or one the account
    /// never gave, is refused in the reply.
    EmptyDeleted {
        nodes: Vec<NodeId>,
        db: String,
        seq: u64,
    },
    /// Delete for good what Recently deleted held when it was listed ([`DeletedAccount::db`],
    /// [`DeletedAccount::seq`]): every removal this device recorded up to `seq`, nothing recorded
    /// since and nothing live (a folder holding either stays). A listing of another DB, or one
    /// the account never gave, is refused in the reply.
    EmptyListed { db: String, seq: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FolderControl {
    /// Let the held deletes the user was shown through: `token` is [`HeldDeletes::token`] as
    /// they saw it. Refused when the held deletes changed since.
    ConfirmDeletes { token: String },
}

/// An entry of a folder's recently synced list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentItem {
    /// The folder's path.
    pub folder: PathBuf,
    /// The folder is the main one (its items need no folder named).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub main: bool,
    #[serde(flatten)]
    pub synced: Synced,
}

/// Recently deleted's newest deletes, every account's, newest first, and each account's that
/// holds anything, as a whole.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeletedList {
    pub items: Vec<DeletedItem>,
    pub accounts: Vec<DeletedAccount>,
}

impl DeletedList {
    /// Every item, the ones the deletes took along too.
    pub fn total(&self) -> usize {
        self.accounts.iter().map(|a| a.items).sum()
    }

    /// The items emptying all of it leaves ([`DeletedAccount::stays`]).
    pub fn staying(&self) -> usize {
        self.accounts.iter().map(|a| a.stays).sum()
    }
}

/// An account's Recently deleted as a whole, when listed: what emptying all of it takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedAccount {
    pub identity: Identity,
    /// The items deleted on their own.
    pub deletes: usize,
    /// What emptying all of it deletes for good: removed nodes only, every item and the ones its
    /// deletes took along, but a folder holding a live node or a removal that stays.
    pub items: usize,
    /// The items deleted on their own that emptying leaves: they hold a live node.
    #[serde(default)]
    pub stays: usize,
    /// What the listing saw, which emptying all of it as listed takes
    /// ([`AccountControl::EmptyListed`]): the account's DB (its own id: one made again is
    /// another), and the counter of its order of recording removals.
    pub db: String,
    pub seq: u64,
}

/// An item of an account's Recently deleted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeletedItem {
    pub identity: Identity,
    #[serde(flatten)]
    pub deleted: Deleted,
    /// Where it was, as people name it and `hbsync restore` takes it
    /// ([`crate::words::deleted_place`], with the daemon's main folder), when that isn't its
    /// path (in the main folder): [`DeletedItem::place`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place: Option<String>,
    /// How many items are in it: all of Recently deleted below it (through removed folders),
    /// what was deleted with it, which restoring it restores, and what was deleted from it before
    /// (listed on its own too), which emptying it takes along. A deleted folder of thousands of
    /// files is one item with its count.
    #[serde(default)]
    pub along: usize,
    /// The listing that saw it, as [`DeletedAccount::db`] and [`DeletedAccount::seq`]: emptying
    /// it takes what was in it then ([`AccountControl::EmptyDeleted`]).
    pub db: String,
    pub seq: u64,
}

impl DeletedItem {
    /// Where it was, as people name it and `hbsync restore` takes it.
    pub fn place(&self) -> &str {
        self.place.as_deref().unwrap_or(&self.deleted.path)
    }

    /// This item and what is in it.
    pub fn count(&self) -> usize {
        1 + self.along
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub version: String,
    pub accounts: Vec<AccountStatus>,
    pub folders: Vec<FolderStatus>,
}

impl DaemonStatus {
    /// The main folder (holding the drive's root) of the first account: the one the tray and
    /// the CLI show first.
    pub fn main_folder(&self) -> Option<&FolderStatus> {
        self.folders
            .iter()
            .find(|f| f.role.is_none())
            .or(self.folders.first())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountStatus {
    pub identity: Identity,
    pub hints: Hints,
    /// Why the credentials aren't in the keychain.
    pub credentials_warning: Option<String>,
    /// The account's own notes, newest last: a request of Recently deleted that failed, a file
    /// on the drive this client can't read. Its folders' are theirs ([`FolderStatus::recent`]).
    pub recent: Vec<Note>,
    /// Notes added since the daemon started, as [`FolderStatus::notes_added`].
    pub notes_added: u64,
}

impl AccountStatus {
    pub fn new(identity: &Identity) -> AccountStatus {
        AccountStatus {
            identity: identity.clone(),
            hints: Hints::Off,
            credentials_warning: None,
            recent: Vec::new(),
            notes_added: 0,
        }
    }
}

/// The notification connection (wake-up hints from the identity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hints {
    /// Not yet: the account waits for its credentials or for the identity to answer.
    Off,
    Connecting,
    Connected,
    Lost(String),
    Refused(String),
}

/// What a folder's sync is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Catching up with the folder and the drive.
    Starting,
    Syncing,
    /// Up to date, or waiting for the next change.
    Idle,
    Paused,
    /// The identity can't be reached; retrying.
    Offline,
    /// The last sync failed for another reason; retrying.
    Failing,
    /// The identity refused this device: authorize it again.
    Denied,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FolderStatus {
    /// The account the folder syncs with.
    pub identity: Identity,
    pub path: PathBuf,
    /// The role folder it is; `None` for the main folder (the drive's root).
    pub role: Option<Role>,
    pub state: State,
    /// Until when a pause lasts (Unix seconds); `None` while not paused or until resumed.
    pub paused_until: Option<u64>,
    /// How far the sync in progress is.
    pub progress: Option<Progress>,
    /// Why the last sync failed (states `Offline`, `Failing`, `Denied`).
    pub error: Option<String>,
    /// When the next attempt is after a failure (Unix seconds).
    pub retry_at: Option<u64>,
    pub last_sync: Option<LastSync>,
    /// Items not in sync yet.
    pub pending: u64,
    /// Deletes waiting for the user's confirmation.
    pub held_deletes: Option<HeldDeletes>,
    /// Conflicts and failures of recent syncs, newest last.
    pub recent: Vec<Note>,
    /// Notes added since the daemon started (more than [`FolderStatus::recent`] keeps): what
    /// is new since a client last looked.
    pub notes_added: u64,
    /// Entries added to the recently synced list since the daemon started: a client reads
    /// the list again ([`Ask::Recent`]) when this changes.
    pub synced_added: u64,
    /// Why there is no live watcher (the folder is then rescanned once a minute).
    pub watcher_error: Option<String>,
    /// Ignore rules in `.hbsync/ignore`.
    pub rules: usize,
}

impl FolderStatus {
    /// Starting to sync the main folder at `path` with `identity`.
    pub fn new(identity: &Identity, path: &Path) -> FolderStatus {
        FolderStatus {
            identity: identity.clone(),
            path: path.to_path_buf(),
            role: None,
            state: State::Starting,
            paused_until: None,
            progress: None,
            error: None,
            retry_at: None,
            last_sync: None,
            pending: 0,
            held_deletes: None,
            recent: Vec::new(),
            notes_added: 0,
            synced_added: 0,
            watcher_error: None,
            rules: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastSync {
    /// When it ended (Unix seconds).
    pub at: u64,
    pub millis: u64,
    pub done: usize,
    pub downloaded_changes: usize,
    pub adopted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldDeletes {
    /// Items deleted here, to be deleted on the drive.
    pub here: u64,
    /// Items deleted elsewhere, to be moved to the trash here.
    pub remotely: u64,
    /// Some of them.
    pub examples: Vec<String>,
    /// Names exactly these deletes: a confirmation carries it back.
    pub token: String,
}

impl HeldDeletes {
    pub fn total(&self) -> u64 {
        self.here + self.remotely
    }

    /// More are held than [`HeldDeletes::examples`] shows.
    pub fn more_than_shown(&self) -> bool {
        (self.examples.len() as u64) < self.total()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    /// Unix seconds.
    pub at: u64,
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A timed pause runs out when its time comes (the daemon goes by this, and the config);
    /// one until resumed never does.
    #[test]
    fn a_timed_pause_runs_out_when_its_time_comes() {
        let timed = Pause::ending_at(100);
        assert!(!timed.ran_out(99));
        assert!(timed.ran_out(100));
        assert!(!Pause::UNTIL_RESUMED.ran_out(u64::MAX));
        let now = Instant::now();
        assert_eq!(timed.end_at(now, 90), Some(now + Duration::from_secs(10)));
        assert_eq!(timed.end_at(now, 200), Some(now), "already over");
        assert_eq!(Pause::UNTIL_RESUMED.end_at(now, 0), None);
        let far = Pause::ending_at(u64::MAX);
        assert_eq!(far.end_at(now, 0), None, "too far ahead for an instant");
    }
}
