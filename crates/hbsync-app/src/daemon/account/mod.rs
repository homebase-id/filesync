//! One account (identity) of the daemon: it waits for the credentials (a keychain locked at
//! logon), connects (retrying while the identity can't be reached: a laptop starts offline),
//! and owns what is the account's: the [`Drive`](hbsync_engine::Drive), the notifications (a
//! change announced is pulled once, which wakes the folders it concerns), Recently deleted and
//! its daily purge. Its folders' runners sync until the daemon stops, or until it starts over
//! with new credentials. Its thread blocks on one channel ([`Msg`]) until the next thing that
//! is due.
//!
//! Its files: this one holds the account's handle, shared with the control socket's thread
//! (what it shows and its folders' runners, under one lock; only the loop changes the
//! runners), and the answers it gives; `run.rs` its loop (the credentials, connecting, serving
//! its channel); `parts.rs` the account's wiring once connected, shared with the dev tool
//! `hbsync-sync`; `feed.rs` the notifications and the puller that bring the drive's changes;
//! `runners.rs` its folders' runners and their restarts.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use hbsync_engine::deleted::Listed;
use hbsync_engine::root::{AccountReader, RootError};
use hbsync_homebase::notify::Event;
use hbsync_state::StateError;
use hbsync_types::NodeId;
use hbsync_types::activity::Deleted;

use crate::protocol::{
    AccountControl, AccountStatus, DeletedAccount, DeletedItem, FolderStatus, Pause, RecentItem,
    State,
};
use crate::runner::{Command, Handle};
use crate::sync::{Wakeup, lock};
use crate::{Identity, Paths, Root};

mod feed;
mod parts;
mod run;
mod runners;

use runners::Running;

pub use feed::{Notifications, Puller, UNANNOUNCED_READ};
pub use parts::AccountParts;

/// An account of the daemon: the handle its thread and the control socket's share. What it
/// shows and its folders' runners are under one lock ([`Shown`]); its thread alone changes the
/// runners ([`Account::run`]).
pub(crate) struct Account {
    pub identity: Identity,
    /// Its state DB.
    db: PathBuf,
    shown: Mutex<Shown>,
    /// What its thread is told.
    requests: Sender<Msg>,
    /// Told to end (to start over with new credentials).
    ended: AtomicBool,
    /// Its thread is ending: joining it waits for no more than that.
    pub done: AtomicBool,
    /// A connection to its DB for the control socket's answers (recent, deleted), opened on
    /// first use.
    reader: Mutex<Option<AccountReader>>,
    /// Raised when its status (or its folders') may have changed.
    changed: Arc<Wakeup>,
}

/// The account's status, its pause and its folders.
struct Shown {
    status: AccountStatus,
    /// Paused (kept in the config too): a runner starts paused.
    paused: Option<Pause>,
    /// Waiting for the credentials or the identity; `None` once its runners run (taken as
    /// they start: there are none before).
    waiting: Option<Waiting>,
    /// Its folders' runners once it runs, changed by its thread alone. Those whose folder was
    /// removed don't show ([`Running::shows`]) while they stop.
    runners: Vec<Running>,
}

/// Before its runners start, the account still answers.
struct Waiting {
    /// Its folders, as the config lists them.
    roots: Vec<Root>,
    state: State,
    error: Option<String>,
}

impl Shown {
    /// The runners the status shows.
    fn shown(&self) -> impl Iterator<Item = &Running> {
        self.runners.iter().filter(|r| r.shows())
    }

    /// Passes `command` to every runner shown; refused, saying why, while it isn't running
    /// yet.
    fn send(&self, identity: &Identity, command: &Command) -> Result<(), String> {
        if let Some(waiting) = &self.waiting {
            return Err(waiting.not_running(identity));
        }
        for r in self.shown() {
            r.handle.send(command.clone());
        }
        Ok(())
    }
}

/// What the account's thread waits for, on one channel.
pub(crate) enum Msg {
    Request(Request),
    /// From its notifications.
    Hint(Event),
    /// The machine woke up.
    Woke,
    /// Look again: it is to end, the daemon stops, its pause changed, or a runner is ending.
    Wake,
}

/// What the account's thread is asked to do.
pub(crate) enum Request {
    Deleted(AccountControl),
    /// Start syncing this folder.
    AddFolder(Root),
    /// Stop syncing this folder.
    RemoveFolder(PathBuf),
}

impl Account {
    /// The account of `root`'s identity syncing `root`, started paused when `paused`; its
    /// thread runs with the returned receiver ([`Account::run`]).
    pub(super) fn new(
        paths: &Paths,
        root: &Root,
        paused: Option<Pause>,
        changed: Arc<Wakeup>,
    ) -> (Arc<Account>, Receiver<Msg>) {
        let (requests, rx) = channel();
        let identity = &root.identity;
        let account = Account {
            identity: identity.clone(),
            db: paths.account_db(identity),
            shown: Mutex::new(Shown {
                status: AccountStatus::new(identity),
                paused,
                waiting: Some(Waiting {
                    roots: vec![root.clone()],
                    state: State::Starting,
                    error: None,
                }),
                runners: Vec::new(),
            }),
            requests,
            ended: AtomicBool::new(false),
            done: AtomicBool::new(false),
            reader: Mutex::new(None),
            changed,
        };
        (Arc::new(account), rx)
    }

    /// Its folders.
    pub(super) fn folders(&self) -> Vec<Root> {
        let shown = lock(&self.shown);
        match &shown.waiting {
            Some(waiting) => waiting.roots.clone(),
            None => shown.shown().map(|r| r.root.clone()).collect(),
        }
    }

    /// Syncs the folder `root` too (once connected).
    pub(super) fn add_folder(&self, root: &Root) {
        match &mut lock(&self.shown).waiting {
            Some(waiting) => {
                if !waiting.roots.iter().any(|r| r.folder == root.folder) {
                    waiting.roots.push(root.clone());
                    self.changed.raise();
                }
            }
            None => self.tell(Msg::Request(Request::AddFolder(root.clone()))),
        }
    }

    /// Stops syncing the folder at `path`.
    pub(super) fn remove_folder(&self, path: &Path) {
        match &mut lock(&self.shown).waiting {
            Some(waiting) => {
                waiting.roots.retain(|r| r.folder != path);
                self.changed.raise();
            }
            None => self.tell(Msg::Request(Request::RemoveFolder(path.to_path_buf()))),
        }
    }

    /// Its status and its folders'.
    pub(super) fn status(&self) -> (AccountStatus, Vec<FolderStatus>) {
        let shown = lock(&self.shown);
        let folders = match &shown.waiting {
            None => shown.shown().map(|r| r.handle.status()).collect(),
            Some(waiting) => waiting
                .roots
                .iter()
                .map(|root| waiting.status(root, shown.paused))
                .collect(),
        };
        (shown.status.clone(), folders)
    }

    /// The runner of its folder at `path`: `None` when it doesn't sync it; refused, saying
    /// why, while it isn't running yet.
    pub(super) fn runner(&self, path: &Path) -> Option<Result<Handle, String>> {
        let shown = lock(&self.shown);
        if let Some(waiting) = &shown.waiting {
            let syncs = waiting.roots.iter().any(|r| r.folder == path);
            return syncs.then(|| Err(waiting.not_running(&self.identity)));
        }
        let found = shown.shown().find(|r| r.root.folder == path);
        found.map(|r| Ok(r.handle.clone()))
    }

    /// Passes `command` to every runner; refused, saying why, while it isn't running yet.
    pub(crate) fn send_all(&self, command: &Command) -> Result<(), String> {
        lock(&self.shown).send(&self.identity, command)
    }

    /// Pauses (`Some`) or resumes its folders, also before they run.
    pub(super) fn pause(&self, pause: Option<Pause>) {
        {
            let mut shown = lock(&self.shown);
            shown.paused = pause;
            // Before they run, they start as `paused` says.
            let _ = shown.send(
                &self.identity,
                &match pause {
                    Some(pause) => Command::Pause(pause),
                    None => Command::Resume,
                },
            );
        }
        self.wake();
        self.changed.raise();
    }

    /// Why it can't take requests yet; `None` once it runs.
    pub(super) fn not_running(&self) -> Option<String> {
        let shown = lock(&self.shown);
        let waiting = shown.waiting.as_ref()?;
        Some(waiting.not_running(&self.identity))
    }

    /// Asks for `control` on its Recently deleted; done on its thread.
    pub(super) fn request(&self, control: AccountControl) -> Result<(), String> {
        if let Some(why) = self.not_running() {
            return Err(why);
        }
        // Answered before it runs: a listing the drive would refuse is refused here, so the
        // refusal reaches whoever asked (the drive checks again as it empties).
        if let AccountControl::EmptyListed { db, seq }
        | AccountControl::EmptyDeleted { db, seq, .. } = &control
        {
            let listed = Listed {
                db: db.clone(),
                seq: *seq,
            };
            match self.try_read(|r| r.listed_here(&listed)) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(format!(
                        "Recently deleted of {} was never listed so: look at it again",
                        self.identity
                    ));
                }
                Err(e) => {
                    return Err(format!(
                        "can't read Recently deleted of {}: {e}",
                        self.identity
                    ));
                }
            }
        }
        let request = Msg::Request(Request::Deleted(control));
        self.requests
            .send(request)
            .map_err(|_| "the account has stopped".to_owned())
    }

    /// Ends the account: its runners stop between operations and [`Account::run`] returns.
    pub(super) fn end(&self) {
        self.ended.store(true, Ordering::Relaxed);
        self.wake();
    }

    /// Has its thread look again (whether to stop, its pause).
    pub(super) fn wake(&self) {
        self.tell(Msg::Wake);
    }

    /// The machine woke up: its notifications reconnect, its folders sync.
    pub(super) fn woke(&self) {
        self.tell(Msg::Woke);
    }

    /// Tells its thread `msg`; lost only once the thread has ended.
    fn tell(&self, msg: Msg) {
        let _ = self.requests.send(msg);
    }

    /// The newest `limit` entries of its folders' recently synced lists.
    pub(super) fn recent(&self, limit: usize) -> Vec<RecentItem> {
        let folders = self.folders();
        self.read(|db| {
            let mut items = Vec::new();
            for f in db.folders()? {
                let path = PathBuf::from(&f.path);
                if !folders.iter().any(|r| r.folder == path) {
                    continue;
                }
                items.extend(
                    db.recent_synced(f.id, limit)?
                        .into_iter()
                        .map(|synced| RecentItem {
                            folder: path.clone(),
                            synced,
                        }),
                );
            }
            Ok(items)
        })
        .unwrap_or_default()
    }

    /// The newest `limit` deletes of its Recently deleted, newest first, each with how many
    /// items are in it; and, when it holds anything, all of it as listed now.
    pub(super) fn deleted(&self, limit: usize) -> (Vec<DeletedItem>, Option<DeletedAccount>) {
        let Some((deletes, summary)) = self.read(|db| db.deletes(limit)) else {
            return (Vec::new(), None);
        };
        let items = self.items(deletes, &summary.listed);
        // Anything to list, though emptying may take nothing (a folder holding a live node).
        let account = (summary.deletes > 0).then(|| DeletedAccount {
            identity: self.identity.clone(),
            deletes: summary.deletes,
            items: summary.items,
            stays: summary.stays,
            db: summary.listed.db,
            seq: summary.listed.seq,
        });
        (items, account)
    }

    /// The items of its Recently deleted that are `nodes`.
    pub(super) fn find_deleted(&self, nodes: &[NodeId]) -> Vec<DeletedItem> {
        self.read(|db| db.find_deleted(nodes))
            .map(|(found, listed)| self.items(found, &listed))
            .unwrap_or_default()
    }

    /// The items of its Recently deleted named `names`.
    pub(super) fn named_deleted(&self, names: &[String]) -> Vec<DeletedItem> {
        self.read(|db| db.named_deleted(names))
            .map(|(found, listed)| self.items(found, &listed))
            .unwrap_or_default()
    }

    /// `found`, as the listing `listed` saw them.
    fn items(&self, found: Vec<(Deleted, usize)>, listed: &Listed) -> Vec<DeletedItem> {
        found
            .into_iter()
            .map(|(deleted, along)| DeletedItem {
                identity: self.identity.clone(),
                deleted,
                along,
                db: listed.db.clone(),
                seq: listed.seq,
            })
            .collect()
    }

    /// `f` on the answers' connection, which only reads, opened on first use; `None` when the
    /// DB isn't there yet (or not brought up to date yet: the drive does as it opens) or `f`
    /// failed, which drops the connection.
    fn read<T>(&self, f: impl FnOnce(&mut AccountReader) -> Result<T, StateError>) -> Option<T> {
        self.try_read(f).ok()
    }

    /// [`Account::read`], saying why there is no answer.
    fn try_read<T>(
        &self,
        f: impl FnOnce(&mut AccountReader) -> Result<T, StateError>,
    ) -> Result<T, String> {
        let mut reader = lock(&self.reader);
        if reader.is_none() {
            *reader = match AccountReader::open(&self.db, &self.identity) {
                Ok(opened) => opened,
                Err(RootError::State(e @ StateError::Behind { .. })) => {
                    tracing::debug!("{}: {e}", self.identity);
                    return Err(e.to_string());
                }
                Err(e) => {
                    tracing::warn!("{}: can't read its state DB: {e}", self.identity);
                    return Err(e.to_string());
                }
            };
        }
        let Some(opened) = reader.as_mut() else {
            return Err("its state DB isn't there yet".to_owned());
        };
        f(opened).map_err(|e| {
            tracing::warn!("{}: can't read its state DB: {e}", self.identity);
            *reader = None;
            e.to_string()
        })
    }
}

impl Waiting {
    /// The status of its folder `root`, `paused` or waiting.
    fn status(&self, root: &Root, paused: Option<Pause>) -> FolderStatus {
        let mut folder = FolderStatus::new(&root.identity, &root.folder);
        folder.role = root.role;
        folder.state = if paused.is_some() {
            State::Paused
        } else {
            self.state
        };
        folder.paused_until = paused.and_then(|p| p.until);
        folder.error.clone_from(&self.error);
        folder
    }

    /// Why the account `identity` can't take requests yet.
    fn not_running(&self, identity: &Identity) -> String {
        let why = self.error.as_ref().map(|e| format!(": {e}"));
        format!("not connected to {identity} yet{}", why.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::Drive;
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    /// Emptying Recently deleted as listed is refused in the answer for a listing of another DB
    /// (the identity's, made again) or one this DB never gave; one it gave is taken.
    #[test]
    fn a_foreign_listing_is_refused_in_the_answer() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let root = Root::main(&"a.example".into(), &home.path().join("Homebase"));
        let (account, _requests) = Account::new(&paths, &root, None, Arc::default());
        // Its DB, from the drive; its runners running (none needed here).
        let db = paths.account_db(&"a.example".into());
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        Drive::open(
            &db,
            "a.example",
            Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default()))),
        )?;
        lock(&account.shown).waiting = None;
        let listed = account.read(|r| r.deletes(0)).ok_or("unreadable")?.1.listed;
        let empty = |db: &str, seq| {
            account.request(AccountControl::EmptyListed {
                db: db.to_owned(),
                seq,
            })
        };
        let refused = empty("another-db", listed.seq).expect_err("another DB's listing");
        assert!(refused.contains("never listed"), "{refused}");
        assert!(empty(&listed.db, listed.seq + 1).is_err(), "never given");
        empty(&listed.db, listed.seq)?;
        Ok(())
    }

    /// Recently deleted holding only a folder that stays (it holds a live node) is still listed:
    /// the folder can be restored, though emptying takes nothing.
    #[test]
    fn a_folder_that_only_stays_is_still_listed() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_state::{MirrorRow, RemovedRow, StateDb};
        use hbsync_types::{NodeId, NodeKind, RemoteFileId, Uuid, VersionTag};
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let root = Root::main(&"a.example".into(), &home.path().join("Homebase"));
        let (account, _requests) = Account::new(&paths, &root, None, Arc::default());
        let db = paths.account_db(&"a.example".into());
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        Drive::open(
            &db,
            "a.example",
            Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default()))),
        )?;
        let row = |node, parent| MirrorRow {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            node,
            parent,
            name: node.0.to_string(),
            kind: NodeKind::Folder,
            file: None,
            content_extra: None,
            token: None,
            changed_at: Some(5),
        };
        let folder = NodeId::new();
        StateDb::open_account(&db)?.write(|s| {
            s.put_removed(&RemovedRow {
                row: row(folder, NodeId::ROOT),
                removed_at: Some(10),
            })?;
            s.put_mirror(&row(NodeId::new(), folder))
        })?;
        let (items, summary) = account.deleted(10);
        let summary = summary.ok_or("Recently deleted shown empty")?;
        assert_eq!((items.len(), summary.deletes, summary.items), (1, 1, 0));
        Ok(())
    }

    /// While it waits for the identity, a role folder shows as that role's (not as a main
    /// folder), from the config's entry; paused, it shows paused.
    #[test]
    fn a_waiting_folder_shows_its_role() {
        let paths = Paths::under(Path::new("/h"));
        let main = Root::main(&"a.example".into(), Path::new("/h/Homebase"));
        let documents = Root {
            role: Some(hbsync_types::Role::Documents),
            ..Root::main(&"a.example".into(), Path::new("/h/Documents"))
        };
        let (account, _requests) = Account::new(&paths, &documents, None, Arc::default());
        account.add_folder(&main);
        let (_, folders) = account.status();
        let shown: Vec<_> = folders.iter().map(|f| (f.path.as_path(), f.role)).collect();
        assert_eq!(
            shown,
            [
                (documents.folder.as_path(), documents.role),
                (main.folder.as_path(), None)
            ]
        );
        assert!(folders.iter().all(|f| f.state == State::Starting));
        account.pause(Some(Pause::UNTIL_RESUMED));
        let (_, folders) = account.status();
        assert!(folders.iter().all(|f| f.state == State::Paused));
        assert_eq!(account.folders(), [documents, main]);
    }
}
