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

use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use hbsync_engine::deleted::Listed;
use hbsync_engine::root::{AccountReader, RootError};
use hbsync_homebase::notify::Event;
use hbsync_remote::Skipped;
use hbsync_state::StateError;
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, unix_now};

use crate::protocol::{
    AccountControl, AccountStatus, DeletedAccount, DeletedItem, FolderStatus, Pause, RecentItem,
    State,
};
use crate::runner::{Command, Handle, add_notes};
use crate::sync::{Wakeup, lock};
use crate::words;
use crate::{Identity, Paths, Root};

mod feed;
mod parts;
mod run;
mod runners;

use runners::Running;

/// Unreadable rows remembered as told, at most ([`Account::note_unreadable`]).
const ROWS_TOLD: usize = 10_000;
/// New unreadable rows noted one by one, at most: more are one note.
const ROWS_NOTED: usize = 3;

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
    /// Its DB is ready ([`crate::folders::ready_account`]): the answers may open it, until
    /// its thread ends (one set aside meanwhile must not be held open, nor read on).
    ready: AtomicBool,
    /// A connection to its DB for the control socket's answers (recent, deleted), opened on
    /// first use.
    reader: Mutex<Option<AccountReader>>,
    /// Raised when its status (or its folders') may have changed.
    changed: Arc<Wakeup>,
}

/// The account's status, its pause and its folders.
struct Shown {
    status: AccountStatus,
    /// The drive's rows it can't read that were told (a hash of each row's version and why:
    /// 8 bytes a row), each once while the daemon runs.
    rows_told: HashSet<u64>,
    /// The last pause command its runners were told (the pause is kept in the config too): a
    /// runner starts paused while it pauses, and one started meanwhile is told it
    /// ([`Shown::missed`]).
    pausing: Command,
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

    /// Its pause.
    fn paused(&self) -> Option<Pause> {
        self.pausing.paused()
    }

    /// What a runner started `paused` missed: the pause command sent since, if it changed the
    /// pause. A timed pause that ran out stays [`Command::PauseEnded`]: not the user's Resume,
    /// which also asks again what the drive refused.
    fn missed(&self, paused: Option<Pause>) -> Option<Command> {
        (self.paused() != paused).then(|| self.pausing.clone())
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
    /// A try at connecting ended (its thread's answer, while the account connects).
    Connected(Box<Result<hbsync_homebase::drive::HomebaseRemote, hbsync_homebase::HomebaseError>>),
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
                rows_told: HashSet::new(),
                pausing: Command::pausing(paused),
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
            ready: AtomicBool::new(false),
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

    /// Its main folder, if it syncs one.
    pub(super) fn main_folder(&self) -> Option<PathBuf> {
        let shown = lock(&self.shown);
        let main = match &shown.waiting {
            Some(waiting) => waiting.roots.iter().find(|r| r.role.is_none()),
            None => shown.shown().map(|r| &r.root).find(|r| r.role.is_none()),
        };
        main.map(|r| r.folder.clone())
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
                .map(|root| waiting.status(root, shown.paused()))
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
        self.set_pause(Command::pausing(pause));
    }

    /// Its timed pause ran out: its folders sync again (no resume asked by the user).
    pub(super) fn pause_ended(&self) {
        self.set_pause(Command::PauseEnded);
    }

    /// Holds the pause `command` sets, and tells its folders.
    fn set_pause(&self, command: Command) {
        {
            let mut shown = lock(&self.shown);
            shown.pausing = command;
            // Before they run, they start as it says.
            let _ = shown.send(&self.identity, &shown.pausing);
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

    /// Its thread ended: the answers' connection to its DB is closed, not opened again.
    pub(super) fn ended_thread(&self) {
        self.done.store(true, Ordering::SeqCst);
        *lock(&self.reader) = None;
    }

    /// Has its thread look again (whether to stop, its pause).
    pub(super) fn wake(&self) {
        self.tell(Msg::Wake);
    }

    /// The machine woke up: its notifications reconnect, its folders sync.
    pub(super) fn woke(&self) {
        self.tell(Msg::Woke);
    }

    /// Tells the user `notes`, the account's (its folders' are theirs).
    pub(super) fn note(&self, notes: Vec<String>) {
        for note in &notes {
            tracing::warn!("{}: {note}", self.identity);
        }
        self.push_notes(notes);
    }

    /// Adds `notes` to its status (logged by the caller).
    fn push_notes(&self, notes: Vec<String>) {
        if notes.is_empty() {
            return;
        }
        let mut shown = lock(&self.shown);
        let status = &mut shown.status;
        add_notes(
            &mut status.recent,
            &mut status.notes_added,
            notes,
            unix_now(),
        );
        drop(shown);
        self.changed.raise();
    }

    /// Tells the user the drive's `rows` it can't read, each version once while the daemon
    /// runs (a row without its file, once a reason); more than a few new at once, as one note.
    pub(super) fn note_unreadable(&self, rows: Vec<Skipped>) {
        let mut notes: Vec<String> = {
            let mut shown = lock(&self.shown);
            // Bounded: forgotten (told again) past this many, their room given back.
            if shown.rows_told.len() > ROWS_TOLD {
                shown.rows_told = HashSet::new();
            }
            let told = &mut shown.rows_told;
            rows.into_iter()
                .filter(|row| {
                    let mut key = DefaultHasher::new();
                    (row.at, &row.why).hash(&mut key);
                    told.insert(key.finish())
                })
                .map(|row| row.why)
                .collect()
        };
        // A drive of another app's files may have thousands: one note (and log line) for them.
        if notes.len() > ROWS_NOTED {
            for why in &notes {
                tracing::debug!("{}: a row of the drive can't be read: {why}", self.identity);
            }
            let more = notes.len() - 1;
            notes = vec![format!("{} and {more} more", notes[0])];
        }
        for why in &notes {
            tracing::warn!("{}: a row of the drive can't be read: {why}", self.identity);
        }
        let notes = notes
            .into_iter()
            .map(|why| format!("a file on Homebase can't be synced: {why}"))
            .collect();
        self.push_notes(notes);
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
                let Some(root) = folders.iter().find(|r| r.folder == path) else {
                    continue;
                };
                items.extend(
                    db.recent_synced(f.id, limit)?
                        .into_iter()
                        .map(|synced| RecentItem {
                            folder: path.clone(),
                            main: root.role.is_none(),
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
    pub(super) fn deleted(
        &self,
        limit: usize,
        main: Option<&Path>,
    ) -> (Vec<DeletedItem>, Option<DeletedAccount>) {
        let Some((deletes, summary)) = self.read(|db| db.deletes(limit)) else {
            return (Vec::new(), None);
        };
        let items = self.items(deletes, &summary.listed, main);
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
    pub(super) fn find_deleted(&self, nodes: &[NodeId], main: Option<&Path>) -> Vec<DeletedItem> {
        self.read(|db| db.find_deleted(nodes))
            .map(|(found, listed)| self.items(found, &listed, main))
            .unwrap_or_default()
    }

    /// The items of its Recently deleted named `names`.
    pub(super) fn named_deleted(&self, names: &[String], main: Option<&Path>) -> Vec<DeletedItem> {
        self.read(|db| db.named_deleted(names))
            .map(|(found, listed)| self.items(found, &listed, main))
            .unwrap_or_default()
    }

    /// `found`, as the listing `listed` saw them, placed as people name them, `main` being
    /// the main folder ([`words::deleted_place`]).
    fn items(
        &self,
        found: Vec<(Deleted, usize)>,
        listed: &Listed,
        main: Option<&Path>,
    ) -> Vec<DeletedItem> {
        found
            .into_iter()
            .map(|(deleted, along)| DeletedItem {
                identity: self.identity.clone(),
                place: Some(words::deleted_place(&deleted, main)).filter(|p| *p != deleted.path),
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
            if !self.ready.load(Ordering::SeqCst) || self.done.load(Ordering::SeqCst) {
                return Err("its state DB isn't ready".to_owned());
            }
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

    /// A runner started while the pause changed is told the command that changed it: a timed
    /// pause that ran out as its end, not as the user's Resume (which would also ask a refusing
    /// identity again).
    #[test]
    fn a_runner_started_meanwhile_is_told_the_pause_command_sent() {
        let home = tempfile::tempdir().expect("a temporary folder");
        let paths = Paths::under(home.path());
        let root = Root::main(&"a.example".into(), &home.path().join("Homebase"));
        let timed = Pause::ending_at(hbsync_types::unix_now() + 60);
        let (account, _requests) = Account::new(&paths, &root, Some(timed), Arc::default());
        let missed = |started| lock(&account.shown).missed(started);
        assert_eq!(missed(Some(timed)), None, "nothing changed");

        account.pause_ended();
        assert_eq!(missed(Some(timed)), Some(Command::PauseEnded));
        assert_eq!(missed(None), None);

        account.pause(Some(Pause::UNTIL_RESUMED));
        assert_eq!(missed(None), Some(Command::Pause(Pause::UNTIL_RESUMED)));
        account.pause(None);
        assert_eq!(missed(Some(timed)), Some(Command::Resume));
    }

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
        account.ready.store(true, Ordering::SeqCst);
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
        // Its DB ready, as its thread makes it.
        account.ready.store(true, Ordering::SeqCst);
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
            s.account().move_to_removed(&RemovedRow {
                row: row(folder, NodeId::ROOT),
                removed_at: Some(10),
            })?;
            s.account().put_live(&row(NodeId::new(), folder))
        })?;
        let (items, summary) = account.deleted(10, Some(Path::new("/h/Homebase")));
        let summary = summary.ok_or("Recently deleted shown empty")?;
        // In no folder here: placed so.
        assert!(items[0].place().starts_with("(on the drive) "), "{items:?}");
        assert_eq!((items.len(), summary.deletes, summary.items), (1, 1, 0));
        Ok(())
    }

    /// Its DB damaged while the account runs (SQLite answers that it is malformed): its folder's
    /// runner ends, the account stops its parts, sets the DB aside, tells it once, and syncs on
    /// in a new one, the folder adopted (nothing sent, nothing deleted). Damaged again, it isn't
    /// set aside again: the folder fails, starting again after a pause.
    #[test]
    fn a_db_damaged_while_running_is_set_aside_once() -> Result<(), Box<dyn std::error::Error>> {
        use std::time::{Duration, Instant};

        use super::feed::{Notifications, Puller};
        use crate::Config;

        let home = tempfile::tempdir()?;
        let paths = Paths::under(&home.path().canonicalize()?);
        let identity: Identity = "a.example".into();
        let folder = paths.state.with_file_name("Homebase");
        std::fs::create_dir_all(&folder)?;
        std::fs::write(folder.join("a.txt"), "a")?;
        let root = Root::main(&identity, &folder);
        let db = paths.account_db(&identity);
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        hbsync_engine::root::SyncRoot::create(&db, &identity, &folder)?;
        let server = FakeServer::new(ServerConfig::default());
        let remote = FakeRemote::new(server.clone());

        let (account, requests) = Account::new(&paths, &root, None, Arc::default());
        let stop = Arc::new(AtomicBool::new(false));
        let running = {
            let (account, paths, stop, remote) = (
                Arc::clone(&account),
                paths.clone(),
                Arc::clone(&stop),
                remote.clone(),
            );
            let identity = identity.clone();
            std::thread::spawn(move || {
                let mut msgs = Vec::new();
                let roots = [root];
                let start = super::run::Readying::Start(&roots);
                account.ready_db(&paths, start, &stop, &requests, &mut msgs)?;
                account.sync(
                    &paths,
                    &Config::default(),
                    &requests,
                    &stop,
                    &mut msgs,
                    |db| {
                        let drive = Drive::open(db, &identity, Arc::new(remote.clone()))?;
                        let puller = Puller::spawn(Arc::clone(&drive));
                        Ok(AccountParts {
                            drive,
                            notifier: Err(std::io::Error::other("none here")),
                            notifications: Notifications::new(puller, Instant::now()),
                        })
                    },
                )
            })
        };
        let wait = |what: &str, done: &dyn Fn(&AccountStatus, &FolderStatus) -> bool| {
            let until = Instant::now() + Duration::from_secs(30);
            loop {
                let (status, folders) = account.status();
                if folders.first().is_some_and(|f| done(&status, f)) {
                    return Ok(());
                }
                if Instant::now() > until {
                    return Err(format!("timed out: {what}: {status:?} {folders:?}"));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        let synced = |_: &AccountStatus, f: &FolderStatus| {
            f.state == State::Idle && f.last_sync.is_some() && f.pending == 0
        };
        let told = |s: &AccountStatus| {
            let set_aside = words::set_aside(hbsync_state::Unusable::Corrupt);
            s.recent.iter().filter(|n| n.text == set_aside).count()
        };
        // Its table's root page past the file's end: SQLite answers that the DB is malformed,
        // on every connection, once each reads the schema again.
        let damage = || -> Result<(), Box<dyn std::error::Error>> {
            let conn = rusqlite::Connection::open(&db)?;
            let version: i64 = conn.pragma_query_value(None, "schema_version", |r| r.get(0))?;
            conn.execute_batch(
                "PRAGMA writable_schema = ON;
                 UPDATE sqlite_schema SET rootpage = 99999 WHERE name = 'dirty';",
            )?;
            conn.pragma_update(None, "schema_version", version + 1)?;
            Ok(())
        };
        let sync_now = || -> Result<(), Box<dyn std::error::Error>> {
            account
                .runner(&folder)
                .ok_or("no runner")??
                .send(Command::SyncNow);
            Ok(())
        };
        let aside = || -> std::io::Result<usize> {
            let named = |e: &std::fs::DirEntry| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with("state.db.corrupt-")
                    && !name.ends_with("-wal")
                    && !name.ends_with("-shm")
            };
            let dir = std::fs::read_dir(db.parent().unwrap_or(&db))?;
            Ok(dir.flatten().filter(named).count())
        };

        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            wait("the first sync", &synced)?;
            let uploads = remote.faults().uploads;
            damage()?;
            sync_now()?;
            wait("set aside and synced again", &|s, f| {
                told(s) == 1 && synced(s, f)
            })?;
            assert_eq!(aside()?, 1, "kept");
            hbsync_engine::root::SyncRoot::find(&db, &identity, &folder)?;
            assert_eq!(
                remote.faults().uploads,
                uploads,
                "adopted: nothing sent again"
            );
            assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 0);

            damage()?;
            sync_now()?;
            wait("failing", &|_, f| {
                f.state == State::Failing
                    && f.error.as_deref().is_some_and(|e| e.contains("malformed"))
            })?;
            std::thread::sleep(Duration::from_millis(300));
            let (status, _) = account.status();
            assert_eq!((told(&status), aside()?), (1, 1), "set aside once a run");
            Ok(())
        };
        let checked = checks();
        // Stopped whatever the checks found: nothing runs on into the other tests.
        stop.store(true, Ordering::SeqCst);
        account.wake();
        running.join().map_err(|_| "panicked")??;
        checked
    }

    /// The account's notes are on its status, its folders' untouched: a row of the drive that
    /// doesn't read is told once a version (again for a new one), a failed request each time;
    /// many rows at once are one note.
    #[test]
    fn the_accounts_notes_are_its_own() {
        use hbsync_types::{RemoteFileId, RemoteRef, Uuid, VersionTag};
        let paths = Paths::under(Path::new("/h"));
        let main = Root::main(&"a.example".into(), Path::new("/h/Homebase"));
        let (account, _requests) = Account::new(&paths, &main, None, Arc::default());
        let file = RemoteFileId(Uuid::new_v4());
        let row = |version: VersionTag| Skipped {
            at: Some(RemoteRef {
                file_id: file,
                version,
            }),
            ..Skipped::why("another key")
        };
        let (v1, v2) = (VersionTag(Uuid::new_v4()), VersionTag(Uuid::new_v4()));
        account.note_unreadable(vec![row(v1), row(v1)]);
        account.note_unreadable(vec![row(v1)]);
        account.note(vec!["Restoring failed".into()]);
        account.note_unreadable(vec![row(v2)]);
        let (status, folders) = account.status();
        let texts: Vec<_> = status.recent.iter().map(|n| n.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                "a file on Homebase can't be synced: another key",
                "Restoring failed",
                "a file on Homebase can't be synced: another key"
            ]
        );
        assert_eq!(status.notes_added, 3);
        assert!(folders.iter().all(|f| f.recent.is_empty()));
        // Many new at once: one note.
        let many = (0..5).map(|_| row(VersionTag(Uuid::new_v4()))).collect();
        account.note_unreadable(many);
        let (status, _) = account.status();
        assert_eq!(status.notes_added, 4);
        assert_eq!(
            status.recent.last().map(|n| n.text.as_str()),
            Some("a file on Homebase can't be synced: another key and 4 more")
        );
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
