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
//! `runners.rs` its folders' runners and their restarts, and whose shares are synced, as
//! the identity's share markers say.

use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use hbsync_engine::deleted::Listed;
use hbsync_engine::root::{AccountReader, RootError};
use hbsync_homebase::drive::HomebaseRemote;
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

mod ending;
mod feed;
mod parts;
mod run;
mod runners;
mod sharers;

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
    /// Sync now asked before its folders run ([`Clicks`]).
    clicks: Clicks,
    /// The main folder's watcher gives what other identities share its events: their
    /// folders, in its folder of shares, hold no watcher of their own (filesync #414).
    feeds: hbsync_local::Feeds,
    /// Where the main folder's runner last told its folder of shares to be
    /// ([`crate::runner::SharesTold`]): kept here, through that runner being started again.
    shares_told: Arc<Mutex<Option<Option<String>>>>,
}

/// Sync now asked while an account isn't running yet, counted: a wait to try again ends for a
/// click since its try began ([`Clicks::begin_try`]), one try a click.
#[derive(Default)]
struct Clicks {
    asked: AtomicU64,
    /// Those a wait ended for.
    served: AtomicU64,
}

/// When a try began, in clicks.
#[derive(Debug, Clone, Copy)]
struct Try(u64);

impl Clicks {
    fn click(&self) {
        self.asked.fetch_add(1, Ordering::Relaxed);
    }

    /// A try begins: clicks before it add nothing to the wait after it (it is the one asked
    /// for).
    fn begin_try(&self) -> Try {
        Try(self.asked.load(Ordering::Relaxed))
    }

    /// Whether a click came since `tried` began that no wait ended for yet; one is taken.
    fn clicked(&self, tried: Try) -> bool {
        let asked = self.asked.load(Ordering::Relaxed);
        let clicked = asked > tried.0.max(self.served.load(Ordering::Relaxed));
        if clicked {
            self.served.store(asked, Ordering::Relaxed);
        }
        clicked
    }
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
    /// It waits to connect again (a try failed): Sync now has it try at once.
    connecting: bool,
    /// Refused for want of the private drive's key ([`FolderStatus::lacks_key`]).
    lacks_key: bool,
}

impl Shown {
    /// The runners the status shows.
    fn shown(&self) -> impl Iterator<Item = &Running> {
        self.runners.iter().filter(|r| r.shows())
    }

    /// Those of them that sync the account's own folders: not what other identities share
    /// with it ([`Running::sharer`]).
    fn own(&self) -> impl Iterator<Item = &Running> {
        self.shown().filter(|r| r.sharer.is_none())
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
    /// From the notifications of what another identity shares with it.
    ShareHint(Identity, Event),
    /// The machine woke up.
    Woke,
    /// Look again: it is to end, the daemon stops, its pause changed, or a runner is ending.
    Wake,
    /// Sync now was asked before its folders run: a wait to try again (to connect, to ready its
    /// DB, to read the keychain) ends.
    SyncNow,
    /// A try at connecting ended (its thread's answer, while the account connects).
    Connected(Box<Result<HomebaseRemote, hbsync_homebase::HomebaseError>>),
    /// The runner of this folder (the main folder) opened it, and its folder of shares has
    /// this name; none: none is at its top ([`crate::runner::SharesTold`]).
    Shares(PathBuf, Option<String>),
    /// What its share markers say, or a share opened: the answer of the thread that asks
    /// ([`runners::Markers`], which the number is of).
    Sharers(u64, Box<runners::Answer>),
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
                waiting: Some(Waiting::new(vec![root.clone()], None)),
                runners: Vec::new(),
            }),
            requests,
            ended: AtomicBool::new(false),
            done: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            reader: Mutex::new(None),
            feeds: hbsync_local::Feeds::default(),
            shares_told: Arc::default(),
            changed,
            clicks: Clicks::default(),
        };
        (Arc::new(account), rx)
    }

    /// Its folders: its own, not those of what other identities share with it (they are in
    /// no config, and in DBs of their own).
    pub(super) fn folders(&self) -> Vec<Root> {
        let shown = lock(&self.shown);
        match &shown.waiting {
            Some(waiting) => waiting.roots.clone(),
            None => shown.own().map(|r| r.root.clone()).collect(),
        }
    }

    /// Its main folder, if it syncs one.
    pub(super) fn main_folder(&self) -> Option<PathBuf> {
        let shown = lock(&self.shown);
        let main = match &shown.waiting {
            Some(waiting) => waiting.roots.iter().find(|r| r.is_main()),
            None => shown.own().map(|r| &r.root).find(|r| r.is_main()),
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
        let folders: Vec<FolderStatus> = match &shown.waiting {
            // Its own first, then what others share with it.
            None => (shown
                .own()
                .chain(shown.shown().filter(|r| r.sharer.is_some())))
            .map(|r| r.handle.status())
            .collect(),
            Some(waiting) => waiting
                .roots
                .iter()
                .map(|root| waiting.status(root, shown.paused()))
                .collect(),
        };
        let mut status = shown.status.clone();
        // The app's want of the owner's approval is the account's to say, whichever share
        // met it: one that can't be opened (kept here) or one that is synced (its own).
        status.needs_approval |= folders.iter().any(|f| f.needs_approval);
        (status, folders)
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

    /// Syncs its folders now. Refused before they run, and the wait to try again (to connect,
    /// to ready its DB, to read the keychain) then ends: the user may have set it right. The
    /// refusal says it tries to connect again while it waits for that, else why it doesn't run.
    pub(super) fn sync_now(&self) -> Result<(), String> {
        let shown = lock(&self.shown);
        if let Some(waiting) = &shown.waiting {
            self.clicks.click();
            self.tell(Msg::SyncNow);
            if waiting.connecting {
                return Err(words::trying_to_connect(&self.identity));
            }
        } else {
            // (Its thread tries what waits of its shares now.)
            self.tell(Msg::SyncNow);
        }
        shown.send(&self.identity, &Command::SyncNow)
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
                            main: root.is_main(),
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
    /// Its folders `roots` wait, starting (`why`, if anything is to be told).
    fn new(roots: Vec<Root>, why: Option<String>) -> Waiting {
        Waiting {
            roots,
            state: State::Starting,
            error: why,
            connecting: false,
            lacks_key: false,
        }
    }

    /// The status of its folder `root`, `paused` or waiting.
    fn status(&self, root: &Root, paused: Option<Pause>) -> FolderStatus {
        let mut folder = FolderStatus::new(&root.identity, &root.folder);
        folder.kind = root.kind;
        folder.state = if paused.is_some() {
            State::Paused
        } else {
            self.state
        };
        folder.refused_paused = paused.is_some() && self.state == State::Denied;
        folder.paused_until = paused.and_then(|p| p.until);
        folder.error.clone_from(&self.error);
        folder.lacks_key = folder.refused() && self.lacks_key;
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
            writable: false,
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

    /// An account started on a thread as the daemon starts one: its DB readied, then synced on
    /// the parts `open` makes of it (no notifications), until stopped.
    struct Started {
        account: Arc<Account>,
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<Result<(), crate::daemon::DaemonError>>,
    }

    /// The account's drive, what other identities share with it, and what says whose shares
    /// are synced, if anything does.
    type Opened = Result<
        (
            Arc<Drive>,
            Vec<parts::Share>,
            Option<Box<dyn parts::Sharers>>,
        ),
        crate::runner::RunnerError,
    >;

    impl Started {
        fn new(
            paths: &Paths,
            root: Root,
            open: impl Fn(&Path) -> Result<Arc<Drive>, crate::runner::RunnerError> + Send + 'static,
        ) -> Started {
            Started::sharing(paths, root, move |db| Ok((open(db)?, Vec::new(), None)))
        }

        /// [`Started::new`], `open` also making what other identities share with it.
        fn sharing(
            paths: &Paths,
            root: Root,
            open: impl Fn(&Path) -> Opened + Send + 'static,
        ) -> Started {
            use super::feed::{Notifications, Puller};

            let (account, requests) = Account::new(paths, &root, None, Arc::default());
            let stop = Arc::new(AtomicBool::new(false));
            let (running, paths, stopping) =
                (Arc::clone(&account), paths.clone(), Arc::clone(&stop));
            let thread = std::thread::spawn(move || {
                let mut msgs = Vec::new();
                let roots = [root];
                let start = super::run::Readying::Start(&roots);
                running.ready_db(&paths, start, &stopping, &requests, &mut msgs)?;
                let config = crate::Config::default();
                running.sync(&paths, &config, &requests, &stopping, &mut msgs, |db| {
                    let (drive, shares, sharers) = open(db)?;
                    let puller = Puller::spawn(Arc::clone(&drive));
                    Ok(AccountParts {
                        drive,
                        notifier: Err(std::io::Error::other("none here")),
                        notifications: Notifications::new(puller, std::time::Instant::now()),
                        shares,
                        sharers,
                    })
                })
            });
            Started {
                account,
                stop,
                thread,
            }
        }

        /// Waits up to 30 s for its first folder to be `done`.
        fn wait(
            &self,
            what: &str,
            done: &dyn Fn(&AccountStatus, &FolderStatus) -> bool,
        ) -> Result<(), String> {
            self.wait_all(what, &|status, folders| {
                folders.first().is_some_and(|f| done(status, f))
            })
        }

        /// Waits up to 30 s for its folders to be `done`.
        fn wait_all(
            &self,
            what: &str,
            done: &dyn Fn(&AccountStatus, &[FolderStatus]) -> bool,
        ) -> Result<(), String> {
            let seen =
                |(status, folders): &(AccountStatus, Vec<FolderStatus>)| done(status, folders);
            match wait_until(|| Some(self.account.status()).filter(seen)) {
                Some(_) => Ok(()),
                None => Err(format!("timed out: {what}: {:?}", self.account.status())),
            }
        }

        /// How often it told that its DB was set aside for `why`.
        fn told(&self, why: hbsync_state::Unusable) -> usize {
            self.said(&words::set_aside(why))
        }

        /// How many of its notes say `text`.
        fn said(&self, text: &str) -> usize {
            let (status, _) = self.account.status();
            status.recent.iter().filter(|n| n.text == text).count()
        }

        /// Stops it, whatever `checked` found: nothing runs on into the other tests.
        fn stop<T>(self, checked: T) -> Result<T, Box<dyn std::error::Error>> {
            self.stop.store(true, Ordering::SeqCst);
            self.account.wake();
            self.thread.join().map_err(|_| "panicked")??;
            Ok(checked)
        }
    }

    /// What `found` gives, asked every 20 ms for up to 30 s; `None` when it never did.
    fn wait_until<T>(mut found: impl FnMut() -> Option<T>) -> Option<T> {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match found() {
                None if std::time::Instant::now() < until => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                found => return found,
            }
        }
    }

    /// Its folder synced, idle, nothing pending.
    fn synced(_: &AccountStatus, f: &FolderStatus) -> bool {
        f.state == State::Idle && f.last_sync.is_some() && f.pending == 0
    }

    /// Both its folders are ([`synced`]): its own, and what another identity shares with it.
    fn both_synced(status: &AccountStatus, folders: &[FolderStatus]) -> bool {
        folders.len() == 2 && folders.iter().all(|f| synced(status, f))
    }

    /// How many DBs were set aside beside `db` for `why`.
    fn set_aside(db: &Path, why: hbsync_state::Unusable) -> std::io::Result<usize> {
        let prefix = format!("state.db.{}-", why.name());
        let dir = std::fs::read_dir(db.parent().unwrap_or(db))?;
        Ok(dir
            .flatten()
            .filter(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with(&prefix) && !name.ends_with("-wal") && !name.ends_with("-shm")
            })
            .count())
    }

    /// A home with the main folder of `identity` set up in its DB, holding `files`: its paths,
    /// the folder and the DB.
    fn set_up(
        home: &Path,
        identity: &Identity,
        files: &[(&str, &str)],
    ) -> Result<(Paths, PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let paths = Paths::under(home);
        let folder = home.join("Homebase");
        for (name, text) in files {
            let path = folder.join(name);
            std::fs::create_dir_all(path.parent().ok_or("no parent")?)?;
            std::fs::write(path, text)?;
        }
        let db = paths.account_db(identity);
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        hbsync_engine::root::SyncRoot::create(&db, identity, &folder)?;
        Ok((paths, folder, db))
    }

    /// What another identity shares with the account is synced by a runner of its own, in a
    /// folder named after the sharer in the main folder's folder of shares, which the main
    /// folder's runner makes and leaves alone: one way, from the sharer's drive, on a DB of
    /// its own. Nothing of it reaches the account's drive; a file changed there moves out to
    /// the main folder's top and the sharer's comes again. It follows the folder of shares
    /// when that one is renamed, and is there as it was after a stop and a start.
    #[test]
    fn what_another_identity_shares_is_synced_into_the_folder_of_shares()
    -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::contract::Shares as _;
        use std::fs::read_to_string as read;

        let (me, tom): (Identity, Identity) = ("me.example".into(), "tom.example".into());
        // The sharer, syncing its own folder with its own drive.
        let toms = tempfile::tempdir()?;
        let files = [("notes.txt", "from tom"), ("docs/plan.txt", "the plan")];
        let (tom_paths, tom_folder, _) = set_up(toms.path(), &tom, &files)?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let tom_remote = FakeRemote::new(tom_server.clone());
        let (remote, sharer) = (tom_remote.clone(), tom.clone());
        let tom_root = Root::main(&tom, &tom_folder);
        let theirs = Started::new(&tom_paths, tom_root, move |db| {
            Ok(Drive::open(db, &sharer, Arc::new(remote.clone()))?)
        });
        // Shares what is named so on its drive, once it is there.
        let share = |names: &[&str]| -> Result<(), Box<dyn std::error::Error>> {
            for name in names {
                let named = |h: &&hbsync_remote::RemoteHeader| {
                    matches!(&h.body, hbsync_remote::RemoteBody::Live { content, .. }
                        if content.name == *name)
                };
                let on_drive = || {
                    let server = tom_server.lock().ok()?;
                    server.live().find(named).map(|h| h.file_id)
                };
                let id = wait_until(on_drive)
                    .ok_or_else(|| format!("{name} never reached the sharer's drive"))?;
                tom_remote.share(id)?;
            }
            tom_server.lock().map_err(|_| "poisoned")?.tick();
            Ok(())
        };

        // The account, with what the sharer shares read as its member reads it.
        let home = tempfile::tempdir()?;
        let (paths, folder, server, start) = shared_with(home.path(), &tom_server)?;
        let shared_db = paths.shared_db(&me, &tom);
        let both = both_synced;
        let own_names = || -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let mut names = server.lock().map_err(|_| "poisoned")?.live_names();
            names.sort();
            Ok(names)
        };

        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            theirs.wait("the sharer's first sync", &synced)?;
            // Its folder is there before anything is shared, in the folder of shares the main
            // folder's runner made.
            let at = folder.join("Shared").join("tom.example");
            started.wait_all("the share's folder is there", &|s, folders| {
                both(s, folders) && at.is_dir()
            })?;
            assert_eq!(hbsync_local::shares::folders(&folder)?, ["Shared"]);
            share(&["notes.txt", "docs", "plan.txt"])?;
            // (Read on a timer until its changes are announced: asked for now.)
            let _ = started.account.sync_now();
            started.wait_all("the share is here", &|s, folders| {
                both(s, folders)
                    && at.join("docs/plan.txt").exists()
                    && at.join("notes.txt").exists()
            })?;
            assert_eq!(read(at.join("notes.txt"))?, "from tom");
            assert_eq!(read(at.join("docs/plan.txt"))?, "the plan");

            // Shown after the account's own, as the sharer's: never its main folder.
            let (_, folders) = started.account.status();
            assert_eq!(folders[0].path, folder);
            assert!(folders[0].is_main() && folders[0].sharer.is_none());
            assert_eq!(folders[1].path, at);
            assert_eq!(folders[1].sharer.as_ref(), Some(&tom));
            assert!(!folders[1].is_main());
            assert_eq!(started.account.main_folder().as_deref(), Some(&*folder));
            let own = started.account.folders();
            assert_eq!(own.len(), 1, "{own:?}");
            // Nothing of it on the account's drive; nothing changed on the sharer's.
            assert_eq!(own_names()?, ["mine.txt"]);
            assert!(shared_db.exists());

            // Changed here: the change moves out to the main folder's top (the account's own
            // from then on), and the sharer's comes again. (Seen at once: the share's folder
            // is watched, by the main folder's watcher (filesync #414); Sync now walks
            // nothing, and a folder without a watcher is walked once a minute.)
            // (Placed read-only: saved over as a user who means to does.)
            hbsync_platform::ops::set_read_only(&at.join("notes.txt"), false)?;
            std::fs::write(at.join("notes.txt"), "changed here")?;
            let _ = started.account.sync_now();
            let moved_out = || {
                let top = std::fs::read_dir(&folder).ok()?;
                top.flatten()
                    .map(|e| e.path())
                    .find(|p| p.is_file() && read(p).is_ok_and(|text| text == "changed here"))
            };
            started.wait_all("the change moved out", &|s, folders| {
                both(s, folders)
                    && moved_out().is_some()
                    && read(at.join("notes.txt")).is_ok_and(|text| text == "from tom")
            })?;
            let out = moved_out().ok_or("moved out")?;
            let out = out
                .file_name()
                .ok_or("no name")?
                .to_string_lossy()
                .into_owned();
            let _ = started.account.sync_now();
            started.wait_all("what moved out is the account's own", &|s, folders| {
                both(s, folders) && own_names().is_ok_and(|names| names.contains(&out))
            })?;
            assert_eq!(own_names()?.len(), 2, "{:?}", own_names()?);

            // The folder of shares renamed while the share syncs (on Windows too: nothing
            // is held open below it): the share is synced there from then on.
            rename_synced(&folder.join("Shared"), &folder.join("From others"))?;
            let at = folder.join("From others").join("tom.example");
            let _ = started.account.sync_now();
            started.wait_all("the share follows the folder of shares", &|s, folders| {
                both(s, folders) && folders[1].path == at
            })?;
            Ok(())
        };
        let checked = checks();
        started.stop(checked)??;

        // Stopped and started: as it was, where the folder of shares is now, and what the
        // sharer shares from now on comes there.
        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            let at = folder.join("From others").join("tom.example");
            started.wait_all("both are synced again", &|s, folders| {
                both(s, folders) && folders[1].path == at
            })?;
            assert_eq!(read(at.join("notes.txt"))?, "from tom");
            assert_eq!(read(at.join("docs/plan.txt"))?, "the plan");
            std::fs::write(tom_folder.join("later.txt"), "later")?;
            let _ = theirs.account.sync_now();
            share(&["later.txt"])?;
            let _ = started.account.sync_now();
            started.wait_all("what the sharer shares later comes there", &|s, folders| {
                both(s, folders) && at.join("later.txt").exists()
            })?;
            assert_eq!(read(at.join("later.txt"))?, "later");
            assert!(!folder.join("Shared").exists());
            assert_eq!(own_names()?.len(), 2, "{:?}", own_names()?);
            let removed = tom_server.lock().map_err(|_| "poisoned")?.removed().count();
            assert_eq!(removed, 0, "the sharer's drive is as it was");
            Ok(())
        };
        let checked = checks();
        started.stop(checked)??;
        theirs.stop(())?;
        Ok(())
    }

    /// A folder at a share's place that holds a marker the share's DB doesn't know (another
    /// folder renamed into its place, say) is never taken over: the share's sync fails,
    /// saying so, and what the folder holds stays where it is, nothing of it moved out or
    /// sent.
    #[test]
    fn a_folder_with_a_marker_of_its_own_at_a_shares_place_is_never_taken_over()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, server, start) = shared_with(home.path(), &tom_server)?;
        use hbsync_local::shares::{Made, make};
        assert_eq!(make(&folder, "Shared")?, Made::Made);
        let at = folder.join("Shared").join("tom.example");
        std::fs::create_dir(&at)?;
        std::fs::write(at.join("theirs.txt"), "another folder's")?;
        let other = home.path().join("other.db");
        hbsync_engine::root::SyncRoot::create(&other, "me.example", &at)?;
        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("the share fails, the account's own syncs", &|s, folders| {
                folders.len() == 2
                    && synced(s, &folders[0])
                    && folders[1].state == State::Failing
                    && folders[1].error.is_some()
            })?;
            assert_eq!(
                std::fs::read_to_string(at.join("theirs.txt"))?,
                "another folder's"
            );
            assert!(!folder.join("theirs.txt").exists(), "moved out");
            assert_eq!(
                server.lock().map_err(|_| "poisoned")?.live_names(),
                ["mine.txt"]
            );
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// An account `me.example` on a new drive whose main folder holds `mine.txt`, with what
    /// `tom.example` shares of the drive on `tom_server`, read as its member reads it. Its
    /// paths, its folder, its drive's server, and how to start it.
    #[allow(clippy::type_complexity)]
    fn shared_with(
        home: &Path,
        tom_server: &Arc<std::sync::Mutex<FakeServer>>,
    ) -> Result<
        (
            Paths,
            PathBuf,
            Arc<std::sync::Mutex<FakeServer>>,
            impl Fn() -> Started,
        ),
        Box<dyn std::error::Error>,
    > {
        let (me, tom): (Identity, Identity) = ("me.example".into(), "tom.example".into());
        let (paths, folder, _) = set_up(home, &me, &[("mine.txt", "mine")])?;
        let server = FakeServer::new(ServerConfig::default());
        let shared_db = paths.shared_db(&me, &tom);
        std::fs::create_dir_all(shared_db.parent().ok_or("no parent")?)?;
        let (starting, root) = (paths.clone(), Root::main(&me, &folder));
        let (own, theirs) = (server.clone(), tom_server.clone());
        let start = move || {
            let (server, tom_server) = (own.clone(), theirs.clone());
            let (me, tom, shared_db) = (me.clone(), tom.clone(), shared_db.clone());
            Started::sharing(&starting, root.clone(), move |db| {
                let drive = Drive::open(db, &me, Arc::new(FakeRemote::new(server.clone())))?;
                let share = open_share(&me, &tom, &shared_db, &tom_server)?;
                Ok((drive, vec![share], None))
            })
        };
        Ok((paths, folder, server, start))
    }

    /// What `sharer` shares of the drive on `server`, as its member `me` reads it, on the
    /// state DB `db`.
    fn open_share(
        me: &Identity,
        sharer: &Identity,
        db: &Path,
        server: &Arc<std::sync::Mutex<FakeServer>>,
    ) -> Result<parts::Share, crate::runner::RunnerError> {
        let member = Arc::new(FakeRemote::member(server.clone()));
        let drive = Drive::open(db, me, member)?;
        Ok(parts::Share::new(sharer, drive, db.to_path_buf()))
    }

    /// An identity's share markers, as a test sets them.
    #[derive(Debug, Default)]
    struct TestMarkers {
        /// Whose shares to sync.
        on: Vec<Identity>,
        /// Whose marker doesn't read.
        unsure: Vec<Identity>,
        /// None of them reads.
        unreadable: bool,
        /// Whose share can't be opened.
        unopenable: Vec<Identity>,
        /// Why not: the app wants the owner's approval to read what others share (else, a
        /// failure of its own).
        unapproved: bool,
        /// How often they were read, and how often a share was tried to open.
        reads: usize,
        opens: usize,
        /// How often a share's notifications were started.
        listens: usize,
        /// Whose share was opened, ever (its state DB is this computer's from then on).
        opened: Vec<Identity>,
        /// How long after a read that says a share is off a second one confirms it (none:
        /// the first read does), as the account starts.
        confirm: std::time::Duration,
    }

    /// `me.example`'s shares by its [`TestMarkers`]: each sharer's drive is the one on
    /// `server` (nothing on it), read as its member reads it.
    #[derive(Debug)]
    struct FakeSharers {
        markers: Arc<Mutex<TestMarkers>>,
        me: Identity,
        paths: Paths,
        server: Arc<std::sync::Mutex<FakeServer>>,
    }

    impl parts::Sharers for FakeSharers {
        fn told(&self, synced: &[Identity]) -> Result<parts::Synced, crate::runner::RunnerError> {
            let mut markers = lock(&self.markers);
            markers.reads += 1;
            if markers.unreadable {
                return Err(std::io::Error::other("the markers don't read").into());
            }
            // (Those it opened whose state DB is still there.)
            let kept = |s: &&Identity| self.paths.shared_db(&self.me, s).exists();
            let kept: Vec<Identity> = markers.opened.iter().filter(kept).cloned().collect();
            let unsure = |s: &&Identity| synced.contains(s) || kept.contains(s);
            Ok(parts::Synced {
                on: markers.on.clone(),
                unsure: markers.unsure.iter().filter(unsure).cloned().collect(),
                kept,
            })
        }

        fn open(
            &self,
            sharer: &Identity,
            to_end: bool,
        ) -> Result<parts::Share, crate::runner::RunnerError> {
            {
                let mut markers = lock(&self.markers);
                markers.opens += 1;
                if !markers.opened.contains(sharer) {
                    markers.opened.push(sharer.clone());
                }
                // (What ended is opened without its sharer.)
                if !to_end && markers.unopenable.contains(sharer) {
                    if markers.unapproved {
                        let why = "the owner hasn't approved it".to_owned();
                        let refused = hbsync_remote::Refusal::Permission;
                        let e = hbsync_remote::RemoteError::Denied(why, refused);
                        return Err(hbsync_engine::EngineError::Remote(e).into());
                    }
                    return Err(std::io::Error::other("no token yet").into());
                }
            }
            let db = self.paths.shared_db(&self.me, sharer);
            std::fs::create_dir_all(db.parent().unwrap_or(&db))?;
            if to_end {
                // As the daemon's own: on a drive that answers nothing.
                let ended = Arc::new(sharers::Ended(sharer.clone()));
                let drive = Drive::open(&db, &self.me, ended)?;
                return Ok(parts::Share::new(sharer, drive, db));
            }
            let markers = self.markers.clone();
            let share = open_share(&self.me, sharer, &db, &self.server)?;
            // (None to start here: only that the account asks for them is counted.)
            Ok(share.hearing(move || {
                lock(&markers).listens += 1;
                None
            }))
        }

        fn confirm_after(&self) -> std::time::Duration {
            lock(&self.markers).confirm
        }
    }

    /// `me.example` on a new drive, its main folder holding `mine.txt`, whose shares follow
    /// the markers returned: its folder, and the account started. [`Marked::marked`] sets
    /// them and tells the account that one changed.
    struct Marked {
        folder: PathBuf,
        markers: Arc<Mutex<TestMarkers>>,
        started: Started,
        /// What it starts with, for [`Marked::again`].
        with: (Identity, Paths, [Arc<std::sync::Mutex<FakeServer>>; 2]),
    }

    impl Marked {
        fn start(
            home: &Path,
            set: impl FnOnce(&mut TestMarkers),
        ) -> Result<Marked, Box<dyn std::error::Error>> {
            let me: Identity = "me.example".into();
            let (paths, folder, _) = set_up(home, &me, &[("mine.txt", "mine")])?;
            let servers = [(); 2].map(|()| FakeServer::new(ServerConfig::default()));
            let markers: Arc<Mutex<TestMarkers>> = Arc::default();
            set(&mut lock(&markers));
            let with = (me, paths, servers);
            let started = Marked::account(&with, &folder, &markers);
            Ok(Marked {
                folder,
                markers,
                started,
                with,
            })
        }

        /// The account, started: its drive on the first server, its sharers' on the second.
        fn account(
            (me, paths, [server, tom_server]): &(
                Identity,
                Paths,
                [Arc<std::sync::Mutex<FakeServer>>; 2],
            ),
            folder: &Path,
            markers: &Arc<Mutex<TestMarkers>>,
        ) -> Started {
            let (me, paths, told) = (me.clone(), paths.clone(), markers.clone());
            let (server, tom_server) = (server.clone(), tom_server.clone());
            let (starting, root) = (paths.clone(), Root::main(&me, folder));
            Started::sharing(&starting, root, move |db| {
                let drive = Drive::open(db, &me, Arc::new(FakeRemote::new(server.clone())))?;
                let by = FakeSharers {
                    markers: told.clone(),
                    me: me.clone(),
                    paths: paths.clone(),
                    server: tom_server.clone(),
                };
                Ok((
                    drive,
                    Vec::new(),
                    Some(Box::new(by) as Box<dyn parts::Sharers>),
                ))
            })
        }

        /// The account stopped, `meanwhile` done to its folder and markers while nothing
        /// runs, and the account started again (the daemon stopped and started: the same
        /// folder, state and markers).
        fn again(
            self,
            meanwhile: impl FnOnce(&Path, &mut TestMarkers) -> std::io::Result<()>,
        ) -> Result<Marked, Box<dyn std::error::Error>> {
            let Marked {
                folder,
                markers,
                started,
                with,
            } = self;
            started.stop(())?;
            meanwhile(&folder, &mut lock(&markers))?;
            let started = Marked::account(&with, &folder, &markers);
            Ok(Marked {
                folder,
                markers,
                started,
                with,
            })
        }

        /// Sets the markers, and tells the account one changed, as its notifications do.
        fn marked(&self, set: impl FnOnce(&mut TestMarkers)) {
            set(&mut lock(&self.markers));
            let changed = hbsync_homebase::notify::Event::Markers;
            self.started.account.tell(Msg::Hint(changed));
        }

        /// Waits until the markers were read once more than `before`.
        fn read_since(&self, before: usize) -> Result<(), String> {
            let read = || Some(()).filter(|()| lock(&self.markers).reads > before);
            wait_until(read).ok_or_else(|| "the markers weren't read again".to_owned())
        }

        fn reads(&self) -> usize {
            lock(&self.markers).reads
        }

        /// The state DB of what `sharer` shares with it.
        fn records_of(&self, sharer: &Identity) -> PathBuf {
            let (me, paths, _) = &self.with;
            paths.shared_db(me, sharer)
        }

        /// Where what `tom.example` shares is synced.
        fn toms(&self) -> PathBuf {
            self.folder.join("Shared").join("tom.example")
        }
    }

    /// The account's own folder synced alone: no share is.
    fn alone(status: &AccountStatus, folders: &[FolderStatus]) -> bool {
        folders.len() == 1 && synced(status, &folders[0])
    }

    /// Whose shares are synced follows the identity's share markers: none while none says
    /// so (no folder of shares is made); one turned on is synced into the folder of shares,
    /// made for it (the main folder's runner told to hold one); turned off, its runner
    /// stops and its copy is taken off this computer, with its records; turned on again, it
    /// is synced again.
    #[test]
    fn a_share_is_synced_while_its_marker_says_so() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let f = Marked::start(home.path(), |_| {})?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            f.read_since(0)?;
            assert!(
                !f.folder.join("Shared").exists(),
                "no share, no folder of shares"
            );

            f.marked(|m| m.on = vec![tom.clone()]);
            f.started.wait_all("the share is synced", &both_synced)?;
            assert!(f.toms().is_dir());
            let (_, folders) = f.started.account.status();
            assert_eq!(folders[1].sharer.as_ref(), Some(&tom));

            // Turned off: its runner stops, and its copy is taken off this computer, with
            // its records. Said in a note.
            let shared_db = f.records_of(&tom);
            assert!(shared_db.exists());
            f.marked(|m| m.on.clear());
            f.started
                .wait_all("the share is no longer synced", &alone)?;
            let gone = || Some(()).filter(|()| !f.toms().exists() && !shared_db.exists());
            wait_until(gone).ok_or("its copy stays")?;
            let said = words::share_ended(&tom, &f.toms(), &f.folder, (0, 0), (None, false));
            f.started
                .wait_all("said", &|_, _| f.started.said(&said) == 1)?;
            assert!(
                f.folder.join("Shared").is_dir(),
                "the folder of shares stays"
            );

            f.marked(|m| m.on = vec![tom.clone()]);
            f.started
                .wait_all("the share is synced again", &both_synced)?;
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share its marker turns off is off once a second read of the markers, after the
    /// time its source names, says so too: the account reads them again by itself, and only
    /// then takes the copy off this computer.
    #[test]
    fn a_share_is_turned_off_by_a_second_read_of_the_markers()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let confirm = std::time::Duration::from_millis(600);
        let f = Marked::start(home.path(), |m| {
            m.on = vec![tom.clone()];
            m.confirm = confirm;
        })?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("the share is synced", &both_synced)?;
            let before = lock(&f.markers).reads;
            let off_at = std::time::Instant::now();
            f.marked(|m| m.on.clear());
            // The read that says so first: the share is synced on.
            f.read_since(before)?;
            let (_, folders) = f.started.account.status();
            if off_at.elapsed() < confirm / 2 {
                assert_eq!(folders.len(), 2, "one read turned it off");
                assert!(f.toms().is_dir());
            }
            // No change is told again: the second read is the account's own.
            f.started
                .wait_all("the share is no longer synced", &alone)?;
            assert!(off_at.elapsed() >= confirm, "off before a second read");
            assert!(lock(&f.markers).reads >= before + 2);
            let gone = || Some(()).filter(|()| !f.toms().exists());
            wait_until(gone).ok_or("its copy stays")?;
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share turned off while the daemon didn't run has its copy taken off this computer
    /// as it starts (it is known by its records here): what was put into its folder
    /// meanwhile isn't the sharer's, and moves out to the main folder; a link there is
    /// moved, not followed.
    #[test]
    fn a_share_turned_off_while_stopped_has_its_copy_removed_at_the_start()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let f = Marked::start(home.path(), |m| m.on = vec!["tom.example".into()])?;
        let synced = f.started.wait_all("the share is synced", &both_synced);
        let elsewhere = tempfile::tempdir()?;
        std::fs::write(elsewhere.path().join("kept.txt"), "not ours")?;
        let f = f.again(|folder, markers| {
            let toms = folder.join("Shared").join("tom.example");
            std::fs::write(toms.join("mine too.txt"), "put here while stopped")?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(elsewhere.path(), toms.join("link"))?;
            markers.on.clear();
            Ok(())
        })?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            synced?;
            let gone = || Some(()).filter(|()| !f.toms().exists());
            wait_until(gone).ok_or("its copy stays")?;
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            // What was put there is in the main folder now, the link as a link.
            let out = f.folder.join("mine too.txt");
            assert_eq!(std::fs::read_to_string(out)?, "put here while stopped");
            let moved = 1 + usize::from(cfg!(unix));
            let said = words::share_ended(&tom, &f.toms(), &f.folder, (moved, 0), (None, false));
            f.started
                .wait_all("said", &|_, _| f.started.said(&said) == 1)?;
            #[cfg(unix)]
            assert!(f.folder.join("link").symlink_metadata()?.is_symlink());
            assert_eq!(
                std::fs::read_to_string(elsewhere.path().join("kept.txt"))?,
                "not ours"
            );
            assert!(!f.records_of(&tom).exists());
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share synced before whose marker doesn't read as the daemon starts keeps its copy:
    /// not synced (nothing says so), and never taken for turned off. Once its marker reads,
    /// and says no, the copy goes.
    #[test]
    fn a_share_kept_from_before_whose_marker_doesnt_read_keeps_its_copy()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let f = Marked::start(home.path(), |m| m.on = vec!["tom.example".into()])?;
        let synced = f.started.wait_all("the share is synced", &both_synced);
        let mut read_before = 0;
        let f = f.again(|_, markers| {
            (markers.on, markers.unsure) = (Vec::new(), vec!["tom.example".into()]);
            read_before = markers.reads;
            Ok(())
        })?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            synced?;
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            f.read_since(read_before)?;
            std::thread::sleep(std::time::Duration::from_millis(500));
            assert!(f.toms().is_dir() && f.records_of(&tom).exists());
            let opens = lock(&f.markers).opens;
            assert_eq!(opens, 1, "not opened to end either");

            f.marked(|m| m.unsure.clear());
            let gone = || Some(()).filter(|()| !f.toms().exists());
            wait_until(gone).ok_or("its copy stays")?;
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share whose marker doesn't read stays as it is, synced, and so does every share
    /// while the markers can't be read at all: never taken for turned off.
    #[test]
    fn a_share_the_markers_dont_say_of_stays_synced() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let f = Marked::start(home.path(), |m| m.on = vec!["tom.example".into()])?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("the share is synced", &both_synced)?;

            // Its marker doesn't read, while another's turns its share on: once that one
            // is synced, the answer was followed, and tom's is as it was.
            let sam: Identity = "sam.example".into();
            f.marked(|m| (m.on, m.unsure) = (vec![sam.clone()], vec![tom.clone()]));
            let three = |status: &AccountStatus, folders: &[FolderStatus]| {
                folders.len() == 3 && folders.iter().all(|f| synced(status, f))
            };
            f.started
                .wait_all("another's share is synced too", &three)?;
            let (_, folders) = f.started.account.status();
            let whose: Vec<_> = folders.iter().filter_map(|f| f.sharer.clone()).collect();
            assert!(whose.contains(&tom) && whose.contains(&sam), "{whose:?}");

            let before = f.reads();
            f.marked(|m| (m.unsure, m.unreadable) = (Vec::new(), true));
            f.read_since(before)?;
            std::thread::sleep(std::time::Duration::from_millis(300));
            f.started.wait_all("still synced: no marker read", &three)?;

            // They read again, and say no for both: now they stop.
            f.marked(|m| (m.on, m.unreadable) = (Vec::new(), false));
            f.started
                .wait_all("the share is no longer synced", &alone)?;
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// While the account is paused its markers aren't read (nothing is asked of the identity
    /// then): a change of theirs is followed at the resume.
    #[test]
    fn the_markers_arent_read_while_paused() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let f = Marked::start(home.path(), |_| {})?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            f.read_since(0)?;

            f.started.account.pause(Some(Pause::UNTIL_RESUMED));
            let before = f.reads();
            f.marked(|m| m.on = vec!["tom.example".into()]);
            std::thread::sleep(std::time::Duration::from_millis(500));
            assert_eq!(f.reads(), before, "read while paused");
            assert!(!f.folder.join("Shared").exists());

            f.started.account.pause(None);
            f.started.wait_all("the share is synced", &both_synced)?;
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share that can't be opened is left out, said once, and tried again by itself; the
    /// account's own folder syncs on meanwhile.
    #[test]
    fn a_share_that_cant_be_opened_is_left_out_and_tried_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom: Identity = "tom.example".into();
        let f = Marked::start(home.path(), |m| {
            (m.on, m.unopenable) = (vec!["tom.example".into()], vec!["tom.example".into()]);
        })?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            let said = words::share_unopened(&tom, &"no token yet");
            f.started
                .wait_all("said why", &|_, _| f.started.said(&said) == 1)?;
            assert!(!f.folder.join("Shared").exists());
            // Not for want of the owner's approval: none is asked for (looked at once it
            // was tried a second time: what the first try found has been told by then).
            f.started.account.sync_now()?;
            f.started
                .wait_all("tried again", &|_, _| lock(&f.markers).opens >= 2)?;
            assert!(!f.started.account.status().0.needs_approval);

            // It can be opened: tried again by itself (no marker changed), and synced.
            lock(&f.markers).unopenable.clear();
            f.started.wait_all("the share is synced", &both_synced)?;
            assert_eq!(f.started.said(&said), 1, "said once");
            assert!(
                lock(&f.markers).opens >= 3,
                "by itself, after the two that failed"
            );
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share that can't be opened because the app wants the owner's approval (to read what
    /// others share) has no folder to say so: the account's status does, so the approval can
    /// be asked for; opened once it is given, it no longer does.
    #[test]
    fn a_share_unopened_for_want_of_the_approval_is_told_by_the_account()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let f = Marked::start(home.path(), |m| {
            (m.on, m.unopenable) = (vec!["tom.example".into()], vec!["tom.example".into()]);
            m.unapproved = true;
        })?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("its own folder syncs, alone", &alone)?;
            let wanted = |status: &AccountStatus, _: &[FolderStatus]| status.needs_approval;
            f.started.wait_all("the approval is wanted", &wanted)?;
            // Given: Sync now (what the approval sends) tries it at once, and opens it.
            lock(&f.markers).unopenable.clear();
            f.started.account.sync_now()?;
            f.started.wait_all("the share is synced", &both_synced)?;
            let (account, _) = f.started.account.status();
            assert!(!account.needs_approval);
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// A share whose folder can't be set up (something else has its place in the folder of
    /// shares) doesn't keep the account from syncing its own: it is tried again after a pause,
    /// and syncs once its place is free.
    #[test]
    fn a_share_that_cant_start_is_tried_again_and_the_account_syncs_on()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, _, start) = shared_with(home.path(), &tom_server)?;
        use hbsync_local::shares::{Made, make};
        assert_eq!(make(&folder, "Shared")?, Made::Made);
        let at = folder.join("Shared").join("tom.example");
        std::fs::write(&at, "in the way")?;
        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait("its own folder syncs", &synced)?;
            std::thread::sleep(std::time::Duration::from_millis(200));
            let (_, folders) = started.account.status();
            assert_eq!(folders.len(), 1, "{folders:?}");
            assert!(at.is_file());

            std::fs::remove_file(&at)?;
            started.wait_all("the share syncs once its place is free", &both_synced)?;
            assert!(at.is_dir());
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// A link to a folder at a share's place isn't the share's folder: what it leads to is
    /// left as it is, nothing of it synced or moved.
    #[cfg(unix)]
    #[test]
    fn a_link_at_a_shares_place_is_never_synced_into() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, server, start) = shared_with(home.path(), &tom_server)?;
        use hbsync_local::shares::{Made, make};
        assert_eq!(make(&folder, "Shared")?, Made::Made);
        let elsewhere = home.path().join("elsewhere");
        std::fs::create_dir(&elsewhere)?;
        std::fs::write(elsewhere.join("private.txt"), "not the share's")?;
        let at = folder.join("Shared").join("tom.example");
        std::os::unix::fs::symlink(&elsewhere, &at)?;
        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait("its own folder syncs", &synced)?;
            std::thread::sleep(std::time::Duration::from_millis(300));
            let (_, folders) = started.account.status();
            assert_eq!(folders.len(), 1, "{folders:?}");
            assert_eq!(
                std::fs::read_to_string(elsewhere.join("private.txt"))?,
                "not the share's"
            );
            assert!(!elsewhere.join(".hbsync").exists());
            assert_eq!(
                server.lock().map_err(|_| "poisoned")?.live_names(),
                ["mine.txt"]
            );
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// A share's folder deleted while it is synced is made again, in the folder of shares,
    /// and synced anew (its runner ends for it, and its account starts it again).
    #[test]
    fn a_shares_folder_deleted_is_made_again() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, _, start) = shared_with(home.path(), &tom_server)?;
        let started = start();
        let at = folder.join("Shared").join("tom.example");
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("both sync", &both_synced)?;
            remove_synced(&at)?;
            let _ = started.account.sync_now();
            started.wait_all(
                "the share's folder is gone for its runner",
                &|_, folders| {
                    folders.get(1).is_some_and(|f| f.state == State::Failing) && !at.exists()
                },
            )?;
            started.wait_all("made again and synced", &|s, folders| {
                both_synced(s, folders) && at.join(".hbsync").is_dir()
            })?;
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// A share's notifications are started as the account takes the share in (their events
    /// come to it from then on), once.
    #[test]
    fn a_shares_notifications_start_as_it_is_taken_in() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let f = Marked::start(home.path(), |m| m.on = vec!["tom.example".into()])?;
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            f.started.wait_all("the share is synced", &both_synced)?;
            assert_eq!(lock(&f.markers).listens, 1);
            Ok(())
        };
        let checked = checks();
        f.started.stop(checked)?
    }

    /// What a sharer's server announces is taken as it is announced, by the share's own
    /// notifications: the change is in the share's folder within seconds, not at the next
    /// read on its timer (five minutes).
    #[test]
    fn a_sharers_notice_brings_its_change_in_at_once() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::contract::Shares;
        use hbsync_remote::{ChangeRef, Remote};

        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, _, start) = shared_with(home.path(), &tom_server)?;
        let started = start();
        let tom: Identity = "tom.example".into();
        let at = folder.join("Shared").join("tom.example");
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("both sync", &both_synced)?;
            let owner = FakeRemote::new(tom_server.clone());
            let plans = hbsync_remote::contract::folder("Plans", hbsync_types::NodeId::ROOT);
            let plans = owner.create(&plans, None)?;
            owner.share(plans.at().file_id)?;
            let notice = ChangeRef::of(&owner.header(plans.at().file_id)?);
            let said = Event::Changed(Some(notice));
            started.account.tell(Msg::ShareHint(tom.clone(), said));
            wait_until(|| at.join("Plans").is_dir().then_some(()))
                .ok_or("the change announced didn't come in")?;
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// The main folder's folder of shares, as its status shows it.
    fn shares_shown(folders: &[FolderStatus]) -> Option<crate::protocol::SharesFolder> {
        folders.iter().find(|f| f.is_main())?.shares.clone()
    }

    /// The folder of shares `name`, as a status shows it.
    fn shares_at(name: &str) -> Option<crate::protocol::SharesFolder> {
        Some(crate::protocol::SharesFolder::At { name: name.into() })
    }

    /// Renames the folder `from` while something is synced into it. (A file in it may be
    /// open for a moment, which Windows refuses a rename for.)
    fn rename_synced(from: &Path, to: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let renamed = || std::fs::rename(from, to).ok();
        Ok(wait_until(renamed).ok_or_else(|| format!("{} can't be renamed", from.display()))?)
    }

    /// Removes the folder `dir` while something is synced into it. (On Windows a folder a
    /// runner is in may take a moment to go.)
    fn remove_synced(dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let gone = || (std::fs::remove_dir_all(dir).is_ok() || !dir.exists()).then_some(());
        Ok(wait_until(gone).ok_or_else(|| format!("{} can't be deleted", dir.display()))?)
    }

    /// The names of what is live on `server`'s drive, sorted.
    fn drive_names(
        server: &Arc<std::sync::Mutex<FakeServer>>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(server.lock().map_err(|_| "poisoned")?.live_names())
    }

    /// Removes the main folder `folder`'s folder of shares `Shared`, and waits till the
    /// account shows its shares off.
    fn shares_gone(started: &Started, folder: &Path) -> Result<(), Box<dyn std::error::Error>> {
        remove_synced(&folder.join("Shared"))?;
        let _ = started.account.sync_now();
        Ok(started.wait_all("shares are off", &|_, folders| {
            shares_shown(folders) == Some(crate::protocol::SharesFolder::Gone)
        })?)
    }

    /// The folder of shares gone from the main folder: shares are off, said in the main
    /// folder's status, and none is made in its place by itself (it may be back). On the
    /// user's word one is made again, and what is shared is synced into it anew. Refused
    /// while one is there.
    #[test]
    fn shares_are_off_while_their_folder_is_gone_and_on_again_once_one_is_made()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::runner::SharesAsk;

        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, server, start) = shared_with(home.path(), &tom_server)?;
        let started = start();
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("both sync, the folder of shares shown", &|s, folders| {
                both_synced(s, folders) && shares_shown(folders) == shares_at("Shared")
            })?;
            let main = started.account.runner(&folder).ok_or("no runner")??;
            let refused = main.shares(SharesAsk::Make);
            assert!(refused.is_err_and(|why| why.contains("is there")));

            shares_gone(&started, &folder)?;
            // Not by itself, also over a sync.
            let _ = started.account.sync_now();
            std::thread::sleep(std::time::Duration::from_millis(500));
            assert!(!folder.join("Shared").exists());

            main.shares(SharesAsk::Make)?;
            started.wait_all(
                "made again, and the shares synced into it",
                &|s, folders| {
                    both_synced(s, folders)
                        && shares_shown(folders) == shares_at("Shared")
                        && folder.join("Shared").join("tom.example").is_dir()
                },
            )?;
            let own = drive_names(&server)?;
            assert_eq!(own, ["mine.txt"], "something of the shares was sent");
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// A folder at the top of the main folder named as its folder of shares on the user's
    /// word, where the one on record is gone: what was sent of it leaves the drive, all of
    /// it stays on disk, and what is shared is synced into it. Refused for what isn't a
    /// folder there.
    #[test]
    fn a_folder_named_as_the_folder_of_shares_leaves_the_drive_and_takes_the_shares()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::runner::SharesAsk;

        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, server, start) = shared_with(home.path(), &tom_server)?;
        std::fs::create_dir(folder.join("Theirs"))?;
        std::fs::write(
            folder.join("Theirs").join("old.txt"),
            "sent before it was named",
        )?;
        let started = start();
        let on_drive = || drive_names(&server);
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("both sync", &both_synced)?;
            assert_eq!(on_drive()?, ["Theirs", "mine.txt", "old.txt"]);
            shares_gone(&started, &folder)?;

            let main = started.account.runner(&folder).ok_or("no runner")??;
            for not_one in ["mine.txt", "Theirs/old.txt", "none"] {
                let refused = main.shares(SharesAsk::Take(not_one.into()));
                assert!(
                    refused.is_err_and(|why| why.contains("isn't a folder")),
                    "{not_one}"
                );
            }
            main.shares(SharesAsk::Take("Theirs".into()))?;
            started.wait_all("taken, and the shares synced into it", &|s, folders| {
                both_synced(s, folders)
                    && shares_shown(folders) == shares_at("Theirs")
                    && folder.join("Theirs").join("tom.example").is_dir()
                    && on_drive().is_ok_and(|names| names == ["mine.txt"])
            })?;
            assert!(folder.join("Theirs").join("old.txt").is_file());
            assert!(!folder.join("Shared").exists());
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// The folder of shares dragged into a role folder of the account is left alone there:
    /// that folder knows it by the main folder's record, and nothing of it is sent. Shares
    /// are off meanwhile; moved back, they are synced again.
    #[test]
    fn the_folder_of_shares_dragged_into_a_role_folder_is_left_alone_there()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::protocol::SharesFolder;

        let home = tempfile::tempdir()?;
        let me: Identity = "me.example".into();
        let tom_server = FakeServer::new(ServerConfig::default());
        let (paths, folder, server, start) = shared_with(home.path(), &tom_server)?;
        let documents = home.path().join("Documents");
        std::fs::create_dir(&documents)?;
        std::fs::write(documents.join("letter.txt"), "mine too")?;
        let role = Root::role(&me, &documents, hbsync_types::Role::Documents);
        let (top, kind) = (role.kind.top(), role.kind.folder_kind());
        let db = paths.account_db(&me);
        hbsync_engine::root::SyncRoot::create_for(&db, &me, &documents, top, kind)?;
        let started = start();
        started.account.add_folder(&role);
        let all_synced = |s: &AccountStatus, folders: &[FolderStatus], n: usize| {
            folders.len() == n && folders.iter().all(|f| synced(s, f))
        };
        let on_drive = || drive_names(&server);
        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait_all("all three sync", &|s, folders| {
                all_synced(s, folders, 3) && shares_shown(folders) == shares_at("Shared")
            })?;
            let own = on_drive()?;
            assert!(own.contains(&"letter.txt".to_owned()), "{own:?}");
            std::fs::write(folder.join("Shared").join("loose.txt"), "not the account's")?;

            rename_synced(&folder.join("Shared"), &documents.join("Shared"))?;
            let _ = started.account.sync_now();
            started.wait_all("shares are off, the role folder synced", &|s, folders| {
                shares_shown(folders) == Some(SharesFolder::Gone)
                    && folders.iter().all(|f| synced(s, f))
            })?;
            // A change beside it in the role folder is sent; nothing of it is.
            std::fs::write(documents.join("more.txt"), "mine as well")?;
            let _ = started.account.sync_now();
            let sent = wait_until(|| on_drive().ok().filter(|n| n.contains(&"more.txt".into())));
            let mut want = own.clone();
            want.push("more.txt".into());
            want.sort();
            assert_eq!(
                sent,
                Some(want),
                "something of the folder of shares was sent"
            );

            rename_synced(&documents.join("Shared"), &folder.join("Shared"))?;
            let _ = started.account.sync_now();
            started.wait_all("back at the top, the shares synced again", &|s, folders| {
                all_synced(s, folders, 3) && shares_shown(folders) == shares_at("Shared")
            })?;
            assert_eq!(on_drive()?.len(), own.len() + 1);
            assert!(folder.join("Shared").join("loose.txt").is_file());
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// The main folder removed: what others share stops with it (its folder of shares is no
    /// synced folder's any more), its files left where they are, and the account ends.
    #[test]
    fn the_shares_stop_with_the_main_folder() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let tom_server = FakeServer::new(ServerConfig::default());
        let (_, folder, _, start) = shared_with(home.path(), &tom_server)?;
        let started = start();
        let at = folder.join("Shared").join("tom.example");
        let run = started.wait_all("both sync", &both_synced);
        started.account.remove_folder(&folder);
        let ended = wait_until(|| started.thread.is_finished().then_some(())).is_some();
        let (_, folders) = started.account.status();
        started.stop(())?;
        run?;
        assert!(ended, "the account didn't end: {folders:?}");
        assert!(folders.is_empty(), "{folders:?}");
        assert!(at.is_dir() && folder.join("mine.txt").is_file());
        Ok(())
    }

    /// Its DB damaged while the account runs (SQLite answers that it is malformed): its folder's
    /// runner ends, the account stops its parts, sets the DB aside, tells it once, and syncs on
    /// in a new one, the folder adopted (nothing sent, nothing deleted). Damaged again, it isn't
    /// set aside again: the folder fails, starting again after a pause.
    #[test]
    fn a_db_damaged_while_running_is_set_aside_once() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_state::Unusable::Corrupt;

        let home = tempfile::tempdir()?;
        let identity: Identity = "a.example".into();
        let (paths, folder, db) =
            set_up(&home.path().canonicalize()?, &identity, &[("a.txt", "a")])?;
        let server = FakeServer::new(ServerConfig::default());
        let remote = FakeRemote::new(server.clone());

        let opening = (identity.clone(), remote.clone());
        let started = Started::new(&paths, Root::main(&identity, &folder), move |db| {
            Ok(Drive::open(db, &opening.0, Arc::new(opening.1.clone()))?)
        });
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
            started
                .account
                .runner(&folder)
                .ok_or("no runner")??
                .send(Command::SyncNow);
            Ok(())
        };

        let checks = || -> Result<(), Box<dyn std::error::Error>> {
            started.wait("the first sync", &synced)?;
            let uploads = remote.faults().uploads;
            damage()?;
            sync_now()?;
            started.wait("set aside and synced again", &|s, f| {
                synced(s, f) && started.told(Corrupt) == 1
            })?;
            assert_eq!(set_aside(&db, Corrupt)?, 1, "kept");
            hbsync_engine::root::SyncRoot::find(&db, &identity, &folder)?;
            assert_eq!(
                remote.faults().uploads,
                uploads,
                "adopted: nothing sent again"
            );
            assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 0);

            damage()?;
            sync_now()?;
            started.wait("failing", &|_, f| {
                f.state == State::Failing
                    && f.error.as_deref().is_some_and(|e| e.contains("malformed"))
            })?;
            std::thread::sleep(std::time::Duration::from_millis(300));
            assert_eq!(
                (started.told(Corrupt), set_aside(&db, Corrupt)?),
                (1, 1),
                "set aside once a run"
            );
            Ok(())
        };
        let checked = checks();
        started.stop(checked)?
    }

    /// The drive deleted and made again while the computer was off (another creation time,
    /// nothing of the old drive on it): as the account starts again, its DB is set aside before
    /// anything syncs, told once, and the folder set up again: nothing here is deleted, its
    /// files go up to the new drive, the old drive is left as it was.
    #[test]
    fn a_db_of_a_drive_made_again_is_set_aside_before_anything_syncs()
    -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_state::Unusable::OtherDrive;

        let home = tempfile::tempdir()?;
        let identity: Identity = "a.example".into();
        let files = [("a.txt", "a"), ("sub/b.txt", "b")];
        let (paths, folder, db) = set_up(&home.path().canonicalize()?, &identity, &files)?;

        // The account started on `remote`'s drive, made at `made`, until its folder synced:
        // how often it told that the DB was set aside.
        let run = |remote: &FakeRemote, made: i64| -> Result<usize, Box<dyn std::error::Error>> {
            let opening = (identity.clone(), remote.clone());
            let started = Started::new(&paths, Root::main(&identity, &folder), move |db| {
                parts::drive_made(&opening.0, Arc::new(opening.1.clone()), made, db)
            });
            let checked = started
                .wait("synced", &synced)
                .map(|()| started.told(OtherDrive));
            Ok(started.stop(checked)??)
        };

        let old = FakeServer::new(ServerConfig::default());
        let old_remote = FakeRemote::new(old.clone());
        assert_eq!(run(&old_remote, 1)?, 0, "the first drive is taken");
        assert_eq!(run(&old_remote, 1)?, 0, "the same drive");
        let on_old = old.lock().map_err(|_| "poisoned")?.live_names();

        let made_again = FakeServer::new(ServerConfig::default());
        let new_remote = FakeRemote::new(made_again.clone());
        assert_eq!(run(&new_remote, 2)?, 1, "told once");
        assert_eq!(set_aside(&db, OtherDrive)?, 1, "kept");
        for (name, text) in files {
            assert_eq!(std::fs::read_to_string(folder.join(name))?, text);
        }
        assert_eq!(new_remote.faults().uploads, 2, "every file up");
        assert_eq!(
            made_again.lock().map_err(|_| "poisoned")?.live_names(),
            on_old
        );
        let old = old.lock().map_err(|_| "poisoned")?;
        assert_eq!((old.live_names(), old.removed().count()), (on_old, 0));
        drop(old);
        assert_eq!(run(&new_remote, 2)?, 0, "the new DB took the new drive");
        assert_eq!(set_aside(&db, OtherDrive)?, 1);
        Ok(())
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
            expires: None,
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
        let documents = Root::role(
            &"a.example".into(),
            Path::new("/h/Documents"),
            hbsync_types::Role::Documents,
        );
        let (account, _requests) = Account::new(&paths, &documents, None, Arc::default());
        account.add_folder(&main);
        let (_, folders) = account.status();
        let shown: Vec<_> = folders.iter().map(|f| (f.path.as_path(), f.kind)).collect();
        assert_eq!(
            shown,
            [
                (documents.folder.as_path(), documents.kind),
                (main.folder.as_path(), crate::RootKind::Main)
            ]
        );
        assert!(folders.iter().all(|f| f.state == State::Starting));
        account.pause(Some(Pause::UNTIL_RESUMED));
        let (_, folders) = account.status();
        assert!(folders.iter().all(|f| f.state == State::Paused));
        assert_eq!(account.folders(), [documents, main]);
    }
}
