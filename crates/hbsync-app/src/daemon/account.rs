//! One account (identity) of the daemon: it waits for the credentials (a keychain locked at
//! logon), connects (retrying while the identity can't be reached: a laptop starts offline),
//! and owns what is the account's: the [`Drive`], the notifications (a change announced is
//! pulled once, which wakes the folders it concerns), Recently deleted and its daily purge.
//! Its folders' runners sync until the daemon stops, or until it starts over with new
//! credentials.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SendError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use hbsync_engine::Drive;
use hbsync_engine::deleted::Listed;
use hbsync_engine::root::{AccountReader, RootError};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_homebase::{Client, HomebaseError};
use hbsync_remote::ChangeRef;
use hbsync_state::StateError;
use hbsync_types::NodeId;
use hbsync_types::activity::Deleted;
use hbsync_types::unix_now;

use super::DaemonError;
use crate::protocol::{
    AccountControl, AccountStatus, DeletedAccount, DeletedItem, FolderStatus, Hints, RecentItem,
    State, Until,
};
use crate::runner::schedule::Announced;
use crate::runner::{Backoff, Command, Handle, Runner, RunnerConfig, RunnerError, lock, slept};
use crate::{Config, CredentialError, CredentialStore, Location, Paths};

/// How often the account looks at its notifier, its requests and its runners.
const TICK: Duration = Duration::from_millis(200);
/// How often the purge of Recently deleted is looked at (it runs once a day).
const PURGE_LOOK: Duration = Duration::from_secs(600);

/// An account of the daemon, shared with the control socket's thread.
pub(crate) struct Account {
    pub identity: String,
    /// Its state DB.
    db: PathBuf,
    status: Mutex<AccountStatus>,
    /// Its folders here, and their runners once it runs.
    phase: Mutex<Phase>,
    /// Paused (kept in the config too): a runner starts paused.
    paused: Mutex<Option<Until>>,
    /// Requests run on its thread.
    requests: Sender<Request>,
    /// Told to end (to start over with new credentials).
    ended: AtomicBool,
    /// A connection to its DB for the control socket's answers (recent, deleted), opened on
    /// first use.
    reader: Mutex<Option<AccountReader>>,
}

/// What the account's thread is asked to do.
pub(crate) enum Request {
    Deleted(AccountControl),
    /// Start syncing this folder.
    AddFolder(PathBuf),
    /// Stop syncing this folder.
    RemoveFolder(PathBuf),
}

/// Before its runners start, the account still answers.
enum Phase {
    /// Waiting for the credentials or the identity.
    Waiting {
        folders: Vec<PathBuf>,
        state: State,
        error: Option<String>,
    },
    Running(Vec<(PathBuf, Handle)>),
}

impl Account {
    /// The account `identity` syncing `folder`, started paused when `paused`; its thread runs
    /// with the returned receiver ([`Account::run`]).
    pub fn new(
        paths: &Paths,
        identity: &str,
        folder: &Path,
        paused: Option<Until>,
    ) -> (Arc<Account>, Receiver<Request>) {
        let (requests, rx) = channel();
        let account = Account {
            identity: identity.to_owned(),
            db: paths.account_db(identity),
            status: Mutex::new(AccountStatus::new(identity)),
            phase: Mutex::new(Phase::Waiting {
                folders: vec![folder.to_path_buf()],
                state: State::Starting,
                error: None,
            }),
            paused: Mutex::new(paused),
            requests,
            ended: AtomicBool::new(false),
            reader: Mutex::new(None),
        };
        (Arc::new(account), rx)
    }

    /// Its folders.
    pub fn folders(&self) -> Vec<PathBuf> {
        match &*lock(&self.phase) {
            Phase::Waiting { folders, .. } => folders.clone(),
            Phase::Running(runners) => runners.iter().map(|(p, _)| p.clone()).collect(),
        }
    }

    /// Syncs the folder at `path` too (once connected).
    pub fn add_folder(&self, path: &Path) {
        match &mut *lock(&self.phase) {
            Phase::Waiting { folders, .. } => {
                if !folders.iter().any(|f| f == path) {
                    folders.push(path.to_path_buf());
                }
            }
            Phase::Running(_) => {
                let _ = self.requests.send(Request::AddFolder(path.to_path_buf()));
            }
        }
    }

    /// Stops syncing the folder at `path`.
    pub fn remove_folder(&self, path: &Path) {
        match &mut *lock(&self.phase) {
            Phase::Waiting { folders, .. } => folders.retain(|f| f != path),
            Phase::Running(_) => {
                let _ = self
                    .requests
                    .send(Request::RemoveFolder(path.to_path_buf()));
            }
        }
    }

    /// Its status and its folders'.
    pub fn status(&self) -> (AccountStatus, Vec<FolderStatus>) {
        let folders = match &*lock(&self.phase) {
            Phase::Running(runners) => runners.iter().map(|(_, h)| h.status()).collect(),
            Phase::Waiting {
                folders,
                state,
                error,
            } => {
                let paused = *lock(&self.paused);
                folders
                    .iter()
                    .map(|path| {
                        let mut folder = FolderStatus::new(&self.identity, path);
                        folder.state = if paused.is_some() {
                            State::Paused
                        } else {
                            *state
                        };
                        folder.paused_until = paused.flatten();
                        folder.error.clone_from(error);
                        folder
                    })
                    .collect()
            }
        };
        (lock(&self.status).clone(), folders)
    }

    /// The runner of its folder at `path`, once running.
    pub fn runner(&self, path: &Path) -> Option<Handle> {
        match &*lock(&self.phase) {
            Phase::Running(runners) => runners
                .iter()
                .find(|(p, _)| p == path)
                .map(|(_, h)| h.clone()),
            Phase::Waiting { .. } => None,
        }
    }

    /// Passes `command` to every runner. False while it isn't running yet.
    pub fn send_all(&self, command: &Command) -> bool {
        match &*lock(&self.phase) {
            Phase::Running(runners) => {
                for (_, h) in runners {
                    h.send(command.clone());
                }
                true
            }
            Phase::Waiting { .. } => false,
        }
    }

    /// Pauses (`Some`) or resumes its folders, also before they run.
    pub fn pause(&self, pause: Option<Until>) {
        *lock(&self.paused) = pause;
        self.send_all(&match pause {
            Some(until) => Command::Pause { until },
            None => Command::Resume,
        });
    }

    /// Why it can't take requests yet; `None` once it runs.
    pub fn not_running(&self) -> Option<String> {
        match &*lock(&self.phase) {
            Phase::Running(_) => None,
            Phase::Waiting { error, .. } => Some(format!(
                "not connected to {} yet{}",
                self.identity,
                error.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
            )),
        }
    }

    /// Asks for `control` on its Recently deleted; done on its thread.
    pub fn request(&self, control: AccountControl) -> Result<(), String> {
        if let Some(why) = self.not_running() {
            return Err(why);
        }
        // Answered before it runs: a listing the drive would refuse is refused here, so the
        // refusal reaches whoever asked (the drive checks again as it empties).
        if let AccountControl::EmptyListed { db, seq } = &control {
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
        let request = Request::Deleted(control);
        self.requests
            .send(request)
            .map_err(|_| "the account has stopped".to_owned())
    }

    /// Ends the account: its runners stop between operations and [`Account::run`] returns.
    pub fn end(&self) {
        self.ended.store(true, Ordering::Relaxed);
    }

    /// The newest `limit` entries of its folders' recently synced lists.
    pub fn recent(&self, limit: usize) -> Vec<RecentItem> {
        let folders = self.folders();
        self.read(|db| {
            let mut items = Vec::new();
            for f in db.folders()? {
                let path = PathBuf::from(&f.path);
                if !folders.contains(&path) {
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
    /// items it took along; and, when it holds anything, all of it as listed now.
    pub fn deleted(&self, limit: usize) -> (Vec<DeletedItem>, Option<DeletedAccount>) {
        let Some((deletes, summary)) = self.read(|db| db.deletes(limit)) else {
            return (Vec::new(), None);
        };
        // Anything to list, though emptying may take nothing (a folder holding a live node).
        let account = (summary.deletes > 0).then(|| DeletedAccount {
            identity: self.identity.clone(),
            deletes: summary.deletes,
            items: summary.items,
            db: summary.listed.db,
            seq: summary.listed.seq,
        });
        (self.items(deletes), account)
    }

    /// The items of its Recently deleted that are `nodes`.
    pub fn find_deleted(&self, nodes: &[NodeId]) -> Vec<DeletedItem> {
        self.items(self.read(|db| db.find_deleted(nodes)).unwrap_or_default())
    }

    /// The items of its Recently deleted named `names`.
    pub fn named_deleted(&self, names: &[String]) -> Vec<DeletedItem> {
        self.items(self.read(|db| db.named_deleted(names)).unwrap_or_default())
    }

    fn items(&self, found: Vec<(Deleted, usize)>) -> Vec<DeletedItem> {
        found
            .into_iter()
            .map(|(deleted, along)| DeletedItem {
                identity: self.identity.clone(),
                deleted,
                along,
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

    fn waiting(&self, to: State, why: String) {
        if let Phase::Waiting { state, error, .. } = &mut *lock(&self.phase) {
            *state = to;
            *error = Some(why);
        }
    }

    /// Whether it should stop: the daemon stops, or the account ends.
    fn stopping(&self, stop: &AtomicBool) -> bool {
        stop.load(Ordering::Relaxed) || self.ended.load(Ordering::Relaxed)
    }

    /// Waits `time`, or less if it should stop meanwhile. Returns whether it should.
    fn wait(&self, time: Duration, stop: &AtomicBool) -> bool {
        let until = Instant::now() + time;
        while Instant::now() < until && !self.stopping(stop) {
            std::thread::sleep(TICK);
        }
        self.stopping(stop)
    }

    /// Connects and syncs its folders until it should stop, then stops them; a folder's runner
    /// that fails starts again after a pause. An error: a runner that couldn't start.
    pub fn run(
        &self,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<Request>,
        stop: &AtomicBool,
    ) -> Result<(), DaemonError> {
        let Some(creds) = self.credentials(paths, stop)? else {
            return Ok(());
        };
        let client = Client::new(&creds)?;
        let Some(remote) = self.connect(&client, stop) else {
            return Ok(());
        };
        let (tx, events) = channel();
        let notifier = match Notifier::spawn(client, remote.drive(), tx) {
            Ok(n) => {
                lock(&self.status).hints = Hints::Connecting;
                Some(n)
            }
            // Not connected, for good: the drive is read on a timer (Notifications).
            Err(e) => {
                tracing::error!(
                    "can't start the notifications ({e}); reading the drive now and then"
                );
                lock(&self.status).hints = Hints::Lost(format!("can't start: {e}"));
                None
            }
        };
        let drive = Drive::open(&self.db, &self.identity, remote)
            .map_err(|e| DaemonError::Runner(e.into()))?;
        // Tokens are sealed under the login's shared secret: another login's don't open.
        let login = format!(
            "{:016x}",
            hbsync_fileio::fnv1a64(creds.shared_secret.as_bytes())
        );
        if drive
            .tokens_sealed_by(&login)
            .map_err(|e| DaemonError::Runner(e.into()))?
        {
            tracing::info!("a new login: the files' tokens are read again as they're written");
        }
        let mut notifications =
            Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now());
        let mut runners: Vec<Running> = Vec::new();
        // A runner that couldn't start: the account stops its runners and ends with it, the
        // daemon with it (restarted whole). One that ended with an error is started again after a
        // pause, its folder showing why meanwhile; the others sync on.
        let mut result: Result<(), DaemonError> = Ok(());
        {
            // Under the lock: no pause or folder gets lost between the two phases.
            let mut phase = lock(&self.phase);
            let folders = match &*phase {
                Phase::Waiting { folders, .. } => folders.clone(),
                Phase::Running(_) => Vec::new(),
            };
            for folder in folders {
                match self.start(&drive, config, &folder) {
                    Ok(r) => runners.push(r),
                    Err(e) => {
                        result = Err(e.into());
                        break;
                    }
                }
            }
            *phase = Phase::Running(shown(&runners));
        }

        // Folders to start once no runner of theirs is left.
        let mut adding: Vec<PathBuf> = Vec::new();
        let keep_days = config.keep_deleted_days.0;
        let mut next_purge = Instant::now();
        loop {
            let (wall, mono) = (SystemTime::now(), Instant::now());
            std::thread::sleep(TICK);
            if self.stopping(stop) || result.is_err() {
                for r in &runners {
                    r.handle.send(Command::Stop);
                }
            }
            if slept(
                SystemTime::now().duration_since(wall).ok(),
                mono.elapsed(),
                false,
            ) && let Some(n) = &notifier
            {
                n.reconnect();
            }
            if pause_ran_out(&mut lock(&self.paused), unix_now()) {
                tracing::info!("the pause is over");
            }
            let paused = lock(&self.paused).is_some();
            while let Ok(event) = events.try_recv() {
                if let Some(hints) = notifications.event(event, Instant::now(), paused) {
                    // Connected again: what waits for the drive goes now.
                    if hints == Hints::Connected {
                        for r in &runners {
                            r.handle.send(Command::Online);
                        }
                    }
                    lock(&self.status).hints = hints;
                }
            }
            notifications.tick(Instant::now(), paused);
            let mut changed = false;
            while let Ok(request) = requests.try_recv() {
                match request {
                    Request::Deleted(control) => run_request(&drive, control),
                    // Started below, once no runner of the folder is left (one removed a moment
                    // ago may still be finishing an operation).
                    Request::AddFolder(folder) => {
                        let synced = runners.iter().any(|r| r.path == folder && r.shows());
                        if !synced && !adding.contains(&folder) {
                            adding.push(folder);
                        }
                    }
                    Request::RemoveFolder(folder) => {
                        adding.retain(|f| *f != folder);
                        for r in runners.iter_mut().filter(|r| r.path == folder) {
                            r.stop_for_good();
                        }
                        changed = true;
                    }
                }
            }
            // Not while paused: no deletes for good, no pulls.
            if Instant::now() >= next_purge && lock(&self.paused).is_none() {
                next_purge = Instant::now() + PURGE_LOOK;
                match drive.purge_when_due(keep_days) {
                    Ok(Some(n)) if n > 0 => {
                        tracing::info!("{n} items deleted for good after {keep_days} days")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("purging Recently deleted failed: {e}"),
                }
            }
            // A runner that ended takes its folder out, unless it failed: then it starts again
            // after a pause. The account ends with the last.
            let before = runners.len();
            let ending = self.stopping(stop) || result.is_err();
            runners.iter_mut().for_each(|r| r.ended(ending));
            if ending {
                adding.clear();
            }
            runners.retain(|r| !matches!(r.life, Life::Gone));
            let now = Instant::now();
            let due = runners.iter().any(|r| r.due(now));
            let ready: Vec<PathBuf> = adding
                .extract_if(.., |f| !runners.iter().any(|r| r.path == *f))
                .collect();
            // The config as it is now (its settings may have changed meanwhile); while it can't
            // be read, restarts wait a while and new folders wait for the next tick.
            let config = (due || !ready.is_empty())
                .then(|| Self::config(paths))
                .flatten();
            for r in runners.iter_mut().filter(|r| r.due(now)) {
                match config.as_ref().map(|c| self.start(&drive, c, &r.path)) {
                    Some(Ok(new)) => {
                        *r = Running {
                            backoff: r.backoff,
                            ..new
                        };
                        changed = true;
                    }
                    Some(Err(e)) => result = Err(e.into()),
                    None => r.life = Life::Waiting(now + r.backoff.after(State::Failing)),
                }
            }
            for folder in ready {
                let Some(config) = &config else {
                    adding.push(folder);
                    continue;
                };
                match self.start(&drive, config, &folder) {
                    Ok(r) => runners.push(r),
                    Err(e) => result = Err(e.into()),
                }
                changed = true;
            }
            if changed || runners.len() != before {
                *lock(&self.phase) = Phase::Running(shown(&runners));
            }
            if runners.is_empty() {
                notifications.finish();
                return result;
            }
        }
    }

    /// The config as it is now; `None` (logged) while it can't be read (being edited, say):
    /// what needs it waits.
    fn config(paths: &Paths) -> Option<Config> {
        match Config::load(&paths.config_file()) {
            Ok(c) => Some(c.unwrap_or_default()),
            Err(e) => {
                tracing::warn!("can't read the config: {e}");
                None
            }
        }
    }

    /// Starts syncing `folder` (as `config` lists it) on the account's `drive`.
    fn start(
        &self,
        drive: &Arc<Drive>,
        config: &Config,
        folder: &Path,
    ) -> Result<Running, RunnerError> {
        let mut cfg = RunnerConfig::new(folder, &self.db, &self.identity, &config.device_name());
        cfg.mass_delete = config.mass_delete;
        cfg.role = config
            .roots
            .iter()
            .find(|r| r.folder == folder)
            .and_then(|r| r.role);
        cfg.paused = *lock(&self.paused);
        let (handle, thread) = Runner::spawn(cfg, Arc::clone(drive))?;
        Ok(Running {
            path: folder.to_path_buf(),
            handle,
            life: Life::Syncing(thread),
            started: Instant::now(),
            backoff: Backoff::new(),
        })
    }

    /// The credentials, waiting while the keychain can't be read; `None` when stopped first.
    fn credentials(
        &self,
        paths: &Paths,
        stop: &AtomicBool,
    ) -> Result<Option<hbsync_homebase::Credentials>, DaemonError> {
        let store = CredentialStore::open(paths);
        let mut backoff = Backoff::new();
        let (creds, location) = loop {
            match store.load(&self.identity) {
                Ok(Some(found)) => break found,
                Ok(None) => {
                    return Err(DaemonError::NotSetUp(format!(
                        "no credentials for {}",
                        self.identity
                    )));
                }
                Err(CredentialError::Keychain(why)) => {
                    let wait_for = backoff.after(State::Failing);
                    tracing::warn!("can't read the keychain ({why}); trying again in {wait_for:?}");
                    self.waiting(State::Failing, format!("can't read the keychain: {why}"));
                    if self.wait(wait_for, stop) {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        };
        if let Location::File { path, why } = location {
            tracing::warn!("credentials in {} (no keychain: {why})", path.display());
            lock(&self.status).credentials_warning =
                Some(format!("kept in {} (no keychain: {why})", path.display()));
        }
        Ok(Some(creds))
    }

    /// The drive, once the identity answers (finding it takes a request); `None` when stopped
    /// first.
    fn connect(&self, client: &Client, stop: &AtomicBool) -> Option<Arc<HomebaseRemote>> {
        let mut backoff = Backoff::new();
        loop {
            match HomebaseRemote::connect(client.clone()) {
                Ok(r) => return Some(Arc::new(r)),
                Err(e) => {
                    let state = match &e {
                        HomebaseError::Denied(_) => State::Denied,
                        HomebaseError::Network(_) => State::Offline,
                        _ => State::Failing,
                    };
                    let wait_for = backoff.after(state);
                    tracing::warn!(
                        "can't connect to {} ({e}); trying again in {wait_for:?}",
                        self.identity
                    );
                    self.waiting(state, e.to_string());
                    if self.wait(wait_for, stop) {
                        return None;
                    }
                }
            }
        }
    }
}

/// While the notifications aren't connected (not yet, not any more, or never: a proxy, a
/// refusal), the drive is read this often; never while they are.
pub const UNANNOUNCED_READ: Duration = Duration::from_secs(300);

/// What the identity's notifications do to its drive: changes announced are taken once the
/// announcements have quieted (a burst of deletes reaches the mass-delete guard whole), the
/// feed read only for what the store doesn't know already ([`Drive::announced`]); connecting
/// (again) pulls at once, as changes made meanwhile weren't announced; while not connected,
/// the drive is read every [`UNANNOUNCED_READ`]. Nothing is pulled while paused. A pull wakes
/// the folders its changes concern.
#[derive(Debug)]
pub struct Notifications {
    puller: Puller,
    announced: Announced,
    /// The live changes announced since the last take, up to [`MAX_LIVE_REFS`], and whether
    /// the feed is to be read too (an announcement didn't say which file, or there were more).
    /// A hard delete isn't gathered here: the puller takes it within [`GATHER_WINDOW`].
    changes: Vec<ChangeRef>,
    unsaid: bool,
    /// Connected: the drive's mark of this connection ([`Drive::connection`]).
    connected: Option<u64>,
    /// Connected while paused: the read that follows connecting waits for the resume.
    connect_read: bool,
    /// While not connected: the next read of the drive.
    next_read: Instant,
}

/// Live changes kept while announcements gather (or the account is paused): more are read
/// from the feed instead, in one go.
const MAX_LIVE_REFS: usize = 2_000;

impl Notifications {
    /// Not connected yet: the first read of the drive comes [`UNANNOUNCED_READ`] after `now`
    /// (the folders' start-up has just read it).
    pub fn new(puller: Puller, now: Instant) -> Self {
        Notifications {
            puller,
            announced: Announced::default(),
            changes: Vec::new(),
            unsaid: false,
            connected: None,
            connect_read: false,
            next_read: now + UNANNOUNCED_READ,
        }
    }

    /// Takes `event` at `now`; the account's notification status when it changed.
    pub fn event(&mut self, event: Event, now: Instant, paused: bool) -> Option<Hints> {
        match event {
            Event::Connected => {
                // Changes made while not connected weren't announced: the feed is read, now or
                // at the resume, and it brings what was gathered too. Only once that read
                // worked are the drive's changes taken as announced ([`Drive::current`]).
                self.connected = Some(self.puller.drive.connection());
                self.announced = Announced::default();
                self.changes.clear();
                self.unsaid = false;
                self.connect_read = true;
                if !paused {
                    self.read_connected();
                }
                Some(Hints::Connected)
            }
            Event::Changed(change) => {
                match change {
                    // In no feed: taken within the puller's window, also while paused (it
                    // writes only the account's DB; the folders sync it at the resume).
                    Some(change) if change.deleted => {
                        self.puller.send(vec![change], Read::No, self.connected);
                    }
                    Some(change) => {
                        self.announced.note(now);
                        if !self.unsaid {
                            self.changes.push(change);
                            if self.changes.len() > MAX_LIVE_REFS {
                                self.read_instead();
                            }
                        }
                    }
                    None => {
                        self.announced.note(now);
                        self.read_instead();
                    }
                }
                None
            }
            Event::Lost(why) => {
                tracing::info!("notifications lost ({why}); reconnecting");
                self.disconnected(now);
                Some(Hints::Lost(why))
            }
            Event::Refused(why) => {
                tracing::warn!("notifications refused ({why}); asking again later");
                self.disconnected(now);
                Some(Hints::Refused(why))
            }
        }
    }

    fn disconnected(&mut self, now: Instant) {
        // What was announced and not taken yet may be lost with it: behind.
        self.puller.drive.not_announcing();
        self.connect_read = false;
        if self.connected.take().is_some() {
            self.next_read = now + UNANNOUNCED_READ;
        }
    }

    /// The read that follows connecting.
    fn read_connected(&mut self) {
        self.connect_read = false;
        self.puller.send(Vec::new(), Read::Asked, self.connected);
    }

    /// The live changes announced are read from the feed instead.
    fn read_instead(&mut self) {
        self.changes.clear();
        self.unsaid = true;
    }

    /// The account is ending: the puller takes what it gathered, and ends.
    pub fn finish(&self) {
        self.puller.finish();
    }

    /// Hands what was announced to the puller.
    fn take(&mut self) {
        let read = match std::mem::take(&mut self.unsaid) {
            true => Read::Unsaid,
            false => Read::No,
        };
        self.puller
            .send(std::mem::take(&mut self.changes), read, self.connected);
    }

    /// Pulls what was announced once it has quieted, and the drive every [`UNANNOUNCED_READ`]
    /// while not connected; nothing while `paused` (a resume syncs every folder anyway).
    pub fn tick(&mut self, now: Instant, paused: bool) {
        if paused {
            return;
        }
        if self.connect_read {
            self.read_connected();
        }
        if self.announced.due(now) {
            self.take();
        }
        if self.connected.is_none() && now >= self.next_read {
            self.next_read = now + UNANNOUNCED_READ;
            self.puller.ask();
        }
    }

    /// What waits to be taken: the changes kept, and whether the feed is to be read too.
    #[cfg(test)]
    fn waiting(&self) -> (&[ChangeRef], bool) {
        (&self.changes, self.unsaid)
    }
}

/// Pulls the drive's changes on a thread of its own, so whoever asks (the account's loop)
/// never waits for the network or the DB: asks while a pull runs are answered by one more.
/// Changes announced are taken first, and the feed read only for what they leave unknown. A
/// pull wakes the folders its changes concern; when it fails every folder is woken, and their
/// syncs pull (and back off) themselves. A hard delete announced that couldn't be taken is
/// kept and tried again: it is in no feed. A thread that ended is started again.
#[derive(Debug, Clone)]
pub struct Puller {
    asks: Arc<Mutex<Sender<Asked>>>,
    /// Its thread, joined when the account ends ([`Puller::finish`]).
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    drive: Arc<Drive>,
    /// Rounds done (taken and read), for tests.
    rounds: Arc<AtomicUsize>,
    /// [`AWAIT_RETRY`], shorter in tests.
    await_retry: Duration,
}

/// What the puller is asked: changes announced, whether to read the feed, and the
/// notifications' connection it was asked at, if connected: a read that works for it means
/// the drive's changes are announced ([`Drive::announcing_from`]).
type Asked = (Vec<ChangeRef>, Read, Option<u64>);

/// Whether the puller reads the feed: no, asked to (a plain ask), or for changes announced
/// without saying which (an event without a file, or too many). Those can't be looked for
/// after the read ([`Drive::unseen`]): the read is confirmed by another past the hold-back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Read {
    No,
    Asked,
    Unsaid,
}

/// Hard deletes not taken (taking them failed) are tried again after this, then twice as
/// long each time up to [`HELD_RETRY_MAX`].
const HELD_RETRY: Duration = Duration::from_secs(1);
const HELD_RETRY_MAX: Duration = Duration::from_secs(300);
/// Hard deletes kept while taking them fails, one per file; more are left stale (written to
/// next, a file deleted for good answers not found).
const MAX_HELD: usize = 10_000;
/// A round that takes announced changes gathers what is announced for this long after its first
/// ask, or until it holds this many changes: one write for a burst (a folder of thousands of
/// files deleted is a notification per file), not one each. A change waits this long at most; a
/// plain read of the feed doesn't wait.
const GATHER_WINDOW: Duration = Duration::from_secs(1);
const GATHER_MAX: usize = 500;
/// An announced change a read of the feed didn't bring is read for again after this (past
/// the server's hold-back of rows written in its last second, `HoldBackWindowMs`), then twice
/// as long each time, [`AWAIT_TRIES`] reads at most: then the account is behind, and every
/// folder's next sync reads the feed. The hold-back is the server's design; a stale answer
/// hiding the row is odin-core #1909 (filesync #90), after whose fix one read again past the
/// hold-back is all that stays.
const AWAIT_RETRY: Duration = Duration::from_millis(1100);
const AWAIT_TRIES: u32 = 6;

/// What a round of the puller did, for the next: hard deletes not taken yet (taking them
/// failed; they are in no feed), announced changes the feed didn't bring yet, whether the
/// read worked, and whether the account may be behind (looking for what it lacks failed).
#[derive(Debug, Default, PartialEq)]
struct Round {
    held: Vec<ChangeRef>,
    awaited: Vec<ChangeRef>,
    read: bool,
    behind: bool,
}

impl Puller {
    pub fn spawn(drive: Arc<Drive>) -> Puller {
        Puller::spawn_timed(drive, AWAIT_RETRY)
    }

    /// [`Puller::spawn`], reading for an announced change the feed didn't bring after
    /// `await_retry` instead of [`AWAIT_RETRY`] (tests).
    #[doc(hidden)]
    pub fn spawn_timed(drive: Arc<Drive>, await_retry: Duration) -> Puller {
        let rounds = Arc::new(AtomicUsize::new(0));
        let (asks, thread) = match start(&drive, &rounds, await_retry) {
            Ok((asks, thread)) => (asks, Some(thread)),
            Err(e) => {
                tracing::error!("can't start reading the drive on its own ({e}): trying again");
                // A sender whose thread is gone: the first ask starts one.
                (channel().0, None)
            }
        };
        Puller {
            asks: Arc::new(Mutex::new(asks)),
            thread: Arc::new(Mutex::new(thread)),
            drive,
            rounds,
            await_retry,
        }
    }

    /// Ends the puller's thread once it has taken what it gathered: the account is ending. A
    /// hard delete announced is in no feed; only a crash loses one gathered (a second's worth).
    pub fn finish(&self) {
        *lock(&self.asks) = channel().0;
        if let Some(thread) = lock(&self.thread).take() {
            let _ = thread.join();
        }
    }

    /// Reads the feed.
    pub fn ask(&self) {
        self.send(Vec::new(), Read::Asked, None);
    }

    /// Takes announced `changes`, reading the feed for what they leave unknown or when one
    /// announcement didn't say which file (`unsaid`).
    pub fn take(&self, changes: Vec<ChangeRef>, unsaid: bool) {
        let read = if unsaid { Read::Unsaid } else { Read::No };
        self.send(changes, read, None);
    }

    /// Rounds the puller's thread has done.
    #[doc(hidden)]
    pub fn rounds(&self) -> usize {
        self.rounds.load(Ordering::SeqCst)
    }

    /// Ends the puller's thread, as a panic in it would (tests: it is started again).
    #[doc(hidden)]
    pub fn end_thread(&self) {
        *lock(&self.asks) = channel().0;
    }

    fn send(&self, changes: Vec<ChangeRef>, read: Read, connection: Option<u64>) {
        let mut asks = lock(&self.asks);
        let Err(SendError(ask)) = asks.send((changes, read, connection)) else {
            return;
        };
        // Never on the asker's thread (the account's loop): a thread of its own again.
        match start(&self.drive, &self.rounds, self.await_retry) {
            Ok((again, thread)) => {
                tracing::warn!("reading the drive had stopped: started again");
                let _ = again.send(ask);
                *asks = again;
                *lock(&self.thread) = Some(thread);
            }
            Err(e) => {
                tracing::error!("can't start reading the drive ({e}): the folders will");
                self.drive.wake_all();
            }
        }
    }
}

/// Starts the puller's thread: it takes what is announced and reads the feed, one round at a
/// time, until every sender is gone (what it gathered taken first).
fn start(
    drive: &Arc<Drive>,
    rounds: &Arc<AtomicUsize>,
    await_retry: Duration,
) -> std::io::Result<(Sender<Asked>, JoinHandle<()>)> {
    let (asks, rx) = channel::<Asked>();
    let (mine, done) = (Arc::clone(drive), Arc::clone(rounds));
    let thread = std::thread::Builder::new()
        .name("hbsync-pull".into())
        .spawn(move || {
            // Hard deletes not taken, and announced changes not seen with the reads tried
            // for each.
            let (mut held, mut held_wait) = (Vec::<ChangeRef>::new(), HELD_RETRY);
            let mut awaited: Vec<(ChangeRef, u32)> = Vec::new();
            // A read for changes announced without saying which is read again past the
            // hold-back, with another page size: when.
            let mut confirm: Option<Instant> = None;
            loop {
                let wait = awaited
                    .iter()
                    .map(|(_, tries)| await_retry * 2u32.pow(tries - 1))
                    .chain((!held.is_empty()).then_some(held_wait))
                    .chain(confirm.map(|at| at.saturating_duration_since(Instant::now())))
                    .min();
                let first = match wait {
                    None => rx.recv().ok(),
                    Some(wait) => match rx.recv_timeout(wait) {
                        Ok(ask) => Some(ask),
                        Err(RecvTimeoutError::Timeout) => Some((Vec::new(), Read::No, None)),
                        Err(RecvTimeoutError::Disconnected) => None,
                    },
                };
                let Some(first) = first else { break };
                let carried = held.drain(..).chain(awaited.iter().map(|(r, _)| *r));
                let (changes, mut read, connection) =
                    merge(carried.collect(), gathered(first, &rx, GATHER_WINDOW));
                let confirming = confirm.is_some_and(|at| Instant::now() >= at);
                if confirming {
                    confirm = None;
                    read = read.max(Read::Asked);
                }
                let r = round(
                    changes,
                    read != Read::No,
                    |c| mine.announced(c).map_err(|e| e.to_string()),
                    || mine.pull(None).map(drop).map_err(|e| e.to_string()),
                    |c| mine.unseen(c).map_err(|e| e.to_string()),
                    || mine.wake_all(),
                );
                if r.read
                    && let Some(connection) = connection
                {
                    mine.announcing_from(connection);
                }
                if read == Read::Unsaid && r.read {
                    confirm = Some(Instant::now() + await_retry);
                }
                if confirming && !r.read {
                    tracing::info!("a read confirming unsaid changes failed: behind");
                    mine.set_behind();
                    mine.wake_all();
                }
                held_wait = match r.held.is_empty() {
                    true => HELD_RETRY,
                    false => (held_wait * 2).min(HELD_RETRY_MAX),
                };
                held = r.held;
                let gave_up;
                (awaited, gave_up) = still_awaited(&awaited, r.awaited);
                if gave_up > 0 || r.behind {
                    if gave_up > 0 {
                        tracing::warn!(
                            "{gave_up} announced changes aren't in the feed after \
                             {AWAIT_TRIES} reads: the folders read it again"
                        );
                    }
                    mine.set_behind();
                    mine.wake_all();
                }
                done.fetch_add(1, Ordering::SeqCst);
            }
        })?;
    Ok((asks, thread))
}

/// The asks of one round: `first`, and when it takes changes, what comes within `window` of
/// it, until they hold [`GATHER_MAX`] changes (the rest wait for the next round); a plain read
/// takes only what is waiting already.
fn gathered(first: Asked, rx: &Receiver<Asked>, window: Duration) -> Vec<Asked> {
    if first.0.is_empty() {
        return std::iter::once(first).chain(rx.try_iter()).collect();
    }
    let until = Instant::now() + window;
    let mut count = first.0.len();
    let mut asks = vec![first];
    while count < GATHER_MAX {
        let Some(left) = until.checked_duration_since(Instant::now()) else {
            break;
        };
        match rx.recv_timeout(left) {
            Ok(ask) => {
                count += ask.0.len();
                asks.push(ask);
            }
            Err(_) => break,
        }
    }
    asks
}

/// The announced changes still awaited after a round that didn't bring `unseen`, each with
/// the reads tried for it (`before`: the last round's; a change new to them starts at one),
/// and how many were given up after [`AWAIT_TRIES`].
fn still_awaited(
    before: &[(ChangeRef, u32)],
    unseen: Vec<ChangeRef>,
) -> (Vec<(ChangeRef, u32)>, usize) {
    let mut gave_up = 0;
    let mut awaited = Vec::new();
    for change in unseen {
        let tries = before
            .iter()
            .find(|(c, _)| *c == change)
            .map_or(1, |(_, tries)| tries + 1);
        if tries > AWAIT_TRIES {
            gave_up += 1;
        } else {
            awaited.push((change, tries));
        }
    }
    (awaited, gave_up)
}

/// The asks of one round, after what the last left (`carried`): all their changes, whether
/// the feed is to be read, and the latest connection asked at.
fn merge(carried: Vec<ChangeRef>, asks: impl IntoIterator<Item = Asked>) -> Asked {
    asks.into_iter().fold(
        (carried, Read::No, None),
        |(mut changes, read, at), (more, also, connection)| {
            changes.extend(more);
            (changes, read.max(also), connection.or(at))
        },
    )
}

/// The hard deletes of `changes`, one per file, at most [`MAX_HELD`].
fn keep(changes: Vec<ChangeRef>) -> Vec<ChangeRef> {
    let mut seen = std::collections::HashSet::new();
    let mut held: Vec<ChangeRef> = changes
        .into_iter()
        .filter(|c| c.deleted && seen.insert(c.file_id))
        .collect();
    if held.len() > MAX_HELD {
        tracing::warn!(
            "{} hard deletes can't be taken: those files stay until written to",
            held.len() - MAX_HELD
        );
        held.truncate(MAX_HELD);
    }
    held
}

/// Whether the account's timed pause (`paused`) ran out at `unix` (seconds): then it is
/// over, as each folder's is ([`crate::runner`]).
fn pause_ran_out(paused: &mut Option<Until>, unix: u64) -> bool {
    if matches!(*paused, Some(Some(until)) if unix >= until) {
        *paused = None;
        return true;
    }
    false
}

/// One round of the puller: takes `changes` (`announced`), the hard deletes first and on
/// their own (a failure of the rest can't hold them back), reads the feed when asked or when
/// they leave something unknown (`read`), and wakes every folder when the read fails. Says
/// which hard deletes are still to take (taking failed, and they are in no feed), and which
/// announced live changes the read didn't bring (`unseen`): the server may hold a row back a
/// moment ([`Drive::unseen`]), so they are read for again.
fn round(
    changes: Vec<ChangeRef>,
    mut pull: bool,
    mut announced: impl FnMut(&[ChangeRef]) -> Result<bool, String>,
    read: impl FnOnce() -> Result<(), String>,
    unseen: impl FnOnce(&[ChangeRef]) -> Result<Vec<ChangeRef>, String>,
    wake_all: impl FnOnce(),
) -> Round {
    let mut round = Round::default();
    let (hard, live): (Vec<ChangeRef>, Vec<ChangeRef>) =
        changes.into_iter().partition(|c| c.deleted);
    for (changes, hard) in [(&hard, true), (&live, false)] {
        if changes.is_empty() {
            continue;
        }
        match announced(changes) {
            Ok(all) => pull |= !all,
            Err(e) => {
                tracing::info!("taking announced changes failed ({e}); reading them");
                if hard {
                    round.held = keep(changes.clone());
                }
                pull = true;
            }
        }
    }
    if !pull {
        return round;
    }
    match read() {
        Ok(()) => {
            round.read = true;
            match unseen(&live) {
                Ok(awaited) => round.awaited = awaited,
                Err(e) => {
                    tracing::info!("looking for announced changes failed ({e}): behind");
                    round.behind = true;
                }
            }
        }
        Err(e) => {
            tracing::info!("pulling the drive's changes failed ({e}); the folders' syncs will");
            wake_all();
        }
    }
    round
}

/// A folder's runner, on the account's thread.
struct Running {
    path: PathBuf,
    handle: Handle,
    life: Life,
    started: Instant,
    backoff: Backoff,
}

enum Life {
    Syncing(JoinHandle<Result<(), RunnerError>>),
    /// Asked to stop for good (its folder removed): it no longer shows.
    Stopping(JoinHandle<Result<(), RunnerError>>),
    /// Failed: started again at this time (not while the account ends).
    Waiting(Instant),
    /// Ended, to be dropped.
    Gone,
}

/// A runner that ran this long without failing starts over with short pauses.
const RAN_WELL: Duration = Duration::from_secs(3600);

impl Running {
    /// Whether the status shows it: not once its folder was removed.
    fn shows(&self) -> bool {
        matches!(self.life, Life::Syncing(_) | Life::Waiting(_))
    }

    /// Its folder was removed: it stops between operations and isn't started again.
    fn stop_for_good(&mut self) {
        self.handle.send(Command::Stop);
        self.life = match std::mem::replace(&mut self.life, Life::Gone) {
            Life::Syncing(t) | Life::Stopping(t) => Life::Stopping(t),
            Life::Waiting(_) | Life::Gone => Life::Gone,
        };
    }

    /// Whether it is to start again at `now`.
    fn due(&self, now: Instant) -> bool {
        matches!(self.life, Life::Waiting(at) if now >= at)
    }

    /// Joins the runner if it ended: one that failed is to start again after a pause, unless
    /// its folder was removed or the account is `ending`; one waiting for that goes when the
    /// account ends.
    fn ended(&mut self, ending: bool) {
        match &self.life {
            Life::Syncing(t) | Life::Stopping(t) if !t.is_finished() => return,
            Life::Waiting(_) if !ending => return,
            _ => {}
        }
        let failed = match std::mem::replace(&mut self.life, Life::Gone) {
            Life::Syncing(t) => t.join().unwrap_or(Err(RunnerError::Panicked)).is_err(),
            Life::Stopping(t) => {
                let _ = t.join();
                false
            }
            Life::Waiting(_) | Life::Gone => false,
        };
        if !failed || ending {
            return;
        }
        if self.started.elapsed() >= RAN_WELL {
            self.backoff = Backoff::new();
        }
        let pause = self.backoff.after(State::Failing);
        tracing::info!("syncing {} again in {pause:?}", self.path.display());
        self.life = Life::Waiting(Instant::now() + pause);
    }
}

/// The runners the status shows.
fn shown(runners: &[Running]) -> Vec<(PathBuf, Handle)> {
    runners
        .iter()
        .filter(|r| r.shows())
        .map(|r| (r.path.clone(), r.handle.clone()))
        .collect()
}

/// Runs a request for Recently deleted; the folders it concerns sync next.
fn run_request(drive: &Drive, control: AccountControl) {
    match control {
        AccountControl::Restore { nodes } => match drive.restore(&nodes) {
            Ok(n) => tracing::info!("{n} items restored from Recently deleted"),
            Err(e) => tracing::warn!("restoring from Recently deleted failed: {e}"),
        },
        AccountControl::EmptyDeleted { nodes } => report_emptied(drive.empty(&nodes)),
        AccountControl::EmptyListed { db, seq } => {
            report_emptied(drive.empty_listed(&Listed { db, seq }))
        }
    }
}

fn report_emptied(result: Result<usize, hbsync_engine::EngineError>) {
    match result {
        Ok(n) => tracing::info!("{n} items deleted for good"),
        Err(e) => tracing::warn!("emptying Recently deleted failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::root::SyncRoot;
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    /// A runner that fails is to start again after a pause, unless its folder was removed or
    /// the account is ending.
    #[test]
    fn a_failed_runner_starts_again_unless_its_folder_went()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(&folder)?;
        // Set up for another identity: its runner fails at once.
        SyncRoot::create(&home.path().join("other.db"), "other.example", &folder)?;
        let db = home.path().join("state.db");
        let drive = Drive::open(
            &db,
            "a.example",
            Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default()))),
        )?;
        let failed = |removed: bool, ending: bool| -> Result<Running, Box<dyn std::error::Error>> {
            let cfg = RunnerConfig::new(&folder, &db, "a.example", "pc");
            let (handle, thread) = Runner::spawn(cfg, Arc::clone(&drive))?;
            let mut r = Running {
                path: folder.clone(),
                handle,
                life: Life::Syncing(thread),
                started: Instant::now(),
                backoff: Backoff::new(),
            };
            if removed {
                r.stop_for_good();
            }
            while matches!(r.life, Life::Syncing(_) | Life::Stopping(_)) {
                std::thread::sleep(Duration::from_millis(10));
                r.ended(ending);
            }
            Ok(r)
        };
        let waiting = |r: &Running| matches!(r.life, Life::Waiting(_));
        let r = failed(false, false)?;
        assert!(waiting(&r));
        let status = r.handle.status();
        assert_eq!(status.state, State::Failing);
        assert!(status.error.is_some());
        assert!(!waiting(&failed(true, false)?), "removed");
        assert!(!waiting(&failed(false, true)?), "the account ends");
        Ok(())
    }

    /// Emptying Recently deleted as listed is refused in the answer for a listing of another DB
    /// (the identity's, made again) or one this DB never gave; one it gave is taken.
    #[test]
    fn a_foreign_listing_is_refused_in_the_answer() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let folder = home.path().join("Homebase");
        let (account, _requests) = Account::new(&paths, "a.example", &folder, None);
        // Its DB, from the drive; its runners running (none needed here).
        let db = paths.account_db("a.example");
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        Drive::open(
            &db,
            "a.example",
            Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default()))),
        )?;
        *lock(&account.phase) = Phase::Running(Vec::new());
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
        let (account, _requests) =
            Account::new(&paths, "a.example", &home.path().join("Homebase"), None);
        let db = paths.account_db("a.example");
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

    /// A ref of a file nobody has: live, or (`deleted`) deleted for good.
    fn a_ref(deleted: bool) -> ChangeRef {
        use hbsync_types::{RemoteFileId, VersionTag};
        let live = ChangeRef {
            file_id: RemoteFileId(uuid::Uuid::new_v4()),
            version: VersionTag(uuid::Uuid::new_v4()),
            deleted: false,
            parent: None,
            removed_at: None,
            changed_at: Some(1),
        };
        if deleted { live.tombstone() } else { live }
    }

    /// Notifications on a drive of their own (a file on it, pulled into the mirror); no
    /// announcement comes but the test's.
    /// The notifications, their puller, the drive, and the file on it.
    type Set = (Notifications, Puller, Arc<Drive>, ChangeRef);

    fn notifications(home: &Path) -> Result<Set, Box<dyn std::error::Error>> {
        use hbsync_remote::{NodeMeta, Remote};
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&home.join("state.db"), "a.example", remote.clone())?;
        let made = remote.create(
            &NodeMeta {
                node: hbsync_types::NodeId::new(),
                parent: hbsync_types::NodeId::ROOT,
                kind: hbsync_types::NodeKind::Folder,
                content: hbsync_types::schema::NodeContent::folder("Docs"),
                removed: false,
            },
            None,
        )?;
        drive.pull(None)?;
        let on_drive = ChangeRef::of(&remote.header(made.at.file_id)?);
        let puller = Puller::spawn(Arc::clone(&drive));
        let n = Notifications::new(puller.clone(), Instant::now());
        Ok((n, puller, drive, on_drive))
    }

    fn wait_for(what: &str, done: impl Fn() -> bool) -> Result<(), Box<dyn std::error::Error>> {
        let until = Instant::now() + Duration::from_secs(10);
        while !done() {
            if Instant::now() > until {
                return Err(format!("timed out waiting until {what}").into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// A hard delete announced is taken within the puller's window, also while paused (it writes
    /// only the account's DB): never held where a pause could lose it.
    #[test]
    fn a_hard_delete_is_taken_even_paused() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, puller, _drive, on_drive) = notifications(home.path())?;
        n.event(
            Event::Changed(Some(on_drive.tombstone())),
            Instant::now(),
            true,
        );
        wait_for("the puller took it", || puller.rounds() == 1)?;
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        assert!(n.waiting().0.is_empty(), "nothing gathered");
        Ok(())
    }

    /// A hard delete gathered when the account ends is taken before its puller ends: only a
    /// crash loses one.
    #[test]
    fn a_hard_delete_gathered_is_taken_when_the_account_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, puller, _drive, on_drive) = notifications(home.path())?;
        n.event(
            Event::Changed(Some(on_drive.tombstone())),
            Instant::now(),
            false,
        );
        let began = Instant::now();
        n.finish();
        assert!(began.elapsed() < GATHER_WINDOW * 3);
        assert_eq!(puller.rounds(), 1, "taken before the end");
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        Ok(())
    }

    /// More live changes than are kept are read from the feed instead.
    #[test]
    fn too_many_live_changes_are_read_instead() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, ..) = notifications(home.path())?;
        let now = Instant::now();
        for _ in 0..MAX_LIVE_REFS {
            n.event(Event::Changed(Some(a_ref(false))), now, true);
        }
        assert_eq!(n.waiting().0.len(), MAX_LIVE_REFS);
        n.event(Event::Changed(Some(a_ref(false))), now, true);
        assert_eq!(n.waiting(), (&[][..], true), "read from the feed instead");
        n.event(Event::Changed(Some(a_ref(false))), now, true);
        assert!(
            n.waiting().0.is_empty(),
            "nothing more is kept: the feed is read"
        );
        Ok(())
    }

    /// A puller whose thread ended (a panic) starts one again: what is asked is never taken on
    /// the asker's thread (the account's loop).
    #[test]
    fn a_puller_whose_thread_ended_starts_one_again() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (_, puller, _drive, on_drive) = notifications(home.path())?;
        puller.end_thread();
        puller.take(vec![on_drive.tombstone()], false);
        wait_for("a new thread took it", || puller.rounds() == 1)?;
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        Ok(())
    }

    /// The hard deletes kept while taking them fails: one per file, and only so many.
    #[test]
    fn hard_deletes_kept_are_one_per_file_and_bounded() {
        let gone = a_ref(true);
        let kept = keep(vec![gone, a_ref(false), gone, gone]);
        assert_eq!(kept, vec![gone]);
        let many: Vec<ChangeRef> = (0..MAX_HELD + 5).map(|_| a_ref(true)).collect();
        assert_eq!(keep(many).len(), MAX_HELD);
    }

    /// The account's timed pause runs out as each folder's does; one without an end doesn't.
    #[test]
    fn the_accounts_timed_pause_runs_out() {
        let mut paused: Option<Until> = Some(Some(100));
        assert!(!pause_ran_out(&mut paused, 99));
        assert_eq!(paused, Some(Some(100)));
        assert!(pause_ran_out(&mut paused, 100));
        assert_eq!(paused, None);
        let mut for_good: Option<Until> = Some(None);
        assert!(!pause_ran_out(&mut for_good, u64::MAX));
        assert_eq!(for_good, Some(None));
        assert!(!pause_ran_out(&mut None, 100));
    }

    /// A drive and the fake remote it reads.
    fn fake_drive(
        home: &Path,
    ) -> Result<(Arc<Drive>, Arc<FakeRemote>), Box<dyn std::error::Error>> {
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&home.join("state.db"), "a.example", remote.clone())?;
        Ok((drive, remote))
    }

    /// The drive's changes count as announced only once the read after connecting worked: not
    /// while it runs, nor after it failed; a lost connection leaves the account behind.
    #[test]
    fn announced_only_once_the_read_after_connecting_worked()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (drive, remote) = fake_drive(home.path())?;
        let puller = Puller::spawn(Arc::clone(&drive));
        let mut n = Notifications::new(puller.clone(), Instant::now());
        remote.faults().latency = Duration::from_millis(300);
        n.event(Event::Connected, Instant::now(), false);
        assert!(!drive.current(), "not before the read");
        wait_for("the read after connecting", || drive.current())?;
        n.event(Event::Lost("gone".into()), Instant::now(), false);
        assert!(!drive.current(), "lost: behind");
        {
            let mut faults = remote.faults();
            faults.latency = Duration::ZERO;
            faults.denied = true;
        }
        let rounds = puller.rounds();
        n.event(Event::Connected, Instant::now(), false);
        wait_for("the read after connecting", || puller.rounds() > rounds)?;
        assert!(!drive.current(), "its read failed");
        Ok(())
    }

    /// Changes announced without saying which are read, and read again past the hold-back with
    /// another page size: a read the server answered from before the write doesn't hide them.
    #[test]
    fn an_unsaid_change_is_read_again() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::Remote;
        let home = tempfile::tempdir()?;
        let (drive, remote) = fake_drive(home.path())?;
        let puller = Puller::spawn_timed(Arc::clone(&drive), Duration::from_millis(20));
        let mut n = Notifications::new(puller, Instant::now());
        n.event(Event::Connected, Instant::now(), false);
        wait_for("the read after connecting", || drive.current())?;
        // The answer to the next read kept from before another client's write.
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        let cursor = db.read(|s| s.account_kv(hbsync_state::keys::REMOTE_CURSOR))?;
        remote
            .server()
            .keep_answer(cursor.as_deref(), drive.next_page_size())?;
        let other = FakeRemote::new(remote.server_handle());
        let made = other.create(
            &hbsync_remote::contract::folder("From b", hbsync_types::NodeId::ROOT),
            None,
        )?;
        n.event(Event::Changed(None), Instant::now(), false);
        n.tick(Instant::now() + Duration::from_secs(5), false);
        let db = Mutex::new(db);
        wait_for("read again", || {
            lock(&db)
                .read(|s| s.mirror_by_file(made.at.file_id))
                .is_ok_and(|m| m.is_some())
        })?;
        Ok(())
    }

    /// An announced change the feed never shows is read for a few times, then given up,
    /// leaving the account behind: every folder's next sync reads the feed.
    #[test]
    fn an_announced_change_never_shown_leaves_the_account_behind()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (drive, remote) = fake_drive(home.path())?;
        let puller = Puller::spawn_timed(Arc::clone(&drive), Duration::from_millis(5));
        let mut n = Notifications::new(puller.clone(), Instant::now());
        n.event(Event::Connected, Instant::now(), false);
        wait_for("the read after connecting", || drive.current())?;
        let pages = || {
            let f = remote.faults();
            f.ref_pages + f.full_pages
        };
        let reads = pages();
        puller.take(vec![a_ref(false)], false);
        wait_for("given up", || !drive.current())?;
        let now = pages();
        assert!(
            now > reads + AWAIT_TRIES as usize,
            "read for {} times",
            now - reads
        );
        Ok(())
    }

    /// A burst of announced changes (a notification each) is taken in rounds of
    /// [`GATHER_MAX`], not one write each; a lone one waits the window at most; a plain read
    /// doesn't wait.
    #[test]
    fn a_burst_is_taken_in_few_rounds() {
        let window = Duration::from_millis(20);
        let (tx, rx) = channel::<Asked>();
        for _ in 0..2000 {
            let _ = tx.send((vec![a_ref(false)], Read::No, None));
        }
        let mut sizes = Vec::new();
        while let Ok(first) = rx.try_recv() {
            let (changes, read, _) = merge(Vec::new(), gathered(first, &rx, window));
            assert_eq!(read, Read::No);
            sizes.push(changes.len());
        }
        assert_eq!(sizes, [GATHER_MAX; 4]);

        let _ = tx.send((vec![a_ref(true)], Read::No, None));
        let began = Instant::now();
        let asks = rx.recv().map(|first| gathered(first, &rx, window));
        assert_eq!(asks.map(|a| a.len()), Ok(1));
        let took = began.elapsed();
        assert!(took >= window && took < window * 50, "{took:?}");

        let began = Instant::now();
        let asks = gathered(
            (Vec::new(), Read::Asked, None),
            &rx,
            Duration::from_secs(60),
        );
        assert_eq!(asks.len(), 1);
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "a plain read goes at once"
        );
    }

    /// The hard deletes of a round are taken first, on their own: taking the live changes
    /// failing doesn't hold them back (they are read from the feed instead).
    #[test]
    fn hard_deletes_are_taken_before_live_changes() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let calls = std::cell::RefCell::new(Vec::new());
        let reads = std::cell::Cell::new(0);
        let left = round(
            vec![live, gone],
            false,
            |c| {
                calls.borrow_mut().push(c.to_vec());
                match c.iter().all(|c| c.deleted) {
                    true => Ok(true),
                    false => Err("timed out".into()),
                }
            },
            || {
                reads.set(reads.get() + 1);
                Ok(())
            },
            |_| Ok(Vec::new()),
            || {},
        );
        assert_eq!(*calls.borrow(), vec![vec![gone], vec![live]]);
        assert!(left.held.is_empty(), "the hard delete was taken");
        assert_eq!(reads.get(), 1, "the live change is read instead");
    }

    /// A take and a plain ask in one round: the changes are taken and the feed is read, once.
    #[test]
    fn a_take_merges_with_a_pull() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let held = vec![gone];
        let asks = [
            (vec![live], Read::No, Some(1)),
            (Vec::new(), Read::Asked, None),
        ];
        let (changes, read, connection) = merge(held, asks);
        assert_eq!(changes, vec![gone, live]);
        assert_eq!(read, Read::Asked);
        assert_eq!(connection, Some(1), "asked while connected");
        assert_eq!(
            merge(Vec::new(), [(vec![live], Read::No, None)]).1,
            Read::No
        );
    }

    /// Taking announced changes that fails keeps the hard deletes for the next round (they
    /// are in no feed) and reads the feed meanwhile; once taken they are gone.
    #[test]
    fn a_hard_delete_survives_a_failed_take() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let reads = std::cell::Cell::new(0);
        let read = || {
            reads.set(reads.get() + 1);
            Ok(())
        };
        let left = round(
            vec![gone, live],
            false,
            |_| Err("the DB is busy".into()),
            read,
            |_| Ok(Vec::new()),
            || {},
        );
        assert_eq!(left.held, vec![gone]);
        assert_eq!(reads.get(), 1, "the live change is read instead");
        let taken = std::cell::RefCell::new(Vec::new());
        let left = round(
            merge(left.held, []).0,
            false,
            |c| {
                taken.borrow_mut().extend_from_slice(c);
                Ok(true)
            },
            read,
            |_| Ok(Vec::new()),
            || {},
        );
        assert_eq!(left, Round::default());
        assert_eq!(*taken.borrow(), vec![gone], "taken the next round");
        assert_eq!(reads.get(), 1, "known: nothing to read");
    }

    /// An announced change a read of the feed didn't bring is kept for another read; one it
    /// brought, or a known one, isn't.
    #[test]
    fn an_announced_change_the_feed_hid_is_read_for_again() {
        let (seen, hidden) = (a_ref(false), a_ref(false));
        let left = round(
            vec![seen, hidden],
            false,
            |_| Ok(false),
            || Ok(()),
            |c| Ok(c.iter().copied().filter(|r| *r == hidden).collect()),
            || {},
        );
        assert_eq!(left.awaited, vec![hidden]);
        assert!(left.held.is_empty() && left.read && !left.behind);
        let left = round(
            vec![seen],
            false,
            |_| Ok(true),
            || Ok(()),
            |_| Ok(vec![seen]),
            || {},
        );
        assert_eq!(left, Round::default(), "known: not read, nothing awaited");
    }

    /// Looking for what a read should have brought that fails leaves the account behind.
    #[test]
    fn a_failed_look_for_announced_changes_is_behind() {
        let left = round(
            vec![a_ref(false)],
            false,
            |_| Ok(false),
            || Ok(()),
            |_| Err("the DB is busy".into()),
            || {},
        );
        assert!(left.read && left.behind && left.awaited.is_empty());
    }

    /// Each awaited change counts its own reads: one announced later gets all its reads, and
    /// one read for [`AWAIT_TRIES`] times is given up.
    #[test]
    fn each_awaited_change_counts_its_own_reads() {
        let (old, late) = (a_ref(false), a_ref(false));
        let (awaited, gave_up) = still_awaited(&[(old, AWAIT_TRIES - 1)], vec![old, late]);
        assert_eq!(awaited, vec![(old, AWAIT_TRIES), (late, 1)]);
        assert_eq!(gave_up, 0);
        let (awaited, gave_up) = still_awaited(&awaited, vec![old, late]);
        assert_eq!(awaited, vec![(late, 2)], "the late one keeps its reads");
        assert_eq!(gave_up, 1);
    }
}
