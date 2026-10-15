//! One account (identity) of the daemon: it waits for the credentials (a keychain locked at
//! logon), connects (retrying while the identity can't be reached: a laptop starts offline),
//! and owns what is the account's: the [`Drive`], the notifications (a change announced is
//! pulled once, which wakes the folders it concerns), Recently deleted and its daily purge.
//! Its folders' runners sync until the daemon stops, or until it starts over with new
//! credentials.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use hbsync_engine::Drive;
use hbsync_engine::root::AccountReader;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_homebase::{Client, HomebaseError};
use hbsync_state::StateError;

use super::DaemonError;
use crate::protocol::{
    AccountControl, AccountStatus, DeletedItem, FolderStatus, Hints, RecentItem, State, Until,
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
        self.requests
            .send(Request::Deleted(control))
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

    /// The newest `limit` items of its Recently deleted, newest first.
    pub fn deleted(&self, limit: usize) -> Vec<DeletedItem> {
        self.read(|db| db.recently_deleted(limit))
            .unwrap_or_default()
            .into_iter()
            .map(|deleted| DeletedItem {
                identity: self.identity.clone(),
                deleted,
            })
            .collect()
    }

    /// `f` on the answers' connection, which only reads, opened on first use; `None` when the
    /// DB isn't there yet or `f` failed.
    fn read<T>(&self, f: impl FnOnce(&mut AccountReader) -> Result<T, StateError>) -> Option<T> {
        let mut reader = lock(&self.reader);
        if reader.is_none() {
            *reader = AccountReader::open(&self.db, &self.identity).ok().flatten();
        }
        f(reader.as_mut()?).ok()
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
        let notifier = Notifier::spawn(client, remote.drive(), tx);
        lock(&self.status).hints = Hints::Connecting;
        let drive = Drive::open(&self.db, &self.identity, remote)
            .map_err(|e| DaemonError::Runner(e.into()))?;
        let mut notifications = Notifications::default();
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
            ) {
                notifier.reconnect();
            }
            while let Ok(event) = events.try_recv() {
                if let Some(hints) = notifications.event(event, &drive) {
                    lock(&self.status).hints = hints;
                }
            }
            notifications.tick(&drive, Instant::now());
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

/// What the identity's notifications do to its drive: a change announced is pulled once the
/// announcements have quieted (a burst of deletes reaches the mass-delete guard whole), which
/// wakes the folders the change concerns; connecting again wakes every folder (changes made
/// meanwhile weren't announced).
#[derive(Debug, Default)]
pub struct Notifications {
    announced: Announced,
}

impl Notifications {
    /// Takes `event`; the account's notification status when it changed.
    pub fn event(&mut self, event: Event, drive: &Drive) -> Option<Hints> {
        match event {
            Event::Connected => {
                drive.wake_all();
                Some(Hints::Connected)
            }
            Event::Changed => {
                self.announced.note(Instant::now());
                None
            }
            Event::Lost(why) => {
                tracing::info!("notifications lost ({why}); reconnecting");
                Some(Hints::Lost(why))
            }
            Event::Refused(why) => {
                tracing::warn!("notifications refused ({why}); syncing on the poll only");
                Some(Hints::Refused(why))
            }
        }
    }

    /// Pulls what was announced, once it has quieted.
    pub fn tick(&mut self, drive: &Drive, now: Instant) {
        if self.announced.due(now)
            && let Err(e) = drive.pull(None)
        {
            tracing::info!("pulling announced changes failed ({e}); the folders' syncs will");
            drive.wake_all();
        }
    }
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
        AccountControl::EmptyDeletedUntil { at_ms } => report_emptied(drive.empty_until(at_ms)),
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
}
