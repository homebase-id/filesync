//! One account (identity) of the daemon: it waits for the credentials (a keychain locked at
//! logon), connects (retrying while the identity can't be reached: a laptop starts offline),
//! and owns what is the account's: the [`Drive`], the notifier (whose hints it passes to each
//! folder's runner), Recently deleted and its daily purge. Its folders' runners sync until
//! the daemon stops.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use hbsync_engine::Drive;
use hbsync_engine::root::SyncRoot;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_homebase::{Client, HomebaseError};

use super::{DaemonError, wait};
use crate::protocol::{AccountControl, AccountStatus, FolderStatus, Hints, State, Until};
use crate::runner::{
    Backoff, Command, Handle, Hint, Runner, RunnerConfig, RunnerError, lock, slept,
};
use crate::{Config, CredentialError, CredentialStore, Location, Paths};

/// How often the account looks at its notifier, its requests and its runners.
const TICK: Duration = Duration::from_millis(200);
/// How often the purge of Recently deleted is looked at (it runs once a day).
const PURGE_LOOK: Duration = Duration::from_secs(600);

/// An account of the daemon, shared with the control socket's thread.
pub(crate) struct Account {
    pub identity: String,
    /// Its folders here.
    pub folders: Vec<PathBuf>,
    /// Its state DB.
    db: PathBuf,
    status: Mutex<AccountStatus>,
    phase: Mutex<Phase>,
    /// Requests for its Recently deleted, run on its thread.
    requests: Mutex<Sender<AccountControl>>,
}

/// Before its runners start, the account still answers.
enum Phase {
    /// Waiting for the credentials or the identity.
    Waiting {
        state: State,
        error: Option<String>,
        /// Kept in the config too: the runners start paused.
        paused: Option<Until>,
    },
    Running(Vec<(PathBuf, Handle)>),
}

impl Account {
    /// The account `identity` syncing `folders`, started paused when `paused`; its thread runs
    /// with the returned receiver ([`Account::run`]).
    pub fn new(
        paths: &Paths,
        identity: &str,
        folders: Vec<PathBuf>,
        paused: Option<Until>,
    ) -> (Arc<Account>, Receiver<AccountControl>) {
        let (tx, rx) = channel();
        let account = Account {
            identity: identity.to_owned(),
            folders,
            db: paths.account_db(identity),
            status: Mutex::new(AccountStatus::new(identity)),
            phase: Mutex::new(Phase::Waiting {
                state: State::Starting,
                error: None,
                paused,
            }),
            requests: Mutex::new(tx),
        };
        (Arc::new(account), rx)
    }

    pub fn db(&self) -> &PathBuf {
        &self.db
    }

    /// Its status and its folders'.
    pub fn status(&self) -> (AccountStatus, Vec<FolderStatus>) {
        let folders = match &*lock(&self.phase) {
            Phase::Running(runners) => runners.iter().map(|(_, h)| h.status()).collect(),
            Phase::Waiting {
                state,
                error,
                paused,
            } => self
                .folders
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
                .collect(),
        };
        (lock(&self.status).clone(), folders)
    }

    /// The runner of its folder at `path`, once running.
    pub fn runner(&self, path: &PathBuf) -> Option<Handle> {
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
        let mut phase = lock(&self.phase);
        match &mut *phase {
            Phase::Running(runners) => {
                let command = match pause {
                    Some(until) => Command::Pause { until },
                    None => Command::Resume,
                };
                for (_, h) in runners.iter() {
                    h.send(command.clone());
                }
            }
            Phase::Waiting { paused, .. } => *paused = pause,
        }
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
        lock(&self.requests)
            .send(control)
            .map_err(|_| "the account has stopped".to_owned())
    }

    fn waiting(&self, to: State, why: String) {
        if let Phase::Waiting { state, error, .. } = &mut *lock(&self.phase) {
            *state = to;
            *error = Some(why);
        }
    }

    /// Connects and syncs its folders until `stop`, then stops them. Returns the first error
    /// a folder's runner ended with.
    pub fn run(
        &self,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<AccountControl>,
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
        let drive = Drive::new(
            SyncRoot::open_account(&self.db, &self.identity)
                .map_err(|e| DaemonError::Runner(e.into()))?,
            remote,
        );
        let (runners, mut hints, mut threads) = {
            // Under the lock: no pause gets lost between the two phases.
            let mut phase = lock(&self.phase);
            let paused = match &*phase {
                Phase::Waiting { paused, .. } => *paused,
                Phase::Running(_) => None,
            };
            let mut runners = Vec::new();
            let (mut hints, mut threads) = (Vec::new(), Vec::new());
            for folder in &self.folders {
                let mut cfg =
                    RunnerConfig::new(folder, &self.db, &self.identity, &config.device_name());
                cfg.mass_delete = config.mass_delete;
                cfg.paused = paused;
                let (tx, rx) = channel();
                let (handle, thread) = Runner::spawn(cfg, Arc::clone(&drive), rx)?;
                runners.push((folder.clone(), handle));
                hints.push(tx);
                threads.push(thread);
            }
            *phase = Phase::Running(runners.clone());
            (runners, hints, threads)
        };

        let keep_days = config.keep_deleted_days.0;
        let mut next_purge = Instant::now();
        let mut result = Ok(());
        loop {
            let (wall, mono) = (SystemTime::now(), Instant::now());
            std::thread::sleep(TICK);
            if stop.load(Ordering::Relaxed) {
                for (_, h) in &runners {
                    h.send(Command::Stop);
                }
            }
            if slept(
                SystemTime::now().duration_since(wall).ok(),
                mono.elapsed(),
                false,
            ) {
                notifier.reconnect();
            }
            self.pass_hints(&events, &mut hints);
            while let Ok(control) = requests.try_recv() {
                run_request(&drive, control);
            }
            if Instant::now() >= next_purge {
                next_purge = Instant::now() + PURGE_LOOK;
                match drive.purge_when_due(keep_days) {
                    Ok(Some(n)) if n > 0 => {
                        tracing::info!("{n} items deleted for good after {keep_days} days")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("purging Recently deleted failed: {e}"),
                }
            }
            // A runner that ended takes its folder out; the account ends with the last.
            let (ended, running): (Vec<_>, Vec<_>) =
                threads.into_iter().partition(JoinHandle::is_finished);
            threads = running;
            for thread in ended {
                let ended = thread.join().map_err(|_| RunnerError::Panicked);
                if let Err(e) = ended.and_then(|r| r)
                    && result.is_ok()
                {
                    result = Err(DaemonError::Runner(e));
                }
            }
            if threads.is_empty() {
                return result;
            }
        }
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
                    if wait(wait_for, stop) {
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
                    if wait(wait_for, stop) {
                        return None;
                    }
                }
            }
        }
    }

    /// The notifier's events: the account's status, and hints for each folder's runner.
    fn pass_hints(&self, events: &Receiver<Event>, runners: &mut Vec<Sender<Hint>>) {
        while let Ok(event) = events.try_recv() {
            let (hint, hints) = match event {
                Event::Connected => (Some(Hint::Connected), Some(Hints::Connected)),
                Event::Changed => (Some(Hint::Changed), None),
                Event::Lost(why) => {
                    tracing::info!("notifications lost ({why}); reconnecting");
                    (None, Some(Hints::Lost(why)))
                }
                Event::Refused(why) => {
                    tracing::warn!("notifications refused ({why}); syncing on the poll only");
                    (None, Some(Hints::Refused(why)))
                }
            };
            if let Some(hints) = hints {
                lock(&self.status).hints = hints;
            }
            if let Some(hint) = hint {
                runners.retain(|tx| tx.send(hint).is_ok());
            }
        }
    }
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
