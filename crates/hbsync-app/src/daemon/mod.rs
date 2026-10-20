//! The daemon: one per user, syncing the configured folders until it is stopped.
//!
//! It takes the one-daemon lock, reads the config, answers on the control socket
//! ([`answer`]), and runs each account ([`account`]: its credentials, connection, drive and
//! folders) until a stop request or signal. Its threads block until there is something to do:
//! nothing wakes it on a timer while nothing happens (but [`sleep`]'s poll off Linux).

mod account;
mod answer;
mod keeper;
mod sleep;
mod watch;

pub use account::{Notifications, Puller, UNANNOUNCED_READ};

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hbsync_homebase::HomebaseError;
use hbsync_types::unix_now;

use crate::ipc;
use crate::lock::{InstanceLock, LockError};
use crate::runner::{Command, RunnerError};
use crate::sync::{OnDrop, WAIT_AT_MOST, Wakeup, lock};
use crate::{Config, ConfigError, CredentialError, Paths};
use account::Account;

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// Nothing to run: `hbsync setup` hasn't been run (or was undone).
    #[error("{0}: run `hbsync setup` first")]
    NotSetUp(String),
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error(transparent)]
    Homebase(#[from] HomebaseError),
    #[error(transparent)]
    Runner(#[from] RunnerError),
    #[error("the control socket: {0}")]
    Ipc(std::io::Error),
}

/// Stops the daemon from another thread (a signal handler's).
#[derive(Debug, Clone, Default)]
pub struct Stop {
    flag: Arc<AtomicBool>,
    wakeup: Arc<Wakeup>,
}

impl Stop {
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
        self.wakeup.raise();
    }

    fn stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

/// Runs the daemon until a stop request, or until `stop` (a signal). It ends when its last
/// account has (one failing stops them all: the daemon is restarted whole).
pub fn run(paths: &Paths, stop: Stop) -> Result<(), DaemonError> {
    let _lock = InstanceLock::acquire(paths)?;
    let config = Config::load(&paths.config_file())?
        .ok_or_else(|| DaemonError::NotSetUp("no config".into()))?;
    if config.roots.is_empty() {
        return Err(DaemonError::NotSetUp("no folder to sync".into()));
    }
    let daemon = Arc::new(Daemon {
        paths: paths.clone(),
        stop,
        accounts: Mutex::default(),
        starting_over: AtomicUsize::new(0),
        changed: Arc::default(),
    });
    {
        let daemon = Arc::downgrade(&daemon);
        std::thread::Builder::new()
            .name("hbsync-sleep".into())
            .spawn(move || {
                sleep::watch(|| {
                    let Some(daemon) = daemon.upgrade() else {
                        return false;
                    };
                    tracing::info!("the machine woke up");
                    daemon.accounts().iter().for_each(|a| a.woke());
                    true
                })
            })
            .map_err(DaemonError::Ipc)?;
    }
    for root in &config.roots {
        daemon.add_folder(&root.identity, &root.folder)?;
        tracing::info!(
            "daemon {} started for {} at {}",
            crate::VERSION,
            root.identity,
            root.folder.display()
        );
    }
    let listener = ipc::listen(paths).map_err(DaemonError::Ipc)?;
    let watchers = Arc::new(watch::Watchers::new(Arc::clone(&daemon.changed)));
    {
        let (weak, watchers) = (Arc::downgrade(&daemon), Arc::clone(&watchers));
        std::thread::Builder::new()
            .name("hbsync-status".into())
            .spawn(move || watch::push(weak, watchers))
            .map_err(DaemonError::Ipc)?;
    }
    {
        let daemon = Arc::clone(&daemon);
        let watch = move |stream| watchers.add(stream);
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || ipc::serve(listener, move |r| answer::answer(r, &daemon), watch))
            .map_err(DaemonError::Ipc)?;
    }
    let mut result = Ok(());
    let (mut rung, mut stopping) = (0, false);
    loop {
        let (ended, left) = daemon.reap();
        if let Err(e) = ended {
            daemon.stop.stop();
            if result.is_ok() {
                result = Err(e);
            }
        }
        if daemon.stop.stopped() && !stopping {
            stopping = true;
            daemon.stop_all();
        }
        if left == 0 && daemon.starting_over.load(Ordering::SeqCst) == 0 {
            break;
        }
        rung = daemon.stop.wakeup.wait(rung, WAIT_AT_MOST);
    }
    tracing::info!("daemon stopped");
    result
}

/// The daemon's accounts, each on a thread of its own; folders and accounts come and go while
/// it runs.
pub(crate) struct Daemon {
    paths: Paths,
    /// Set to stop; its wakeup is raised when an account ends too.
    stop: Stop,
    accounts: Mutex<Vec<(Arc<Account>, AccountThread)>>,
    /// Accounts starting over ([`Daemon::start_over`]): the daemon doesn't end meanwhile.
    starting_over: AtomicUsize,
    /// Raised when the status may have changed ([`watch`]).
    changed: Arc<Wakeup>,
}

type AccountThread = JoinHandle<Result<(), DaemonError>>;

impl Daemon {
    /// The accounts running.
    fn accounts(&self) -> Vec<Arc<Account>> {
        lock(&self.accounts)
            .iter()
            .filter(|(_, thread)| !thread.is_finished())
            .map(|(a, _)| Arc::clone(a))
            .collect()
    }

    /// Syncs the folder at `path` (set up, in the config) with `identity`: in its account, or
    /// in a new one. Under one lock: two requests for a new identity start one account.
    fn add_folder(&self, identity: &str, path: &Path) -> Result<(), DaemonError> {
        let mut accounts = lock(&self.accounts);
        if let Some((account, _)) = accounts
            .iter()
            .find(|(a, thread)| a.identity == identity && !thread.is_finished())
        {
            account.add_folder(path);
            return Ok(());
        }
        // A pause that ended while the daemon wasn't running is over.
        let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
        let (account, requests) = Account::new(
            &self.paths,
            identity,
            path,
            config.pause(unix_now()),
            Arc::clone(&self.changed),
        );
        let thread = {
            let (account, paths, stop) =
                (Arc::clone(&account), self.paths.clone(), self.stop.clone());
            std::thread::Builder::new()
                .name(format!("hbsync-account-{identity}"))
                .spawn(move || {
                    // Told also when it panics.
                    let _ended = OnDrop(Some(|| {
                        account.done.store(true, Ordering::SeqCst);
                        stop.wakeup.raise();
                    }));
                    account.run(&paths, &config, &requests, &stop.flag)
                })
                .map_err(DaemonError::Ipc)?
        };
        accounts.push((account, thread));
        self.changed.raise();
        Ok(())
    }

    /// Starts the account `identity` over (its credentials changed): it stops its folders
    /// between operations, then syncs them again with the new credentials.
    fn start_over(&self, identity: &str) -> Result<(), DaemonError> {
        let found = {
            let mut accounts = lock(&self.accounts);
            let at = accounts.iter().position(|(a, _)| a.identity == identity);
            at.map(|at| accounts.remove(at))
        };
        let Some((account, thread)) = found else {
            return Ok(());
        };
        self.starting_over.fetch_add(1, Ordering::SeqCst);
        // Its folders as they are now: those of an ending account go as their runners stop.
        let folders = account.folders();
        account.end();
        let ended = thread
            .join()
            .unwrap_or(Err(DaemonError::Runner(RunnerError::Panicked)));
        let again = folders
            .iter()
            .try_for_each(|folder| self.add_folder(identity, folder));
        self.starting_over.fetch_sub(1, Ordering::SeqCst);
        self.stop.wakeup.raise();
        ended.and(again)
    }

    /// Stops every account (the stop flag set): their runners stop between operations.
    fn stop_all(&self) {
        for a in self.accounts() {
            a.send_all(&Command::Stop);
            a.wake();
        }
    }

    /// Joins the accounts that ended (or are ending: their thread is about to): the first error
    /// among them, and how many still run.
    fn reap(&self) -> (Result<(), DaemonError>, usize) {
        let mut accounts = lock(&self.accounts);
        let ended = |(a, t): &(Arc<Account>, AccountThread)| {
            t.is_finished() || a.done.load(Ordering::SeqCst)
        };
        let before = accounts.len();
        let result = join_finished(&mut accounts, ended, |(_, t)| t);
        if accounts.len() != before {
            self.changed.raise();
        }
        (result, accounts.len())
    }
}

/// Joins the threads of `items` that ended (`finished` tells, `thread` takes an item's out)
/// and keeps the rest: the first error among them.
pub(crate) fn join_finished<T, E: From<RunnerError>>(
    items: &mut Vec<T>,
    finished: impl Fn(&T) -> bool,
    thread: impl Fn(T) -> JoinHandle<Result<(), E>>,
) -> Result<(), E> {
    let (ended, running): (Vec<_>, Vec<_>) = std::mem::take(items).into_iter().partition(finished);
    *items = running;
    let mut result = Ok(());
    for item in ended {
        let ended = thread(item)
            .join()
            .unwrap_or_else(|_| Err(RunnerError::Panicked.into()));
        if result.is_ok() {
            result = ended;
        }
    }
    result
}
