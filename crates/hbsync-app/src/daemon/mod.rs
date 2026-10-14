//! The daemon: one per user, syncing the configured folders until it is stopped.
//!
//! It takes the one-daemon lock, reads the config, answers on the control socket
//! ([`answer`]), and runs each account ([`account`]: its credentials, connection, drive and
//! folders) until a stop request or signal.

mod account;
mod answer;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_homebase::HomebaseError;
use hbsync_types::unix_now;

use crate::ipc;
use crate::lock::{InstanceLock, LockError};
use crate::runner::{RunnerError, lock};
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

/// Runs the daemon until a stop request, or until `stop` is set (a signal). It ends when its
/// last account has (one failing stops them all: the daemon is restarted whole).
pub fn run(paths: &Paths, stop: Arc<AtomicBool>) -> Result<(), DaemonError> {
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
    });
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
    {
        let daemon = Arc::clone(&daemon);
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || ipc::serve(listener, move |r| answer::answer(r, &daemon)))
            .map_err(DaemonError::Ipc)?;
    }
    let mut result = Ok(());
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let (ended, left) = daemon.reap();
        if let Err(e) = ended {
            daemon.stop.store(true, Ordering::Relaxed);
            if result.is_ok() {
                result = Err(e);
            }
        }
        if left == 0 {
            break;
        }
    }
    tracing::info!("daemon stopped");
    result
}

/// The daemon's accounts, each on a thread of its own; folders and accounts come and go while
/// it runs.
pub(crate) struct Daemon {
    paths: Paths,
    stop: Arc<AtomicBool>,
    accounts: Mutex<Vec<(Arc<Account>, AccountThread)>>,
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
    /// in a new one.
    fn add_folder(&self, identity: &str, path: &Path) -> Result<(), DaemonError> {
        if let Some(account) = self.accounts().into_iter().find(|a| a.identity == identity) {
            account.add_folder(path);
            return Ok(());
        }
        // A pause that ended while the daemon wasn't running is over.
        let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
        let (account, requests) =
            Account::new(&self.paths, identity, path, config.pause(unix_now()));
        let thread = {
            let (account, paths, stop) = (
                Arc::clone(&account),
                self.paths.clone(),
                Arc::clone(&self.stop),
            );
            std::thread::Builder::new()
                .name(format!("hbsync-account-{identity}"))
                .spawn(move || account.run(&paths, &config, &requests, &stop))
                .map_err(DaemonError::Ipc)?
        };
        lock(&self.accounts).push((account, thread));
        Ok(())
    }

    /// Joins the accounts that ended: the first error among them, and how many still run.
    fn reap(&self) -> (Result<(), DaemonError>, usize) {
        let mut accounts = lock(&self.accounts);
        let (ended, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *accounts)
            .into_iter()
            .partition(|(_, thread)| thread.is_finished());
        *accounts = running;
        let mut result = Ok(());
        for (_, thread) in ended {
            let ended = thread
                .join()
                .unwrap_or(Err(DaemonError::Runner(RunnerError::Panicked)));
            if result.is_ok() {
                result = ended;
            }
        }
        (result, accounts.len())
    }
}

/// Waits `time`, or less if `stop` is set meanwhile. Returns whether it was.
fn wait(time: Duration, stop: &AtomicBool) -> bool {
    let until = Instant::now() + time;
    while Instant::now() < until && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
    }
    stop.load(Ordering::Relaxed)
}
