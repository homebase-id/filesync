//! The daemon: one per user, syncing the configured folders until it is stopped.
//!
//! It takes the one-daemon lock, reads the config, answers on the control socket
//! ([`answer`]), and runs each account ([`account`]: its credentials, connection, drive and
//! folders) until a stop request or signal.

mod account;
mod answer;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_homebase::HomebaseError;
use hbsync_types::unix_now;

use crate::ipc;
use crate::lock::{InstanceLock, LockError};
use crate::runner::RunnerError;
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

/// Runs the daemon until a stop request, or until `stop` is set (a signal).
pub fn run(paths: &Paths, stop: Arc<AtomicBool>) -> Result<(), DaemonError> {
    let _lock = InstanceLock::acquire(paths)?;
    let config = Config::load(&paths.config_file())?
        .ok_or_else(|| DaemonError::NotSetUp("no config".into()))?;
    if config.roots.is_empty() {
        return Err(DaemonError::NotSetUp("no folder to sync".into()));
    }
    // A pause that ended while the daemon wasn't running is over.
    let paused = config.pause(unix_now());
    let mut identities: Vec<&str> = config.roots.iter().map(|r| r.identity.as_str()).collect();
    identities.dedup();
    let accounts: Vec<_> = identities
        .into_iter()
        .map(|identity| {
            let folders = config
                .roots
                .iter()
                .filter(|r| r.identity == identity)
                .map(|r| r.folder.clone())
                .collect();
            Account::new(paths, identity, folders, paused)
        })
        .collect();
    let shared: Vec<Arc<Account>> = accounts.iter().map(|(a, _)| Arc::clone(a)).collect();

    let listener = ipc::listen(paths).map_err(DaemonError::Ipc)?;
    {
        let (stop, paths) = (Arc::clone(&stop), paths.clone());
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || ipc::serve(listener, move |r| answer::answer(r, &shared, &stop, &paths)))
            .map_err(DaemonError::Ipc)?;
    }
    for root in &config.roots {
        tracing::info!(
            "daemon {} started for {} at {}",
            crate::VERSION,
            root.identity,
            root.folder.display()
        );
    }

    let results: Vec<Result<(), DaemonError>> = std::thread::scope(|scope| {
        let (config, stop) = (&config, &*stop);
        let threads: Vec<_> = accounts
            .into_iter()
            .map(|(account, requests)| {
                scope.spawn(move || {
                    let result = account.run(paths, config, &requests, stop);
                    // One account failing stops the daemon: it is restarted whole.
                    if result.is_err() {
                        stop.store(true, Ordering::Relaxed);
                    }
                    result
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap_or(Err(RunnerError::Panicked.into())))
            .collect()
    });
    tracing::info!("daemon stopped");
    results.into_iter().collect()
}

/// Waits `time`, or less if `stop` is set meanwhile. Returns whether it was.
fn wait(time: Duration, stop: &AtomicBool) -> bool {
    let until = Instant::now() + time;
    while Instant::now() < until && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
    }
    stop.load(Ordering::Relaxed)
}
