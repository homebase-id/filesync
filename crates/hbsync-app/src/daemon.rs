//! The daemon: one per user, syncing the configured folder until it is stopped.
//!
//! It takes the one-daemon lock, reads the config and the credentials, answers on the control
//! socket, connects to the identity (retrying while it can't be reached: a laptop starts
//! offline), and runs the [`Runner`] until a stop request or signal.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::Notifier;
use hbsync_homebase::{Client, HomebaseError};

use crate::ipc::{self, Ask, Reply, Request};
use crate::lock::{InstanceLock, LockError};
use crate::runner::{
    Control, Handle, HintSource, Runner, RunnerConfig, RunnerError, State, Status,
};
use crate::{Config, ConfigError, CredentialError, CredentialStore, Location, Paths, Root};

const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

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

/// Before the runner starts, the control socket still answers.
enum Phase {
    Connecting { error: Option<String> },
    Running(Handle),
}

/// Runs the daemon until a stop request, or until `stop` is set (a signal).
pub fn run(paths: &Paths, stop: Arc<AtomicBool>) -> Result<(), DaemonError> {
    let _lock = InstanceLock::acquire(paths)?;
    let config = Config::load(&paths.config_file())?
        .ok_or_else(|| DaemonError::NotSetUp("no config".into()))?;
    let root = config
        .root()?
        .ok_or_else(|| DaemonError::NotSetUp("no folder to sync".into()))?
        .clone();
    let store = CredentialStore::open(paths);
    let (creds, location) = store
        .load(&root.identity)?
        .ok_or_else(|| DaemonError::NotSetUp(format!("no credentials for {}", root.identity)))?;
    let warning = match location {
        Location::Keychain => None,
        Location::File { path, why } => {
            tracing::warn!("credentials in {} (no keychain: {why})", path.display());
            Some(format!("kept in {} (no keychain: {why})", path.display()))
        }
    };

    let phase = Arc::new(Mutex::new(Phase::Connecting { error: None }));
    let listener = ipc::listen(paths).map_err(DaemonError::Ipc)?;
    {
        let (phase, stop, root, warning, paths) = (
            Arc::clone(&phase),
            Arc::clone(&stop),
            root.clone(),
            warning.clone(),
            paths.clone(),
        );
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || {
                ipc::serve(listener, move |r| {
                    answer(r, &phase, &stop, &root, warning.as_deref(), &paths)
                })
            })
            .map_err(DaemonError::Ipc)?;
    }
    tracing::info!(
        "daemon {} started for {} at {}",
        env!("CARGO_PKG_VERSION"),
        root.identity,
        root.folder.display()
    );

    // Connect: finding the drive takes a request.
    let client = Client::new(&creds)?;
    let mut backoff = FIRST_BACKOFF;
    let remote = loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        match HomebaseRemote::connect(client.clone()) {
            Ok(r) => break Arc::new(r),
            Err(e) => {
                tracing::warn!(
                    "can't reach {} ({e}); trying again in {backoff:?}",
                    root.identity
                );
                *lock(&phase) = Phase::Connecting {
                    error: Some(e.to_string()),
                };
                wait(backoff, &stop);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    };
    let (tx, rx) = channel();
    let hints = HintSource {
        notifier: Some(Notifier::spawn(client, remote.drive(), tx)),
        events: rx,
    };
    let mut cfg = RunnerConfig::new(&root.folder, &root.identity, &config.device_name());
    cfg.mass_delete = config.mass_delete;
    cfg.paused = config.paused;
    cfg.credentials_warning = warning;
    let (handle, thread) = Runner::spawn(cfg, remote, hints)?;
    *lock(&phase) = Phase::Running(handle.clone());

    while !thread.is_finished() {
        if stop.load(Ordering::Relaxed) {
            handle.send(Control::Stop);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let result = thread.join().map_err(|_| RunnerError::Panicked)?;
    tracing::info!("daemon stopped");
    Ok(result?)
}

fn answer(
    request: Request,
    phase: &Mutex<Phase>,
    stop: &AtomicBool,
    root: &Root,
    warning: Option<&str>,
    paths: &Paths,
) -> Reply {
    let phase = lock(phase);
    match (request.ask, &*phase) {
        (Ask::Status, Phase::Running(h)) => Reply::Status(Box::new(h.status())),
        (Ask::Status, Phase::Connecting { error }) => {
            let mut s = Status::new(&root.identity, &root.folder);
            if error.is_some() {
                s.state = State::Offline;
            }
            s.error.clone_from(error);
            s.credentials_warning = warning.map(str::to_owned);
            Reply::Status(Box::new(s))
        }
        (Ask::Control(Control::Stop), phase) => {
            stop.store(true, Ordering::Relaxed);
            if let Phase::Running(h) = phase {
                h.send(Control::Stop);
            }
            Reply::Done
        }
        (Ask::Control(c), Phase::Running(h)) => {
            // Pausing survives a restart.
            let paused = match c {
                Control::Pause => Some(true),
                Control::Resume => Some(false),
                _ => None,
            };
            if let Some(paused) = paused
                && let Err(e) = remember_paused(paths, paused)
            {
                tracing::warn!("couldn't save the pause in the config: {e}");
            }
            if h.send(c) {
                Reply::Done
            } else {
                Reply::Refused("the sync has stopped".into())
            }
        }
        (Ask::Control(_), Phase::Connecting { error }) => Reply::Refused(format!(
            "not connected to {} yet{}",
            root.identity,
            error.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
        )),
    }
}

fn remember_paused(paths: &Paths, paused: bool) -> Result<(), ConfigError> {
    let path = paths.config_file();
    let mut config = Config::load(&path)?.unwrap_or_default();
    if config.paused != paused {
        config.paused = paused;
        config.save(&path)?;
    }
    Ok(())
}

/// Waits `time`, or less if `stop` is set meanwhile.
fn wait(time: Duration, stop: &AtomicBool) {
    let until = Instant::now() + time;
    while Instant::now() < until && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
