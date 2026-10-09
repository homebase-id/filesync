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
    Control, FIRST_BACKOFF, Handle, HintSource, MAX_BACKOFF, Runner, RunnerConfig, RunnerError,
    State, Status, lock,
};
use crate::{Config, ConfigError, CredentialError, CredentialStore, Location, Paths, Root};

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
    /// Waiting for the credentials (a locked keychain) or the identity.
    Waiting {
        state: State,
        error: Option<String>,
        warning: Option<String>,
    },
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

    let phase = Arc::new(Mutex::new(Phase::Waiting {
        state: State::Starting,
        error: None,
        warning: None,
    }));
    let set = |state: State, error: String, warning: &Option<String>| {
        *lock(&phase) = Phase::Waiting {
            state,
            error: Some(error),
            warning: warning.clone(),
        };
    };
    let listener = ipc::listen(paths).map_err(DaemonError::Ipc)?;
    {
        let (phase, stop, root, paths) = (
            Arc::clone(&phase),
            Arc::clone(&stop),
            root.clone(),
            paths.clone(),
        );
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || ipc::serve(listener, move |r| answer(r, &phase, &stop, &root, &paths)))
            .map_err(DaemonError::Ipc)?;
    }
    tracing::info!(
        "daemon {} started for {} at {}",
        env!("CARGO_PKG_VERSION"),
        root.identity,
        root.folder.display()
    );

    // The credentials: a keychain locked at logon is waited for, not taken for "not set up".
    let store = CredentialStore::open(paths);
    let mut backoff = FIRST_BACKOFF;
    let (creds, location) = loop {
        match store.load(&root.identity) {
            Ok(Some(found)) => break found,
            Ok(None) => {
                return Err(DaemonError::NotSetUp(format!(
                    "no credentials for {}",
                    root.identity
                )));
            }
            Err(CredentialError::Keychain(why)) => {
                tracing::warn!("can't read the keychain ({why}); trying again in {backoff:?}");
                set(
                    State::Failing,
                    format!("can't read the keychain: {why}"),
                    &None,
                );
                if wait(backoff, &stop) {
                    return Ok(());
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    };
    let warning = match location {
        Location::Keychain => None,
        Location::File { path, why } => {
            tracing::warn!("credentials in {} (no keychain: {why})", path.display());
            Some(format!("kept in {} (no keychain: {why})", path.display()))
        }
    };
    if let Phase::Waiting { warning: w, .. } = &mut *lock(&phase) {
        w.clone_from(&warning);
    }

    // Connect: finding the drive takes a request.
    let client = Client::new(&creds)?;
    let mut backoff = FIRST_BACKOFF;
    let remote = loop {
        match HomebaseRemote::connect(client.clone()) {
            Ok(r) => break Arc::new(r),
            Err(e) => {
                // A refusal won't change until this computer is authorized again (which
                // restarts the daemon): ask rarely.
                let (state, wait_for) = match &e {
                    HomebaseError::Denied(_) => (State::Denied, MAX_BACKOFF),
                    HomebaseError::Network(_) => (State::Offline, backoff),
                    _ => (State::Failing, backoff),
                };
                tracing::warn!(
                    "can't connect to {} ({e}); trying again in {wait_for:?}",
                    root.identity
                );
                set(state, e.to_string(), &warning);
                if wait(wait_for, &stop) {
                    return Ok(());
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    };
    let (tx, rx) = channel();
    let hints = HintSource {
        notifier: Some(Notifier::spawn(client, remote.drive(), tx)),
        events: rx,
    };
    // The config again: a pause may have come in meanwhile.
    let config = Config::load(&paths.config_file())?.unwrap_or(config);
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
    paths: &Paths,
) -> Reply {
    let phase = lock(phase);
    match (request.ask, &*phase) {
        (Ask::Status, Phase::Running(h)) => Reply::Status(Box::new(h.status())),
        (
            Ask::Status,
            Phase::Waiting {
                state,
                error,
                warning,
            },
        ) => {
            let mut s = Status::new(&root.identity, &root.folder);
            s.state = *state;
            if Config::load(&paths.config_file()).is_ok_and(|c| c.is_some_and(|c| c.paused)) {
                s.state = State::Paused;
            }
            s.error.clone_from(error);
            s.credentials_warning.clone_from(warning);
            Reply::Status(Box::new(s))
        }
        (Ask::Control(Control::Stop), phase) => {
            stop.store(true, Ordering::Relaxed);
            if let Phase::Running(h) = phase {
                h.send(Control::Stop);
            }
            Reply::Done
        }
        (Ask::Control(Control::ConfirmDeletes { token }), Phase::Running(h))
            if h.status().held_deletes.map(|held| held.token) != Some(token.clone()) =>
        {
            Reply::Refused(
                "the held deletes changed since you looked: check them again with \
                 `hbsync confirm-deletes`"
                    .into(),
            )
        }
        (Ask::Control(c @ (Control::Pause | Control::Resume)), phase) => {
            // Pausing survives a restart, and is kept until the sync starts.
            if let Err(e) = remember_paused(paths, c == Control::Pause) {
                return Reply::Refused(format!("couldn't save the pause in the config: {e}"));
            }
            match phase {
                Phase::Running(h) if !h.send(c) => Reply::Refused("the sync has stopped".into()),
                _ => Reply::Done,
            }
        }
        (Ask::Control(c), Phase::Running(h)) => {
            if h.send(c) {
                Reply::Done
            } else {
                Reply::Refused("the sync has stopped".into())
            }
        }
        (Ask::Control(_), Phase::Waiting { error, .. }) => Reply::Refused(format!(
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

/// Waits `time`, or less if `stop` is set meanwhile. Returns whether it was.
fn wait(time: Duration, stop: &AtomicBool) -> bool {
    let until = Instant::now() + time;
    while Instant::now() < until && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
    }
    stop.load(Ordering::Relaxed)
}
