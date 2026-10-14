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
use hbsync_local::state_db_path;
use hbsync_state::StateDb;
use hbsync_types::unix_now;

use crate::ipc::{self, Ask, Reply, Request};
use crate::lock::{InstanceLock, LockError};
use crate::runner::{
    Backoff, Control, Handle, HintSource, Runner, RunnerConfig, RunnerError, State, Status, Until,
    lock,
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
        /// Kept in the config too: the runner starts paused.
        paused: Option<Until>,
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
        // A pause that ended while the daemon wasn't running is over.
        paused: config.pause(unix_now()),
    }));
    let set = |to: State, why: String| {
        if let Phase::Waiting { state, error, .. } = &mut *lock(&phase) {
            *state = to;
            *error = Some(why);
        }
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
        crate::VERSION,
        root.identity,
        root.folder.display()
    );

    // The credentials: a keychain locked at logon is waited for, not taken for "not set up".
    let store = CredentialStore::open(paths);
    let mut backoff = Backoff::new();
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
                let wait_for = backoff.after(State::Failing);
                tracing::warn!("can't read the keychain ({why}); trying again in {wait_for:?}");
                set(State::Failing, format!("can't read the keychain: {why}"));
                if wait(wait_for, &stop) {
                    return Ok(());
                }
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
    let mut backoff = Backoff::new();
    let remote = loop {
        match HomebaseRemote::connect(client.clone()) {
            Ok(r) => break Arc::new(r),
            Err(e) => {
                let state = match &e {
                    HomebaseError::Denied(_) => State::Denied,
                    HomebaseError::Network(_) => State::Offline,
                    _ => State::Failing,
                };
                let wait_for = backoff.after(state);
                tracing::warn!(
                    "can't connect to {} ({e}); trying again in {wait_for:?}",
                    root.identity
                );
                set(state, e.to_string());
                if wait(wait_for, &stop) {
                    return Ok(());
                }
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
    cfg.keep_deleted_days = config.keep_deleted_days.0;
    cfg.credentials_warning = warning;
    let (handle, thread) = {
        // Under the lock: no pause gets lost between the two phases.
        let mut phase = lock(&phase);
        if let Phase::Waiting { paused, .. } = &*phase {
            cfg.paused = *paused;
        }
        let spawned = Runner::spawn(cfg, remote.clone(), hints)?;
        *phase = Phase::Running(spawned.0.clone());
        spawned
    };

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
    // From the folder's state DB, in any phase (WAL: a sync writing doesn't block it), and
    // without holding up the other requests.
    if let Ask::Recent { limit } = request.ask {
        return Reply::Recent(
            StateDb::open(&state_db_path(&root.folder))
                .and_then(|mut db| db.read(|s| s.recent_synced(limit)))
                .unwrap_or_default(),
        );
    }
    if let Ask::Deleted { limit } = request.ask {
        return Reply::Deleted(
            StateDb::open(&state_db_path(&root.folder))
                .and_then(|mut db| hbsync_engine::deleted::list(&mut db, limit))
                .unwrap_or_default(),
        );
    }
    let mut phase = lock(phase);
    match (request.ask, &mut *phase) {
        (Ask::Recent { .. } | Ask::Deleted { .. }, _) => unreachable!("answered above"),
        (Ask::Status, Phase::Running(h)) => Reply::Status(Box::new(h.status())),
        (
            Ask::Status,
            Phase::Waiting {
                state,
                error,
                warning,
                paused,
            },
        ) => {
            let mut s = Status::new(&root.identity, &root.folder);
            s.state = if paused.is_some() {
                State::Paused
            } else {
                *state
            };
            s.paused_until = paused.flatten();
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
        (Ask::Control(c @ (Control::Pause { .. } | Control::Resume)), phase) => {
            let pause = match c {
                Control::Pause { until } => Some(until),
                _ => None,
            };
            // Pausing survives a restart, and is kept until the sync starts.
            if let Err(e) = remember_paused(paths, pause) {
                return Reply::Refused(format!("couldn't save the pause in the config: {e}"));
            }
            match phase {
                Phase::Running(h) => send(h, c),
                Phase::Waiting { paused, .. } => {
                    *paused = pause;
                    Reply::Done
                }
            }
        }
        (Ask::Control(c), Phase::Running(h)) => send(h, c),
        (Ask::Control(_), Phase::Waiting { error, .. }) => Reply::Refused(format!(
            "not connected to {} yet{}",
            root.identity,
            error.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
        )),
    }
}

fn send(handle: &Handle, control: Control) -> Reply {
    if handle.send(control) {
        Reply::Done
    } else {
        Reply::Refused("the sync has stopped".into())
    }
}

fn remember_paused(paths: &Paths, pause: Option<Until>) -> Result<(), ConfigError> {
    let path = paths.config_file();
    let mut config = Config::load(&path)?.unwrap_or_default();
    let was = (config.paused, config.paused_until);
    config.set_pause(pause);
    if (config.paused, config.paused_until) != was {
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
