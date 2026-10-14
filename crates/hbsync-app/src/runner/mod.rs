//! The sync loop for one folder, on its own thread: what the daemon runs.
//!
//! A sync runs when the folder changes (live watcher), when the identity announces a change
//! on the drive (notification hints), at least once a minute regardless, and when asked. After
//! a failed sync the runner waits, doubling up to five minutes. It is controlled through a
//! [`Handle`] and publishes a [`FolderStatus`].
//!
//! - **Pause** stops a running sync between operations (and before the next transfer) and
//!   syncs nothing until **resume**; the watcher keeps recording changes meanwhile.
//! - **Sleep**: a wall-clock gap across a short wait means the machine slept. The runner then
//!   syncs at once and forgets its backoff (the account has the notifier reconnect).
//! - **Ignore rules** (`.hbsync/ignore`) are checked for changes at every poll and on request;
//!   a change restarts the engine, which re-plans everything (sync semantics §10).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use hbsync_engine::activity::Counters;
use hbsync_engine::exec::Stopped;
use hbsync_engine::feed::FeedError;
use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_engine::{Drive, Engine, EngineConfig, EngineError, SyncReport};
use hbsync_local::watch::Debounce;
use hbsync_local::{Filter, FilterError, LocalObserver};
use hbsync_remote::RemoteError;
use hbsync_state::StateError;
use hbsync_types::NodeId;
pub use hbsync_types::activity::Progress;
use hbsync_types::unix_now;

use crate::MassDelete;
use crate::protocol::{FolderStatus, State, Until};

mod report;
mod schedule;

pub(crate) use schedule::Backoff;
use schedule::Schedule;

/// How long the loop waits for local changes before looking at everything else.
const TICK: Duration = Duration::from_millis(250);
/// A wall-clock gap this much longer than a tick means the machine slept.
const SLEPT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Filter(#[from] FilterError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("the sync thread panicked")]
    Panicked,
    #[error(transparent)]
    Folder(#[from] RootError),
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub folder: PathBuf,
    /// The account's state DB ([`crate::Paths::account_db`]).
    pub db: PathBuf,
    pub identity: String,
    /// The role folder it is; `None` for the main folder.
    pub role: Option<hbsync_types::Role>,
    /// Shown in conflict-copy names.
    pub device: String,
    pub mass_delete: MassDelete,
    /// Start paused: until then, or until resumed (`Some(None)`).
    pub paused: Option<Until>,
    /// A sync at least this often.
    pub poll: Duration,
    /// Catch up, sync once, and stop (development).
    pub once: bool,
    /// Tests: adjusts the engine's config.
    pub tweak: Option<fn(&mut EngineConfig)>,
}

impl RunnerConfig {
    pub fn new(folder: &Path, db: &Path, identity: &str, device: &str) -> Self {
        RunnerConfig {
            folder: folder.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            role: None,
            device: device.to_owned(),
            mass_delete: MassDelete::default(),
            paused: None,
            poll: Duration::from_secs(60),
            once: false,
            tweak: None,
        }
    }
}

/// Requests to the runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Sync now, without waiting out a backoff.
    SyncNow,
    /// Pause until then (Unix seconds), or until resumed (`None`).
    Pause {
        until: Until,
    },
    Resume,
    /// Let the held deletes the user was shown through: `token` is
    /// [`HeldDeletes::token`] as they saw it. Ignored when the held deletes changed since.
    ConfirmDeletes {
        token: String,
    },
    /// Stop the runner (between operations).
    Stop,
}

/// What the identity's notifications tell a folder's runner (passed on by its account).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hint {
    /// Connected (again): changes may have been missed meanwhile.
    Connected,
    /// The drive changed.
    Changed,
}

/// Controls a running [`Runner`] from other threads.
#[derive(Debug, Clone)]
pub struct Handle {
    control: Sender<Command>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<FolderStatus>>,
    progress: Arc<Counters>,
}

impl Handle {
    /// Passes `control` on. Pause and stop also interrupt a sync in progress. False when the
    /// runner has ended.
    pub fn send(&self, control: Command) -> bool {
        if matches!(control, Command::Pause { .. } | Command::Stop) {
            self.stop.store(true, Ordering::Relaxed);
        }
        self.control.send(control).is_ok()
    }

    /// Paused while the pause flag is set: seen at once, not only once the runner gets to it.
    pub fn status(&self) -> FolderStatus {
        let mut status = lock(&self.status).clone();
        if self.stop.load(Ordering::Relaxed) && status.state != State::Stopped {
            status.state = State::Paused;
        }
        let progress = self.progress.get();
        if matches!(status.state, State::Starting | State::Syncing) && progress.files_total > 0 {
            status.progress = Some(progress);
        }
        status
    }
}

/// The lock, also after a panic elsewhere (the data is plain values).
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct Runner {
    cfg: RunnerConfig,
    drive: Arc<Drive>,
    hints: Receiver<Hint>,
    control: Receiver<Command>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<FolderStatus>>,
    progress: Arc<Counters>,
    /// The nodes of the deletes held in the last sync (what the status shows).
    held: Vec<NodeId>,
    /// The conflicts and failures of the last sync, to tell each one only once.
    told: report::Told,
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner")
            .field("folder", &self.cfg.folder)
            .finish_non_exhaustive()
    }
}

/// The engine and the local observer, rebuilt when the ignore rules change.
struct Parts {
    engine: Engine,
    observer: LocalObserver,
    /// Woken when another folder's sync changed this one's part of the drive.
    wake: Receiver<()>,
}

impl Parts {
    /// Whether the ignore rules on disk differ from the ones in use (or don't parse): their
    /// fingerprints differ (sync semantics §10).
    fn rules_changed(&self, root: &Path) -> bool {
        let exec = &self.engine.config().exec;
        Filter::load(root, exec.case_insensitive)
            .map_or(true, |f| f.fingerprint() != exec.filter.fingerprint())
    }
}

impl Runner {
    /// Starts syncing `cfg.folder` on its account's `drive` on a new thread, woken by `hints`.
    pub fn spawn(
        cfg: RunnerConfig,
        drive: Arc<Drive>,
        hints: Receiver<Hint>,
    ) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
        let (tx, rx) = channel();
        let stop = Arc::new(AtomicBool::new(cfg.paused.is_some()));
        let mut status = FolderStatus::new(&cfg.identity, &cfg.folder);
        status.role = cfg.role;
        status.paused_until = cfg.paused.flatten();
        let status = Arc::new(Mutex::new(status));
        let progress = Arc::new(Counters::default());
        let handle = Handle {
            control: tx,
            stop: Arc::clone(&stop),
            status: Arc::clone(&status),
            progress: Arc::clone(&progress),
        };
        let runner = Runner {
            cfg,
            drive,
            hints,
            control: rx,
            stop,
            status,
            progress,
            held: Vec::new(),
            told: Default::default(),
        };
        let thread = std::thread::Builder::new()
            .name("hbsync-runner".into())
            .spawn(move || {
                let status = Arc::clone(&runner.status);
                let result = runner.run();
                lock(&status).state = State::Stopped;
                if let Err(e) = &result {
                    lock(&status).error = Some(e.to_string());
                    tracing::error!("the sync stopped: {e}");
                }
                result
            })?;
        Ok((handle, thread))
    }

    fn set(&self, f: impl FnOnce(&mut FolderStatus)) {
        f(&mut lock(&self.status));
    }

    /// The end of a pause: syncing goes on.
    fn resume(&self) {
        self.stop.store(false, Ordering::Relaxed);
        self.set(|s| {
            s.state = State::Idle;
            s.paused_until = None;
        });
    }

    /// The engine and observer for the folder, which must have been set up (its marker names
    /// it): an unmounted disk's empty mount point must not become a new sync folder.
    fn open(&self) -> Result<Parts, RunnerError> {
        let root = &self.cfg.folder;
        let mut cfg = EngineConfig::new(root, &self.cfg.device);
        cfg.mass_delete_min = self.cfg.mass_delete.min_files;
        cfg.mass_delete_fraction = self.cfg.mass_delete.fraction;
        cfg.exec.filter = Arc::new(Filter::load(root, cfg.exec.case_insensitive)?);
        if let Some(tweak) = self.cfg.tweak {
            tweak(&mut cfg);
        }
        let rules = cfg.exec.filter.rule_count();
        // A folder belongs to one identity (its marker says which): never mix two drives
        // through one folder.
        let sync_root = SyncRoot::open(&self.cfg.db, &self.cfg.identity, root)?;
        let mut engine = Engine::new(sync_root.state()?, Arc::clone(&self.drive), cfg);
        engine.set_stop(Arc::clone(&self.stop));
        engine.set_progress(Arc::clone(&self.progress));
        let observer = LocalObserver::start(root, engine.scan_options().clone());
        if let Some(why) = &observer.watcher_error {
            tracing::warn!("no live watcher ({why}); the folder is rescanned at every poll");
        }
        self.set(|s| {
            s.rules = rules;
            s.watcher_error = observer.watcher_error.clone();
        });
        let wake = self.drive.subscribe(sync_root.folder());
        Ok(Parts {
            engine,
            observer,
            wake,
        })
    }

    fn run(mut self) -> Result<(), RunnerError> {
        // Opened when there is something to sync and the folder is there; dropped when the
        // folder goes away or the ignore rules change.
        let mut parts: Option<Parts> = None;
        let mut confirmed = None;
        let mut schedule = Schedule::new(Instant::now(), self.cfg.poll, self.cfg.paused);
        let mut started = false;
        let mut check_rules = false;
        let mut hints_open = true;
        tracing::info!(
            "syncing {} with {}{}",
            self.cfg.folder.display(),
            self.cfg.identity,
            if schedule.paused().is_some() {
                " (paused)"
            } else {
                ""
            }
        );
        loop {
            // Local changes: the watcher's batches are recorded in the DB as they come.
            let (wall, mono) = (SystemTime::now(), Instant::now());
            let mut worked = false;
            match &mut parts {
                Some(p) => {
                    if p.observer
                        .poll(p.engine.db(), TICK, Debounce::default())?
                        .is_some()
                    {
                        schedule.want();
                        worked = true;
                    }
                }
                None => std::thread::sleep(TICK),
            }
            if slept(
                SystemTime::now().duration_since(wall).ok(),
                mono.elapsed(),
                worked,
            ) {
                tracing::info!("the machine woke up: syncing now");
                schedule.woke(Instant::now());
            }

            loop {
                match self.control.try_recv() {
                    Ok(Command::SyncNow) => {
                        schedule.asked(Instant::now());
                        check_rules = true;
                    }
                    Ok(Command::Pause { until }) => {
                        schedule.pause(until);
                        self.set(|s| s.paused_until = until);
                        match until {
                            Some(t) => tracing::info!(
                                "paused for {} min",
                                t.saturating_sub(unix_now()) / 60
                            ),
                            None => tracing::info!("paused"),
                        }
                    }
                    Ok(Command::Resume) => {
                        if schedule.resume(Instant::now()) {
                            tracing::info!("resumed");
                        }
                        self.resume();
                    }
                    Ok(Command::ConfirmDeletes { token }) => {
                        if token != report::held_token(&self.held) {
                            tracing::warn!("a confirmation of deletes that changed since: ignored");
                            continue;
                        }
                        tracing::info!("{} held deletes confirmed", self.held.len());
                        confirmed = Some(self.held.clone());
                        schedule.asked(Instant::now());
                    }
                    Ok(Command::Stop) | Err(TryRecvError::Disconnected) => {
                        tracing::info!("stopped");
                        return Ok(());
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }

            while hints_open {
                match self.hints.try_recv() {
                    Ok(Hint::Connected) => schedule.want(),
                    Ok(Hint::Changed) => schedule.announced(Instant::now()),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => hints_open = false,
                }
            }
            // Another folder's sync changed this one's part of the drive.
            if let Some(p) = &parts
                && p.wake.try_iter().count() > 0
            {
                schedule.want();
            }

            let now = Instant::now();
            if schedule.pause_over(now, unix_now()) {
                tracing::info!("the pause is over: resuming");
                self.resume();
            }
            if schedule.poll_due(now) {
                check_rules = true;
                if let Some(p) = &mut parts
                    && schedule.paused().is_none()
                    && !p.observer.has_watcher()
                    && let Err(e) = p.engine.scan_local()
                {
                    tracing::warn!("scanning the folder failed: {e}");
                }
            }
            if std::mem::take(&mut check_rules)
                && parts
                    .as_ref()
                    .is_some_and(|p| p.rules_changed(&self.cfg.folder))
            {
                tracing::info!("the ignore rules changed: starting over");
                parts = None;
                schedule.want();
            }
            if !schedule.due(now) {
                continue;
            }
            match SyncRoot::check(&self.cfg.folder, &self.cfg.identity) {
                Ok(_) => {}
                // Unmounted, moved or deleted: syncing would take everything off the drive,
                // or fill an empty mount point. Wait for it to come back.
                Err(RootError::NoMarker(_) | RootError::Io(_)) => {
                    const GONE: &str = "the folder is gone (moved, deleted or not mounted)";
                    if lock(&self.status).error.as_deref() != Some(GONE) {
                        tracing::warn!("{GONE}: waiting for it to come back");
                    }
                    parts = None;
                    self.failing(GONE.into(), schedule.later(Instant::now()));
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
            let p = match &mut parts {
                Some(p) => p,
                None => match self.open() {
                    Ok(p) => {
                        started = false;
                        parts.insert(p)
                    }
                    // Rules that don't parse: say so, and try again at the next poll.
                    Err(RunnerError::Filter(e)) if !self.cfg.once => {
                        tracing::warn!("{e}");
                        self.failing(e.to_string(), schedule.later(Instant::now()));
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            if let Some(seen) = confirmed.take() {
                p.engine.confirm_deletes(seen);
            }
            let began = Instant::now();
            // Before showing it syncs: the last sync's numbers aren't this one's.
            self.progress.reset();
            self.set(|s| {
                s.state = if started {
                    State::Syncing
                } else {
                    State::Starting
                }
            });
            let today = crate::today();
            let result = if started {
                p.engine.sync_once(&today)
            } else {
                p.engine.start_up(&mut p.observer, &today).map(|r| r.sync)
            };
            match result {
                Ok(report) => {
                    started = true;
                    schedule.synced(report.stopped);
                    self.record(&mut p.engine, &report, began.elapsed())?;
                }
                Err(e) => {
                    let state = if e.is_denied() {
                        State::Denied
                    } else if is_offline(&e) {
                        State::Offline
                    } else {
                        State::Failing
                    };
                    let wait = schedule.failed(Instant::now(), state);
                    tracing::warn!("sync failed ({e}); trying again in {wait:?}");
                    self.set(|s| {
                        s.state = state;
                        s.error = Some(e.to_string());
                        s.retry_at = Some(unix_now() + wait.as_secs());
                    });
                }
            }
            if self.cfg.once {
                return Ok(());
            }
        }
    }

    /// Shows `why` nothing syncs, until another try after `wait`.
    fn failing(&self, why: String, wait: Duration) {
        self.set(|s| {
            s.state = State::Failing;
            s.error = Some(why);
            s.retry_at = Some(unix_now() + wait.as_secs());
        });
    }

    /// Publishes a finished sync.
    fn record(
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
        took: Duration,
    ) -> Result<(), RunnerError> {
        let skipped = self.drive.take_skipped();
        let notes = self.told.fresh(report::notes(engine, report, skipped)?);
        let held = report::held(engine, report)?;
        self.held = report.held.iter().map(|op| op.node()).collect();
        if report.ops_done > 0 || !notes.is_empty() {
            tracing::info!(
                "synced in {took:.1?}: {} done, {} remote changes, {} adopted, {} pending",
                report.ops_done,
                report.feed.live + report.feed.deleted,
                report.adopted,
                report.pending
            );
        }
        for note in &notes {
            tracing::warn!("{note}");
        }
        if let Some(h) = &held {
            tracing::warn!(
                "deletes held for confirmation: {} here, {} remotely",
                h.here,
                h.remotely
            );
        }
        self.set(|s| report::record(s, report, notes, held, took, unix_now()));
        Ok(())
    }
}

/// Whether the machine slept during a wait that took `wall` by the wall clock (`None`: it was
/// set back) and `mono` by the monotonic clock, `worked` telling whether the wait also did
/// work (recorded a batch, rescanned). The monotonic clock stops in sleep on Linux and macOS:
/// a wall clock far ahead of it is a sleep. On Windows it runs on, so a long wait that did no
/// work counts too. A clock set forward only costs a sync.
pub(crate) fn slept(wall: Option<Duration>, mono: Duration, worked: bool) -> bool {
    let Some(wall) = wall else {
        return false;
    };
    wall > mono + SLEPT || (!worked && wall > TICK + SLEPT)
}

fn is_offline(e: &EngineError) -> bool {
    match e {
        EngineError::Stopped(Stopped::Unreachable(_)) => true,
        EngineError::Feed(FeedError::Remote(r)) => {
            r.is_transient() || matches!(r, RemoteError::Io(_))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_wall_clock_gap_means_the_machine_slept() {
        let s = Duration::from_secs;
        // A tick.
        assert!(!slept(Some(TICK), TICK, false));
        // Linux, macOS: the monotonic clock stood still.
        assert!(slept(Some(s(3600)), TICK, false));
        assert!(slept(Some(s(3600)), TICK, true));
        // Windows: both ran on, and the wait did nothing else.
        assert!(slept(Some(s(3600)), s(3600), false));
        // A long scan in the wait is no sleep.
        assert!(!slept(Some(s(45)), s(45), true));
        // The clock set back.
        assert!(!slept(None, TICK, false));
    }
}
