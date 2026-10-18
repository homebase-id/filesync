//! The sync loop for one folder, on its own thread: what the daemon runs.
//!
//! A sync runs when the folder changes (live watcher), when its part of the drive changed (the
//! drive wakes it: a change the identity announced, the notifications connecting again, another
//! folder's sync), when asked, and after a backoff when a sync left work unfinished; never on
//! a timer of its own (daemon.md, "No poll"). After a failed sync the runner waits, doubling
//! up to five minutes. It is controlled through a [`Handle`] and publishes a [`FolderStatus`].
//!
//! - **Pause** stops a running sync between operations (and before the next transfer) and
//!   syncs nothing until **resume**; the watcher keeps recording changes meanwhile.
//! - **Sleep**: told by its account ([`Command::Woke`]), the runner syncs at once and forgets
//!   its backoff.
//! - **Ignore rules** (`.hbsync/ignore`) are checked for changes when the watcher sees the file
//!   change (once a minute without a watcher) and on request; a change restarts the engine,
//!   which re-plans everything (sync semantics §10).
//!
//! No poll: the runner blocks on one channel (its commands, the watcher's bell, the drive's
//! wakes) until the next thing time alone changes ([`Schedule::next_wake`]), so an idle folder
//! with a watcher wakes only every [`WAIT_AT_MOST`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_engine::activity::Counters;
use hbsync_engine::exec::Stopped;
use hbsync_engine::feed::FeedError;
use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_engine::{Drive, Engine, EngineConfig, EngineError, SyncReport};
use hbsync_local::{Bell, Filter, FilterError, LocalObserver};
use hbsync_remote::RemoteError;
use hbsync_state::StateError;
use hbsync_types::NodeId;
use hbsync_types::unix_now;

use crate::MassDelete;
use crate::protocol::{FolderStatus, State, Until};

mod report;
pub(crate) mod schedule;

pub(crate) use schedule::Backoff;
use schedule::{Schedule, Synced};

/// The longest any of the daemon's loops waits for an event: a bound on what a lost wake-up
/// could hold back.
pub(crate) const WAIT_AT_MOST: Duration = Duration::from_secs(600);
/// The ignore rules are read this long after their file was last touched: not half written.
const RULES_QUIET: Duration = Duration::from_secs(1);

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
    /// How often what is local is looked at: the ignore rules, whether a folder that is gone
    /// is back, a rescan of a folder without a watcher.
    pub look: Duration,
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
            look: Duration::from_secs(60),
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
    /// The drive can be reached again (the notifications connected): a sync waiting for a
    /// backoff or to try unfinished work again goes now.
    Online,
    /// Pause until then (Unix seconds), or until resumed (`None`).
    Pause {
        until: Until,
    },
    Resume,
    /// Let the held deletes the user was shown through: `token` is
    /// [`crate::protocol::HeldDeletes::token`] as they saw it. Ignored when the held deletes changed since.
    ConfirmDeletes {
        token: String,
    },
    /// The machine woke up: sync now, the backoff forgotten.
    Woke,
    /// Stop the runner (between operations).
    Stop,
}

/// What the runner's thread waits for, on one channel.
#[derive(Debug)]
enum Msg {
    Command(Command),
    /// Its part of the drive changed (announced, or another folder's sync).
    Drive,
    /// The watcher has changes to record, or the ignore rules' file was touched.
    Local,
    /// Every [`Handle`] is gone: it stops.
    Orphaned,
}

/// The sending end the handles share: when the last one goes, the runner is told.
#[derive(Debug)]
struct Owner(Sender<Msg>);

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.send(Msg::Orphaned);
    }
}

/// Controls a running [`Runner`] from other threads.
#[derive(Debug, Clone)]
pub struct Handle {
    control: Arc<Owner>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<FolderStatus>>,
    progress: Arc<Counters>,
    woken: Arc<AtomicU64>,
}

impl Handle {
    /// Passes `control` on. Pause and stop also interrupt a sync in progress. False when the
    /// runner has ended.
    pub fn send(&self, control: Command) -> bool {
        if matches!(control, Command::Pause { .. } | Command::Stop) {
            self.stop.store(true, Ordering::Relaxed);
        }
        self.control.0.send(Msg::Command(control)).is_ok()
    }

    /// How often the runner's loop has woken up (tests: none while idle).
    #[doc(hidden)]
    pub fn woken(&self) -> u64 {
        self.woken.load(Ordering::Relaxed)
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

/// How long to wait from `now` for the earliest of `deadlines`, [`WAIT_AT_MOST`] at most.
pub(crate) fn wait_for(now: Instant, deadlines: impl IntoIterator<Item = Instant>) -> Duration {
    deadlines
        .into_iter()
        .fold(now + WAIT_AT_MOST, Instant::min)
        .saturating_duration_since(now)
}

/// Calls its function when dropped, also in a panic's unwinding: a thread's end told for sure.
pub(crate) struct OnDrop<F: FnOnce()>(pub Option<F>);

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// The lock, also after a panic elsewhere (the data is plain values).
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct Runner {
    cfg: RunnerConfig,
    drive: Arc<Drive>,
    control: Receiver<Msg>,
    /// Its own channel, for the watcher's bell and the drive's wakes.
    wakes: Sender<Msg>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<FolderStatus>>,
    progress: Arc<Counters>,
    /// How often its loop woke up ([`Handle::woken`]).
    woken: Arc<AtomicU64>,
    /// The nodes of the deletes held in the last sync (what the status shows).
    held: Vec<NodeId>,
    /// The conflicts and failures of the last sync, to tell each one only once.
    told: report::Told,
    /// Stale skips sync after sync: one that keeps coming back is told after all.
    stalled: report::Stalled,
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
    /// Set when the drive changed for the folder ([`Drive::subscribe_with`], which wakes the
    /// runner while this lives).
    drive_changed: Arc<AtomicBool>,
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
    /// Starts syncing `cfg.folder` on its account's `drive` on a new thread; the drive wakes it
    /// when its part changed.
    pub fn spawn(
        cfg: RunnerConfig,
        drive: Arc<Drive>,
    ) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
        Self::spawn_with(cfg, drive, || {})
    }

    /// [`Runner::spawn`], calling `ended` as its thread ends (after its status says so, unless
    /// it panicked).
    pub fn spawn_with(
        cfg: RunnerConfig,
        drive: Arc<Drive>,
        ended: impl FnOnce() + Send + 'static,
    ) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
        let (tx, rx) = channel();
        let stop = Arc::new(AtomicBool::new(cfg.paused.is_some()));
        let mut status = FolderStatus::new(&cfg.identity, &cfg.folder);
        status.role = cfg.role;
        status.paused_until = cfg.paused.flatten();
        let status = Arc::new(Mutex::new(status));
        let progress = Arc::new(Counters::default());
        let woken = Arc::new(AtomicU64::new(0));
        let handle = Handle {
            control: Arc::new(Owner(tx.clone())),
            stop: Arc::clone(&stop),
            status: Arc::clone(&status),
            progress: Arc::clone(&progress),
            woken: Arc::clone(&woken),
        };
        let runner = Runner {
            cfg,
            drive,
            control: rx,
            wakes: tx,
            stop,
            status,
            progress,
            woken,
            held: Vec::new(),
            told: Default::default(),
            stalled: Default::default(),
        };
        let thread = std::thread::Builder::new()
            .name("hbsync-runner".into())
            .spawn(move || {
                // Told also when it panics.
                let _ended = OnDrop(Some(ended));
                let status = Arc::clone(&runner.status);
                let result = runner.run();
                // Its account starts a failed runner again after a pause.
                let mut status = lock(&status);
                status.state = State::Stopped;
                if let Err(e) = &result {
                    status.state = State::Failing;
                    status.error = Some(e.to_string());
                    tracing::error!("the sync of {} stopped: {e}", status.path.display());
                }
                drop(status);
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
        let mut engine = Engine::open(&sync_root, Arc::clone(&self.drive), cfg)?;
        engine.set_stop(Arc::clone(&self.stop));
        engine.set_progress(Arc::clone(&self.progress));
        let wakes = self.wakes.clone();
        let bell: Bell = Arc::new(move || {
            let _ = wakes.send(Msg::Local);
        });
        let observer = LocalObserver::ringing(root, engine.scan_options().clone(), Some(bell));
        if let Some(why) = &observer.watcher_error {
            tracing::warn!("no live watcher ({why}); the folder is rescanned once a minute");
        }
        self.set(|s| {
            s.rules = rules;
            s.watcher_error = observer.watcher_error.clone();
        });
        let drive_changed = Arc::new(AtomicBool::new(false));
        let (alive, wakes) = (Arc::downgrade(&drive_changed), self.wakes.clone());
        // One message until the runner takes the change, however many wakes come.
        self.drive.subscribe_with(sync_root.folder(), move || {
            alive.upgrade().is_some_and(|changed| {
                changed.swap(true, Ordering::SeqCst) || wakes.send(Msg::Drive).is_ok()
            })
        });
        Ok(Parts {
            engine,
            observer,
            drive_changed,
        })
    }

    fn run(mut self) -> Result<(), RunnerError> {
        // Opened when there is something to sync and the folder is there; dropped when the
        // folder goes away or the ignore rules change.
        let mut parts: Option<Parts> = None;
        let mut confirmed = None;
        let mut schedule = Schedule::new(Instant::now(), self.cfg.look, self.cfg.paused);
        let mut started = false;
        let mut check_rules = false;
        // When to read the ignore rules, touched a moment ago.
        let mut rules_at: Option<Instant> = None;
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
            // Until something happens, or time alone changes something.
            let now = Instant::now();
            let looking = parts.as_ref().is_none_or(|p| !p.observer.has_watcher());
            let observer = parts.as_ref().and_then(|p| p.observer.deadline());
            let wait = wait_for(
                now,
                [
                    schedule.next_wake(now, unix_now(), looking),
                    observer,
                    rules_at,
                ]
                .into_iter()
                .flatten(),
            );
            let mut first = self.control.recv_timeout(wait).ok();
            self.woken.fetch_add(1, Ordering::Relaxed);
            while let Some(msg) = first.take().or_else(|| self.control.try_recv().ok()) {
                match msg {
                    Msg::Command(Command::Stop) | Msg::Orphaned => {
                        tracing::info!("stopped");
                        return Ok(());
                    }
                    Msg::Command(command) => {
                        self.command(command, &mut schedule, &mut confirmed, &mut check_rules)
                    }
                    // Taken below.
                    Msg::Drive | Msg::Local => {}
                }
            }

            // Local changes: the watcher's batches are recorded in the DB as they come. Only a
            // change syncs: a sync's own writes come back as batches that change nothing. Taken
            // whether the bell rang or not: with nothing queued, that costs nothing.
            if let Some(p) = &mut parts {
                // A change in this folder's part of the drive.
                if p.drive_changed.swap(false, Ordering::SeqCst) {
                    schedule.want();
                }
                if let Some(scan) = p.engine.observe(&mut p.observer, Duration::ZERO)?
                    && scan.dirty > 0
                {
                    schedule.want();
                }
                // After the poll, which has the watcher ring again for a touch from now on.
                if p.observer.rules_touched() {
                    rules_at = Some(Instant::now() + RULES_QUIET);
                }
                if let Some(why) = &p.observer.watcher_error
                    && lock(&self.status).watcher_error.is_none()
                {
                    tracing::warn!(
                        "no live watcher ({why}); the folder is rescanned once a minute"
                    );
                    self.set(|s| s.watcher_error = Some(why.clone()));
                }
            }

            let now = Instant::now();
            if rules_at.is_some_and(|at| now >= at) {
                rules_at = None;
                check_rules = true;
            }
            if schedule.pause_over(now, unix_now()) {
                tracing::info!("the pause is over: resuming");
                self.resume();
            }
            if schedule.look_due(now) {
                check_rules = true;
                // Without a watcher, the folder's changes are found by scanning it.
                if let Some(p) = &mut parts
                    && schedule.paused().is_none()
                    && !p.observer.has_watcher()
                {
                    match p.engine.scan_local() {
                        Ok(scan) if scan.dirty > 0 => schedule.want(),
                        Ok(_) => {}
                        Err(e) => tracing::warn!("scanning the folder failed: {e}"),
                    }
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
                    self.failing(State::Failing, GONE.into(), schedule.later(Instant::now()));
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
                    // Rules that don't parse: say so, and look again at the next look.
                    Err(RunnerError::Filter(e)) if !self.cfg.once => {
                        tracing::warn!("{e}");
                        self.failing(
                            State::Failing,
                            e.to_string(),
                            schedule.later(Instant::now()),
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            if let Some(seen) = confirmed.take() {
                p.engine.confirm_deletes(seen);
            }
            // This sync pulls what woke it so far.
            p.drive_changed.store(false, Ordering::SeqCst);
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
            let read = schedule.read();
            let result = if started {
                p.engine.sync(&today, read)
            } else {
                p.engine.start_up(&mut p.observer, &today).map(|r| r.sync)
            };
            match result {
                Ok(report) => {
                    started = true;
                    let outcome = if report.stopped {
                        Synced::Stopped
                    } else if report.unfinished {
                        Synced::Unfinished {
                            progressed: report.ops_done > 0,
                        }
                    } else {
                        Synced::Done
                    };
                    schedule.synced(Instant::now(), outcome);
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
                    self.failing(state, e.to_string(), wait);
                }
            }
            if self.cfg.once {
                return Ok(());
            }
        }
    }

    /// Takes `command` (but a stop).
    fn command(
        &mut self,
        command: Command,
        schedule: &mut Schedule,
        confirmed: &mut Option<Vec<NodeId>>,
        check_rules: &mut bool,
    ) {
        match command {
            Command::SyncNow => {
                schedule.asked(Instant::now());
                *check_rules = true;
                // The user may have made room on the drive.
                self.drive.clear_full();
            }
            Command::Online => schedule.online(Instant::now()),
            Command::Woke => {
                tracing::info!("the machine woke up: syncing now");
                schedule.woke(Instant::now());
            }
            Command::Pause { until } => {
                schedule.pause(until);
                self.set(|s| s.paused_until = until);
                match until {
                    Some(t) => {
                        tracing::info!("paused for {} min", t.saturating_sub(unix_now()) / 60)
                    }
                    None => tracing::info!("paused"),
                }
            }
            Command::Resume => {
                if schedule.resume(Instant::now()) {
                    tracing::info!("resumed");
                }
                self.resume();
            }
            Command::ConfirmDeletes { token } => {
                if token != report::held_token(&self.held) {
                    tracing::warn!("a confirmation of deletes that changed since: ignored");
                    return;
                }
                tracing::info!("{} held deletes confirmed", self.held.len());
                *confirmed = Some(self.held.clone());
                schedule.asked(Instant::now());
            }
            Command::Stop => {}
        }
    }

    /// Shows `why` nothing syncs (in `state`), until another try after `wait`.
    fn failing(&self, state: State, why: String, wait: Duration) {
        self.set(|s| {
            s.state = state;
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
        let skips = self.stalled.told(&report.skipped, Instant::now());
        let rows = self.drive.take_skipped();
        let notes = self
            .told
            .fresh(report::notes(engine, report, &skips, rows)?);
        self.held = report.held.iter().map(|op| op.node()).collect();
        let held = report::held(engine, report, &self.held)?;
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

fn is_offline(e: &EngineError) -> bool {
    match e {
        EngineError::Stopped(Stopped::Unreachable(_)) => true,
        EngineError::Feed(FeedError::Remote(r)) => {
            r.is_transient() || matches!(r, RemoteError::Io(_))
        }
        _ => false,
    }
}
