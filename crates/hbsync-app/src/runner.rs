//! The sync loop for one folder, on its own thread: what the daemon runs.
//!
//! A sync runs when the folder changes (live watcher), when the identity announces a change
//! on the drive (notification hints), at least once a minute regardless, and when asked. After
//! a failed sync the runner waits, doubling up to five minutes. It is controlled through a
//! [`Handle`] and publishes a [`Status`].
//!
//! - **Pause** stops a running sync between operations (and before the next transfer) and
//!   syncs nothing until **resume**; the watcher keeps recording changes meanwhile.
//! - **Sleep**: a wall-clock gap across a short wait means the machine slept. The runner then
//!   syncs at once, forgets its backoff and has the notifier reconnect.
//! - **Ignore rules** (`.hbsync/ignore`) are checked for changes at every poll and on request;
//!   a change restarts the engine, which re-plans everything (sync semantics §10).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hbsync_engine::exec::Stopped;
use hbsync_engine::feed::FeedError;
use hbsync_engine::{Conflict, Engine, EngineConfig, EngineError, SyncReport};
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_local::watch::Debounce;
use hbsync_local::{Filter, FilterError, IGNORE_FILE, LocalObserver, STATE_DIR, state_db_path};
use hbsync_remote::{Remote, RemoteError};
use hbsync_state::{StateDb, StateError};
use serde::{Deserialize, Serialize};

use crate::MassDelete;

const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// How long the loop waits for local changes before looking at everything else.
const TICK: Duration = Duration::from_millis(250);
/// A wall-clock gap this much longer than a tick means the machine slept.
const SLEPT: Duration = Duration::from_secs(30);
/// Entries kept in [`Status::recent`].
const RECENT: usize = 50;
/// Paths shown for held deletes.
const EXAMPLES: usize = 10;

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
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub folder: PathBuf,
    pub identity: String,
    /// Shown in conflict-copy names.
    pub device: String,
    pub mass_delete: MassDelete,
    /// Start paused.
    pub paused: bool,
    /// A sync at least this often.
    pub poll: Duration,
    /// Catch up, sync once, and stop (development).
    pub once: bool,
    /// Why the credentials aren't in the keychain, for the status.
    pub credentials_warning: Option<String>,
    /// Tests: adjusts the engine's config.
    pub tweak: Option<fn(&mut EngineConfig)>,
}

impl RunnerConfig {
    pub fn new(folder: &Path, identity: &str, device: &str) -> Self {
        RunnerConfig {
            folder: folder.to_path_buf(),
            identity: identity.to_owned(),
            device: device.to_owned(),
            mass_delete: MassDelete::default(),
            paused: false,
            poll: Duration::from_secs(60),
            once: false,
            credentials_warning: None,
            tweak: None,
        }
    }
}

/// Requests to the runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Control {
    /// Sync now, without waiting out a backoff.
    SyncNow,
    Pause,
    Resume,
    /// Let the held deletes through: (gone here, gone remotely) as the user saw them in
    /// [`Status::held_deletes`]. More than that are held again.
    ConfirmDeletes {
        here: u64,
        remotely: u64,
    },
    /// Stop the runner (between operations).
    Stop,
}

/// What the runner is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Catching up with the folder and the drive.
    Starting,
    Syncing,
    /// Up to date, or waiting for the next change.
    Idle,
    Paused,
    /// The identity can't be reached; retrying.
    Offline,
    /// The last sync failed for another reason; retrying.
    Failing,
    /// The identity refused this device: authorize it again.
    Denied,
    Stopped,
}

/// The notification connection (wake-up hints from the identity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hints {
    /// None: syncs come from local changes and the poll.
    Off,
    Connecting,
    Connected,
    Lost(String),
    Refused(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub state: State,
    pub identity: String,
    pub folder: PathBuf,
    /// Why the last sync failed (states `Offline`, `Failing`, `Denied`).
    pub error: Option<String>,
    /// When the next attempt is after a failure (Unix seconds).
    pub retry_at: Option<u64>,
    pub last_sync: Option<LastSync>,
    /// Items not in sync yet.
    pub pending: u64,
    /// Deletes waiting for the user's confirmation.
    pub held_deletes: Option<HeldDeletes>,
    /// Conflicts and failures of recent syncs, newest last.
    pub recent: Vec<Note>,
    /// Why there is no live watcher (the folder is then rescanned at every poll).
    pub watcher_error: Option<String>,
    pub hints: Hints,
    /// Ignore rules in `.hbsync/ignore`.
    pub rules: usize,
    /// Why the credentials aren't in the keychain.
    pub credentials_warning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastSync {
    /// When it ended (Unix seconds).
    pub at: u64,
    pub millis: u64,
    pub done: usize,
    pub downloaded_changes: usize,
    pub adopted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldDeletes {
    /// Files deleted here, to be deleted on the drive.
    pub here: u64,
    /// Files deleted elsewhere, to be moved to the trash here.
    pub remotely: u64,
    /// Some of them.
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    /// Unix seconds.
    pub at: u64,
    pub text: String,
}

/// Wake-up hints from the identity: the notifier's events, and the notifier to reconnect.
#[derive(Debug)]
pub struct HintSource {
    pub events: Receiver<Event>,
    pub notifier: Option<Notifier>,
}

impl HintSource {
    pub fn none() -> HintSource {
        HintSource {
            events: channel().1,
            notifier: None,
        }
    }
}

/// Controls a running [`Runner`] from other threads.
#[derive(Debug, Clone)]
pub struct Handle {
    control: Sender<Control>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<Status>>,
}

impl Handle {
    /// Passes `control` on. Pause and stop also interrupt a sync in progress. False when the
    /// runner has ended.
    pub fn send(&self, control: Control) -> bool {
        if matches!(control, Control::Pause | Control::Stop) {
            self.stop.store(true, Ordering::Relaxed);
        }
        self.control.send(control).is_ok()
    }

    pub fn status(&self) -> Status {
        lock(&self.status).clone()
    }
}

fn lock(status: &Mutex<Status>) -> std::sync::MutexGuard<'_, Status> {
    status.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct Runner {
    cfg: RunnerConfig,
    remote: Arc<dyn Remote>,
    hints: HintSource,
    control: Receiver<Control>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<Status>>,
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
    /// The ignore file as it was read: (modified, length), `None` when absent.
    rules_file: Option<(SystemTime, u64)>,
}

impl Runner {
    /// Starts syncing `cfg.folder` with `remote` on a new thread.
    pub fn spawn(
        cfg: RunnerConfig,
        remote: Arc<dyn Remote>,
        hints: HintSource,
    ) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
        let (tx, rx) = channel();
        let stop = Arc::new(AtomicBool::new(cfg.paused));
        let status = Arc::new(Mutex::new(Status {
            state: State::Starting,
            identity: cfg.identity.clone(),
            folder: cfg.folder.clone(),
            error: None,
            retry_at: None,
            last_sync: None,
            pending: 0,
            held_deletes: None,
            recent: Vec::new(),
            watcher_error: None,
            hints: if hints.notifier.is_some() {
                Hints::Connecting
            } else {
                Hints::Off
            },
            rules: 0,
            credentials_warning: cfg.credentials_warning.clone(),
        }));
        let handle = Handle {
            control: tx,
            stop: Arc::clone(&stop),
            status: Arc::clone(&status),
        };
        let runner = Runner {
            cfg,
            remote,
            hints,
            control: rx,
            stop,
            status,
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

    fn set(&self, f: impl FnOnce(&mut Status)) {
        f(&mut lock(&self.status));
    }

    /// The engine and observer for the folder, which must have been set up (its `.hbsync`
    /// exists): an unmounted disk's empty mount point must not become a new sync folder.
    fn open(&self) -> Result<Parts, RunnerError> {
        let root = &self.cfg.folder;
        let mut cfg = EngineConfig::new(root, &self.cfg.device);
        cfg.mass_delete_min = self.cfg.mass_delete.min_files;
        cfg.mass_delete_fraction = self.cfg.mass_delete.fraction;
        let rules_file = rules_file(root);
        cfg.exec.filter = Arc::new(Filter::load(root, cfg.exec.case_insensitive)?);
        if let Some(tweak) = self.cfg.tweak {
            tweak(&mut cfg);
        }
        let rules = cfg.exec.filter.rule_count();
        let mut engine = Engine::new(
            StateDb::open(&state_db_path(root))?,
            Box::new(Arc::clone(&self.remote)),
            cfg,
        );
        engine.set_stop(Arc::clone(&self.stop));
        let observer = LocalObserver::start(root, engine.scan_options().clone());
        if let Some(why) = &observer.watcher_error {
            tracing::warn!("no live watcher ({why}); the folder is rescanned at every poll");
        }
        self.set(|s| {
            s.rules = rules;
            s.watcher_error = observer.watcher_error.clone();
        });
        Ok(Parts {
            engine,
            observer,
            rules_file,
        })
    }

    fn run(self) -> Result<(), RunnerError> {
        // Opened when there is something to sync and the folder is there; dropped when the
        // folder goes away or the ignore rules change.
        let mut parts: Option<Parts> = None;
        let mut confirmed = None;
        let db_path = state_db_path(&self.cfg.folder);
        let mut paused = self.cfg.paused;
        let mut started = false;
        let mut wanted = true;
        let mut check_rules = false;
        let mut hints_open = true;
        let mut next_poll = Instant::now() + self.cfg.poll;
        let mut not_before = Instant::now();
        let mut backoff = FIRST_BACKOFF;
        tracing::info!(
            "syncing {} with {}{}",
            self.cfg.folder.display(),
            self.cfg.identity,
            if paused { " (paused)" } else { "" }
        );
        if paused {
            self.set(|s| s.state = State::Paused);
        }
        loop {
            // Local changes: the watcher's batches are recorded in the DB as they come.
            let before = SystemTime::now();
            match &mut parts {
                Some(p) => {
                    if p.observer
                        .poll(p.engine.db(), TICK, Debounce::default())?
                        .is_some()
                    {
                        wanted = true;
                    }
                }
                None => std::thread::sleep(TICK),
            }
            if slept(before, SystemTime::now()) {
                tracing::info!("the machine woke up: syncing now");
                wanted = true;
                backoff = FIRST_BACKOFF;
                not_before = Instant::now();
                if let Some(n) = &self.hints.notifier {
                    n.reconnect();
                }
            }

            loop {
                match self.control.try_recv() {
                    Ok(Control::SyncNow) => {
                        wanted = true;
                        check_rules = true;
                        not_before = Instant::now();
                    }
                    Ok(Control::Pause) => {
                        paused = true;
                        self.set(|s| s.state = State::Paused);
                        tracing::info!("paused");
                    }
                    Ok(Control::Resume) => {
                        if paused {
                            tracing::info!("resumed");
                        }
                        paused = false;
                        self.stop.store(false, Ordering::Relaxed);
                        wanted = true;
                        not_before = Instant::now();
                        self.set(|s| s.state = State::Idle);
                    }
                    Ok(Control::ConfirmDeletes { here, remotely }) => {
                        tracing::info!("deletes confirmed: {here} here, {remotely} remotely");
                        confirmed = Some((here, remotely));
                        wanted = true;
                        not_before = Instant::now();
                    }
                    Ok(Control::Stop) | Err(TryRecvError::Disconnected) => {
                        tracing::info!("stopped");
                        return Ok(());
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }

            while hints_open {
                let hint = match self.hints.events.try_recv() {
                    Ok(e) => e,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        hints_open = false;
                        break;
                    }
                };
                let hints = match hint {
                    Event::Connected => {
                        wanted = true;
                        Hints::Connected
                    }
                    Event::Changed => {
                        wanted = true;
                        continue;
                    }
                    Event::Lost(why) => {
                        tracing::info!("notifications lost ({why}); reconnecting");
                        Hints::Lost(why)
                    }
                    Event::Refused(why) => {
                        tracing::warn!("notifications refused ({why}); syncing on the poll only");
                        Hints::Refused(why)
                    }
                };
                self.set(|s| s.hints = hints);
            }

            let now = Instant::now();
            if now >= next_poll {
                // At every poll, whether the syncs succeed or back off.
                next_poll = now + self.cfg.poll;
                wanted = true;
                check_rules = true;
                if let Some(p) = &mut parts
                    && !p.observer.has_watcher()
                    && let Err(e) = p.engine.scan_local()
                {
                    tracing::warn!("scanning the folder failed: {e}");
                }
            }
            if std::mem::take(&mut check_rules)
                && parts
                    .as_ref()
                    .is_some_and(|p| rules_file(&self.cfg.folder) != p.rules_file)
            {
                tracing::info!("the ignore rules changed: starting over");
                parts = None;
                wanted = true;
            }
            if paused || !wanted || now < not_before {
                continue;
            }
            if !self.cfg.folder.join(STATE_DIR).is_dir() {
                // Unmounted, moved or deleted: syncing would take everything off the drive,
                // or fill an empty mount point. Wait for it to come back.
                const GONE: &str = "the folder is gone (moved, deleted or not mounted)";
                if lock(&self.status).error.as_deref() != Some(GONE) {
                    tracing::warn!("{GONE}: waiting for it to come back");
                }
                parts = None;
                not_before = Instant::now() + self.cfg.poll;
                self.set(|s| {
                    s.state = State::Failing;
                    s.error = Some(GONE.into());
                    s.retry_at = Some(unix_now() + self.cfg.poll.as_secs());
                });
                continue;
            }
            let p = match &mut parts {
                Some(p) => p,
                None => match self.open() {
                    Ok(p) => {
                        started = false;
                        parts.insert(p)
                    }
                    // Rules that don't parse: say so, and try again at the next poll.
                    Err(RunnerError::Filter(e)) => {
                        tracing::warn!("{e}");
                        not_before = Instant::now() + self.cfg.poll;
                        self.set(|s| {
                            s.state = State::Failing;
                            s.error = Some(e.to_string());
                            s.retry_at = Some(unix_now() + self.cfg.poll.as_secs());
                        });
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            if let Some(seen) = confirmed.take() {
                p.engine.confirm_deletes(seen);
            }

            let began = Instant::now();
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
                p.engine
                    .start_up(&mut p.observer, &db_path, &today)
                    .map(|r| r.sync)
            };
            match result {
                Ok(report) => {
                    started = true;
                    wanted = false;
                    backoff = FIRST_BACKOFF;
                    self.record(&mut p.engine, &report, began.elapsed())?;
                    if self.cfg.once {
                        return Ok(());
                    }
                }
                // Catching up is done by the time a sync can be stopped.
                Err(e) if e.is_stop_request() => {
                    started = true;
                    self.set(|s| s.state = State::Paused);
                }
                Err(e) => {
                    let (state, wait) = if e.is_denied() {
                        (State::Denied, MAX_BACKOFF)
                    } else if is_offline(&e) {
                        (State::Offline, backoff)
                    } else {
                        (State::Failing, backoff)
                    };
                    tracing::warn!("sync failed ({e}); trying again in {wait:?}");
                    not_before = Instant::now() + wait;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    self.set(|s| {
                        s.state = state;
                        s.error = Some(e.to_string());
                        s.retry_at = Some(unix_now() + wait.as_secs());
                    });
                    if self.cfg.once {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Publishes a finished sync.
    fn record(
        &self,
        engine: &mut Engine,
        report: &SyncReport,
        took: Duration,
    ) -> Result<(), RunnerError> {
        let at = unix_now();
        let mut notes = Vec::new();
        for c in &report.conflicts {
            notes.push(describe_conflict(engine, c)?);
        }
        for (op, why) in report.failed.iter().chain(&report.skipped) {
            let path = engine.path_of(op.node())?.unwrap_or_else(|| "?".into());
            notes.push(format!("{path}: {} ({why})", op_name(op)));
        }
        let held = if report.held.is_empty() {
            None
        } else {
            let (here, remotely) = engine.pending_deletes()?;
            let mut examples = Vec::new();
            for op in report.held.iter().take(EXAMPLES) {
                examples.extend(engine.path_of(op.node())?);
            }
            Some(HeldDeletes {
                here,
                remotely,
                examples,
            })
        };
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
        self.set(|s| {
            s.state = State::Idle;
            s.error = None;
            s.retry_at = None;
            s.pending = report.pending;
            s.held_deletes = held;
            s.last_sync = Some(LastSync {
                at,
                millis: took.as_millis() as u64,
                done: report.ops_done,
                downloaded_changes: report.feed.live + report.feed.deleted,
                adopted: report.adopted,
            });
            s.recent
                .extend(notes.into_iter().map(|text| Note { at, text }));
            let excess = s.recent.len().saturating_sub(RECENT);
            s.recent.drain(..excess);
        });
        Ok(())
    }
}

/// The ignore file's (modified, length), to notice an edit.
fn rules_file(root: &Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(root.join(STATE_DIR).join(IGNORE_FILE)).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

/// Whether the wall clock moved on much more than a tick between `before` and `after`: the
/// machine slept (or the clock was set, which only costs a sync).
fn slept(before: SystemTime, after: SystemTime) -> bool {
    after
        .duration_since(before)
        .is_ok_and(|gap| gap > TICK + SLEPT)
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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The operation's kind, from its debug form (`RemoteCreate { .. }` → `RemoteCreate`).
fn op_name(op: &hbsync_engine::Op) -> String {
    let debug = format!("{op:?}");
    debug
        .split([' ', '{', '('])
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn describe_conflict(engine: &mut Engine, c: &Conflict) -> Result<String, StateError> {
    let mut path = |node| -> Result<String, StateError> {
        Ok(engine.path_of(node)?.unwrap_or_else(|| "?".into()))
    };
    Ok(match c {
        Conflict::EditEdit { node, copy } => format!(
            "{}: edited on two devices; this device's version is kept as {}",
            path(*node)?,
            path(*copy)?
        ),
        Conflict::MoveMove { node } => {
            format!("{}: moved on two devices; the other move won", path(*node)?)
        }
        Conflict::EditDelete { node } => format!(
            "{}: deleted on one device and edited on another; the edit was kept",
            path(*node)?
        ),
        Conflict::NameClash { node } => {
            format!("{}: renamed, as the name was taken", path(*node)?)
        }
        Conflict::Cycle { node } => format!(
            "{}: a move that would have put a folder inside itself was undone",
            path(*node)?
        ),
        Conflict::Orphan { node } => format!(
            "{}: its folder was deleted everywhere; it was moved to the top folder",
            path(*node)?
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_wall_clock_gap_means_the_machine_slept() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert!(!slept(t, t + TICK));
        assert!(!slept(t, t + Duration::from_secs(20)));
        assert!(slept(t, t + Duration::from_secs(31)));
        assert!(slept(t, t + Duration::from_secs(3600)));
        // The clock set back: not a sleep.
        assert!(!slept(t, t - Duration::from_secs(3600)));
    }
}
