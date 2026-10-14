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
use std::time::{Duration, Instant, SystemTime};

use hbsync_engine::activity::Counters;
use hbsync_engine::exec::Stopped;
use hbsync_engine::feed::FeedError;
use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_engine::{Conflict, Drive, Engine, EngineConfig, EngineError, SyncReport};
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_local::watch::Debounce;
use hbsync_local::{Filter, FilterError, LocalObserver};
use hbsync_remote::{Remote, RemoteError};
use hbsync_state::StateError;
use hbsync_types::NodeId;
pub use hbsync_types::activity::Progress;
use hbsync_types::unix_now;
use serde::{Deserialize, Serialize};

use crate::MassDelete;
use crate::config::KeepDays;

/// After a failure: wait this long, doubling up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// How long to wait after each failure in a row.
#[derive(Debug)]
pub(crate) struct Backoff(Duration);

impl Backoff {
    pub(crate) fn new() -> Backoff {
        Backoff(FIRST_BACKOFF)
    }

    /// The wait after a failure that left `state`. A refusal won't change until this device is
    /// authorized again: ask rarely.
    pub(crate) fn after(&mut self, state: State) -> Duration {
        let wait = if state == State::Denied {
            MAX_BACKOFF
        } else {
            self.0
        };
        self.0 = (self.0 * 2).min(MAX_BACKOFF);
        wait
    }
}
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
    #[error(transparent)]
    Folder(#[from] RootError),
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub folder: PathBuf,
    /// The account's state DB ([`crate::Paths::account_db`]).
    pub db: PathBuf,
    pub identity: String,
    /// Shown in conflict-copy names.
    pub device: String,
    pub mass_delete: MassDelete,
    /// Recently deleted keeps an item this many days.
    pub keep_deleted_days: u32,
    /// Start paused: until then, or until resumed (`Some(None)`).
    pub paused: Option<Until>,
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
    pub fn new(folder: &Path, db: &Path, identity: &str, device: &str) -> Self {
        RunnerConfig {
            folder: folder.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            device: device.to_owned(),
            mass_delete: MassDelete::default(),
            keep_deleted_days: KeepDays::default().0,
            paused: None,
            poll: Duration::from_secs(60),
            once: false,
            credentials_warning: None,
            tweak: None,
        }
    }
}

/// A sync announced by the drive waits until its changes have stopped coming this long...
const REMOTE_QUIET: Duration = Duration::from_millis(1000);
/// ...but no longer than this after the first.
const REMOTE_MAX_WAIT: Duration = Duration::from_secs(10);

/// How long a pause lasts: until then (Unix seconds), or until resumed (`None`).
pub type Until = Option<u64>;

/// Requests to the runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
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
    /// Restore these items of Recently deleted, each with what was deleted with it.
    Restore {
        nodes: Vec<NodeId>,
    },
    /// Delete these items of Recently deleted for good, each with what was deleted with it.
    EmptyDeleted {
        nodes: Vec<NodeId>,
    },
    /// Delete everything in Recently deleted removed until then (Unix ms) for good: what the
    /// user saw, nothing removed since.
    EmptyDeletedUntil {
        at_ms: i64,
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
    /// The daemon's version (empty from a daemon too old to say).
    #[serde(default)]
    pub version: String,
    pub state: State,
    /// Until when a pause lasts (Unix seconds); `None` while not paused or until resumed.
    pub paused_until: Option<u64>,
    /// How far the sync in progress is.
    pub progress: Option<Progress>,
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
    /// Notes added since the daemon started (more than [`Status::recent`] keeps): what is
    /// new since a client last looked.
    pub notes_added: u64,
    /// Entries added to the recently synced list since the daemon started: a client reads
    /// the list again ([`crate::ipc::Ask::Recent`]) when this changes.
    pub synced_added: u64,
    /// Why there is no live watcher (the folder is then rescanned at every poll).
    pub watcher_error: Option<String>,
    pub hints: Hints,
    /// Ignore rules in `.hbsync/ignore`.
    pub rules: usize,
    /// Why the credentials aren't in the keychain.
    pub credentials_warning: Option<String>,
}

impl Status {
    /// Starting to sync `folder` with `identity`.
    pub fn new(identity: &str, folder: &Path) -> Status {
        Status {
            version: crate::VERSION.into(),
            state: State::Starting,
            paused_until: None,
            progress: None,
            identity: identity.to_owned(),
            folder: folder.to_path_buf(),
            error: None,
            retry_at: None,
            last_sync: None,
            pending: 0,
            held_deletes: None,
            recent: Vec::new(),
            notes_added: 0,
            synced_added: 0,
            watcher_error: None,
            hints: Hints::Off,
            rules: 0,
            credentials_warning: None,
        }
    }
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
    /// Items deleted here, to be deleted on the drive.
    pub here: u64,
    /// Items deleted elsewhere, to be moved to the trash here.
    pub remotely: u64,
    /// Some of them.
    pub examples: Vec<String>,
    /// Names exactly these deletes: a confirmation carries it back.
    pub token: String,
}

impl HeldDeletes {
    pub fn total(&self) -> u64 {
        self.here + self.remotely
    }

    /// More are held than [`HeldDeletes::examples`] shows.
    pub fn more_than_shown(&self) -> bool {
        (self.examples.len() as u64) < self.total()
    }
}

/// The token of the held deletes of `nodes`, a hash of their sorted ids (made and checked by
/// one daemon process).
fn held_token(nodes: &[NodeId]) -> String {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut sorted = nodes.to_vec();
    sorted.sort();
    let mut hasher = DefaultHasher::new();
    sorted.hash(&mut hasher);
    format!("{:016x}-{}", hasher.finish(), sorted.len())
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
    progress: Arc<Counters>,
}

impl Handle {
    /// Passes `control` on. Pause and stop also interrupt a sync in progress. False when the
    /// runner has ended.
    pub fn send(&self, control: Control) -> bool {
        if matches!(control, Control::Pause { .. } | Control::Stop) {
            self.stop.store(true, Ordering::Relaxed);
        }
        self.control.send(control).is_ok()
    }

    /// Paused while the pause flag is set: seen at once, not only once the runner gets to it.
    pub fn status(&self) -> Status {
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
    remote: Arc<dyn Remote>,
    hints: HintSource,
    control: Receiver<Control>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<Status>>,
    progress: Arc<Counters>,
    /// The nodes of the deletes held in the last sync (what the status shows).
    held: Vec<NodeId>,
    /// The conflicts and failures of the last sync, to tell each one only once.
    last_notes: std::collections::HashSet<String>,
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
    /// Starts syncing `cfg.folder` with `remote` on a new thread.
    pub fn spawn(
        cfg: RunnerConfig,
        remote: Arc<dyn Remote>,
        hints: HintSource,
    ) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
        let (tx, rx) = channel();
        let stop = Arc::new(AtomicBool::new(cfg.paused.is_some()));
        let mut status = Status::new(&cfg.identity, &cfg.folder);
        if hints.notifier.is_some() {
            status.hints = Hints::Connecting;
        }
        status.credentials_warning = cfg.credentials_warning.clone();
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
            remote,
            hints,
            control: rx,
            stop,
            status,
            progress,
            held: Vec::new(),
            last_notes: Default::default(),
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
        let drive = Drive::new(sync_root.account()?, Arc::clone(&self.remote));
        let mut engine = Engine::new(sync_root.state()?, drive, cfg);
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
        Ok(Parts { engine, observer })
    }

    fn run(mut self) -> Result<(), RunnerError> {
        // Opened when there is something to sync and the folder is there; dropped when the
        // folder goes away or the ignore rules change.
        let mut parts: Option<Parts> = None;
        let mut confirmed = None;
        // Restores and empties of Recently deleted asked for, run when the engine is open.
        let mut restore: Vec<NodeId> = Vec::new();
        let mut empty: Vec<NodeId> = Vec::new();
        let mut empty_until: Option<i64> = None;
        // Changes announced on the drive: synced once they stop coming (a burst of deletes is
        // judged whole by the mass-delete guard), or after REMOTE_MAX_WAIT.
        // (the first announcement, quiet from then on)
        let mut announced: Option<(Instant, Instant)> = None;
        let mut pause = self.cfg.paused;
        let mut started = false;
        let mut wanted = true;
        let mut check_rules = false;
        let mut hints_open = true;
        let mut next_poll = Instant::now() + self.cfg.poll;
        let mut not_before = Instant::now();
        let mut backoff = Backoff::new();
        tracing::info!(
            "syncing {} with {}{}",
            self.cfg.folder.display(),
            self.cfg.identity,
            if pause.is_some() { " (paused)" } else { "" }
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
                        wanted = true;
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
                wanted = true;
                backoff = Backoff::new();
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
                    Ok(Control::Pause { until }) => {
                        pause = Some(until);
                        self.set(|s| s.paused_until = until);
                        match until {
                            Some(t) => tracing::info!(
                                "paused for {} min",
                                t.saturating_sub(unix_now()) / 60
                            ),
                            None => tracing::info!("paused"),
                        }
                    }
                    Ok(Control::Resume) => {
                        if pause.take().is_some() {
                            tracing::info!("resumed");
                        }
                        wanted = true;
                        not_before = Instant::now();
                        self.resume();
                    }
                    Ok(Control::ConfirmDeletes { token }) => {
                        if token != held_token(&self.held) {
                            tracing::warn!("a confirmation of deletes that changed since: ignored");
                            continue;
                        }
                        tracing::info!("{} held deletes confirmed", self.held.len());
                        confirmed = Some(self.held.clone());
                        wanted = true;
                        not_before = Instant::now();
                    }
                    Ok(Control::Restore { nodes }) => {
                        restore.extend(nodes);
                        wanted = true;
                        not_before = Instant::now();
                    }
                    Ok(Control::EmptyDeleted { nodes }) => {
                        empty.extend(nodes);
                        wanted = true;
                        not_before = Instant::now();
                    }
                    Ok(Control::EmptyDeletedUntil { at_ms }) => {
                        empty_until = Some(empty_until.map_or(at_ms, |t| t.max(at_ms)));
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
                        let now = Instant::now();
                        let first = announced.map_or(now, |(first, _)| first);
                        announced = Some((first, now + REMOTE_QUIET));
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

            if pause.flatten().is_some_and(|t| unix_now() >= t) {
                tracing::info!("the pause is over: resuming");
                pause = None;
                wanted = true;
                not_before = Instant::now();
                self.resume();
            }
            let now = Instant::now();
            if now >= next_poll {
                // At every poll, whether the syncs succeed or back off.
                next_poll = now + self.cfg.poll;
                wanted = true;
                check_rules = true;
                if let Some(p) = &mut parts
                    && pause.is_none()
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
                wanted = true;
            }
            if pause.is_some() || !wanted || now < not_before {
                continue;
            }
            if announced.is_some_and(|(first, quiet)| now < quiet && now < first + REMOTE_MAX_WAIT)
            {
                continue;
            }
            announced = None;
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
                    not_before = Instant::now() + self.cfg.poll;
                    self.set(|s| {
                        s.state = State::Failing;
                        s.error = Some(GONE.into());
                        s.retry_at = Some(unix_now() + self.cfg.poll.as_secs());
                    });
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
            if !restore.is_empty() {
                match p.engine.drive().restore(&std::mem::take(&mut restore)) {
                    Ok(n) => tracing::info!("{n} items restored from Recently deleted"),
                    Err(e) => tracing::warn!("restoring from Recently deleted failed: {e}"),
                }
            }
            let nodes = std::mem::take(&mut empty);
            let emptied = [
                empty_until
                    .take()
                    .map(|at| p.engine.drive().empty_until(at)),
                (!nodes.is_empty()).then(|| p.engine.drive().empty(&nodes)),
            ];
            for result in emptied.into_iter().flatten() {
                match result {
                    Ok(n) => tracing::info!("{n} items deleted for good"),
                    Err(e) => tracing::warn!("emptying Recently deleted failed: {e}"),
                }
            }
            // Once a day; a failure waits for the next sync.
            let keep_days = self.cfg.keep_deleted_days;
            match p.engine.drive().purge_when_due(keep_days) {
                Ok(Some(n)) if n > 0 => {
                    tracing::info!("{n} items deleted for good after {keep_days} days")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("purging Recently deleted failed: {e}"),
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
                    backoff = Backoff::new();
                    self.record(&mut p.engine, &report, began.elapsed())?;
                    // Stopped by a pause (or a stop): the rest waits for resume.
                    if !report.stopped {
                        wanted = false;
                    }
                    if self.cfg.once {
                        return Ok(());
                    }
                }
                Err(e) => {
                    let state = if e.is_denied() {
                        State::Denied
                    } else if is_offline(&e) {
                        State::Offline
                    } else {
                        State::Failing
                    };
                    let wait = backoff.after(state);
                    tracing::warn!("sync failed ({e}); trying again in {wait:?}");
                    not_before = Instant::now() + wait;
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
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
        took: Duration,
    ) -> Result<(), RunnerError> {
        let at = unix_now();
        // Each conflict and failure, worded for people. One that keeps coming back sync after
        // sync (a file that can't be read) is told once, not every minute.
        let mut notes = Vec::new();
        for c in &report.conflicts {
            notes.push(describe_conflict(engine, c)?);
        }
        for (op, why) in report.failed.iter().chain(&report.skipped) {
            let path = engine.path_of(op.node())?.unwrap_or_else(|| "?".into());
            notes.push(format!("{path}: {} ({why})", op_name(op)));
        }
        // Drive rows the client can't read (not encrypted, another key, not a sync node).
        for why in self.remote.take_skipped() {
            notes.push(format!("a file on Homebase can't be synced: {why}"));
        }
        let seen = std::mem::take(&mut self.last_notes);
        self.last_notes = notes.iter().cloned().collect();
        notes.retain(|n| !seen.contains(n));
        self.held = report.held.iter().map(|op| op.node()).collect();
        let held = if report.held.is_empty() {
            None
        } else {
            let here = report
                .held
                .iter()
                .filter(|op| matches!(op, hbsync_engine::Op::RemoteDelete { .. }))
                .count() as u64;
            let mut examples = Vec::new();
            for op in report.held.iter().take(EXAMPLES) {
                examples.extend(engine.path_of(op.node())?);
            }
            Some(HeldDeletes {
                here,
                remotely: report.held.len() as u64 - here,
                examples,
                token: held_token(&self.held),
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
            s.notes_added += notes.len() as u64;
            s.recent
                .extend(notes.into_iter().map(|text| Note { at, text }));
            let excess = s.recent.len().saturating_sub(RECENT);
            s.recent.drain(..excess);
            s.synced_added += report.synced.len() as u64;
        });
        Ok(())
    }
}

/// Whether the machine slept during a wait that took `wall` by the wall clock (`None`: it was
/// set back) and `mono` by the monotonic clock, `worked` telling whether the wait also did
/// work (recorded a batch, rescanned). The monotonic clock stops in sleep on Linux and macOS:
/// a wall clock far ahead of it is a sleep. On Windows it runs on, so a long wait that did no
/// work counts too. A clock set forward only costs a sync.
fn slept(wall: Option<Duration>, mono: Duration, worked: bool) -> bool {
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
