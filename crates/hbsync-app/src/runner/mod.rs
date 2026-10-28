//! The sync loop for one folder, on its own thread: what the daemon runs.
//!
//! A sync runs when the folder changes (live watcher), when its part of the drive changed (the
//! drive wakes it: a change the identity announced, the notifications connecting again, another
//! folder's sync), when asked, and after a backoff when a sync left work unfinished; never on
//! a timer of its own (daemon.md, "No poll"). After a failed sync the runner waits, doubling
//! up to five minutes. It is controlled through a [`Handle`] and publishes a [`FolderStatus`].
//!
//! - **Pause** stops a running sync between operations (and before the next transfer) and
//!   syncs nothing until **resume**; the watcher keeps recording changes meanwhile. A timed
//!   pause shows its end, and ends with a resume too: the daemon's one timer sends it.
//! - **Sleep**: told by its account ([`Command::Woke`]), the runner syncs at once and forgets
//!   its backoff.
//! - **Ignore rules** (`.hbsync/ignore`) are checked for changes when the watcher sees the file
//!   change (once a minute without a watcher) and on request; a change restarts the engine,
//!   which re-plans everything (sync semantics §10).
//!
//! No poll: the runner blocks on one channel (its commands, the watcher's bell, the drive's
//! wakes) until the next thing time alone changes ([`Schedule::next_wake`]), so an idle folder
//! with a watcher wakes only every [`WAIT_AT_MOST`](crate::sync::WAIT_AT_MOST).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_engine::activity::Counters;
use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_engine::{Drive, Engine, EngineConfig, EngineError, SyncReport};
use hbsync_local::journal::Tier;
use hbsync_local::{Bell, Filter, FilterError, LocalObserver};
use hbsync_platform::io::Io;
use hbsync_remote::ErrorClass;
use hbsync_state::{StateError, Unusable};
use hbsync_types::NodeId;
use hbsync_types::unix_now;

use crate::protocol::{FolderStatus, HeldAnswer, HeldDeletes, Pause, State};
use crate::sync::{OnDrop, Wakeup, lock, wait_for};
use crate::{Identity, MassDelete};

mod report;
mod schedule;

pub(crate) use report::add_notes;
pub(crate) use schedule::{Backoff, shown};
use schedule::{Schedule, Synced, Why, Whys};

/// While the identity refuses this computer, how often a folder asks again (one sync).
const DENIED_LOOK: Duration = Duration::from_secs(3600);

/// Told when an answer to held deletes that was taken can't be used: what it named is no
/// longer held there.
const ANSWER_DROPPED_GONE: &str = "Your answer to the deletions to review wasn't used: the folder is gone. Once it is back, answer again.";
const ANSWER_DROPPED_SET_UP: &str = "Your answer to the deletions to review wasn't used: the folder was set up again. Answer again.";

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

impl RunnerError {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            RunnerError::State(e) => e.class(),
            RunnerError::Engine(e) => e.class(),
            // A rules file that can't be read: the user's to fix.
            RunnerError::Filter(_) => ErrorClass::NeedsUser,
            RunnerError::Io(e) => ErrorClass::of_io(e),
            RunnerError::Panicked => ErrorClass::Bug,
            RunnerError::Folder(e) => e.class(),
        }
    }

    /// Why the account's state DB can't be used at all, if that is what it means
    /// ([`StateError::unusable`]): its account sets it aside ([`crate::folders::heal_account`]).
    pub fn unusable(&self) -> Option<Unusable> {
        match self {
            RunnerError::State(e) => e.unusable(),
            RunnerError::Folder(e) => e.unusable(),
            RunnerError::Engine(e) => e.unusable(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub folder: PathBuf,
    /// The account's state DB ([`crate::Paths::account_db`]).
    pub db: PathBuf,
    pub identity: Identity,
    /// The role folder it is; `None` for the main folder.
    pub role: Option<hbsync_types::Role>,
    /// Shown in conflict-copy names.
    pub device: String,
    pub mass_delete: MassDelete,
    /// Start paused, with this pause.
    pub paused: Option<Pause>,
    /// How often what is local is looked at: the ignore rules, whether a folder that is gone
    /// is back, a rescan of a folder without a watcher.
    pub look: Duration,
    /// Catch up, sync once, and stop (development).
    pub once: bool,
    /// How long after a put back that failed it runs once more (the same answer), while the
    /// same deletes are held: what the identity said once wasn't there is gone for good when
    /// it says so again a minute on (sync semantics §8b).
    pub put_back_again: Duration,
    /// Tests: adjusts the engine's config.
    pub tweak: Option<fn(&mut EngineConfig)>,
    /// Raised when its status may have changed (the daemon pushes it to the tray).
    pub(crate) changed: Arc<Wakeup>,
    /// Its account sets the state DB aside when it can't be used ([`RunnerError::unusable`]):
    /// the runner ends for that, rather than backing off as after any failed sync.
    pub(crate) end_if_unusable: bool,
}

impl RunnerConfig {
    pub fn new(folder: &Path, db: &Path, identity: &Identity, device: &str) -> Self {
        RunnerConfig {
            folder: folder.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.clone(),
            role: None,
            device: device.to_owned(),
            mass_delete: MassDelete::default(),
            paused: None,
            look: Duration::from_secs(60),
            once: false,
            put_back_again: hbsync_engine::gone::ASK_AGAIN,
            tweak: None,
            changed: Arc::default(),
            end_if_unusable: false,
        }
    }
}

/// The deletes held in the last sync, as the status shows them until they are answered, and the
/// user's answer to them for the next sync: one lock, so an answer is checked against what it
/// answers and kept with the nodes it named, and the status shows what is unanswered.
#[derive(Debug, Default)]
struct Held {
    nodes: Arc<[NodeId]>,
    /// As the status shows them: `None` once answered, until a sync shows them again.
    shown: Option<HeldDeletes>,
    /// Their token ([`report::held_token`]).
    token: String,
    /// What the next sync does with the nodes the user answered for (the ones shown then).
    answer: Option<(HeldAnswer, Arc<[NodeId]>)>,
    /// They were answered: another answer is refused until a sync shows them again.
    answered: bool,
    /// The answer is a put back that failed, run once more ([`RunnerConfig::put_back_again`]).
    again: bool,
}

impl Held {
    /// Nothing held, and no answer kept: whether an answer was dropped.
    fn clear(&mut self) -> bool {
        std::mem::take(self).answer.is_some()
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
    /// Pause syncing: until `Resume` or `PauseEnded`, also a timed pause (its end is shown).
    Pause(Pause),
    /// The user resumed: what the drive refused is asked again too.
    Resume,
    /// A timed pause ran out (the daemon's timer): syncing goes on as it was.
    PauseEnded,
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
    /// The user answered the held deletes ([`Handle::answer`]): a sync is asked for.
    Answered,
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
    /// Its `held_deletes` is filled from `held` by [`Handle::status`], not stored here.
    status: Arc<Mutex<FolderStatus>>,
    held: Arc<Mutex<Held>>,
    progress: Arc<Counters>,
    #[cfg(feature = "testkit")]
    woken: crate::TestCount,
    /// Raised when its status may have changed.
    changed: Arc<Wakeup>,
}

impl Command {
    /// What pauses (`Some`) or resumes a folder.
    pub fn pausing(pause: Option<Pause>) -> Command {
        match pause {
            Some(pause) => Command::Pause(pause),
            None => Command::Resume,
        }
    }

    /// The pause it sets: `Some` for a pause, `None` for anything else.
    pub(crate) fn paused(&self) -> Option<Pause> {
        match self {
            Command::Pause(pause) => Some(*pause),
            _ => None,
        }
    }
}

impl Handle {
    /// Passes `control` on. Pause and stop also interrupt a sync in progress. False when the
    /// runner has ended.
    pub fn send(&self, control: Command) -> bool {
        let interrupts = matches!(control, Command::Pause(_) | Command::Stop);
        let sent = self.control.0.send(Msg::Command(control)).is_ok();
        // Raised once queued: a Resume the runner takes meanwhile lowers it, but then the
        // runner takes this command right after it, which raises it again.
        if interrupts {
            self.stop.store(true, Ordering::Relaxed);
            // Shown as paused at once.
            self.changed.raise();
        }
        sent
    }

    /// Answers the held deletes named by `token` (as the user saw them) with `answer`. Checked
    /// against what the runner holds now: refused when they changed since, or the folder is
    /// gone (nothing is held there). Taken, it goes with the next sync (after one in progress;
    /// paused, once syncing resumes), for exactly the deletes it named: any held since stay
    /// held, shown again.
    pub fn answer(&self, answer: HeldAnswer, token: &str) -> Result<(), String> {
        {
            let mut held = lock(&self.held);
            if held.nodes.is_empty() || held.token != token {
                return Err(
                    "the deletions to review changed since you looked (or the folder is gone): \
                            look at them again"
                        .into(),
                );
            }
            if held.answered {
                return Err(
                    "they were answered already: look at them again after the next sync".into(),
                );
            }
            // Under `held`: the runner takes the answer once it is set.
            (self.control.0.send(Msg::Answered)).map_err(|_| "the sync has stopped".to_owned())?;
            tracing::info!("{} held deletes answered: {answer:?}", held.nodes.len());
            held.answer = Some((answer, Arc::clone(&held.nodes)));
            held.answered = true;
            // No longer to review, while the sync carrying the answer out runs.
            held.shown = None;
        }
        self.changed.raise();
        Ok(())
    }

    /// How often the runner's loop has woken up (tests: none while idle).
    #[cfg(feature = "testkit")]
    pub fn woken(&self) -> u64 {
        self.woken.get()
    }

    /// Paused while the pause flag is set: seen at once, not only once the runner gets to it.
    pub fn status(&self) -> FolderStatus {
        // `held`, then the status, as a sync's end writes them: one sync's picture.
        let held = lock(&self.held);
        let mut status = lock(&self.status).clone();
        status.held_deletes = held.shown.clone();
        drop(held);
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

pub struct Runner {
    cfg: RunnerConfig,
    drive: Arc<Drive>,
    control: Receiver<Msg>,
    /// Its own channel, for the watcher's bell and the drive's wakes.
    wakes: Sender<Msg>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<FolderStatus>>,
    progress: Arc<Counters>,
    /// How often its loop woke up (`Handle::woken`).
    woken: crate::TestCount,
    /// The deletes held in the last sync, and the answer to them.
    held: Arc<Mutex<Held>>,
    /// The conflicts and failures of the last sync, to tell each one only once.
    told: report::Told,
    /// Stale skips sync after sync: one that keeps coming back is told after all.
    stalled: report::Stalled,
    /// What scans skipped and told, each once ([`report::scan_notes`]).
    scan_told: std::collections::HashSet<u64>,
    /// The version of the ignore rules last told about (a hash of their fingerprint; 0: rules
    /// that can't be used, [`Runner::tell_rules_failing`]), and the note told for their lines
    /// that aren't valid rules, if any ([`report::rules_note`]).
    rules_told: Option<(u64, Option<String>)>,
    /// Sync now was asked: the next sync sends what the drive refused once more.
    retry_refused: bool,
    /// Waited out a refusal: the next due sync asks the drive again ([`DENIED_LOOK`]).
    probing: bool,
    /// The folder without a live watcher: told, asked for again, caught up on.
    unwatched: Unwatched,
    /// What the process had read and written as the last sync ended ([`hbsync_platform::io`]).
    io_after: Option<Io>,
}

/// A folder without a live watcher ([`Runner::watcher_is`]): its changes are found by the
/// look's walk till a watcher starts again.
#[derive(Debug, Default)]
struct Unwatched {
    /// Why there is none, as the status shows it.
    why: Option<String>,
    /// The note shown, taken back once a watcher is there again; told once a run.
    note: Option<String>,
    told: bool,
    /// When to ask for a watcher again, and how long after that if it fails then (from the
    /// look, doubling: a tree over the watch limit fails each time, and each try walks it).
    retry_at: Option<Instant>,
    retry_wait: Duration,
    /// A watcher started again: the look's walk to catch up on what changed without one is
    /// still to be done.
    catch_up: bool,
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
    /// Whether the ignore rules on disk differ from the ones in use (or can't be read): their
    /// fingerprints differ (sync semantics §10).
    fn rules_changed(&self, root: &Path) -> bool {
        let folder = &self.engine.config().folder;
        Filter::load(root, folder.case_insensitive)
            .map_or(true, |f| f.fingerprint() != folder.filter.fingerprint())
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
        status.paused_until = cfg.paused.and_then(|p| p.until);
        let status = Arc::new(Mutex::new(status));
        let held = Arc::new(Mutex::new(Held::default()));
        let progress = Arc::new(Counters::default());
        let woken = crate::TestCount::default();
        let handle = Handle {
            control: Arc::new(Owner(tx.clone())),
            stop: Arc::clone(&stop),
            status: Arc::clone(&status),
            held: Arc::clone(&held),
            progress: Arc::clone(&progress),
            #[cfg(feature = "testkit")]
            woken: woken.clone(),
            changed: cfg.changed.clone(),
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
            held,
            told: Default::default(),
            stalled: Default::default(),
            scan_told: Default::default(),
            rules_told: None,
            retry_refused: false,
            probing: false,
            unwatched: Unwatched::default(),
            io_after: None,
        };
        let thread = std::thread::Builder::new()
            .name("hbsync-runner".into())
            .spawn(move || {
                // Told also when it panics.
                let _ended = OnDrop(Some(ended));
                let (status, changed) = (Arc::clone(&runner.status), runner.cfg.changed.clone());
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
                changed.raise();
                result
            })?;
        Ok((handle, thread))
    }

    fn set(&self, f: impl FnOnce(&mut FolderStatus)) {
        f(&mut lock(&self.status));
        self.cfg.changed.raise();
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
        // The rules are read before the watcher is there to see them change: an edit in
        // between would wait for the next look (a minute). Read again once it is (one read
        // more an open); changed, opened again with them.
        // Edited all the while: the watcher, there now, sees the next edit.
        for _ in 0..2 {
            let parts = self.open_once()?;
            if parts.observer.watcher_error.is_some() || !parts.rules_changed(&self.cfg.folder) {
                return Ok(parts);
            }
            tracing::info!("the ignore rules changed as the folder opened: opening it again");
        }
        self.open_once()
    }

    fn open_once(&self) -> Result<Parts, RunnerError> {
        let root = &self.cfg.folder;
        let mut cfg = EngineConfig::new(root, &self.cfg.device);
        cfg.guard.min = self.cfg.mass_delete.min_files;
        cfg.guard.fraction = self.cfg.mass_delete.fraction;
        cfg.folder.filter = Arc::new(Filter::load(root, cfg.folder.case_insensitive)?);
        if let Some(tweak) = self.cfg.tweak {
            tweak(&mut cfg);
        }
        let rules = cfg.folder.filter.rule_count();
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
        self.set(|s| s.rules = rules);
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
        // The folder's marker when it was last opened: another one means it was set up again
        // (a new folder in the DB, the engine's gone), also while nothing is open.
        let mut opened: Option<String> = None;
        let mut schedule = Schedule::new(Instant::now(), self.cfg.look, self.cfg.paused.is_some());
        let mut started = false;
        let mut check_rules = false;
        // When to read the ignore rules, touched a moment ago.
        let mut rules_at: Option<Instant> = None;
        // After taking local changes failed: not again before then.
        let mut observe_after: Option<Instant> = None;
        // A put back that failed: when it runs once more, and the nodes it was answered for.
        let mut put_back_again: Option<(Instant, Arc<[NodeId]>)> = None;
        tracing::info!(
            "syncing {} with {}{}",
            self.cfg.folder.display(),
            self.cfg.identity,
            if schedule.paused() { " (paused)" } else { "" }
        );
        loop {
            // Until something happens, or time alone changes something.
            let now = Instant::now();
            let looking =
                parts.as_ref().is_none_or(|p| !p.observer.has_watcher()) || self.unwatched.catch_up;
            let observer = match observe_after {
                Some(at) if now < at => Some(at),
                _ => parts.as_ref().and_then(|p| p.observer.deadline()),
            };
            let wait = wait_for(
                now,
                [
                    schedule.next_wake(now, looking),
                    observer,
                    rules_at,
                    put_back_again.as_ref().map(|(at, _)| *at),
                ]
                .into_iter()
                .flatten(),
            );
            let mut first = self.control.recv_timeout(wait).ok();
            self.woken.add();
            while let Some(msg) = first.take().or_else(|| self.control.try_recv().ok()) {
                match msg {
                    Msg::Command(Command::Stop) | Msg::Orphaned => {
                        tracing::info!("stopped");
                        return Ok(());
                    }
                    Msg::Command(command) => self.command(command, &mut schedule, &mut check_rules),
                    Msg::Answered => schedule.asked(Instant::now(), Why::Asked),
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
                    schedule.want(Why::Drive);
                }
                let observing = observe_after.is_none_or(|at| Instant::now() >= at);
                match observing.then(|| p.engine.observe(&mut p.observer, Duration::ZERO)) {
                    Some(Ok(scan)) => {
                        observe_after = None;
                        if let Some(scan) = scan {
                            self.tell_scan(&scan, false);
                            if scan.dirty > 0 {
                                schedule.want(Why::Local);
                            }
                        }
                    }
                    // A DB that can't be used: its account sets it aside.
                    Some(Err(e)) if self.ends_for(e.unusable()) => return Err(e.into()),
                    // Kept (a busy DB, say): the folder backs off, as after a failed sync,
                    // instead of starting over, and doesn't take them again before then.
                    Some(Err(e)) => {
                        let wait = self.backs_off(
                            &mut schedule,
                            "taking local changes",
                            e.class(),
                            &e,
                            None,
                        );
                        observe_after = Some(Instant::now() + wait);
                    }
                    None => {}
                }
                // After the poll, which has the watcher ring again for a touch from now on.
                if p.observer.rules_touched() {
                    rules_at = Some(Instant::now() + RULES_QUIET);
                }
                // One that stopped.
                if self.unwatched.why.is_none()
                    && let Some(why) = p.observer.watcher_error.clone()
                {
                    self.watcher_is(Some(why));
                }
            }

            let now = Instant::now();
            // Once more, for what of it is still held (what the user saw: one held since isn't
            // covered), unless the user answered meanwhile.
            if put_back_again.as_ref().is_some_and(|(at, _)| now >= *at)
                && let Some((_, seen)) = put_back_again.take()
            {
                let mut held = lock(&self.held);
                let still: Arc<[NodeId]> = held
                    .nodes
                    .iter()
                    .filter(|n| seen.contains(n))
                    .copied()
                    .collect();
                if held.answer.is_none() && !held.answered && !still.is_empty() {
                    tracing::info!("putting {} held deletes back once more", still.len());
                    held.answer = Some((HeldAnswer::PutBack, still));
                    held.answered = true;
                    held.again = true;
                    held.shown = None;
                    self.cfg.changed.raise();
                    schedule.asked(now, Why::Asked);
                }
            }
            if rules_at.is_some_and(|at| now >= at) {
                rules_at = None;
                check_rules = true;
            }
            if schedule.look_due(now) {
                check_rules = true;
                // Without a watcher, one is asked for again (its error gone: inotify's
                // instances freed); the folder's changes are found by scanning it, and once
                // one is back, what changed while there was none.
                if let Some(p) = &mut parts {
                    if !p.observer.has_watcher()
                        && self.unwatched.retry_at.is_none_or(|at| now >= at)
                    {
                        let back = p.observer.rewatch();
                        let why = p.observer.watcher_error.clone();
                        self.watcher_is(why);
                        self.unwatched.catch_up |= back;
                    }
                    if self.unwatched.catch_up || (!p.observer.has_watcher() && !schedule.paused())
                    {
                        match p.engine.scan_local() {
                            Ok(scan) => {
                                self.unwatched.catch_up = false;
                                self.tell_scan(&scan, true);
                                if scan.dirty > 0 {
                                    schedule.want(Why::Scan);
                                }
                            }
                            Err(e) if self.ends_for(e.unusable()) => return Err(e.into()),
                            // Tried again at the next look.
                            Err(e) => tracing::warn!("scanning the folder failed: {e}"),
                        }
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
                schedule.want(Why::Rules);
            }
            if !schedule.due(now) {
                continue;
            }
            // The identity refused this computer: not asked again until the user acts or the
            // notifications connect (`Drive::deny`); a look now and then, asking nothing.
            // An hour on, one sync asks again (a 403 for one write needn't hold the account for
            // good): refused again, it latches again.
            match (self.drive.denied(), std::mem::take(&mut self.probing)) {
                (Some(why), false) => {
                    let class = ErrorClass::Denied;
                    self.backs_off(&mut schedule, "the sync", class, &why, Some(DENIED_LOOK));
                    self.probing = true;
                    continue;
                }
                (Some(_), true) => self.drive.allow(),
                (None, _) => {}
            }
            let marker = match SyncRoot::check(&self.cfg.folder, &self.cfg.identity) {
                Ok(marker) => marker.id,
                // Unmounted, moved or deleted: syncing would take everything off the drive,
                // or fill an empty mount point. Wait for it to come back.
                Err(RootError::NoMarker(_) | RootError::Io(_)) => {
                    const GONE: &str = "the folder is gone (moved, deleted or not mounted)";
                    if lock(&self.status).error.as_deref() != Some(GONE) {
                        tracing::warn!("{GONE}: waiting for it to come back");
                    }
                    parts = None;
                    // Nothing is held, sent or put back there meanwhile: an answer is refused,
                    // and one taken is dropped.
                    if lock(&self.held).clear() {
                        self.tell(vec![ANSWER_DROPPED_GONE.into()]);
                    }
                    self.failing(State::Failing, GONE.into(), schedule.later(Instant::now()));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            // Set up again meanwhile (a new folder in the DB, the open one removed): the
            // engine would only fail now. Start over with the new one.
            if opened.as_ref().is_some_and(|m| *m != marker) {
                tracing::info!("the folder was set up again: starting over");
                parts = None;
                // What is held, and an answer, were about the old one's nodes.
                if lock(&self.held).clear() {
                    self.tell(vec![ANSWER_DROPPED_SET_UP.into()]);
                }
                self.cfg.changed.raise();
            }
            let p = match &mut parts {
                Some(p) => p,
                None => match self.open() {
                    Ok(p) => {
                        self.tell_rules(Some(&p.engine.config().folder.filter));
                        self.watcher_is(p.observer.watcher_error.clone());
                        started = false;
                        opened = Some(marker);
                        parts.insert(p)
                    }
                    // Rules that can't be read, or with a negation that isn't a valid rule
                    // (skipped, it would ignore what it keeps synced, §10: told once): say so,
                    // and look again at the next look.
                    Err(RunnerError::Filter(e)) if !self.cfg.once => {
                        match &e {
                            FilterError::Negation { .. } => self.tell_rules_failing(&e),
                            _ => {
                                tracing::warn!("{e}");
                                self.tell_rules(None);
                            }
                        }
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
            let (answer, once_more) = {
                let mut held = lock(&self.held);
                let again = std::mem::take(&mut held.again);
                (held.answer.take(), again)
            };
            match answer {
                Some((HeldAnswer::Delete, seen)) => p.engine.confirm_deletes(seen.iter().copied()),
                Some((HeldAnswer::PutBack, seen)) => {
                    if let Err(e) = p.engine.put_back(&seen) {
                        // What it didn't put back is still held, and shown again; the first
                        // time, it runs once more a minute on (a file the identity said once
                        // isn't there is gone for good then, and goes up again from here).
                        let then = if once_more {
                            "what is still to review is shown again, to answer again"
                        } else {
                            let at = Instant::now() + self.cfg.put_back_again;
                            put_back_again = Some((at, Arc::clone(&seen)));
                            "tried again in a minute; what is still to review is shown again"
                        };
                        self.tell(vec![format!(
                            "Not all of the deletions to review could be put back ({e}): {then}"
                        )]);
                    }
                }
                None => {}
            }
            if std::mem::take(&mut self.retry_refused)
                && let Err(e) = p.engine.clear_refused()
            {
                tracing::warn!("can't forget what the drive refused: {e}");
            }
            // This sync pulls what woke it so far.
            p.drive_changed.store(false, Ordering::SeqCst);
            let began = Instant::now();
            let why = schedule.take_why(began);
            let io_before = hbsync_platform::io::counted();
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
                p.engine.start_up(&mut p.observer, &today).map(|r| {
                    self.tell_scan(
                        &r.local.report,
                        matches!(r.local.tier, Tier::FullWalk { .. }),
                    );
                    r.sync
                })
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
                    let took = Sync {
                        took: began.elapsed(),
                        why,
                        io_before,
                    };
                    match self.record(&mut p.engine, &report, took) {
                        Err(e) if self.ends_for(e.unusable()) => return Err(e.into()),
                        Err(e) => {
                            let class = e.class();
                            self.backs_off(&mut schedule, "recording the sync", class, &e, None);
                        }
                        Ok(()) => {}
                    }
                }
                Err(e) => {
                    // What it did before is told all the same (a conflict copy made).
                    if let Some(partial) = p.engine.take_unreported()
                        && let Err(e) = self.tell_unreported(&mut p.engine, &partial)
                    {
                        tracing::warn!("telling what the failed sync did failed: {e}");
                    }
                    // A DB that can't be used: its account sets it aside.
                    if self.ends_for(e.unusable()) {
                        return Err(e.into());
                    }
                    self.backs_off(&mut schedule, "the sync", e.class(), &e, e.retry_after());
                }
            }
            if self.cfg.once {
                return Ok(());
            }
        }
    }

    /// Whether it ends for an error that means the DB can't be used (`unusable`): when its
    /// account sets the DB aside for that ([`RunnerConfig::end_if_unusable`]).
    fn ends_for(&self, unusable: Option<Unusable>) -> bool {
        self.cfg.end_if_unusable && unusable.is_some()
    }

    /// Takes `command` (but a stop).
    fn command(&mut self, command: Command, schedule: &mut Schedule, check_rules: &mut bool) {
        match command {
            Command::SyncNow => {
                schedule.asked(Instant::now(), Why::Asked);
                *check_rules = true;
                // The user may have made room on the drive, changed what it refused, or let
                // this computer in again.
                self.drive.clear_full();
                self.retry_refused = true;
                self.drive.allow();
            }
            Command::Online => schedule.online(Instant::now()),
            Command::Woke => {
                tracing::info!("the machine woke up: syncing now");
                schedule.woke(Instant::now());
            }
            Command::Pause(pause) => {
                // Raised again: a Resume taken before it lowered the flag `Handle::send`
                // raised, and the folder must show paused while it is.
                self.stop.store(true, Ordering::Relaxed);
                schedule.pause();
                self.set(|s| s.paused_until = pause.until);
                match pause.until {
                    Some(t) => {
                        tracing::info!("paused for {} min", t.saturating_sub(unix_now()) / 60)
                    }
                    None => tracing::info!("paused"),
                }
            }
            Command::Resume => {
                self.drive.allow();
                if schedule.resume(Instant::now()) {
                    tracing::info!("resumed");
                }
                self.resume();
            }
            Command::PauseEnded => {
                if schedule.resume(Instant::now()) {
                    tracing::info!("the pause is over: resuming");
                    self.resume();
                }
            }
            Command::Stop => {}
        }
    }

    /// `what` failed with `e` (of `class`; the server said to ask again after `retry_after`):
    /// the folder shows it and backs off.
    fn backs_off(
        &self,
        schedule: &mut Schedule,
        what: &str,
        class: hbsync_remote::ErrorClass,
        e: &dyn std::fmt::Display,
        retry_after: Option<Duration>,
    ) -> Duration {
        let wait = schedule.failed(Instant::now(), class, retry_after);
        tracing::warn!("{what} failed ({e}); trying again in {wait:?}");
        self.failing(schedule::shown(class), e.to_string(), wait);
        wait
    }

    /// Shows `why` nothing syncs (in `state`), until another try after `wait`.
    fn failing(&self, state: State, why: String, wait: Duration) {
        self.set(|s| {
            s.state = state;
            s.error = Some(why);
            s.retry_at = Some(unix_now() + wait.as_secs());
        });
    }

    /// The watcher as it is now: `why` there is none. Lost, the status says why, the user is
    /// told (once a run: a watcher that keeps stopping isn't told each time), and a watcher is
    /// asked for again at the looks, less often each time it fails; back, the status and the
    /// note say no more of it.
    fn watcher_is(&mut self, why: Option<String>) {
        let look = self.cfg.look;
        let w = &mut self.unwatched;
        match why {
            Some(why) => {
                if w.why.is_none() {
                    w.retry_wait = look;
                } else {
                    w.retry_wait = (w.retry_wait * 2).min(look * 32);
                }
                w.retry_at = Some(Instant::now() + w.retry_wait);
                if w.why.as_deref() == Some(why.as_str()) {
                    return;
                }
                w.why = Some(why.clone());
                let note = (!w.told).then(|| unwatched_note(&why, look));
                self.set(|s| {
                    s.watcher_error = Some(why.clone());
                    s.rescan_secs = Some(look.as_secs());
                });
                tracing::warn!("no live watcher ({why}); the folder is walked every {look:?}");
                if let Some(note) = note {
                    self.unwatched.told = true;
                    self.unwatched.note = Some(note.clone());
                    self.tell(vec![note]);
                }
            }
            None if w.why.is_some() => {
                w.why = None;
                w.retry_at = None;
                tracing::info!("the live watcher is back");
                let note = w.note.take();
                self.set(|s| {
                    s.watcher_error = None;
                    s.rescan_secs = None;
                    s.recent.retain(|n| Some(&n.text) != note.as_ref());
                });
            }
            None => {}
        }
    }

    /// Adds `notes` to what the user is told.
    fn tell(&self, notes: Vec<String>) {
        if notes.is_empty() {
            return;
        }
        for note in &notes {
            tracing::warn!("{note}");
        }
        self.set(|s| report::add_notes(&mut s.recent, &mut s.notes_added, notes, unix_now()));
    }

    /// Tells the lines of the ignore rules in use (`None`: none, they can't be read) that
    /// aren't valid rules, once a version of the rules; the note told for another version goes
    /// (fixed, told anew, or no longer what is in use).
    fn tell_rules(&mut self, filter: Option<&Filter>) {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let version = filter.map(|f| {
            let mut hasher = DefaultHasher::new();
            f.fingerprint().hash(&mut hasher);
            hasher.finish()
        });
        if version.is_some() && self.rules_told.as_ref().map(|(v, _)| *v) == version {
            return;
        }
        let note = filter.and_then(|f| report::rules_note(f.skipped()));
        if let Some((_, Some(old))) = self.rules_told.take() {
            self.set(|s| s.recent.retain(|n| n.text != old));
        }
        self.tell(note.clone().into_iter().collect());
        self.rules_told = version.map(|v| (v, note));
    }

    /// Tells why the ignore rules can't be used (`e`: negations that aren't valid rules), once
    /// while it is the same; the note told for other rules goes. Kept with no version: rules
    /// in use, read again, are told anew.
    fn tell_rules_failing(&mut self, e: &FilterError) {
        let note = e.to_string();
        if matches!(&self.rules_told, Some((0, Some(told))) if *told == note) {
            return;
        }
        if let Some((_, Some(old))) = self.rules_told.take() {
            self.set(|s| s.recent.retain(|n| n.text != old));
        }
        self.tell(vec![note.clone()]);
        self.rules_told = Some((0, Some(note)));
    }

    /// Tells what a scan skipped and the user may want to know (a folder it can't read, a
    /// name or kind it can't sync), each once.
    /// `whole`: the scan walked the whole folder, so what it didn't skip is no longer skipped
    /// (told again if it is again).
    fn tell_scan(&mut self, scan: &hbsync_local::ScanReport, whole: bool) {
        let notes = report::scan_notes(scan, &mut self.scan_told, whole);
        self.tell(notes);
    }

    /// Tells the conflicts and failures of a sync that then failed (`partial`).
    fn tell_unreported(
        &mut self,
        engine: &mut Engine,
        partial: &SyncReport,
    ) -> Result<(), StateError> {
        // Told once, the notes told so far kept: a part of a sync doesn't say what went away.
        let notes = self.notes_of(engine, partial)?;
        let notes = self.told.unseen(notes);
        self.tell(notes);
        Ok(())
    }

    /// `report`'s notes.
    fn notes_of(
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
    ) -> Result<Vec<String>, StateError> {
        let skips = self.stalled.told(&report.skipped, Instant::now());
        report::notes(engine, report, &skips)
    }

    /// Publishes a finished sync.
    fn record(
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
        sync: Sync,
    ) -> Result<(), StateError> {
        let took = sync.took;
        let notes = self.notes_of(engine, report)?;
        let notes = self.told.fresh(notes);
        let token = report::held_token(&report.held);
        // Before the status shows the new token, so no answer can name it before it is held;
        // the status is set under this lock too ([`Handle::status`] reads them so).
        // An answer taken during this sync stays for the next: it names what the user saw.
        let mut kept = lock(&self.held);
        // ... and the same deletes (each its way too) aren't shown again: they were answered.
        kept.shown = if kept.answer.is_some() && kept.token == token {
            None
        } else {
            report::held(engine, report, &token)?
        };
        kept.nodes = report.held.iter().map(|op| op.node()).collect();
        kept.token = token;
        // Shown again: one answer each time, but one taken during this sync still waits.
        kept.answered = kept.answer.is_some();
        // Every sync, what asked for it and what it read: what an idle one costs, and why it
        // ran at all.
        let io_after = hbsync_platform::io::counted();
        let io = match (sync.io_before, io_after) {
            (Some(before), Some(after)) => {
                let during = after.since(before);
                let between = self.io_after.map(|last| before.since(last));
                format!(
                    "; the process read {}, wrote {}{}",
                    crate::words::bytes(during.read),
                    crate::words::bytes(during.written),
                    between.map_or(String::new(), |b| format!(
                        " (since the last sync: read {}, wrote {})",
                        crate::words::bytes(b.read),
                        crate::words::bytes(b.written)
                    ))
                )
            }
            _ => String::new(),
        };
        self.io_after = io_after;
        tracing::info!(
            "synced in {took:.1?} ({}): done {}, remote changes {}, feed pages {}, adopted {}, \
             pending {}{io}",
            sync.why,
            report.ops_done,
            report.feed.live + report.feed.deleted,
            report.feed.pages,
            report.adopted,
            report.pending
        );
        for note in &notes {
            tracing::warn!("{note}");
        }
        if let Some(h) = &kept.shown {
            tracing::warn!(
                "deletions to review: {} here, {} remotely",
                h.here,
                h.remotely
            );
        }
        self.set(|s| report::record(s, report, notes, took, unix_now()));
        drop(kept);
        Ok(())
    }
}

/// A sync that ended, for its log line: how long it took, what asked for it, and what the
/// process had read and written as it began.
struct Sync {
    took: Duration,
    why: Whys,
    io_before: Option<Io>,
}

/// The note for a folder without a live watcher (`why`), its changes found every `look`.
fn unwatched_note(why: &str, look: Duration) -> String {
    let every = crate::words::rescan_every(look.as_secs());
    let fix = if cfg!(target_os = "linux") {
        "Closing programs that watch many files, or raising the system's limits \
         (fs.inotify.max_user_instances, max_user_watches), lets it watch again"
    } else {
        "It watches again once the system lets it"
    };
    format!(
        "No live watcher here ({why}): changes in the folder are found {every}, a walk of all \
         of it each time. {fix}; it asks again from time to time"
    )
}
