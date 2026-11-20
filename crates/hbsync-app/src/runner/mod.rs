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
use hbsync_remote::{ErrorClass, Refusal};
use hbsync_state::{StateError, Unusable};
use hbsync_types::NodeId;
use hbsync_types::unix_now;

use crate::protocol::{FolderStatus, HeldAnswer, HeldDeletes, Pause, SharesFolder, State};
use crate::sync::{OnDrop, Wakeup, lock, wait_for};
use crate::{Identity, MassDelete};

mod report;
mod schedule;

pub(crate) use report::add_notes;
pub(crate) use schedule::{Backoff, Showing};
use schedule::{Schedule, Synced, Why, Whys};

/// While the identity refuses this computer, how often a folder asks again (one sync).
const DENIED_LOOK: Duration = Duration::from_secs(3600);

/// Told when an answer to held deletes that was taken can't be used: what it named is no
/// longer held there.
const ANSWER_DROPPED_GONE: &str = "Your answer to the deletions to review wasn't used: the folder is gone. Once it is back, answer again.";
const ANSWER_DROPPED_SET_UP: &str = "Your answer to the deletions to review wasn't used: the folder was set up again. Answer again.";

/// The ignore rules are read this long after their file was last touched: not half written.
const RULES_QUIET: Duration = Duration::from_secs(1);

/// A folder of shares that couldn't be made is asked for again after a look's time this many
/// times, then after [`RunnerConfig::shares_again`].
const UNMADE_SOON: u32 = 3;

/// [`RunnerConfig::shares_again`], unless a test says otherwise.
const SHARES_AGAIN: Duration = Duration::from_secs(600);

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
    /// The account's DB synced with a drive deleted and made again since (another creation
    /// time): set aside before anything syncs ([`Unusable::OtherDrive`]).
    #[error("this computer's sync records are of a Filesync drive deleted and made again since")]
    OtherDrive,
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
            RunnerError::OtherDrive => ErrorClass::NeedsUser,
        }
    }

    /// Why the account's state DB can't be used at all, if that is what it means
    /// ([`StateError::unusable`]): its account sets it aside ([`crate::folders::heal_account`]).
    pub fn unusable(&self) -> Option<Unusable> {
        match self {
            RunnerError::State(e) => e.unusable(),
            RunnerError::Folder(e) => e.unusable(),
            RunnerError::Engine(e) => e.unusable(),
            RunnerError::OtherDrive => Some(Unusable::OtherDrive),
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
    /// What of the drive it holds (shown; the DB keeps it).
    pub kind: crate::RootKind,
    /// Whose drive `drive` is, when another identity's that shares it with `identity` (sync
    /// semantics §13, "Another identity's drive"): shown. Such a folder is `read_only`.
    pub sharer: Option<Identity>,
    /// The folder syncs one way, the drive deciding (sync semantics §13): what changes here
    /// moves out to this folder (outside `folder`, on its volume; made where missing).
    /// `None`: it syncs both ways.
    pub read_only: Option<PathBuf>,
    /// The folder at the top that holds what other identities share with the account (sync
    /// semantics §10) is made under this name where none is there, as the folder opens (or
    /// under the first free of `<name>-2`, …). `None`: none is made (one that is there is
    /// left alone all the same, and gives way to what else has its name).
    pub shares: Option<String>,
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
    /// How long after the folder of shares couldn't give way to the drive's item of its name
    /// (the item masked meanwhile) a sync is run to ask it again, and after it couldn't be
    /// made the folder is opened again to make it: each asking opens the folder anew.
    pub shares_again: Duration,
    /// Tests: adjusts the engine's config.
    pub tweak: Option<fn(&mut EngineConfig)>,
    /// Raised when its status may have changed (the daemon pushes it to the tray).
    pub(crate) changed: Arc<Wakeup>,
    /// Its account sets the state DB aside when it can't be used ([`RunnerError::unusable`]):
    /// the runner ends for that, rather than backing off as after any failed sync.
    pub(crate) end_if_unusable: bool,
    /// Told where the folder of shares is, each time the folder opens ([`SharesTold`]): its
    /// account runs the shares' folders in it.
    pub(crate) shares_told: Option<SharesTold>,
    /// Whose watcher the folder's is ([`hbsync_local::Feed`]): the main folder's gives to
    /// those of what is shared into its folder of shares, which hold none of their own.
    pub feed: Option<hbsync_local::Feed>,
    /// The account's main folder, for a folder it syncs beside it (a role folder, a
    /// Desktop): the folder of shares, dragged in here, is known by the record there and
    /// left alone ([`Filter::load_beside`]).
    pub(crate) shares_main: Option<PathBuf>,
}

/// What a runner calls with the name of its folder's folder of shares, each time the folder
/// opens and that one was put in its place ([`RunnerConfig::shares_told`]): `None` where
/// none is at the top (none on record, gone, or not made).
#[derive(Clone)]
pub(crate) struct SharesTold {
    pub(crate) tell: Arc<dyn Fn(Option<String>) + Send + Sync>,
    /// What was told last, by any runner of the folder (its account keeps it through a
    /// runner started again); `None`: nothing yet.
    pub(crate) last: Arc<Mutex<Option<Option<String>>>>,
}

impl std::fmt::Debug for SharesTold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharesTold")
    }
}

impl RunnerConfig {
    pub fn new(folder: &Path, db: &Path, identity: &Identity, device: &str) -> Self {
        RunnerConfig {
            folder: folder.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.clone(),
            kind: crate::RootKind::Main,
            sharer: None,
            read_only: None,
            shares: None,
            device: device.to_owned(),
            mass_delete: MassDelete::default(),
            paused: None,
            look: Duration::from_secs(60),
            once: false,
            put_back_again: hbsync_engine::gone::ASK_AGAIN,
            shares_again: SHARES_AGAIN,
            tweak: None,
            changed: Arc::default(),
            end_if_unusable: false,
            shares_told: None,
            feed: None,
            shares_main: None,
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

/// The filter of the sync folder at `root`: its own ([`Filter::load`]), or for a folder
/// beside the account's main folder `main` ([`Filter::load_beside`]).
fn load_filter(
    root: &Path,
    case_insensitive: bool,
    main: Option<&Path>,
) -> Result<Filter, FilterError> {
    match main {
        Some(main) => Filter::load_beside(root, case_insensitive, main),
        None => Filter::load(root, case_insensitive),
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
    /// The user's word on the main folder's folder of shares ([`Handle::shares`]).
    Shares(SharesAsk),
    /// The account has a share to sync: its main folder is to hold a folder of shares, made
    /// under this name where none is on record (placed at once, the folder opened again only
    /// where one was made). For a runner started while nothing was shared.
    HoldShares(String),
    /// Look at the ignore rules and the folder of shares now (the account's main folder
    /// opened: its record may say something else of the folder of shares, which a folder
    /// beside it goes by).
    Look,
}

/// What the user asks of the main folder's folder of shares (sync semantics §10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharesAsk {
    /// Make it again: the one on record isn't at the top.
    Make,
    /// Take this folder at the top as it.
    Take(String),
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
    /// Its account's drive: whether it refuses this computer while paused.
    drive: Arc<Drive>,
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

    /// Asks `ask` of the main folder's folder of shares. Checked against what the folder
    /// shows now: refused for a folder nothing is shared into, while a folder of shares is
    /// at its top, and (to take one) for anything but a folder there. Taken, the runner does
    /// it between syncs, and tells in a note if it couldn't.
    pub fn shares(&self, ask: SharesAsk) -> Result<(), String> {
        use hbsync_local::shares::{takeable, there};
        let (shares, path, identity) = {
            let status = lock(&self.status);
            (
                status.shares.clone(),
                status.path.clone(),
                status.identity.clone(),
            )
        };
        match &shares {
            None => {
                let into = path.display();
                return Err(format!("nothing is shared with {identity} into {into}"));
            }
            Some(SharesFolder::At { name }) => return Err(there(name).to_string()),
            Some(SharesFolder::Gone | SharesFolder::Unmade { .. }) => {}
        }
        // (Nothing in a paused folder is made or marked.)
        if self.status().state == State::Paused {
            return Err("syncing is paused: resume it first".to_owned());
        }
        // (The folder as the top lists it, whatever spelling of its name was given.)
        let ask = match ask {
            SharesAsk::Take(name) => {
                SharesAsk::Take(takeable(&path, &name).map_err(|e| e.to_string())?)
            }
            make => make,
        };
        match self.send(Command::Shares(ask)) {
            true => Ok(()),
            false => Err("the sync has stopped".to_owned()),
        }
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
            // Refused before or since the pause (the notifications', say): shown till a
            // request goes well, as running.
            // (Not what another identity shares, where its sharer's server or the owner's
            // approval is what it waits for: paused, and no login is asked for.)
            let refused = self.drive.refused();
            let refused = refused.filter(|(_, r)| status.sharer.is_none() || r.login_lifts());
            if let Some(refused) = refused {
                let shown = Showing::refused(refused);
                status.refused_paused = true;
                status.error = Some(shown.why);
                status.lacks_key = shown.lacks_key;
            }
        }
        // Only while it shows refused, or waiting.
        status.lacks_key &= status.refused();
        status.needs_approval &= status.state == State::Waiting;
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
    /// The folder of shares is to be placed at the loop's next turn ([`Command::HoldShares`]).
    place_shares_now: bool,
    /// Asked of the folder of shares, and not done yet ([`Command::Shares`]), oldest first.
    asked: Vec<SharesAsk>,
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
    /// The folder of shares gave way as the folder opened.
    gave_way: bool,
    /// It couldn't, to the drive's item of its name ([`hbsync_local::shares::Placed::stuck`]).
    stuck: bool,
    /// The folder of shares was to be made and couldn't be
    /// ([`hbsync_local::shares::Placed::unmade`]): made as the folder opens again.
    unmade: bool,
}

impl Parts {
    /// Whether the ignore rules on disk, or the folders at the top that hold what other
    /// identities share, differ from the ones in use (or can't be read): their fingerprints
    /// differ (sync semantics §10).
    fn rules_changed(&self, root: &Path, main: Option<&Path>) -> bool {
        let folder = &self.engine.config().folder;
        load_filter(root, folder.case_insensitive, main)
            .map_or(true, |f| folder.filter.changed_from(&f))
    }

    /// [`Parts::rules_changed`] for the folder of shares alone, the rules neither read nor
    /// compiled: its record's read, and where one is on record a listing of the top and a
    /// look for its marker.
    fn shares_changed(&self, root: &Path) -> bool {
        self.engine.config().folder.filter.shared_changed(root)
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
        // Shown as whose it is: a share never passes for the account's own folder, nor that
        // for a share.
        if cfg.sharer.is_some() != drive.shared() {
            return Err(RunnerError::State(StateError::Conflict(
                "a folder says whose drive it syncs when another identity's, and only then".into(),
            )));
        }
        let (tx, rx) = channel();
        let stop = Arc::new(AtomicBool::new(cfg.paused.is_some()));
        let mut status = FolderStatus::new(&cfg.identity, &cfg.folder);
        status.kind = cfg.kind;
        status.sharer.clone_from(&cfg.sharer);
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
            drive: Arc::clone(&drive),
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
            place_shares_now: false,
            asked: Vec::new(),
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
        let mut gave_way = false;
        for last in [false, false, true] {
            let mut parts = self.open_once()?;
            gave_way |= parts.gave_way;
            parts.gave_way = gave_way;
            let watching = parts.observer.watcher_error.is_none();
            let main = self.cfg.shares_main.as_deref();
            if last || !watching || !parts.rules_changed(&self.cfg.folder, main) {
                return Ok(parts);
            }
            tracing::info!(
                "the ignore rules or the folder of shares changed as the folder opened: opening it again"
            );
        }
        unreachable!("the last one is returned")
    }

    /// Whether this is a share's runner whose folder is there: a real folder, not a link to
    /// one (what a link leads to is never synced as the share's).
    fn shares_folder_is_there(&self) -> bool {
        self.cfg.sharer.is_some()
            && std::fs::symlink_metadata(&self.cfg.folder).is_ok_and(|meta| meta.is_dir())
    }

    /// Puts the folder of shares in its place again while the folder is open, as
    /// [`Runner::open_once`] does when it opens it (made where it couldn't be, renamed where
    /// it couldn't give way): what that did, for the folder to be opened again where it did
    /// something. Costs a connection to the state DB, the record's read and a listing of the
    /// top.
    fn place_again(&self, parts: &Parts) -> Result<hbsync_local::shares::Placed, RunnerError> {
        let root = SyncRoot::open(&self.cfg.db, &self.cfg.identity, &self.cfg.folder)?;
        let caseless = parts.engine.config().folder.case_insensitive;
        let placed = root.place_shares(self.cfg.shares.as_deref(), caseless)?;
        self.show_shares(&placed);
        Ok(placed)
    }

    /// Shows where the folder of shares is, as `placed` found or put it
    /// ([`FolderStatus::shares`]), while anything is shared with the account (one is to be
    /// there); told only when it is another than shown.
    fn show_shares(&self, placed: &hbsync_local::shares::Placed) {
        if self.cfg.shares.is_none() {
            return;
        }
        let shown = match placed.at.first() {
            Some(name) => Some(SharesFolder::At { name: name.clone() }),
            None if placed.missing => Some(SharesFolder::Gone),
            None => placed
                .unmade
                .clone()
                .map(|why| SharesFolder::Unmade { why }),
        };
        if lock(&self.status).shares != shown {
            self.set(|s| s.shares = shown);
        }
    }

    fn open_once(&self) -> Result<Parts, RunnerError> {
        let root = &self.cfg.folder;
        let mut cfg = EngineConfig::new(root, &self.cfg.device);
        cfg.guard.min = self.cfg.mass_delete.min_files;
        cfg.guard.fraction = self.cfg.mass_delete.fraction;
        // A folder belongs to one identity (its marker says which): never mix two drives
        // through one folder.
        let sync_root = match SyncRoot::open(&self.cfg.db, &self.cfg.identity, root) {
            // A share's folder, which its account made in the folder of shares, is set up
            // here (on this thread: its DB may be busy) while it holds no marker: what is
            // in it that the sharer's drive doesn't have moves out. One that holds a marker
            // its DB doesn't know (another folder renamed into its place, or the share's
            // DB lost) is never taken over: its sync fails, saying so.
            Err(RootError::NoMarker(_)) if self.cfg.sharer.is_some() => {
                SyncRoot::create(&self.cfg.db, &self.cfg.identity, root)?
            }
            opened => opened?,
        };
        // Before the rules are read: a folder of shares taken now is left out by them.
        // What can't be told of it (its record, the top's listing, a marker) stops the sync
        // as rules that can't be read do: failing, tried again at the next look.
        let unread = |e| match e {
            RootError::Io(source) => RunnerError::Filter(FilterError::Read {
                path: root.clone(),
                source,
            }),
            e => e.into(),
        };
        let adopted = sync_root
            .adopt_shares(cfg.folder.case_insensitive)
            .map_err(unread)?;
        if let Some(name) = adopted {
            tracing::info!("{name}: taken as the folder of shares (the one on record is gone)");
        }
        // Before the rules are read too: with a filter that holds its name, what else has the
        // name (on the drive, or here beside it) would be left out with it.
        let make_as = self.cfg.shares.as_deref();
        let placed = sync_root
            .place_shares(make_as, cfg.folder.case_insensitive)
            .map_err(unread)?;
        if let Some(name) = &placed.made {
            tracing::info!("{name}: made as the folder of shares");
        }
        for (from, to) in &placed.moved {
            tracing::info!("the folder of shares {from} is now {to}: something else has its name");
        }
        if let Some(why) = &placed.stuck {
            tracing::warn!("{why}: the item stays out of this folder meanwhile");
        }
        if let Some(why) = &placed.unmade {
            tracing::warn!("{why}: tried again later; what is the account's own syncs on");
        }
        if placed.missing && make_as.is_some() {
            tracing::warn!(
                "the folder of shares on record isn't at the top of the folder: none is made in its place"
            );
        }
        self.show_shares(&placed);
        // Where the folder of shares is (where there is more than one: the one on record
        // now, or the first), told to the account, which syncs each share there. At once
        // where it was told another place before: a share's runner still syncs there, and
        // is to stop before it makes that place again. Else as this opening ends, with the
        // watcher there (or failed): a share started on it takes its events from that
        // watcher, and one that began before it would walk its folder a second time for
        // what it may have missed.
        let at = placed.at.first().cloned();
        let _told = self.cfg.shares_told.as_ref().and_then(|told| {
            let before = lock(&told.last).replace(at.clone());
            let moved = before.is_some_and(|before| before.is_some() && before != at);
            let tell = Arc::clone(&told.tell);
            let tell = move || tell(at);
            if moved {
                tell();
                return None;
            }
            Some(OnDrop(Some(tell)))
        });
        let main = self.cfg.shares_main.as_deref();
        cfg.folder.filter = Arc::new(load_filter(root, cfg.folder.case_insensitive, main)?);
        if !cfg.folder.filter.returned().is_empty() {
            tracing::warn!(
                "the folder of shares on record isn't at the top of the folder, and what holds \
                 its marker there wasn't taken as it: it is synced as the account's own, or \
                 more than one holds it"
            );
        }
        cfg.folder.read_only.clone_from(&self.cfg.read_only);
        if let Some(tweak) = self.cfg.tweak {
            tweak(&mut cfg);
        }
        let rules = cfg.folder.filter.rule_count();
        let mut engine = Engine::open(&sync_root, Arc::clone(&self.drive), cfg)?;
        engine.set_stop(Arc::clone(&self.stop));
        engine.set_progress(Arc::clone(&self.progress));
        let wakes = self.wakes.clone();
        let bell: Bell = Arc::new(move || {
            let _ = wakes.send(Msg::Local);
        });
        let (opts, feed) = (engine.scan_options().clone(), self.cfg.feed.clone());
        let observer = LocalObserver::watching(root, opts, Some(bell), feed);
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
            gave_way: !placed.moved.is_empty(),
            stuck: placed.stuck.is_some(),
            unmade: placed.unmade.is_some(),
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
        // The folders of shares alone ([`Parts::shares_changed`]).
        let mut check_shares = false;
        // The folder is opened again for the folder of shares to give way to the drive's item
        // of its name ([`SyncReport::in_the_way`]).
        let mut yielding = false;
        // When the folder of shares is asked again: to give way (it couldn't, or nothing was
        // found to give way to: the item stays masked meanwhile), or to be made.
        let mut shares_at: Option<Instant> = None;
        // How often in a row the folder of shares couldn't be made.
        let mut unmade_tries = 0u32;
        // Its timer came round while the folder was paused.
        let mut shares_held = false;
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
                    shares_at,
                ]
                .into_iter()
                .flatten(),
            );
            let mut first = self.control.recv_timeout(wait).ok();
            self.woken.add();
            while let Some(msg) = first.take().or_else(|| self.control.try_recv().ok()) {
                match msg {
                    Msg::Command(Command::Stop) | Msg::Orphaned => {
                        // (Taken, and not done: said, for whoever was told it would be.)
                        if !self.asked.is_empty() {
                            let n = self.asked.len();
                            tracing::warn!("stopped with {n} asked of the folder of shares");
                            self.tell(vec![
                                "What was asked of the folder for what is shared wasn't done: \
                                 syncing stopped first. Ask again."
                                    .to_owned(),
                            ]);
                        }
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
                        let what = "taking local changes";
                        let failed = (e.class(), None, None);
                        let wait = self.backs_off(&mut schedule, what, failed, &e, None);
                        observe_after = Some(Instant::now() + wait);
                    }
                    None => {}
                }
                // After the poll, which has the watcher ring again for a touch from now on.
                if p.observer.rules_touched() {
                    rules_at = Some(Instant::now() + RULES_QUIET);
                }
                // A folder that holds what other identities share is there or isn't, with
                // nothing half written to wait for: read at once, before the sync below would
                // send what the poll just took of it.
                if p.observer.shared_touched() {
                    check_shares = true;
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
                    schedule.asked(now, Why::Retry);
                }
            }
            if rules_at.is_some_and(|at| now >= at) {
                rules_at = None;
                check_rules = true;
            }
            // The folder of shares is asked again, to give way or to be made, by itself (its
            // record, a listing, a lookup, a rename): an idle folder is asked too, and the
            // folder is opened again only where that did something, never to find out.
            // (Come round while the folder was paused: asked now that it syncs again.)
            if std::mem::take(&mut self.place_shares_now) || (shares_held && !schedule.paused()) {
                shares_held = false;
                shares_at = Some(now);
            }
            if shares_at.is_some_and(|at| now >= at) {
                shares_at = None;
                let again = now + self.cfg.shares_again;
                // Not while paused: nothing in the folder is touched then, and its watcher
                // stays. Held till the folder syncs again (no timer runs meanwhile: a paused
                // folder isn't woken for it).
                let placed = match schedule.paused() {
                    true => {
                        shares_held = true;
                        None
                    }
                    false => parts.as_ref().map(|p| self.place_again(p)),
                };
                match (placed, &mut parts) {
                    // Made, or out of the way: opened again with it.
                    (Some(Ok(placed)), _) if placed.made.is_some() || !placed.moved.is_empty() => {
                        parts = None;
                        schedule.want(Why::Rules);
                    }
                    // It still can't (as found now, not as the folder was opened). One that
                    // couldn't be made is asked sooner the first few times (what is in the
                    // way of a making is often gone a moment later: a scanner in the new
                    // folder), then as rarely as the rest.
                    (Some(Ok(placed)), _) if placed.unmade.is_some() => {
                        unmade_tries += 1;
                        let soon = unmade_tries <= UNMADE_SOON;
                        let wait = if soon {
                            self.cfg.look
                        } else {
                            self.cfg.shares_again
                        };
                        shares_at = Some(now + wait.min(self.cfg.shares_again));
                    }
                    (Some(Ok(placed)), _) if placed.stuck.is_some() => shares_at = Some(again),
                    // Nothing to do any more (what it was to give way to is gone, or nothing
                    // was found when it was asked): the engine asks again, should it find
                    // something.
                    (Some(Ok(_)), Some(p)) => p.engine.yield_shares(true),
                    (Some(Err(e)), _) => {
                        tracing::warn!("the folder of shares can't be put in its place: {e}");
                        shares_at = Some(again);
                    }
                    _ => {}
                }
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
                    // (Not for a watcher that went a moment ago and may be back in one: a
                    // share's, as the main folder is opened again. What changed meanwhile
                    // is told as lost events then, and walked for once, not twice.)
                    let look = self.cfg.look;
                    let back_soon = (p.observer.unwatched_for()).is_some_and(|d| d < look / 2);
                    let unwatched = !p.observer.has_watcher() && !schedule.paused() && !back_soon;
                    if self.unwatched.catch_up || unwatched {
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
            // What was asked of the folder of shares, in the order asked (the second of two
            // finds the first done, and says so). Not while paused: nothing in a paused
            // folder is made or marked; done as it syncs again (the resume wakes this).
            if !schedule.paused() {
                for ask in std::mem::take(&mut self.asked) {
                    check_rules |= self.shares_asked(&ask);
                }
            }
            let check = (
                std::mem::take(&mut check_rules),
                std::mem::take(&mut check_shares),
            );
            if parts.as_ref().is_some_and(|p| match check {
                (true, _) => p.rules_changed(&self.cfg.folder, self.cfg.shares_main.as_deref()),
                (false, true) => p.shares_changed(&self.cfg.folder),
                (false, false) => false,
            }) {
                tracing::info!("the ignore rules or the folder of shares changed: starting over");
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
                (Some(latched), false) => {
                    // The identity's answer, as shown (a refused login keeps one for want of
                    // the key shown, [`Drive::login_refused`]).
                    let (why, refusal) = self.drive.refused().unwrap_or(latched);
                    let failed = (ErrorClass::Denied, Some(refusal), None);
                    self.backs_off(&mut schedule, "the sync", failed, &why, Some(DENIED_LOOK));
                    self.probing = true;
                    continue;
                }
                (Some(_), true) => self.drive.allow(),
                (None, _) => {}
            }
            let checked = SyncRoot::check(&self.cfg.folder, &self.cfg.identity);
            // A share's folder is a real folder at its place in the folder of shares, never a
            // link to one: what a link leads to (another folder of the account's, say) isn't
            // the share's, whatever marker it holds.
            if self.cfg.sharer.is_some() && !self.shares_folder_is_there() {
                // Gone (deleted, renamed, the folder of shares with it), or something else
                // there: it ends, for its account to make it again where the folder of
                // shares is now, or not at all.
                let gone = format!("{}: not the share's folder", self.cfg.folder.display());
                return Err(std::io::Error::new(std::io::ErrorKind::NotFound, gone).into());
            }
            let marker = match checked {
                Ok(marker) => marker.id,
                // A share's folder is its account's to make and set up as it opens: one
                // just made has no marker yet to compare.
                Err(RootError::NoMarker(_)) if self.cfg.sharer.is_some() => String::new(),
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
                    let later = schedule.later(Instant::now());
                    self.failing(Showing::failing(State::Failing, GONE.into()), later);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            // Before what this sync does first (opening the folder, an answer's put back).
            let io_before = hbsync_platform::io::counted();
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
                    Ok(mut p) => {
                        // Asked to give way and didn't (it can't just now, or nothing was
                        // found to give way to): not asked again at every sync.
                        let asked = std::mem::take(&mut yielding);
                        if p.unmade {
                            // (Counted on: a folder opened again and again for other
                            // reasons isn't asked soon each time.)
                            unmade_tries += 1;
                            let soon = unmade_tries <= UNMADE_SOON;
                            let wait = if soon {
                                self.cfg.look
                            } else {
                                self.cfg.shares_again
                            };
                            shares_at = Some(Instant::now() + wait.min(self.cfg.shares_again));
                        } else if p.stuck || (asked && !p.gave_way) {
                            shares_at = Some(Instant::now() + self.cfg.shares_again);
                        } else {
                            unmade_tries = 0;
                            p.engine.yield_shares(true);
                            shares_at = None;
                        }
                        // (Its timer is set anew by this opening.)
                        shares_held = false;
                        self.tell_rules(Some(&p.engine.config().folder.filter));
                        self.watcher_is(p.observer.watcher_error.clone());
                        started = false;
                        // (A share's folder set up as it opened has its marker now.)
                        let set_up = || SyncRoot::check(&self.cfg.folder, &self.cfg.identity);
                        opened = Some(match marker.is_empty() {
                            true => set_up().map(|m| m.id).unwrap_or_default(),
                            false => marker,
                        });
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
                        let later = schedule.later(Instant::now());
                        self.failing(Showing::failing(State::Failing, e.to_string()), later);
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
            // The engine keeps it for its next plan only: given again if that one isn't made.
            let confirmed = answer
                .clone()
                .filter(|(answer, _)| *answer == HeldAnswer::Delete);
            match answer {
                Some((HeldAnswer::Delete, seen)) => p.engine.confirm_deletes(seen.iter().copied()),
                Some((HeldAnswer::PutBack, seen)) => {
                    match p.engine.put_back(&seen) {
                        Ok(_) => {}
                        // A read-only folder never puts back (§13): nothing to try again.
                        Err(hbsync_engine::EngineError::ReadOnly(_)) => self.tell(vec![
                            "This folder is read-only: what it holds back can't be put back on \
                             Homebase, only deleted here or left held"
                                .into(),
                        ]),
                        // What it didn't put back is still held, and shown again; the first
                        // time, it runs once more a minute on (a file the identity said once
                        // isn't there is gone for good then, and goes up again from here).
                        Err(e) => {
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
            let why = schedule.why(began);
            // A start-up's catch-up, which can walk the whole folder.
            let mut catch_up = None;
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
                    let whole = matches!(r.local.tier, Tier::FullWalk { .. });
                    self.tell_scan(&r.local.report, whole);
                    catch_up = Some(r.local.tier);
                    r.sync
                })
            };
            let took = began.elapsed();
            self.log_sync(&result, took, &why, catch_up, io_before);
            // The drive has an item at its top under the folder of shares' name, and nothing
            // was planned with it masked: not a sync to show or to schedule by. Opened again,
            // the folder of shares gives way (a run that syncs once goes on too).
            if result.as_ref().is_ok_and(|report| report.in_the_way) {
                tracing::info!(
                    "the drive has an item of the folder of shares' name: starting over"
                );
                if let Some(answer) = confirmed {
                    let mut held = lock(&self.held);
                    if held.answer.is_none() {
                        held.answer = Some(answer);
                        held.again = once_more;
                    }
                }
                parts = None;
                yielding = true;
                schedule.want(Why::Rules);
                continue;
            }
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
                    match self.record(&mut p.engine, &report, took) {
                        Err(e) if self.ends_for(e.unusable()) => return Err(e.into()),
                        Err(e) => {
                            let (what, failed) = ("recording the sync", (e.class(), None, None));
                            self.backs_off(&mut schedule, what, failed, &e, None);
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
                    let failed = (e.class(), e.refusal(), e.waiting());
                    self.backs_off(&mut schedule, "the sync", failed, &e, e.retry_after());
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
            Command::Shares(ask) => self.asked.push(ask),
            Command::HoldShares(name) => {
                if self.cfg.shares.is_none() {
                    self.cfg.shares = Some(name);
                    self.place_shares_now = true;
                }
            }
            Command::Look => *check_rules = true,
        }
    }

    /// Does what the user asked of the folder of shares ([`Command::Shares`]): whether it was
    /// done (a folder that is open is then opened again with it, as for rules that changed).
    /// One that fails is told as a note.
    fn shares_asked(&mut self, ask: &SharesAsk) -> bool {
        let folder = &self.cfg.folder;
        // (Opened for both: only in the folder that is set up, by its marker.)
        let root = SyncRoot::open(&self.cfg.db, &self.cfg.identity, folder);
        let done = root.and_then(|root| match (ask, self.cfg.shares.as_deref()) {
            (SharesAsk::Take(name), _) => {
                hbsync_local::shares::take(folder, name)?;
                Ok(name.clone())
            }
            (SharesAsk::Make, Some(name)) => {
                let caseless = EngineConfig::new(folder, &self.cfg.device)
                    .folder
                    .case_insensitive;
                root.make_shares_again(name, caseless)
            }
            (SharesAsk::Make, None) => Err(RootError::Io(std::io::Error::other(
                "nothing is shared with this account",
            ))),
        });
        match done {
            Ok(name) => {
                tracing::info!("{name}: the folder of shares now, as asked");
                true
            }
            Err(e) => {
                let what = match &ask {
                    SharesAsk::Make => "The folder of shares couldn't be made again".to_owned(),
                    SharesAsk::Take(name) => {
                        format!("{name} couldn't be taken as the folder of shares")
                    }
                };
                tracing::warn!("{what}: {e}");
                self.tell(vec![format!("{what}: {e}")]);
                false
            }
        }
    }

    /// `what` failed with `e`, of `class` (`refusal`: why the identity refused, if it did;
    /// `waiting`: who is waited for, where someone else has to act first; the server said to
    /// ask again after `retry_after`): the folder shows it (the account's refusal through what
    /// would show offline, [`Drive::refused`]; what another identity shares as waiting, where
    /// this login isn't what stands in the way: [`Showing::shared`]) and backs off.
    fn backs_off(
        &self,
        schedule: &mut Schedule,
        what: &str,
        (class, refusal, waiting): (ErrorClass, Option<Refusal>, Option<&str>),
        e: &dyn std::fmt::Display,
        retry_after: Option<Duration>,
    ) -> Duration {
        let wait = schedule.failed(Instant::now(), class, retry_after);
        tracing::warn!("{what} failed ({e}); trying again in {wait:?}");
        let kept = self.drive.refused();
        let showing = match self.cfg.sharer {
            Some(_) => Showing::shared(class, refusal, (waiting, e.to_string()), kept),
            None => Showing::failed(class, refusal, e.to_string(), kept),
        };
        self.failing(showing, wait);
        wait
    }

    /// Shows why nothing syncs (`showing`), until another try after `wait`.
    fn failing(&self, showing: Showing, wait: Duration) {
        self.set(|s| {
            s.state = showing.state;
            s.error = Some(showing.why);
            s.lacks_key = showing.lacks_key;
            s.needs_approval = showing.needs_approval;
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
    /// name or kind it can't sync), each once; links, skipped by design, only in the log.
    /// `whole`: the scan walked the whole folder, so what it didn't skip is no longer skipped
    /// (told again if it is again).
    fn tell_scan(&mut self, scan: &hbsync_local::ScanReport, whole: bool) {
        let told = report::scan_notes(scan, &mut self.scan_told, whole);
        if !told.links.is_empty() {
            tracing::info!("not synced, as links aren't: {}", told.links.join(", "));
        }
        self.tell(told.notes);
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

    /// `report`'s notes, but where it moved out of a read-only folder (§13): told here, each
    /// time, before the rest (which reads the DB, and may fail).
    fn notes_of(
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
    ) -> Result<Vec<String>, StateError> {
        self.tell(report::moved_out_notes(report));
        let skips = self.stalled.told(&report.skipped, Instant::now());
        report::notes(engine, report, &skips)
    }

    /// Logs a sync that ended (its `result`), at info, every one: the folder, how long it took,
    /// what asked for it (`why`), a start-up's catch-up, what it did (a failure is told right
    /// after, with its wait), and what the process read and wrote during it and in all (the gap
    /// between two lines is what happened between syncs). What an idle sync costs, and why it
    /// ran. About 300 bytes of log a sync.
    fn log_sync(
        &self,
        result: &Result<SyncReport, EngineError>,
        took: Duration,
        why: &Whys,
        catch_up: Option<Tier>,
        io_before: Option<Io>,
    ) {
        use std::fmt::Write;
        let how = if result.is_ok() { "synced" } else { "failed" };
        let mut line = format!("{}: {how} in {took:.1?} ({why}", self.cfg.folder.display());
        match catch_up {
            Some(Tier::Journal { dirs }) => {
                let _ = write!(line, ", start-up: {dirs} folders rescanned");
            }
            Some(Tier::FullWalk { reason }) => {
                let _ = write!(line, ", start-up: a full walk, {reason}");
            }
            None => {}
        }
        line.push(')');
        if let Ok(r) = result {
            let _ = write!(
                line,
                ": done {}, remote changes {}, feed pages {}, adopted {}, pending {}",
                r.ops_done,
                r.feed.live + r.feed.deleted,
                r.feed.pages,
                r.adopted,
                r.pending
            );
        }
        if let (Some(before), Some(after)) = (io_before, hbsync_platform::io::counted()) {
            let _ = write!(
                line,
                "; the process {} (in all: {})",
                io_words(after.since(before)),
                io_words(after)
            );
        }
        tracing::info!("{line}");
    }

    /// Publishes a finished sync.
    fn record(
        &mut self,
        engine: &mut Engine,
        report: &SyncReport,
        took: Duration,
    ) -> Result<(), StateError> {
        let notes = self.notes_of(engine, report)?;
        let notes = self.told.fresh(notes);
        let shown = report::shown(&report.held, engine.held_wipes());
        let token = report::held_token(shown.ops.iter().copied());
        // Before the status shows the new token, so no answer can name it before it is held;
        // the status is set under this lock too ([`Handle::status`] reads them so).
        // An answer taken during this sync stays for the next: it names what the user saw.
        let mut kept = lock(&self.held);
        // ... and the same deletes (each its way too) aren't shown again: they were answered.
        kept.shown = if kept.answer.is_some() && kept.token == token {
            None
        } else {
            report::held(engine, &shown, &token)?
        };
        kept.nodes = report.held.iter().map(|op| op.node()).collect();
        kept.token = token;
        // Shown again: one answer each time, but one taken during this sync still waits.
        kept.answered = kept.answer.is_some();
        for note in &notes {
            tracing::warn!("{note}");
        }
        if let Some(h) = &kept.shown {
            tracing::warn!(
                "deletions to review: {} here, {} remotely ({} not finished on the drive)",
                h.here,
                h.remotely,
                h.unfinished
            );
        }
        self.set(|s| report::record(s, report, notes, took, unix_now()));
        drop(kept);
        Ok(())
    }
}

/// "read 1.2 MB, wrote 40 kB", with Windows' other I/O where there was some.
fn io_words(io: Io) -> String {
    let other = match io.other {
        0 => String::new(),
        n => format!(", other I/O {}", crate::words::bytes(n)),
    };
    format!(
        "read {}, wrote {}{other}",
        crate::words::bytes(io.read),
        crate::words::bytes(io.written)
    )
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
