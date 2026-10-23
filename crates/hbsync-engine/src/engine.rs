//! The sync loop for one sync root.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_local::{CatchUp, Debounce, LocalObserver, ScanOptions, ScanReport, rescan_dirs, scan};
use hbsync_remote::Remote;
use hbsync_state::{DirtyCursor, Mark, StateDb, StateError, keys};
use hbsync_types::NodeId;
use hbsync_types::activity::Synced;

use crate::activity::{self, Counters};
use crate::adopt::adopt;
use crate::drive::Drive;
use crate::error::EngineError;
use crate::exec::{
    ExecConfig, Executor, Stopped, record_dirs, recover, recover_aside, recover_placements,
};
use crate::feed::FeedReport;
use crate::folder::FolderCtx;
use crate::holding::{HeldDeletes, HoldOut};
use crate::reconcile::{Conflict, DeleteGuard, Op, Options, Plan, reconcile_with_known};
use crate::refused::FolderRefusals;
use crate::root::{RootError, SyncRoot};
use crate::scope::Scope;
use crate::view;
use crate::waiting::Waited;

/// Start-up's pull is the first sync's if the local catch-up ended within this of it.
const FRESH_PULL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// The folder the engine syncs.
    pub folder: FolderCtx,
    pub exec: ExecConfig,
    /// Shown in conflict-copy names.
    pub device: String,
    /// Plan/execute rounds per sync before giving the rest to the next sync.
    pub max_rounds: usize,
    /// Dirty nodes planned per round.
    pub batch: usize,
    /// The mass-delete guard's settings, as each plan takes them.
    pub guard: DeleteGuard,
    /// Tests: every plan from a narrow view ([`view::build_narrow`]) is checked against one
    /// from whole folders ([`view::build_input`]).
    #[cfg(feature = "testkit")]
    pub check_narrow: bool,
    /// Tests: called between a round's plan and its clearing of the dirty marks.
    #[cfg(feature = "testkit")]
    pub after_plan: Option<crate::testkit::AfterPlan>,
    /// Tests: called between a sweep's reading of the guard's counts and its first page.
    #[cfg(feature = "testkit")]
    pub after_counts: Option<crate::testkit::AfterPlan>,
}

impl EngineConfig {
    pub fn new(root: &Path, device: &str) -> Self {
        EngineConfig {
            folder: FolderCtx::new(root),
            exec: ExecConfig::default(),
            device: device.to_owned(),
            max_rounds: 8,
            batch: 20_000,
            guard: DeleteGuard::default(),
            #[cfg(feature = "testkit")]
            check_narrow: false,
            #[cfg(feature = "testkit")]
            after_plan: None,
            #[cfg(feature = "testkit")]
            after_counts: None,
        }
    }
}

/// What start-up did.
#[derive(Debug)]
pub struct StartupReport {
    /// How the local side caught up (journal or full walk).
    pub local: CatchUp,
    /// The first sync after both catch-ups; its `feed` is start-up's pull.
    pub sync: SyncReport,
}

/// What one sync did.
#[derive(Debug, Default)]
pub struct SyncReport {
    pub feed: FeedReport,
    pub rounds: usize,
    pub ops_done: usize,
    pub skipped: Vec<(Op, crate::exec::Skip)>,
    pub failed: Vec<(Op, String)>,
    /// Deletes held by the mass-delete guard, waiting for confirmation.
    pub held: Vec<Op>,
    pub conflicts: Vec<Conflict>,
    /// Nodes still dirty at the end (deferred, failed or held).
    pub pending: u64,
    /// Local items recognized as copies of remote nodes (no transfer needed).
    pub adopted: usize,
    /// Stopped on request ([`Engine::set_stop`]) before the end: what was done is in the
    /// report, the rest stays dirty.
    pub stopped: bool,
    /// What was done, for people, oldest first (also kept in the state DB).
    pub synced: Vec<Synced>,
    /// The drive is out of storage: uploads wait until there is room.
    pub full: bool,
    /// Operations not sent because the drive refused them as they are ([`crate::refused`]),
    /// with why: they wait for a change, an hour, or Sync now.
    pub refused: Vec<(Op, String)>,
    /// Files here not hashed yet (locked, being written, unreadable) that other changes wait
    /// for (sync semantics §6.3), as paths, with why they couldn't be read.
    pub waiting: Vec<(String, String)>,
    /// What is in the folder's `.hbsync/aside` that this device can't place (its state DB lost
    /// or replaced since, say), as paths in the folder: left there, for the user to look at and
    /// delete. Each sync's recovery lists them all again.
    pub left_aside: Vec<String>,
    /// Work is left that another try may do: the sweep was cut short by the round limit, or
    /// operations were skipped or failed with nodes still pending. Deletes held for a
    /// confirmation never count: another try doesn't let them through.
    pub unfinished: bool,
}

impl SyncReport {
    pub fn is_idle(&self) -> bool {
        self.ops_done == 0 && self.pending == 0
    }

    /// Adds a later sync's report to this one: counts and lists add up, `held`, `pending`,
    /// `left_aside` and `unfinished` are the later sync's.
    pub fn absorb(&mut self, later: SyncReport) {
        self.feed += later.feed;
        self.rounds += later.rounds;
        self.ops_done += later.ops_done;
        self.skipped.extend(later.skipped);
        self.failed.extend(later.failed);
        self.held = later.held;
        self.conflicts.extend(later.conflicts);
        self.pending = later.pending;
        self.adopted += later.adopted;
        self.stopped = later.stopped;
        self.synced.extend(later.synced);
        self.full = later.full;
        self.refused.extend(later.refused);
        self.waiting.extend(later.waiting);
        self.left_aside = later.left_aside;
        self.unfinished = later.unfinished;
    }
}

/// What else may stay dirty, at most, for held deletes to leave the dirty set: it is planned
/// alone once more at the end of the sync, to see that its view doesn't reach them.
const LEFTOVERS_MAX: usize = 1_000;

/// The state DB as [`crate::pages::OwnPages`] reads it: a read transaction a query.
struct DbPages<'a>(&'a mut StateDb);

impl crate::pages::PageStore for DbPages<'_> {
    type Mark = Mark;
    type Error = StateError;
    fn marks_of(&mut self, nodes: &[NodeId]) -> Result<Vec<Mark>, StateError> {
        self.0.read(|s| {
            let mut marks = Vec::new();
            for &node in nodes {
                marks.extend(s.dirty_mark(node)?);
            }
            Ok(marks)
        })
    }
    fn marks_under(&mut self, folder: NodeId, limit: usize) -> Result<Vec<Mark>, StateError> {
        self.0.read(|s| s.dirty_marks_under(folder, limit))
    }
    fn folders_in(&mut self, folder: NodeId) -> Result<Vec<NodeId>, StateError> {
        self.0.read(|s| s.base_child_folders(folder))
    }
}

pub struct Engine {
    db: StateDb,
    drive: Arc<Drive>,
    /// The drive's remote as this engine writes to it ([`Scope::remote`]).
    remote: Arc<dyn Remote>,
    cfg: EngineConfig,
    scan_opts: ScanOptions,
    /// Where the sweep over the dirty set stopped when the last sync hit its round limit.
    sweep_after: Option<DirtyCursor>,
    /// What pages wait for, to plan as pages of their own, each once a sweep
    /// ([`crate::pages::OwnPages`]).
    own_pages: crate::pages::OwnPages,
    /// Set by the caller to stop a sync between operations.
    stop: Arc<AtomicBool>,
    /// The guard's held deletes (§7).
    held: HeldDeletes,
    /// How far the current sync is.
    progress: Arc<Counters>,
    /// Nodes whose placement records the current sync pass rescanned for: each once (a file
    /// that can't be hashed yet isn't read again every round).
    rescanned: std::collections::HashSet<NodeId>,
    /// The files here not hashed yet that something waits for, read again and told
    /// ([`SyncReport::waiting`]).
    waited: Waited,
    /// What a sync that failed had done before ([`Engine::take_unreported`]).
    unreported: Option<SyncReport>,
    /// The folder's refusals ([`crate::refused`]).
    refusals: FolderRefusals,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("root", &self.cfg.folder.root)
            .finish()
    }
}

impl Engine {
    /// The engine of the folder `root`, on its account's `drive` ([`Engine::new`]), which
    /// must keep its state in the same DB.
    pub fn open(root: &SyncRoot, drive: Arc<Drive>, cfg: EngineConfig) -> Result<Self, RootError> {
        let db = root.state()?;
        if !drive.same_file(&db) {
            return Err(RootError::State(StateError::Conflict(format!(
                "{}: its drive keeps its state in another DB",
                root.path().display()
            ))));
        }
        Ok(Engine::new(db, drive, cfg)?)
    }

    /// The engine of the folder `db` sees, on the account's `drive`. The folder's top (the
    /// state DB's) decides which part of the drive it syncs: `cfg`'s scope is kept only when
    /// it has that top. A top that can't be read fails the open: no other part of the drive
    /// stands in for it (a role folder run as the main folder would see its files gone from
    /// the drive, and the main folder's coming into it).
    pub fn new(
        mut db: StateDb,
        drive: Arc<Drive>,
        mut cfg: EngineConfig,
    ) -> Result<Self, StateError> {
        let top = db.read(|s| Ok(s.coords()?.top))?;
        if cfg.folder.scope.top != top {
            cfg.folder.scope = Arc::new(Scope::of(top));
        }
        let scan_opts = cfg.folder.scan_options();
        let remote = cfg.folder.scope.remote(drive.shared_remote());
        Ok(Engine {
            db,
            drive,
            remote,
            cfg,
            scan_opts,
            sweep_after: None,
            own_pages: Default::default(),
            stop: Arc::default(),
            held: HeldDeletes::new(),
            progress: Arc::default(),
            rescanned: Default::default(),
            refusals: FolderRefusals::default(),
            waited: Waited::default(),
            unreported: None,
        })
    }

    /// How far the current sync is (started over at each sync), to show while it runs.
    pub fn progress(&self) -> Arc<Counters> {
        Arc::clone(&self.progress)
    }

    /// Counts into `progress` from now on (shared with whoever shows it).
    pub fn set_progress(&mut self, progress: Arc<Counters>) {
        self.progress = progress;
    }

    /// A sync stops between operations while `stop` is set, with [`SyncReport::stopped`].
    /// What was done is kept; the rest stays dirty for a later sync.
    pub fn set_stop(&mut self, stop: Arc<AtomicBool>) {
        self.stop = stop;
    }

    /// Lets the held deletes of `nodes` (those of [`SyncReport::held`] the user was shown)
    /// through, until they are done or undone (a delete waiting for a file not hashed yet
    /// lasts, and what was in such a folder goes with it: [`Plan::delete_waits`]). Deletes of
    /// other nodes are judged as before: one the user didn't see is never let through by a
    /// confirmation.
    pub fn confirm_deletes(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.held.confirm(nodes);
    }

    /// Puts back the held removals of `nodes` the user was shown (sync semantics §7, "Put them
    /// back"), as the drive has them now (read first: what it removed or changed since wins):
    /// - deleted here, live on the drive: its base goes, with what below it is gone here too,
    ///   so it comes down again (a newly ignored item: synced again where the drive has it).
    ///   What below it was moved elsewhere here keeps its base: the move goes on;
    /// - removed elsewhere, still here: restored on the drive from Recently deleted, and so is
    ///   what in it was removed apart before; what is gone for good there loses its base, so
    ///   it goes up again from here, also what the restore itself finds so; a folder gone for
    ///   good only once nothing synced below it waits in Recently deleted (till then it stays
    ///   held with what waits).
    ///
    /// The next sync does the rest; what a failure leaves is still held, and shown again.
    /// Costs a feed read, then one restore of them all: a request per wave and per thousand rows,
    /// and after it two reads per node restored ([`crate::held::after_restore`]). Returns how
    /// many it put back.
    pub fn put_back(&mut self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.held.put_back(&mut self.db, &self.drive, nodes)
    }

    /// Waits up to `timeout` for the live changes `observer`'s watcher reports, and records
    /// them ([`LocalObserver::poll`]). `None` if nothing happened.
    pub fn observe(
        &mut self,
        observer: &mut LocalObserver,
        timeout: Duration,
    ) -> Result<Option<ScanReport>, StateError> {
        observer.poll(&mut self.db, timeout, Debounce::default())
    }

    /// The engine's connection, for tests that set up what a crash leaves behind.
    #[cfg(feature = "testkit")]
    pub fn db(&mut self) -> &mut StateDb {
        &mut self.db
    }

    /// The account's drive: the feed, Recently deleted.
    pub fn drive(&self) -> &Arc<Drive> {
        &self.drive
    }

    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }

    pub fn scan_options(&self) -> &ScanOptions {
        &self.scan_opts
    }

    /// Start-up: the local catch-up (journal or walk, on its own connection to the state DB)
    /// and the remote feed run in parallel. Planning starts only when both are done, so nothing
    /// is uploaded while the local state is still unknown.
    pub fn start_up(
        &mut self,
        observer: &mut LocalObserver,
        date: &str,
    ) -> Result<StartupReport, EngineError> {
        self.apply_filter_change()?;
        let (drive, folder) = (Arc::clone(&self.drive), self.db.folder());
        let db = &mut self.db;
        let (local, feed) = std::thread::scope(|scope| {
            let local =
                scope.spawn(|| -> Result<CatchUp, EngineError> { Ok(observer.catch_up(db)?) });
            let feed = drive.pull(Some(folder));
            (local.join(), (feed, Instant::now()))
        });
        let local = local.map_err(|_| EngineError::Panic("local catch-up"))??;
        let (feed, pulled) = feed;
        let mut feed = feed?;
        // The first sync plans from that pull, unless a long local catch-up (a walk) made it
        // old: then what came meanwhile is pulled too.
        if pulled.elapsed() > FRESH_PULL {
            feed += drive.pull(Some(folder))?;
        }
        let sync = self.sync_with(date, Pull::Done(feed))?;
        Ok(StartupReport { local, sync })
    }

    /// Prepares for a filter that changed since the last run (§10): items may have come into
    /// view or left it, which no journal reports, so the next catch-up walks the whole tree and
    /// every node is planned again. Returns whether the filter changed. In one transaction with
    /// the new fingerprint, so a crash can't skip it.
    pub fn apply_filter_change(&mut self) -> Result<bool, StateError> {
        let fingerprint = self.cfg.folder.filter.fingerprint();
        let scope = self.cfg.folder.scope.fingerprint();
        self.db.write(|s| {
            // A new scope (§12) changes what is in view remotely only: no walk. The folders it
            // or the last one left out are planned again from their tops, which brings what
            // is in them: what this device holds there goes, what comes into view comes.
            let last = s.get_kv(keys::SCOPE)?;
            if last.as_deref() != Some(scope.as_str()) {
                let last = last.as_deref().map(Scope::outside_of).unwrap_or_default();
                last.union(&self.cfg.folder.scope.outside)
                    .try_for_each(|n| s.folder().mark_dirty(*n))?;
                s.folder().set_kv(keys::SCOPE, &scope)?;
            }
            if s.get_kv(keys::FILTER)?.as_deref() == Some(fingerprint.as_str()) {
                return Ok(false);
            }
            s.folder().delete_kv(keys::JOURNAL_CURSOR)?;
            s.folder().mark_synced_dirty()?;
            s.folder().set_kv(keys::FILTER, &fingerprint)?;
            Ok(true)
        })
    }

    /// Where `node` is, for people: its local path, or else the path it had when last synced
    /// (a node deleted here). `None` for a node this device never had.
    pub fn path_of(&mut self, node: NodeId) -> Result<Option<String>, StateError> {
        self.db.read(|s| activity::path_of(s, node))
    }

    /// A full local scan (tier C). The live observer does this incrementally in production.
    pub fn scan_local(&mut self) -> Result<ScanReport, EngineError> {
        Ok(scan(&mut self.db, &self.cfg.folder.root, &self.scan_opts)?)
    }

    /// Pulls remote changes, then plans and executes until nothing is left to do (or the
    /// round limit is hit). `date` (`YYYY-MM-DD`) is used for conflict-copy names.
    pub fn sync_once(&mut self, date: &str) -> Result<SyncReport, EngineError> {
        self.sync_with(date, Pull::Now)
    }

    /// [`Engine::sync_once`], reading the feed first only if `read` (asked for: sync now, a
    /// resume, a wake-up) or the account's picture isn't [`Drive::current`]: while the
    /// notifications are connected, the drive's changes reach the account through them, and
    /// this device's own writes are recorded from their answers.
    pub fn sync(&mut self, date: &str, read: bool) -> Result<SyncReport, EngineError> {
        self.sync_with(date, if read { Pull::Now } else { Pull::IfBehind })
    }

    /// [`Engine::sync_once`], with the feed read as `pulled` says.
    fn sync_with(&mut self, date: &str, pulled: Pull) -> Result<SyncReport, EngineError> {
        // What earlier syncs did is durable before this one puts anything back on disk
        // (unpark), and what this one did is when it ends, however it ends: a power cut loses
        // at most the sync in progress (sync semantics §8a).
        self.db.flush()?;
        let synced = self.sync_pass(date, pulled);
        // Best effort: a failure here leaves what was done undone in no way, and the next
        // sync's opening flush reports it.
        let _ = self.db.flush();
        synced
    }

    fn sync_pass(&mut self, date: &str, pulled: Pull) -> Result<SyncReport, EngineError> {
        let mut report = SyncReport::default();
        self.progress.reset();
        self.rescanned.clear();
        self.waited.new_pass();
        self.unreported = None;
        match self.sync_into(date, pulled, &mut report) {
            Ok(()) => Ok(report),
            // What it did before is told all the same (its conflicts: their copies are made):
            // [`Engine::take_unreported`]. Kept only when there is something to tell.
            Err(e) => {
                // Refused: the account asks nothing more until the user acts ([`Drive::deny`]).
                self.drive.latch(e.class(), &e);
                let tell = !(report.conflicts.is_empty()
                    && report.failed.is_empty()
                    && report.skipped.is_empty()
                    && report.left_aside.is_empty());
                self.unreported = tell.then_some(report);
                Err(e)
            }
        }
    }

    /// One sync, into `report`.
    fn sync_into(
        &mut self,
        date: &str,
        pulled: Pull,
        report: &mut SyncReport,
    ) -> Result<(), EngineError> {
        // What a delete or a download left aside (a crash, an item that couldn't be put back or
        // read) is decided first, what it puts back read again where it is now. A listing of an
        // empty folder where nothing is.
        let aside = recover_aside(&mut self.db, &self.cfg.folder)?;
        report.left_aside = aside.unknown;
        if !aside.rescan.is_empty() {
            rescan_dirs(
                &mut self.db,
                &self.cfg.folder.root,
                &aside.rescan,
                &self.scan_opts,
            )?;
        }
        self.rescan_records()?;
        recover(&mut self.db, &self.cfg.folder)?;
        let folder = Some(self.db.folder());
        report.feed = match pulled {
            Pull::Done(feed) => feed,
            Pull::Now => self.drive.pull(folder)?,
            Pull::IfBehind => self.drive.catch_up(folder)?,
        };
        // A role folder's top on the drive: joined, restored, or made.
        if let Some(role) = hbsync_types::Role::of(self.cfg.folder.scope.top) {
            self.drive.ensure_role(role)?;
        }
        report.adopted = adopt(&mut self.db, &self.cfg.folder)?;
        // What an earlier run held, or one the user answered since, is planned again.
        self.held.start(&mut self.db)?;
        // Once more if a plan reached the held deletes out of the dirty set: then they are
        // dirty again (and none out, so it ends).
        let (swept, after) = loop {
            if let Some(after) = self.sweep(date, report)? {
                break after;
            }
        };
        // A sync cut short by the round limit leaves the rest of the sweep to the next one (the
        // node it stopped at may have gone clean meanwhile; the page query doesn't need it).
        self.sweep_after = after;
        report.pending = self.db.read(|s| s.pending_count())?;
        report.unfinished = !swept
            || (report.pending > 0 && !(report.skipped.is_empty() && report.failed.is_empty()));
        if report.ops_done > 0 || report.feed.deleted > 0 {
            // Ids of nodes gone everywhere.
            self.db.write(|s| s.account().collect_nodes())?;
        }
        // Its writes changed other folders' parts of the drive (a move across them): their
        // engines look.
        let folder = self.db.folder();
        self.drive.wake_touched(&mut self.db, Some(folder));
        // Also when nothing ran: the drive is still full for this folder.
        report.full = self.drive.full();
        Ok(())
    }

    /// Plans and executes the dirty set, round after round, into `report`. Returns whether a
    /// sweep ended and where the last one stopped; `None` when a plan reached the held deletes
    /// out of the dirty set: then they are dirty again, and all is to be planned again. A sync
    /// that ends with nothing dirty but held deletes takes those out of the dirty set
    /// ([`crate::held`]).
    fn sweep(
        &mut self,
        date: &str,
        report: &mut SyncReport,
    ) -> Result<Option<(bool, Option<DirtyCursor>)>, EngineError> {
        let (guarding, mut counts, computer) =
            self.held.begin_sweep(&mut self.db, self.cfg.guard)?;
        let mut opts = Options {
            device: self.cfg.device.clone(),
            computer,
            date: date.to_owned(),
            guarding,
            taken: Arc::default(),
            own_page: false,
        };
        #[cfg(feature = "testkit")]
        if let Some(after) = &self.cfg.after_counts {
            (after.0)();
        }
        // The dirty set is planned page by page in node id order and swept round after
        // round, across syncs, so nodes that stay dirty (deferred, failing) can't starve the
        // rest; held deletes leave it once nothing else is (`hold_out`). A sync stops after a
        // full sweep in which nothing was done.
        let mut after = self.sweep_after.take();
        let mut progress = false;
        let mut held: Vec<Op> = Vec::new();
        // What this sweep's pages hold, and of the last sweep, which ended with nothing done.
        let (mut hold_out, mut settled) = (HoldOut::default(), None);
        // Whether the sweep under way started at the first page (not the rest of the last
        // sync's): only such a one settles what is held.
        let mut whole = after.is_none();
        let mut swept = false;
        self.own_pages.new_sweep();
        // The nodes pages of their own planned this sweep, by row id (8 bytes a node), sorted.
        let mut planned_own: Vec<i64> = Vec::new();
        // Rounds that planned something: a page whose nodes a page of their own planned this
        // sweep is passed over without one.
        let mut rounds = 0;
        // The sweep's last page was planned: it ends once what its pages wait for is planned
        // too, each as a page of its own (§6.5), or when the sync stops (round limit, a stop).
        let mut ending = false;
        loop {
            let stop = report.stopped || self.stop.load(Ordering::Relaxed);
            let go = rounds < self.cfg.max_rounds && !stop;
            if ending && (!go || self.own_pages.is_empty()) {
                // End of a sweep: what it held, and what waits now.
                ending = false;
                report.held = self.held.shown(std::mem::take(&mut held));
                report.waiting = self.waited.take_told();
                swept = true;
                after = None;
                self.own_pages.new_sweep();
                planned_own.clear();
                let page = std::mem::take(&mut hold_out);
                if !progress {
                    settled = whole.then_some(page);
                    break;
                }
                progress = false;
                whole = true;
                continue;
            }
            if !go {
                report.stopped |= stop;
                break;
            }
            // What a page waits for (a folder it waits for with its dirty subtree; a clash
            // across pages) comes first: a page of its own, so a folder's delete goes as one
            // (§6.5). Else the sweep's next page. Each page with the guard's counts of what it
            // holds: counted again, in the same read, when they are stale
            // ([`HeldDeletes::recount`]).
            let own_page = self.next_own_page()?;
            if ending && own_page.is_none() {
                // What they waited for had nothing left to plan: the sweep ends.
                continue;
            }
            let (dirty, next, own, fresh, read) = self.db.read(|s| {
                let (dirty, next, own) = match own_page {
                    Some((page, piece)) => (page, after, Some(piece)),
                    None => {
                        let (dirty, next) = s.dirty_marks(after, self.cfg.batch)?;
                        (dirty, next, None)
                    }
                };
                // What a page of its own planned this sweep isn't planned again in it: its
                // plan stands (what it holds, what waits).
                let read = dirty.len();
                let dirty: Vec<Mark> = match own {
                    Some(_) => dirty,
                    None => dirty
                        .into_iter()
                        .filter(|m| planned_own.binary_search(&m.row()).is_err())
                        .collect(),
                };
                let fresh = match dirty.is_empty() {
                    true => None,
                    false => self.held.recount(s, &counts)?,
                };
                Ok((dirty, next, own, fresh, read))
            })?;
            if let Some(fresh) = fresh {
                self.held.recounted(&mut opts.guarding, &fresh);
                counts = fresh;
            }
            if !dirty.is_empty() {
                rounds += 1;
                report.rounds += 1;
                let done_before = report.ops_done;
                // A piece of a folder too big for a page doesn't wait for the folders above it
                // (`Options::own_page`).
                opts.own_page = own == Some(true);
                match self.plan_and_execute(&dirty, &opts, &mut hold_out, report)? {
                    Some(page) => {
                        // A node planned again counts as its last plan says.
                        let planned: std::collections::HashSet<NodeId> =
                            dirty.iter().map(|m| m.node).collect();
                        held.retain(|op| !planned.contains(&op.node()));
                        held.extend(page);
                    }
                    None => return Ok(None),
                }
                progress |= report.ops_done > done_before;
            }
            if own.is_some() {
                planned_own.extend(dirty.iter().map(|m| m.row()));
                planned_own.sort_unstable();
                planned_own.dedup();
                continue;
            }
            after = next;
            ending = read < self.cfg.batch;
        }
        if !swept {
            report.held = self.held.shown(held);
            report.waiting.extend(self.waited.take_told());
        }
        if let Some(page) = settled
            && !report.stopped
        {
            self.hold_out(page, &opts)?;
        }
        Ok(Some((swept, after)))
    }

    /// The next page of its own ([`crate::pages::OwnPages::next`]): one planned whole, more
    /// than a page directly in a folder, costs about 1 KB a node at its peak (measured: a flat
    /// folder of 100,000 items, 100 MB). A query a folder (two for one too big, three for one
    /// planned whole), one a node waiting together.
    fn next_own_page(&mut self) -> Result<Option<(Vec<Mark>, bool)>, EngineError> {
        Ok(self
            .own_pages
            .next(self.cfg.batch, &mut DbPages(&mut self.db))?)
    }

    /// Takes the held deletes of a sweep that did nothing out of the dirty set: unless what
    /// else stays dirty (refused, failing, waiting for a hash) is more than
    /// [`LEFTOVERS_MAX`], or its view, planned alone with them out, reaches them (then they
    /// are planned with it every sync, as before). Checked and written against the same dirty
    /// set: a mark that came meanwhile leaves them dirty.
    fn hold_out(&mut self, page: HoldOut, opts: &Options) -> Result<(), EngineError> {
        let out = page.nodes();
        if out.is_empty() {
            return Ok(());
        }
        let limit = out.len() + LEFTOVERS_MAX;
        let (dirty, _) = self.db.read(|s| s.dirty_marks(None, limit + 1))?;
        if dirty.len() > limit {
            return Ok(());
        }
        let rest: Vec<NodeId> = dirty
            .iter()
            .map(|m| m.node)
            .filter(|n| !out.contains(n))
            .collect();
        if !rest.is_empty() {
            self.plan_with(&rest, opts, Some(&out))?;
            if self.held.take_reached() {
                return Ok(());
            }
        }
        self.held.take_out(&mut self.db, page, dirty.len() as u64)?;
        Ok(())
    }

    /// Rescans the folders that downloads and folders being placed go into (their records), and
    /// those their nodes' items are in now (an old version a download replaced from another
    /// folder went aside), so recovery judges what is on disk: an item put in place but
    /// not committed has its row then, and a record is never dropped only because its item had
    /// none yet. Each record's folders once a sync pass. One query when there is no record.
    fn rescan_records(&mut self) -> Result<(), EngineError> {
        let rescanned = &mut self.rescanned;
        let dirs: Vec<hbsync_platform::RelPath> = self.db.read(|s| {
            let mut dirs = Vec::new();
            for p in s.all_placing()? {
                if rescanned.insert(p.base.node) {
                    dirs.extend(record_dirs(s, &p)?);
                }
            }
            Ok(dirs)
        })?;
        if !dirs.is_empty() {
            rescan_dirs(&mut self.db, &self.cfg.folder.root, &dirs, &self.scan_opts)?;
        }
        Ok(())
    }

    /// Something in `plan` waits for files here not hashed yet (sync semantics §6.3): those
    /// that can be are read now ([`Waited::read`]), and `dirty` planned again if that hashed
    /// one or found another change. What still waits is told, by name.
    fn read_waiting(
        &mut self,
        plan: Plan,
        dirty: &[NodeId],
        opts: &Options,
    ) -> Result<Plan, EngineError> {
        let root = &self.cfg.folder.root;
        let mut plan = plan;
        if self
            .waited
            .read(&mut self.db, root, &self.scan_opts, &plan.waiting)?
        {
            plan = self.plan(dirty, opts)?;
        }
        self.waited.tell(&mut self.db, &plan.waiting)?;
        Ok(plan)
    }

    /// Plans `dirty` from a view of what the rules may need ([`view::build_narrow`]), again
    /// with the folders a plan judged in part covered whole, until a plan needs no more.
    fn plan(&mut self, dirty: &[NodeId], opts: &Options) -> Result<Plan, EngineError> {
        self.plan_with(dirty, opts, None)
    }

    /// [`Engine::plan`], noting whether a view reached the held deletes out of the dirty set,
    /// or `more` ([`crate::held::touches`]).
    fn plan_with(
        &mut self,
        dirty: &[NodeId],
        opts: &Options,
        more: Option<&std::collections::HashSet<NodeId>>,
    ) -> Result<Plan, EngineError> {
        let folder = &self.cfg.folder;
        let db = &mut self.db;
        let reconcile = |input: &crate::reconcile::Input, db: &mut StateDb| {
            reconcile_with_known(input, opts, folder.masking(), |id| {
                db.read(|s| s.knows_node(id)).unwrap_or(true)
            })
        };
        // Whether a view reached the held deletes out of the dirty set.
        let held = self.held.out();
        let reached = std::cell::Cell::new(false);
        let plan = view::plan_narrow(
            db,
            dirty,
            folder,
            |input: &crate::reconcile::Input, db: &mut StateDb| {
                reached.set(
                    reached.get()
                        || crate::held::touches(input, held)
                        || more.is_some_and(|more| crate::held::touches(input, more)),
                );
                reconcile(input, db)
            },
        )?;
        self.held.reached(reached.get());
        #[cfg(feature = "testkit")]
        if self.cfg.check_narrow {
            let input = db.read(|s| view::build_input(s, dirty, folder))?;
            assert_eq!(
                plan,
                reconcile(&input, db),
                "the narrow view plans as the whole one"
            );
        }
        Ok(plan)
    }

    /// What the last sync, which failed (stopped denied or unreachable, say), had done before:
    /// its conflicts are to be told all the same.
    pub fn take_unreported(&mut self) -> Option<SyncReport> {
        self.unreported.take()
    }

    /// Forgets the folder's refusals (Sync now): each refused operation is sent once more.
    pub fn clear_refused(&mut self) -> Result<(), StateError> {
        self.refusals.clear(&mut self.db)
    }

    /// Plans one page of dirty nodes and executes the plan. No feed read after it: each write
    /// is recorded from its answer (version, token, the server's time), and the notifications
    /// leave this device's own writes out. Returns the deletes the mass-delete guard held (into
    /// `hold_out`, those that may leave the dirty set at the end of the sync: [`crate::held`]).
    /// `None` when the plan reached the held deletes out of it: they are dirty again, nothing
    /// ran.
    fn plan_and_execute(
        &mut self,
        marks: &[Mark],
        opts: &Options,
        hold_out: &mut HoldOut,
        report: &mut SyncReport,
    ) -> Result<Option<Vec<Op>>, EngineError> {
        let dirty: Vec<NodeId> = marks.iter().map(|m| m.node).collect();
        // What an earlier round left placed (its operation failed after), as at the start of a
        // sync: no round places over a record it didn't write. One query when there is none.
        self.rescan_records()?;
        recover_placements(&mut self.db, &self.cfg.folder)?;
        let plan = self.plan(&dirty, opts)?;
        let plan = self.read_waiting(plan, &dirty, opts)?;
        if self.held.take_reached() {
            self.held.plan_again(&mut self.db)?;
            return Ok(None);
        }
        self.own_pages
            .add(&plan.waits_for_pages, &plan.waits_together);
        self.held.after_plan(&mut self.db, &plan, &dirty)?;
        #[cfg(feature = "testkit")]
        if let Some(after) = &self.cfg.after_plan {
            (after.0)();
        }
        // Each mark as the page read it: one made since (another engine's pull, an
        // announcement) may be of a change the plan didn't see, and stays.
        let clean: std::collections::HashSet<NodeId> = plan.clean.iter().copied().collect();
        self.db.write(|s| {
            marks
                .iter()
                .filter(|m| clean.contains(&m.node))
                .try_for_each(|m| s.folder().clear_mark(*m))
        })?;
        // Held till answered: out of the dirty set at the end of the sync, if that is all.
        hold_out.add(&plan, &dirty, marks);
        report.conflicts.extend(plan.conflicts);
        let mut ops = plan.ops;
        let case_insensitive = self.cfg.folder.case_insensitive;
        let (refusals, refused) = self
            .refusals
            .take(&mut self.db, &mut ops, case_insensitive)?;
        report.refused.extend(refused);
        if ops.is_empty() {
            return Ok(Some(plan.held));
        }
        let exec = Executor::new(
            &mut self.db,
            self.remote.as_ref(),
            &self.cfg.folder,
            &self.cfg.exec,
        )
        .with_stop(Arc::clone(&self.stop))
        .with_progress(Arc::clone(&self.progress))
        .with_full(self.drive.full())
        .with_refusals(refusals);
        self.refusals.running();
        let exec = exec.run(ops)?;
        self.refusals.ran(exec.refused);
        self.drive.absorb(exec.news);
        // What was done counts, and the folders it asks to look at are looked at, however
        // the run ended.
        report.ops_done += exec.done;
        report.skipped.extend(exec.skipped);
        report.failed.extend(exec.failed);
        let synced = activity::group(exec.synced, activity::GROUP_OVER);
        self.db
            .write(|s| s.folder().add_synced(&synced, activity::KEEP))?;
        report.synced.extend(synced);
        if !exec.rescan.is_empty() {
            rescan_dirs(
                &mut self.db,
                &self.cfg.folder.root,
                &exec.rescan,
                &self.scan_opts,
            )?;
        }
        match exec.stopped {
            None => {}
            Some(Stopped::Requested) => {
                report.stopped = true;
                return Ok(Some(plan.held));
            }
            Some(stopped) => return Err(stopped.into()),
        }
        // A write the drive refused for its state: the next round plans from the feed read
        // now (the drive being behind, it is read whether the notifications run or not).
        if exec.news.behind {
            report.feed += self.drive.catch_up(Some(self.db.folder()))?;
        }
        Ok(Some(plan.held))
    }
}

/// Whether a sync reads the feed first.
enum Pull {
    /// Read just before (start-up): its report.
    Done(FeedReport),
    Now,
    /// Only if the account's picture isn't [`Drive::current`].
    IfBehind,
}
