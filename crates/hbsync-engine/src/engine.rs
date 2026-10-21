//! The sync loop for one sync root.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_local::{
    CatchUp, Debounce, LocalObserver, ScanOptions, ScanReport, rescan_dirs, rescan_entries, scan,
};
use hbsync_platform::RelPath;
use hbsync_remote::Remote;
use hbsync_state::{DirtyCursor, Mark, ReadTx, StateDb, StateError, keys};
use hbsync_types::activity::Synced;
use hbsync_types::{NodeId, NodeKind};

use crate::activity::{self, Counters};
use crate::adopt::adopt;
use crate::drive::Drive;
use crate::error::EngineError;
use crate::exec::{ExecConfig, Executor, Stopped, record_dirs, recover, recover_placements};
use crate::feed::FeedReport;
use crate::folder::FolderCtx;
use crate::reconcile::{Conflict, DeleteGuard, Op, Options, Plan, reconcile_with_known};
use crate::refused::FolderRefusals;
use crate::root::{RootError, SyncRoot};
use crate::scope::Scope;
use crate::view;

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
        }
    }
}

/// The rows of `nodes` here, each with the path the user knows it by (a parked item's
/// original one).
fn waiting_rows(
    s: &ReadTx<'_>,
    nodes: &[NodeId],
) -> Result<Vec<(hbsync_state::LocalRow, RelPath)>, StateError> {
    let mut rows = Vec::new();
    for &node in nodes {
        if let Some(row) = s.local_by_node(node)? {
            let shown = view::parked_place(s, &row)?.unwrap_or_else(|| row.path.clone());
            rows.push((row, shown));
        }
    }
    Ok(rows)
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
    /// Work is left that another try may do: the sweep was cut short by the round limit, or
    /// operations were skipped or failed with nodes still pending. Deletes held for a
    /// confirmation never count: another try doesn't let them through.
    pub unfinished: bool,
}

impl SyncReport {
    pub fn is_idle(&self) -> bool {
        self.ops_done == 0 && self.pending == 0
    }

    /// Adds a later sync's report to this one: counts and lists add up, `held`, `pending` and
    /// `unfinished` are the later sync's.
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
        self.unfinished = later.unfinished;
    }
}

/// The guard's held deletes out of the dirty set ([`crate::held`]), as this engine took them
/// out (the store keeps which nodes; a new engine plans them again).
#[derive(Debug, Default)]
struct HeldOut {
    /// The deletes, shown with every sync's report.
    ops: Vec<Op>,
    /// Their nodes out of the dirty set.
    nodes: std::collections::HashSet<NodeId>,
    /// The files they take ([`Options::held_files`]).
    files: (usize, usize),
    /// Their part of the guard's totals ([`ReadTx::held_file_deletes`]), once read: what
    /// changes them marks a node of theirs dirty, and the plan that reaches it plans them all
    /// again.
    pending: Option<(u64, u64)>,
    /// A plan's view reached one of `nodes` ([`crate::held::touches`]): set by
    /// [`Engine::plan`], taken by the page's round.
    reached: bool,
    /// To plan again at the next sync: an answer came, or this engine is new.
    plan_again: bool,
}

/// The held deletes of a sweep's pages that may leave the dirty set at its end
/// ([`crate::held::out_of_dirty`]): the deletes, the marks of the nodes they cover as each
/// page read them, and the files they take.
#[derive(Debug, Default)]
struct HoldOut {
    ops: Vec<Op>,
    marks: Vec<Mark>,
    files: (usize, usize),
}

impl HoldOut {
    fn add(&mut self, ops: impl IntoIterator<Item = Op>, (here, there): (usize, usize)) {
        self.ops.extend(ops);
        self.files = (self.files.0 + here, self.files.1 + there);
    }
}

impl HeldOut {
    fn add(&mut self, ops: impl IntoIterator<Item = Op>, (here, there): (usize, usize)) {
        self.ops.extend(ops);
        self.files = (self.files.0 + here, self.files.1 + there);
    }
}

/// What else may stay dirty, at most, for held deletes to leave the dirty set: it is planned
/// alone once more at the end of the sync, to see that its view doesn't reach them.
const LEFTOVERS_MAX: usize = 1_000;

pub struct Engine {
    db: StateDb,
    drive: Arc<Drive>,
    /// The drive's remote as this engine writes to it ([`Scope::remote`]).
    remote: Arc<dyn Remote>,
    cfg: EngineConfig,
    scan_opts: ScanOptions,
    /// Where the sweep over the dirty set stopped when the last sync hit its round limit.
    sweep_after: Option<DirtyCursor>,
    /// Set by the caller to stop a sync between operations.
    stop: Arc<AtomicBool>,
    /// Nodes whose held deletes the user confirmed, until they are done (or undone).
    confirmed: std::collections::HashSet<NodeId>,
    /// Nodes whose removal elsewhere the guard held as every synced file, till answered
    /// ([`keys::WIPED`]).
    wiped: std::collections::HashSet<NodeId>,
    /// The guard's held deletes out of the dirty set ([`crate::held`]).
    held: HeldOut,
    /// How far the current sync is.
    progress: Arc<Counters>,
    /// Nodes whose placement records the current sync pass rescanned for: each once (a file
    /// that can't be hashed yet isn't read again every round).
    rescanned: std::collections::HashSet<NodeId>,
    /// The files the current sweep found something waiting for, that couldn't be read
    /// ([`SyncReport::waiting`]).
    waiting: Vec<(String, String)>,
    /// The files waited for that this sync pass read already, and why those it couldn't read
    /// couldn't: each read once a pass.
    read_once: std::collections::HashMap<NodeId, Option<String>>,
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
            stop: Arc::default(),
            confirmed: Default::default(),
            wiped: Default::default(),
            // What an earlier run held out is planned again at the first sync.
            held: HeldOut {
                plan_again: true,
                ..HeldOut::default()
            },
            progress: Arc::default(),
            rescanned: Default::default(),
            refusals: FolderRefusals::default(),
            waiting: Vec::new(),
            read_once: Default::default(),
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
        self.held.plan_again = true;
        self.confirmed.extend(nodes);
    }

    /// Puts back the held removals of `nodes` the user was shown (sync semantics §7, "Put them
    /// back"), as the drive has them now (read first: what it removed or changed since wins):
    /// - deleted here, live on the drive: its base goes, with what below it is gone here too,
    ///   so it comes down again (a newly ignored item: synced again where the drive has it).
    ///   What below it was moved elsewhere here keeps its base: the move goes on;
    /// - removed elsewhere, still here: restored on the drive from Recently deleted, and so is
    ///   what in it was removed apart before; what is gone for good there loses its base, so
    ///   it goes up again from here.
    ///
    /// The next sync does the rest; what a failure leaves is still held, and shown again.
    /// Costs a feed read, and a restore (a read and a write or more) per item restored.
    /// Returns how many it put back.
    pub fn put_back(&mut self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        // Whatever it puts back, the held deletes are planned again at the next sync.
        self.held.plan_again = true;
        self.drive.pull(None)?;
        // Each to restore, with its folder and its row of Recently deleted.
        let (mut forget, mut restore) = (Vec::new(), Vec::new());
        // One restore per removal, the outermost first: what was removed with its folder comes
        // back with it.
        let mut tops: Vec<(usize, NodeId)> = Vec::new();
        // Each once: a folder's held with what is in it.
        let mut seen = std::collections::HashSet::new();
        self.db.read(|s| {
            let here = |n: NodeId| s.local_by_node(n);
            let live = |n: NodeId| Ok::<_, StateError>(s.get_mirror(n)?.is_some());
            let base = |n: NodeId| s.get_base(n);
            for &node in nodes {
                if base(node)?.is_none() {
                    continue;
                }
                match (here(node)?, live(node)?) {
                    (None, true) => {
                        // Below it in the base, gone here and live there.
                        let mut below = vec![node];
                        while let Some(n) = below.pop() {
                            if !seen.insert(n) {
                                continue;
                            }
                            forget.push(n);
                            for b in s.base_children(n)? {
                                if here(b.node)?.is_none() && live(b.node)? {
                                    below.push(b.node);
                                }
                            }
                        }
                    }
                    (Some(row), _) => {
                        // Here, parents first: each synced and gone there restored, or gone for
                        // good (never one made here since); a folder already back (a put back
                        // half done) has what is in it looked at.
                        let mut below = vec![row];
                        while let Some(row) = below.pop() {
                            if !seen.insert(row.node) {
                                continue;
                            }
                            if let Some(b) = base(row.node)?
                                && !live(row.node)?
                            {
                                match s.removed_by_node(row.node)? {
                                    Some(r) => restore.push((row.node, b.state.parent, r)),
                                    None => forget.push(row.node),
                                }
                            }
                            if row.kind == NodeKind::Folder {
                                below.extend(s.local_children_of(&row)?);
                            }
                        }
                    }
                    // The drive removed it too meanwhile: its state wins.
                    (None, false) => {}
                }
            }
            let mut topped = std::collections::HashSet::new();
            for (node, parent, removed) in &restore {
                // Up through the folders it was deleted with (Recently deleted's own rule).
                let (mut top, mut parent, mut at) = (*node, *parent, removed.time());
                while let Some(folder) = s.removed_by_node(parent)?
                    && crate::deleted::deleted_with(at, &folder)
                {
                    top = parent;
                    at = folder.time();
                    parent = folder.row.parent;
                }
                if topped.insert(top) {
                    let (mut depth, mut up) = (0, top);
                    while let Some(b) = base(up)? {
                        depth += 1;
                        up = b.state.parent;
                    }
                    tops.push((depth, top));
                }
            }
            Ok(())
        })?;
        self.db.write(|s| {
            for &node in &forget {
                s.folder().delete_base(node)?;
                s.folder().mark_dirty(node)?;
            }
            Ok(())
        })?;
        tops.sort();
        let tops: Vec<NodeId> = tops.into_iter().map(|(_, n)| n).collect();
        if !tops.is_empty() {
            self.drive.restore_read(&tops)?;
        }
        Ok(forget.len() + restore.len())
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
        self.waiting.clear();
        self.read_once.clear();
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
                    && report.skipped.is_empty());
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
        if std::mem::take(&mut self.held.plan_again) {
            self.plan_held_again()?;
        }
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
        self.held.reached = false;
        // The guard must judge the whole delete, even when it is planned in several batches;
        // confirmed deletes don't count. Confirmations of nodes with nothing left to do are
        // dropped (those whose plan no longer deletes them, in plan_and_execute).
        let confirmed = std::mem::take(&mut self.confirmed);
        // This device's own removals held out of the dirty set: weighed against the totals.
        let own = self
            .held
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { .. }));
        let held_pending = match self.held.pending {
            Some(p) => p,
            None => {
                let p = self.db.read(|s| s.held_file_deletes())?;
                *self.held.pending.insert(p)
            }
        };
        let (pending_deletes, synced_files, confirmed, computer, saved) = self.db.read(|s| {
            let (here, there) = s.pending_file_deletes()?;
            let (here, there) = (here + held_pending.0, there + held_pending.1);
            // Held as every synced file: until answered, each while its removal is pending.
            let saved: std::collections::HashSet<NodeId> =
                read_wiped(s.get_kv(keys::WIPED)?.as_deref())
                    .into_iter()
                    .collect();
            let mut still = std::collections::HashSet::new();
            // Each file once, however many confirmed trashes it is under.
            let (mut confirmed_here, mut confirmed_there) = (0, std::collections::HashSet::new());
            for node in confirmed {
                if let Some((gone_here, gone_there)) = s.confirmed_file_deletes(node)? {
                    confirmed_here += u64::from(gone_here);
                    confirmed_there.extend(gone_there);
                    still.insert(node);
                }
            }
            let pending = (
                here.saturating_sub(confirmed_here),
                there.saturating_sub(confirmed_there.len() as u64),
            );
            // Nothing dirty, nothing to plan: the guard has nothing to weigh against them.
            let synced = match s.dirty_count()? == 0 && self.held.ops.is_empty() {
                true => 0,
                false => s.base_file_count()?,
            };
            // This computer's id for the account ([`Options::computer`]): every open gives the
            // DB one, so none is a fault, never a reason to plan without.
            let computer = s.db_id()?;
            if computer.is_empty() {
                return Err(StateError::Corrupt("it has no id (db.id)".into()));
            }
            Ok((pending, synced, still, computer, saved))
        })?;
        self.confirmed = confirmed;
        let guard = self.cfg.guard;
        // This device's own removals held out are planned again once the guard would let them
        // through (more files synced since, say), as it would if they had stayed dirty.
        // Before the hold of removals elsewhere is looked at: that of one out of the dirty set
        // is kept as it was.
        let (here, _) = self.held.files;
        // And all of them with the guard off.
        if !self.held.ops.is_empty() && guard.allow
            || own
                && !guard.trips(pending_deletes.0 as usize, synced_files as usize)
                && !guard.trips(here, synced_files as usize)
        {
            self.plan_held_again()?;
        }
        // Held as every synced file: until answered (Delete, or put back), each while a
        // removal elsewhere is still to be followed in it: it, or (back on the drive) what is
        // below it. A row lookup or two each; a subtree walk only for one back on the drive.
        // One held out of the dirty set is as it was then: anything that changed it marked it
        // dirty, and the plan that reaches it plans all of them again, from here.
        let mut wiped = std::collections::HashSet::new();
        self.db.read(|s| {
            for &node in &saved {
                let keep = self.held.nodes.contains(&node)
                    || !self.confirmed.contains(&node)
                        && s.get_base(node)?.is_some()
                        && match s.get_mirror(node)?.is_some() {
                            false => s.local_by_node(node)?.is_some(),
                            true => s.removal_pending_under(node)?,
                        };
                if keep {
                    wiped.insert(node);
                }
            }
            Ok(())
        })?;
        if wiped != saved {
            save_wiped(&mut self.db, &wiped)?;
        }
        self.wiped = wiped.clone();
        let opts = Options {
            device: self.cfg.device.clone(),
            computer,
            date: date.to_owned(),
            guard,
            synced_files: synced_files as usize,
            confirmed: Arc::new(self.confirmed.clone()),
            wiped: Arc::new(wiped),
            pending_deletes: (pending_deletes.0 as usize, pending_deletes.1 as usize),
            held_files: self.held.files,
            taken: Arc::default(),
        };
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
        for _ in 0..self.cfg.max_rounds {
            if report.stopped || self.stop.load(Ordering::Relaxed) {
                report.stopped = true;
                break;
            }
            let (dirty, next) = self.db.read(|s| s.dirty_marks(after, self.cfg.batch))?;
            if !dirty.is_empty() {
                report.rounds += 1;
                let done_before = report.ops_done;
                match self.plan_and_execute(&dirty, &opts, &mut hold_out, report)? {
                    Some(page) => held.extend(page),
                    None => return Ok(None),
                }
                progress |= report.ops_done > done_before;
            }
            after = next;
            if dirty.len() < self.cfg.batch {
                // End of a sweep: what it held, and what waits now.
                report.held = self.with_held_out(std::mem::take(&mut held));
                report.waiting = std::mem::take(&mut self.waiting);
                swept = true;
                after = None;
                let page = std::mem::take(&mut hold_out);
                if !progress {
                    settled = whole.then_some(page);
                    break;
                }
                progress = false;
                whole = true;
            }
        }
        if !swept {
            report.held = self.with_held_out(held);
            report.waiting.append(&mut self.waiting);
        }
        if let Some(page) = settled
            && !report.stopped
        {
            self.hold_out(page, &opts)?;
        }
        Ok(Some((swept, after)))
    }

    /// Takes the held deletes of a sweep that did nothing out of the dirty set: unless what
    /// else stays dirty (refused, failing, waiting for a hash) is more than
    /// [`LEFTOVERS_MAX`], or its view, planned alone with them out, reaches them (then they
    /// are planned with it every sync, as before). Checked and written against the same dirty
    /// set: a mark that came meanwhile leaves them dirty.
    fn hold_out(&mut self, page: HoldOut, opts: &Options) -> Result<(), EngineError> {
        if page.marks.is_empty() {
            return Ok(());
        }
        let out: std::collections::HashSet<NodeId> = page.marks.iter().map(|m| m.node).collect();
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
            let before = self.held.nodes.clone();
            self.held.nodes.extend(&out);
            let planned = self.plan(&rest, opts);
            self.held.nodes = before;
            planned?;
            if std::mem::take(&mut self.held.reached) {
                return Ok(());
            }
        }
        let dirty = dirty.len() as u64;
        let held = self.db.write(|s| {
            if s.dirty_count()? != dirty {
                return Ok(false);
            }
            page.marks
                .iter()
                .try_for_each(|m| s.folder().hold_delete(*m))?;
            Ok(true)
        })?;
        if held {
            self.held.add(page.ops, page.files);
            self.held.nodes.extend(out);
            self.held.pending = None;
        }
        Ok(())
    }

    /// `held`, the deletes a sweep held, with those held out of the dirty set
    /// ([`crate::held`]): what the user is shown.
    fn with_held_out(&self, mut held: Vec<Op>) -> Vec<Op> {
        if self.held.ops.is_empty() {
            return held;
        }
        let mut nodes: std::collections::HashSet<NodeId> = held.iter().map(Op::node).collect();
        held.extend(
            self.held
                .ops
                .iter()
                .filter(|op| nodes.insert(op.node()))
                .cloned(),
        );
        held
    }

    /// Puts the held deletes out of the dirty set back into it ([`crate::held`]): planned
    /// again, all of them.
    fn plan_held_again(&mut self) -> Result<(), StateError> {
        // Should the write fail: at the next sync.
        self.held.plan_again = true;
        self.db.write(|s| s.folder().unhold_deletes().map(drop))?;
        self.held = HeldOut::default();
        Ok(())
    }

    /// Rescans the folders that downloads and folders being placed go into (their records), and
    /// those their nodes' items are in now (an old version a download replaced from another
    /// folder went to the trash), so recovery judges what is on disk: an item put in place but
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
    /// unchanged since the scan that couldn't read them (locked, unreadable: one still changing
    /// is read by the scan its change brings) are read now, each once a sync pass, and `dirty`
    /// planned again if that hashed one or found another change. What still waits is told, by name (`waiting`). Costs nothing unless
    /// something waits; then a row and a stat a file, an open (or a read) and a small write of
    /// each unchanged one, and, if that hashed or found a change, one more plan.
    fn read_waiting(
        &mut self,
        plan: Plan,
        dirty: &[NodeId],
        opts: &Options,
    ) -> Result<Plan, EngineError> {
        if plan.waiting.is_empty() {
            return Ok(plan);
        }
        let root = &self.cfg.folder.root;
        let fresh: Vec<NodeId> = plan
            .waiting
            .iter()
            .copied()
            .filter(|n| !self.read_once.contains_key(n))
            .collect();
        let rows = self.db.read(|s| waiting_rows(s, &fresh))?;
        let unchanged: Vec<(NodeId, RelPath)> = rows
            .into_iter()
            .filter(|(row, _)| {
                // Its bytes as the scan saw them: size and mtime (a lock, or a mode change,
                // moves ctime at most).
                hbsync_platform::meta::stat(&row.path.to_native(root))
                    .is_ok_and(|m| row.fingerprint.same_after_rename(&m))
            })
            .map(|(row, _)| (row.node, row.path))
            .collect();
        for node in fresh {
            self.read_once.insert(node, None);
        }
        let mut plan = plan;
        if !unchanged.is_empty() {
            let paths: Vec<RelPath> = unchanged.iter().map(|(_, p)| p.clone()).collect();
            let scan = rescan_entries(&mut self.db, root, &[], &paths, &self.scan_opts)?;
            // A read that failed again marks its node dirty again and records nothing new:
            // planned again only for a hash or another change found.
            if scan.hashed > 0 || scan.dirty > scan.unhashed.len() {
                plan = self.plan(dirty, opts)?;
            }
            for (path, why) in scan.unhashed {
                if let Some((node, _)) = unchanged.iter().find(|(_, p)| *p == path) {
                    self.read_once.insert(*node, Some(why));
                }
            }
        }
        if !plan.waiting.is_empty() {
            let rows = self.db.read(|s| waiting_rows(s, &plan.waiting))?;
            for (row, shown) in rows {
                let why = self.read_once.get(&row.node).cloned().flatten();
                let told = (
                    shown.to_string(),
                    why.unwrap_or_else(|| "being written".into()),
                );
                if !self.waiting.contains(&told) {
                    self.waiting.push(told);
                }
            }
        }
        Ok(plan)
    }

    /// Plans `dirty` from a view of what the rules may need ([`view::build_narrow`]), again
    /// with the folders a plan judged in part covered whole, until a plan needs no more.
    fn plan(&mut self, dirty: &[NodeId], opts: &Options) -> Result<Plan, EngineError> {
        let folder = &self.cfg.folder;
        let db = &mut self.db;
        let reconcile = |input: &crate::reconcile::Input, db: &mut StateDb| {
            reconcile_with_known(input, opts, folder.masking(), |id| {
                db.read(|s| s.knows_node(id)).unwrap_or(true)
            })
        };
        // Whether a view reached the held deletes out of the dirty set.
        let held = &self.held.nodes;
        let reached = std::cell::Cell::new(false);
        let plan = view::plan_narrow(
            db,
            dirty,
            folder,
            |input: &crate::reconcile::Input, db: &mut StateDb| {
                reached.set(reached.get() || crate::held::touches(input, held));
                reconcile(input, db)
            },
        )?;
        self.held.reached |= reached.get();
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
        if std::mem::take(&mut self.held.reached) {
            self.plan_held_again()?;
            return Ok(None);
        }
        // What the guard holds of removals elsewhere stays held till answered (§7): only the
        // every-file rule holds those. Kept before anything of this page runs or goes clean.
        let before = self.wiped.len();
        self.wiped
            .extend(plan.held.iter().filter_map(|op| match op {
                Op::LocalTrash { node, .. } => Some(*node),
                _ => None,
            }));
        if self.wiped.len() != before {
            save_wiped(&mut self.db, &self.wiped)?;
        }
        #[cfg(feature = "testkit")]
        if let Some(after) = &self.cfg.after_plan {
            (after.0)();
        }
        // A confirmation lasts while its node's plan deletes it, or its delete waits for a
        // file in it not hashed yet (§6.3): one whose delete was undone (the file back) is
        // forgotten, so a later delete is judged anew.
        let deleting: std::collections::HashSet<NodeId> = plan
            .ops
            .iter()
            .filter_map(Op::deletes)
            .chain(plan.delete_waits.iter().copied())
            .collect();
        for node in &dirty {
            if !deleting.contains(node) {
                self.confirmed.remove(node);
            }
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
        let (out, nodes) = crate::held::out_of_dirty(&plan, &dirty);
        let files = crate::held::files(&plan, out.iter().copied());
        hold_out.add(out.into_iter().cloned(), files);
        hold_out
            .marks
            .extend(marks.iter().filter(|m| nodes.contains(&m.node)));
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

/// The nodes [`keys::WIPED`] holds; an id that doesn't parse is left out.
fn read_wiped(value: Option<&str>) -> Vec<NodeId> {
    crate::scope::Scope::outside_of(value.unwrap_or_default())
        .into_iter()
        .collect()
}

/// Keeps `wiped` as [`keys::WIPED`] (in [`crate::scope::Scope`]'s id list).
fn save_wiped(
    db: &mut StateDb,
    wiped: &std::collections::HashSet<NodeId>,
) -> Result<(), StateError> {
    let mut ids: Vec<String> = wiped.iter().map(|n| n.0.to_string()).collect();
    ids.sort();
    db.write(|s| match ids.is_empty() {
        true => s.folder().delete_kv(keys::WIPED),
        false => s.folder().set_kv(keys::WIPED, &ids.join(",")),
    })
}
