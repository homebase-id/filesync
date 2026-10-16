//! The sync loop for one sync root.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_local::{CatchUp, Debounce, LocalObserver, ScanOptions, ScanReport, rescan_dirs, scan};
use hbsync_remote::{Remote, RemoteError};
use hbsync_state::{DirtyCursor, StateDb, StateError, keys};
use hbsync_types::NodeId;
use hbsync_types::activity::Synced;

use crate::activity::{self, Counters};
use crate::adopt::adopt;
use crate::drive::Drive;
use crate::exec::{ExecConfig, Executor, Stopped, record_dirs, recover, recover_placements};
use crate::feed::{FeedError, FeedReport};
use crate::reconcile::{Conflict, Op, Options, reconcile_with_known};
use crate::root::{RootError, SyncRoot};
use crate::scope::Scope;
use crate::view;

/// Start-up's pull is the first sync's if the local catch-up ended within this of it.
const FRESH_PULL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub exec: ExecConfig,
    /// Shown in conflict-copy names.
    pub device: String,
    /// Plan/execute rounds per sync before giving the rest to the next sync.
    pub max_rounds: usize,
    /// Dirty nodes planned per round.
    pub batch: usize,
    pub mass_delete_min: usize,
    pub mass_delete_fraction: f64,
    pub allow_mass_delete: bool,
}

impl EngineConfig {
    pub fn new(root: &Path, device: &str) -> Self {
        EngineConfig {
            exec: ExecConfig {
                root: root.to_path_buf(),
                os: hbsync_platform::Os::current(),
                algorithm: hbsync_fileio::HashAlgorithm::Blake3,
                attempts: 4,
                retry_pause: std::time::Duration::from_millis(500),
                transfer: crate::transfer::TransferConfig::default(),
                case_insensitive: crate::exec::probe_case_insensitive(root),
                filter: Default::default(),
                tags_kept: Default::default(),
                scope: Arc::new(Scope::main()),
                crash_at: None,
                faults: Arc::default(),
            },
            device: device.to_owned(),
            max_rounds: 8,
            batch: 20_000,
            mass_delete_min: 50,
            mass_delete_fraction: 0.25,
            allow_mass_delete: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Feed(#[from] FeedError),
    #[error("{0} panicked")]
    Panic(&'static str),
    /// The executor stopped early: access denied, or the remote unreachable.
    #[error(transparent)]
    Stopped(#[from] Stopped),
    /// A remote call outside a sync (Recently deleted) failed.
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

impl EngineError {
    /// The remote refused access (a sync, a feed read): retrying won't help.
    pub fn is_denied(&self) -> bool {
        matches!(
            self,
            EngineError::Stopped(Stopped::Denied(_))
                | EngineError::Feed(FeedError::Remote(RemoteError::Denied(_)))
                | EngineError::Remote(RemoteError::Denied(_))
        )
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
        self.unfinished = later.unfinished;
    }
}

pub struct Engine {
    db: StateDb,
    drive: Arc<Drive>,
    /// The drive's remote as this engine writes to it ([`Scope::remote`]).
    remote: Arc<dyn Remote>,
    cfg: EngineConfig,
    scan_opts: ScanOptions,
    /// Nodes whose transfers go first (a file the user just opened).
    urgent: std::collections::HashSet<hbsync_types::NodeId>,
    /// Where the sweep over the dirty set stopped when the last sync hit its round limit.
    sweep_after: Option<DirtyCursor>,
    /// Set by the caller to stop a sync between operations.
    stop: Arc<AtomicBool>,
    /// Nodes whose held deletes the user confirmed, until they are done (or undone).
    confirmed: std::collections::HashSet<NodeId>,
    /// How far the current sync is.
    progress: Arc<Counters>,
    /// Nodes whose placement records the current sync pass rescanned for: each once (a file
    /// that can't be hashed yet isn't read again every round).
    rescanned: std::collections::HashSet<NodeId>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("root", &self.cfg.exec.root)
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
        Ok(Engine::new(db, drive, cfg))
    }

    /// The engine of the folder `db` sees, on the account's `drive`. The folder's top (the
    /// state DB's) decides which part of the drive it syncs: `cfg`'s scope is kept only when
    /// it has that top.
    pub fn new(mut db: StateDb, drive: Arc<Drive>, mut cfg: EngineConfig) -> Self {
        let top = db
            .read(|s| Ok(s.coords()?.top))
            .unwrap_or(hbsync_types::NodeId::ROOT);
        if cfg.exec.scope.top != top {
            cfg.exec.scope = Arc::new(Scope::of(top));
        }
        let mut scan_opts = ScanOptions::for_root(&cfg.exec.root, cfg.exec.algorithm);
        scan_opts.filter = Arc::clone(&cfg.exec.filter);
        let remote = cfg.exec.scope.remote(drive.shared_remote());
        Engine {
            db,
            drive,
            remote,
            cfg,
            scan_opts,
            urgent: Default::default(),
            sweep_after: None,
            stop: Arc::default(),
            confirmed: Default::default(),
            progress: Arc::default(),
            rescanned: Default::default(),
        }
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
    /// through, until they are done or undone. Deletes of other nodes are judged as before:
    /// one the user didn't see is never let through by a confirmation.
    pub fn confirm_deletes(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.confirmed.extend(nodes);
    }

    /// Transfers of these nodes go before all others in the next syncs.
    pub fn set_urgent(&mut self, nodes: impl IntoIterator<Item = hbsync_types::NodeId>) {
        self.urgent = nodes.into_iter().collect();
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
        let fingerprint = self.cfg.exec.filter.fingerprint();
        let scope = self.cfg.exec.scope.fingerprint();
        self.db.write(|s| {
            // A new scope (§12) changes what is in view remotely only: no walk. The folders it
            // or the last one left out are planned again from their tops, which brings what
            // is in them: what this device holds there goes, what comes into view comes.
            let last = s.get_kv(keys::SCOPE)?;
            if last.as_deref() != Some(scope.as_str()) {
                let last = last.as_deref().map(Scope::outside_of).unwrap_or_default();
                last.union(&self.cfg.exec.scope.outside)
                    .try_for_each(|n| s.mark_dirty(*n))?;
                s.set_kv(keys::SCOPE, &scope)?;
            }
            if s.get_kv(keys::FILTER)?.as_deref() == Some(fingerprint.as_str()) {
                return Ok(false);
            }
            s.delete_kv(keys::JOURNAL_CURSOR)?;
            s.mark_synced_dirty()?;
            s.set_kv(keys::FILTER, &fingerprint)?;
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
        Ok(scan(&mut self.db, &self.cfg.exec.root, &self.scan_opts)?)
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
        self.rescan_records()?;
        recover(&mut self.db, &self.cfg.exec)?;
        let folder = Some(self.db.folder());
        report.feed = match pulled {
            Pull::Done(feed) => feed,
            Pull::Now => self.drive.pull(folder)?,
            Pull::IfBehind => self.drive.catch_up(folder)?,
        };
        // A role folder's top on the drive: joined, restored, or made.
        if let Some(role) = hbsync_types::Role::of(self.cfg.exec.scope.top) {
            self.drive.ensure_role(role)?;
        }
        report.adopted = adopt(
            &mut self.db,
            &self.cfg.exec.root,
            self.cfg.exec.os,
            &self.cfg.exec.scope,
        )?;

        // The guard must judge the whole delete, even when it is planned in several batches;
        // confirmed deletes don't count. Confirmations of nodes with nothing left to do are
        // dropped (those whose plan no longer deletes them, in plan_and_execute).
        let confirmed = std::mem::take(&mut self.confirmed);
        let (pending_deletes, synced_files, confirmed) = self.db.read(|s| {
            let (here, there) = s.pending_file_deletes()?;
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
            let synced = match s.dirty_count()? {
                0 => 0,
                _ => s.base_file_count()?,
            };
            Ok((pending, synced, still))
        })?;
        self.confirmed = confirmed;
        let opts = Options {
            device: self.cfg.device.clone(),
            date: date.to_owned(),
            mass_delete_min: self.cfg.mass_delete_min,
            mass_delete_fraction: self.cfg.mass_delete_fraction,
            synced_files: synced_files as usize,
            allow_mass_delete: self.cfg.allow_mass_delete,
            confirmed: Arc::new(self.confirmed.clone()),
            pending_deletes: (pending_deletes.0 as usize, pending_deletes.1 as usize),
            filter: Arc::clone(&self.cfg.exec.filter),
            scope: Arc::clone(&self.cfg.exec.scope),
            taken: Arc::default(),
        };
        // The dirty set is planned page by page in node id order and swept round after
        // round, across syncs, so nodes that stay dirty (held, deferred, failing) can't starve
        // the rest. A sync stops after a full sweep in which nothing was done.
        let mut after = self.sweep_after.take();
        let mut progress = false;
        let mut held: Vec<Op> = Vec::new();
        let mut swept = false;
        for _ in 0..self.cfg.max_rounds {
            if report.stopped || self.stop.load(Ordering::Relaxed) {
                report.stopped = true;
                break;
            }
            let (dirty, next) = self.db.read(|s| s.dirty_page(after, self.cfg.batch))?;
            if !dirty.is_empty() {
                report.rounds += 1;
                let done_before = report.ops_done;
                held.extend(self.plan_and_execute(&dirty, &opts, &mut report)?);
                progress |= report.ops_done > done_before;
            }
            after = next;
            if dirty.len() < self.cfg.batch {
                // End of a sweep.
                report.held = std::mem::take(&mut held);
                swept = true;
                after = None;
                if !progress {
                    break;
                }
                progress = false;
            }
        }
        if !swept {
            report.held = held;
        }
        // A sync cut short by the round limit leaves the rest of the sweep to the next one (the
        // node it stopped at may have gone clean meanwhile; the page query doesn't need it).
        self.sweep_after = after;
        report.pending = self.db.read(|s| s.dirty_count())?;
        report.unfinished = !swept
            || (report.pending > 0 && !(report.skipped.is_empty() && report.failed.is_empty()));
        if report.ops_done > 0 || report.feed.deleted > 0 {
            // Ids of nodes gone everywhere.
            self.db.write(|s| s.collect_nodes())?;
        }
        // Its writes changed other folders' parts of the drive (a move across them): their
        // engines look.
        let folder = self.db.folder();
        self.drive.wake_touched(&mut self.db, Some(folder));
        // Also when nothing ran: the drive is still full for this folder.
        report.full = self.drive.full();
        Ok(report)
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
            rescan_dirs(&mut self.db, &self.cfg.exec.root, &dirs, &self.scan_opts)?;
        }
        Ok(())
    }

    /// Plans one page of dirty nodes and executes the plan. No feed read after it: each write
    /// is recorded from its answer (version, token, the server's time), and the notifications
    /// leave this device's own writes out. Returns the deletes the mass-delete guard held.
    fn plan_and_execute(
        &mut self,
        dirty: &[NodeId],
        opts: &Options,
        report: &mut SyncReport,
    ) -> Result<Vec<Op>, EngineError> {
        let os = self.cfg.exec.os;
        // What an earlier round left placed (its operation failed after), as at the start of a
        // sync: no round places over a record it didn't write. One query when there is none.
        self.rescan_records()?;
        recover_placements(&mut self.db, &self.cfg.exec)?;
        let input = self.db.read(|s| {
            view::build_input(s, dirty, os, &self.cfg.exec.filter, &self.cfg.exec.scope)
        })?;
        let db = &mut self.db;
        let plan = reconcile_with_known(&input, opts, |id| {
            db.read(|s| s.knows_node(id)).unwrap_or(true)
        });
        // A confirmation lasts while its node's plan deletes it: one whose delete was undone
        // (the file back) is forgotten, so a later delete is judged anew.
        let deleting: std::collections::HashSet<NodeId> =
            plan.ops.iter().filter_map(Op::deletes).collect();
        for node in dirty {
            if !deleting.contains(node) {
                self.confirmed.remove(node);
            }
        }
        self.db
            .write(|s| plan.clean.iter().try_for_each(|n| s.clear_dirty(*n)))?;
        report.conflicts.extend(plan.conflicts);
        if plan.ops.is_empty() {
            return Ok(plan.held);
        }
        let exec = Executor::new(&mut self.db, self.remote.as_ref(), &self.cfg.exec)
            .with_urgent(self.urgent.clone())
            .with_stop(Arc::clone(&self.stop))
            .with_progress(Arc::clone(&self.progress))
            .with_full(self.drive.full())
            .run(plan.ops)?;
        if exec.full {
            self.drive.set_full();
        }
        // What was done counts, and the folders it asks to look at are looked at, however
        // the run ended.
        report.ops_done += exec.done;
        report.skipped.extend(exec.skipped);
        report.failed.extend(exec.failed);
        let synced = activity::group(exec.synced, activity::GROUP_OVER);
        self.db.write(|s| s.add_synced(&synced, activity::KEEP))?;
        report.synced.extend(synced);
        if !exec.rescan.is_empty() {
            rescan_dirs(
                &mut self.db,
                &self.cfg.exec.root,
                &exec.rescan,
                &self.scan_opts,
            )?;
        }
        match exec.stopped {
            None => {}
            Some(Stopped::Requested) => {
                report.stopped = true;
                return Ok(plan.held);
            }
            Some(stopped) => return Err(stopped.into()),
        }
        Ok(plan.held)
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
