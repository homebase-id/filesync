//! The sync loop for one sync root.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hbsync_local::{CatchUp, LocalObserver, ScanOptions, ScanReport, rescan_dirs, scan};
use hbsync_remote::{Remote, RemoteError};
use hbsync_state::{DirtyCursor, StateDb, StateError};
use hbsync_types::NodeId;
use hbsync_types::activity::{Deleted, Synced};

use crate::activity::{self, Counters};
use crate::adopt::adopt;
use crate::deleted;
use crate::exec::{ExecConfig, Executor, Stopped, recover_forks, sweep_tmp, unpark};
use crate::feed::{self, FeedError, FeedReport};
use crate::reconcile::{Conflict, Op, Options, reconcile};
use crate::view;

/// The state DB key of the fingerprint of the filter the DB was last synced with.
const FILTER_KEY: &str = "filter.fingerprint";

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub exec: ExecConfig,
    /// Shown in conflict-copy names.
    pub device: String,
    pub feed_page_size: usize,
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
                crash_at: None,
            },
            device: device.to_owned(),
            feed_page_size: 500,
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
    pub feed: FeedReport,
    /// The first sync after both catch-ups.
    pub sync: SyncReport,
}

/// What one sync did.
#[derive(Debug, Default)]
pub struct SyncReport {
    pub feed: FeedReport,
    pub rounds: usize,
    pub ops_done: usize,
    pub skipped: Vec<(Op, String)>,
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
}

impl SyncReport {
    pub fn is_idle(&self) -> bool {
        self.ops_done == 0 && self.pending == 0
    }

    /// Adds a later sync's report to this one: counts and lists add up, `held` and `pending`
    /// are the later sync's.
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
    }
}

pub struct Engine {
    db: StateDb,
    remote: Box<dyn Remote>,
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
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("root", &self.cfg.exec.root)
            .finish()
    }
}

impl Engine {
    pub fn new(db: StateDb, remote: Box<dyn Remote>, cfg: EngineConfig) -> Self {
        let mut scan_opts = ScanOptions::for_root(&cfg.exec.root, cfg.exec.algorithm);
        scan_opts.filter = Arc::clone(&cfg.exec.filter);
        Engine {
            db,
            remote,
            cfg,
            scan_opts,
            urgent: Default::default(),
            sweep_after: None,
            stop: Arc::default(),
            confirmed: Default::default(),
            progress: Arc::default(),
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

    pub fn db(&mut self) -> &mut StateDb {
        &mut self.db
    }

    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }

    pub fn scan_options(&self) -> &ScanOptions {
        &self.scan_opts
    }

    /// Start-up: the local catch-up (journal or walk, on its own connection to the state DB
    /// at `db_path`) and the remote feed run in parallel. Planning starts only when both are
    /// done, so nothing is uploaded while the local state is still unknown.
    pub fn start_up(
        &mut self,
        observer: &mut LocalObserver,
        db_path: &Path,
        date: &str,
    ) -> Result<StartupReport, EngineError> {
        self.apply_filter_change()?;
        let remote = self.remote.as_ref();
        let page = self.cfg.feed_page_size;
        let db = &mut self.db;
        let (local, feed) = std::thread::scope(|scope| {
            let local = scope.spawn(|| -> Result<CatchUp, EngineError> {
                let mut own = StateDb::open(db_path)?;
                Ok(observer.catch_up(&mut own)?)
            });
            let feed = feed::pull(db, remote, page);
            (local.join(), feed)
        });
        let local = local.map_err(|_| EngineError::Panic("local catch-up"))??;
        let feed = feed?;
        let sync = self.sync_once(date)?;
        Ok(StartupReport { local, feed, sync })
    }

    /// Prepares for a filter that changed since the last run (§10): items may have come into
    /// view or left it, which no journal reports, so the next catch-up walks the whole tree and
    /// every node is planned again. Returns whether the filter changed. In one transaction with
    /// the new fingerprint, so a crash can't skip it.
    pub fn apply_filter_change(&mut self) -> Result<bool, StateError> {
        let fingerprint = self.cfg.exec.filter.fingerprint();
        self.db.write(|s| {
            if s.get_kv(FILTER_KEY)?.as_deref() == Some(fingerprint.as_str()) {
                return Ok(false);
            }
            s.delete_kv(hbsync_local::journal::CURSOR_KEY)?;
            s.mark_synced_dirty()?;
            s.set_kv(FILTER_KEY, &fingerprint)?;
            Ok(true)
        })
    }

    /// Where `node` is, for people: its local path, or else the path it had when last synced
    /// (a node deleted here). `None` for a node this device never had.
    pub fn path_of(&mut self, node: NodeId) -> Result<Option<String>, StateError> {
        self.db.read(|s| activity::path_of(s, node))
    }

    /// Recently deleted, newest first.
    pub fn recently_deleted(&mut self, limit: usize) -> Result<Vec<Deleted>, StateError> {
        deleted::list(&mut self.db, limit)
    }

    /// Restores these items of Recently deleted (each with what was deleted with it); the
    /// next sync brings them to this folder. Returns how many items came back.
    pub fn restore(&mut self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        let mut restored = 0;
        for &node in nodes {
            restored += deleted::restore(&mut self.db, self.remote.as_ref(), node)?;
        }
        Ok(restored)
    }

    /// Deletes these items of Recently deleted for good (all of them with `None`).
    pub fn empty_deleted(&mut self, nodes: Option<&[NodeId]>) -> Result<usize, EngineError> {
        feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        deleted::empty(&mut self.db, self.remote.as_ref(), nodes)
    }

    /// Deletes for good what was deleted before `before_ms` (Unix ms): Recently deleted and
    /// the local trash.
    pub fn purge(&mut self, before_ms: i64) -> Result<usize, EngineError> {
        feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        let state_dir = self.cfg.exec.state_dir();
        deleted::purge(&mut self.db, self.remote.as_ref(), before_ms, &state_dir)
    }

    /// A full local scan (tier C). The live observer does this incrementally in production.
    pub fn scan_local(&mut self) -> Result<ScanReport, EngineError> {
        Ok(scan(&mut self.db, &self.cfg.exec.root, &self.scan_opts)?)
    }

    /// Pulls remote changes, then plans and executes until nothing is left to do (or the
    /// round limit is hit). `date` (`YYYY-MM-DD`) is used for conflict-copy names.
    pub fn sync_once(&mut self, date: &str) -> Result<SyncReport, EngineError> {
        let mut report = SyncReport::default();
        self.progress.reset();
        // After the scan, before planning: see recover_forks.
        recover_forks(&mut self.db, &self.cfg.exec.root)?;
        unpark(&mut self.db, &self.cfg.exec)?;
        sweep_tmp(&mut self.db, &self.cfg.exec.state_dir())?;
        report.feed = feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        report.adopted = adopt(&mut self.db, &self.cfg.exec.root, self.cfg.exec.os)?;

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
            Ok((pending, s.base_file_count()?, still))
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
        if report.ops_done > 0 || report.feed.deleted > 0 {
            // Ids of nodes gone everywhere.
            self.db.write(|s| s.collect_nodes())?;
        }
        Ok(report)
    }

    /// Plans one page of dirty nodes and executes the plan; then pulls the feed again.
    /// Returns the deletes the mass-delete guard held.
    fn plan_and_execute(
        &mut self,
        dirty: &[NodeId],
        opts: &Options,
        report: &mut SyncReport,
    ) -> Result<Vec<Op>, EngineError> {
        let os = self.cfg.exec.os;
        let input = self
            .db
            .read(|s| view::build_input(s, dirty, os, &self.cfg.exec.filter))?;
        let plan = reconcile(&input, opts);
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
            .run(plan.ops)?;
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
        let more = feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        report.feed += more;
        Ok(plan.held)
    }
}
