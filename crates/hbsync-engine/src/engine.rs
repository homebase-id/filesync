//! The sync loop for one sync root.

use std::path::Path;

use hbsync_local::{CatchUp, LocalObserver, ScanOptions, ScanReport, rescan_dirs, scan};
use hbsync_remote::{Remote, RemoteError};
use hbsync_state::{DirtyCursor, StateDb, StateError};
use hbsync_types::NodeId;

use crate::adopt::adopt;
use crate::exec::{ExecConfig, Executor, recover_forks, sweep_tmp, unpark};
use crate::feed::{self, FeedError, FeedReport};
use crate::reconcile::{Conflict, Op, Options, reconcile};
use crate::view;

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
    /// The remote refused access: syncing stops until the user authorizes again.
    #[error("access denied: {0}")]
    Denied(String),
}

impl EngineError {
    /// The remote refused access (a sync, a feed read): retrying won't help.
    pub fn is_denied(&self) -> bool {
        matches!(
            self,
            EngineError::Denied(_) | EngineError::Feed(FeedError::Remote(RemoteError::Denied(_)))
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
}

impl SyncReport {
    pub fn is_idle(&self) -> bool {
        self.ops_done == 0 && self.pending == 0
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
        let scan_opts = ScanOptions::for_root(&cfg.exec.root, cfg.exec.algorithm);
        Engine {
            db,
            remote,
            cfg,
            scan_opts,
            urgent: Default::default(),
            sweep_after: None,
        }
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

    /// A full local scan (tier C). The live observer does this incrementally in production.
    pub fn scan_local(&mut self) -> Result<ScanReport, EngineError> {
        Ok(scan(&mut self.db, &self.cfg.exec.root, &self.scan_opts)?)
    }

    /// Pulls remote changes, then plans and executes until nothing is left to do (or the
    /// round limit is hit). `date` (`YYYY-MM-DD`) is used for conflict-copy names.
    pub fn sync_once(&mut self, date: &str) -> Result<SyncReport, EngineError> {
        let mut report = SyncReport::default();
        // After the scan, before planning: see recover_forks.
        recover_forks(&mut self.db, &self.cfg.exec.root)?;
        unpark(&mut self.db, &self.cfg.exec.root)?;
        sweep_tmp(&mut self.db, &self.cfg.exec.state_dir())?;
        report.feed = feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        report.adopted = adopt(&mut self.db, &self.cfg.exec.root, self.cfg.exec.os)?;

        // The guard must judge the whole delete, even when it is planned in several batches.
        let (pending_deletes, synced_files) = self
            .db
            .read(|s| Ok((s.pending_file_deletes()?, s.base_file_count()?)))?;
        let opts = Options {
            device: self.cfg.device.clone(),
            date: date.to_owned(),
            mass_delete_min: self.cfg.mass_delete_min,
            mass_delete_fraction: self.cfg.mass_delete_fraction,
            synced_files: synced_files as usize,
            allow_mass_delete: self.cfg.allow_mass_delete,
            pending_deletes: (pending_deletes.0 as usize, pending_deletes.1 as usize),
        };
        // The dirty set is planned page by page in node id order and swept round after
        // round, across syncs, so nodes that stay dirty (held, deferred, failing) can't starve
        // the rest. A sync stops after a full sweep in which nothing was done.
        let mut after = self.sweep_after.take();
        let mut progress = false;
        let mut held: Vec<Op> = Vec::new();
        let mut swept = false;
        for _ in 0..self.cfg.max_rounds {
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
        let input = self.db.read(|s| view::build_input(s, dirty, os))?;
        let plan = reconcile(&input, opts);
        self.db
            .write(|s| plan.clean.iter().try_for_each(|n| s.clear_dirty(*n)))?;
        report.conflicts.extend(plan.conflicts);
        if plan.ops.is_empty() {
            return Ok(plan.held);
        }
        let exec = Executor::new(&mut self.db, self.remote.as_ref(), &self.cfg.exec)
            .with_urgent(self.urgent.clone())
            .run(plan.ops)?;
        if let Some(why) = exec.stopped {
            return Err(EngineError::Denied(why));
        }
        report.ops_done += exec.done;
        report.skipped.extend(exec.skipped);
        report.failed.extend(exec.failed);
        if !exec.rescan.is_empty() {
            rescan_dirs(
                &mut self.db,
                &self.cfg.exec.root,
                &exec.rescan,
                &self.scan_opts,
            )?;
        }
        let more = feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        report.feed.pages += more.pages;
        report.feed.live += more.live;
        report.feed.deleted += more.deleted;
        Ok(plan.held)
    }
}
