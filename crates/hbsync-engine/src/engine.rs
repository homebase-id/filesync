//! The sync loop for one sync root.

use std::path::Path;

use hbsync_local::{CatchUp, LocalObserver, ScanOptions, ScanReport, rescan_dirs, scan};
use hbsync_platform::FsFamily;
use hbsync_remote::Remote;
use hbsync_state::{StateDb, StateError};

use crate::adopt::adopt;
use crate::exec::{ExecConfig, ExecError, Executor, recover_forks, sweep_tmp, unpark};
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
                transfer: crate::transfer::TransferConfig::default(),
                case_insensitive: crate::exec::probe_case_insensitive(root),
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
    #[error(transparent)]
    Exec(#[from] ExecError),
    #[error(transparent)]
    Scan(#[from] hbsync_local::ScanError),
    #[error("{0} panicked")]
    Panic(&'static str),
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
        let fs_family = hbsync_platform::fskind::detect(&cfg.exec.root).unwrap_or(FsFamily::Other);
        let scan_opts = ScanOptions {
            algorithm: cfg.exec.algorithm,
            fs_family,
            exclude_at_root: vec![hbsync_local::STATE_DIR.to_owned()],
        };
        Engine {
            db,
            remote,
            cfg,
            scan_opts,
            urgent: Default::default(),
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
        // Before the local catch-up reads the disk: see recover_forks.
        recover_forks(&mut self.db, &self.cfg.exec.root)?;
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
        recover_forks(&mut self.db, &self.cfg.exec.root)?;
        Ok(scan(&mut self.db, &self.cfg.exec.root, &self.scan_opts)?)
    }

    /// Pulls remote changes, then plans and executes until nothing is left to do (or the
    /// round limit is hit). `date` (`YYYY-MM-DD`) is used for conflict-copy names.
    pub fn sync_once(&mut self, date: &str) -> Result<SyncReport, EngineError> {
        let mut report = SyncReport::default();
        recover_forks(&mut self.db, &self.cfg.exec.root)?;
        unpark(&mut self.db, &self.cfg.exec.root)?;
        sweep_tmp(&mut self.db, &self.cfg.exec.state_dir())?;
        report.feed = feed::pull(&mut self.db, self.remote.as_ref(), self.cfg.feed_page_size)?;
        report.adopted = adopt(&mut self.db, &self.cfg.exec.root, self.cfg.exec.os)?;

        // The guard must judge the whole delete, even when it is planned in several batches.
        let pending_deletes = self.db.read(|s| s.pending_file_deletes())?;
        let pending_deletes = (pending_deletes.0 as usize, pending_deletes.1 as usize);
        for _ in 0..self.cfg.max_rounds {
            let dirty = self.db.read(|s| s.dirty_nodes(self.cfg.batch))?;
            if dirty.is_empty() {
                break;
            }
            report.rounds += 1;
            let os = self.cfg.exec.os;
            let input = self.db.read(|s| view::build_input(s, &dirty, os, true))?;
            let opts = Options {
                device: self.cfg.device.clone(),
                date: date.to_owned(),
                mass_delete_min: self.cfg.mass_delete_min,
                mass_delete_fraction: self.cfg.mass_delete_fraction,
                synced_files: self.db.read(|s| s.base_file_count())? as usize,
                allow_mass_delete: self.cfg.allow_mass_delete,
                pending_deletes,
            };
            let plan = reconcile(&input, &opts);
            self.db
                .write(|s| plan.clean.iter().try_for_each(|n| s.clear_dirty(*n)))?;
            report.conflicts.extend(plan.conflicts.iter().cloned());
            report.held = plan.held.clone();
            if plan.ops.is_empty() {
                break;
            }
            let exec = Executor::new(&mut self.db, self.remote.as_ref(), &self.cfg.exec)
                .with_urgent(self.urgent.clone())
                .run(&plan)?;
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
        }
        report.pending = self.db.read(|s| s.dirty_count())?;
        Ok(report)
    }
}
