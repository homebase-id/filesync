//! Executing a plan against the disk, the state DB and the remote.
//!
//! Every operation is guarded and idempotent, as the simulator requires:
//! - a local item is only touched if it still matches its `local` row (the fingerprint
//!   guard); folders are compared by kind and file id;
//! - nothing local is ever overwritten by surprise: new names use no-replace renames, content
//!   swaps check the fingerprint first, and deletes move items to `.hbsync/trash`;
//! - a remote write carries the version the plan saw;
//! - a target name held by a node that is about to leave is freed by **parking** that node
//!   under a temporary name, recorded in the DB until it is moved on or back;
//! - an operation blocked only by the order of the plan is retried later in the same run.
//!
//! Each operation's row updates (`local`, mirror) and the **settling** of its node (the base
//! takes each attribute both sides now agree on) are one transaction: the base never falls
//! behind what was done on disk or remotely.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_fileio::{FileIoError, HashAlgorithm};
use hbsync_namemap::{to_local, to_remote};
use hbsync_platform::name::{NameIssue, name_key};
use hbsync_platform::{EntryKind, IoClass, Os, RelPath, classify, ops};
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{
    BaseRow, LocalRow, MirrorFile, MirrorRow, RemovedRow, StateDb, StateError, Store,
};
use hbsync_types::activity::Synced;
use hbsync_types::schema::{NodeContent, Removal};
use hbsync_types::{ContentState, NodeId, NodeKind, RemoteRef};

use crate::activity::{self, Counters};
use crate::reconcile::{Agreement, Op, Requirement};
use crate::transfer::{Download, Job, JobError, JobResult, TransferConfig, run_jobs_while};
use crate::{feed, view};

/// Where the executor works.
#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub root: PathBuf,
    pub os: Os,
    /// The hash this client writes and keeps in its `local` rows.
    pub algorithm: HashAlgorithm,
    /// Attempts per operation within one run (ordering blocks and transient errors).
    pub attempts: u8,
    /// The pause after a transient remote error, doubled for each one in a row. The whole run
    /// waits: the next request would most likely fail the same way.
    pub retry_pause: Duration,
    pub transfer: TransferConfig,
    /// Names that differ only in case (or Unicode normalization) are the same file here
    /// (Windows, default macOS). Probe with [`probe_case_insensitive`].
    pub case_insensitive: bool,
    /// What this device never syncs (sync semantics §10).
    pub filter: std::sync::Arc<hbsync_local::Filter>,
    /// Tests only: stop the run at this point, as if the process died there.
    #[doc(hidden)]
    pub crash_at: Option<CrashPoint>,
}

/// Places where a test can make the executor stop as if it crashed.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// After parking an occupant on disk, before its row follows.
    AfterParkRename,
    /// After a conflict-copy rename on disk, before the rows follow.
    AfterForkRename,
}

/// Probes the volume of the sync root, inside the client's own state folder so nothing shows
/// up in the user's tree. Falls back to the OS default.
pub fn probe_case_insensitive(root: &Path) -> bool {
    let dir = root.join(hbsync_local::STATE_DIR);
    let _ = fs::create_dir_all(&dir);
    match hbsync_platform::fskind::probe_case_sensitive(&dir) {
        Ok(sensitive) => !sensitive,
        Err(_) => cfg!(any(windows, target_os = "macos")),
    }
}

impl ExecConfig {
    pub fn state_dir(&self) -> PathBuf {
        self.root.join(hbsync_local::STATE_DIR)
    }
}

/// What a run did.
#[derive(Debug, Default)]
pub struct ExecReport {
    pub done: usize,
    /// Dropped because the world changed since planning; the next plan decides again.
    pub skipped: Vec<(Op, String)>,
    /// Gave up after retries (transient errors, locked files, ordering).
    pub failed: Vec<(Op, String)>,
    /// Local folders whose contents differ from the DB: rescan them before planning again.
    pub rescan: Vec<RelPath>,
    /// Why the run stopped early, leaving the rest dirty for a later sync.
    pub stopped: Option<Stopped>,
    /// What was done, for people (one entry per file or item).
    pub synced: Vec<Synced>,
}

/// Why a run stopped before its plan was done.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Stopped {
    /// The remote refused access: nothing else can succeed until the user authorizes again.
    #[error("access denied: {0}")]
    Denied(String),
    /// Transient remote errors kept coming: the network or the server is down.
    #[error("remote unreachable: {0}")]
    Unreachable(String),
    /// The caller asked the run to stop (pause, shutdown).
    #[error("stopped on request")]
    Requested,
}

/// Transient remote errors in a row after which a run gives up (with the pauses between them
/// doubling from `retry_pause`, 0.5 s by default: about 15 s in all).
const UNREACHABLE_AFTER: u32 = 6;

#[derive(Debug)]
enum Outcome {
    /// Done; the node still needs settling.
    Done,
    /// Done, and settled in the operation's own transaction.
    Settled,
    Retry(String),
    /// A transient remote error: retry after a pause.
    Backoff(String),
    Skip(String),
    /// The remote refused access: stop the run.
    Stop(String),
}

/// A DB error inside an operation is retried like any transient failure.
impl From<StateError> for Outcome {
    fn from(e: StateError) -> Self {
        Outcome::Retry(e.to_string())
    }
}

/// An operation's result. `Err` ends it early: a guard that stopped it, or a DB error.
type Step<T = Outcome> = Result<T, Outcome>;

pub struct Executor<'a> {
    db: &'a mut StateDb,
    remote: &'a dyn Remote,
    cfg: &'a ExecConfig,
    queue: VecDeque<(Op, u8)>,
    /// The nodes with a `Forget` still to run in this run.
    forgetting: HashSet<NodeId>,
    report: ExecReport,
    urgent: HashSet<NodeId>,
    /// Set when a test's crash point was reached: the run stops.
    crashed: bool,
    /// Transient remote errors in a row, and until when the run waits because of them.
    streak: u32,
    resume_at: Option<Instant>,
    /// Set by the caller to stop the run between operations.
    stop: Option<Arc<AtomicBool>>,
    progress: Arc<Counters>,
    /// The transfers already in `progress`'s total.
    counted: HashSet<NodeId>,
}

/// A transfer whose guards passed, waiting for a worker.
struct Prepared {
    op: Op,
    attempts: u8,
    job: Job,
    ctx: Ctx,
}

/// What the commit of a transfer needs to know.
enum Ctx {
    Download {
        node: NodeId,
        target: RelPath,
        current: Option<LocalRow>,
        /// The node's, given to the file once in place.
        created_ms: Option<i64>,
    },
    Upload {
        node: NodeId,
        parent: NodeId,
        kind: NodeKind,
        content: NodeContent,
        hash: hbsync_types::ContentHash,
    },
}

/// Operations that move file bytes and run on the worker pool.
fn is_transfer(op: &Op) -> bool {
    matches!(
        op,
        Op::LocalDownload { .. }
            | Op::RemoteCreate {
                kind: NodeKind::File,
                ..
            }
            | Op::RemoteUpdate { upload: true, .. }
    )
}

impl std::fmt::Debug for Executor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executor")
            .field("queued", &self.queue.len())
            .finish()
    }
}

impl<'a> Executor<'a> {
    pub fn new(db: &'a mut StateDb, remote: &'a dyn Remote, cfg: &'a ExecConfig) -> Self {
        Executor {
            db,
            remote,
            cfg,
            queue: VecDeque::new(),
            forgetting: HashSet::new(),
            report: ExecReport::default(),
            urgent: HashSet::new(),
            crashed: false,
            streak: 0,
            resume_at: None,
            stop: None,
            progress: Arc::default(),
            counted: HashSet::new(),
        }
    }

    /// Stops the run between operations once `stop` is set: each operation is done or not
    /// started, as at a crash, and the rest stays dirty. Transfers in flight finish first.
    pub fn with_stop(mut self, stop: Arc<AtomicBool>) -> Self {
        self.stop = Some(stop);
        self
    }

    /// Whether the run should stop now: on request, or after a denied or unreachable remote.
    fn should_stop(&mut self) -> bool {
        if self.report.stopped.is_none()
            && self
                .stop
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
        {
            self.report.stopped = Some(Stopped::Requested);
        }
        self.report.stopped.is_some()
    }

    /// Counts what the run does (and, for transfers, the bytes) into `progress`.
    pub fn with_progress(mut self, progress: Arc<Counters>) -> Self {
        self.progress = progress;
        self
    }

    /// Marks nodes whose transfers go first (files the user is waiting for).
    pub fn with_urgent(mut self, urgent: HashSet<NodeId>) -> Self {
        self.urgent = urgent;
        self
    }

    /// Runs a plan's operations, in order.
    pub fn run(mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        self.forgetting = ops
            .iter()
            .filter(|op| matches!(op, Op::Forget { .. }))
            .map(Op::node)
            .collect();
        let visible = ops.iter().filter(|op| activity::kind_of(op).is_some());
        self.progress
            .files_total
            .fetch_add(visible.count() as u64, Ordering::Relaxed);
        self.count_bytes(&ops)?;
        self.queue = ops.into_iter().map(|op| (op, 0)).collect();
        let mut batch: Vec<Prepared> = Vec::new();
        loop {
            if self.should_stop() {
                break;
            }
            let Some((op, attempts)) = self.queue.pop_front() else {
                if batch.is_empty() {
                    break;
                }
                self.flush(&mut batch)?;
                continue;
            };
            if let Err(wait) = self.ready(&op) {
                self.handle(op, attempts, wait, None)?;
                continue;
            }
            if is_transfer(&op) {
                // Transfers are collected and run together on the worker pool.
                match self.prepare(&op) {
                    Ok((job, ctx)) => batch.push(Prepared {
                        op,
                        attempts,
                        job,
                        ctx,
                    }),
                    Err(outcome) => self.handle(op, attempts, outcome, None)?,
                }
                continue;
            }
            if !batch.is_empty() {
                self.flush(&mut batch)?;
                if self.should_stop() {
                    break;
                }
            }
            if let Op::Settle { node } = op {
                self.settle_run(node)?;
                continue;
            }
            if op.touches_remote() {
                self.wait_out_backoff();
            }
            // What a delete removes is described before its rows go.
            let gone = match activity::kind_of(&op) {
                Some(kind) if kind.removes() => {
                    self.db.read(|s| activity::path_of(s, op.node()))?
                }
                _ => None,
            };
            let outcome = self.execute(&op);
            self.handle(op, attempts, outcome, gone)?;
            if self.crashed {
                return Err(StateError::Conflict("crash point reached".into()));
            }
        }
        Ok(self.report)
    }

    /// Settles `first` and the run of `Settle` ops after it (the plan puts them last) in one
    /// transaction: they change nothing on disk or remotely, so a crash only loses settling
    /// the next plan asks for again.
    fn settle_run(&mut self, first: NodeId) -> Result<(), StateError> {
        const CHUNK: usize = 1000;
        let mut nodes = vec![first];
        while nodes.len() < CHUNK
            && let Some((Op::Settle { node }, _)) = self.queue.front()
        {
            nodes.push(*node);
            self.queue.pop_front();
        }
        let os = self.cfg.os;
        self.db
            .write(|s| nodes.iter().try_for_each(|n| settle(s, *n, os, false)))?;
        self.report.done += nodes.len();
        Ok(())
    }

    /// Writes an operation's DB changes and settles `nodes` (with whether the operation made
    /// their bytes equal) in the same transaction: the base can't fall behind what was done,
    /// and it costs one commit.
    fn finish(
        &mut self,
        nodes: &[(NodeId, bool)],
        f: impl FnOnce(&Store<'_>) -> Result<(), StateError>,
    ) -> Step {
        let os = self.cfg.os;
        self.db.write(|s| {
            f(s)?;
            nodes
                .iter()
                .try_for_each(|(node, same_bytes)| settle(s, *node, os, *same_bytes))
        })?;
        Ok(Outcome::Settled)
    }

    /// Whether the test crash point `at` is reached; the run then stops.
    fn crash_here(&mut self, at: CrashPoint) -> bool {
        self.crashed |= self.cfg.crash_at == Some(at);
        self.crashed
    }

    /// The server-side order guards ([`Op::requires`]), checked against the mirror.
    fn ready(&mut self, op: &Op) -> Step<()> {
        let met = match op.requires() {
            None => true,
            Some(Requirement::RemoteFolder(folder)) => {
                self.db.read(|s| s.get_mirror(folder))?.is_some()
            }
            Some(Requirement::NoRemoteChildren(node)) => {
                self.db.read(|s| s.mirror_children(node))?.is_empty()
            }
        };
        if met {
            Ok(())
        } else {
            Err(Outcome::Retry(format!("waiting for {:?}", op.requires())))
        }
    }

    /// `gone`: the path of what the operation removed, taken before it ran.
    fn handle(
        &mut self,
        op: Op,
        attempts: u8,
        outcome: Outcome,
        gone: Option<String>,
    ) -> Result<(), StateError> {
        if matches!(outcome, Outcome::Done | Outcome::Settled | Outcome::Skip(_)) {
            self.streak = 0; // the remote answered (or wasn't needed)
        }
        if matches!(op, Op::Forget { .. })
            && !matches!(outcome, Outcome::Retry(_) | Outcome::Backoff(_))
        {
            self.forgetting.remove(&op.node());
        }
        match outcome {
            Outcome::Done => {
                self.record(&op, gone)?;
                self.report.done += 1;
                let os = self.cfg.os;
                let same_bytes = is_transfer(&op);
                self.db.write(|s| settle(s, op.node(), os, same_bytes))?;
            }
            Outcome::Settled => {
                self.record(&op, gone)?;
                self.report.done += 1;
            }
            Outcome::Backoff(why) => {
                self.streak += 1;
                if self.streak >= UNREACHABLE_AFTER {
                    self.report.stopped.get_or_insert(Stopped::Unreachable(why));
                    return Ok(());
                }
                // One pause for the whole run, longer for each failure in a row.
                let pause = self.cfg.retry_pause * (1u32 << (self.streak - 1));
                self.resume_at = Some(Instant::now() + pause);
                self.retry_or_fail(op, attempts, why);
            }
            Outcome::Retry(why) => self.retry_or_fail(op, attempts, why),
            Outcome::Skip(why) => {
                let node = op.node();
                self.db.write(|s| s.mark_dirty(node))?;
                self.report.skipped.push((op, why));
            }
            Outcome::Stop(why) => {
                let node = op.node();
                self.db.write(|s| s.mark_dirty(node))?;
                self.report.stopped.get_or_insert(Stopped::Denied(why));
            }
        }
        Ok(())
    }

    /// Adds what `op` did to the report, for people: transfers, moves and deletes. `gone`:
    /// the path of an item it removed, taken before it ran (its rows may be gone now).
    fn record(&mut self, op: &Op, gone: Option<String>) -> Result<(), StateError> {
        let Some(kind) = activity::kind_of(op) else {
            return Ok(());
        };
        self.progress.files_done.fetch_add(1, Ordering::Relaxed);
        let node = op.node();
        let (path, bytes) = match gone {
            Some(path) => (Some(path), 0),
            None => self.db.read(|s| {
                Ok(match s.local_by_node(node)? {
                    Some(row) if kind.moves_bytes() => {
                        (Some(row.path.as_str().to_owned()), row.fingerprint.size)
                    }
                    Some(row) => (Some(row.path.as_str().to_owned()), 0),
                    None => (activity::path_of(s, node)?, 0),
                })
            })?,
        };
        if let Some(path) = path {
            self.report.synced.push(Synced {
                at: hbsync_types::unix_now(),
                kind,
                path,
                files: 1,
                bytes,
            });
        }
        Ok(())
    }

    fn retry_or_fail(&mut self, op: Op, attempts: u8, why: String) {
        if attempts + 1 < self.cfg.attempts {
            self.queue.push_back((op, attempts + 1));
        } else {
            self.forgetting.remove(&op.node());
            self.report.failed.push((op, why));
        }
    }

    /// Waits out the pause after a transient remote error, if there is one. Only remote work
    /// waits; local operations go on meanwhile.
    fn wait_out_backoff(&mut self) {
        if let Some(at) = self.resume_at.take() {
            std::thread::sleep(at.saturating_duration_since(Instant::now()));
        }
    }

    /// Adds the bytes of every transfer in the plan to `progress`'s total before any starts, so
    /// the total doesn't grow batch by batch. One the guards skip later stays in it.
    fn count_bytes(&mut self, ops: &[Op]) -> Result<(), StateError> {
        let sizes = self.db.read(|s| {
            let mut sizes = Vec::new();
            for op in ops.iter().filter(|op| is_transfer(op)) {
                let node = op.node();
                let size = match op {
                    Op::LocalDownload { .. } => {
                        s.get_mirror(node)?.and_then(|m| m.file).map(|f| f.size)
                    }
                    _ => s.local_by_node(node)?.map(|row| row.fingerprint.size),
                };
                sizes.extend(size.map(|size| (node, size)));
            }
            Ok(sizes)
        })?;
        let total: u64 = sizes
            .into_iter()
            .filter(|(node, _)| self.counted.insert(*node))
            .map(|(_, size)| size)
            .sum();
        self.progress
            .bytes_total
            .fetch_add(total, Ordering::Relaxed);
        Ok(())
    }

    /// Runs the collected transfers in parallel and commits each as it finishes.
    fn flush(&mut self, batch: &mut Vec<Prepared>) -> Result<(), StateError> {
        self.wait_out_backoff();
        let mut pending: HashMap<NodeId, Prepared> = HashMap::new();
        let mut jobs = Vec::new();
        for p in batch.drain(..) {
            jobs.push((self.urgent.contains(&p.job.node()), p.job.clone()));
            pending.insert(p.job.node(), p);
        }
        // Counted up front by `count_bytes`; a retried transfer's bytes count once.
        let progress = Arc::clone(&self.progress);
        let new_bytes: u64 = jobs
            .iter()
            .filter(|(_, j)| self.counted.insert(j.node()))
            .map(|(_, j)| j.size())
            .sum();
        progress.bytes_total.fetch_add(new_bytes, Ordering::Relaxed);
        // Commit each transfer as soon as it finishes: small files don't wait for a large
        // one, and a crash loses only transfers still in flight.
        let remote = self.remote;
        let transfer = self.cfg.transfer;
        let mut first_error = None;
        // A DB error or a stopped run (requested, denied, unreachable) starts no further
        // transfers; the nodes stay dirty for the next sync.
        run_jobs_while(
            remote,
            transfer,
            Some(&progress.bytes_done),
            jobs,
            |job, result| {
                if first_error.is_some() {
                    return false;
                }
                if let Some(p) = pending.remove(&job.node()) {
                    let outcome = self.commit(p.ctx, result).unwrap_or_else(|o| o);
                    if let Err(e) = self.handle(p.op, p.attempts, outcome, None) {
                        first_error = Some(e);
                    }
                }
                first_error.is_none() && !self.should_stop()
            },
        );
        if let Some(e) = first_error {
            return Err(e);
        }
        if self.crashed {
            return Err(StateError::Conflict("crash point reached".into()));
        }
        Ok(())
    }

    fn execute(&mut self, op: &Op) -> Outcome {
        let result = match op {
            Op::LocalFork {
                node,
                new_node,
                name,
            } => self.fork(*node, *new_node, name),
            Op::LocalMkdir { node, parent, name } => self.mkdir(*node, *parent, name),
            Op::LocalMove { node, parent, name } => self.local_move(*node, *parent, name),
            Op::LocalDownload { .. } => unreachable!("transfers run through flush"),
            Op::LocalTrash { node, subtree } => self.trash(*node, subtree, false),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind,
            } => self.create_folder(*node, *parent, name, *kind),
            Op::RemoteUpdate {
                node,
                file_id,
                expected,
                parent,
                name,
                upload,
            } => {
                debug_assert!(!upload, "uploads run through flush");
                self.rename_remote(*node, *file_id, *expected, *parent, name)
            }
            Op::RemoteDelete {
                node,
                file_id,
                expected,
            } => self.remove(*node, *file_id, *expected),
            Op::Settle { .. } => Ok(Outcome::Done),
            Op::LocalDetach { node, new_node } => self.detach(*node, *new_node),
            Op::Forget { node, subtree } if subtree.is_empty() => self.forget_base(*node),
            Op::Forget { node, subtree } => self.trash(*node, subtree, true),
        };
        result.unwrap_or_else(|o| o)
    }

    // ---- paths ----

    fn abs(&self, path: &RelPath) -> PathBuf {
        path.to_native(&self.cfg.root)
    }

    /// The local path where (parent, name) lives. Retried while the parent isn't local yet;
    /// skipped if the name can't be spelled on this OS (it stays remote-only and is reported).
    fn target_path(&mut self, parent: NodeId, name: &str) -> Step<RelPath> {
        let parent_path = if parent.is_root() {
            RelPath::root()
        } else {
            match self.db.read(|s| s.local_by_node(parent))? {
                Some(row) if row.kind == NodeKind::Folder => row.path,
                _ => return Err(Outcome::Retry("parent not local yet".into())),
            }
        };
        let unspellable = |e: NameIssue| Outcome::Skip(format!("name can't be used here: {e}"));
        let local = to_local(name, self.cfg.os).map_err(unspellable)?;
        parent_path.join(&local.name).map_err(unspellable)
    }

    /// Whether the item on disk still matches its row.
    fn on_disk_unchanged(&self, row: &LocalRow) -> bool {
        hbsync_platform::stat(&self.abs(&row.path)).is_ok_and(|meta| matches_row(row, &meta))
    }

    /// Whether everything on disk at `top` is what `rows` recorded (its subtree in the DB, `top`
    /// included), unchanged, apart from filtered items (§10): junk may go along, ignored items
    /// must stay, and the folders they are in are returned. `None` if anything else is there or
    /// changed. One walk, compared with the metadata the listing returns.
    fn subtree_check(&self, top: &LocalRow, rows: &[LocalRow]) -> Option<Vec<RelPath>> {
        if !self.on_disk_unchanged(top) {
            return None;
        }
        let mut expected: HashMap<&RelPath, &LocalRow> = rows
            .iter()
            .filter(|r| r.path != top.path)
            .map(|r| (&r.path, r))
            .collect();
        let mut holding = Vec::new();
        if top.kind == NodeKind::Folder {
            let walk =
                hbsync_local::Walker::at(&self.cfg.root, top.path.clone(), &self.cfg.filter, None);
            for event in walk {
                match event {
                    hbsync_local::WalkEvent::Entry { path, meta } => {
                        if !expected
                            .remove(&path)
                            .is_some_and(|r| matches_row(r, &meta))
                        {
                            return None;
                        }
                    }
                    hbsync_local::WalkEvent::Skipped {
                        reason: hbsync_local::SkipReason::Junk,
                        ..
                    } => {}
                    hbsync_local::WalkEvent::Skipped {
                        path,
                        reason: hbsync_local::SkipReason::Ignored,
                    } => {
                        // Its own name may be one we can't sync; its folder's we walked.
                        let folder = path.parent()?;
                        holding.push(hbsync_local::journal::relpath_prefix(folder));
                    }
                    // Anything else we never sync (a link, a bad name) would be trashed silently.
                    _ => return None,
                }
            }
        }
        expected.is_empty().then_some(holding)
    }

    /// See [`occupant`].
    fn occupant(&mut self, target: &RelPath) -> Result<Option<LocalRow>, StateError> {
        occupant(self.db, self.cfg.case_insensitive, target)
    }

    /// Frees `target` if a node that the plan is about to move away holds it: that node is
    /// parked under a temporary name (recorded in the DB). Returns `false` if it stays taken.
    /// The mover itself may hold it under another case (a case-only rename).
    fn free_target(&mut self, target: &RelPath, mover: Option<NodeId>) -> Result<bool, StateError> {
        let on_disk = fs::symlink_metadata(self.abs(target)).is_ok();
        let Some(occupant) = self.occupant(target)? else {
            return Ok(!on_disk);
        };
        if Some(occupant.node) == mover {
            return Ok(true);
        }
        let leaving = self
            .queue
            .iter()
            .any(|(op, _)| op.vacates_local_name() == Some(occupant.node));
        if !leaving || !self.on_disk_unchanged(&occupant) {
            return Ok(false);
        }
        let parent = occupant.path.parent().unwrap_or_default();
        let Ok(parked) = parent.join(&parked_name(occupant.node)) else {
            return Ok(false);
        };
        // Write-ahead: after a crash between the rename and the row update, the scan finds the
        // item under its temporary name, and this record maps it back to its real place (a
        // record counts only while the item carries that name), so the temporary name can't
        // reach the remote as a rename.
        self.db
            .write(|s| s.put_parked(occupant.node, &occupant.path))?;
        if ops::rename_noreplace(&self.abs(&occupant.path), &self.abs(&parked)).is_err() {
            self.db.write(|s| s.delete_parked(occupant.node))?;
            return Ok(false);
        }
        if self.crash_here(CrashPoint::AfterParkRename) {
            return Ok(false);
        }
        let refreshed = self.renamed(&occupant, &parked);
        self.db.write(|s| {
            s.move_local_subtree(&occupant.path, &parked)?;
            refreshed.as_ref().map_or(Ok(()), |r| s.put_local(r))
        })?;
        Ok(true)
    }

    // ---- local operations ----

    fn fork(&mut self, node: NodeId, new_node: NodeId, name: &str) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let parent = row.path.parent().unwrap_or_default();
        let Ok(local) = to_local(name, self.cfg.os) else {
            return Ok(Outcome::Skip("bad name".into()));
        };
        let Ok(target) = parent.join(&local.name) else {
            return Ok(Outcome::Skip("bad name".into()));
        };
        if self.db.read(|s| s.local_by_node(new_node))?.is_some() {
            return Ok(Outcome::Skip("conflict copy id already in use".into()));
        }
        // The name may be held by something that stays: the copy can exist remotely already,
        // made by another device with the same bytes, named to fit its folder there. Then a
        // numbered name here; a later plan moves it where it belongs.
        let target = if self.free_target(&target, None)? {
            target
        } else {
            match numbered_place(self.db, self.cfg, &target, false)? {
                Some(t) => t,
                None => return Ok(Outcome::Retry("conflict name taken".into())),
            }
        };
        // Write-ahead: if we crash after the rename, recovery completes the fork instead of a
        // scan mistaking it for the user renaming the original.
        self.db.write(|s| s.put_fork(node, new_node, &target))?;
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            self.db.write(|s| s.delete_fork(node))?;
            return Ok(io_outcome(e));
        }
        let moved = LocalRow {
            node: new_node,
            ..self.renamed(&row, &target).unwrap_or_else(|| LocalRow {
                path: target.clone(),
                ..row.clone()
            })
        };
        if self.crash_here(CrashPoint::AfterForkRename) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        self.finish(&[(new_node, false), (node, false)], |s| {
            complete_fork(s, node, &moved)
        })
    }

    fn mkdir(&mut self, node: NodeId, parent: NodeId, name: &str) -> Step {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::Skip("already local".into()));
        }
        let target = self.target_path(parent, name)?;
        let abs = self.abs(&target);
        let created_ms = if ops::CAN_SET_CREATED {
            self.db
                .read(|s| s.get_mirror(node))?
                .as_ref()
                .and_then(feed::created_ms)
        } else {
            None
        };
        // Idempotent: a folder left by an interrupted run, unknown to the DB (under any case),
        // is adopted. One the DB tracks for another node is never taken over.
        let adopt = self.occupant(&target)?.is_none()
            && fs::symlink_metadata(&abs).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink());
        if !adopt {
            if !self.free_target(&target, None)? {
                return Ok(Outcome::Retry("name taken".into()));
            }
            if let Err(e) = fs::create_dir(&abs) {
                return Ok(io_outcome(e));
            }
        }
        // Only shown, never synced on: a file system that won't take it is no failure.
        if let Some(ct) = created_ms {
            let _ = ops::set_created(&abs, ct);
        }
        let meta = match hbsync_platform::stat(&abs) {
            Ok(m) => m,
            Err(e) => return Ok(io_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::Folder,
                fingerprint: meta.fingerprint(),
                executable: None,
                hash: None,
            })
        })
    }

    fn local_move(&mut self, node: NodeId, parent: NodeId, name: &str) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        if target == row.path {
            self.db.write(|s| s.delete_parked(node))?;
            return Ok(Outcome::Done);
        }
        if target.starts_with(&row.path) {
            return Ok(Outcome::Retry(
                "would move a folder into itself for now".into(),
            ));
        }
        if !self.free_target(&target, Some(node))? {
            return Ok(Outcome::Retry("name taken".into()));
        }
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            return Ok(io_outcome(e));
        }
        let refreshed = self.renamed(&row, &target);
        self.finish(&[(node, false)], |s| {
            s.move_local_subtree(&row.path, &target)?;
            s.delete_parked(node)?;
            refreshed.as_ref().map_or(Ok(()), |r| s.put_local(r))
        })
    }

    /// See [`renamed_row`].
    fn renamed(&self, old: &LocalRow, path: &RelPath) -> Option<LocalRow> {
        renamed_row(&self.cfg.root, old, path)
    }

    // ---- transfers: prepare (guards) and commit (placement, DB) around the worker pool ----

    fn prepare(&mut self, op: &Op) -> Step<(Job, Ctx)> {
        match op {
            Op::LocalDownload {
                node,
                parent,
                name,
                replace,
            } => self.prepare_download(*node, *parent, name, *replace),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind,
            } => self.prepare_upload(*node, None, *parent, name, *kind),
            Op::RemoteUpdate {
                node,
                file_id,
                expected,
                parent,
                name,
                ..
            } => self.prepare_upload(
                *node,
                Some((*file_id, *expected)),
                *parent,
                name,
                NodeKind::File,
            ),
            _ => Err(Outcome::Skip("not a transfer".into())),
        }
    }

    fn prepare_download(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        replace: bool,
    ) -> Step<(Job, Ctx)> {
        let (mirror, current) = self
            .db
            .read(|s| Ok((s.get_mirror(node)?, s.local_by_node(node)?)))?;
        let Some(mirror) = mirror else {
            return Err(Outcome::Skip("gone remotely".into()));
        };
        let Some(file) = mirror.file else {
            return Err(Outcome::Skip("not a file".into()));
        };
        let expected = file.hash;
        if replace != current.is_some() {
            return Err(Outcome::Skip("local presence changed".into()));
        }
        if let Some(row) = &current
            && !self.on_disk_unchanged(row)
        {
            return Err(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        let created_ms = feed::created_ms(&mirror);
        let job = Job::Download(Download {
            node,
            file_id: mirror.file_id,
            expected,
            size: file.size,
            mtime_ms: file.mtime_ms,
            executable: (self.cfg.os != Os::Windows).then_some(file.executable),
            tmp_dir: self.cfg.state_dir().join(TMP_DIR),
            key: download_key(node, &expected),
            local_algorithm: self.cfg.algorithm,
        });
        Ok((
            job,
            Ctx::Download {
                node,
                target,
                current,
                created_ms,
            },
        ))
    }

    fn prepare_upload(
        &mut self,
        node: NodeId,
        existing: Option<(hbsync_types::RemoteFileId, hbsync_types::VersionTag)>,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
    ) -> Step<(Job, Ctx)> {
        let (row, mirror, base) = self.db.read(|s| {
            Ok((
                s.local_by_node(node)?,
                s.get_mirror(node)?,
                s.get_base(node)?,
            ))
        })?;
        let Some(row) = row else {
            return Err(Outcome::Skip("gone locally".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Err(rescan(&mut self.report, &row.path));
        }
        let Some(file) = view::local_content(&row, base.as_ref()) else {
            return Err(Outcome::Skip("not hashed yet".into()));
        };
        let mut content = NodeContent::file(name, file.mtime_ms, file.size, file.executable);
        let (hash, size) = (file.hash, file.size);
        let path = self.abs(&row.path);
        let removed = match existing {
            None => self.db.read(|s| s.removed_by_node(node))?,
            Some(_) => None,
        };
        // Recently deleted holds a version this device never had (edited elsewhere, then
        // deleted): restoring over it would lose it. It stays there, and this version goes up
        // as a new item.
        if let Some(r) = &removed
            && r.row.file.map(|f| f.hash)
                != base.as_ref().and_then(|b| b.state.content).map(|c| c.hash)
        {
            return Err(self.detach(node, NodeId::new()).unwrap_or_else(|o| o));
        }
        let target = match existing {
            // In Recently deleted: restored with these bytes (an edit wins over the delete).
            None if let Some(r) = &removed => {
                content.unknown = feed::unknown_keys(&r.row);
                Some(r.row.remote_ref())
            }
            // A new node: the file's own creation time, kept from then on.
            None => {
                content.set_created_ms(ops::created_ms(&path));
                None
            }
            Some((file_id, version)) => {
                let Some(mirror) = &mirror else {
                    return Err(Outcome::Skip("gone remotely".into()));
                };
                content.unknown = feed::unknown_keys(mirror);
                // A node from before creation times, or from a client that sends none.
                if content.created_ms().is_none() {
                    content.set_created_ms(ops::created_ms(&path));
                }
                Some(RemoteRef { file_id, version })
            }
        };
        let job = Job::Upload {
            meta: NodeMeta {
                node,
                parent,
                kind,
                content: content.clone(),
            },
            target,
            path,
            hash,
            size,
        };
        Ok((
            job,
            Ctx::Upload {
                node,
                parent,
                kind,
                content,
                hash,
            },
        ))
    }

    fn commit(&mut self, ctx: Ctx, result: JobResult) -> Step {
        match (ctx, result) {
            (_, JobResult::Failed(e)) => Ok(match e {
                JobError::Remote(RemoteError::AlreadyExists) => Outcome::Done, // an earlier try landed
                JobError::Remote(r) => remote_outcome(r),
                // Local file errors (locked, changed mid-read, short) are worth another try.
                e => Outcome::Retry(e.to_string()),
            }),
            (
                Ctx::Download {
                    node,
                    target,
                    current,
                    created_ms,
                },
                JobResult::Downloaded { file, local_hash },
            ) => self.place_download(node, &target, current, created_ms, file, local_hash),
            (
                Ctx::Upload {
                    node,
                    parent,
                    kind,
                    content,
                    hash,
                },
                JobResult::Uploaded(written),
            ) => {
                let row = mirror_from(node, parent, kind, &content, Some(hash), written);
                // The bytes were just sent: equal even if the hashes use other algorithms.
                self.finish(&[(node, true)], |s| {
                    s.delete_removed(node)?;
                    s.put_mirror(&row)
                })
            }
            _ => Ok(Outcome::Skip("mismatched transfer result".into())),
        }
    }

    /// Puts a verified download into place: over the old version (if it's still what the plan
    /// saw) or as a new file; an old version at another path goes to the trash first.
    fn place_download(
        &mut self,
        node: NodeId,
        target: &RelPath,
        current: Option<LocalRow>,
        created_ms: Option<i64>,
        done: hbsync_fileio::CompletedFile,
        local_hash: hbsync_types::ContentHash,
    ) -> Step {
        if let Some(row) = &current
            && self.db.read(|s| s.local_by_node(node))?.as_ref() != Some(row)
        {
            done.keep_for_retry();
            return Ok(Outcome::Skip(
                "local item changed during the transfer".into(),
            ));
        }
        let placed = match &current {
            Some(row) if &row.path == target => {
                done.persist_over(&self.abs(target), &row.fingerprint, created_ms)
            }
            Some(row) => {
                if !self.on_disk_unchanged(row) || !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("old version changed or name taken".into()));
                }
                match self.move_to_trash(&row.path, node) {
                    Ok(()) => done.persist_new(&self.abs(target), created_ms),
                    Err(e) => {
                        done.keep_for_retry();
                        return Ok(io_outcome(e));
                    }
                }
            }
            None => {
                if !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("name taken".into()));
                }
                done.persist_new(&self.abs(target), created_ms)
            }
        };
        let meta = match placed {
            Ok(m) => m,
            Err(FileIoError::TargetChanged) => return Ok(rescan(&mut self.report, target)),
            Err(FileIoError::Io(e)) => return Ok(io_outcome(e)),
            Err(e) => return Ok(Outcome::Retry(e.to_string())),
        };
        let target = target.clone();
        // The bytes were just fetched: equal even if the hashes use other algorithms.
        self.finish(&[(node, true)], |s| {
            if let Some(row) = &current {
                s.delete_local(row.node)?;
            }
            s.delete_parked(node)?;
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::File,
                fingerprint: meta.fingerprint(),
                executable: meta.executable,
                hash: Some(local_hash),
            })
        })
    }

    /// Moves the local item of `node` to the trash; with `forget`, the bases of `node` and
    /// `expected` go too (§10).
    fn trash(&mut self, node: NodeId, expected: &[NodeId], forget: bool) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            if forget {
                return self.forget_base(node);
            }
            return Ok(Outcome::Done); // already gone
        };
        let rows = self.db.read(|s| s.local_subtree(&row.path))?;
        let mut nodes: Vec<NodeId> = rows.iter().map(|r| r.node).collect();
        nodes.sort();
        if nodes != expected {
            return Ok(Outcome::Skip(
                "the folder holds something the plan didn't expect".into(),
            ));
        }
        // Everything on disk must be known and unchanged; otherwise rescan first.
        let Some(holding) = self.subtree_check(&row, &rows) else {
            return Ok(rescan(&mut self.report, &row.path));
        };
        if holding.is_empty() {
            // What Recently deleted holds on Homebase needs no second copy here.
            let held = !forget
                && self.db.read(|s| {
                    rows.iter().try_fold(true, |all, r| {
                        Ok(all && s.removed_by_node(r.node)?.is_some())
                    })
                })?;
            // A delete that fails (a read-only file on Windows) falls back to the trash.
            let gone = if held {
                remove_all(&self.abs(&row.path)).or_else(|_| self.move_to_trash(&row.path, node))
            } else {
                self.move_to_trash(&row.path, node)
            };
            if let Err(e) = gone {
                return Ok(io_outcome(e));
            }
        } else {
            // Ignored items are kept here, and so are the folders they are in (§10): the rest
            // goes, and those folders are released, for the scan to find as new ones.
            let holders: HashSet<RelPath> = holding
                .into_iter()
                .flat_map(|folder| {
                    std::iter::successors(Some(folder), |p| p.parent())
                        .take_while(|p| p.starts_with(&row.path))
                })
                .collect();
            for r in &rows {
                let in_holder = r.path.parent().is_some_and(|p| holders.contains(&p));
                if in_holder
                    && !holders.contains(&r.path)
                    && let Err(e) = self.move_to_trash(&r.path, r.node)
                {
                    self.report.rescan.push(row.path.clone());
                    return Ok(io_outcome(e));
                }
            }
            // A released folder can't keep a temporary name (it carries the old id): it gets
            // a real one. Deepest first, so the paths above stay valid.
            let mut parked: Vec<(&LocalRow, RelPath)> = Vec::new();
            for r in rows.iter().filter(|r| holders.contains(&r.path)) {
                if let Some(original) = self.db.read(|s| view::parked_place(s, r))? {
                    parked.push((r, original));
                }
            }
            parked.sort_by_key(|(r, _)| std::cmp::Reverse(r.path.depth()));
            for (r, original) in parked {
                if let Some(to) = release_place(self.db, self.cfg, &original, true)? {
                    // Best effort: if it fails, the item keeps its name and the scan finds it there.
                    let _ = ops::rename_noreplace(&self.abs(&r.path), &self.abs(&to));
                }
            }
            self.report
                .rescan
                .push(row.path.parent().unwrap_or_default());
        }
        self.finish(&[(node, false)], |s| {
            for r in s.delete_local_subtree(&row.path)? {
                s.delete_parked(r.node)?;
                if r.node != node {
                    s.mark_dirty(r.node)?;
                }
                if forget {
                    s.delete_base(r.node)?;
                }
            }
            Ok(())
        })
    }

    /// Makes the local item of `node` the new node `new_node`, in place (§10). Everything
    /// below it is planned again: it sits in a new folder now.
    fn detach(&mut self, node: NodeId, new_node: NodeId) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(if self.db.read(|s| s.local_by_node(new_node))?.is_some() {
                Outcome::Settled // an earlier try did it
            } else {
                Outcome::Skip("gone locally".into())
            });
        };
        // A parked item's temporary name carries its old id: it gets a real one first, its
        // original if that is free. After a crash in between, the scan finds it there.
        let row = match self.db.read(|s| view::parked_place(s, &row))? {
            None => row,
            Some(original) => {
                if !self.on_disk_unchanged(&row) {
                    return Ok(rescan(&mut self.report, &row.path));
                }
                let is_dir = row.kind == NodeKind::Folder;
                let Some(to) = release_place(self.db, self.cfg, &original, is_dir)? else {
                    return Ok(Outcome::Skip("no free name to leave the parked one".into()));
                };
                match restore_parked(self.db, &self.cfg.root, &row, &to)? {
                    Ok(row) => row,
                    Err(e) => return Ok(io_outcome(e)),
                }
            }
        };
        self.finish(&[], |s| {
            let below = s.local_subtree(&row.path)?;
            s.rekey_local(node, new_node)?;
            // The old node isn't synced here any more; a base left behind would read as a
            // local delete once it is back in view.
            s.delete_base(node)?;
            s.mark_dirty(node)?;
            s.mark_dirty(new_node)?;
            below
                .iter()
                .filter(|r| r.node != node)
                .try_for_each(|r| s.mark_dirty(r.node))
        })
    }

    /// Drops the base of a node this device no longer syncs and holds nothing of (§10).
    fn forget_base(&mut self, node: NodeId) -> Step {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::Skip("still here".into()));
        }
        // The base stays a tree: a child's base may still be needed (its delete held or failed),
        // unless the child is forgotten in this run too (children go first; in a cycle of
        // bases, as a remote cycle leaves them, one must go before the others).
        let children = self.db.read(|s| s.base_children(node))?;
        if children.iter().any(|c| !self.forgetting.contains(&c.node)) {
            return Ok(Outcome::Skip("its children first".into()));
        }
        self.finish(&[], |s| s.delete_base(node).map(drop))
    }

    /// Moves a local item into `.hbsync/trash/<time>-<node>/`.
    fn move_to_trash(&self, path: &RelPath, node: NodeId) -> io::Result<()> {
        let stamp = hbsync_types::unix_now();
        let dir = self
            .cfg
            .state_dir()
            .join("trash")
            .join(format!("{stamp}-{}", node.0.simple()));
        fs::create_dir_all(&dir)?;
        let name = path.name().unwrap_or("item");
        ops::rename_noreplace(&self.abs(path), &dir.join(name))
    }

    // ---- remote operations ----

    fn create_folder(&mut self, node: NodeId, parent: NodeId, name: &str, kind: NodeKind) -> Step {
        debug_assert_eq!(kind, NodeKind::Folder, "file creates run through flush");
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone locally".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let removed = self.db.read(|s| s.removed_by_node(node))?;
        let mut content = NodeContent::folder(name);
        match &removed {
            // In Recently deleted: restored, keeping its keys (`ct`).
            Some(r) => content.unknown = feed::unknown_keys(&r.row),
            None => content.set_created_ms(ops::created_ms(&self.abs(&row.path))),
        }
        let new = NodeMeta {
            node,
            parent,
            kind,
            content: content.clone(),
        };
        let result = match &removed {
            Some(r) => self.remote.update(r.row.file_id, r.row.version, &new, None),
            None => self.remote.create(&new, None),
        };
        let written = match result {
            Ok(w) => w,
            Err(RemoteError::AlreadyExists) => return Ok(Outcome::Done),
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.delete_removed(node)?;
            s.put_mirror(&mirror_from(node, parent, kind, &content, None, written))
        })
    }

    /// A metadata-only update: move or rename, keeping the payload.
    fn rename_remote(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
        parent: NodeId,
        name: &str,
    ) -> Step {
        let Some(mirror) = self.db.read(|s| s.get_mirror(node))? else {
            return Ok(Outcome::Skip("gone remotely".into()));
        };
        let mut content = content_of(&mirror, name);
        // A node from before creation times, or from a client that sends none.
        if content.created_ms().is_none()
            && let Some(row) = self.db.read(|s| s.local_by_node(node))?
        {
            content.set_created_ms(ops::created_ms(&self.abs(&row.path)));
        }
        let meta = NodeMeta {
            node,
            parent,
            kind: mirror.kind,
            content,
        };
        let written = match self.remote.update(file_id, expected, &meta, None) {
            Ok(w) => w,
            Err(e) => return Ok(remote_outcome(e)),
        };
        let hash = mirror.file.map(|f| f.hash);
        let row = mirror_from(node, parent, mirror.kind, &meta.content, hash, written);
        self.finish(&[(node, false)], |s| s.put_mirror(&row))
    }

    /// A delete: the node goes to Recently deleted (a metadata-only update setting its
    /// removal), its bytes kept there until the purge.
    fn remove(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> Step {
        let Some(mirror) = self
            .db
            .read(|s| s.mirror_by_file(file_id))?
            .filter(|m| m.node == node)
        else {
            return Ok(Outcome::Done); // already out of the tree
        };
        let removal = Removal {
            at_ms: unix_now_ms(),
            root: self.db.read(|s| removal_root(s, node))?,
        };
        let mut content = content_of(&mirror, &mirror.name);
        content.set_removal(Some(removal));
        let meta = NodeMeta {
            node,
            parent: mirror.parent,
            kind: mirror.kind,
            content,
        };
        let removed = match self.remote.update(file_id, expected, &meta, None) {
            Ok(written) => Some(RemovedRow {
                row: MirrorRow {
                    file_id: written.file_id,
                    version: written.version,
                    ..mirror
                },
                removal,
            }),
            Err(RemoteError::NotFound) => None, // deleted for good meanwhile
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.delete_mirror(file_id)?;
            if let Some(removed) = &removed {
                s.put_removed(removed)?;
            }
            s.mark_dirty(node)
        })
    }
}

/// Records in the base whatever both sides now agree on (sync-semantics: the base only takes
/// a value when both sides hold it). `content_hint`: the operation just made the local and
/// remote bytes equal (a transfer), even if their hashes use other algorithms.
pub fn settle(s: &Store<'_>, node: NodeId, os: Os, content_hint: bool) -> Result<(), StateError> {
    let local = view::local_node(s, node, os)?;
    let mirror = s.get_mirror(node)?;
    let base = s.get_base(node)?;
    let online_only = base
        .as_ref()
        .is_some_and(|b| b.content_state == ContentState::OnlineOnly);
    let (local, mirror) = match (local, mirror) {
        (None, None) => {
            s.delete_base(node)?;
            return Ok(());
        }
        (Some(l), Some(m)) => (l, m),
        (None, Some(m)) if online_only => {
            // No local bytes by design: the base follows the remote.
            if let (Some(mut b), Some(r)) = (base, m.state()) {
                b.state = r;
                b.remote = m.remote_ref();
                s.put_base(&b)?;
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    let (Some(l), Some(r)) = (local.state.clone(), mirror.state()) else {
        return Ok(());
    };
    let agreed = Agreement::of(&l, &r, content_hint);
    let Some(state) = agreed.settle(base.as_ref().map(|b| b.state.clone()), &r) else {
        return Ok(());
    };
    let mut row = base.unwrap_or(BaseRow {
        node,
        state: r.clone(),
        remote: mirror.remote_ref(),
        content_state: ContentState::Local,
        local_name: None,
        local_hash: None,
    });
    row.state = state;
    row.remote = mirror.remote_ref();
    if agreed.place {
        // The local spelling is recorded when the name map wouldn't give it back.
        row.local_name = local
            .row
            .path
            .name()
            .filter(|ln| to_remote(ln, os).as_deref() != Ok(r.name.as_str()))
            .map(str::to_owned);
    }
    if agreed.content {
        row.local_hash = l.content.map(|c| c.hash);
    }
    s.put_base(&row)
}

fn mirror_from(
    node: NodeId,
    parent: NodeId,
    kind: NodeKind,
    content: &NodeContent,
    hash: Option<hbsync_types::ContentHash>, // `Some` for files
    written: hbsync_types::RemoteRef,
) -> MirrorRow {
    MirrorRow {
        file_id: written.file_id,
        version: written.version,
        node,
        parent,
        name: content.name.clone(),
        kind,
        file: hash.map(|hash| MirrorFile {
            hash,
            size: content.size.unwrap_or(0),
            mtime_ms: content.mtime.unwrap_or(0),
            executable: content.x,
        }),
        content_extra: feed::content_extra(content),
    }
}

/// The content of a node as `row` has it, under `name`, with the keys sync carries along.
pub(crate) fn content_of(row: &MirrorRow, name: &str) -> NodeContent {
    let mut content = match (&row.kind, &row.file) {
        (NodeKind::File, Some(f)) => NodeContent::file(name, f.mtime_ms, f.size, f.executable),
        _ => NodeContent::folder(name),
    };
    content.unknown = feed::unknown_keys(row);
    content
}

/// The node whose delete takes `node` along: its topmost synced ancestor that is gone here
/// too (a folder deleted with everything in it), or `node` itself.
fn removal_root(s: &Store<'_>, node: NodeId) -> Result<NodeId, StateError> {
    let mut root = node;
    while let Some(base) = s.get_base(root)?
        && !base.state.parent.is_root()
        && s.local_by_node(base.state.parent)?.is_none()
        && s.get_base(base.state.parent)?.is_some()
    {
        root = base.state.parent;
    }
    Ok(root)
}

/// Deletes a file or a folder with everything in it (a link, not what it points to).
pub(crate) fn remove_all(path: &Path) -> io::Result<()> {
    if fs::symlink_metadata(path)?.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

/// Now, in Unix ms.
fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The tracked item that holds `target` on disk: the row at that exact path, or, on a
/// case-insensitive volume, a sibling whose name differs only in case or normalization.
fn occupant(
    db: &mut StateDb,
    case_insensitive: bool,
    target: &RelPath,
) -> Result<Option<LocalRow>, StateError> {
    if let Some(row) = db.read(|s| s.get_local(target))? {
        return Ok(Some(row));
    }
    if !case_insensitive {
        return Ok(None);
    }
    let (Some(name), parent) = (target.name(), target.parent().unwrap_or_default()) else {
        return Ok(None);
    };
    let key = name_key(name);
    let siblings = db.read(|s| s.local_children(&parent))?;
    Ok(siblings
        .into_iter()
        .find(|r| r.path.name().is_some_and(|n| name_key(n) == key)))
}

/// Whether nothing holds `path`, on disk or in the DB.
fn is_free(db: &mut StateDb, cfg: &ExecConfig, path: &RelPath) -> Result<bool, StateError> {
    Ok(fs::symlink_metadata(path.to_native(&cfg.root)).is_err()
        && occupant(db, cfg.case_insensitive, path)?.is_none())
}

/// The first free numbered variant of `target`, one this device syncs if any is (§10). `None`
/// if all that are tried are taken.
fn numbered_place(
    db: &mut StateDb,
    cfg: &ExecConfig,
    target: &RelPath,
    is_dir: bool,
) -> Result<Option<RelPath>, StateError> {
    let (Some(name), folder) = (target.name(), target.parent().unwrap_or_default()) else {
        return Ok(None);
    };
    let mut filtered = None;
    for candidate in crate::tree::numbered_names(name) {
        let Ok(candidate) = folder.join(&candidate) else {
            break;
        };
        if is_free(db, cfg, &candidate)? {
            if cfg.filter.path(&candidate, is_dir).is_sync() {
                return Ok(Some(candidate));
            }
            filtered.get_or_insert(candidate);
        }
    }
    Ok(filtered)
}

/// A real name for a parked item that leaves its temporary one: its `original` place if that is
/// free, else a numbered one ([`numbered_place`]).
fn release_place(
    db: &mut StateDb,
    cfg: &ExecConfig,
    original: &RelPath,
    is_dir: bool,
) -> Result<Option<RelPath>, StateError> {
    if is_free(db, cfg, original)? {
        return Ok(Some(original.clone()));
    }
    numbered_place(db, cfg, original, is_dir)
}

/// Moves the parked item `row` to `to` and drops its parking record. After a crash in between,
/// the scan finds it at `to`, which voids the record.
fn restore_parked(
    db: &mut StateDb,
    root: &Path,
    row: &LocalRow,
    to: &RelPath,
) -> Result<io::Result<LocalRow>, StateError> {
    if let Err(e) = ops::rename_noreplace(&row.path.to_native(root), &to.to_native(root)) {
        return Ok(Err(e));
    }
    let refreshed = renamed_row(root, row, to);
    db.write(|s| {
        s.move_local_subtree(&row.path, to)?;
        if let Some(r) = &refreshed {
            s.put_local(r)?;
        }
        s.delete_parked(row.node)
    })?;
    Ok(Ok(refreshed.unwrap_or_else(|| LocalRow {
        path: to.clone(),
        ..row.clone()
    })))
}

/// The temporary name a node is parked under (see [`Executor`]).
pub fn parked_name(node: NodeId) -> String {
    format!(".hbsync-move-{}", node.0.simple())
}

/// Whether `meta` (from disk) is what `row` recorded: folders by kind and file id, files by
/// their full fingerprint.
fn matches_row(row: &LocalRow, meta: &hbsync_platform::LocalMeta) -> bool {
    match row.kind {
        NodeKind::Folder => meta.kind == EntryKind::Dir && meta.file_id == row.fingerprint.file_id,
        NodeKind::File => meta.kind == EntryKind::File && meta.fingerprint() == row.fingerprint,
    }
}

fn rescan(report: &mut ExecReport, path: &RelPath) -> Outcome {
    report.rescan.push(path.parent().unwrap_or_default());
    Outcome::Skip(format!("{path} changed on disk"))
}

fn io_outcome(e: io::Error) -> Outcome {
    match classify(&e) {
        IoClass::Locked | IoClass::DiskFull | IoClass::Other => Outcome::Retry(e.to_string()),
        IoClass::Raced => Outcome::Retry(format!("raced: {e}")),
        IoClass::Denied | IoClass::TooLong => Outcome::Skip(e.to_string()),
    }
}

fn remote_outcome(e: RemoteError) -> Outcome {
    match e {
        RemoteError::Transient(_) => Outcome::Backoff(e.to_string()),
        // The local source or sink failed: worth another try, but no reason to pause.
        RemoteError::Io(_) => Outcome::Retry(e.to_string()),
        RemoteError::Denied(_) => Outcome::Stop(e.to_string()),
        // The remote moved on, or rejected the bytes (changed mid-upload): plan again.
        _ => Outcome::Skip(e.to_string()),
    }
}

/// A rename changes ctime on most systems: the row of a file renamed from `old` to `path`,
/// with the new fingerprint and the old hash, if size and mtime show the content didn't
/// change. `None` for folders, or if the content may have changed (a rescan decides then).
fn renamed_row(root: &Path, old: &LocalRow, path: &RelPath) -> Option<LocalRow> {
    let meta = hbsync_platform::stat(&path.to_native(root)).ok()?;
    (old.kind == NodeKind::File && old.fingerprint.same_after_rename(&meta)).then(|| LocalRow {
        path: path.clone(),
        fingerprint: meta.fingerprint(),
        ..old.clone()
    })
}

/// The DB half of a fork: the original's row leaves, the copy's row arrives, the record goes.
fn complete_fork(s: &Store<'_>, node: NodeId, moved: &LocalRow) -> Result<(), StateError> {
    s.delete_local(node)?;
    s.put_local(moved)?;
    s.delete_parked(node)?;
    s.delete_fork(node)?;
    s.mark_dirty(moved.node)?;
    s.mark_dirty(node)
}

/// Finishes or forgets forks interrupted by a crash. Run after the scan and before planning:
/// the scan has followed the renamed file (same node, now under the conflict-copy name, in
/// whatever folder it is now), and completing hands that row to the copy's node, so the plan
/// never reads the fork as the user renaming the original.
pub fn recover_forks(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let mut completed = 0;
    for (node, new_node, copy_name) in db.read(|s| s.all_forks())? {
        let Some(row) = db.read(|s| s.local_by_node(node))? else {
            db.write(|s| s.delete_fork(node))?;
            continue;
        };
        if row.path.name() == Some(copy_name.as_str()) {
            db.write(|s| {
                s.rekey_local(node, new_node)?;
                s.delete_parked(node)?;
                s.delete_fork(node)?;
                s.mark_dirty(new_node)?;
                s.mark_dirty(node)
            })?;
            completed += 1;
        } else if hbsync_platform::stat(&row.path.to_native(root))
            .is_ok_and(|meta| meta.file_id == row.fingerprint.file_id)
        {
            // Still under its own name: the rename never happened.
            db.write(|s| s.delete_fork(node))?;
        }
        // Otherwise the scan hasn't caught up with the disk yet: decide next time.
    }
    Ok(completed)
}

/// The temp folder for downloads, inside the state folder.
const TMP_DIR: &str = "tmp";

/// The temp file name stem of a download: node and content, so a partial file is resumed only
/// for the same bytes.
fn download_key(node: NodeId, expected: &hbsync_types::ContentHash) -> String {
    format!("{}-{}", node.0.simple(), &expected.to_hex()[..16])
}

/// Removes temp files no download can use any more: anything that isn't a partial download of
/// the current remote content of a node that still needs syncing (interrupted downloads of
/// content since replaced, files that were placed by other means, leftovers of other
/// versions). Run before each plan, while no transfer is running.
pub fn sweep_tmp(db: &mut StateDb, state_dir: &Path) -> Result<usize, StateError> {
    let Ok(entries) = fs::read_dir(state_dir.join(TMP_DIR)) else {
        return Ok(0);
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let wanted = match entry.file_name().to_str() {
            Some(name) => wanted_download(db, name)?,
            None => false,
        };
        if !wanted && fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    Ok(removed)
}

fn wanted_download(db: &mut StateDb, file_name: &str) -> Result<bool, StateError> {
    let Some(node) = file_name
        .strip_suffix(".partial")
        .and_then(|key| key.split_once('-'))
        .and_then(|(node, _)| hbsync_types::Uuid::try_parse(node).ok())
        .map(NodeId)
    else {
        return Ok(false);
    };
    db.read(|s| {
        let current = s
            .get_mirror(node)?
            .and_then(|m| m.file)
            .map(|f| download_key(node, &f.hash));
        Ok(s.is_dirty(node)?
            && current.is_some_and(|key| file_name.strip_suffix(".partial") == Some(&key)))
    })
}

/// Puts parked items back where they came from when that is possible, and drops records that
/// no longer apply (the item was renamed, by us or by the user). Run before each plan.
/// An original name this device now filters (§10) may be held by an item it doesn't track, which
/// would keep the parked item away for good: it takes the first free numbered name instead.
pub fn unpark(db: &mut StateDb, cfg: &ExecConfig) -> Result<usize, StateError> {
    let mut restored = 0;
    for node in db.read(|s| s.all_parked())? {
        let place = db.read(|s| {
            let Some(row) = s.local_by_node(node)? else {
                return Ok(None);
            };
            Ok(view::parked_place(s, &row)?.map(|original| (row, original)))
        })?;
        let Some((current, original)) = place else {
            db.write(|s| s.delete_parked(node))?;
            continue;
        };
        let is_dir = current.kind == NodeKind::Folder;
        let to = if cfg.filter.path(&original, is_dir).is_sync() {
            // Taken by something synced: that changes, wait for it.
            is_free(db, cfg, &original)?.then_some(original)
        } else {
            release_place(db, cfg, &original, is_dir)?
        };
        if let Some(to) = to
            && restore_parked(db, &cfg.root, &current, &to)?.is_ok()
        {
            restored += 1;
        }
    }
    Ok(restored)
}
