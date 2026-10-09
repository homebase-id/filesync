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
use std::time::{Duration, Instant};

use hbsync_fileio::{FileIoError, HashAlgorithm};
use hbsync_namemap::{to_local, to_remote};
use hbsync_platform::name::{NameIssue, name_key};
use hbsync_platform::{EntryKind, IoClass, Os, RelPath, classify, ops};
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{BaseRow, LocalRow, MirrorFile, MirrorRow, StateDb, StateError, Store};
use hbsync_types::schema::NodeContent;
use hbsync_types::{ContentState, NodeId, NodeKind, RemoteRef};

use crate::reconcile::{Agreement, Op, Requirement};
use crate::transfer::{Job, JobError, JobResult, TransferConfig, run_jobs_while};
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
    report: ExecReport,
    urgent: HashSet<NodeId>,
    /// Set when a test's crash point was reached: the run stops.
    crashed: bool,
    /// Transient remote errors in a row, and until when the run waits because of them.
    streak: u32,
    resume_at: Option<Instant>,
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
    },
    Upload {
        node: NodeId,
        parent: NodeId,
        kind: NodeKind,
        content: NodeContent,
        hash: hbsync_types::ContentHash,
        extra: Option<String>,
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
            report: ExecReport::default(),
            urgent: HashSet::new(),
            crashed: false,
            streak: 0,
            resume_at: None,
        }
    }

    /// Marks nodes whose transfers go first (files the user is waiting for).
    pub fn with_urgent(mut self, urgent: HashSet<NodeId>) -> Self {
        self.urgent = urgent;
        self
    }

    /// Runs a plan's operations, in order.
    pub fn run(mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        self.queue = ops.into_iter().map(|op| (op, 0)).collect();
        let mut batch: Vec<Prepared> = Vec::new();
        loop {
            if self.report.stopped.is_some() {
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
                self.handle(op, attempts, wait)?;
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
                    Err(outcome) => self.handle(op, attempts, outcome)?,
                }
                continue;
            }
            if !batch.is_empty() {
                self.flush(&mut batch)?;
                if self.report.stopped.is_some() {
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
            let outcome = self.execute(&op);
            self.handle(op, attempts, outcome)?;
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

    fn handle(&mut self, op: Op, attempts: u8, outcome: Outcome) -> Result<(), StateError> {
        if matches!(outcome, Outcome::Done | Outcome::Settled | Outcome::Skip(_)) {
            self.streak = 0; // the remote answered (or wasn't needed)
        }
        match outcome {
            Outcome::Done => {
                self.report.done += 1;
                let os = self.cfg.os;
                let same_bytes = is_transfer(&op);
                self.db.write(|s| settle(s, op.node(), os, same_bytes))?;
            }
            Outcome::Settled => self.report.done += 1,
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

    fn retry_or_fail(&mut self, op: Op, attempts: u8, why: String) {
        if attempts + 1 < self.cfg.attempts {
            self.queue.push_back((op, attempts + 1));
        } else {
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

    /// Runs the collected transfers in parallel and commits each as it finishes.
    fn flush(&mut self, batch: &mut Vec<Prepared>) -> Result<(), StateError> {
        self.wait_out_backoff();
        let mut pending: HashMap<NodeId, Prepared> = HashMap::new();
        let mut jobs = Vec::new();
        for p in batch.drain(..) {
            jobs.push((self.urgent.contains(&p.job.node()), p.job.clone()));
            pending.insert(p.job.node(), p);
        }
        // Commit each transfer as soon as it finishes: small files don't wait for a large
        // one, and a crash loses only transfers still in flight.
        let remote = self.remote;
        let transfer = self.cfg.transfer;
        let mut first_error = None;
        // A DB error or a stopped run (denied, unreachable) starts no further transfers; the
        // nodes stay dirty for the next sync.
        run_jobs_while(remote, transfer, jobs, |job, result| {
            if first_error.is_some() {
                return false;
            }
            if let Some(p) = pending.remove(&job.node()) {
                let outcome = self.commit(p.ctx, result).unwrap_or_else(|o| o);
                if let Err(e) = self.handle(p.op, p.attempts, outcome) {
                    first_error = Some(e);
                }
            }
            first_error.is_none() && self.report.stopped.is_none()
        });
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
            Op::LocalTrash { node, subtree } => self.trash(*node, subtree),
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
            } => self.delete(*node, *file_id, *expected),
            Op::Settle { .. } => Ok(Outcome::Done),
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

    /// Whether the disk at and below `top` holds exactly `rows` (its subtree in the DB, `top`
    /// included), unchanged. One walk, compared with the metadata the listing returns.
    fn subtree_unchanged(&self, top: &LocalRow, rows: &[LocalRow]) -> bool {
        if !self.on_disk_unchanged(top) {
            return false;
        }
        let mut expected: HashMap<&RelPath, &LocalRow> = rows
            .iter()
            .filter(|r| r.path != top.path)
            .map(|r| (&r.path, r))
            .collect();
        if top.kind == NodeKind::Folder {
            let filter = hbsync_local::Filter::default();
            let walk = hbsync_local::Walker::at(&self.cfg.root, top.path.clone(), &filter, None);
            for event in walk {
                // Anything we never sync (a link, a bad name) would be trashed silently.
                let hbsync_local::WalkEvent::Entry { path, meta } = event else {
                    return false;
                };
                if !expected
                    .remove(&path)
                    .is_some_and(|r| matches_row(r, &meta))
                {
                    return false;
                }
            }
        }
        expected.is_empty()
    }

    /// The tracked item that holds `target` on disk: the row at that exact path, or, on a
    /// case-insensitive volume, a sibling whose name differs only in case or normalization.
    fn occupant(&mut self, target: &RelPath) -> Result<Option<LocalRow>, StateError> {
        if let Some(row) = self.db.read(|s| s.get_local(target))? {
            return Ok(Some(row));
        }
        if !self.cfg.case_insensitive {
            return Ok(None);
        }
        let (Some(name), parent) = (target.name(), target.parent().unwrap_or_default()) else {
            return Ok(None);
        };
        let key = name_key(name);
        let siblings = self.db.read(|s| s.local_children(&parent))?;
        Ok(siblings
            .into_iter()
            .find(|r| r.path.name().is_some_and(|n| name_key(n) == key)))
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
        if !self.free_target(&target, None)? {
            return Ok(Outcome::Retry("conflict name taken".into()));
        }
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
        let Some(expected) = file.hash else {
            return Err(Outcome::Skip("no remote hash".into()));
        };
        if replace != current.is_some() {
            return Err(Outcome::Skip("local presence changed".into()));
        }
        if let Some(row) = &current
            && !self.on_disk_unchanged(row)
        {
            return Err(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        let job = Job::Download {
            node,
            file_id: mirror.file_id,
            expected,
            size: file.size,
            mtime_ms: file.mtime_ms,
            executable: (self.cfg.os != Os::Windows).then_some(file.executable),
            tmp_dir: self.cfg.state_dir().join(TMP_DIR),
            key: download_key(node, &expected),
            local_algorithm: self.cfg.algorithm,
        };
        Ok((
            job,
            Ctx::Download {
                node,
                target,
                current,
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
        let extra = mirror.as_ref().and_then(|m| m.content_extra.clone());
        let target = match existing {
            None => None,
            Some((file_id, version)) => {
                let Some(mirror) = &mirror else {
                    return Err(Outcome::Skip("gone remotely".into()));
                };
                content.unknown = feed::unknown_keys(mirror);
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
                extra,
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
                },
                JobResult::Downloaded { file, local_hash },
            ) => self.place_download(node, &target, current, file, local_hash),
            (
                Ctx::Upload {
                    node,
                    parent,
                    kind,
                    content,
                    hash,
                    extra,
                },
                JobResult::Uploaded(written),
            ) => {
                let row = mirror_from(node, parent, kind, &content, Some(hash), written, extra);
                // The bytes were just sent: equal even if the hashes use other algorithms.
                self.finish(&[(node, true)], |s| s.put_mirror(&row))
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
                done.persist_over(&self.abs(target), &row.fingerprint)
            }
            Some(row) => {
                if !self.on_disk_unchanged(row) || !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("old version changed or name taken".into()));
                }
                match self.move_to_trash(&row.path, node) {
                    Ok(()) => done.persist_new(&self.abs(target)),
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
                done.persist_new(&self.abs(target))
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

    fn trash(&mut self, node: NodeId, expected: &[NodeId]) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
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
        if !self.subtree_unchanged(&row, &rows) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        if let Err(e) = self.move_to_trash(&row.path, node) {
            return Ok(io_outcome(e));
        }
        self.finish(&[(node, false)], |s| {
            for r in s.delete_local_subtree(&row.path)? {
                s.delete_parked(r.node)?;
                if r.node != node {
                    s.mark_dirty(r.node)?;
                }
            }
            Ok(())
        })
    }

    /// Moves a local item into `.hbsync/trash/<time>-<node>/`.
    fn move_to_trash(&self, path: &RelPath, node: NodeId) -> io::Result<()> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
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
        let content = NodeContent::folder(name);
        let new = NodeMeta {
            node,
            parent,
            kind,
            content: content.clone(),
        };
        let written = match self.remote.create(&new, None) {
            Ok(w) => w,
            Err(RemoteError::AlreadyExists) => return Ok(Outcome::Done),
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.put_mirror(&mirror_from(
                node, parent, kind, &content, None, written, None,
            ))
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
        let mut content = match (&mirror.kind, &mirror.file) {
            (NodeKind::File, Some(f)) => NodeContent::file(name, f.mtime_ms, f.size, f.executable),
            _ => NodeContent::folder(name),
        };
        content.unknown = feed::unknown_keys(&mirror);
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
        let hash = mirror.file.and_then(|f| f.hash);
        let row = mirror_from(
            node,
            parent,
            mirror.kind,
            &meta.content,
            hash,
            written,
            mirror.content_extra.clone(),
        );
        self.finish(&[(node, false)], |s| s.put_mirror(&row))
    }

    fn delete(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> Step {
        match self.remote.delete(file_id, expected) {
            Ok(()) | Err(RemoteError::NotFound) => {}
            Err(e) => return Ok(remote_outcome(e)),
        }
        self.finish(&[(node, false)], |s| {
            s.delete_mirror(file_id)?;
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
    hash: Option<hbsync_types::ContentHash>,
    written: hbsync_types::RemoteRef,
    extra: Option<String>,
) -> MirrorRow {
    MirrorRow {
        file_id: written.file_id,
        version: written.version,
        node,
        parent,
        name: content.name.clone(),
        kind,
        file: (kind == NodeKind::File).then(|| MirrorFile {
            hash,
            size: content.size.unwrap_or(0),
            mtime_ms: content.mtime.unwrap_or(0),
            executable: content.x,
        }),
        content_extra: extra,
    }
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
            .and_then(|f| f.hash)
            .map(|h| download_key(node, &h));
        Ok(s.is_dirty(node)?
            && current.is_some_and(|key| file_name.strip_suffix(".partial") == Some(&key)))
    })
}

/// Puts parked items back where they came from when that is possible, and drops records that
/// no longer apply (the item was renamed, by us or by the user). Run before each plan.
pub fn unpark(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
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
        let parked = current.path.clone();
        let free = fs::symlink_metadata(original.to_native(root)).is_err()
            && db.read(|s| s.get_local(&original))?.is_none();
        if free && ops::rename_noreplace(&parked.to_native(root), &original.to_native(root)).is_ok()
        {
            let refreshed = renamed_row(root, &current, &original);
            db.write(|s| {
                s.move_local_subtree(&parked, &original)?;
                if let Some(r) = &refreshed {
                    s.put_local(r)?;
                }
                s.delete_parked(node)
            })?;
            restored += 1;
        }
    }
    Ok(restored)
}
