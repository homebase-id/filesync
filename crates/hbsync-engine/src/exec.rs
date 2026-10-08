//! Executing a plan against the disk, the state DB and the remote.
//!
//! Every operation is guarded and idempotent, as the simulator requires:
//! - a local item is only touched if it still matches its `local_items` row (the fingerprint
//!   guard); folders are compared by kind and file id;
//! - nothing local is ever overwritten by surprise: new names use no-replace renames, content
//!   swaps check the fingerprint first, and deletes move items to `.hbsync/trash`;
//! - a remote write carries the version the plan saw;
//! - a target name held by a node that is about to leave is freed by **parking** that node
//!   under a temporary name, recorded in the DB until it is moved on or back;
//! - an operation blocked only by the order of the plan is retried later in the same run.
//!
//! After each operation the affected rows (`local_items`, mirror) are updated and the node is
//! **settled**: the base takes each attribute both sides now agree on.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hbsync_fileio::{FileIoError, HashAlgorithm};
use hbsync_namemap::{to_local, to_remote};
use hbsync_platform::name::{NameIssue, name_key};
use hbsync_platform::{EntryKind, IoClass, Os, RelPath, classify, ops};
use hbsync_remote::{NewNode, Remote, RemoteError};
use hbsync_state::{
    BaseRow, LocalRow, MirrorFile, MirrorRow, RemoteRef as BaseRemoteRef, StateDb, StateError,
    Store,
};
use hbsync_types::schema::NodeContent;
use hbsync_types::{ContentState, NodeId, NodeKind};

use crate::reconcile::{Op, Plan};
use crate::transfer::{Job, JobError, JobResult, TransferConfig, run_jobs};
use crate::view;

/// Where the executor works.
#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub root: PathBuf,
    pub os: Os,
    /// The hash this client writes and keeps in `local_items`.
    pub algorithm: HashAlgorithm,
    /// Attempts per operation within one run (ordering blocks and transient errors).
    pub attempts: u8,
    pub transfer: TransferConfig,
    /// Names that differ only in case (or Unicode normalization) are the same file here
    /// (Windows, default macOS). Probe with [`probe_case_insensitive`].
    pub case_insensitive: bool,
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
}

#[derive(Debug)]
enum Outcome {
    Done,
    Retry(String),
    Skip(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error(transparent)]
    State(#[from] StateError),
}

impl From<StateError> for Outcome {
    fn from(e: StateError) -> Self {
        Outcome::Retry(e.to_string())
    }
}

pub struct Executor<'a> {
    db: &'a mut StateDb,
    remote: &'a dyn Remote,
    cfg: &'a ExecConfig,
    queue: VecDeque<(Op, u8)>,
    report: ExecReport,
    urgent: HashSet<NodeId>,
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
        }
    }

    /// Marks nodes whose transfers go first (files the user is waiting for).
    pub fn with_urgent(mut self, urgent: HashSet<NodeId>) -> Self {
        self.urgent = urgent;
        self
    }

    pub fn run(mut self, plan: &Plan) -> Result<ExecReport, ExecError> {
        self.queue = plan.ops.iter().cloned().map(|op| (op, 0)).collect();
        let mut batch: Vec<Prepared> = Vec::new();
        loop {
            let Some((op, attempts)) = self.queue.pop_front() else {
                if batch.is_empty() {
                    break;
                }
                self.flush(&mut batch)?;
                continue;
            };
            if is_transfer(&op) {
                // Transfers are collected and run together on the worker pool.
                match self.prepare(&op) {
                    Ok(Ok((job, ctx))) => batch.push(Prepared {
                        op,
                        attempts,
                        job,
                        ctx,
                    }),
                    Ok(Err(outcome)) => self.handle(op, attempts, outcome)?,
                    Err(e) => self.handle(op, attempts, Outcome::from(e))?,
                }
                continue;
            }
            if !batch.is_empty() {
                self.flush(&mut batch)?;
            }
            let outcome = self.execute(&op);
            self.handle(op, attempts, outcome)?;
        }
        Ok(self.report)
    }

    fn handle(&mut self, op: Op, attempts: u8, outcome: Outcome) -> Result<(), ExecError> {
        match outcome {
            Outcome::Done => {
                self.report.done += 1;
                if let Op::LocalFork { new_node, .. } = op {
                    self.settle(new_node, false)?;
                }
                self.settle(op.node(), is_transfer(&op))?;
            }
            Outcome::Retry(_) if attempts + 1 < self.cfg.attempts => {
                self.queue.push_back((op, attempts + 1));
            }
            Outcome::Retry(why) => self.report.failed.push((op, why)),
            Outcome::Skip(why) => {
                let node = op.node();
                self.db.write(|s| s.mark_dirty(node))?;
                self.report.skipped.push((op, why));
            }
        }
        Ok(())
    }

    /// Runs the collected transfers in parallel and commits each as it finishes.
    fn flush(&mut self, batch: &mut Vec<Prepared>) -> Result<(), ExecError> {
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
        run_jobs(remote, transfer, jobs, |job, result| {
            if first_error.is_some() {
                return; // the DB failed; leave the rest dirty for the next sync
            }
            let Some(p) = pending.remove(&job.node()) else {
                return;
            };
            let outcome = self.commit(p.ctx, result).unwrap_or_else(Outcome::from);
            if let Err(e) = self.handle(p.op, p.attempts, outcome) {
                first_error = Some(e);
            }
        });
        if let Some(e) = first_error {
            return Err(e);
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
        result.unwrap_or_else(Outcome::from)
    }

    // ---- paths ----

    fn abs(&self, path: &RelPath) -> PathBuf {
        path.to_native(&self.cfg.root)
    }

    /// The local path where (parent, name) lives. Retried while the parent isn't local yet;
    /// skipped if the name can't be spelled on this OS (it stays remote-only and is reported).
    fn target_path(
        &mut self,
        parent: NodeId,
        name: &str,
    ) -> Result<Result<RelPath, Outcome>, StateError> {
        let parent_path = if parent.is_root() {
            RelPath::root()
        } else {
            match self.db.read(|s| s.local_by_node(parent))? {
                Some(row) if row.kind == NodeKind::Folder => row.path,
                _ => return Ok(Err(Outcome::Retry("parent not local yet".into()))),
            }
        };
        let unspellable = |e: NameIssue| Outcome::Skip(format!("name can't be used here: {e}"));
        let local = match to_local(name, self.cfg.os) {
            Ok(local) => local,
            Err(e) => return Ok(Err(unspellable(e))),
        };
        Ok(parent_path.join(&local.name).map_err(unspellable))
    }

    /// Whether the item on disk still matches its row.
    fn on_disk_unchanged(&self, row: &LocalRow) -> bool {
        match hbsync_platform::stat(&self.abs(&row.path)) {
            Ok(meta) => match row.kind {
                NodeKind::Folder => {
                    meta.kind == EntryKind::Dir && meta.file_id == row.fingerprint.file_id
                }
                NodeKind::File => {
                    meta.kind == EntryKind::File && meta.fingerprint() == row.fingerprint
                }
            },
            Err(_) => false,
        }
    }

    /// Frees `target` if a node that the plan is about to move away holds it: that node is
    /// parked under a temporary name (recorded in the DB). Returns `false` if it stays taken.
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
        let leaving = self.queue.iter().any(|(op, _)| match op {
            Op::LocalMove { node, .. }
            | Op::LocalTrash { node, .. }
            | Op::LocalFork { node, .. }
            | Op::LocalDownload {
                node,
                replace: true,
                ..
            } => *node == occupant.node,
            _ => false,
        });
        if !leaving || !self.on_disk_unchanged(&occupant) {
            return Ok(false);
        }
        let parent = occupant.path.parent().unwrap_or_default();
        let Ok(parked) = parent.join(&parked_name(occupant.node)) else {
            return Ok(false);
        };
        if ops::rename_noreplace(&self.abs(&occupant.path), &self.abs(&parked)).is_err() {
            return Ok(false);
        }
        let original = occupant.path.clone();
        self.db.write(|s| {
            s.move_local_subtree(&original, &parked)?;
            s.put_parked(occupant.node, &parked, &original)
        })?;
        // The rename changed ctime: record it, or the occupant's own next step fails its check.
        self.refresh_fingerprint(&parked, &occupant)?;
        Ok(true)
    }

    // ---- local operations ----

    fn fork(&mut self, node: NodeId, new_node: NodeId, name: &str) -> Result<Outcome, StateError> {
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
        self.db
            .write(|s| s.put_fork(node, new_node, &row.path, &target))?;
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            self.db.write(|s| s.delete_fork(node))?;
            return Ok(io_outcome(e));
        }
        let moved = LocalRow {
            path: target.clone(),
            node: new_node,
            ..row.clone()
        };
        self.db
            .write(|s| complete_fork(s, node, &row.path, &moved))?;
        self.refresh_fingerprint(&target, &moved)?;
        Ok(Outcome::Done)
    }

    fn mkdir(&mut self, node: NodeId, parent: NodeId, name: &str) -> Result<Outcome, StateError> {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::Skip("already local".into()));
        }
        let target = match self.target_path(parent, name)? {
            Ok(target) => target,
            Err(outcome) => return Ok(outcome),
        };
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
        self.db.write(|s| {
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::Folder,
                fingerprint: meta.fingerprint(),
                executable: None,
                hash: None,
            })
        })?;
        Ok(Outcome::Done)
    }

    fn local_move(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
    ) -> Result<Outcome, StateError> {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let target = match self.target_path(parent, name)? {
            Ok(target) => target,
            Err(outcome) => return Ok(outcome),
        };
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
        self.db.write(|s| {
            s.move_local_subtree(&row.path, &target)?;
            s.delete_parked(node)
        })?;
        self.refresh_fingerprint(&target, &row)?;
        Ok(Outcome::Done)
    }

    /// A rename changes ctime on most systems: record the new fingerprint, keeping the hash,
    /// if size and mtime show the content didn't change.
    fn refresh_fingerprint(&mut self, path: &RelPath, old: &LocalRow) -> Result<(), StateError> {
        let Ok(meta) = hbsync_platform::stat(&self.abs(path)) else {
            return Ok(());
        };
        let same = meta.size == old.fingerprint.size && meta.mtime_ns == old.fingerprint.mtime_ns;
        if old.kind == NodeKind::File && same {
            let row = LocalRow {
                path: path.clone(),
                fingerprint: meta.fingerprint(),
                ..old.clone()
            };
            self.db.write(|s| s.put_local(&row))?;
        }
        Ok(())
    }

    // ---- transfers: prepare (guards) and commit (placement, DB) around the worker pool ----

    fn prepare(&mut self, op: &Op) -> Result<Result<(Job, Ctx), Outcome>, StateError> {
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
            _ => Ok(Err(Outcome::Skip("not a transfer".into()))),
        }
    }

    fn prepare_download(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        replace: bool,
    ) -> Result<Result<(Job, Ctx), Outcome>, StateError> {
        let (mirror, current) = self
            .db
            .read(|s| Ok((s.get_mirror(node)?, s.local_by_node(node)?)))?;
        let Some(mirror) = mirror else {
            return Ok(Err(Outcome::Skip("gone remotely".into())));
        };
        let Some(file) = mirror.file else {
            return Ok(Err(Outcome::Skip("not a file".into())));
        };
        let Some(expected) = file.hash else {
            return Ok(Err(Outcome::Skip("no remote hash".into())));
        };
        if replace != current.is_some() {
            return Ok(Err(Outcome::Skip("local presence changed".into())));
        }
        if let Some(row) = &current
            && !self.on_disk_unchanged(row)
        {
            return Ok(Err(rescan(&mut self.report, &row.path)));
        }
        let target = match self.target_path(parent, name)? {
            Ok(target) => target,
            Err(outcome) => return Ok(Err(outcome)),
        };
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
        Ok(Ok((
            job,
            Ctx::Download {
                node,
                target,
                current,
            },
        )))
    }

    fn prepare_upload(
        &mut self,
        node: NodeId,
        existing: Option<(hbsync_types::RemoteFileId, hbsync_types::VersionTag)>,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
    ) -> Result<Result<(Job, Ctx), Outcome>, StateError> {
        let (row, mirror) = self
            .db
            .read(|s| Ok((s.local_by_node(node)?, s.get_mirror(node)?)))?;
        let Some(row) = row else {
            return Ok(Err(Outcome::Skip("gone locally".into())));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(Err(rescan(&mut self.report, &row.path)));
        }
        let (mut content, payload) = match self.content_for(&row, name) {
            Ok(x) => x,
            Err(o) => return Ok(Err(o)),
        };
        let Some((hash, size)) = payload else {
            return Ok(Err(Outcome::Skip("no payload".into())));
        };
        let path = self.abs(&row.path);
        let extra = mirror.as_ref().and_then(|m| m.content_extra.clone());
        let job = match existing {
            None => Job::Create {
                node,
                new: NewNode {
                    node,
                    parent,
                    kind,
                    content: content.clone(),
                },
                path,
                hash,
                size,
            },
            Some((file_id, expected)) => {
                if mirror.is_none() {
                    return Ok(Err(Outcome::Skip("gone remotely".into())));
                }
                if let Some(extra) = &extra
                    && let Ok(serde_json::Value::Object(map)) = serde_json::from_str(extra)
                {
                    content.unknown = map;
                }
                Job::Update {
                    node,
                    file_id,
                    expected,
                    parent,
                    content: content.clone(),
                    path,
                    hash,
                    size,
                }
            }
        };
        Ok(Ok((
            job,
            Ctx::Upload {
                node,
                parent,
                kind,
                content,
                hash,
                extra,
            },
        )))
    }

    fn commit(&mut self, ctx: Ctx, result: JobResult) -> Result<Outcome, StateError> {
        match (ctx, result) {
            (_, JobResult::Failed(e)) => Ok(match e {
                JobError::Remote(RemoteError::AlreadyExists) => Outcome::Done, // an earlier try landed
                JobError::Remote(r) => remote_outcome(r),
                e if e.is_transient() => Outcome::Retry(e.to_string()),
                e => Outcome::Skip(e.to_string()),
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
                self.db.write(|s| s.put_mirror(&row))?;
                Ok(Outcome::Done)
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
    ) -> Result<Outcome, StateError> {
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
        self.db.write(|s| {
            if let Some(row) = &current {
                s.delete_local(&row.path)?;
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
        })?;
        Ok(Outcome::Done)
    }

    fn trash(&mut self, node: NodeId, expected: &[NodeId]) -> Result<Outcome, StateError> {
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
        let folder_has_unknown =
            row.kind == NodeKind::Folder && self.disk_has_unknown(&row.path, rows.len());
        if !rows.iter().all(|r| self.on_disk_unchanged(r)) || folder_has_unknown {
            return Ok(rescan(&mut self.report, &row.path));
        }
        if let Err(e) = self.move_to_trash(&row.path, node) {
            return Ok(io_outcome(e));
        }
        self.db.write(|s| {
            for r in s.delete_local_subtree(&row.path)? {
                s.delete_parked(r.node)?;
                if r.node != node {
                    s.mark_dirty(r.node)?;
                }
            }
            Ok(())
        })?;
        Ok(Outcome::Done)
    }

    /// Whether the folder at `path` has entries on disk the DB doesn't know.
    fn disk_has_unknown(&self, path: &RelPath, known: usize) -> bool {
        let mut count = 1usize; // the folder itself
        for event in hbsync_local::Walker::at(
            &self.cfg.root,
            path.clone(),
            &[hbsync_local::STATE_DIR],
            None,
        ) {
            match event {
                hbsync_local::WalkEvent::Entry { .. } => count += 1,
                // Something we never sync (a link, a bad name) would be trashed silently.
                _ => return true,
            }
        }
        count != known
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

    fn create_folder(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
    ) -> Result<Outcome, StateError> {
        debug_assert_eq!(kind, NodeKind::Folder, "file creates run through flush");
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone locally".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let content = NodeContent::folder(name);
        let new = NewNode {
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
        self.db.write(|s| {
            s.put_mirror(&mirror_from(
                node, parent, kind, &content, None, written, None,
            ))
        })?;
        Ok(Outcome::Done)
    }

    /// A metadata-only update: move or rename, keeping the payload.
    fn rename_remote(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
        parent: NodeId,
        name: &str,
    ) -> Result<Outcome, StateError> {
        let Some(mirror) = self.db.read(|s| s.get_mirror(node))? else {
            return Ok(Outcome::Skip("gone remotely".into()));
        };
        let mut content = match (&mirror.kind, &mirror.file) {
            (NodeKind::File, Some(f)) => NodeContent::file(name, f.mtime_ms, f.size, f.executable),
            _ => NodeContent::folder(name),
        };
        if let Some(extra) = &mirror.content_extra
            && let Ok(serde_json::Value::Object(map)) = serde_json::from_str(extra)
        {
            content.unknown = map;
        }
        let written = match self
            .remote
            .update(file_id, expected, parent, &content, None)
        {
            Ok(w) => w,
            Err(e) => return Ok(remote_outcome(e)),
        };
        let hash = mirror.file.and_then(|f| f.hash);
        let row = mirror_from(
            node,
            parent,
            mirror.kind,
            &content,
            hash,
            written,
            mirror.content_extra.clone(),
        );
        self.db.write(|s| s.put_mirror(&row))?;
        Ok(Outcome::Done)
    }

    fn delete(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> Result<Outcome, StateError> {
        match self.remote.delete(file_id, expected) {
            Ok(()) | Err(RemoteError::NotFound) => {}
            Err(e) => return Ok(remote_outcome(e)),
        }
        self.db.write(|s| {
            s.delete_mirror(file_id)?;
            s.mark_dirty(node)
        })?;
        Ok(Outcome::Done)
    }

    /// The content JSON and payload (hash, size) to send for a local item.
    fn content_for(
        &self,
        row: &LocalRow,
        name: &str,
    ) -> Result<(NodeContent, Option<(hbsync_types::ContentHash, u64)>), Outcome> {
        match row.kind {
            NodeKind::Folder => Ok((NodeContent::folder(name), None)),
            NodeKind::File => {
                let Some(hash) = row.hash else {
                    return Err(Outcome::Skip("not hashed yet".into()));
                };
                let size = row.fingerprint.size;
                let mtime_ms = row.fingerprint.mtime_ns.div_euclid(1_000_000);
                Ok((
                    NodeContent::file(name, mtime_ms, size, row.executable.unwrap_or(false)),
                    Some((hash, size)),
                ))
            }
        }
    }

    // ---- settling ----

    /// Records in the base whatever both sides now agree on (sync-semantics: the base only
    /// takes a value when both sides hold it). `content_hint`: the operation just made the
    /// local and remote bytes equal (a transfer), even if their hashes use other algorithms.
    fn settle(&mut self, node: NodeId, content_hint: bool) -> Result<(), StateError> {
        let os = self.cfg.os;
        self.db.write(|s| settle(s, node, os, content_hint))
    }
}

/// See [`Executor::settle`].
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
                b.remote = BaseRemoteRef {
                    file_id: m.file_id,
                    version: m.version,
                };
                s.put_base(&b)?;
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    let (Some(l), Some(r)) = (local.state.clone(), mirror.state()) else {
        return Ok(());
    };
    let place = l.same_place(&r);
    let content = match (&l.content, &r.content) {
        (None, None) => true,
        (Some(lc), Some(rc)) => {
            content_hint || (lc.hash == rc.hash && lc.executable == rc.executable)
        }
        _ => false,
    };
    let local_name = local.row.path.name().map(str::to_owned);
    let recorded = |name: &str| {
        local_name
            .clone()
            .filter(|ln| to_remote(ln, os).as_deref() != Ok(name))
    };
    let remote_ref = BaseRemoteRef {
        file_id: mirror.file_id,
        version: mirror.version,
    };
    match base {
        None if place && content => {
            s.put_base(&BaseRow {
                node,
                local_name: recorded(&r.name),
                local_hash: l.content.map(|c| c.hash),
                state: r,
                remote: remote_ref,
                content_state: ContentState::Local,
            })?;
        }
        None => {}
        Some(mut b) => {
            if place {
                b.state.parent = r.parent;
                b.local_name = recorded(&r.name);
                b.state.name = r.name.clone();
            }
            if content {
                b.state.content = r.content;
                b.local_hash = l.content.map(|c| c.hash);
            }
            b.remote = remote_ref;
            s.put_base(&b)?;
        }
    }
    Ok(())
}

fn mirror_from(
    node: NodeId,
    parent: NodeId,
    kind: NodeKind,
    content: &NodeContent,
    hash: Option<hbsync_types::ContentHash>,
    written: hbsync_remote::Written,
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

pub(crate) fn parked_name(node: NodeId) -> String {
    format!(".hbsync-move-{}", node.0.simple())
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
        RemoteError::Transient(_) | RemoteError::Io(_) => Outcome::Retry(e.to_string()),
        // The remote moved on, or rejected the bytes (changed mid-upload): plan again.
        _ => Outcome::Skip(e.to_string()),
    }
}

/// The DB half of a fork: the original's row leaves, the copy's row arrives, the record goes.
fn complete_fork(
    s: &Store<'_>,
    node: NodeId,
    from: &RelPath,
    moved: &LocalRow,
) -> Result<(), StateError> {
    s.delete_local(from)?;
    s.put_local(moved)?;
    s.delete_parked(node)?;
    s.delete_fork(node)?;
    s.mark_dirty(moved.node)?;
    s.mark_dirty(node)
}

/// Finishes or forgets forks interrupted by a crash. Run before scanning: a scan would read a
/// finished rename without its DB update as the user renaming the original.
pub fn recover_forks(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let mut completed = 0;
    for (node, new_node, from, to) in db.read(|s| s.all_forks())? {
        let row = db.read(|s| s.get_local(&from))?.filter(|r| r.node == node);
        let at_to = hbsync_platform::stat(&to.to_native(root)).ok();
        let from_gone = fs::symlink_metadata(from.to_native(root)).is_err();
        match (row, at_to) {
            (Some(row), Some(meta)) if from_gone && meta.file_id == row.fingerprint.file_id => {
                // The bytes are the forked ones only if size and mtime agree; else rehash later.
                let same =
                    meta.size == row.fingerprint.size && meta.mtime_ns == row.fingerprint.mtime_ns;
                let moved = LocalRow {
                    path: to.clone(),
                    node: new_node,
                    fingerprint: meta.fingerprint(),
                    hash: if same { row.hash } else { None },
                    ..row
                };
                db.write(|s| complete_fork(s, node, &from, &moved))?;
                completed += 1;
            }
            _ => db.write(|s| s.delete_fork(node))?,
        }
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
/// no longer apply (the item was moved on, by us or by the user). Run before each plan.
pub fn unpark(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let mut restored = 0;
    for (node, parked, original) in db.read(|s| s.all_parked())? {
        let row = db.read(|s| s.local_by_node(node))?;
        if row.as_ref().map(|r| &r.path) != Some(&parked) {
            db.write(|s| s.delete_parked(node))?;
            continue;
        }
        let free = fs::symlink_metadata(original.to_native(root)).is_err()
            && db.read(|s| s.get_local(&original))?.is_none();
        if free && ops::rename_noreplace(&parked.to_native(root), &original.to_native(root)).is_ok()
        {
            // Keep the fingerprint current: the rename changed ctime, not the content.
            let refreshed = row.as_ref().and_then(|r| {
                let meta = hbsync_platform::stat(&original.to_native(root)).ok()?;
                let same =
                    meta.size == r.fingerprint.size && meta.mtime_ns == r.fingerprint.mtime_ns;
                (r.kind == NodeKind::File && same).then(|| LocalRow {
                    path: original.clone(),
                    fingerprint: meta.fingerprint(),
                    ..r.clone()
                })
            });
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
