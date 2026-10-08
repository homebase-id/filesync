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

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hbsync_fileio::{FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_namemap::{to_local, to_remote};
use hbsync_platform::{EntryKind, IoClass, Os, RelPath, classify, ops};
use hbsync_remote::{NewNode, Remote, RemoteError, Upload};
use hbsync_state::{
    BaseRow, LocalRow, MirrorFile, MirrorRow, RemoteRef as BaseRemoteRef, StateDb, StateError,
    Store,
};
use hbsync_types::schema::NodeContent;
use hbsync_types::{ContentState, NodeId, NodeKind};

use crate::reconcile::{Op, Plan};
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
        }
    }

    pub fn run(mut self, plan: &Plan) -> Result<ExecReport, ExecError> {
        self.queue = plan.ops.iter().cloned().map(|op| (op, 0)).collect();
        while let Some((op, attempts)) = self.queue.pop_front() {
            let outcome = self.execute(&op);
            match outcome {
                Outcome::Done => {
                    self.report.done += 1;
                    if let Op::LocalFork { new_node, .. } = op {
                        self.settle(new_node, false)?;
                    }
                    let hint = matches!(op, Op::LocalDownload { .. } | Op::RemoteCreate { .. })
                        || matches!(op, Op::RemoteUpdate { upload: true, .. });
                    self.settle(op.node(), hint)?;
                }
                Outcome::Retry(why) if attempts + 1 < self.cfg.attempts => {
                    self.queue.push_back((op, attempts + 1));
                    let _ = why;
                }
                Outcome::Retry(why) => self.report.failed.push((op, why)),
                Outcome::Skip(why) => {
                    let node = op.node();
                    self.db.write(|s| s.mark_dirty(node))?;
                    self.report.skipped.push((op, why));
                }
            }
        }
        Ok(self.report)
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
            Op::LocalDownload {
                node,
                parent,
                name,
                replace,
            } => self.download(*node, *parent, name, *replace),
            Op::LocalTrash { node, subtree } => self.trash(*node, subtree),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind,
            } => self.create(*node, *parent, name, *kind),
            Op::RemoteUpdate {
                node,
                file_id,
                expected,
                parent,
                name,
                upload,
            } => self.update(*node, *file_id, *expected, *parent, name, *upload),
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

    /// The local path where (parent, name) lives, if the parent is local.
    fn target_path(&mut self, parent: NodeId, name: &str) -> Result<Option<RelPath>, StateError> {
        let parent_path = if parent.is_root() {
            RelPath::root()
        } else {
            match self.db.read(|s| s.local_by_node(parent))? {
                Some(row) if row.kind == NodeKind::Folder => row.path,
                _ => return Ok(None),
            }
        };
        let Ok(local) = to_local(name, self.cfg.os) else {
            return Ok(None);
        };
        Ok(parent_path.join(&local.name).ok())
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
    fn free_target(&mut self, target: &RelPath, mover: Option<NodeId>) -> Result<bool, StateError> {
        let occupant_row = self.db.read(|s| s.get_local(target))?;
        let on_disk = fs::symlink_metadata(self.abs(target)).is_ok();
        let Some(occupant) = occupant_row else {
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
        let parent = target.parent().unwrap_or_default();
        let Ok(parked) = parent.join(&parked_name(occupant.node)) else {
            return Ok(false);
        };
        if ops::rename_noreplace(&self.abs(target), &self.abs(&parked)).is_err() {
            return Ok(false);
        }
        let original = target.clone();
        self.db.write(|s| {
            s.move_local_subtree(&original, &parked)?;
            s.put_parked(occupant.node, &parked, &original)
        })?;
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
        if !self.free_target(&target, None)? {
            return Ok(Outcome::Retry("conflict name taken".into()));
        }
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            return Ok(io_outcome(e));
        }
        self.db.write(|s| {
            s.delete_local(&row.path)?;
            s.put_local(&LocalRow {
                path: target.clone(),
                node: new_node,
                ..row.clone()
            })?;
            s.delete_parked(node)?;
            s.mark_dirty(new_node)
        })?;
        Ok(Outcome::Done)
    }

    fn mkdir(&mut self, node: NodeId, parent: NodeId, name: &str) -> Result<Outcome, StateError> {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::Skip("already local".into()));
        }
        let Some(target) = self.target_path(parent, name)? else {
            return Ok(Outcome::Retry("parent not local yet".into()));
        };
        let abs = self.abs(&target);
        // Idempotent: a folder left by an interrupted run, unknown to the DB, is adopted.
        let adopt = self.db.read(|s| s.get_local(&target))?.is_none()
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
        let Some(target) = self.target_path(parent, name)? else {
            return Ok(Outcome::Retry("parent not local yet".into()));
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

    fn download(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        replace: bool,
    ) -> Result<Outcome, StateError> {
        let (mirror, current) = self
            .db
            .read(|s| Ok((s.get_mirror(node)?, s.local_by_node(node)?)))?;
        let Some(mirror) = mirror else {
            return Ok(Outcome::Skip("gone remotely".into()));
        };
        let Some(file) = mirror.file else {
            return Ok(Outcome::Skip("not a file".into()));
        };
        let Some(expected) = file.hash else {
            return Ok(Outcome::Skip("no remote hash".into()));
        };
        if replace != current.is_some() {
            return Ok(Outcome::Skip("local presence changed".into()));
        }
        if let Some(row) = &current
            && !self.on_disk_unchanged(row)
        {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let Some(target) = self.target_path(parent, name)? else {
            return Ok(Outcome::Retry("parent not local yet".into()));
        };

        // Fetch into a resumable temp file keyed by node and content.
        let tmp = self.cfg.state_dir().join("tmp");
        let key = format!("{}-{}", node.0.simple(), &expected.to_hex()[..16]);
        let mut partial = match PartialFile::open(&tmp, &key, expected.algorithm) {
            Ok(p) => p,
            Err(e) => return Ok(io_outcome(e)),
        };
        let offset = partial.offset();
        let range = (offset > 0).then_some(offset..file.size);
        match self
            .remote
            .read(mirror.file_id, range, &mut WriteTo(&mut partial))
        {
            Ok(_) => {}
            Err(e) => return Ok(remote_outcome(e)),
        }
        let done = match partial.finish(Some(expected), Some(file.size)) {
            Ok(d) => d,
            Err(FileIoError::HashMismatch { .. } | FileIoError::SizeMismatch { .. }) => {
                return Ok(Outcome::Retry(
                    "downloaded bytes don't match the remote hash".into(),
                ));
            }
            Err(e) => return Ok(Outcome::Retry(e.to_string())),
        };
        let exec = if self.cfg.os == Os::Windows {
            None
        } else {
            Some(file.executable)
        };
        if let Err(e) = done.set_attributes(Some(file.mtime_ms.saturating_mul(1_000_000)), exec) {
            return Ok(io_outcome(e));
        }
        let local_hash = if expected.algorithm == self.cfg.algorithm {
            expected
        } else {
            match hash_file(done.path(), self.cfg.algorithm) {
                Ok(h) => h.hash,
                Err(e) => return Ok(Outcome::Retry(e.to_string())),
            }
        };

        // Into place: over the old version (if it's still what the plan saw), or as a new file.
        let placed = match &current {
            Some(row) if row.path == target => {
                done.persist_over(&self.abs(&target), &row.fingerprint)
            }
            Some(row) => {
                if !self.free_target(&target, Some(node))? {
                    let _ = done.discard();
                    return Ok(Outcome::Retry("name taken".into()));
                }
                // Move the old version aside to the trash, then place the new one.
                match self.move_to_trash(&row.path, node) {
                    Ok(()) => done.persist_new(&self.abs(&target)),
                    Err(e) => {
                        let _ = done.discard();
                        return Ok(io_outcome(e));
                    }
                }
            }
            None => {
                if !self.free_target(&target, Some(node))? {
                    let _ = done.discard();
                    return Ok(Outcome::Retry("name taken".into()));
                }
                done.persist_new(&self.abs(&target))
            }
        };
        let meta = match placed {
            Ok(m) => m,
            Err(FileIoError::TargetChanged) => return Ok(rescan(&mut self.report, &target)),
            Err(FileIoError::Io(e)) => return Ok(io_outcome(e)),
            Err(e) => return Ok(Outcome::Retry(e.to_string())),
        };
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

    fn create(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
    ) -> Result<Outcome, StateError> {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone locally".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let (content, payload) = match self.content_for(&row, name) {
            Ok(x) => x,
            Err(o) => return Ok(o),
        };
        let new = NewNode {
            node,
            parent,
            kind,
            content: content.clone(),
        };
        let written = match payload {
            None => self.remote.create(&new, None),
            Some((hash, size)) => {
                let mut file = match fs::File::open(self.abs(&row.path)) {
                    Ok(f) => f,
                    Err(e) => return Ok(io_outcome(e)),
                };
                self.remote.create(
                    &new,
                    Some(Upload {
                        hash,
                        size,
                        reader: &mut file,
                    }),
                )
            }
        };
        let written = match written {
            Ok(w) => w,
            // An earlier attempt landed; the feed brings its header.
            Err(RemoteError::AlreadyExists) => return Ok(Outcome::Done),
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.db.write(|s| {
            s.put_mirror(&mirror_from(
                node,
                parent,
                kind,
                &content,
                payload.map(|p| p.0),
                written,
                None,
            ))
        })?;
        Ok(Outcome::Done)
    }

    #[allow(clippy::too_many_arguments)]
    fn update(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
        parent: NodeId,
        name: &str,
        upload: bool,
    ) -> Result<Outcome, StateError> {
        let (mirror, row) = self
            .db
            .read(|s| Ok((s.get_mirror(node)?, s.local_by_node(node)?)))?;
        let Some(mirror) = mirror else {
            return Ok(Outcome::Skip("gone remotely".into()));
        };
        let extra = mirror.content_extra.clone();
        let (content, payload, source) = if upload {
            let Some(row) = row else {
                return Ok(Outcome::Skip("gone locally".into()));
            };
            if !self.on_disk_unchanged(&row) {
                return Ok(rescan(&mut self.report, &row.path));
            }
            match self.content_for(&row, name) {
                Ok((c, p)) => (c, p, Some(row.path)),
                Err(o) => return Ok(o),
            }
        } else {
            let content = match (&mirror.kind, &mirror.file) {
                (NodeKind::File, Some(f)) => {
                    NodeContent::file(name, f.mtime_ms, f.size, f.executable)
                }
                _ => NodeContent::folder(name),
            };
            (content, None, None)
        };
        let mut content = content;
        if let Some(extra) = &extra
            && let Ok(serde_json::Value::Object(map)) = serde_json::from_str(extra)
        {
            content.unknown = map;
        }
        let written = match (payload, &source) {
            (Some((hash, size)), Some(path)) => {
                let mut file = match fs::File::open(self.abs(path)) {
                    Ok(f) => f,
                    Err(e) => return Ok(io_outcome(e)),
                };
                self.remote.update(
                    file_id,
                    expected,
                    parent,
                    &content,
                    Some(Upload {
                        hash,
                        size,
                        reader: &mut file,
                    }),
                )
            }
            _ => self
                .remote
                .update(file_id, expected, parent, &content, None),
        };
        let written = match written {
            Ok(w) => w,
            Err(e) => return Ok(remote_outcome(e)),
        };
        let hash = payload.map(|p| p.0).or(mirror.file.and_then(|f| f.hash));
        self.db.write(|s| {
            s.put_mirror(&mirror_from(
                node,
                parent,
                mirror.kind,
                &content,
                hash,
                written,
                extra.clone(),
            ))
        })?;
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

/// Adapts a [`PartialFile`] to `io::Write` for the remote's sink.
struct WriteTo<'a>(&'a mut PartialFile);

impl io::Write for WriteTo<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_all(buf)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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
            db.write(|s| {
                s.move_local_subtree(&parked, &original)?;
                s.delete_parked(node)
            })?;
            restored += 1;
        }
    }
    Ok(restored)
}
