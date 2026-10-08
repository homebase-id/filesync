//! Merging a walk into the state DB: the local half of catch-up.
//!
//! 1. **Streaming merge** of the walk against `local_items`, both in [`RelPath`] order.
//!    Unchanged entries cost one comparison; only differences are kept in memory.
//! 2. **Identity** (sync-semantics §3): a vanished path whose file id or (for files) content
//!    hash shows up at a new path is a move and keeps its node id; everything else new gets a
//!    fresh node id.
//! 3. **Hashing** only for files whose fingerprint changed (never at start for unchanged files).
//! 4. **One transaction** updates `local_items` and marks every node whose local state changed
//!    as dirty.

use std::collections::HashMap;
use std::path::Path;

use hbsync_fileio::{FileIoError, HashAlgorithm, hash_file};
use hbsync_platform::{EntryKind, FileId, FsFamily, LocalMeta, RelPath};
use hbsync_state::{LocalRow, StateDb, StateError};
use hbsync_types::{ContentHash, NodeId, NodeKind};

use crate::walk::{SkipReason, WalkEvent, Walker};

/// Rows read from the DB per page during the merge.
const PAGE: usize = 2000;

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub algorithm: HashAlgorithm,
    /// Decides whether file ids can be trusted for move detection.
    pub fs_family: FsFamily,
    /// Root-level names the walk skips (the client's own state folder).
    pub exclude_at_root: Vec<String>,
}

/// What a scan found and did.
#[derive(Debug, Default)]
pub struct ScanReport {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
    pub moved: usize,
    pub hashed: usize,
    /// Nodes marked dirty.
    pub dirty: usize,
    /// Files that couldn't be hashed (locked, changing). They stay dirty and unhashed.
    pub unhashed: Vec<(RelPath, String)>,
    /// Folders whose contents are unknown this time; their DB rows were left alone.
    pub unreadable: Vec<RelPath>,
    pub skipped: Vec<(std::path::PathBuf, SkipReason)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error(transparent)]
    State(#[from] StateError),
}

/// Runs a full tier-C scan of `root` and records the result in `db`.
pub fn scan(db: &mut StateDb, root: &Path, opts: &ScanOptions) -> Result<ScanReport, ScanError> {
    let excludes: Vec<&str> = opts.exclude_at_root.iter().map(String::as_str).collect();
    scan_events(db, root, Walker::new(root, &excludes), opts)
}

/// Like [`scan`], over any event source in walk order (used by tests and the journal path).
pub fn scan_events(
    db: &mut StateDb,
    root: &Path,
    events: impl Iterator<Item = WalkEvent>,
    opts: &ScanOptions,
) -> Result<ScanReport, ScanError> {
    let mut report = ScanReport::default();
    let diff = merge(db, events, &mut report)?;
    let plan = resolve(db, root, diff, opts, &mut report)?;
    apply(db, plan, &mut report)?;
    Ok(report)
}

/// The differences between the walk and the DB.
#[derive(Debug, Default)]
struct Diff {
    added: Vec<(RelPath, LocalMeta)>,
    removed: Vec<LocalRow>,
    changed: Vec<(LocalRow, LocalMeta)>,
}

fn merge(
    db: &mut StateDb,
    events: impl Iterator<Item = WalkEvent>,
    report: &mut ScanReport,
) -> Result<Diff, ScanError> {
    let mut diff = Diff::default();
    let mut rows = DbRows::new();
    let mut unknown: Vec<RelPath> = Vec::new();
    let under_unknown =
        |unknown: &[RelPath], p: &RelPath| unknown.iter().any(|u| p != u && p.starts_with(u));

    for event in events {
        let (path, meta) = match event {
            WalkEvent::Entry { path, meta } => (path, meta),
            WalkEvent::Unreadable { path, .. } => {
                report.unreadable.push(path.clone());
                unknown.push(path);
                continue;
            }
            WalkEvent::Skipped { path, reason } => {
                report.skipped.push((path, reason));
                continue;
            }
        };
        // DB rows before this path are gone from disk (unless their subtree was unreadable).
        while let Some(row) = rows.peek(db)? {
            if row.path >= path {
                break;
            }
            let row = rows.take();
            if !under_unknown(&unknown, &row.path) {
                diff.removed.push(row);
            }
        }
        match rows.peek(db)? {
            Some(row) if row.path == path => {
                let row = rows.take();
                if differs(&row, &meta) {
                    diff.changed.push((row, meta));
                }
            }
            _ => diff.added.push((path, meta)),
        }
    }
    while let Some(row) = rows.next(db)? {
        if !under_unknown(&unknown, &row.path) {
            diff.removed.push(row);
        }
    }
    Ok(diff)
}

fn differs(row: &LocalRow, meta: &LocalMeta) -> bool {
    kind_of(meta) != row.kind
        || row.fingerprint != meta.fingerprint()
        || (row.kind == NodeKind::File && meta.executable.is_some() && meta.executable != row.executable)
        // A file that couldn't be hashed last time gets another try.
        || (row.kind == NodeKind::File && row.hash.is_none())
}

fn kind_of(meta: &LocalMeta) -> NodeKind {
    if meta.kind == EntryKind::Dir {
        NodeKind::Folder
    } else {
        NodeKind::File
    }
}

/// Pages through `local_items` in walk order.
struct DbRows {
    page: std::collections::VecDeque<LocalRow>,
    last: Option<RelPath>,
    done: bool,
}

impl DbRows {
    fn new() -> Self {
        DbRows {
            page: Default::default(),
            last: None,
            done: false,
        }
    }

    fn fill(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        if self.page.is_empty() && !self.done {
            let rows = db.read(|s| s.local_page(self.last.as_ref(), PAGE))?;
            self.done = rows.len() < PAGE;
            self.last = rows.last().map(|r| r.path.clone());
            self.page.extend(rows);
        }
        Ok(())
    }

    fn peek(&mut self, db: &mut StateDb) -> Result<Option<&LocalRow>, StateError> {
        self.fill(db)?;
        Ok(self.page.front())
    }

    fn next(&mut self, db: &mut StateDb) -> Result<Option<LocalRow>, StateError> {
        self.fill(db)?;
        Ok(self.page.pop_front())
    }

    fn take(&mut self) -> LocalRow {
        self.page
            .pop_front()
            .unwrap_or_else(|| unreachable!("take after a successful peek"))
    }
}

/// The writes a scan will make.
#[derive(Debug, Default)]
struct Plan {
    delete_paths: Vec<RelPath>,
    put_rows: Vec<LocalRow>,
    dirty: Vec<NodeId>,
}

fn resolve(
    db: &mut StateDb,
    root: &Path,
    diff: Diff,
    opts: &ScanOptions,
    report: &mut ScanReport,
) -> Result<Plan, ScanError> {
    let mut plan = Plan::default();
    let hash_now =
        |path: &RelPath, meta: &LocalMeta, report: &mut ScanReport| -> Option<ContentHash> {
            match hash_file(&path.to_native(root), opts.algorithm) {
                Ok(h) if h.meta.fingerprint() == meta.fingerprint() => {
                    report.hashed += 1;
                    Some(h.hash)
                }
                Ok(_) => {
                    report
                        .unhashed
                        .push((path.clone(), FileIoError::ChangedWhileReading.to_string()));
                    None
                }
                Err(e) => {
                    report.unhashed.push((path.clone(), e.to_string()));
                    None
                }
            }
        };

    // Changed in place. A kind change (file ↔ folder) is a delete plus a create.
    let mut removed = diff.removed;
    let mut added = diff.added;
    for (row, meta) in diff.changed {
        if kind_of(&meta) != row.kind {
            added.push((row.path.clone(), meta));
            removed.push(row);
            continue;
        }
        let mut new = row_from(&row.path, row.node, &meta, row.hash);
        let mut content_changed = false;
        if row.kind == NodeKind::File
            && (row.fingerprint != meta.fingerprint() || row.hash.is_none())
        {
            new.hash = hash_now(&row.path, &meta, report);
            content_changed = new.hash.is_none() || new.hash != row.hash;
        }
        let x_changed = row.kind == NodeKind::File
            && meta.executable.is_some()
            && meta.executable != row.executable;
        if content_changed || x_changed {
            report.modified += 1;
            plan.dirty.push(row.node);
        }
        plan.put_rows.push(new);
    }

    // Moves by file id.
    let mut removed_by_id: HashMap<FileId, usize> = HashMap::new();
    if opts.fs_family.has_stable_file_ids() {
        for (i, row) in removed.iter().enumerate() {
            removed_by_id.insert(row.fingerprint.file_id, i);
        }
    }
    let mut matched = vec![false; removed.len()];
    let mut fresh: Vec<(RelPath, LocalMeta)> = Vec::new();
    // node id → (old path, new path) for every moved node, to decide which moves are real.
    let mut moved: Vec<(LocalRow, RelPath)> = Vec::new();
    for (path, meta) in added {
        let hit = removed_by_id
            .get(&meta.file_id)
            .copied()
            .filter(|&i| !matched[i] && removed[i].kind == kind_of(&meta));
        let Some(i) = hit else {
            fresh.push((path, meta));
            continue;
        };
        matched[i] = true;
        let old = &removed[i];
        let mut new = row_from(&path, old.node, &meta, old.hash);
        if old.kind == NodeKind::File {
            // A rename changes ctime on most file systems; size and mtime are what matter here.
            let same =
                old.fingerprint.size == meta.size && old.fingerprint.mtime_ns == meta.mtime_ns;
            if !same || old.hash.is_none() {
                new.hash = hash_now(&path, &meta, report);
                if new.hash.is_none() || new.hash != old.hash {
                    report.modified += 1;
                    plan.dirty.push(old.node);
                }
            }
        }
        moved.push((old.clone(), path));
        plan.put_rows.push(new);
    }

    // Moves by content hash (rule 3): a new file whose hash matches exactly one vanished file.
    let mut by_hash: HashMap<ContentHash, Vec<usize>> = HashMap::new();
    for (i, row) in removed.iter().enumerate() {
        if let (false, NodeKind::File, Some(h)) = (matched[i], row.kind, row.hash) {
            by_hash.entry(h).or_default().push(i);
        }
    }
    let mut new_nodes = Vec::new();
    for (path, meta) in fresh {
        let kind = kind_of(&meta);
        let hash = if kind == NodeKind::File {
            hash_now(&path, &meta, report)
        } else {
            None
        };
        let candidate = hash
            .and_then(|h| by_hash.get(&h))
            .filter(|c| c.len() == 1)
            .map(|c| c[0])
            .filter(|&i| !matched[i] && removed[i].fingerprint.size == meta.size);
        if let Some(i) = candidate {
            matched[i] = true;
            let old = &removed[i];
            moved.push((old.clone(), path.clone()));
            plan.put_rows.push(LocalRow {
                hash,
                ..row_from(&path, old.node, &meta, None)
            });
            continue;
        }
        let node = NodeId::new();
        report.added += 1;
        plan.dirty.push(node);
        new_nodes.push((path.clone(), node));
        plan.put_rows.push(LocalRow {
            hash,
            ..row_from(&path, node, &meta, None)
        });
    }

    // Unmatched vanished rows are local deletes.
    for (i, row) in removed.iter().enumerate() {
        plan.delete_paths.push(row.path.clone());
        if !matched[i] {
            report.removed += 1;
            plan.dirty.push(row.node);
        }
    }

    // A move is only a change for the node if its parent node or its name changed. Children of
    // a moved folder keep both, so they aren't dirtied one by one.
    let mut new_path_node: HashMap<RelPath, NodeId> = moved
        .iter()
        .map(|(old, new)| (new.clone(), old.node))
        .collect();
    new_path_node.extend(new_nodes);
    let old_path_node: HashMap<RelPath, NodeId> =
        removed.iter().map(|r| (r.path.clone(), r.node)).collect();
    for (old, new_path) in &moved {
        let old_parent = parent_node(db, &old.path, &old_path_node)?;
        let new_parent = parent_node(db, new_path, &new_path_node)?;
        if old.path.name() != new_path.name() || old_parent != new_parent {
            report.moved += 1;
            plan.dirty.push(old.node);
        }
    }
    Ok(plan)
}

/// The node of `path`'s parent folder, preferring this scan's view over the DB's.
fn parent_node(
    db: &mut StateDb,
    path: &RelPath,
    overrides: &HashMap<RelPath, NodeId>,
) -> Result<Option<NodeId>, StateError> {
    let Some(parent) = path.parent() else {
        return Ok(None);
    };
    if parent.is_root() {
        return Ok(Some(NodeId::ROOT));
    }
    if let Some(node) = overrides.get(&parent) {
        return Ok(Some(*node));
    }
    Ok(db.read(|s| s.get_local(&parent))?.map(|r| r.node))
}

fn row_from(path: &RelPath, node: NodeId, meta: &LocalMeta, hash: Option<ContentHash>) -> LocalRow {
    let kind = kind_of(meta);
    LocalRow {
        path: path.clone(),
        node,
        kind,
        fingerprint: meta.fingerprint(),
        // The x bit is synced for files only; a folder's search bit means nothing remotely.
        executable: if kind == NodeKind::File {
            meta.executable
        } else {
            None
        },
        hash: if kind == NodeKind::File { hash } else { None },
    }
}

fn apply(db: &mut StateDb, plan: Plan, report: &mut ScanReport) -> Result<(), StateError> {
    let mut dirty = plan.dirty;
    dirty.sort();
    dirty.dedup();
    report.dirty = dirty.len();
    db.write(|s| {
        // Deletes first, so a node that moved can take its new path.
        for path in &plan.delete_paths {
            s.delete_local(path)?;
        }
        for row in &plan.put_rows {
            s.put_local(row)?;
        }
        for node in &dirty {
            s.mark_dirty(*node)?;
        }
        Ok(())
    })
}
