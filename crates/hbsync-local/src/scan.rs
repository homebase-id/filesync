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

/// Runs a full tier-C scan of `root` and records the result in `db`.
pub fn scan(db: &mut StateDb, root: &Path, opts: &ScanOptions) -> Result<ScanReport, StateError> {
    let excludes: Vec<&str> = opts.exclude_at_root.iter().map(String::as_str).collect();
    let mut report = ScanReport::default();
    let diff = merge(
        db,
        Walker::new(root, &excludes),
        DbRows::paged(),
        &mut report,
    )?;
    let plan = resolve(db, root, diff, opts, &mut report)?;
    apply(db, plan, &mut report)?;
    Ok(report)
}

/// Rescans only the given folders: their direct entries, plus the full contents of any child
/// folder that is new or was replaced. This is what a journal or watcher event leads to.
///
/// A folder that no longer exists, or that the DB doesn't know yet, is replaced by its nearest
/// known ancestor. Moves between the rescanned folders are detected, since all of them go
/// through one identity pass.
pub fn rescan_dirs(
    db: &mut StateDb,
    root: &Path,
    dirs: &[RelPath],
    opts: &ScanOptions,
) -> Result<ScanReport, StateError> {
    let excludes: Vec<&str> = opts.exclude_at_root.iter().map(String::as_str).collect();
    let mut targets = std::collections::BTreeSet::new();
    for dir in dirs {
        // Never look inside excluded root folders (the client's own state, temp files, trash),
        // whoever reported them.
        if dir
            .components()
            .next()
            .is_some_and(|first| excludes.contains(&first))
        {
            continue;
        }
        // A folder the DB doesn't know yet is new: its parent's rescan finds it and walks it in
        // full (its own creation event may come in a later batch, or never).
        let mut dir = real_dir_prefix(root, dir);
        while !dir.is_root()
            && !db
                .read(|s| s.get_local(&dir))?
                .is_some_and(|r| r.kind == NodeKind::Folder)
        {
            dir = dir.parent().unwrap_or_default();
        }
        targets.insert(dir);
    }

    let mut report = ScanReport::default();
    let mut diff = Diff::default();
    // Subtrees already walked in full; targets inside them are covered.
    let mut covered: Vec<RelPath> = Vec::new();
    for dir in targets {
        if covered.iter().any(|c| dir.starts_with(c)) {
            continue;
        }
        let known = db.read(|s| s.local_subtree(&dir))?;
        let is_child = |p: &RelPath| p.parent().as_ref() == Some(&dir);
        let known_children: HashMap<RelPath, (NodeKind, FileId)> = known
            .iter()
            .filter(|r| is_child(&r.path))
            .map(|r| (r.path.clone(), (r.kind, r.fingerprint.file_id)))
            .collect();

        let mut descended: Vec<RelPath> = Vec::new();
        let mut present: std::collections::HashSet<RelPath> = std::collections::HashSet::new();
        let events: Vec<WalkEvent> = {
            let descend = |path: &RelPath, meta: &LocalMeta| {
                if !is_child(path) {
                    return true; // inside a folder we already decided to walk in full
                }
                let unchanged = known_children.get(path) == Some(&(NodeKind::Folder, meta.file_id));
                if !unchanged {
                    descended.push(path.clone());
                }
                !unchanged
            };
            let walker = Walker::at(root, dir.clone(), &excludes, Some(Box::new(descend)));
            walker
                .inspect(|e| {
                    if let WalkEvent::Entry { path, .. } = e
                        && is_child(path)
                    {
                        present.insert(path.clone());
                    }
                })
                .collect()
        };
        let gone: Vec<&RelPath> = known_children
            .keys()
            .filter(|p| !present.contains(p))
            .collect();
        let rows: Vec<LocalRow> = known
            .into_iter()
            .filter(|r| {
                r.path != dir
                    && (is_child(&r.path)
                        || descended
                            .iter()
                            .chain(gone.iter().copied())
                            .any(|d| r.path.starts_with(d)))
            })
            .collect();
        let part = merge(db, events.into_iter(), DbRows::fixed(rows), &mut report)?;
        diff.added.extend(part.added);
        diff.removed.extend(part.removed);
        diff.changed.extend(part.changed);
        covered.extend(descended);
    }
    let plan = resolve(db, root, diff, opts, &mut report)?;
    apply(db, plan, &mut report)?;
    Ok(report)
}

/// The longest leading part of `dir` that is a real folder: a folder that no longer exists, or
/// that a symlink (or junction) now stands in for, is replaced by its nearest real ancestor.
/// Links are never followed, so a rescan can't reach outside the root through one.
fn real_dir_prefix(root: &Path, dir: &RelPath) -> RelPath {
    let mut real = RelPath::default();
    for name in dir.components() {
        let Ok(next) = real.join(name) else { break };
        match std::fs::symlink_metadata(next.to_native(root)) {
            Ok(m) if m.is_dir() => real = next,
            _ => break,
        }
    }
    real
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
    mut rows: DbRows,
    report: &mut ScanReport,
) -> Result<Diff, StateError> {
    let mut diff = Diff::default();
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

/// Pages through `local_items` in walk order, or serves a fixed, already sorted set of rows.
struct DbRows {
    page: std::collections::VecDeque<LocalRow>,
    last: Option<RelPath>,
    done: bool,
}

impl DbRows {
    fn paged() -> Self {
        DbRows {
            page: Default::default(),
            last: None,
            done: false,
        }
    }

    fn fixed(rows: Vec<LocalRow>) -> Self {
        DbRows {
            page: rows.into(),
            last: None,
            done: true,
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
) -> Result<Plan, StateError> {
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
    // New entries without an identity yet, with their hash if one was already computed.
    let mut fresh: Vec<(RelPath, LocalMeta, Option<Option<ContentHash>>)> = Vec::new();
    // node id → (old path, new path) for every moved node, to decide which moves are real.
    let mut moved: Vec<(LocalRow, RelPath)> = Vec::new();
    // Folders matched by id, old path → new path. Walk order puts a folder before its contents.
    let mut moved_folders: HashMap<RelPath, RelPath> = HashMap::new();
    for (path, meta) in added {
        let hit = removed_by_id
            .get(&meta.file_id)
            .copied()
            .filter(|&i| !matched[i] && removed[i].kind == kind_of(&meta));
        let Some(i) = hit else {
            fresh.push((path, meta, None));
            continue;
        };
        let old = &removed[i];
        let mut new = row_from(&path, old.node, &meta, old.hash);
        if old.kind == NodeKind::File {
            // A rename changes ctime on most file systems; size and mtime are what matter here.
            let same =
                old.fingerprint.size == meta.size && old.fingerprint.mtime_ns == meta.mtime_ns;
            // Where inode numbers get recycled, a matching id plus size and mtime is still not
            // proof (an extracted or rsynced file can match all three). It is when the file
            // moved together with its folder: same name inside a folder that moved as well.
            let moved_with_parent = old.path.name() == path.name()
                && old.path.parent().and_then(|p| moved_folders.get(&p)) == path.parent().as_ref();
            let trusted = same
                && old.hash.is_some()
                && (!opts.fs_family.reuses_file_ids() || moved_with_parent);
            if !trusted {
                new.hash = hash_now(&path, &meta, report);
                let content_changed = new.hash.is_none() || new.hash != old.hash;
                if content_changed && opts.fs_family.reuses_file_ids() {
                    // Probably a recycled inode, not the same file: no identity from the id.
                    fresh.push((path, meta, Some(new.hash)));
                    continue;
                }
                if content_changed {
                    report.modified += 1;
                    plan.dirty.push(old.node);
                }
            }
        }
        matched[i] = true;
        if old.kind == NodeKind::Folder {
            moved_folders.insert(old.path.clone(), path.clone());
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
    for (path, meta, known_hash) in fresh {
        let kind = kind_of(&meta);
        let hash = match known_hash {
            Some(h) => h,
            None if kind == NodeKind::File => hash_now(&path, &meta, report),
            None => None,
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
    if let Some(node) = overrides.get(&parent) {
        return Ok(Some(*node));
    }
    db.read(|s| s.local_node_at(&parent))
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
