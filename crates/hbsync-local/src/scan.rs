//! Merging a walk into the state DB: the local half of catch-up.
//!
//! 1. **Streaming merge** of the walk against the `local` rows, both in [`RelPath`] order (the
//!    DB side is read folder by folder, depth first). Unchanged entries cost one comparison;
//!    only differences are kept in memory.
//! 2. **Identity** (sync-semantics §3): a vanished path whose file id or (for files) content
//!    hash shows up at a new path is a move and keeps its node id, and so is a vanished file's
//!    id showing up at a known path (moved onto that name); everything else new gets a fresh
//!    node id.
//! 3. **Hashing** only for files whose fingerprint changed (never at start for unchanged files).
//! 4. **Tags** are read with the metadata of every new or changed item (a tag change moves the
//!    change time).
//! 5. **One transaction** updates the `local` rows and marks every node whose local state changed
//!    as dirty.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use hbsync_fileio::{FileIoError, HashAlgorithm, hash_file};
use hbsync_platform::{EntryKind, FileId, FsFamily, LocalMeta, RelPath, tags};
use hbsync_state::{LocalRow, StateDb, StateError};
use hbsync_types::{ContentHash, NodeId, NodeKind, Tags};

use crate::filter::Filter;
use crate::walk::{SkipReason, WalkEvent, Walker};

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub algorithm: HashAlgorithm,
    /// Decides whether file ids can be trusted for move detection.
    pub fs_family: FsFamily,
    /// What is never synced (the client's own state folder, junk, ignored items).
    pub filter: Arc<Filter>,
}

impl ScanOptions {
    /// The options for a sync root: its file system family (`Other` if it can't be told) and
    /// only the built-in filter (the client's state folder and junk).
    pub fn for_root(root: &Path, algorithm: HashAlgorithm) -> Self {
        ScanOptions {
            algorithm,
            fs_family: hbsync_platform::fskind::detect(root).unwrap_or(FsFamily::Other),
            filter: Arc::default(),
        }
    }
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
    /// Folders seen for the first time.
    pub new_folders: Vec<RelPath>,
}

/// Runs a full tier-C scan of `root` and records the result in `db`.
pub fn scan(db: &mut StateDb, root: &Path, opts: &ScanOptions) -> Result<ScanReport, StateError> {
    let mut report = ScanReport::default();
    let diff = merge(
        db,
        Walker::new(root, &opts.filter),
        DbRows::tree(),
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
    let mut targets = std::collections::BTreeSet::new();
    for dir in dirs {
        // Never look inside filtered folders (the client's own state, junk, ignored folders),
        // whoever reported them. Their parent's rescan notices one that just became filtered.
        if !opts.filter.path(dir, true).is_sync() {
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
        // Only the direct children are loaded up front; deeper rows only for the folders
        // that are walked in full or gone (a rescan of the root must not read the whole DB).
        let children = db.read(|s| s.local_children(&dir))?;
        let is_child = |p: &RelPath| p.parent().as_ref() == Some(&dir);
        let known_children: HashMap<RelPath, (NodeKind, FileId)> = children
            .iter()
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
            let walker = Walker::at(root, dir.clone(), &opts.filter, Some(Box::new(descend)));
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
        let gone = known_children.keys().filter(|p| !present.contains(*p));
        let mut rows = children;
        for deep in descended.iter().chain(gone) {
            let below = db.read(|s| s.local_subtree(deep))?;
            rows.extend(below.into_iter().filter(|r| r.path != *deep));
        }
        rows.sort_by(|a, b| a.path.cmp(&b.path)); // walk order
        let part = merge(db, events.into_iter(), DbRows::fixed(rows), &mut report)?;
        diff.added.extend(part.added);
        diff.removed.extend(part.removed);
        diff.changed.extend(part.changed);
        diff.unknown.extend(part.unknown);
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
    /// Folders whose contents couldn't be read this time.
    unknown: Vec<RelPath>,
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
            let row = rows.take(db)?;
            if !under_unknown(&unknown, &row.path) {
                diff.removed.push(row);
            }
        }
        match rows.peek(db)? {
            Some(row) if row.path == path => {
                let row = rows.take(db)?;
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
    diff.unknown = unknown;
    Ok(diff)
}

/// The known paths another synced file now stands at (§3 rule 1): a file at a known path
/// whose new file id is that of a synced file which left its own path (gone from it, moved
/// onto yet another name, or swapped with this one) was moved onto this name. Where file ids
/// are recycled, only with the bytes it had. Anything else at the same path is the node that
/// was there: an editor's save through a temp file brings a new id (and the temp file, if a
/// scan saw it, was never synced), one keeping a backup by renaming leaves the path. `hash`
/// hashes what is at a path now.
fn moved_onto(
    db: &mut StateDb,
    removed: &[LocalRow],
    changed: &[(LocalRow, LocalMeta)],
    opts: &ScanOptions,
    hash: &mut dyn FnMut(&RelPath, &LocalMeta) -> Option<ContentHash>,
) -> Result<HashSet<RelPath>, StateError> {
    let mut onto = HashSet::new();
    let is_file = |m: &LocalMeta| kind_of(m) == NodeKind::File;
    let switched: Vec<&(LocalRow, LocalMeta)> = changed
        .iter()
        .filter(|(r, m)| {
            r.kind == NodeKind::File && is_file(m) && m.file_id != r.fingerprint.file_id
        })
        .collect();
    if switched.is_empty() || !opts.fs_family.has_stable_file_ids() {
        return Ok(onto);
    }
    // Files that left their path, by the id they had there.
    let mut left: HashMap<FileId, &LocalRow> = removed
        .iter()
        .filter(|r| r.kind == NodeKind::File)
        .map(|r| (r.fingerprint.file_id, r))
        .collect();
    // Known paths that hold another file now, by the id they had: two holding each other's
    // file swapped.
    let held: HashMap<FileId, &(LocalRow, LocalMeta)> = switched
        .iter()
        .map(|&pair| (pair.0.fingerprint.file_id, pair))
        .collect();
    loop {
        let mut more = false;
        for &(row, meta) in &switched {
            if onto.contains(&row.path) {
                continue;
            }
            let from = left.get(&meta.file_id).copied().or_else(|| {
                held.get(&meta.file_id)
                    .filter(|(_, m)| m.file_id == row.fingerprint.file_id)
                    .map(|(r, _)| r)
            });
            let Some(from) = from else {
                continue;
            };
            let synced = db.read(|s| s.get_base(from.node))?.is_some();
            let same = !opts.fs_family.reuses_file_ids() || {
                let h = hash(&row.path, meta);
                h.is_some() && h == from.hash
            };
            if synced && same {
                onto.insert(row.path.clone());
                left.insert(row.fingerprint.file_id, row);
                more = true;
            }
        }
        if !more {
            return Ok(onto);
        }
    }
}

fn differs(row: &LocalRow, meta: &LocalMeta) -> bool {
    kind_of(meta) != row.kind
        || row.fingerprint != meta.fingerprint()
        || (row.kind == NodeKind::File && meta.executable.is_some() && meta.executable != row.executable)
        // A file that couldn't be hashed last time gets another try, and so do tags.
        || (row.kind == NodeKind::File && row.hash.is_none())
        || (tags::CAN_TAG && row.tags.is_none())
}

/// The node kind of an entry the scan keeps (a folder, else a file).
pub fn kind_of(meta: &LocalMeta) -> NodeKind {
    if meta.kind == EntryKind::Dir {
        NodeKind::Folder
    } else {
        NodeKind::File
    }
}

/// The DB's rows in walk order: the whole tree depth first, a folder's children loaded when
/// the folder is reached (memory grows with depth times width, not with the tree), or a fixed,
/// already sorted set of rows.
///
/// Depth first with children sorted by name is walk order: `/` sorts below every other byte,
/// so everything under `a` comes before a sibling such as `a-b`.
struct DbRows {
    /// One level per open folder; the front of the last level is next.
    levels: Vec<std::collections::VecDeque<LocalRow>>,
    /// Load each folder's children when it is taken (the full walk); off for fixed rows.
    descend: bool,
    started: bool,
}

impl DbRows {
    fn tree() -> Self {
        DbRows {
            levels: Vec::new(),
            descend: true,
            started: false,
        }
    }

    fn fixed(rows: Vec<LocalRow>) -> Self {
        DbRows {
            levels: vec![rows.into()],
            descend: false,
            started: true,
        }
    }

    fn fill(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        if !self.started {
            self.started = true;
            let top = db.read(|s| s.local_children(&RelPath::root()))?;
            self.levels.push(top.into());
        }
        while self.levels.last().is_some_and(|l| l.is_empty()) {
            self.levels.pop();
        }
        Ok(())
    }

    fn peek(&mut self, db: &mut StateDb) -> Result<Option<&LocalRow>, StateError> {
        self.fill(db)?;
        Ok(self.levels.last().and_then(|l| l.front()))
    }

    fn next(&mut self, db: &mut StateDb) -> Result<Option<LocalRow>, StateError> {
        if self.peek(db)?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.take(db)?))
    }

    /// Takes the next row (after a successful peek); a folder's children come next.
    fn take(&mut self, db: &mut StateDb) -> Result<LocalRow, StateError> {
        let row = self
            .levels
            .last_mut()
            .and_then(|l| l.pop_front())
            .unwrap_or_else(|| unreachable!("take after a successful peek"));
        if self.descend && row.kind == NodeKind::Folder {
            let children = db.read(|s| s.local_children_of(&row))?;
            if !children.is_empty() {
                self.levels.push(children.into());
            }
        }
        Ok(row)
    }
}

/// The writes a scan will make.
#[derive(Debug, Default)]
struct Plan {
    /// Rows to delete, by their old path (moved nodes get theirs back in `put_rows`).
    delete: Vec<(RelPath, NodeId)>,
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

    // A row for what the walk found at `path`, with its tags (carried over from `old` when
    // they can't have changed or can't be read now), and whether its tags changed.
    let row_from = |path: &RelPath,
                    meta: &LocalMeta,
                    hash: Option<ContentHash>,
                    node: NodeId,
                    old: Option<&LocalRow>| {
        let tags = match old {
            // A tag write moves the change time; inside a moved folder, nothing does.
            Some(o) if o.tags.is_some() && o.fingerprint.ctime_ns == meta.ctime_ns => {
                o.tags.clone()
            }
            _ => tags::read(&path.to_native(root)).or_else(|| old.and_then(|o| o.tags.clone())),
        };
        let changed = old.is_some_and(|o| tags_changed(&o.tags, &tags));
        (row_from(path, node, meta, hash, tags), changed)
    };

    // Changed in place. A kind change (file ↔ folder) is a delete plus a create, and so is a
    // known path another synced file was moved onto (§3 rule 1, see `moved_onto`).
    let mut removed = diff.removed;
    let mut added = diff.added;
    // Hashes taken to tell, used again below.
    let mut hashed = HashMap::new();
    let onto = moved_onto(db, &removed, &diff.changed, opts, &mut |p, m| {
        *hashed
            .entry(p.clone())
            .or_insert_with(|| hash_now(p, m, report))
    })?;
    let mut hash_of = |p: &RelPath, m: &LocalMeta, report: &mut ScanReport| {
        hashed.remove(p).unwrap_or_else(|| hash_now(p, m, report))
    };
    for (row, meta) in diff.changed {
        if kind_of(&meta) != row.kind || onto.contains(&row.path) {
            added.push((row.path.clone(), meta));
            removed.push(row);
            continue;
        }
        let (mut new, tags_changed) = row_from(&row.path, &meta, row.hash, row.node, Some(&row));
        let mut content_changed = false;
        if row.kind == NodeKind::File
            && (row.fingerprint != meta.fingerprint() || row.hash.is_none())
        {
            new.hash = hash_of(&row.path, &meta, report);
            content_changed = new.hash.is_none() || new.hash != row.hash;
        }
        let x_changed = row.kind == NodeKind::File
            && meta.executable.is_some()
            && meta.executable != row.executable;
        if content_changed || x_changed || tags_changed {
            report.modified += 1;
            plan.dirty.push(row.node);
        }
        // Tags that still can't be read leave the row as it was.
        if new != row {
            plan.put_rows.push(new);
        }
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
    // (old row, new path) for every node matched by file id, to decide which moves are real.
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
        let (mut new, tags_changed) = row_from(&path, &meta, old.hash, old.node, Some(old));
        if tags_changed {
            plan.dirty.push(old.node);
        }
        if old.kind == NodeKind::File {
            let same = old.fingerprint.same_after_rename(&meta);
            // Where inode numbers get recycled, a matching id plus size and mtime is still not
            // proof (an extracted or rsynced file can match all three). It is when the file
            // moved together with its folder: same name inside a folder that moved as well.
            let moved_with_parent = old.path.name() == path.name()
                && old.path.parent().and_then(|p| moved_folders.get(&p)) == path.parent().as_ref();
            let trusted = same
                && old.hash.is_some()
                && (!opts.fs_family.reuses_file_ids() || moved_with_parent);
            if !trusted {
                new.hash = hash_of(&path, &meta, report);
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

    // A folder that moved to where it can't be read now took its contents along: they aren't
    // gone, they follow it unchanged (the next readable scan checks them).
    let unreadable = |p: &RelPath| diff.unknown.iter().any(|u| p.starts_with(u));
    for (i, row) in removed.iter().enumerate() {
        if matched[i] {
            continue;
        }
        let Some((old_dir, new_dir)) = moved_folders
            .iter()
            .filter(|(old, new)| row.path.starts_with(old) && row.path != **old && unreadable(new))
            .max_by_key(|(old, _)| old.as_str().len())
        else {
            continue;
        };
        let rest = &row.path.as_str()[old_dir.as_str().len() + 1..];
        let Ok(new_path) = RelPath::new(&format!("{new_dir}/{rest}")) else {
            continue;
        };
        matched[i] = true;
        moved.push((row.clone(), new_path.clone()));
        plan.put_rows.push(LocalRow {
            path: new_path,
            ..row.clone()
        });
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
            let (new, tags_changed) = row_from(&path, &meta, hash, old.node, Some(old));
            if tags_changed {
                plan.dirty.push(old.node);
            }
            plan.put_rows.push(new);
            continue;
        }
        let node = NodeId::new();
        report.added += 1;
        if kind_of(&meta) == NodeKind::Folder {
            report.new_folders.push(path.clone());
        }
        plan.dirty.push(node);
        new_nodes.push((path.clone(), node));
        plan.put_rows
            .push(row_from(&path, &meta, hash, node, None).0);
    }

    // Unmatched vanished rows are local deletes.
    for (i, row) in removed.iter().enumerate() {
        plan.delete.push((row.path.clone(), row.node));
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

/// Whether tags read as `new` differ from `old`. No tags where none were known (no tags on this
/// system, or the read failed) aren't a change. Tags that can't be read now are the ones known
/// before, so that removing every tag later is still seen.
fn tags_changed(old: &Option<Tags>, new: &Option<Tags>) -> bool {
    match (old, new) {
        (None, new) => new.as_ref().is_some_and(|t| !t.is_empty()),
        (Some(old), new) => new.as_ref() != Some(old),
    }
}

/// The row of the item at `path` the scan keeps, as `node`, with what was read of it.
pub fn row_from(
    path: &RelPath,
    node: NodeId,
    meta: &LocalMeta,
    hash: Option<ContentHash>,
    tags: Option<Tags>,
) -> LocalRow {
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
        tags,
    }
}

fn apply(db: &mut StateDb, mut plan: Plan, report: &mut ScanReport) -> Result<(), StateError> {
    let mut dirty = plan.dirty;
    dirty.sort();
    dirty.dedup();
    report.dirty = dirty.len();
    // Deletes first, so a node that moved can take its new place, and contents before their
    // folder; then new rows parents first (walk order).
    plan.delete.sort_by(|a, b| b.0.cmp(&a.0));
    plan.put_rows.sort_by(|a, b| a.path.cmp(&b.path));
    db.write(|s| {
        for (_, node) in &plan.delete {
            s.delete_local(*node)?;
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
