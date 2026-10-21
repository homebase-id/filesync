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

use std::collections::{BTreeSet, HashMap, HashSet};
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
    /// The volume tells names apart byte for byte (case-sensitive, no normalization folding),
    /// so a path names one entry whatever else is in its folder: a watcher's event is looked at
    /// by itself ([`rescan_entries`]). Otherwise only what a look at one path can't mistake.
    pub exact_names: bool,
}

impl ScanOptions {
    /// The options for a sync root: its file system family (`Other` if it can't be told) and
    /// only the built-in filter (the client's state folder and junk).
    pub fn for_root(root: &Path, algorithm: HashAlgorithm) -> Self {
        ScanOptions {
            algorithm,
            fs_family: hbsync_platform::fskind::detect(root).unwrap_or(FsFamily::Other),
            filter: Arc::default(),
            exact_names: false,
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
    rescan_entries(db, root, dirs, &[], opts)
}

/// [`rescan_dirs`] for `dirs`, and a look at each of `entries` by itself (what a watcher's
/// events name, all of them: both names of a rename): one `stat` and one row instead of the
/// whole folder. An entry is looked at alone only where that can't mistake anything: a file
/// the filter lets through, in a known folder that is still the one recorded. Where the
/// folder's names are byte-exact ([`ScanOptions::exact_names`], and not a casefold folder) a
/// new name too, if no other row holds its file id; elsewhere only the same file still at its
/// row's path, or a row's name with nothing there. Anything else, and a folder with many
/// entries, rescans the folder.
pub fn rescan_entries(
    db: &mut StateDb,
    root: &Path,
    dirs: &[RelPath],
    entries: &[RelPath],
    opts: &ScanOptions,
) -> Result<ScanReport, StateError> {
    let mut dirs: BTreeSet<RelPath> = dirs.iter().cloned().collect();
    // A folder with many entries is listed instead: one bulk listing beats a stat each.
    let mut per_folder: HashMap<RelPath, usize> = HashMap::new();
    for entry in entries {
        *per_folder
            .entry(entry.parent().unwrap_or_default())
            .or_default() += 1;
    }
    dirs.extend(
        per_folder
            .into_iter()
            .filter(|(_, n)| *n > ENTRIES_PER_FOLDER)
            .map(|(folder, _)| folder),
    );
    let entries: BTreeSet<&RelPath> = entries
        .iter()
        .filter(|e| !dirs.contains(&e.parent().unwrap_or_default()))
        .collect();
    // Each entry's row and its folder's file id, if the folder is a known one (else it is
    // rescanned), and for an entry without a row whether another one there has a name of the
    // same sibling hash (another spelling of it, maybe), in one read.
    let rows = db.read(|s| {
        let mut folders: HashMap<RelPath, Option<Option<FileId>>> = HashMap::new();
        let mut rows = Vec::with_capacity(entries.len());
        for &entry in &entries {
            let parent = entry.parent().unwrap_or_default();
            let folder = match folders.get(&parent) {
                Some(f) => *f,
                None => {
                    let f = known_folder(s, &parent)?;
                    folders.insert(parent.clone(), f);
                    f
                }
            };
            rows.push(match folder {
                Some(id) => {
                    let row = s.get_local(entry)?;
                    let spelled = match (&row, entry.name()) {
                        (None, Some(name)) => {
                            let key = hbsync_namemap::sibling_hash(name);
                            !s.local_named(&parent, key)?.is_empty()
                        }
                        _ => false,
                    };
                    Some((id, row, spelled))
                }
                None => None,
            });
        }
        Ok(rows)
    })?;
    // What the entries looked at alone show. Each folder is looked at once: still the one
    // recorded there (not moved or replaced meanwhile, its contents with it; the root not
    // gone, as with a volume unmounted), a real folder (not a link to elsewhere), and whether
    // its names are byte-exact.
    let mut alone = Diff::default();
    let mut here: HashMap<RelPath, Option<bool>> = HashMap::new();
    for (&entry, row) in entries.iter().zip(rows) {
        let parent = entry.parent().unwrap_or_default();
        let exact = match row {
            Some((folder_id, _, _)) => *here.entry(parent.clone()).or_insert_with(|| {
                let dir = parent.to_native(root);
                match hbsync_platform::meta::stat(&dir) {
                    Ok(m)
                        if m.kind == EntryKind::Dir
                            && folder_id.is_none_or(|id| m.file_id == id) =>
                    {
                        Some(opts.exact_names && !hbsync_platform::fskind::folds_case(&dir))
                    }
                    _ => None,
                }
            }),
            None => None,
        };
        let looked = match (exact, row) {
            (Some(exact), Some((_, row, spelled))) => {
                let names = Names { exact, spelled };
                look_alone(root, entry, row, names, opts, &mut alone)
            }
            _ => false,
        };
        if !looked {
            dirs.insert(entry.parent().unwrap_or_default());
        }
    }
    // A new name whose file id another row holds (one not gone in this batch) was moved or
    // is another spelling of that row: both folders are rescanned together.
    if !alone.added.is_empty() {
        let gone: HashSet<FileId> = alone
            .removed
            .iter()
            .map(|r| r.fingerprint.file_id)
            .collect();
        let held = db.read(|s| {
            let mut held = Vec::new();
            for (path, meta) in &alone.added {
                if gone.contains(&meta.file_id) {
                    continue;
                }
                for row in s.local_by_file_id(&meta.file_id)? {
                    held.push(path.parent().unwrap_or_default());
                    held.push(row.path.parent().unwrap_or_default());
                }
            }
            Ok(held)
        })?;
        dirs.extend(held);
    }
    // What a folder's rescan sees is its own.
    alone.retain(|p| !dirs.contains(&p.parent().unwrap_or_default()));
    let dirs: Vec<RelPath> = dirs.into_iter().collect();
    let mut report = ScanReport::default();
    let mut diff = rescan_diff(db, root, &dirs, opts, &mut report)?;
    if !alone.is_empty() {
        // An entry below a folder the rescan walked in full (or found gone) is its too.
        let mine: HashSet<&RelPath> = alone.paths().collect();
        let walked: HashSet<RelPath> = diff.paths().filter(|p| mine.contains(p)).cloned().collect();
        alone.retain(|p| !walked.contains(p));
        diff.append(alone);
    }
    let plan = resolve(db, root, diff, opts, &mut report)?;
    apply(db, plan, &mut report)?;
    Ok(report)
}

/// Past this many entries in one folder, [`rescan_entries`] lists the folder.
const ENTRIES_PER_FOLDER: usize = 64;

/// Whether `path` is the root or a folder with a row: `Some` with the folder's file id (`None`
/// for the root), else `None`.
fn known_folder(
    s: &hbsync_state::ReadTx<'_>,
    path: &RelPath,
) -> Result<Option<Option<FileId>>, StateError> {
    if path.is_root() {
        return Ok(Some(None));
    }
    Ok(s.get_local(path)?
        .filter(|r| r.kind == NodeKind::Folder)
        .map(|r| Some(r.fingerprint.file_id)))
}

/// What is known of the names in an entry's folder ([`look_alone`]).
#[derive(Clone, Copy)]
struct Names {
    /// The folder's names are byte-exact.
    exact: bool,
    /// Another row there has a name of the entry's sibling hash.
    spelled: bool,
}

/// Looks at `entry` by itself (`row`: its row, if any), as [`rescan_entries`] says, adding
/// what it finds to `diff`; `false` if its folder must be rescanned instead.
fn look_alone(
    root: &Path,
    entry: &RelPath,
    row: Option<LocalRow>,
    names: Names,
    opts: &ScanOptions,
    diff: &mut Diff,
) -> bool {
    // A file the filter lets through (the walk's verdict); anything else is its folder's to
    // judge, and so is a folder.
    if !opts.filter.path(entry, false).is_sync()
        || row.as_ref().is_some_and(|r| r.kind != NodeKind::File)
    {
        return false;
    }
    let meta = match hbsync_platform::meta::stat(&entry.to_native(root)) {
        Ok(meta) if meta.kind == EntryKind::File => Some(meta),
        Ok(_) => return false,
        Err(e) if hbsync_platform::meta::nothing_there(&e) => None,
        Err(_) => return false,
    };
    // Where names aren't byte-exact, only what no other spelling can be: the same file still
    // at its row's path (a rename to another spelling names that spelling too, which rescans
    // the folder); nothing of any spelling where a row was; nothing, or a new file, where no
    // row has a name of its sibling hash (case and normalization; Windows' short names have
    // another, so there an event under a name without a row rescans the folder). A new file
    // whose id a row holds is looked at with that row's folder (`rescan_entries`).
    let Names { exact, spelled } = names;
    let unspelled = exact || !spelled;
    match (row, meta) {
        (None, None) => return unspelled && (exact || !cfg!(windows)),
        (Some(row), None) => diff.removed.push(row),
        (None, Some(meta)) => {
            if !unspelled || !opts.fs_family.has_stable_file_ids() {
                return false;
            }
            diff.added.push((entry.clone(), meta));
        }
        (Some(row), Some(meta)) => {
            if !exact && row.fingerprint.file_id != meta.file_id {
                return false;
            }
            if differs(&row, &meta) {
                diff.changed.push((row, meta));
            }
        }
    }
    true
}

/// The differences a rescan of `dirs` ([`rescan_dirs`]) finds.
fn rescan_diff(
    db: &mut StateDb,
    root: &Path,
    dirs: &[RelPath],
    opts: &ScanOptions,
    report: &mut ScanReport,
) -> Result<Diff, StateError> {
    let mut targets = BTreeSet::new();
    for dir in dirs {
        // Never look inside filtered folders (the client's own state, junk, ignored folders),
        // whoever reported them. Their parent's rescan notices one that just became filtered.
        if !opts.filter.path(dir, true).is_sync() {
            continue;
        }
        // A folder the DB doesn't know yet is new: its parent's rescan finds it and walks it in
        // full (its own creation event may come in a later batch, or never).
        let mut dir = real_dir_prefix(root, dir);
        while db.read(|s| known_folder(s, &dir))?.is_none() {
            dir = dir.parent().unwrap_or_default();
        }
        targets.insert(dir);
    }

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
        diff.append(merge(db, events.into_iter(), DbRows::fixed(rows), report)?);
        covered.extend(descended);
    }
    Ok(diff)
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

impl Diff {
    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }

    /// The paths of the entries it holds.
    fn paths(&self) -> impl Iterator<Item = &RelPath> {
        let added = self.added.iter().map(|(p, _)| p);
        let removed = self.removed.iter().map(|r| &r.path);
        added
            .chain(removed)
            .chain(self.changed.iter().map(|(r, _)| &r.path))
    }

    /// Keeps the entries whose paths `keep` says.
    fn retain(&mut self, mut keep: impl FnMut(&RelPath) -> bool) {
        self.added.retain(|(p, _)| keep(p));
        self.removed.retain(|r| keep(&r.path));
        self.changed.retain(|(r, _)| keep(&r.path));
    }

    fn append(&mut self, other: Diff) {
        self.added.extend(other.added);
        self.removed.extend(other.removed);
        self.changed.extend(other.changed);
        self.unknown.extend(other.unknown);
    }
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
/// whose new file id is that of a synced file which left its own path (gone from it, a folder
/// there now, moved onto yet another name, or swapped with this one) was moved onto this name. Where file ids
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
        .chain(changed.iter().filter(|(_, m)| !is_file(m)).map(|(r, _)| r))
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
    // Nothing to write (a sync's own writes, looked at again): no transaction.
    if plan.delete.is_empty() && plan.put_rows.is_empty() && dirty.is_empty() {
        return Ok(());
    }
    // Deletes first, so a node that moved can take its new place, and contents before their
    // folder; then new rows parents first (walk order).
    plan.delete.sort_by(|a, b| b.0.cmp(&a.0));
    plan.put_rows.sort_by(|a, b| a.path.cmp(&b.path));
    db.write(|s| {
        for (_, node) in &plan.delete {
            s.folder().delete_local(*node)?;
        }
        for row in &plan.put_rows {
            s.folder().put_local(row)?;
        }
        for node in &dirty {
            s.folder().mark_dirty(*node)?;
        }
        Ok(())
    })
}
