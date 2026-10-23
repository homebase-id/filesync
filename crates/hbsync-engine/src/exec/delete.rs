//! Deleting here what was deleted elsewhere (sync semantics §7): the item is renamed aside
//! (`.hbsync/aside`, one rename on its volume), checked there to be exactly what the plan saw,
//! then unlinked, nothing kept. Anything else found (an edit, a new item, an ignored one, a
//! folder on the junk list, which may hold what was never synced) puts it back where it was,
//! for the next plan; an item that went is never one that wasn't checked.

use std::path::PathBuf;

use hbsync_local::layout::AsideEntry;
use hbsync_local::{SkipReason, WalkEvent, Walker};
use hbsync_platform::LocalMeta;

use super::disk::Unlink;
use super::*;

/// What became of an item a delete moved aside ([`Executor::delete_item`]).
pub(super) enum Deleted {
    /// All of it went.
    All,
    /// It holds what stays here (ignored items, folders on the junk list: §10), in these
    /// folders: left at its place, to go item by item.
    Holding(Vec<RelPath>),
    /// Not what the plan saw, or something came while it went: left or returned to its place, after `gone`
    /// went (the nodes unlinked before that was found).
    Returned { gone: Vec<NodeId> },
}

/// What a check found to unlink: each synced item with its row, its place on disk, and whether
/// it was renamed there itself (its ctime moved); the junk files in it (disposable, §10); and
/// the folders in it that hold what stays here (by their place in the sync root).
pub(super) struct Checked<'r> {
    items: Vec<(PathBuf, &'r LocalRow, bool)>,
    junk: Vec<PathBuf>,
    pub(super) holding: Vec<RelPath>,
}

impl Executor<'_> {
    /// Deletes the item of `row`, with what is below it (`rows`, its subtree's rows in walk
    /// order, `row`'s first), still as the plan saw it here (the caller checked the item): a
    /// folder first looked at in place (what changed, what stays here: then it doesn't move),
    /// then moved aside, checked there, unlinked. `whole`: one that holds what stays here is
    /// left ([`Deleted::Holding`]); otherwise that, too, is something the plan didn't see. An
    /// [`Outcome`] where it didn't go aside, or the run stops ([`CrashPoint::AfterAside`]).
    pub(super) fn delete_item(
        &mut self,
        row: &LocalRow,
        rows: &[LocalRow],
        whole: bool,
    ) -> Result<Result<Deleted, Outcome>, StateError> {
        let entry = match (row.kind, row.hash) {
            (NodeKind::Folder, _) => AsideEntry::Folder(row.node),
            (NodeKind::File, Some(hash)) => AsideEntry::File(row.node, hash),
            // Never planned so (§6.3: it waits for its hash); read again first.
            (NodeKind::File, None) => return Ok(Err(rescan(&mut self.report, &row.path))),
        };
        // A folder is looked at where it is first (its items' metadata, one walk): a change
        // found so costs no move, and the folder stays in place while its users work in it.
        if row.kind == NodeKind::Folder {
            let here = self.disk.abs(&row.path);
            match check(self.folder, &here, row, rows, Look::InPlace) {
                None => return Ok(Err(rescan(&mut self.report, &row.path))),
                Some(c) if !c.holding.is_empty() => {
                    return Ok(Ok(match whole {
                        true => Deleted::Holding(c.holding),
                        false => Deleted::Returned { gone: Vec::new() },
                    }));
                }
                Some(_) => {}
            }
        }
        let aside = match self.move_aside(row, entry)? {
            Ok(at) => at,
            Err(outcome) => return Ok(Err(outcome)),
        };
        if self.crash_here(CrashPoint::AfterAside) {
            return Ok(Err(Outcome::Retry("crash point reached".into())));
        }
        self.cfg.hooks.aside(&aside);
        let gone = match check(self.folder, &aside, row, rows, Look::Aside) {
            Some(c) if c.holding.is_empty() => {
                let gone = unlink_checked(self.db, &self.disk, &aside, c);
                if gone.len() == rows.len() {
                    return Ok(Ok(Deleted::All));
                }
                // Something came while it went: what is left goes back, for the next plan.
                gone
            }
            _ => Vec::new(),
        };
        if let Some(to) = return_from_aside(self.db, &self.disk, self.folder, &aside, row)? {
            self.report.rescan.push(to.parent().unwrap_or_default());
        }
        self.report
            .rescan
            .push(row.path.parent().unwrap_or_default());
        Ok(Ok(Deleted::Returned { gone }))
    }
}

impl Executor<'_> {
    /// Moves the item of `row` aside as `entry`. An [`Outcome`] where it didn't go: one of that
    /// name still aside (for the next sync's recovery) waits for it.
    pub(super) fn move_aside(
        &mut self,
        row: &LocalRow,
        entry: AsideEntry,
    ) -> Result<Result<PathBuf, Outcome>, StateError> {
        Ok(match self.disk.move_aside(self.db, &row.path, entry)? {
            Ok(at) => Ok(at),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(Outcome::waiting(
                "an earlier version of it is still aside, for the next sync",
            )),
            Err(e) => Err(io_outcome(e)),
        })
    }

    /// Unlinks the old version a download replaced from another folder (`old`, aside at
    /// `aside`), the new one committed, where it is still what was there (checked as any delete
    /// checks a file: on a volume whose metadata can't prove the bytes, a read of them). Anything
    /// else (it changed, or it couldn't be read or unlinked now) leaves it aside, for the next
    /// sync's recovery to decide ([`super::recover_aside`]).
    pub(super) fn unlink_old(&mut self, aside: &Path, old: &LocalRow) {
        let one = std::slice::from_ref(old);
        if let Some(c) = check(self.folder, aside, old, one, Look::Aside) {
            unlink_checked(self.db, &self.disk, aside, c);
        }
    }
}

/// How a check looks at an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Look {
    /// Where it is, before it moves: by metadata alone.
    InPlace,
    /// Aside, renamed there: its bytes too where metadata can't prove them.
    Aside,
}

/// Checks the item of `row` at `at` against the rows of its subtree (`rows`, `row`'s included),
/// as [`Look`] says: a file by [`same_item`] (and, aside, [`proven`]); a folder in one walk
/// that judges each item at its place in the sync root (ignore rules go by path): every synced
/// item there must be one of `rows`, unchanged, and every one of `rows` there; junk files go
/// along; ignored items, and folders on the junk list (they may hold what was never synced),
/// are reported. `None` if anything else is there.
pub(super) fn check<'r>(
    folder: &FolderCtx,
    at: &Path,
    row: &'r LocalRow,
    rows: &'r [LocalRow],
    look: Look,
) -> Option<Checked<'r>> {
    let renamed = look == Look::Aside;
    let top = hbsync_platform::stat(at).ok()?;
    if !(same_item(folder, &top, row, renamed) && (!renamed || proven(folder, at, &top, row))) {
        return None;
    }
    let mut checked = Checked {
        items: vec![(at.to_path_buf(), row, renamed)],
        junk: Vec::new(),
        holding: Vec::new(),
    };
    if row.kind == NodeKind::File {
        return Some(checked);
    }
    let mut expected: HashMap<&RelPath, &LocalRow> = rows
        .iter()
        .filter(|r| r.path != row.path)
        .map(|r| (&r.path, r))
        .collect();
    let walk = Walker::moved(&folder.root, row.path.clone(), at, &folder.filter, None);
    for event in walk {
        match event {
            WalkEvent::Entry { path, meta } => {
                let r = expected.remove(&path)?;
                let place = path.strip_prefix(&row.path)?.to_native(at);
                if !(same_item(folder, &meta, r, false)
                    && (!renamed || proven(folder, &place, &meta, r)))
                {
                    return None;
                }
                checked.items.push((place, r, false));
            }
            WalkEvent::Skipped {
                path,
                reason: reason @ (SkipReason::Junk | SkipReason::Ignored),
            } => {
                let below: PathBuf = path.components().skip(row.path.depth()).collect();
                let place = at.join(below);
                let file = std::fs::symlink_metadata(&place).is_ok_and(|m| !m.is_dir());
                if reason == SkipReason::Junk && file {
                    checked.junk.push(place);
                } else {
                    // Its own name may be one we can't sync; its folder's we walked.
                    let folder = path.parent()?;
                    checked
                        .holding
                        .push(hbsync_local::journal::relpath_prefix(folder));
                }
            }
            // Anything else we never sync (a link, a bad name) would go unseen.
            _ => return None,
        }
    }
    expected.is_empty().then_some(checked)
}

/// Whether `meta` is of `row`'s item, as it was: its kind, its id where ids are stable here, and
/// a file's size and mtime, and its ctime too unless it was `renamed` itself (a rename moves
/// it) or ids aren't stable here.
fn same_item(folder: &FolderCtx, meta: &LocalMeta, row: &LocalRow, renamed: bool) -> bool {
    let fp = &row.fingerprint;
    let ids = folder.ids_stable();
    match row.kind {
        NodeKind::Folder => meta.kind == EntryKind::Dir && (!ids || meta.file_id == fp.file_id),
        NodeKind::File if renamed || !ids => {
            meta.kind == EntryKind::File
                && (!ids || meta.file_id == fp.file_id)
                && fp.same_after_rename(meta)
        }
        NodeKind::File => meta.kind == EntryKind::File && meta.fingerprint() == *fp,
    }
}

/// Whether the file at `at`, read as `meta`, holds the bytes `row` records: proven by its
/// metadata where the volume's mtimes and ids can ([`FolderCtx::mtime_proves_bytes`]), else by
/// its hash, read unchanged. A folder needs nothing more.
fn proven(folder: &FolderCtx, at: &Path, meta: &LocalMeta, row: &LocalRow) -> bool {
    if row.kind == NodeKind::Folder || folder.mtime_proves_bytes() {
        return true;
    }
    let Some(want) = row.hash else {
        return false;
    };
    hbsync_fileio::hash_file(at, want.algorithm)
        .is_ok_and(|h| h.hash == want && h.meta.fingerprint() == meta.fingerprint())
}

/// Unlinks what a check of the item aside at `aside` found, deepest first: junk files as they
/// are, each synced file only while it is still the one checked (read again, as the check read
/// it), each folder only once empty. Returns the nodes that went; something that came meanwhile
/// stays, and so do the folders it is in. What changed is noted ([`StateDb::changed`]): synced
/// before the next durable commit, so a durable commit that forgets an item never comes back to
/// it on disk. Where all went, the aside folder is all there is to note; where some goes back,
/// each folder something went from.
pub(super) fn unlink_checked(
    db: &mut StateDb,
    disk: &Disk<'_>,
    aside: &Path,
    checked: Checked<'_>,
) -> Vec<NodeId> {
    let folder = disk.folder();
    let mut all: Vec<(PathBuf, Option<(&LocalRow, bool)>)> = checked
        .items
        .into_iter()
        .map(|(p, r, renamed)| (p, Some((r, renamed))))
        .chain(checked.junk.into_iter().map(|p| (p, None)))
        .collect();
    all.sort_by_cached_key(|(p, _)| std::cmp::Reverse(p.components().count()));
    let mut gone = Vec::new();
    let mut left = false;
    let mut unlinked_at = Vec::new();
    for (at, row) in all {
        let unlinked = match row {
            None => disk.unlink(&at, Unlink::Junk).is_ok(),
            Some((r, renamed)) if r.kind == NodeKind::File => {
                hbsync_platform::stat(&at).is_ok_and(|m| same_item(folder, &m, r, renamed))
                    && disk.unlink(&at, Unlink::File).is_ok()
            }
            Some(_) => disk.unlink(&at, Unlink::Folder).is_ok(),
        };
        match (unlinked, row) {
            (true, Some((r, _))) => gone.push(r.node),
            (true, None) => {}
            (false, _) => left = true,
        }
        if unlinked {
            unlinked_at.push(at);
        }
    }
    match left {
        false => db.changed(aside),
        true => unlinked_at.iter().for_each(|at| db.changed(at)),
    }
    gone
}

/// Returns the item of `row` from `aside`: to its place, else under the first free numbered
/// name there, else at the folder's top (its folder gone). A parked item takes its own name,
/// never its temporary one (it carries the old id), and its record goes. Returns where it went;
/// where none of that goes through, it stays aside, for the next sync's recovery (never a
/// delete).
pub(super) fn return_from_aside(
    db: &mut StateDb,
    disk: &Disk<'_>,
    folder: &FolderCtx,
    aside: &Path,
    row: &LocalRow,
) -> Result<Option<RelPath>, StateError> {
    let is_dir = row.kind == NodeKind::Folder;
    let place = db
        .read(|s| view::parked_place(s, row))?
        .unwrap_or_else(|| row.path.clone());
    let to = return_to(db, disk, folder, aside, &place, Some(row.node), is_dir)?;
    if to.as_ref().is_some_and(|to| to != &row.path) {
        db.write(|s| s.folder().delete_parked(row.node))?;
    }
    Ok(to)
}

/// Returns what is at `aside` to `place` (free but for `own`'s row there: the item's own
/// node), else under the first free numbered name there, else at the folder's top. Returns where
/// it went; `None` where it stays aside.
pub(super) fn return_to(
    db: &mut StateDb,
    disk: &Disk<'_>,
    folder: &FolderCtx,
    aside: &Path,
    place: &RelPath,
    own: Option<NodeId>,
    is_dir: bool,
) -> Result<Option<RelPath>, StateError> {
    let at_top = place.name().and_then(|n| RelPath::new(n).ok());
    for wanted in [Some(place.clone()), at_top].into_iter().flatten() {
        let Some(to) = release_place_for(db, folder, &wanted, own, is_dir)? else {
            continue;
        };
        if disk.rename_from(db, aside, &to)?.is_ok() {
            return Ok(Some(to));
        }
    }
    tracing::warn!(
        "{} stays aside: it couldn't go back to {place}; the next sync tries again",
        aside.display()
    );
    Ok(None)
}
