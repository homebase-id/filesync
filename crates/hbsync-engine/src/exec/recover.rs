//! What a crash left behind: conflict copies half made, temporary files no download can use.

use super::delete::{Look, check, put_back, put_back_at, unlink_checked};
use super::local::went;
use super::park::unpark;
use super::*;
use crate::adopt;

/// Puts right what a crash or a power cut left behind, from the executor's write-ahead records
/// and its temporary names, once the scan has seen the disk and before planning: conflict
/// copies half made ([`recover_forks`]), parked items ([`unpark`]), temporary files no
/// download can use ([`sweep_tmp`]), downloads and folders placed but not recorded
/// ([`recover_placements`]). In that order: a completed fork drops its parking record, and a
/// placement is looked for where every item has its own name again.
pub fn recover(db: &mut StateDb, folder: &FolderCtx) -> Result<(), StateError> {
    recover_forks(db, &folder.root)?;
    unpark(db, folder)?;
    sweep_tmp(db, &folder.root)?;
    recover_placements(db, folder)
}

/// Decides what a delete left aside (sync semantics §7: a crash between its move aside and its
/// commit, or a put back that failed; or a download's replaced old version, §7 Old versions)
/// by what it is, as the delete's own check does: where it is still its node's synced version
/// (each item as its row has it, each row as its base has it: the drive's delete stands), it
/// is unlinked, its rows and bases with it, whatever became of its folder meanwhile; anything
/// else (an edit, a new item, an ignored one) goes back to its place, under its own name, else
/// a numbered one there, else at the folder's top, to be planned again. One whose node this
/// folder doesn't know (its state DB lost or replaced since), or one to go back whose place it
/// doesn't, is left aside, and told ([`AsideOutcome::unknown`], §8a). Run at the start of a
/// sync, before the scan's records are read (the scan keeps an item's rows while it is aside).
pub fn recover_aside(db: &mut StateDb, folder: &FolderCtx) -> Result<AsideOutcome, StateError> {
    let hooks = Hooks::default();
    let disk = Disk::new(folder, &hooks);
    let mut dirs = Vec::new();
    let mut unknown = Vec::new();
    let left = |aside: &Path| {
        layout::aside_shown(&aside.file_name().unwrap_or_default().to_string_lossy())
    };
    for (aside, parsed) in layout::aside_entries(&folder.root) {
        let Some(parsed) = parsed else {
            tracing::debug!("{} is left aside: no delete made it", aside.display());
            unknown.push(left(&aside));
            continue;
        };
        let node = parsed.node();
        let Some(row) = db.read(|s| s.local_by_node(node))? else {
            // Its row let go (an item came at its place meanwhile): judged by its base alone.
            match recover_by_base(db, &disk, folder, &aside, parsed)? {
                ByBase::Rescan(dir) => dirs.extend(dir),
                ByBase::Unknown => unknown.push(left(&aside)),
            }
            continue;
        };
        // An old version a download replaced, its node holding the new one now: it goes once
        // its bytes read as the old version's; written to, it comes back as a new item beside
        // the new one; unreadable now, it waits.
        if let layout::AsideEntry::File(_, hash) = parsed
            && row.hash != Some(hash)
        {
            dirs.extend(
                discard_old(db, &disk, folder, &aside, hash, Some(&row.path))?.into_inner(),
            );
            continue;
        }
        let rows = db.read(|s| s.local_subtree(&row.path))?;
        // Each row as its base has it: what the drive's delete took.
        let synced = db.read(|s| {
            rows.iter().try_fold(true, |all, r| {
                Ok(all
                    && s.get_base(r.node)?.is_some_and(|b| {
                        r.kind == NodeKind::Folder || (r.hash.is_some() && b.local_hash == r.hash)
                    }))
            })
        })?;
        let named = match parsed {
            layout::AsideEntry::Folder(_) => row.kind == NodeKind::Folder,
            layout::AsideEntry::File(_, hash) => row.hash == Some(hash),
        };
        let gone = match (synced && named)
            .then(|| check(folder, &aside, &row, &rows, Look::Aside))
            .flatten()
        {
            Some(c) if c.holding.is_empty() => unlink_checked(db, &disk, &aside, c),
            _ => Vec::new(),
        };
        if !gone.is_empty() {
            db.write(|s| went(s, gone.iter().copied(), row.node))?;
            // The folder it was in is read again: the scan kept it for the item, gone or not.
            dirs.push(row.path.parent().unwrap_or_default());
        }
        if gone.len() < rows.len()
            && let Some(to) = put_back(db, &disk, folder, &aside, &row)?
        {
            dirs.push(to.parent().unwrap_or_default());
        }
    }
    // In one order, for the note to read the same each sync (told once).
    unknown.sort();
    Ok(AsideOutcome {
        rescan: dirs,
        unknown,
    })
}

/// What a delete (or a download's replace) left aside of a node with no row here (an item took
/// its place meanwhile, and the scan let the row go, or the node went since), judged by what
/// this device knows of it: a file goes once its bytes read as the name says (its base, if any,
/// with it where those are the synced ones), else comes back as a new item where its node was
/// last synced; a folder comes back there. One of a node this device never knew (its state DB
/// lost or replaced since), or one to go back whose place it doesn't know, is left aside, and
/// told.
fn recover_by_base(
    db: &mut StateDb,
    disk: &Disk<'_>,
    folder: &FolderCtx,
    aside: &Path,
    entry: layout::AsideEntry,
) -> Result<ByBase, StateError> {
    let node = entry.node();
    let known = db.read(|s| {
        let base = s.get_base(node)?;
        if base.is_none() && s.get_mirror(node)?.is_none() && s.removed_by_node(node)?.is_none() {
            return Ok(None);
        }
        let place = crate::activity::path_of(s, node)?.and_then(|p| RelPath::new(&p).ok());
        Ok(Some((base, place)))
    })?;
    let Some((base, place)) = known else {
        tracing::debug!(
            "{} is left aside: nothing synced here is its",
            aside.display()
        );
        return Ok(ByBase::Unknown);
    };
    let layout::AsideEntry::File(_, hash) = entry else {
        let Some(place) = place else {
            tracing::debug!("{} is left aside: its place isn't known", aside.display());
            return Ok(ByBase::Unknown);
        };
        return Ok(ByBase::Rescan(
            put_back_at(db, disk, folder, aside, &place, Some(node), true)?
                .map(|to| to.parent().unwrap_or_default()),
        ));
    };
    let gone = discard_old(db, disk, folder, aside, hash, place.as_ref())?;
    match gone {
        // It was the synced version: its delete is done, and its base goes with it.
        Discarded::Gone if base.is_some_and(|b| b.local_hash == Some(hash)) => {
            db.write(|s| went(s, [node], node))?;
            Ok(ByBase::Rescan(place.and_then(|p| p.parent())))
        }
        Discarded::Stranded => Ok(ByBase::Unknown),
        gone => Ok(ByBase::Rescan(gone.into_inner())),
    }
}

/// What [`recover_by_base`] did with an entry.
enum ByBase {
    /// Decided: the folder to rescan, if any.
    Rescan(Option<RelPath>),
    /// Left aside, to be told: nothing this device knows places it.
    Unknown,
}

/// What [`recover_aside`] did.
#[derive(Debug, Default)]
pub struct AsideOutcome {
    /// The folders to rescan: what it put back is there.
    pub rescan: Vec<RelPath>,
    /// What it left aside, as nothing here places it (sync semantics §8a: told, never
    /// deleted), as sorted paths in the folder (`.hbsync/aside/<name>`).
    pub unknown: Vec<String>,
}

/// What became of a file a run left aside ([`discard_old`]).
enum Discarded {
    /// Its bytes read as its name says: unlinked.
    Gone,
    /// Written to aside: back as a new item, in this folder (to rescan), if it could go.
    Back(Option<RelPath>),
    /// Not read now, or not back: left aside, for the next sync.
    Left,
    /// Written to aside, with no place to go back to: left aside, to be told.
    Stranded,
}

impl Discarded {
    /// The folder to rescan, if any.
    fn into_inner(self) -> Option<RelPath> {
        match self {
            Discarded::Back(dir) => dir,
            Discarded::Gone | Discarded::Left | Discarded::Stranded => None,
        }
    }
}

/// A file aside at `aside`, named for the bytes `hash`: unlinked where it holds them, still as
/// read (read again before the unlink); written to, it goes back as a new item at `place`
/// (numbered where that is taken: never onto a synced item, whose edit it would read as); what
/// can't be read now, or has no place, stays aside, for the next sync.
fn discard_old(
    db: &mut StateDb,
    disk: &Disk<'_>,
    folder: &FolderCtx,
    aside: &Path,
    hash: hbsync_types::ContentHash,
    place: Option<&RelPath>,
) -> Result<Discarded, StateError> {
    let Ok(read) = hbsync_fileio::hash_file(aside, hash.algorithm) else {
        tracing::warn!("{} stays aside: it can't be read now", aside.display());
        return Ok(Discarded::Left);
    };
    if read.hash == hash {
        let same =
            hbsync_platform::stat(aside).is_ok_and(|m| m.fingerprint() == read.meta.fingerprint());
        if same && disk.unlink(aside, super::disk::Unlink::File).is_ok() {
            db.changed(aside);
            return Ok(Discarded::Gone);
        }
        return Ok(Discarded::Left);
    }
    let Some(place) = place else {
        tracing::debug!("{} is left aside: its place isn't known", aside.display());
        return Ok(Discarded::Stranded);
    };
    Ok(Discarded::Back(
        put_back_at(db, disk, folder, aside, place, None, false)?
            .map(|to| to.parent().unwrap_or_default()),
    ))
}

/// The DB half of a fork: the original's row leaves, the copy's row arrives, the record goes.
pub(super) fn complete_fork(
    s: FolderTx<'_>,
    node: NodeId,
    moved: &LocalRow,
) -> Result<(), StateError> {
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
pub(super) fn recover_forks(db: &mut StateDb, root: &Path) -> Result<(), StateError> {
    for (node, new_node, copy_name) in db.read(|s| s.all_forks())? {
        let Some(row) = db.read(|s| s.local_by_node(node))? else {
            db.write(|s| s.folder().delete_fork(node))?;
            continue;
        };
        if row.path.name() == Some(copy_name.as_str()) {
            db.write(|s| {
                s.folder().rekey_local(node, new_node)?;
                s.folder().delete_parked(node)?;
                s.folder().delete_fork(node)?;
                s.folder().mark_dirty(new_node)?;
                s.folder().mark_dirty(node)
            })?;
        } else if hbsync_platform::stat(&row.path.to_native(root))
            .is_ok_and(|meta| meta.file_id == row.fingerprint.file_id)
        {
            // Still under its own name: the rename never happened.
            db.write(|s| s.folder().delete_fork(node))?;
        }
        // Otherwise the scan hasn't caught up with the disk yet: decide next time.
    }
    Ok(())
}

/// Finds downloads (and folders made again) that were put in place but whose commit was lost:
/// the file at a record's target holding its bytes (new to the scan, or the node's own file
/// replaced), or the folder there new to the scan, is that node, with the base the lost commit
/// wrote. Later changes on the drive are then plain changes against that base, whenever the
/// feed brings them. Each record is judged by what is on disk at its target ([`look`]), read
/// as the scan read it: settled when it is the item; dropped when nothing is there, or another
/// node is (the next batch writes its own); kept when it can't be told yet (its file not
/// hashed, its folder unreadable), for a later recovery, for [`UNHASHED_HOLD`] at most from
/// when it was first found so.
pub fn recover_placements(db: &mut StateDb, folder: &FolderCtx) -> Result<(), StateError> {
    let (root, algorithm) = (folder.root.as_path(), folder.algorithm);
    // Each record with the scan's row at its target: the node's own (or a new one), or another
    // node's.
    let found = db.read(|s| {
        let mut found = Vec::new();
        for p in s.all_placing()? {
            let node = p.base.node;
            let scanned = match s.get_local(&p.target)? {
                None => Scanned::None,
                Some(row) => {
                    let new = s.get_base(row.node)?.is_none() && s.get_mirror(row.node)?.is_none();
                    if row.node == node || (new && s.local_by_node(node)?.is_none()) {
                        Scanned::Ours(row)
                    } else {
                        Scanned::Other
                    }
                }
            };
            found.push((p, scanned));
        }
        Ok(found)
    })?;
    if found.is_empty() {
        return Ok(());
    }
    // Outside the write: a file of another client's hash algorithm is hashed.
    let now = hbsync_types::unix_now_ms();
    let judged: Vec<(Placing, Judged)> = found
        .into_iter()
        .map(|(p, scanned)| {
            let judged = match (look(root, &p.target), scanned) {
                (OnDisk::Absent, _) => Judged::Drop,
                (OnDisk::Unreadable, _) => Judged::Keep,
                (OnDisk::There(_), Scanned::Other) => Judged::Drop,
                (OnDisk::There(_), Scanned::Ours(row)) => judge(root, &p, Some(row)),
                // On disk, not scanned yet: read as the scan would.
                (OnDisk::There(meta), Scanned::None) => {
                    let row = read_row(root, &p.target, p.base.node, &meta, algorithm);
                    judge(root, &p, Some(row))
                }
            };
            (p, judged)
        })
        .collect();
    db.write(|s| {
        judged
            .into_iter()
            .try_for_each(|(p, judged)| apply(s.folder(), &p, judged, folder, now).map(drop))
    })
}

/// The scan's row at a record's target.
enum Scanned {
    None,
    /// The node's own, or a new item's.
    Ours(LocalRow),
    /// Another node's.
    Other,
}

/// How long a record whose file is there but can't be hashed yet holds its node.
pub const UNHASHED_HOLD: std::time::Duration = std::time::Duration::from_secs(600);

/// What recovery does with a record.
pub(super) enum Judged {
    /// The item at its target is it: settled, its row this one.
    Settle(LocalRow),
    /// Nothing of it is there (or something else): the record goes.
    Drop,
    /// It can't be told yet (its file not hashed, its folder unreadable): the record stays, for
    /// a later recovery.
    Keep,
}

/// What recovery did with a record.
pub(super) enum Applied {
    /// Its base written (where it put the base, if it did).
    Settled(Option<hbsync_types::RemoteRef>),
    Kept,
    Dropped,
}

/// Judges record `p` by the item at its target (`row`, of the node or new); one file there not
/// hashed yet is kept ([`hold`] says how long).
pub(super) fn judge(root: &Path, p: &Placing, row: Option<LocalRow>) -> Judged {
    let Some(row) = row else {
        return Judged::Drop;
    };
    if row.kind == NodeKind::File && row.hash.is_none() {
        return Judged::Keep;
    }
    let there = match &p.base.state.content {
        Some(c) => row.kind == NodeKind::File && adopt::same_bytes(root, &row, &c.hash, c.size),
        None => row.kind == NodeKind::Folder,
    };
    if there {
        Judged::Settle(row)
    } else {
        Judged::Drop
    }
}

/// Does what recovery judged of record `p` (as of `now`); settled, the item's row is taken
/// over (a new one) or written (one read off the disk, not in the DB yet), and no other: where
/// another node's row is at the target, the record stays instead. One kept is held from now,
/// if it wasn't already.
pub(super) fn apply(
    s: FolderTx<'_>,
    p: &Placing,
    judged: Judged,
    folder: &FolderCtx,
    now: i64,
) -> Result<Applied, StateError> {
    let node = p.base.node;
    match judged {
        Judged::Keep => hold(s, p, now),
        Judged::Drop => {
            // Not put in place (or changed since): nothing to settle.
            s.delete_placing(node)?;
            Ok(Applied::Dropped)
        }
        Judged::Settle(row) => {
            let at_target = s.get_local(&row.path)?;
            if row.node != node {
                adopt::take_over(s, row.node, node)?;
            } else if at_target.as_ref() != Some(&row) {
                // Read off the disk. Another node's row there is never overwritten: the record
                // stays, for the recovery between rounds to judge after its rescan.
                if at_target.is_some_and(|r| r.node != node) {
                    return Ok(Applied::Kept);
                }
                // The node's row moves there, as the commit does.
                if s.local_by_node(node)?.is_some_and(|r| r.path != row.path) {
                    s.delete_local(node)?;
                }
                s.delete_parked(node)?;
                s.put_local(&row)?;
            }
            // What the commit does: the record goes with the base written, or stays until the
            // item can be read (its folder scanned).
            match settle_placement(s, p, folder)? {
                // Its node gone on the drive, its item not one the drive takes (its name): held
                // as a file not hashed yet is, then dropped.
                Placed::Kept => hold(s, p, now),
                Placed::Settled(at) => {
                    s.mark_dirty(node)?;
                    Ok(Applied::Settled(at))
                }
            }
        }
    }
}

/// Holds record `p` as of `now`: from now, if it wasn't held yet; dropped once held for
/// [`UNHASHED_HOLD`].
fn hold(s: FolderTx<'_>, p: &Placing, now: i64) -> Result<Applied, StateError> {
    match p.held_ms {
        None => {
            s.put_placing(&Placing {
                held_ms: Some(now),
                ..p.clone()
            })?;
            Ok(Applied::Kept)
        }
        Some(since) if now.saturating_sub(since) >= UNHASHED_HOLD.as_millis() as i64 => {
            s.delete_placing(p.base.node)?;
            Ok(Applied::Dropped)
        }
        Some(_) => Ok(Applied::Kept),
    }
}

/// What is at a placement's target on disk, read as the scan reads it.
pub(super) enum OnDisk {
    /// Nothing can be there (not found, a file where a folder would be, a link loop), or only
    /// what the scan skips (a link, a special file).
    Absent,
    /// Something whose metadata can't be read now (denied, say): it may be the item.
    Unreadable,
    There(hbsync_platform::LocalMeta),
}

/// What is at `path` (under `root`), as both recoveries read it.
pub(super) fn look(root: &Path, path: &RelPath) -> OnDisk {
    let meta = match hbsync_platform::stat(&path.to_native(root)) {
        Ok(m) => m,
        Err(e) if hbsync_platform::nothing_there(&e) => return OnDisk::Absent,
        Err(_) => return OnDisk::Unreadable,
    };
    match meta.kind {
        hbsync_platform::EntryKind::Symlink | hbsync_platform::EntryKind::Other => OnDisk::Absent,
        _ => OnDisk::There(meta),
    }
}

/// The row of the item (`meta`) at `path` as `node`'s, as the scan would make it: a file hashed
/// if it can be read now (not, locked).
pub(super) fn read_row(
    root: &Path,
    path: &RelPath,
    node: NodeId,
    meta: &hbsync_platform::LocalMeta,
    algorithm: hbsync_fileio::HashAlgorithm,
) -> LocalRow {
    let abs = path.to_native(root);
    let hash = (hbsync_local::scan::kind_of(meta) == NodeKind::File)
        .then(|| hbsync_fileio::hash_file(&abs, algorithm).ok())
        .flatten()
        .map(|h| h.hash);
    hbsync_local::scan::row_from(path, node, meta, hash, tags::read(&abs))
}

/// The folders to rescan for record `p` before recovery judges it: the one its target is in,
/// and the one its node's item is in now (an old version a download replaced from another
/// folder). The rescan only updates rows; what is on disk is judged by [`look`].
pub fn record_dirs(s: &ReadTx<'_>, p: &Placing) -> Result<Vec<RelPath>, StateError> {
    let mut dirs = vec![p.target.parent().unwrap_or_default()];
    if let Some(row) = s.local_by_node(p.base.node)? {
        dirs.push(row.path.parent().unwrap_or_default());
    }
    Ok(dirs)
}

/// The temp file name stem of a download: node and content, so a partial file is resumed only
/// for the same bytes.
pub(super) fn download_key(node: NodeId, expected: &hbsync_types::ContentHash) -> String {
    format!("{}-{}", node.0.simple(), &expected.to_hex()[..16])
}

/// Removes temp files no download can use any more: anything that isn't a partial download of
/// the current remote content of a node that still needs syncing (interrupted downloads of
/// content since replaced, files that were placed by other means, leftovers of other
/// versions). Run before each plan, while no transfer is running.
pub(super) fn sweep_tmp(db: &mut StateDb, root: &Path) -> Result<(), StateError> {
    let Ok(entries) = fs::read_dir(layout::tmp_dir(root)) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let wanted = match entry.file_name().to_str() {
            Some(name) => wanted_download(db, name)?,
            None => false,
        };
        if !wanted {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

pub(super) fn wanted_download(db: &mut StateDb, file_name: &str) -> Result<bool, StateError> {
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
