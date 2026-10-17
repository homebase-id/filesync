//! What a crash left behind: conflict copies half made, temporary files no download can use.

use super::park::unpark;
use super::*;
use crate::adopt;

/// Puts right what a crash or a power cut left behind, from the executor's write-ahead records
/// and its temporary names, once the scan has seen the disk and before planning: conflict
/// copies half made ([`recover_forks`]), parked items ([`unpark`]), temporary files no
/// download can use ([`sweep_tmp`]), downloads and folders placed but not recorded
/// ([`recover_placements`]). In that order: a completed fork drops its parking record, and a
/// placement is looked for where every item has its own name again.
pub fn recover(db: &mut StateDb, cfg: &ExecConfig) -> Result<(), StateError> {
    recover_forks(db, &cfg.root)?;
    unpark(db, cfg)?;
    sweep_tmp(db, &cfg.root)?;
    recover_placements(db, &cfg.root, cfg.os, cfg.algorithm)
}

/// The DB half of a fork: the original's row leaves, the copy's row arrives, the record goes.
pub(super) fn complete_fork(
    s: &Store<'_>,
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
        } else if hbsync_platform::stat(&row.path.to_native(root))
            .is_ok_and(|meta| meta.file_id == row.fingerprint.file_id)
        {
            // Still under its own name: the rename never happened.
            db.write(|s| s.delete_fork(node))?;
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
pub fn recover_placements(
    db: &mut StateDb,
    root: &Path,
    os: Os,
    algorithm: hbsync_fileio::HashAlgorithm,
) -> Result<(), StateError> {
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
    // Each judged, and whether its item verifiably isn't in place (nothing there, or another
    // node's): only then may an old version it put in the trash come back.
    let judged: Vec<(Placing, Judged, bool)> = found
        .into_iter()
        .map(|(p, scanned)| {
            let at_target = look(root, &p.target);
            let not_placed =
                matches!(at_target, OnDisk::Absent) || matches!(scanned, Scanned::Other);
            let judged = match (at_target, scanned) {
                (OnDisk::Absent, _) => Judged::Drop,
                (OnDisk::Unreadable, _) => Judged::Keep,
                (OnDisk::There(_), Scanned::Other) => Judged::Drop,
                (OnDisk::There(_), Scanned::Ours(row)) => judge(root, &p, Some(row), now),
                // On disk, not scanned yet: read as the scan would.
                (OnDisk::There(meta), Scanned::None) => {
                    let row = read_row(root, &p.target, p.base.node, &meta, algorithm);
                    judge(root, &p, Some(row), now)
                }
            };
            (p, judged, not_placed)
        })
        .collect();
    // An old version a download put in the trash comes back where the new one verifiably isn't
    // in place (never where a file is there, with other bytes or not hashed in time) and its
    // place is free (on disk; in the DB, but for the node's own row).
    let mut restored = Vec::with_capacity(judged.len());
    for (p, judged, not_placed) in judged {
        let Some(t) = p.trashed.as_ref().filter(|_| not_placed) else {
            restored.push((p, judged));
            continue;
        };
        let in_trash = fs::symlink_metadata(t.to.to_native(&layout::trash_dir(root))).is_ok();
        let free = matches!(look(root, &t.from), OnDisk::Absent)
            && db
                .read(|s| s.get_local(&t.from))?
                .is_none_or(|r| r.node == p.base.node);
        if !in_trash || !free {
            restored.push((p, judged));
            continue;
        }
        let judged = match restore_entry(db, root, &t.to, &t.from)? {
            Ok(()) => match look(root, &t.from) {
                OnDisk::There(meta) => {
                    Judged::Restored(read_row(root, &t.from, p.base.node, &meta, algorithm))
                }
                _ => Judged::Drop,
            },
            // Its folder gone since (the user deleted it): it stays in the trash, as what was
            // in that folder would have gone there.
            Err(e) if hbsync_platform::nothing_there(&e) => judged,
            // Tried again by the next recovery.
            Err(_) => Judged::Keep,
        };
        restored.push((p, judged));
    }
    let judged = restored;
    db.write(|s| {
        judged
            .into_iter()
            .try_for_each(|(p, judged)| apply(s, &p, judged, os, now).map(drop))
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
    /// Not put in place, the old version it put in the trash brought back (its row this one):
    /// the record goes.
    Restored(LocalRow),
}

/// What recovery did with a record.
pub(super) enum Applied {
    /// Its base written (where it put the base, if it did).
    Settled(Option<hbsync_types::RemoteRef>),
    Kept,
    Dropped,
}

/// Judges record `p` by the item at its target (`row`, of the node or new), as of `now`: one
/// file there not hashed yet holds the record for [`UNHASHED_HOLD`] from when it was first
/// found so.
pub(super) fn judge(root: &Path, p: &Placing, row: Option<LocalRow>, now: i64) -> Judged {
    let Some(row) = row else {
        return Judged::Drop;
    };
    if row.kind == NodeKind::File && row.hash.is_none() {
        let held = p
            .held_ms
            .is_none_or(|since| now.saturating_sub(since) < UNHASHED_HOLD.as_millis() as i64);
        return if held { Judged::Keep } else { Judged::Drop };
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
    s: &Store<'_>,
    p: &Placing,
    judged: Judged,
    os: Os,
    now: i64,
) -> Result<Applied, StateError> {
    let node = p.base.node;
    match judged {
        Judged::Keep => {
            if p.held_ms.is_none() {
                s.put_placing(&Placing {
                    held_ms: Some(now),
                    ..p.clone()
                })?;
            }
            Ok(Applied::Kept)
        }
        Judged::Drop => {
            // Not put in place (or changed since): nothing to settle.
            s.delete_placing(node)?;
            Ok(Applied::Dropped)
        }
        Judged::Restored(row) => {
            if s.local_by_node(node)?.is_some_and(|r| r.path != row.path) {
                s.delete_local(node)?;
            }
            s.put_local(&row)?;
            s.delete_placing(node)?;
            s.mark_dirty(node)?;
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
            let placed = settle_placement(s, p, os)?;
            // Its node gone on the drive, its item not one the drive takes (its name): held as
            // a file not hashed yet is, then dropped.
            if placed.gone && s.is_placing(node)? {
                return hold(s, p, now);
            }
            s.mark_dirty(node)?;
            Ok(Applied::Settled(placed.at))
        }
    }
}

/// Holds record `p` as of `now`: from now, if it wasn't held yet; dropped once held for
/// [`UNHASHED_HOLD`].
fn hold(s: &Store<'_>, p: &Placing, now: i64) -> Result<Applied, StateError> {
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
pub fn record_dirs(s: &Store<'_>, p: &Placing) -> Result<Vec<RelPath>, StateError> {
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
