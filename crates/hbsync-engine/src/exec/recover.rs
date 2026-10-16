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
    recover_placements(db, &cfg.root, cfg.os)
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

/// Finds downloads (and folders made again) that were put in place but whose commit a crash
/// lost: the file at a record's target holding its bytes (new to the scan, or the node's own
/// file replaced), or the folder there new to the scan, is that node, with the base the lost
/// commit wrote. Later changes on the drive are then plain
/// changes against that base, whenever the feed brings them. A record goes once it is
/// settled, or where nothing was put in place (the next batch writes its own); one whose file
/// is there but can't be hashed yet (locked) stays, for a later recovery, for
/// [`UNHASHED_HOLD`] at most: then it goes, as one whose item isn't there (the file is the
/// scan's new item then).
pub fn recover_placements(db: &mut StateDb, root: &Path, os: Os) -> Result<(), StateError> {
    // Every record, judged by the scanned item at its target (the node's own row, or a new
    // one).
    let found = db.read(|s| {
        let mut found = Vec::new();
        for p in s.all_placing()? {
            let node = p.base.node;
            let Some(row) = s.get_local(&p.target)? else {
                found.push((p, None));
                continue;
            };
            let new = s.get_base(row.node)?.is_none() && s.get_mirror(row.node)?.is_none();
            let ours = row.node == node || (new && s.local_by_node(node)?.is_none());
            found.push((p, ours.then_some(row)));
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
        .map(|(p, row)| {
            let judged = judge(root, &p, row, now);
            (p, judged)
        })
        .collect();
    db.write(|s| {
        judged
            .into_iter()
            .try_for_each(|(p, judged)| apply(s, &p, judged, os).map(drop))
    })
}

/// How long a record whose file is there but can't be hashed yet holds its node.
pub const UNHASHED_HOLD: std::time::Duration = std::time::Duration::from_secs(600);

/// What recovery does with a record.
pub(super) enum Judged {
    /// The item at its target is it: settled, its row this one.
    Settle(LocalRow),
    /// Nothing of it is there (or something else): the record goes.
    Drop,
    /// Its file is there but can't be hashed yet: the record stays, for a later recovery.
    Keep,
}

/// Judges record `p` by the item at its target (`row`, of the node or new; `None`: nothing
/// there, or another node's), as of `now`: one file there not hashed yet holds the record for
/// [`UNHASHED_HOLD`] from when it was placed.
pub(super) fn judge(root: &Path, p: &Placing, row: Option<LocalRow>, now: i64) -> Judged {
    let Some(row) = row else {
        return Judged::Drop;
    };
    if row.kind == NodeKind::File && row.hash.is_none() {
        let held = now.saturating_sub(p.placed_ms) < UNHASHED_HOLD.as_millis() as i64;
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

/// Does what recovery judged of record `p`; settled, the item's row is taken over (a new one)
/// or written (one read off the disk, not in the DB yet), and no other. Returns where it put
/// the base, if it did.
pub(super) fn apply(
    s: &Store<'_>,
    p: &Placing,
    judged: Judged,
    os: Os,
) -> Result<Option<hbsync_types::RemoteRef>, StateError> {
    let node = p.base.node;
    match judged {
        Judged::Keep => Ok(None),
        Judged::Drop => {
            // Not put in place (or changed since): nothing to settle.
            s.delete_placing(node)?;
            Ok(None)
        }
        Judged::Settle(row) => {
            let at_target = s.get_local(&row.path)?;
            if row.node != node {
                adopt::take_over(s, row.node, node)?;
            } else if at_target.as_ref() != Some(&row) {
                // Read off the disk. Another node's row there is never overwritten: the record
                // stays, for the recovery between rounds to judge after its rescan.
                if at_target.is_some_and(|r| r.node != node) {
                    return Ok(None);
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
            let at = settle_placement(s, p, os)?;
            s.mark_dirty(node)?;
            Ok(at)
        }
    }
}

/// What is at a placement's target on disk, read as the scan reads it.
pub(super) enum OnDisk {
    /// Verifiably nothing (not found), or what the scan skips (a link, a special file).
    Absent,
    /// Something whose metadata can't be read now (denied, say): it may be the item.
    Unreadable,
    /// The item, as `node`'s row; a file not hashed if it can't be read now (locked).
    Item(LocalRow),
}

/// Reads the item at `path` (under `root`) as `node`'s row, as the scan would.
pub(super) fn read_item(
    root: &Path,
    path: &RelPath,
    node: NodeId,
    algorithm: hbsync_fileio::HashAlgorithm,
) -> OnDisk {
    let abs = path.to_native(root);
    let meta = match hbsync_platform::stat(&abs) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return OnDisk::Absent,
        Err(_) => return OnDisk::Unreadable,
    };
    if matches!(
        meta.kind,
        hbsync_platform::EntryKind::Symlink | hbsync_platform::EntryKind::Other
    ) {
        return OnDisk::Absent;
    }
    let hash = (hbsync_local::scan::kind_of(&meta) == NodeKind::File)
        .then(|| hbsync_fileio::hash_file(&abs, algorithm).ok())
        .flatten()
        .map(|h| h.hash);
    OnDisk::Item(hbsync_local::scan::row_from(
        path,
        node,
        &meta,
        hash,
        tags::read(&abs),
    ))
}

/// The folders to rescan for record `p` before recovery judges it: the one its target is in,
/// and the one its node's item is in now (an old version a download replaced from another
/// folder).
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
