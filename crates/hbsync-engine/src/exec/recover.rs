//! What a crash left behind: conflict copies half made, temporary files no download can use.

use super::*;

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
pub fn recover_forks(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let mut completed = 0;
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
            completed += 1;
        } else if hbsync_platform::stat(&row.path.to_native(root))
            .is_ok_and(|meta| meta.file_id == row.fingerprint.file_id)
        {
            // Still under its own name: the rename never happened.
            db.write(|s| s.delete_fork(node))?;
        }
        // Otherwise the scan hasn't caught up with the disk yet: decide next time.
    }
    Ok(completed)
}

/// Finds downloads that were put in place but whose DB update a crash lost: the scan took
/// each for a new item. The file at a record's target, new to this device and holding the
/// record's bytes, becomes that node again, settled as the lost update would have while the
/// drive still has that version. Every record goes: the next batch writes its own. Run after
/// the local catch-up and before the feed is pulled (the drive may have moved the node since).
pub fn recover_placements(db: &mut StateDb, root: &Path, os: Os) -> Result<usize, StateError> {
    let records = db.read(|s| s.all_placing())?;
    if records.is_empty() {
        return Ok(0);
    }
    // The new file at each target (outside the write: a file may need hashing).
    let mut found = Vec::new();
    for p in &records {
        let row = db.read(|s| {
            if s.local_by_node(p.node)?.is_some() {
                return Ok(None); // recorded after all
            }
            let Some(row) = s.get_local(&p.target)? else {
                return Ok(None);
            };
            let new = s.get_base(row.node)?.is_none() && s.get_mirror(row.node)?.is_none();
            Ok((new && row.kind == NodeKind::File).then_some(row))
        })?;
        if let Some(row) = row
            && crate::adopt::same_bytes(root, &row, &p.hash, p.size)
        {
            found.push((p, row));
        }
    }
    db.write(|s| {
        for p in &records {
            s.delete_placing(p.node)?;
        }
        for (p, row) in &found {
            s.rekey_local(row.node, p.node)?;
            s.clear_dirty(row.node)?;
            s.mark_dirty(p.node)?;
            if s.get_mirror(p.node)?
                .is_some_and(|m| m.version == p.version)
            {
                settle(s, p.node, os, true)?;
            }
        }
        Ok(())
    })?;
    Ok(found.len())
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
pub fn sweep_tmp(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let Ok(entries) = fs::read_dir(layout::tmp_dir(root)) else {
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
