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
/// each for a new item, here or wherever the user moved it since. Its record names the node
/// and the bytes; the one new file holding them becomes that node again, with the base the
/// lost update wrote while the mirror still has that version. Run after the scan and before
/// the feed is pulled (the mirror is then the one the downloads were planned from).
pub fn recover_placements(db: &mut StateDb, root: &Path) -> Result<usize, StateError> {
    let records = db.read(|s| s.all_placing())?;
    if records.is_empty() {
        return Ok(0);
    }
    let unpaired = db.read(|s| s.unpaired_local())?;
    let mut recovered = 0;
    for (node, version, hash, size) in &records {
        // Two downloads of the same bytes can't tell their files apart.
        let twin = records
            .iter()
            .any(|(other, _, h, _)| other != node && h == hash);
        let mut found = unpaired
            .iter()
            .filter(|r| r.kind == NodeKind::File && crate::adopt::same_bytes(root, r, hash, *size));
        let found = match (twin, found.next(), found.next()) {
            (false, Some(row), None) => Some(row),
            _ => None,
        };
        db.write(|s| {
            let mirror = s.get_mirror(*node)?;
            let current = mirror
                .as_ref()
                .is_some_and(|m| m.remote_ref().version == *version);
            if let Some(row) = found
                && s.local_by_node(*node)?.is_none()
            {
                s.rekey_local(row.node, *node)?;
                s.clear_dirty(row.node)?;
                s.mark_dirty(*node)?;
                if let Some(m) = mirror.filter(|_| current)
                    && let Some(state) = m.state()
                {
                    s.put_base(&BaseRow {
                        node: *node,
                        state,
                        remote: m.remote_ref(),
                        content_state: ContentState::Local,
                        local_name: None,
                        local_hash: row.hash,
                    })?;
                }
                recovered += 1;
            } else if current && s.local_by_node(*node)?.is_none() {
                // Not placed yet, or not scanned yet: still wanted, decided next time.
                return Ok(());
            }
            s.delete_placing(*node)
        })?;
    }
    Ok(recovered)
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
