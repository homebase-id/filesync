//! What a crash left behind: conflict copies half made, temporary files no download can use.

use super::park::unpark;
use super::*;
use crate::adopt;
use hbsync_types::NodeState;

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
/// changes against that base, whenever the feed brings them. Every record goes: the next
/// batch writes its own.
pub(super) fn recover_placements(db: &mut StateDb, root: &Path, os: Os) -> Result<(), StateError> {
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
    let found: Vec<(Placing, Option<LocalRow>)> = found
        .into_iter()
        .map(|(p, row)| {
            let row = row.filter(|row| match &p.base.state.content {
                Some(c) => {
                    row.kind == NodeKind::File && adopt::same_bytes(root, row, &c.hash, c.size)
                }
                None => row.kind == NodeKind::Folder,
            });
            (p, row)
        })
        .collect();
    db.write(|s| {
        for (p, row) in &found {
            let node = p.base.node;
            s.delete_placing(node)?;
            let Some(row) = row else {
                continue;
            };
            if row.node != node {
                adopt::take_over(s, row.node, node)?;
            }
            let state = &p.base.state;
            s.put_base(&BaseRow {
                // What both sides have.
                state: NodeState {
                    tags: row
                        .tags
                        .as_ref()
                        .map_or(state.tags.clone(), |t| t.intersection(&state.tags)),
                    ..state.clone()
                },
                content_state: s
                    .get_base(node)?
                    .map_or(ContentState::Local, |b| b.content_state),
                local_name: recorded_name(&row.path, &state.name, os),
                local_hash: row.hash,
                ..p.base.clone()
            })?;
            s.mark_dirty(node)?;
        }
        Ok(())
    })
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
