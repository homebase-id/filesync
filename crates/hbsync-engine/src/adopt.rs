//! Adoption: recognizing that a local item and a remote node are the same thing.
//!
//! On a first run over an existing copy (or after a crash between placing a download and
//! recording it), the scan gives local items fresh node ids, while the remote already has
//! nodes at the same places. Left alone, the reconciler would see two different nodes with the
//! same name and keep both. Adoption instead hands the local item the remote node's id when
//! they agree on parent, name (under [`name_key`]), kind and, for files, content. The
//! reconciler then finds both sides equal and only records the base: nothing is transferred.
//!
//! Folders are adopted before their contents (candidates go folder by folder, shallowest
//! first), so children of an adopted folder find the remote folder as their parent. Each
//! folder's remote children are loaded and indexed once, and its matches are written in one
//! transaction, so adoption is linear in the number of items however wide the folders are.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use hbsync_fileio::hash_file;
use hbsync_namemap::to_remote;
use hbsync_platform::Os;
use hbsync_platform::name::name_key;
use hbsync_state::{LocalRow, MirrorRow, StateDb, StateError};
use hbsync_types::{NodeId, NodeKind};

/// Adopts what it can among the dirty nodes. Returns the number of adoptions.
pub fn adopt(db: &mut StateDb, root: &Path, os: Os) -> Result<usize, StateError> {
    // Local-only candidates: dirty, no base, no mirror row.
    let mut candidates = db.read(|s| s.unpaired_local())?;
    // Grouped by folder, parents before children.
    candidates.sort_by_cached_key(|r| {
        let parent = r.path.parent().unwrap_or_default();
        (r.path.depth(), parent, r.path.clone())
    });

    let mut adopted = 0;
    let mut start = 0;
    while start < candidates.len() {
        let parent_path = candidates[start].path.parent().unwrap_or_default();
        let end = start
            + candidates[start..]
                .iter()
                .take_while(|r| r.path.parent().unwrap_or_default() == parent_path)
                .count();
        let group = &candidates[start..end];
        start = end;

        let Some(parent) = db.read(|s| s.local_node_at(&parent_path))? else {
            continue;
        };
        let mut siblings: HashMap<String, Vec<MirrorRow>> = HashMap::new();
        for remote in db.read(|s| s.mirror_children(parent))? {
            siblings
                .entry(name_key(&remote.name))
                .or_default()
                .push(remote);
        }
        if siblings.is_empty() {
            continue;
        }
        // Matches are written per folder, in one transaction each.
        let mut claimed: HashSet<NodeId> = HashSet::new();
        let mut matches: Vec<(&LocalRow, NodeId, bool)> = Vec::new();
        for row in group {
            if let Some((remote_node, proven_equal)) =
                find_match(db, root, os, row, &siblings, &claimed)?
            {
                claimed.insert(remote_node);
                matches.push((row, remote_node, proven_equal));
            }
        }
        db.write(|s| {
            for (row, remote_node, proven_equal) in &matches {
                s.delete_local(&row.path)?;
                s.put_local(&LocalRow {
                    node: *remote_node,
                    ..(*row).clone()
                })?;
                s.clear_dirty(row.node)?;
                s.mark_dirty(*remote_node)?;
                // A match proven by hashing with the remote's algorithm is recorded right away:
                // the reconciler can't compare hashes of different algorithms by itself.
                if *proven_equal {
                    crate::exec::settle(s, *remote_node, os, true)?;
                }
            }
            Ok(())
        })?;
        adopted += matches.len();
    }
    Ok(adopted)
}

/// The remote node `row` is a copy of, if any, and whether equality was proven across hash
/// algorithms (which only this function can know). `siblings` are the remote children of the
/// row's parent by [`name_key`].
fn find_match(
    db: &mut StateDb,
    root: &Path,
    os: Os,
    row: &LocalRow,
    siblings: &HashMap<String, Vec<MirrorRow>>,
    claimed: &HashSet<NodeId>,
) -> Result<Option<(NodeId, bool)>, StateError> {
    let Some(local_name) = row.path.name() else {
        return Ok(None);
    };
    let Ok(name) = to_remote(local_name, os) else {
        return Ok(None);
    };
    let Some(same_key) = siblings.get(&name_key(&name)) else {
        return Ok(None);
    };
    for remote in same_key {
        if remote.kind != row.kind || claimed.contains(&remote.node) {
            continue;
        }
        // Only a remote node that has no local copy and no history yet.
        let taken = db.read(|s| {
            Ok(s.get_base(remote.node)?.is_some() || s.local_by_node(remote.node)?.is_some())
        })?;
        if taken {
            continue;
        }
        if row.kind == NodeKind::Folder {
            return Ok(Some((remote.node, false)));
        }
        let (Some(local_hash), Some(file)) = (row.hash, remote.file) else {
            continue;
        };
        let Some(remote_hash) = file.hash else {
            continue;
        };
        if file.size != row.fingerprint.size {
            continue;
        }
        let cross = local_hash.algorithm != remote_hash.algorithm;
        // Recorded as agreed at once (see `adopt`), so the place must match exactly.
        if cross && remote.name != name {
            continue;
        }
        let same = if !cross {
            local_hash == remote_hash
        } else {
            // Another client's algorithm: hash this one file the same way.
            hash_file(&row.path.to_native(root), remote_hash.algorithm)
                .is_ok_and(|h| h.hash == remote_hash && h.meta.fingerprint() == row.fingerprint)
        };
        if same {
            return Ok(Some((remote.node, cross)));
        }
    }
    Ok(None)
}
