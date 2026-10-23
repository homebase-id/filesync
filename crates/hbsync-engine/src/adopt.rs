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
use hbsync_local::{ScanOptions, rescan_entries};
use hbsync_namemap::to_remote;
use hbsync_platform::RelPath;
use hbsync_platform::name::name_key;
use hbsync_state::{FolderTx, LocalRow, MirrorRow, StateDb, StateError};
use hbsync_types::{ContentHash, NodeId, NodeKind};

use crate::folder::FolderCtx;
use crate::settle::{Settle, settle, spells};

/// Adopts what it can among the dirty nodes, never as a folder the scope leaves out (§12: a
/// plain local `Documents` isn't the role folder). Returns the number of adoptions.
pub fn adopt(db: &mut StateDb, folder: &FolderCtx) -> Result<usize, StateError> {
    let (os, scope) = (folder.os, &folder.scope);
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
        let group = start..end;
        start = end;

        let Some(parent) = db.read(|s| s.local_node_at(&parent_path))? else {
            continue;
        };
        let mut siblings: HashMap<String, Vec<MirrorRow>> = HashMap::new();
        for remote in db.read(|s| s.mirror_children(parent))? {
            if scope.outside.contains(&remote.node) {
                continue;
            }
            siblings
                .entry(name_key(&remote.name))
                .or_default()
                .push(remote);
        }
        if siblings.is_empty() {
            continue;
        }
        // A file left to be hashed on send with a name the drive has here is hashed now: its
        // bytes tell whether it is that node.
        hash_twins(db, folder, &mut candidates[group.clone()], &siblings)?;
        let group = &candidates[group];
        // Matches are written per folder, in one transaction each.
        let mut claimed: HashSet<NodeId> = HashSet::new();
        let mut matches: Vec<(&LocalRow, NodeId, bool)> = Vec::new();
        for row in group {
            if let Some((remote_node, agreed)) = find_match(db, folder, row, &siblings, &claimed)? {
                claimed.insert(remote_node);
                matches.push((row, remote_node, agreed));
            }
        }
        db.write(|s| {
            for (row, remote_node, agreed) in &matches {
                take_over(s.folder(), row.node, *remote_node)?;
                // Recorded right away where the reconciler couldn't tell they agree: a match
                // proven by hashing with the remote's algorithm (it can't compare hashes of
                // different algorithms), or at this device's spelling of a drive name the name
                // map doesn't give back (it reads that as another name).
                if *agreed {
                    let how = Settle {
                        same_bytes: true,
                        ..Settle::node(*remote_node)
                    };
                    settle(s.folder(), how, os)?;
                }
            }
            Ok(())
        })?;
        adopted += matches.len();
    }
    Ok(adopted)
}

/// The files of `group` left to be hashed on send (sync semantics §3) that a remote node of
/// `siblings` may be: read now, as the scan reads a file (its hash on send off), their rows
/// taken back into `group` as recorded.
fn hash_twins(
    db: &mut StateDb,
    folder: &FolderCtx,
    group: &mut [LocalRow],
    siblings: &HashMap<String, Vec<MirrorRow>>,
) -> Result<(), StateError> {
    let twin = |r: &LocalRow| {
        r.send_hash
            && r.path
                .name()
                .and_then(|n| to_remote(n, folder.os).ok())
                .is_some_and(|n| siblings.contains_key(&name_key(&n)))
    };
    let paths: Vec<RelPath> = group
        .iter()
        .filter(|r| twin(r))
        .map(|r| r.path.clone())
        .collect();
    if paths.is_empty() {
        return Ok(());
    }
    let opts = ScanOptions {
        hash_on_send: false,
        ..folder.scan_options()
    };
    rescan_entries(db, &folder.root, &[], &paths, &opts)?;
    db.read(|s| {
        for row in group.iter_mut().filter(|r| r.send_hash) {
            if let Some(now) = s.local_by_node(row.node)? {
                *row = now;
            }
        }
        Ok(())
    })
}

/// Hands the local item of `local` to `node`: it is that node from now on, to be planned.
pub(crate) fn take_over(s: FolderTx<'_>, local: NodeId, node: NodeId) -> Result<(), StateError> {
    s.rekey_local(local, node)?;
    s.clear_dirty(local)?;
    s.mark_dirty(node)
}

/// Whether the local file of `row` holds `size` bytes of content `hash` (another client's
/// algorithm, maybe: then this one file is hashed the same way).
pub(crate) fn same_bytes(root: &Path, row: &LocalRow, hash: &ContentHash, size: u64) -> bool {
    let Some(local) = row.hash else {
        return false;
    };
    if row.fingerprint.size != size {
        false
    } else if local.algorithm == hash.algorithm {
        local == *hash
    } else {
        hash_file(&row.path.to_native(root), hash.algorithm)
            .is_ok_and(|h| h.hash == *hash && h.meta.fingerprint() == row.fingerprint)
    }
}

/// The remote node `row` is a copy of, if any, and whether it's to be recorded as agreed at
/// once (see [`adopt`]). `siblings` are the remote children of the row's parent by
/// [`name_key`].
fn find_match(
    db: &mut StateDb,
    folder: &FolderCtx,
    row: &LocalRow,
    siblings: &HashMap<String, Vec<MirrorRow>>,
    claimed: &HashSet<NodeId>,
) -> Result<Option<(NodeId, bool)>, StateError> {
    let os = folder.os;
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
        let Some(file) = remote.file else {
            continue;
        };
        let cross = row.hash.is_some_and(|h| h.algorithm != file.hash.algorithm);
        let spelled = remote.name != name && spells(&row.path, &remote.name, os);
        // Recorded as agreed at once (see `adopt`), so the place must match exactly.
        if cross && remote.name != name && !spelled {
            continue;
        }
        if same_bytes(&folder.root, row, &file.hash, file.size) {
            return Ok(Some((remote.node, cross || spelled)));
        }
    }
    Ok(None)
}
