//! Adoption: recognizing that a local item and a remote node are the same thing.
//!
//! On a first run over an existing copy (or after a crash between placing a download and
//! recording it), the scan gives local items fresh node ids, while the remote already has
//! nodes at the same places. Left alone, the reconciler would see two different nodes with the
//! same name and keep both. Adoption instead hands the local item the remote node's id when
//! they agree on parent, name (under [`name_key`]), kind and, for files, content. The
//! reconciler then finds both sides equal and only records the base: nothing is transferred.
//!
//! Folders are adopted before their contents (walk order), so children of an adopted folder
//! find the remote folder as their parent.

use std::path::Path;

use hbsync_fileio::hash_file;
use hbsync_namemap::to_remote;
use hbsync_platform::name::name_key;
use hbsync_platform::{Os, RelPath};
use hbsync_state::{LocalRow, StateDb, StateError};
use hbsync_types::{NodeId, NodeKind};

/// Adopts what it can among the dirty nodes. Returns the number of adoptions.
pub fn adopt(db: &mut StateDb, root: &Path, os: Os) -> Result<usize, StateError> {
    // Local-only candidates: dirty, no base, no mirror row.
    let dirty = db.read(|s| s.dirty_nodes(usize::MAX >> 1))?;
    let mut candidates: Vec<LocalRow> = Vec::new();
    db.read(|s| {
        for node in &dirty {
            if s.get_base(*node)?.is_none()
                && s.get_mirror(*node)?.is_none()
                && let Some(row) = s.local_by_node(*node)?
            {
                candidates.push(row);
            }
        }
        Ok(())
    })?;
    candidates.sort_by(|a, b| a.path.cmp(&b.path));

    let mut adopted = 0;
    for row in candidates {
        if let Some(remote_node) = find_match(db, root, os, &row)? {
            db.write(|s| {
                s.delete_local(&row.path)?;
                s.put_local(&LocalRow {
                    node: remote_node,
                    ..row.clone()
                })?;
                s.clear_dirty(row.node)?;
                s.mark_dirty(remote_node)
            })?;
            adopted += 1;
        }
    }
    Ok(adopted)
}

/// The remote node `row` is a copy of, if any.
fn find_match(
    db: &mut StateDb,
    root: &Path,
    os: Os,
    row: &LocalRow,
) -> Result<Option<NodeId>, StateError> {
    let Some(local_name) = row.path.name() else {
        return Ok(None);
    };
    let Ok(name) = to_remote(local_name, os) else {
        return Ok(None);
    };
    let parent_path = row.path.parent().unwrap_or_default();
    let parent = if parent_path.is_root() {
        NodeId::ROOT
    } else {
        match db.read(|s| s.get_local(&parent_path))? {
            Some(p) => p.node,
            None => return Ok(None),
        }
    };
    let key = name_key(&name);
    let siblings = db.read(|s| s.mirror_children(parent))?;
    for remote in siblings {
        if remote.kind != row.kind || name_key(&remote.name) != key {
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
            return Ok(Some(remote.node));
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
        let same = if local_hash.algorithm == remote_hash.algorithm {
            local_hash == remote_hash
        } else {
            // Another client's algorithm: hash this one file the same way.
            hash_file(&path_of(root, &row.path), remote_hash.algorithm)
                .is_ok_and(|h| h.hash == remote_hash && h.meta.fingerprint() == row.fingerprint)
        };
        if same {
            return Ok(Some(remote.node));
        }
    }
    Ok(None)
}

fn path_of(root: &Path, path: &RelPath) -> std::path::PathBuf {
    path.to_native(root)
}
