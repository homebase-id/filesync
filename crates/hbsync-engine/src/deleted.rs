//! Recently deleted (node schema): what a delete took out of the tree, kept on the drive with
//! its bytes until the purge. A restore puts it back in the tree, where the next sync brings it
//! to every device; the purge deletes it for good.

use std::fs;
use std::path::Path;

use hbsync_platform::name::name_key;
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store};
use hbsync_types::{NodeId, NodeKind};

use crate::engine::EngineError;
use crate::exec::{content_of, remove_all};
use crate::tree::restored_name;

/// One item in Recently deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleted {
    pub node: NodeId,
    /// Where it was, for people.
    pub path: String,
    pub kind: NodeKind,
    /// Files only.
    pub size: Option<u64>,
    /// Unix ms.
    pub removed_at_ms: i64,
    /// The node whose delete took it along (itself, or a folder it was in).
    pub root: NodeId,
}

/// Recently deleted, newest first.
pub fn list(db: &mut StateDb, limit: usize) -> Result<Vec<Deleted>, StateError> {
    db.read(|s| {
        s.list_removed(limit)?
            .into_iter()
            .map(|r| {
                Ok(Deleted {
                    node: r.row.node,
                    path: path_of(s, &r.row)?,
                    kind: r.row.kind,
                    size: r.row.file.map(|f| f.size),
                    removed_at_ms: r.removal.at_ms,
                    root: r.removal.root,
                })
            })
            .collect()
    })
}

/// The path `row` had: its folders from the tree or from Recently deleted.
fn path_of(s: &Store<'_>, row: &MirrorRow) -> Result<String, StateError> {
    let mut names = vec![row.name.clone()];
    let mut at = row.parent;
    // Bounded: a damaged DB must not hang the list.
    while !at.is_root() && names.len() < 256 {
        let Some(folder) = s.get_mirror(at)?.or(s.removed_by_node(at)?.map(|r| r.row)) else {
            break; // a folder deleted for good: the path starts below it
        };
        names.push(folder.name);
        at = folder.parent;
    }
    names.reverse();
    Ok(names.join("/"))
}

/// Restores `node` with what was deleted with it, and the folders it was in that are in
/// Recently deleted too (just those, not their other contents). Returns how many items came
/// back; 0 when `node` isn't in Recently deleted (any more).
pub fn restore(db: &mut StateDb, remote: &dyn Remote, node: NodeId) -> Result<usize, EngineError> {
    let Some(top) = db.read(|s| s.removed_by_node(node))? else {
        return Ok(0);
    };
    let mut restored = 0;
    // Its folders first, top-down, each alone.
    let mut chain = Vec::new();
    let mut at = top.row.parent;
    while !at.is_root() && chain.len() < 256 {
        if db.read(|s| s.get_mirror(at))?.is_some() {
            break;
        }
        let Some(folder) = db.read(|s| s.removed_by_node(at))? else {
            break; // deleted for good: restored at the top instead (below)
        };
        at = folder.row.parent;
        chain.push(folder);
    }
    for folder in chain.into_iter().rev() {
        restored += usize::from(restore_one(db, remote, &folder)?);
    }
    // Then the item and what its delete took along, each once its folder is back.
    let mut group = db.read(|s| s.removed_with(node))?;
    if !group.iter().any(|r| r.row.node == node) {
        group.push(top);
    }
    while !group.is_empty() {
        let before = group.len();
        let mut later = Vec::new();
        for r in group {
            let parent_back = r.row.parent.is_root()
                || db.read(|s| s.get_mirror(r.row.parent))?.is_some()
                || db.read(|s| s.removed_by_node(r.row.parent))?.is_none();
            if parent_back {
                restored += usize::from(restore_one(db, remote, &r)?);
            } else {
                later.push(r);
            }
        }
        if later.len() == before {
            break; // folders that didn't come back: their items stay where they are
        }
        group = later;
    }
    Ok(restored)
}

/// Puts one removed node back in the tree, under a free name in its folder (or at the top,
/// if its folder is gone for good). Whether it came back.
fn restore_one(db: &mut StateDb, remote: &dyn Remote, r: &RemovedRow) -> Result<bool, EngineError> {
    let row = &r.row;
    let parent = if row.parent.is_root() || db.read(|s| s.get_mirror(row.parent))?.is_some() {
        row.parent
    } else {
        NodeId::ROOT
    };
    let taken: Vec<String> = db.read(|s| {
        Ok(s.mirror_children(parent)?
            .into_iter()
            .map(|m| name_key(&m.name))
            .collect())
    })?;
    let name = (0..)
        .map(|n| match n {
            0 => row.name.clone(),
            n => restored_name(&row.name, n),
        })
        .find(|name| !taken.contains(&name_key(name)))
        .unwrap_or_else(|| row.name.clone());
    let meta = NodeMeta {
        node: row.node,
        parent,
        kind: row.kind,
        content: content_of(row, &name),
    };
    let written = match remote.update(row.file_id, row.version, &meta, None) {
        Ok(written) => written,
        // Changed or deleted for good meanwhile: the feed tells what it is now.
        Err(RemoteError::VersionMismatch | RemoteError::NotFound) => return Ok(false),
        Err(e) => return Err(EngineError::Remote(e)),
    };
    let back = MirrorRow {
        file_id: written.file_id,
        version: written.version,
        parent,
        name,
        ..row.clone()
    };
    db.write(|s| {
        s.delete_removed(row.node)?;
        s.put_mirror(&back)?;
        s.mark_dirty(row.node)
    })?;
    Ok(true)
}

/// Deletes for good what was removed before `before_ms` (Unix ms), files before their
/// folders, and empties the local trash (`state_dir/trash`) of the same age. Returns how many
/// items went.
pub fn purge(
    db: &mut StateDb,
    remote: &dyn Remote,
    before_ms: i64,
    state_dir: &Path,
) -> Result<usize, EngineError> {
    let expired = db.read(|s| s.removed_before(before_ms))?;
    let gone = delete_for_good(db, remote, &expired)?;
    empty_trash(&state_dir.join("trash"), before_ms);
    Ok(gone)
}

/// Deletes for good the items `nodes` name, each with what was deleted with it; all of
/// Recently deleted with `None`. Returns how many items went.
pub fn empty(
    db: &mut StateDb,
    remote: &dyn Remote,
    nodes: Option<&[NodeId]>,
) -> Result<usize, EngineError> {
    let mut rows = match nodes {
        None => db.read(|s| s.removed_before(i64::MAX))?,
        Some(nodes) => db.read(|s| {
            let mut rows = Vec::new();
            for &node in nodes {
                rows.extend(s.removed_with(node)?);
                rows.extend(s.removed_by_node(node)?);
            }
            Ok(rows)
        })?,
    };
    rows.sort_by_key(|r| (r.row.kind == NodeKind::Folder, r.row.node));
    rows.dedup_by_key(|r| r.row.node);
    delete_for_good(db, remote, &rows)
}

fn delete_for_good(
    db: &mut StateDb,
    remote: &dyn Remote,
    rows: &[RemovedRow],
) -> Result<usize, EngineError> {
    let mut gone = 0;
    for r in rows {
        match remote.delete(r.row.file_id, r.row.version) {
            Ok(()) | Err(RemoteError::NotFound) => {}
            // Restored or changed meanwhile: not ours to delete.
            Err(RemoteError::VersionMismatch) => continue,
            Err(e) => return Err(EngineError::Remote(e)),
        }
        db.write(|s| s.delete_removed(r.row.node))?;
        gone += 1;
    }
    Ok(gone)
}

/// Deletes the trash entries (`<Unix s>-<node>`) from before `before_ms`. Best effort: one
/// that can't go now goes at the next purge.
fn empty_trash(trash: &Path, before_ms: i64) {
    let Ok(entries) = fs::read_dir(trash) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let stamp = name
            .to_str()
            .and_then(|n| n.split_once('-'))
            .and_then(|(s, _)| s.parse::<i64>().ok());
        if stamp.is_some_and(|s| s.saturating_mul(1000) < before_ms) {
            let _ = remove_all(&entry.path());
        }
    }
}
