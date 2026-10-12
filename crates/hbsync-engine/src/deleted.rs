//! Recently deleted (node schema): what a delete took out of the tree, kept on the drive with
//! its bytes until the purge. A restore puts it back in the tree, where the next sync brings it
//! to every device; the purge deletes it for good.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use hbsync_platform::name::name_key;
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind};

use crate::engine::EngineError;
use crate::exec::{content_of, remove_all, trash_dir, trashed_at};
use crate::tree::restored_names;

/// Recently deleted, newest first.
pub fn list(db: &mut StateDb, limit: usize) -> Result<Vec<Deleted>, StateError> {
    db.read(|s| {
        // Folder paths, each worked out once.
        let mut folders: HashMap<NodeId, String> = HashMap::new();
        s.list_removed(limit)?
            .into_iter()
            .map(|r| {
                let folder = folder_path(s, r.row.parent, &mut folders)?;
                Ok(Deleted {
                    node: r.row.node,
                    parent: r.row.parent,
                    path: join(&folder, &r.row.name),
                    kind: r.row.kind,
                    size: r.row.file.map(|f| f.size),
                    removed_at_ms: r.removal.at_ms,
                    root: r.removal.root,
                })
            })
            .collect()
    })
}

/// The path of folder `at` (empty for the top), from the tree or from Recently deleted; a
/// folder deleted for good ends it (the path starts below it).
fn folder_path(
    s: &Store<'_>,
    at: NodeId,
    known: &mut HashMap<NodeId, String>,
) -> Result<String, StateError> {
    if let Some(path) = known.get(&at) {
        return Ok(path.clone());
    }
    let mut names = Vec::new();
    let mut up = at;
    let mut base = String::new();
    // Bounded: a damaged DB must not hang the list.
    while !up.is_root() && names.len() < 256 {
        if let Some(path) = known.get(&up) {
            base = path.clone();
            break;
        }
        let Some(folder) = s.get_mirror(up)?.or(s.removed_by_node(up)?.map(|r| r.row)) else {
            break;
        };
        names.push((up, folder.name));
        up = folder.parent;
    }
    // Top-down, remembering each folder's path on the way.
    for (id, name) in names.into_iter().rev() {
        base = join(&base, &name);
        known.insert(id, base.clone());
    }
    Ok(base)
}

fn join(folder: &str, name: &str) -> String {
    if folder.is_empty() {
        name.to_owned()
    } else {
        format!("{folder}/{name}")
    }
}

/// Restores `node` with what was deleted with it, and the folders it was in that are in
/// Recently deleted too (just those, not their other contents). Returns how many items came
/// back; 0 when `node` isn't in Recently deleted (any more).
pub fn restore(db: &mut StateDb, remote: &dyn Remote, node: NodeId) -> Result<usize, EngineError> {
    let Some(plan) = db.read(|s| plan_restore(s, node))? else {
        return Ok(0);
    };
    let RestorePlan {
        rows,
        mut live,
        mut taken,
    } = plan;
    let in_plan: HashSet<NodeId> = rows.iter().map(|r| r.row.node).collect();
    let mut restored = 0;
    for r in &rows {
        let row = &r.row;
        // Its folder is back (or never left). One that was to come back and didn't keeps it in
        // Recently deleted; one deleted for good, the top takes it.
        let parent = if live.contains(&row.parent) {
            row.parent
        } else if in_plan.contains(&row.parent) {
            continue;
        } else {
            NodeId::ROOT
        };
        let names = taken.entry(parent).or_default();
        let Some(name) = restored_names(&row.name).find(|n| !names.contains(&name_key(n))) else {
            continue; // no free name: it stays in Recently deleted
        };
        let meta = NodeMeta {
            node: row.node,
            parent,
            kind: row.kind,
            content: content_of(row, &name),
        };
        let written = match remote.update(row.file_id, row.version, &meta, None) {
            Ok(written) => written,
            // Changed or deleted for good meanwhile: the feed tells what it is now.
            Err(RemoteError::VersionMismatch | RemoteError::NotFound) => continue,
            Err(e) => return Err(EngineError::Remote(e)),
        };
        names.insert(name_key(&name));
        live.insert(row.node);
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
        restored += 1;
    }
    Ok(restored)
}

/// What a restore of a node does, worked out in one read.
struct RestorePlan {
    /// In order: the folders above that are in Recently deleted (top-down), then the node and
    /// what was deleted with it (each after its folder).
    rows: Vec<RemovedRow>,
    /// Folders in the tree that the rows may go into (the top included).
    live: HashSet<NodeId>,
    /// The name keys taken in each of those folders.
    taken: HashMap<NodeId, HashSet<String>>,
}

/// `top` and what its delete took along below it: the removed items in it, level by level,
/// whose delete is the same (`rm.root`). What was deleted from it before stays out.
fn group(s: &Store<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    let root = top.removal.root;
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        if rows[next].row.kind == NodeKind::Folder {
            let below = s.removed_children(rows[next].row.node)?;
            rows.extend(below.into_iter().filter(|r| r.removal.root == root));
        }
        next += 1;
    }
    Ok(rows)
}

fn plan_restore(s: &Store<'_>, node: NodeId) -> Result<Option<RestorePlan>, StateError> {
    let Some(top) = s.removed_by_node(node)? else {
        return Ok(None);
    };
    // The folders above, as far as they are in Recently deleted.
    let mut chain = Vec::new();
    let mut at = top.row.parent;
    while !at.is_root() && chain.len() < 256 {
        match s.removed_by_node(at)? {
            Some(folder) => {
                at = folder.row.parent;
                chain.push(folder);
            }
            None => break, // in the tree, or deleted for good (then the top it is)
        }
    }
    chain.reverse();
    // Then the node and what was deleted with it, each after its folder.
    let rows: Vec<RemovedRow> = chain.into_iter().chain(group(s, top)?).collect();
    // The folders in the tree the rows may go into, and their names.
    let mut live = HashSet::from([NodeId::ROOT]);
    let mut taken: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for parent in rows.iter().map(|r| r.row.parent).chain([NodeId::ROOT]) {
        if taken.contains_key(&parent) {
            continue;
        }
        if !parent.is_root() && s.get_mirror(parent)?.is_some() {
            live.insert(parent);
        }
        let names = s
            .mirror_children(parent)?
            .iter()
            .map(|m| name_key(&m.name))
            .collect();
        taken.insert(parent, names);
    }
    Ok(Some(RestorePlan { rows, live, taken }))
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
    // Only what went there since the purge has been emptying it: older entries (from before
    // Recently deleted) may be the only copy left, and stay.
    let since = db.write(|s| match s.get_kv(TRASH_SINCE_KEY)? {
        Some(v) => Ok(v.parse().unwrap_or(u64::MAX)),
        None => {
            let now = hbsync_types::unix_now();
            s.set_kv(TRASH_SINCE_KEY, &now.to_string())?;
            Ok(now)
        }
    })?;
    empty_trash(&trash_dir(state_dir), since, before_ms);
    Ok(gone)
}

/// Deletes for good the items `nodes` name, each with what was deleted with it. Returns how
/// many items went.
pub fn empty(
    db: &mut StateDb,
    remote: &dyn Remote,
    nodes: &[NodeId],
) -> Result<usize, EngineError> {
    let mut rows = db.read(|s| {
        let mut rows = Vec::new();
        for &node in nodes {
            if let Some(top) = s.removed_by_node(node)? {
                rows.extend(group(s, top)?);
            }
        }
        Ok(rows)
    })?;
    rows.sort_by_key(|r| (r.row.kind == NodeKind::Folder, r.row.node));
    rows.dedup_by_key(|r| r.row.node);
    delete_for_good(db, remote, &rows)
}

/// Deletes for good everything removed until `at_ms` (Unix ms): what the user saw when they
/// asked, nothing removed since. Returns how many items went.
pub fn empty_until(
    db: &mut StateDb,
    remote: &dyn Remote,
    at_ms: i64,
) -> Result<usize, EngineError> {
    let rows = db.read(|s| s.removed_before(at_ms.saturating_add(1)))?;
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

/// The state DB key of when the purge started emptying the local trash (Unix s).
const TRASH_SINCE_KEY: &str = "trash.purged_since";

/// Deletes the trash entries (`<Unix s>-<node>`) that went there from `since` (Unix s) until
/// before `before_ms`. Best effort: one that can't go now goes at the next purge.
fn empty_trash(trash: &Path, since: u64, before_ms: i64) {
    let Ok(entries) = fs::read_dir(trash) else {
        return;
    };
    for entry in entries.flatten() {
        let at = entry.file_name().to_str().and_then(trashed_at);
        if at.is_some_and(|s| s >= since && (s as i64).saturating_mul(1000) < before_ms) {
            let _ = remove_all(&entry.path());
        }
    }
}
