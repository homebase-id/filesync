//! Recently deleted (node schema): what a delete took out of the tree, kept on the drive with
//! its bytes until the purge. A restore puts it back in the tree, where the next sync brings it
//! to every device; the purge deletes it for good.
//!
//! It is the whole drive's, kept with the account ([`crate::drive::Drive`]): each item comes
//! back to the folder of this device whose part of the drive it was in, if any.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use hbsync_platform::name::name_key;
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind, Role};

use crate::engine::EngineError;
use crate::exec::{remove_all, trashed_at, update_meta};
use crate::transfer::Target;
use crate::tree::restored_names;
use crate::walk::End;
use hbsync_local::layout;

/// Recently deleted, newest first: the drive's, each item with the folder of this device it
/// comes back to and its path below that folder's top.
pub fn list(s: &Store<'_>, limit: usize) -> Result<Vec<Deleted>, StateError> {
    // Where a walk up stops: a folder's top (here or not), the role folders whether
    // synced here or not; above them all, the main folder's top (the drive's root).
    let folders: HashMap<NodeId, String> =
        s.folders()?.into_iter().map(|f| (f.top, f.path)).collect();
    let tops: HashSet<NodeId> = folders
        .keys()
        .copied()
        .chain(Role::ALL.iter().map(|r| r.node()))
        .collect();
    let mut known = HashMap::new();
    let mut deleted_by = HashMap::new();
    s.list_removed(limit)?
        .into_iter()
        .map(|r| {
            let (path, top) = place(s, &tops, r.row.parent, &mut known)?;
            Ok(Deleted {
                node: r.row.node,
                parent: r.row.parent,
                path: join(&path, &r.row.name),
                kind: r.row.kind,
                size: r.row.file.map(|f| f.size),
                removed_at_ms: r.removal.at_ms,
                root: deleted_by_item(s, &r, &mut deleted_by)?,
                folder: folders.get(&top).cloned(),
            })
        })
        .collect()
}

/// Whether `r` was deleted with `folder`, the folder it is in, in Recently deleted too: removed
/// no earlier than the folder (a folder's delete removes the folder first). Two removals an
/// older writer made say it with their `rm`: the same root.
fn deleted_with(r: &RemovedRow, folder: &RemovedRow) -> bool {
    match (r.removal.root, folder.removal.root) {
        (Some(root), Some(folders)) => root == folders,
        _ => folder.removal.at_ms <= r.removal.at_ms,
    }
}

/// The item of Recently deleted whose delete took `r` along: up through the folders it was
/// deleted with, as far as they go (`r` itself, deleted on its own). Memoized in `known`.
fn deleted_by_item(
    s: &Store<'_>,
    r: &RemovedRow,
    known: &mut HashMap<NodeId, NodeId>,
) -> Result<NodeId, StateError> {
    let mut chain = vec![r.row.node];
    let mut at = r.clone();
    let top = loop {
        if let Some(top) = known.get(&at.row.node) {
            break *top;
        }
        match s.removed_by_node(at.row.parent)? {
            // Each node once: removed rows may form a cycle (left by a remote one).
            Some(folder) if deleted_with(&at, &folder) && !chain.contains(&folder.row.node) => {
                chain.push(folder.row.node);
                at = folder;
            }
            _ => break at.row.node,
        }
    };
    known.extend(chain.into_iter().map(|n| (n, top)));
    Ok(top)
}

/// Whether `r` is an item of its own in Recently deleted: not deleted with its folder.
fn deleted_on_its_own(s: &Store<'_>, r: &RemovedRow) -> Result<bool, StateError> {
    Ok(!s
        .removed_by_node(r.row.parent)?
        .is_some_and(|folder| deleted_with(r, &folder)))
}

/// The path of folder `at` below the top it is under (one of `tops`, else the drive's root:
/// a folder deleted for good ends the path, which starts below it), and that top, from the
/// tree or from Recently deleted. Memoized in `known`.
fn place(
    s: &Store<'_>,
    tops: &HashSet<NodeId>,
    at: NodeId,
    known: &mut HashMap<NodeId, (String, NodeId)>,
) -> Result<(String, NodeId), StateError> {
    let up = crate::walk::up::<_, StateError>(at, |n| {
        if known.contains_key(&n) || tops.contains(&n) {
            return Ok(None);
        }
        let row = match s.get_mirror(n)? {
            Some(row) => Some(row),
            None => s.removed_by_node(n)?.map(|r| r.row),
        };
        Ok(row.map(|r| ((n, r.name), r.parent)))
    })?;
    let (mut path, top) = match up.end {
        End::At(n) if tops.contains(&n) => (String::new(), n),
        End::At(n) => known
            .get(&n)
            .cloned()
            .unwrap_or((String::new(), NodeId::ROOT)),
        End::Top | End::Cycle => (String::new(), NodeId::ROOT),
    };
    // Top-down, remembering each folder on the way.
    for (id, name) in up.items.into_iter().rev() {
        path = join(&path, &name);
        known.insert(id, (path.clone(), top));
    }
    Ok((path, top))
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
            content: row.content(&name),
        };
        let back = match update_meta(remote, Target::of(row), &meta, row.file.map(|f| f.hash)) {
            Ok(back) => back,
            // Changed or deleted for good meanwhile: the feed tells what it is now.
            Err(RemoteError::VersionMismatch | RemoteError::NotFound) => continue,
            Err(e) => return Err(EngineError::Remote(e)),
        };
        names.insert(name_key(&name));
        live.insert(row.node);
        // The mirror's write marks it dirty in the folders it comes back to.
        db.write(|s| {
            s.delete_removed(row.node)?;
            s.put_mirror(&back)
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
/// deleted with their folder ([`deleted_with`]). What was deleted from it before stays out.
fn group(s: &Store<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    // Each node once: removed rows may form a cycle (left by a remote one).
    let mut seen = HashSet::from([top.row.node]);
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        if rows[next].row.kind == NodeKind::Folder {
            let folder = &rows[next];
            let with: Vec<RemovedRow> = s
                .removed_children(folder.row.node)?
                .into_iter()
                .filter(|r| deleted_with(r, folder) && seen.insert(r.row.node))
                .collect();
            rows.extend(with);
        }
        next += 1;
    }
    Ok(rows)
}

fn plan_restore(s: &Store<'_>, node: NodeId) -> Result<Option<RestorePlan>, StateError> {
    let Some(top) = s.removed_by_node(node)? else {
        return Ok(None);
    };
    // The folders above, as far as they are in Recently deleted (then in the tree, or deleted
    // for good: the top it is).
    let mut chain = crate::walk::up::<_, StateError>(top.row.parent, |at| {
        Ok(s.removed_by_node(at)?.map(|f| {
            let parent = f.row.parent;
            (f, parent)
        }))
    })?
    .items;
    chain.reverse();
    // Then the node and what was deleted with it, each after its folder; each node once.
    let mut seen = HashSet::new();
    let rows: Vec<RemovedRow> = chain
        .into_iter()
        .chain(group(s, top)?)
        .filter(|r| seen.insert(r.row.node))
        .collect();
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

/// Deletes for good the items of Recently deleted removed before `before_ms` (Unix ms), each
/// with what was deleted with it (a folder's delete goes as a whole), files before their
/// folders, and empties every folder's trash (`.hbsync/trash`) of the same age. Returns how
/// many items went.
pub fn purge(db: &mut StateDb, remote: &dyn Remote, before_ms: i64) -> Result<usize, EngineError> {
    let gone = expire(db, remote, before_ms)?;
    for folder in db.read(|s| s.folders())? {
        if folder.path.is_empty() {
            continue;
        }
        // Only what went there since the purge has been emptying it: older entries (from
        // before Recently deleted) may be the only copy left, and stay.
        let since = db.write(
            |s| match s.folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE)? {
                Some(v) => Ok(v.parse().unwrap_or(u64::MAX)),
                None => {
                    let now = hbsync_types::unix_now();
                    s.set_folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE, &now.to_string())?;
                    Ok(now)
                }
            },
        )?;
        empty_trash(
            &layout::trash_dir(Path::new(&folder.path)),
            since,
            before_ms,
        );
    }
    Ok(gone)
}

/// Deletes for good the items `nodes` name, each with what was deleted with it. Returns how
/// many items went.
pub fn empty(
    db: &mut StateDb,
    remote: &dyn Remote,
    nodes: &[NodeId],
) -> Result<usize, EngineError> {
    let tops = db.read(|s| {
        let mut tops = Vec::new();
        for &node in nodes {
            tops.extend(s.removed_by_node(node)?);
        }
        Ok(tops)
    })?;
    delete_groups(db, remote, tops)
}

/// Deletes for good the items removed until `at_ms` (Unix ms), each with what was deleted with
/// it: what the user saw when they asked. Returns how many items went.
pub fn empty_until(
    db: &mut StateDb,
    remote: &dyn Remote,
    at_ms: i64,
) -> Result<usize, EngineError> {
    expire(db, remote, at_ms.saturating_add(1))
}

/// Deletes for good the items of Recently deleted removed before `before_ms`, each with what
/// was deleted with it.
fn expire(db: &mut StateDb, remote: &dyn Remote, before_ms: i64) -> Result<usize, EngineError> {
    let tops = db.read(|s| {
        let mut tops = Vec::new();
        for r in s.removed_before(before_ms)? {
            if deleted_on_its_own(s, &r)? {
                tops.push(r);
            }
        }
        Ok(tops)
    })?;
    delete_groups(db, remote, tops)
}

/// Deletes for good `tops`, each with what was deleted with it: files before folders.
fn delete_groups(
    db: &mut StateDb,
    remote: &dyn Remote,
    tops: Vec<RemovedRow>,
) -> Result<usize, EngineError> {
    let mut rows = db.read(|s| {
        let mut rows = Vec::new();
        for top in tops {
            rows.extend(group(s, top)?);
        }
        Ok(rows)
    })?;
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

#[cfg(test)]
mod tests {
    use hbsync_state::MirrorRow;
    use hbsync_types::schema::Removal;
    use hbsync_types::{RemoteFileId, Uuid, VersionTag};

    use super::*;

    fn removed(node: NodeId, parent: NodeId, at_ms: i64, root: Option<NodeId>) -> RemovedRow {
        RemovedRow {
            row: MirrorRow {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: VersionTag(Uuid::new_v4()),
                node,
                parent,
                name: node.0.to_string(),
                kind: NodeKind::Folder,
                file: None,
                content_extra: None,
                seal: None,
            },
            removal: Removal { at_ms, root },
        }
    }

    /// Removed folders in a cycle (a remote one left them so) are restored once each, and the
    /// plan ends; so does the walk to the item that took them along.
    #[test]
    fn a_cycle_in_recently_deleted_ends() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (a, b) = (NodeId::new(), NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(a, b, 1, None))?;
            s.put_removed(&removed(b, a, 1, None))
        })?;
        let plan = db.read(|s| plan_restore(s, a))?.map(|p| p.rows.len());
        assert_eq!(plan, Some(2));
        let items = db.read(|s| list(s, 10))?;
        assert_eq!(items.len(), 2);
        Ok(())
    }

    /// A folder's restore brings back what was removed with it or after it, not what was removed
    /// from it before; each such item is one of its own in Recently deleted.
    #[test]
    fn what_went_before_its_folder_stays() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (f, early, with, deeper) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(early, f, 5, None))?;
            s.put_removed(&removed(f, NodeId::ROOT, 10, None))?;
            s.put_removed(&removed(with, f, 11, None))?;
            s.put_removed(&removed(deeper, with, 12, None))
        })?;
        let plan: Option<Vec<NodeId>> = db
            .read(|s| plan_restore(s, f))?
            .map(|p| p.rows.iter().map(|r| r.row.node).collect());
        assert_eq!(plan, Some(vec![f, with, deeper]));
        let tops: HashMap<NodeId, NodeId> = db
            .read(|s| list(s, 10))?
            .into_iter()
            .map(|d| (d.node, d.root))
            .collect();
        assert_eq!(tops[&early], early, "deleted on its own");
        assert_eq!(
            (tops[&with], tops[&deeper]),
            (f, f),
            "deleted with the folder"
        );
        Ok(())
    }

    /// An older writer's removals go by their `rm` roots: it removed what was in a folder before
    /// the folder.
    #[test]
    fn older_writers_go_by_their_roots() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (f, early, with) = (NodeId::new(), NodeId::new(), NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(early, f, 5, Some(early)))?;
            s.put_removed(&removed(with, f, 9, Some(f)))?;
            s.put_removed(&removed(f, NodeId::ROOT, 10, Some(f)))
        })?;
        let plan: Option<Vec<NodeId>> = db
            .read(|s| plan_restore(s, f))?
            .map(|p| p.rows.iter().map(|r| r.row.node).collect());
        assert_eq!(plan, Some(vec![f, with]));
        Ok(())
    }
}
