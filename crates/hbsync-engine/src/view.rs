//! Building the reconciler's input from the state DB.
//!
//! For each dirty node the three sides are loaded, plus the context the rules need:
//! - every ancestor on every side (paths, cycles, orphans);
//! - the children of every folder involved (name clashes, live children of deleted folders);
//! - the full local subtree of a folder that is gone remotely (what a trash would take).
//!
//! The local side is translated from paths to the node model here: the parent is the node at
//! the parent path, the name is the remote spelling (recorded mapping or [`to_remote`]), and a
//! node parked under a temporary name is reported at its original place ([`parked_place`]).

use std::collections::{HashSet, VecDeque};

use hbsync_local::Filter;
use hbsync_namemap::to_remote;
use hbsync_platform::{Os, RelPath};
use hbsync_state::{BaseRow, LocalRow, MirrorRow, StateError, Store};
use hbsync_types::{FileContent, NodeId, NodeKind, NodeState, Tags};

use crate::exec::parked_name;
use crate::reconcile::{BaseExtra, Input, Removed};
use crate::scope::Scope;

/// The local state of one node, as the reconciler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNode {
    pub row: LocalRow,
    /// `None` if the parent path isn't known in the DB (the scan hasn't caught up).
    pub state: Option<NodeState>,
}

/// Reads one node's local state. `remote_tags`: the drive's tags of the node, if it is there
/// (what this device can't hold of them stands).
pub fn local_node(
    s: &Store<'_>,
    node: NodeId,
    os: Os,
    remote_tags: Option<&Tags>,
) -> Result<Option<LocalNode>, StateError> {
    let Some(row) = s.local_by_node(node)? else {
        return Ok(None);
    };
    let state = local_state(s, &row, os, remote_tags)?;
    Ok(Some(LocalNode { row, state }))
}

fn local_state(
    s: &Store<'_>,
    row: &LocalRow,
    os: Os,
    remote_tags: Option<&Tags>,
) -> Result<Option<NodeState>, StateError> {
    let base = s.get_base(row.node)?;
    // A parked node counts as being at its original place.
    let path = parked_place(s, row)?.unwrap_or_else(|| row.path.clone());
    let Some(local_name) = path.name() else {
        return Ok(None);
    };
    // Parking never leaves the folder, so the row's folder is the place's folder too.
    let Some(parent) = s.local_parent(row.node)? else {
        return Ok(None);
    };
    let name = match &base {
        Some(b) if b.local_name.as_deref() == Some(local_name) => b.state.name.clone(),
        _ => match to_remote(local_name, os) {
            Ok(n) => n,
            Err(_) => return Ok(None),
        },
    };
    // What this device can't hold of the tags is the drive's (else the agreed) (§11).
    let reference = match remote_tags {
        Some(t) => t.clone(),
        None => base
            .as_ref()
            .map(|b| b.state.tags.clone())
            .unwrap_or_default(),
    };
    Ok(Some(NodeState {
        parent,
        name,
        kind: row.kind,
        // An unhashed file is reported without content; the caller defers it.
        content: local_content(row, base.as_ref()),
        tags: Tags::fill(row.tags.as_ref(), &reference, os),
    }))
}

/// The synced content of a local file: `None` for folders and files not hashed yet. Where the
/// OS has no x bit, the base's value is kept.
pub fn local_content(row: &LocalRow, base: Option<&BaseRow>) -> Option<FileContent> {
    if row.kind != NodeKind::File {
        return None;
    }
    Some(FileContent {
        hash: row.hash?,
        size: row.fingerprint.size,
        mtime_ms: row.fingerprint.mtime_ns.div_euclid(1_000_000),
        executable: row.executable.unwrap_or_else(|| {
            base.and_then(|b| b.state.content)
                .is_some_and(|c| c.executable)
        }),
    })
}

/// Where a parked node really belongs: its current folder, under the name it had before it
/// was parked. `None` if it isn't parked: no record, or it no longer carries its temporary
/// name (the user renamed it, and the record is void). Parking never leaves the folder, so the
/// place follows the folder (or the item) wherever it is moved meanwhile.
pub fn parked_place(s: &Store<'_>, row: &LocalRow) -> Result<Option<RelPath>, StateError> {
    let Some(name) = s.get_parked(row.node)? else {
        return Ok(None);
    };
    if row.path.name() != Some(parked_name(row.node).as_str()) {
        return Ok(None);
    }
    Ok(row.path.parent().unwrap_or_default().join(&name).ok())
}

/// Loads everything the reconciler needs for `dirty`; `filter` is this device's (§10), `scope`
/// the engine's (§12).
pub fn build_input(
    s: &Store<'_>,
    dirty: &[NodeId],
    os: Os,
    filter: &Filter,
    scope: &Scope,
) -> Result<Input, StateError> {
    let mut b = Builder {
        s,
        os,
        input: Input::default(),
        base_done: HashSet::new(),
        mirror_done: HashSet::new(),
        local_done: HashSet::new(),
    };
    b.input.dirty = dirty.iter().copied().collect();
    // The feed is drained before every plan, and every loaded node brings its ancestors.
    b.input.remote_complete = true;

    let mut folders: HashSet<NodeId> = HashSet::from([NodeId::ROOT]);
    for &node in dirty {
        b.load(node)?;
        for tree in [&b.input.base, &b.input.remote, &b.input.local] {
            if let Some(state) = tree.get(node) {
                folders.insert(state.parent);
                if state.kind == NodeKind::Folder {
                    folders.insert(node);
                }
            }
        }
    }
    for folder in folders {
        b.load_children(folder)?;
    }
    // A folder that is gone remotely (or masked) but present locally may be trashed or
    // detached: load all of it.
    // Each folder once: a dirty folder inside one already loaded is in it.
    let hidden = crate::mask::hidden(&b.input.remote, filter, scope);
    let mut loaded: HashSet<NodeId> = HashSet::new();
    for &node in dirty {
        if !loaded.contains(&node)
            && b.input.base.contains(node)
            && (!b.input.remote.contains(node) || hidden.contains(&node))
            && let Some(row) = s
                .local_by_node(node)?
                .filter(|r| r.kind == NodeKind::Folder)
        {
            for child in s.local_subtree(&row.path)? {
                loaded.insert(child.node);
                b.load(child.node)?;
            }
        }
    }
    // Whether a masked folder synced here leaves the drive depends on everything in it
    // remotely (§10): load all of it.
    let masked = crate::mask::masked(&b.input.remote, filter);
    let mut expanded: HashSet<NodeId> = HashSet::new();
    for &node in dirty {
        if !(masked.contains(&node) && b.input.base.contains(node)) {
            continue;
        }
        let mut folders = vec![node];
        while let Some(folder) = folders.pop() {
            if b.input.remote.get(folder).map(|s| s.kind) != Some(NodeKind::Folder)
                || !expanded.insert(folder)
            {
                continue;
            }
            b.load_children(folder)?;
            folders.extend(b.input.remote.children(folder));
        }
    }
    // A role folder at the top that this device holds nothing of (always, but for a bug) isn't
    // the plan's at all: nothing to mask. One moved into a folder stays, masked, and keeps that
    // folder alive (§6.3).
    let top: Vec<NodeId> = scope
        .outside
        .iter()
        .copied()
        .filter(|n| b.input.remote.get(*n).is_some_and(|s| s.parent.is_root()))
        .collect();
    let held = |n: &NodeId| b.input.base.contains(*n) || b.input.local.contains(*n);
    for folder in top {
        let subtree = b.input.remote.subtree(folder);
        if !subtree.iter().any(held) {
            for node in subtree {
                b.input.remote.remove(node);
            }
        }
    }
    // The folders in Recently deleted that remote nodes are in, and the removed ones above them
    // in this folder's part of the drive: what a folder's delete took along goes by their times
    // (node schema, Recently deleted).
    for mut folder in b.input.remote.missing_parents() {
        while !b.input.removed.contains_key(&folder)
            && !scope.outside.contains(&folder)
            && let Some(r) = s.removed_by_node(folder)?
            && let Some(state) = r.row.state()
        {
            b.input.removed.insert(
                folder,
                Removed {
                    at: r.removed_at,
                    state,
                },
            );
            folder = r.row.parent;
        }
    }
    Ok(b.input)
}

struct Builder<'s, 'c> {
    s: &'s Store<'c>,
    os: Os,
    input: Input,
    base_done: HashSet<NodeId>,
    mirror_done: HashSet<NodeId>,
    local_done: HashSet<NodeId>,
}

impl Builder<'_, '_> {
    /// Loads a node on all three sides, and its ancestors.
    fn load(&mut self, node: NodeId) -> Result<(), StateError> {
        let mut queue = VecDeque::from([node]);
        while let Some(n) = queue.pop_front() {
            if n.is_root() {
                continue;
            }
            if self.base_done.insert(n)
                && let Some(row) = self.s.get_base(n)?
            {
                queue.push_back(row.state.parent);
                self.add_base(row);
            }
            if self.mirror_done.insert(n)
                && let Some(row) = self.s.get_mirror(n)?
            {
                queue.push_back(row.parent);
                self.add_mirror(row);
            }
            if self.local_done.insert(n)
                && let Some(row) = self.s.local_by_node(n)?
            {
                if let Some(parent) = self.s.local_parent(n)? {
                    queue.push_back(parent);
                }
                self.add_local(row)?;
            }
        }
        Ok(())
    }

    /// Loads the children of `folder` (which is loaded) on every side. The rows the listings
    /// return are used directly; `load` then only reads the sides they didn't cover.
    fn load_children(&mut self, folder: NodeId) -> Result<(), StateError> {
        let mut kids: Vec<NodeId> = Vec::new();
        for row in self.s.base_children(folder)? {
            kids.push(row.node);
            if self.base_done.insert(row.node) {
                self.add_base(row);
            }
        }
        for row in self.s.mirror_children(folder)? {
            kids.push(row.node);
            if self.mirror_done.insert(row.node) {
                self.add_mirror(row);
            }
        }
        let local_path = if folder.is_root() {
            Some(RelPath::root())
        } else {
            self.s.local_by_node(folder)?.map(|r| r.path)
        };
        if let Some(path) = local_path {
            for row in self.s.local_children(&path)? {
                kids.push(row.node);
                if self.local_done.insert(row.node) {
                    self.add_local(row)?;
                }
            }
        }
        for kid in kids {
            self.load(kid)?;
        }
        Ok(())
    }

    fn add_base(&mut self, row: BaseRow) {
        self.input.base_extra.insert(
            row.node,
            BaseExtra {
                local_hash: row.local_hash,
                content_state: row.content_state,
                version: Some(row.remote.version),
            },
        );
        self.input.base.insert(row.node, row.state);
    }

    fn add_mirror(&mut self, row: MirrorRow) {
        self.input.remote_refs.insert(row.node, row.remote_ref());
        if let Some(at) = row.changed_at {
            self.input.changed_at.insert(row.node, at);
        }
        if let Some(state) = row.state() {
            self.input.remote.insert(row.node, state);
        }
    }

    fn add_local(&mut self, row: LocalRow) -> Result<(), StateError> {
        let node = row.node;
        // The drive's tags, from the mirror row loaded already, or else read for them alone
        // (loading it here would skip its ancestors).
        let remote_tags = match self.input.remote.get(node) {
            Some(r) => Some(r.tags.clone()),
            None if self.mirror_done.contains(&node) => None,
            None => self.s.get_mirror(node)?.map(|m| m.tags()),
        };
        match local_state(self.s, &row, self.os, remote_tags.as_ref())? {
            Some(state) => {
                if state.kind == NodeKind::File && state.content.is_none() {
                    self.input.local_unhashed.insert(node);
                }
                self.input.local.insert(node, state);
            }
            // The scan hasn't recorded the parent yet: decide later.
            None => {
                self.input.local_unhashed.insert(node);
            }
        }
        Ok(())
    }
}
