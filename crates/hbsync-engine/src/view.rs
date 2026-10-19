//! Building the reconciler's input from the state DB.
//!
//! For each dirty node the three sides are loaded, plus the context the rules need:
//! - every ancestor on every side (paths, cycles, orphans);
//! - in each folder it is in on any side, the children whose names may clash with its names
//!   there (name clashes): [`build_narrow`] reads those by their sibling hash, [`build_input`]
//!   all children;
//! - the children of a dirty folder that may go (live children keep it);
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
use crate::plan::Listed;
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

/// Loads everything the reconciler needs for `dirty`, with every child of each folder involved;
/// `filter` is this device's (§10), `scope` the engine's (§12).
pub fn build_input(
    s: &Store<'_>,
    dirty: &[NodeId],
    os: Os,
    filter: &Filter,
    scope: &Scope,
) -> Result<Input, StateError> {
    build(s, dirty, os, filter, scope, None)
}

/// [`build_input`] with only the children the rules may need, except in the folders in `full`
/// ([`Input::listed`]): planning a change in a large folder reads a few rows, not the folder.
/// A plan that needs more says so ([`crate::plan::Plan::unlisted`]).
pub fn build_narrow(
    s: &Store<'_>,
    dirty: &[NodeId],
    os: Os,
    filter: &Filter,
    scope: &Scope,
    full: &HashSet<NodeId>,
) -> Result<Input, StateError> {
    build(s, dirty, os, filter, scope, Some(full))
}

/// Past this many names to look up in one folder, it is listed whole: one query instead.
const NAMED_MAX: usize = 32;

fn build(
    s: &Store<'_>,
    dirty: &[NodeId],
    os: Os,
    filter: &Filter,
    scope: &Scope,
    full: Option<&HashSet<NodeId>>,
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

    for &node in dirty {
        b.load(node)?;
    }
    let mut listed = Listed::default();
    match full {
        None => {
            // Every child of every folder involved, and of the root.
            listed.full.insert(NodeId::ROOT);
            for &node in dirty {
                for tree in [&b.input.base, &b.input.remote, &b.input.local] {
                    if let Some(state) = tree.get(node) {
                        listed.full.insert(state.parent);
                        if state.kind == NodeKind::Folder {
                            listed.full.insert(node);
                        }
                    }
                }
            }
        }
        Some(full) => {
            // In each folder a dirty node is in, what may clash with its names; all of a dirty
            // folder that may go (gone on a side, hidden, or in a folder gone remotely).
            listed.full.extend(full);
            let hidden = crate::mask::hidden(&b.input.remote, filter, scope);
            for &node in dirty {
                let mut folder = false;
                for tree in [&b.input.base, &b.input.remote, &b.input.local] {
                    if let Some(state) = tree.get(node) {
                        listed
                            .named
                            .entry(state.parent)
                            .or_default()
                            .insert(hbsync_namemap::sibling_hash(&state.name));
                        folder |= state.kind == NodeKind::Folder;
                    }
                }
                let is_folder =
                    |t: &crate::tree::Tree| t.get(node).is_some_and(|s| s.kind == NodeKind::Folder);
                let stays = is_folder(&b.input.local)
                    && is_folder(&b.input.remote)
                    && b.input.remote.path(node).is_some()
                    && !hidden.contains(&node);
                if folder && !stays {
                    listed.full.insert(node);
                }
            }
            let Listed { full, named } = &mut listed;
            named.retain(|folder, keys| {
                if keys.len() > NAMED_MAX {
                    full.insert(*folder);
                }
                !full.contains(folder)
            });
            // A parked item is at its original name (`parked_place`), which its row's hash
            // isn't: those in the folders read by name come along (there are hardly ever any).
            for node in s.all_parked()? {
                if s.local_parent(node)?
                    .is_some_and(|p| listed.named.contains_key(&p))
                {
                    b.load(node)?;
                }
            }
        }
    }
    let mut folders: Vec<NodeId> = listed.full.iter().copied().collect();
    folders.sort();
    for folder in folders {
        b.load_children(folder)?;
    }
    let mut named: Vec<(NodeId, Vec<i32>)> = listed
        .named
        .iter()
        .map(|(folder, keys)| {
            let mut keys: Vec<i32> = keys.iter().copied().collect();
            keys.sort();
            (*folder, keys)
        })
        .collect();
    named.sort();
    for (folder, keys) in named {
        b.load_named(folder, &keys)?;
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
            if listed.full.insert(folder) {
                listed.named.remove(&folder);
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
    // A remote node whose folder isn't in the mirror may be an orphan, which the main folder
    // puts into Recovered files: where that is (live, removed or synced here before), and what
    // in it may clash with the names coming (the plan asks for the rest: `Plan::unlisted`).
    let mut missing = b.input.remote.missing_parents();
    if scope.top.is_root() && !missing.is_empty() {
        b.load(NodeId::RECOVERED)?;
        missing.insert(NodeId::RECOVERED);
    }
    // The folders in Recently deleted that remote nodes are in, and the removed ones above them
    // in this folder's part of the drive: what a folder's delete took along goes by their times
    // (node schema, Recently deleted).
    for mut folder in missing {
        while !b.input.removed.contains_key(&folder)
            && !scope.outside.contains(&folder)
            && let Some(r) = s.removed_by_node(folder)?
            && let Some(state) = r.row.state()
        {
            b.input.removed.insert(
                folder,
                Removed {
                    at: r.time(),
                    state,
                },
            );
            folder = r.row.parent;
        }
    }
    if full.is_some() {
        b.input.listed = Some(listed);
    }
    Ok(b.input)
}

/// Whether the mirror no longer has `node` as the plan reads it: not there, or taken along by
/// the folder in Recently deleted its folders reach first in this folder's part of the drive,
/// written no later than that folder's removal (the reconciler's `taken_along`; one outside,
/// as the view leaves it out, isn't this folder's to bring back).
pub(crate) fn gone_on_drive(
    s: &Store<'_>,
    node: NodeId,
    scope: &Scope,
) -> Result<bool, StateError> {
    let Some(row) = s.get_mirror(node)? else {
        return Ok(true);
    };
    let coords = s.coords()?;
    let Some((folder, at)) = s.removed_above(coords.to_drive(row.parent), coords.top)? else {
        return Ok(false);
    };
    if scope.outside.contains(&coords.from_drive(folder)) {
        return Ok(false);
    }
    let removed = hbsync_types::activity::removal_time(at);
    Ok(row.changed_at.is_none_or(|c| c <= removed))
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
        self.load_kids(folder, None)
    }

    /// Loads the children of `folder` (which is loaded) whose names have one of the sibling
    /// hashes `keys`, on every side.
    fn load_named(&mut self, folder: NodeId, keys: &[i32]) -> Result<(), StateError> {
        self.load_kids(folder, Some(keys))
    }

    /// The children of `folder` on every side: all, or those with one of `keys`.
    fn load_kids(&mut self, folder: NodeId, keys: Option<&[i32]>) -> Result<(), StateError> {
        let s = self.s;
        let path = if folder.is_root() {
            Some(RelPath::root())
        } else {
            s.local_by_node(folder)?.map(|r| r.path)
        };
        let (mut base, mut mirror, mut local) = (Vec::new(), Vec::new(), Vec::new());
        match keys {
            None => {
                base = s.base_children(folder)?;
                mirror = s.mirror_children(folder)?;
                if let Some(path) = &path {
                    local = s.local_children(path)?;
                }
            }
            Some(keys) => {
                for &key in keys {
                    base.extend(s.base_named(folder, key)?);
                    mirror.extend(s.mirror_named(folder, key)?);
                    if let Some(path) = &path {
                        local.extend(s.local_named(path, key)?);
                    }
                }
            }
        }
        let mut kids: Vec<NodeId> = Vec::new();
        for row in base {
            kids.push(row.node);
            if self.base_done.insert(row.node) {
                self.add_base(row);
            }
        }
        for row in mirror {
            kids.push(row.node);
            if self.mirror_done.insert(row.node) {
                self.add_mirror(row);
            }
        }
        for row in local {
            kids.push(row.node);
            if self.local_done.insert(row.node) {
                self.add_local(row)?;
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
