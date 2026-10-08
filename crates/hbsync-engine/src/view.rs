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

use std::collections::{HashMap, HashSet, VecDeque};

use hbsync_namemap::to_remote;
use hbsync_platform::{Os, RelPath};
use hbsync_state::{BaseRow, LocalRow, MirrorRow, StateError, Store};
use hbsync_types::{FileContent, NodeId, NodeKind, NodeState};

use crate::exec::parked_name;
use crate::reconcile::{BaseExtra, Input, RemoteRef};

/// The local state of one node, as the reconciler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNode {
    pub row: LocalRow,
    /// `None` if the parent path isn't known in the DB (the scan hasn't caught up).
    pub state: Option<NodeState>,
}

/// Reads one node's local state.
pub fn local_node(s: &Store<'_>, node: NodeId, os: Os) -> Result<Option<LocalNode>, StateError> {
    let Some(row) = s.local_by_node(node)? else {
        return Ok(None);
    };
    let state = local_state(s, &row, os)?;
    Ok(Some(LocalNode { row, state }))
}

fn local_state(s: &Store<'_>, row: &LocalRow, os: Os) -> Result<Option<NodeState>, StateError> {
    let base = s.get_base(row.node)?;
    // A parked node counts as being at its original place.
    let path = parked_place(s, row)?.unwrap_or_else(|| row.path.clone());
    let Some(local_name) = path.name() else {
        return Ok(None);
    };
    let parent_path = path.parent().unwrap_or_default();
    let parent = if parent_path.is_root() {
        NodeId::ROOT
    } else {
        match s.get_local(&parent_path)? {
            Some(p) => p.node,
            None => return Ok(None),
        }
    };
    let name = match &base {
        Some(b) if b.local_name.as_deref() == Some(local_name) => b.state.name.clone(),
        _ => match to_remote(local_name, os) {
            Ok(n) => n,
            Err(_) => return Ok(None),
        },
    };
    let content = match (row.kind, row.hash) {
        (NodeKind::Folder, _) => None,
        (NodeKind::File, Some(hash)) => Some(FileContent {
            hash,
            size: row.fingerprint.size,
            mtime_ms: row.fingerprint.mtime_ns.div_euclid(1_000_000),
            // Where the OS has no x bit, keep the synced value.
            executable: row.executable.unwrap_or_else(|| {
                base.as_ref()
                    .and_then(|b| b.state.content)
                    .is_some_and(|c| c.executable)
            }),
        }),
        (NodeKind::File, None) => {
            // Unhashed: report the place, with a placeholder; the caller defers it.
            return Ok(Some(NodeState {
                parent,
                name,
                kind: NodeKind::File,
                content: None,
            }));
        }
    };
    Ok(Some(NodeState {
        parent,
        name,
        kind: row.kind,
        content,
    }))
}

/// Where a parked node really belongs: its current folder, under the name it had before it
/// was parked. `None` if it isn't parked: no record, or it no longer carries its temporary
/// name (the user renamed it, and the record is void). Parking never leaves the folder, so the
/// place follows the folder (or the item) wherever it is moved meanwhile.
pub fn parked_place(s: &Store<'_>, row: &LocalRow) -> Result<Option<RelPath>, StateError> {
    let Some((_, original)) = s.get_parked(row.node)? else {
        return Ok(None);
    };
    if row.path.name() != Some(parked_name(row.node).as_str()) {
        return Ok(None);
    }
    let Some(name) = original.name() else {
        return Ok(None);
    };
    Ok(row.path.parent().unwrap_or_default().join(name).ok())
}

/// Loads everything the reconciler needs for `dirty`.
pub fn build_input(
    s: &Store<'_>,
    dirty: &[NodeId],
    os: Os,
    remote_complete: bool,
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
    b.input.remote_complete = remote_complete;

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
    // A folder that is gone remotely but present locally may be trashed: load all of it.
    for &node in dirty {
        if b.input.base.contains(node)
            && !b.input.remote.contains(node)
            && let Some(row) = s
                .local_by_node(node)?
                .filter(|r| r.kind == NodeKind::Folder)
        {
            for child in s.local_subtree(&row.path)? {
                b.load(child.node)?;
            }
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
                if let Some(parent) = row.path.parent().filter(|p| !p.is_root())
                    && let Some(p) = self.s.get_local(&parent)?
                {
                    queue.push_back(p.node);
                }
                self.add_local(row)?;
            }
        }
        Ok(())
    }

    fn load_children(&mut self, folder: NodeId) -> Result<(), StateError> {
        let mut kids: Vec<NodeId> = Vec::new();
        kids.extend(self.s.base_children(folder)?.into_iter().map(|r| r.node));
        kids.extend(self.s.mirror_children(folder)?.into_iter().map(|r| r.node));
        let local_path = if folder.is_root() {
            Some(RelPath::root())
        } else {
            self.s.local_by_node(folder)?.map(|r| r.path)
        };
        if let Some(path) = local_path {
            kids.extend(self.s.local_children(&path)?.into_iter().map(|r| r.node));
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
        self.input.remote_refs.insert(
            row.node,
            RemoteRef {
                file_id: row.file_id,
                version: row.version,
            },
        );
        match row.state() {
            Some(state) => {
                self.input.remote.insert(row.node, state);
            }
            None => {
                // A file without a hash: present, but its content can't be compared.
                self.input.remote_unhashed.insert(row.node);
                self.input.remote.insert(
                    row.node,
                    NodeState {
                        parent: row.parent,
                        name: row.name,
                        kind: row.kind,
                        content: None,
                    },
                );
            }
        }
    }

    fn add_local(&mut self, row: LocalRow) -> Result<(), StateError> {
        let node = row.node;
        match local_state(self.s, &row, self.os)? {
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

/// Node → local path for every loaded local row (used by the executor's checks).
pub fn local_paths(
    s: &Store<'_>,
    nodes: &[NodeId],
) -> Result<HashMap<NodeId, RelPath>, StateError> {
    let mut out = HashMap::new();
    for &n in nodes {
        if let Some(row) = s.local_by_node(n)? {
            out.insert(n, row.path);
        }
    }
    Ok(out)
}
