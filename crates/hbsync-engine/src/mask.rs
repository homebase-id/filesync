//! Remote nodes this device doesn't sync (sync semantics §10): the reconciler's view of them.
//!
//! A remote node whose path the device's filter matches is **masked**, with its subtree: for
//! this device it isn't there. What the device still holds of such a node is settled before
//! the merge:
//! - an unchanged local copy is trashed (the node moved away, to where this device doesn't
//!   follow), and its base is forgotten in the same step: a base left behind would read as a
//!   local delete once the node is back in view;
//! - a local copy with changes the remote doesn't have (edited bytes, or never synced) is
//!   **detached**: it becomes a new node at its local place, created remotely like any new
//!   local item, and the old base goes with the detach. So are the masked folders such an item
//!   sits in, so it keeps its place, and the masked folders holding anything else this device
//!   syncs.
//!
//! Detaching is a new identity rather than a resurrection: the node id is still live remotely
//! (at the masked place), so it can't be created again.
//!
//! The parent of a masked node keeps a live child (§6.3): a local delete of it can't take
//! children this device never saw.
//!
//! A masked node this device synced *at a place it now filters* (its rules changed) is
//! **leaving**: ignored means kept here only, so it leaves the drive, unless the remote changed
//! it since (a folder: unless it holds anything that isn't leaving). If the user brings the
//! local item back into view instead, it isn't masked at all, and its local place wins over a
//! remote move (which would take it out of view again). The names masked nodes hold stay taken
//! for this device's own items.

use std::collections::{BTreeMap, HashMap, HashSet};

use hbsync_local::Filter;
use hbsync_platform::name::name_key;
use hbsync_types::{NodeId, NodeKind};

use crate::reconcile::Input;
use crate::tree::Tree;

/// The masks of one plan.
#[derive(Debug, Default)]
pub struct Masks {
    /// Remote nodes this device doesn't sync.
    pub masked: HashSet<NodeId>,
    /// Folders a masked live child (one that isn't leaving) keeps alive.
    pub keep_alive: HashSet<NodeId>,
    /// Masked nodes to delete remotely: synced here, now filtered where they were.
    pub leaving: HashSet<NodeId>,
    /// Nodes at a filtered place remotely whose local item the user brought back into view:
    /// not masked, and the local place wins a move conflict.
    pub brought_back: HashSet<NodeId>,
    /// The name keys masked nodes hold, per folder: still taken remotely.
    pub names: HashMap<NodeId, HashSet<String>>,
    /// Local items that become new nodes: old id → new id.
    pub detached: BTreeMap<NodeId, NodeId>,
    /// The input as the merge sees it, if anything is masked: masked nodes left out of the
    /// remote tree, detached items under their new ids (and dirty), every masked node with a
    /// base or a local item dirty.
    pub input: Option<Input>,
}

impl Masks {
    pub fn of(input: &Input, filter: &Filter, device: &str) -> Masks {
        let mut masked = masked(&input.remote, filter);
        if masked.is_empty() {
            return Masks::default();
        }
        // A node synced at a place this device filters now, whose local item the user brought
        // back into view (moved out of the ignored place), syncs as usual.
        let filtered_base = self::masked(&input.base, filter);
        let brought_back: HashSet<NodeId> = masked
            .iter()
            .copied()
            .filter(|m| filtered_base.contains(m) && input.local.contains(*m))
            .collect();
        masked.retain(|m| !brought_back.contains(m));
        if masked.is_empty() {
            return Masks {
                brought_back,
                ..Masks::default()
            };
        }
        let leaving: HashSet<NodeId> = masked
            .iter()
            .copied()
            .filter(|m| filtered_base.contains(m))
            .filter(|m| match (input.base.get(*m), input.remote.get(*m)) {
                (Some(b), Some(r)) => {
                    b.same_place(r)
                        && b.content.map(|c| (c.hash, c.executable))
                            == r.content.map(|c| (c.hash, c.executable))
                }
                _ => false,
            })
            .collect();
        let leaving = only_whole_folders(&input.remote, leaving);
        let keep_alive = masked
            .iter()
            .filter(|m| !leaving.contains(m))
            .filter_map(|m| input.remote.get(*m).map(|s| s.parent))
            .filter(|p| !p.is_root() && !masked.contains(p))
            .collect();

        // What must stay here, and the masked folders it needs.
        let mut detach: Vec<NodeId> = Vec::new();
        let mut seen: HashSet<NodeId> = HashSet::new();
        for (node, local) in input.local.iter() {
            let needs_place = if masked.contains(&node) {
                let changed = match (input.base.get(node), local.kind) {
                    (None, _) => true,
                    (Some(_), NodeKind::Folder) => false,
                    (Some(base), NodeKind::File) => match (base.content, local.content) {
                        (Some(b), Some(l)) => {
                            let base_bytes = input
                                .base_extra
                                .get(&node)
                                .and_then(|e| e.local_hash)
                                .unwrap_or(b.hash);
                            base_bytes != l.hash || b.executable != l.executable
                        }
                        // Not hashed yet: it may hold anything.
                        _ => true,
                    },
                };
                if changed && seen.insert(node) {
                    detach.push(node);
                }
                changed
            } else {
                masked.contains(&local.parent)
            };
            if needs_place {
                for a in input.local.ancestors(node) {
                    if masked.contains(&a) && input.local.contains(a) && seen.insert(a) {
                        detach.push(a);
                    }
                }
            }
        }
        let detached: BTreeMap<NodeId, NodeId> = detach
            .into_iter()
            .map(|n| (n, detach_id(n, device)))
            .collect();

        let mut local = Tree::new();
        for (node, state) in input.local.iter() {
            let mut state = state.clone();
            if let Some(p) = detached.get(&state.parent) {
                state.parent = *p;
            }
            local.insert(detached.get(&node).copied().unwrap_or(node), state);
        }
        let mut remote = input.remote.clone();
        for m in &masked {
            remote.remove(*m);
        }
        let mut dirty = input.dirty.clone();
        dirty.extend(detached.values().copied());
        dirty.extend(
            masked
                .iter()
                .copied()
                .filter(|m| input.base.contains(*m) || input.local.contains(*m)),
        );
        let local_unhashed = input
            .local_unhashed
            .iter()
            .map(|n| detached.get(n).copied().unwrap_or(*n))
            .collect();
        let view = Input {
            base: input.base.clone(),
            local,
            remote,
            base_extra: input.base_extra.clone(),
            remote_refs: input.remote_refs.clone(),
            local_unhashed,
            remote_unhashed: input.remote_unhashed.clone(),
            dirty,
            remote_complete: input.remote_complete,
        };
        let mut names: HashMap<NodeId, HashSet<String>> = HashMap::new();
        for m in &masked {
            if let Some(state) = input.remote.get(*m) {
                names
                    .entry(state.parent)
                    .or_default()
                    .insert(name_key(&state.name));
            }
        }
        Masks {
            masked,
            keep_alive,
            leaving,
            brought_back,
            names,
            detached,
            input: Some(view),
        }
    }
}

/// `leaving` without the folders that hold anything else remotely (another device added it):
/// those stay, and only what this device synced in them leaves.
fn only_whole_folders(remote: &Tree, mut leaving: HashSet<NodeId>) -> HashSet<NodeId> {
    let mut folders: Vec<(usize, NodeId)> = leaving
        .iter()
        .filter(|n| remote.get(**n).is_some_and(|s| s.kind == NodeKind::Folder))
        .map(|n| (remote.ancestors(*n).len(), *n))
        .collect();
    // Deepest first: a folder's children are decided before it.
    folders.sort_by(|a, b| b.cmp(a));
    for (_, folder) in folders {
        if remote.children(folder).any(|c| !leaving.contains(&c)) {
            leaving.remove(&folder);
        }
    }
    leaving
}

/// The remote nodes `filter` doesn't sync: those whose path it matches, and everything below
/// them. A node not connected to the root in `remote` isn't judged.
pub fn masked(remote: &Tree, filter: &Filter) -> HashSet<NodeId> {
    // Per folder: its path, and whether it is masked.
    let mut folders: HashMap<NodeId, Option<(String, bool)>> = HashMap::new();
    folders.insert(NodeId::ROOT, Some((String::new(), false)));
    let mut out = HashSet::new();
    for (node, state) in remote.iter() {
        let Some((path, parent_masked)) = folder_path(remote, filter, state.parent, &mut folders)
        else {
            continue;
        };
        if parent_masked
            || !filter
                .in_folder(&path, &state.name, state.kind == NodeKind::Folder)
                .is_sync()
        {
            out.insert(node);
        }
    }
    out
}

/// The path of `folder` and whether it is masked, memoized in `memo`. `None` if it isn't
/// connected to the root (a missing ancestor, or a cycle).
fn folder_path(
    remote: &Tree,
    filter: &Filter,
    folder: NodeId,
    memo: &mut HashMap<NodeId, Option<(String, bool)>>,
) -> Option<(String, bool)> {
    // Up to the nearest folder already known, then down again.
    let mut chain: Vec<NodeId> = Vec::new();
    let mut current = folder;
    let mut known = loop {
        if let Some(known) = memo.get(&current) {
            break known.clone();
        }
        if chain.contains(&current) {
            break None; // a cycle
        }
        chain.push(current);
        match remote.get(current) {
            Some(state) => current = state.parent,
            None => break None,
        }
    };
    while let Some(node) = chain.pop() {
        known = match (known, remote.get(node)) {
            (Some((path, masked)), Some(state)) => {
                let masked = masked || !filter.in_folder(&path, &state.name, true).is_sync();
                let path = if path.is_empty() {
                    state.name.clone()
                } else {
                    format!("{path}/{}", state.name)
                };
                Some((path, masked))
            }
            _ => None,
        };
        memo.insert(node, known.clone());
    }
    known
}

/// The new id of a detached item: fixed for the node and the device, so a replan before the
/// detach ran picks the same one, and two devices detaching the same node don't collide.
fn detach_id(node: NodeId, device: &str) -> NodeId {
    let mut bytes = *node.0.as_bytes();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in device.bytes().chain(*b"detach") {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    let mix = h
        .to_le_bytes()
        .into_iter()
        .chain(h.rotate_left(29).to_le_bytes());
    for (b, x) in bytes.iter_mut().zip(mix) {
        *b ^= x;
    }
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // a well-formed v4 UUID
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    NodeId(hbsync_types::Uuid::from_bytes(bytes))
}
