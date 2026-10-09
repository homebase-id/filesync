//! Remote nodes this device doesn't sync (sync semantics §10): the reconciler's view of them.
//!
//! A remote node whose path the device's filter matches is **masked**, with its subtree: for
//! this device it isn't there. What the device still holds of such a node is settled before
//! the merge:
//! - an unchanged local copy is trashed (the node moved away, to where this device doesn't
//!   follow), and the base is forgotten once nothing is left here;
//! - a local copy with changes the remote doesn't have (edited bytes, or never synced) is
//!   **detached**: it becomes a new node at its local place, created remotely like any new
//!   local item. So are the masked folders such an item sits in, so it keeps its place, and
//!   the masked folders holding anything else this device syncs.
//!
//! Detaching is a new identity rather than a resurrection: the node id is still live remotely
//! (at the masked place), so it can't be created again.
//!
//! The parent of a masked node keeps a live child (§6.3): a local delete of it can't take
//! children this device never saw.

use std::collections::{BTreeMap, HashMap, HashSet};

use hbsync_local::Filter;
use hbsync_types::{NodeId, NodeKind};

use crate::reconcile::Input;
use crate::tree::Tree;

/// The masks of one plan.
#[derive(Debug, Default)]
pub struct Masks {
    /// Remote nodes this device doesn't sync.
    pub masked: HashSet<NodeId>,
    /// Folders a masked live child keeps alive.
    pub keep_alive: HashSet<NodeId>,
    /// Local items that become new nodes: old id → new id.
    pub detached: BTreeMap<NodeId, NodeId>,
    /// The input as the merge sees it, if anything is masked: masked nodes left out of the
    /// remote tree, detached items under their new ids (and dirty), every masked node with a
    /// base or a local item dirty.
    pub input: Option<Input>,
}

impl Masks {
    pub fn of(input: &Input, filter: &Filter, device: &str) -> Masks {
        let masked = masked(&input.remote, filter);
        if masked.is_empty() {
            return Masks::default();
        }
        let keep_alive = masked
            .iter()
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
                    if masked.contains(&a) && seen.insert(a) {
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
        Masks {
            masked,
            keep_alive,
            detached,
            input: Some(view),
        }
    }
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
    for (b, x) in bytes.iter_mut().zip(h.to_le_bytes()) {
        *b ^= x;
    }
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // a well-formed v4 UUID
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    NodeId(hbsync_types::Uuid::from_bytes(bytes))
}
