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
//! A node this device synced *at a place it now filters* (its rules changed) is **leaving**:
//! ignored means kept here only, so it leaves the drive, unless the remote changed it since (a
//! folder: unless it holds anything that isn't leaving). It isn't masked: the scanner no longer
//! reports it, which the merge sees as an ordinary local delete. If the user brings the
//! local item back into view instead, it isn't masked at all, and its local place wins over a
//! remote move (which would take it out of view again). The names masked nodes hold stay taken
//! for this device's own items.
//!
//! What lies outside the engine's scope (§12: the role folders, for the main folder) is masked
//! the same way, but is never leaving or brought back: it is synced elsewhere, by another
//! engine or another device. A role folder's name stays free for the scope's own items.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::Infallible;

use hbsync_local::Filter;
use hbsync_platform::name::name_key;
use hbsync_types::{NodeId, NodeKind};

use crate::id::derived_id;
use crate::plan::Masking;
use crate::reconcile::Input;
use crate::scope::Scope;
use crate::tree::Tree;
use crate::walk::End;

/// The masks of one plan.
#[derive(Debug, Default)]
pub(crate) struct Masks {
    /// Remote nodes this device doesn't sync.
    pub masked: HashSet<NodeId>,
    /// Folders a masked live child keeps alive.
    pub keep_alive: HashSet<NodeId>,
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
    pub(crate) fn of(
        input: &Input,
        masking: Masking<'_>,
        computer: &str,
        taken: &HashSet<NodeId>,
    ) -> Masks {
        let Masking { filter, scope } = masking;
        // Where a node is: through the folders in Recently deleted too (a node in one is where
        // that folder was).
        let with_removed;
        let paths = if input.removed.is_empty() {
            &input.remote
        } else {
            let mut tree = input.remote.clone();
            for (node, r) in &input.removed {
                tree.insert(*node, r.state.clone());
            }
            with_removed = tree;
            &with_removed
        };
        let outside = scope.out_of_scope(paths);
        // The filter's masks, on what the scope keeps; the rules below are the filter's (§10).
        let mut masked: HashSet<NodeId> = masked(&input.remote, filter)
            .into_iter()
            .filter(|m| !outside.contains(m))
            .collect();
        // A folder in Recently deleted the filter masks isn't this device's to bring back.
        if !input.removed.is_empty() {
            let filtered = self::masked(paths, filter);
            masked.extend(input.removed.keys().filter(|n| filtered.contains(n)));
        }
        if masked.is_empty() && outside.is_empty() {
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
        let leaving: HashSet<NodeId> = masked
            .iter()
            .copied()
            .filter(|m| filtered_base.contains(m))
            .filter(|m| match (input.base.get(*m), input.remote.get(*m)) {
                // Unchanged remotely since it synced here: place, bytes and tags (§11).
                (Some(b), Some(r)) => {
                    b.same_place(r)
                        && b.content.map(|c| (c.hash, c.executable))
                            == r.content.map(|c| (c.hash, c.executable))
                        && b.tags == r.tags
                }
                _ => false,
            })
            .collect();
        // Leaving is an ordinary local delete: the scanner no longer reports the item.
        let leaving = only_whole_folders(&input.remote, leaving);
        masked.retain(|m| !leaving.contains(m));
        // The names the filter's masks hold stay taken; a role folder holds none for the scope:
        // a plain folder of the same name keeps it (two at the top, told apart by id).
        let mut names: HashMap<NodeId, HashSet<String>> = HashMap::new();
        for m in &masked {
            if let Some(state) = input.remote.get(*m) {
                names
                    .entry(state.parent)
                    .or_default()
                    .insert(name_key(&state.name));
            }
        }
        masked.extend(outside);
        if masked.is_empty() {
            return Masks {
                brought_back,
                ..Masks::default()
            };
        }
        let keep_alive = masked
            .iter()
            .filter_map(|m| input.remote.get(*m).map(|s| s.parent))
            .filter(|p| !p.is_root() && !masked.contains(p))
            .collect();

        // What must stay here, and the masked folders it needs.
        let mut detach: BTreeSet<NodeId> = BTreeSet::new();
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
                if changed {
                    detach.insert(node);
                }
                changed
            } else {
                masked.contains(&local.parent)
            };
            if needs_place {
                // Up to the first masked folder already in: what is above it is in too.
                for a in input.local.ancestors(node) {
                    if masked.contains(&a) && input.local.contains(a) && !detach.insert(a) {
                        break;
                    }
                }
            }
        }
        let detached: BTreeMap<NodeId, NodeId> = detach
            .into_iter()
            .map(|n| (n, detach_id(n, computer, taken)))
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
            dirty,
            remote_complete: input.remote_complete,
            removed: input.removed.clone(),
            changed_at: input.changed_at.clone(),
            coverage: input.coverage.clone(),
        };
        Masks {
            masked,
            keep_alive,
            brought_back,
            names,
            detached,
            input: Some(view),
        }
    }
}

/// What this device doesn't sync of `remote`: what `filter` matches (see [`masked`]) and what
/// lies outside `scope`.
pub(crate) fn hidden(remote: &Tree, filter: &Filter, scope: &Scope) -> HashSet<NodeId> {
    let mut hidden = masked(remote, filter);
    hidden.extend(scope.out_of_scope(remote));
    hidden
}

/// `leaving` without the folders that hold anything else remotely (another device added it):
/// those stay, and only what this device synced in them leaves. A file another writer put
/// something in (§6.3) is judged alike.
fn only_whole_folders(remote: &Tree, mut leaving: HashSet<NodeId>) -> HashSet<NodeId> {
    let mut folders: Vec<(usize, NodeId)> = leaving
        .iter()
        .filter(|n| remote.children(**n).next().is_some())
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
    let Ok(up) = crate::walk::up::<_, Infallible>(folder, |n| {
        Ok((!memo.contains_key(&n))
            .then(|| remote.get(n).map(|state| (n, state.parent)))
            .flatten())
    });
    let mut known = match up.end {
        End::Top => Some((String::new(), false)),
        End::At(n) => memo.get(&n).cloned().flatten(),
        End::Cycle | End::Deep => None,
    };
    for node in up.items.into_iter().rev() {
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

/// The new id of a detached item: fixed for the node and the computer ([`Options::computer`]),
/// so a replan before the detach ran picks the same one, and two computers detaching the same
/// node don't collide. Never a mix that undoes itself: an item detached twice must not get its
/// first id back (that id is still live remotely, maybe where this device doesn't sync). Nor
/// one in `taken`: the node detached here before, whose first copy holds that id still.
///
/// [`Options::computer`]: crate::plan::Options::computer
fn detach_id(node: NodeId, computer: &str, taken: &HashSet<NodeId>) -> NodeId {
    (0u32..)
        .map(|round| {
            let round = round.to_le_bytes();
            derived_id(&[node.0.as_bytes(), computer.as_bytes(), &round], b"detach")
        })
        .find(|id| !taken.contains(id))
        .unwrap_or_else(NodeId::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Detach ids are stored and on the drive: the hash must never change.
    #[test]
    fn detach_ids_are_stable() {
        let node = NodeId(
            "0274575b-c2cf-4ac7-b39b-d56b2c121b75"
                .parse()
                .expect("uuid"),
        );
        let once = detach_id(node, "laptop", &HashSet::new());
        let again = detach_id(node, "laptop", &HashSet::from([once]));
        assert_eq!(once.0.to_string(), "c6aa197b-18be-48cc-9349-15f8054da6c4");
        assert_eq!(again.0.to_string(), "f72689c1-6928-45fe-825e-bb8930f210bf");
    }

    #[test]
    fn detaching_twice_never_gives_the_first_id_back() {
        let none = HashSet::new();
        let node = NodeId::new();
        let once = detach_id(node, "a", &none);
        assert_ne!(once, node);
        assert_ne!(detach_id(once, "a", &none), node);
        assert_eq!(detach_id(node, "a", &none), once);
        assert_ne!(detach_id(node, "b", &none), once);
        // The same node detached here again, its first copy still around: another id.
        let again = detach_id(node, "a", &HashSet::from([once]));
        assert!(again != once && again != node);
        assert_eq!(again, detach_id(node, "a", &HashSet::from([once])));
    }
}
