//! Remote nodes this device doesn't sync (sync semantics §10): the reconciler's view of them.
//!
//! A remote node whose path the device's filter matches is **masked**, with its subtree: for
//! this device it isn't there. What the device still holds of such a node is settled before
//! the merge:
//! - an unchanged local copy is deleted (the node moved away, to where this device doesn't
//!   follow), and its base is forgotten in the same step: a base left behind would read as a
//!   local delete once the node is back in view;
//! - a local copy with changes the remote doesn't have (edited bytes, or never synced) is
//!   **detached**: it becomes a new node at its local place, created remotely like any new
//!   local item, and the old base goes with the detach. So are the masked folders such an item
//!   sits in, so it keeps its place, and the masked folders holding anything else this device
//!   syncs, but for what the plan moves out of a folder the filter masks: a folder held only
//!   for that is deleted as an unchanged copy, not made again on the drive, empty. The plan
//!   tells: made with every such folder detached first, it is made again with the items it
//!   moves out of one holding none ([`Masks::moved_away`], [`Masks::sparing`]), and kept
//!   where it still moves each of them out unforked ([`Masks::not_moved_away`]); one it
//!   doesn't holds its folder again, for another round (`reconcile`). The folder's forget
//!   expects only what stays in it: where a move didn't run, it goes stale.
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
use hbsync_types::{ContentState, NodeId, NodeKind, Role};

use crate::id::derived_id;
use crate::plan::{Masking, Op, Plan};
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
    /// The nodes the filter masks (the rest of `masked` is outside the scope), kept only where
    /// the input holds something removed (what a delete took along is read without them).
    pub filtered: HashSet<NodeId>,
    /// Local items that become new nodes: old id → new id.
    pub detached: BTreeMap<NodeId, NodeId>,
    /// The folder is read-only (§13): an executable bit changed here holds no item.
    read_only: bool,
    /// The detached folders held for the filter's masks alone, outside the scope (§12) and in
    /// no folder outside it: what moves away from one may spare it ([`Masks::moved_away`]).
    by_filter: HashSet<NodeId>,
    /// `detached` the other way round: a new id's old one.
    stored: HashMap<NodeId, NodeId>,
    /// The input as the merge sees it, if anything is masked: masked nodes left out of the
    /// remote tree, detached items under their new ids (and dirty), every masked node with a
    /// base or a local item dirty.
    pub input: Option<Input>,
}

/// The filter's masks of one plan, apart from the scope's ([`Masks::of`]): the reconciler and
/// the pages ([`crate::pages::dirty_elsewhere`]) read what a delete took along without them
/// alike.
pub(crate) struct FilterMasks {
    /// Remote nodes (and folders in Recently deleted) the filter masks, of what the scope
    /// keeps: not those brought back, nor those leaving.
    pub masked: HashSet<NodeId>,
    /// What lies outside the scope.
    pub outside: HashSet<NodeId>,
    /// Nodes at a filtered place whose local item the user brought back into view.
    pub brought_back: HashSet<NodeId>,
}

impl FilterMasks {
    pub(crate) fn of(input: &Input, masking: Masking<'_>) -> FilterMasks {
        let Masking {
            filter,
            scope,
            read_only,
        } = masking;
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
        let mut outside = scope.out_of_scope(paths);
        // A Desktop's folder the scope leaves out that this folder still holds from before
        // (added since, deleted on the drive meanwhile: the plan doesn't see it there) is left
        // out all the same: let go or detached as any masked node, never written back under
        // its id. (Not a role folder: its rules stay as they are.)
        let held = |n: &&NodeId| {
            Role::of(**n).is_none() && (input.base.contains(**n) || input.local.contains(**n))
        };
        outside.extend(scope.outside.iter().filter(held));
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
        // Read-only (§13), nothing is brought back nor leaves: the drive's place wins, and this
        // device filters it, so it stays masked, as one never synced here, the item here let
        // go of as any masked node's (detached where it changed); and nothing leaves the drive.
        if masked.is_empty() || read_only {
            return FilterMasks {
                masked,
                outside,
                brought_back: HashSet::new(),
            };
        }
        // A node synced at a place this device filters now, whose local item the user brought
        // back into view (moved out of the ignored place), syncs as usual. One whose base is
        // wanted (its item gone into a conflict copy, §5) has nothing here to keep or bring
        // back: it is masked as one never synced here.
        let mut filtered_base = self::masked(&input.base, filter);
        filtered_base.retain(|n| input.content_state(*n) != ContentState::Wanted);
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
        FilterMasks {
            masked,
            outside,
            brought_back,
        }
    }
}

impl Masks {
    /// The id the store knows `node` by: a detached item's old id (§10), else its own.
    pub(crate) fn store_id(&self, node: NodeId) -> NodeId {
        self.stored.get(&node).copied().unwrap_or(node)
    }

    pub(crate) fn of(
        input: &Input,
        masking: Masking<'_>,
        computer: &str,
        taken: &HashSet<NodeId>,
    ) -> Masks {
        let Masking { scope, .. } = masking;
        let FilterMasks {
            mut masked,
            outside,
            brought_back,
        } = FilterMasks::of(input, masking);
        if masked.is_empty() && outside.is_empty() && brought_back.is_empty() {
            return Masks::default();
        }
        // The names the filter's masks hold stay taken, and so does a Desktop's folder the
        // scope leaves out, where it sits in a folder the scope syncs (the computer's own in
        // Desktops): another item there takes another. A role folder holds none for the scope:
        // a plain folder of the same name keeps it (two at the top, told apart by id).
        let desktops = scope.outside.iter().filter(|t| {
            Role::of(**t).is_none() && input.remote.get(**t).is_some_and(|s| !s.parent.is_root())
        });
        let mut names: HashMap<NodeId, HashSet<String>> = HashMap::new();
        for m in masked.iter().chain(desktops) {
            if let Some(state) = input.remote.get(*m) {
                names
                    .entry(state.parent)
                    .or_default()
                    .insert(name_key(&state.name));
            }
        }
        // What a delete took along is read without them (only where something is removed).
        let filtered = match input.removed.is_empty() {
            true => HashSet::new(),
            false => masked.clone(),
        };
        masked.extend(outside.iter().copied());
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
        let detach = to_detach(input, &masked, &HashMap::new(), masking.read_only);
        // The scope's detached folders stay as they are (§12): not one outside it, nor one in a
        // folder outside it.
        let by_filter = (detach.iter().copied())
            .filter(|n| {
                input
                    .local
                    .get(*n)
                    .is_some_and(|l| l.kind == NodeKind::Folder)
                    && !outside.contains(n)
                    && !input.local.ancestors_iter(*n).any(|a| outside.contains(&a))
            })
            .collect();
        let masks = Masks {
            masked,
            keep_alive,
            brought_back,
            names,
            filtered,
            by_filter,
            read_only: masking.read_only,
            ..Masks::default()
        };
        masks.with_view(input, detach, computer, taken)
    }

    /// These masks, where an item in `away` ([`Masks::moved_away`]) doesn't keep the masked
    /// folder it is in here, nor the masked folders above that: `None` unless that detaches
    /// fewer items than `first_detached` (no folder spared), and other ones than these masks
    /// (planned so already): no view built.
    pub(crate) fn sparing(
        self,
        input: &Input,
        computer: &str,
        taken: &HashSet<NodeId>,
        away: &HashMap<NodeId, NodeId>,
        first_detached: usize,
    ) -> Option<Masks> {
        let detach = to_detach(input, &self.masked, away, self.read_only);
        let again = detach.iter().eq(self.detached.keys());
        (detach.len() < first_detached && !again)
            .then(|| self.with_view(input, detach, computer, taken))
    }

    /// These masks with `detach` detached, and the view of `input` with them: what doesn't
    /// depend on what is detached taken from the last view, if there was one.
    fn with_view(
        mut self,
        input: &Input,
        detach: BTreeSet<NodeId>,
        computer: &str,
        taken: &HashSet<NodeId>,
    ) -> Masks {
        let detached: BTreeMap<NodeId, NodeId> = detach
            .into_iter()
            .map(|n| (n, detach_id(n, computer, taken)))
            .collect();
        let mut view = match self.input.take() {
            Some(last) => last,
            None => {
                let mut remote = input.remote.clone();
                for m in &self.masked {
                    remote.remove(*m);
                }
                // Every field named: a new one is placed here, not left empty unseen. Those
                // that depend on what is detached are set below.
                Input {
                    base: input.base.clone(),
                    local: Tree::new(),
                    remote,
                    base_extra: input.base_extra.clone(),
                    remote_refs: input.remote_refs.clone(),
                    local_unhashed: HashSet::new(),
                    dirty: BTreeSet::new(),
                    remote_complete: input.remote_complete,
                    unreadable: input.unreadable.clone(),
                    removed: input.removed.clone(),
                    changed_at: input.changed_at.clone(),
                    coverage: input.coverage.clone(),
                    dirty_elsewhere: HashSet::new(),
                    going: HashSet::new(),
                    going_above: HashSet::new(),
                }
            }
        };
        let mut local = Tree::new();
        for (node, state) in input.local.iter() {
            let mut state = state.clone();
            if let Some(p) = detached.get(&state.parent) {
                state.parent = *p;
            }
            local.insert(detached.get(&node).copied().unwrap_or(node), state);
        }
        let mut dirty = input.dirty.clone();
        dirty.extend(detached.values().copied());
        dirty.extend(
            (self.masked.iter().copied())
                .filter(|m| input.base.contains(*m) || input.local.contains(*m)),
        );
        let renamed = |nodes: &HashSet<NodeId>| -> HashSet<NodeId> {
            (nodes.iter())
                .map(|n| detached.get(n).copied().unwrap_or(*n))
                .collect()
        };
        // What another page plans, but what this view plans now (masked, or detached: a new
        // node in its place).
        let elsewhere = |nodes: &HashSet<NodeId>| -> HashSet<NodeId> {
            (nodes.iter().copied())
                .filter(|n| !dirty.contains(n) && !detached.contains_key(n))
                .collect()
        };
        view.dirty_elsewhere = elsewhere(&input.dirty_elsewhere);
        view.going_above = elsewhere(&input.going_above);
        view.local_unhashed = renamed(&input.local_unhashed);
        view.going = renamed(&input.going);
        view.local = local;
        view.dirty = dirty;
        self.stored = detached.iter().map(|(old, new)| (*new, *old)).collect();
        self.detached = detached;
        self.input = Some(view);
        self
    }

    /// What `plan`, made with these masks, moves away from a folder the filter masks, detached
    /// to hold it (§10): each item, and the folder it goes to. Such an item needs no place
    /// there: planned again without it ([`Masks::sparing`]), the folder may not be needed
    /// at all, and goes as an unchanged copy instead of coming back on the drive, empty. Not
    /// one whose bytes fork into a conflict copy, which stays where the item is.
    pub(crate) fn moved_away(&self, plan: &Plan) -> HashMap<NodeId, NodeId> {
        let Some(view) = self.input.as_ref().filter(|_| !self.by_filter.is_empty()) else {
            return HashMap::new();
        };
        let mut moves: HashMap<NodeId, NodeId> = (plan.ops.iter())
            .filter_map(Op::moves_here)
            .filter(|(node, to)| {
                let from = view.local.get(*node).map(|l| l.parent);
                let out_of = from.and_then(|f| self.stored.get(&f));
                out_of.is_some_and(|f| self.by_filter.contains(f))
                    && !self.stored.contains_key(to)
                    && !self.masked.contains(to)
            })
            .collect();
        if !moves.is_empty() {
            let forked = forked(plan);
            moves.retain(|node, _| !forked.contains(node));
        }
        moves
    }

    /// The items of `away` whose folder these masks don't detach that `plan`, made with them,
    /// doesn't move where `away` says ([`Masks::moved_away`]), or forks: they keep their folder
    /// after all.
    pub(crate) fn not_moved_away(
        &self,
        input: &Input,
        plan: &Plan,
        away: &HashMap<NodeId, NodeId>,
    ) -> Vec<NodeId> {
        let moved: HashMap<NodeId, NodeId> = (plan.ops.iter())
            .filter_map(Op::moves_here)
            .filter(|(node, _)| away.contains_key(node))
            .collect();
        let forked = forked(plan);
        (away.iter())
            .filter(|(node, to)| {
                input
                    .local
                    .get(**node)
                    .is_some_and(|l| !self.detached.contains_key(&l.parent))
                    && (moved.get(*node) != Some(*to) || forked.contains(*node))
            })
            .map(|(node, _)| *node)
            .collect()
    }
}

/// The items here that must stay, detached (§10), and the masked (`masked`) folders they need:
/// a masked item changed here (or never synced), and the masked folders above it; the masked
/// folders above an item this device syncs, but for one in `away`, which takes what is in it
/// along.
fn to_detach(
    input: &Input,
    masked: &HashSet<NodeId>,
    away: &HashMap<NodeId, NodeId>,
    read_only: bool,
) -> BTreeSet<NodeId> {
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
                        // Read-only (§13), a bit changed here is no change to keep.
                        base_bytes != l.hash || (!read_only && b.executable != l.executable)
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
            masked.contains(&local.parent) && !away.contains_key(&node)
        };
        if needs_place {
            // Up to the first masked folder already in: what is above it is in too. One that
            // moves away takes what is in it along: the folders above it aren't needed.
            for a in input.local.ancestors_iter(node) {
                if away.contains_key(&a)
                    || (masked.contains(&a) && input.local.contains(a) && !detach.insert(a))
                {
                    break;
                }
            }
        }
    }
    detach
}

/// The items whose bytes `plan` forks into a conflict copy, which stays where the item is.
fn forked(plan: &Plan) -> HashSet<NodeId> {
    (plan.ops.iter())
        .filter(|op| matches!(op, Op::LocalFork { .. }))
        .map(Op::node)
        .collect()
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
    use hbsync_types::NodeState;

    /// `proj` (`d`) holding the folder `f`, synced; the drive moved `f` to the top and `d` into
    /// `build/`, which this device ignores.
    fn moved_out() -> (Input, NodeId, NodeId) {
        let (b, d, f) = (NodeId::new(), NodeId::new(), NodeId::new());
        let mut input = Input::default();
        let item = |parent| NodeState::folder(parent, "f");
        for tree in [&mut input.base, &mut input.local] {
            tree.insert(d, NodeState::folder(NodeId::ROOT, "proj"));
            tree.insert(f, item(d));
        }
        input
            .remote
            .insert(b, NodeState::folder(NodeId::ROOT, "build"));
        input.remote.insert(d, NodeState::folder(b, "proj"));
        input.remote.insert(f, item(NodeId::ROOT));
        input.dirty.extend([b, d, f]);
        (input, d, f)
    }

    /// The masks of `input` ignoring `build/`, in `scope`.
    fn masks_in(scope: &Scope, input: &Input) -> Masks {
        let filter = Filter::from_rules("build/\n", false).expect("rules");
        let masking = Masking {
            filter: &filter,
            scope,
            read_only: false,
        };
        Masks::of(input, masking, "c", &HashSet::new())
    }

    /// [`masks_in`] the main folder, planned again with `away` where that spares a folder.
    fn sparing(input: &Input, away: &HashMap<NodeId, NodeId>) -> Option<Masks> {
        let masks = masks_in(&Scope::main(), input);
        let detached = masks.detached.len();
        masks.sparing(input, "c", &HashSet::new(), away, detached)
    }

    fn moving(node: NodeId, parent: NodeId) -> Op {
        Op::LocalMove {
            node,
            parent,
            name: "f".into(),
            drive: None,
        }
    }

    fn plan(ops: Vec<Op>) -> Plan {
        Plan {
            ops,
            ..Plan::default()
        }
    }

    /// What moves away from a detached folder: to a place the drive has, not into another
    /// detached folder, nor with a conflict copy left behind.
    #[test]
    fn what_moves_away_from_a_detached_folder() {
        let (input, d, f) = moved_out();
        let masks = masks_in(&Scope::main(), &input);
        let new_d = masks.detached[&d];
        let other = NodeId::new();
        let away = |ops| masks.moved_away(&plan(ops));
        assert_eq!(
            away(vec![moving(f, NodeId::ROOT)]),
            HashMap::from([(f, NodeId::ROOT)])
        );
        assert!(away(vec![moving(f, new_d)]).is_empty());
        assert!(away(vec![]).is_empty());
        let fork = Op::LocalFork {
            node: f,
            new_node: other,
            name: "f (conflict).txt".into(),
        };
        assert!(away(vec![fork, moving(f, NodeId::ROOT)]).is_empty());
        // Planned with it away, the folder isn't detached; with nothing away, none is spared
        // and no view is built.
        let away = HashMap::from([(f, NodeId::ROOT)]);
        let spared = sparing(&input, &away).expect("spared");
        assert!(spared.detached.is_empty());
        assert!(sparing(&input, &HashMap::new()).is_none());
        // Nor is a round that detaches what the last one did planned again.
        assert!(
            spared
                .sparing(&input, "c", &HashSet::new(), &away, 1)
                .is_none()
        );
    }

    /// A folder the filter masks inside this computer's own Desktop folder, held in the main
    /// folder (outside its scope, §12): what moves away from it spares neither, which stay
    /// detached as before.
    #[test]
    fn what_moves_away_from_a_folder_in_one_outside_the_scope_spares_nothing() {
        let (mut input, d, f) = moved_out();
        let desk = NodeId::new();
        let folder = NodeState::folder(NodeId::ROOT, "Desktop");
        input.base.insert(desk, folder.clone());
        input.local.insert(desk, folder.clone());
        input.remote.insert(desk, folder);
        let inside = NodeState::folder(desk, "proj");
        input.base.insert(d, inside.clone());
        input.local.insert(d, inside);
        let scope = Scope::main().with_tops([desk]);
        let masks = masks_in(&scope, &input);
        assert!(masks.detached.contains_key(&d) && masks.detached.contains_key(&desk));
        let moved = plan(vec![moving(f, NodeId::ROOT)]);
        assert!(masks.moved_away(&moved).is_empty());
    }

    /// Planned again with an item away, the plan must move it where the first one did; else it
    /// keeps its folder.
    #[test]
    fn an_item_away_that_the_plan_does_not_move_there_stayed() {
        let (input, _, f) = moved_out();
        let away = HashMap::from([(f, NodeId::ROOT)]);
        let masks = sparing(&input, &away).expect("spared");
        let stayed = |ops| masks.not_moved_away(&input, &plan(ops), &away);
        assert!(stayed(vec![moving(f, NodeId::ROOT)]).is_empty());
        assert_eq!(stayed(vec![]), [f]);
        assert_eq!(stayed(vec![moving(f, NodeId::new())]), [f]);
        // Moved there, but its bytes forked into a copy that stays where it is.
        let fork = Op::LocalFork {
            node: f,
            new_node: NodeId::new(),
            name: "f (conflict)".into(),
        };
        assert_eq!(stayed(vec![fork, moving(f, NodeId::ROOT)]), [f]);
    }

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
