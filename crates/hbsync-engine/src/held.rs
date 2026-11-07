//! The guard's held deletes out of the dirty set (sync semantics §7). A hold lasts until the
//! user answers, so what it holds isn't planned again every sync: a sync that ends with nothing
//! dirty but held deletes keeps those nodes in the store's held deletes
//! ([`hbsync_state::FolderTx::hold_delete`]), the deletes in memory, shown with every sync's
//! report, and their files counted by the guard as if planned with each plan
//! ([`crate::Guarding::held`]). They are planned again, all of them, when a plan's view
//! reaches one of them (something changed in, above or beside them: planned together, as
//! before), on an answer, at the engine's first sync (a restart, a new setting), and once the
//! guard's totals would let this device's own removals through.
//!
//! The rules here are pure, shared with the simulator: what leaves the dirty set
//! ([`out_of_dirty`], [`touches`]: the simulator checks that a plan without it plans as one
//! with it), whether a hold of removals elsewhere stays ([`keep_held`]), and what "Put them
//! back" does ([`put_back_plan`]), each over what a [`Sides`] reads.

use std::collections::{HashMap, HashSet};

use hbsync_types::{NodeId, NodeKind};

use crate::plan::{Input, Op, Plan};

/// What of `plan`, made for the dirty nodes `page`, is held and leaves the dirty set: the held
/// deletes whose node is in `page`, and the nodes of `page` each of them covers (its node, and
/// what is below a held delete's node), where the plan has nothing else for any of them. What else
/// the plan does, defers or waits for stays dirty, and so does the delete it is part of.
pub fn out_of_dirty<'p>(plan: &'p Plan, page: &[NodeId]) -> (Vec<&'p Op>, HashSet<NodeId>) {
    let mut nodes = HashSet::new();
    if plan.held.is_empty() {
        return (Vec::new(), nodes);
    }
    let page: HashSet<NodeId> = page.iter().copied().collect();
    let busy: HashSet<NodeId> = plan
        .ops
        .iter()
        .map(Op::node)
        .chain(plan.conflicts.iter().map(|c| c.node()))
        .chain(plan.deferred.iter().copied())
        .chain(plan.waiting.iter().copied())
        .chain(plan.delete_waits.iter().copied())
        .collect();
    let covers = |op: &'p Op| {
        let subtree = match op {
            Op::LocalDelete { subtree, .. } => &subtree[..],
            _ => &[],
        };
        std::iter::once(op.node()).chain(subtree.iter().copied())
    };
    let mut ops = Vec::new();
    for op in &plan.held {
        if page.contains(&op.node()) && !covers(op).any(|n| busy.contains(&n)) {
            ops.push(op);
            nodes.extend(covers(op).filter(|n| page.contains(n)));
        }
    }
    (ops, nodes)
}

/// The files `ops` (held deletes of `plan`) take, as the guard counted them
/// ([`Plan::held_takes`]): (removed from the drive, deleted here).
pub fn files<'p>(plan: &Plan, ops: impl IntoIterator<Item = &'p Op>) -> (usize, usize) {
    ops.into_iter()
        .filter_map(|op| plan.held_takes.get(&op.node()))
        .fold((0, 0), |(here, there), (h, t)| (here + h, there + t))
}

/// Whether `input` (a plan's view) holds any of `out`, the nodes out of the dirty set, or plans
/// one dirty again (whatever is left of it): what is planned from it may depend on them, so
/// they are planned with it. A view that holds none of them plans as one with them dirty would
/// (their deletes held, counted through [`crate::Guarding::held`]).
pub fn touches(input: &Input, out: &HashSet<NodeId>) -> bool {
    let has = |n: &NodeId| {
        input.dirty.contains(n)
            || input.base.contains(*n)
            || input.local.contains(*n)
            || input.remote.contains(*n)
            || input.removed.contains_key(n)
    };
    // Whichever is fewer: the nodes out, or the view's (a page's is up to thousands).
    let view = input.dirty.len() + input.base.len() + input.local.len() + input.remote.len();
    match out.len() < view {
        true => out.iter().any(has),
        false => input
            .dirty
            .iter()
            .copied()
            .chain(input.base.ids())
            .chain(input.local.ids())
            .chain(input.remote.ids())
            .chain(input.removed.keys().copied())
            .any(|n| out.contains(&n)),
    }
}

/// What the rules of held deletes read of a node: the engine from its store, the simulator
/// from its model.
pub trait Sides {
    type Error;
    /// The folder `node` was in when last synced (its base); `None` when it isn't synced.
    fn synced(&self, node: NodeId) -> Result<Option<NodeId>, Self::Error>;
    /// The synced nodes in `folder` (in the base).
    fn synced_children(&self, folder: NodeId) -> Result<Vec<NodeId>, Self::Error>;
    /// What `node` is here, if it is here.
    fn here(&self, node: NodeId) -> Result<Option<NodeKind>, Self::Error>;
    /// What is in folder `node` here, each with what it is.
    fn here_children(&self, node: NodeId) -> Result<Vec<(NodeId, NodeKind)>, Self::Error>;
    /// Whether the drive has `node`, live (in Recently deleted or not there: not live; a node
    /// taken along by a folder in Recently deleted is live, to come back with that folder).
    fn live(&self, node: NodeId) -> Result<bool, Self::Error>;
    /// Whether the drive no longer has `node` as the plan reads it: not there, or taken along
    /// by a folder in Recently deleted above it (the one rule, `along::taken_along`).
    fn gone(&self, node: NodeId) -> Result<bool, Self::Error>;
    /// `node`'s row of Recently deleted: the folder it was in, and the removal's time
    /// ([`hbsync_types::removal_time`]).
    fn removed(&self, node: NodeId) -> Result<Option<(NodeId, i64)>, Self::Error>;
}

/// Whether removals elsewhere held as every synced file at `node` stay held (sync semantics
/// §7): until the user answers (`confirmed`: Delete), each while a removal elsewhere is still
/// to be followed in it: it (gone on the drive as the plan reads it, taken along too), or (back
/// on the drive) what is below it.
pub fn keep_held<S: Sides>(node: NodeId, confirmed: bool, sides: &S) -> Result<bool, S::Error> {
    Ok(!confirmed
        && sides.synced(node)?.is_some()
        && match sides.gone(node)? {
            true => sides.here(node)?.is_some(),
            false => removal_pending_under(node, sides)?,
        })
}

/// Whether a removal elsewhere is still to be followed below `node` in the base: a synced item
/// still here that the drive no longer has ([`Sides::gone`]). Costs, per node walked till the
/// first, a here, a gone and a base read: only for a held wipe back on the drive.
fn removal_pending_under<S: Sides>(node: NodeId, sides: &S) -> Result<bool, S::Error> {
    let mut below = sides.synced_children(node)?;
    while let Some(n) = below.pop() {
        if sides.here(n)?.is_some() && sides.gone(n)? {
            return Ok(true);
        }
        below.extend(sides.synced_children(n)?);
    }
    Ok(false)
}

/// What "Put them back" does (sync semantics §7), as the drive has things now.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PutBack {
    /// Nodes whose base goes, so they come down again (deleted here, live there) or go up again
    /// (gone for good there).
    pub forget: Vec<NodeId>,
    /// The items of Recently deleted to restore, the outermost (by the base) first: what was
    /// removed with its folder comes back with it.
    pub items: Vec<NodeId>,
    /// The synced nodes it brings back from Recently deleted ([`after_restore`] looks at them
    /// again).
    pub restored: Vec<NodeId>,
    /// Nodes gone for good kept with their base for now: folders below which something synced
    /// waits in Recently deleted ([`after_restore`] looks at them again).
    pub kept: Vec<NodeId>,
}

/// The folders, synced, from each of `folders` up, when `gone` has any (else none: nothing to
/// keep). A base read a folder, each once.
fn waits_below<S: Sides>(
    sides: &S,
    folders: impl IntoIterator<Item = NodeId>,
    gone: &[NodeId],
) -> Result<HashSet<NodeId>, S::Error> {
    let mut up = HashSet::new();
    if gone.is_empty() {
        return Ok(up);
    }
    for folder in folders {
        let mut at = folder;
        while up.insert(at) {
            match sides.synced(at)? {
                Some(parent) => at = parent,
                None => break,
            }
        }
    }
    Ok(up)
}

/// What [`put_back_plan`]'s `plan` forgets more once its restore is done (or failed: what it
/// did stands), as the drive has things then: each node it was to restore that the restore
/// found deleted for good meanwhile (synced, neither live nor in Recently deleted), and each
/// folder it kept, gone for good, so they go up again from here; a folder of them only once
/// nothing synced below it waits in Recently deleted any more. Costs, per node restored or
/// kept, a base, a live and a Recently deleted read (and base reads up from what still waits,
/// when something is gone).
pub fn after_restore<S: Sides>(plan: &PutBack, sides: &S) -> Result<Vec<NodeId>, S::Error> {
    // Gone for good now: found so by the restore, or kept for what waited below.
    let mut gone = Vec::new();
    let mut waiting = Vec::new();
    for &n in plan.restored.iter().chain(&plan.kept) {
        let Some(folder) = sides.synced(n)? else {
            continue;
        };
        if sides.live(n)? {
            continue;
        }
        match sides.removed(n)? {
            Some(_) => waiting.push(folder),
            None => gone.push(n),
        }
    }
    let above_waiting = waits_below(sides, waiting, &gone)?;
    Ok(gone
        .into_iter()
        .filter(|n| !above_waiting.contains(n))
        .collect())
}

/// Puts back the held removals of `nodes`, as the drive has them now (what it removed or
/// changed since wins):
/// - deleted here, live on the drive: its base goes, with what below it is gone here too, so
///   it comes down again. What below it was moved elsewhere here keeps its base: the move goes
///   on;
/// - removed elsewhere, still here: restored on the drive from Recently deleted, and so is what
///   in it was removed apart before (each by the item whose delete took it along, up from the
///   folder it was synced in); what is gone for good there loses its base, so it goes up again
///   from here (a folder only once nothing synced below it waits in Recently deleted: else it
///   is [`PutBack::kept`], for [`after_restore`], which also takes what the restore itself
///   finds gone); a folder already back has what is in it looked at.
///
/// Costs, per node walked, a base, a live and (here) a row lookup, a row lookup and a listing
/// per folder here, and a row of Recently deleted per node restored and per folder above it
/// (each read once).
pub fn put_back_plan<S: Sides>(nodes: &[NodeId], sides: &S) -> Result<PutBack, S::Error> {
    let mut plan = PutBack::default();
    // Synced and here, gone for good on the drive.
    let mut gone = Vec::new();
    // Each once: a folder's held with what is in it.
    let mut seen = HashSet::new();
    // What to restore: each node, with the folder it was synced in and its removal's time.
    let mut restore: Vec<(NodeId, NodeId, i64)> = Vec::new();
    // Rows of Recently deleted, each read once.
    let removed = std::cell::RefCell::new(HashMap::new());
    let removed = |n: NodeId| -> Result<Option<(NodeId, i64)>, S::Error> {
        if let Some(r) = removed.borrow().get(&n) {
            return Ok(*r);
        }
        let r = sides.removed(n)?;
        removed.borrow_mut().insert(n, r);
        Ok(r)
    };
    for &node in nodes {
        if sides.synced(node)?.is_none() {
            continue;
        }
        match (sides.here(node)?, sides.live(node)?) {
            (None, true) => {
                // Below it in the base, gone here and live there.
                let mut below = vec![node];
                while let Some(n) = below.pop() {
                    if !seen.insert(n) {
                        continue;
                    }
                    plan.forget.push(n);
                    for b in sides.synced_children(n)? {
                        if sides.here(b)?.is_none() && sides.live(b)? {
                            below.push(b);
                        }
                    }
                }
            }
            (Some(kind), _) => {
                // Here, parents first: each synced and gone there restored, or gone for good
                // (never one made here since).
                let mut below = vec![(node, kind)];
                while let Some((n, kind)) = below.pop() {
                    if !seen.insert(n) {
                        continue;
                    }
                    if let Some(folder) = sides.synced(n)?
                        && !sides.live(n)?
                    {
                        match removed(n)? {
                            Some((_, at)) => restore.push((n, folder, at)),
                            None => gone.push(n),
                        }
                    }
                    if kind == NodeKind::Folder {
                        below.extend(sides.here_children(n)?);
                    }
                }
            }
            // The drive removed it too meanwhile: its state wins.
            (None, false) => {}
        }
    }
    // Gone for good: its base goes, but a folder's only once nothing synced below it waits in
    // Recently deleted (its restore not done yet, or failed: the guard's hold on that walks up
    // the base). Looked at again after the restore ([`after_restore`]), it goes then.
    let above_waiting = waits_below(sides, restore.iter().map(|&(_, folder, _)| folder), &gone)?;
    for n in gone {
        match above_waiting.contains(&n) {
            true => plan.kept.push(n),
            false => plan.forget.push(n),
        }
    }
    plan.restored = restore.iter().map(|&(n, ..)| n).collect();
    // Up through the folders each was deleted with, from the folder it was synced in, as
    // Recently deleted lists it.
    let with = |at: NodeId| -> Result<Option<NodeId>, S::Error> {
        let Some((folder, time)) = removed(at)? else {
            return Ok(None);
        };
        Ok(removed(folder)?
            .filter(|(_, folder_at)| crate::deleted::deleted_with_at(time, *folder_at))
            .map(|_| folder))
    };
    let mut known = HashMap::new();
    let mut items = HashSet::new();
    for (node, folder, at) in restore {
        let in_folder = removed(folder)?
            .is_some_and(|(_, folder_at)| crate::deleted::deleted_with_at(at, folder_at));
        items.insert(match in_folder {
            true => crate::deleted::item_by(folder, &mut known, with)?,
            false => node,
        });
    }
    let mut items: Vec<(usize, NodeId)> = items
        .into_iter()
        .map(|item| {
            let up = crate::walk::up(item, |at| Ok(sides.synced(at)?.map(|p| ((), p))))?;
            Ok((up.items.len(), item))
        })
        .collect::<Result<_, S::Error>>()?;
    items.sort();
    plan.items = items.into_iter().map(|(_, n)| n).collect();
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// The sides as plain maps.
    #[derive(Default)]
    struct Model {
        /// Synced: the folder each is in.
        base: HashMap<NodeId, NodeId>,
        /// Here: the folder each is in, and what it is.
        here: HashMap<NodeId, (NodeId, NodeKind)>,
        live: HashSet<NodeId>,
        /// Live, but taken along by a folder in Recently deleted.
        taken: HashSet<NodeId>,
        /// In Recently deleted: its folder there and the removal's time.
        removed: HashMap<NodeId, (NodeId, i64)>,
    }

    impl Sides for Model {
        type Error = Infallible;
        fn synced(&self, node: NodeId) -> Result<Option<NodeId>, Infallible> {
            Ok(self.base.get(&node).copied())
        }
        fn synced_children(&self, folder: NodeId) -> Result<Vec<NodeId>, Infallible> {
            Ok(self
                .base
                .iter()
                .filter(|(_, p)| **p == folder)
                .map(|(n, _)| *n)
                .collect())
        }
        fn here(&self, node: NodeId) -> Result<Option<NodeKind>, Infallible> {
            Ok(self.here.get(&node).map(|(_, kind)| *kind))
        }
        fn here_children(&self, node: NodeId) -> Result<Vec<(NodeId, NodeKind)>, Infallible> {
            Ok(self
                .here
                .iter()
                .filter(|(_, (p, _))| *p == node)
                .map(|(n, (_, kind))| (*n, *kind))
                .collect())
        }
        fn live(&self, node: NodeId) -> Result<bool, Infallible> {
            Ok(self.live.contains(&node))
        }
        fn gone(&self, node: NodeId) -> Result<bool, Infallible> {
            Ok(!self.live.contains(&node) || self.taken.contains(&node))
        }
        fn removed(&self, node: NodeId) -> Result<Option<(NodeId, i64)>, Infallible> {
            Ok(self.removed.get(&node).copied())
        }
    }

    /// A wipe held on folder `g`, which a folder removed on the drive took along (live there,
    /// in a folder in Recently deleted), stays held: `g` is gone as the plan reads it, and it is
    /// still here. Read as live, nothing below it would be pending and the hold would go
    /// without an answer (scan A1: not reached by the simulator, where a removed folder above
    /// holds it too, but one rule for "live" either way).
    #[test]
    fn a_wipe_held_on_a_node_taken_along_stays_held() {
        let (r, g, f) = (NodeId::new(), NodeId::new(), NodeId::new());
        let model = Model {
            base: HashMap::from([(r, NodeId::ROOT), (g, r), (f, g)]),
            here: HashMap::from([
                (r, (NodeId::ROOT, NodeKind::Folder)),
                (g, (r, NodeKind::Folder)),
                (f, (g, NodeKind::File)),
            ]),
            live: HashSet::from([g, f]),
            taken: HashSet::from([g, f]),
            removed: HashMap::from([(r, (NodeId::ROOT, 5))]),
        };
        assert_eq!(keep_held(g, false, &model), Ok(true));
        assert_eq!(keep_held(g, true, &model), Ok(false), "answered");
        let back = Model {
            taken: HashSet::new(),
            ..model
        };
        assert_eq!(
            keep_held(g, false, &back),
            Ok(false),
            "live again, nothing pending"
        );
    }

    /// Put back restores a removed item from the folder it is in here, and up from there only
    /// if it was deleted with that folder: a file moved elsewhere into a folder then deleted
    /// there is put back on its own, not with that folder and what else was in it.
    #[test]
    fn an_item_is_found_up_from_the_folder_here() {
        let (here, there, x) = (NodeId::new(), NodeId::new(), NodeId::new());
        let model = |folder: NodeId| Model {
            base: HashMap::from([(x, folder)]),
            here: HashMap::from([(x, (folder, NodeKind::File))]),
            // `here` removed after it: not with it.
            removed: HashMap::from([
                (there, (NodeId::ROOT, 5)),
                (x, (there, 5)),
                (here, (NodeId::ROOT, 9)),
            ]),
            ..Model::default()
        };
        let items = |folder| {
            let Ok(plan) = put_back_plan(&[x], &model(folder));
            (plan.items, plan.restored.len())
        };
        assert_eq!(items(here), (vec![x], 1), "on its own");
        assert_eq!(
            items(there),
            (vec![there], 1),
            "with the folder deleted with it"
        );
    }

    /// Removed folders in a cycle, each deleted with the other: put back ends at one of them.
    #[test]
    fn put_back_ends_at_a_cycle_in_recently_deleted() {
        let (p, q, x) = (NodeId::new(), NodeId::new(), NodeId::new());
        let model = Model {
            base: HashMap::from([(x, p)]),
            here: HashMap::from([(x, (p, NodeKind::File))]),
            removed: HashMap::from([(p, (q, i64::MAX)), (q, (p, i64::MAX)), (x, (p, i64::MAX))]),
            ..Model::default()
        };
        let Ok(plan) = put_back_plan(&[x], &model);
        assert_eq!(plan.items, vec![p.min(q)]);
    }

    /// A folder gone for good keeps its base while something synced below it waits in
    /// Recently deleted (the guard's hold on that walks up the base); once that is back
    /// ([`after_restore`]), it goes, to go up again from here.
    #[test]
    fn a_folder_gone_for_good_goes_once_nothing_below_it_waits() {
        let (d, e, x) = (NodeId::new(), NodeId::new(), NodeId::new());
        let mut model = Model {
            base: HashMap::from([(d, NodeId::ROOT), (e, d), (x, e)]),
            here: HashMap::from([
                (d, (NodeId::ROOT, NodeKind::Folder)),
                (e, (d, NodeKind::Folder)),
                (x, (e, NodeKind::File)),
            ]),
            // `d` gone for good; `e` back on the drive; `x` in Recently deleted.
            live: HashSet::from([e]),
            removed: HashMap::from([(x, (e, 5))]),
            ..Model::default()
        };
        let Ok(plan) = put_back_plan(&[d], &model);
        assert_eq!(
            (&plan.forget, &plan.items, &plan.kept),
            (&vec![], &vec![x], &vec![d])
        );
        // Not back yet: still kept.
        assert_eq!(after_restore(&plan, &model), Ok(vec![]));
        // Back.
        model.removed.clear();
        model.live.insert(x);
        assert_eq!(after_restore(&plan, &model), Ok(vec![d]));
        // Found deleted for good by the restore: it goes too.
        model.live.remove(&x);
        assert_eq!(after_restore(&plan, &model), Ok(vec![x, d]));
    }
}
