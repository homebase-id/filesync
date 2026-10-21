//! The guard's held deletes out of the dirty set (sync semantics §7). A hold lasts until the
//! user answers, so what it holds isn't planned again every sync: those nodes are kept in the
//! store's held deletes ([`hbsync_state::FolderTx::hold_delete`]), the deletes in memory, shown
//! with every sync's report, and their files counted by the guard as if planned with each plan
//! ([`crate::Options::held_files`]). They are planned again, all of them, when a plan's view
//! reaches one of them (something changed in, above or beside them: planned together, as
//! before), on an answer, at the engine's first sync (a restart, a new setting), and once the
//! guard's totals would let this device's own removals through.
//!
//! The rules here are pure; the simulator checks that a plan without what they take out plans
//! as one with it ([`touches`]).

use std::collections::HashSet;

use hbsync_types::NodeId;

use crate::plan::{Input, Op, Plan};

/// What of `plan`, made for the dirty nodes `page`, is held and leaves the dirty set: the held
/// deletes whose node is in `page`, and the nodes of `page` each of them covers (its node, and
/// what a held trash takes along), where the plan has nothing else for any of them. What else
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
            Op::LocalTrash { subtree, .. } => &subtree[..],
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
/// ([`Plan::held_takes`]): (removed from the drive, trashed here).
pub fn files<'p>(plan: &Plan, ops: impl IntoIterator<Item = &'p Op>) -> (usize, usize) {
    ops.into_iter()
        .filter_map(|op| plan.held_takes.get(&op.node()))
        .fold((0, 0), |(here, there), (h, t)| (here + h, there + t))
}

/// Whether `input` (a plan's view) holds any of `out`, the nodes out of the dirty set: what
/// is planned from it may depend on them, so they are planned with it. A view that holds none
/// of them plans as one with them dirty would (their deletes held, counted through
/// [`crate::Options::held_files`]).
pub fn touches(input: &Input, out: &HashSet<NodeId>) -> bool {
    let has = |n: &NodeId| {
        input.base.contains(*n)
            || input.local.contains(*n)
            || input.remote.contains(*n)
            || input.removed.contains_key(n)
    };
    // Whichever is fewer: the nodes out, or the view's (a page's is up to thousands).
    match out.len() < input.dirty.len() {
        true => out.iter().any(has),
        false => input
            .base
            .ids()
            .chain(input.local.ids())
            .chain(input.remote.ids())
            .chain(input.removed.keys().copied())
            .any(|n| out.contains(&n)),
    }
}
