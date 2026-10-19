//! What a folder's delete took along (node schema, Recently deleted): one rule, for the
//! reconciler, the purge and the simulator. Deleting for good takes removed nodes only: the
//! purge first removes what a delete took along and left live ([`taken_levels`], node schema,
//! The purge, 2.), and a removed folder holding a live node stays.

use std::collections::{HashMap, HashSet};

use hbsync_types::NodeId;

/// Of the live nodes `live` (each with its folder), those the removal of a folder took along:
/// below a removed folder (`removed_at` gives its removal's time, the server's), directly or
/// through live folders it took along too, and written no later than the removal of the
/// nearest removed folder above them (`changed_at`, the server's time of the node's version).
/// A node whose time isn't known (this device's own write, not back from the feed yet) counts
/// as written before.
///
/// A live node written after the removal (moved in, edited, new) wasn't taken along: neither is
/// anything in it, and it keeps the live folders above it, up to the removed one.
pub fn taken_along(
    live: impl IntoIterator<Item = (NodeId, NodeId)>,
    removed_at: impl Fn(NodeId) -> Option<i64>,
    changed_at: impl Fn(NodeId) -> Option<i64>,
) -> HashSet<NodeId> {
    let written_by = |node: NodeId, removed: i64| changed_at(node).is_none_or(|at| at <= removed);
    verdicts(live, removed_at, written_by)
        .1
        .into_keys()
        .collect()
}

/// What the purge removes of what deletes left live (node schema, The purge, 2.), in levels
/// top-down, each node after the folder it is in: of `live` (each with its folder), those
/// [`taken_along`] takes, but that a node whose time isn't known (`changed_at` `None`) or that
/// `keep` names (a role folder) isn't, and neither are the folders above it: what is in them
/// would be left without a folder.
pub fn taken_levels(
    live: impl IntoIterator<Item = (NodeId, NodeId)>,
    removed_at: impl Fn(NodeId) -> Option<i64>,
    changed_at: impl Fn(NodeId) -> Option<i64>,
    keep: impl Fn(NodeId) -> bool,
) -> Vec<Vec<NodeId>> {
    let written_by = |node: NodeId, removed: i64| {
        !keep(node) && changed_at(node).is_some_and(|at| at <= removed)
    };
    let (parent, depths) = verdicts(live, removed_at, written_by);
    let mut levels: Vec<Vec<NodeId>> = Vec::new();
    for (node, depth) in depths.iter().map(|(n, d)| (*n, *d)) {
        if levels.len() <= depth {
            levels.resize_with(depth + 1, Vec::new);
        }
        levels[depth].push(node);
    }
    // Only below what goes: one in a folder another node keeps (taken along, for the
    // reconciler, but its folder stays) would go on its own.
    let mut going: HashSet<NodeId> = HashSet::new();
    for (depth, level) in levels.iter_mut().enumerate() {
        level.retain(|n| depth == 0 || going.contains(&parent[n]));
        level.sort();
        going.extend(level.iter().copied());
    }
    while levels.last().is_some_and(Vec::is_empty) {
        levels.pop();
    }
    levels
}

/// The live nodes `live` (each with its folder) taken along by the removal of a folder, each
/// with its depth below it (0: right in it): those `written_by(node, removal)` says were
/// written by that removal's time, through live folders taken along too, but the folders above
/// one not taken. And `live` by node.
#[allow(clippy::type_complexity)]
fn verdicts(
    live: impl IntoIterator<Item = (NodeId, NodeId)>,
    removed_at: impl Fn(NodeId) -> Option<i64>,
    written_by: impl Fn(NodeId, i64) -> bool,
) -> (HashMap<NodeId, NodeId>, HashMap<NodeId, usize>) {
    let parent: HashMap<NodeId, NodeId> = live.into_iter().collect();
    // Each node's verdict: taken by the removal at that time, at that depth below it, or not
    // (`None`).
    let mut verdict: HashMap<NodeId, Option<(i64, usize)>> = HashMap::new();
    let mut chain = Vec::new();
    for &start in parent.keys() {
        // Up to a node decided, a removed folder, or the top of the live nodes (or a cycle):
        // the removal the last node of the chain is under, and its depth below it.
        chain.clear();
        let mut at = start;
        let mut above = loop {
            if let Some(&known) = verdict.get(&at) {
                break known.map(|(removed, depth)| (removed, depth + 1));
            }
            chain.push(at);
            let up = parent[&at];
            if let Some(removed) = removed_at(up) {
                break Some((removed, 0));
            }
            if !parent.contains_key(&up) || chain.contains(&up) {
                break None;
            }
            if let Some(&known) = verdict.get(&up) {
                break known.map(|(removed, depth)| (removed, depth + 1));
            }
            at = up;
        };
        // Down from there.
        for &node in chain.iter().rev() {
            let taken = above.filter(|(removed, _)| written_by(node, *removed));
            verdict.insert(node, taken);
            above = taken.map(|(removed, depth)| (removed, depth + 1));
        }
    }
    // A node not taken keeps the folders above it.
    let alive: Vec<NodeId> = verdict
        .iter()
        .filter(|(_, v)| v.is_none())
        .map(|(n, _)| *n)
        .collect();
    for node in alive {
        let mut at = node;
        while let Some(&up) = parent.get(&at)
            && verdict.get(&up).copied().flatten().is_some()
        {
            verdict.insert(up, None);
            at = up;
        }
    }
    let taken = verdict
        .into_iter()
        .filter_map(|(n, v)| Some((n, v?.1)))
        .collect();
    (parent, taken)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids<const N: usize>() -> [NodeId; N] {
        std::array::from_fn(|_| NodeId::new())
    }

    /// d removed at 10: what was written before goes with it, through live folders; a folder
    /// moved in after (and all in it) doesn't, nor do the folders above a file written after.
    #[test]
    fn one_rule_for_what_a_delete_took_along() {
        let [d, e, x, moved, inside, f, late, y, g, z] = ids();
        let live = [
            (e, d),
            (x, e),
            (moved, d),
            (inside, moved),
            (f, d),
            (late, f),
            (y, f),
            (g, NodeId::ROOT),
            (z, g),
        ];
        let times: HashMap<NodeId, i64> = [
            (e, 5),
            (x, 6),
            (moved, 20),
            (inside, 1),
            (f, 5),
            (late, 30),
            (y, 2),
            (g, 1),
            (z, 1),
        ]
        .into();
        let removed = |n: NodeId| (n == d).then_some(10);
        let taken = taken_along(live, removed, |n| times.get(&n).copied());
        let mut taken: Vec<NodeId> = taken.into_iter().collect();
        taken.sort();
        let mut expected = vec![e, x, y];
        expected.sort();
        assert_eq!(taken, expected);
    }

    /// A time not known (this device's own write, not back yet) counts as written before.
    #[test]
    fn a_time_not_known_is_taken() {
        let [d, x] = ids();
        let removed = |n: NodeId| (n == d).then_some(10);
        assert!(taken_along([(x, d)], removed, |_| None).contains(&x));
    }

    /// Live folders in a cycle (left by remote moves) are in no removed folder: not taken.
    #[test]
    fn a_cycle_takes_nothing() {
        let [a, b] = ids();
        let taken = taken_along([(a, b), (b, a)], |_| None, |_| Some(1));
        assert!(taken.is_empty());
    }

    /// What the purge removes: by the same rule, top-down; a node whose time isn't known, a
    /// role folder, and the folders above them, stay.
    #[test]
    fn the_purge_takes_what_was_taken_along_top_down() {
        let [f, a, b, c, late, in_late, unknown_dir, unknown] = ids();
        let [role_parent, role, beside, late_dir, late_file] = ids();
        let live = [
            (a, f, Some(5)),
            (b, a, Some(6)),
            (c, b, Some(1)),
            // Written after the removal: it stays, with what is in it.
            (late, f, Some(20)),
            (in_late, late, Some(1)),
            // Its time isn't known here: it stays, and so does its folder.
            (unknown_dir, f, Some(1)),
            (unknown, unknown_dir, None),
            // A role folder in it (never so, but for a broken client): it stays, and so does
            // the folder above it.
            (role_parent, f, Some(1)),
            (role, role_parent, Some(1)),
            // Taken along, but its folder stays (late keeps it): not on its own.
            (beside, late_dir, Some(1)),
            (late_dir, f, Some(1)),
            (late_file, late_dir, Some(30)),
        ];
        let times: HashMap<NodeId, Option<i64>> = live.iter().map(|(n, _, t)| (*n, *t)).collect();
        let levels = |removed: Option<i64>| {
            taken_levels(
                live.iter().map(|(n, p, _)| (*n, *p)),
                |n| (n == f).then_some(removed).flatten(),
                |n| times[&n],
                |n| n == role,
            )
        };
        assert_eq!(levels(Some(10)), vec![vec![a], vec![b], vec![c]]);
        assert!(levels(None).is_empty());
    }
}
