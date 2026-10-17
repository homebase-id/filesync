//! What a folder's delete took along (node schema, Recently deleted): one rule, for the
//! reconciler and the simulator. Deleting for good doesn't go by it: it takes removed nodes
//! only, and a removed folder holding one of these stays (node schema, The purge).

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
    let parent: HashMap<NodeId, NodeId> = live.into_iter().collect();
    let written_by = |node: NodeId, removed: i64| changed_at(node).is_none_or(|at| at <= removed);
    // Each node's verdict: taken by the removal at that time, or not (`None`).
    let mut verdict: HashMap<NodeId, Option<i64>> = HashMap::new();
    for &start in parent.keys() {
        // Up to a node decided, a removed folder, or the top of the live nodes (or a cycle).
        let mut chain = Vec::new();
        let mut at = start;
        let mut above = loop {
            if let Some(&known) = verdict.get(&at) {
                break known;
            }
            chain.push(at);
            let up = parent[&at];
            if let Some(removed) = removed_at(up) {
                break Some(removed);
            }
            if !parent.contains_key(&up) || chain.contains(&up) {
                break None;
            }
            if let Some(&known) = verdict.get(&up) {
                break known;
            }
            at = up;
        };
        // `above` is what the last node of the chain is in: down from there.
        for node in chain.into_iter().rev() {
            let taken = above.filter(|removed| written_by(node, *removed));
            verdict.insert(node, taken);
            above = taken;
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
    verdict
        .into_iter()
        .filter(|(_, v)| v.is_some())
        .map(|(n, _)| n)
        .collect()
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
}
