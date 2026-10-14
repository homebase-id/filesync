//! Walking up from a node to the root through parents kept in the state DB (base, mirror or
//! Recently deleted) or a tree loaded from it: one walk, so every caller stops the same way at
//! a missing parent, at a cycle (a damaged DB, or bases left by a remote cycle) and at the
//! deepest a path may be. The in-memory trees of planning (`Tree::ancestors`, `path`) keep
//! their own cheaper walks.

use std::collections::HashSet;

use hbsync_state::MAX_DEPTH;
use hbsync_types::NodeId;

/// What each step gave, from the start up, and where the walk ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Up<T> {
    pub items: Vec<T>,
    pub end: End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// At the root.
    Top,
    /// At a node the step gave nothing for: one it knows nothing of, or one it stops at (a
    /// folder already worked out, say).
    At(NodeId),
    /// At a node already passed, or deeper than any path may be.
    Cycle,
}

/// Walks up from `start`: `step(at)` gives what `at` holds and its parent, or `None` where the
/// walk ends.
pub fn up<T, E>(
    start: NodeId,
    mut step: impl FnMut(NodeId) -> Result<Option<(T, NodeId)>, E>,
) -> Result<Up<T>, E> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    let mut at = start;
    loop {
        let end = if at.is_root() {
            End::Top
        } else if !seen.insert(at) || items.len() >= MAX_DEPTH {
            End::Cycle
        } else if let Some((item, parent)) = step(at)? {
            items.push(item);
            at = parent;
            continue;
        } else {
            End::At(at)
        };
        return Ok(Up { items, end });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;

    use super::*;

    fn walk(parents: &HashMap<NodeId, NodeId>, start: NodeId) -> (Vec<NodeId>, End) {
        let Ok(up) = up::<_, Infallible>(start, |at| Ok(parents.get(&at).map(|p| (at, *p))));
        (up.items, up.end)
    }

    #[test]
    fn walks_end_at_the_root_a_gap_or_a_cycle() {
        let (a, b, c, d) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
        let parents = HashMap::from([(a, NodeId::ROOT), (b, a), (c, d), (d, c)]);
        let gap = NodeId::new();
        assert_eq!(walk(&parents, b), (vec![b, a], End::Top));
        assert_eq!(walk(&parents, gap), (vec![], End::At(gap)));
        assert_eq!(walk(&parents, c), (vec![c, d], End::Cycle));
        assert_eq!(walk(&parents, NodeId::ROOT), (vec![], End::Top));
    }
}
