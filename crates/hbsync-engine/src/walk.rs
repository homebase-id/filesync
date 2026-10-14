//! Walking up from a node to the root through parents kept anywhere (the state DB's base,
//! mirror or Recently deleted): one walk, so every caller stops the same way at a missing
//! parent, at a cycle (a damaged DB, or bases left by a remote cycle) and at the deepest
//! a path may be.

use std::collections::HashSet;

use hbsync_state::MAX_DEPTH;
use hbsync_types::NodeId;

/// How a walk up ended, with what each step gave, from the start up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Up<T> {
    /// At the root.
    Top(Vec<T>),
    /// At a node the step knows nothing of.
    Missing(Vec<T>),
    /// At a node already passed, or deeper than any path may be.
    Cycle(Vec<T>),
}

impl<T> Up<T> {
    /// What the steps gave, however the walk ended.
    pub fn items(self) -> Vec<T> {
        match self {
            Up::Top(items) | Up::Missing(items) | Up::Cycle(items) => items,
        }
    }
}

/// Walks up from `start`: `step(at)` gives what `at` holds and its parent, or `None` where the
/// chain ends.
pub fn up<T, E>(
    start: NodeId,
    mut step: impl FnMut(NodeId) -> Result<Option<(T, NodeId)>, E>,
) -> Result<Up<T>, E> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    let mut at = start;
    loop {
        if at.is_root() {
            return Ok(Up::Top(items));
        }
        if !seen.insert(at) || items.len() >= MAX_DEPTH {
            return Ok(Up::Cycle(items));
        }
        match step(at)? {
            Some((item, parent)) => {
                items.push(item);
                at = parent;
            }
            None => return Ok(Up::Missing(items)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn walk(parents: &HashMap<NodeId, NodeId>, start: NodeId) -> Up<NodeId> {
        up::<_, ()>(start, |at| Ok(parents.get(&at).map(|p| (at, *p)))).unwrap_or(Up::Top(vec![]))
    }

    #[test]
    fn walks_end_at_the_root_a_gap_or_a_cycle() {
        let (a, b, c, d) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
        let parents = HashMap::from([(a, NodeId::ROOT), (b, a), (c, d), (d, c)]);
        assert_eq!(walk(&parents, b), Up::Top(vec![b, a]));
        assert_eq!(walk(&parents, NodeId::new()), Up::Missing(vec![]));
        assert_eq!(walk(&parents, c), Up::Cycle(vec![c, d]));
        assert_eq!(walk(&parents, NodeId::ROOT), Up::Top(vec![]));
    }
}
