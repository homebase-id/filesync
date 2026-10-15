//! Which part of the drive an engine syncs (sync semantics §12). The main folder syncs the
//! drive without the role folders, on every device whether it syncs them or not; they are
//! synced where they are, each by an engine of its own (to come).

use std::collections::{BTreeSet, HashSet};

use hbsync_state::{StateError, Store};
use hbsync_types::{NodeId, Role};

use crate::tree::Tree;

/// The state DB key of the scope's fingerprint: what the last run left out.
pub(crate) const KEY: &str = "scope.fingerprint";

/// The part of the drive one engine syncs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// Folders left out, with everything in them.
    pub outside: BTreeSet<NodeId>,
}

impl Scope {
    /// The main folder's: the drive without the role folders.
    pub fn main() -> Scope {
        Scope {
            outside: Role::ALL.iter().map(|r| r.node()).collect(),
        }
    }

    /// Stored with the state DB: tells a later run which folders were left out.
    pub fn fingerprint(&self) -> String {
        let outside: Vec<String> = self.outside.iter().map(|n| n.0.to_string()).collect();
        outside.join(",")
    }

    /// The scope the engine of this state DB syncs, as its last run recorded it; the main
    /// folder's before any.
    pub fn stored(s: &Store<'_>) -> Result<Scope, StateError> {
        Ok(match s.get_kv(KEY)? {
            Some(fingerprint) => Scope {
                outside: Scope::outside_of(&fingerprint),
            },
            None => Scope::main(),
        })
    }

    /// The folders a [`Scope::fingerprint`] left out.
    pub fn outside_of(fingerprint: &str) -> BTreeSet<NodeId> {
        fingerprint
            .split(',')
            .filter_map(|id| id.parse().ok().map(NodeId))
            .collect()
    }

    /// The nodes of `tree` outside this scope: the folders left out and everything in them.
    /// Costs what is outside, not the tree; a cycle (a folder moved into itself remotely)
    /// ends the walk.
    pub fn out_of_scope(&self, tree: &Tree) -> HashSet<NodeId> {
        let mut out = HashSet::new();
        let mut stack: Vec<NodeId> = self
            .outside
            .iter()
            .copied()
            .filter(|n| tree.contains(*n))
            .collect();
        while let Some(node) = stack.pop() {
            if out.insert(node) {
                stack.extend(tree.children(node));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_types::NodeState;

    fn folder(parent: NodeId, name: &str) -> NodeState {
        NodeState::folder(parent, name)
    }

    #[test]
    fn the_main_scope_leaves_out_the_role_folders_and_what_is_in_them() {
        let docs = Role::Documents.node();
        let (a, b, c) = (NodeId::new(), NodeId::new(), NodeId::new());
        let mut remote = Tree::new();
        remote.insert(docs, folder(NodeId::ROOT, "Documents"));
        remote.insert(a, folder(docs, "a"));
        remote.insert(b, folder(a, "b"));
        remote.insert(c, folder(NodeId::ROOT, "Documents"));
        let main = Scope::main();
        assert_eq!(main.out_of_scope(&remote), HashSet::from([docs, a, b]));
        assert_eq!(Scope::outside_of(&main.fingerprint()), main.outside);
        assert!(Scope::outside_of("").is_empty());
    }

    #[test]
    fn a_role_folder_moved_into_itself_ends_the_walk() {
        let docs = Role::Documents.node();
        let a = NodeId::new();
        let mut remote = Tree::new();
        remote.insert(docs, folder(a, "Documents"));
        remote.insert(a, folder(docs, "a"));
        assert_eq!(
            Scope::main().out_of_scope(&remote),
            HashSet::from([docs, a])
        );
    }
}
