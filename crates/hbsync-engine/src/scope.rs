//! Which part of the drive an engine syncs (sync semantics §12). The main folder syncs the
//! drive's top without the role folders, on every device whether it syncs them or not; they
//! are synced where they are, each by an engine of its own.

use std::collections::{BTreeSet, HashMap, HashSet};

use hbsync_types::{NodeId, Role};

use crate::tree::Tree;

/// The part of the drive one engine syncs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// The folder the local folder is: the drive's root for the main folder.
    pub root: NodeId,
    /// Folders left out, with everything in them.
    pub outside: BTreeSet<NodeId>,
}

impl Scope {
    /// The main folder's: the drive without the role folders.
    pub fn main() -> Scope {
        Scope {
            root: NodeId::ROOT,
            outside: Role::ALL.iter().map(|r| r.node()).collect(),
        }
    }

    /// Changes when the scope does: a different scope plans every synced node again.
    pub fn fingerprint(&self) -> String {
        let outside: Vec<String> = self.outside.iter().map(|n| n.0.to_string()).collect();
        format!("{}-{}", self.root.0, outside.join(","))
    }

    /// The nodes of `remote` outside this scope: the folders left out and everything in them.
    pub fn out_of_scope(&self, remote: &Tree) -> HashSet<NodeId> {
        let mut memo: HashMap<NodeId, bool> = HashMap::new();
        remote
            .iter()
            .map(|(node, _)| node)
            .filter(|&node| self.left_out(remote, node, &mut memo))
            .collect()
    }

    /// Whether `node` is or lies in a folder left out, memoized in `memo`.
    fn left_out(&self, remote: &Tree, node: NodeId, memo: &mut HashMap<NodeId, bool>) -> bool {
        // Up to the first node already known (or the top, or a cycle), then down again.
        let mut chain = Vec::new();
        let mut current = node;
        let mut out = loop {
            if let Some(&known) = memo.get(&current) {
                break known;
            }
            if self.outside.contains(&current) {
                break true;
            }
            if chain.contains(&current) {
                break false;
            }
            chain.push(current);
            match remote.get(current) {
                Some(state) => current = state.parent,
                None => break false,
            }
        };
        while let Some(n) = chain.pop() {
            out |= self.outside.contains(&n);
            memo.insert(n, out);
        }
        out
    }
}

impl Default for Scope {
    fn default() -> Self {
        Scope::main()
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
        let out = Scope::main().out_of_scope(&remote);
        assert_eq!(out, HashSet::from([docs, a, b]));
        assert_ne!(
            Scope::main().fingerprint(),
            Scope {
                root: NodeId::ROOT,
                outside: BTreeSet::new()
            }
            .fingerprint()
        );
    }
}
