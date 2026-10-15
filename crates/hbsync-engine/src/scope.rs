//! Which part of the drive an engine syncs (sync semantics §12). The main folder syncs the
//! drive without the role folders, on every device whether it syncs them or not; they are
//! synced where they are, each by an engine of its own, which sees its top as its root and
//! the rest of the drive as one folder it leaves out ([`NodeId::OUTSIDE`]; the state DB
//! translates, [`hbsync_state::Coords`], and so does [`ScopedRemote`] for what it writes).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_remote::{
    ChangesPage, NodeMeta, Remote, RemoteError, RemoteHeader, Result as RemoteResult, Upload,
};
use hbsync_state::Coords;
use hbsync_types::{NodeId, RemoteFileId, RemoteRef, Role, VersionTag};

use crate::tree::Tree;
use crate::walk::End;

/// The part of the drive one engine syncs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// The drive's node the engine's root is: the drive's root for the main folder.
    pub top: NodeId,
    /// Folders left out, with everything in them (as the engine sees them).
    pub outside: BTreeSet<NodeId>,
}

impl Scope {
    /// The main folder's: the drive without the role folders.
    pub fn main() -> Scope {
        Scope {
            top: NodeId::ROOT,
            outside: Role::ALL.iter().map(|r| r.node()).collect(),
        }
    }

    /// A folder holding the drive's node `top` (a role folder), and nothing else.
    pub fn below(top: NodeId) -> Scope {
        Scope {
            top,
            outside: BTreeSet::from([NodeId::OUTSIDE]),
        }
    }

    /// The scope of a folder whose top is `top`: the main folder's, or a role folder's.
    pub fn of(top: NodeId) -> Scope {
        match top {
            t if t.is_root() => Scope::main(),
            t => Scope::below(t),
        }
    }

    /// `remote` as this engine writes to it: a node at its root is at its top.
    pub fn remote(&self, remote: Arc<dyn Remote>) -> Arc<dyn Remote> {
        if self.top.is_root() {
            return remote;
        }
        Arc::new(ScopedRemote {
            inner: remote,
            coords: Coords { top: self.top },
        })
    }

    /// Stored with the state DB: tells a later run which folders were left out.
    pub fn fingerprint(&self) -> String {
        let outside: Vec<String> = self.outside.iter().map(|n| n.0.to_string()).collect();
        outside.join(",")
    }

    /// The folders a [`Scope::fingerprint`] left out.
    pub fn outside_of(fingerprint: &str) -> BTreeSet<NodeId> {
        fingerprint
            .split(',')
            .filter_map(|id| id.parse().ok().map(NodeId))
            .collect()
    }

    /// The nodes of `tree` outside this scope: the folders left out and everything in them.
    /// For the main folder this costs what is outside, not the tree; a cycle (a folder moved
    /// into itself remotely) ends the walk. A folder holding part of the drive holds only what
    /// reaches its top: a node whose place isn't known (its folder gone, a cycle) is outside it,
    /// for the main folder to repair.
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
        if !self.top.is_root() {
            let mut reaches: HashMap<NodeId, bool> = HashMap::new();
            for (node, _) in tree.iter() {
                if !reaches_root(tree, node, &mut reaches) {
                    out.insert(node);
                }
            }
        }
        out
    }
}

/// Whether `node` reaches the root through `tree`'s parents, memoized in `known` for every
/// node on the way.
fn reaches_root(tree: &Tree, node: NodeId, known: &mut HashMap<NodeId, bool>) -> bool {
    let Ok(up) = crate::walk::up::<_, Infallible>(node, |at| {
        Ok(match known.contains_key(&at) {
            true => None,
            false => tree.get(at).map(|state| (at, state.parent)),
        })
    });
    let reached = match up.end {
        End::Top => true,
        End::At(at) => known.get(&at).copied().unwrap_or(false),
        End::Cycle => false,
    };
    for n in up.items {
        known.insert(n, reached);
    }
    reached
}

/// The remote as an engine holding part of the drive writes to it: a node it puts at its root
/// is at its top. The rest of the drive ([`NodeId::OUTSIDE`]) is never written.
struct ScopedRemote {
    inner: Arc<dyn Remote>,
    coords: Coords,
}

impl ScopedRemote {
    fn on_drive(&self, meta: &NodeMeta) -> RemoteResult<NodeMeta> {
        if self.coords.is_outside(meta.node) || self.coords.is_outside(meta.parent) {
            return Err(RemoteError::Fatal(
                "a write into the part of the drive this folder leaves out".into(),
            ));
        }
        Ok(NodeMeta {
            parent: self.coords.to_drive(meta.parent),
            ..meta.clone()
        })
    }
}

impl Remote for ScopedRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<ChangesPage> {
        self.inner.changes(cursor, limit)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<RemoteRef> {
        self.inner.create(&self.on_drive(node)?, payload)
    }
    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<RemoteRef> {
        self.inner
            .update(file_id, expected, &self.on_drive(meta)?, payload)
    }
    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> RemoteResult<()> {
        self.inner.delete(file_id, expected)
    }
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> RemoteResult<u64> {
        self.inner.read(file_id, range, sink)
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
