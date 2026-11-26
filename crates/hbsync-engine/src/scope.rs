//! Which part of the drive an engine syncs (sync semantics §12). The main folder syncs the
//! drive without the role folders, on every device whether it syncs them or not, and without
//! the device's own Desktop; they are synced where they are, each by an engine of its own,
//! which sees its top as its root and the rest of the drive as one folder it leaves out
//! ([`NodeId::OUTSIDE`]; the state DB translates, [`hbsync_state::Coords`], and so does
//! [`ScopedRemote`] for what it writes).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_remote::{
    ChangesPage, NodeMeta, RefsPage, Remote, RemoteError, RemoteHeader, Result as RemoteResult,
    Upload,
};
use hbsync_state::Coords;
use hbsync_types::{Doomed, NodeId, RemoteFileId, Role, Target};

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

    /// This scope, leaving out the drive's nodes `tops` too: the main folder's on a device
    /// whose other folders hold them (its own Desktop's folder, wherever it is on the drive).
    pub fn with_tops(&self, tops: impl IntoIterator<Item = NodeId>) -> Scope {
        Scope {
            top: self.top,
            outside: self.outside.iter().copied().chain(tops).collect(),
        }
    }

    /// A folder holding the drive's node `top` (a role folder, a Desktop), and nothing else.
    pub(crate) fn below(top: NodeId) -> Scope {
        Scope {
            top,
            outside: BTreeSet::from([NodeId::OUTSIDE]),
        }
    }

    /// The scope of a folder whose top is `top`, on a device whose other folders hold `tops`:
    /// the main folder's ([`Scope::with_tops`]), or a role folder's or a Desktop's.
    pub fn on_device(top: NodeId, tops: impl IntoIterator<Item = NodeId>) -> Scope {
        match top {
            t if t.is_root() => Scope::main().with_tops(tops),
            t => Scope::below(t),
        }
    }

    /// [`Scope::on_device`] on a device with no other folder but role folders.
    pub fn of(top: NodeId) -> Scope {
        Scope::on_device(top, [])
    }

    /// `remote` as this engine writes to it: a node at its root is at its top.
    pub(crate) fn remote(&self, remote: Arc<dyn Remote>) -> Arc<dyn Remote> {
        if self.top.is_root() {
            return remote;
        }
        Arc::new(ScopedRemote {
            inner: remote,
            coords: Coords { top: self.top },
        })
    }

    /// Stored with the state DB: tells a later run which folders were left out.
    pub(crate) fn fingerprint(&self) -> String {
        let outside: Vec<String> = self.outside.iter().map(|n| n.0.to_string()).collect();
        outside.join(",")
    }

    /// The folders a [`Scope::fingerprint`] left out.
    pub(crate) fn outside_of(fingerprint: &str) -> BTreeSet<NodeId> {
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
        let mut stack: Vec<NodeId> = Vec::new();
        for &left_out in &self.outside {
            if tree.contains(left_out) {
                stack.push(left_out);
            } else {
                // A role folder in Recently deleted: what still sits in it (a file another
                // device put there as it was deleted) is its, never the main folder's.
                stack.extend(tree.children(left_out));
            }
        }
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
        End::Cycle | End::Deep => false,
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

    fn others_added(&self, nodes: &[NodeId]) -> RemoteResult<()> {
        self.inner.others_added(nodes)
    }
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn holding(
        &self,
        folders: &[NodeId],
        holds: hbsync_remote::Holds<'_>,
    ) -> RemoteResult<std::collections::HashSet<NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn hold_back(&self) -> std::time::Duration {
        self.inner.hold_back()
    }
    fn shared(&self) -> bool {
        self.inner.shared()
    }
    fn writes(&self, asked: bool) -> RemoteResult<bool> {
        self.inner.writes(asked)
    }
    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<hbsync_types::Written> {
        self.inner.create(&self.on_drive(node)?, payload)
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<hbsync_types::Written> {
        self.inner.update(target, &self.on_drive(meta)?, payload)
    }
    fn update_from(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
        from: NodeId,
    ) -> RemoteResult<hbsync_types::Written> {
        // (The mirror's parent of the node, in this folder's coordinates as `meta` is.)
        let from = self.coords.to_drive(from);
        (self.inner).update_from(target, &self.on_drive(meta)?, payload, from)
    }
    fn delete(&self, files: &[Doomed]) -> RemoteResult<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(&self, files: &[Doomed]) -> RemoteResult<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Written>> {
        self.inner.set_removed(files, removed)
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

    /// A write through a folder's remote names the folder the node is in as the drive has
    /// it. A node at a role folder's top isn't moved by a write that leaves it there: a move
    /// would take the top's shares along ([`Remote::update_from`]).
    #[test]
    fn a_write_at_the_top_of_a_role_folder_is_no_move() -> Result<(), RemoteError> {
        use hbsync_remote::contract::Shares;
        use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
        use hbsync_types::NodeKind;
        use hbsync_types::schema::NodeContent;
        let docs = Role::Documents.node();
        let sharer = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let member = FakeRemote::member(sharer.server_handle());
        let folder = |node: NodeId, parent: NodeId, name: &str| NodeMeta {
            node,
            parent,
            kind: NodeKind::Folder,
            content: NodeContent::folder(name),
            removed: false,
        };
        // The role folder, shared once something was in it already: that isn't shared.
        let top = sharer.create(&folder(docs, NodeId::ROOT, "Documents"), None)?;
        let inside = NodeId::new();
        let made = sharer.create(&folder(inside, docs, "inside"), None)?;
        sharer.share(top.at().file_id)?;
        assert!(member.header(made.at().file_id).is_err());

        // Renamed where it is: to this folder, it is at the root before and after.
        let scoped = Scope::of(docs).remote(sharer.clone());
        let renamed = folder(inside, NodeId::ROOT, "renamed");
        let written = scoped.update_from(&made.target, &renamed, None, NodeId::ROOT)?;
        assert!(
            member.header(written.at().file_id).is_err(),
            "shared by a rename: taken for a move from the drive's top"
        );
        Ok(())
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
