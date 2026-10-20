//! The server: live nodes and Recently deleted, its clock, and the purge.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;

use hbsync_engine::tree::Tree;
use hbsync_engine::walk::End;
use hbsync_types::{NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Role, Uuid, VersionTag};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServerNode {
    pub(crate) state: NodeState,
    pub(crate) remote: RemoteRef,
}

/// [`Server::snapshot`].
pub(crate) type Snapshot = (BTreeMap<NodeId, ServerNode>, BTreeMap<NodeId, ServerNode>);

#[derive(Debug, Default)]
pub(crate) struct Server {
    pub(crate) live: BTreeMap<NodeId, ServerNode>,
    /// Recently deleted: out of the tree, with their bytes, until restored or purged.
    pub(crate) removed: BTreeMap<NodeId, ServerNode>,
    /// When each node went to Recently deleted (the server's time of that write: a header's
    /// `updated`, [`time`]).
    pub(crate) removed_at: BTreeMap<NodeId, i64>,
    /// The tags each node had when it was last removed: a restore keeps them (§11).
    pub(crate) tags_at_removal: BTreeMap<NodeId, BTreeSet<String>>,
    next: u128,
    /// Every node that was ever live (the server doesn't check parents, like Homebase).
    pub(crate) ever: BTreeSet<NodeId>,
}

/// How long an item stays in Recently deleted, and a delete for good leaves a trace in the feed
/// (30 days: `hbsync_engine::feed::TRACE_MS`), on the server's clock ([`time`]).
pub const TRACE: i64 = 24;

/// How far the server's clock moves on from one purge to the next.
pub const PURGE_EVERY: i64 = TRACE / 2;

/// The server's time of the write that made `version`: every write takes the next id, so ids
/// are the server's clock (Homebase's `updated`).
pub(crate) fn time(version: VersionTag) -> i64 {
    version.0.as_u128() as i64
}

impl Server {
    /// A row of Recently deleted as a listing names it: its node at its version.
    pub(crate) fn at((node, n): (&NodeId, &ServerNode)) -> (NodeId, VersionTag) {
        (*node, n.remote.version)
    }

    pub(crate) fn uuid(&mut self) -> Uuid {
        self.next += 1;
        Uuid::from_u128(self.next)
    }

    /// Puts `node` in Recently deleted: a write, at its time.
    pub(crate) fn remove(&mut self, node: NodeId, mut n: ServerNode) {
        let tags = n.state.tags.iter().map(|t| t.name.clone()).collect();
        self.tags_at_removal.insert(node, tags);
        n.remote.version = VersionTag(self.uuid());
        self.removed_at.insert(node, time(n.remote.version));
        self.removed.insert(node, n);
    }

    /// The nodes in Recently deleted that were deleted with folder `node` (node schema): in it,
    /// removed no earlier than it.
    pub(crate) fn deleted_with(&self, node: NodeId) -> Vec<NodeId> {
        let Some(at) = self.removed_at.get(&node) else {
            return Vec::new();
        };
        self.removed
            .iter()
            .filter(|(n, r)| {
                r.state.parent == node && self.removed_at.get(n).is_some_and(|t| t >= at)
            })
            .map(|(n, _)| *n)
            .collect()
    }

    /// The live nodes a folder's removal took along, by the engine's one rule
    /// ([`hbsync_engine::along::taken_along`]). They count as removed, whether or not a device
    /// marked them so yet.
    pub(crate) fn taken_along(&self) -> BTreeSet<NodeId> {
        hbsync_engine::along::taken_along(
            self.live.iter().map(|(n, l)| (*n, l.state.parent)),
            |f| {
                self.removed
                    .contains_key(&f)
                    .then(|| self.removed_at.get(&f).copied())
                    .flatten()
            },
            |n| self.live.get(&n).map(|l| time(l.remote.version)),
        )
        .into_iter()
        .collect()
    }

    /// The engine's purge (`deleted::AccountOps::purge`), its clock moved on [`PURGE_EVERY`], first
    /// part: what the deletes of the items removed [`TRACE`] before now (each deleted on its own)
    /// took along and left live, top-down, as the engine picks it
    /// ([`hbsync_engine::along::taken_levels`]; the purging device knows every version here),
    /// for [`Sim::remove_live`]. Then [`Server::due`].
    ///
    /// [`Sim::remove_live`]: crate::Sim::remove_live
    pub(crate) fn purge(&mut self) -> Vec<NodeId> {
        self.next += PURGE_EVERY as u128;
        let folders: HashMap<NodeId, i64> = self
            .items(self.purge_before())
            .into_iter()
            .flatten()
            .filter(|n| self.removed[n].state.kind == NodeKind::Folder)
            .filter_map(|n| Some((n, *self.removed_at.get(&n)?)))
            .collect();
        hbsync_engine::along::taken_levels(
            self.live.iter().map(|(n, l)| (*n, l.state.parent)),
            |f| folders.get(&f).copied(),
            |n| self.live.get(&n).map(|l| time(l.remote.version)),
            |n| n == Role::Documents.node(),
        )
        .into_iter()
        .flatten()
        .collect()
    }

    /// The purge's cut-off: removed before it is kept long enough.
    pub(crate) fn purge_before(&self) -> i64 {
        self.next as i64 - TRACE
    }

    /// The purge's second part: the items all removed [`TRACE`] before now, with what was
    /// deleted with each, at their versions: what [`Sim::delete_for_good`] deletes as the
    /// engine selects it (never a live node, nor a folder holding one). Without a trace or
    /// leaving a tombstone is the same here (no feed: a client finds it missing).
    ///
    /// [`Sim::delete_for_good`]: crate::Sim::delete_for_good
    pub(crate) fn due(&self) -> BTreeSet<(NodeId, VersionTag)> {
        let before = self.purge_before();
        let mut due = BTreeSet::new();
        for item in self.items(before) {
            // Due once all of it is: one removed later has its own time.
            if item
                .iter()
                .all(|n| self.removed_at.get(n).is_some_and(|at| *at < before))
            {
                due.extend(item.iter().map(|n| (*n, self.removed[n].remote.version)));
            }
        }
        due
    }

    /// The items of Recently deleted removed before `before` (each deleted on its own), each
    /// with what was deleted with it: removed nodes deleted with their folder, level by level.
    fn items(&self, before: i64) -> Vec<Vec<NodeId>> {
        let time = |n: &NodeId| self.removed_at.get(n).copied().unwrap_or(i64::MAX);
        let tops = self.removed.iter().filter(|(n, r)| {
            time(n) < before
                && !(self.removed.contains_key(&r.state.parent) && time(&r.state.parent) <= time(n))
        });
        tops.map(|(top, _)| {
            let mut item = vec![*top];
            let mut next = 0;
            while next < item.len() {
                let folder = item[next];
                let at = time(&folder);
                let with: Vec<NodeId> = self
                    .removed
                    .iter()
                    .filter(|(n, r)| r.state.parent == folder && time(n) >= at && !item.contains(n))
                    .map(|(n, _)| *n)
                    .collect();
                item.extend(with);
                next += 1;
            }
            item
        })
        .collect()
    }

    pub(crate) fn tree(&self) -> Tree {
        let mut t = Tree::new();
        for (id, n) in &self.live {
            t.insert(*id, n.state.clone());
        }
        t
    }

    /// Puts `state` on the server as another device would, as a new file.
    pub(crate) fn put(&mut self, node: NodeId, state: NodeState) -> RemoteRef {
        let remote = RemoteRef {
            file_id: RemoteFileId(self.uuid()),
            version: VersionTag(self.uuid()),
        };
        self.ever.insert(node);
        self.live.insert(node, ServerNode { state, remote });
        remote
    }

    /// Where a walk up from `node` ends: at the role folder (`End::At` it) for the role folder
    /// and what lies in it, at the drive's top for the rest of what reaches it.
    pub(crate) fn place(&self, node: NodeId) -> End {
        let docs = Role::Documents.node();
        let Ok(up) = hbsync_engine::walk::up::<_, Infallible>(node, |at| {
            Ok((at != docs)
                .then(|| self.live.get(&at).map(|n| ((), n.state.parent)))
                .flatten())
        });
        up.end
    }

    /// Whether `node` is the role folder or lies in it.
    pub(crate) fn in_role_folder(&self, node: NodeId) -> bool {
        self.place(node) == End::At(Role::Documents.node())
    }

    /// What the drive holds, live and in Recently deleted: to tell whether it changed since.
    pub(crate) fn snapshot(&self) -> Snapshot {
        (self.live.clone(), self.removed.clone())
    }

    pub(crate) fn changed_since(&self, snapshot: &Snapshot) -> bool {
        (&self.live, &self.removed) != (&snapshot.0, &snapshot.1)
    }
}
