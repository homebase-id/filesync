//! The server: live nodes and Recently deleted, its clock, and the purge.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;

use hbsync_engine::tree::Tree;
use hbsync_engine::walk::End;
use hbsync_types::{
    NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Role, Tags, Uuid, VersionTag,
};

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
    /// `versionUpdated`, [`time`]).
    pub(crate) removed_at: BTreeMap<NodeId, i64>,
    /// Each node's last removal: its folder then, its time and kind. Kept after the purge, so
    /// what was removed apart from its folder is told whether it is still listed or not
    /// ([`crate::Sim::removed_files_apart`]).
    pub(crate) removals: BTreeMap<NodeId, (NodeId, i64, NodeKind)>,
    /// The tags each node had when it was last removed, colours too: a restore keeps them
    /// (§11).
    pub(crate) tags_at_removal: BTreeMap<NodeId, Tags>,
    next: u128,
    /// Every node that was ever live (the server doesn't check parents, like Homebase).
    pub(crate) ever: BTreeSet<NodeId>,
    /// Nodes no client reads (a newer writer's), with what was put in each: they are on the
    /// drive, and what is in them stays there.
    pub(crate) unreadable: BTreeMap<NodeId, BTreeSet<NodeId>>,
    /// Of what is in those, what a newer client moved in ([`crate::Action::IntoNewer`]), at
    /// the version it moved it at.
    pub(crate) moved_in: BTreeMap<NodeId, VersionTag>,
    /// The files the owner shares with the member ([`crate::Sim::with_member`]), by file id, as
    /// the server keeps them: a node made again as another file isn't shared.
    pub(crate) shared: BTreeSet<RemoteFileId>,
    /// Of the shared files, those the member may change (shared for editing: sync semantics
    /// §13, "What may be changed is told per item"; [`crate::Action::Editable`]).
    pub(crate) editable: BTreeSet<RemoteFileId>,
}

/// How long an item stays in Recently deleted, and a delete for good leaves a trace in the feed
/// (30 days: `hbsync_engine::time::TRACE_MS`), on the server's clock ([`time`]).
pub const TRACE: i64 = 24;

/// How far the server's clock moves on from one purge to the next.
pub const PURGE_EVERY: i64 = TRACE / 2;

/// The server's time of the write that made `version`: every write takes the next id, so ids
/// are the server's clock (Homebase's `versionUpdated`: a share given or withdrawn makes no
/// version, so it moves no time here).
pub(crate) fn time(version: VersionTag) -> i64 {
    version.0.as_u128() as i64
}

impl Server {
    /// The folders no client reads ([`Server::unreadable`]) still on the drive, live or in
    /// Recently deleted: one deleted for good is known gone (its tombstone, a delete for good
    /// announced, or asked about).
    /// Whether the drive holds file `file` of `node`, live or in Recently deleted.
    pub(crate) fn holds(&self, (node, file): (NodeId, RemoteFileId)) -> bool {
        [&self.live, &self.removed]
            .iter()
            .any(|m| m.get(&node).is_some_and(|n| n.remote.file_id == file))
    }

    pub(crate) fn unreadable_on_drive(&self) -> impl Iterator<Item = (&NodeId, &BTreeSet<NodeId>)> {
        self.unreadable
            .iter()
            .filter(|(n, _)| self.live.contains_key(n) || self.removed.contains_key(n))
    }

    /// A row of Recently deleted as a listing names it: its node at its version.
    pub(crate) fn at((node, n): (&NodeId, &ServerNode)) -> (NodeId, VersionTag) {
        (*node, n.remote.version)
    }

    /// The server's clock now ([`time`]): past its last write, and past the last purge.
    pub(crate) fn now(&self) -> i64 {
        self.next as i64
    }

    pub(crate) fn uuid(&mut self) -> Uuid {
        self.next += 1;
        Uuid::from_u128(self.next)
    }

    /// Puts `node` in Recently deleted: a write, at its time.
    pub(crate) fn remove(&mut self, node: NodeId, mut n: ServerNode) {
        n.remote.version = VersionTag(self.uuid());
        self.put_removed(node, n);
    }

    /// `node` in Recently deleted as `n`, removed at its version's time.
    fn put_removed(&mut self, node: NodeId, n: ServerNode) {
        let at = time(n.remote.version);
        self.tags_at_removal.insert(node, n.state.tags.clone());
        self.removed_at.insert(node, at);
        self.removals
            .insert(node, (n.state.parent, at, n.state.kind));
        self.removed.insert(node, n);
    }

    /// The nodes in Recently deleted that were deleted with folder `node` (node schema): in it,
    /// removed no earlier than it. A file holds none (another app may have put nodes in it).
    pub(crate) fn deleted_with(&self, node: NodeId) -> Vec<NodeId> {
        let folder = self
            .removed
            .get(&node)
            .is_some_and(|r| r.state.kind == NodeKind::Folder);
        let Some(&at) = self.removed_at.get(&node).filter(|_| folder) else {
            return Vec::new();
        };
        let with = |n: &NodeId| {
            self.removed_at
                .get(n)
                .is_some_and(|&t| hbsync_engine::deleted::deleted_with_at(t, at))
        };
        self.removed
            .iter()
            .filter(|(n, r)| r.state.parent == node && with(n))
            .map(|(n, _)| *n)
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
        let removed: HashMap<NodeId, (NodeKind, i64)> = self
            .items(self.purge_before())
            .into_iter()
            .flatten()
            .filter_map(|n| Some((n, (self.removed[&n].state.kind, *self.removed_at.get(&n)?))))
            .collect();
        hbsync_engine::along::taken_levels(
            self.live
                .iter()
                .map(|(n, l)| (*n, l.state.parent, l.state.kind)),
            |f| removed.get(&f).copied(),
            |n| self.live.get(&n).map(|l| time(l.remote.version)),
            |n| {
                let s = &self.live[&n].state;
                hbsync_engine::along::kept_by_purge(n, s.parent, s.kind)
            },
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
    ///
    /// A copy of the engine's grouping (`deleted::listing::deletes_before`, `group`), kept: the
    /// engine's reads rows lazily from its store, memoized and shared with the listing, which a
    /// closure-driven version would slow on the purge's path; and the rule is the node
    /// schema's ("deleted with": removed no earlier than its folder), stated here on the
    /// drive's rows directly. Rows deleted with each other in a cycle (the engine takes the
    /// smallest as their item) can't happen here: removal times are unique.
    fn items(&self, before: i64) -> Vec<Vec<NodeId>> {
        let time = |n: &NodeId| self.removed_at.get(n).copied().unwrap_or(i64::MAX);
        let tops = self.removed.iter().filter(|(n, r)| {
            time(n) < before
                && !(self.removed.contains_key(&r.state.parent) && time(&r.state.parent) <= time(n))
        });
        tops.map(|(top, _)| self.group(*top)).collect()
    }

    /// `top` and what was deleted with it ([`Server::deleted_with`]), level by level, each once:
    /// what restoring it restores, and the purge's item of it (the engine's `group`, by its
    /// rules: `deleted::inside`).
    pub(crate) fn group(&self, top: NodeId) -> Vec<NodeId> {
        // `deleted_with` holds nothing for a file: each is asked.
        let Ok(item) = hbsync_engine::deleted::inside::<_, std::convert::Infallible>(
            top,
            |n| (*n, true),
            |folder| Ok(self.deleted_with(*folder)),
        );
        item
    }

    /// `node` on the drive, live or in Recently deleted, and whether it is there.
    pub(crate) fn node(&self, node: NodeId) -> Option<(&ServerNode, bool)> {
        match self.live.get(&node) {
            Some(n) => Some((n, false)),
            None => self.removed.get(&node).map(|n| (n, true)),
        }
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
        self.place_below(node, Role::Documents.node())
    }

    /// Where a walk up from `node` ends: at `top` (`End::At` it) for `top` and what lies in it,
    /// at the drive's top for the rest of what reaches it.
    pub(crate) fn place_below(&self, node: NodeId, top: NodeId) -> End {
        let Ok(up) = hbsync_engine::walk::up::<_, Infallible>(node, |at| {
            Ok((at != top)
                .then(|| self.live.get(&at).map(|n| ((), n.state.parent)))
                .flatten())
        });
        up.end
    }

    /// Whether `node` is the role folder or lies in it.
    pub(crate) fn in_role_folder(&self, node: NodeId) -> bool {
        self.place(node) == End::At(Role::Documents.node())
    }

    /// `top` and all the drive has in it, live and in Recently deleted.
    pub(crate) fn with_all_in(&self, top: NodeId) -> Vec<NodeId> {
        let inside = |n: &NodeId| {
            let Ok(up) = hbsync_engine::walk::up::<_, Infallible>(*n, |at| {
                Ok((at != top)
                    .then(|| self.node(at).map(|(n, _)| ((), n.state.parent)))
                    .flatten())
            });
            up.end == End::At(top)
        };
        (self.live.keys().chain(self.removed.keys()).copied())
            .filter(inside)
            .collect()
    }

    /// The drive as a member of what its owner shares reads it (sync semantics §13, another
    /// identity's drive): the shared files only, live and in Recently deleted; a live one whose
    /// folder isn't among them, or is one no client reads, is at the top, one in Recently
    /// deleted where the drive has it.
    /// Read from the drive as it is now: also what was deleted for good shows at once (the
    /// engine's member learns of that at its next full read).
    pub(crate) fn as_shared(&self) -> Server {
        let shared = |(_, n): &(&NodeId, &ServerNode)| self.shared.contains(&n.remote.file_id);
        let own = |(id, n): (&NodeId, &ServerNode)| (*id, n.clone());
        let removed: BTreeMap<NodeId, ServerNode> =
            self.removed.iter().filter(shared).map(own).collect();
        let mut live: BTreeMap<NodeId, ServerNode> =
            self.live.iter().filter(shared).map(own).collect();
        let folderless: Vec<NodeId> = live
            .iter()
            .filter(|(_, n)| {
                let folder = n.state.parent;
                let read = |m: &BTreeMap<NodeId, ServerNode>| {
                    m.contains_key(&folder) && !self.unreadable.contains_key(&folder)
                };
                !folder.is_root() && !read(&live) && !read(&removed)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in folderless {
            live.get_mut(&id).expect("live").state.parent = NodeId::ROOT;
        }
        self.holding(live, removed)
    }

    /// The drive as a member reads it that doesn't sync what is shared any more
    /// (`Sim::end_share`): nothing of it.
    pub(crate) fn as_unshared(&self) -> Server {
        self.holding(BTreeMap::new(), BTreeMap::new())
    }

    /// This drive holding `live` and `removed` in place of what it holds: the rest as it is.
    pub(crate) fn holding(
        &self,
        live: BTreeMap<NodeId, ServerNode>,
        removed: BTreeMap<NodeId, ServerNode>,
    ) -> Server {
        Server {
            live,
            removed,
            removed_at: self.removed_at.clone(),
            removals: self.removals.clone(),
            tags_at_removal: self.tags_at_removal.clone(),
            next: self.next,
            ever: self.ever.clone(),
            unreadable: self.unreadable.clone(),
            moved_in: self.moved_in.clone(),
            shared: self.shared.clone(),
            editable: self.editable.clone(),
        }
    }

    /// The live node `node`, if its file is one the member may change ([`Server::editable`]):
    /// a folder that is one may be added to.
    pub(crate) fn editable_node(&self, node: NodeId) -> Option<&ServerNode> {
        (self.live.get(&node)).filter(|n| self.editable.contains(&n.remote.file_id))
    }

    /// Takes what a member that writes changed on the drive as it read it (`seen`, which was
    /// as `before` when its sync began: [`Server::as_shared`]) into the drive:
    /// - a change of a file it may change, where the file is: the drive keeps the folder it
    ///   has the file in (the member reads one whose folder isn't shared at the top, and
    ///   sends the drive's: never its own);
    /// - such a file's move between two folders it may change;
    /// - a node made in a folder it may change: shared as that folder is, for editing;
    /// - a file or folder it may change removed: to Recently deleted, where the drive has it;
    ///   a folder only where all the member reads in it may be changed too.
    ///
    /// Anything else it did is a breach, named, and dropped: one made or moved elsewhere
    /// (the top is no folder of the drive's), a change or a removal of a file it may not
    /// change, anything else of Recently deleted. The drive's ids go on from the member's,
    /// so no version is given twice.
    ///
    /// Returns the breach, and the folders it removed (what each took along is the whole
    /// drive's to say, not the member's view's).
    pub(crate) fn take_from_member(
        &mut self,
        before: &Snapshot,
        seen: &Server,
    ) -> (Option<String>, Vec<NodeId>) {
        self.next = seen.next;
        let mut breach = None;
        let adds_to = |drive: &Server, folder: NodeId| drive.editable_node(folder).is_some();
        let mut removed_now = BTreeSet::new();
        for (id, was) in &before.0 {
            let Some(now) = seen.live.get(id) else {
                // Removed: of a file it may change, as the drive has it.
                let file = was.remote.file_id;
                let there = seen.removed.get(id).filter(|r| r.remote.file_id == file);
                let mine = self.editable_node(*id).filter(|m| m.remote.file_id == file);
                let (Some(there), Some(mine)) = (there, mine) else {
                    breach.get_or_insert_with(|| format!("it removed {id:?}"));
                    continue;
                };
                let parent = mine.state.parent;
                let gone = ServerNode {
                    state: NodeState {
                        parent,
                        ..there.state.clone()
                    },
                    remote: there.remote,
                };
                self.live.remove(id);
                self.put_removed(*id, gone);
                removed_now.insert(*id);
                continue;
            };
            if now == was {
                continue;
            }
            let file = was.remote.file_id;
            let moved = now.state.parent != was.state.parent;
            let may = now.remote.file_id == file
                && self.editable.contains(&file)
                && (!moved || (adds_to(self, was.state.parent) && adds_to(self, now.state.parent)));
            let mine = self.live.get_mut(id).filter(|m| m.remote.file_id == file);
            let Some(mine) = mine.filter(|_| may) else {
                breach.get_or_insert_with(|| format!("it changed {id:?}: {was:?} to {now:?}"));
                continue;
            };
            let parent = match moved {
                true => now.state.parent,
                false => mine.state.parent,
            };
            *mine = ServerNode {
                state: NodeState {
                    parent,
                    ..now.state.clone()
                },
                remote: now.remote,
            };
        }
        // Nothing else of Recently deleted: none restored, emptied or changed.
        let untouched = |(id, r): (&NodeId, &ServerNode)| {
            removed_now.contains(id) || before.1.get(id) == Some(r)
        };
        if !seen.removed.iter().all(untouched)
            || before.1.keys().any(|id| !seen.removed.contains_key(id))
        {
            breach.get_or_insert_with(|| "it changed Recently deleted".to_owned());
        }
        for (id, made) in (seen.live.iter()).filter(|(id, _)| !before.0.contains_key(id)) {
            let new = !self.live.contains_key(id) && !self.removed.contains_key(id);
            if !new || !adds_to(self, made.state.parent) {
                breach.get_or_insert_with(|| format!("it made {id:?}: {made:?}"));
                continue;
            }
            self.live.insert(*id, made.clone());
            self.ever.insert(*id);
            self.shared.insert(made.remote.file_id);
            self.editable.insert(made.remote.file_id);
        }
        let folders: Vec<NodeId> = (removed_now.into_iter())
            .filter(|id| (self.removed.get(id)).is_some_and(|r| r.state.kind == NodeKind::Folder))
            .collect();
        // A folder's removal takes along what is in it: of what the member reads there (live
        // in its view still: taken along, or removed by its next writes), all must be the
        // member's to change. Through live folders, as the drive's rule: what is in a folder
        // removed earlier is that removal's.
        let in_one_removed = |mut at: NodeId| {
            for _ in 0..64 {
                if folders.contains(&at) {
                    return true;
                }
                match seen.live.get(&at) {
                    Some(n) if !n.state.parent.is_root() => at = n.state.parent,
                    _ => return false,
                }
            }
            false
        };
        let not_its = (seen.live.iter()).find(|(_, n)| {
            in_one_removed(n.state.parent) && !self.editable.contains(&n.remote.file_id)
        });
        if let Some((id, _)) = not_its {
            breach.get_or_insert_with(|| format!("it removed the folder {id:?} is in"));
        }
        (breach, folders)
    }

    /// What the drive holds, live and in Recently deleted: to tell whether it changed since.
    pub(crate) fn snapshot(&self) -> Snapshot {
        (self.live.clone(), self.removed.clone())
    }

    pub(crate) fn changed_since(&self, snapshot: &Snapshot) -> bool {
        (&self.live, &self.removed) != (&snapshot.0, &snapshot.1)
    }
}
