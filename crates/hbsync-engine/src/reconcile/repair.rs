//! Step 3: the merged tree made whole again: orphans, cycles, duplicate names (§6).

use super::*;
use hbsync_types::RECOVERED_NAME;

/// What becomes of a node whose folder isn't in the merged tree as a folder
/// ([`Reconciler::fix_orphans`]).
enum Parent {
    /// Its folder comes back.
    Folder(NodeState),
    /// It moves into Recovered files.
    Recover,
    /// It waits for more information.
    Wait,
}

impl Reconciler<'_> {
    // ---- 3: invariants on the merged tree ----

    /// The node whose fork makes the conflict copy `copy`, if one does.
    fn forked_into(&self, copy: NodeId) -> Option<NodeId> {
        self.forks
            .iter()
            .find_map(|(&node, &c)| (c == copy).then_some(node))
    }

    /// The fork of `node` can't make its copy in this plan (its folder waits, §6.3): never one
    /// named "". The node waits with it, as it is, and says so instead of its conflicts.
    pub(super) fn fork_waits(&mut self, node: NodeId) {
        self.forks.remove(&node);
        self.scope.remove(&node);
        self.origin.remove(&node);
        self.plan.conflicts.retain(|c| c.node() != node);
        self.plan.conflicts.push(Conflict::CopyWaits { node });
        self.defer(node);
    }

    /// A repair puts `node` at `state` in the merged tree; ops follow for it.
    pub(super) fn retarget(&mut self, node: NodeId, state: NodeState) {
        self.merged.insert(node, state);
        self.scope.insert(node);
        self.origin.entry(node).or_insert(Origin::Base);
    }

    /// The best known state of a node that the merge removed or never saw, Recently deleted
    /// included. A masked node has none: it can't come back here under its id.
    pub(super) fn known_state(&self, node: NodeId) -> Option<NodeState> {
        if self.masks.masked.contains(&node) {
            return None;
        }
        let i = self.input;
        i.remote
            .get(node)
            .or(i.local.get(node))
            .or(i.base.get(node))
            .or(i.removed.get(&node).map(|r| &r.state))
            .cloned()
    }

    /// Where the orphan `node` goes: Recovered files ([`NodeId::RECOVERED`]), made here if no
    /// side knows it (one in Recently deleted, or synced here before, comes back as any folder
    /// a live child keeps does). The main folder adopts orphans (§12); a role folder's never
    /// meets one but through a folder it filters, and puts that at its top, as does the main
    /// folder where Recovered files isn't synced here (filtered) or is the orphan itself.
    fn recovered_files(&mut self, node: NodeId) -> NodeId {
        // Never into itself: Recovered files moved by the user into what is now the orphan.
        let inside = |at: NodeId| at == node || self.merged.ancestors(at).contains(&node);
        if node == NodeId::RECOVERED
            || !self.masking.scope.top.is_root()
            || self.masks.masked.contains(&NodeId::RECOVERED)
            || inside(NodeId::RECOVERED)
            || self
                .known_state(NodeId::RECOVERED)
                .is_some_and(|known| inside(known.parent))
        {
            return NodeId::ROOT;
        }
        if !self.merged.exists(NodeId::RECOVERED) && self.known_state(NodeId::RECOVERED).is_none() {
            // Made here: under the name it gets, if this device syncs that.
            if !self
                .masking
                .filter
                .in_folder("", RECOVERED_NAME, true)
                .is_sync()
            {
                return NodeId::ROOT;
            }
            self.retarget(
                NodeId::RECOVERED,
                NodeState::folder(NodeId::ROOT, RECOVERED_NAME),
            );
        }
        NodeId::RECOVERED
    }

    /// A node whose folder is nowhere, or a file: moved into Recovered files if it is on the
    /// drive (live, or a folder in Recently deleted that a node brings back), to keep it
    /// visible; else it waits. One deferred waits too: its merge is decided later (its item
    /// not hashed yet), and the state it holds till then is the drive's, which adopting would
    /// write over the item (its tags) and send up as decided.
    fn recover_or_wait(&self, node: NodeId) -> Parent {
        let i = self.input;
        if self.deferred.contains(&node) {
            return Parent::Wait;
        }
        let on_drive = i.remote.contains(node)
            || (self.masking.scope.top.is_root() && i.removed.contains_key(&node));
        if on_drive {
            Parent::Recover
        } else {
            Parent::Wait
        }
    }

    pub(super) fn fix_orphans(&mut self) {
        let mut queue: Vec<NodeId> = self.merged.ids().collect();
        queue.sort();
        // Nodes taken out below; their children follow them.
        let mut withdrawn: HashSet<NodeId> = HashSet::new();
        while let Some(node) = queue.pop() {
            let Some(parent) = self.merged.get(node).map(|s| s.parent) else {
                continue;
            };
            let decided = match self.merged.get(parent) {
                Some(p) if p.kind == NodeKind::Folder => continue,
                _ if parent.is_root() => continue,
                // There, in a version this device can't read (a folder it doesn't read, a file
                // that is a folder now, maybe): what is in it stays in it.
                _ if self.input.unreadable.contains(&parent) => Parent::Wait,
                // A file: another writer put the node in it (§6.3).
                Some(_) => self.recover_or_wait(node),
                None if withdrawn.contains(&parent) => Parent::Wait,
                None => match self.known_state(parent) {
                    Some(state) if state.kind == NodeKind::Folder => Parent::Folder(state),
                    // Nowhere, or known as a file only before (another writer's), though the
                    // drive's picture is whole: the server kept a child of a folder deleted for
                    // good (a create racing the delete, another app), live or brought back from
                    // Recently deleted by what is in it.
                    _ if self.input.remote_complete => self.recover_or_wait(node),
                    _ => Parent::Wait,
                },
            };
            match decided {
                Parent::Folder(state) => {
                    // A live child keeps its folder alive (§6.3), one in Recently deleted too.
                    // One a folder's removal took along is merged after all, as if no removal
                    // had: its own changes count (a tag, which the merge doesn't call one).
                    if self.taken.remove(&parent) {
                        self.merge_node(parent);
                        if self.merged.exists(parent) {
                            queue.push(parent);
                            continue;
                        }
                    }
                    self.retarget(parent, state);
                    queue.push(parent);
                }
                Parent::Recover => {
                    if let Some(mut state) = self.merged.get(node).cloned() {
                        state.parent = self.recovered_files(node);
                        self.retarget(node, state);
                        self.adopted.insert(node);
                        self.plan.conflicts.push(Conflict::Orphan { node });
                        // Its new folder may have to come back first.
                        queue.push(node);
                    }
                }
                Parent::Wait => {
                    // The parent is unknown (or not a folder, or itself waiting): wait for more
                    // information. Children already checked against this node are re-checked.
                    queue.extend(self.merged.children(node).collect::<Vec<_>>());
                    self.merged.remove(node);
                    self.scope.remove(&node);
                    withdrawn.insert(node);
                    // An orphan whose Recovered files waits too: not moved after all.
                    if self.adopted.remove(&node) {
                        self.plan
                            .conflicts
                            .retain(|c| *c != Conflict::Orphan { node });
                    }
                    if self.input.dirty.contains(&node) {
                        self.deferred.insert(node);
                    }
                    // A conflict copy taken out: its fork waits, before the names are judged.
                    if let Some(original) = self.forked_into(node) {
                        self.fork_waits(original);
                    }
                }
            }
        }
    }

    /// Undoes moves that would put a folder inside itself. Whether it undid any.
    pub(super) fn fix_cycles(&mut self) -> bool {
        let mut fixed = false;
        let candidates: Vec<NodeId> = self.scope.iter().copied().collect();
        for node in candidates {
            loop {
                let members = self.merged.cycle_members(node);
                if members.is_empty() {
                    break;
                }
                // Undo a local move first: the remote side has already published its moves.
                let local_moved = members.iter().copied().find(|m| {
                    self.input
                        .local
                        .get(*m)
                        .zip(self.merged.get(*m))
                        .is_some_and(|(l, t)| l.same_place(t))
                        && self
                            .input
                            .remote
                            .get(*m)
                            .zip(self.merged.get(*m))
                            .is_some_and(|(r, t)| !r.same_place(t))
                });
                let victim = local_moved.unwrap_or_else(|| *members.iter().min().unwrap_or(&node));
                let fallback = self
                    .input
                    .remote
                    .get(victim)
                    .or(self.input.base.get(victim))
                    .map(|s| (s.parent, s.name.clone()));
                let Some(mut state) = self.merged.get(victim).cloned() else {
                    break;
                };
                match fallback {
                    Some((parent, name)) if !self.merged.would_cycle(victim, parent) => {
                        state.parent = parent;
                        state.name = name;
                    }
                    _ => state.parent = NodeId::ROOT,
                }
                self.retarget(victim, state);
                self.plan.conflicts.push(Conflict::Cycle { node: victim });
                fixed = true;
            }
        }
        fixed
    }

    /// The folder whose coverage holds `folder`'s children: a detached folder's are its old
    /// node's.
    fn covered_as(&self, folder: NodeId) -> NodeId {
        self.masks
            .detached
            .iter()
            .find_map(|(&old, &new)| (new == folder).then_some(old))
            .unwrap_or(folder)
    }

    pub(super) fn fix_names(&mut self) {
        let parents: BTreeSet<NodeId> = self
            .scope
            .iter()
            .filter_map(|n| self.merged.get(*n).map(|s| s.parent))
            .collect();
        for parent in parents {
            let mut by_key: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
            for child in self.merged.children(parent) {
                if let Some(s) = self.merged.get(child) {
                    by_key.entry(name_key(&s.name)).or_default().push(child);
                }
            }
            // Judged on part of the folder: right only for the names whose possible clashes
            // the input holds, and with no rename (a numbered name must be free among all).
            let covered_as = self.covered_as(parent);
            let input = self.input;
            let part = input
                .coverage
                .as_ref()
                .filter(|c| !c.full.contains(&covered_as));
            let mut uncovered = part.is_some_and(|coverage| {
                self.merged.children(parent).any(|c| {
                    self.scope.contains(&c)
                        && self
                            .merged
                            .get(c)
                            .is_some_and(|s| !coverage.holds(covered_as, &s.name))
                })
            });
            // Names held remotely by nodes this device doesn't sync (§10) are taken: their
            // holders keep them, and every node here loses.
            let masked = self.masks.names.get(&parent);
            let taken = |key: &str| masked.is_some_and(|m| m.contains(key));
            for (key, mut group) in by_key {
                let keep = usize::from(!taken(&key));
                if group.len() <= keep {
                    continue;
                }
                uncovered |= part.is_some();
                // The lowest id keeps the name, except that a deferred node (a file still
                // being written) is never renamed: it wins, and is left alone if it loses;
                // and an orphan moving in, or Recovered files made beside a folder of its
                // name, loses to what is there.
                group.sort_by_key(|n| {
                    let arriving = self.adopted.contains(n) || *n == NodeId::RECOVERED;
                    (!self.deferred.contains(n), arriving, *n)
                });
                for loser in group.into_iter().skip(keep) {
                    if self.deferred.contains(&loser) {
                        continue;
                    }
                    let Some(mut state) = self.merged.get(loser).cloned() else {
                        continue;
                    };
                    // Unbounded, and syncing or not: some numbered name is always free.
                    let Some(new_name) = (1..)
                        .map(|n| numbered_conflict_name(&state.name, n))
                        .find(|c| {
                            self.merged.child_named(parent, c, Some(loser)).is_none()
                                && !taken(&name_key(c))
                        })
                    else {
                        continue;
                    };
                    state.name = new_name;
                    self.retarget(loser, state);
                    self.plan
                        .conflicts
                        .push(Conflict::NameClash { node: loser });
                }
            }
            if uncovered {
                self.plan.uncovered.insert(covered_as);
            }
        }
    }
}
