//! Step 3: the merged tree made whole again: orphans, cycles, duplicate names (§6).

use super::*;

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

    pub(super) fn fix_orphans(&mut self) {
        let mut queue: Vec<NodeId> = self.merged.ids().collect();
        queue.sort();
        // Nodes taken out below; their children follow them.
        let mut withdrawn: HashSet<NodeId> = HashSet::new();
        while let Some(node) = queue.pop() {
            let Some(parent) = self.merged.get(node).map(|s| s.parent) else {
                continue;
            };
            if self.merged.exists(parent) {
                continue;
            }
            let parent_state = if withdrawn.contains(&parent) {
                None
            } else {
                self.known_state(parent)
            };
            match parent_state {
                Some(state) if state.kind == NodeKind::Folder => {
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
                None if self.input.remote_complete
                    && self.input.remote.contains(node)
                    && !withdrawn.contains(&parent) =>
                {
                    // Nobody knows the parent: the server kept a child of a folder deleted
                    // concurrently. Keep the data visible by moving it to the root.
                    if let Some(mut state) = self.merged.get(node).cloned() {
                        state.parent = NodeId::ROOT;
                        self.retarget(node, state);
                        self.plan.conflicts.push(Conflict::Orphan { node });
                    }
                }
                _ => {
                    // The parent is unknown (or not a folder, or itself waiting): wait for more
                    // information. Children already checked against this node are re-checked.
                    queue.extend(self.merged.children(node).collect::<Vec<_>>());
                    self.merged.remove(node);
                    self.scope.remove(&node);
                    withdrawn.insert(node);
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
            // Names held remotely by nodes this device doesn't sync (§10) are taken: their
            // holders keep them, and every node here loses.
            let masked = self.masks.names.get(&parent);
            let taken = |key: &str| masked.is_some_and(|m| m.contains(key));
            for (key, mut group) in by_key {
                let keep = usize::from(!taken(&key));
                if group.len() <= keep {
                    continue;
                }
                // The lowest id keeps the name, except that a deferred node (a file still
                // being written) is never renamed: it wins, and is left alone if it loses.
                group.sort_by_key(|n| (!self.deferred.contains(n), *n));
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
        }
    }
}
