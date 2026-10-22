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
            // Its id taken on the drive by a row this client can't read (a file another writer
            // gave it, #170): no device can make it there, and a move into it would wait for
            // good, tried again at every sync.
            || self.input.unreadable.contains(&NodeId::RECOVERED)
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
    /// visible; else it waits.
    fn recover_or_wait(&self, node: NodeId) -> Parent {
        let i = self.input;
        let on_drive = i.remote.contains(node)
            || (self.masking.scope.top.is_root() && i.removed.contains_key(&node));
        if on_drive {
            Parent::Recover
        } else {
            Parent::Wait
        }
    }

    /// What the removals of folders this plan keeps (restored by a live child, or by their own
    /// change) took along, still live on the drive below them there, and isn't changed here:
    /// merged as if no removal had, as restoring a folder restores what was deleted with it
    /// (node schema, Recently deleted), whether the plan holds those nodes or not. One changed
    /// here stays taken: the drive's version, which this device never had, stays with the
    /// removal, and this device's goes up as a new item (§4). A dirty one is merged again, what
    /// it said as taken undone; the rest is context. Whether any was.
    pub(super) fn untake_kept(&mut self) -> bool {
        if self.taken.is_empty() {
            return false;
        }
        let kept: Vec<NodeId> = self
            .input
            .removed
            .keys()
            .copied()
            // Kept by this plan: planned (in its scope), and in the merged tree.
            .filter(|f| self.scope.contains(f) && self.merged.get(*f).is_some())
            .collect();
        let mut any = false;
        for folder in kept {
            let below: Vec<NodeId> = self
                .input
                .remote
                .subtree(folder)
                .into_iter()
                .filter(|n| {
                    *n != folder && self.taken.contains(n) && !self.local_change(*n).is_change()
                })
                .collect();
            for node in below {
                any = true;
                self.taken.remove(&node);
                if self.input.dirty.contains(&node) {
                    self.plan.conflicts.retain(|c| c.node() != node);
                    self.plan.delete_waits.retain(|n| *n != node);
                    self.deferred.remove(&node);
                    self.merge_node(node);
                } else {
                    self.context(node);
                }
            }
        }
        any
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
            // Left as it is till an item here is hashed: never an orphan to adopt. What another
            // page plans, or its folder, waits to be planned with it, as a page of its own next
            // (`Plan::waits_together`): no folder brought back for it here, none merged (its
            // change unmerged, it read alive and brought its deleted folder back: review 14 R1).
            let decided = match decided {
                Parent::Recover if self.waits.contains(&node) => Parent::Wait,
                // Waits for another page, with its folders: decided there, never brought back
                // here (its folder may go with it there).
                Parent::Recover | Parent::Folder(_) if self.paged.contains(&node) => Parent::Wait,
                Parent::Recover if self.elsewhere(node) => {
                    self.wait_together(vec![node]);
                    Parent::Wait
                }
                Parent::Folder(_) if self.elsewhere(node) || self.elsewhere(parent) => {
                    self.wait_together(vec![node, parent]);
                    Parent::Wait
                }
                decided => decided,
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
                // What waits for a hash stays as it is (`waits`), and what another page plans.
                let stays = |m: &NodeId| self.waits.contains(m) || self.elsewhere(*m);
                let local_moved = local_moved.filter(|m| !stays(m));
                let victim = local_moved.unwrap_or_else(|| {
                    *members.iter().filter(|m| !stays(m)).min().unwrap_or(&node)
                });
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
        self.masks.store_id(folder)
    }

    /// Whether `node` holds its name as it is: it waits, or waits for a name in this plan.
    fn holds_name(&self, node: NodeId) -> bool {
        self.waits.contains(&node) || self.waiters.contains(&node)
    }

    /// The fork of `node`, if any, is undone: its copy isn't made.
    pub(super) fn drop_fork(&mut self, node: NodeId) {
        if let Some(copy) = self.forks.remove(&node) {
            self.scope.remove(&copy);
            self.origin.remove(&copy);
            self.merged.remove(copy);
        }
    }

    /// Scoped `node` and what is below it wait for a hash ([`super::waits`]): it would take a
    /// name what waits holds. Left as they are (a conflict copy isn't made); the plan is made
    /// again with them waiting, with their folders ([`reconcile`]). Whether any did.
    fn wait_for_name(&mut self, node: NodeId) -> bool {
        let mut any = false;
        for n in self.merged.subtree(node) {
            if !self.scope.remove(&n) {
                continue;
            }
            any = true;
            // A conflict copy waits as its original: planned again with that waiting, no copy.
            self.waiters.insert(self.forked_into(n).unwrap_or(n));
            self.origin.remove(&n);
            self.adopted.remove(&n);
            self.plan.conflicts.retain(|c| c.node() != n);
            self.drop_fork(n);
            if let Some(original) = self.forked_into(n) {
                self.merged.remove(n);
                self.fork_waits(original);
            } else {
                self.leave_as_is(n);
            }
        }
        any
    }

    pub(super) fn fix_names(&mut self) {
        let parents: BTreeSet<NodeId> = self
            .scope
            .iter()
            .filter_map(|n| self.merged.get(*n).map(|s| s.parent))
            .collect();
        // The drive's names of what waits for a hash elsewhere here (`waits`, and this plan's
        // waiters): taken for it.
        let mut drive_holders: HashMap<(NodeId, String), Vec<NodeId>> = HashMap::new();
        for &n in self.waits.iter().chain(&self.waiters) {
            let Some(r) = self
                .input
                .remote
                .get(n)
                .filter(|_| !self.taken.contains(&n))
            else {
                continue;
            };
            if self.merged.get(n).is_none_or(|h| !h.same_place(r)) {
                let key = (r.parent, name_key(&r.name));
                drive_holders.entry(key).or_default().push(n);
            }
        }
        for parent in parents {
            let mut by_key: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
            for child in self.merged.children(parent) {
                if let Some(s) = self.merged.get(child) {
                    by_key.entry(name_key(&s.name)).or_default().push(child);
                }
            }
            // Judged on part of the folder ([`super::coverage`]).
            let covered_as = self.covered_as(parent);
            let part = self.judged_in_part(covered_as);
            let mut uncovered = part && self.may_clash_unseen(parent, covered_as);
            // Names on the drive of nodes this device doesn't sync (§10) are taken: their
            // holders keep them, and every node here loses.
            // And names this device's held removals keep there (§7): the same.
            let masked = self.masks.names.get(&parent);
            let kept = self.kept.get(&parent);
            let taken = |key: &str| {
                masked.is_some_and(|m| m.contains(key)) || kept.is_some_and(|k| k.contains(key))
            };
            // Each name once, and again when a node left where it is for a name comes to it
            // (a chain of name waits in one folder, judged in one pass).
            let mut queue: std::collections::VecDeque<String> = by_key.keys().cloned().collect();
            while let Some(key) = queue.pop_front() {
                let Some(mut group) = by_key.get(&key).cloned() else {
                    continue;
                };
                // A name what waits for a hash holds, here or on the drive: what would take it
                // waits too, renamed never (§6.3).
                // Taken for its drive name by another than the group's own (a folder that waits
                // whose name differs from the drive's in case only holds no name against
                // itself).
                let on_drive: Vec<NodeId> = drive_holders
                    .get(&(parent, key.clone()))
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|n| !group.contains(n))
                    .collect();
                // What another page plans holds its name too: what of this page would take it
                // waits, planned with it as a page of its own next (`Plan::waits_together`),
                // the clash decided there as in one page.
                let elsewhere: Vec<NodeId> = group
                    .iter()
                    .copied()
                    .filter(|n| self.elsewhere(*n))
                    .collect();
                let holders: Vec<NodeId> = on_drive
                    .iter()
                    .copied()
                    .chain(group.iter().copied().filter(|n| self.holds_name(*n)))
                    .chain(elsewhere.iter().copied())
                    .collect();
                // A clash with what waits for a hash: everything planned in it waits, waiting
                // already or not (a folder that waits made or tagged too: never onto a name
                // taken); what isn't planned stays as it is, and what is in it with it.
                if !holders.is_empty() && (group.len() > 1 || !on_drive.is_empty()) {
                    // What waits for the name: this plan's, or an earlier one's
                    // (`name_takers`).
                    let mut waited = group.iter().any(|n| self.name_takers.contains(n));
                    let planned: Vec<NodeId> = group
                        .into_iter()
                        .filter(|n| self.scope.contains(n))
                        .collect();
                    if !elsewhere.is_empty() && !planned.is_empty() {
                        self.wait_together([&elsewhere[..], &planned[..]].concat());
                    }
                    for n in planned {
                        if !self.wait_for_name(n) {
                            continue;
                        }
                        waited = true;
                        // Left where it is: under another name in this folder, it holds that
                        // (and its drive name, if another).
                        let remote = self
                            .input
                            .remote
                            .get(n)
                            .filter(|_| !self.taken.contains(&n));
                        let mut names = Vec::new();
                        if let Some(left) = self.merged.get(n) {
                            if let Some(r) = remote.filter(|r| !r.same_place(left)) {
                                drive_holders
                                    .entry((r.parent, name_key(&r.name)))
                                    .or_default()
                                    .push(n);
                                if r.parent == parent {
                                    names.push(name_key(&r.name));
                                }
                            }
                            if left.parent == parent {
                                let to = name_key(&left.name);
                                if to != key {
                                    if let Some(g) = by_key.get_mut(&key) {
                                        g.retain(|m| *m != n);
                                    }
                                    by_key.entry(to.clone()).or_default().push(n);
                                    names.push(to);
                                }
                            }
                        }
                        queue.extend(names.into_iter().filter(|k| *k != key));
                    }
                    if waited {
                        self.name_waits.extend(holders);
                    }
                    continue;
                }
                let keep = usize::from(!taken(&key));
                if group.len() <= keep {
                    continue;
                }
                // A rename needs every name of the folder ([`super::coverage`]).
                uncovered |= part;
                // The lowest id keeps the name, except that a deferred node (a file still
                // being written) is never renamed: it wins, and is left alone if it loses;
                // and an orphan moving in, or Recovered files made beside a folder of its
                // name, loses to what is there.
                group.sort_by_key(|n| {
                    let arriving = self.adopted.contains(n) || *n == NodeId::RECOVERED;
                    (!self.deferred.contains(n), arriving, *n)
                });
                for loser in group.into_iter().skip(keep) {
                    // Never what waits for a hash, or for a name (§6.3): planned, it waits.
                    if self.deferred.contains(&loser) || self.holds_name(loser) {
                        if self.scope.contains(&loser) {
                            self.wait_for_name(loser);
                        }
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
