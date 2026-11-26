//! Step 3: the merged tree made whole again: orphans, cycles, duplicate names (§6).

use std::convert::Infallible;

use super::*;
use hbsync_types::RECOVERED_NAME;

use crate::tree::{NUMBERED_TRIES, first_free_by, numbered_names};
use crate::walk::End;

/// What becomes of a node whose folder isn't in the merged tree as a folder
/// ([`Reconciler::fix_orphans`]).
enum Parent {
    /// Its folder comes back.
    Folder(NodeState),
    /// It moves into Recovered files.
    Recover,
    /// It waits for more information.
    Wait,
    /// Read-only (§13), it has no place here: nothing goes to the drive, Recovered files
    /// neither, so what is here of it goes (moved out where it changed here, or never synced),
    /// as its folder's would. Once the drive gives it a place, it comes back.
    LetGo,
    /// It isn't this device's: the place it would take is one this device doesn't sync (§10),
    /// or it is in a folder in Recently deleted at a place it filters. Left as it is, for a
    /// device that syncs that place: brought in, it would read as an item this device stopped
    /// syncing, and leave the drive; or another device's folder would stay deleted without it.
    Leave,
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
        // Out of the plan as it is here, edit and all: it keeps its folders
        // ([`Reconciler::keep_for_what_is_out`]).
        if !self.masking.read_only && self.input.local.contains(node) {
            self.out_here.insert(node);
        }
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
    /// a live child keeps does). The main folder adopts orphans (§12); a role folder's meets
    /// one only where it holds it (what is in a folder it filters is left, Parent::Leave), or
    /// in a file, and puts that at its top, as does the main folder where Recovered files isn't
    /// synced here (filtered) or is the orphan itself.
    fn recovered_files(&mut self, node: NodeId) -> NodeId {
        let place = self.recovery_place(node);
        if place == NodeId::RECOVERED
            && !self.merged.exists(NodeId::RECOVERED)
            && self.known_state(NodeId::RECOVERED).is_none()
        {
            // Made here.
            self.retarget(
                NodeId::RECOVERED,
                NodeState::folder(NodeId::ROOT, RECOVERED_NAME),
            );
        }
        place
    }

    /// Where the orphan `node` goes ([`Self::recovered_files`]), nothing made yet.
    fn recovery_place(&self, node: NodeId) -> NodeId {
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
        // Made here: under the name it gets, if this device syncs that.
        if !self.merged.exists(NodeId::RECOVERED)
            && self.known_state(NodeId::RECOVERED).is_none()
            && !self
                .masking
                .filter
                .in_folder("", RECOVERED_NAME, true)
                .is_sync()
        {
            return NodeId::ROOT;
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

    /// The names down to where the orphan `node` would go ([`Self::recovered_files`]), if this
    /// device syncs that place and `node` there (§10): an orphan isn't connected to the top, so
    /// the filter never masked it. `None` also where Recovered files' place isn't known yet (a
    /// folder of it not in this plan): judged once it is. Only for an orphan this device holds
    /// nothing of: one it holds is synced here, and goes as before.
    fn recovered_path(&self, node: NodeId) -> Option<Vec<String>> {
        let state = self.merged.get(node)?;
        let mut path: Vec<String> = Vec::new();
        if self.recovery_place(node) == NodeId::RECOVERED {
            let at = |n: NodeId| -> Option<Vec<String>> {
                Some(
                    self.merged
                        .path(n)?
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                )
            };
            path = match (at(NodeId::RECOVERED), self.known_state(NodeId::RECOVERED)) {
                (Some(path), _) => path,
                // Coming back (from Recently deleted, say): where its folder is.
                (None, Some(known)) => {
                    let mut path = at(known.parent)?;
                    path.push(known.name);
                    path
                }
                // Made here, at the top.
                (None, None) => vec![RECOVERED_NAME.to_owned()],
            };
        }
        path.push(state.name.clone());
        let is_dir = state.kind == NodeKind::Folder;
        let filter = self.masking.filter;
        filter
            .names(path.iter().map(String::as_str), is_dir)
            .is_sync()
            .then_some(path)
    }

    /// Whether `node`'s folder is one this device doesn't sync (masked, §10): a folder in
    /// Recently deleted at a place it filters, which the merge never holds.
    fn in_masked(&self, node: NodeId) -> bool {
        self.merged.get(node).is_some_and(|state| {
            self.input.removed.contains_key(&state.parent)
                && self.masks.masked.contains(&state.parent)
        })
    }

    /// Whether this device syncs the place the folder `state` would come back to, through the
    /// folders that would come back with it (§10). It doesn't where what it filters changed
    /// since the folder synced here (its rules, the built-in ones): deleted for good on the
    /// drive meanwhile, the folder is known from before only, which no mask covers, and the
    /// item kept there is this device's own. A place not known down from the top counts as
    /// synced (a folder above it masked, too): its folder waits there, as it did.
    fn syncs_place(&self, state: &NodeState) -> bool {
        let Ok(up) = crate::walk::up::<_, Infallible>(state.parent, |at| {
            let known = self.merged.get(at).cloned();
            let known = known.or_else(|| self.known_state(at));
            Ok(known.map(|known| (known.name, known.parent)))
        });
        if up.end != End::Top {
            return true;
        }
        let names = up.items.iter().rev().chain([&state.name]);
        let filter = self.masking.filter;
        filter.names(names.map(String::as_str), true).is_sync()
    }

    /// Whether the drive has `node` live, and no folder's removal took it along.
    fn live_on_drive(&self, node: NodeId) -> bool {
        self.input.remote.contains(node) && !self.taken.contains(&node)
    }

    /// The folders the drive still has above `node` there, up to the first in the plan, are
    /// not removed by this plan as ones this device stopped syncing (§10, rule 1): `node`,
    /// which it doesn't remove, is live in them. A removal took it along: another device's
    /// file, gone with the folder. Where `node` stays there (`stays`: left to other
    /// devices), so do they, and their bases go (a folder that holds what isn't leaving
    /// isn't leaving: only its base is dropped, rule 2); where it is on its way out
    /// (recovered), their removal waits for the plan after, when it has left.
    fn keep_folders_above(&mut self, node: NodeId, stays: bool) {
        let i = self.input;
        let mut at = i.remote.get(node).map(|state| state.parent);
        let mut seen = HashSet::new();
        while let Some(folder) = at.filter(|f| seen.insert(*f)) {
            if !self.live_on_drive(folder) || self.merged.exists(folder) {
                break;
            }
            if self.scope.remove(&folder) {
                let own = i.base.contains(folder) && !i.local.contains(folder);
                if stays && own && !self.elsewhere(folder) {
                    self.released.insert(folder);
                } else if i.dirty.contains(&folder) {
                    self.deferred.insert(folder);
                }
            }
            at = i.remote.get(folder).map(|state| state.parent);
        }
    }

    /// Whether this device holds anything of `node` or below it here: an item, in view. A
    /// base alone is no hold: its item was deleted here, or is the device's own since a rule
    /// hid it (§10, rule 1: the scanner no longer tells it), and either way nothing here is
    /// the node's. Taken for one, a base from before a rule brought the node's folder back at
    /// the place the device filters since, tried at every sync, for good (#427); and it sent
    /// an orphan where the device doesn't sync it, to leave the drive from there. (Not told
    /// from the base's place: the bases above it go with their folders, in any order.)
    fn holds_any(&self, node: NodeId) -> bool {
        let local = &self.input.local;
        self.merged.subtree(node).iter().any(|n| local.contains(*n))
    }

    /// What is below `node`, connected to the top again at `path` (an orphan recovered, a
    /// folder taken out of a cycle), at a place this device doesn't sync (§10), with what is
    /// below that: out of the plan, left to the devices that sync it, as the filter masks such
    /// a node once its folder is connected again. Brought in, it would read as an item this
    /// device stopped syncing, and leave the drive. Hardly ever any.
    fn leave_unsynced_below(&mut self, node: NodeId, mut path: Vec<String>) {
        let filter = self.masking.filter;
        let depth = path.len();
        let mut stack: Vec<(NodeId, usize)> =
            self.merged.children(node).map(|c| (c, depth)).collect();
        let mut gone = Vec::new();
        while let Some((at, depth)) = stack.pop() {
            let Some(state) = self.merged.get(at) else {
                continue;
            };
            path.truncate(depth);
            path.push(state.name.clone());
            let is_dir = state.kind == NodeKind::Folder;
            if !filter
                .names(path.iter().map(String::as_str), is_dir)
                .is_sync()
            {
                if !self.holds_any(at) {
                    gone.extend(self.merged.subtree(at));
                }
                continue;
            }
            stack.extend(self.merged.children(at).map(|c| (c, depth + 1)));
        }
        for at in gone {
            self.merged.remove(at);
            self.scope.remove(&at);
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
                    *n != folder
                        && self.taken.contains(n)
                        && !self.local_change_to_keep(*n).is_change()
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
        // Of those, the ones that wait for a node another page plans, in the groups they wait
        // in: what is in one waits with it, in its group, so each group is planned as one
        // page of its own (§6.5), whatever is below its nodes.
        let mut groups: Vec<Vec<NodeId>> = Vec::new();
        let mut group_of: HashMap<NodeId, usize> = HashMap::new();
        // Nodes left to the devices that sync where they would go; their children too.
        let mut left: HashSet<NodeId> = HashSet::new();
        // Nodes let go of (read-only, §13); their children too.
        let mut let_go: HashSet<NodeId> = HashSet::new();
        // What a check that a node may be let go of passed: its subtree here.
        let mut cleared: HashSet<NodeId> = HashSet::new();
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
                None if left.contains(&parent) => Parent::Leave,
                None if let_go.contains(&parent) => Parent::LetGo,
                None if withdrawn.contains(&parent) => {
                    // Its parent waits for a node another page plans: so does it, in the
                    // parent's group. (Named there where it is to be planned, in this page
                    // or another: context that isn't dirty is only passed through, for what
                    // is below it.)
                    if let Some(&group) = group_of.get(&parent) {
                        if self.input.dirty.contains(&node) || self.elsewhere(node) {
                            groups[group].push(node);
                        }
                        group_of.insert(node, group);
                    }
                    Parent::Wait
                }
                None => match self.known_state(parent) {
                    // Its folder comes back for it (§6.3), but not at a place this device
                    // filters since the folder synced here (§10), for a node it holds nothing
                    // of ([`Self::holds_any`]: an item, never a base alone): the folder would
                    // take over the item this device keeps to itself, and the node would come
                    // down into it and leave the drive with it. This device forgets that
                    // folder, and the node is an orphan as below.
                    Some(state)
                        if state.kind == NodeKind::Folder
                            && (self.holds_any(node) || self.syncs_place(&state)) =>
                    {
                        Parent::Folder(state)
                    }
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
                    group_of.insert(node, groups.len());
                    groups.push(vec![node]);
                    Parent::Wait
                }
                Parent::Folder(_) if self.elsewhere(node) || self.elsewhere(parent) => {
                    group_of.insert(node, groups.len());
                    groups.push(vec![node, parent]);
                    Parent::Wait
                }
                // Read-only (§13): no folder made again, nothing moved into Recovered files, as
                // either is a write to the drive. Its item here goes with what is below it, once
                // nothing of that is planned elsewhere or waits (a new item there moves out
                // first, in its own plan).
                Parent::Recover | Parent::Folder(_) | Parent::LetGo if self.masking.read_only => {
                    match self.may_let_go(node, &mut cleared) {
                        true => Parent::LetGo,
                        false => Parent::Wait,
                    }
                }
                // Left to the devices that sync where it would go, or where it is: in a folder
                // in Recently deleted at a place this device filters (masked, §10), it is
                // another device's to bring back with its folder, no orphan (§6.3).
                Parent::Recover
                    if !self.holds_any(node)
                        && (self.in_masked(node) || self.recovered_path(node).is_none()) =>
                {
                    Parent::Leave
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
                    self.brought_back.insert(parent);
                    queue.push(parent);
                }
                Parent::Recover => {
                    if let Some(mut state) = self.merged.get(node).cloned() {
                        self.keep_folders_above(node, false);
                        let path = self.recovered_path(node);
                        state.parent = self.recovered_files(node);
                        self.retarget(node, state);
                        if let Some(path) = path {
                            self.leave_unsynced_below(node, path);
                        }
                        self.adopted.insert(node);
                        self.plan.conflicts.push(Conflict::Orphan { node });
                        // Its new folder may have to come back first.
                        queue.push(node);
                    }
                }
                Parent::Leave => {
                    // Out of this plan, and nothing to retry: its dirty mark goes. Its children
                    // follow it (Recovered files left, what moved into it too).
                    queue.extend(self.merged.children(node).collect::<Vec<_>>());
                    self.merged.remove(node);
                    self.scope.remove(&node);
                    left.insert(node);
                    self.unadopt(node);
                    self.keep_folders_above(node, true);
                    // A base of it with no item here goes (nothing here is the node's, and
                    // no base outlives what it tells of): as that of a node the drive still
                    // has at a place this device filters since (§10, rule 2). Not one another
                    // page plans: its own page decides it.
                    let i = self.input;
                    if i.base.contains(node) && !i.local.contains(node) && !self.elsewhere(node) {
                        self.released.insert(node);
                    }
                }
                Parent::LetGo => {
                    // Out of the merged tree, in the plan: its item here goes, and so do its
                    // children's, which are let go of in turn.
                    queue.extend(self.merged.children(node).collect::<Vec<_>>());
                    self.merged.remove(node);
                    self.scope.insert(node);
                    let_go.insert(node);
                    self.follows.insert(node);
                    // Only what was synced here is deleted, as the drive had it: what changed
                    // here that the drive doesn't hold, or was never synced, moves out.
                    if self.input.local.contains(node)
                        && (!self.input.base.contains(node) || self.loses_here(node))
                    {
                        self.evicted.insert(node);
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
                    self.unadopt(node);
                    if self.input.dirty.contains(&node) {
                        self.deferred.insert(node);
                    }
                    // A conflict copy taken out: its fork waits, before the names are judged.
                    if let Some(original) = self.forked_into(node) {
                        self.fork_waits(original);
                    }
                    // And one edited on both sides that waits itself makes no copy meanwhile:
                    // it stays as it is here, edit and all, judged again once its folder
                    // reads, and says so, as one whose copy waits. (Its copy stayed in the
                    // plan and was sent, with no item here to send, at every sync: found by
                    // the simulator.)
                    // (Instead of its other conflicts, as where its copy is met first:
                    // `fork_waits`.)
                    if self.forks.contains_key(&node) {
                        self.unfork(node);
                        self.plan.conflicts.retain(|c| c.node() != node);
                        self.plan.conflicts.push(Conflict::CopyWaits { node });
                    }
                }
            }
        }
        // What waits and changed here keeps the folders it is in here: one gone there would be
        // deleted here with it, change and all (it is out of the plan, so nothing sends the
        // change or keeps the folder alive for it, as an edit in the plan does). An identity's
        // own drive only: read-only, a change here moves out (`emit`). Here for the first
        // names judged; again after them, with what they leave out
        // ([`Reconciler::keep_for_what_is_out`]).
        if !self.masking.read_only {
            let here = |n: &&NodeId| self.input.local.contains(**n);
            self.out_here.extend(withdrawn.iter().filter(here));
            self.keep_for_what_is_out();
        }
        // What waits is where it is here, on its name: taken here till the wait ends, or
        // what the drive has under that name now (another made there since, one renamed to
        // it) was planned onto it, refused by the disk at every sync (found by the
        // simulator). Whatever changed here: out of the plan, it doesn't move out either.
        // Not a parked item: that isn't on its name on disk. Nor what is below a folder that
        // waits and isn't kept: no name is judged in that one.
        for &node in &withdrawn {
            let judged = (self.input.local.get(node)).is_some_and(|here| {
                !withdrawn.contains(&here.parent) || self.merged.contains(here.parent)
            });
            if judged && !self.input.parked.contains(&node) {
                self.holds_waiting(node);
            }
        }
        // Each group once, with all that waits in it: one page of its own, not one a node.
        for group in groups {
            self.wait_together(group);
        }
    }

    /// What is here, out of the plan ([`Reconciler::out_here`]) and changed here keeps the
    /// folders it is in here, on an identity's own drive: one the plan deletes here would
    /// take the change along. After each judging of names, till none more is kept: what a
    /// name leaves out is put back where it is here, and a folder kept holds its name
    /// against what the plan had under it.
    pub(super) fn keep_for_what_is_out(&mut self) {
        if self.masking.read_only {
            return;
        }
        let out: Vec<NodeId> = self.out_here.iter().copied().collect();
        for node in out {
            // Made here counts as changed, whatever the drive has of it: an item with no
            // base isn't one this device may unlink (§7), the bytes the same there or not
            // (sent up under a numbered name, say, its rename here still to run).
            if !self.input.base.contains(node) || self.loses_here(node) {
                self.keep_folders_here(node);
            }
        }
    }

    /// The folders `node` is in here that the plan doesn't have are left as they are: out of
    /// the plan, in the merged tree where they are here, each holding its name
    /// ([`Reconciler::held_waiting`]) in a folder judged again for it. Not deleted here, not
    /// made again there, planned again with `node`. On up through those the plan has where
    /// they are here (one put back there for a name may be in a folder the plan doesn't
    /// have), to the first it moves: that one takes what is in it along.
    fn keep_folders_here(&mut self, node: NodeId) {
        let i = self.input;
        for folder in i.local.ancestors_iter(node) {
            let Some(here) = i.local.get(folder) else {
                break;
            };
            match self.merged.get(folder) {
                Some(to) if to.parent == here.parent => continue,
                Some(_) => break,
                None => {}
            }
            self.scope.remove(&folder);
            self.origin.remove(&folder);
            self.leave_as_is(folder);
            self.holds_waiting(folder);
            self.rejudge.insert(here.parent);
        }
    }

    /// `node` holds the name it has here while it waits for its folder there, or is kept for
    /// what does ([`Reconciler::held_waiting`]).
    fn holds_waiting(&mut self, node: NodeId) {
        if let Some(here) = self.input.local.get(node) {
            let names = self.held_waiting.entry(here.parent).or_default();
            names.insert(name_key(&here.name));
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
                if self.cycle_across_pages(&members) {
                    fixed = true;
                    continue;
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
                // Read-only (§13), it follows the drive from here on, its tags too: what the
                // merge took of this device's (a node that may be changed) isn't sent.
                if self.masking.read_only {
                    if let Some(r) = self.remote_state(victim) {
                        state.tags = r.tags.clone();
                    }
                    self.follows.insert(victim);
                }
                self.retarget(victim, state);
                self.plan.conflicts.push(Conflict::Cycle { node: victim });
                // A cycle on the drive itself (another writer's): no mask judged what is in
                // it, as none does what the drive doesn't connect to the top.
                let path = self.merged.path(victim);
                let path = path.map(|p| p.into_iter().map(str::to_owned).collect());
                if let Some(path) = path {
                    self.leave_unsynced_below(victim, path);
                }
                fixed = true;
            }
        }
        fixed
    }

    /// A cycle with a node another page plans (`Reconciler::elsewhere`, §6.5): that page
    /// decides that node, so no move here is undone for it. What of this page is in the cycle
    /// waits to be planned with it, as a page of its own next (`Plan::waits_together`), left
    /// where the drive has it meanwhile (`placed`): planned together, the moves go as one page
    /// plans them (a folder and the folder in it swapped here: both). Planned apart, the
    /// cycle's repair undid the user's move. Only while a member is outside this page, as for
    /// any wait across pages: in one page the cycle is this plan's to repair. Whether it did.
    fn cycle_across_pages(&mut self, members: &[NodeId]) -> bool {
        if !members.iter().any(|m| self.elsewhere(*m)) {
            return false;
        }
        let i = self.input;
        let mut left = false;
        let here: Vec<NodeId> = members
            .iter()
            .copied()
            .filter(|m| !self.elsewhere(*m))
            .collect();
        for m in here {
            let Some(at) = i.remote.get(m).or(i.base.get(m)) else {
                continue;
            };
            let Some(mut state) = self.merged.get(m).cloned() else {
                continue;
            };
            if state.same_place(at) || self.merged.would_cycle(m, at.parent) {
                continue;
            }
            state.parent = at.parent;
            state.name = at.name.clone();
            self.merged.insert(m, state);
            if self.scope.remove(&m) {
                self.origin.remove(&m);
                self.plan.conflicts.retain(|c| c.node() != m);
                self.drop_fork(m);
                self.placed.insert(m);
            }
            if i.dirty.contains(&m) {
                self.deferred.insert(m);
            }
            left = true;
        }
        if left {
            self.wait_together(members.to_vec());
        }
        left
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

    /// `node` leaves the plan as it is here: its fork, if any, is undone with what it told
    /// (edited on both sides: found again once the node is planned).
    fn unfork(&mut self, node: NodeId) {
        let Some(copy) = self.forks.get(&node).copied() else {
            return;
        };
        // What was told of the copy goes too (its name taken where it was to be made: it
        // isn't renamed, it isn't made).
        let copied = |c: &Conflict| matches!(c, Conflict::EditEdit { node: n, .. } if *n == node);
        self.plan
            .conflicts
            .retain(|c| !copied(c) && c.node() != copy);
        let mut folder = self.merged.get(copy).map(|s| s.parent);
        self.drop_fork(node);
        // A folder gone there that was brought back for the copy alone goes again, with those
        // above it brought back for it: not made again there for a copy that isn't made.
        // (Planned as before it was brought back; what else keeps it here does so after.)
        while let Some(f) = folder.filter(|f| self.brought_back.contains(f)) {
            if self.merged.children(f).next().is_some() {
                break;
            }
            self.brought_back.remove(&f);
            folder = self.merged.remove(f).map(|s| s.parent);
        }
    }

    /// The fork of `node`, if any, is undone: its copy isn't made.
    pub(super) fn drop_fork(&mut self, node: NodeId) {
        if let Some(copy) = self.forks.remove(&node) {
            self.scope.remove(&copy);
            self.origin.remove(&copy);
            self.merged.remove(copy);
        }
    }

    /// An orphan taken out of Recovered files after all: not reported as moved there.
    fn unadopt(&mut self, node: NodeId) {
        if self.adopted.remove(&node) {
            self.plan
                .conflicts
                .retain(|c| *c != Conflict::Orphan { node });
        }
    }

    /// Whether read-only `node`'s item here may go with what is below it ([`Parent::LetGo`]):
    /// none of that waits, here or for another page, nor is planned in another. What passed
    /// once (`cleared`, a subtree here) isn't walked again.
    fn may_let_go(&self, node: NodeId, cleared: &mut HashSet<NodeId>) -> bool {
        if cleared.contains(&node) {
            return true;
        }
        let below = self.input.local.subtree(node);
        let free = below
            .iter()
            .all(|n| !self.waits.contains(n) && !self.elsewhere(*n));
        if free {
            cleared.extend(below);
        }
        free
    }

    /// `node`, which lost its name where none is renamed on the drive for it (in a read-only
    /// folder, §13; to what waits here, §8a), and what is below it stay as they are: out of
    /// the plan (dirty still), in the merged tree where they are here (the names they hold
    /// here stay held: nothing is planned onto them), else out of it. Told once, for `node`.
    /// False where it was left out already.
    fn leave_out(&mut self, node: NodeId) -> bool {
        if !self.left.insert(node) {
            return false;
        }
        for n in self.merged.subtree(node) {
            // Its conflict copy isn't made while it is left out (edited on both sides: judged
            // again once the name is free): half of one, sent with no item here to send, is
            // planned at every sync (a member's of an item it may change, too).
            self.unfork(n);
            // A conflict copy left out with the folder it is to be made in, its original
            // elsewhere (the drive moved that one out): the original waits with it.
            if let Some(original) = self.forked_into(n) {
                self.fork_waits(original);
            }
            // On an identity's own drive nothing moves out: where it is put back here it may
            // keep its folders ([`Reconciler::keep_for_what_is_out`]).
            if !self.masking.read_only && self.input.local.contains(n) {
                self.out_here.insert(n);
            }
            // What changed here still moves out (its name aside): a folder deleted here takes
            // it along otherwise.
            if !self.evicted.contains(&n) {
                self.scope.remove(&n);
                self.origin.remove(&n);
            }
            // Where it is here, it holds that name: nothing else is planned onto it. Put
            // back in another place than the plan had it (a file of the folder at the top
            // here, from when it was shared without the folder), it holds a name the folder
            // it is in here may not have been judged with (the one being judged, or one
            // judged before it): that folder is judged again, or a newcomer of the name is
            // downloaded onto it, refused by the disk at every sync. (`node` itself is the
            // caller's, which knows the folder being judged.)
            match self.input.local.get(n) {
                Some(here) => {
                    let moved = self.merged.get(n).is_none_or(|was| !was.same_place(here));
                    if n != node && moved && !self.evicted.contains(&n) {
                        self.rejudge.insert(here.parent);
                    }
                    self.merged.insert(n, here.clone())
                }
                None => self.merged.remove(n),
            };
            if self.input.dirty.contains(&n) && !self.evicted.contains(&n) {
                self.deferred.insert(n);
            }
        }
        self.plan.conflicts.push(Conflict::NameTaken { node });
        true
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

    /// On another identity's drive (§13) a name is taken there only where it is free: one
    /// another node holds, on the drive or in the plan, or one this device leaves out there
    /// or keeps (§10, §7), is the other's. A rename or a move here onto one is undone (the
    /// item here goes back to the drive's place and name; what else changed here of the
    /// node still goes up); what this plan adds there (new here, a conflict copy) goes under
    /// the first numbered name that is free, as a clash's loser does on an identity's own
    /// drive. A clash of names on the drive is its owner's to mend, so none is made there.
    pub(super) fn keep_off_taken_names(&mut self) {
        if !self.writes_there() {
            return;
        }
        let i = self.input;
        let scope: Vec<NodeId> = self.scope.iter().copied().collect();
        for n in scope {
            // Only what takes a name there: a node whose place differs from the drive's, or
            // one added.
            let (r, added) = (self.remote_state(n), self.adds.contains(&n));
            if self.one_way(n) || (r.is_none() && !added) {
                continue;
            }
            let Some(to) = self
                .merged
                .get(n)
                .filter(|to| r.is_none_or(|r| !to.same_place(r)))
            else {
                continue;
            };
            let to = to.clone();
            let taken = |name: &str| {
                let key = name_key(name);
                let held =
                    |names: Option<&HashSet<String>>| names.is_some_and(|k| k.contains(&key));
                self.merged.child_named(to.parent, name, Some(n)).is_some()
                    || i.remote.child_named(to.parent, name, Some(n)).is_some()
                    || held(self.masks.names.get(&to.parent))
                    || held(self.kept.get(&to.parent))
            };
            match r {
                Some(r) => {
                    // (Its own name spelled otherwise, where it is, is no other's.)
                    let its_own = to.parent == r.parent
                        && (to.name == r.name || name_key(&to.name) == name_key(&r.name));
                    if its_own || !taken(&to.name) {
                        continue;
                    }
                    let back = NodeState {
                        parent: r.parent,
                        name: r.name.clone(),
                        ..to
                    };
                    self.retarget(n, back);
                }
                // The first free one this device syncs, as `fix_names` picks one (§10: one
                // it ignores would leave the drive at the next scan); else the first free
                // one. (Judged on what the view holds of the folder: `fix_names`, next,
                // asks for the whole folder where the new name may clash with what it
                // doesn't hold, `may_clash_unseen`.)
                None if taken(&to.name) => {
                    let is_dir = to.kind == NodeKind::Folder;
                    let dir: Option<Vec<String>> = (self.merged.path(to.parent))
                        .map(|p| p.into_iter().map(str::to_owned).collect());
                    let syncs = |c: &String| {
                        dir.as_ref().is_none_or(|dir| {
                            let names = dir.iter().map(String::as_str).chain([c.as_str()]);
                            self.masking.filter.names(names, is_dir).is_sync()
                        })
                    };
                    let free = |c: &String| !taken(c);
                    let name = first_free_by(numbered_names(&to.name), free, syncs)
                        .or_else(|| {
                            (NUMBERED_TRIES + 1..)
                                .map(|k| numbered_conflict_name(&to.name, k))
                                .find(|c| free(c))
                        })
                        .unwrap_or_else(|| to.name.clone());
                    self.retarget(n, NodeState { name, ..to });
                    self.plan.conflicts.push(Conflict::NameClash { node: n });
                }
                None => {}
            }
        }
    }

    /// Only the folders in `only`, where given.
    pub(super) fn fix_names(&mut self, only: Option<&BTreeSet<NodeId>>) {
        let parents: BTreeSet<NodeId> = self
            .scope
            .iter()
            .filter_map(|n| self.merged.get(*n).map(|s| s.parent))
            .filter(|p| only.is_none_or(|o| o.contains(p)))
            .collect();
        // The drive's names of what waits for a hash elsewhere here (`waits`, and this plan's
        // waiters): taken for it.
        let mut drive_holders: HashMap<(NodeId, String), Vec<NodeId>> = HashMap::new();
        for &n in self.waits.iter().chain(&self.waiters) {
            let Some(r) = self.remote_state(n) else {
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
            // And names what waits for its folder there holds here: its to keep till the
            // wait ends. Nothing is renamed there for one (the clash is this device's alone:
            // the drive has what waits elsewhere): judged as a read-only folder's names are,
            // what is on the name on disk keeping it and anything else left out here.
            let waiting = self.held_waiting.get(&parent).cloned();
            let held = |key: &str| waiting.as_ref().is_some_and(|w| w.contains(key));
            let taken = |key: &str| {
                masked.is_some_and(|m| m.contains(key))
                    || kept.is_some_and(|k| k.contains(key))
                    || held(key)
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
                        let remote = self.remote_state(n);
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
                // Alone on a name taken here: what another page plans is its page's (§6.5).
                group.retain(|n| !self.elsewhere(*n));
                // Read-only (§13), what is synced here under the name keeps it (or holds it
                // here): a loser can't be renamed on the drive. A name this device's masks hold
                // is kept by such an item all the same (none of theirs is here with it); one
                // held by them alone leaves every newcomer out (one ignored here may be on it).
                // What moves out holds no name on disk: it goes first.
                let at_name = |s: &NodeState| s.parent == parent && name_key(&s.name) == key;
                let one_way = self.masking.read_only || held(&key);
                // Nor does a parked item: its name is free on disk, or another item's.
                let on_disk = |n: &NodeId| {
                    one_way
                        && !self.evicted.contains(n)
                        && !self.input.parked.contains(n)
                        && self.input.local.get(*n).is_some_and(at_name)
                };
                // (One that moves out holds the name where its drive version comes back to it.)
                // (Not where what waits holds the name: that one is on it on disk, and one
                // synced under the name before, its item gone, doesn't come back to it.)
                let here = |n: &NodeId| {
                    on_disk(n)
                        || (self.masking.read_only
                            && !held(&key)
                            && self.input.base.get(*n).is_some_and(at_name)
                            && (!self.evicted.contains(n)
                                || self.remote_state(*n).is_some_and(at_name)))
                };
                let keep = usize::from(!taken(&key) || group.iter().any(here));
                if group.len() <= keep {
                    continue;
                }
                // A rename needs every name of the folder ([`super::coverage`]); read-only, none
                // is renamed here (what is added there under a numbered name was renamed
                // before, `keep_off_taken_names`, and is judged with the names the view holds
                // above: `may_clash_unseen`).
                uncovered |= part && !self.masking.read_only;
                // The lowest id keeps the name, except that a deferred node (a file still
                // being written) is never renamed: it wins, and is left alone if it loses;
                // and an orphan moving in, or Recovered files or Desktops made beside a folder
                // of its name, loses to what is there.
                // (Read-only, what is synced here comes first, what is on the name on disk
                // before what was synced there and isn't now: a new item still being written
                // moves out once it is read, and never takes a name.)
                // (Read-only, with none of them on the name on disk and another item of the
                // folder there that is: the one that item moves into, its folder on the drive
                // now, goes first. It is made, the item moved into it, and the name is its.
                // Any other would wait for the name that item holds, which waits for its
                // folder, which waited for the name: for good.)
                let makes_way = (self.masking.read_only)
                    .then(|| self.input.local.children(parent).find(|c| on_disk(c)))
                    .flatten()
                    .filter(|holder| !group.contains(holder))
                    .and_then(|holder| {
                        let mut into = self.merged.ancestors_iter(holder);
                        into.find(|folder| group.contains(folder))
                    });
                group.sort_by_cached_key(|n| {
                    let arriving = self.adopted.contains(n) || n.is_fixed_folder();
                    let leaving = self.evicted.contains(n);
                    let made_way_for = makes_way == Some(*n);
                    let first = (
                        !(here(n) || made_way_for),
                        !(on_disk(n) || made_way_for),
                        leaving,
                    );
                    (first, !self.deferred.contains(n), arriving, *n)
                });
                // The folder's path here, for the filter (`None`: not connected), read once
                // a loser is renamed.
                let folder_path: std::cell::OnceCell<Option<Vec<String>>> =
                    std::cell::OnceCell::new();
                for loser in group.into_iter().skip(keep) {
                    // Never what waits for a hash, or for a name (§6.3): planned, it waits.
                    if self.deferred.contains(&loser) || self.holds_name(loser) {
                        if self.scope.contains(&loser) {
                            self.wait_for_name(loser);
                        }
                        continue;
                    }
                    // Left out, where nothing is renamed there: read-only, and what the drive
                    // has under a name what waits holds here. (What is only here under such a
                    // name, a new item parked for it, is no newcomer of the drive's: it takes
                    // a numbered name, as any clash's loser on an identity's own drive.)
                    if self.masking.read_only || (one_way && self.remote_state(loser).is_some()) {
                        // Once: held in another folder here, that one is judged again with it.
                        if !self.leave_out(loser) {
                            continue;
                        }
                        if let Some(h) = self.input.local.get(loser).filter(|h| h.parent != parent)
                        {
                            self.rejudge.insert(h.parent);
                        }
                        // Where it is here, in this folder, it holds that name: judged with what
                        // would take it, as one left where it is for a name. (One that moves
                        // out holds none.)
                        let stays = (self.input.local.get(loser))
                            .filter(|h| h.parent == parent && !self.evicted.contains(&loser));
                        if let Some(to) = stays.map(|h| name_key(&h.name)).filter(|to| *to != key) {
                            by_key.entry(to.clone()).or_default().push(loser);
                            queue.push_back(to);
                        }
                        continue;
                    }
                    let Some(mut state) = self.merged.get(loser).cloned() else {
                        continue;
                    };
                    // The first free one this device syncs (§10: one it ignores would leave the
                    // drive at the next scan), as the executor picks one ([`first_free`]); else
                    // the first free one, unbounded: some numbered name is always free.
                    let free = |c: &String| {
                        self.merged.child_named(parent, c, Some(loser)).is_none()
                            && !taken(&name_key(c))
                    };
                    let is_dir = state.kind == NodeKind::Folder;
                    let syncs = |c: &String| {
                        let dir = folder_path.get_or_init(|| {
                            let path = self.merged.path(parent)?;
                            Some(path.into_iter().map(str::to_owned).collect())
                        });
                        dir.as_ref().is_none_or(|dir| {
                            let names = dir.iter().map(String::as_str).chain([c.as_str()]);
                            self.masking.filter.names(names, is_dir).is_sync()
                        })
                    };
                    let found = first_free_by(numbered_names(&state.name), free, syncs);
                    let Some(new_name) = found.or_else(|| {
                        (NUMBERED_TRIES + 1..)
                            .map(|n| numbered_conflict_name(&state.name, n))
                            .find(|c| free(c))
                    }) else {
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
