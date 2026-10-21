//! Steps 1–2: each node's change on either side against the base, merged into a target (§2, §4, §5, §11).

use super::*;

/// What the remote did to `node` since the base.
fn remote_change(i: &Input, node: NodeId) -> Change {
    match (i.base.get(node), i.remote.get(node)) {
        (None, None) => Change::Absent,
        (None, Some(_)) => Change::Created,
        (Some(_), None) => Change::Deleted,
        (Some(b), Some(r)) => {
            let moved = !b.same_place(r);
            let edited = match (&b.content, &r.content) {
                (Some(bc), Some(rc)) => bc.hash != rc.hash || bc.executable != rc.executable,
                _ => false,
            };
            if moved || edited {
                Change::Changed { moved, edited }
            } else {
                Change::Unchanged
            }
        }
    }
}

/// The remote nodes a folder's removal took along ([`crate::along::taken_along`]): one this
/// device wrote, whose time it doesn't know, was. A folder's delete removes the folder first,
/// then what is in it: until it has, they count as gone. One written after the removal is a
/// change of its own, and keeps the folder alive (§6.3).
pub(super) fn taken_along(i: &Input) -> HashSet<NodeId> {
    if i.removed.is_empty() {
        return HashSet::new();
    }
    crate::along::taken_along(
        i.remote
            .ids()
            .filter_map(|n| Some((n, i.remote.get(n)?.parent))),
        |folder| i.removed.get(&folder).map(|r| r.at),
        |node| i.changed_at.get(&node).copied(),
    )
}

impl Reconciler<'_> {
    // ---- 1–2: classify and merge ----

    pub(super) fn build_context(&mut self) {
        let i = self.input;
        let ids: BTreeSet<NodeId> = i
            .base
            .ids()
            .chain(i.local.ids())
            .chain(i.remote.ids())
            .filter(|n| !i.dirty.contains(n) && (!self.taken.contains(n) || self.held.contains(n)))
            .collect();
        for node in ids {
            if self.held.contains(&node) {
                self.hold(node);
                continue;
            }
            if let Some(state) = i
                .remote
                .get(node)
                .or(i.local.get(node))
                .or(i.base.get(node))
            {
                self.merged.insert(node, state.clone());
            }
        }
    }

    /// Leaves held `node` ([`super::held`]) as it is on both sides: out of the plan's scope
    /// (nothing done to it, dirty still), where it is here (else as the base has it, else the
    /// drive), so that its folder here stays and its name is taken.
    pub(super) fn hold(&mut self, node: NodeId) {
        let i = self.input;
        if i.dirty.contains(&node) {
            self.deferred.insert(node);
        }
        self.holding.insert(node);
        let state = self
            .local_state(node)
            .or_else(|| i.base.get(node).cloned())
            .or_else(|| i.remote.get(node).cloned());
        if let Some(state) = state {
            self.merged.insert(node, state);
        }
    }

    /// Whether held folder `node` may go to `target` ([`super::held`]): it stays where it is
    /// on each side it is on, and isn't gone from one it was on (a delete, or its undoing).
    /// Making it where it isn't yet, and its tags, may go.
    fn stays_put(&self, node: NodeId, target: Option<&NodeState>) -> bool {
        let i = self.input;
        let Some(t) = target else {
            return false;
        };
        let remote = i.remote.get(node).filter(|_| !self.taken.contains(&node));
        let gone = i.base.contains(node) || i.removed.contains_key(&node);
        [self.local_state(node).as_ref(), remote]
            .into_iter()
            .all(|side| side.map_or(!gone, |s| s.same_place(t)))
    }

    pub(super) fn content_state(&self, node: NodeId) -> ContentState {
        self.input
            .base_extra
            .get(&node)
            .map_or(ContentState::Local, |e| e.content_state)
    }

    /// The local state, with what the local side can't express filled in from the base: an
    /// online-only file has no local bytes but is still there.
    pub(super) fn local_state(&self, node: NodeId) -> Option<NodeState> {
        let i = self.input;
        match i.local.get(node) {
            Some(l) => Some(l.clone()),
            None if self.content_state(node) == ContentState::OnlineOnly => {
                i.base.get(node).cloned()
            }
            None => None,
        }
    }

    pub(super) fn local_change(&self, node: NodeId) -> Change {
        let i = self.input;
        let (b, l) = (i.base.get(node), self.local_state(node));
        match (b, &l) {
            (None, None) => Change::Absent,
            (None, Some(_)) => Change::Created,
            (Some(_), None) => Change::Deleted,
            (Some(b), Some(l)) => {
                let moved = !b.same_place(l);
                let edited = match (&b.content, &l.content) {
                    (Some(bc), Some(lc)) => {
                        let base_local = i.base_extra.get(&node).and_then(|e| e.local_hash);
                        let local_now = lc.hash;
                        let bytes_changed = match base_local {
                            Some(h) => h != local_now,
                            // Never had local bytes (online-only): unchanged unless replaced.
                            None => {
                                self.content_state(node) != ContentState::OnlineOnly
                                    && bc.hash != local_now
                            }
                        };
                        bytes_changed || bc.executable != lc.executable
                    }
                    _ => false,
                };
                if moved || edited {
                    Change::Changed { moved, edited }
                } else {
                    Change::Unchanged
                }
            }
        }
    }

    /// The remote change of `node`; one a folder's removal took along is gone from the tree.
    pub(super) fn remote_change(&self, node: NodeId) -> Change {
        match self.taken.contains(&node) {
            true if self.input.base.contains(node) => Change::Deleted,
            true => Change::Absent,
            false => remote_change(self.input, node),
        }
    }

    /// Leaves `node` for a later plan, keeping it in the merged tree where it is now, so that
    /// a new local file still being written keeps its folder alive (§6.3) and its name.
    pub(super) fn defer(&mut self, node: NodeId) {
        self.deferred.insert(node);
        let i = self.input;
        if let Some(state) = i
            .remote
            .get(node)
            .or(i.base.get(node))
            .or(i.local.get(node))
        {
            self.merged.insert(node, state.clone());
        }
    }

    pub(super) fn merge_node(&mut self, node: NodeId) {
        let i = self.input;
        if self.waits_for_hash(node) {
            self.hold(node);
            return;
        }
        let (lc, rc) = (self.local_change(node), self.remote_change(node));
        // One a folder's removal took along isn't in the tree.
        let r = i.remote.get(node).filter(|_| !self.taken.contains(&node));
        let (b, l) = (i.base.get(node), self.local_state(node));
        let (target, origin): (Option<NodeState>, Origin) = match (lc, rc) {
            // Gone from what this device syncs; what must stay here was detached (§10).
            _ if self.masks.masked.contains(&node) => (None, Origin::Base),
            (Change::Absent | Change::Unchanged, Change::Absent | Change::Unchanged) => {
                (r.or(l.as_ref()).or(b).cloned(), Origin::Base)
            }
            (Change::Deleted, Change::Deleted | Change::Unchanged)
            | (Change::Unchanged, Change::Deleted) => (None, Origin::Base),
            (Change::Deleted, Change::Created | Change::Changed { .. }) => {
                self.plan.conflicts.push(Conflict::EditDelete { node });
                (r.cloned(), Origin::Remote)
            }
            (Change::Created | Change::Changed { .. }, Change::Deleted) => {
                self.plan.conflicts.push(Conflict::EditDelete { node });
                (l.clone(), Origin::Local)
            }
            (lc, Change::Absent | Change::Unchanged) if lc.is_change() => (
                l.clone(),
                if content_changed(lc) {
                    Origin::Local
                } else {
                    Origin::Base
                },
            ),
            (Change::Absent | Change::Unchanged, rc) if rc.is_change() => (
                r.cloned(),
                if content_changed(rc) {
                    Origin::Remote
                } else {
                    Origin::Base
                },
            ),
            // Both sides changed (or both have a node the base doesn't know).
            (lc, rc) => {
                let (Some(l), Some(r)) = (l.as_ref(), r) else {
                    self.defer(node);
                    return;
                };
                self.merge_both(node, l, r, lc, rc)
            }
        };
        // A masked live child keeps its folder (§10): never delete what this device can't see.
        let (mut target, origin) = match (target, r) {
            (None, Some(r)) if self.masks.keep_alive.contains(&node) => {
                (Some(r.clone()), Origin::Base)
            }
            (target, _) => (target, origin),
        };
        // Tags merge on their own, whatever else changed (§11); a merge too large for the
        // drive leaves the drive's.
        if let (Some(t), Some(l), Some(r)) = (&mut target, &l, r)
            && !self.masks.masked.contains(&node)
        {
            let none = Tags::default();
            let merged = Tags::merge(b.map_or(&none, |b| &b.tags), &l.tags, &r.tags);
            t.tags = if merged.fits() {
                merged
            } else {
                r.tags.clone()
            };
        }
        if self.held.contains(&node) && !self.stays_put(node, target.as_ref()) {
            self.plan.conflicts.retain(|c| c.node() != node);
            self.drop_fork(node);
            self.hold(node);
            return;
        }
        self.scope.insert(node);
        self.origin.insert(node, origin);
        match target {
            Some(t) => {
                self.merged.insert(node, t);
            }
            None => {
                self.merged.remove(node);
            }
        }
    }

    /// Both sides changed: combine location and content.
    pub(super) fn merge_both(
        &mut self,
        node: NodeId,
        l: &NodeState,
        r: &NodeState,
        lc: Change,
        rc: Change,
    ) -> (Option<NodeState>, Origin) {
        let (l_moved, l_edited) = match lc {
            Change::Changed { moved, edited } => (moved, edited),
            _ => (true, true), // created on both sides: every attribute may differ
        };
        let (r_moved, r_edited) = match rc {
            Change::Changed { moved, edited } => (moved, edited),
            _ => (true, true),
        };
        let mut target = r.clone();
        // A node brought back into view from where this device filters (§10) keeps its local
        // place: the remote one is filtered too, and would take it out of view again.
        if l_moved && (!r_moved || self.masks.brought_back.contains(&node)) {
            target.parent = l.parent;
            target.name = l.name.clone();
        } else if l_moved && r_moved && !l.same_place(r) {
            self.plan.conflicts.push(Conflict::MoveMove { node });
        }
        let mut origin = Origin::Remote;
        if l.kind == NodeKind::File {
            let same_edit = match (&l.content, &r.content) {
                (Some(lc), Some(rc)) => lc.hash == rc.hash && lc.executable == rc.executable,
                _ => false,
            };
            if l_edited && r_edited && !same_edit {
                // Keep both: the remote version keeps the node, the local bytes become a copy.
                let base_version = self.input.base_extra.get(&node).and_then(|e| e.version);
                let copy = fork_id(node, l.content, base_version, &self.opts.computer);
                self.forks.insert(node, copy);
                self.plan.conflicts.push(Conflict::EditEdit { node, copy });
                let copy_state = NodeState {
                    parent: l.parent,
                    name: conflict_copy_name(&l.name, &self.opts.device, &self.opts.date),
                    kind: NodeKind::File,
                    content: l.content,
                    tags: l.tags.clone(),
                };
                self.merged.insert(copy, copy_state);
                self.scope.insert(copy);
                self.origin.insert(copy, Origin::Local);
            } else if l_edited && !r_edited {
                target.content = l.content;
                origin = Origin::Local;
            } else if same_edit || !r_edited {
                origin = Origin::Base;
            }
        }
        (Some(target), origin)
    }
}
