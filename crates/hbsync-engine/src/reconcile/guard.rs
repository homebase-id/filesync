//! Step 5: deletes that trip the mass-delete guard are held for the user (§7): this device's
//! own removals from the drive over the limit, and another device's removal of every synced
//! file.

use super::*;

/// The drive's names `plan`'s held removals keep there (§7), by folder, where something else
/// here has one of them: none, most of the time. They stay taken while held: what would take
/// one here gets a numbered name ([`Reconciler::fix_names`]).
pub(super) fn kept_names(view: &Input, plan: &Plan) -> HashMap<NodeId, HashSet<String>> {
    let held: HashSet<NodeId> = plan
        .held
        .iter()
        .filter_map(|op| match op {
            Op::RemoteDelete { node, .. } => Some(*node),
            _ => None,
        })
        .collect();
    let mut kept: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for node in &held {
        if let Some(r) = view.remote.get(*node) {
            kept.entry(r.parent).or_default().insert(name_key(&r.name));
        }
    }
    kept.retain(|parent, keys| {
        view.local.children(*parent).any(|c| {
            !held.contains(&c)
                && view
                    .local
                    .get(c)
                    .is_some_and(|l| keys.contains(&name_key(&l.name)))
        })
    });
    kept
}

impl Reconciler<'_> {
    // ---- 5: mass-delete guard ----

    pub(super) fn guard_mass_delete(&mut self) {
        if self.opts.guarding.settings.allow {
            return;
        }
        let i = self.input;
        let files_under = |tree: &Tree, node: NodeId| {
            tree.subtree(node)
                .into_iter()
                .filter(|n| tree.get(*n).is_some_and(|s| s.kind == NodeKind::File))
                .count()
        };
        // Deletes the user confirmed are theirs, and so is each of what was in a folder whose
        // delete they confirmed and that waits for a file in it not hashed yet (planned apart
        // meanwhile, §6.3): only the others are judged.
        let yes = &self.opts.guarding.confirmed;
        let waiting: HashSet<NodeId> = self
            .plan
            .delete_waits
            .iter()
            .copied()
            .filter(|n| yes.contains(n))
            .collect();
        let confirmed = |op: &Op| {
            let node = op.node();
            yes.contains(&node)
                || (!waiting.is_empty()
                    && i.base.ancestors(node).iter().any(|a| waiting.contains(a)))
        };
        // The files each delete takes: a file removed from the drive, what a delete takes here.
        let takes = |op: &Op| match op {
            Op::RemoteDelete { node, .. }
                if i.remote
                    .get(*node)
                    .is_some_and(|s| s.kind == NodeKind::File) =>
            {
                Some((1, 0))
            }
            // A removal elsewhere: a move out of view (`Forget`) keeps the files live on
            // the drive, so it isn't one.
            Op::LocalDelete { node, .. } => Some((0, files_under(&i.local, *node))),
            _ => None,
        };
        // With the held deletes out of the dirty set: counted as if planned with this one.
        let (mut remote_files, mut local_files) = self.opts.guarding.held;
        for (here, there) in self
            .plan
            .ops
            .iter()
            .filter(|op| !confirmed(op))
            .filter_map(takes)
        {
            remote_files += here;
            local_files += there;
        }
        let synced = self.opts.guarding.synced;
        let guard = self.opts.guarding.settings;
        let trips = |n: usize| guard.trips(n, synced);
        let every = |n: usize| guard.every(n, synced);
        let (gone_locally, gone_remotely) = self.opts.guarding.pending;
        let hold_local = every(local_files) || every(gone_remotely);
        let hold_remote = trips(remote_files) || trips(gone_locally);
        // Once held so, a removal elsewhere stays held until the user answers, whatever the
        // count says since (one file of it back on the drive, a put back half done).
        let wiped = &self.opts.guarding.wiped;
        let was_wiped = |node: NodeId| {
            wiped.contains(&node) || i.base.ancestors(node).iter().any(|a| wiped.contains(a))
        };
        if !hold_local && !hold_remote && wiped.is_empty() {
            return;
        }
        let ops = std::mem::take(&mut self.plan.ops);
        for op in ops {
            let held = !confirmed(&op)
                && match &op {
                    Op::RemoteDelete { .. } => hold_remote,
                    // Itself, a folder it is in, or anything it takes along.
                    Op::LocalDelete { node, subtree } => {
                        hold_local || was_wiped(*node) || subtree.iter().any(|n| wiped.contains(n))
                    }
                    _ => false,
                };
            if held {
                if let Some(takes) = takes(&op) {
                    self.plan.held_takes.insert(op.node(), takes);
                }
                self.plan.held.push(op);
            } else {
                self.plan.ops.push(op);
            }
        }
    }
}
