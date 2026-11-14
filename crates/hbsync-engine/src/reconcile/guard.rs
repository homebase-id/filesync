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
        // Deletes the user confirmed are theirs, and so is each of what was in a folder whose
        // delete they confirmed and that waits ([`Plan::delete_waits`]: for a file in it not
        // hashed yet, §6.3, what else is in it planned apart meanwhile; or for another page's
        // plan, §6.5, what is in it waiting with it): only the others are judged.
        let yes = &self.opts.guarding.confirmed;
        let waiting: HashSet<NodeId> = self
            .plan
            .delete_waits
            .iter()
            .copied()
            .filter(|n| yes.contains(n))
            .collect();
        // What a folder's delete here took along, removed from the drive with it (planned
        // with it, [`Reconciler::with_what_it_took_along`]), and a node's own removal with its
        // delete here: one removal elsewhere, answered as one, by the delete's answer too.
        let along = crate::held::together(self.plan.ops.iter());
        let confirmed_node = |node: NodeId| {
            yes.contains(&node)
                || (!waiting.is_empty()
                    && i.base.ancestors(node).iter().any(|a| waiting.contains(a)))
        };
        let confirmed = |op: &Op| {
            confirmed_node(op.node())
                || matches!(op, Op::RemoteDelete { node, .. }
                    if along.get(node).is_some_and(|f| confirmed_node(*f)))
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
            // the drive, so it isn't one. What it takes is what it expects: not what the plan
            // moves out of it first.
            Op::LocalDelete { subtree, .. } => Some((
                0,
                subtree
                    .iter()
                    .filter(|n| i.local.get(**n).is_some_and(|s| s.kind == NodeKind::File))
                    .count(),
            )),
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
        // A removal elsewhere is held with what it took along (§6.3): held as this device's
        // own removals from the drive, they hold the folder's delete here (only while they
        // are: not kept as a wipe is), and a held delete here holds them. Nothing goes here
        // while the rest of it waits on the drive.
        let mut own = HashSet::new();
        let mut wipes = HashSet::new();
        let mut with_along: HashSet<NodeId> = HashSet::new();
        for op in &self.plan.ops {
            match op {
                Op::LocalDelete { node, subtree } => {
                    if !confirmed(op)
                        && (hold_local
                            || was_wiped(*node)
                            || subtree.iter().any(|n| wiped.contains(n)))
                    {
                        wipes.insert(*node);
                    }
                }
                Op::RemoteDelete { node, .. } if hold_remote && !confirmed(op) => {
                    own.insert(*node);
                    with_along.extend(along.get(node));
                }
                _ => {}
            }
        }
        let held_folder = |n: &NodeId| wipes.contains(n) || with_along.contains(n);
        let ops = std::mem::take(&mut self.plan.ops);
        for op in ops {
            let held = match &op {
                Op::LocalDelete { node, .. } => held_folder(node),
                Op::RemoteDelete { node, .. } => {
                    own.contains(node) || along.get(node).is_some_and(held_folder)
                }
                _ => false,
            };
            if held {
                if let Some(takes) = takes(&op) {
                    self.plan.held_takes.insert(op.key(), takes);
                }
                self.plan.held.push(op);
            } else {
                self.plan.ops.push(op);
            }
        }
        self.plan.held_wipes = wipes.into_iter().collect();
    }
}
