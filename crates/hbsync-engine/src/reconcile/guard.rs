//! Step 5: deletes that trip the mass-delete guard are held for the user (§7): this device's
//! own removals from the drive over the limit, and another device's removal of every synced
//! file.

use super::*;

impl Reconciler<'_> {
    // ---- 5: mass-delete guard ----

    pub(super) fn guard_mass_delete(&mut self) {
        if self.opts.guard.allow {
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
        let yes = &self.opts.confirmed;
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
        let (mut local_files, mut remote_files) = (0usize, 0usize);
        for op in self.plan.ops.iter().filter(|op| !confirmed(op)) {
            match op {
                Op::RemoteDelete { node, .. }
                    if i.remote
                        .get(*node)
                        .is_some_and(|s| s.kind == NodeKind::File) =>
                {
                    remote_files += 1
                }
                // A removal elsewhere: a move out of view (`Forget`) keeps the files live on
                // the drive, so it isn't one.
                Op::LocalTrash { node, .. } => local_files += files_under(&i.local, *node),
                _ => {}
            }
        }
        let synced = self.opts.synced_files;
        let limit = self
            .opts
            .guard
            .min
            .max((synced as f64 * self.opts.guard.fraction) as usize);
        // What this device would remove from the drive (deleted here, or newly ignored, §10
        // rule 1): held over the limit, or every synced file.
        let trips = |n: usize| n > limit || (synced > 0 && n >= synced && n > 1);
        // What another device removed is followed here (into `.hbsync/trash`, and Recently
        // deleted on the drive keeps it), but every synced file at once over the limit: a
        // broken device or a wiped drive, held for the user.
        let every = |n: usize| synced > 0 && n >= synced && n > limit;
        let (gone_locally, gone_remotely) = self.opts.pending_deletes;
        let hold_local = every(local_files) || every(gone_remotely);
        let hold_remote = trips(remote_files) || trips(gone_locally);
        // Once held so, a removal elsewhere stays held until the user answers, whatever the
        // count says since (one file of it back on the drive, a put back half done).
        let wiped = &self.opts.wiped;
        let was_wiped = |node: NodeId| {
            wiped.contains(&node) || i.base.ancestors(node).iter().any(|a| wiped.contains(a))
        };
        if !hold_local && !hold_remote && wiped.is_empty() {
            return;
        }
        let ops = std::mem::take(&mut self.plan.ops);
        for op in ops {
            let held = !confirmed(&op)
                && match op {
                    Op::RemoteDelete { .. } => hold_remote,
                    Op::LocalTrash { node, .. } => hold_local || was_wiped(node),
                    _ => false,
                };
            if held {
                self.plan.held.push(op);
            } else {
                self.plan.ops.push(op);
            }
        }
    }
}
