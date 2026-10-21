//! Step 5: deletes that trip the mass-delete guard are held for the user (§7).

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
        // delete they confirmed (planned apart while the folder's waits for a hash, §6.3):
        // only the others are judged.
        let yes = &self.opts.confirmed;
        let confirmed = |op: &Op| {
            let node = op.node();
            yes.contains(&node)
                || (!yes.is_empty() && i.base.ancestors(node).iter().any(|a| yes.contains(a)))
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
                _ => {
                    if let Some(node) = op.trashes() {
                        local_files += files_under(&i.local, node);
                    }
                }
            }
        }
        let synced = self.opts.synced_files;
        let limit = self
            .opts
            .guard
            .min
            .max((synced as f64 * self.opts.guard.fraction) as usize);
        let trips = |n: usize| n > limit || (synced > 0 && n >= synced && n > 1);
        let (gone_locally, gone_remotely) = self.opts.pending_deletes;
        // Trashing locally follows remote deletes; remote deletes follow local ones.
        let hold_local = trips(local_files) || trips(gone_remotely);
        let hold_remote = trips(remote_files) || trips(gone_locally);
        if !hold_local && !hold_remote {
            return;
        }
        let ops = std::mem::take(&mut self.plan.ops);
        for op in ops {
            let held = !confirmed(&op)
                && match op {
                    Op::RemoteDelete { .. } => hold_remote,
                    _ => op.trashes().is_some() && hold_local,
                };
            if held {
                self.plan.held.push(op);
            } else {
                self.plan.ops.push(op);
            }
        }
    }
}
