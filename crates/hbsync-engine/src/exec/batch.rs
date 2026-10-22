//! The operations a run collects for the worker pool ([`Batch`]), which runs them in any
//! order: what may join it, and what must wait until it ran.

use super::*;

/// Operations collected for the worker pool, which runs them in any order.
#[derive(Default)]
pub(super) struct Batch {
    pub(super) jobs: Vec<(Job, Prepared)>,
    /// Their nodes.
    nodes: HashSet<NodeId>,
    /// The folders they create (or restore).
    created: HashSet<NodeId>,
    /// The folders on the drive they put a node into or take one from.
    folders: HashSet<NodeId>,
    /// The nodes they move to another folder, and the folders those go into with every folder
    /// above them.
    moved: HashSet<NodeId>,
    under: HashSet<NodeId>,
}

impl Batch {
    /// Adds `job`; `target`: where it moves its node to, if it does
    /// ([`Executor::move_target`]).
    pub(super) fn push(&mut self, job: Job, p: Prepared, target: Option<Vec<NodeId>>) {
        let node = job.node();
        self.nodes.insert(node);
        if let Job::Upload { meta, .. } | Job::Meta { meta, .. } = &job {
            self.folders.insert(meta.parent);
        }
        match &p.ctx {
            Ctx::Upload { from, .. } => self.folders.extend(*from),
            Ctx::Meta { from, after, .. } => {
                self.folders.extend(*from);
                if matches!(after, remote::After::Created) {
                    self.created.insert(node);
                }
            }
            Ctx::Download(_) => {}
        }
        if let Some(target) = target {
            self.moved.insert(node);
            self.under.extend(target);
        }
        self.jobs.push((job, p));
    }

    /// Whether moving `node` to `target` (the folder and every one above it) could make a
    /// cycle on the drive with a move in the batch, landing in either order: one of them
    /// takes along a folder the other goes into. Moves that don't can't, together in any
    /// number: each one's target keeps the folders above it.
    pub(super) fn crosses(&self, node: NodeId, target: &[NodeId]) -> bool {
        self.under.contains(&node) || target.iter().any(|n| self.moved.contains(n))
    }

    /// Whether `op` must wait until the batch ran: it is about a node in it, its server-side
    /// order guard ([`Op::requires`]) waits on one (a folder created in it, or the removal of
    /// the folder a removal is in), or it removes a folder the batch puts a node into or takes
    /// one from.
    pub(super) fn holds_up(&self, op: &Op) -> bool {
        self.nodes.contains(&op.node())
            || matches!(op, Op::RemoteDelete { .. }) && self.folders.contains(&op.node())
            || match op.requires() {
                Some(Requirement::RemoteFolder(folder)) => self.created.contains(&folder),
                Some(Requirement::FolderFirst(folder)) => self.nodes.contains(&folder),
                None => false,
            }
    }
}
