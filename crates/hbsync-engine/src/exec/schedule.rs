//! The order of a run's operations and its retry policy: the queue, the attempts each
//! operation has had, which operations others wait for, and the pause after transient remote
//! errors.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use hbsync_state::StateError;
use hbsync_types::NodeId;

use super::{Outcome, Step};
use crate::reconcile::{Op, Requirement};

/// Transient remote errors in a row after which a run gives up (with the pauses between them
/// doubling from `retry_pause`, 0.5 s by default: about 15 s in all).
const UNREACHABLE_AFTER: u32 = 6;

/// What [`Schedule::retry`] made of an operation.
pub(super) enum Retried {
    /// Queued again.
    Queued,
    /// Out of attempts.
    Failed(Op),
    /// Left for the next plan, read from the feed first.
    Replan(Op),
    /// Transient errors kept coming: the run stops, unreachable.
    Unreachable,
}

pub(super) struct Schedule {
    /// What is still to run, with the attempts each has had.
    queue: VecDeque<(Op, u8)>,
    /// The nodes with a `Forget` still to run in this run.
    forgetting: HashSet<NodeId>,
    /// The nodes with a `RemoteDelete` still to run in this run (or to retry).
    removing: HashSet<NodeId>,
    /// The nodes whose `RemoteDelete` ended in this run without removing them: what is in them
    /// waits for the next plan, so nothing is removed before its folder (node schema, Recently
    /// deleted).
    unremoved: HashSet<NodeId>,
    /// Transient remote errors in a row, and until when the run waits because of them.
    streak: u32,
    resume_at: Option<Instant>,
    /// Attempts per operation within one run.
    attempts: u8,
    /// The pause after a transient remote error, doubled for each one in a row.
    retry_pause: Duration,
}

impl Schedule {
    pub(super) fn new(attempts: u8, retry_pause: Duration) -> Self {
        Schedule {
            queue: VecDeque::new(),
            forgetting: HashSet::new(),
            removing: HashSet::new(),
            unremoved: HashSet::new(),
            streak: 0,
            resume_at: None,
            attempts,
            retry_pause,
        }
    }

    /// Starts a run of `ops`, in order.
    pub(super) fn load(&mut self, ops: Vec<Op>) {
        self.forgetting.clear();
        self.removing.clear();
        self.unremoved.clear();
        for op in &ops {
            if let Some(pending) = self.pending(op) {
                pending.insert(op.node());
            }
        }
        self.queue = ops.into_iter().map(|op| (op, 0)).collect();
    }

    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    /// The next operation to run, with the attempts it had.
    pub(super) fn next(&mut self) -> Option<(Op, u8)> {
        self.queue.pop_front()
    }

    /// The next operation's node, taken off the queue, if it is a `Settle`.
    pub(super) fn next_settle(&mut self) -> Option<NodeId> {
        self.queue
            .pop_front_if(|(op, _)| matches!(op, Op::Settle { .. }))
            .map(|(op, _)| op.node())
    }

    /// Puts an operation that didn't start back at the front of the queue.
    pub(super) fn put_back(&mut self, op: Op, attempts: u8) {
        self.queue.push_front((op, attempts));
    }

    /// Whether an operation still queued moves `node` away from its local name.
    pub(super) fn vacating(&self, node: NodeId) -> bool {
        self.queue
            .iter()
            .any(|(op, _)| op.vacates_local_name() == Some(node))
    }

    /// Whether `node` has a `Forget` still to run in this run.
    pub(super) fn forgetting(&self, node: NodeId) -> bool {
        self.forgetting.contains(&node)
    }

    /// The nodes with operations like `op` still to run in this run (or to retry), where others
    /// wait for them: a folder's `Forget` for its children's, a removal for its folder's.
    fn pending(&mut self, op: &Op) -> Option<&mut HashSet<NodeId>> {
        match op {
            Op::Forget { .. } => Some(&mut self.forgetting),
            Op::RemoteDelete { .. } => Some(&mut self.removing),
            _ => None,
        }
    }

    /// The server-side order guards ([`Op::requires`]); `on_drive`: whether the mirror has a
    /// folder.
    pub(super) fn ready(
        &self,
        op: &Op,
        on_drive: impl FnOnce(NodeId) -> Result<bool, StateError>,
    ) -> Step<()> {
        let met = match op.requires() {
            None => true,
            Some(Requirement::RemoteFolder(folder)) => on_drive(folder)?,
            Some(Requirement::FolderFirst(folder)) if self.unremoved.contains(&folder) => {
                return Err(Outcome::waiting("its folder wasn't removed"));
            }
            Some(Requirement::FolderFirst(folder)) => !self.removing.contains(&folder),
        };
        if met {
            Ok(())
        } else {
            Err(Outcome::Retry(format!("waiting for {:?}", op.requires())))
        }
    }

    /// Notes what became of `op` for the operations that wait on it, and whether the remote
    /// answered.
    pub(super) fn ended(&mut self, op: &Op, outcome: &Outcome) {
        if matches!(
            outcome,
            Outcome::Done | Outcome::Settled | Outcome::Skip(_) | Outcome::Full
        ) {
            self.streak = 0; // the remote answered (or wasn't needed)
        }
        if !matches!(outcome, Outcome::Retry(_) | Outcome::Backoff(_)) {
            let removed = matches!(outcome, Outcome::Done | Outcome::Settled);
            self.finished(op, removed);
        }
    }

    /// `op` won't run again in this run; `removed`: it was done.
    fn finished(&mut self, op: &Op, removed: bool) {
        if let Some(pending) = self.pending(op) {
            pending.remove(&op.node());
        }
        if matches!(op, Op::RemoteDelete { .. }) && !removed {
            self.unremoved.insert(op.node());
        }
    }

    /// What becomes of `op`, which failed for now (`transient`: a transient remote error;
    /// else an ordering block or a local fault) after `attempts`: queued again, unless it was
    /// the last; a remote write whose answer didn't come goes back to the plan.
    pub(super) fn retry(&mut self, op: Op, attempts: u8, transient: bool) -> Retried {
        if transient {
            if !self.backoff() {
                return Retried::Unreachable;
            }
            if op.writes_remote() {
                // A write whose answer didn't come may have landed: not sent again blind (a
                // large upload twice), but planned again from the feed, read first.
                return Retried::Replan(op);
            }
        }
        if attempts + 1 < self.attempts {
            self.queue.push_back((op, attempts + 1));
            return Retried::Queued;
        }
        self.finished(&op, false);
        Retried::Failed(op)
    }

    /// Notes a transient remote error: one pause for the whole run, longer for each failure in
    /// a row. Writes failing together on the pool, before the pause, are one failure. `false`
    /// once they kept coming: the run gives up, unreachable.
    fn backoff(&mut self) -> bool {
        if self.resume_at.is_none() {
            self.streak += 1;
            if self.streak >= UNREACHABLE_AFTER {
                return false;
            }
            let pause = self.retry_pause * (1u32 << (self.streak - 1));
            self.resume_at = Some(Instant::now() + pause);
        }
        true
    }

    /// Whether the run waits after a transient remote error.
    pub(super) fn backing_off(&self) -> bool {
        self.resume_at.is_some()
    }

    /// Waits out the pause after a transient remote error, if there is one. Only remote work
    /// waits; local operations go on meanwhile.
    pub(super) fn wait_out_backoff(&mut self) {
        if let Some(at) = self.resume_at.take() {
            std::thread::sleep(at.saturating_duration_since(Instant::now()));
        }
    }
}

#[cfg(test)]
mod tests {
    use hbsync_types::{RemoteFileId, Uuid, VersionTag};

    use crate::exec::Skip;

    use super::*;

    fn removal(node: NodeId, parent: NodeId) -> Op {
        Op::RemoteDelete {
            node,
            parent,
            file_id: RemoteFileId(Uuid::new_v4()),
            expected: VersionTag(Uuid::new_v4()),
        }
    }

    fn schedule() -> Schedule {
        Schedule::new(3, Duration::ZERO)
    }

    /// A removal waits for its folder's: retried while that is still to run, run once it
    /// went, and left for the next plan when it ended without removing the folder.
    #[test]
    fn a_removal_waits_for_its_folders() {
        let (folder, child) = (NodeId::new(), NodeId::new());
        let mut s = schedule();
        s.load(vec![removal(folder, NodeId::ROOT), removal(child, folder)]);
        let (first, _) = s.next().expect("queued");
        let (second, _) = s.next().expect("queued");
        assert!(matches!(
            s.ready(&second, |_| Ok(true)),
            Err(Outcome::Retry(_))
        ));
        s.ended(&first, &Outcome::Done);
        assert!(s.ready(&second, |_| Ok(true)).is_ok());

        let mut s = schedule();
        s.load(vec![removal(folder, NodeId::ROOT), removal(child, folder)]);
        let (first, _) = s.next().expect("queued");
        s.ended(&first, &Outcome::stale("moved on"));
        assert!(matches!(
            s.ready(&removal(child, folder), |_| Ok(true)),
            Err(Outcome::Skip(Skip::Waiting(_)))
        ));
    }

    /// An operation failing for now is queued again until its last attempt; a remote write
    /// whose answer didn't come goes back to the plan; one failing on its last attempt keeps
    /// what waits on it from running.
    #[test]
    fn a_failure_is_retried_then_given_up() {
        let (folder, child) = (NodeId::new(), NodeId::new());
        let mut s = schedule();
        s.load(vec![removal(folder, NodeId::ROOT)]);
        let (op, _) = s.next().expect("queued");
        assert!(matches!(s.retry(op, 0, false), Retried::Queued));
        let (op, attempts) = s.next().expect("queued again");
        assert_eq!(attempts, 1);
        assert!(matches!(s.retry(op.clone(), 2, false), Retried::Failed(_)));
        assert!(matches!(
            s.ready(&removal(child, folder), |_| Ok(true)),
            Err(Outcome::Skip(Skip::Waiting(_)))
        ));
        assert!(matches!(s.retry(op, 0, true), Retried::Replan(_)));
    }

    /// Transient errors in a row, with no answer between them, stop the run; an answer starts
    /// the count again.
    #[test]
    fn transient_errors_in_a_row_stop_the_run() {
        let settle = || Op::Settle {
            node: NodeId::new(),
        };
        let mut s = schedule();
        for _ in 1..UNREACHABLE_AFTER {
            assert!(matches!(s.retry(settle(), 0, true), Retried::Queued));
            assert!(s.backing_off());
            s.wait_out_backoff();
        }
        s.ended(&settle(), &Outcome::Done);
        assert!(matches!(s.retry(settle(), 0, true), Retried::Queued));
        s.wait_out_backoff();
        for _ in 2..UNREACHABLE_AFTER {
            s.retry(settle(), 0, true);
            s.wait_out_backoff();
        }
        assert!(matches!(s.retry(settle(), 0, true), Retried::Unreachable));
    }
}
