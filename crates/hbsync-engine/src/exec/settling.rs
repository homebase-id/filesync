//! Settling a run's nodes (sync semantics §8a): each operation's DB changes and its nodes'
//! settling in one transaction, the answers of this device's remote writes, a run of `Settle`
//! operations, where this run settled each base, and the placement records it wrote.

use super::*;

/// Where this run settled bases, and the placement records it wrote.
#[derive(Default)]
pub(super) struct Settling {
    /// Where this run's operations settled each node's base: a later operation's compare and
    /// set takes the base there as the one its plan saw.
    pub(super) settled: HashMap<NodeId, RemoteRef>,
    /// Nodes whose operation wrote a placement record and hasn't put its item in place yet:
    /// only such a record its failure removes. Once in place, a record is recovery's to settle;
    /// and no operation places over one it didn't write.
    pub(super) placing: HashSet<NodeId>,
}

impl Executor<'_> {
    /// Settles `first` and the run of `Settle` ops after it (the plan puts them last) in one
    /// transaction: they change nothing on disk or remotely, so a crash only loses settling
    /// the next plan asks for again.
    pub(super) fn settle_run(&mut self, first: NodeId) -> Result<(), StateError> {
        const CHUNK: usize = 1000;
        let mut nodes = vec![first];
        while nodes.len() < CHUNK
            && let Some(node) = self.schedule.next_settle()
        {
            nodes.push(node);
        }
        let os = self.folder.os;
        let settled = self.db.write(|s| {
            nodes
                .iter()
                .map(|n| Ok((*n, settle(s.folder(), Settle::node(*n), os)?)))
                .collect::<Result<Vec<_>, StateError>>()
        })?;
        self.note(settled);
        self.report.done += nodes.len();
        Ok(())
    }

    /// Writes an operation's DB changes and settles `nodes` in the same transaction: the base
    /// can't fall behind what was done, and it costs one commit.
    pub(super) fn finish(
        &mut self,
        nodes: &[Settle<'_>],
        f: impl FnOnce(&Store<'_>) -> Result<(), StateError>,
    ) -> Step {
        let os = self.folder.os;
        let nodes: Vec<Settle<'_>> = nodes.iter().map(|how| self.ours(*how)).collect();
        let settled = self.db.write(|s| {
            f(s)?;
            nodes
                .iter()
                .map(|how| Ok((how.node, settle(s.folder(), *how, os)?)))
                .collect::<Result<Vec<_>, StateError>>()
        })?;
        self.note(settled);
        Ok(Outcome::Settled)
    }

    /// Records this device's remote writes' answers ([`AccountTx::answered`]), each settling its
    /// node as `how` against the row the answer left, in one transaction: no pull commits
    /// between an answer's check and its write, nor before the settle (writes to the state DB
    /// take turns: [`StateDb::write`]). An answer a pull superseded since its write landed
    /// settles its node as if that pull came after ([`settle_with`]), and plans it again; the
    /// run reads the feed again then (what superseded it may be a page fetched before the
    /// write landed). A removal plans its node again too (what was in it).
    ///
    /// [`AccountTx::answered`]: hbsync_state::AccountTx::answered
    pub(super) fn finish_answered(&mut self, answers: &[(Settle<'_>, Answer)]) -> Step {
        let os = self.folder.os;
        let answers: Vec<(Settle<'_>, &Answer)> = answers
            .iter()
            .map(|(how, a)| (self.ours(*how), a))
            .collect();
        let (settled, superseded) = self.db.write(|s| {
            let mut superseded = false;
            let mut settled = Vec::with_capacity(answers.len());
            for (how, answer) in &answers {
                let newer = s.account().answered_in_run(answer)? == Answered::Superseded;
                superseded |= newer;
                if newer || answer.live_row().is_none() {
                    s.folder().mark_dirty(how.node)?;
                }
                let row = answer.live_row().cloned();
                settled.push((how.node, settle_with(s.folder(), *how, row, os)?));
            }
            Ok((settled, superseded))
        })?;
        if superseded {
            self.report.news.behind = true;
        }
        self.note(settled);
        Ok(Outcome::Settled)
    }

    /// `how`, with where this run's operations settled its node's base.
    pub(super) fn ours<'s>(&self, how: Settle<'s>) -> Settle<'s> {
        Settle {
            ours: self.settling.settled.get(&how.node).copied(),
            ..how
        }
    }

    /// Notes where this run's operations settled bases.
    pub(super) fn note(&mut self, settled: impl IntoIterator<Item = (NodeId, Option<RemoteRef>)>) {
        for (node, at) in settled {
            if let Some(at) = at {
                self.settling.settled.insert(node, at);
            }
        }
    }

    /// Whether a placement record this operation didn't write waits for `node` (one recovery
    /// keeps until its item can be read, or one an earlier operation left with its item in
    /// place): no placement runs over it; the node stays dirty, and the next round's recovery
    /// settles it.
    pub(super) fn placing_earlier(&mut self, node: NodeId) -> Result<bool, StateError> {
        Ok(!self.settling.placing.contains(&node) && self.db.read(|s| s.is_placing(node))?)
    }

    /// Notes that `node`'s placement put its item in place: its record is recovery's now.
    pub(super) fn placed(&mut self, node: NodeId) {
        self.settling.placing.remove(&node);
    }

    /// A placement whose item is in place and whose commit failed ends here: recovery settles
    /// it right away (the one way an item put in place settles then), judging its record by its
    /// item alone, read off the disk: no other item is read, nor its row touched. One it can't
    /// settle yet (a file not hashed yet) stays, for a later recovery.
    pub(super) fn placed_not_committed(&mut self, node: NodeId) -> Step {
        let Some(record) = self.db.read(|s| s.get_placing(node))? else {
            return Ok(Outcome::stale("put in place, its record gone"));
        };
        let (root, now) = (&self.folder.root, hbsync_types::unix_now_ms());
        let judged = match look(root, &record.target) {
            OnDisk::Absent => Judged::Drop,
            OnDisk::Unreadable => Judged::Keep,
            OnDisk::There(meta) => {
                let row = read_row(root, &record.target, node, &meta, self.folder.algorithm);
                judge(root, &record, Some(row))
            }
        };
        let folder = self.folder;
        match self
            .db
            .write(|s| apply(s.folder(), &record, judged, folder, now))?
        {
            Applied::Settled(at) => {
                self.note([(node, at)]);
                Ok(Outcome::Settled)
            }
            Applied::Kept => Ok(Outcome::stale("put in place: recovery settles it later")),
            Applied::Dropped => Ok(Outcome::stale("put in place, not there since")),
        }
    }

    /// Settles the node of `op`, done, as an operation's own settle (a transfer: its bytes the
    /// same on both sides now).
    pub(super) fn settle_done(&mut self, op: &Op) -> Result<(), StateError> {
        let os = self.folder.os;
        let how = self.ours(Settle {
            same_bytes: op.is_transfer(),
            ..Settle::node(op.node())
        });
        let at = self.db.write(|s| settle(s.folder(), how, os))?;
        self.note([(how.node, at)]);
        Ok(())
    }

    /// After a placement (`op`, ended with `outcome`): one that failed before its item was in
    /// place leaves no record, so recovery never takes an item the user puts at that place for
    /// it (a retry writes its own).
    pub(super) fn placement_ended(&mut self, op: &Op, outcome: &Outcome) -> Result<(), StateError> {
        if !matches!(op, Op::LocalDownload { .. } | Op::LocalMkdir { .. }) {
            return Ok(());
        }
        match matches!(outcome, Outcome::Done | Outcome::Settled) || self.crashed {
            true => drop(self.settling.placing.remove(&op.node())),
            false => self.drop_unplaced([op.node()])?,
        }
        Ok(())
    }

    /// Of `nodes`, those whose placement record this run wrote and whose item it never put in
    /// place: their records go (one that runs later writes its own), in one write.
    pub(super) fn drop_unplaced(
        &mut self,
        nodes: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), StateError> {
        let unplaced: Vec<NodeId> = nodes
            .into_iter()
            .filter(|n| self.settling.placing.remove(n))
            .collect();
        if unplaced.is_empty() {
            return Ok(());
        }
        self.db.write(|s| {
            unplaced
                .iter()
                .try_for_each(|n| s.folder().delete_placing(*n))
        })
    }
}
