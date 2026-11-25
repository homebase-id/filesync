//! Executing a plan against the disk, the state DB and the remote.
//!
//! Every operation is guarded and idempotent, as the simulator requires:
//! - a local item is only touched if it still matches its `local` row (the fingerprint
//!   guard); folders are compared by kind and file id;
//! - nothing local is ever overwritten by surprise: new names use no-replace renames, content
//!   swaps check the fingerprint first, and deletes move items aside, check them there, then
//!   unlink them (sync semantics §7);
//! - a remote write carries the version the plan saw;
//! - a target name held by a node that is about to leave is freed by **parking** that node
//!   under a temporary name, recorded in the DB until it is moved on or back;
//! - an operation blocked only by the order of the plan is retried later in the same run.
//!
//! Each operation's row updates (`local`, mirror) and the **settling** of its node (the base
//! takes each attribute both sides now agree on) are one transaction: the base never falls
//! behind what was done on disk or remotely.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hbsync_fileio::FileIoError;
use hbsync_platform::name::name_key;
use hbsync_platform::{EntryKind, RelPath, executable_on_disk, holds_executable, ops, tags};
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{
    Answer, Answered, BaseRow, FolderTx, LocalRow, MirrorRow, Placing, ReadTx, Refused, StateDb,
    StateError, Store,
};
use hbsync_types::activity::Synced;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, RemoteRef, Tags, Target, Written};

use crate::activity::{self, Counters};
use crate::folder::FolderCtx;
use crate::reconcile::{Op, Requirement};
use crate::refused::Refusals;
use crate::settle::rules::{DriveVersion, TagWrite, not_reached};
use crate::settle::{
    Placed, Settle, mark_children, settle, settle_placement, settle_with, spell, tags_recorded,
};
use crate::transfer::{
    Download, Job, JobError, JobResult, SendHash, TransferConfig, run_jobs_while,
};
use crate::tree::moved_out_names;
use crate::view;
use hbsync_local::layout;

/// How the executor runs a plan (the folder it works in is a [`FolderCtx`]).
#[derive(Debug, Clone)]
pub struct ExecConfig {
    /// Attempts per operation within one run (ordering blocks and transient errors).
    pub attempts: u8,
    /// The pause after a transient remote error, doubled for each one in a row. The whole run
    /// waits: the next request would most likely fail the same way.
    pub retry_pause: Duration,
    pub transfer: TransferConfig,
    /// Tests only: where the run fails or stops as if it crashed (none without `testkit`).
    pub hooks: Hooks,
}

impl Default for ExecConfig {
    fn default() -> Self {
        ExecConfig {
            attempts: 4,
            retry_pause: Duration::from_millis(500),
            transfer: TransferConfig::default(),
            hooks: Hooks::default(),
        }
    }
}

pub(crate) struct Executor<'a> {
    db: &'a mut StateDb,
    remote: &'a dyn Remote,
    folder: &'a FolderCtx,
    cfg: &'a ExecConfig,
    /// The queue, and the retry policy.
    schedule: Schedule,
    report: ExecReport,
    /// Set when a test's crash point was reached: the run stops.
    crashed: bool,
    /// Where this run settled bases, and the placement records it wrote ([`Settling`]).
    settling: Settling,
    /// What the run goes by of the drive ([`Heard`]).
    heard: Heard,
    /// Set by the caller to stop the run between operations.
    stop: Option<Arc<AtomicBool>>,
    progress: Arc<Counters>,
    /// The transfers already in `progress`'s total.
    counted: HashSet<NodeId>,
    /// The folders something goes into, looked at this run: whether each is on disk the one
    /// its row has ([`Executor::target_path`]).
    parents: HashMap<NodeId, bool>,
    /// The folder's refusals of the operations planned ([`crate::refused`]).
    refusals: Refusals,
    /// The nodes whose write to another identity's drive is on its way there or was held, in
    /// this run or an earlier one of this sync ([`Outcome::Wait`]), and what was left for
    /// one of those as its folder. None is asked again in this sync (their server said
    /// when), and what is to go into one waits for it, told so (it isn't refused: the folder
    /// isn't there yet).
    on_its_way: HashSet<NodeId>,
    /// The sync root, written only through it.
    disk: Disk<'a>,
    /// Where what moved out of a read-only folder is told (§13), whatever the run's end
    /// (none: not told).
    moved_out: Option<&'a mut Vec<(String, std::path::PathBuf)>>,
}

/// What a run goes by of the drive: what its operations' answers said, and what it read of it
/// (the files in versions it can't read, read once a run; full since an earlier run).
#[derive(Default)]
struct Heard {
    /// The drive is full: uploads wait (from the start, when it was lately).
    full: bool,
    /// The files the drive has in a version this device can't read
    /// ([`ReadTx::unreadable_files`]), read at the run's first write to an existing file.
    unreadable: Option<HashSet<hbsync_types::RemoteFileId>>,
    /// Writes to another identity's drive that ended on their way there, in a row
    /// ([`Outcome::Wait`]): none taken between them.
    waits: u32,
}

/// So many writes on their way in a row, and the run's other writes to that drive are held:
/// their server isn't taking any, and each more would cost the adapter's wait for it (and a
/// file its upload) to learn the same.
const WAIT_HOLD: u32 = 3;

impl Heard {
    /// Notes what `op`'s `outcome` told of the drive: it is full; its owner's server takes
    /// this identity's writes, or doesn't for now.
    fn ended(&mut self, op: &Op, outcome: &Outcome) {
        match outcome {
            Outcome::Full(_) => self.full = true,
            Outcome::Wait(..) => self.waits += 1,
            Outcome::Done | Outcome::Settled if op.writes_remote() => self.waits = 0,
            _ => {}
        }
    }

    /// Whether the run's writes to another identity's drive are held ([`WAIT_HOLD`]).
    fn holds_writes(&self) -> bool {
        self.waits >= WAIT_HOLD
    }
}

/// An operation whose guards passed, its job waiting for a worker.
struct Prepared {
    op: Op,
    attempts: u8,
    ctx: Ctx,
    /// The path of what it removes, taken before it ran.
    gone: Option<String>,
}

/// What a download's commit needs to know.
struct Placement {
    /// Its write-ahead record (`recover_placements`): the node, where it goes, and the
    /// version it acts on, with the bytes fetched (its commit settles against it).
    record: Placing,
    current: Option<LocalRow>,
    /// The node's, given to the file once in place.
    created_ms: Option<i64>,
    /// What it writes on the file.
    tags: Tags,
}

/// What the commit of a transfer needs to know.
enum Ctx {
    Download(Box<Placement>),
    /// `from`: the folder on the drive an updated file is in now.
    Upload {
        from: Option<NodeId>,
    },
    /// A metadata write; `from`: the folder on the drive the node leaves, if any.
    Meta {
        hash: Option<hbsync_types::ContentHash>,
        from: Option<NodeId>,
        after: remote::After,
    },
}

impl std::fmt::Debug for Executor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executor")
            .field("queued", &self.schedule.len())
            .finish()
    }
}

impl<'a> Executor<'a> {
    pub(crate) fn new(
        db: &'a mut StateDb,
        remote: &'a dyn Remote,
        folder: &'a FolderCtx,
        cfg: &'a ExecConfig,
    ) -> Self {
        Executor {
            db,
            remote,
            folder,
            cfg,
            schedule: Schedule::new(cfg.attempts, cfg.retry_pause),
            report: ExecReport::default(),
            crashed: false,
            settling: Settling::default(),
            heard: Heard::default(),
            stop: None,
            progress: Arc::default(),
            counted: HashSet::new(),
            parents: HashMap::new(),
            refusals: Refusals::default(),
            on_its_way: HashSet::new(),
            disk: Disk::new(folder, &cfg.hooks),
            moved_out: None,
        }
    }

    /// Tells what moves out of a read-only folder into `sink` as it goes (§13), so a run that
    /// fails later still tells it.
    pub(crate) fn with_moved_out(
        mut self,
        sink: &'a mut Vec<(String, std::path::PathBuf)>,
    ) -> Self {
        self.moved_out = Some(sink);
        self
    }

    /// Stops the run between operations once `stop` is set: each operation is done or not
    /// started, as at a crash, and the rest stays dirty. Transfers in flight finish first.
    pub(crate) fn with_stop(mut self, stop: Arc<AtomicBool>) -> Self {
        self.stop = Some(stop);
        self
    }

    /// Whether the run should stop now: on request, or after a denied or unreachable remote.
    fn should_stop(&mut self) -> bool {
        if self.report.stopped.is_none()
            && self
                .stop
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
        {
            self.report.stopped = Some(Stopped::Requested);
        }
        self.report.stopped.is_some()
    }

    /// Counts what the run does (and, for transfers, the bytes) into `progress`.
    pub(crate) fn with_progress(mut self, progress: Arc<Counters>) -> Self {
        self.progress = progress;
        self
    }

    /// Holds every upload: the drive was full lately (an earlier run's
    /// [`DriveNews::full`](crate::news::DriveNews::full)).
    pub(crate) fn with_full(mut self, full: bool) -> Self {
        self.heard.full = full;
        self
    }

    /// The folder's refusals of the planned operations: one that is done forgets its own.
    pub(crate) fn with_refusals(mut self, refusals: Refusals) -> Self {
        self.refusals = refusals;
        self
    }

    /// With the nodes whose write was on its way, or held, in the sync's earlier runs.
    pub(crate) fn with_on_its_way(mut self, nodes: HashSet<NodeId>) -> Self {
        self.on_its_way = nodes;
        self
    }

    /// Runs a plan's operations, in order.
    pub(crate) fn run(mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        self.run_ops(ops)?;
        self.report.on_its_way = self.on_its_way;
        Ok(self.report)
    }

    fn run_ops(&mut self, ops: Vec<Op>) -> Result<(), StateError> {
        let visible = ops.iter().filter(|op| activity::kind_of(op).is_some());
        self.progress
            .files_total
            .fetch_add(visible.count() as u64, Ordering::Relaxed);
        self.count_bytes(&ops)?;
        self.schedule.load(ops);
        let mut batch = Batch::default();
        loop {
            if self.should_stop() {
                break;
            }
            let Some((op, attempts)) = self.schedule.next() else {
                if batch.jobs.is_empty() {
                    break;
                }
                self.run_batch(&mut batch)?;
                continue;
            };
            // Everything else runs on its own, in order: after the batch.
            let pooled = op.pooled();
            let target = if pooled { self.move_target(&op)? } else { None };
            if !batch.jobs.is_empty()
                && (!pooled
                    || batch.holds_up(&op)
                    || target.as_ref().is_some_and(|t| batch.crosses(op.node(), t)))
            {
                // Still to run while the batch does: what it takes away leaves its name to a
                // download in the batch ([`Executor::free_target`]). Looked at again after
                // it: what the batch did may have moved the target's folders.
                self.schedule.requeue(op, attempts);
                self.run_batch(&mut batch)?;
                continue;
            }
            // Read-only (§13): nothing goes to the drive, whatever planned it, but what this
            // identity may write there. Asked once what the op waits for in the batch ran (a
            // folder made there: what is made in it is asked of that folder's row).
            if self.folder.read_only.is_some() && op.writes_remote() {
                // What is to go into a folder whose own write is on its way waits for it (it
                // has no row to ask yet), and what is in that with it. And where their
                // server took none of the last few writes, the rest wait unsent.
                let held = if self.on_its_way.contains(&op.node()) {
                    Some("on its way to the drive: asked again later")
                } else if self.goes_into_one_on_its_way(&op)? {
                    Some("its folder is on its way to the drive")
                } else if self.heard.holds_writes() {
                    Some("held: the drive's server isn't taking changes for now")
                } else {
                    None
                };
                if let Some(why) = held {
                    self.on_its_way.insert(op.node());
                    self.handle(op, attempts, Outcome::waiting(why), None)?;
                    continue;
                }
                if !self.may_change(&op)? {
                    let never =
                        Outcome::unsyncable("the folder is read-only: the drive isn't written");
                    self.handle(op, attempts, never, None)?;
                    continue;
                }
            }
            // A folder temporarily inside its own destination waits for the move that takes
            // the destination out (§8a): moved now, the drive would hold a cycle.
            if target.as_ref().is_some_and(|t| t.contains(&op.node())) {
                let wait = Outcome::Retry("its destination is inside it on the drive".into());
                self.handle(op, attempts, wait, None)?;
                continue;
            }
            if let Err(wait) = self.ready(&op) {
                self.handle(op, attempts, wait, None)?;
                continue;
            }
            // What a delete removes is described before its rows go.
            let gone = match activity::kind_of(&op) {
                Some(kind) if kind.removes() => {
                    self.db.read(|s| activity::shown_path_of(s, op.node()))?
                }
                _ => None,
            };
            if pooled {
                // Transfers and remote writes are collected and run together on the worker
                // pool.
                match self.prepare(&op) {
                    Ok((job, ctx)) => batch.push(
                        job,
                        Prepared {
                            op,
                            attempts,
                            ctx,
                            gone,
                        },
                        target,
                    ),
                    Err(outcome) => self.handle(op, attempts, outcome, gone)?,
                }
                continue;
            }
            if let Op::Settle { node } = op {
                self.settle_run(node)?;
                continue;
            }
            let outcome = self.execute(&op);
            self.handle(op, attempts, outcome, gone)?;
            if self.crashed {
                return Err(StateError::Conflict("crash point reached".into()));
            }
        }
        Ok(())
    }

    /// Whether `op`, which writes the drive from a read-only folder (§13), is a write this
    /// identity may make there: the folder writes to what is shared with it
    /// ([`FolderCtx::writes`]), and the mirror's rows say so, now (the plan asked the view's:
    /// a share taken back since stops the write here). A change of a node it may change,
    /// where the mirror has it, or its move between two folders it may add to; a node made
    /// in a folder it may add to (one it may change: what is added is shared as that folder
    /// is; a folder this run made there is one); a node it may change removed (to its
    /// owner's Recently deleted). Nothing made at or moved to or from the top, and nothing
    /// deleted for good.
    /// Whether `op` puts its node into a folder whose own write to the drive is on its way
    /// there, or was held, in this run ([`Executor::on_its_way`]): one the store has no row
    /// of yet.
    fn goes_into_one_on_its_way(&mut self, op: &Op) -> Result<bool, StateError> {
        let into = match op {
            Op::RemoteCreate { parent, .. } | Op::RemoteUpdate { parent, .. } => *parent,
            _ => return Ok(false),
        };
        if !self.on_its_way.contains(&into) {
            return Ok(false);
        }
        Ok(self.db.read(|s| s.get_mirror(into))?.is_none())
    }

    fn may_change(&mut self, op: &Op) -> Result<bool, StateError> {
        if !self.folder.writes {
            return Ok(false);
        }
        self.db.read(|s| {
            let adds_to = |folder: NodeId| -> Result<bool, StateError> {
                Ok(s.get_mirror(folder)?.is_some_and(|row| row.writable))
            };
            let changes = |to: Option<NodeId>| -> Result<bool, StateError> {
                let Some(row) = s.get_mirror(op.node())?.filter(|row| row.writable) else {
                    return Ok(false);
                };
                match to {
                    Some(to) if to != row.parent => Ok(adds_to(to)? && adds_to(row.parent)?),
                    _ => Ok(true),
                }
            };
            match op {
                Op::RemoteUpdate { parent, .. } => changes(Some(*parent)),
                Op::RemoteTags { .. } => changes(None),
                // A removal: to its owner's Recently deleted, where the node is. A folder's
                // takes along what is in it there: all the mirror has in it must be this
                // identity's to change too, now (the share of one item in it turned back to
                // reading since the plan stops the folder's removal here).
                Op::RemoteDelete { node, .. } => {
                    if !changes(None)? {
                        return Ok(false);
                    }
                    // (Each folder once: the drive may hold a cycle of them, another
                    // writer's, which a removal is never sent into.)
                    let mut folders = vec![*node];
                    let mut walked = HashSet::from([*node]);
                    while let Some(folder) = folders.pop() {
                        for row in s.mirror_children(folder)? {
                            if !row.writable {
                                return Ok(false);
                            }
                            if row.kind == NodeKind::Folder {
                                if !walked.insert(row.node) {
                                    return Ok(false);
                                }
                                folders.push(row.node);
                            }
                        }
                    }
                    Ok(true)
                }
                Op::RemoteCreate { parent, .. } => adds_to(*parent),
                _ => Ok(false),
            }
        })
    }

    /// Where `op` moves its node on the drive, if it moves it to another folder: that folder
    /// and every one above it, as the mirror has them.
    fn move_target(&mut self, op: &Op) -> Result<Option<Vec<NodeId>>, StateError> {
        let Op::RemoteUpdate { node, parent, .. } = op else {
            return Ok(None);
        };
        self.db.read(|s| {
            if s.get_mirror(*node)?.is_none_or(|m| m.parent == *parent) {
                return Ok(None);
            }
            let above = crate::walk::up::<_, StateError>(*parent, |at| {
                Ok(s.get_mirror(at)?.map(|m| (at, m.parent)))
            })?;
            Ok(Some(above.items))
        })
    }

    /// Whether test fault `point` fails now ([`ExecConfig::hooks`]).
    fn fails(&self, point: FailPoint) -> bool {
        self.cfg.hooks.fails(point)
    }

    /// Whether the test crash point `at` is reached; the run then stops.
    fn crash_here(&mut self, at: CrashPoint) -> bool {
        self.crashed |= self.cfg.hooks.crashes_at(at);
        self.crashed
    }

    /// The server-side order guards ([`Op::requires`]), checked against the mirror.
    fn ready(&mut self, op: &Op) -> Step<()> {
        let db = &mut *self.db;
        self.schedule.ready(op, |folder| {
            Ok(db.read(|s| s.get_mirror(folder))?.is_some())
        })
    }

    /// `gone`: the path of what the operation removed, taken before it ran.
    fn handle(
        &mut self,
        op: Op,
        attempts: u8,
        outcome: Outcome,
        gone: Option<String>,
    ) -> Result<(), StateError> {
        let (outcome, mut record) = self.refusals_after(&op, outcome)?;
        self.placement_ended(&op, &outcome)?;
        self.schedule.ended(&op, &outcome);
        self.heard.ended(&op, &outcome);
        match outcome {
            Outcome::Done => {
                self.record(&op, gone)?;
                self.report.done += 1;
                self.settle_done(&op)?;
            }
            Outcome::Settled => {
                self.record(&op, gone)?;
                self.report.done += 1;
            }
            Outcome::Backoff(why) => self.retried(op, attempts, true, why)?,
            Outcome::Retry(why) => self.retried(op, attempts, false, why)?,
            Outcome::Skip(why) => {
                let node = op.node();
                let refused = record.take().filter(|_| matches!(why, Skip::Refused(_)));
                self.db.write(|s| {
                    s.folder().mark_dirty(node)?;
                    refused
                        .as_ref()
                        .map_or(Ok(()), |r| s.folder().put_refused(r))
                })?;
                self.report.refused |= refused.is_some();
                self.report.skipped.push((op, why));
            }
            Outcome::Mismatch(why) => {
                // The first time at this version: the drive is read again (it may have been
                // written meanwhile), and the strike recorded.
                let Some(strike) = record else {
                    unreachable!("a mismatch has its strike");
                };
                let node = op.node();
                self.db.write(|s| {
                    s.folder().mark_dirty(node)?;
                    s.folder().put_refused(&strike)
                })?;
                self.report.refused = true;
                self.report.news.behind = true;
                self.report.skipped.push((op, Skip::Stale(why)));
            }
            Outcome::Stop(stopped) => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                // A write the server answered busy may have landed before (a 5xx): planned
                // again from the feed, read first.
                let busy = matches!(stopped, Stopped::Later(..) | Stopped::Waiting(..));
                if busy && op.writes_remote() {
                    self.report.news.behind = true;
                }
                self.report.stopped.get_or_insert(stopped);
            }
            Outcome::Full(after) => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.report.news.full = Some(crate::time::full_hold(after));
            }
            // Left for a later plan, as it is: the same write asked again is answered from
            // what was sent. (Not behind: its landing is announced, as any change there.)
            Outcome::Wait(why, after) => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.on_its_way.insert(node);
                let soonest = self.report.not_before.map_or(after, |s| s.min(after));
                self.report.not_before = Some(soonest);
                self.report.skipped.push((op, Skip::Waiting(why)));
            }
            Outcome::Fatal(e) => return Err(e),
        }
        Ok(())
    }

    /// Takes `op`, which failed for now ([`Schedule::retry`]), with `why`.
    fn retried(
        &mut self,
        op: Op,
        attempts: u8,
        transient: bool,
        why: String,
    ) -> Result<(), StateError> {
        match self.schedule.retry(op, attempts, transient) {
            Retried::Queued => {}
            Retried::Failed(op) => self.report.failed.push((op, why)),
            Retried::Replan { op, stop } => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.report.news.behind = true;
                if stop {
                    let stopped = Stopped::Unreachable(why.clone());
                    self.report.stopped.get_or_insert(stopped);
                }
                self.report.skipped.push((op, Skip::Waiting(why)));
            }
            Retried::Unreachable => {
                self.report.stopped.get_or_insert(Stopped::Unreachable(why));
            }
        }
        Ok(())
    }

    /// Adds what `op` did to the report, for people: transfers, moves and deletes. `gone`:
    /// the path of an item it removed, taken before it ran (its rows may be gone now).
    fn record(&mut self, op: &Op, gone: Option<String>) -> Result<(), StateError> {
        let Some(kind) = activity::kind_of(op) else {
            return Ok(());
        };
        self.progress.files_done.fetch_add(1, Ordering::Relaxed);
        let node = op.node();
        let (path, bytes) = match gone {
            Some(path) => (Some(path), 0),
            None => self.db.read(|s| {
                Ok(match s.local_by_node(node)? {
                    Some(row) if kind.moves_bytes() => {
                        (Some(row.path.as_str().to_owned()), row.fingerprint.size)
                    }
                    Some(row) => (Some(row.path.as_str().to_owned()), 0),
                    None => (activity::synced_path_of(s, node)?, 0),
                })
            })?,
        };
        if let Some(path) = path {
            self.report.synced.push(Synced {
                at: hbsync_types::unix_now(),
                kind,
                path,
                files: 1,
                bytes,
            });
        }
        Ok(())
    }

    /// Adds the bytes of every transfer in the plan to `progress`'s total before any starts, so
    /// the total doesn't grow batch by batch. One the guards skip later stays in it.
    fn count_bytes(&mut self, ops: &[Op]) -> Result<(), StateError> {
        let sizes = self.db.read(|s| {
            let mut sizes = Vec::new();
            for op in ops.iter().filter(|op| op.is_transfer()) {
                let node = op.node();
                let size = match op {
                    Op::LocalDownload { .. } => {
                        s.get_mirror(node)?.and_then(|m| m.file).map(|f| f.size)
                    }
                    _ => s.local_by_node(node)?.map(|row| row.fingerprint.size),
                };
                sizes.extend(size.map(|size| (node, size)));
            }
            Ok(sizes)
        })?;
        let total: u64 = sizes
            .into_iter()
            .filter(|(node, _)| self.counted.insert(*node))
            .map(|(_, size)| size)
            .sum();
        self.progress
            .bytes_total
            .fetch_add(total, Ordering::Relaxed);
        Ok(())
    }

    /// Runs the collected transfers in parallel and commits each as it finishes.
    fn run_batch(&mut self, batch: &mut Batch) -> Result<(), StateError> {
        self.schedule.wait_out_backoff();
        // In node order: what didn't start goes back to the queue in an order that doesn't
        // change from run to run.
        let mut pending: BTreeMap<NodeId, Prepared> = BTreeMap::new();
        let mut jobs = Vec::new();
        let mut records = Vec::new();
        for (job, p) in std::mem::take(batch).jobs {
            if let Ctx::Download(d) = &p.ctx {
                records.push(d.record.clone());
            }
            pending.insert(job.node(), p);
            jobs.push(job);
        }
        // Write-ahead: a download put in place whose commit a crash loses is found there again
        // by its record (`recover_placements`).
        if !records.is_empty() {
            self.disk.write_durable(self.db, |s| {
                records.iter().try_for_each(|r| s.put_placing(r))
            })?;
            self.settling
                .placing
                .extend(records.iter().map(|r| r.base.node));
        } else {
            self.disk.flush_ids(self.db)?;
        }
        // Counted up front by `count_bytes`; a retried transfer's bytes count once.
        let progress = Arc::clone(&self.progress);
        let new_bytes: u64 = jobs
            .iter()
            .filter(|j| self.counted.insert(j.node()))
            .map(Job::size)
            .sum();
        progress.bytes_total.fetch_add(new_bytes, Ordering::Relaxed);
        // Commit transfers as soon as they finish (with the others that finished meanwhile):
        // small files don't wait for a large one, and a crash loses only transfers still in
        // flight.
        let remote = self.remote;
        let transfer = self.cfg.transfer;
        let mut first_error = None;
        // A DB error or a stopped run (requested, denied, unreachable) starts no further
        // transfers; the nodes stay dirty for the next sync. A remote that failed for now
        // starts none either: they go back to the queue, to run after the pause.
        let halt = self.stop.clone();
        run_jobs_while(
            remote,
            transfer,
            Some(&progress.bytes_done),
            halt.as_deref(),
            jobs,
            |ran| {
                if first_error.is_some() {
                    return false;
                }
                if let Err(e) = self.commit_ran(ran, &mut pending) {
                    first_error = Some(e);
                }
                first_error.is_none()
                    && !self.should_stop()
                    && !self.schedule.backing_off()
                    && !self.heard.holds_writes()
            },
        );
        // What never started leaves no record (one that runs later writes its own).
        if !self.crashed && !pending.is_empty() {
            let gone = self.drop_unplaced(pending.keys().copied());
            if first_error.is_none() {
                gone?;
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        if !self.should_stop() {
            for p in pending.into_values().rev() {
                self.schedule.requeue(p.op, p.attempts);
            }
        }
        if self.crashed {
            return Err(StateError::Conflict("crash point reached".into()));
        }
        Ok(())
    }

    /// Records what jobs that finished together did (taking them out of `pending`): the
    /// removals that landed in one go, in one transaction ([`Executor::commit_removed`]: one
    /// commit for their answers, not one each); the downloads' bytes made durable together;
    /// every other result on its own.
    fn commit_ran(
        &mut self,
        ran: Vec<(Job, JobResult)>,
        pending: &mut BTreeMap<NodeId, Prepared>,
    ) -> Result<(), StateError> {
        let (mut ops, mut landed, mut rest) = (Vec::new(), Vec::new(), Vec::new());
        for (job, result) in ran {
            let Some(p) = pending.remove(&job.node()) else {
                continue;
            };
            match (p.ctx, job, result) {
                (
                    Ctx::Meta {
                        hash,
                        after: remote::After::Removed { .. },
                        ..
                    },
                    Job::Meta { meta, target },
                    JobResult::Written(written),
                ) => {
                    ops.push((p.op, p.attempts, p.gone));
                    landed.push(remote::answer(&meta, hash, target.as_ref(), written, true));
                }
                (ctx, job, result) => rest.push((p.op, p.attempts, p.gone, ctx, job, result)),
            }
        }
        // What landed first: it is recorded whatever becomes of the rest.
        if !landed.is_empty() {
            // Not recorded: planned again, as one that failed on its own.
            let failed = match self.commit_removed(landed) {
                Ok(_) => None,
                Err(Outcome::Retry(why)) => Some(why),
                Err(_) => Some("removals answered, not recorded".to_owned()),
            };
            for (op, attempts, gone) in ops {
                let outcome = failed.clone().map_or(Outcome::Settled, Outcome::Retry);
                self.handle(op, attempts, outcome, gone)?;
            }
        }
        // The bytes of every download among them made durable together, before any is put in
        // place; one whose bytes didn't get there isn't (it is tried again, from its kept file).
        let mut files: Vec<&mut hbsync_fileio::CompletedFile> = rest
            .iter_mut()
            .filter_map(|r| match &mut r.5 {
                JobResult::Downloaded { file, .. } => Some(file),
                _ => None,
            })
            .collect();
        let downloads = !files.is_empty();
        hbsync_fileio::sync_completed(&mut files);
        let barrier_fails = downloads && self.fails(FailPoint::Barrier);
        for r in &mut rest {
            if matches!(&r.5, JobResult::Downloaded { file, .. } if barrier_fails || !file.synced())
            {
                r.5 = JobResult::Failed(JobError::Io(io::Error::other(
                    "its bytes couldn't be made durable",
                )));
            }
        }
        for (op, attempts, gone, ctx, job, result) in rest {
            let outcome = self.commit(ctx, job, result).unwrap_or_else(|o| o);
            self.handle(op, attempts, outcome, gone)?;
        }
        Ok(())
    }

    fn execute(&mut self, op: &Op) -> Outcome {
        let result = match op {
            Op::LocalFork {
                node,
                new_node,
                name,
            } => self.fork(*node, *new_node, name),
            Op::LocalMkdir {
                node,
                parent,
                name,
                tags,
                drive,
            } => self.mkdir(*node, *parent, name, tags, drive.as_deref()),
            Op::LocalMove {
                node,
                parent,
                name,
                drive,
            } => self.local_move(*node, *parent, name, drive.as_deref()),
            Op::LocalTags { node, tags, drive } => self.local_tags(*node, tags, drive.as_deref()),
            Op::LocalDelete { node, subtree } => self.delete_here(*node, subtree, false),
            Op::LocalDownload { .. }
            | Op::RemoteCreate { .. }
            | Op::RemoteUpdate { .. }
            | Op::RemoteTags { .. }
            | Op::RemoteDelete { .. } => {
                unreachable!("transfers and remote writes run through flush")
            }
            Op::Settle { .. } => Ok(Outcome::Done),
            Op::LocalDetach { node, new_node } => self.detach(*node, *new_node),
            Op::LocalEvict { node, subtree } => self.evict(*node, subtree),
            Op::Forget { node, subtree } if subtree.is_empty() => self.drop_unsynced_base(*node),
            Op::Forget { node, subtree } => self.delete_here(*node, subtree, true),
        };
        result.unwrap_or_else(|o| o)
    }

    // ---- paths ----

    /// The local path where (parent, name) lives. Retried while the parent isn't local yet;
    /// skipped if the name can't be spelled on this OS (it stays remote-only and is reported),
    /// and as stale where the folder at the parent's place is no longer the parent.
    fn target_path(&mut self, parent: NodeId, name: &str) -> Step<RelPath> {
        let parent_path = if parent.is_root() {
            RelPath::root()
        } else {
            match self.db.read(|s| s.local_by_node(parent))? {
                Some(row) if row.kind == NodeKind::Folder => {
                    // The folder there is the row's still ([`folder_still`]): looked at
                    // once a run for each folder something goes into (a stat), and again
                    // as a download is placed ([`Executor::still_goes_into`]).
                    let still = match self.parents.get(&parent) {
                        Some(still) => *still,
                        None => {
                            let still = folder_still(&self.disk, self.folder, &row);
                            let still = still.map_err(io_outcome)?;
                            self.parents.insert(parent, still);
                            still
                        }
                    };
                    if !still {
                        return Err(not_its_folder(&row.path));
                    }
                    row.path
                }
                _ => return Err(Outcome::Retry("parent not local yet".into())),
            }
        };
        spell(&parent_path, name, self.folder.os)
            .map_err(|e| Outcome::unsyncable(format!("name can't be used here: {e}")))
    }

    /// [`Executor::target_path`]'s look at the folder an item goes into, once more as the
    /// item is placed at `target`, a folder made there, or an item moved to it (a transfer
    /// may have taken long, and a run goes on while the user does; nothing is cached here):
    /// the row there, and a stat. A folder with no row any more isn't it either.
    fn still_goes_into(&mut self, target: &RelPath) -> Step<()> {
        match in_its_folder(self.db, &self.disk, self.folder, target)? {
            Ok(Some(true)) => Ok(()),
            Ok(_) => Err(not_its_folder(&target.parent().unwrap_or_default())),
            Err(e) => Err(io_outcome(e)),
        }
    }

    /// Whether the item on disk still matches its row.
    fn on_disk_unchanged(&self, row: &LocalRow) -> bool {
        hbsync_platform::stat(&self.disk.abs(&row.path)).is_ok_and(|meta| matches_row(row, &meta))
    }

    /// See [`occupant`].
    fn occupant(&mut self, target: &RelPath) -> Result<Option<LocalRow>, StateError> {
        occupant(self.db, self.folder.case_insensitive, target)
    }

    /// Whether `target` is free for `mover` after [`Executor::park_occupant`].
    fn free_target(&mut self, target: &RelPath, mover: Option<NodeId>) -> Result<bool, StateError> {
        Ok(!matches!(self.park_occupant(target, mover)?, Freed::Taken))
    }

    /// [`Executor::free_target`] for the item of `row`, which moves there. A folder that held
    /// the name, parked, took what is in it along: the row of an item that leaves it for its
    /// name is read again (`false` too where it is gone).
    fn free_target_for(
        &mut self,
        target: &RelPath,
        row: &mut LocalRow,
    ) -> Result<bool, StateError> {
        match self.park_occupant(target, Some(row.node))? {
            Freed::Taken => Ok(false),
            Freed::Parked(from) if row.path.starts_with(&from) => {
                let Some(now) = self.db.read(|s| s.local_by_node(row.node))? else {
                    return Ok(false);
                };
                *row = now;
                Ok(true)
            }
            Freed::Free | Freed::Parked(_) => Ok(true),
        }
    }

    /// Frees `target` if a node that the plan is about to move away holds it: that node is
    /// parked under a temporary name (recorded in the DB). The mover itself may hold it under
    /// another case (a case-only rename).
    fn park_occupant(
        &mut self,
        target: &RelPath,
        mover: Option<NodeId>,
    ) -> Result<Freed, StateError> {
        let on_disk = fs::symlink_metadata(self.disk.abs(target)).is_ok();
        let Some(occupant) = self.occupant(target)? else {
            return Ok(if on_disk { Freed::Taken } else { Freed::Free });
        };
        if Some(occupant.node) == mover {
            return Ok(Freed::Free);
        }
        let leaving = self.schedule.vacating(occupant.node);
        if !leaving || !self.on_disk_unchanged(&occupant) {
            return Ok(Freed::Taken);
        }
        let parent = occupant.path.parent().unwrap_or_default();
        let Ok(parked) = parent.join(&layout::parked_name(occupant.node)) else {
            return Ok(Freed::Taken);
        };
        // Write-ahead: after a crash between the rename and the row update, the scan finds the
        // item under its temporary name, and this record maps it back to its real place (a
        // record counts only while the item carries that name), so the temporary name can't
        // reach the remote as a rename.
        self.disk
            .write_durable(self.db, |s| s.put_parked(occupant.node, &occupant.path))?;
        if self.disk.rename(self.db, &occupant.path, &parked)?.is_err() {
            self.db.write(|s| s.folder().delete_parked(occupant.node))?;
            return Ok(Freed::Taken);
        }
        if self.crash_here(CrashPoint::AfterParkRename) {
            return Ok(Freed::Taken);
        }
        let refreshed = self.renamed(&occupant, &parked);
        self.db.write(|s| {
            s.folder().move_local_subtree(&occupant.path, &parked)?;
            refreshed
                .as_ref()
                .map_or(Ok(()), |r| s.folder().put_local(r))
        })?;
        Ok(Freed::Parked(occupant.path))
    }
}

/// What became of a name an item is to take ([`Executor::park_occupant`]).
enum Freed {
    /// Something that stays holds it.
    Taken,
    /// Nothing holds it.
    Free,
    /// What held it is parked: taken from this path, with all that is in it.
    Parked(RelPath),
}

mod batch;
mod delete;
mod disk;
mod hooks;
mod local;
mod outcome;
mod park;
mod recover;
mod refusing;
mod remote;
mod schedule;
mod settling;
mod transfer_ops;

use batch::Batch;
use settling::Settling;

#[cfg(test)]
mod tests;

use delete::Deleted;
pub use delete::keeps_its_folder;
use disk::Disk;
#[cfg(feature = "testkit")]
pub use hooks::{Aside, Faults, Meanwhile};
pub use hooks::{CrashPoint, FailPoint, Hooks};
pub use local::holds_tags;
pub(crate) use outcome::ExecReport;
use outcome::{Outcome, Step, io_outcome, remote_outcome, rescan};
pub use outcome::{Skip, Stopped};
use park::{numbered_place, occupant, release_place, release_place_for, restore_parked};
use recover::{Applied, apply, judge_on_disk};
pub use recover::{
    AsideOutcome, UNHASHED_WAIT, record_dirs, recover, recover_aside, recover_placements,
};
use recover::{complete_fork, download_key};
use schedule::{Retried, Schedule};
pub use transfer_ops::{never_had, removal_forgotten, tags_lost};

/// Whether the folder at `row`'s place is `row`'s still: a folder is there, with the row's
/// file id. One that took its place since the last scan gets nothing of what the drive has
/// for the folder that was there (the folder of shares renamed onto the name of a synced
/// folder just deleted, before the watcher told: sync semantics §10). By the file's id
/// without the volume's number (a restart may change that before a scan has the rows anew),
/// and only where ids tell an item ([`FolderCtx::ids_stable`]: a folder there is taken for
/// it elsewhere). An error where it can't be looked at.
fn folder_still(disk: &Disk<'_>, folder: &FolderCtx, row: &LocalRow) -> io::Result<bool> {
    match hbsync_platform::stat(&disk.abs(&row.path)) {
        Ok(meta) => Ok(meta.kind == EntryKind::Dir
            && (!folder.ids_stable() || meta.file_id.file == row.fingerprint.file_id.file)),
        Err(e) if hbsync_platform::meta::nothing_there(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

/// [`folder_still`] for the folder `target` is in, by its row (the root always is); `None`
/// where no folder has a row at that path.
fn in_its_folder(
    db: &mut StateDb,
    disk: &Disk<'_>,
    folder: &FolderCtx,
    target: &RelPath,
) -> Result<io::Result<Option<bool>>, StateError> {
    let Some(parent) = target.parent().filter(|parent| !parent.is_root()) else {
        return Ok(Ok(Some(true)));
    };
    Ok(match db.read(|s| s.get_local(&parent))? {
        Some(row) if row.kind == NodeKind::Folder => folder_still(disk, folder, &row).map(Some),
        _ => Ok(None),
    })
}

/// The skip for an item that isn't placed in `folder`, which is no longer the one synced
/// there: the next plan decides, once a scan has seen what is. No scan is asked for here:
/// one of that place alone would take a folder moved elsewhere for deleted, where the
/// watcher's own tells both of its ends.
fn not_its_folder(folder: &RelPath) -> Outcome {
    Outcome::stale(format!("{folder} isn't the folder synced there any more"))
}

/// Whether `meta` (from disk) is what `row` recorded: folders by kind and file id, files by
/// their full fingerprint.
fn matches_row(row: &LocalRow, meta: &hbsync_platform::LocalMeta) -> bool {
    match row.kind {
        NodeKind::Folder => meta.kind == EntryKind::Dir && meta.file_id == row.fingerprint.file_id,
        NodeKind::File => meta.kind == EntryKind::File && meta.fingerprint() == row.fingerprint,
    }
}

/// A rename changes ctime on most systems: the row of a file renamed from `old` to `path`,
/// with the new fingerprint and the old hash, if size and mtime show the content didn't
/// change. `None` for folders, or if the content may have changed (a rescan decides then).
fn renamed_row(root: &Path, old: &LocalRow, path: &RelPath) -> Option<LocalRow> {
    let meta = hbsync_platform::stat(&path.to_native(root)).ok()?;
    (old.kind == NodeKind::File && old.fingerprint.same_after_rename(&meta)).then(|| LocalRow {
        path: path.clone(),
        fingerprint: meta.fingerprint(),
        executable: meta.executable.or(old.executable),
        ..old.clone()
    })
}
