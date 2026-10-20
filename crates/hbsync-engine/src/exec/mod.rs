//! Executing a plan against the disk, the state DB and the remote.
//!
//! Every operation is guarded and idempotent, as the simulator requires:
//! - a local item is only touched if it still matches its `local` row (the fingerprint
//!   guard); folders are compared by kind and file id;
//! - nothing local is ever overwritten by surprise: new names use no-replace renames, content
//!   swaps check the fingerprint first, and deletes move items to `.hbsync/trash`;
//! - a remote write carries the version the plan saw;
//! - a target name held by a node that is about to leave is freed by **parking** that node
//!   under a temporary name, recorded in the DB until it is moved on or back;
//! - an operation blocked only by the order of the plan is retried later in the same run.
//!
//! Each operation's row updates (`local`, mirror) and the **settling** of its node (the base
//! takes each attribute both sides now agree on) are one transaction: the base never falls
//! behind what was done on disk or remotely.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_fileio::FileIoError;
use hbsync_platform::name::name_key;
use hbsync_platform::{EntryKind, IoClass, Os, RelPath, classify, ops, tags};
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{
    BaseRow, FolderTx, LocalRow, MirrorRow, Placing, ReadTx, StateDb, StateError, Store,
};
use hbsync_types::activity::Synced;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, RemoteRef, Tags, Target};

use crate::activity::{self, Counters};
use crate::folder::FolderCtx;
use crate::plan::{DriveVersion, TagWrite, not_reached};
use crate::reconcile::{Op, Requirement};
use crate::settle::{Placed, Settle, mark_children, settle, settle_placement, spell};
use crate::transfer::{Download, Job, JobError, JobResult, TransferConfig, run_jobs_while};
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

/// What a tag write left on an item ([`Executor::write_tags`]).
#[derive(Debug, Clone)]
struct TagsLeft {
    /// What read back, for a placement's record to claim (`None`: not known: the write failed,
    /// read back as nothing to go by, or the volume keeps none).
    claimed: Option<Tags>,
    /// What the item holds: what read back, or where the write failed, what it holds as read
    /// (`None` where that can't be read either: its row takes what it held before then,
    /// [`Executor::held_after`]). Taken for not known while it can be read, the item read as
    /// holding the drive's tags, and the next scan took their absence for the user removing
    /// them.
    held: Option<Tags>,
    failed: bool,
}

/// What a run did.
#[derive(Debug, Default)]
pub(crate) struct ExecReport {
    pub done: usize,
    /// Left for the next plan, and why.
    pub skipped: Vec<(Op, Skip)>,
    /// Gave up after retries (transient errors, locked files, ordering).
    pub failed: Vec<(Op, String)>,
    /// Local folders whose contents differ from the DB: rescan them before planning again.
    pub rescan: Vec<RelPath>,
    /// Why the run stopped early, leaving the rest dirty for a later sync.
    pub stopped: Option<Stopped>,
    /// The drive refused an upload, out of storage: the run's uploads from then on were held
    /// (left dirty).
    pub full: bool,
    /// What was done, for people (one entry per file or item).
    pub synced: Vec<Synced>,
    /// A remote write was refused for the drive's state, an earlier try of it had landed
    /// unanswered, or one's answer didn't come: the account's picture may lack something, so
    /// the feed is to be read.
    pub drive_moved: bool,
}

/// Why a run stopped before its plan was done.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Stopped {
    /// The remote refused access: nothing else can succeed until the user authorizes again.
    #[error("access denied: {0}")]
    Denied(String),
    /// Transient remote errors kept coming: the network or the server is down.
    #[error("remote unreachable: {0}")]
    Unreachable(String),
    /// The caller asked the run to stop (pause, shutdown).
    #[error("stopped on request")]
    Requested,
}

/// Transient remote errors in a row after which a run gives up (with the pauses between them
/// doubling from `retry_pause`, 0.5 s by default: about 15 s in all).
const UNREACHABLE_AFTER: u32 = 6;

#[derive(Debug)]
enum Outcome {
    /// Done; the node still needs settling.
    Done,
    /// Done, and settled in the operation's own transaction.
    Settled,
    Retry(String),
    /// A transient remote error: retry after a pause.
    Backoff(String),
    /// Left for the next plan.
    Skip(Skip),
    /// The remote refused access: stop the run.
    Stop(String),
    /// The drive is out of storage: hold this and every later upload of the run.
    Full,
    /// The state DB failed (a placement's commit, say): the run stops, as at a crash.
    Fatal(StateError),
}

impl Outcome {
    fn stale(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Stale(why.into()))
    }

    fn waiting(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Waiting(why.into()))
    }

    fn unsyncable(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Unsyncable(why.into()))
    }
}

/// Why an operation was left for the next plan.
#[derive(Debug)]
pub enum Skip {
    /// The world moved on since the plan (an item changed or went, on either side, or what the
    /// operation needed isn't so any more): the next plan decides again.
    Stale(String),
    /// Another operation of the plan must go first and didn't (held or failed: that one says
    /// why).
    Waiting(String),
    /// This device can't do it as planned: a name or path it can't hold, access denied, the
    /// drive refusing it, a fault of its own.
    Unsyncable(String),
}

impl Skip {
    pub fn as_str(&self) -> &str {
        match self {
            Skip::Stale(why) | Skip::Waiting(why) | Skip::Unsyncable(why) => why,
        }
    }
}

/// A DB error inside an operation is retried like any transient failure.
impl From<StateError> for Outcome {
    fn from(e: StateError) -> Self {
        Outcome::Retry(e.to_string())
    }
}

/// An operation's result. `Err` ends it early: a guard that stopped it, or a DB error.
type Step<T = Outcome> = Result<T, Outcome>;

pub(crate) struct Executor<'a> {
    db: &'a mut StateDb,
    remote: &'a dyn Remote,
    folder: &'a FolderCtx,
    cfg: &'a ExecConfig,
    queue: VecDeque<(Op, u8)>,
    /// The nodes with a `Forget` still to run in this run.
    forgetting: HashSet<NodeId>,
    /// The nodes with a `RemoteDelete` still to run in this run (or to retry).
    removing: HashSet<NodeId>,
    /// The nodes whose `RemoteDelete` ended in this run without removing them: what is in them
    /// waits for the next plan, so nothing is removed before its folder (node schema, Recently
    /// deleted).
    unremoved: HashSet<NodeId>,
    report: ExecReport,
    /// Set when a test's crash point was reached: the run stops.
    crashed: bool,
    /// Nodes whose operation wrote a placement record and hasn't put its item in place yet: only
    /// such a record its failure removes. Once in place, a record is recovery's to settle; and
    /// no operation places over one it didn't write.
    records: HashSet<NodeId>,
    /// Nodes whose tags the drive refused this run for good (access, a drive that won't take
    /// it): a tag only the item here has can't go up now, so a download no longer waits for it.
    tags_refused: HashSet<NodeId>,
    /// Where this run's operations settled each node's base: a later operation's compare and
    /// set takes the base there as the one its plan saw.
    settled: HashMap<NodeId, RemoteRef>,
    /// Transient remote errors in a row, and until when the run waits because of them.
    streak: u32,
    resume_at: Option<Instant>,
    /// Set by the caller to stop the run between operations.
    stop: Option<Arc<AtomicBool>>,
    progress: Arc<Counters>,
    /// The transfers already in `progress`'s total.
    counted: HashSet<NodeId>,
    /// The drive is full: uploads are held (from the start, when it was lately).
    full: bool,
    /// The sync root, written only through it.
    disk: Disk<'a>,
}

/// An operation whose guards passed, its job waiting for a worker.
struct Prepared {
    op: Op,
    attempts: u8,
    ctx: Ctx,
    /// The path of what it removes, taken before it ran.
    gone: Option<String>,
}

/// Operations collected for the worker pool, which runs them in any order.
#[derive(Default)]
struct Batch {
    jobs: Vec<(Job, Prepared)>,
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
    fn push(&mut self, job: Job, p: Prepared, target: Option<Vec<NodeId>>) {
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
    fn crosses(&self, node: NodeId, target: &[NodeId]) -> bool {
        self.under.contains(&node) || target.iter().any(|n| self.moved.contains(n))
    }

    /// Whether `op` must wait until the batch ran: it is about a node in it, its server-side
    /// order guard ([`Op::requires`]) waits on one (a folder created in it, or the removal of
    /// the folder a removal is in), or it removes a folder the batch puts a node into or takes
    /// one from.
    fn holds_up(&self, op: &Op) -> bool {
        self.nodes.contains(&op.node())
            || matches!(op, Op::RemoteDelete { .. }) && self.folders.contains(&op.node())
            || match op.requires() {
                Some(Requirement::RemoteFolder(folder)) => self.created.contains(&folder),
                Some(Requirement::FolderFirst(folder)) => self.nodes.contains(&folder),
                None => false,
            }
    }
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
        hash: hbsync_types::ContentHash,
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
            .field("queued", &self.queue.len())
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
            queue: VecDeque::new(),
            forgetting: HashSet::new(),
            removing: HashSet::new(),
            unremoved: HashSet::new(),
            report: ExecReport::default(),
            crashed: false,
            records: HashSet::new(),
            tags_refused: HashSet::new(),
            settled: HashMap::new(),
            streak: 0,
            resume_at: None,
            stop: None,
            progress: Arc::default(),
            counted: HashSet::new(),
            full: false,
            disk: Disk::new(&folder.root),
        }
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

    /// Holds every upload: the drive was full lately ([`ExecReport::full`] of an earlier run).
    pub(crate) fn with_full(mut self, full: bool) -> Self {
        self.full = full;
        self
    }

    /// Runs a plan's operations, in order.
    pub(crate) fn run(mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        self.run_ops(ops)?;
        Ok(self.report)
    }

    fn run_ops(&mut self, ops: Vec<Op>) -> Result<(), StateError> {
        self.forgetting.clear();
        self.removing.clear();
        self.unremoved.clear();
        for op in &ops {
            if let Some(pending) = self.pending(op) {
                pending.insert(op.node());
            }
        }
        let visible = ops.iter().filter(|op| activity::kind_of(op).is_some());
        self.progress
            .files_total
            .fetch_add(visible.count() as u64, Ordering::Relaxed);
        self.count_bytes(&ops)?;
        self.queue = ops.into_iter().map(|op| (op, 0)).collect();
        let mut batch = Batch::default();
        loop {
            if self.should_stop() {
                break;
            }
            let Some((op, attempts)) = self.queue.pop_front() else {
                if batch.jobs.is_empty() {
                    break;
                }
                self.run_batch(&mut batch)?;
                continue;
            };
            // Everything else runs on its own, in order: after the batch.
            let pooled = op.pooled();
            let mut target = if pooled { self.move_target(&op)? } else { None };
            if !batch.jobs.is_empty()
                && (!pooled
                    || batch.holds_up(&op)
                    || target.as_ref().is_some_and(|t| batch.crosses(op.node(), t)))
            {
                self.run_batch(&mut batch)?;
                if self.should_stop() {
                    break;
                }
                // What the batch did may have moved the target's folders.
                if target.is_some() {
                    target = self.move_target(&op)?;
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
                    self.db.read(|s| activity::path_of(s, op.node()))?
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

    /// Settles `first` and the run of `Settle` ops after it (the plan puts them last) in one
    /// transaction: they change nothing on disk or remotely, so a crash only loses settling
    /// the next plan asks for again.
    fn settle_run(&mut self, first: NodeId) -> Result<(), StateError> {
        const CHUNK: usize = 1000;
        let mut nodes = vec![first];
        while nodes.len() < CHUNK
            && let Some((Op::Settle { node }, _)) = self.queue.front()
        {
            nodes.push(*node);
            self.queue.pop_front();
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
    fn finish(
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

    /// Whether a placement record this operation didn't write waits for `node` (one recovery
    /// keeps until its item can be read, or one an earlier operation left with its item in
    /// place): no placement runs over it; the node stays dirty, and the next round's recovery
    /// settles it.
    fn placing_earlier(&mut self, node: NodeId) -> Result<bool, StateError> {
        Ok(!self.records.contains(&node) && self.db.read(|s| s.is_placing(node))?)
    }

    /// Notes that `node`'s placement put its item in place: its record is recovery's now.
    fn placed(&mut self, node: NodeId) {
        self.records.remove(&node);
    }

    /// A placement whose item is in place and whose commit failed ends here: recovery settles
    /// it right away (the one way an item put in place settles then), judging its record by its
    /// item alone, read off the disk: no other item is read, nor its row touched. One it can't
    /// settle yet (a file not hashed yet) stays, for a later recovery.
    fn placed_not_committed(&mut self, node: NodeId) -> Step {
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

    /// `how`, with where this run's operations settled its node's base.
    fn ours<'s>(&self, how: Settle<'s>) -> Settle<'s> {
        Settle {
            ours: self.settled.get(&how.node).copied(),
            ..how
        }
    }

    /// Notes where this run's operations settled bases.
    fn note(&mut self, settled: impl IntoIterator<Item = (NodeId, Option<RemoteRef>)>) {
        for (node, at) in settled {
            if let Some(at) = at {
                self.settled.insert(node, at);
            }
        }
    }

    /// Whether test fault `point` fails now ([`ExecConfig::hooks`]).
    fn fails(&self, point: FailPoint) -> bool {
        self.cfg.hooks.fails(point)
    }

    /// Whether the root's volume keeps tags, as far as known (this OS keeps some, and no tag
    /// write found it doesn't).
    fn tags_kept(&self) -> bool {
        tags::CAN_TAG && self.folder.tags_kept.get() != Some(&false)
    }

    /// Writes `tags` on the item at `path` ([`tags::write_on`]), noting whether the volume
    /// keeps tags, and says what that left ([`TagsLeft`]).
    fn write_tags(&self, path: &Path, tags: &Tags) -> TagsLeft {
        let written = match self.fails(FailPoint::TagWrite) {
            true => Err(io::Error::other("its tag write failed (a test)")),
            false => tags::write_on(path, tags),
        };
        match written {
            Ok(tags::Written::Unsupported) => {
                let _ = self.folder.tags_kept.set(false);
                TagsLeft {
                    claimed: None,
                    held: None,
                    failed: false,
                }
            }
            Ok(tags::Written::Done(read)) => {
                let _ = self.folder.tags_kept.set(true);
                TagsLeft {
                    claimed: read.clone(),
                    held: read,
                    failed: false,
                }
            }
            Err(_) => TagsLeft {
                claimed: None,
                held: (self.tags_kept() && !self.fails(FailPoint::TagRead))
                    .then(|| tags::read(path))
                    .flatten(),
                failed: true,
            },
        }
    }

    /// Whether a tag write failed and what it left couldn't be read back, on a volume that
    /// keeps tags.
    fn unread(&self, left: &TagsLeft) -> bool {
        left.failed && left.held.is_none() && self.tags_kept()
    }

    /// What a tag write left on an item that held `before`, for its row: what read back, or
    /// where the write failed and that read failed too, `before`, as a failed write changes
    /// nothing. Not known read as the drive's tags: a commit settled its base on them, and the
    /// scan that then read the item without them sent their removal up.
    fn held_after(&self, left: TagsLeft, before: Tags) -> Option<Tags> {
        if self.unread(&left) {
            Some(before)
        } else {
            left.held
        }
    }

    /// Whether the test crash point `at` is reached; the run then stops.
    fn crash_here(&mut self, at: CrashPoint) -> bool {
        self.crashed |= self.cfg.hooks.crashes_at(at);
        self.crashed
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

    /// The server-side order guards ([`Op::requires`]), checked against the mirror.
    fn ready(&mut self, op: &Op) -> Step<()> {
        let met = match op.requires() {
            None => true,
            Some(Requirement::RemoteFolder(folder)) => {
                self.db.read(|s| s.get_mirror(folder))?.is_some()
            }
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

    /// `gone`: the path of what the operation removed, taken before it ran.
    fn handle(
        &mut self,
        op: Op,
        attempts: u8,
        outcome: Outcome,
        gone: Option<String>,
    ) -> Result<(), StateError> {
        // A placement that failed before its item was in place leaves no record: recovery
        // never takes an item the user puts at that place for it (a retry writes its own).
        if matches!(op, Op::LocalDownload { .. } | Op::LocalMkdir { .. })
            && self.records.remove(&op.node())
            && !matches!(outcome, Outcome::Done | Outcome::Settled)
            && !self.crashed
        {
            let node = op.node();
            self.db.write(|s| s.folder().delete_placing(node))?;
        }
        if matches!(
            outcome,
            Outcome::Done | Outcome::Settled | Outcome::Skip(_) | Outcome::Full
        ) {
            self.streak = 0; // the remote answered (or wasn't needed)
        }
        if !matches!(outcome, Outcome::Retry(_) | Outcome::Backoff(_))
            && let Some(pending) = self.pending(&op)
        {
            pending.remove(&op.node());
        }
        if matches!(
            op,
            Op::RemoteTags { .. } | Op::RemoteUpdate { .. } | Op::RemoteCreate { .. }
        ) && matches!(
            outcome,
            Outcome::Skip(Skip::Unsyncable(_)) | Outcome::Stop(_)
        ) {
            self.tags_refused.insert(op.node());
        }
        if matches!(op, Op::RemoteDelete { .. })
            && !matches!(
                outcome,
                Outcome::Done | Outcome::Settled | Outcome::Retry(_) | Outcome::Backoff(_)
            )
        {
            self.unremoved.insert(op.node());
        }
        match outcome {
            Outcome::Done => {
                self.record(&op, gone)?;
                self.report.done += 1;
                let os = self.folder.os;
                let how = self.ours(Settle {
                    same_bytes: op.is_transfer(),
                    ..Settle::node(op.node())
                });
                let at = self.db.write(|s| settle(s.folder(), how, os))?;
                self.note([(how.node, at)]);
            }
            Outcome::Settled => {
                self.record(&op, gone)?;
                self.report.done += 1;
            }
            Outcome::Backoff(why) => {
                // Writes failing together on the pool, before the pause, are one failure.
                if self.resume_at.is_none() {
                    self.streak += 1;
                    if self.streak >= UNREACHABLE_AFTER {
                        self.report.stopped.get_or_insert(Stopped::Unreachable(why));
                        return Ok(());
                    }
                    // One pause for the whole run, longer for each failure in a row.
                    let pause = self.cfg.retry_pause * (1u32 << (self.streak - 1));
                    self.resume_at = Some(Instant::now() + pause);
                }
                if matches!(
                    op,
                    Op::RemoteCreate { .. }
                        | Op::RemoteUpdate { .. }
                        | Op::RemoteTags { .. }
                        | Op::RemoteDelete { .. }
                ) {
                    // A write whose answer didn't come may have landed: not sent again blind
                    // (a large upload twice), but planned again from the feed, read first.
                    let node = op.node();
                    self.db.write(|s| s.folder().mark_dirty(node))?;
                    self.report.drive_moved = true;
                    self.report.skipped.push((op, Skip::Waiting(why)));
                    return Ok(());
                }
                self.retry_or_fail(op, attempts, why);
            }
            Outcome::Retry(why) => self.retry_or_fail(op, attempts, why),
            Outcome::Skip(why) => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.report.skipped.push((op, why));
            }
            Outcome::Stop(why) => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.report.stopped.get_or_insert(Stopped::Denied(why));
            }
            Outcome::Full => {
                let node = op.node();
                self.db.write(|s| s.folder().mark_dirty(node))?;
                self.full = true;
                self.report.full = true;
            }
            Outcome::Fatal(e) => return Err(e),
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
                    None => (activity::path_of(s, node)?, 0),
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

    fn retry_or_fail(&mut self, op: Op, attempts: u8, why: String) {
        if attempts + 1 < self.cfg.attempts {
            self.queue.push_back((op, attempts + 1));
        } else {
            if let Some(pending) = self.pending(&op) {
                pending.remove(&op.node());
            }
            if matches!(op, Op::RemoteDelete { .. }) {
                self.unremoved.insert(op.node());
            }
            self.report.failed.push((op, why));
        }
    }

    /// Waits out the pause after a transient remote error, if there is one. Only remote work
    /// waits; local operations go on meanwhile.
    fn wait_out_backoff(&mut self) {
        if let Some(at) = self.resume_at.take() {
            std::thread::sleep(at.saturating_duration_since(Instant::now()));
        }
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
        self.wait_out_backoff();
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
            self.records.extend(records.iter().map(|r| r.base.node));
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
                first_error.is_none() && !self.should_stop() && self.resume_at.is_none()
            },
        );
        // What never started leaves no record (one that runs later writes its own).
        if !self.crashed && !pending.is_empty() {
            let unstarted: Vec<NodeId> = pending
                .keys()
                .filter(|n| self.records.contains(*n))
                .copied()
                .collect();
            for n in &unstarted {
                self.records.remove(n);
            }
            let gone = self.db.write(|s| {
                unstarted
                    .iter()
                    .try_for_each(|n| s.folder().delete_placing(*n))
            });
            if first_error.is_none() {
                gone?;
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        if !self.should_stop() {
            for p in pending.into_values().rev() {
                self.queue.push_front((p.op, p.attempts));
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
                        after: remote::After::Removed { file_id },
                        ..
                    },
                    Job::Meta { meta, .. },
                    JobResult::Written(written),
                ) => {
                    ops.push((p.op, p.attempts, p.gone));
                    landed.push(remote::Landed {
                        meta,
                        hash,
                        file_id,
                        written,
                    });
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
            Op::LocalTrash { node, subtree } => self.trash(*node, subtree, false),
            Op::LocalDownload { .. }
            | Op::RemoteCreate { .. }
            | Op::RemoteUpdate { .. }
            | Op::RemoteTags { .. }
            | Op::RemoteDelete { .. } => {
                unreachable!("transfers and remote writes run through flush")
            }
            Op::Settle { .. } => Ok(Outcome::Done),
            Op::LocalDetach { node, new_node } => self.detach(*node, *new_node),
            Op::Forget { node, subtree } if subtree.is_empty() => self.forget_base(*node),
            Op::Forget { node, subtree } => self.trash(*node, subtree, true),
        };
        result.unwrap_or_else(|o| o)
    }

    // ---- paths ----

    /// The local path where (parent, name) lives. Retried while the parent isn't local yet;
    /// skipped if the name can't be spelled on this OS (it stays remote-only and is reported).
    fn target_path(&mut self, parent: NodeId, name: &str) -> Step<RelPath> {
        let parent_path = if parent.is_root() {
            RelPath::root()
        } else {
            match self.db.read(|s| s.local_by_node(parent))? {
                Some(row) if row.kind == NodeKind::Folder => row.path,
                _ => return Err(Outcome::Retry("parent not local yet".into())),
            }
        };
        spell(&parent_path, name, self.folder.os)
            .map_err(|e| Outcome::unsyncable(format!("name can't be used here: {e}")))
    }

    /// Whether the item on disk still matches its row.
    fn on_disk_unchanged(&self, row: &LocalRow) -> bool {
        hbsync_platform::stat(&self.disk.abs(&row.path)).is_ok_and(|meta| matches_row(row, &meta))
    }

    /// Whether everything on disk at `top` is what `rows` recorded (its subtree in the DB, `top`
    /// included), unchanged, apart from filtered items (§10): junk may go along, ignored items
    /// must stay, and the folders they are in are returned. `None` if anything else is there or
    /// changed. One walk, compared with the metadata the listing returns.
    fn subtree_check(&self, top: &LocalRow, rows: &[LocalRow]) -> Option<Vec<RelPath>> {
        if !self.on_disk_unchanged(top) {
            return None;
        }
        let mut expected: HashMap<&RelPath, &LocalRow> = rows
            .iter()
            .filter(|r| r.path != top.path)
            .map(|r| (&r.path, r))
            .collect();
        let mut holding = Vec::new();
        if top.kind == NodeKind::Folder {
            let walk = hbsync_local::Walker::at(
                &self.folder.root,
                top.path.clone(),
                &self.folder.filter,
                None,
            );
            for event in walk {
                match event {
                    hbsync_local::WalkEvent::Entry { path, meta } => {
                        if !expected
                            .remove(&path)
                            .is_some_and(|r| matches_row(r, &meta))
                        {
                            return None;
                        }
                    }
                    hbsync_local::WalkEvent::Skipped {
                        reason: hbsync_local::SkipReason::Junk,
                        ..
                    } => {}
                    hbsync_local::WalkEvent::Skipped {
                        path,
                        reason: hbsync_local::SkipReason::Ignored,
                    } => {
                        // Its own name may be one we can't sync; its folder's we walked.
                        let folder = path.parent()?;
                        holding.push(hbsync_local::journal::relpath_prefix(folder));
                    }
                    // Anything else we never sync (a link, a bad name) would be trashed silently.
                    _ => return None,
                }
            }
        }
        expected.is_empty().then_some(holding)
    }

    /// See [`occupant`].
    fn occupant(&mut self, target: &RelPath) -> Result<Option<LocalRow>, StateError> {
        occupant(self.db, self.folder.case_insensitive, target)
    }

    /// Frees `target` if a node that the plan is about to move away holds it: that node is
    /// parked under a temporary name (recorded in the DB). Returns `false` if it stays taken.
    /// The mover itself may hold it under another case (a case-only rename).
    fn free_target(&mut self, target: &RelPath, mover: Option<NodeId>) -> Result<bool, StateError> {
        let on_disk = fs::symlink_metadata(self.disk.abs(target)).is_ok();
        let Some(occupant) = self.occupant(target)? else {
            return Ok(!on_disk);
        };
        if Some(occupant.node) == mover {
            return Ok(true);
        }
        let leaving = self
            .queue
            .iter()
            .any(|(op, _)| op.vacates_local_name() == Some(occupant.node));
        if !leaving || !self.on_disk_unchanged(&occupant) {
            return Ok(false);
        }
        let parent = occupant.path.parent().unwrap_or_default();
        let Ok(parked) = parent.join(&parked_name(occupant.node)) else {
            return Ok(false);
        };
        // Write-ahead: after a crash between the rename and the row update, the scan finds the
        // item under its temporary name, and this record maps it back to its real place (a
        // record counts only while the item carries that name), so the temporary name can't
        // reach the remote as a rename.
        self.disk
            .write_durable(self.db, |s| s.put_parked(occupant.node, &occupant.path))?;
        if self.disk.rename(self.db, &occupant.path, &parked)?.is_err() {
            self.db.write(|s| s.folder().delete_parked(occupant.node))?;
            return Ok(false);
        }
        if self.crash_here(CrashPoint::AfterParkRename) {
            return Ok(false);
        }
        let refreshed = self.renamed(&occupant, &parked);
        self.db.write(|s| {
            s.folder().move_local_subtree(&occupant.path, &parked)?;
            refreshed
                .as_ref()
                .map_or(Ok(()), |r| s.folder().put_local(r))
        })?;
        Ok(true)
    }
}

mod disk;
mod hooks;
mod local;
mod park;
mod recover;
mod remote;
mod transfer_ops;

#[cfg(test)]
mod tests;

use disk::Disk;
/// Re-exported for the simulator, which names what the executor parks.
pub use hbsync_local::layout::parked_name;
#[cfg(feature = "testkit")]
pub use hooks::Faults;
pub use hooks::{CrashPoint, FailPoint, Hooks};
use park::{numbered_place, occupant, release_place, restore_parked};
use recover::{Applied, Judged, OnDisk, apply, judge, look, read_row};
pub use recover::{UNHASHED_HOLD, record_dirs, recover, recover_placements};
use recover::{complete_fork, download_key};
pub use transfer_ops::tags_lost;

/// Whether `meta` (from disk) is what `row` recorded: folders by kind and file id, files by
/// their full fingerprint.
fn matches_row(row: &LocalRow, meta: &hbsync_platform::LocalMeta) -> bool {
    match row.kind {
        NodeKind::Folder => meta.kind == EntryKind::Dir && meta.file_id == row.fingerprint.file_id,
        NodeKind::File => meta.kind == EntryKind::File && meta.fingerprint() == row.fingerprint,
    }
}

fn rescan(report: &mut ExecReport, path: &RelPath) -> Outcome {
    report.rescan.push(path.parent().unwrap_or_default());
    Outcome::stale(format!("{path} changed on disk"))
}

fn io_outcome(e: io::Error) -> Outcome {
    match classify(&e) {
        IoClass::Locked | IoClass::DiskFull | IoClass::Other => Outcome::Retry(e.to_string()),
        IoClass::Raced => Outcome::Retry(format!("raced: {e}")),
        IoClass::Denied | IoClass::TooLong => Outcome::unsyncable(e.to_string()),
    }
}

fn remote_outcome(e: RemoteError) -> Outcome {
    match e {
        RemoteError::Transient(_) => Outcome::Backoff(e.to_string()),
        // The local source or sink failed: worth another try, but no reason to pause.
        RemoteError::Io(_) => Outcome::Retry(e.to_string()),
        RemoteError::Denied(_) => Outcome::Stop(e.to_string()),
        RemoteError::Full(_) => Outcome::Full,
        // The drive won't take it as it is: retrying the same won't help.
        RemoteError::Fatal(_) | RemoteError::InvalidHash | RemoteError::HashRequired => {
            Outcome::unsyncable(e.to_string())
        }
        // The remote moved on, or rejected the bytes (changed mid-upload), or the file grew
        // shorter than its header said (a read past its end): plan again, from what the feed
        // says it is now.
        RemoteError::AlreadyExists
        | RemoteError::VersionMismatch
        | RemoteError::NotFound
        | RemoteError::Deleted
        | RemoteError::StaleRange(_)
        | RemoteError::HashMismatch => Outcome::stale(e.to_string()),
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
        ..old.clone()
    })
}
