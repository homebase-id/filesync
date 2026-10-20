//! Running transfers in parallel: a bounded worker pool, priorities, and a bandwidth cap.
//!
//! Workers only move bytes (network and temp files). Everything that touches the state DB or
//! places files in the sync root happens on the caller's thread, before ([`Job`] preparation)
//! and after (the `commit` callback), so the DB keeps a single writer.
//!
//! Order: removals first (they move no bytes; a request takes a thousand), then smallest
//! first, so many small files aren't stuck behind one large one.
//!
//! Small files wait on round trips, so many run at once. Large ones wait on bandwidth, so only
//! a few do: more wouldn't move bytes faster, each would just finish later (and an upload cut
//! off starts over).

use std::collections::VecDeque;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hbsync_fileio::{CompletedFile, FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_remote::{Answers, ErrorClass, NodeMeta, Pace, Remote, RemoteError, Upload};
use hbsync_types::{ContentHash, NodeId, RemoteFileId, Target, Written};

use crate::policy::{InRun, policy};

#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    /// Concurrent transfers.
    pub workers: usize,
    /// Concurrent transfers of large files, the rest of the workers being free for small ones.
    pub large_at_once: usize,
    /// Size from which a file counts as large, in bytes.
    pub large_from: u64,
    /// Combined bandwidth cap for all transfers, bytes per second.
    pub max_bytes_per_sec: Option<u64>,
}

impl Default for TransferConfig {
    fn default() -> Self {
        TransferConfig {
            workers: 8,
            large_at_once: 2,
            large_from: 8 << 20,
            max_bytes_per_sec: None,
        }
    }
}

/// A token bucket shared by all workers. Holds at most one second of tokens.
#[derive(Debug)]
pub(crate) struct Throttle {
    rate: f64,
    state: Mutex<(f64, Instant)>,
}

impl Throttle {
    pub(crate) fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        Throttle {
            rate,
            state: Mutex::new((0.0, Instant::now())),
        }
    }

    /// Blocks until `n` bytes may pass.
    pub(crate) fn take(&self, n: usize) {
        let wait = {
            let Ok(mut s) = self.state.lock() else { return };
            let now = Instant::now();
            let (tokens, last) = &mut *s;
            *tokens =
                (*tokens + now.duration_since(*last).as_secs_f64() * self.rate).min(self.rate);
            *last = now;
            *tokens -= n as f64;
            if *tokens < 0.0 {
                Duration::from_secs_f64(-*tokens / self.rate)
            } else {
                Duration::ZERO
            }
        };
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }
}

/// What every byte of a transfer goes through: the bandwidth cap, if any, and the count of
/// bytes moved, if anyone watches.
#[derive(Debug, Default)]
pub(crate) struct Meter<'a> {
    pub throttle: Option<Throttle>,
    pub moved: Option<&'a AtomicU64>,
}

/// Uploads: the remote paces what it sends.
impl Pace for Meter<'_> {
    fn take(&self, bytes: usize) {
        if let Some(t) = &self.throttle {
            t.take(bytes);
        }
        if let Some(moved) = self.moved {
            moved.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}

/// A writer whose bytes go through a [`Meter`].
#[derive(Debug)]
pub(crate) struct MeteredWrite<'a, W> {
    pub inner: W,
    pub meter: &'a Meter<'a>,
}

impl<W: Write> Write for MeteredWrite<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let max = buf.len().min(64 * 1024);
        self.meter.take(max);
        self.inner.write(&buf[..max])
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Bytes of a download gathered before they are written to its temp file.
const DOWNLOAD_BUFFER: usize = 64 * 1024;

/// A download: the payload of `file_id`, verified against `expected`, into the temp folder.
#[derive(Debug, Clone)]
pub struct Download {
    pub node: NodeId,
    pub file_id: RemoteFileId,
    pub expected: ContentHash,
    pub size: u64,
    /// Applied to the temp file before it becomes visible; `None` (a file the drive has no
    /// date for) leaves the file's own.
    pub mtime_ms: Option<i64>,
    pub executable: Option<bool>,
    pub tmp_dir: PathBuf,
    /// Identifies the exact content, so a partial file is only resumed for the same bytes.
    pub key: String,
    /// The hash the `local` rows keep (computed too if the remote uses another algorithm).
    pub local_algorithm: HashAlgorithm,
    /// Files on this volume that may hold the bytes already (in the folder's trash: a delete
    /// taken back), in the order to try them: one that holds exactly `expected` is taken
    /// instead of a transfer ([`CompletedFile::adopt`]).
    pub from_trash: Vec<PathBuf>,
}

/// The byte-moving half of a transfer.
#[derive(Debug, Clone)]
pub enum Job {
    Download(Download),
    /// Uploads the file at `path` as `meta`: a create, or with `target` an update of that
    /// file at that version.
    Upload {
        meta: NodeMeta,
        target: Option<Target>,
        path: PathBuf,
        hash: ContentHash,
        size: u64,
    },
    /// Writes `meta` without bytes: a create, or with `target` an update of that node at that
    /// version (a folder restored, a rename or move, tags, a removal).
    Meta {
        meta: NodeMeta,
        target: Option<Target>,
    },
}

impl Job {
    pub fn node(&self) -> NodeId {
        match self {
            Job::Download(d) => d.node,
            Job::Upload { meta, .. } | Job::Meta { meta, .. } => meta.node,
        }
    }

    /// The file it reads or writes at a version it knows; `None` for a create.
    pub(crate) fn file_id(&self) -> Option<RemoteFileId> {
        match self {
            Job::Download(d) => Some(d.file_id),
            Job::Upload { target, .. } | Job::Meta { target, .. } => {
                target.as_ref().map(|t| t.at.file_id)
            }
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Job::Download(Download { size, .. }) | Job::Upload { size, .. } => *size,
            Job::Meta { .. } => 0,
        }
    }

    /// The file a removal (to Recently deleted) removes, at the version it knows: a write of the
    /// removal alone (`prepare_remove` writes the node as the mirror has it), which goes with
    /// others in one request ([`Remote::set_removed`]).
    /// A node of the part of the drive a role folder's engine leaves out isn't one: its write
    /// goes to the remote's guard as an update ([`NodeId::OUTSIDE`]).
    fn removal(&self) -> Option<&Target> {
        match self {
            Job::Meta {
                meta,
                target: Some(target),
            } if meta.removed && ![meta.node, meta.parent].contains(&NodeId::OUTSIDE) => {
                Some(target)
            }
            _ => None,
        }
    }
}

/// Removals sent in one request: as many as Homebase takes in one. A level of more goes in
/// requests that run at once, on the workers.
pub(crate) const REMOVALS_AT_ONCE: usize = 1000;

/// What a worker takes on: a job, or removals sent together.
enum Task {
    One(Box<Job>),
    Removals(Vec<Job>),
}

/// What a job produced.
#[derive(Debug)]
pub enum JobResult {
    Downloaded {
        file: CompletedFile,
        local_hash: ContentHash,
    },
    /// An upload or a [`Job::Meta`] landed.
    Written(Written),
    Failed(JobError),
}

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error(transparent)]
    File(#[from] FileIoError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl JobError {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            JobError::Remote(e) => e.class(),
            JobError::File(e) => e.class(),
            JobError::Io(e) => ErrorClass::of_io(e),
        }
    }
}

/// Runs `jobs` on a worker pool: removals together, then smaller jobs first. `commit` is called
/// on this thread for every finished job, in completion order. A small download's file isn't
/// durable yet: `hbsync_fileio::sync_completed` before putting it in place.
pub fn run_jobs(
    remote: &dyn Remote,
    cfg: TransferConfig,
    jobs: Vec<Job>,
    mut commit: impl FnMut(Job, JobResult),
) {
    run_jobs_while(remote, cfg, None, None, jobs, |ran| {
        for (job, result) in ran {
            commit(job, result);
        }
        true
    });
}

/// [`run_jobs`], until `commit` returns `false`, `halt` is set, or the remote is out (a job
/// failed for now, [`InRun::Backoff`]: the next would too). Jobs not
/// started by then are dropped; jobs already running finish and are committed. After the
/// drive said it is full ([`InRun::HoldUploads`]), the uploads not started are dropped. `commit` is given every job that
/// finished since the last call, each with its result (removals answered by one request
/// together, to record in one go). A small download's file may not be durable yet: the
/// caller makes it so before putting it in place (`hbsync_fileio::sync_completed`). The bytes
/// moved are added to `moved`.
pub(crate) fn run_jobs_while(
    remote: &dyn Remote,
    cfg: TransferConfig,
    moved: Option<&AtomicU64>,
    halt: Option<&AtomicBool>,
    jobs: Vec<Job>,
    mut commit: impl FnMut(Vec<(Job, JobResult)>) -> bool,
) {
    let (removals, mut jobs): (Vec<Job>, Vec<Job>) =
        jobs.into_iter().partition(|j| j.removal().is_some());
    jobs.sort_by_key(Job::size);
    // Removals first, a request for each thousand: they move no bytes.
    let mut tasks = VecDeque::new();
    let mut removals = removals.into_iter();
    loop {
        let batch: Vec<Job> = removals.by_ref().take(REMOVALS_AT_ONCE).collect();
        if batch.is_empty() {
            break;
        }
        tasks.push_back(Task::Removals(batch));
    }
    tasks.extend(jobs.into_iter().map(|j| Task::One(Box::new(j))));
    let workers = cfg.workers.min(tasks.len()).max(1);
    let queue = Mutex::new(Queue {
        jobs: tasks,
        large: 0,
        large_from: cfg.large_from,
        large_at_once: cfg.large_at_once.max(1),
        uploads_dropped: false,
    });
    let meter = Meter {
        throttle: cfg.max_bytes_per_sec.map(Throttle::new),
        moved,
    };
    let out = AtomicBool::new(false);
    let (tx, rx) = mpsc::channel::<Vec<(Job, JobResult)>>();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let queue = &queue;
            let meter = &meter;
            let out = &out;
            scope.spawn(move || {
                // Workers take the next job without waiting for the commits: a halt or an
                // outage is seen here, or fast jobs would all have started before it counted.
                while !out.load(Ordering::Relaxed)
                    && !halt.is_some_and(|h| h.load(Ordering::Relaxed))
                    && let Some(task) = queue.lock().ok().and_then(|mut q| q.take())
                {
                    let results = match task {
                        Task::One(job) => {
                            let result = run_job(remote, &job, meter);
                            if let Ok(mut q) = queue.lock() {
                                q.finished(&job);
                            }
                            vec![(*job, result)]
                        }
                        Task::Removals(jobs) => remove(remote, jobs),
                    };
                    let failed = results.iter().filter_map(|(_, result)| match result {
                        JobResult::Failed(e) => Some(policy(e.class()).in_run),
                        _ => None,
                    });
                    for in_run in failed {
                        match in_run {
                            // The next would fail the same way.
                            InRun::Backoff => out.store(true, Ordering::Relaxed),
                            // Out of storage: no other upload sends its bytes to hear so too
                            // (they go back to the caller, which holds them).
                            InRun::HoldUploads => {
                                if let Ok(mut q) = queue.lock() {
                                    q.drop_uploads();
                                }
                            }
                            _ => {}
                        }
                    }
                    if tx.send(results).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        // Everything that finished meanwhile, committed together (downloads share one
        // barrier for their bytes).
        while let Ok(mut ran) = rx.recv() {
            ran.extend(rx.try_iter().flatten());
            if !commit(ran)
                && let Ok(mut q) = queue.lock()
            {
                q.jobs.clear();
            }
        }
    });
}

/// Jobs not started yet, and how many large ones are running. `jobs` are the removals, then
/// the rest smallest first, as [`run_jobs_while`] queues them; none are added later
/// ([`Queue::take`] relies on it).
struct Queue {
    jobs: VecDeque<Task>,
    large: usize,
    large_from: u64,
    large_at_once: usize,
    uploads_dropped: bool,
}

impl Queue {
    fn is_large(&self, job: &Job) -> bool {
        job.size() >= self.large_from
    }

    /// The next job, unless it is large and the large ones' slots are all taken. Jobs go
    /// smallest first, so then only large ones are left: the workers holding the slots take
    /// them on, so the one asking can end.
    fn take(&mut self) -> Option<Task> {
        if let Some(Task::One(job)) = self.jobs.front()
            && self.is_large(job)
        {
            if self.large >= self.large_at_once {
                return None;
            }
            self.large += 1;
        }
        self.jobs.pop_front()
    }

    /// Drops the uploads not started (once: none are added later).
    fn drop_uploads(&mut self) {
        if !std::mem::replace(&mut self.uploads_dropped, true) {
            self.jobs
                .retain(|t| !matches!(t, Task::One(job) if matches!(**job, Job::Upload { .. })));
        }
    }

    fn finished(&mut self, job: &Job) {
        self.large -= usize::from(self.is_large(job));
    }
}

/// Writes `meta` (and `upload`): a create, or with `target` an update at that version.
fn write(
    remote: &dyn Remote,
    meta: &NodeMeta,
    target: Option<&Target>,
    upload: Option<Upload<'_>>,
) -> Result<Written, RemoteError> {
    match target {
        None => remote.create(meta, upload),
        Some(t) => remote.update(t, meta, upload),
    }
}

fn run_job(remote: &dyn Remote, job: &Job, meter: &Meter<'_>) -> JobResult {
    let result = match job {
        Job::Download(d) => download(remote, d, meter),
        Job::Upload {
            meta,
            target,
            path,
            hash,
            size,
        } => (|| {
            let mut file = std::fs::File::open(path)?;
            // Changed size since it was hashed: no use sending it to be refused.
            if file.metadata()?.len() != *size {
                return Err(JobError::Remote(RemoteError::HashMismatch));
            }
            let upload = Some(Upload {
                hash: *hash,
                size: *size,
                reader: &mut file,
                pace: Some(meter),
            });
            Ok(JobResult::Written(write(
                remote,
                meta,
                target.as_ref(),
                upload,
            )?))
        })(),
        Job::Meta { meta, target } => write(remote, meta, target.as_ref(), None)
            .map(JobResult::Written)
            .map_err(JobError::Remote),
    };
    result.unwrap_or_else(JobResult::Failed)
}

/// Removes the files of `jobs` (removals, [`Job::removal`]) in one call: each job with its file's
/// answer. Those a stopped call left unanswered fail as it did: such a call may have written
/// them (an answer lost), so they are planned again, never sent again blind.
fn remove(remote: &dyn Remote, jobs: Vec<Job>) -> Vec<(Job, JobResult)> {
    let files: Vec<Target> = jobs.iter().filter_map(Job::removal).cloned().collect();
    let (answers, _) = remote
        .set_removed(&files, true)
        .unwrap_or_else(Answers::stopped)
        .padded(jobs.len());
    jobs.into_iter()
        .zip(answers)
        .map(|(job, answer)| {
            let result = answer.map_or_else(|e| JobResult::Failed(e.into()), JobResult::Written);
            (job, result)
        })
        .collect()
}

fn download(remote: &dyn Remote, d: &Download, meter: &Meter<'_>) -> Result<JobResult, JobError> {
    let file = match from_trash(d) {
        Some(file) => file,
        None => fetch(remote, d, meter)?,
    };
    let mtime_ns = d.mtime_ms.map(|ms| ms.saturating_mul(1_000_000));
    file.set_attributes(mtime_ns, d.executable)?;
    let local_hash = if d.expected.algorithm == d.local_algorithm {
        d.expected
    } else {
        hash_file(file.path(), d.local_algorithm)?.hash
    };
    Ok(JobResult::Downloaded { file, local_hash })
}

/// `d`'s bytes from the drive, into its partial file (resumed where an earlier try stopped).
fn fetch(remote: &dyn Remote, d: &Download, meter: &Meter<'_>) -> Result<CompletedFile, JobError> {
    let mut partial = PartialFile::open(&d.tmp_dir, &d.key, d.expected.algorithm)?;
    let offset = partial.offset();
    // The size is known, so always name the range: a remote needn't look it up first.
    if offset < d.size {
        let range = Some(offset..d.size);
        // A body comes a few KB per read (a TLS record or less): each piece written to the
        // file on its own was a system call and a change for the folder's watcher. Whatever
        // ends the read, what came reaches the file (on an error, as the buffer is dropped).
        let mut file = BufWriter::with_capacity(DOWNLOAD_BUFFER, &mut partial);
        let mut sink = MeteredWrite {
            inner: &mut file,
            meter,
        };
        remote.read(d.file_id, range, &mut sink)?;
        file.flush()?;
    }
    // Made durable with the others that finished meanwhile, before any is put in place
    // (`sync_completed`, in the commit).
    Ok(partial.finish_in_group(d.expected, d.size)?)
}

/// The first of `d`'s trash files that holds exactly the bytes it wants, taken for it where it
/// is ([`CompletedFile::adopt`]); none where none holds them.
fn from_trash(d: &Download) -> Option<CompletedFile> {
    d.from_trash.iter().find_map(|src| {
        CompletedFile::adopt(src, d.expected, d.size, d.mtime_ms)
            .inspect_err(|e| tracing::debug!("{} not taken from the trash: {e}", src.display()))
            .ok()
            .flatten()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_paces_bytes() {
        let throttle = Throttle::new(200_000);
        let start = Instant::now();
        for _ in 0..10 {
            throttle.take(10_000);
        }
        // 100 kB at 200 kB/s ≈ 0.5 s (the bucket starts empty).
        assert!(
            start.elapsed() >= Duration::from_millis(400),
            "{:?}",
            start.elapsed()
        );
    }
}
