//! Running transfers in parallel: a bounded worker pool, priorities, and a bandwidth cap.
//!
//! Workers only move bytes (network and temp files). Everything that touches the state DB or
//! places files in the sync root happens on the caller's thread, before ([`Job`] preparation)
//! and after (the `commit` callback), so the DB keeps a single writer.
//!
//! Order: jobs marked urgent (a file the user is waiting for) first, then smallest first, so
//! many small files aren't stuck behind one large one.
//!
//! Small files wait on round trips, so many run at once. Large ones wait on bandwidth, so only
//! a few do: more wouldn't move bytes faster, each would just finish later (and an upload cut
//! off starts over).

use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hbsync_fileio::{CompletedFile, FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_remote::{NodeMeta, Pace, Remote, RemoteError, Upload};
use hbsync_state::MirrorRow;
use hbsync_types::{ContentHash, NodeId, RemoteFileId, RemoteRef, Seal};

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
pub struct Throttle {
    rate: f64,
    state: Mutex<(f64, Instant)>,
}

impl Throttle {
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        Throttle {
            rate,
            state: Mutex::new((0.0, Instant::now())),
        }
    }

    /// Blocks until `n` bytes may pass.
    pub fn take(&self, n: usize) {
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
pub struct Meter<'a> {
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
pub struct MeteredWrite<'a, W> {
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

/// A download: the payload of `file_id`, verified against `expected`, into the temp folder.
#[derive(Debug, Clone)]
pub struct Download {
    pub node: NodeId,
    pub file_id: RemoteFileId,
    pub expected: ContentHash,
    pub size: u64,
    /// Applied to the temp file before it becomes visible.
    pub mtime_ms: i64,
    pub executable: Option<bool>,
    pub tmp_dir: PathBuf,
    /// Identifies the exact content, so a partial file is only resumed for the same bytes.
    pub key: String,
    /// The hash the `local` rows keep (computed too if the remote uses another algorithm).
    pub local_algorithm: HashAlgorithm,
}

/// The file a write updates: where it is and the version it must still be at, and that file's
/// [`Seal`] if known (never another file's: it would be written under the wrong key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
    pub at: RemoteRef,
    pub seal: Option<Seal>,
}

impl Target {
    /// The file `row` is, at the version it has.
    pub fn of(row: &MirrorRow) -> Target {
        Target {
            at: row.remote_ref(),
            seal: row.seal,
        }
    }

    /// File `at`, with the seal `row` has if `row` is that file.
    pub fn at(at: RemoteRef, row: Option<&MirrorRow>) -> Target {
        Target {
            at,
            seal: row.filter(|r| r.file_id == at.file_id).and_then(|r| r.seal),
        }
    }
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

    pub fn size(&self) -> u64 {
        match self {
            Job::Download(Download { size, .. }) | Job::Upload { size, .. } => *size,
            Job::Meta { .. } => 0,
        }
    }
}

/// What a job produced.
#[derive(Debug)]
pub enum JobResult {
    Downloaded {
        file: CompletedFile,
        local_hash: ContentHash,
    },
    /// An upload or a [`Job::Meta`] landed.
    Written(RemoteRef),
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
    /// Worth trying again soon.
    pub fn is_transient(&self) -> bool {
        match self {
            JobError::Remote(e) => e.is_transient() || matches!(e, RemoteError::Io(_)),
            // Local trouble (locked, changing) passes; a download that didn't verify was
            // deleted, so a fresh try may succeed.
            JobError::File(_) | JobError::Io(_) => true,
        }
    }
}

/// Runs `jobs` on a worker pool. `urgent` jobs go first, then smaller ones. `commit` is called
/// on this thread for every finished job, in completion order.
pub fn run_jobs(
    remote: &dyn Remote,
    cfg: TransferConfig,
    jobs: Vec<(bool, Job)>,
    mut commit: impl FnMut(Job, JobResult),
) {
    run_jobs_while(remote, cfg, None, None, jobs, |job, result| {
        commit(job, result);
        true
    });
}

/// [`run_jobs`], until `commit` returns `false`, `halt` is set, or the remote is out (a job
/// failed with [`RemoteError::Transient`]: the next would too). Jobs not started by then are
/// dropped; jobs already running finish and are committed. The bytes moved are added to
/// `moved`.
pub fn run_jobs_while(
    remote: &dyn Remote,
    cfg: TransferConfig,
    moved: Option<&AtomicU64>,
    halt: Option<&AtomicBool>,
    mut jobs: Vec<(bool, Job)>,
    mut commit: impl FnMut(Job, JobResult) -> bool,
) {
    jobs.sort_by_key(|(urgent, job)| (!*urgent, job.size()));
    let workers = cfg.workers.min(jobs.len()).max(1);
    let queue = Mutex::new(Queue {
        jobs: jobs.into_iter().map(|(_, j)| j).collect(),
        large: 0,
        large_from: cfg.large_from,
        large_at_once: cfg.large_at_once.max(1),
    });
    let meter = Meter {
        throttle: cfg.max_bytes_per_sec.map(Throttle::new),
        moved,
    };
    let out = AtomicBool::new(false);
    let (tx, rx) = mpsc::channel::<(Job, JobResult)>();
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
                    && let Some(job) = queue.lock().ok().and_then(|mut q| q.take())
                {
                    let result = run_job(remote, &job, meter);
                    if let JobResult::Failed(JobError::Remote(RemoteError::Transient(_))) = &result
                    {
                        out.store(true, Ordering::Relaxed);
                    }
                    if let Ok(mut q) = queue.lock() {
                        q.finished(&job);
                    }
                    if tx.send((job, result)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        for (job, result) in rx {
            if !commit(job, result)
                && let Ok(mut q) = queue.lock()
            {
                q.jobs.clear();
            }
        }
    });
}

/// Jobs not started yet, and how many large ones are running.
struct Queue {
    jobs: VecDeque<Job>,
    large: usize,
    large_from: u64,
    large_at_once: usize,
}

impl Queue {
    fn is_large(&self, job: &Job) -> bool {
        job.size() >= self.large_from
    }

    /// The first job that may start: a small one, or a large one while a slot is free. `None`
    /// when only large ones are left and their slots are all taken: the workers holding the
    /// slots take them on, so the one asking can end.
    fn take(&mut self) -> Option<Job> {
        let free = self.large < self.large_at_once;
        let i = self.jobs.iter().position(|j| free || !self.is_large(j))?;
        let job = self.jobs.remove(i)?;
        self.large += usize::from(self.is_large(&job));
        Some(job)
    }

    fn finished(&mut self, job: &Job) {
        self.large -= usize::from(self.is_large(job));
    }
}

/// Writes `meta` (and `upload`): a create, or with `target` an update at that version.
fn write(
    remote: &dyn Remote,
    meta: &NodeMeta,
    target: Option<Target>,
    upload: Option<Upload<'_>>,
) -> Result<RemoteRef, RemoteError> {
    match target {
        None => remote.create(meta, upload),
        Some(t) => remote.update(t.at.file_id, t.at.version, t.seal, meta, upload),
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
            Ok(JobResult::Written(write(remote, meta, *target, upload)?))
        })(),
        Job::Meta { meta, target } => write(remote, meta, *target, None)
            .map(JobResult::Written)
            .map_err(JobError::Remote),
    };
    result.unwrap_or_else(JobResult::Failed)
}

fn download(remote: &dyn Remote, d: &Download, meter: &Meter<'_>) -> Result<JobResult, JobError> {
    let mut partial = PartialFile::open(&d.tmp_dir, &d.key, d.expected.algorithm)?;
    let offset = partial.offset();
    // Resume where an earlier attempt stopped. The size is known, so always name the range:
    // a remote needn't look it up first.
    if offset < d.size {
        let range = Some(offset..d.size);
        let mut sink = MeteredWrite {
            inner: &mut partial,
            meter,
        };
        remote.read(d.file_id, range, &mut sink)?;
    }
    let file = partial.finish(Some(d.expected), Some(d.size))?;
    file.set_attributes(Some(d.mtime_ms.saturating_mul(1_000_000)), d.executable)?;
    let local_hash = if d.expected.algorithm == d.local_algorithm {
        d.expected
    } else {
        hash_file(file.path(), d.local_algorithm)?.hash
    };
    Ok(JobResult::Downloaded { file, local_hash })
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
