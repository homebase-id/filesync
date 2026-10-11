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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hbsync_fileio::{CompletedFile, FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_remote::{NodeMeta, Pace, Remote, RemoteError, Upload};
use hbsync_types::{ContentHash, NodeId, RemoteFileId, RemoteRef};

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

/// The byte-moving half of a transfer.
#[derive(Debug, Clone)]
pub enum Job {
    Download {
        node: NodeId,
        file_id: RemoteFileId,
        expected: ContentHash,
        size: u64,
        /// Applied to the temp file before it becomes visible.
        mtime_ms: i64,
        executable: Option<bool>,
        tmp_dir: PathBuf,
        /// Identifies the exact content, so a partial file is only resumed for the same bytes.
        key: String,
        /// The hash the `local` rows keep (computed too if the remote uses another algorithm).
        local_algorithm: HashAlgorithm,
    },
    /// Uploads the file at `path` as `meta`: a create, or with `target` an update of that
    /// file at that version.
    Upload {
        meta: NodeMeta,
        target: Option<RemoteRef>,
        path: PathBuf,
        hash: ContentHash,
        size: u64,
    },
}

impl Job {
    pub fn node(&self) -> NodeId {
        match self {
            Job::Download { node, .. } => *node,
            Job::Upload { meta, .. } => meta.node,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Job::Download { size, .. } | Job::Upload { size, .. } => *size,
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
    Uploaded(RemoteRef),
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
    run_jobs_while(remote, cfg, None, jobs, |job, result| {
        commit(job, result);
        true
    });
}

/// [`run_jobs`], until `commit` returns `false`: jobs not started by then are dropped (jobs
/// already running finish and are committed). The bytes moved are added to `moved`.
pub fn run_jobs_while(
    remote: &dyn Remote,
    cfg: TransferConfig,
    moved: Option<&AtomicU64>,
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
    let (tx, rx) = mpsc::channel::<(Job, JobResult)>();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let queue = &queue;
            let meter = &meter;
            scope.spawn(move || {
                while let Some(job) = queue.lock().ok().and_then(|mut q| q.take()) {
                    let result = run_job(remote, &job, meter);
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

fn run_job(remote: &dyn Remote, job: &Job, meter: &Meter<'_>) -> JobResult {
    let result = match job {
        Job::Download {
            file_id,
            expected,
            size,
            mtime_ms,
            executable,
            tmp_dir,
            key,
            local_algorithm,
            ..
        } => download(
            remote,
            *file_id,
            *expected,
            *size,
            *mtime_ms,
            *executable,
            tmp_dir,
            key,
            *local_algorithm,
            meter,
        ),
        Job::Upload {
            meta,
            target,
            path,
            hash,
            size,
        } => (|| {
            let mut file = std::fs::File::open(path)?;
            let upload = Some(Upload {
                hash: *hash,
                size: *size,
                reader: &mut file,
                pace: Some(meter),
            });
            Ok(JobResult::Uploaded(match target {
                None => remote.create(meta, upload)?,
                Some(at) => remote.update(at.file_id, at.version, meta, upload)?,
            }))
        })(),
    };
    result.unwrap_or_else(JobResult::Failed)
}

#[allow(clippy::too_many_arguments)]
fn download(
    remote: &dyn Remote,
    file_id: RemoteFileId,
    expected: ContentHash,
    size: u64,
    mtime_ms: i64,
    executable: Option<bool>,
    tmp_dir: &std::path::Path,
    key: &str,
    local_algorithm: HashAlgorithm,
    meter: &Meter<'_>,
) -> Result<JobResult, JobError> {
    let mut partial = PartialFile::open(tmp_dir, key, expected.algorithm)?;
    let offset = partial.offset();
    // Resume where an earlier attempt stopped. The size is known, so always name the range:
    // a remote needn't look it up first.
    if offset < size {
        let range = Some(offset..size);
        let mut sink = MeteredWrite {
            inner: &mut partial,
            meter,
        };
        remote.read(file_id, range, &mut sink)?;
    }
    let file = partial.finish(Some(expected), Some(size))?;
    file.set_attributes(Some(mtime_ms.saturating_mul(1_000_000)), executable)?;
    let local_hash = if expected.algorithm == local_algorithm {
        expected
    } else {
        hash_file(file.path(), local_algorithm)?.hash
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
