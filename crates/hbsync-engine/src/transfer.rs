//! Running transfers in parallel: a bounded worker pool, priorities, and a bandwidth cap.
//!
//! Workers only move bytes (network and temp files). Everything that touches the state DB or
//! places files in the sync root happens on the caller's thread, before ([`Job`] preparation)
//! and after (the `commit` callback), so the DB keeps a single writer.
//!
//! Order: jobs marked urgent (a file the user is waiting for) first, then smallest first, so
//! many small files aren't stuck behind one large one.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hbsync_fileio::{CompletedFile, FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_remote::{NodeMeta, Pace, Remote, RemoteError, Upload};
use hbsync_types::{ContentHash, NodeId, RemoteFileId, RemoteRef};

#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    /// Concurrent transfers.
    pub workers: usize,
    /// Combined bandwidth cap for all transfers, bytes per second.
    pub max_bytes_per_sec: Option<u64>,
}

impl Default for TransferConfig {
    fn default() -> Self {
        TransferConfig {
            workers: 4,
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

/// Uploads: the remote paces what it sends.
impl Pace for Throttle {
    fn take(&self, bytes: usize) {
        Throttle::take(self, bytes);
    }
}

/// A writer that respects a [`Throttle`].
#[derive(Debug)]
pub struct ThrottledWrite<'a, W> {
    pub inner: W,
    pub throttle: Option<&'a Throttle>,
}

impl<W: Write> Write for ThrottledWrite<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let max = buf.len().min(64 * 1024);
        if let Some(t) = self.throttle {
            t.take(max);
        }
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
    run_jobs_while(remote, cfg, jobs, |job, result| {
        commit(job, result);
        true
    });
}

/// [`run_jobs`], until `commit` returns `false`: jobs not started by then are dropped (jobs
/// already running finish and are committed).
pub fn run_jobs_while(
    remote: &dyn Remote,
    cfg: TransferConfig,
    mut jobs: Vec<(bool, Job)>,
    mut commit: impl FnMut(Job, JobResult) -> bool,
) {
    jobs.sort_by_key(|(urgent, job)| (!*urgent, job.size()));
    let queue: Mutex<VecDeque<Job>> = Mutex::new(jobs.into_iter().map(|(_, j)| j).collect());
    let throttle = cfg.max_bytes_per_sec.map(Throttle::new);
    let (tx, rx) = mpsc::channel::<(Job, JobResult)>();
    std::thread::scope(|scope| {
        for _ in 0..cfg.workers.max(1) {
            let tx = tx.clone();
            let queue = &queue;
            let throttle = throttle.as_ref();
            scope.spawn(move || {
                while let Some(job) = queue.lock().ok().and_then(|mut q| q.pop_front()) {
                    let result = run_job(remote, &job, throttle);
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
                q.clear();
            }
        }
    });
}

fn run_job(remote: &dyn Remote, job: &Job, throttle: Option<&Throttle>) -> JobResult {
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
            throttle,
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
                pace: throttle.map(|t| t as &dyn Pace),
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
    throttle: Option<&Throttle>,
) -> Result<JobResult, JobError> {
    let mut partial = PartialFile::open(tmp_dir, key, expected.algorithm)?;
    let offset = partial.offset();
    // Resume where an earlier attempt stopped. The size is known, so always name the range:
    // a remote needn't look it up first.
    if offset < size {
        let range = Some(offset..size);
        let mut sink = ThrottledWrite {
            inner: &mut partial,
            throttle,
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
