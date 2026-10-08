//! Running transfers in parallel: a bounded worker pool, priorities, and a bandwidth cap.
//!
//! Workers only move bytes (network and temp files). Everything that touches the state DB or
//! places files in the sync root happens on the caller's thread, before ([`Job`] preparation)
//! and after (the `commit` callback), so the DB keeps a single writer.
//!
//! Order: jobs marked urgent (a file the user is waiting for) first, then smallest first, so
//! many small files aren't stuck behind one large one.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hbsync_fileio::{CompletedFile, FileIoError, HashAlgorithm, PartialFile, hash_file};
use hbsync_remote::{NewNode, Remote, RemoteError, Upload, Written};
use hbsync_types::schema::NodeContent;
use hbsync_types::{ContentHash, NodeId, RemoteFileId, VersionTag};

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

/// A reader that respects a [`Throttle`].
#[derive(Debug)]
pub struct ThrottledRead<'a, R> {
    pub inner: R,
    pub throttle: Option<&'a Throttle>,
}

impl<R: Read> Read for ThrottledRead<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Small slices keep the pacing smooth.
        let max = buf.len().min(64 * 1024);
        let n = self.inner.read(&mut buf[..max])?;
        if let Some(t) = self.throttle {
            t.take(n);
        }
        Ok(n)
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
        /// The hash `local_items` keeps (computed too if the remote uses another algorithm).
        local_algorithm: HashAlgorithm,
    },
    Create {
        node: NodeId,
        new: NewNode,
        path: PathBuf,
        hash: ContentHash,
        size: u64,
    },
    Update {
        node: NodeId,
        file_id: RemoteFileId,
        expected: VersionTag,
        parent: NodeId,
        content: NodeContent,
        path: PathBuf,
        hash: ContentHash,
        size: u64,
    },
}

impl Job {
    pub fn node(&self) -> NodeId {
        match self {
            Job::Download { node, .. } | Job::Create { node, .. } | Job::Update { node, .. } => {
                *node
            }
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Job::Download { size, .. } | Job::Create { size, .. } | Job::Update { size, .. } => {
                *size
            }
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
    Uploaded(Written),
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
            // A resumable download that didn't verify was deleted; a fresh try may succeed.
            JobError::File(FileIoError::HashMismatch { .. } | FileIoError::SizeMismatch { .. }) => {
                true
            }
            JobError::File(_) | JobError::Io(_) => true,
        }
    }
}

/// Runs `jobs` on a worker pool. `urgent` jobs go first, then smaller ones. `commit` is called
/// on this thread for every finished job, in completion order.
pub fn run_jobs(
    remote: &dyn Remote,
    cfg: TransferConfig,
    mut jobs: Vec<(bool, Job)>,
    mut commit: impl FnMut(Job, JobResult),
) {
    jobs.sort_by_key(|(urgent, job)| (!*urgent, job.size()));
    let queue: Mutex<VecDeque<Job>> = Mutex::new(jobs.into_iter().map(|(_, j)| j).collect());
    let throttle = cfg.max_bytes_per_sec.map(|r| Arc::new(Throttle::new(r)));
    let (tx, rx) = mpsc::channel::<(Job, JobResult)>();
    std::thread::scope(|scope| {
        for _ in 0..cfg.workers.max(1) {
            let tx = tx.clone();
            let queue = &queue;
            let throttle = throttle.clone();
            scope.spawn(move || {
                while let Some(job) = queue.lock().ok().and_then(|mut q| q.pop_front()) {
                    let result = run_job(remote, &job, throttle.as_deref());
                    if tx.send((job, result)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        for (job, result) in rx {
            commit(job, result);
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
        Job::Create {
            new,
            path,
            hash,
            size,
            ..
        } => (|| {
            let file = std::fs::File::open(path)?;
            let mut reader = ThrottledRead {
                inner: file,
                throttle,
            };
            Ok(JobResult::Uploaded(remote.create(
                new,
                Some(Upload {
                    hash: *hash,
                    size: *size,
                    reader: &mut reader,
                }),
            )?))
        })(),
        Job::Update {
            file_id,
            expected,
            parent,
            content,
            path,
            hash,
            size,
            ..
        } => (|| {
            let file = std::fs::File::open(path)?;
            let mut reader = ThrottledRead {
                inner: file,
                throttle,
            };
            let upload = Upload {
                hash: *hash,
                size: *size,
                reader: &mut reader,
            };
            Ok(JobResult::Uploaded(remote.update(
                *file_id,
                *expected,
                *parent,
                content,
                Some(upload),
            )?))
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
    // Resume where an earlier attempt stopped.
    if offset < size {
        let range = (offset > 0).then_some(offset..size);
        let mut sink = ThrottledWrite {
            inner: PartialSink(&mut partial),
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

/// Adapts a [`PartialFile`] to `io::Write`.
struct PartialSink<'a>(&'a mut PartialFile);

impl Write for PartialSink<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_all(buf)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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
