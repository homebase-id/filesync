//! Reading and writing whole files safely.
//!
//! - [`hash_file`] hashes a file and fails if it changed while being read, so a hash always
//!   belongs to one consistent version of the file.
//! - [`PartialFile`] receives a download into a temp file next to the sync root, can resume
//!   after a crash or a dropped connection, verifies the hash, and is then moved into place with
//!   [`CompletedFile::persist_new`] (never overwrites) or [`CompletedFile::persist_over`].

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use hbsync_platform::{Fingerprint, LocalMeta, ops};

use crate::hash::{ContentHash, HashAlgorithm, Hasher, feed};

#[derive(Debug, thiserror::Error)]
pub enum FileIoError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("file changed while it was being read")]
    ChangedWhileReading,
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error("size mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("target changed since it was planned")]
    TargetChanged,
}

/// A file's hash plus the metadata it was taken under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashedFile {
    pub hash: ContentHash,
    pub size: u64,
    pub meta: LocalMeta,
}

/// Hashes the file at `path` with constant memory.
///
/// Fails with [`FileIoError::ChangedWhileReading`] if the fingerprint differs before and after
/// the read, or the byte count doesn't match the size.
pub fn hash_file(path: &Path, algorithm: HashAlgorithm) -> Result<HashedFile, FileIoError> {
    let before = hbsync_platform::stat(path)?;
    let file = File::open(path)?;
    let (hash, size) = crate::hash::hash_reader(file, algorithm)?;
    let after = hbsync_platform::stat(path)?;
    if before.fingerprint() != after.fingerprint() || size != after.size {
        return Err(FileIoError::ChangedWhileReading);
    }
    Ok(HashedFile {
        hash,
        size,
        meta: after,
    })
}

/// A download in progress, stored at `<dir>/<key>.partial`.
///
/// The key must identify the exact content being fetched (for example the node id plus the
/// expected hash), so a resumed partial file always continues the same bytes.
#[derive(Debug)]
pub struct PartialFile {
    path: PathBuf,
    file: File,
    hasher: Hasher,
    written: u64,
}

impl PartialFile {
    /// Opens or resumes `<dir>/<key>.partial`. An existing partial file is re-hashed so the
    /// final hash covers every byte.
    pub fn open(dir: &Path, key: &str, algorithm: HashAlgorithm) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("{key}.partial"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut hasher = Hasher::new(algorithm);
        let written = feed(&mut file, &mut hasher)?;
        file.seek(SeekFrom::Start(written))?;
        Ok(PartialFile {
            path,
            file,
            hasher,
            written,
        })
    }

    /// Bytes already on disk: where a ranged request should resume.
    pub fn offset(&self) -> u64 {
        self.written
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes to disk and verifies size and hash. On a mismatch the partial file is deleted,
    /// since resuming it would only repeat the error.
    pub fn finish(
        self,
        expected_hash: Option<ContentHash>,
        expected_size: Option<u64>,
    ) -> Result<CompletedFile, FileIoError> {
        self.file.sync_all()?;
        let actual = self.hasher.finalize();
        let fail = |err: FileIoError| {
            let _ = fs::remove_file(&self.path);
            Err(err)
        };
        if let Some(expected) = expected_size
            && expected != self.written
        {
            return fail(FileIoError::SizeMismatch {
                expected,
                actual: self.written,
            });
        }
        if let Some(expected) = expected_hash
            && expected != actual
        {
            return fail(FileIoError::HashMismatch { expected, actual });
        }
        Ok(CompletedFile {
            path: self.path,
            hash: actual,
            size: self.written,
        })
    }
}

/// Appends to the partial file, hashing as it goes.
impl Write for PartialFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A verified temp file, ready to be moved into place.
#[derive(Debug)]
#[must_use = "a completed file stays in the temp dir until persisted"]
pub struct CompletedFile {
    path: PathBuf,
    pub hash: ContentHash,
    pub size: u64,
}

impl CompletedFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sets mtime and executable bit before the file becomes visible.
    pub fn set_attributes(
        &self,
        mtime_ns: Option<i64>,
        executable: Option<bool>,
    ) -> io::Result<()> {
        if let Some(x) = executable {
            ops::set_executable(&self.path, x)?;
        }
        if let Some(t) = mtime_ns {
            ops::set_mtime(&self.path, t)?;
        }
        Ok(())
    }

    /// Moves the file to `target`, which must not exist. Returns the metadata at the target.
    /// On failure the file stays in the temp folder (see [`CompletedFile::keep_for_retry`]).
    pub fn persist_new(self, target: &Path) -> Result<LocalMeta, FileIoError> {
        ops::rename_noreplace(&self.path, target)?;
        sync_parent(target)?;
        Ok(hbsync_platform::stat(target)?)
    }

    /// Atomically replaces `target`, but only if it still has the `expected` fingerprint (the
    /// version the plan saw). Otherwise the user changed it meanwhile and nothing is touched.
    ///
    /// A small window remains between the check and the swap; the caller keeps the old version
    /// recoverable (conflict copy or trash) when it matters.
    pub fn persist_over(
        self,
        target: &Path,
        expected: &Fingerprint,
    ) -> Result<LocalMeta, FileIoError> {
        let current = hbsync_platform::stat(target)?;
        if current.fingerprint() != *expected {
            return Err(FileIoError::TargetChanged);
        }
        ops::replace(&self.path, target)?;
        sync_parent(target)?;
        Ok(hbsync_platform::stat(target)?)
    }

    /// Leaves the verified file in the temp folder: reopening its [`PartialFile`] finds it
    /// complete, so a retry needs no transfer. The caller cleans up files nobody resumes.
    pub fn keep_for_retry(self) {}
}

fn sync_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => ops::sync_dir(parent),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    const ALG: HashAlgorithm = HashAlgorithm::Blake3;

    #[test]
    fn hash_file_matches_bytes() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("f");
        fs::write(&path, b"some content")?;
        let hashed = hash_file(&path, ALG)?;
        assert_eq!(hashed.hash, hash_bytes(b"some content", ALG));
        assert_eq!(hashed.size, 12);
        Ok(())
    }

    #[test]
    fn download_resume_and_persist() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let tmp = dir.path().join("tmp");
        let data: Vec<u8> = (0..700_000u32).map(|i| (i % 251) as u8).collect();
        let expected = hash_bytes(&data, ALG);

        // First attempt stops halfway (connection drop, crash).
        let mut partial = PartialFile::open(&tmp, "node-1", ALG)?;
        partial.write_all(&data[..300_000])?;
        drop(partial);

        // Resume from the offset the partial file reports.
        let mut partial = PartialFile::open(&tmp, "node-1", ALG)?;
        assert_eq!(partial.offset(), 300_000);
        io::copy(&mut &data[300_000..], &mut partial)?;
        let done = partial.finish(Some(expected), Some(data.len() as u64))?;
        assert_eq!(done.hash, expected);

        let target = dir.path().join("out.bin");
        done.set_attributes(Some(1_700_000_000_000_000_000), None)?;
        let meta = done.persist_new(&target)?;
        assert_eq!(fs::read(&target)?, data);
        assert_eq!(meta.mtime_ns, 1_700_000_000_000_000_000);
        assert_eq!(
            fs::read_dir(&tmp)?.count(),
            0,
            "temp dir is empty after persist"
        );
        Ok(())
    }

    #[test]
    fn hash_mismatch_deletes_partial() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let mut partial = PartialFile::open(dir.path(), "k", ALG)?;
        partial.write_all(b"corrupted")?;
        let path = partial.path().to_owned();
        let err = partial
            .finish(Some(hash_bytes(b"original", ALG)), None)
            .expect_err("mismatch");
        assert!(matches!(err, FileIoError::HashMismatch { .. }));
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn persist_new_never_overwrites() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("exists");
        fs::write(&target, b"user data")?;
        let mut partial = PartialFile::open(dir.path(), "k", ALG)?;
        partial.write_all(b"download")?;
        let done = partial.finish(None, None)?;
        let err = done.persist_new(&target).expect_err("target exists");
        assert!(matches!(err, FileIoError::Io(e) if e.kind() == io::ErrorKind::AlreadyExists));
        assert_eq!(fs::read(&target)?, b"user data");
        Ok(())
    }

    #[test]
    fn persist_over_checks_fingerprint() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("t");
        fs::write(&target, b"v1")?;
        let planned = hbsync_platform::stat(&target)?.fingerprint();

        // The user edits the file after the plan was made.
        fs::write(&target, b"v1 edited by user")?;
        let mut partial = PartialFile::open(dir.path(), "k", ALG)?;
        partial.write_all(b"v2 from remote")?;
        let done = partial.finish(None, None)?;
        let tmp_path = done.path().to_owned();
        let err = done
            .persist_over(&target, &planned)
            .expect_err("fingerprint changed");
        assert!(matches!(err, FileIoError::TargetChanged));
        assert_eq!(fs::read(&target)?, b"v1 edited by user");
        fs::remove_file(tmp_path)?;

        // With the current fingerprint the swap goes through.
        let current = hbsync_platform::stat(&target)?.fingerprint();
        let mut partial = PartialFile::open(dir.path(), "k2", ALG)?;
        partial.write_all(b"v2 from remote")?;
        partial
            .finish(None, None)?
            .persist_over(&target, &current)?;
        assert_eq!(fs::read(&target)?, b"v2 from remote");
        Ok(())
    }
}
