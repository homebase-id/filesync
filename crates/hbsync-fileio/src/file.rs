//! Reading and writing whole files safely.
//!
//! - [`hash_file`] hashes a file and fails if it changed while being read, so a hash always
//!   belongs to one consistent version of the file.
//! - [`PartialFile`] receives a download into a temp file next to the sync root, can resume
//!   after a crash or a dropped connection, verifies the hash, and is then moved into place with
//!   [`CompletedFile::persist_new`] (never overwrites) or [`CompletedFile::persist_over`]. Its
//!   bytes are synced before; the rename is the caller's to sync with its folder (many at once,
//!   before it records them durably).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use hbsync_platform::{Fingerprint, LocalMeta, ops};

use crate::hash::{Hasher, feed};
use hbsync_types::{ContentHash, ErrorClass, HashAlgorithm};

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
    /// The file is in place, but reading it back there failed.
    #[error("put in place, but not read back: {0}")]
    PlacedUnread(io::Error),
}

impl FileIoError {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            FileIoError::Io(e) => ErrorClass::of_io(e),
            FileIoError::ChangedWhileReading | FileIoError::TargetChanged => ErrorClass::LocalRaced,
            // The partial file was deleted: a fresh try may get other bytes.
            FileIoError::HashMismatch { .. } | FileIoError::SizeMismatch { .. } => {
                ErrorClass::LocalFault
            }
            FileIoError::PlacedUnread(e) => ErrorClass::of_io(e),
        }
    }
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
        let path = partial_path(dir, key);
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
        expected_hash: ContentHash,
        expected_size: u64,
    ) -> Result<CompletedFile, FileIoError> {
        self.file.sync_all()?;
        self.complete(expected_hash, expected_size, true)
    }

    /// [`PartialFile::finish`] for one of many finishing together: a small file's bytes may
    /// not be on the disk yet (their write-back is started), and it can't be put in place until
    /// [`sync_completed`] has made them durable with the others' (in fewer flushes of the
    /// drive's cache). A large one, and every one on Windows (which has no cheaper way to
    /// flush many), is flushed here.
    pub fn finish_in_group(
        self,
        expected_hash: ContentHash,
        expected_size: u64,
    ) -> Result<CompletedFile, FileIoError> {
        if cfg!(windows) || self.written >= SYNC_ALONE_FROM {
            return self.finish(expected_hash, expected_size);
        }
        ops::start_writeback(&self.file);
        self.complete(expected_hash, expected_size, false)
    }

    fn complete(
        self,
        expected_hash: ContentHash,
        expected_size: u64,
        synced: bool,
    ) -> Result<CompletedFile, FileIoError> {
        let actual = self.hasher.finalize();
        let fail = |err: FileIoError| {
            let _ = fs::remove_file(&self.path);
            Err(err)
        };
        if expected_size != self.written {
            return fail(FileIoError::SizeMismatch {
                expected: expected_size,
                actual: self.written,
            });
        }
        if expected_hash != actual {
            return fail(FileIoError::HashMismatch {
                expected: expected_hash,
                actual,
            });
        }
        Ok(CompletedFile {
            path: self.path,
            hash: actual,
            size: self.written,
            adopted: false,
            synced,
        })
    }
}

/// Files from this size on flush their own bytes ([`PartialFile::finish_in_group`]): one flush
/// costs little against writing them, and they flush in parallel.
const SYNC_ALONE_FROM: u64 = 4 << 20;

/// Where [`PartialFile::open`] keeps the download `key`.
fn partial_path(dir: &Path, key: &str) -> PathBuf {
    dir.join(format!("{key}.partial"))
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
    adopted: bool,
    /// Its bytes are durable: only then may it be put in place (a rename that a power cut
    /// keeps, of a file whose bytes it lost, would show garbage under the name).
    synced: bool,
}

/// Makes the bytes of `files` durable together ([`ops::sync_files`]: a few flushes of the
/// drive's cache, not one each), so they can be put in place. Those already durable are left
/// alone; one that fails stays not durable ([`CompletedFile::synced`]), the others count.
pub fn sync_completed(files: &mut [&mut CompletedFile]) {
    let mut waiting: Vec<&mut &mut CompletedFile> =
        files.iter_mut().filter(|f| !f.synced).collect();
    let paths: Vec<&Path> = waiting.iter().map(|f| f.path.as_path()).collect();
    let answers = ops::sync_files(&paths);
    for (f, answer) in waiting.iter_mut().zip(answers) {
        f.synced = answer.is_ok();
    }
}

impl CompletedFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file at `src` (on the volume the download goes to) as a completed download, where
    /// it is, if it holds exactly the bytes `expected` (`size` of them): hashed now (unchanged
    /// while read), its bytes synced, as a download's are. `None` where it holds other bytes, or
    /// where its size or mtime (Unix ms) isn't `size` and `mtime_ms` (nothing read then). It
    /// stays where it is until persisted ([`CompletedFile::adopted`]).
    pub fn adopt(
        src: &Path,
        expected: ContentHash,
        size: u64,
        mtime_ms: i64,
    ) -> Result<Option<CompletedFile>, FileIoError> {
        let meta = hbsync_platform::stat(src)?;
        if meta.kind != hbsync_platform::EntryKind::File
            || meta.size != size
            || meta.mtime_ns.div_euclid(1_000_000) != mtime_ms
        {
            return Ok(None);
        }
        if hash_file(src, expected.algorithm)?.hash != expected {
            return Ok(None);
        }
        // Windows flushes a file only through a handle that may write it.
        OpenOptions::new().write(true).open(src)?.sync_data()?;
        Ok(Some(CompletedFile {
            path: src.to_owned(),
            hash: expected,
            size,
            adopted: true,
            synced: true,
        }))
    }

    /// Whether it is a file taken where it was ([`CompletedFile::adopt`]), not a download into
    /// the temp folder: until it is persisted it stays there, its bytes as they were.
    pub fn adopted(&self) -> bool {
        self.adopted
    }

    /// Whether its bytes are durable, so it may be put in place ([`sync_completed`]).
    pub fn synced(&self) -> bool {
        self.synced
    }

    /// A file whose bytes may not be durable is never put in place.
    fn durable(&self) -> Result<(), FileIoError> {
        match self.synced {
            true => Ok(()),
            false => Err(FileIoError::Io(io::Error::other(
                "its bytes aren't on the disk yet (a fault of this client)",
            ))),
        }
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

    /// Moves the file to `target`, which must not exist, with creation time `created_ms`.
    /// Returns the metadata at the target. On failure the file stays in the temp folder (see
    /// [`CompletedFile::keep_for_retry`]).
    pub fn persist_new(
        self,
        target: &Path,
        created_ms: Option<i64>,
    ) -> Result<LocalMeta, FileIoError> {
        self.durable()?;
        ops::rename_noreplace(&self.path, target)?;
        placed(target, created_ms)
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
        created_ms: Option<i64>,
    ) -> Result<LocalMeta, FileIoError> {
        self.durable()?;
        let current = hbsync_platform::stat(target)?;
        if current.fingerprint() != *expected {
            return Err(FileIoError::TargetChanged);
        }
        ops::replace(&self.path, target)?;
        placed(target, created_ms)
    }

    /// Leaves the verified file in the temp folder: reopening its [`PartialFile`] finds it
    /// complete, so a retry needs no transfer. The caller cleans up files nobody resumes. One
    /// [`CompletedFile::adopted`] stays where it was.
    pub fn keep_for_retry(self) {}
}

/// The file just put at `target`: its creation time set (once in place, after the mtime: NTFS
/// "tunneling" gives a name removed and made again within seconds the old file's), its folder
/// flushed, its metadata read. It is in place whatever fails here
/// ([`FileIoError::PlacedUnread`]).
fn placed(target: &Path, created_ms: Option<i64>) -> Result<LocalMeta, FileIoError> {
    // Only shown, never synced on: a file system that won't take it fails nothing.
    if let Some(t) = created_ms {
        let _ = ops::set_created(target, t);
    }
    hbsync_platform::stat(target).map_err(FileIoError::PlacedUnread)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    const ALG: HashAlgorithm = HashAlgorithm::Blake3;

    /// A download whose bytes may not be on the disk is never put in place: only once a
    /// barrier made them durable.
    #[test]
    fn an_unsynced_download_is_not_put_in_place() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let mut done = Vec::new();
        for name in ["a", "b"] {
            let mut partial = PartialFile::open(dir.path(), name, ALG)?;
            partial.write_all(name.as_bytes())?;
            done.push(partial.finish_in_group(hash_bytes(name.as_bytes(), ALG), 1)?);
        }
        assert!(done.iter().all(|f| f.synced() == cfg!(windows)));
        let target = dir.path().join("placed");
        let first = done.remove(0);
        if !cfg!(windows) {
            assert!(first.persist_new(&target, None).is_err());
            assert!(!target.exists());
        }
        let mut again =
            PartialFile::open(dir.path(), "a", ALG)?.finish_in_group(hash_bytes(b"a", ALG), 1)?;
        sync_completed(&mut [&mut again, &mut done[0]]);
        assert!(again.synced() && done[0].synced());
        again.persist_new(&target, None)?;
        assert_eq!(fs::read(&target)?, b"a");
        Ok(())
    }

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
        let done = partial.finish(expected, data.len() as u64)?;
        assert_eq!(done.hash, expected);

        let target = dir.path().join("out.bin");
        done.set_attributes(Some(1_700_000_000_000_000_000), None)?;
        let meta = done.persist_new(&target, None)?;
        assert_eq!(fs::read(&target)?, data);
        assert_eq!(meta.mtime_ns, 1_700_000_000_000_000_000);
        assert_eq!(
            fs::read_dir(&tmp)?.count(),
            0,
            "temp dir is empty after persist"
        );
        Ok(())
    }

    /// A file with the bytes wanted is taken as the download, where it is, and persisted from
    /// there; one with other bytes, or of another size or mtime, isn't.
    #[test]
    fn adopt_takes_only_the_bytes_wanted() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let (same, other) = (dir.path().join("same"), dir.path().join("other"));
        fs::write(&same, b"wanted")?;
        fs::write(&other, b"wanteD")?;
        let expected = hash_bytes(b"wanted", ALG);
        let mtime = |p: &Path| hbsync_platform::stat(p).map(|m| m.mtime_ns.div_euclid(1_000_000));
        let (at, other_at) = (mtime(&same)?, mtime(&other)?);
        assert!(CompletedFile::adopt(&other, expected, 6, other_at)?.is_none());
        assert!(CompletedFile::adopt(&same, expected, 5, at)?.is_none());
        assert!(CompletedFile::adopt(&same, expected, 6, at + 1)?.is_none());
        let done = CompletedFile::adopt(&same, expected, 6, at)?.expect("taken");
        assert!(done.adopted() && same.exists(), "where it was");
        let target = dir.path().join("placed");
        done.persist_new(&target, None)?;
        assert_eq!(fs::read(&target)?, b"wanted");
        assert!(!same.exists());
        assert_eq!(fs::read(&other)?, b"wanteD");
        Ok(())
    }

    #[test]
    fn hash_mismatch_deletes_partial() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let mut partial = PartialFile::open(dir.path(), "k", ALG)?;
        partial.write_all(b"corrupted")?;
        let path = partial.path().to_owned();
        let err = partial
            .finish(hash_bytes(b"original", ALG), 9)
            .expect_err("mismatch");
        assert!(matches!(err, FileIoError::HashMismatch { .. }));
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn size_mismatch_deletes_partial() -> Result<(), FileIoError> {
        let dir = tempfile::tempdir()?;
        let mut partial = PartialFile::open(dir.path(), "k", ALG)?;
        partial.write_all(b"short")?;
        let path = partial.path().to_owned();
        let err = partial
            .finish(hash_bytes(b"short", ALG), 6)
            .expect_err("mismatch");
        assert!(matches!(
            err,
            FileIoError::SizeMismatch {
                expected: 6,
                actual: 5
            }
        ));
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
        let done = partial.finish(hash_bytes(b"download", ALG), 8)?;
        let err = done.persist_new(&target, None).expect_err("target exists");
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
        let done = partial.finish(hash_bytes(b"v2 from remote", ALG), 14)?;
        let tmp_path = done.path().to_owned();
        let err = done
            .persist_over(&target, &planned, None)
            .expect_err("fingerprint changed");
        assert!(matches!(err, FileIoError::TargetChanged));
        assert_eq!(fs::read(&target)?, b"v1 edited by user");
        fs::remove_file(tmp_path)?;

        // With the current fingerprint the swap goes through.
        let current = hbsync_platform::stat(&target)?.fingerprint();
        let mut partial = PartialFile::open(dir.path(), "k2", ALG)?;
        partial.write_all(b"v2 from remote")?;
        partial
            .finish(hash_bytes(b"v2 from remote", ALG), 14)?
            .persist_over(&target, &current, None)?;
        assert_eq!(fs::read(&target)?, b"v2 from remote");
        Ok(())
    }
}
