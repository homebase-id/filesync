//! File metadata: stable file id, kind, size and timestamps, read without following links.

use std::fs;
use std::io;
use std::path::Path;

/// Identifies a file on one machine, stable across renames inside a volume.
///
/// Unix: `(st_dev, st_ino)`. Windows: `(VolumeSerialNumber, FILE_ID_128)` from `FILE_ID_INFO`,
/// which also covers ReFS's 128-bit ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId {
    pub volume: u64,
    pub file: u128,
}

/// What a directory entry is. Symlinks and other special files are never followed or synced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Other,
}

/// The metadata the sync engine compares. Times are nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalMeta {
    pub kind: EntryKind,
    pub size: u64,
    pub mtime_ns: i64,
    /// Unix `st_ctime`, Windows `ChangeTime`: changes on any write or metadata change and
    /// can't be set by applications, so it catches edits that restore the old mtime.
    pub ctime_ns: i64,
    pub file_id: FileId,
    /// `None` where the OS has no executable bit (Windows); the engine keeps the synced value.
    pub executable: Option<bool>,
}

impl LocalMeta {
    /// The fingerprint that decides whether a file must be re-hashed.
    pub fn fingerprint(&self) -> Fingerprint {
        Fingerprint {
            size: self.size,
            mtime_ns: self.mtime_ns,
            ctime_ns: self.ctime_ns,
            file_id: self.file_id,
        }
    }
}

/// `(size, mtime, ctime, file id)`: if all four are unchanged the content is assumed unchanged
/// and the stored hash is reused (foundations §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub file_id: FileId,
}

/// Reads the metadata of `path` itself (a symlink is reported as a symlink).
pub fn stat(path: &Path) -> io::Result<LocalMeta> {
    imp::stat(path)
}

/// Converts a [`std::time::SystemTime`] to nanoseconds since the Unix epoch, saturating.
pub fn system_time_ns(t: std::time::SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}

fn kind_of(file_type: fs::FileType) -> EntryKind {
    if file_type.is_symlink() {
        EntryKind::Symlink
    } else if file_type.is_dir() {
        EntryKind::Dir
    } else if file_type.is_file() {
        EntryKind::File
    } else {
        EntryKind::Other
    }
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    pub fn stat(path: &Path) -> io::Result<LocalMeta> {
        Ok(from_metadata(&fs::symlink_metadata(path)?))
    }

    pub fn from_metadata(m: &fs::Metadata) -> LocalMeta {
        let kind = kind_of(m.file_type());
        LocalMeta {
            kind,
            size: if kind == EntryKind::File { m.size() } else { 0 },
            mtime_ns: m
                .mtime()
                .saturating_mul(1_000_000_000)
                .saturating_add(m.mtime_nsec()),
            ctime_ns: m
                .ctime()
                .saturating_mul(1_000_000_000)
                .saturating_add(m.ctime_nsec()),
            file_id: FileId {
                volume: m.dev(),
                file: u128::from(m.ino()),
            },
            executable: Some(m.mode() & 0o111 != 0),
        }
    }
}

#[cfg(unix)]
pub use imp::from_metadata;

#[cfg(windows)]
mod imp {
    use super::*;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileBasicInfo,
        FileIdInfo, GetFileInformationByHandleEx,
    };

    /// 100 ns intervals between 1601-01-01 and 1970-01-01.
    const EPOCH_DIFF_100NS: i64 = 116_444_736_000_000_000;

    pub fn filetime_to_unix_ns(ft: i64) -> i64 {
        (ft - EPOCH_DIFF_100NS).saturating_mul(100)
    }

    /// Opens `path` for attribute reads only, sharing everything, without following reparse
    /// points. Works for directories too.
    pub fn open_attributes(path: &Path) -> io::Result<fs::File> {
        fs::OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
    }

    pub fn stat(path: &Path) -> io::Result<LocalMeta> {
        let m = fs::symlink_metadata(path)?;
        let file = open_attributes(path)?;
        let handle = file.as_raw_handle();

        // SAFETY: plain-old-data out structs sized by size_of; the handle is open for the
        // duration of the calls.
        let mut id: FILE_ID_INFO = unsafe { std::mem::zeroed() };
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileIdInfo,
                (&raw mut id).cast(),
                size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut basic: FILE_BASIC_INFO = unsafe { std::mem::zeroed() };
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileBasicInfo,
                (&raw mut basic).cast(),
                size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        let kind = kind_of(m.file_type());
        Ok(LocalMeta {
            kind,
            size: if kind == EntryKind::File { m.len() } else { 0 },
            mtime_ns: filetime_to_unix_ns(basic.LastWriteTime),
            ctime_ns: filetime_to_unix_ns(basic.ChangeTime),
            file_id: FileId {
                volume: id.VolumeSerialNumber,
                file: u128::from_le_bytes(id.FileId.Identifier),
            },
            executable: None,
        })
    }
}

#[cfg(windows)]
pub use imp::{filetime_to_unix_ns, open_attributes};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_and_dir_metadata() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("a.txt");
        fs::write(&file, b"hello")?;
        let meta = stat(&file)?;
        assert_eq!(meta.kind, EntryKind::File);
        assert_eq!(meta.size, 5);
        assert!(meta.mtime_ns > 1_600_000_000_000_000_000);
        assert_eq!(stat(dir.path())?.kind, EntryKind::Dir);
        assert_eq!(stat(dir.path())?.size, 0);
        Ok(())
    }

    #[test]
    fn file_id_survives_rename_and_differs_between_files() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (a, b, c) = (
            dir.path().join("a"),
            dir.path().join("b"),
            dir.path().join("c"),
        );
        fs::write(&a, b"1")?;
        fs::write(&c, b"2")?;
        let before = stat(&a)?.file_id;
        fs::rename(&a, &b)?;
        assert_eq!(stat(&b)?.file_id, before);
        assert_ne!(stat(&c)?.file_id, before);
        Ok(())
    }

    #[test]
    fn mtime_matches_std() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("t");
        fs::write(&file, b"x")?;
        let ours = stat(&file)?.mtime_ns;
        let std_ns = system_time_ns(fs::metadata(&file)?.modified()?);
        assert_eq!(ours, std_ns);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_not_followed() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("target");
        fs::write(&target, b"x")?;
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        assert_eq!(stat(&link)?.kind, EntryKind::Symlink);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn executable_bit() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("run.sh");
        fs::write(&file, b"#!/bin/sh")?;
        assert_eq!(stat(&file)?.executable, Some(false));
        fs::set_permissions(&file, fs::Permissions::from_mode(0o755))?;
        assert_eq!(stat(&file)?.executable, Some(true));
        Ok(())
    }

    #[test]
    fn negative_and_positive_system_time() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(system_time_ns(UNIX_EPOCH + Duration::from_nanos(5)), 5);
        assert_eq!(system_time_ns(UNIX_EPOCH - Duration::from_nanos(5)), -5);
    }
}
