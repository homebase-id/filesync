//! File operations with the guarantees the engine relies on.
//!
//! - [`rename_noreplace`] never overwrites: a user file that appeared at the target in the
//!   meantime is never clobbered. This is the default for moves.
//! - [`replace`] atomically swaps a fully written temp file over the target.
//! - [`sync_dir`] makes a rename durable (Unix; a no-op on Windows, where NTFS journals it).

use std::fs;
use std::io;
use std::path::Path;

use crate::meta;

/// Renames `from` to `to`, failing with [`io::ErrorKind::AlreadyExists`] if `to` exists.
///
/// A case-only rename (`a.txt` → `A.txt`) on a case-insensitive volume is allowed: the target
/// "exists" only because it is the source itself.
pub fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    if let (Ok(src), Ok(dst)) = (meta::stat(from), meta::stat(to))
        && src.file_id == dst.file_id
    {
        return fs::rename(from, to);
    }
    imp::rename_noreplace(from, to)
}

/// Atomically replaces `to` with `from` (both on the same volume). Readers see either the old
/// or the new file, never a partial one.
pub fn replace(from: &Path, to: &Path) -> io::Result<()> {
    // std uses rename(2) on Unix and MoveFileExW(MOVEFILE_REPLACE_EXISTING) or POSIX-semantics
    // SetFileInformationByHandle on Windows, both atomic within a volume.
    fs::rename(from, to)
}

/// Flushes a directory entry change (create, rename, delete) to disk.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Sets the modification time of a file or directory, in nanoseconds since the Unix epoch.
pub fn set_mtime(path: &Path, mtime_ns: i64) -> io::Result<()> {
    let secs = mtime_ns.div_euclid(1_000_000_000);
    let nanos = u32::try_from(mtime_ns.rem_euclid(1_000_000_000)).unwrap_or(0);
    filetime::set_symlink_file_times(
        path,
        filetime::FileTime::now(),
        filetime::FileTime::from_unix_time(secs, nanos),
    )
}

/// Sets or clears the executable bits (`u+x`, `g+x`, `o+x` following the read bits).
/// No-op on Windows.
pub fn set_executable(path: &Path, executable: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)?.permissions().mode();
        let read_bits = mode & 0o444;
        let new_mode = if executable {
            mode | (read_bits >> 2)
        } else {
            mode & !0o111
        };
        if new_mode != mode {
            fs::set_permissions(path, fs::Permissions::from_mode(new_mode))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, executable);
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    pub fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
        let c_from = CString::new(from.as_os_str().as_bytes())?;
        let c_to = CString::new(to.as_os_str().as_bytes())?;
        // The system call itself: musl (the static release build) has no renameat2 wrapper.
        // SAFETY: valid NUL-terminated strings; AT_FDCWD resolves relative paths like rename(2).
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                c_from.as_ptr(),
                libc::AT_FDCWD,
                c_to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            // File systems without RENAME_NOREPLACE (some FUSE and network file systems).
            Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) => {
                super::check_then_rename(from, to)
            }
            _ => Err(err),
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    pub fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
        let c_from = CString::new(from.as_os_str().as_bytes())?;
        let c_to = CString::new(to.as_os_str().as_bytes())?;
        // SAFETY: valid NUL-terminated strings.
        let rc = unsafe { libc::renamex_np(c_from.as_ptr(), c_to.as_ptr(), libc::RENAME_EXCL) };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::ENOTSUP) | Some(libc::EINVAL) => super::check_then_rename(from, to),
            _ => Err(err),
        }
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
mod imp {
    use super::*;

    pub fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
        super::check_then_rename(from, to)
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    fn wide(path: &Path) -> Vec<u16> {
        crate::winpath::extended(path)
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect()
    }

    pub fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
        let (w_from, w_to) = (wide(from), wide(to));
        // SAFETY: NUL-terminated wide strings. Without MOVEFILE_REPLACE_EXISTING the call
        // fails with ERROR_ALREADY_EXISTS when the target exists.
        let ok = unsafe { MoveFileExW(w_from.as_ptr(), w_to.as_ptr(), MOVEFILE_WRITE_THROUGH) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Fallback with a small race window, for file systems without an atomic no-replace rename.
#[cfg(unix)]
fn check_then_rename(from: &Path, to: &Path) -> io::Result<()> {
    match fs::symlink_metadata(to) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "rename target exists",
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => fs::rename(from, to),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noreplace_refuses_existing_target() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        fs::write(&a, b"from")?;
        fs::write(&b, b"user data")?;
        let err = rename_noreplace(&a, &b).expect_err("must not overwrite");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&b)?, b"user data");
        assert_eq!(fs::read(&a)?, b"from");
        Ok(())
    }

    #[test]
    fn noreplace_moves_files_and_dirs() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        fs::create_dir(&a)?;
        fs::write(a.join("inner"), b"x")?;
        rename_noreplace(&a, &b)?;
        assert!(!a.exists());
        assert_eq!(fs::read(b.join("inner"))?, b"x");
        Ok(())
    }

    #[test]
    fn case_only_rename() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (lower, upper) = (dir.path().join("name.txt"), dir.path().join("NAME.txt"));
        fs::write(&lower, b"x")?;
        rename_noreplace(&lower, &upper)?;
        let names: Vec<_> = fs::read_dir(dir.path())?
            .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<io::Result<_>>()?;
        assert_eq!(names, ["NAME.txt"]);
        Ok(())
    }

    #[test]
    fn replace_swaps_content() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (tmp, target) = (dir.path().join("tmp"), dir.path().join("target"));
        fs::write(&target, b"old")?;
        fs::write(&tmp, b"new")?;
        replace(&tmp, &target)?;
        sync_dir(dir.path())?;
        assert_eq!(fs::read(&target)?, b"new");
        assert!(!tmp.exists());
        Ok(())
    }

    #[test]
    fn set_mtime_round_trips() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        fs::write(&file, b"x")?;
        let t = 1_700_000_000_123_456_700; // 100 ns aligned for NTFS
        set_mtime(&file, t)?;
        assert_eq!(meta::stat(&file)?.mtime_ns, t);
        set_mtime(dir.path(), t)?;
        assert_eq!(meta::stat(dir.path())?.mtime_ns, t);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn executable_toggle() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        fs::write(&file, b"x")?;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644))?;
        set_executable(&file, true)?;
        assert_eq!(fs::metadata(&file)?.permissions().mode() & 0o777, 0o755);
        set_executable(&file, false)?;
        assert_eq!(fs::metadata(&file)?.permissions().mode() & 0o777, 0o644);
        Ok(())
    }
}
