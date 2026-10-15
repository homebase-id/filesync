//! File operations with the guarantees the engine relies on.
//!
//! - [`rename_noreplace`] never overwrites: a user file that appeared at the target in the
//!   meantime is never clobbered. This is the default for moves.
//! - [`replace`] atomically swaps a fully written temp file over the target.
//! - [`sync_dirs`] makes renames durable (Unix; a no-op on Windows, where NTFS journals them).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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

/// Syncs each of `dirs`, so the changes of their entries (made, renamed, removed) are on the
/// disk (best effort: one that is gone needs none). On macOS as far as the drive's cache: a
/// full sync after them (the state DB's durable commit) flushes it once for all. Many on one
/// volume in one call where the system has one (Linux: `syncfs`).
pub fn sync_dirs(dirs: impl IntoIterator<Item = PathBuf>) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        const MANY: usize = 32;
        let mut volumes: std::collections::HashMap<u64, Vec<PathBuf>> = Default::default();
        for dir in dirs {
            if let Ok(meta) = fs::metadata(&dir) {
                volumes.entry(meta.dev()).or_default().push(dir);
            }
        }
        for dirs in volumes.into_values() {
            if dirs.len() > MANY && sync_fd(&dirs[0], libc::syncfs).is_ok() {
                continue;
            }
            for dir in &dirs {
                let _ = sync_fd(dir, libc::fsync);
            }
        }
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    for dir in dirs {
        let _ = sync_fd(&dir, libc::fsync);
    }
    #[cfg(windows)]
    drop(dirs);
}

/// Calls `sync` (`fsync`, `syncfs`) on `path` opened.
#[cfg(unix)]
fn sync_fd(path: &Path, sync: unsafe extern "C" fn(libc::c_int) -> libc::c_int) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let file = fs::File::open(path)?;
    // SAFETY: a valid open descriptor.
    if unsafe { sync(file.as_raw_fd()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
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

/// Whether [`set_created`] can set a creation time here (Windows, macOS). Where it can't,
/// callers needn't look one up to apply.
pub const CAN_SET_CREATED: bool = cfg!(any(windows, target_os = "macos"));

/// The creation ("birth") time of a file or directory (not followed if a link), Unix ms;
/// `None` where the system or file system doesn't record one.
pub fn created_ms(path: &Path) -> Option<i64> {
    #[cfg(target_os = "linux")]
    let ns = linux::birth_time_ns(path)?;
    #[cfg(not(target_os = "linux"))]
    let ns = meta::system_time_ns(fs::symlink_metadata(path).ok()?.created().ok()?);
    Some(ns.div_euclid(1_000_000))
}

/// Sets the creation time of a file or directory (not followed if a link), Unix ms, where the
/// system allows it ([`CAN_SET_CREATED`]); elsewhere (Linux has no way to) it does nothing.
/// Call it after [`set_mtime`]: on macOS a modification time earlier than the creation time
/// moves the creation time back.
pub fn set_created(path: &Path, created_ms: i64) -> io::Result<()> {
    #[cfg(any(windows, target_os = "macos"))]
    {
        #[cfg(target_os = "macos")]
        use std::os::macos::fs::FileTimesExt;
        #[cfg(target_os = "macos")]
        use std::os::unix::fs::OpenOptionsExt;
        #[cfg(windows)]
        use std::os::windows::fs::FileTimesExt;
        use std::time::{Duration, UNIX_EPOCH};

        // It comes from other clients: a value out of the system's range is refused, not a
        // panic.
        let offset = Duration::from_millis(created_ms.unsigned_abs());
        let at = if created_ms >= 0 {
            UNIX_EPOCH.checked_add(offset)
        } else {
            UNIX_EPOCH.checked_sub(offset)
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "creation time out of range"))?;
        // Opened only to set its times, a directory too, and a link itself, not its target.
        #[cfg(windows)]
        let file = meta::open_attributes_with(
            path,
            windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES,
        )?;
        #[cfg(target_os = "macos")]
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_SYMLINK)
            .open(path)?;
        file.set_times(fs::FileTimes::new().set_created(at))
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = (path, created_ms);
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    /// statx's birth time. std's `created()` gets it only through glibc: on musl, which the
    /// released Linux programs are built with, it's always "unsupported".
    pub(super) fn birth_time_ns(path: &Path) -> Option<i64> {
        let path = CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: a plain C struct, filled in by the kernel.
        let mut st: libc::statx = unsafe { std::mem::zeroed() };
        // SAFETY: a NUL-terminated path and a buffer of the right type, both live for the call.
        let r = unsafe {
            libc::statx(
                libc::AT_FDCWD,
                path.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
                libc::STATX_BTIME,
                &mut st,
            )
        };
        if r != 0 || st.stx_mask & libc::STATX_BTIME == 0 {
            return None;
        }
        let ns = i64::from(st.stx_btime.tv_nsec);
        Some(
            st.stx_btime
                .tv_sec
                .saturating_mul(1_000_000_000)
                .saturating_add(ns),
        )
    }
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

/// Hides `path` from the file manager: sets the hidden attribute on Windows, where a leading
/// dot hides nothing. No-op elsewhere (the dot does it).
pub fn set_hidden(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        imp::set_hidden(path)
    }
    #[cfg(not(windows))]
    {
        let _ = path;
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
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_HIDDEN, GetFileAttributesW, INVALID_FILE_ATTRIBUTES, MOVEFILE_WRITE_THROUGH,
        MoveFileExW, SetFileAttributesW,
    };

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

    pub fn set_hidden(path: &Path) -> io::Result<()> {
        let w = wide(path);
        // SAFETY: a NUL-terminated wide string.
        let now = unsafe { GetFileAttributesW(w.as_ptr()) };
        if now == INVALID_FILE_ATTRIBUTES {
            return Err(io::Error::last_os_error());
        }
        if now & FILE_ATTRIBUTE_HIDDEN != 0 {
            return Ok(());
        }
        // SAFETY: a NUL-terminated wide string.
        if unsafe { SetFileAttributesW(w.as_ptr(), now | FILE_ATTRIBUTE_HIDDEN) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
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
    fn set_hidden_hides_a_folder_and_can_repeat() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let state = dir.path().join(".hbsync");
        fs::create_dir(&state)?;
        set_hidden(&state)?;
        set_hidden(&state)?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            const HIDDEN: u32 = 0x2;
            assert_ne!(fs::metadata(&state)?.file_attributes() & HIDDEN, 0);
        }
        assert!(state.is_dir());
        Ok(())
    }

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
        sync_dirs([dir.path().to_path_buf()]);
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

    /// Both orders: a file older than its last edit (the usual case), and a copy, whose
    /// creation is later than the modification it kept. Linux can't set it and reports what
    /// the file system has.
    #[test]
    fn set_created_round_trips_where_the_system_allows() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        fs::write(&file, b"x")?;
        let (y2016, y2020) = (1_451_606_400_123, 1_577_836_800_456);
        for (created, modified) in [(y2016, y2020), (y2020, y2016)] {
            for path in [file.as_path(), dir.path()] {
                set_mtime(path, modified * 1_000_000)?;
                set_created(path, created)?;
                if CAN_SET_CREATED {
                    assert_eq!(created_ms(path), Some(created), "{}", path.display());
                }
                assert_eq!(meta::stat(path)?.mtime_ns, modified * 1_000_000);
            }
        }
        Ok(())
    }

    /// Linux reads it through statx itself: std's `created()` gets nothing on musl, which the
    /// released programs are built with. (ext4, btrfs, xfs and tmpfs record it.)
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reads_the_creation_time_on_glibc_and_musl() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        let before = meta::system_time_ns(std::time::SystemTime::now()).div_euclid(1_000_000);
        fs::write(&file, b"x")?;
        let created = created_ms(&file).expect("this file system records a birth time");
        assert!(
            (before - 1_000..=before + 60_000).contains(&created),
            "{created} vs {before}"
        );
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
