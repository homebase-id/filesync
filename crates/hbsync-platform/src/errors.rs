//! Classifying I/O errors into what the engine should do about them.

use std::io;

/// How the executor should treat a failed local operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoClass {
    /// Another process holds the file open without sharing (Windows) or holds a lock.
    /// Retry later; never treat as a delete.
    Locked,
    /// The volume is full or over quota. Pause downloads.
    DiskFull,
    /// The item vanished or appeared meanwhile. Re-scan and re-plan.
    Raced,
    /// Permission denied. Report per item.
    Denied,
    /// Name or path too long for this OS.
    TooLong,
    Other,
}

/// Classifies an error from a local file operation.
pub fn classify(err: &io::Error) -> IoClass {
    if let Some(code) = err.raw_os_error()
        && let Some(class) = classify_os(code)
    {
        return class;
    }
    match err.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::AlreadyExists => IoClass::Raced,
        io::ErrorKind::PermissionDenied => IoClass::Denied,
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded => IoClass::DiskFull,
        io::ErrorKind::InvalidFilename => IoClass::TooLong,
        _ => IoClass::Other,
    }
}

#[cfg(unix)]
fn classify_os(code: i32) -> Option<IoClass> {
    match code {
        libc::ETXTBSY | libc::EBUSY => Some(IoClass::Locked),
        libc::ENOSPC | libc::EDQUOT => Some(IoClass::DiskFull),
        libc::ENAMETOOLONG => Some(IoClass::TooLong),
        libc::ENOTEMPTY => Some(IoClass::Raced),
        _ => None,
    }
}

#[cfg(windows)]
fn classify_os(code: i32) -> Option<IoClass> {
    // winerror.h values.
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    const ERROR_USER_MAPPED_FILE: i32 = 1224;
    const ERROR_DISK_FULL: i32 = 112;
    const ERROR_HANDLE_DISK_FULL: i32 = 39;
    const ERROR_FILENAME_EXCED_RANGE: i32 = 206;
    const ERROR_DIR_NOT_EMPTY: i32 = 145;
    match code {
        ERROR_SHARING_VIOLATION | ERROR_LOCK_VIOLATION | ERROR_USER_MAPPED_FILE => {
            Some(IoClass::Locked)
        }
        ERROR_DISK_FULL | ERROR_HANDLE_DISK_FULL => Some(IoClass::DiskFull),
        ERROR_FILENAME_EXCED_RANGE => Some(IoClass::TooLong),
        ERROR_DIR_NOT_EMPTY => Some(IoClass::Raced),
        _ => None,
    }
}

#[cfg(not(any(unix, windows)))]
fn classify_os(_code: i32) -> Option<IoClass> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds() {
        assert_eq!(
            classify(&io::Error::from(io::ErrorKind::NotFound)),
            IoClass::Raced
        );
        assert_eq!(
            classify(&io::Error::from(io::ErrorKind::PermissionDenied)),
            IoClass::Denied
        );
        assert_eq!(classify(&io::Error::other("x")), IoClass::Other);
    }

    #[cfg(unix)]
    #[test]
    fn unix_codes() {
        assert_eq!(
            classify(&io::Error::from_raw_os_error(libc::ENOSPC)),
            IoClass::DiskFull
        );
        assert_eq!(
            classify(&io::Error::from_raw_os_error(libc::ETXTBSY)),
            IoClass::Locked
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_sharing_violation() -> io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("locked");
        std::fs::write(&path, b"x")?;
        // Open with no sharing, like an editor holding a file exclusively.
        let _holder = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&path)?;
        let err = std::fs::File::open(&path).expect_err("exclusively held");
        assert_eq!(classify(&err), IoClass::Locked);
        Ok(())
    }
}
