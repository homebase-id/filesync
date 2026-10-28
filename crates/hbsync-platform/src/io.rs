//! What this process has read and written so far, as the OS counts it: what a piece of work
//! cost, for the log. Counted for the whole process (every thread), so work running beside it
//! counts too.

/// Bytes this process read and wrote since it started ([`COUNTS`] says which).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Io {
    pub read: u64,
    pub written: u64,
}

impl Io {
    /// What was read and written from `earlier` to `self`.
    pub fn since(self, earlier: Io) -> Io {
        Io {
            read: self.read.saturating_sub(earlier.read),
            written: self.written.saturating_sub(earlier.written),
        }
    }
}

/// What the counters count on this OS.
#[cfg(target_os = "linux")]
pub const COUNTS: &str = "every read and write call: files, sockets, pipes, cache hits too";
#[cfg(windows)]
pub const COUNTS: &str = "every read and write operation: files, network, devices";
#[cfg(target_os = "macos")]
pub const COUNTS: &str = "the storage's reads and writes: not the cache, not the network";
#[cfg(not(any(target_os = "linux", windows, target_os = "macos")))]
pub const COUNTS: &str = "nothing (not counted here)";

/// This process's counters now; `None` where the OS doesn't say. One small read of
/// `/proc/self/io` on Linux, one call elsewhere.
pub fn counted() -> Option<Io> {
    os_counted()
}

#[cfg(target_os = "linux")]
fn os_counted() -> Option<Io> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(':'))
            .and_then(|v| v.trim().parse().ok())
    };
    Some(Io {
        read: field("rchar")?,
        written: field("wchar")?,
    })
}

#[cfg(windows)]
fn os_counted() -> Option<Io> {
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessIoCounters, IO_COUNTERS,
    };
    let mut counters: IO_COUNTERS = unsafe { std::mem::zeroed() };
    // SAFETY: the pseudo handle of this process, and a counters struct of the size it writes.
    let ok = unsafe { GetProcessIoCounters(GetCurrentProcess(), &mut counters) };
    (ok != 0).then_some(Io {
        read: counters.ReadTransferCount,
        written: counters.WriteTransferCount,
    })
}

#[cfg(target_os = "macos")]
fn os_counted() -> Option<Io> {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    // SAFETY: this process's id, and a buffer of the flavour's size it writes.
    let ok = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::c_int,
            libc::RUSAGE_INFO_V2,
            (&mut info as *mut libc::rusage_info_v2).cast(),
        )
    };
    (ok == 0).then_some(Io {
        read: info.ri_diskio_bytesread,
        written: info.ri_diskio_byteswritten,
    })
}

#[cfg(not(any(target_os = "linux", windows, target_os = "macos")))]
fn os_counted() -> Option<Io> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reading a file is counted where the OS counts every read (Linux, Windows); the counters
    /// never go back.
    #[test]
    fn a_read_is_counted() -> std::io::Result<()> {
        let Some(before) = counted() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("x");
        std::fs::write(&path, vec![7u8; 64 * 1024])?;
        std::fs::read(&path)?;
        let after = counted().unwrap_or_default();
        let done = after.since(before);
        if cfg!(any(target_os = "linux", windows)) {
            assert!(done.read >= 64 * 1024, "{done:?}");
            assert!(done.written >= 64 * 1024, "{done:?}");
        }
        assert_eq!(before.since(after), Io::default(), "never negative");
        Ok(())
    }
}
