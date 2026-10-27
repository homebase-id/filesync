//! One daemon (and one tray) per user: an exclusive lock on a file in the state folder, held
//! while the program runs and released by the OS when it exits, however it exits.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use crate::Paths;

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("{} is already running{}", if *program == DAEMON { "the daemon" } else { program }, pid.map(|p| format!(" (process {p})")).unwrap_or_default())]
    Held {
        program: &'static str,
        pid: Option<u32>,
    },
    #[error("the lock file: {0}")]
    Io(#[from] io::Error),
}

/// The daemon's program name, and its lock's.
pub const DAEMON: &str = "hbsyncd";

/// Held while this process is the user's daemon (or tray).
#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Takes the daemon's lock, or says which process has it.
    pub fn acquire(paths: &Paths) -> Result<InstanceLock, LockError> {
        Self::acquire_for(paths, DAEMON)
    }

    /// Takes the lock of `program`, or says which process has it.
    pub fn acquire_for(paths: &Paths, program: &'static str) -> Result<InstanceLock, LockError> {
        let Some(file) = try_lock(paths, program)? else {
            let pid = pid(paths, program);
            return Err(LockError::Held { program, pid });
        };
        // Beside the lock file: Windows doesn't let others read a locked file.
        std::fs::write(pid_path(paths, program), std::process::id().to_string())?;
        Ok(InstanceLock { _file: file })
    }
}

/// Whether the daemon runs: its lock is held. Taken for a moment if it isn't, so only while
/// the config's lock is held ([`crate::config::ConfigLock`]): a daemon starting takes that
/// one first, and doesn't find its own taken.
pub(crate) fn daemon_runs(paths: &Paths) -> io::Result<bool> {
    Ok(try_lock(paths, DAEMON)?.is_none())
}

/// The running daemon's process id, as it wrote it.
pub fn daemon_pid(paths: &Paths) -> Option<u32> {
    pid(paths, DAEMON)
}

/// The process id the holder of `program`'s lock wrote.
fn pid(paths: &Paths, program: &str) -> Option<u32> {
    std::fs::read_to_string(pid_path(paths, program))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// The lock file of `program`, locked; `None` when another process holds it.
fn try_lock(paths: &Paths, program: &str) -> io::Result<Option<File>> {
    let file = lock_file(&lock_path(paths, program))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// The file at `path`, made (and its folder) if missing, to be locked: its content is never
/// read or written.
pub(crate) fn lock_file(path: &Path) -> io::Result<File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
}

fn lock_path(paths: &Paths, program: &str) -> PathBuf {
    paths.state.join(format!("{program}.lock"))
}

fn pid_path(paths: &Paths, program: &str) -> PathBuf {
    paths.state.join(format!("{program}.pid"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_holder_at_a_time() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        let held = InstanceLock::acquire(&paths)?;
        match InstanceLock::acquire(&paths) {
            Err(LockError::Held { pid, .. }) => assert_eq!(pid, Some(std::process::id())),
            other => panic!("{other:?}"),
        }
        let err = InstanceLock::acquire(&paths).expect_err("held").to_string();
        assert!(err.contains("already running"), "{err}");
        // The tray's lock is another one.
        let tray = InstanceLock::acquire_for(&paths, "homebase-filesync")?;
        let err = InstanceLock::acquire_for(&paths, "homebase-filesync").expect_err("held");
        assert!(
            err.to_string()
                .starts_with("homebase-filesync is already running"),
            "{err}"
        );
        drop((held, tray));
        let _again = InstanceLock::acquire(&paths)?;
        Ok(())
    }

    /// Whether the daemon runs, told by its lock; looking doesn't take its place (its pid).
    #[test]
    fn whether_the_daemon_runs() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        assert!(!daemon_runs(&paths)?);
        let held = InstanceLock::acquire(&paths)?;
        assert!(daemon_runs(&paths)?);
        let pid = std::fs::read_to_string(pid_path(&paths, DAEMON))?;
        assert_eq!(pid, std::process::id().to_string());
        drop(held);
        assert!(!daemon_runs(&paths)?);
        let _again = InstanceLock::acquire(&paths)?;
        Ok(())
    }
}
