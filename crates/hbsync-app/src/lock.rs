//! One daemon per user: an exclusive lock on a file in the state folder, held while the daemon
//! runs and released by the OS when it exits, however it exits.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::PathBuf;

use crate::Paths;

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("the daemon is already running{}", pid.map(|p| format!(" (process {p})")).unwrap_or_default())]
    Held { pid: Option<u32> },
    #[error("the daemon's lock file: {0}")]
    Io(#[from] io::Error),
}

/// Held while this process is the user's daemon.
#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Takes the lock, or says which process has it.
    pub fn acquire(paths: &Paths) -> Result<InstanceLock, LockError> {
        std::fs::create_dir_all(&paths.state)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path(paths))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let pid = std::fs::read_to_string(pid_path(paths))
                    .ok()
                    .and_then(|p| p.trim().parse().ok());
                return Err(LockError::Held { pid });
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        // Beside the lock file: Windows doesn't let others read a locked file.
        std::fs::write(pid_path(paths), std::process::id().to_string())?;
        Ok(InstanceLock { _file: file })
    }
}

fn lock_path(paths: &Paths) -> PathBuf {
    paths.state.join("hbsyncd.lock")
}

fn pid_path(paths: &Paths) -> PathBuf {
    paths.state.join("hbsyncd.pid")
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
            Err(LockError::Held { pid }) => assert_eq!(pid, Some(std::process::id())),
            other => panic!("{other:?}"),
        }
        let err = InstanceLock::acquire(&paths).expect_err("held").to_string();
        assert!(err.contains("already running"), "{err}");
        drop(held);
        let _again = InstanceLock::acquire(&paths)?;
        Ok(())
    }
}
