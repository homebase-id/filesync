//! One daemon (and one tray) per user: an exclusive lock on a file in the state folder, held
//! while the program runs and released by the OS when it exits, however it exits.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::PathBuf;

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
        std::fs::create_dir_all(&paths.state)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path(paths, program))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let pid = std::fs::read_to_string(pid_path(paths, program))
                    .ok()
                    .and_then(|p| p.trim().parse().ok());
                return Err(LockError::Held { program, pid });
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        // Beside the lock file: Windows doesn't let others read a locked file.
        std::fs::write(pid_path(paths, program), std::process::id().to_string())?;
        Ok(InstanceLock { _file: file })
    }
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
        let tray = InstanceLock::acquire_for(&paths, "hbsync-tray")?;
        let err = InstanceLock::acquire_for(&paths, "hbsync-tray").expect_err("held");
        assert!(
            err.to_string()
                .starts_with("hbsync-tray is already running"),
            "{err}"
        );
        drop((held, tray));
        let _again = InstanceLock::acquire(&paths)?;
        Ok(())
    }
}
