//! Leaving: logging out (the credentials forgotten) and unlinking (no folder synced), each
//! stopping the daemon first.

use super::process::{autostart_off_unless_dev, stop};
use super::{Result, configured};
use crate::config::ConfigLock;
use crate::{CredentialStore, Paths, Root};

/// What [`log_out`] did.
#[derive(Debug)]
pub struct LoggedOut {
    pub root: Root,
    /// The keychain couldn't be reached, so credentials kept there may remain.
    pub keychain_unreachable: Option<String>,
    pub autostart_was_on: bool,
}

/// Stops the daemon and forgets this computer's credentials. The folder and its files stay.
/// `None` when nothing is set up.
pub fn log_out(paths: &Paths) -> Result<Option<LoggedOut>> {
    let Some(root) = configured(paths)? else {
        return Ok(None);
    };
    let keychain_unreachable = {
        // Held while the daemon stops and the credentials go: a daemon starting meanwhile
        // (the tray's) waits, and finds none.
        let _held = ConfigLock::take(&paths.config_file())?;
        stop(paths)?;
        CredentialStore::open(paths).delete(&root.identity)?
    };
    let autostart_was_on = autostart_off_unless_dev()?;
    Ok(Some(LoggedOut {
        root,
        keychain_unreachable,
        autostart_was_on,
    }))
}

/// What [`unlink`] did.
#[derive(Debug)]
pub struct Unlinked {
    /// The folders no longer synced; their files stay.
    pub roots: Vec<Root>,
    pub autostart_was_on: bool,
}

/// Stops the daemon and stops syncing the folder; setting it up again picks up where it was.
/// `None` when nothing is set up.
pub fn unlink(paths: &Paths) -> Result<Option<Unlinked>> {
    let roots = {
        // Held while the daemon stops and the config is emptied: a daemon starting meanwhile
        // (the tray's) waits, and finds nothing to sync.
        let held = ConfigLock::take(&paths.config_file())?;
        if held.load()?.is_none() {
            return Ok(None);
        }
        stop(paths)?;
        held.update(|config| {
            // Setting a folder up again starts afresh.
            config.paused = false;
            std::mem::take(&mut config.roots)
        })?
    };
    let autostart_was_on = autostart_off_unless_dev()?;
    // Only the main folder was decorated: a role folder's shortcuts are the user's own.
    for root in roots.iter().filter(|r| r.role.is_none()) {
        crate::shell::undecorate(paths, &root.folder, &root.identity);
    }
    Ok(Some(Unlinked {
        roots,
        autostart_was_on,
    }))
}
