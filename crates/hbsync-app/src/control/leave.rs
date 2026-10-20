//! Leaving: logging out (the credentials forgotten) and unlinking (no folder synced), each
//! stopping the daemon first.

use super::process::{autostart_off_unless_dev, stop};
use super::{Result, configured};
use crate::{Config, CredentialStore, Paths, Root};

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
    stop(paths)?;
    let autostart_was_on = autostart_off_unless_dev()?;
    let keychain_unreachable = CredentialStore::open(paths).delete(&root.identity)?;
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
    let path = paths.config_file();
    let Some(mut config) = Config::load(&path)? else {
        return Ok(None);
    };
    stop(paths)?;
    let autostart_was_on = autostart_off_unless_dev()?;
    // Setting a folder up again starts afresh.
    config.paused = false;
    let roots = std::mem::take(&mut config.roots);
    config.save(&path)?;
    // Only the main folder was decorated: a role folder's shortcuts are the user's own.
    for root in roots.iter().filter(|r| r.role.is_none()) {
        crate::shell::undecorate(paths, &root.folder, &root.identity);
    }
    Ok(Some(Unlinked {
        roots,
        autostart_was_on,
    }))
}
