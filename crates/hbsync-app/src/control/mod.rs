//! Controlling the daemon from another process (the CLI, the tray): asking it (`client`),
//! starting and stopping it (`process`), and leaving an identity or folder (`leave`).

use crate::{Config, Paths, Result, Root};

mod client;
mod leave;
mod process;

pub(crate) use client::remove_folder;
pub use client::{
    NotRunning, OlderDaemon, Watch, account, add_folder, confirm_deletes, daemon, deleted, empty,
    empty_listed, find_deleted, put_back, recent, restore, start_over, status, watch,
};
pub use leave::{LoggedOut, Unlinked, log_out, unlink};
pub use process::{clear_stopped, dev_home, running, start, stop, stop_syncing, stopped};

/// The folder synced, if set up.
pub fn configured(paths: &Paths) -> Result<Option<Root>> {
    Ok(Config::load(&paths.config_file())?.and_then(|c| c.root().ok().flatten().cloned()))
}
