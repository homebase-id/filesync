//! Controlling the daemon from another process (the CLI, the tray): asking it (`client`),
//! starting and stopping it (`process`), and leaving an identity or folder (`leave`).

use crate::{Config, Paths, Root};

mod client;
mod leave;
mod process;

pub(crate) use client::remove_folder;
pub use client::{
    OlderDaemon, Watch, account, add_folder, confirm_deletes, daemon, deleted, empty, empty_listed,
    find_deleted, recent, restore, start_over, status, watch,
};
pub use leave::{LoggedOut, Unlinked, log_out, unlink};
pub use process::{dev_home, running, start, stop};

/// An error worded for people: the CLI prints it, the tray shows it.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// The folder synced, if set up.
pub fn configured(paths: &Paths) -> Result<Option<Root>> {
    Ok(Config::load(&paths.config_file())?.and_then(|c| c.root().ok().flatten().cloned()))
}
