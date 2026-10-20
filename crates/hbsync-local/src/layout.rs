//! What the client keeps in a synced folder's [`STATE_DIR`], in one place.

use std::path::{Path, PathBuf};

use crate::STATE_DIR;

/// The folder's [`STATE_DIR`].
pub fn state_dir(root: &Path) -> PathBuf {
    root.join(STATE_DIR)
}

/// The folder's marker ([`crate::Marker`]).
pub(crate) fn marker_file(root: &Path) -> PathBuf {
    state_dir(root).join("folder")
}

/// The user's ignore rules for this device ([`crate::Filter::load`]).
pub fn ignore_file(root: &Path) -> PathBuf {
    state_dir(root).join(IGNORE_FILE)
}

/// Downloads in progress: on the folder's volume, so they land with an atomic rename.
pub fn tmp_dir(root: &Path) -> PathBuf {
    state_dir(root).join("tmp")
}

/// What syncing took from the folder (`<Unix s>-<node>`), until the purge.
pub fn trash_dir(root: &Path) -> PathBuf {
    state_dir(root).join("trash")
}

/// The user's ignore rules' file name in [`STATE_DIR`] (gitignore syntax): per device, never
/// synced.
pub const IGNORE_FILE: &str = "ignore";
