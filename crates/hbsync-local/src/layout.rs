//! What the client keeps in a synced folder's [`STATE_DIR`], and the names it gives what it moves
//! about in the folder, in one place.

use std::path::{Path, PathBuf};

use hbsync_types::{NodeId, Uuid};

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

/// What syncing took from the folder (an entry each, [`trash_entry`]), until the purge.
pub fn trash_dir(root: &Path) -> PathBuf {
    state_dir(root).join("trash")
}

/// The name of `node`'s entry in the [`trash_dir`]: `<Unix s>-<node>`, by when it went there
/// (`at`, Unix s; a second or so later where the node's entry of that second was taken).
pub fn trash_entry(at: u64, node: NodeId) -> String {
    format!("{at}-{}", node.0.simple())
}

/// When a trash entry ([`trash_entry`]) went there, Unix s.
pub fn trashed_at(entry: &str) -> Option<u64> {
    entry.split_once('-')?.0.parse().ok()
}

/// When a trash entry ([`trash_entry`]) went there, Unix s, and whose it is.
pub fn parse_trash_entry(entry: &str) -> Option<(u64, NodeId)> {
    let node = Uuid::try_parse(entry.split_once('-')?.1).ok()?;
    Some((trashed_at(entry)?, NodeId(node)))
}

/// The temporary name a node is parked under, in its own folder, while its place is wanted by
/// another (the engine's executor).
pub fn parked_name(node: NodeId) -> String {
    format!(".hbsync-move-{}", node.0.simple())
}

/// The user's ignore rules' file name in [`STATE_DIR`] (gitignore syntax): per device, never
/// synced.
pub const IGNORE_FILE: &str = "ignore";
