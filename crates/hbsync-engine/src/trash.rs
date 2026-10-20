//! A synced folder's trash (`.hbsync/trash`): what syncing took from the folder, an entry each
//! ([`layout::trash_entry`], named by this device's clock), until the purge empties it. The
//! executor moves items into it and downloads look in it ([`Trash`]); the purge ages it out
//! ([`empty_trash`]).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hbsync_local::layout;
use hbsync_types::NodeId;

/// The folder's trash, as a run's downloads look in it ([`crate::exec::Executor`]).
#[derive(Debug, Default)]
pub(crate) struct Trash {
    /// Its entries ([`layout::trash_entry`]) by node, newest first.
    pub(crate) entries: HashMap<NodeId, Vec<PathBuf>>,
    /// The nodes a file taken from it didn't do for (its tags weren't written): downloaded.
    pub(crate) refused: HashSet<NodeId>,
}

impl Trash {
    pub(crate) fn read(dir: &Path) -> Trash {
        let mut entries: HashMap<NodeId, Vec<(u64, PathBuf)>> = HashMap::new();
        for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
            if let Some((at, node)) = entry
                .file_name()
                .to_str()
                .and_then(layout::parse_trash_entry)
            {
                entries.entry(node).or_default().push((at, entry.path()));
            }
        }
        let mut trash = Trash::default();
        for (node, mut e) in entries {
            e.sort_by(|a, b| b.cmp(a));
            trash
                .entries
                .insert(node, e.into_iter().map(|(_, p)| p).collect());
        }
        trash
    }
}

/// Deletes the trash entries ([`layout::trash_entry`]) that went there from `since` (Unix s)
/// until before `before_ms`. Best effort: one that can't go now goes at the next purge.
pub(crate) fn empty_trash(trash: &Path, since: u64, before_ms: i64) {
    let Ok(entries) = fs::read_dir(trash) else {
        return;
    };
    for entry in entries.flatten() {
        let at = entry.file_name().to_str().and_then(layout::trashed_at);
        if at.is_some_and(|s| s >= since && (s as i64).saturating_mul(1000) < before_ms) {
            let _ = remove_all(&entry.path());
        }
    }
}

/// Deletes a file or a folder with everything in it (a link, not what it points to).
fn remove_all(path: &Path) -> io::Result<()> {
    if fs::symlink_metadata(path)?.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}
