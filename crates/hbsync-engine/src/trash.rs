//! A synced folder's trash (`.hbsync/trash`), which versions before verified deletes (sync
//! semantics §7) filled: what syncing took from the folder, an entry each
//! ([`layout::trash_entry`], named by this device's clock). Nothing goes there any more; the
//! purge ages it out ([`empty_trash`]).

use std::fs;
use std::io;
use std::path::Path;

use hbsync_local::layout;

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
