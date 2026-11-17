//! Which folder at a sync folder's top is its folder of shares (sync semantics §10): the one
//! that holds what other identities share with the account, this device's own.
//!
//! The marker in it ([`crate::layout::SHARED_MARKER`]) is an empty file anyone can copy, so it
//! says nothing alone: the folder is the one whose file id the sync folder's
//! [`crate::STATE_DIR`] has on record ([`Record`]), while it holds its marker. A rename, or a
//! move within the volume, keeps the id; a marker anywhere else does nothing. A copy of the
//! folder (a restore from a backup) has another id: [`adopt`] takes it at the next start.

use std::io;
use std::path::Path;

use hbsync_platform::{EntryKind, FileId};

use crate::filter::{holds_marker, is_state_dir};
use crate::layout::{shared_marker, shares_file, tmp_dir};

/// The folder of shares on record for a sync folder: its file id on the sync folder's volume,
/// or none.
///
/// Kept without the volume: on Unix a volume's number may change with a restart, and the
/// folders it is compared with are at the sync folder's top.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record(Option<u128>);

impl Record {
    /// The record of the sync folder at `root`: none where nothing was recorded. One that
    /// can't be read, or isn't one, is an error: a folder of shares taken for a plain one
    /// would be sent.
    pub fn read(root: &Path) -> io::Result<Record> {
        let path = shares_file(root);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Record(None)),
            Err(e) => return Err(e),
        };
        text.lines()
            .find_map(|l| l.strip_prefix("folder ")?.trim().parse().ok())
            .map(|id| Record(Some(id)))
            .ok_or_else(|| {
                let what = format!("{}: not a record of a folder of shares", path.display());
                io::Error::new(io::ErrorKind::InvalidData, what)
            })
    }

    /// Records the folder with `id` as the sync folder's at `root`, in one step.
    fn write(root: &Path, id: FileId) -> io::Result<()> {
        let text = format!("folder {}\n", id.file);
        hbsync_fileio::write_atomic(&shares_file(root), text.as_bytes())
    }

    /// Whether a folder with `id` is the one on record, whatever it holds.
    fn names(&self, id: FileId) -> bool {
        self.0 == Some(id.file)
    }

    /// The folder on record at the top of `root`, whatever it holds. Nothing is listed where
    /// nothing is on record.
    fn at_top(&self, root: &Path) -> io::Result<Option<String>> {
        if self.0.is_none() {
            return Ok(None);
        }
        let found = top_folders(root)?
            .into_iter()
            .find(|(_, id)| self.names(*id));
        Ok(found.map(|(name, _)| name))
    }

    /// Whether the folder at `dir` with `id`, at the sync folder's top, is its folder of
    /// shares: the one on record, holding its marker. Looks on disk for the marker of that one
    /// folder only.
    pub fn holds(&self, id: FileId, dir: &Path) -> io::Result<bool> {
        Ok(self.names(id) && holds_marker(dir)?)
    }

    /// [`Record::holds`] for a folder whose id isn't known yet. Nothing is looked at where
    /// nothing is on record; a folder that isn't there is none.
    pub fn is(&self, dir: &Path) -> io::Result<bool> {
        if self.0.is_none() {
            return Ok(false);
        }
        match hbsync_platform::stat(dir) {
            Ok(meta) => self.holds(meta.file_id, dir),
            Err(e) if hbsync_platform::meta::nothing_there(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// The real folders (no links) at the top of `root`, the state folder left out, each with its
/// file id: one listing, as the walk's. None in a root that isn't there. One whose name isn't
/// Unicode is never synced anyway ([`crate::SkipReason`]).
fn top_folders(root: &Path) -> io::Result<Vec<(String, FileId)>> {
    let items = match hbsync_platform::listdir::list_dir(root) {
        Ok(items) => items,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let folders = items.into_iter().filter_map(|item| {
        let name = item.name.into_string().ok()?;
        (item.meta.kind == EntryKind::Dir && !is_state_dir(&name))
            .then_some((name, item.meta.file_id))
    });
    Ok(folders.collect())
}

/// The folder of shares at the top of `root`, by name: the folder on record there
/// ([`Record`]), while it holds its marker. Found by the record, never by a name: the user may
/// rename it. A marker that can't be looked for is an error, as a root that can't be listed.
///
/// Nothing but the record's read where none is on record; else one listing of the root and
/// one look for the marker.
pub fn folder(root: &Path) -> io::Result<Option<String>> {
    match Record::read(root)?.at_top(root)? {
        Some(name) if holds_marker(&root.join(&name))? => Ok(Some(name)),
        _ => Ok(None),
    }
}

/// What [`make`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Made {
    /// The folder of shares is there under the name asked for, on record.
    Made,
    /// Something else has the name: nothing is at the top that wasn't. Asked again with
    /// another name.
    Taken,
    /// The folder on record is at the top already, under this name: nothing was made.
    Kept(String),
}

/// Makes the folder of shares of the sync folder at `root`, named `name` at its top. Never
/// there without its marker, nor before it is on record: prepared in the sync folder's tmp
/// (on its volume), recorded by its id, then renamed into place, never over anything. A crash
/// in between leaves the prepared folder to the next call.
///
/// The folder on record that is at the top keeps its place ([`Made::Kept`]), with its marker
/// or without (the user took it away: a plain folder now, and theirs to decide).
pub fn make(root: &Path, name: &str) -> io::Result<Made> {
    if let Some(kept) = Record::read(root)?.at_top(root)? {
        return Ok(Made::Kept(kept));
    }
    let tmp = tmp_dir(root);
    std::fs::create_dir_all(&tmp)?;
    let prepared = tmp.join(PREPARED);
    match std::fs::create_dir(&prepared) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let marker = shared_marker(&prepared);
    if !holds_marker(&prepared)? {
        std::fs::write(&marker, "")?;
    }
    // Only looks (a leading dot hides it elsewhere): a marker that can't be hidden still works.
    let _ = hbsync_platform::ops::set_hidden(&marker);
    Record::write(root, hbsync_platform::stat(&prepared)?.file_id)?;
    match hbsync_platform::ops::rename_noreplace(&prepared, &root.join(name)) {
        Ok(()) => Ok(Made::Made),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Made::Taken),
        Err(e) => Err(e),
    }
}

/// The name of the folder [`make`] prepares in the sync folder's tmp.
const PREPARED: &str = "shares";

/// Takes a folder of shares whose record no longer finds it (a sync folder restored from a
/// backup: every folder in it has a new id), at a start: where the folder on record isn't at
/// the top of `root` (or none is on record), and exactly one folder there that isn't synced
/// (`synced`, by name) holds a marker, that one goes on record. Its name, if one was taken.
///
/// Never a synced folder: a marker copied into one does nothing. With two to choose from,
/// neither.
///
/// Costs one listing of the root, and a look for the marker in each folder at its top where
/// the folder on record isn't there.
pub fn adopt<E: From<io::Error>>(
    root: &Path,
    mut synced: impl FnMut(&str) -> Result<bool, E>,
) -> Result<Option<String>, E> {
    let record = Record::read(root)?;
    let top = top_folders(root)?;
    if top.iter().any(|(_, id)| record.names(*id)) {
        return Ok(None);
    }
    let mut found = None;
    for (name, id) in top {
        if !holds_marker(&root.join(&name))? || synced(&name)? {
            continue;
        }
        if found.is_some() {
            return Ok(None);
        }
        found = Some((name, id));
    }
    let Some((name, id)) = found else {
        return Ok(None);
    };
    Record::write(root, id)?;
    Ok(Some(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn never(_: &str) -> io::Result<bool> {
        Ok(false)
    }

    /// The folder made is the folder of shares under any name it is given later; a marker
    /// copied into another folder makes that one nothing, and a copy of the folder itself
    /// neither.
    #[test]
    fn the_folder_made_is_the_one_and_a_copied_marker_makes_none() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(folder(root)?, None);
        assert_eq!(folder(&root.join("missing"))?, None);
        fs::create_dir_all(root.join("Projects"))?;
        assert_eq!(make(root, "Projects")?, Made::Taken);
        assert_eq!(folder(root)?, None, "nothing is at the top that wasn't");
        assert_eq!(make(root, "Shared")?, Made::Made);
        assert_eq!(folder(root)?.as_deref(), Some("Shared"));
        assert_eq!(make(root, "Shared-2")?, Made::Kept("Shared".into()));
        assert!(!root.join("Shared-2").exists());

        fs::copy(
            shared_marker(&root.join("Shared")),
            shared_marker(&root.join("Projects")),
        )?;
        fs::create_dir(root.join("Copy"))?;
        fs::write(shared_marker(&root.join("Copy")), "")?;
        assert_eq!(folder(root)?.as_deref(), Some("Shared"));
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Shared"))?);
        assert!(!record.is(&root.join("Projects"))?);
        assert!(!record.is(&root.join("missing"))?);

        fs::rename(root.join("Shared"), root.join("Renamed by hand"))?;
        assert_eq!(folder(root)?.as_deref(), Some("Renamed by hand"));
        // Without its marker it is a plain folder, and stays the one on record.
        fs::remove_file(shared_marker(&root.join("Renamed by hand")))?;
        assert_eq!(folder(root)?, None);
        assert_eq!(make(root, "Shared")?, Made::Kept("Renamed by hand".into()));
        assert_eq!(adopt(root, never)?, None, "the folder on record is there");
        Ok(())
    }

    /// A folder left prepared by a crash is the one the next call puts in place.
    #[test]
    fn a_folder_left_prepared_is_taken_again() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir(root.join("Shared"))?;
        assert_eq!(make(root, "Shared")?, Made::Taken);
        let prepared = tmp_dir(root).join(PREPARED);
        let id = hbsync_platform::stat(&prepared)?.file_id;
        assert_eq!(make(root, "Shared-2")?, Made::Made);
        assert!(Record::read(root)?.holds(id, &root.join("Shared-2"))?);
        assert!(!prepared.exists());
        Ok(())
    }

    /// A folder of shares the record doesn't find (restored: a new id) is taken at a start,
    /// if it is the only one: never a synced folder holding a marker, and neither of two.
    #[test]
    fn a_restored_folder_of_shares_is_taken_when_it_is_the_only_one() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        fs::remove_dir_all(root.join("Shared"))?;
        for name in ["Restored", "Projects", "plain"] {
            fs::create_dir(root.join(name))?;
        }
        fs::write(shared_marker(&root.join("Restored")), "")?;
        fs::write(shared_marker(&root.join("Projects")), "")?;
        let synced = |name: &str| io::Result::Ok(name == "Projects");
        assert_eq!(folder(root)?, None);
        assert_eq!(adopt(root, never)?, None, "two to choose from");
        assert_eq!(adopt(root, synced)?.as_deref(), Some("Restored"));
        assert_eq!(folder(root)?.as_deref(), Some("Restored"));
        assert_eq!(adopt(root, never)?, None);

        // With nothing on record, the same.
        fs::remove_file(shares_file(root))?;
        assert_eq!(adopt(root, |_| io::Result::Ok(true))?, None);
        assert_eq!(adopt(root, synced)?.as_deref(), Some("Restored"));
        Ok(())
    }

    /// A record that isn't one is an error, never "none".
    #[test]
    fn a_record_that_cant_be_read_isnt_none() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir_all(crate::layout::state_dir(root))?;
        fs::write(shares_file(root), "junk")?;
        assert!(Record::read(root).is_err());
        assert!(folder(root).is_err());
        Ok(())
    }
}
