//! Which folder at a sync folder's top is its folder of shares (sync semantics §10): the one
//! that holds what other identities share with the account, this device's own.
//!
//! The marker in it ([`crate::layout::SHARED_MARKER`]) is a file anyone can copy, so it says
//! nothing alone. The sync folder's [`crate::STATE_DIR`] has the folder on record
//! ([`Record`]): the token its marker holds, and the folder's file id. The folder of shares is
//! the folder with that id holding a marker with that token. A rename, or a move within the
//! volume, keeps both; a marker copied elsewhere is in a folder with another id; a folder
//! that got the id of one deleted holds no such marker. A copy of the folder (a restore from a
//! backup) has another id: [`adopt`] takes it as the sync folder opens.
//!
//! Where the volume's file ids aren't stable (FAT, exFAT, a network or FUSE volume:
//! [`hbsync_platform::FsFamily::has_stable_file_ids`]) no id is recorded, and the token alone
//! tells: there a copied marker makes its folder one too (a known limit).

use std::io;
use std::path::Path;

use hbsync_platform::{EntryKind, FileId};

use crate::filter::{holds_marker, is_state_dir};
use crate::layout::{shared_marker, shares_file, tmp_dir};

/// The folder of shares on record for a sync folder, or none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record(Option<Known>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Known {
    /// What the folder's marker holds.
    token: String,
    /// The folder's file id on the sync folder's volume; none where ids aren't stable. Kept
    /// without the volume: on Unix a volume's number may change with a restart, and the
    /// folders it is compared with are at the sync folder's top.
    id: Option<u128>,
}

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
        let field = |name: &str| {
            let value = text.lines().find_map(|l| l.strip_prefix(name));
            value.map(str::trim).filter(|v| !v.is_empty())
        };
        // A line that starts as the id's must hold one: never taken for a record without.
        let id = text.lines().find(|l| l.starts_with("folder"));
        let id = id.map(|l| l.strip_prefix("folder ").unwrap_or("").trim().parse());
        match (field("token "), id.transpose()) {
            (Some(token), Ok(id)) => Ok(Record(Some(Known {
                token: token.to_owned(),
                id,
            }))),
            _ => {
                let what = format!("{}: not a record of a folder of shares", path.display());
                Err(io::Error::new(io::ErrorKind::InvalidData, what))
            }
        }
    }

    /// Records the folder at `dir`, whose marker holds `token`, as the folder of shares of the
    /// sync folder at `root`, in one step: with its file id where the volume's are stable.
    fn write(root: &Path, token: &str, dir: &Path) -> io::Result<()> {
        let stable = hbsync_platform::fskind::detect(root).is_ok_and(|f| f.has_stable_file_ids());
        let mut text = format!("token {token}\n");
        if stable {
            text += &format!("folder {}\n", hbsync_platform::stat(dir)?.file_id.file);
        }
        hbsync_fileio::write_atomic(&shares_file(root), text.as_bytes())
    }

    /// Whether the folder at `dir` with `id`, at the sync folder's top, is its folder of
    /// shares: the one on record (by its id, where one is recorded), holding the record's
    /// marker. Nothing is looked at where nothing is on record, or the id is another's.
    pub fn holds(&self, id: FileId, dir: &Path) -> io::Result<bool> {
        match &self.0 {
            Some(known) if known.id.is_none_or(|want| want == id.file) => {
                Ok(token_in(dir)?.is_some_and(|token| token == known.token))
            }
            _ => Ok(false),
        }
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

/// What the marker of the folder at `dir` holds; `None` for a folder holding none
/// ([`holds_marker`]), or one gone as it was read.
fn token_in(dir: &Path) -> io::Result<Option<String>> {
    if !holds_marker(dir)? {
        return Ok(None);
    }
    match std::fs::read(shared_marker(dir)) {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).trim().to_owned())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
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
/// ([`Record::holds`]). Found by the record, never by a name: the user may rename it. More
/// than one only where the volume's file ids aren't stable (the module's words). A marker
/// that can't be read is an error, as a root that can't be listed.
///
/// Nothing but the record's read where none is on record; else one listing of the root and
/// one read of a marker (where no id is recorded: a look for one in each folder there).
pub fn folders(root: &Path) -> io::Result<Vec<String>> {
    let record = Record::read(root)?;
    let mut found = Vec::new();
    if record.0.is_none() {
        return Ok(found);
    }
    for (name, id) in top_folders(root)? {
        if record.holds(id, &root.join(&name))? {
            found.push(name);
        }
    }
    found.sort_unstable();
    Ok(found)
}

/// What [`make`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Made {
    /// The folder of shares is there under the name asked for, on record.
    Made,
    /// Something else has the name: nothing is at the top that wasn't. Asked again with
    /// another name.
    Taken,
    /// The folder of shares is at the top already, under this name: nothing was made.
    Kept(String),
}

/// Makes the folder of shares of the sync folder at `root`, named `name` at its top. Never
/// there without its marker, nor before it is on record: prepared in the sync folder's tmp
/// (on its volume) with a marker holding a new token, recorded, then renamed into place,
/// never over anything. A crash in between leaves a prepared folder, which the next call
/// makes anew.
///
/// A folder on record that lost its marker is a plain folder (the user took the marker away:
/// theirs now): another is made, and goes on record in its place.
pub fn make(root: &Path, name: &str) -> io::Result<Made> {
    if let Some(kept) = folders(root)?.into_iter().next() {
        return Ok(Made::Kept(kept));
    }
    let tmp = tmp_dir(root);
    std::fs::create_dir_all(&tmp)?;
    let prepared = tmp.join(PREPARED);
    let marker = shared_marker(&prepared);
    // One left by a crash: this program's own, holding its marker and nothing else.
    for gone in [
        std::fs::remove_file(&marker),
        std::fs::remove_dir(&prepared),
    ] {
        match gone {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    std::fs::create_dir(&prepared)?;
    let token = hbsync_types::Uuid::new_v4().simple().to_string();
    std::fs::write(&marker, &token)?;
    // Only looks (a leading dot hides it elsewhere): a marker that can't be hidden still works.
    let _ = hbsync_platform::ops::set_hidden(&marker);
    Record::write(root, &token, &prepared)?;
    match hbsync_platform::ops::rename_noreplace(&prepared, &root.join(name)) {
        Ok(()) => Ok(Made::Made),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Made::Taken),
        Err(e) => Err(e),
    }
}

/// The name of the folder [`make`] prepares in the sync folder's tmp.
const PREPARED: &str = "shares";

/// Takes a folder of shares whose record no longer finds it (a sync folder restored from a
/// backup: every folder in it has a new id), as the sync folder opens: where one is on record
/// and isn't at the top of `root`, and exactly one folder there that isn't synced (`synced`,
/// by its name and file id) holds a marker with the record's token, that one goes on record.
/// Its name, if one was taken.
///
/// Never a synced folder: a marker copied into one does nothing. With two to choose from,
/// neither. With nothing on record, nothing is looked at, and nothing taken.
///
/// Costs the record's read; where one is on record, one listing of the root and a marker's
/// read, and where the folder on record isn't there a look for a marker in each folder.
pub fn adopt<E: From<io::Error>>(
    root: &Path,
    mut synced: impl FnMut(&str, FileId) -> Result<bool, E>,
) -> Result<Option<String>, E> {
    let record = Record::read(root)?;
    let Some(known) = &record.0 else {
        return Ok(None);
    };
    let top = top_folders(root)?;
    // The one on record first: where it is there, no other folder is looked into.
    for (name, id) in &top {
        if record.holds(*id, &root.join(name))? {
            return Ok(None);
        }
    }
    let mut found = None;
    for (name, id) in &top {
        let at = root.join(name);
        if token_in(&at)?.is_none_or(|token| token != known.token) || synced(name, *id)? {
            continue;
        }
        if found.is_some() {
            return Ok(None);
        }
        found = Some((name, at));
    }
    let Some((name, at)) = found else {
        return Ok(None);
    };
    Record::write(root, &known.token, &at)?;
    Ok(Some(name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn never(_: &str, _: FileId) -> io::Result<bool> {
        Ok(false)
    }

    /// Whether this volume's file ids are stable: a test of what the id tells can't run
    /// where they aren't.
    fn stable(root: &Path) -> bool {
        Record::read(root).is_ok_and(|r| r.0.is_some_and(|k| k.id.is_some()))
    }

    /// The folder made is the folder of shares under any name it is given later; a marker
    /// copied into another folder makes that one nothing, and a copy of the folder itself
    /// neither.
    #[test]
    fn the_folder_made_is_the_one_and_a_copied_marker_makes_none() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert!(folders(root)?.is_empty());
        assert!(folders(&root.join("missing"))?.is_empty());
        fs::create_dir_all(root.join("Projects"))?;
        assert_eq!(make(root, "Projects")?, Made::Taken);
        assert!(folders(root)?.is_empty(), "nothing is there that wasn't");
        assert_eq!(make(root, "Shared")?, Made::Made);
        assert_eq!(folders(root)?, ["Shared"]);
        assert_eq!(make(root, "Shared-2")?, Made::Kept("Shared".into()));
        assert!(!root.join("Shared-2").exists());
        if !stable(root) {
            return Ok(());
        }

        let marker = shared_marker(&root.join("Shared"));
        fs::copy(&marker, shared_marker(&root.join("Projects")))?;
        fs::create_dir(root.join("Copy"))?;
        fs::copy(&marker, shared_marker(&root.join("Copy")))?;
        assert_eq!(folders(root)?, ["Shared"]);
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Shared"))?);
        assert!(!record.is(&root.join("Projects"))?);
        assert!(!record.is(&root.join("missing"))?);

        fs::rename(root.join("Shared"), root.join("Renamed by hand"))?;
        assert_eq!(folders(root)?, ["Renamed by hand"]);
        assert_eq!(adopt(root, never)?, None, "the folder on record is there");
        // Another marker in it (an empty file, another folder of shares' own) isn't its own.
        let marker = shared_marker(&root.join("Renamed by hand"));
        let token = fs::read(&marker)?;
        fs::remove_file(&marker)?;
        fs::write(&marker, "")?;
        assert!(folders(root)?.is_empty());
        fs::remove_file(&marker)?;
        fs::write(&marker, token)?;
        assert_eq!(folders(root)?, ["Renamed by hand"]);
        Ok(())
    }

    /// Without its marker the folder on record is a plain folder: the next one made takes its
    /// place on record. A folder left prepared by a crash is made anew.
    #[test]
    fn another_is_made_once_the_one_on_record_is_plain() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        let old = fs::read(shared_marker(&root.join("Shared")))?;
        fs::remove_file(shared_marker(&root.join("Shared")))?;
        assert!(folders(root)?.is_empty());
        assert_eq!(make(root, "Shared")?, Made::Taken);
        assert!(tmp_dir(root).join(PREPARED).is_dir());
        assert_eq!(make(root, "Shared-2")?, Made::Made);
        assert_eq!(folders(root)?, ["Shared-2"]);
        assert!(!tmp_dir(root).join(PREPARED).exists());
        // Its old marker back makes the plain one nothing: another's token.
        fs::write(shared_marker(&root.join("Shared")), old)?;
        assert_eq!(folders(root)?, ["Shared-2"]);
        Ok(())
    }

    /// A folder of shares the record doesn't find (restored: a new id) is taken as the folder
    /// opens, if it is the only one: never a synced folder holding the marker, a folder
    /// holding another marker, or one of two. With nothing on record, none.
    #[test]
    fn a_restored_folder_of_shares_is_taken_when_it_is_the_only_one() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir(root.join("Loose"))?;
        fs::write(shared_marker(&root.join("Loose")), "")?;
        assert_eq!(adopt(root, never)?, None, "nothing is on record");
        assert_eq!(make(root, "Shared")?, Made::Made);
        if !stable(root) {
            return Ok(());
        }
        let token = fs::read(shared_marker(&root.join("Shared")))?;
        fs::remove_dir_all(root.join("Shared"))?;
        for name in ["Restored", "Projects", "plain"] {
            fs::create_dir(root.join(name))?;
        }
        fs::write(shared_marker(&root.join("Restored")), &token)?;
        fs::write(shared_marker(&root.join("Projects")), &token)?;
        let projects = hbsync_platform::stat(&root.join("Projects"))?.file_id;
        let by_name = |name: &str, _: FileId| io::Result::Ok(name == "Projects");
        let by_id = |_: &str, id: FileId| io::Result::Ok(id == projects);
        assert!(folders(root)?.is_empty());
        assert_eq!(adopt(root, never)?, None, "two to choose from");
        assert_eq!(adopt(root, by_id)?.as_deref(), Some("Restored"));
        assert_eq!(folders(root)?, ["Restored"]);
        assert_eq!(adopt(root, never)?, None);

        fs::rename(root.join("Restored"), root.join("old"))?;
        fs::create_dir(root.join("Again"))?;
        fs::write(shared_marker(&root.join("Again")), &token)?;
        fs::remove_dir_all(root.join("old"))?;
        assert_eq!(adopt(root, |_, _| io::Result::Ok(true))?, None);
        assert_eq!(adopt(root, by_name)?.as_deref(), Some("Again"));
        Ok(())
    }

    /// A record that isn't one is an error, never "none"; one without an id (a volume whose
    /// ids aren't stable) goes by the token alone.
    #[test]
    fn a_record_that_cant_be_read_isnt_none_and_one_without_an_id_goes_by_the_token()
    -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir_all(root.join("Shared"))?;
        fs::create_dir_all(crate::layout::state_dir(root))?;
        for junk in [
            "junk",
            "folder 12\n",
            "token t\nfolder x\n",
            "token t\nfolder \n",
            "token t\nfolder\n",
        ] {
            fs::write(shares_file(root), junk)?;
            assert!(Record::read(root).is_err(), "{junk:?}");
            assert!(folders(root).is_err());
        }
        fs::write(shares_file(root), "token t\n")?;
        assert!(folders(root)?.is_empty());
        fs::write(shared_marker(&root.join("Shared")), "t\n")?;
        assert_eq!(folders(root)?, ["Shared"]);
        assert_eq!(make(root, "Shared-2")?, Made::Kept("Shared".into()));
        Ok(())
    }
}
