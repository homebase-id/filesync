//! A synced folder on disk and its place in the account's state DB. The DB lives with the
//! account, outside the folder (an unplugged disk doesn't take its state along, and the
//! watcher never sees the DB's writes); the folder's marker (`.hbsync/folder`) names the
//! account and which of its folders it is. What else the folder keeps in `.hbsync` is
//! [`hbsync_local::layout`].

use std::path::{Path, PathBuf};

use hbsync_local::{Marker, layout};
use hbsync_remote::ErrorClass;
use hbsync_state::{FolderRow, Reader, StateDb, StateError, Unusable, keys};
use hbsync_types::NodeId;
use hbsync_types::activity::Synced;

#[derive(Debug, thiserror::Error)]
pub enum RootError {
    /// No marker: the folder is gone, not mounted, or was never set up.
    #[error("{0} has no Filesync marker: it is gone, not mounted, or not set up")]
    NoMarker(PathBuf),
    #[error("{folder} is synced with {theirs}, not {ours}")]
    OtherIdentity {
        folder: PathBuf,
        theirs: String,
        ours: String,
    },
    /// Its marker names a folder the account's DB doesn't have (the state folder was wiped).
    #[error("{0} isn't known to this computer's state: set it up again")]
    Unknown(PathBuf),
    /// Set up as another folder (the main folder, another role's).
    #[error("{0} is synced as another folder already: remove it first")]
    OtherTop(PathBuf),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl RootError {
    /// What it means ([`ErrorClass`]): a folder or a state that only the user can set right,
    /// or its DB's or disk's error.
    pub fn class(&self) -> ErrorClass {
        match self {
            RootError::State(e) => e.class(),
            RootError::Io(e) => ErrorClass::of_io(e),
            // A volume not mounted yet may come back.
            RootError::NoMarker(_) => ErrorClass::LocalFault,
            RootError::OtherIdentity { .. } | RootError::Unknown(_) | RootError::OtherTop(_) => {
                ErrorClass::NeedsUser
            }
        }
    }

    /// Why the account's DB can't be used at all, if that is what it means
    /// ([`StateError::unusable`]).
    pub fn unusable(&self) -> Option<Unusable> {
        match self {
            RootError::State(e) => e.unusable(),
            _ => None,
        }
    }
}

/// A folder synced with an account, found through its marker.
#[derive(Debug, Clone)]
pub struct SyncRoot {
    path: PathBuf,
    db: PathBuf,
    identity: String,
    folder: i64,
    top: NodeId,
}

impl SyncRoot {
    /// The folder at `path` of `identity`'s DB at `db`, which its marker must name. A folder
    /// moved since is found all the same, and its new place recorded.
    pub fn open(db: &Path, identity: &str, path: &Path) -> Result<SyncRoot, RootError> {
        let (root, recorded) = match Self::locate(db, identity, path) {
            Err(RootError::State(StateError::Behind { .. })) => {
                Self::open_account(db, identity)?;
                Self::locate(db, identity, path)?
            }
            found => found?,
        };
        let text = path.to_string_lossy();
        if recorded != text {
            Self::open_account(db, identity)?
                .write(|s| s.account().set_folder_path(root.folder, &text))?;
        }
        Ok(root)
    }

    /// [`SyncRoot::open`] without writing anything (a moved folder's new place isn't
    /// recorded), for a check while the daemon may be syncing. A DB of an older schema is
    /// refused ([`StateError::Behind`]) rather than brought up to date: see
    /// [`SyncRoot::ready`].
    pub fn find(db: &Path, identity: &str, path: &Path) -> Result<SyncRoot, RootError> {
        Ok(Self::locate(db, identity, path)?.0)
    }

    /// The folder at `path` and the path its DB has for it.
    fn locate(db: &Path, identity: &str, path: &Path) -> Result<(SyncRoot, String), RootError> {
        let marker = Self::check(path, identity)?;
        let unknown = || RootError::Unknown(path.to_path_buf());
        let folder = AccountReader::open(db, identity)?
            .ok_or_else(unknown)?
            .folders()?
            .into_iter()
            .find(|f| f.marker == marker.id)
            .ok_or_else(unknown)?;
        let root = SyncRoot {
            path: path.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            folder: folder.id,
            top: folder.top,
        };
        Ok((root, folder.path))
    }

    /// Sets the folder at `path` up in `identity`'s DB at `db` as the main folder, holding the
    /// drive's root ([`SyncRoot::create_for`]).
    pub fn create(db: &Path, identity: &str, path: &Path) -> Result<SyncRoot, RootError> {
        Self::create_for(db, identity, path, NodeId::ROOT)
    }

    /// Sets the folder at `path` up in `identity`'s DB at `db`, holding the drive's node `top`
    /// (the root, or a role folder's): it starts over (an earlier folder holding `top` goes,
    /// with its rows), and its marker is written last.
    pub fn create_for(
        db: &Path,
        identity: &str,
        path: &Path,
        top: NodeId,
    ) -> Result<SyncRoot, RootError> {
        let marker = Marker::new(identity);
        let mut account = Self::open_account(db, identity)?;
        let text = path.to_string_lossy();
        // Durable before the marker exists: a marker the DB lost would be refused as foreign.
        let folder = account.write_durable(|s| {
            for old in s.folders()?.into_iter().filter(|f| f.top == top) {
                s.account().remove_folder(old.id)?;
            }
            s.account().add_folder(top, &text, &marker.id)
        })?;
        std::fs::create_dir_all(layout::state_dir(path))?;
        marker.write(path)?;
        Ok(SyncRoot {
            path: path.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            folder,
            top,
        })
    }

    /// [`SyncRoot::open`], or [`SyncRoot::create`] when it has no marker or isn't known to the
    /// DB: the caller has checked that starting it over is wanted.
    pub fn open_or_create(db: &Path, identity: &str, path: &Path) -> Result<SyncRoot, RootError> {
        Self::open_or_create_for(db, identity, path, NodeId::ROOT)
    }

    /// [`SyncRoot::open_or_create`] for a folder holding the drive's node `top`; one set up for
    /// another top is refused.
    pub fn open_or_create_for(
        db: &Path,
        identity: &str,
        path: &Path,
        top: NodeId,
    ) -> Result<SyncRoot, RootError> {
        match Self::open(db, identity, path) {
            Err(RootError::NoMarker(_) | RootError::Unknown(_)) => {
                Self::create_for(db, identity, path, top)
            }
            Ok(found) if found.top != top => Err(RootError::OtherTop(path.to_path_buf())),
            other => other,
        }
    }

    /// The marker of the folder at `path`, which must be `identity`'s.
    pub fn check(path: &Path, identity: &str) -> Result<Marker, RootError> {
        let Some(marker) = Marker::read(path)? else {
            return Err(RootError::NoMarker(path.to_path_buf()));
        };
        if marker.identity != identity {
            return Err(RootError::OtherIdentity {
                folder: path.to_path_buf(),
                theirs: marker.identity,
                ours: identity.to_owned(),
            });
        }
        Ok(marker)
    }

    /// Whether `identity`'s DB at `db` has a folder at `path` (whatever its marker says).
    pub fn knows_path(db: &Path, identity: &str, path: &Path) -> Result<bool, RootError> {
        let Some(mut account) = AccountReader::open(db, identity)? else {
            return Ok(false);
        };
        let text = path.to_string_lossy();
        Ok(account.folders()?.iter().any(|f| f.path == text))
    }

    /// Readies `identity`'s DB at `db` as the account starts (its daemon's start, or setup's
    /// while none runs), for what only looks next ([`SyncRoot::find`], [`AccountReader`]).
    /// Only while nothing else may write it: an older daemon would find a schema it doesn't
    /// know. The whole file is checked first ([`StateDb::check_file`]: it reads every page),
    /// then brought up to date (made if new; refused if another's), then its schema checked.
    /// One that can't be used at all ([`StateError::unusable`]: from before 0.7, not a
    /// database, damaged) is set aside, never deleted ([`set_aside`]), and a new one made,
    /// which says so ([`keys::SET_ASIDE`]) until it knows each of `folders` (their places and
    /// the drive's nodes they hold, as the config lists them): each is set up again as a folder
    /// with files ([`SyncRoot::create_for`]: what it holds is adopted, nothing is copied twice,
    /// nothing deleted). One not there now (a disk not plugged in), or one that fails, is set
    /// up at a later start. Any other error is returned as it is: a newer version's DB, or one
    /// busy, locked, unreadable for now. With `found`, the DB was found unusable while the
    /// account ran (SQLite said so: a check of its pages may not see an index's damage), and is
    /// set aside unchecked. What was set aside now, if anything.
    pub fn ready(
        db: &Path,
        identity: &str,
        folders: &[(&Path, NodeId)],
        found: Option<Unusable>,
    ) -> Result<Option<SetAside>, RootError> {
        let checked = match found {
            Some(why) => Err((why, "found unusable while syncing".to_owned())),
            None => match Self::check_and_open(db, identity) {
                Ok(account) => Ok(account),
                Err(e) => {
                    let Some(why) = e.unusable() else {
                        return Err(e);
                    };
                    Err((why, e.to_string()))
                }
            },
        };
        let (mut account, set) = match checked {
            Ok(account) => (account, None),
            Err((why, e)) => {
                let to = set_aside(db, why)?;
                tracing::warn!(
                    "{}: {e}: set aside as {}, and made again",
                    db.display(),
                    to.display()
                );
                let mut account = Self::open_account(db, identity)?;
                account
                    .write_durable(|s| s.account().set_account_kv(keys::SET_ASIDE, why.name()))?;
                (account, Some(SetAside { why, to }))
            }
        };
        // Made in place of one set aside; or knowing no folder at all beside one set aside
        // (its key lost to a crash between the rename and its write): what it can't have set
        // up is set up again.
        let (replaced, known) =
            account.read(|s| Ok((s.account_kv(keys::SET_ASIDE)?.is_some(), s.folders()?)))?;
        if replaced || (known.is_empty() && beside_one_set_aside(db)) {
            Self::set_up_again(&mut account, db, identity, folders, &known, replaced)?;
        }
        Ok(set)
    }

    /// Whether `identity`'s DB at `db` synced with another drive than the one made at `made`
    /// (its marker's time, node schema "The drive's marker"): one deleted and made again, the
    /// same alias, nothing of the old one on it. Then the DB must be set aside
    /// ([`Unusable::OtherDrive`]) before anything syncs: read against the new drive, what it
    /// knows would look deleted there. A DB that knows no drive yet (new, set up again, or from
    /// before the marker) takes this one. Only while nothing else writes it.
    pub fn other_drive(db: &Path, identity: &str, made: i64) -> Result<bool, RootError> {
        let mut account = Self::open_account(db, identity)?;
        let made = made.to_string();
        match account.read(|s| s.account_kv(keys::DRIVE_MADE))? {
            Some(known) => Ok(known != made),
            None => {
                account.write_durable(|s| s.account().set_account_kv(keys::DRIVE_MADE, &made))?;
                Ok(false)
            }
        }
    }

    /// The account's DB at `db`, its whole file checked, then brought up to date (made if new;
    /// refused if another's), then its schema checked ([`SyncRoot::ready`]).
    fn check_and_open(db: &Path, identity: &str) -> Result<StateDb, RootError> {
        let whole = StateDb::check_file(db)?;
        let account = Self::open_account(db, identity)?;
        // One brought up to date (or made) is whole only now.
        if !whole {
            account.check_schema()?;
        }
        Ok(account)
    }

    /// Sets each of `folders` that is `identity`'s (its marker says so) and unknown to the
    /// `account` (at `db`; it knows those of `known`) up again; once it knows every one there
    /// is, it forgets it `replaced` one. One not there now, or that fails, waits for a later
    /// start; one another identity's is left to its runner, which refuses it.
    fn set_up_again(
        account: &mut StateDb,
        db: &Path,
        identity: &str,
        folders: &[(&Path, NodeId)],
        known: &[FolderRow],
        replaced: bool,
    ) -> Result<(), RootError> {
        let mut waiting = false;
        for &(path, top) in folders {
            let again = match Self::check(path, identity) {
                Ok(marker) if known.iter().any(|f| f.marker == marker.id) => continue,
                Ok(_) => Self::create_for(db, identity, path, top).map(drop),
                Err(e) => Err(e),
            };
            if let Err(e) = again {
                tracing::warn!("{} isn't set up again now: {e}", path.display());
                waiting |= !matches!(e, RootError::OtherIdentity { .. });
            }
        }
        if !waiting && replaced {
            account.write(|s| s.account().delete_account_kv(keys::SET_ASIDE))?;
        }
        Ok(())
    }

    /// The account DB at `db`, for `identity` (made if new; refused if another's).
    pub(crate) fn open_account(db: &Path, identity: &str) -> Result<StateDb, RootError> {
        if let Some(dir) = db.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut account = StateDb::open_account(db)?;
        // Read first: a DB that has its identity is opened without writing.
        let theirs = account.read(|s| s.account_kv(keys::IDENTITY))?;
        if theirs.is_none() {
            account.write(|s| s.account().set_account_kv(keys::IDENTITY, identity))?;
        }
        refuse_other(db, theirs, identity)?;
        Ok(account)
    }

    /// A connection to the account's DB seeing this folder.
    pub(crate) fn state(&self) -> Result<StateDb, RootError> {
        Ok(StateDb::open_folder(&self.db, self.folder)?)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The folder's id in the account's DB.
    pub fn folder(&self) -> i64 {
        self.folder
    }

    /// The drive's node the folder holds: the drive's root for the main folder.
    pub fn top(&self) -> NodeId {
        self.top
    }
}

/// An account's DB that couldn't be used, set aside ([`SyncRoot::ready`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAside {
    pub why: Unusable,
    /// Where it is now, its `-wal` and `-shm` files beside it, named alike.
    pub to: PathBuf,
}

/// A DB's file, and its log's and shared memory's: `state.db`, `state.db-wal`, `state.db-shm`.
const SUFFIXES: [&str; 3] = ["", "-wal", "-shm"];

/// `path` with `suffix` added to its name.
fn with(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// What the name of a DB set aside for `why` adds to its own, before the time: `.too-old-`.
fn aside(why: Unusable) -> String {
    format!(".{}-", why.name())
}

/// Whether a DB was set aside beside the one at `db` ([`set_aside`]'s names).
fn beside_one_set_aside(db: &Path) -> bool {
    let (Some(dir), Some(name)) = (db.parent(), db.file_name()) else {
        return false;
    };
    let name = name.to_string_lossy();
    let named = [Unusable::TooOld, Unusable::Corrupt, Unusable::OtherDrive]
        .map(|why| format!("{name}{}", aside(why)));
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            let file = e.file_name();
            let file = file.to_string_lossy();
            named.iter().any(|n| file.starts_with(n.as_str()))
        })
}

/// Moves the DB at `db` aside, beside it as `<name>.<why>-<Unix time>` (`state.db.too-old-…`,
/// `state.db.corrupt-…`; numbered on if taken, never over another file), with its `-wal` and
/// `-shm` files named alike. The DB first: its place never holds it without its log (SQLite
/// drops a log it finds beside a new, empty DB). Where it is now.
fn set_aside(db: &Path, why: Unusable) -> std::io::Result<PathBuf> {
    let stamp = format!("{}{}", aside(why), hbsync_types::unix_now());
    let mut to = with(db, &stamp);
    let mut n = 0;
    while SUFFIXES
        .iter()
        .any(|s| std::fs::symlink_metadata(with(&to, s)).is_ok())
    {
        n += 1;
        to = with(db, &format!("{stamp}-{n}"));
    }
    // What moved, put back if the rest can't follow: never a DB apart from its log.
    let mut moved: Vec<&str> = Vec::new();
    for suffix in SUFFIXES {
        match hbsync_platform::ops::rename_noreplace(&with(db, suffix), &with(&to, suffix)) {
            Ok(()) => moved.push(suffix),
            Err(e) if !suffix.is_empty() && e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                for back in moved.iter().rev() {
                    if let Err(e) =
                        hbsync_platform::ops::rename_noreplace(&with(&to, back), &with(db, back))
                    {
                        tracing::error!("{} can't be put back: {e}", with(&to, back).display());
                    }
                }
                return Err(e);
            }
        }
    }
    hbsync_platform::ops::sync_dirs(db.parent().map(Path::to_path_buf));
    Ok(to)
}

/// Refuses the DB at `db` if it is another identity's: `theirs`, the one it has (`None`: it
/// has nothing yet, its identity is written on the first open).
fn refuse_other(db: &Path, theirs: Option<String>, identity: &str) -> Result<(), RootError> {
    match theirs {
        Some(theirs) if theirs != identity => Err(RootError::OtherIdentity {
            folder: db.to_path_buf(),
            theirs,
            ours: identity.to_owned(),
        }),
        _ => Ok(()),
    }
}

/// The account's DB for reading only, its identity checked: what is answered (Recent,
/// Recently deleted) and checked (setup) while the daemon's engines may be writing it.
#[derive(Debug)]
pub struct AccountReader {
    /// The crate's: Recently deleted's queries live with its listing (`deleted::listing`).
    pub(crate) db: Reader,
}

impl AccountReader {
    /// `identity`'s DB at `db` (refused if another's); `None` if there is none.
    pub fn open(db: &Path, identity: &str) -> Result<Option<AccountReader>, RootError> {
        let Some(mut reader) = Reader::open(db)? else {
            return Ok(None);
        };
        refuse_other(db, reader.read(|s| s.account_kv(keys::IDENTITY))?, identity)?;
        Ok(Some(AccountReader { db: reader }))
    }

    /// The folders synced with the account on this device.
    pub fn folders(&mut self) -> Result<Vec<FolderRow>, StateError> {
        self.db.read(|s| s.folders())
    }

    /// The newest `limit` entries of `folder`'s recently synced list.
    pub fn recent_synced(&mut self, folder: i64, limit: usize) -> Result<Vec<Synced>, StateError> {
        self.db.read_folder(folder, |s| s.recent_synced(limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_is_found_through_its_marker() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("accounts/anna/state.db");
        let root = home.path().join("Homebase");
        let anna = "anna.example.com";
        assert!(matches!(
            SyncRoot::open(&db, anna, &root),
            Err(RootError::NoMarker(_))
        ));
        let first = SyncRoot::create(&db, anna, &root)?.folder();
        assert_eq!(SyncRoot::open(&db, anna, &root)?.folder(), first);
        assert!(matches!(
            SyncRoot::open(&db, "bob.example.com", &root),
            Err(RootError::OtherIdentity { .. })
        ));
        // Set up again: a new folder, the old one's rows gone.
        let again = SyncRoot::create(&db, anna, &root)?;
        assert_ne!(again.folder(), first);
        let account = AccountReader::open(&db, anna)?.ok_or("no DB")?.folders()?;
        assert_eq!(account.len(), 1);
        // Moved: found through its marker, its new place recorded.
        let moved = home.path().join("Moved");
        std::fs::rename(&root, &moved)?;
        assert_eq!(SyncRoot::open(&db, anna, &moved)?.folder(), again.folder());
        assert!(SyncRoot::knows_path(&db, anna, &moved)?);
        // A wiped state folder doesn't know the marker.
        std::fs::remove_dir_all(db.parent().ok_or("parent")?)?;
        assert!(matches!(
            SyncRoot::open(&db, anna, &moved),
            Err(RootError::Unknown(_))
        ));
        Ok(())
    }

    /// A DB from before 0.7, with its log, is set aside, both kept and named alike, and a new
    /// one made: it knows no folder. Once is enough.
    #[test]
    fn a_db_too_old_is_set_aside() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("accounts/anna/state.db");
        let anna = "anna.example.com";
        let root = home.path().join("Homebase");
        std::fs::create_dir_all(&root)?;
        SyncRoot::create(&db, anna, &root)?;
        // As 0.6 left it: an older version, a log beside it.
        rusqlite::Connection::open(&db)?.pragma_update(None, "user_version", 6)?;
        std::fs::write(with(&db, "-wal"), "its log")?;
        let before = std::fs::read(&db)?;

        // Its folder not there yet (a disk not plugged in): set up at a later start.
        let away = home.path().join("unplugged");
        std::fs::rename(&root, &away)?;
        let folders = [(root.as_path(), NodeId::ROOT)];
        let set = SyncRoot::ready(&db, anna, &folders, None)?.ok_or("not set aside")?;
        assert_eq!(set.why, Unusable::TooOld);
        let name = set.to.file_name().ok_or("name")?.to_string_lossy();
        assert!(name.starts_with("state.db.too-old-"), "{name}");
        assert_eq!(std::fs::read(&set.to)?, before, "kept as it was");
        assert_eq!(std::fs::read(with(&set.to, "-wal"))?, b"its log");
        let mut reader = AccountReader::open(&db, anna)?.ok_or("no DB")?;
        assert!(reader.folders()?.is_empty());
        drop(reader);
        assert!(!root.exists(), "nothing made where it was");
        std::fs::rename(&away, &root)?;
        assert!(matches!(
            SyncRoot::find(&db, anna, &root),
            Err(RootError::Unknown(_))
        ));
        assert_eq!(SyncRoot::ready(&db, anna, &folders, None)?, None);
        SyncRoot::find(&db, anna, &root)?;
        // Every folder known again: it no longer says it replaced one.
        let replaced = StateDb::open_account(&db)?.read(|s| s.account_kv(keys::SET_ASIDE))?;
        assert_eq!(replaced, None);
        Ok(())
    }

    /// Garbage where the DB was is set aside as corrupt; a newer version's DB is refused and
    /// left as it is.
    #[test]
    fn a_corrupt_db_is_set_aside_and_a_newer_one_refused() -> Result<(), Box<dyn std::error::Error>>
    {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let anna = "anna.example.com";
        std::fs::write(&db, [0x5a_u8; 8192])?;
        let set = SyncRoot::ready(&db, anna, &[], None)?.ok_or("not set aside")?;
        assert_eq!(set.why, Unusable::Corrupt);
        assert!(set.to.to_string_lossy().contains("state.db.corrupt-"));
        assert_eq!(std::fs::read(&set.to)?, [0x5a_u8; 8192]);

        rusqlite::Connection::open(&db)?.pragma_update(None, "user_version", 9999)?;
        let before = std::fs::read(&db)?;
        assert!(matches!(
            SyncRoot::ready(&db, anna, &[], None),
            Err(RootError::State(StateError::TooNew { .. }))
        ));
        assert_eq!(std::fs::read(&db)?, before);
        let aside = std::fs::read_dir(home.path())?
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("state.db."))
            .count();
        assert_eq!(aside, 1, "nothing else set aside");
        Ok(())
    }

    /// A DB of an older version whose upgrade fails for a part of its schema it lacks is set
    /// aside as corrupt, not returned as the upgrade's error.
    #[test]
    fn a_db_whose_upgrade_lacks_a_table_is_set_aside() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let anna = "anna.example.com";
        let root = home.path().join("Homebase");
        std::fs::create_dir_all(&root)?;
        SyncRoot::create(&db, anna, &root)?;
        let conn = rusqlite::Connection::open(&db)?;
        conn.execute_batch("DROP TABLE remote")?;
        conn.pragma_update(None, "user_version", 13)?;
        drop(conn);
        let folders = [(root.as_path(), NodeId::ROOT)];
        let set = SyncRoot::ready(&db, anna, &folders, None)?.ok_or("not set aside")?;
        assert_eq!(set.why, Unusable::Corrupt);
        SyncRoot::find(&db, anna, &root)?;
        Ok(())
    }

    /// A DB found unusable while the account ran is set aside though its check finds nothing
    /// (an index's damage, which `quick_check` doesn't look for), and its folders set up again.
    #[test]
    fn a_db_found_unusable_is_set_aside_unchecked() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let anna = "anna.example.com";
        let root = home.path().join("Homebase");
        std::fs::create_dir_all(&root)?;
        SyncRoot::create(&db, anna, &root)?;
        let folders = [(root.as_path(), NodeId::ROOT)];
        assert_eq!(SyncRoot::ready(&db, anna, &folders, None)?, None, "whole");
        let found = Some(Unusable::Corrupt);
        let set = SyncRoot::ready(&db, anna, &folders, found)?.ok_or("not set aside")?;
        assert_eq!(set.why, Unusable::Corrupt);
        assert!(set.to.exists(), "kept");
        SyncRoot::find(&db, anna, &root)?;
        let replaced = StateDb::open_account(&db)?.read(|s| s.account_kv(keys::SET_ASIDE))?;
        assert_eq!(replaced, None, "every folder set up again");
        Ok(())
    }

    /// One set aside in the same second as another takes the next name, never its place.
    #[test]
    fn a_set_aside_never_overwrites_one() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let mut kept = Vec::new();
        for n in 0..3 {
            std::fs::write(&db, format!("db {n}"))?;
            kept.push(super::set_aside(&db, Unusable::Corrupt)?);
        }
        for (n, at) in kept.iter().enumerate() {
            assert_eq!(std::fs::read_to_string(at)?, format!("db {n}"));
        }
        Ok(())
    }

    /// What only looks ([`SyncRoot::find`], [`SyncRoot::knows_path`], [`AccountReader`])
    /// writes nothing: no DB is made, a moved folder's place isn't recorded.
    #[test]
    fn looking_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let db = home.path().join("accounts/anna/state.db");
        let root = home.path().join("Homebase");
        let anna = "anna.example.com";
        std::fs::create_dir_all(&root)?;
        assert!(!SyncRoot::knows_path(&db, anna, &root)?);
        assert!(AccountReader::open(&db, anna)?.is_none());
        assert!(!db.parent().ok_or("parent")?.exists(), "no DB is made");

        let folder = SyncRoot::create(&db, anna, &root)?.folder();
        let moved = home.path().join("Moved");
        std::fs::rename(&root, &moved)?;
        assert_eq!(SyncRoot::find(&db, anna, &moved)?.folder(), folder);
        assert!(
            SyncRoot::knows_path(&db, anna, &root)?,
            "the old place stays"
        );
        assert!(!SyncRoot::knows_path(&db, anna, &moved)?);
        assert!(matches!(
            AccountReader::open(&db, "bob.example.com"),
            Err(RootError::OtherIdentity { .. })
        ));
        Ok(())
    }
}
