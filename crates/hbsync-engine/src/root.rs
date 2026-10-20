//! A synced folder on disk and its place in the account's state DB. The DB lives with the
//! account, outside the folder (an unplugged disk doesn't take its state along, and the
//! watcher never sees the DB's writes); the folder's marker (`.hbsync/folder`) names the
//! account and which of its folders it is. What else the folder keeps in `.hbsync` is
//! [`hbsync_local::layout`].

use std::path::{Path, PathBuf};

use hbsync_local::{Marker, layout};
use hbsync_remote::ErrorClass;
use hbsync_state::{FolderRow, Reader, StateDb, StateError, keys};
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
                Self::upgrade(db, identity)?;
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
    /// [`SyncRoot::upgrade`].
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

    /// Brings `identity`'s DB at `db` up to date (made if new; refused if another's), for
    /// what only looks next ([`SyncRoot::find`], [`AccountReader`]). Only while nothing else
    /// may write it: an older daemon would find a schema it doesn't know.
    pub fn upgrade(db: &Path, identity: &str) -> Result<(), RootError> {
        Self::open_account(db, identity).map(drop)
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
