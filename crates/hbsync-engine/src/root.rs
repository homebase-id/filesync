//! A synced folder on disk and its place in the account's state DB. The DB lives with the
//! account, outside the folder (an unplugged disk doesn't take its state along, and the
//! watcher never sees the DB's writes); the folder's marker (`.hbsync/folder`) names the
//! account and which of its folders it is. What else the folder keeps in `.hbsync` is
//! [`hbsync_local::layout`].

use std::path::{Path, PathBuf};

use hbsync_local::{Marker, layout};
use hbsync_state::{StateDb, StateError, keys};
use hbsync_types::NodeId;

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
    /// A folder of an older Filesync: its state DB in `.hbsync`, no marker.
    #[error(
        "{0} was synced by an older Filesync: set it up again (its files are merged, not copied twice)"
    )]
    Older(PathBuf),
    /// Set up as another folder (the main folder, another role's).
    #[error("{0} is synced as another folder already: remove it first")]
    OtherTop(PathBuf),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
        let marker = Self::check(path, identity)?;
        let mut account = Self::open_account(db, identity)?;
        let folder = account
            .read(|s| s.folders())?
            .into_iter()
            .find(|f| f.marker == marker.id)
            .ok_or_else(|| RootError::Unknown(path.to_path_buf()))?;
        let text = path.to_string_lossy();
        if folder.path != text {
            account.write(|s| s.set_folder_path(folder.id, &text))?;
        }
        Ok(SyncRoot {
            path: path.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            folder: folder.id,
            top: folder.top,
        })
    }

    /// Sets the folder at `path` up in `identity`'s DB at `db` as the main folder, holding the
    /// drive's root ([`SyncRoot::create_for`]).
    pub fn create(db: &Path, identity: &str, path: &Path) -> Result<SyncRoot, RootError> {
        Self::create_for(db, identity, path, NodeId::ROOT)
    }

    /// Sets the folder at `path` up in `identity`'s DB at `db`, holding the drive's node `top`
    /// (the root, or a role folder's): it starts over (an earlier folder holding `top` goes,
    /// with its rows; a DB of an older Filesync in its `.hbsync` too), and its marker is
    /// written last.
    pub fn create_for(
        db: &Path,
        identity: &str,
        path: &Path,
        top: NodeId,
    ) -> Result<SyncRoot, RootError> {
        let marker = Marker::new(identity);
        let mut account = Self::open_account(db, identity)?;
        let text = path.to_string_lossy();
        let folder = account.write(|s| {
            for old in s.folders()?.into_iter().filter(|f| f.top == top) {
                s.remove_folder(old.id)?;
            }
            s.add_folder(top, &text, &marker.id)
        })?;
        std::fs::create_dir_all(layout::state_dir(path))?;
        for old in layout::older_db(path) {
            match std::fs::remove_file(old) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
        }
        marker.write(path)?;
        Ok(SyncRoot {
            path: path.to_path_buf(),
            db: db.to_path_buf(),
            identity: identity.to_owned(),
            folder,
            top,
        })
    }

    /// [`SyncRoot::open`], or [`SyncRoot::create`] when it has no marker, isn't known to the
    /// DB, or is an older Filesync's: the caller has checked that starting it over is wanted.
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
            Err(RootError::NoMarker(_) | RootError::Unknown(_) | RootError::Older(_)) => {
                Self::create_for(db, identity, path, top)
            }
            Ok(found) if found.top != top => Err(RootError::OtherTop(path.to_path_buf())),
            other => other,
        }
    }

    /// The marker of the folder at `path`, which must be `identity`'s.
    pub fn check(path: &Path, identity: &str) -> Result<Marker, RootError> {
        let Some(marker) = Marker::read(path)? else {
            return Err(if layout::older_db(path)[0].exists() {
                RootError::Older(path.to_path_buf())
            } else {
                RootError::NoMarker(path.to_path_buf())
            });
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
        if !db.exists() {
            return Ok(false);
        }
        let text = path.to_string_lossy();
        Ok(Self::open_account(db, identity)?
            .read(|s| s.folders())?
            .iter()
            .any(|f| f.path == text))
    }

    /// The account DB at `db`, for `identity` (made if new; refused if another's).
    pub fn open_account(db: &Path, identity: &str) -> Result<StateDb, RootError> {
        if let Some(dir) = db.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut account = StateDb::open_account(db)?;
        // Read first: a DB that has its identity is opened without writing.
        let theirs = match account.read(|s| s.account_kv(keys::IDENTITY))? {
            Some(theirs) => theirs,
            None => {
                account.write(|s| s.set_account_kv(keys::IDENTITY, identity))?;
                identity.to_owned()
            }
        };
        if theirs != identity {
            return Err(RootError::OtherIdentity {
                folder: db.to_path_buf(),
                theirs,
                ours: identity.to_owned(),
            });
        }
        Ok(account)
    }

    /// A connection to the account's DB seeing this folder.
    pub fn state(&self) -> Result<StateDb, RootError> {
        Ok(StateDb::open_folder(&self.db, self.folder)?)
    }

    /// A connection to the account's DB seeing no folder (for its [`crate::Drive`]).
    pub fn account(&self) -> Result<StateDb, RootError> {
        Self::open_account(&self.db, &self.identity)
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
        std::fs::create_dir_all(layout::state_dir(&root))?;
        std::fs::write(&layout::older_db(&root)[0], "an older Filesync's")?;
        assert!(matches!(
            SyncRoot::open(&db, anna, &root),
            Err(RootError::Older(_))
        ));
        let first = SyncRoot::create(&db, anna, &root)?.folder();
        assert!(!layout::older_db(&root)[0].exists());
        assert_eq!(SyncRoot::open(&db, anna, &root)?.folder(), first);
        assert!(matches!(
            SyncRoot::open(&db, "bob.example.com", &root),
            Err(RootError::OtherIdentity { .. })
        ));
        // Set up again: a new folder, the old one's rows gone.
        let again = SyncRoot::create(&db, anna, &root)?;
        assert_ne!(again.folder(), first);
        assert_eq!(again.account()?.read(|s| s.folders())?.len(), 1);
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
}
