//! Each account's state DB, kept with the account in the state folder (not in the synced
//! folders: an unplugged disk doesn't take its state along, and the watcher never sees the
//! DB's writes), and the folders it syncs here, each found through its marker.

use std::path::{Path, PathBuf};

use hbsync_local::{Marker, STATE_DIR};
use hbsync_state::{StateDb, StateError};
use hbsync_types::NodeId;

use crate::Paths;
use crate::runner::IDENTITY_KEY;

#[derive(Debug, thiserror::Error)]
pub enum FolderError {
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
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The state DB of `identity`: `<state>/accounts/<identity in lower case, : as _>/state.db`.
pub fn db_path(paths: &Paths, identity: &str) -> PathBuf {
    let key = identity.to_lowercase().replace(':', "_");
    paths.state.join("accounts").join(key).join("state.db")
}

/// The account DB at `db`, for `identity` (made if new; refused if another's).
pub fn open_account(db: &Path, identity: &str) -> Result<StateDb, FolderError> {
    if let Some(dir) = db.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut account = StateDb::open_account(db)?;
    account
        .write(|s| match s.account_kv(IDENTITY_KEY)? {
            Some(theirs) if theirs != identity => Ok(Err(theirs)),
            Some(_) => Ok(Ok(())),
            None => s.set_account_kv(IDENTITY_KEY, identity).map(Ok),
        })?
        .map_err(|theirs| FolderError::OtherIdentity {
            folder: db.to_path_buf(),
            theirs,
            ours: identity.to_owned(),
        })?;
    Ok(account)
}

/// The account DB at `db` seen as the folder at `root`, which its marker must name.
pub fn open_folder(db: &Path, identity: &str, root: &Path) -> Result<StateDb, FolderError> {
    let marker = Marker::read(root)?.ok_or_else(|| FolderError::NoMarker(root.to_path_buf()))?;
    if marker.identity != identity {
        return Err(FolderError::OtherIdentity {
            folder: root.to_path_buf(),
            theirs: marker.identity,
            ours: identity.to_owned(),
        });
    }
    let mut account = open_account(db, identity)?;
    let folder = account
        .read(|s| s.folders())?
        .into_iter()
        .find(|f| f.marker == marker.id)
        .ok_or_else(|| FolderError::Unknown(root.to_path_buf()))?;
    Ok(account.with_folder(folder.id))
}

/// Sets the folder at `root` up in `identity`'s DB at `db`, holding the drive's root: it
/// starts over (an earlier folder holding the root goes, with its rows; a DB of an older
/// Filesync in its `.hbsync` too), and its marker is written last.
pub fn add_folder(db: &Path, identity: &str, root: &Path) -> Result<StateDb, FolderError> {
    let marker = Marker::new(identity);
    let mut account = open_account(db, identity)?;
    let path = root.to_string_lossy();
    let id = account.write(|s| {
        for old in s.folders()?.into_iter().filter(|f| f.top == NodeId::ROOT) {
            s.remove_folder(old.id)?;
        }
        s.add_folder(NodeId::ROOT, &path, &marker.id)
    })?;
    let state = root.join(STATE_DIR);
    std::fs::create_dir_all(&state)?;
    for old in ["state.db", "state.db-wal", "state.db-shm"] {
        crate::paths::remove_if_exists(&state.join(old))?;
    }
    marker.write(root)?;
    Ok(account.with_folder(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_is_found_through_its_marker() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let db = db_path(&paths, "Anna.Example.com:8443");
        assert!(db.ends_with("accounts/anna.example.com_8443/state.db"));
        let root = home.path().join("Homebase");
        assert!(matches!(
            open_folder(&db, "anna.example.com", &root),
            Err(FolderError::NoMarker(_))
        ));
        std::fs::create_dir_all(root.join(STATE_DIR))?;
        std::fs::write(root.join(STATE_DIR).join("state.db"), "an older Filesync's")?;
        let first = add_folder(&db, "anna.example.com", &root)?.folder();
        assert!(!root.join(STATE_DIR).join("state.db").exists());
        assert_eq!(open_folder(&db, "anna.example.com", &root)?.folder(), first);
        assert!(matches!(
            open_folder(&db, "bob.example.com", &root),
            Err(FolderError::OtherIdentity { .. })
        ));
        // Set up again: a new folder, the old one's rows gone.
        let again = add_folder(&db, "anna.example.com", &root)?.folder();
        assert_ne!(again, first);
        assert_eq!(
            open_account(&db, "anna.example.com")?
                .read(|s| s.folders())?
                .len(),
            1
        );
        // A wiped state folder doesn't know the marker.
        std::fs::remove_dir_all(db.parent().ok_or("parent")?)?;
        assert!(matches!(
            open_folder(&db, "anna.example.com", &root),
            Err(FolderError::Unknown(_))
        ));
        Ok(())
    }
}
