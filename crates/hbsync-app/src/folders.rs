//! The folders this computer syncs: the daemon's half of setting one up or taking it away. A
//! folder is set up in its account's state DB (its marker written, [`SyncRoot`]) and listed in
//! the config; the daemon then starts syncing it.

use std::path::Path;

use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_types::Role;

use crate::{Config, Paths, Root};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Sets the folder at `path` up for `identity` (kept as it was if it is already: its marker
/// names it), and lists it in the config. `hbsync setup` has checked that this is what the
/// user wants (a folder whose state is gone is merged only when asked).
pub fn add(paths: &Paths, identity: &str, path: &Path, role: Option<Role>) -> Result<(), Error> {
    if role.is_some() {
        return Err("role folders can't be added yet".into());
    }
    let db = paths.account_db(identity);
    match SyncRoot::open(&db, identity, path) {
        Ok(_) => {}
        Err(RootError::NoMarker(_) | RootError::Unknown(_) | RootError::Older(_)) => {
            SyncRoot::create(&db, identity, path)?;
        }
        Err(e) => return Err(e.into()),
    }
    let config_path = paths.config_file();
    let mut config = Config::load(&config_path)?.unwrap_or_default();
    if !config.roots.iter().any(|r| r.folder == path) {
        config.roots.push(Root {
            role,
            ..Root::main(identity, path)
        });
        config.save(&config_path)?;
    }
    Ok(())
}

/// Takes the folder at `path` off the config; its files and its state stay (setting it up again
/// picks up where it was). The folder it was; `None` if it wasn't listed.
pub fn remove(paths: &Paths, path: &Path) -> Result<Option<Root>, Error> {
    let config_path = paths.config_file();
    let Some(mut config) = Config::load(&config_path)? else {
        return Ok(None);
    };
    let Some(at) = config.roots.iter().position(|r| r.folder == path) else {
        return Ok(None);
    };
    let root = config.roots.remove(at);
    config.save(&config_path)?;
    Ok(Some(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_is_set_up_once_and_taken_off_again() -> Result<(), Error> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(&folder)?;
        add(&paths, "a.example", &folder, None)?;
        let marker = hbsync_local::Marker::read(&folder)?;
        add(&paths, "a.example", &folder, None)?;
        assert_eq!(hbsync_local::Marker::read(&folder)?, marker, "kept");
        let config = Config::load(&paths.config_file())?.ok_or("no config")?;
        assert_eq!(config.roots, [Root::main("a.example", &folder)]);
        assert!(add(&paths, "a.example", &folder, Some(Role::Documents)).is_err());

        assert_eq!(
            remove(&paths, &folder)?,
            Some(Root::main("a.example", &folder))
        );
        assert_eq!(remove(&paths, &folder)?, None);
        assert!(
            folder.join(hbsync_local::STATE_DIR).exists(),
            "its state stays"
        );
        Ok(())
    }
}
