//! The folders this computer syncs: the daemon's half of setting one up or taking it away. A
//! folder is set up in its account's state DB (its marker written, [`SyncRoot`]) and listed in
//! the config; the daemon then starts syncing it.

use std::path::Path;

use hbsync_engine::root::SyncRoot;
use hbsync_types::Role;

use crate::{Config, Paths, Root};

pub use crate::control::Error;

/// Sets the folder at `path` up for `identity` (kept as it was if it is already: its marker
/// names it), and lists it in the config. `hbsync setup` has checked that this is what the
/// user wants (a folder whose state is gone is merged only when asked). An identity has one
/// main folder: another one is refused.
pub fn add(paths: &Paths, identity: &str, path: &Path, role: Option<Role>) -> Result<(), Error> {
    if role.is_some() {
        return Err("role folders can't be added yet".into());
    }
    let config_path = paths.config_file();
    let config = Config::load(&config_path)?.unwrap_or_default();
    if let Some(other) = config
        .roots
        .iter()
        .find(|r| r.identity == identity && r.role.is_none() && r.folder != path)
    {
        return Err(format!(
            "{identity} is synced with {} already: unlink it first",
            other.folder.display()
        )
        .into());
    }
    SyncRoot::open_or_create(&paths.account_db(identity), identity, path)?;
    Config::update(&config_path, |config| {
        if !config.roots.iter().any(|r| r.folder == path) {
            config.roots.push(Root {
                role,
                ..Root::main(identity, path)
            });
        }
    })?;
    Ok(())
}

/// Takes the folder at `path` off the config; its files and its state stay (setting it up again
/// picks up where it was). The folder it was; `None` if it wasn't listed.
pub fn remove(paths: &Paths, path: &Path) -> Result<Option<Root>, Error> {
    Ok(Config::update(&paths.config_file(), |config| {
        let at = config.roots.iter().position(|r| r.folder == path)?;
        Some(config.roots.remove(at))
    })?)
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
        let other = home.path().join("Other");
        std::fs::create_dir_all(&other)?;
        assert!(
            add(&paths, "a.example", &other, None).is_err(),
            "one main folder per identity"
        );

        assert_eq!(
            remove(&paths, &folder)?,
            Some(Root::main("a.example", &folder))
        );
        assert_eq!(remove(&paths, &folder)?, None);
        assert!(
            hbsync_local::layout::state_dir(&folder).exists(),
            "its state stays"
        );
        Ok(())
    }
}
