//! The folders this computer syncs: the daemon's half of setting one up or taking it away. A
//! folder is set up in its account's state DB (its marker written, [`SyncRoot`]) and listed in
//! the config; the daemon then starts syncing it.

use std::path::Path;

use hbsync_engine::root::SyncRoot;
use hbsync_types::{NodeId, Role};

use crate::{Config, Paths, Root};

pub use crate::control::Error;

/// Sets the folder at `path` up for `identity` (kept as it was if it is already: its marker
/// names it), and lists it in the config. `hbsync setup` has checked that this is what the
/// user wants (a folder whose state is gone is merged only when asked). With `role`, it syncs
/// that role folder of the drive (joined if the drive has it, back from Recently deleted, or
/// made when the folder first syncs). An identity has one main folder and one of each role;
/// a folder inside another synced folder, or holding one, is refused.
pub fn add(paths: &Paths, identity: &str, path: &Path, role: Option<Role>) -> Result<(), Error> {
    // One at a time in this process (the tray's pages serve each request on a thread): two
    // set-ups of one folder would each make it anew, the second undoing the first.
    static ADDING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _one = ADDING.lock().unwrap_or_else(|e| e.into_inner());
    let home = crate::paths::home().map(|h| crate::setup::resolve(&h));
    if let Some(why) = crate::setup::refuse_folder(path, home.as_deref()) {
        return Err(why.into());
    }
    // One this identity syncs already stays so.
    let ours = hbsync_local::Marker::read(path)?.is_some_and(|m| m.identity == identity);
    if !ours && let Some(why) = crate::setup::other_app(path) {
        return Err(why.into());
    }
    let config_path = paths.config_file();
    let config = Config::load(&config_path)?.unwrap_or_default();
    for other in config.roots.iter().filter(|r| r.folder != path) {
        if other.identity == identity && other.role == role {
            let what = role.map_or("its main folder", |r| r.name());
            return Err(format!(
                "{identity} syncs {what} with {} already: remove it first",
                other.folder.display()
            )
            .into());
        }
        if path.starts_with(&other.folder) || other.folder.starts_with(path) {
            return Err(format!(
                "{} and {} can't both be synced: one holds the other",
                path.display(),
                other.folder.display()
            )
            .into());
        }
    }
    let top = role.map_or(NodeId::ROOT, Role::node);
    SyncRoot::open_or_create_for(&paths.account_db(identity), identity, path, top)?;
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
        assert!(
            add(&paths, "a.example", &folder, Some(Role::Documents)).is_err(),
            "set up as the main folder"
        );
        let other = home.path().join("Other");
        std::fs::create_dir_all(&other)?;
        assert!(
            add(&paths, "a.example", &other, None).is_err(),
            "one main folder per identity"
        );

        let inside = folder.join("Documents");
        std::fs::create_dir_all(&inside)?;
        assert!(
            add(&paths, "b.example", &inside, None).is_err(),
            "not inside another synced folder"
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

    #[test]
    fn a_role_folder_is_set_up_once_per_identity() -> Result<(), Error> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let (main, docs, more) = (
            home.path().join("Homebase"),
            home.path().join("Documents"),
            home.path().join("More documents"),
        );
        for f in [&main, &docs, &more] {
            std::fs::create_dir_all(f)?;
        }
        add(&paths, "a.example", &main, None)?;
        add(&paths, "a.example", &docs, Some(Role::Documents))?;
        add(&paths, "a.example", &docs, Some(Role::Documents))?;
        assert!(
            add(&paths, "a.example", &more, Some(Role::Documents)).is_err(),
            "one Documents per identity"
        );
        add(&paths, "b.example", &more, Some(Role::Documents))?;
        let config = Config::load(&paths.config_file())?.ok_or("no config")?;
        let roles: Vec<_> = config
            .roots
            .iter()
            .map(|r| (r.identity.as_str(), r.role))
            .collect();
        assert_eq!(
            roles,
            [
                ("a.example", None),
                ("a.example", Some(Role::Documents)),
                ("b.example", Some(Role::Documents))
            ]
        );
        // Its DB knows it as the role folder: set up as another, it is refused.
        remove(&paths, &docs)?;
        assert!(add(&paths, "a.example", &docs, Some(Role::Pictures)).is_err());
        Ok(())
    }
}
