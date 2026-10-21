//! The folders this computer syncs: the daemon's half of setting one up or taking it away. A
//! folder is set up in its account's state DB (its marker written, [`SyncRoot`]) and listed in
//! the config; the daemon then starts syncing it.

use std::path::{Path, PathBuf};

use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_local::STATE_DIR;
use hbsync_types::{NodeId, Role};

use crate::config::ConfigLock;
use crate::{Config, Error, Identity, Paths, Root};

/// Whether the folder at `path` (resolved: [`crate::setup::resolve`]) can be set up for
/// `identity` as `role` (`None`: its main folder), `home` being the home folder (resolved:
/// [`home`]); `Ok(true)` when its marker says it is this identity's already. The one set of
/// rules for setup, adding a role folder and the daemon. Refused:
/// - where no folder is synced: the home folder or one above it, a disk's top, a system
///   folder ([`refuse_folder`]);
/// - a second main folder (one in all, for now), or a second folder of one role for the
///   identity;
/// - one that holds, or sits in, another synced folder;
/// - another identity's (its marker says whose it is);
/// - one another app syncs (OneDrive, iCloud), unless it is this identity's already.
///
/// It reads the folder's marker only, never lists the folder (macOS asks the user before a
/// folder like Documents is read).
pub fn check(
    config: &Config,
    identity: &Identity,
    path: &Path,
    role: Option<Role>,
    home: Option<&Path>,
) -> Result<bool, String> {
    if let Some(why) = refuse_folder(path, home) {
        return Err(why);
    }
    let main_elsewhere =
        |r: &&Root| r.role.is_none() && (r.folder != path || r.identity != *identity);
    if role.is_none()
        && let Some(main) = config.roots.iter().find(main_elsewhere)
    {
        return Err(format!(
            "already set up to sync {} with {}: unlink it first",
            main.folder.display(),
            main.identity
        ));
    }
    for other in config.roots.iter().filter(|r| r.folder != path) {
        if let Some(role) = role
            && other.identity == *identity
            && other.role == Some(role)
        {
            return Err(format!(
                "{identity} syncs {} with {} already: remove it first",
                role.name(),
                other.folder.display()
            ));
        }
        if within(path, &other.folder) || within(&other.folder, path) {
            return Err(format!(
                "{} and {} can't both be synced: one holds the other",
                path.display(),
                other.folder.display()
            ));
        }
    }
    // Its marker says whose it is (one that can't be read says nothing).
    let ours = match SyncRoot::check(path, identity) {
        Ok(_) => true,
        Err(e @ RootError::OtherIdentity { .. }) => return Err(e.to_string()),
        Err(_) => false,
    };
    if !ours && let Some(why) = other_app(path) {
        return Err(why);
    }
    Ok(ours)
}

/// The home folder of this computer's user, resolved, for [`check`].
pub fn home() -> Option<PathBuf> {
    crate::paths::home().map(|h| crate::setup::resolve(&h))
}

/// Why `folder` (absolute) must not be synced, if it mustn't: the home folder itself or a folder
/// above it, the top of a disk, or a system folder. Syncing those would take in (and on
/// another device, put down) files that aren't the user's to sync.
///
/// It compares the paths as given: pass them through [`crate::setup::resolve`] first, so a
/// symlink or `..` can't hide what a folder really is.
pub fn refuse_folder(folder: &Path, home: Option<&Path>) -> Option<String> {
    if folder.parent().is_none() {
        return Some(format!("{} is the top of a disk", folder.display()));
    }
    if let Some(home) = home
        && within(home, folder)
    {
        return Some(format!(
            "{} holds your whole home folder: choose a folder inside it",
            folder.display()
        ));
    }
    const SYSTEM: &[&str] = &[
        "/bin",
        "/boot",
        "/dev",
        "/etc",
        "/lib",
        "/opt",
        "/proc",
        "/sbin",
        "/sys",
        "/usr",
        "/Applications",
        "/Library",
        "/System",
        "C:\\Windows",
        "C:\\Program Files",
        "C:\\Program Files (x86)",
        "C:\\ProgramData",
    ];
    SYSTEM
        .iter()
        .find(|s| within(folder, Path::new(s)))
        .map(|s| format!("{} is in a system folder ({s})", folder.display()))
}

/// Why `folder` must not be set up for syncing, if another app syncs it (OneDrive, iCloud): two
/// apps syncing one folder fight over every change. A folder synced already stays so.
fn other_app(folder: &Path) -> Option<String> {
    hbsync_platform::user_dirs::owner(folder).map(|owner| {
        format!(
            "{} syncs {} already: two apps syncing one folder fight over every change",
            owner.name(),
            folder.display()
        )
    })
}

/// Whether `path` is `folder` or inside it; ignoring case where the file system usually does
/// (Windows, macOS).
fn within(path: &Path, folder: &Path) -> bool {
    if cfg!(any(windows, target_os = "macos")) {
        let lower = |p: &Path| PathBuf::from(p.to_string_lossy().to_lowercase());
        lower(path).starts_with(lower(folder))
    } else {
        path.starts_with(folder)
    }
}

/// Whether `folder` holds anything besides its `.hbsync` (a missing one holds nothing).
pub(crate) fn has_files(folder: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(folder) {
        Ok(entries) => Ok(entries.flatten().any(|e| e.file_name() != STATE_DIR)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Sets the folder at `path` up for `identity` (kept as it was if it is already: its marker
/// names it), and lists it in the config, unless [`check`] refuses it. `hbsync setup` has
/// checked that this is what the user wants (a folder whose state is gone is merged only when
/// asked). With `role`, it syncs that role folder of the drive (joined if the drive has it,
/// back from Recently deleted, or made when the folder first syncs). The daemon's half: see
/// [`add_synced`]. One at a time, in any process or thread (the tray's pages serve each request
/// on a thread): two set-ups of one folder would each make it anew, the second undoing the
/// first.
pub fn add(
    paths: &Paths,
    identity: &Identity,
    path: &Path,
    role: Option<Role>,
) -> Result<(), Error> {
    add_locked(
        &ConfigLock::take(&paths.config_file())?,
        paths,
        identity,
        path,
        role,
    )
}

/// [`add`], the config's lock held.
fn add_locked(
    held: &ConfigLock,
    paths: &Paths,
    identity: &Identity,
    path: &Path,
    role: Option<Role>,
) -> Result<(), Error> {
    let config = held.load()?.unwrap_or_default();
    check(&config, identity, path, role, home().as_deref())?;
    let top = role.map_or(NodeId::ROOT, Role::node);
    SyncRoot::open_or_create_for(&paths.account_db(identity), identity, path, top)?;
    held.update(|config| {
        if !config.roots.iter().any(|r| r.folder == path) {
            config.roots.push(Root {
                role,
                ..Root::main(identity, path)
            });
        }
    })?;
    Ok(())
}

/// Who took a folder change: the running daemon (it syncs now), or the config (the daemon
/// does when it starts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Taken {
    Now,
    AtStart,
}

/// Has the running daemon set the folder at `path` up and sync it ([`add`]), or sets it up for
/// the daemon's start when none runs ([`changed`]).
pub fn add_synced(
    paths: &Paths,
    identity: &Identity,
    path: &Path,
    role: Option<Role>,
) -> Result<Taken, Error> {
    changed(
        paths,
        |held| add_locked(held, paths, identity, path, role),
        || crate::control::add_folder(paths, identity, path, role),
    )
}

/// Has the running daemon stop syncing the folder at `path` and take it off the config, or
/// takes it off when none runs ([`remove`], [`changed`]).
pub fn remove_synced(paths: &Paths, path: &Path) -> Result<Taken, Error> {
    changed(
        paths,
        |held| remove_locked(held, path).map(drop),
        || crate::control::remove_folder(paths, path),
    )
}

/// A change of the config: asked of the daemon (`ask`) when it runs, else made here
/// (`here`). Whether it runs is looked at holding the config's lock, kept while the config is
/// changed here ([`ConfigLock`]): a daemon starting meanwhile reads the change, and one
/// running has its socket bound. One gone before it was asked is looked at again.
fn changed(
    paths: &Paths,
    here: impl Fn(&ConfigLock) -> Result<(), Error>,
    ask: impl Fn() -> Result<(), Error>,
) -> Result<Taken, Error> {
    let runs = || -> Result<bool, Error> {
        let held = ConfigLock::take(&paths.config_file())?;
        if crate::lock::daemon_runs(paths)? {
            return Ok(true);
        }
        here(&held)?;
        Ok(false)
    };
    let mut tries = 3;
    loop {
        tries -= 1;
        if !runs()? {
            return Ok(Taken::AtStart);
        }
        match ask() {
            Ok(()) => return Ok(Taken::Now),
            // Gone, or about to be (its lock goes as it exits).
            Err(e) if tries > 0 && e.is::<crate::control::NotRunning>() => {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Takes the folder at `path` off the config; its files and its state stay (setting it up again
/// picks up where it was). The folder it was; `None` if it wasn't listed.
pub(crate) fn remove(paths: &Paths, path: &Path) -> Result<Option<Root>, Error> {
    remove_locked(&ConfigLock::take(&paths.config_file())?, path)
}

/// [`remove`], the config's lock held.
fn remove_locked(held: &ConfigLock, path: &Path) -> Result<Option<Root>, Error> {
    Ok(held.update(|config| {
        let at = config.roots.iter().position(|r| r.folder == path)?;
        Some(config.roots.remove(at))
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn some_folders_are_refused() {
        let home = Path::new("/home/anna");
        let refused = |f: &str| refuse_folder(Path::new(f), Some(home));
        assert!(refused("/").is_some());
        assert!(refused("/home").is_some(), "above home");
        assert!(refused("/home/anna").is_some(), "home itself");
        assert!(refused("/usr/share/docs").is_some());
        assert_eq!(refused("/home/anna/Homebase"), None);
        assert_eq!(refused("/media/disk/Homebase"), None);
        if cfg!(any(windows, target_os = "macos")) {
            assert!(refused("/library/Stuff").is_some(), "in any case");
        }
    }

    #[test]
    fn a_folder_is_set_up_once_and_taken_off_again() -> Result<(), Error> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(&folder)?;
        add(&paths, &"a.example".into(), &folder, None)?;
        let marker = hbsync_local::Marker::read(&folder)?;
        add(&paths, &"a.example".into(), &folder, None)?;
        assert_eq!(hbsync_local::Marker::read(&folder)?, marker, "kept");
        let config = Config::load(&paths.config_file())?.ok_or("no config")?;
        assert_eq!(config.roots, [Root::main(&"a.example".into(), &folder)]);
        assert!(
            add(&paths, &"a.example".into(), &folder, Some(Role::Documents)).is_err(),
            "set up as the main folder"
        );
        let other = home.path().join("Other");
        std::fs::create_dir_all(&other)?;
        assert!(
            add(&paths, &"a.example".into(), &other, None).is_err(),
            "one main folder per identity"
        );

        let inside = folder.join("Documents");
        std::fs::create_dir_all(&inside)?;
        assert!(
            add(&paths, &"b.example".into(), &inside, None).is_err(),
            "not inside another synced folder"
        );

        assert_eq!(
            remove(&paths, &folder)?,
            Some(Root::main(&"a.example".into(), &folder))
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
        add(&paths, &"a.example".into(), &main, None)?;
        add(&paths, &"a.example".into(), &docs, Some(Role::Documents))?;
        add(&paths, &"a.example".into(), &docs, Some(Role::Documents))?;
        assert!(
            add(&paths, &"a.example".into(), &more, Some(Role::Documents)).is_err(),
            "one Documents per identity"
        );
        add(&paths, &"b.example".into(), &more, Some(Role::Documents))?;
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
        assert!(add(&paths, &"a.example".into(), &docs, Some(Role::Pictures)).is_err());
        Ok(())
    }

    /// One set of rules for setup and role folders: a main folder can't sit in a synced role
    /// folder, and a role folder that is another identity's is refused before it is set up.
    #[test]
    fn setup_and_role_folders_go_by_the_same_rules() -> Result<(), Error> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let a: Identity = "a.example".into();
        let docs = home.path().join("Documents");
        std::fs::create_dir_all(&docs)?;
        add(&paths, &a, &docs, Some(Role::Documents))?;
        let config = Config::load(&paths.config_file())?.ok_or("no config")?;
        let refused = |path: &Path, identity: &Identity, role| {
            check(&config, identity, path, role, None).expect_err("refused")
        };
        let inside = docs.join("Homebase");
        assert!(refused(&inside, &a, None).contains("one holds the other"));
        assert_eq!(
            check(&config, &a, &home.path().join("Homebase"), None, None),
            Ok(false)
        );
        assert_eq!(
            check(&config, &a, &docs, Some(Role::Documents), None),
            Ok(true)
        );
        // b's folder, by its marker.
        let b: Identity = "b.example".into();
        let pictures = home.path().join("Pictures");
        std::fs::create_dir_all(&pictures)?;
        SyncRoot::create(&paths.account_db(&b), &b, &pictures)?;
        assert!(
            refused(&pictures, &a, Some(Role::Pictures)).contains("b.example"),
            "another identity's"
        );
        Ok(())
    }

    /// A daemon that runs as it is looked at, but stops before it is asked (its request
    /// fails), is looked at again: gone, the change is made here.
    #[test]
    fn a_change_asked_of_a_daemon_that_stopped_is_made_here() -> Result<(), Error> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let daemon = std::cell::RefCell::new(Some(crate::lock::InstanceLock::acquire(&paths)?));
        let (asked, made) = (std::cell::Cell::new(0), std::cell::Cell::new(0));
        let taken = changed(
            &paths,
            |_| {
                made.set(made.get() + 1);
                Ok(())
            },
            || {
                asked.set(asked.get() + 1);
                // It stops: its lock goes, its socket is closed.
                daemon.borrow_mut().take();
                Err(crate::control::NotRunning.into())
            },
        )?;
        assert_eq!((taken, asked.get(), made.get()), (Taken::AtStart, 1, 1));
        // While it runs and answers, it is asked, and nothing is made here; its refusal is
        // passed on, asked once.
        let _runs = crate::lock::InstanceLock::acquire(&paths)?;
        let taken = changed(&paths, |_| Err("made here".into()), || Ok(()))?;
        assert_eq!(taken, Taken::Now);
        asked.set(0);
        let refused = changed(
            &paths,
            |_| Err("made here".into()),
            || {
                asked.set(asked.get() + 1);
                Err("refused".into())
            },
        );
        assert_eq!(refused.map_err(|e| e.to_string()), Err("refused".into()));
        assert_eq!(asked.get(), 1);
        Ok(())
    }
}
