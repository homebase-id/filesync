//! Setting the app up on an identity: authorizing this device (the owner approves in the
//! browser), then the one-time work on the identity (the app's circle) and a first look at the
//! sync drive.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, Credentials, HomebaseError, app};
use hbsync_local::{STATE_DIR, state_db_path};
use hbsync_remote::Remote;
use hbsync_state::StateDb;

use crate::control;
use crate::runner::IDENTITY_KEY;
use crate::{Config, CredentialStore, Location, Paths, Root, autostart};

/// How long the owner has to approve in the browser.
pub const APPROVAL_TIMEOUT: Duration = Duration::from_secs(1800);

/// Authorizes this device for Filesync on `identity`: opens the authorize page in `browser`
/// (the system's default when `None`), passes its URL to `show` for when no browser opens,
/// and waits for the owner to approve. A new app is registered on the way.
pub fn authorize(
    identity: &str,
    device: &str,
    browser: Option<&str>,
    show: impl FnOnce(&str),
) -> Result<Credentials, HomebaseError> {
    let pending = begin(identity, device)?;
    show(&pending.authorize_url);
    // The URL was shown: a browser that doesn't start isn't an error.
    let _ = open_browser(browser, &pending.authorize_url);
    pending.finish(APPROVAL_TIMEOUT)
}

/// Starts authorizing this device (named after `device` in the owner's list) on `identity`:
/// the caller takes the browser to `authorize_url` and waits with `finish`
/// ([`APPROVAL_TIMEOUT`]).
pub fn begin(identity: &str, device: &str) -> Result<hbsync_homebase::Pending, HomebaseError> {
    hbsync_homebase::begin(identity, &format!("Filesync on {device}"))
}

/// What [`prepare`] found and did.
#[derive(Debug)]
pub struct Prepared {
    pub remote: HomebaseRemote,
    /// The app's circle, created now (`creds` changed, and were saved).
    pub circle_created: Option<uuid::Uuid>,
    /// Changes on the first page of the drive's feed: it answers.
    pub feed_entries: usize,
}

/// Gets the identity ready for syncing once this device is authorized: finds the sync drive,
/// creates the app's circle once, and reads the drive's change feed. `save` keeps `creds` as
/// soon as they change (the circle created), before anything else can fail: otherwise the
/// next try would create a second circle.
pub fn prepare<E: std::fmt::Display>(
    creds: &mut Credentials,
    save: impl FnOnce(&Credentials) -> Result<(), E>,
) -> Result<Prepared, HomebaseError> {
    let remote = HomebaseRemote::connect(Client::new(creds)?)?;
    let circle_created = app::ensure_circle(&remote, creds)?;
    if circle_created.is_some() {
        save(creds).map_err(|e| HomebaseError::Protocol(format!("saving the credentials: {e}")))?;
    }
    let feed_entries = remote
        .changes(None, 10)
        .map_err(|e| HomebaseError::Protocol(format!("the sync drive's feed: {e}")))?
        .entries
        .len();
    Ok(Prepared {
        remote,
        circle_created,
        feed_entries,
    })
}

/// Opens `url` with `command`, or with the system's opener.
pub fn open_browser(command: Option<&str>, url: &str) -> std::io::Result<()> {
    let opener = command.unwrap_or(if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(windows) {
        "explorer"
    } else {
        "xdg-open"
    });
    Command::new(opener).arg(url).spawn().map(drop)
}

/// An identity as typed (`Anna.Example.com/`) as the client keeps it (`anna.example.com`).
pub fn normalize_identity(typed: &str) -> String {
    typed.trim().trim_end_matches('/').to_lowercase()
}

/// The folder synced when none is chosen: `Homebase` in the home folder.
pub fn default_folder() -> control::Result<PathBuf> {
    home()
        .map(|home| home.join("Homebase"))
        .ok_or_else(|| "no home folder (HOME unset): choose a folder".into())
}

/// Why `folder` (absolute) must not be synced, if it mustn't: the home folder itself or a folder
/// above it, the top of a disk, or a system folder. Syncing those would take in (and on
/// another device, put down) files that aren't the user's to sync.
pub fn refuse_folder(folder: &Path, home: Option<&Path>) -> Option<String> {
    if folder.parent().is_none() {
        return Some(format!("{} is the top of a disk", folder.display()));
    }
    if let Some(home) = home
        && home.starts_with(folder)
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
        .find(|s| folder.starts_with(s))
        .map(|s| format!("{} is in a system folder ({s})", folder.display()))
}

/// The user's home folder.
pub fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// What a folder holds, as far as setting it up goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FolderState {
    /// Missing or empty.
    Fresh,
    /// Synced with this identity before: setting it up picks up where it was.
    Ours,
    /// Has files: they would be uploaded and merged with the drive.
    HasFiles,
}

/// Whether `folder` can be synced with `identity`: refused if another folder or identity is
/// set up, or the folder is another identity's.
pub fn check_folder(
    config: &Config,
    identity: &str,
    folder: &Path,
) -> control::Result<FolderState> {
    if let Some(root) = config.root()?
        && (root.identity != identity || root.folder != folder)
    {
        return Err(format!(
            "already set up to sync {} with {}: unlink it first",
            root.folder.display(),
            root.identity
        )
        .into());
    }
    if folder.join(STATE_DIR).is_dir() {
        let theirs = StateDb::open(&state_db_path(folder))?.read(|s| s.get_kv(IDENTITY_KEY))?;
        return match theirs.filter(|t| t != identity) {
            Some(theirs) => Err(format!("{} is synced with {theirs}", folder.display()).into()),
            None => Ok(FolderState::Ours),
        };
    }
    match std::fs::read_dir(folder) {
        Ok(mut entries) => Ok(match entries.next() {
            Some(_) => FolderState::HasFiles,
            None => FolderState::Fresh,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(FolderState::Fresh),
        Err(e) => Err(e.into()),
    }
}

/// What to set up.
#[derive(Debug, Clone)]
pub struct SetUp {
    /// As [`normalize_identity`] leaves it.
    pub identity: String,
    /// Absolute; created if missing.
    pub folder: PathBuf,
    /// Merge the files of a folder that has some with the drive.
    pub existing: bool,
    /// Start (or restart) the daemon.
    pub start: bool,
    /// Start the daemon at logon (not in a private instance).
    pub at_logon: bool,
}

/// Sets syncing up: authorizes this computer on the identity unless it already is and the
/// identity still accepts it (`authorize` gets the identity and the device's name, and waits
/// for the owner's approval), prepares the identity, the folder and the config, and starts the
/// daemon. Progress goes to `say`, a line at a time.
pub fn set_up(
    paths: &Paths,
    request: &SetUp,
    authorize: &mut dyn FnMut(&str, &str) -> Result<Credentials, HomebaseError>,
    say: &mut dyn FnMut(&str),
) -> control::Result<()> {
    let identity = request.identity.as_str();
    let config_path = paths.config_file();
    let mut config = Config::load(&config_path)?.unwrap_or_default();

    // The folder: new, empty, already this identity's, or adopted on purpose.
    if let Some(why) = refuse_folder(&request.folder, home().as_deref()) {
        return Err(why.into());
    }
    std::fs::create_dir_all(&request.folder)?;
    let folder = request.folder.canonicalize()?;
    if check_folder(&config, identity, &folder)? == FolderState::HasFiles && !request.existing {
        return Err(format!(
            "{} has files in it: they would be uploaded and merged with the drive. Add \
             --existing to do that, or pick an empty or new folder with --folder.",
            folder.display()
        )
        .into());
    }

    // This computer, authorized on the identity. New credentials are kept at once: an
    // approval lost to a later failure would have to be given again.
    let store = CredentialStore::open(paths);
    let device = config.device_name();
    let mut authorize = || -> control::Result<Credentials> {
        let creds = authorize(identity, &device)?;
        store.save(&creds)?;
        Ok(creds)
    };
    let save = |c: &Credentials| store.save(c).map(drop);
    let mut creds = match store.load(identity)? {
        Some((creds, _)) => {
            say(&format!(
                "This computer is already authorized on {identity}."
            ));
            creds
        }
        None => authorize()?,
    };
    match prepare(&mut creds, save) {
        // Revoked since: authorize again.
        Err(HomebaseError::Denied(why)) => {
            say(&format!(
                "{identity} no longer accepts this computer's authorization ({why})."
            ));
            store.delete(identity)?;
            creds = authorize()?;
            prepare(&mut creds, save)?;
        }
        other => drop(other?),
    }
    match store.save(&creds)? {
        Location::Keychain => say("Credentials saved in the system keychain."),
        Location::File { path, why } => say(&format!(
            "No keychain ({why}): credentials saved in {}, readable only by you.",
            path.display()
        )),
    }

    std::fs::create_dir_all(folder.join(STATE_DIR))?;
    StateDb::open(&state_db_path(&folder))?.write(|s| s.set_kv(IDENTITY_KEY, identity))?;
    config.roots = vec![Root {
        identity: identity.to_owned(),
        folder: folder.clone(),
    }];
    config.save(&config_path)?;
    say(&format!("{} syncs with {identity}.", folder.display()));

    if request.at_logon {
        if control::dev_home() {
            say("HBSYNC_HOME is set: not starting at logon (`hbsync autostart on` does).");
        } else {
            autostart::install(&autostart::logon_program()?)?;
            say("Filesync starts at logon (`hbsync autostart off` stops that).");
        }
    }
    if !request.start {
        return Ok(());
    }
    if control::running(paths)? {
        control::stop(paths)?;
    }
    control::start(paths)?;
    say("The daemon is running: `hbsync status` shows what it does.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type R = control::Result<()>;

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
    }

    #[test]
    fn identities_are_kept_lowercase_without_a_slash() {
        assert_eq!(
            normalize_identity(" Anna.Example.com/ "),
            "anna.example.com"
        );
    }

    #[test]
    fn folders_are_checked_before_setting_up() -> R {
        let dir = tempfile::tempdir()?;
        let folder = dir.path().join("Homebase");
        let config = Config::default();
        assert_eq!(
            check_folder(&config, "a.example", &folder)?,
            FolderState::Fresh
        );
        std::fs::create_dir(&folder)?;
        assert_eq!(
            check_folder(&config, "a.example", &folder)?,
            FolderState::Fresh
        );
        std::fs::write(folder.join("notes.txt"), "x")?;
        assert_eq!(
            check_folder(&config, "a.example", &folder)?,
            FolderState::HasFiles
        );

        // Synced before: with this identity it picks up where it was, with another it's refused.
        std::fs::create_dir(folder.join(STATE_DIR))?;
        StateDb::open(&state_db_path(&folder))?.write(|s| s.set_kv(IDENTITY_KEY, "a.example"))?;
        assert_eq!(
            check_folder(&config, "a.example", &folder)?,
            FolderState::Ours
        );
        assert!(check_folder(&config, "b.example", &folder).is_err());

        // Another folder already set up.
        let config = Config {
            roots: vec![Root {
                identity: "a.example".into(),
                folder: dir.path().join("Other"),
            }],
            ..Config::default()
        };
        assert!(check_folder(&config, "a.example", &folder).is_err());
        Ok(())
    }
}
