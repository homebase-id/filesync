//! Setting the app up on an identity: authorizing this device (the owner approves in the
//! browser), then the one-time work on the identity (the app's circle) and a first look at the
//! sync drive.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, Credentials, HomebaseError, app};
use hbsync_local::STATE_DIR;
use hbsync_remote::Remote;

use hbsync_types::Role;

use crate::control;
use crate::paths::home;
use crate::{Config, CredentialStore, Location, Paths, autostart};

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
    let login = begin(identity, device)?;
    show(login.authorize_url());
    // The URL was shown: a browser that doesn't start isn't an error.
    let _ = open_browser(browser, login.authorize_url());
    login.finish(APPROVAL_TIMEOUT)
}

/// Starts authorizing this device (named after `device` in the owner's list) on `identity`:
/// the caller takes the browser to `authorize_url` and waits with `finish`
/// ([`APPROVAL_TIMEOUT`]).
pub fn begin(identity: &str, device: &str) -> Result<crate::login::Login, HomebaseError> {
    crate::login::begin(identity, &format!("Filesync on {device}"))
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
pub fn open_browser(
    command: Option<&str>,
    url: impl AsRef<std::ffi::OsStr>,
) -> std::io::Result<()> {
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

/// Where this computer keeps `role`'s folder (resolved): what `hbsync add <role>` syncs.
pub fn role_folder(role: Role) -> control::Result<PathBuf> {
    hbsync_platform::user_dirs::user_dir(role.user_dir())
        .map(|p| resolve(&p))
        .ok_or_else(|| format!("this computer has no {} folder", role.name()).into())
}

/// Why `folder` (absolute) must not be synced, if it mustn't: the home folder itself or a folder
/// above it, the top of a disk, or a system folder. Syncing those would take in (and on
/// another device, put down) files that aren't the user's to sync.
///
/// It compares the paths as given: pass them through [`resolve`] first, so a symlink or `..`
/// can't hide what a folder really is.
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
pub fn other_app(folder: &Path) -> Option<String> {
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

/// `canonicalize`, without the `\\?\` it puts on Windows paths: kept in the config and shown
/// to people, they must be paths the shell and file managers take.
fn canonical(path: &Path) -> std::io::Result<PathBuf> {
    Ok(hbsync_platform::winpath::plain(&path.canonicalize()?))
}

/// `path` as it really is: absolute, its longest existing part with symlinks resolved, and
/// `..` in the rest taken away. What [`plan`] looks at.
pub fn resolve(path: &Path) -> PathBuf {
    use std::path::Component;
    let path = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let parts: Vec<Component> = path.components().collect();
    for exists in (1..=parts.len()).rev() {
        let Ok(mut out) = canonical(&parts[..exists].iter().collect::<PathBuf>()) else {
            continue;
        };
        for part in &parts[exists..] {
            match part {
                Component::ParentDir => {
                    out.pop();
                }
                Component::CurDir => {}
                other => out.push(other),
            }
        }
        return out;
    }
    path
}

/// What a folder holds, as far as setting it up goes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FolderState {
    /// Missing or empty.
    Fresh,
    /// Synced with this identity before: setting it up picks up where it was.
    Ours,
    /// Has files: they would be uploaded and merged with the drive.
    HasFiles,
}

/// What `folder` (resolved) holds for `identity`: refused if another folder or identity is
/// set up, or the folder is another identity's.
fn folder_state(config: &Config, identity: &str, folder: &Path) -> Result<FolderState, String> {
    if let Some(root) = config.root().map_err(|e| e.to_string())?
        && (root.identity != identity || root.folder != folder)
    {
        return Err(format!(
            "already set up to sync {} with {}: unlink it first",
            root.folder.display(),
            root.identity
        ));
    }
    // Its marker says whose it is. Without one (an older Filesync's folder, or one half set
    // up) it is set up again, its files merged: identical ones are adopted, not copied.
    match SyncRoot::check(folder, identity) {
        Ok(_) => return Ok(FolderState::Ours),
        Err(e @ RootError::OtherIdentity { .. }) => return Err(e.to_string()),
        Err(_) => {}
    }
    match has_files(folder) {
        Ok(true) => Ok(FolderState::HasFiles),
        Ok(false) => Ok(FolderState::Fresh),
        Err(e) => Err(format!("{}: {e}", folder.display())),
    }
}

/// Whether `folder` holds anything besides its `.hbsync` (a missing one holds nothing).
fn has_files(folder: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(folder) {
        Ok(entries) => Ok(entries.flatten().any(|e| e.file_name() != STATE_DIR)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// `anna.example.com`, `frodo.dotyou.cloud:8443`: letters, digits, dots, hyphens, a port.
fn looks_like_a_domain(s: &str) -> bool {
    let host = s.split(':').next().unwrap_or_default();
    host.contains('.')
        && !host.starts_with('.')
        && !host.ends_with('.')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'))
}

/// Why setting up can't go ahead as asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The folder has files in it: they would be uploaded and merged with the drive. Only
    /// when asked for (`merge`).
    NeedsMerge(PathBuf),
    No(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::NeedsMerge(folder) => write!(
                f,
                "{} has files in it: they would be uploaded and merged with your Homebase",
                folder.display()
            ),
            Refusal::No(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for Refusal {}

/// Checks what to set up, changing nothing: the address as `typed` (normalised; it must look
/// like a domain), and the folder, resolved (symlinks, `..`) so it is judged as it really is:
/// never the home folder or one above it, a disk's top or a system folder; not another
/// identity's, nor a second folder; with files in it only when `merge`.
pub fn plan(
    config: &Config,
    typed: &str,
    folder: &Path,
    merge: bool,
    home: Option<&Path>,
) -> Result<SetUp, Refusal> {
    let identity = normalize_identity(typed);
    if !looks_like_a_domain(&identity) {
        return Err(Refusal::No(format!(
            "{identity:?} doesn't look like a Homebase address (like anna.example.com)"
        )));
    }
    let folder = resolve(folder);
    if let Some(why) = refuse_folder(&folder, home.map(resolve).as_deref()) {
        return Err(Refusal::No(why));
    }
    let state = folder_state(config, &identity, &folder).map_err(Refusal::No)?;
    if state != FolderState::Ours
        && let Some(why) = other_app(&folder)
    {
        return Err(Refusal::No(why));
    }
    match state {
        FolderState::HasFiles if !merge => Err(Refusal::NeedsMerge(folder)),
        _ => Ok(SetUp {
            identity,
            folder,
            existing: merge,
            start: true,
            at_logon: true,
        }),
    }
}

/// What to set up, as [`plan`] makes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetUp {
    /// As [`normalize_identity`] leaves it.
    pub identity: String,
    /// Resolved ([`resolve`]); created if missing.
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
    let config_path = paths.config_file();
    let config = Config::load(&config_path)?.unwrap_or_default();
    // Checked again: the config may have changed since the request was planned.
    let checked = plan(
        &config,
        &request.identity,
        &request.folder,
        request.existing,
        home().as_deref(),
    )?;
    let identity = checked.identity.as_str();
    std::fs::create_dir_all(&checked.folder)?;
    let folder = canonical(&checked.folder)?;

    // This computer, authorized on the identity. New credentials are kept at once: an
    // approval lost to a later failure would have to be given again.
    let store = CredentialStore::open(paths);
    let device = config.device_name();
    // New credentials reach a running daemon's account only when it starts over.
    let mut authorized = false;
    let mut authorize = || -> control::Result<Credentials> {
        let creds = authorize(identity, &device)?;
        store.save(&creds)?;
        authorized = true;
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

    // The folder in the account's state DB, and its marker: kept if set up before. Checked
    // here, set up by the daemon (or here, when it isn't running).
    let db = paths.account_db(identity);
    let has_files = has_files(&folder)?;
    match SyncRoot::find(&db, identity, &folder) {
        Ok(_) => {}
        // Synced here before, its marker gone: an empty mount point, most likely. Setting it
        // up would start it over; only when asked to merge.
        Err(RootError::NoMarker(_))
            if !checked.existing && SyncRoot::knows_path(&db, identity, &folder)? =>
        {
            return Err(Refusal::No(format!(
                "{} was synced here, but its .hbsync is gone: if it is on a disk, plug it in; \
                 to start it over, set it up again merging its files",
                folder.display()
            ))
            .into());
        }
        // Its state is gone (or it was copied here): its files are merged, when asked to.
        Err(RootError::Unknown(_) | RootError::Older(_)) if has_files && !checked.existing => {
            return Err(Refusal::NeedsMerge(folder).into());
        }
        Err(RootError::NoMarker(_) | RootError::Unknown(_) | RootError::Older(_)) => {}
        Err(e) => return Err(e.into()),
    }
    let running = control::running(paths)?;
    if running && authorized {
        control::start_over(paths, identity)?;
    }
    // A running daemon sets the folder up and starts syncing it, without a restart.
    crate::folders::add_synced(paths, identity, &folder, None)?;
    say(&format!("{} syncs with {identity}.", folder.display()));

    if request.at_logon {
        if control::dev_home() {
            say("HBSYNC_HOME is set: not starting at logon (`hbsync autostart on` does).");
        } else {
            // Set up all the same: syncing doesn't depend on it, and it can be turned on later.
            match autostart::logon_program().and_then(|p| autostart::install(&p)) {
                Ok(()) => say("Filesync starts at logon (`hbsync autostart off` stops that)."),
                Err(e) => say(&format!("Not starting at logon: {e}.")),
            }
        }
    }
    if !request.start {
        return Ok(());
    }
    if !running {
        control::start(paths)?;
    }
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
        if cfg!(any(windows, target_os = "macos")) {
            assert!(refused("/library/Stuff").is_some(), "in any case");
        }
    }

    #[test]
    fn folders_are_resolved_before_they_are_judged() -> control::Result<()> {
        let dir = tempfile::tempdir()?;
        let home = canonical(dir.path())?.join("anna");
        std::fs::create_dir_all(home.join("Docs"))?;
        // `..` and a symlink can't make a folder look like something else.
        assert_eq!(
            resolve(&home.join("Docs/../..")),
            home.parent().unwrap_or(&home)
        );
        assert_eq!(resolve(&home.join("New/Sub/..")), home.join("New"));
        #[cfg(unix)]
        {
            let link = home.join("Docs/everything");
            std::os::unix::fs::symlink(&home, &link)?;
            assert_eq!(resolve(&link), home);
            assert!(refuse_folder(&resolve(&link), Some(&home)).is_some());
            assert_eq!(resolve(&link.join("Homebase")), home.join("Homebase"));
        }
        Ok(())
    }

    #[test]
    fn identities_are_kept_lowercase_without_a_slash() {
        assert_eq!(
            normalize_identity(" Anna.Example.com/ "),
            "anna.example.com"
        );
    }

    #[test]
    fn plans_are_checked() -> R {
        let dir = tempfile::tempdir()?;
        let home = canonical(dir.path())?.join("anna");
        let fresh = home.join("Homebase");
        let full = home.join("Mine");
        std::fs::create_dir_all(&full)?;
        std::fs::write(full.join("notes.txt"), "x")?;
        let none = Config::default();
        let plan = |typed, folder: &Path, merge, config: &Config| {
            plan(config, typed, folder, merge, Some(&home))
        };
        let refused = |r: Result<SetUp, Refusal>, why: &str| match r {
            Err(Refusal::No(said)) => assert!(said.contains(why), "{said}"),
            other => panic!("{other:?}"),
        };

        // The address normalised, the folder resolved.
        let ok = plan(
            " Anna.Example.com/ ",
            &home.join("x/../Homebase"),
            false,
            &none,
        )?;
        assert_eq!(
            (ok.identity.as_str(), &ok.folder),
            ("anna.example.com", &fresh)
        );
        refused(plan("anna", &fresh, false, &none), "doesn't look like");
        refused(plan("a b.com", &fresh, false, &none), "doesn't look like");
        assert!(plan("frodo.dotyou.cloud:8443", &fresh, false, &none).is_ok());
        // Never the home folder (also through a symlink), a disk's top, a system folder.
        refused(plan("a.example", &home, false, &none), "whole home folder");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&home, home.join("everything"))?;
            refused(
                plan("a.example", &home.join("everything"), true, &none),
                "whole home folder",
            );
            refused(
                plan("a.example", Path::new("/"), false, &none),
                "top of a disk",
            );
            refused(
                plan("a.example", Path::new("/usr/x"), false, &none),
                "system",
            );
        }
        // Files in it: merged only when asked.
        assert_eq!(
            plan("a.example", &full, false, &none),
            Err(Refusal::NeedsMerge(full.clone()))
        );
        assert!(plan("a.example", &full, true, &none)?.existing);
        // Synced before: with this identity it picks up where it was, with another it's refused.
        hbsync_local::Marker::new("a.example").write(&full)?;
        assert!(plan("a.example", &full, false, &none).is_ok());
        refused(
            plan("b.example", &full, false, &none),
            "is synced with a.example",
        );
        // Another folder or identity already set up.
        let set = Config {
            roots: vec![crate::Root::main("a.example", &fresh)],
            ..Config::default()
        };
        assert!(plan("a.example", &fresh, false, &set).is_ok());
        refused(plan("b.example", &fresh, false, &set), "unlink it first");
        refused(plan("a.example", &full, false, &set), "unlink it first");
        Ok(())
    }
}
