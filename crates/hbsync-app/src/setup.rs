//! Setting the app up on an identity: authorizing this device (the owner approves in the
//! browser), then a first look at the sync drive and, best effort, the app's circle (found by its
//! slug on every setup; nothing uses it yet).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, Credentials, HomebaseError, app};
use hbsync_remote::Remote;

use hbsync_state::Unusable;
use hbsync_types::Role;

use crate::config::ConfigLock;
use crate::paths::home;
use crate::{Config, CredentialStore, Identity, Location, Paths, autostart};
use crate::{control, folders};

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
pub struct Prepared<R = HomebaseRemote> {
    pub remote: R,
    /// The app's circle ([`app::app_circle`]), unless finding it failed.
    pub circle: Option<uuid::Uuid>,
    /// Changes on the first page of the drive's feed: it answers.
    pub feed_entries: usize,
}

/// Gets the identity ready for syncing once this device is authorized: finds the sync drive,
/// reads its change feed, and finds the app's circle by its slug (installing the app created it;
/// made now if not).
pub fn prepare(creds: &Credentials) -> Result<Prepared, HomebaseError> {
    let remote = HomebaseRemote::connect(Client::new(creds)?)?;
    prepared(
        remote,
        |remote| Ok(remote.change_refs(None, 10)?.refs.len()),
        app::app_circle,
    )
}

/// [`prepare`] on a connected `remote`: its feed must answer (what proves this device's
/// authorization, with the drive found), its circle is best effort. Nothing uses the circle yet
/// (it is for sharing), so a failure to find it (refused, say) is logged and fails nothing: it
/// is never taken for a revoked authorization.
fn prepared<R>(
    remote: R,
    feed: impl FnOnce(&R) -> Result<usize, hbsync_remote::RemoteError>,
    circle: impl FnOnce(&R) -> Result<uuid::Uuid, HomebaseError>,
) -> Result<Prepared<R>, HomebaseError> {
    let feed_entries = feed(&remote).map_err(|e| match e {
        hbsync_remote::RemoteError::Denied(why) => HomebaseError::Denied(why),
        e => HomebaseError::Protocol(format!("the sync drive's feed: {e}")),
    })?;
    let circle = circle(&remote)
        .inspect_err(|e| tracing::warn!("the app's circle: {e}"))
        .ok();
    Ok(Prepared {
        remote,
        circle,
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

/// The folder synced when none is chosen: `Homebase` in the home folder.
pub fn default_folder() -> crate::Result<PathBuf> {
    home()
        .map(|home| home.join("Homebase"))
        .ok_or_else(|| "no home folder (HOME unset): choose a folder".into())
}

/// Where this computer keeps `role`'s folder (resolved): what `hbsync add <role>` syncs.
pub fn role_folder(role: Role) -> crate::Result<PathBuf> {
    hbsync_platform::user_dirs::user_dir(role.user_dir())
        .map(|p| resolve(&p))
        .ok_or_else(|| format!("this computer has no {} folder", role.name()).into())
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
/// like a domain), and the folder, resolved (symlinks, `..`) so it is judged as it really is,
/// by the rules every folder goes by ([`crate::folders::check`]); with files in it only when
/// `merge`.
pub fn plan(
    config: &Config,
    typed: &str,
    folder: &Path,
    merge: bool,
    home: Option<&Path>,
) -> Result<SetUp, Refusal> {
    let identity = Identity::new(typed);
    if !looks_like_a_domain(&identity) {
        return Err(Refusal::No(format!(
            "{identity:?} doesn't look like a Homebase address (like anna.example.com)"
        )));
    }
    let folder = resolve(folder);
    let home = home.map(resolve);
    let ours =
        folders::check(config, &identity, &folder, None, home.as_deref()).map_err(Refusal::No)?;
    // Synced with this identity before, it picks up where it was; with files in it, they
    // are uploaded and merged with the drive.
    let has_files = !ours
        && folders::has_files(&folder)
            .map_err(|e| Refusal::No(format!("{}: {e}", folder.display())))?;
    match has_files {
        true if !merge => Err(Refusal::NeedsMerge(folder)),
        _ => Ok(SetUp {
            identity,
            folder,
            existing: merge,
            start: true,
            at_logon: true,
        }),
    }
}

/// Checks the folder at `folder` (resolved) against `identity`'s state DB before it is set up
/// there, changing nothing but the DB: when no daemon runs, it is brought up to date, or set
/// aside and made again if it can't be used ([`folders::ready_account`]: from before 0.7,
/// damaged), its folders set up again as folders with files; meanwhile the config's lock is
/// held, so a daemon starting waits. Refused: a folder synced here before whose marker is gone
/// (an empty mount point, most likely), and one with files whose state is gone, unless
/// `existing` (merge them) or the DB was set aside here (its folders are merged). Whether the
/// daemon runs, and why the DB was set aside here, if it was.
pub fn check_state(
    paths: &Paths,
    config: &Config,
    identity: &Identity,
    folder: &Path,
    existing: bool,
) -> crate::Result<(bool, Option<Unusable>)> {
    let db = paths.account_db(identity);
    let (running, set_aside) = {
        let _held = ConfigLock::take(&paths.config_file())?;
        let running = control::running(paths)?;
        let set_aside = match !running && db.exists() {
            // Nothing else writes it: brought up to date here (or set aside and made again),
            // for the checks (which only look).
            true => folders::ready_account(&db, identity, &config.roots)?.map(|set| set.why),
            false => None,
        };
        (running, set_aside)
    };
    match SyncRoot::find(&db, identity, folder) {
        Ok(_) => {}
        // The running daemon sets it aside as it adds the folder, where its account doesn't
        // run there (it would have as it started), and tells.
        Err(RootError::State(e)) if running && e.unusable().is_some() => {}
        // Synced here before, its marker gone: an empty mount point, most likely. Setting it
        // up would start it over; only when asked to merge.
        Err(RootError::NoMarker(_))
            if !existing && SyncRoot::knows_path(&db, identity, folder)? =>
        {
            return Err(Refusal::No(format!(
                "{} was synced here, but its .hbsync is gone: if it is on a disk, plug it in; \
                 to start it over, set it up again merging its files",
                folder.display()
            ))
            .into());
        }
        // Its state is gone (or it was copied here): its files are merged, when asked to, or
        // as the DB was set aside.
        Err(RootError::Unknown(_))
            if !existing && set_aside.is_none() && folders::has_files(folder)? =>
        {
            return Err(Refusal::NeedsMerge(folder.to_path_buf()).into());
        }
        Err(RootError::NoMarker(_) | RootError::Unknown(_)) => {}
        Err(e) => return Err(e.into()),
    }
    Ok((running, set_aside))
}

/// What to set up, as [`plan`] makes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetUp {
    pub identity: Identity,
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
) -> crate::Result<()> {
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
    let identity = &checked.identity;
    std::fs::create_dir_all(&checked.folder)?;
    let folder = canonical(&checked.folder)?;

    // This computer, authorized on the identity. New credentials are kept at once: an
    // approval lost to a later failure would have to be given again.
    let store = CredentialStore::open(paths);
    let device = config.device_name();
    // New credentials reach a running daemon's account only when it starts over.
    let mut authorized = false;
    let mut authorize = || -> crate::Result<Credentials> {
        let creds = authorize(identity, &device)?;
        store.save(&creds)?;
        authorized = true;
        Ok(creds)
    };
    let mut creds = match store.load(identity)? {
        Some((creds, _)) => {
            say(&format!(
                "This computer is already authorized on {identity}."
            ));
            creds
        }
        None => authorize()?,
    };
    match prepare(&creds) {
        // Revoked since: authorize again.
        Err(HomebaseError::Denied(why)) => {
            say(&format!(
                "{identity} no longer accepts this computer's authorization ({why})."
            ));
            store.delete(identity)?;
            creds = authorize()?;
            prepare(&creds)?;
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
    let (running, set_aside) = check_state(paths, &config, identity, &folder, checked.existing)?;
    if running && authorized {
        control::start_over(paths, identity)?;
    }
    // A running daemon sets the folder up and starts syncing it, without a restart.
    let taken = crate::folders::add_synced(paths, identity, &folder, None)?;
    if let Some(why) = set_aside {
        say(&crate::words::set_aside(why));
    }
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
    if taken == crate::folders::Taken::AtStart {
        control::start(paths)?;
    }
    say("The daemon is running: `hbsync status` shows what it does.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refused circle fails nothing: setup goes on without it (and so keeps the credentials).
    /// Only a refused drive or feed is a revoked authorization.
    #[test]
    fn a_refused_circle_fails_nothing() {
        let refused = |_: &()| Err(HomebaseError::Denied("403 on circles/create".into()));
        let prepared_ = prepared((), |_| Ok(3), refused);
        assert!(
            matches!(
                prepared_,
                Ok(Prepared {
                    circle: None,
                    feed_entries: 3,
                    ..
                })
            ),
            "{prepared_:?}"
        );
        let revoked = prepared(
            (),
            |_| Err(hbsync_remote::RemoteError::Denied("401".into())),
            |_| Ok(uuid::Uuid::nil()),
        );
        assert!(
            matches!(revoked, Err(HomebaseError::Denied(_))),
            "{revoked:?}"
        );
    }

    type R = crate::Result<()>;

    #[test]
    fn folders_are_resolved_before_they_are_judged() -> R {
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
            assert!(folders::refuse_folder(&resolve(&link), Some(&home)).is_some());
            assert_eq!(resolve(&link.join("Homebase")), home.join("Homebase"));
        }
        Ok(())
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
            roots: vec![crate::Root::main(&"a.example".into(), &fresh)],
            ..Config::default()
        };
        assert!(plan("a.example", &fresh, false, &set).is_ok());
        refused(plan("b.example", &fresh, false, &set), "unlink it first");
        refused(plan("a.example", &full, false, &set), "unlink it first");
        Ok(())
    }
}
