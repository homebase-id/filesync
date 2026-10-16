//! Controlling the daemon from another process (the CLI, the tray): asking it, starting it
//! beside the calling program, stopping it, and leaving an identity or folder.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::ipc::{self, IpcError};
use crate::protocol::{
    Ask, Control, DaemonControl, DaemonStatus, DeletedItem, RecentItem, Reply, Request,
};
use crate::{Config, CredentialStore, Paths, Root, autostart};

/// An error worded for people: the CLI prints it, the tray shows it.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub fn ask(paths: &Paths, what: Ask) -> std::result::Result<Reply, IpcError> {
    ipc::call(paths, &Request::new(what))
}

/// A daemon of an older build is running (the programs were updated while it ran): it speaks
/// an older protocol; [`stop`] stops it all the same.
#[derive(Debug, thiserror::Error)]
#[error("the daemon running is of an older version: restart it")]
pub struct OlderDaemon;

/// The protocol of the daemon that refused a request as `why`, when it isn't ours.
fn older_protocol(why: &str) -> Option<u32> {
    let version = why
        .strip_prefix(crate::protocol::OTHER_PROTOCOL)?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    (version != crate::protocol::PROTOCOL).then_some(version)
}

/// The daemon's status; `None` when it isn't running.
pub fn status(paths: &Paths) -> Result<Option<DaemonStatus>> {
    match ask(paths, Ask::Status) {
        Ok(Reply::Status(s)) => Ok(Some(*s)),
        Ok(Reply::Refused(why)) if older_protocol(&why).is_some() => Err(OlderDaemon.into()),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The newest `limit` entries of the recently synced list, newest first; `None` when the
/// daemon isn't running.
pub fn recent(paths: &Paths, limit: usize) -> Result<Option<Vec<RecentItem>>> {
    match ask(paths, Ask::Recent { limit }) {
        Ok(Reply::Recent(list)) => Ok(Some(list)),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Recently deleted's newest `limit` deletes, newest first, each with how many items it took
/// along; `None` when the daemon isn't running.
pub fn deleted(paths: &Paths, limit: usize) -> Result<Option<Vec<DeletedItem>>> {
    listed(ask(paths, Ask::Deleted { limit }))
}

/// The items of Recently deleted that `items` name (a node id, or the place `hbsync deleted`
/// shows), any it holds, deleted on their own or along with a folder; `None` when the daemon
/// isn't running.
pub fn find_deleted(paths: &Paths, items: Vec<String>) -> Result<Option<Vec<DeletedItem>>> {
    listed(ask(paths, Ask::FindDeleted { items }))
}

fn listed(answer: std::result::Result<Reply, IpcError>) -> Result<Option<Vec<DeletedItem>>> {
    match answer {
        Ok(Reply::Deleted(list)) => Ok(Some(list)),
        Ok(Reply::Refused(why)) => Err(why.into()),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Sends `c`; the daemon's refusal is an error.
pub fn control(paths: &Paths, c: Control) -> Result<()> {
    done(ask(paths, Ask::Control(c)))
}

/// A request's answer: done, or why not.
fn done(answer: std::result::Result<Reply, IpcError>) -> Result<()> {
    match answer {
        Ok(Reply::Done) => Ok(()),
        Ok(Reply::Refused(why)) => Err(why.into()),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => {
            Err("the daemon isn't running (`hbsync start` starts it)".into())
        }
        Err(e) => Err(e.into()),
    }
}

/// Sends `control` to every folder (sync now, pause, resume).
pub fn daemon(paths: &Paths, control: DaemonControl) -> Result<()> {
    self::control(paths, Control::Daemon(control))
}

/// Lets the held deletes of the folder at `path` through: `token` names those the user saw.
pub fn confirm_deletes(paths: &Paths, path: &std::path::Path, token: String) -> Result<()> {
    self::control(
        paths,
        Control::Folder {
            path: path.to_path_buf(),
            control: crate::protocol::FolderControl::ConfirmDeletes { token },
        },
    )
}

/// What emptying Recently deleted takes after the user saw it: everything up to the newest
/// time the server confirmed among what was shown (anything deleted after the look is newer),
/// and this device's own removals shown with their time still open, by node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Shown {
    pub until: i64,
    /// `(identity, node)`.
    pub open: Vec<(String, hbsync_types::NodeId)>,
}

impl Shown {
    pub fn of(items: &[DeletedItem]) -> Shown {
        Shown {
            until: items
                .iter()
                .filter_map(|i| i.deleted.removed_at_ms)
                .max()
                .unwrap_or(0),
            open: items
                .iter()
                .filter(|i| i.deleted.removed_at_ms.is_none())
                .map(|i| (i.identity.clone(), i.deleted.node))
                .collect(),
        }
    }

    /// The open ones as a form field: `identity/node`, comma-separated.
    pub fn open_field(&self) -> String {
        let pairs: Vec<String> = self
            .open
            .iter()
            .map(|(identity, node)| format!("{identity}/{}", node.0))
            .collect();
        pairs.join(",")
    }

    /// [`Shown::open_field`] read back; what doesn't read is left out.
    pub fn with_open_field(until: i64, field: &str) -> Shown {
        let open = field
            .split(',')
            .filter_map(|pair| {
                let (identity, node) = pair.split_once('/')?;
                Some((
                    identity.to_owned(),
                    hbsync_types::NodeId(node.parse().ok()?),
                ))
            })
            .collect();
        Shown { until, open }
    }
}

/// Empties Recently deleted of what was `shown`, in each of `identities`.
pub fn empty_shown(paths: &Paths, identities: &[String], shown: &Shown) -> Result<()> {
    empty_deleted_until(paths, identities, shown.until)?;
    let mut open: std::collections::BTreeMap<&str, Vec<hbsync_types::NodeId>> =
        std::collections::BTreeMap::new();
    for (identity, node) in &shown.open {
        open.entry(identity).or_default().push(*node);
    }
    for (identity, nodes) in open {
        account(
            paths,
            identity,
            crate::protocol::AccountControl::EmptyDeleted { nodes },
        )?;
    }
    Ok(())
}

/// Deletes for good what Recently deleted had until `at_ms` (Unix ms, its newest item the user
/// saw), in each of `identities`: anything deleted since is newer.
pub fn empty_deleted_until(paths: &Paths, identities: &[String], at_ms: i64) -> Result<()> {
    for identity in identities {
        account(
            paths,
            identity,
            crate::protocol::AccountControl::EmptyDeletedUntil { at_ms },
        )?;
    }
    Ok(())
}

/// Has the account `identity` start over: its credentials changed.
pub fn start_over(paths: &Paths, identity: &str) -> Result<()> {
    done(ask(
        paths,
        Ask::StartOver {
            identity: identity.to_owned(),
        },
    ))
}

/// Sends `control` to the account `identity` (its Recently deleted).
pub fn account(
    paths: &Paths,
    identity: &str,
    control: crate::protocol::AccountControl,
) -> Result<()> {
    self::control(
        paths,
        Control::Account {
            identity: identity.to_owned(),
            control,
        },
    )
}

/// Has the daemon set the folder at `path` up for `identity` and sync it.
pub fn add_folder(
    paths: &Paths,
    identity: &str,
    path: &std::path::Path,
    role: Option<hbsync_types::Role>,
) -> Result<()> {
    done(ask(
        paths,
        Ask::AddFolder {
            identity: identity.to_owned(),
            path: path.to_path_buf(),
            role,
        },
    ))
}

/// Has the daemon stop syncing the folder at `path`; its files and state stay.
pub fn remove_folder(paths: &Paths, path: &std::path::Path) -> Result<()> {
    done(ask(
        paths,
        Ask::RemoveFolder {
            path: path.to_path_buf(),
        },
    ))
}

/// Whether a daemon answers.
pub fn running(paths: &Paths) -> std::result::Result<bool, IpcError> {
    match ask(paths, Ask::Status) {
        Ok(_) => Ok(true),
        Err(IpcError::NotRunning) => Ok(false),
        Err(e) => Err(e),
    }
}

/// A private instance (`HBSYNC_HOME`): tests, development. It leaves the user's logon entry
/// alone unless asked by `hbsync autostart`.
pub fn dev_home() -> bool {
    std::env::var_os("HBSYNC_HOME").is_some_and(|h| !h.is_empty())
}

/// Starts the daemon and waits until it answers: through the service manager when it starts
/// the daemon at logon (so that it supervises it), else `hbsyncd` beside this program, on its
/// own (on a Linux desktop, in a scope of the app's: [`crate::unit`]).
pub fn start(paths: &Paths) -> Result<()> {
    if !dev_home() && autostart::installed()? && autostart::start()? {
        return wait_until_running(paths, None);
    }
    let exe = autostart::daemon_exe()?;
    #[cfg(windows)]
    keep_std_handles_from_children();
    let mut process = Command::new(&exe);
    process
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut process, 0);
    #[cfg(windows)]
    {
        // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP: not tied to this console.
        std::os::windows::process::CommandExt::creation_flags(&mut process, 0x8 | 0x200);
    }
    let child = process
        .spawn()
        .map_err(|e| format!("can't start {}: {e}", exe.display()))?;
    #[cfg(target_os = "linux")]
    crate::unit::move_to_scope(child.id());
    wait_until_running(paths, Some(child))
}

/// Windows children inherit every inheritable handle, and ours are when we were started with
/// redirected output: the daemon would hold our stdout open, and whoever reads it would wait
/// until the daemon stops.
#[cfg(windows)]
fn keep_std_handles_from_children() {
    use windows_sys::Win32::Foundation::{
        HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
    };
    use windows_sys::Win32::System::Console::{
        GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: GetStdHandle has no preconditions; SetHandleInformation gets a handle of
        // this process (or fails harmlessly on a missing one).
        unsafe {
            let handle = GetStdHandle(which);
            if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

/// Waits until the daemon answers, or `child` (when we started it) exits.
fn wait_until_running(paths: &Paths, mut child: Option<std::process::Child>) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(15);
    while Instant::now() < until {
        if running(paths)? {
            return Ok(());
        }
        if let Some(status) = child.as_mut().map(|c| c.try_wait()).transpose()?.flatten() {
            return Err(format!(
                "the daemon exited ({status}); its log is in {}",
                paths.logs.display()
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "the daemon didn't answer; its log is in {}",
        paths.logs.display()
    )
    .into())
}

/// Stops the daemon and waits until it's gone. False when it wasn't running.
pub fn stop(paths: &Paths) -> Result<bool> {
    match ask(paths, Ask::Stop) {
        Err(IpcError::NotRunning) => return Ok(false),
        // The daemon may exit before its answer is out: the connection just ends.
        Err(IpcError::Protocol(_)) => {}
        Err(IpcError::Io(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            ) => {}
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let until = Instant::now() + Duration::from_secs(60);
    while Instant::now() < until {
        // A daemon on its way out may drop a connection: that's not an answer.
        if matches!(running(paths), Ok(false)) {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("the daemon is still finishing a transfer; try again in a moment".into())
}

/// Turns starting at logon off, unless this is a private instance. True when it was on.
pub fn autostart_off_unless_dev() -> Result<bool> {
    if !dev_home() && autostart::installed()? {
        autostart::uninstall()?;
        return Ok(true);
    }
    Ok(false)
}

/// The folder synced, if set up.
pub fn configured(paths: &Paths) -> Result<Option<Root>> {
    Ok(Config::load(&paths.config_file())?.and_then(|c| c.root().ok().flatten().cloned()))
}

/// What [`log_out`] did.
#[derive(Debug)]
pub struct LoggedOut {
    pub root: Root,
    /// The keychain couldn't be reached, so credentials kept there may remain.
    pub keychain_unreachable: Option<String>,
    pub autostart_was_on: bool,
}

/// Stops the daemon and forgets this computer's credentials. The folder and its files stay.
/// `None` when nothing is set up.
pub fn log_out(paths: &Paths) -> Result<Option<LoggedOut>> {
    let Some(root) = configured(paths)? else {
        return Ok(None);
    };
    stop(paths)?;
    let autostart_was_on = autostart_off_unless_dev()?;
    let keychain_unreachable = CredentialStore::open(paths).delete(&root.identity)?;
    Ok(Some(LoggedOut {
        root,
        keychain_unreachable,
        autostart_was_on,
    }))
}

/// What [`unlink`] did.
#[derive(Debug)]
pub struct Unlinked {
    /// The folders no longer synced; their files stay.
    pub roots: Vec<Root>,
    pub autostart_was_on: bool,
}

/// Stops the daemon and stops syncing the folder; setting it up again picks up where it was.
/// `None` when nothing is set up.
pub fn unlink(paths: &Paths) -> Result<Option<Unlinked>> {
    let path = paths.config_file();
    let Some(mut config) = Config::load(&path)? else {
        return Ok(None);
    };
    stop(paths)?;
    let autostart_was_on = autostart_off_unless_dev()?;
    // Setting a folder up again starts afresh.
    config.paused = false;
    let roots = std::mem::take(&mut config.roots);
    config.save(&path)?;
    // Only the main folder was decorated: a role folder's shortcuts are the user's own.
    for root in roots.iter().filter(|r| r.role.is_none()) {
        crate::shell::undecorate(paths, &root.folder, &root.identity);
    }
    Ok(Some(Unlinked {
        roots,
        autostart_was_on,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What emptying takes after a look: up to the newest confirmed time, and the open items
    /// by node, also through the tray's form field.
    #[test]
    fn emptying_takes_what_was_shown_the_open_items_too() {
        use hbsync_types::activity::Deleted;
        use hbsync_types::{NodeId, NodeKind};
        let item = |identity: &str, at: Option<i64>| DeletedItem {
            identity: identity.into(),
            deleted: Deleted {
                node: NodeId::new(),
                parent: NodeId::ROOT,
                path: "x".into(),
                kind: NodeKind::File,
                size: None,
                removed_at_ms: at,
                root: NodeId::ROOT,
                folder: None,
            },
            along: 0,
        };
        let items = [item("a", None), item("a", Some(7)), item("b", Some(9))];
        let shown = Shown::of(&items);
        assert_eq!(shown.until, 9);
        assert_eq!(shown.open, [("a".to_owned(), items[0].deleted.node)]);
        assert_eq!(Shown::with_open_field(9, &shown.open_field()), shown);
        assert_eq!(Shown::with_open_field(3, "").open, []);
    }

    #[test]
    fn an_older_daemon_is_told_by_its_refusal() {
        let refusal =
            |v: u32| format!("this daemon speaks protocol {v}, the request 4: restart the daemon");
        assert_eq!(older_protocol(&refusal(3)), Some(3));
        assert_eq!(older_protocol(&refusal(crate::protocol::PROTOCOL)), None);
        assert_eq!(older_protocol("not a request: eof"), None);
    }
}
