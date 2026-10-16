//! Controlling the daemon from another process (the CLI, the tray): asking it, starting it
//! beside the calling program, stopping it, and leaving an identity or folder.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::ipc::{self, IpcError};
use crate::protocol::{
    AccountControl, Ask, Control, DaemonControl, DaemonStatus, DeletedAccount, DeletedItem,
    DeletedList, RecentItem, Reply, Request,
};
use crate::{Config, CredentialStore, Paths, Root, autostart};
use hbsync_types::NodeId;

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
/// along, and each account's as a whole; `None` when the daemon isn't running.
pub fn deleted(paths: &Paths, limit: usize) -> Result<Option<DeletedList>> {
    match ask(paths, Ask::Deleted { limit }) {
        Ok(Reply::Deleted(list)) => Ok(Some(list)),
        other => unexpected(other),
    }
}

/// The items of Recently deleted that `items` name (a node id, or the place `hbsync deleted`
/// shows), deleted on their own or along with a folder; `None` when the daemon isn't running.
pub fn find_deleted(paths: &Paths, items: Vec<String>) -> Result<Option<Vec<DeletedItem>>> {
    match ask(paths, Ask::FindDeleted { items }) {
        Ok(Reply::Found(found)) => Ok(Some(found)),
        other => unexpected(other),
    }
}

fn unexpected<T>(answer: std::result::Result<Reply, IpcError>) -> Result<Option<T>> {
    match answer {
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

/// Restores `items` (each with what its delete took along), in their accounts.
pub fn restore(paths: &Paths, items: &[DeletedItem]) -> Result<()> {
    for (identity, nodes) in by_account(items) {
        account(paths, &identity, AccountControl::Restore { nodes })?;
    }
    Ok(())
}

/// Deletes `items` (each with what its delete took along) for good, in their accounts.
pub fn empty(paths: &Paths, items: &[DeletedItem]) -> Result<()> {
    for (identity, nodes) in by_account(items) {
        account(paths, &identity, AccountControl::EmptyDeleted { nodes })?;
    }
    Ok(())
}

fn by_account(items: &[DeletedItem]) -> std::collections::BTreeMap<String, Vec<NodeId>> {
    let mut by = std::collections::BTreeMap::<String, Vec<NodeId>>::new();
    for item in items {
        by.entry(item.identity.clone())
            .or_default()
            .push(item.deleted.node);
    }
    by
}

/// Empties Recently deleted of everything it held when `accounts` were listed, nothing removed
/// since: each account is asked, the ones that refuse are named.
pub fn empty_listed(paths: &Paths, accounts: &[DeletedAccount]) -> Result<()> {
    let refused: Vec<String> = accounts
        .iter()
        .filter_map(|a| {
            let control = AccountControl::EmptyListed { seq: a.seq };
            account(paths, &a.identity, control)
                .err()
                .map(|e| format!("{}: {e}", a.identity))
        })
        .collect();
    match refused.is_empty() {
        true => Ok(()),
        false => Err(refused.join("; ").into()),
    }
}

/// `accounts` as a form field (what emptying all of them takes): `identity/seq`,
/// comma-separated.
pub fn listed_field(accounts: &[DeletedAccount]) -> String {
    let parts: Vec<String> = accounts
        .iter()
        .map(|a| format!("{}/{}", a.identity, a.seq))
        .collect();
    parts.join(",")
}

/// [`listed_field`] read back (without the counts); what doesn't read is left out.
pub fn listed_from_field(field: &str) -> Vec<DeletedAccount> {
    field
        .split(',')
        .filter_map(|part| {
            let (identity, seq) = part.rsplit_once('/')?;
            Some(DeletedAccount {
                identity: identity.to_owned(),
                deletes: 0,
                items: 0,
                seq: seq.parse().ok()?,
            })
        })
        .collect()
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

    /// What emptying everything takes goes through the tray's form field and back.
    #[test]
    fn the_listed_accounts_go_through_a_form_field() {
        let a = |identity: &str, seq| DeletedAccount {
            identity: identity.into(),
            deletes: 0,
            items: 0,
            seq,
        };
        let accounts = [a("a.example", 9), a("b/c.example", 4)];
        assert_eq!(listed_from_field(&listed_field(&accounts)), accounts);
        assert_eq!(listed_from_field(""), []);
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
