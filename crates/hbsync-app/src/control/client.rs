//! Asking the running daemon over the control socket: its status, the recently synced and
//! Recently deleted lists, and requests to its folders and accounts.

use hbsync_types::NodeId;

use super::Result;
use crate::Identity;
use crate::Paths;
use crate::ipc::{self, IpcError};
use crate::protocol::{
    AccountControl, Ask, Control, DaemonControl, DaemonStatus, DeletedAccount, DeletedItem,
    DeletedList, FolderControl, RecentItem, Reply, Request,
};

/// Asks the daemon `what` over the control socket and waits for its reply.
pub(super) fn ask(paths: &Paths, what: Ask) -> std::result::Result<Reply, IpcError> {
    ipc::call(paths, &Request::new(what))
}

/// A daemon of an older build is running (the programs were updated while it ran): it speaks
/// an older protocol; [`stop`](super::stop) stops it all the same.
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
        Ok(reply) => status_of(reply).map(Some),
        Err(IpcError::NotRunning) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The status a reply carries; an older daemon's refusal is [`OlderDaemon`].
fn status_of(reply: Reply) -> Result<DaemonStatus> {
    match reply {
        Reply::Status(s) => Ok(*s),
        Reply::Refused(why) if older_protocol(&why).is_some() => Err(OlderDaemon.into()),
        other => Err(format!("unexpected answer: {other:?}").into()),
    }
}

/// Watches the daemon's status: `None` when it isn't running. [`Watch::status`] waits for each.
pub fn watch(paths: &Paths) -> Result<Option<Watch>> {
    match ipc::watch(paths) {
        Ok(watching) => Ok(Some(Watch(watching))),
        Err(IpcError::NotRunning) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The daemon's status as it changes ([`watch`]).
#[derive(Debug)]
pub struct Watch(ipc::Watching);

impl Watch {
    /// The status now (at first), then each time it changed: waits until then. `None` once the
    /// daemon stopped.
    pub fn status(&mut self) -> Result<Option<DaemonStatus>> {
        self.0.reply()?.map(status_of).transpose()
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
fn control(paths: &Paths, c: Control) -> Result<()> {
    done(ask(paths, Ask::Control(c)))
}

/// A request's answer: done, or why not.
fn done(answer: std::result::Result<Reply, IpcError>) -> Result<()> {
    match answer {
        Ok(Reply::Done) => Ok(()),
        Ok(Reply::Refused(why)) => Err(why.into()),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => Err(NotRunning.into()),
        Err(e) => Err(e.into()),
    }
}

/// No daemon took the request: none runs (any more).
#[derive(Debug, thiserror::Error)]
#[error("the daemon isn't running (`hbsync start` starts it)")]
pub struct NotRunning;

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
            control: FolderControl::ConfirmDeletes { token },
        },
    )
}

/// Puts the held deletes of the folder at `path` back: `token` names those the user saw.
pub fn put_back(paths: &Paths, path: &std::path::Path, token: String) -> Result<()> {
    self::control(
        paths,
        Control::Folder {
            path: path.to_path_buf(),
            control: FolderControl::PutBack { token },
        },
    )
}

/// Restores `items` (each with what its delete took along), in their accounts.
pub fn restore(paths: &Paths, items: &[DeletedItem]) -> Result<()> {
    // A restore takes the items as they are now: one request per account, whatever listed them.
    for (identity, nodes) in grouped(items, |i| i.identity.clone()) {
        account(paths, &identity, AccountControl::Restore { nodes })?;
    }
    Ok(())
}

/// Deletes `items` for good as they were listed (each with what was in it then), in their
/// accounts.
pub fn empty(paths: &Paths, items: &[DeletedItem]) -> Result<()> {
    for ((identity, db, seq), nodes) in
        grouped(items, |i| (i.identity.clone(), i.db.clone(), i.seq))
    {
        account(
            paths,
            &identity,
            AccountControl::EmptyDeleted { nodes, db, seq },
        )?;
    }
    Ok(())
}

/// The nodes of `items` by `key` (their account, say, or the listing that gave them).
fn grouped<K: Ord>(
    items: &[DeletedItem],
    key: impl Fn(&DeletedItem) -> K,
) -> std::collections::BTreeMap<K, Vec<NodeId>> {
    let mut by = std::collections::BTreeMap::<K, Vec<NodeId>>::new();
    for item in items {
        by.entry(key(item)).or_default().push(item.deleted.node);
    }
    by
}

/// Empties Recently deleted of everything it held when `accounts` were listed, nothing removed
/// since: each account is asked, the ones that refuse are named.
pub fn empty_listed(paths: &Paths, accounts: &[DeletedAccount]) -> Result<()> {
    let refused: Vec<String> = accounts
        .iter()
        .filter_map(|a| {
            let control = AccountControl::EmptyListed {
                db: a.db.clone(),
                seq: a.seq,
            };
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

/// Has the account `identity` start over: its credentials changed.
pub fn start_over(paths: &Paths, identity: &Identity) -> Result<()> {
    done(ask(
        paths,
        Ask::StartOver {
            identity: identity.clone(),
        },
    ))
}

/// Sends `control` to the account `identity` (its Recently deleted).
pub fn account(paths: &Paths, identity: &Identity, control: AccountControl) -> Result<()> {
    self::control(
        paths,
        Control::Account {
            identity: identity.clone(),
            control,
        },
    )
}

/// Has the daemon set the folder at `path` up for `identity` and sync it.
pub fn add_folder(
    paths: &Paths,
    identity: &Identity,
    path: &std::path::Path,
    role: Option<hbsync_types::Role>,
) -> Result<()> {
    done(ask(
        paths,
        Ask::AddFolder {
            identity: identity.clone(),
            path: path.to_path_buf(),
            role,
        },
    ))
}

/// Has the daemon stop syncing the folder at `path`; its files and state stay.
pub(crate) fn remove_folder(paths: &Paths, path: &std::path::Path) -> Result<()> {
    done(ask(
        paths,
        Ask::RemoveFolder {
            path: path.to_path_buf(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_older_daemon_is_told_by_its_refusal() {
        let refusal =
            |v: u32| format!("this daemon speaks protocol {v}, the request 4: restart the daemon");
        assert_eq!(older_protocol(&refusal(3)), Some(3));
        assert_eq!(older_protocol(&refusal(crate::protocol::PROTOCOL)), None);
        assert_eq!(older_protocol("not a request: eof"), None);
    }
}
