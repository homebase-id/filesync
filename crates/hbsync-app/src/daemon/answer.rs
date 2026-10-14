//! Answering the control socket: the status of every account and folder, their lists, and
//! requests routed to the daemon, an account or a folder.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hbsync_engine::root::SyncRoot;

use super::Daemon;
use super::account::Account;
use crate::protocol::{
    Ask, Control, DaemonControl, DaemonStatus, DeletedItem, FolderControl, RecentItem, Reply,
    Request, Until,
};
use crate::runner::Command;
use crate::{Config, ConfigError, Paths};

pub(super) fn answer(request: Request, daemon: &Daemon) -> Reply {
    let accounts = &daemon.accounts();
    match request.ask {
        Ask::Status => Reply::Status(Box::new(status(accounts))),
        Ask::Recent { limit } => Reply::Recent(recent(accounts, limit)),
        Ask::Deleted { limit } => Reply::Deleted(deleted(accounts, limit)),
        Ask::Control(control) => self::control(control, accounts, &daemon.stop, &daemon.paths),
        Ask::AddFolder {
            identity,
            path,
            role,
        } => {
            let added = crate::folders::add(&daemon.paths, &identity, &path, role)
                .map_err(|e| e.to_string())
                .and_then(|()| {
                    daemon
                        .add_folder(&identity, &path)
                        .map_err(|e| e.to_string())
                });
            match added {
                Ok(()) => Reply::Done,
                Err(why) => Reply::Refused(why),
            }
        }
        Ask::RemoveFolder { path } => match crate::folders::remove(&daemon.paths, &path) {
            Ok(_) => {
                for a in accounts {
                    a.remove_folder(&path);
                }
                Reply::Done
            }
            Err(e) => Reply::Refused(e.to_string()),
        },
    }
}

fn status(accounts: &[Arc<Account>]) -> DaemonStatus {
    let mut status = DaemonStatus {
        version: crate::VERSION.into(),
        accounts: Vec::new(),
        folders: Vec::new(),
    };
    for a in accounts {
        let (account, folders) = a.status();
        status.accounts.push(account);
        status.folders.extend(folders);
    }
    status
}

/// Every folder's recently synced list, from the account's state DB (WAL: a sync writing
/// doesn't block it), in any phase; newest first.
fn recent(accounts: &[Arc<Account>], limit: usize) -> Vec<RecentItem> {
    let mut items: Vec<RecentItem> = accounts
        .iter()
        .flat_map(|a| a.folders().into_iter().map(move |f| (a, f)))
        .flat_map(|(a, folder)| {
            SyncRoot::open(a.db(), &a.identity, &folder)
                .and_then(|r| r.state())
                .ok()
                .and_then(|mut db| db.read(|s| s.recent_synced(limit)).ok())
                .unwrap_or_default()
                .into_iter()
                .map(move |synced| RecentItem {
                    folder: folder.clone(),
                    synced,
                })
        })
        .collect();
    items.sort_by_key(|i| std::cmp::Reverse(i.synced.at));
    items.truncate(limit);
    items
}

/// Every account's Recently deleted, each item placed in drive coordinates; newest first.
fn deleted(accounts: &[Arc<Account>], limit: usize) -> Vec<DeletedItem> {
    let mut items: Vec<DeletedItem> = accounts
        .iter()
        .filter(|a| a.db().exists())
        .flat_map(|a| {
            SyncRoot::open_account(a.db(), &a.identity)
                .ok()
                .and_then(|mut db| hbsync_engine::deleted::list(&mut db, limit).ok())
                .unwrap_or_default()
                .into_iter()
                .map(|deleted| DeletedItem {
                    identity: a.identity.clone(),
                    deleted,
                })
        })
        .collect();
    items.sort_by_key(|i| std::cmp::Reverse(i.deleted.removed_at_ms));
    items.truncate(limit);
    items
}

fn control(control: Control, accounts: &[Arc<Account>], stop: &AtomicBool, paths: &Paths) -> Reply {
    match control {
        Control::Daemon(DaemonControl::Stop) => {
            stop.store(true, Ordering::Relaxed);
            for a in accounts {
                a.send_all(&Command::Stop);
            }
            Reply::Done
        }
        Control::Daemon(c @ (DaemonControl::Pause { .. } | DaemonControl::Resume)) => {
            let pause = match c {
                DaemonControl::Pause { until } => Some(until),
                _ => None,
            };
            // Pausing survives a restart, and is kept until the sync starts.
            if let Err(e) = remember_paused(paths, pause) {
                return Reply::Refused(format!("couldn't save the pause in the config: {e}"));
            }
            for a in accounts {
                a.pause(pause);
            }
            Reply::Done
        }
        Control::Daemon(DaemonControl::SyncNow) => {
            let mut not_running = Vec::new();
            for a in accounts {
                if !a.send_all(&Command::SyncNow) {
                    not_running.extend(a.not_running());
                }
            }
            match not_running.len() < accounts.len() {
                true => Reply::Done,
                false => Reply::Refused(not_running.join("; ")),
            }
        }
        Control::Account { identity, control } => {
            match accounts.iter().find(|a| a.identity == identity) {
                None => Reply::Refused(format!("{identity} isn't set up here")),
                Some(a) => match a.request(control) {
                    Ok(()) => Reply::Done,
                    Err(why) => Reply::Refused(why),
                },
            }
        }
        Control::Folder { path, control } => {
            let Some(account) = accounts.iter().find(|a| a.folders().contains(&path)) else {
                return Reply::Refused(format!("{} isn't synced here", path.display()));
            };
            let Some(runner) = account.runner(&path) else {
                return Reply::Refused(account.not_running().unwrap_or_default());
            };
            let FolderControl::ConfirmDeletes { token } = control;
            if runner.status().held_deletes.map(|held| held.token) != Some(token.clone()) {
                return Reply::Refused(
                    "the held deletes changed since you looked: check them again with \
                     `hbsync confirm-deletes`"
                        .into(),
                );
            }
            match runner.send(Command::ConfirmDeletes { token }) {
                true => Reply::Done,
                false => Reply::Refused("the sync has stopped".into()),
            }
        }
    }
}

fn remember_paused(paths: &Paths, pause: Option<Until>) -> Result<(), ConfigError> {
    let path = paths.config_file();
    let mut config = Config::load(&path)?.unwrap_or_default();
    let was = (config.paused, config.paused_until);
    config.set_pause(pause);
    if (config.paused, config.paused_until) != was {
        config.save(&path)?;
    }
    Ok(())
}
