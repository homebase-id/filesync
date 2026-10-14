//! Answering the control socket: the status of every account and folder, their lists, and
//! requests routed to the daemon, an account or a folder.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Daemon;
use super::account::Account;
use crate::Config;
use crate::protocol::{Ask, Control, DaemonControl, DaemonStatus, FolderControl, Reply, Request};
use crate::runner::Command;

pub(super) fn answer(request: Request, daemon: &Daemon) -> Reply {
    let accounts = &daemon.accounts();
    let done = |result: Result<(), Box<dyn std::error::Error + Send + Sync>>| match result {
        Ok(()) => Reply::Done,
        Err(why) => Reply::Refused(why.to_string()),
    };
    match request.ask {
        Ask::Status => Reply::Status(Box::new(status(accounts))),
        Ask::Recent { limit } => {
            let mut items: Vec<_> = accounts.iter().flat_map(|a| a.recent(limit)).collect();
            items.sort_by_key(|i| std::cmp::Reverse(i.synced.at));
            items.truncate(limit);
            Reply::Recent(items)
        }
        Ask::Deleted { limit } => {
            let mut items: Vec<_> = accounts.iter().flat_map(|a| a.deleted(limit)).collect();
            items.sort_by_key(|i| std::cmp::Reverse(i.deleted.removed_at_ms));
            items.truncate(limit);
            Reply::Deleted(items)
        }
        Ask::Stop => {
            daemon.stop.store(true, Ordering::Relaxed);
            for a in accounts {
                a.send_all(&Command::Stop);
            }
            Reply::Done
        }
        Ask::Control(control) => self::control(control, accounts, daemon),
        Ask::AddFolder {
            identity,
            path,
            role,
        } => done((|| {
            crate::folders::add(&daemon.paths, &identity, &path, role)?;
            daemon.add_folder(&identity, &path)?;
            Ok(())
        })()),
        Ask::RemoveFolder { path } => done((|| {
            crate::folders::remove(&daemon.paths, &path)?;
            for a in accounts {
                a.remove_folder(&path);
            }
            Ok(())
        })()),
        Ask::StartOver { identity } => done(daemon.start_over(&identity).map_err(Into::into)),
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

fn control(control: Control, accounts: &[Arc<Account>], daemon: &Daemon) -> Reply {
    match control {
        Control::Daemon(c @ (DaemonControl::Pause { .. } | DaemonControl::Resume)) => {
            let pause = match c {
                DaemonControl::Pause { until } => Some(until),
                _ => None,
            };
            // Pausing survives a restart, and is kept until the sync starts.
            if let Err(e) = Config::update(&daemon.paths.config_file(), |c| c.set_pause(pause)) {
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
