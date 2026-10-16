//! Answering the control socket: the status of every account and folder, their lists, and
//! requests routed to the daemon, an account or a folder.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use hbsync_types::NodeId;
use hbsync_types::activity::Deleted;

use super::Daemon;
use super::account::Account;
use crate::Config;
use crate::protocol::{
    Ask, Control, DaemonControl, DaemonStatus, DeletedItem, FolderControl, Reply, Request,
};
use crate::runner::Command;
use crate::words;

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
            // Each account's newest `limit` deletes; newest first, `limit` deletes of them all.
            let mut items: Vec<_> = accounts.iter().flat_map(|a| a.deleted(limit)).collect();
            items.sort_by_key(|i| std::cmp::Reverse(i.deleted.time()));
            items.truncate(limit);
            Reply::Deleted(items)
        }
        Ask::FindDeleted { items } => {
            let main = crate::control::configured(&daemon.paths)
                .ok()
                .flatten()
                .map(|r| r.folder);
            let lists: Vec<_> = accounts
                .iter()
                .map(|a| (a.identity.as_str(), a.recently_deleted(usize::MAX)))
                .collect();
            match find_deleted(&lists, &items, main.as_deref()) {
                Ok(found) => Reply::Deleted(found),
                Err(why) => Reply::Refused(why),
            }
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

/// The items of `lists` (each account's Recently deleted, as the engine lists it) that `picks`
/// name: a node id, or the place [`words::deleted_place`] shows; refused naming the first one
/// that isn't there.
fn find_deleted(
    lists: &[(&str, Vec<Deleted>)],
    picks: &[String],
    main: Option<&Path>,
) -> Result<Vec<DeletedItem>, String> {
    picks
        .iter()
        .map(|pick| {
            let id = pick.parse().ok().map(NodeId);
            let place = pick.trim_end_matches('/');
            lists
                .iter()
                .find_map(|(identity, list)| {
                    let d = list
                        .iter()
                        .find(|d| Some(d.node) == id || words::deleted_place(d, main) == place)?;
                    let along = list
                        .iter()
                        .filter(|o| o.root == d.node && o.node != d.node)
                        .count();
                    Some(DeletedItem {
                        identity: (*identity).to_owned(),
                        deleted: d.clone(),
                        along,
                    })
                })
                .ok_or_else(|| format!("{pick} isn't in Recently deleted"))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_types::{NodeKind, Uuid};

    fn removed(n: u128, root: u128, path: &str) -> Deleted {
        Deleted {
            node: NodeId(Uuid::from_u128(n)),
            parent: NodeId::ROOT,
            path: path.into(),
            kind: NodeKind::File,
            size: None,
            removed_at_ms: Some(1),
            root: NodeId(Uuid::from_u128(root)),
            folder: Some("/home/a/Homebase".into()),
        }
    }

    /// Picks are found in each account's whole Recently deleted, whatever its size: by node id
    /// or place, a delete or an item it took along.
    #[test]
    fn picks_are_found_by_id_or_place_in_any_account() {
        let mut anna = vec![removed(1, 1, "Photos")];
        anna.extend((0..5000).map(|i| removed(10 + i, 1, &format!("Photos/p{i}.jpg"))));
        let bob = vec![removed(2, 2, "notes.txt")];
        let lists = [("anna.example", anna), ("bob.example", bob)];
        let main = Some(Path::new("/home/a/Homebase"));
        let picks = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let found = find_deleted(&lists, &picks(&["Photos/", "Photos/p4999.jpg"]), main);
        let found = found.expect("found");
        assert_eq!(
            (found[0].identity.as_str(), found[0].along),
            ("anna.example", 5000)
        );
        assert_eq!(found[1].deleted.path, "Photos/p4999.jpg");
        assert_eq!(found[1].along, 0);

        let id = Uuid::from_u128(2).to_string();
        let found = find_deleted(&lists, &picks(&[&id]), main).expect("found");
        assert_eq!(found[0].identity, "bob.example");

        assert_eq!(
            find_deleted(&lists, &picks(&["Photos", "gone.txt"]), main),
            Err("gone.txt isn't in Recently deleted".to_owned())
        );
    }
}
