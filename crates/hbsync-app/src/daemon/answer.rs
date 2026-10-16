//! Answering the control socket: the status of every account and folder, their lists, and
//! requests routed to the daemon, an account or a folder.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use hbsync_types::NodeId;

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
            let find = |nodes: &HashSet<NodeId>, all| {
                accounts
                    .iter()
                    .flat_map(|a| a.find_deleted(nodes, all))
                    .collect()
            };
            match find_deleted(find, &items, main.as_deref()) {
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

/// The items of Recently deleted, any account's, that `picks` name: a node id, or the place
/// [`words::deleted_place`] shows (a delete, or an item a delete took along); refused naming the
/// first one that isn't there.
/// `find` gives the items of Recently deleted that are those nodes, or every one when asked
/// (to pick by place).
fn find_deleted(
    find: impl Fn(&HashSet<NodeId>, bool) -> Vec<DeletedItem>,
    picks: &[String],
    main: Option<&Path>,
) -> Result<Vec<DeletedItem>, String> {
    let pick = |p: &String| match p.parse() {
        Ok(id) => Pick::Node(NodeId(id)),
        Err(_) => Pick::Place(p.trim_end_matches('/').to_owned()),
    };
    let picks: Vec<(&String, Pick)> = picks.iter().map(|p| (p, pick(p))).collect();
    let nodes: HashSet<NodeId> = picks
        .iter()
        .filter_map(|(_, p)| match p {
            Pick::Node(n) => Some(*n),
            Pick::Place(_) => None,
        })
        .collect();
    let by_place = picks.iter().any(|(_, p)| matches!(p, Pick::Place(_)));
    // Places are worked out only when a pick names one.
    let found: Vec<(DeletedItem, Option<String>)> = find(&nodes, by_place)
        .into_iter()
        .map(|item| {
            let place = by_place.then(|| words::deleted_place(&item.deleted, main));
            (item, place)
        })
        .collect();
    picks
        .iter()
        .map(|(named, p)| {
            found
                .iter()
                .find(|(item, place)| match p {
                    Pick::Node(n) => item.deleted.node == *n,
                    Pick::Place(want) => place.as_deref() == Some(want.as_str()),
                })
                .map(|(item, _)| item.clone())
                .ok_or_else(|| format!("{named} isn't in Recently deleted"))
        })
        .collect()
}

enum Pick {
    Node(NodeId),
    Place(String),
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
    use hbsync_types::activity::Deleted;
    use hbsync_types::{NodeKind, Uuid};

    fn item(identity: &str, n: u128, root: u128, path: &str, along: usize) -> DeletedItem {
        DeletedItem {
            identity: identity.into(),
            deleted: Deleted {
                node: NodeId(Uuid::from_u128(n)),
                parent: NodeId::ROOT,
                path: path.into(),
                kind: NodeKind::File,
                size: None,
                removed_at_ms: Some(1),
                root: NodeId(Uuid::from_u128(root)),
                folder: Some("/home/a/Homebase".into()),
            },
            along,
        }
    }

    /// Picks by node id ask only for those nodes; a place asks for everything once. A delete
    /// or an item a delete took along can be picked, in any account.
    #[test]
    fn picks_are_found_by_id_or_place_in_any_account() {
        let all = [
            item("anna.example", 1, 1, "Photos", 2),
            item("anna.example", 10, 1, "Photos/p1.jpg", 0),
            item("bob.example", 2, 2, "notes.txt", 0),
        ];
        let asked = std::cell::RefCell::new(Vec::new());
        let find = |nodes: &HashSet<NodeId>, every: bool| {
            asked.borrow_mut().push((nodes.len(), every));
            all.iter()
                .filter(|i| every || nodes.contains(&i.deleted.node))
                .cloned()
                .collect()
        };
        let main = Some(Path::new("/home/a/Homebase"));
        let picks = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let id = Uuid::from_u128(2).to_string();
        let found = find_deleted(find, &picks(&[&id]), main).expect("found");
        assert_eq!(found[0].identity, "bob.example");
        assert_eq!(asked.borrow().last(), Some(&(1, false)), "only that node");

        let found = find_deleted(find, &picks(&["Photos/", "Photos/p1.jpg"]), main);
        let found = found.expect("found");
        assert_eq!(
            (found[0].deleted.path.as_str(), found[0].along),
            ("Photos", 2)
        );
        assert_eq!(found[1].deleted.path, "Photos/p1.jpg");
        assert_eq!(asked.borrow().last(), Some(&(0, true)));

        assert_eq!(
            find_deleted(find, &picks(&["Photos", "gone.txt"]), main),
            Err("gone.txt isn't in Recently deleted".to_owned())
        );
    }
}
