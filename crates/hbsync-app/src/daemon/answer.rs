//! Answering the control socket: the status of every account and folder, their lists, and
//! requests routed to the daemon, an account or a folder.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use hbsync_types::NodeId;

use super::Daemon;
use super::account::Account;
use crate::Config;
use crate::protocol::{
    Ask, Control, DaemonControl, DaemonStatus, DeletedItem, DeletedList, FolderControl, Reply,
    Request,
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
        // A watch is taken by the control socket; asked here, it is answered once.
        Ask::Status | Ask::Watch => Reply::Status(Box::new(status(accounts))),
        Ask::Recent { limit } => {
            let mut items: Vec<_> = accounts.iter().flat_map(|a| a.recent(limit)).collect();
            items.sort_by_key(|i| std::cmp::Reverse(i.synced.at));
            items.truncate(limit);
            Reply::Recent(items)
        }
        Ask::Deleted { limit } => {
            // Each account's newest `limit` deletes; newest first, `limit` deletes of them all.
            let mut list = DeletedList::default();
            for a in accounts {
                let (items, account) = a.deleted(limit);
                list.items.extend(items);
                list.accounts.extend(account);
            }
            list.items
                .sort_by_key(|i| std::cmp::Reverse(i.deleted.time()));
            list.items.truncate(limit);
            Reply::Deleted(list)
        }
        Ask::FindDeleted { items } => {
            // The main folder names places: read only when a place is to be found.
            let main = || match crate::control::configured(&daemon.paths) {
                Ok(root) => Ok(root.map(|r| r.folder)),
                Err(e) => Err(format!("reading the config: {e}")),
            };
            let by_node = |nodes: &[NodeId]| {
                accounts
                    .iter()
                    .flat_map(|a| a.find_deleted(nodes))
                    .collect()
            };
            let by_name = |names: &[String]| {
                accounts
                    .iter()
                    .flat_map(|a| a.named_deleted(names))
                    .collect()
            };
            match find_deleted(by_node, by_name, &items, main) {
                Ok(found) => Reply::Found(found),
                Err(why) => Reply::Refused(why),
            }
        }
        Ask::Stop => {
            daemon.stop.stop();
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
/// [`words::deleted_place`] shows. A place names what `hbsync deleted` lists there: a delete
/// (the newest, if there are more); only where none is, an item a delete took along. Refused
/// naming the first pick that isn't there. `by_node` gives the items that are those nodes;
/// `by_name` the items with the names the places' items have ([`words::deleted_place_name`]),
/// and `main` the main folder (it names places), both asked only when a pick names a place (a
/// pick shaped like a node id that isn't one is tried as a place too).
fn find_deleted(
    by_node: impl Fn(&[NodeId]) -> Vec<DeletedItem>,
    by_name: impl Fn(&[String]) -> Vec<DeletedItem>,
    picks: &[String],
    main: impl Fn() -> Result<Option<PathBuf>, String>,
) -> Result<Vec<DeletedItem>, String> {
    let ids: Vec<NodeId> = picks
        .iter()
        .filter_map(|p| p.parse().ok().map(NodeId))
        .collect();
    let mut by_id: HashMap<NodeId, DeletedItem> = HashMap::new();
    if !ids.is_empty() {
        by_id.extend(by_node(&ids).into_iter().map(|i| (i.deleted.node, i)));
    }
    let place = |p: &String| p.trim_end_matches('/').to_owned();
    let found = |p: &String| p.parse().ok().map(NodeId).and_then(|id| by_id.get(&id));
    // Each place's item: a delete before an item taken along, the newest of either.
    let mut by_place: HashMap<String, DeletedItem> = HashMap::new();
    let names: Vec<String> = picks
        .iter()
        .filter(|p| found(p).is_none())
        .map(|p| words::deleted_place_name(p).to_owned())
        .collect();
    if !names.is_empty() {
        let main = main()?;
        let mut all = by_name(&names);
        all.sort_by_key(|i| {
            (
                i.deleted.root != i.deleted.node,
                std::cmp::Reverse(i.deleted.time()),
            )
        });
        for item in all {
            by_place
                .entry(words::deleted_place(&item.deleted, main.as_deref()))
                .or_insert(item);
        }
    }
    picks
        .iter()
        .map(|p| {
            found(p)
                .or_else(|| by_place.get(&place(p)))
                .cloned()
                .ok_or_else(|| format!("{p} isn't in Recently deleted"))
        })
        .collect()
}

pub(super) fn status(accounts: &[Arc<Account>]) -> DaemonStatus {
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

    fn item(identity: &str, n: u128, path: &str, at: i64, along: usize) -> DeletedItem {
        DeletedItem {
            identity: identity.into(),
            deleted: Deleted {
                node: NodeId(Uuid::from_u128(n)),
                parent: NodeId::ROOT,
                path: path.into(),
                kind: NodeKind::File,
                size: None,
                removed_at_ms: Some(at),
                root: NodeId(Uuid::from_u128(n)),
                folder: Some("/home/a/Homebase".into()),
                stays: false,
            },
            along,
            db: "d1".into(),
            seq: 1,
        }
    }

    /// An item a delete took along: `root`'s.
    fn along(n: u128, path: &str, at: i64, root: u128) -> DeletedItem {
        let mut i = item("anna.example", n, path, at, 0);
        i.deleted.root = NodeId(Uuid::from_u128(root));
        i
    }

    /// A pick by node id asks only for those nodes, and needs no config; a place asks once for
    /// the items with its item's name, and names what `hbsync deleted` lists there: the newest
    /// delete, an item taken along only where no delete is; a node-id-shaped name that isn't one
    /// is tried as a place; a place no folder here syncs is found as listed.
    #[test]
    fn picks_are_found_by_id_or_place_in_any_account() {
        let uuid_name = Uuid::from_u128(77).to_string();
        let all = [
            item("anna.example", 1, "Photos", 30, 2),
            item("anna.example", 3, "Photos", 10, 0),
            // Newer, but taken along by another delete: a delete at the same place wins.
            along(5, "Photos", 40, 9),
            along(6, "Old/x.txt", 40, 9),
            item("bob.example", 2, "notes.txt", 20, 0),
            item("bob.example", 4, &uuid_name, 5, 0),
            // In no folder here.
            DeletedItem {
                deleted: Deleted {
                    folder: None,
                    ..item("bob.example", 8, "cv.txt", 50, 0).deleted
                },
                ..item("bob.example", 8, "cv.txt", 50, 0)
            },
        ];
        let everything = std::cell::Cell::new(0);
        let by_node = |nodes: &[NodeId]| {
            all.iter()
                .filter(|i| nodes.contains(&i.deleted.node))
                .cloned()
                .collect()
        };
        // As the engine finds them: by the item's own name.
        let every = |names: &[String]| {
            everything.set(everything.get() + 1);
            all.iter()
                .filter(|i| names.iter().any(|n| n == words::last_name(&i.deleted.path)))
                .cloned()
                .collect()
        };
        let main = || Ok(Some(PathBuf::from("/home/a/Homebase")));
        let broken = || Err("reading the config: bad".to_owned());
        let picks = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let node = |n: u128| NodeId(Uuid::from_u128(n));

        let id = Uuid::from_u128(2).to_string();
        let found = find_deleted(by_node, every, &picks(&[&id]), broken).expect("found");
        assert_eq!(found[0].identity, "bob.example");
        assert_eq!(
            everything.get(),
            0,
            "a node id needs no listing and no config"
        );

        let found = find_deleted(by_node, every, &picks(&["Photos/", &uuid_name]), main);
        let found = found.expect("found");
        assert_eq!((found[0].deleted.node, found[0].along), (node(1), 2));
        assert_eq!(found[1].deleted.node, node(4));
        assert_eq!(everything.get(), 1, "one listing for every place");

        let found = find_deleted(by_node, every, &picks(&["Old/x.txt"]), main).expect("found");
        assert_eq!(
            found[0].deleted.node,
            node(6),
            "taken along, where no delete is"
        );

        let found = find_deleted(by_node, every, &picks(&["(on the drive) cv.txt"]), main);
        assert_eq!(found.map(|f| f[0].deleted.node), Ok(node(8)));

        assert_eq!(
            find_deleted(by_node, every, &picks(&["Photos", "gone.txt"]), main),
            Err("gone.txt isn't in Recently deleted".to_owned())
        );
        assert_eq!(
            find_deleted(by_node, every, &picks(&["Photos"]), broken),
            Err("reading the config: bad".to_owned())
        );
    }
}
