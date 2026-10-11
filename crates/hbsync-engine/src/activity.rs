//! The "recently synced" list: what the executor did, worded as paths for people, with many
//! changes of one kind in one folder taken together.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use hbsync_state::{StateError, Store};
use hbsync_types::activity::{Progress, Synced, SyncedKind};
use hbsync_types::{NodeId, NodeKind};

use crate::reconcile::Op;

/// More than this many changes of one kind in one folder, in one run, become one entry.
pub const GROUP_OVER: usize = 10;

/// Entries kept in the state DB.
pub const KEEP: usize = 500;

/// How far a sync is, counted as it goes ([`Progress`] when read). Shared with whoever shows
/// it; started over at each sync.
#[derive(Debug, Default)]
pub struct Counters {
    pub files_done: AtomicU64,
    pub files_total: AtomicU64,
    pub bytes_done: AtomicU64,
    pub bytes_total: AtomicU64,
}

impl Counters {
    pub fn reset(&self) {
        for n in [
            &self.files_done,
            &self.files_total,
            &self.bytes_done,
            &self.bytes_total,
        ] {
            n.store(0, Ordering::Relaxed);
        }
    }

    pub fn get(&self) -> Progress {
        let get = |n: &AtomicU64| n.load(Ordering::Relaxed);
        Progress {
            files_done: get(&self.files_done),
            files_total: get(&self.files_total),
            bytes_done: get(&self.bytes_done),
            bytes_total: get(&self.bytes_total),
        }
    }
}

/// What `op` shows as in the list, if anything: transfers, moves and deletes.
pub(crate) fn kind_of(op: &Op) -> Option<SyncedKind> {
    Some(match op {
        Op::RemoteCreate {
            kind: NodeKind::File,
            ..
        }
        | Op::RemoteUpdate { upload: true, .. } => SyncedKind::Uploaded,
        Op::LocalDownload { .. } => SyncedKind::Downloaded,
        Op::RemoteUpdate { upload: false, .. } => SyncedKind::MovedRemotely,
        Op::LocalMove { .. } => SyncedKind::MovedLocally,
        Op::RemoteDelete { .. } => SyncedKind::DeletedRemotely,
        Op::LocalTrash { .. } => SyncedKind::TrashedLocally,
        _ => return None,
    })
}

/// Where `node` is, for people: its local path, or else the path it had when last synced (a
/// node deleted here). `None` for a node this device never had.
pub fn path_of(s: &Store<'_>, node: NodeId) -> Result<Option<String>, StateError> {
    if let Some(row) = s.local_by_node(node)? {
        return Ok(Some(row.path.as_str().to_owned()));
    }
    let mut names = Vec::new();
    let mut at = node;
    // Bounded: the base is a tree, but a damaged DB must not hang the status.
    while !at.is_root() && names.len() < 256 {
        let Some(base) = s.get_base(at)? else {
            return Ok(None);
        };
        names.push(base.local_name.unwrap_or(base.state.name));
        at = base.state.parent;
    }
    names.reverse();
    Ok(Some(names.join("/")))
}

fn folder_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(folder, _)| folder)
}

/// Takes the changes of one kind in one folder together when there are more than `over`:
/// one entry for the folder, at the place of the first, with their count and sizes added up.
pub fn group(entries: Vec<Synced>, over: usize) -> Vec<Synced> {
    let mut counts: HashMap<(SyncedKind, &str), usize> = HashMap::new();
    for e in &entries {
        *counts.entry((e.kind, folder_of(&e.path))).or_default() += 1;
    }
    let grouped: Vec<(SyncedKind, String)> = counts
        .into_iter()
        .filter(|(_, n)| *n > over)
        .map(|((kind, folder), _)| (kind, folder.to_owned()))
        .collect();
    if grouped.is_empty() {
        return entries;
    }
    let mut out: Vec<Synced> = Vec::new();
    let mut at: HashMap<(SyncedKind, String), usize> = HashMap::new();
    for e in entries {
        let key = (e.kind, folder_of(&e.path).to_owned());
        if !grouped.contains(&key) {
            out.push(e);
            continue;
        }
        match at.get(&key) {
            Some(&i) => {
                let g = &mut out[i];
                g.files += e.files;
                g.bytes += e.bytes;
                g.at = g.at.max(e.at);
            }
            None => {
                at.insert(key.clone(), out.len());
                out.push(Synced { path: key.1, ..e });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(kind: SyncedKind, path: &str, bytes: u64) -> Synced {
        Synced {
            at: 1_700_000_000,
            kind,
            path: path.into(),
            files: 1,
            bytes,
        }
    }

    #[test]
    fn many_in_one_folder_become_one() {
        let mut entries = vec![entry(SyncedKind::Uploaded, "notes.txt", 5)];
        for i in 0..12 {
            entries.push(entry(
                SyncedKind::Downloaded,
                &format!("Photos/2024/p{i}.jpg"),
                10,
            ));
        }
        // Another kind in the same folder, and few in another folder: kept one by one.
        entries.push(entry(SyncedKind::Uploaded, "Photos/2024/mine.jpg", 7));
        entries.push(entry(SyncedKind::Downloaded, "Docs/a.pdf", 3));
        let out = group(entries, GROUP_OVER);
        assert_eq!(
            out,
            vec![
                entry(SyncedKind::Uploaded, "notes.txt", 5),
                Synced {
                    files: 12,
                    ..entry(SyncedKind::Downloaded, "Photos/2024", 120)
                },
                entry(SyncedKind::Uploaded, "Photos/2024/mine.jpg", 7),
                entry(SyncedKind::Downloaded, "Docs/a.pdf", 3),
            ]
        );
        // The top folder's group is the empty path.
        let top = (0..11)
            .map(|i| entry(SyncedKind::TrashedLocally, &format!("f{i}"), 0))
            .collect();
        assert_eq!(group(top, GROUP_OVER)[0].path, "");
    }
}
