//! The "recently synced" list: what the executor did, worded as paths for people, with many
//! changes of one kind in one folder taken together.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use hbsync_state::{ReadTx, StateError};
use hbsync_types::activity::{Progress, Synced, SyncedKind};
use hbsync_types::{NodeId, NodeKind};

use crate::reconcile::Op;

/// More than this many changes of one kind in one folder, in one run, become one entry.
pub(crate) const GROUP_OVER: usize = 10;

/// Entries kept in the state DB.
pub(crate) const KEEP: usize = 500;

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
pub(crate) fn path_of(s: &ReadTx<'_>, node: NodeId) -> Result<Option<String>, StateError> {
    if let Some(row) = s.local_by_node(node)? {
        return Ok(Some(row.path.as_str().to_owned()));
    }
    let up = crate::walk::up::<_, StateError>(node, |at| {
        Ok(s.get_base(at)?
            .map(|b| (b.local_name.unwrap_or(b.state.name), b.state.parent)))
    })?;
    // Only a path that reaches the top is one: a gap or a cycle gives none.
    if up.end != crate::walk::End::Top {
        return Ok(None);
    }
    let mut names = up.items;
    names.reverse();
    Ok(Some(names.join("/")))
}

fn folder_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(folder, _)| folder)
}

/// Takes the changes of one kind in one folder together when there are more than `over`:
/// one entry for the folder, at the place of the first, with their count and sizes added up.
pub(crate) fn group(entries: Vec<Synced>, over: usize) -> Vec<Synced> {
    let key = |e: &Synced| (e.kind, folder_of(&e.path).to_owned());
    let mut counts: HashMap<(SyncedKind, String), usize> = HashMap::new();
    for e in &entries {
        *counts.entry(key(e)).or_default() += 1;
    }
    // Where each group's entry is in `out`.
    let mut at: HashMap<(SyncedKind, String), usize> = HashMap::new();
    let mut out: Vec<Synced> = Vec::new();
    for e in entries {
        let k = key(&e);
        if counts[&k] <= over {
            out.push(e);
            continue;
        }
        match at.get(&k) {
            Some(&i) => {
                let g = &mut out[i];
                g.files += e.files;
                g.bytes += e.bytes;
                g.at = g.at.max(e.at);
            }
            None => {
                at.insert(k.clone(), out.len());
                out.push(Synced { path: k.1, ..e });
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
