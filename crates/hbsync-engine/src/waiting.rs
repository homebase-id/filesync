//! The files here not hashed yet that something waits for (sync semantics §6.3,
//! [`crate::Plan::waiting`]), as one folder's engine reads them again and tells them.

use std::collections::HashMap;
use std::path::Path;

use hbsync_local::{ScanOptions, rescan_entries};
use hbsync_platform::RelPath;
use hbsync_state::{LocalRow, ReadTx, StateDb, StateError};
use hbsync_types::NodeId;

use crate::view;

/// What one sync pass read of the files waited for, and what it tells.
#[derive(Debug, Default)]
pub(crate) struct Waited {
    /// The files waited for that this sync pass read already, and why those it couldn't read
    /// couldn't: each read once a pass.
    read_once: HashMap<NodeId, Option<String>>,
    /// The files the current sweep found something waiting for, that couldn't be read
    /// ([`crate::SyncReport::waiting`]).
    waiting: Vec<(String, String)>,
}

impl Waited {
    /// A new sync pass: each file waited for is read once more.
    pub(crate) fn new_pass(&mut self) {
        self.waiting.clear();
        self.read_once.clear();
    }

    /// What is told of the current sweep so far, taken.
    pub(crate) fn take_told(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.waiting)
    }

    /// Something in a plan waits for the files here not hashed yet `waited` (sync semantics
    /// §6.3): those unchanged since the scan that couldn't read them (locked, unreadable: one
    /// still changing is read by the scan its change brings) are read now, each once a sync
    /// pass. True when that hashed one or found another change: the plan is to be made again.
    /// Costs nothing unless something waits; then a row and a stat a file, an open (or a read)
    /// and a small write of each unchanged one, and, if that hashed or found a change, one
    /// more plan.
    pub(crate) fn read(
        &mut self,
        db: &mut StateDb,
        root: &Path,
        scan_opts: &ScanOptions,
        waited: &[NodeId],
    ) -> Result<bool, StateError> {
        if waited.is_empty() {
            return Ok(false);
        }
        let fresh: Vec<NodeId> = waited
            .iter()
            .copied()
            .filter(|n| !self.read_once.contains_key(n))
            .collect();
        if fresh.is_empty() {
            return Ok(false);
        }
        let rows = db.read(|s| rows_of(s, &fresh))?;
        let unchanged: Vec<(NodeId, RelPath)> = rows
            .into_iter()
            .filter(|(row, _)| {
                // Its bytes as the scan saw them: size and mtime (a lock, or a mode change,
                // moves ctime at most).
                hbsync_platform::meta::stat(&row.path.to_native(root))
                    .is_ok_and(|m| row.fingerprint.same_after_rename(&m))
            })
            .map(|(row, _)| (row.node, row.path))
            .collect();
        for node in fresh {
            self.read_once.insert(node, None);
        }
        if unchanged.is_empty() {
            return Ok(false);
        }
        let paths: Vec<RelPath> = unchanged.iter().map(|(_, p)| p.clone()).collect();
        let scan = rescan_entries(db, root, &[], &paths, scan_opts)?;
        // A read that failed again marks its node dirty again and records nothing new:
        // planned again only for a hash or another change found.
        let again = scan.hashed > 0 || scan.dirty > scan.unhashed.len();
        for (path, why) in scan.unhashed {
            if let Some((node, _)) = unchanged.iter().find(|(_, p)| *p == path) {
                self.read_once.insert(*node, Some(why));
            }
        }
        Ok(again)
    }

    /// Tells what still waits for the files `waited` (after [`Waited::read`]), by name: the
    /// path the user knows each by, and why it couldn't be read.
    pub(crate) fn tell(&mut self, db: &mut StateDb, waited: &[NodeId]) -> Result<(), StateError> {
        if waited.is_empty() {
            return Ok(());
        }
        let rows = db.read(|s| rows_of(s, waited))?;
        for (row, shown) in rows {
            let why = self.read_once.get(&row.node).cloned().flatten();
            let told = (
                shown.to_string(),
                why.unwrap_or_else(|| "being written".into()),
            );
            if !self.waiting.contains(&told) {
                self.waiting.push(told);
            }
        }
        Ok(())
    }
}

/// The rows of `nodes` here, each with the path the user knows it by (a parked item's
/// original one).
fn rows_of(s: &ReadTx<'_>, nodes: &[NodeId]) -> Result<Vec<(LocalRow, RelPath)>, StateError> {
    let mut rows = Vec::new();
    for &node in nodes {
        if let Some(row) = s.local_by_node(node)? {
            let shown = view::parked_place(s, &row)?.unwrap_or_else(|| row.path.clone());
            rows.push((row, shown));
        }
    }
    Ok(rows)
}
