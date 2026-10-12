//! What was synced, for people to see: the "recently synced" list.

use serde::{Deserialize, Serialize};

use crate::{NodeId, NodeKind};

/// One thing synced, or several of one kind in one folder taken together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Synced {
    /// Unix seconds.
    pub at: u64,
    pub kind: SyncedKind,
    /// Relative to the synced folder, `/`-separated. For several taken together: their folder
    /// (empty for the top).
    pub path: String,
    /// 1, or how many were taken together.
    pub files: u64,
    /// The file's size, or the sizes added up; 0 for moves and deletes.
    pub bytes: u64,
}

impl Synced {
    /// Several taken together, under their folder.
    pub fn is_group(&self) -> bool {
        self.files > 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncedKind {
    /// A new or changed file here, sent to Homebase.
    Uploaded,
    /// A new or changed file from Homebase.
    Downloaded,
    /// Moved or renamed here; Homebase follows.
    MovedRemotely,
    /// Moved or renamed on another device; followed here.
    MovedLocally,
    /// Deleted here; deleted from Homebase.
    DeletedRemotely,
    /// Deleted on another device; deleted here too (kept in Recently deleted).
    TrashedLocally,
}

impl SyncedKind {
    /// The item is gone from where it was (here or on Homebase).
    pub fn removes(self) -> bool {
        matches!(
            self,
            SyncedKind::DeletedRemotely | SyncedKind::TrashedLocally
        )
    }

    /// A file's bytes went over.
    pub fn moves_bytes(self) -> bool {
        matches!(self, SyncedKind::Uploaded | SyncedKind::Downloaded)
    }
}

/// How far a sync is: its files (transfers, moves and deletes) and the bytes of its transfers,
/// done and to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Progress {
    pub files_done: u64,
    pub files_total: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

/// One item in Recently deleted (node schema): out of the tree, restorable until the purge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deleted {
    pub node: NodeId,
    /// The folder it was in.
    pub parent: NodeId,
    /// Where it was, `/`-separated, for people.
    pub path: String,
    pub kind: NodeKind,
    /// Files only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Unix ms.
    pub removed_at_ms: i64,
    /// The node whose delete took it along (itself, or a folder it was in): restoring that
    /// one restores this one.
    pub root: NodeId,
}
