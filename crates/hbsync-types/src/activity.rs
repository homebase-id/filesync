//! What was synced, for people to see: the "recently synced" list.

use serde::{Deserialize, Serialize};

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
    /// Deleted on another device; moved to this computer's trash.
    TrashedLocally,
}
