//! The node vocabulary shared by the state DB, the remote layer and the sync engine.
//! See `docs/spec/sync-semantics.md` and `docs/spec/node-schema-v1.md`.

use std::fmt;

pub use hbsync_fileio::{ContentHash, HashAlgorithm};
pub use uuid::Uuid;

pub mod activity;
pub mod schema;

/// A node's identity: a client-generated UUID, stable across rename and move. Stored remotely
/// as the Homebase `UniqueId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// The implicit root of every sync drive (node schema v1).
    pub const ROOT: NodeId = NodeId(Uuid::from_u128(0x2a1c0d55_8ad3_417e_a5d7_42c3d0baba0e));

    /// A fresh id. UUIDv7 keeps DB index inserts roughly sequential.
    pub fn new() -> Self {
        NodeId(Uuid::now_v7())
    }

    pub fn is_root(self) -> bool {
        self == Self::ROOT
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            f.write_str("NodeId(ROOT)")
        } else {
            write!(f, "NodeId({})", short(self.0))
        }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The server's id for one stored version lineage of a node (Homebase `fileId`). A node that is
/// deleted and re-created gets a new one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RemoteFileId(pub Uuid);

impl fmt::Debug for RemoteFileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RemoteFileId({})", short(self.0))
    }
}

/// Homebase `VersionTag`: changes on every header write. A concurrency token, not a content id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VersionTag(pub Uuid);

impl fmt::Debug for VersionTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VersionTag({})", short(self.0))
    }
}

/// Where a node lives on the server: its file, and the version a guarded write must name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RemoteRef {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
}

/// The last 8 hex digits: UUIDv7 ids share their leading timestamp digits.
fn short(id: Uuid) -> String {
    id.simple().to_string()[24..].to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeKind {
    File,
    Folder,
}

/// Whether a file's bytes are kept locally. Local-only state, never synced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ContentState {
    /// Bytes are local and may be evicted.
    #[default]
    Local,
    /// Bytes are always kept locally.
    Pinned,
    /// No local bytes; a missing local file is not a delete.
    OnlineOnly,
}

/// The synced attributes of a file's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileContent {
    pub hash: ContentHash,
    pub size: u64,
    /// Original local modification time, Unix ms.
    pub mtime_ms: i64,
    pub executable: bool,
}

impl FileContent {
    /// Equal for sync purposes: same bytes and executable bit. The mtime is informational.
    pub fn same_content(&self, other: &FileContent) -> bool {
        self.hash.bytes == other.hash.bytes
            && self.hash.algorithm == other.hash.algorithm
            && self.size == other.size
            && self.executable == other.executable
    }
}

/// One node's synced state: where it is, what it's called and (for files) what it holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeState {
    pub parent: NodeId,
    /// The remote name, Unicode NFC.
    pub name: String,
    pub kind: NodeKind,
    /// `Some` for files, `None` for folders.
    pub content: Option<FileContent>,
}

impl NodeState {
    pub fn folder(parent: NodeId, name: impl Into<String>) -> Self {
        NodeState {
            parent,
            name: name.into(),
            kind: NodeKind::Folder,
            content: None,
        }
    }

    pub fn file(parent: NodeId, name: impl Into<String>, content: FileContent) -> Self {
        NodeState {
            parent,
            name: name.into(),
            kind: NodeKind::File,
            content: Some(content),
        }
    }

    /// Same location (parent and name).
    pub fn same_place(&self, other: &NodeState) -> bool {
        self.parent == other.parent && self.name == other.name
    }

    /// Same content, ignoring location. Folders always match.
    pub fn same_content(&self, other: &NodeState) -> bool {
        match (&self.content, &other.content) {
            (Some(a), Some(b)) => a.same_content(b),
            (None, None) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_id_matches_spec() {
        assert_eq!(
            NodeId::ROOT.0.to_string(),
            "2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e"
        );
    }

    #[test]
    fn new_ids_are_unique_and_not_root() {
        let (a, b) = (NodeId::new(), NodeId::new());
        assert_ne!(a, b);
        assert!(!a.is_root());
    }
}
