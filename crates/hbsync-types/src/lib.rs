//! The node vocabulary shared by the state DB, the remote layer and the sync engine.
//! See `docs/spec/sync-semantics.md` and `docs/spec/node-schema-v1.md`.

use std::fmt;

pub use class::ErrorClass;
pub use hash::{ContentHash, HashAlgorithm, HashParseError};
pub use hbsync_platform::tags::{Tag, Tags};
pub use role::Role;
pub use uuid::Uuid;

pub mod activity;
mod class;
mod hash;
mod role;
pub mod schema;

/// The name [`NodeId::RECOVERED`] gets when a device makes it; the user may rename it.
pub const RECOVERED_NAME: &str = "Recovered files";

/// Now, in Unix milliseconds.
pub fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A removal's time for comparing (Unix ms, the server's): an open one (this device's own, its
/// time not back from the server yet) is the newest of all.
pub fn removal_time(removed_at_ms: Option<i64>) -> i64 {
    removed_at_ms.unwrap_or(i64::MAX)
}

/// Now, in Unix seconds.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A node's identity: a client-generated UUID, stable across rename and move. Stored remotely
/// as the Homebase `UniqueId`.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// The implicit root of every sync drive (node schema v1).
    pub const ROOT: NodeId = NodeId(Uuid::from_u128(0x2a1c0d55_8ad3_417e_a5d7_42c3d0baba0e));

    /// Never on a drive: in a folder holding part of the drive (a role folder), the rest of
    /// the drive, seen as one folder at its top that the folder leaves out.
    pub const OUTSIDE: NodeId = NodeId(Uuid::from_u128(0x5c1e7f0a_3b2d_4e8f_9a61_0d4c2b7e9f13));

    /// The folder at the drive's top that holds what lost its folder for good (node schema,
    /// "Recovered files"), named [`RECOVERED_NAME`] when made. Fixed, the same on every drive,
    /// so devices that adopt at once share one.
    pub const RECOVERED: NodeId = NodeId(Uuid::from_u128(0xfa4646cb_2df0_4f18_a3c6_1fc2b2f6ac4b));

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

/// A file a delete may name (`Remote::delete`, `Remote::delete_for_good`), at its version: in
/// the product, one this device recorded in Recently deleted (`RemovedRow::doomed`) or as a
/// tombstone (`ReadTx::tombstones_before`). The type marks the rule and lint keeps it: the
/// engine's and the app's `clippy.toml` disallow [`Doomed::unchecked`], so they get one only
/// from the store. It is no proof against a `RemovedRow` made by hand, and the server's version
/// check still guards every delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Doomed(RemoteRef);

impl Doomed {
    /// Any file, unchecked, live ones too: for the store, which makes one from what it
    /// recorded, and for tools and tests that delete what they wrote themselves
    /// (`hbsync-clean`).
    pub fn unchecked(at: RemoteRef) -> Self {
        Doomed(at)
    }

    /// The file and the version the delete names.
    pub fn at(self) -> RemoteRef {
        self.0
    }
}

/// Opaque bytes a remote attaches to a file so it can write it again without reading it first
/// (Homebase: the file's key header as the server hands it out, sealed with the app's shared
/// secret). It belongs to one file: never written with another.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FileToken(pub Box<[u8]>);

impl fmt::Debug for FileToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileToken(..)")
    }
}

/// A file to write: where it is, the version a guarded write names, and its token if known.
/// A create or update returns the file as written: the target of the next write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub at: RemoteRef,
    pub token: Option<FileToken>,
    /// The server's time of this version (Unix ms, Homebase's `updated`, what the feed shows for
    /// it), when the write's answer gave it; not known in a target the client makes to write to.
    pub changed_at: Option<i64>,
}

/// The last 8 hex digits: UUIDv7 ids share their leading timestamp digits.
fn short(id: Uuid) -> String {
    id.simple().to_string()[24..].to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// Finder tags (files and folders): synced, merged three-way (sync-semantics §11).
    pub tags: Tags,
}

impl NodeState {
    pub fn folder(parent: NodeId, name: impl Into<String>) -> Self {
        NodeState {
            parent,
            name: name.into(),
            kind: NodeKind::Folder,
            content: None,
            tags: Tags::default(),
        }
    }

    pub fn file(parent: NodeId, name: impl Into<String>, content: FileContent) -> Self {
        NodeState {
            parent,
            name: name.into(),
            kind: NodeKind::File,
            content: Some(content),
            tags: Tags::default(),
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

    /// On every drive: the node schema names it, and other clients know the folder by it.
    #[test]
    fn recovered_files_id_matches_spec() {
        assert_eq!(
            NodeId::RECOVERED.0.to_string(),
            "fa4646cb-2df0-4f18-a3c6-1fc2b2f6ac4b"
        );
        assert!(Role::of(NodeId::RECOVERED).is_none());
        assert!(![NodeId::ROOT, NodeId::OUTSIDE].contains(&NodeId::RECOVERED));
    }

    #[test]
    fn new_ids_are_unique_and_not_root() {
        let (a, b) = (NodeId::new(), NodeId::new());
        assert_ne!(a, b);
        assert!(!a.is_root());
    }
}
