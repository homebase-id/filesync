//! The remote side: one trait for a sync drive, modeled on Homebase (node schema v1).
//!
//! The trait is blocking on purpose. Transfers run on a bounded worker pool, the deterministic
//! simulator needs no async runtime, and the Homebase adapter can still use async HTTP
//! internally.
//!
//! Implementations: [`fake::FakeServer`] / [`fake::FakeRemote`] (in memory, Homebase
//! semantics) and, later, the Homebase adapter.

use std::io::{Read, Seek, Write};

use hbsync_types::schema::NodeContent;
use hbsync_types::{
    ContentHash, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, VersionTag,
};

pub mod contract;
pub mod fake;

/// One file header as the change feed or a header read returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteHeader {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
    /// `GroupId`: kept on tombstones.
    pub parent: NodeId,
    /// From `FileType`: kept on tombstones.
    pub kind: NodeKind,
    pub body: RemoteBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoteBody {
    Live {
        /// `UniqueId`.
        node: NodeId,
        content: NodeContent,
        /// Files only.
        payload: Option<PayloadInfo>,
    },
    /// A tombstone: `UniqueId`, content and payloads are gone.
    Deleted,
}

/// What the header says about the `sync_data` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadInfo {
    /// The content hash; `None` when the writer didn't send one (drive without the
    /// requirement): such a node is downloaded and hashed locally, and treated as unverified.
    pub hash: Option<ContentHash>,
    /// Stored size (equals the plaintext size for unencrypted payloads).
    pub bytes: u64,
}

impl RemoteHeader {
    pub fn node(&self) -> Option<NodeId> {
        match &self.body {
            RemoteBody::Live { node, .. } => Some(*node),
            RemoteBody::Deleted => None,
        }
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self.body, RemoteBody::Deleted)
    }

    /// The node state, if the header is live and complete (files need a hash).
    pub fn state(&self) -> Option<NodeState> {
        let RemoteBody::Live {
            content, payload, ..
        } = &self.body
        else {
            return None;
        };
        match self.kind {
            NodeKind::Folder => Some(NodeState::folder(self.parent, content.name.clone())),
            NodeKind::File => {
                let payload = payload.as_ref()?;
                Some(NodeState::file(
                    self.parent,
                    content.name.clone(),
                    FileContent {
                        hash: payload.hash?,
                        size: content.size.unwrap_or(payload.bytes),
                        mtime_ms: content.mtime.unwrap_or(0),
                        executable: content.x,
                    },
                ))
            }
        }
    }
}

/// One page of the change feed, oldest change first.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesPage {
    pub entries: Vec<RemoteHeader>,
    /// Opaque; persist it after the page is applied.
    pub cursor: String,
    pub has_more: bool,
}

/// A node's header as a client writes it, on create and on every update. Homebase replaces
/// the whole header on update, so an update sends all of it again, node id and kind included.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMeta {
    pub node: NodeId,
    pub parent: NodeId,
    pub kind: NodeKind,
    pub content: NodeContent,
}

/// Payload bytes to upload, with the hash they must have.
pub struct Upload<'a> {
    pub hash: ContentHash,
    pub size: u64,
    pub reader: &'a mut dyn Source,
    /// Paces the bytes sent (a bandwidth cap); not those a remote reads for itself.
    pub pace: Option<&'a dyn Pace>,
}

/// A bandwidth cap: blocks until `bytes` more may go.
pub trait Pace: Sync {
    fn take(&self, bytes: usize);
}

/// `inner`, paced by `pace` as it is read: what a remote sends.
pub struct Paced<'a, R> {
    pub inner: R,
    pub pace: Option<&'a dyn Pace>,
}

impl<R> std::fmt::Debug for Paced<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Paced")
            .field("paced", &self.pace.is_some())
            .finish_non_exhaustive()
    }
}

impl<R: Read> Read for Paced<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Small slices keep the pacing smooth.
        let max = buf.len().min(64 * 1024);
        let n = self.inner.read(&mut buf[..max])?;
        if let Some(pace) = self.pace {
            pace.take(n);
        }
        Ok(n)
    }
}

/// Where an upload's bytes come from. A remote may read it more than once: an encrypting one
/// reads it first to hash the ciphertext it will send.
pub trait Source: Read + Seek {}

impl<T: Read + Seek> Source for T {}

impl std::fmt::Debug for Upload<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("hash", &self.hash)
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// A live file with this node id exists (Homebase 4105): a retried create already landed.
    #[error("a node with this id already exists")]
    AlreadyExists,
    /// The identity refused the credentials or the drive (401/403): stop syncing until the
    /// user re-authorizes; retrying changes nothing.
    #[error("access denied: {0}")]
    Denied(String),
    /// The file changed since the given version (Homebase 4160).
    #[error("version mismatch")]
    VersionMismatch,
    #[error("not found")]
    NotFound,
    /// The received bytes don't match the declared hash (Homebase 4175).
    #[error("payload hash mismatch")]
    HashMismatch,
    /// Hash incomplete or malformed (Homebase 4176).
    #[error("invalid payload hash")]
    InvalidHash,
    /// The drive requires hashes (Homebase 4177).
    #[error("payload hash required")]
    HashRequired,
    /// Network trouble, timeouts, 5xx: retry with backoff.
    #[error("transient: {0}")]
    Transient(String),
    /// Reading the local upload source or writing the download sink failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Anything that won't get better by retrying (auth, bad request).
    #[error("fatal: {0}")]
    Fatal(String),
}

impl RemoteError {
    pub fn is_transient(&self) -> bool {
        matches!(self, RemoteError::Transient(_))
    }
}

pub type Result<T, E = RemoteError> = std::result::Result<T, E>;

/// A sync drive.
pub trait Remote: Send + Sync {
    /// Up to `limit` changes after `cursor` (`None` = from the beginning), oldest first.
    /// Includes tombstones.
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage>;

    /// The current header of a file, live or deleted.
    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader>;

    /// Creates a node. Fails with [`RemoteError::AlreadyExists`] if a live file already has
    /// this node id (a retry of a create that landed).
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef>;

    /// Rewrites the header (parent and content; node id and kind stay what they are) and, with
    /// a payload, replaces the bytes, if the file is still at `expected`.
    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef>;

    /// Deletes (tombstones) a file if it is still at `expected`.
    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()>;

    /// Streams the payload (or `start..end` of it) into `sink`. Returns the bytes written: fewer
    /// than asked when the range runs past the payload's end.
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64>;
}

/// A shared remote: the engine owns one handle, the caller keeps another (to read what the
/// implementation reports beyond the trait).
impl<T: Remote + ?Sized> Remote for std::sync::Arc<T> {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        (**self).changes(cursor, limit)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        (**self).header(file_id)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef> {
        (**self).create(node, payload)
    }

    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        (**self).update(file_id, expected, meta, payload)
    }

    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        (**self).delete(file_id, expected)
    }

    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        (**self).read(file_id, range, sink)
    }
}
