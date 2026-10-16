//! The remote side: one trait for a sync drive, modeled on Homebase (node schema v1).
//!
//! The trait is blocking on purpose. Transfers run on a bounded worker pool, the deterministic
//! simulator needs no async runtime, and the Homebase adapter can still use async HTTP
//! internally.
//!
//! Implementations: [`fake::FakeServer`] / [`fake::FakeRemote`] (in memory, Homebase
//! semantics) and, later, the Homebase adapter.

use std::io::{Read, Write};

use hbsync_types::schema::NodeContent;
use hbsync_types::{
    ContentHash, FileContent, FileToken, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef,
    Target, VersionTag,
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
    /// A live node in Recently deleted (`archivalStatus` 2 on Homebase): when it went there,
    /// Unix ms, the server's time of the write that removed it (Homebase's `updated`).
    pub removed_at: Option<i64>,
    /// When this version was written, Unix ms, the server's time (Homebase's `updated`): what
    /// decides whether a node in a removed folder went with it (node schema, Recently deleted).
    /// `None` where the remote doesn't say (a tombstone).
    pub changed_at: Option<i64>,
    /// The file's [`FileToken`], where the remote has one (a tombstone has none).
    pub token: Option<FileToken>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoteBody {
    /// A node, in the tree or, with [`RemoteHeader::removed_at`], in Recently deleted.
    Live {
        /// `UniqueId`.
        node: NodeId,
        content: NodeContent,
        /// Every file has one; folders have none.
        payload: Option<PayloadInfo>,
    },
    /// A tombstone: `UniqueId`, content and payloads are gone.
    Deleted,
}

/// What the header says about the `sync_data` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadInfo {
    /// The content hash. Every file has one: a row without it isn't a node.
    pub hash: ContentHash,
    /// Stored size: padded on Homebase, where payloads are encrypted; the plaintext size on
    /// the fake.
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

    /// The node state, if the header is in the tree (and, for a file, has its payload).
    pub fn state(&self) -> Option<NodeState> {
        let RemoteBody::Live {
            content, payload, ..
        } = &self.body
        else {
            return None;
        };
        if self.removed_at.is_some() {
            return None;
        }
        let state = match self.kind {
            NodeKind::Folder => NodeState::folder(self.parent, content.name.clone()),
            NodeKind::File => {
                let payload = payload.as_ref()?;
                NodeState::file(
                    self.parent,
                    content.name.clone(),
                    FileContent {
                        hash: payload.hash,
                        size: content.size.unwrap_or(payload.bytes),
                        mtime_ms: content.mtime.unwrap_or(0),
                        executable: content.x,
                    },
                )
            }
        };
        Some(NodeState {
            tags: content.tags(),
            ..state
        })
    }
}

/// One page of the change feed, oldest change first.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesPage {
    pub entries: Vec<RemoteHeader>,
    /// Opaque; persist it after the page is applied.
    pub cursor: String,
    pub has_more: bool,
    /// Rows left out because they can't be read as nodes (another app's file, one that doesn't
    /// decrypt), each with why.
    pub skipped: Vec<String>,
}

/// One row of the change feed as a slim page carries it: which version of which file changed,
/// without the header (Homebase's minimal `query-batch`, odin-core #1967).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeRef {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
    /// A tombstone.
    pub deleted: bool,
    /// As in [`RemoteHeader`].
    pub removed_at: Option<i64>,
    /// As in [`RemoteHeader`].
    pub changed_at: Option<i64>,
}

impl ChangeRef {
    pub fn of(header: &RemoteHeader) -> ChangeRef {
        ChangeRef {
            file_id: header.file_id,
            version: header.version,
            deleted: header.is_deleted(),
            removed_at: header.removed_at,
            changed_at: header.changed_at,
        }
    }

    pub fn at(&self) -> RemoteRef {
        RemoteRef {
            file_id: self.file_id,
            version: self.version,
        }
    }
}

/// One page of the change feed in [`ChangeRef`]s: the same rows and cursor as
/// [`Remote::changes`] gives for the same cursor and limit.
#[derive(Debug, Clone, PartialEq)]
pub struct RefsPage {
    pub refs: Vec<ChangeRef>,
    pub cursor: String,
    pub has_more: bool,
}

impl RefsPage {
    /// A whole page as refs: for a remote that has only whole pages (a test double).
    pub fn of(page: &ChangesPage) -> RefsPage {
        RefsPage {
            refs: page.entries.iter().map(ChangeRef::of).collect(),
            cursor: page.cursor.clone(),
            has_more: page.has_more,
        }
    }
}

/// A node's header as a client writes it, on create and on every update. Homebase replaces
/// the whole header on update, so an update sends all of it again, node id and kind included.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMeta {
    pub node: NodeId,
    pub parent: NodeId,
    pub kind: NodeKind,
    pub content: NodeContent,
    /// In Recently deleted (`archivalStatus` 2): the server stamps when.
    pub removed: bool,
}

/// Payload bytes to upload, with the hash they must have.
pub struct Upload<'a> {
    pub hash: ContentHash,
    pub size: u64,
    /// Read once, by the remote that sends it.
    pub reader: &'a mut dyn Read,
    /// Paces the bytes sent (a bandwidth cap).
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
    /// The bytes don't match the declared hash: the source changed since it was hashed, or
    /// the server received other bytes (Homebase 4175, 4178).
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
    /// The identity is out of storage (HTTP 507): nothing more goes up until there is room.
    #[error("out of storage: {0}")]
    Full(String),
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

    /// The same page as [`Remote::changes`], each row only as a [`ChangeRef`]: what the feed
    /// compares with what it has before reading a page whole (`hbsync_engine::feed::pull`). A
    /// row the remote can't read as a node is in it too (unknown: the page is read whole and
    /// skips it).
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage>;

    /// The current header of a file, live or deleted.
    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader>;

    /// Creates a node. Fails with [`RemoteError::AlreadyExists`] if a live file already has
    /// this node id (a retry of a create that landed). Returns the file as written, with the
    /// token its header will give for this version.
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Target>;

    /// Rewrites the header (parent, content and removal; node id and kind stay what they are)
    /// and, with a payload, replaces the bytes, if the file is still at `target`'s version. Its
    /// token, if known, saves reading the file first. Returns the file as written, as
    /// [`Remote::create`] does.
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Target>;

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

    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        (**self).change_refs(cursor, limit)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        (**self).header(file_id)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Target> {
        (**self).create(node, payload)
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Target> {
        (**self).update(target, meta, payload)
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
