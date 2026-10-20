//! The remote side: one trait for a sync drive, modeled on Homebase (node schema v1).
//!
//! The trait is blocking on purpose. Transfers run on a bounded worker pool, the deterministic
//! simulator needs no async runtime, and the Homebase adapter can still use async HTTP
//! internally.
//!
//! Implementations: `fake::FakeServer` / `fake::FakeRemote` (in memory, Homebase semantics;
//! feature `testkit`, with the `contract` every remote passes) and the Homebase drive
//! (`hbsync-homebase`).

use std::collections::HashSet;
use std::io::{Read, Write};

pub use hbsync_types::ErrorClass;
use hbsync_types::schema::NodeContent;
use hbsync_types::{
    ContentHash, FileContent, FileToken, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef,
    Target, VersionTag,
};

#[cfg(feature = "testkit")]
pub mod contract;
#[cfg(feature = "testkit")]
pub mod fake;

/// The cursor at the end of the feed, read on from `from` (`None`: the beginning) in slim pages.
pub fn feed_end(remote: &dyn Remote, from: Option<&str>) -> Result<String, RemoteError> {
    let mut page = remote.change_refs(from, 1000)?;
    while page.has_more {
        page = remote.change_refs(Some(&page.cursor), 1000)?;
    }
    Ok(page.cursor)
}

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
    /// decides whether a node in a removed folder went with it (node schema, Recently deleted),
    /// and, for a tombstone, when it may be deleted hard. `None` where the remote doesn't say.
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
    /// This file at this version.
    pub fn at(&self) -> RemoteRef {
        RemoteRef {
            file_id: self.file_id,
            version: self.version,
        }
    }

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
    /// The remote's clock when it answered, Unix ms (Homebase's `queryTime`): the times of
    /// [`RemoteHeader`] are this clock's.
    pub server_time: i64,
}

/// One row of the change feed as a slim page carries it: which version of which file changed,
/// without the header (Homebase's minimal `query-batch`, odin-core #1967).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeRef {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
    /// A tombstone; without [`ChangeRef::changed_at`], a delete for good that left none (a
    /// hard delete, as a notification names it).
    pub deleted: bool,
    /// As in [`RemoteHeader`]; `None` for a tombstone.
    pub parent: Option<NodeId>,
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
            parent: (!header.is_deleted()).then_some(header.parent),
            removed_at: header.removed_at,
            changed_at: header.changed_at,
        }
    }

    /// The same file deleted for good: what a hard delete leaves (only the ref of what it was).
    pub fn tombstone(self) -> ChangeRef {
        ChangeRef {
            deleted: true,
            parent: None,
            removed_at: None,
            changed_at: None,
            ..self
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
    /// As in [`ChangesPage`].
    pub server_time: i64,
    /// Rows that couldn't be read slim: the page is read whole, which says what they are.
    pub unread: usize,
}

impl RefsPage {
    /// A whole page as refs: for a remote that has only whole pages (a test double).
    pub fn of(page: &ChangesPage) -> RefsPage {
        RefsPage {
            refs: page.entries.iter().map(ChangeRef::of).collect(),
            cursor: page.cursor.clone(),
            has_more: page.has_more,
            server_time: page.server_time,
            unread: 0,
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
    /// The file is a tombstone: deleted for good, leaving a trace (Homebase 4002,
    /// `cannotUpdateNonActiveFile`, to a write). Gone, as a read of its header would say: no need
    /// to read it.
    #[error("deleted for good")]
    Deleted,
    /// A payload range past its end (HTTP 416): the file changed since its header was read.
    /// Read the header again, then retry; it says nothing about the file being gone.
    #[error("range not satisfiable: {0}")]
    StaleRange(String),
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

/// What a write of many files answered, file by file ([`Remote::delete`],
/// [`Remote::delete_for_good`], [`Remote::set_removed`]).
#[derive(Debug)]
pub struct Answers<T> {
    /// The drive's answer for each of the first files asked, in order (all of them unless
    /// `stopped`): [`RemoteError::VersionMismatch`] for one written since (left as it is),
    /// [`RemoteError::NotFound`] for one not there, [`RemoteError::Deleted`] for a tombstone
    /// (where the remote tells them apart).
    pub answers: Vec<Result<T>>,
    /// The error of the request that stopped it after those: the files left weren't answered,
    /// and weren't written.
    pub stopped: Option<RemoteError>,
}

/// Files a batch write takes per request (Homebase's `soft-delete-batch`, `hard-delete-batch`,
/// `update-archival-status-batch`: odin-core `FileVersionTagBatchV2.MaxFiles`).
pub const BATCH_FILES: usize = 1000;

/// What a delete answered: for each file deleted, its tombstone's time (Unix ms, the server's,
/// as the feed shows it), if the remote said.
pub type Deletes = Answers<Option<i64>>;

/// What a delete for good answered.
pub type ForGood = Answers<()>;

impl<T> Default for Answers<T> {
    fn default() -> Self {
        Answers {
            answers: Vec::new(),
            stopped: None,
        }
    }
}

impl<T> Answers<T> {
    /// Every file answered.
    pub fn all(answers: Vec<Result<T>>) -> Self {
        Answers {
            answers,
            stopped: None,
        }
    }

    /// Stopped by `e` before any file was answered (the call's own error, as callers that take
    /// what was answered before it see it).
    pub fn stopped(e: RemoteError) -> Self {
        Answers {
            answers: Vec::new(),
            stopped: Some(e),
        }
    }

    /// An answer for each of `asked` files, in order, and what stopped the call, if anything
    /// did: a file it left unanswered fails as it did (it may have been written: an answer
    /// lost). A call that answered fewer without saying why counts as stopped.
    pub fn padded(self, asked: usize) -> (Vec<Result<T>>, Option<RemoteError>) {
        let Answers {
            mut answers,
            stopped,
        } = self;
        let stopped = stopped.or_else(|| {
            (answers.len() < asked)
                .then(|| RemoteError::Fatal("the drive didn't answer every file".into()))
        });
        if let Some(e) = &stopped {
            answers.resize_with(asked.max(answers.len()), || Err(e.again()));
        }
        (answers, stopped)
    }

    /// The answers, if every file was answered; else what stopped it.
    pub fn into_answers(self) -> Result<Vec<Result<T>>> {
        match self.stopped {
            Some(e) => Err(e),
            None => Ok(self.answers),
        }
    }

    /// The answer for the one file asked: the call's error, else that file's.
    pub fn single(self) -> Result<T> {
        let answers = self.into_answers()?;
        let count = answers.len();
        match <[_; 1]>::try_from(answers) {
            Ok([answer]) => answer,
            Err(_) => Err(RemoteError::Fatal(format!("{count} answers for one file"))),
        }
    }

    /// `files` sent `size` at a time ([`BATCH_FILES`] a request on Homebase): each one's
    /// answer. A request that fails after earlier ones went keeps their answers and stops
    /// there: the files left weren't answered (nor written). One that fails first is the whole
    /// call's error.
    pub fn in_batches<F>(
        files: &[F],
        size: usize,
        mut send: impl FnMut(&[F]) -> Result<Vec<Result<T>>>,
    ) -> Result<Self> {
        let mut done = Answers::default();
        for batch in files.chunks(size) {
            match send(batch) {
                Ok(answers) => done.answers.extend(answers),
                Err(e) if done.answers.is_empty() => return Err(e),
                Err(e) => {
                    done.stopped = Some(e);
                    break;
                }
            }
        }
        Ok(done)
    }
}

impl RemoteError {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            RemoteError::Transient(_) => ErrorClass::Transient,
            RemoteError::Denied(_) => ErrorClass::Denied,
            RemoteError::Full(_) => ErrorClass::Full,
            // The remote moved on, or rejected the bytes (changed mid-upload), or the file grew
            // shorter than its header said (a read past its end).
            RemoteError::AlreadyExists
            | RemoteError::VersionMismatch
            | RemoteError::StaleRange(_)
            | RemoteError::HashMismatch => ErrorClass::Stale,
            RemoteError::NotFound | RemoteError::Deleted => ErrorClass::Gone,
            // The drive won't take it as it is: retrying the same won't help.
            RemoteError::Fatal(_) | RemoteError::InvalidHash | RemoteError::HashRequired => {
                ErrorClass::Refused
            }
            // The local source or sink failed.
            RemoteError::Io(_) => ErrorClass::LocalFault,
        }
    }

    /// Whether it is [`ErrorClass::Transient`].
    pub fn is_transient(&self) -> bool {
        self.class() == ErrorClass::Transient
    }

    /// The same error once more, for another file of a request it ended.
    pub fn again(&self) -> RemoteError {
        match self {
            RemoteError::AlreadyExists => RemoteError::AlreadyExists,
            RemoteError::Denied(why) => RemoteError::Denied(why.clone()),
            RemoteError::VersionMismatch => RemoteError::VersionMismatch,
            RemoteError::NotFound => RemoteError::NotFound,
            RemoteError::Deleted => RemoteError::Deleted,
            RemoteError::StaleRange(why) => RemoteError::StaleRange(why.clone()),
            RemoteError::HashMismatch => RemoteError::HashMismatch,
            RemoteError::InvalidHash => RemoteError::InvalidHash,
            RemoteError::HashRequired => RemoteError::HashRequired,
            RemoteError::Transient(why) => RemoteError::Transient(why.clone()),
            RemoteError::Io(e) => RemoteError::Io(std::io::Error::new(e.kind(), e.to_string())),
            RemoteError::Full(why) => RemoteError::Full(why.clone()),
            RemoteError::Fatal(why) => RemoteError::Fatal(why.clone()),
        }
    }
}

pub type Result<T, E = RemoteError> = std::result::Result<T, E>;

/// What [`Remote::holding`] counts in a folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holds<'a> {
    /// A live file: not deleted, not removed.
    Live,
    /// Any file not deleted, live or removed (in Recently deleted), but these, each at its
    /// version: one written since counts. A tombstone doesn't count.
    AnyBut(&'a HashSet<RemoteRef>),
}

impl Holds<'_> {
    /// Whether a file not deleted counts: `at` its file and version, `removed` whether it is.
    pub fn counts(&self, at: RemoteRef, removed: bool) -> bool {
        match self {
            Holds::Live => !removed,
            Holds::AnyBut(but) => !but.contains(&at),
        }
    }
}

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

    /// The time of the last change a cursor of [`Remote::changes`] stands after, Unix ms by the
    /// remote's clock, if the cursor says: how long ago the feed was last read to its end.
    fn cursor_time(&self, cursor: &str) -> Option<i64>;

    /// The current header of a file, live or deleted.
    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader>;

    /// Which of `folders` hold a file of the kind `holds` names, whatever it is: also one this
    /// client can't read as a node, which isn't in its mirror. Asked before folders are deleted
    /// for good, and after the purge removed live folders; a file written in the last
    /// [`Remote::hold_back`] may be missing.
    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>>;

    /// How long after a write the remote's queries may still leave it out (Homebase's feed
    /// holds back its last second): a delete that relies on a query asks it that long after
    /// the moment it must see up to.
    fn hold_back(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

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

    /// Deletes (tombstones) files, each only if still at its version: each stays in the feed as
    /// a tombstone, which is how a device learns of the delete. What the drive answered for
    /// each, in order ([`Deletes`]): a file already a tombstone (a retry, another device's
    /// delete) answers as deleted, its version not looked at, with its tombstone's own time. An
    /// outer error stopped it before any file was answered.
    fn delete(&self, files: &[RemoteRef]) -> Result<Deletes>;

    /// Deletes files for good, leaving nothing in the feed (Homebase's hard delete), live ones
    /// and tombstones, each only if still at its version: what the drive answered for each
    /// ([`ForGood`]). An outer error stopped it before any file was answered.
    fn delete_for_good(&self, files: &[RemoteRef]) -> Result<ForGood>;

    /// Moves files to Recently deleted (`removed`), or back, each only if still at its version,
    /// leaving the rest of its header as it is: what the drive answered for each, in order, the
    /// file as written (its token kept) with the server's time of the write. A file already so
    /// is left as it is, its version and time too. An outer error stopped it before any file
    /// was answered.
    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Target>>;

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

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        (**self).cursor_time(cursor)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        (**self).header(file_id)
    }

    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        (**self).holding(folders, holds)
    }

    fn hold_back(&self) -> std::time::Duration {
        (**self).hold_back()
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

    fn delete(&self, files: &[RemoteRef]) -> Result<Deletes> {
        (**self).delete(files)
    }

    fn delete_for_good(&self, files: &[RemoteRef]) -> Result<ForGood> {
        (**self).delete_for_good(files)
    }

    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Target>> {
        (**self).set_removed(files, removed)
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
