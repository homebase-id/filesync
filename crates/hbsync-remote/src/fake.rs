//! An in-memory sync drive with the Homebase semantics the engine depends on.
//!
//! [`FakeServer`] models the server (verified in odin-core, see
//! `docs/spec/homebase-api-notes.md`):
//! - creating a live file with an existing `UniqueId` fails (4105); tombstones don't count;
//! - updates are `VersionTag`-guarded (4160); every write gets a new tag;
//! - a delete tombstones the file: `UniqueId`, content and payload are dropped, `fileId`,
//!   `GroupId` and `FileType` stay; deletes are `VersionTag`-guarded too;
//! - the change feed is `anyChangeDate` + `oldestFirst`, resumes after the last row seen, and
//!   only returns rows modified strictly before "now": a row written in the current
//!   millisecond is held back, the page saying more rows follow (odin-core #1906), and shows
//!   up once the clock moves on, as it has by a client's next read ([`FakeRemote`]);
//! - payload hashes: the stored hash is checked over the received bytes (4175), unencrypted
//!   payloads need `contentHash == storedHash` (here: one hash), and a drive can require
//!   hashes (4177);
//! - notifications: every write is announced to the subscribers as its file's row
//!   ([`FakeServer::subscribe`]), except to those that exclude their own client's writes
//!   (`excludeOwnWrites`, odin-core #1971): each [`FakeRemote::new`] is a client registration
//!   (a token) of its own. A write not made through a client (a test's, on the server) is
//!   announced to all, as Homebase does when it can't tell whose write it is.
//!
//! [`FakeRemote`] is a client of it implementing [`Remote`] the way the Homebase adapter does:
//! a delete refused for its version (4160) is read once more and counts as done when the file
//! is deleted (a retry). Faults and latency are injected per client.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use hbsync_fileio::hash_bytes;
use hbsync_types::schema::NodeContent;
use hbsync_types::{
    Doomed, FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef, Target, Uuid, VersionTag, Written,
};

use crate::{
    Answers, BATCH_FILES, ChangeRef, ChangesPage, Deletes, ForGood, Holds, NodeMeta, PayloadInfo,
    RefsPage, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Skipped, Upload,
};

#[derive(Debug, Clone)]
struct StoredFile {
    header: RemoteHeader,
    /// `None` for folders and tombstones.
    bytes: Option<Arc<Vec<u8>>>,
    /// Feed position: (modified time, row id). The row id never changes.
    modified: i64,
    row: u64,
}

/// Server-wide settings.
#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    /// The drive's `requirePayloadHashes` setting.
    pub require_hashes: bool,
    /// Advance the clock by 1 ms after every write, so a write is visible in the feed right
    /// after the next write. With `false`, tests call [`FakeServer::tick`].
    pub auto_tick: bool,
    /// The payload bytes the drive holds at most: an upload past it is refused
    /// ([`RemoteError::Full`], Homebase's 507).
    pub quota: Option<u64>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            require_hashes: true,
            auto_tick: true,
            quota: None,
        }
    }
}

/// A client registration: whose token a write was made with. Each [`FakeRemote::new`] is one;
/// its clones share it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

/// A notification subscription: what it is told, and whose writes it leaves out.
#[derive(Debug)]
struct Subscriber {
    events: Sender<ChangeRef>,
    excluded: Option<ClientId>,
}

/// The shared in-memory drive.
#[derive(Debug)]
pub struct FakeServer {
    config: ServerConfig,
    files: HashMap<RemoteFileId, StoredFile>,
    /// Live files only, removed ones included: their node id stays taken, as on Homebase.
    by_node: HashMap<NodeId, RemoteFileId>,
    /// Feed index: (modified, row) → file.
    feed: BTreeMap<(i64, u64), RemoteFileId>,
    clock_ms: i64,
    /// Files the feed leaves out though they exist (odin-core leaves out a row with a later
    /// `modified` than its holdback, or one the caller may not see), for tests.
    unlisted: std::collections::HashSet<RemoteFileId>,
    /// Files whose rows don't read as nodes (another writer's newer version, say): the feed
    /// skips them ([`ChangesPage::skipped`]) and their header doesn't read, for tests.
    unreadable: std::collections::HashSet<RemoteFileId>,
    next_row: u64,
    /// Deterministic id source.
    next_id: u64,
    subscribers: Vec<Subscriber>,
    next_client: u64,
}

impl FakeServer {
    pub fn new(config: ServerConfig) -> Arc<Mutex<FakeServer>> {
        Arc::new(Mutex::new(FakeServer {
            config,
            files: HashMap::new(),
            by_node: HashMap::new(),
            feed: BTreeMap::new(),
            // A server's clock: removal times are compared with the purge's cut-off.
            clock_ms: hbsync_types::unix_now_ms(),
            unlisted: Default::default(),
            unreadable: Default::default(),
            next_row: 1,
            next_id: 1,
            subscribers: Vec::new(),
            next_client: 1,
        }))
    }

    /// Sets [`ServerConfig::quota`].
    pub fn set_quota(&mut self, quota: Option<u64>) {
        self.config.quota = quota;
    }

    /// Advances the server clock, making writes from the previous millisecond visible.
    pub fn tick(&mut self) {
        self.clock_ms += 1;
    }

    fn fresh_uuid(&mut self) -> Uuid {
        self.next_id += 1;
        // Deterministic, but with the version and variant bits of a v4 UUID so it looks like
        // one. The counter goes in the low 62 bits, below the variant bits, so no two ids meet.
        Uuid::from_u128(u128::from(self.next_id) | 0x0000_0000_0000_4000_8000_0000_0000_0000)
    }

    /// A write of `file_id` by `writer` (`None`: not through a client) landed: its new place
    /// in the feed, and its announcement.
    fn touch(&mut self, file_id: RemoteFileId, writer: Option<ClientId>) {
        let modified = self.clock_ms;
        if let Some(f) = self.files.get_mut(&file_id) {
            self.feed.remove(&(f.modified, f.row));
            f.modified = modified;
            self.feed.insert((modified, f.row), file_id);
            let change = ChangeRef::of(&f.header);
            // A subscriber that went away is dropped.
            self.subscribers.retain(|s| {
                (writer.is_some() && s.excluded == writer) || s.events.send(change).is_ok()
            });
        }
        if self.config.auto_tick {
            self.tick();
        }
    }

    /// A new client registration.
    fn new_client(&mut self) -> ClientId {
        self.next_client += 1;
        ClientId(self.next_client)
    }

    /// Subscribes to the drive's notifications: every write from now on, as its file's row,
    /// but those of `excluded` (a client's own writes, with `excludeOwnWrites`).
    pub fn subscribe(&mut self, excluded: Option<ClientId>) -> Receiver<ChangeRef> {
        let (events, rx) = channel();
        self.subscribers.push(Subscriber { events, excluded });
        rx
    }

    /// Reads an upload, checking its hash.
    fn receive(
        &self,
        kind: NodeKind,
        upload: Option<Upload<'_>>,
    ) -> Result<Option<(Arc<Vec<u8>>, PayloadInfo)>> {
        match (kind, upload) {
            (NodeKind::Folder, None) => Ok(None),
            (NodeKind::Folder, Some(_)) => {
                Err(RemoteError::Fatal("folders have no payload".into()))
            }
            (NodeKind::File, None) => {
                if self.config.require_hashes {
                    Err(RemoteError::HashRequired)
                } else {
                    Err(RemoteError::Fatal("a new file needs a payload".into()))
                }
            }
            (NodeKind::File, Some(upload)) => {
                let mut bytes = Vec::with_capacity(upload.size as usize);
                upload.reader.read_to_end(&mut bytes)?;
                if hash_bytes(&bytes, upload.hash.algorithm) != upload.hash {
                    return Err(RemoteError::HashMismatch);
                }
                if let Some(quota) = self.config.quota {
                    let held: u64 = self
                        .files
                        .values()
                        .filter_map(|f| f.bytes.as_ref())
                        .map(|b| b.len() as u64)
                        .sum();
                    if held + bytes.len() as u64 > quota {
                        return Err(RemoteError::Full {
                            why: format!("{quota} bytes at most"),
                            retry_after: None,
                        });
                    }
                }
                let info = PayloadInfo {
                    hash: upload.hash,
                    bytes: bytes.len() as u64,
                };
                Ok(Some((Arc::new(bytes), info)))
            }
        }
    }

    // ---- server operations (the HTTP API) ----

    pub fn create(&mut self, node: &NodeMeta, upload: Option<Upload<'_>>) -> Result<RemoteRef> {
        self.create_by(None, node, upload).map(|w| w.at())
    }

    /// A create by `writer`, answered as Homebase answers it: the file as written, with its
    /// token and the write's time (`updated`, odin-core #1971).
    fn create_by(
        &mut self,
        writer: Option<ClientId>,
        node: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<Written> {
        if self.by_node.contains_key(&node.node) {
            return Err(RemoteError::AlreadyExists);
        }
        let payload = self.receive(node.kind, upload)?;
        let file_id = RemoteFileId(self.fresh_uuid());
        let version = VersionTag(self.fresh_uuid());
        let row = self.next_row;
        self.next_row += 1;
        let header = RemoteHeader {
            file_id,
            version,
            parent: node.parent,
            kind: node.kind,
            body: RemoteBody::Live {
                node: node.node,
                content: node.content.clone(),
                payload: payload.as_ref().map(|(_, info)| *info),
            },
            removed: node.removed,
            changed_at: Some(self.clock_ms),
            token: Some(token_of(file_id)),
        };
        self.files.insert(
            file_id,
            StoredFile {
                header,
                bytes: payload.map(|(b, _)| b),
                modified: 0,
                row,
            },
        );
        self.feed.insert((0, row), file_id);
        self.by_node.insert(node.node, file_id);
        let at = self.clock_ms;
        self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, at))
    }

    pub fn update(
        &mut self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        self.update_by(None, file_id, expected, meta, upload)
            .map(|w| w.at())
    }

    /// An update by `writer`, answered as [`FakeServer::create_by`] is.
    fn update_by(
        &mut self,
        writer: Option<ClientId>,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<Written> {
        let current = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        // As Homebase (`AssertActiveForUpdate`): a tombstone answers so before the version is
        // looked at.
        let RemoteBody::Live {
            node,
            payload: old_payload,
            ..
        } = &current.header.body
        else {
            return Err(RemoteError::Deleted);
        };
        if current.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        let (node, old_payload, kind) = (*node, *old_payload, current.header.kind);
        // Homebase replaces the whole header: a client that sent another node id or kind
        // would change (or clear) them. Ours never may.
        if meta.node != node || meta.kind != kind {
            return Err(RemoteError::Fatal(format!(
                "update would change the node {node} ({kind:?}) to {} ({:?})",
                meta.node, meta.kind
            )));
        }
        // Without an upload the payload stays.
        let new_payload = match upload {
            Some(upload) => self.receive(kind, Some(upload))?,
            None => None,
        };
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.parent = meta.parent;
        f.header.removed = meta.removed;
        f.header.changed_at = Some(self.clock_ms);
        let payload = match new_payload {
            Some((bytes, info)) => {
                f.bytes = Some(bytes);
                Some(info)
            }
            None => old_payload,
        };
        f.header.body = RemoteBody::Live {
            node,
            content: meta.content.clone(),
            payload,
        };
        let at = self.clock_ms;
        self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, at))
    }

    /// Deletes a file if it is still at `expected` (4160 otherwise), as Homebase's
    /// `soft-delete-batch` does for each file: a tombstone is left as it is, whatever the
    /// version asked (a retry, another device's delete). Returns the tombstone's time.
    pub fn delete(&mut self, file_id: RemoteFileId, expected: VersionTag) -> Result<Option<i64>> {
        self.delete_by(None, file_id, expected)
    }

    fn delete_by(
        &mut self,
        writer: Option<ClientId>,
        file_id: RemoteFileId,
        expected: VersionTag,
    ) -> Result<Option<i64>> {
        let f = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        if f.header.body == RemoteBody::Deleted {
            return Ok(f.header.changed_at);
        }
        if f.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        if let RemoteBody::Live { node, .. } = f.header.body {
            self.by_node.remove(&node);
        }
        let version = VersionTag(self.fresh_uuid());
        let now = self.clock_ms;
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.body = RemoteBody::Deleted;
        f.header.removed = false;
        f.header.changed_at = Some(now);
        f.header.token = None;
        f.bytes = None;
        self.touch(file_id, writer);
        Ok(Some(now))
    }

    /// Sets only a file's removal, if it is still at `expected`, as Homebase's
    /// `update-archival-status-batch` does for each file: a deleted file answers so (4002,
    /// before the version is looked at), one already so is left as it is (its version and time
    /// too), anything else gets a new version at the server's time.
    fn set_removed_by(
        &mut self,
        writer: Option<ClientId>,
        file_id: RemoteFileId,
        expected: VersionTag,
        removed: bool,
    ) -> Result<Written> {
        let f = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        // As Homebase's archival-status batch: a tombstone, whatever the version asked.
        if f.header.body == RemoteBody::Deleted {
            return Err(RemoteError::Deleted);
        }
        if f.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        if f.header.removed == removed {
            return Ok(Written {
                target: Target {
                    at: RemoteRef {
                        file_id,
                        version: expected,
                    },
                    token: Some(token_of(file_id)),
                },
                changed_at: f.header.changed_at,
            });
        }
        let version = VersionTag(self.fresh_uuid());
        let now = self.clock_ms;
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.removed = removed;
        f.header.changed_at = Some(now);
        self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, now))
    }

    /// Deletes a file for good if it is still at `expected`, as Homebase's hard delete: gone,
    /// its row in the feed too (a client learns of it from the notification only).
    pub fn hard_delete(&mut self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        let f = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        if f.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        if let RemoteBody::Live { node, .. } = f.header.body {
            self.by_node.remove(&node);
        }
        self.files.remove(&file_id);
        self.feed.retain(|_, f| *f != file_id);
        Ok(())
    }

    /// Moves the server's clock by `ms` (for tests: writes that look old to the purge).
    pub fn advance(&mut self, ms: i64) {
        self.clock_ms += ms;
    }

    /// Leaves `file_id` out of the feed from now on, though it stays ([`FakeServer::unlisted`]).
    pub fn unlist(&mut self, file_id: RemoteFileId) {
        self.unlisted.insert(file_id);
    }

    /// Makes `file_id`'s rows unreadable from now on, or readable again
    /// ([`FakeServer::unreadable`]).
    pub fn set_unreadable(&mut self, file_id: RemoteFileId, unreadable: bool) {
        if unreadable {
            self.unreadable.insert(file_id);
        } else {
            self.unreadable.remove(&file_id);
        }
    }

    /// Puts `file_id` back in the feed ([`FakeServer::unlist`]).
    pub fn relist(&mut self, file_id: RemoteFileId) {
        self.unlisted.remove(&file_id);
    }

    pub fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        if self.unreadable.contains(&file_id) {
            return Err(RemoteError::Fatal(format!(
                "{file_id:?}: unreadable (test)"
            )));
        }
        self.files
            .get(&file_id)
            .map(|f| f.header.clone())
            .ok_or(RemoteError::NotFound)
    }

    /// The name of file `file_id`, unless it is a tombstone or missing.
    fn live_name(&self, file_id: RemoteFileId) -> Option<&str> {
        match &self.files.get(&file_id)?.header.body {
            RemoteBody::Live { content, .. } => Some(&content.name),
            RemoteBody::Deleted => None,
        }
    }

    /// Which of `folders` hold a file not deleted that `holds` counts (unlisted ones too).
    pub fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> HashSet<NodeId> {
        self.files
            .values()
            .filter(|f| !f.header.is_deleted() && holds.counts(f.header.at(), f.header.removed))
            .map(|f| f.header.parent)
            .filter(|p| folders.contains(p))
            .collect()
    }

    pub fn payload(&self, file_id: RemoteFileId) -> Result<Arc<Vec<u8>>> {
        self.files
            .get(&file_id)
            .and_then(|f| f.bytes.clone())
            .ok_or(RemoteError::NotFound)
    }

    /// A client's read of the feed ([`FakeServer::changes`]): when it held rows back, the
    /// clock has moved on by the client's next read.
    fn read(&mut self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        let page = self.changes(cursor, limit)?;
        if page.has_more && page.entries.len() < limit {
            self.tick();
        }
        Ok(page)
    }

    pub fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        let after = match cursor {
            None => None,
            Some(c) => Some(parse_cursor(c)?),
        };
        let rows = match after {
            None => self.feed.range(..),
            Some(pos) => self
                .feed
                .range((std::ops::Bound::Excluded(pos), std::ops::Bound::Unbounded)),
        }
        // A file the feed leaves out (unlisted) isn't in it.
        .filter(|(_, file_id)| !self.unlisted.contains(*file_id));
        let mut entries = Vec::new();
        let mut skipped = Vec::new();
        let mut last = after;
        let mut has_more = false;
        for (key, file_id) in rows {
            // A row of the current millisecond is held back, with more rows to come (the
            // feed is in time order: so is everything after it).
            if entries.len() + skipped.len() == limit || key.0 >= self.clock_ms {
                has_more = true;
                break;
            }
            let header = &self.files[file_id].header;
            if self.unreadable.contains(file_id) && !header.is_deleted() {
                skipped.push(Skipped {
                    at: Some(header.at()),
                    node: header.node(),
                    parent: Some(header.parent),
                    why: format!("{file_id:?}: unreadable (test)"),
                });
            } else {
                entries.push(header.clone());
            }
            last = Some(*key);
        }
        let cursor = match last {
            Some((t, r)) => format!("{t}:{r}"),
            None => cursor.unwrap_or("").to_owned(),
        };
        Ok(ChangesPage {
            entries,
            cursor,
            has_more,
            skipped,
            server_time: self.clock_ms,
        })
    }

    /// Creates a file the way another client would (for tests): returns its node id and the
    /// write result.
    pub fn seed_file(
        &mut self,
        parent: NodeId,
        name: &str,
        data: &[u8],
        algorithm: hbsync_fileio::HashAlgorithm,
    ) -> Result<(NodeId, RemoteRef)> {
        let node = NodeId::new();
        let new = NodeMeta {
            node,
            parent,
            kind: NodeKind::File,
            content: NodeContent::file(name, 1_700_000_000_000, data.len() as u64, false),
            removed: false,
        };
        let hash = hbsync_fileio::hash_bytes(data, algorithm);
        let upload = Upload {
            hash,
            size: data.len() as u64,
            reader: &mut std::io::Cursor::new(data),
            pace: None,
        };
        Ok((node, self.create(&new, Some(upload))?))
    }

    /// The content names of the files in the tree, straight from their headers, for test
    /// assertions.
    pub fn live_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .live()
            .filter_map(|h| match &h.body {
                RemoteBody::Live { content, .. } => Some(content.name.clone()),
                RemoteBody::Deleted => None,
            })
            .collect();
        names.sort();
        names
    }

    /// The files in the tree (neither deleted nor removed), for test assertions.
    pub fn live(&self) -> impl Iterator<Item = &RemoteHeader> {
        self.files
            .values()
            .map(|f| &f.header)
            .filter(|h| !h.is_deleted() && !h.removed)
    }

    /// The files in Recently deleted, for test assertions.
    pub fn removed(&self) -> impl Iterator<Item = &RemoteHeader> {
        self.files.values().map(|f| &f.header).filter(|h| h.removed)
    }

    pub fn file_id_of(&self, node: NodeId) -> Option<RemoteFileId> {
        self.by_node.get(&node).copied()
    }
}

/// The fake's token of a file: its id. A file's own and no other.
pub fn token_of(file_id: RemoteFileId) -> FileToken {
    FileToken(file_id.0.as_bytes().as_slice().into())
}

fn parse_cursor(c: &str) -> Result<(i64, u64)> {
    if c.is_empty() {
        return Ok((i64::MIN, 0));
    }
    let bad = || RemoteError::Fatal(format!("bad cursor {c:?}"));
    let (t, r) = c.split_once(':').ok_or_else(bad)?;
    Ok((t.parse().map_err(|_| bad())?, r.parse().map_err(|_| bad())?))
}

/// Faults injected into one client's requests.
#[derive(Debug, Default)]
pub struct Faults {
    /// The next requests fail with [`RemoteError::Transient`] before reaching the server.
    pub fail_next: usize,
    /// Scripted outcomes, consumed one per request: `true` = fail transiently.
    pub script: VecDeque<bool>,
    /// The next upload breaks after this many bytes (a dropped connection).
    pub cut_upload_after: Option<u64>,
    /// The next download breaks after this many bytes.
    pub cut_download_after: Option<u64>,
    /// Creates, updates and deletes of nodes with this name fail transiently (a write that
    /// keeps failing).
    pub fail_writes_named: Option<String>,
    /// Every request is refused with [`RemoteError::Denied`] (credentials revoked).
    pub denied: bool,
    /// Creates, updates and deletes are refused with [`RemoteError::Denied`] (the app lost
    /// write access to the drive); reads still work.
    pub deny_writes: bool,
    /// Creates, updates and deletes are refused with [`RemoteError::Fatal`] (a drive that won't
    /// take them, read-only); reads still work.
    pub refuse_writes: bool,
    /// Added to every request.
    pub latency: Duration,
    /// Updates and payload reads answer [`RemoteError::NotFound`] though the file is there (as
    /// a read past a payload's end might, or another reason a server has).
    pub not_found_writes: bool,
    /// Headers answer [`RemoteError::NotFound`] though the file is there: the identity's own
    /// 404 for a moment (a server's hiccup), which must not make a live file gone.
    pub not_found_headers: bool,
    /// Requests seen, for assertions.
    pub requests: usize,
    /// Headers read ([`Remote::header`]), for assertions.
    pub headers: usize,
    /// Payload bytes handed out by `read`, for assertions about resumed downloads.
    pub served: u64,
    /// Updates that came without a token (on Homebase: a header read first), for assertions.
    pub updates_without_token: usize,
    /// Feed pages read whole ([`Remote::changes`]) and as refs ([`Remote::change_refs`]).
    pub full_pages: usize,
    pub ref_pages: usize,
    /// Slim pages say one of their rows couldn't be read (a row the client doesn't decode).
    pub unread_refs: bool,
    /// Creates and updates of nodes with this name land, but their answer is lost
    /// ([`RemoteError::Transient`], a timeout after the server wrote).
    pub lose_answer_named: Option<String>,
    /// Payloads received, for assertions.
    pub uploads: usize,
    /// Payload reads give other bytes than the file holds (the first one flipped): stored
    /// bytes gone bad.
    pub corrupt_downloads: bool,
    /// Payload reads served, for assertions.
    pub reads: usize,
    /// Writes refused by `refuse_writes`, for assertions.
    pub refused: usize,
}

/// A client of a [`FakeServer`], implementing [`Remote`].
#[derive(Debug, Clone)]
pub struct FakeRemote {
    server: Arc<Mutex<FakeServer>>,
    faults: Arc<Mutex<Faults>>,
    /// Its client registration: whose writes these are.
    client: ClientId,
}

impl FakeRemote {
    /// A client of `server` with a registration of its own.
    pub fn new(server: Arc<Mutex<FakeServer>>) -> Self {
        let client = server
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .new_client();
        FakeRemote {
            server,
            faults: Arc::default(),
            client,
        }
    }

    /// Another client on this one's registration (a copied credentials file): faults of its
    /// own, the same token.
    pub fn sharing_token(&self) -> Self {
        FakeRemote {
            server: Arc::clone(&self.server),
            faults: Arc::default(),
            client: self.client,
        }
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// Subscribes to the drive's notifications ([`FakeServer::subscribe`]), leaving out the
    /// writes made with this client's token if `exclude_own_writes`.
    pub fn subscribe(&self, exclude_own_writes: bool) -> Receiver<ChangeRef> {
        self.server()
            .subscribe(exclude_own_writes.then_some(self.client))
    }

    pub fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The server it is a client of, to make another client of it.
    pub fn server_handle(&self) -> Arc<Mutex<FakeServer>> {
        Arc::clone(&self.server)
    }

    pub fn server(&self) -> MutexGuard<'_, FakeServer> {
        self.server.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Runs the per-request fault logic. Returns an error if this request should fail.
    fn gate(&self) -> Result<()> {
        let latency = {
            let mut f = self.faults();
            f.requests += 1;
            if f.denied {
                return Err(RemoteError::Denied("injected: not authorized".into()));
            }
            let scripted = f.script.pop_front().unwrap_or(false);
            if scripted || f.fail_next > 0 {
                f.fail_next = f.fail_next.saturating_sub(1);
                return Err(RemoteError::Transient("injected failure".into()));
            }
            f.latency
        };
        if !latency.is_zero() {
            std::thread::sleep(latency);
        }
        Ok(())
    }
}

impl FakeRemote {
    /// Loses the answer to a write that landed, of a node named `name`, if that is injected.
    fn answered(&self, name: &str) -> Result<()> {
        match self.faults().lose_answer_named.as_deref() == Some(name) {
            true => Err(RemoteError::Transient(
                "injected: the answer was lost".into(),
            )),
            false => Ok(()),
        }
    }

    /// Fails a write to a node named `name` if that is injected.
    fn gate_name(&self, name: &str) -> Result<()> {
        if self.faults().deny_writes {
            return Err(RemoteError::Denied("injected: no write access".into()));
        }
        {
            let mut faults = self.faults();
            if faults.refuse_writes {
                faults.refused += 1;
                return Err(RemoteError::Fatal("injected: read-only".into()));
            }
        }
        if self.faults().fail_writes_named.as_deref() == Some(name) {
            return Err(RemoteError::Transient("injected write failure".into()));
        }
        Ok(())
    }

    /// One request of [`Remote::delete`]: each file's answer.
    fn delete_batch(&self, files: &[Doomed]) -> Result<Vec<Result<Option<i64>>>> {
        self.gate()?;
        batch_of(files.iter().map(|f| f.at().file_id))?;
        // The faults on every write (no file is named so).
        self.gate_name("")?;
        let failing = self.faults().fail_writes_named.clone();
        let mut server = self.server();
        Ok(files
            .iter()
            .map(|f| {
                let f = f.at();
                match failing.is_some() && server.live_name(f.file_id) == failing.as_deref() {
                    true => Err(RemoteError::Transient("injected write failure".into())),
                    false => server.delete_by(Some(self.client), f.file_id, f.version),
                }
            })
            .collect())
    }

    /// Reads an upload stream outside the server lock, applying an injected connection cut.
    fn receive(
        &self,
        payload: Option<Upload<'_>>,
    ) -> Result<Option<(hbsync_types::ContentHash, Vec<u8>)>> {
        let Some(upload) = payload else {
            return Ok(None);
        };
        let cut = {
            let mut faults = self.faults();
            faults.uploads += 1;
            faults.cut_upload_after.take()
        };
        let mut bytes = Vec::with_capacity(upload.size as usize);
        let mut buf = [0u8; 64 * 1024];
        loop {
            if cut.is_some_and(|limit| bytes.len() as u64 >= limit) {
                return Err(RemoteError::Transient("injected upload cut".into()));
            }
            let n = upload.reader.read(&mut buf)?;
            if n == 0 {
                return Ok(Some((upload.hash, bytes)));
            }
            if let Some(pace) = upload.pace {
                pace.take(n);
            }
            bytes.extend_from_slice(&buf[..n]);
        }
    }
}

/// The file written at `at` at server time `changed_at`, with its token.
fn written(at: RemoteRef, changed_at: i64) -> Written {
    Written {
        target: Target {
            at,
            token: Some(token_of(at.file_id)),
        },
        changed_at: Some(changed_at),
    }
}

/// Hands bytes taken in by [`FakeRemote::receive`] on to the server as an upload.
fn replay<T>(
    received: Option<(hbsync_types::ContentHash, Vec<u8>)>,
    send: impl FnOnce(Option<Upload<'_>>) -> T,
) -> T {
    let Some((hash, bytes)) = received else {
        return send(None);
    };
    send(Some(Upload {
        hash,
        size: bytes.len() as u64,
        reader: &mut std::io::Cursor::new(bytes.as_slice()),
        pace: None,
    }))
}

impl Remote for FakeRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        self.gate()?;
        self.faults().full_pages += 1;
        self.server().read(cursor, limit)
    }

    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        self.gate()?;
        let unread = {
            let mut faults = self.faults();
            faults.ref_pages += 1;
            usize::from(faults.unread_refs)
        };
        Ok(RefsPage {
            unread,
            ..RefsPage::of(&self.server().read(cursor, limit)?)
        })
    }

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        parse_cursor(cursor).ok().map(|(time, _)| time)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.gate()?;
        let not_found = {
            let mut faults = self.faults();
            faults.headers += 1;
            faults.not_found_headers
        };
        if not_found {
            return Err(RemoteError::NotFound);
        }
        self.server().header(file_id)
    }

    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        self.gate()?;
        Ok(self.server().holding(folders, holds))
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Written> {
        self.gate()?;
        self.gate_name(&node.content.name)?;
        // Receive the bytes before taking the server lock, as a real server handles several
        // uploads at once; only the commit is serialized.
        let received = self.receive(payload)?;
        let written = replay(received, |upload| {
            self.server().create_by(Some(self.client), node, upload)
        })?;
        self.answered(&node.content.name)?;
        Ok(written)
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        let file_id = target.at.file_id;
        // On Homebase another file's token would write content no one can read.
        match &target.token {
            Some(t) if *t != token_of(file_id) => {
                return Err(RemoteError::Fatal(format!(
                    "update of {file_id:?} with another file's token"
                )));
            }
            Some(_) => {}
            None => self.faults().updates_without_token += 1,
        }
        self.gate()?;
        self.gate_name(&meta.content.name)?;
        if self.faults().not_found_writes {
            return Err(RemoteError::NotFound);
        }
        let received = self.receive(payload)?;
        let written = replay(received, |upload| {
            self.server()
                .update_by(Some(self.client), file_id, target.at.version, meta, upload)
        })?;
        self.answered(&meta.content.name)?;
        Ok(written)
    }

    /// A request per [`BATCH_FILES`] files, as Homebase's `soft-delete-batch`: a refusal of all
    /// writes is the request's, and so is a batch odin-core refuses whole (a file twice); a
    /// write failure injected for a name ([`Faults::fail_writes_named`]) fails that file
    /// alone, undeleted.
    fn delete(&self, files: &[Doomed]) -> Result<Deletes> {
        Answers::in_batches(files, BATCH_FILES, |batch| self.delete_batch(batch))
    }

    /// A request per [`BATCH_FILES`] files, as Homebase's `hard-delete-batch`.
    fn delete_for_good(&self, files: &[Doomed]) -> Result<ForGood> {
        Answers::in_batches(files, BATCH_FILES, |batch| {
            self.gate()?;
            batch_of(batch.iter().map(|f| f.at().file_id))?;
            let mut server = self.server();
            Ok(batch
                .iter()
                .map(|f| {
                    let RemoteRef { file_id, version } = f.at();
                    server.hard_delete(file_id, version)
                })
                .collect())
        })
    }

    /// One request: a refusal of all writes, or an answer lost (after the server wrote the
    /// rest), is the request's. A write failure injected for a name
    /// ([`Faults::fail_writes_named`]) fails that file alone, unwritten: a lever of the fake's
    /// (Homebase answers a file only with a client error) to hold one file back.
    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Written>> {
        self.gate()?;
        batch_of(files.iter().map(|t| t.at.file_id))?;
        let names: Vec<Option<String>> = {
            let server = self.server();
            files
                .iter()
                .map(|t| server.live_name(t.at.file_id).map(str::to_owned))
                .collect()
        };
        // The faults on every write (no file is named so).
        self.gate_name("")?;
        if self.faults().not_found_writes {
            return Ok(Answers::all(
                files.iter().map(|_| Err(RemoteError::NotFound)).collect(),
            ));
        }
        let failing = self.faults().fail_writes_named.clone();
        let answers = {
            let mut server = self.server();
            files
                .iter()
                .zip(&names)
                .map(|(t, name)| match failing.is_some() && *name == failing {
                    true => Err(RemoteError::Transient("injected write failure".into())),
                    false => server.set_removed_by(
                        Some(self.client),
                        t.at.file_id,
                        t.at.version,
                        removed,
                    ),
                })
                .collect()
        };
        for name in names.iter().flatten() {
            self.answered(name)?;
        }
        Ok(Answers::all(answers))
    }

    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        self.gate()?;
        let bytes = self.server().payload(file_id)?;
        let len = bytes.len() as u64;
        let range = range.unwrap_or(0..len);
        let (start, end) = (range.start.min(len), range.end.min(len));
        let slice = &bytes[start as usize..end as usize];
        let (cut, corrupt) = {
            let mut faults = self.faults();
            faults.reads += 1;
            (faults.cut_download_after.take(), faults.corrupt_downloads)
        };
        // Stored bytes gone bad: the first one flipped.
        let flipped;
        let slice = match (corrupt, slice.split_first()) {
            (true, Some((first, rest))) => {
                flipped = [[first ^ 0xff].as_slice(), rest].concat();
                flipped.as_slice()
            }
            _ => slice,
        };
        match cut {
            Some(limit) if (limit as usize) < slice.len() => {
                sink.write_all(&slice[..limit as usize])?;
                self.faults().served += limit;
                Err(RemoteError::Transient("injected download cut".into()))
            }
            _ => {
                sink.write_all(slice)?;
                self.faults().served += slice.len() as u64;
                Ok(slice.len() as u64)
            }
        }
    }
}

/// Whether `files` make a batch odin-core takes (`FileVersionTagBatchV2.AssertValidBatch`): 1
/// to 1,000 files, none twice; else a 400 for all of it.
fn batch_of(files: impl ExactSizeIterator<Item = RemoteFileId>) -> Result<()> {
    let asked = files.len();
    let ids: HashSet<RemoteFileId> = files.collect();
    if asked == 0 || asked > BATCH_FILES || ids.len() != asked {
        return Err(RemoteError::Fatal(format!(
            "1 to {BATCH_FILES} files, none twice: {asked} asked, {} different",
            ids.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A subscriber that leaves out its own writes hears another client's, and one with the
    /// same token (a copied credentials file) is taken for itself.
    #[test]
    fn own_writes_are_left_out_only_for_their_client() -> Result<()> {
        let server = FakeServer::new(ServerConfig::default());
        let (a, b) = (
            FakeRemote::new(server.clone()),
            FakeRemote::new(server.clone()),
        );
        let copy = a.sharing_token();
        let (quiet, all) = (a.subscribe(true), a.subscribe(false));
        let folder = |name: &str| NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            content: NodeContent::folder(name),
            removed: false,
        };
        let mine = a.create(&folder("mine"), None)?;
        let theirs = b.create(&folder("theirs"), None)?;
        let copied = copy.create(&folder("copied"), None)?;
        a.delete(&crate::contract::doomed(&[mine.at()]))?.single()?;
        let seeded = server
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .create(&folder("seeded"), None)?;
        let heard = |rx: &Receiver<ChangeRef>| -> Vec<RemoteFileId> {
            rx.try_iter().map(|r| r.file_id).collect()
        };
        assert_eq!(
            heard(&quiet),
            [theirs.at().file_id, seeded.file_id],
            "a's and its copy's writes left out"
        );
        assert_eq!(
            heard(&all),
            [
                mine.at().file_id,
                theirs.at().file_id,
                copied.at().file_id,
                mine.at().file_id,
                seeded.file_id
            ]
        );
        // The delete is announced as the tombstone.
        let header = server
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .header(mine.at().file_id)?;
        assert!(ChangeRef::of(&header).deleted);
        Ok(())
    }

    #[test]
    fn fresh_ids_are_unique_and_look_like_v4() {
        let server = FakeServer::new(ServerConfig::default());
        let mut s = server.lock().unwrap_or_else(|p| p.into_inner());
        let mut seen = std::collections::HashSet::new();
        for _ in 0..40_000 {
            let id = s.fresh_uuid();
            assert_eq!(id.get_version_num(), 4);
            assert_eq!(id.as_u128() >> 62 & 0b11, 0b10, "RFC 4122 variant");
            assert!(seen.insert(id), "id {id} handed out twice");
        }
    }
}
