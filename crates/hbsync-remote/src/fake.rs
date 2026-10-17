//! An in-memory sync drive with the Homebase semantics the engine depends on.
//!
//! [`FakeServer`] models the server (verified in odin-core, see
//! `docs/spec/homebase-api-notes.md`):
//! - creating a live file with an existing `UniqueId` fails (4105); tombstones don't count;
//! - updates are `VersionTag`-guarded (4160); every write gets a new tag;
//! - a delete tombstones the file: `UniqueId`, content and payload are dropped, `fileId`,
//!   `GroupId` and `FileType` stay; deletes are `VersionTag`-guarded too;
//! - the change feed is `anyChangeDate` + `oldestFirst`, resumes after the last row seen, and
//!   only returns rows modified strictly before "now" (a row written in the current
//!   millisecond shows up once the clock moves on);
//! - payload hashes: the stored hash is checked over the received bytes (4175), unencrypted
//!   payloads need `contentHash == storedHash` (here: one hash), and a drive can require
//!   hashes (4177);
//! - notifications: every write is announced to the subscribers as its file's row
//!   ([`FakeServer::subscribe`]), except to those that exclude their own client's writes
//!   (`excludeOwnWrites`, odin-core #1971): each [`FakeRemote::new`] is a client registration
//!   (a token) of its own. A write not made through a client (a test's, on the server) is
//!   announced to all, as Homebase does when it can't tell whose write it is;
//! - a kept answer ([`FakeServer::keep_answer`]): odin-core keeps a feed query's answer by the
//!   query (cursor and page size), and one computed while a write commits can be kept after
//!   it, hiding that write from the same query until the drive's next write.
//!
//! [`FakeRemote`] is a client of it implementing [`Remote`] the way the Homebase adapter does:
//! a delete refused for its version (4160) is read once more and counts as done when the file
//! is deleted (a retry). Faults and latency are injected per client.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use hbsync_fileio::hash_bytes;
use hbsync_types::schema::NodeContent;
use hbsync_types::{
    FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef, Target, Uuid, VersionTag,
};

use crate::{
    ChangeRef, ChangesPage, NodeMeta, PayloadInfo, RefsPage, Remote, RemoteBody, RemoteError,
    RemoteHeader, Result, Upload,
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
    next_row: u64,
    /// Deterministic id source.
    next_id: u64,
    subscribers: Vec<Subscriber>,
    /// Feed answers kept by query (cursor, page size), each until the write count passes its
    /// mark ([`FakeServer::keep_answer`]).
    kept: HashMap<(String, usize), (ChangesPage, u64)>,
    writes: u64,
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
            next_row: 1,
            next_id: 1,
            subscribers: Vec::new(),
            kept: HashMap::new(),
            writes: 0,
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

    /// When a write leaves `meta` removed: now, as Homebase's `updated` is the write's time.
    fn removed_at(&self, meta: &NodeMeta) -> Option<i64> {
        meta.removed.then_some(self.clock_ms)
    }

    /// A write of `file_id` by `writer` (`None`: not through a client) landed: its new place
    /// in the feed, and its announcement.
    fn touch(&mut self, file_id: RemoteFileId, writer: Option<ClientId>) {
        self.writes += 1;
        let writes = self.writes;
        self.kept.retain(|_, (_, until)| writes <= *until);
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
                        return Err(RemoteError::Full(format!("{quota} bytes at most")));
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
        self.create_by(None, node, upload).map(|w| w.at)
    }

    /// A create by `writer`, answered as Homebase answers it: the file as written, with its
    /// token and the write's time (`updated`, odin-core #1971).
    fn create_by(
        &mut self,
        writer: Option<ClientId>,
        node: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<Target> {
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
            removed_at: self.removed_at(node),
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
            .map(|w| w.at)
    }

    /// An update by `writer`, answered as [`FakeServer::create_by`] is.
    fn update_by(
        &mut self,
        writer: Option<ClientId>,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<Target> {
        let current = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        if current.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        let RemoteBody::Live {
            node,
            payload: old_payload,
            ..
        } = &current.header.body
        else {
            return Err(RemoteError::NotFound);
        };
        let (node, old_payload, kind) = (*node, *old_payload, current.header.kind);
        // Homebase replaces the whole header: a client that sent another node id or kind
        // would change (or clear) them. Ours never may.
        if meta.node != node || meta.kind != kind {
            return Err(RemoteError::Fatal(format!(
                "update would change the node {node} ({kind:?}) to {} ({:?})",
                meta.node, meta.kind
            )));
        }
        let removed_at = self.removed_at(meta);
        // Without an upload the payload stays.
        let new_payload = match upload {
            Some(upload) => self.receive(kind, Some(upload))?,
            None => None,
        };
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.parent = meta.parent;
        f.header.removed_at = removed_at;
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

    /// Deletes a file if it is still at `expected` (4160 otherwise). Like Homebase, deleting a
    /// tombstone at its own version rewrites it: a new version and a new feed row.
    pub fn delete(&mut self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        self.delete_by(None, file_id, expected)
    }

    fn delete_by(
        &mut self,
        writer: Option<ClientId>,
        file_id: RemoteFileId,
        expected: VersionTag,
    ) -> Result<()> {
        let f = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        if f.header.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        if let RemoteBody::Live { node, .. } = f.header.body {
            self.by_node.remove(&node);
        }
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.body = RemoteBody::Deleted;
        f.header.removed_at = None;
        f.header.changed_at = None;
        f.header.token = None;
        f.bytes = None;
        self.touch(file_id, writer);
        Ok(())
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

    pub fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.files
            .get(&file_id)
            .map(|f| f.header.clone())
            .ok_or(RemoteError::NotFound)
    }

    pub fn payload(&self, file_id: RemoteFileId) -> Result<Arc<Vec<u8>>> {
        self.files
            .get(&file_id)
            .and_then(|f| f.bytes.clone())
            .ok_or(RemoteError::NotFound)
    }

    /// Keeps the feed's answer to a read from `cursor` with pages of `limit` as it is now,
    /// through the next write (the one it raced with) until the one after: the same query
    /// gets it meanwhile, another page size reads the feed as it is.
    pub fn keep_answer(&mut self, cursor: Option<&str>, limit: usize) -> Result<()> {
        let page = self.read_feed(cursor, limit)?;
        let key = (cursor.unwrap_or("").to_owned(), limit);
        self.kept.insert(key, (page, self.writes + 1));
        Ok(())
    }

    pub fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        if let Some((page, _)) = self.kept.get(&(cursor.unwrap_or("").to_owned(), limit)) {
            return Ok(page.clone());
        }
        self.read_feed(cursor, limit)
    }

    fn read_feed(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        let after = match cursor {
            None => None,
            Some(c) => Some(parse_cursor(c)?),
        };
        let visible = |&(modified, _): &(i64, u64)| modified < self.clock_ms;
        let mut range = match after {
            None => self.feed.range(..),
            Some(pos) => self
                .feed
                .range((std::ops::Bound::Excluded(pos), std::ops::Bound::Unbounded)),
        }
        .filter(|(k, _)| visible(k));
        let mut entries = Vec::new();
        let mut last = after;
        for (key, file_id) in range.by_ref().take(limit) {
            entries.push(self.files[file_id].header.clone());
            last = Some(*key);
        }
        let has_more = range.next().is_some();
        let cursor = match last {
            Some((t, r)) => format!("{t}:{r}"),
            None => cursor.unwrap_or("").to_owned(),
        };
        Ok(ChangesPage {
            entries,
            cursor,
            has_more,
            skipped: Vec::new(),
            skipped_files: Vec::new(),
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
            .filter(|h| !h.is_deleted() && h.removed_at.is_none())
    }

    /// The files in Recently deleted, for test assertions.
    pub fn removed(&self) -> impl Iterator<Item = &RemoteHeader> {
        self.files
            .values()
            .map(|f| &f.header)
            .filter(|h| h.removed_at.is_some())
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
    /// Requests seen, for assertions.
    pub requests: usize,
    /// Payload bytes handed out by `read`, for assertions about resumed downloads.
    pub served: u64,
    /// Updates that came without a token (on Homebase: a header read first), for assertions.
    pub updates_without_token: usize,
    /// Feed pages read whole ([`Remote::changes`]) and as refs ([`Remote::change_refs`]).
    pub full_pages: usize,
    pub ref_pages: usize,
    /// Slim pages say one of their rows couldn't be read (a row the client doesn't decode).
    pub unread_refs: bool,
    /// The page size of the last feed read, for assertions.
    pub last_page: Option<usize>,
    /// Rows of these files can't be read (as the Homebase adapter skips a row that doesn't
    /// decrypt): whole pages leave them out, with why.
    pub unreadable: std::collections::HashSet<RemoteFileId>,
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
    /// Fails a write to a node named `name` if that is injected.
    fn gate_name(&self, name: &str) -> Result<()> {
        if self.faults().deny_writes {
            return Err(RemoteError::Denied("injected: no write access".into()));
        }
        if self.faults().refuse_writes {
            return Err(RemoteError::Fatal("injected: read-only".into()));
        }
        if self.faults().fail_writes_named.as_deref() == Some(name) {
            return Err(RemoteError::Transient("injected write failure".into()));
        }
        Ok(())
    }

    /// Reads an upload stream outside the server lock, applying an injected connection cut.
    fn receive(
        &self,
        payload: Option<Upload<'_>>,
    ) -> Result<Option<(hbsync_types::ContentHash, Vec<u8>)>> {
        let Some(upload) = payload else {
            return Ok(None);
        };
        let cut = self.faults().cut_upload_after.take();
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
fn written(at: RemoteRef, changed_at: i64) -> Target {
    Target {
        at,
        token: Some(token_of(at.file_id)),
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
        let unreadable = {
            let mut faults = self.faults();
            faults.full_pages += 1;
            faults.last_page = Some(limit);
            faults.unreadable.clone()
        };
        let mut page = self.server().changes(cursor, limit)?;
        let (gone, entries) = page
            .entries
            .into_iter()
            .partition(|h| unreadable.contains(&h.file_id));
        page.entries = entries;
        for h in gone {
            let h: RemoteHeader = h;
            page.skipped
                .push(format!("{:?}: injected: unreadable", h.file_id));
            page.skipped_files.push(h.file_id);
        }
        Ok(page)
    }

    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        self.gate()?;
        let unread = {
            let mut faults = self.faults();
            faults.ref_pages += 1;
            faults.last_page = Some(limit);
            usize::from(faults.unread_refs || !faults.unreadable.is_empty())
        };
        Ok(RefsPage {
            unread,
            ..RefsPage::of(&self.server().changes(cursor, limit)?)
        })
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.gate()?;
        self.server().header(file_id)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Target> {
        self.gate()?;
        self.gate_name(&node.content.name)?;
        // Receive the bytes before taking the server lock, as a real server handles several
        // uploads at once; only the commit is serialized.
        let received = self.receive(payload)?;
        replay(received, |upload| {
            self.server().create_by(Some(self.client), node, upload)
        })
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Target> {
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
        let received = self.receive(payload)?;
        replay(received, |upload| {
            self.server()
                .update_by(Some(self.client), file_id, target.at.version, meta, upload)
        })
    }

    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        self.gate()?;
        let body = self.server().header(file_id)?.body;
        if let RemoteBody::Live { content, .. } = body {
            self.gate_name(&content.name)?;
        }
        let result = self
            .server()
            .delete_by(Some(self.client), file_id, expected);
        match result {
            // A retry, as the Homebase adapter takes it.
            Err(RemoteError::VersionMismatch)
                if self
                    .server()
                    .header(file_id)
                    .is_ok_and(|h| h.body == RemoteBody::Deleted) =>
            {
                Ok(())
            }
            result => result,
        }
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
        let cut = self.faults().cut_download_after.take();
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
        a.delete(mine.at.file_id, mine.at.version)?;
        let seeded = server
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .create(&folder("seeded"), None)?;
        let heard = |rx: &Receiver<ChangeRef>| -> Vec<RemoteFileId> {
            rx.try_iter().map(|r| r.file_id).collect()
        };
        assert_eq!(
            heard(&quiet),
            [theirs.at.file_id, seeded.file_id],
            "a's and its copy's writes left out"
        );
        assert_eq!(
            heard(&all),
            [
                mine.at.file_id,
                theirs.at.file_id,
                copied.at.file_id,
                mine.at.file_id,
                seeded.file_id
            ]
        );
        // The delete is announced as the tombstone.
        let header = server
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .header(mine.at.file_id)?;
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
