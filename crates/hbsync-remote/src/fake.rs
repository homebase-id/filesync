//! An in-memory sync drive with the Homebase semantics the engine depends on.
//!
//! [`FakeServer`] models the server (verified in odin-core, see
//! `docs/spec/homebase-api-notes.md`):
//! - creating a live file with an existing `UniqueId` fails (4105); tombstones don't count;
//! - updates are `VersionTag`-guarded (4160); every write gets a new tag;
//! - a delete tombstones the file: `UniqueId`, content and payload are dropped, `fileId`,
//!   `GroupId` and `FileType` stay; **the server's delete takes no version**;
//! - the change feed is `anyChangeDate` + `oldestFirst`, resumes after the last row seen, and
//!   only returns rows modified strictly before "now" (a row written in the current
//!   millisecond shows up once the clock moves on);
//! - payload hashes: the stored hash is checked over the received bytes (4175), unencrypted
//!   payloads need `contentHash == storedHash` (here: one hash), and a drive can require
//!   hashes (4177).
//!
//! [`FakeRemote`] is a client of it implementing [`Remote`] the way the Homebase adapter will:
//! `delete` reads the header, compares the version, then deletes (a check-then-act race the
//! real server can't close yet). Faults and latency are injected per client.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use hbsync_fileio::hash_bytes;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, RemoteFileId, RemoteRef, Uuid, VersionTag};

use crate::{
    ChangesPage, NodeMeta, PayloadInfo, Remote, RemoteBody, RemoteError, RemoteHeader, Result,
    Upload,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            require_hashes: true,
            auto_tick: true,
        }
    }
}

/// The shared in-memory drive.
#[derive(Debug)]
pub struct FakeServer {
    config: ServerConfig,
    files: HashMap<RemoteFileId, StoredFile>,
    /// Live files only.
    by_node: HashMap<NodeId, RemoteFileId>,
    /// Feed index: (modified, row) → file.
    feed: BTreeMap<(i64, u64), RemoteFileId>,
    clock_ms: i64,
    next_row: u64,
    /// Deterministic id source.
    next_id: u64,
}

impl FakeServer {
    pub fn new(config: ServerConfig) -> Arc<Mutex<FakeServer>> {
        Arc::new(Mutex::new(FakeServer {
            config,
            files: HashMap::new(),
            by_node: HashMap::new(),
            feed: BTreeMap::new(),
            clock_ms: 1_000,
            next_row: 1,
            next_id: 1,
        }))
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

    fn touch(&mut self, file_id: RemoteFileId) {
        let modified = self.clock_ms;
        if let Some(f) = self.files.get_mut(&file_id) {
            self.feed.remove(&(f.modified, f.row));
            f.modified = modified;
            self.feed.insert((modified, f.row), file_id);
        }
        if self.config.auto_tick {
            self.tick();
        }
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
                let info = PayloadInfo {
                    hash: Some(upload.hash),
                    bytes: bytes.len() as u64,
                };
                Ok(Some((Arc::new(bytes), info)))
            }
        }
    }

    // ---- server operations (the HTTP API) ----

    pub fn create(&mut self, node: &NodeMeta, upload: Option<Upload<'_>>) -> Result<RemoteRef> {
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
        self.touch(file_id);
        Ok(RemoteRef { file_id, version })
    }

    pub fn update(
        &mut self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        upload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
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
        let (parent, content) = (meta.parent, &meta.content);
        // Without an upload the payload stays.
        let new_payload = match upload {
            Some(upload) => self.receive(kind, Some(upload))?,
            None => None,
        };
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.parent = parent;
        let payload = match new_payload {
            Some((bytes, info)) => {
                f.bytes = Some(bytes);
                Some(info)
            }
            None => old_payload,
        };
        f.header.body = RemoteBody::Live {
            node,
            content: content.clone(),
            payload,
        };
        self.touch(file_id);
        Ok(RemoteRef { file_id, version })
    }

    /// Homebase's delete: no version check.
    pub fn delete(&mut self, file_id: RemoteFileId) -> Result<()> {
        let f = self.files.get(&file_id).ok_or(RemoteError::NotFound)?;
        if let RemoteBody::Live { node, .. } = f.header.body {
            self.by_node.remove(&node);
        } else {
            return Ok(()); // deleting a tombstone is a no-op
        }
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.body = RemoteBody::Deleted;
        f.bytes = None;
        self.touch(file_id);
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

    pub fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
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
        };
        let hash = hbsync_fileio::hash_bytes(data, algorithm);
        let upload = Upload {
            hash,
            size: data.len() as u64,
            reader: &mut std::io::Cursor::new(data),
        };
        Ok((node, self.create(&new, Some(upload))?))
    }

    /// The content names of all live files, straight from their headers (also for files
    /// without a hash), for test assertions.
    pub fn live_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .files
            .values()
            .filter_map(|f| match &f.header.body {
                RemoteBody::Live { content, .. } => Some(content.name.clone()),
                RemoteBody::Deleted => None,
            })
            .collect();
        names.sort();
        names
    }

    /// Live files, for test assertions.
    pub fn live(&self) -> impl Iterator<Item = &RemoteHeader> {
        self.files
            .values()
            .map(|f| &f.header)
            .filter(|h| !h.is_deleted())
    }

    pub fn file_id_of(&self, node: NodeId) -> Option<RemoteFileId> {
        self.by_node.get(&node).copied()
    }
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
    /// Added to every request.
    pub latency: Duration,
    /// Requests seen, for assertions.
    pub requests: usize,
    /// Payload bytes handed out by `read`, for assertions about resumed downloads.
    pub served: u64,
}

/// A client of a [`FakeServer`], implementing [`Remote`].
#[derive(Debug, Clone)]
pub struct FakeRemote {
    server: Arc<Mutex<FakeServer>>,
    faults: Arc<Mutex<Faults>>,
}

impl FakeRemote {
    pub fn new(server: Arc<Mutex<FakeServer>>) -> Self {
        FakeRemote {
            server,
            faults: Arc::default(),
        }
    }

    pub fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults.lock().unwrap_or_else(|p| p.into_inner())
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
            bytes.extend_from_slice(&buf[..n]);
        }
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
    }))
}

impl Remote for FakeRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        self.gate()?;
        self.server().changes(cursor, limit)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.gate()?;
        self.server().header(file_id)
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<RemoteRef> {
        self.gate()?;
        self.gate_name(&node.content.name)?;
        // Receive the bytes before taking the server lock, as a real server handles several
        // uploads at once; only the commit is serialized.
        let received = self.receive(payload)?;
        replay(received, |upload| self.server().create(node, upload))
    }

    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<RemoteRef> {
        self.gate()?;
        self.gate_name(&meta.content.name)?;
        let received = self.receive(payload)?;
        replay(received, |upload| {
            self.server().update(file_id, expected, meta, upload)
        })
    }

    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> Result<()> {
        // Check, then act: the server's delete has no version guard.
        self.gate()?;
        let current = self.server().header(file_id)?;
        let RemoteBody::Live { content, .. } = &current.body else {
            return Ok(()); // already deleted: a retry
        };
        self.gate_name(&content.name)?;
        if current.version != expected {
            return Err(RemoteError::VersionMismatch);
        }
        self.gate()?;
        self.server().delete(file_id)
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
