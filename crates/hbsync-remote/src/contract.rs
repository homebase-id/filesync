//! The behavior every [`Remote`] must have, as runnable scenarios: the fake runs them in CI,
//! the Homebase adapter against a live identity.
//!
//! The scenarios only use the trait, and only inside a scratch folder of their own, so they
//! can run on a drive that holds other files. Everything they create is deleted by
//! [`Bench::cleanup`].

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;

use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_types::schema::{NodeContent, Removal};
use hbsync_types::{NodeId, NodeKind, RemoteFileId, RemoteRef, Tag, Tags};

use crate::{NodeMeta, Remote, RemoteBody, RemoteError, RemoteHeader, Upload};

pub type Outcome = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A remote under test and the scratch folder the scenarios work in.
pub struct Bench<'a> {
    pub remote: &'a dyn Remote,
    scratch: NodeId,
    /// How long a write takes to show up in the feed (Homebase: rows of the current
    /// millisecond are held back).
    settle: Duration,
    created: Mutex<Vec<RemoteFileId>>,
}

impl std::fmt::Debug for Bench<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bench")
            .field("scratch", &self.scratch)
            .finish_non_exhaustive()
    }
}

/// The scenarios, by name.
pub type Scenario = fn(&Bench) -> Outcome;

pub const SCENARIOS: &[(&str, Scenario)] = &[
    ("create_read_and_feed", create_read_and_feed),
    ("empty_file", empty_file),
    (
        "retried_create_and_recreate_after_delete",
        retried_create_and_recreate_after_delete,
    ),
    ("guarded_updates", guarded_updates),
    ("content_round_trips", content_round_trips),
    ("deletes", deletes),
    ("tombstone_in_the_feed", tombstone_in_the_feed),
    (
        "removal_keeps_the_bytes_and_restores",
        removal_keeps_the_bytes_and_restores,
    ),
    (
        "a_removed_node_deleted_for_good",
        a_removed_node_deleted_for_good,
    ),
    ("hash_mismatch_stores_nothing", hash_mismatch_stores_nothing),
    ("feed_pages_and_resumes", feed_pages_and_resumes),
    (
        "every_write_shows_up_promptly",
        every_write_shows_up_promptly,
    ),
    ("ranged_reads", ranged_reads),
];

impl<'a> Bench<'a> {
    /// Creates a scratch folder named `name` at the top of the drive.
    pub fn new(remote: &'a dyn Remote, name: &str, settle: Duration) -> Result<Self, RemoteError> {
        let scratch = folder(name, NodeId::ROOT);
        let written = remote.create(&scratch, None)?;
        Ok(Bench {
            remote,
            scratch: scratch.node,
            settle,
            created: Mutex::new(vec![written.file_id]),
        })
    }

    /// Runs every scenario; returns the names and errors of the ones that failed.
    pub fn run_all(&self) -> Vec<(&'static str, String)> {
        SCENARIOS
            .iter()
            .filter_map(|(name, run)| run(self).err().map(|e| (*name, e.to_string())))
            .collect()
    }

    /// Deletes everything the scenarios (and the scratch folder) left, children first.
    pub fn cleanup(&self) -> Result<(), RemoteError> {
        let created = std::mem::take(&mut *self.created.lock().unwrap_or_else(|e| e.into_inner()));
        for file_id in created.into_iter().rev() {
            let header = match self.remote.header(file_id) {
                Ok(h) => h,
                Err(RemoteError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            if !header.is_deleted() {
                match self.remote.delete(file_id, header.version) {
                    Ok(()) | Err(RemoteError::NotFound) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    /// A create, remembered for cleanup.
    pub fn create(&self, meta: &NodeMeta, data: Option<&[u8]>) -> Result<RemoteRef, RemoteError> {
        let written = with_upload(data, |upload| self.remote.create(meta, upload))?;
        self.track(written.file_id);
        Ok(written)
    }

    /// Remembers a file created some other way, for cleanup.
    pub fn track(&self, file_id: RemoteFileId) {
        self.created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(file_id);
    }

    /// A folder in the scratch folder, remembered for cleanup.
    pub fn folder(&self, name: &str) -> Result<(NodeMeta, RemoteRef), RemoteError> {
        let meta = folder(name, self.scratch);
        let written = self.create(&meta, None)?;
        Ok((meta, written))
    }

    fn file(&self, name: &str, data: &[u8]) -> Result<(NodeMeta, RemoteRef), RemoteError> {
        let meta = file(name, self.scratch, data);
        let written = self.create(&meta, Some(data))?;
        Ok((meta, written))
    }

    /// An update with the seal the file's header gives, as the engine has it from the feed.
    fn update(
        &self,
        at: RemoteRef,
        meta: &NodeMeta,
        data: Option<&[u8]>,
    ) -> Result<RemoteRef, RemoteError> {
        let seal = match self.remote.header(at.file_id) {
            Ok(header) => header.seal,
            Err(RemoteError::NotFound) => None,
            Err(e) => return Err(e),
        };
        with_upload(data, |upload| {
            self.remote
                .update(at.file_id, at.version, seal, meta, upload)
        })
    }

    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Vec<u8>, RemoteError> {
        let mut out = Vec::new();
        let n = self.remote.read(file_id, range, &mut out)?;
        if n != out.len() as u64 {
            return Err(RemoteError::Fatal(format!(
                "read returned {n} but wrote {} bytes",
                out.len()
            )));
        }
        Ok(out)
    }

    /// The feed's end: a cursor after every change so far.
    fn cursor_now(&self) -> Result<String, RemoteError> {
        Ok(self.changes_after(None)?.1)
    }

    /// Every change after `cursor` (all pages) and the cursor after them.
    fn changes_after(
        &self,
        cursor: Option<&str>,
    ) -> Result<(Vec<RemoteHeader>, String), RemoteError> {
        std::thread::sleep(self.settle);
        let mut cursor = cursor.map(str::to_owned);
        let mut entries = Vec::new();
        loop {
            let page = self.remote.changes(cursor.as_deref(), 100)?;
            if !page.skipped.is_empty() {
                return Err(RemoteError::Fatal(format!(
                    "feed rows skipped: {:?}",
                    page.skipped
                )));
            }
            entries.extend(page.entries);
            if !page.has_more {
                return Ok((entries, page.cursor));
            }
            cursor = Some(page.cursor);
        }
    }

    /// The changes after `cursor` to the files in `ids` (other writers may share the drive).
    fn changes_to(
        &self,
        cursor: &str,
        ids: &[RemoteFileId],
    ) -> Result<Vec<RemoteHeader>, RemoteError> {
        let ids: HashSet<_> = ids.iter().collect();
        Ok(self
            .changes_after(Some(cursor))?
            .0
            .into_iter()
            .filter(|h| ids.contains(&h.file_id))
            .collect())
    }
}

/// `data` as an upload with its BLAKE3 hash.
pub fn upload<'a>(reader: &'a mut Cursor<&[u8]>) -> Upload<'a> {
    let data: &[u8] = reader.get_ref();
    Upload {
        hash: hash_bytes(data, HashAlgorithm::Blake3),
        size: data.len() as u64,
        reader,
        pace: None,
    }
}

/// Calls `f` with `data` as an upload, or with none.
fn with_upload<T>(data: Option<&[u8]>, f: impl FnOnce(Option<Upload<'_>>) -> T) -> T {
    match data {
        Some(data) => f(Some(upload(&mut Cursor::new(data)))),
        None => f(None),
    }
}

pub fn folder(name: &str, parent: NodeId) -> NodeMeta {
    NodeMeta {
        node: NodeId::new(),
        parent,
        kind: NodeKind::Folder,
        content: NodeContent::folder(name),
    }
}

pub fn file(name: &str, parent: NodeId, data: &[u8]) -> NodeMeta {
    NodeMeta {
        node: NodeId::new(),
        parent,
        kind: NodeKind::File,
        content: NodeContent::file(name, 1_700_000_000_123, data.len() as u64, false),
    }
}

/// `meta` with new content (a rename, an edit).
pub fn with(meta: &NodeMeta, content: NodeContent) -> NodeMeta {
    NodeMeta {
        content,
        ..meta.clone()
    }
}

macro_rules! ensure {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+).into());
        }
    };
}

macro_rules! ensure_err {
    ($result:expr, $pattern:pat) => {
        match $result {
            Err($pattern) => {}
            other => {
                return Err(format!(
                    "{}: expected {}, got {other:?}",
                    stringify!($result),
                    stringify!($pattern)
                )
                .into())
            }
        }
    };
}

fn create_read_and_feed(b: &Bench) -> Outcome {
    let start = b.cursor_now()?;
    let (dir, dir_ref) = b.folder("Docs")?;
    let f = file("a.txt", dir.node, b"hello");
    let written = b.create(&f, Some(b"hello"))?;

    let header = b.remote.header(written.file_id)?;
    ensure!(
        header.node() == Some(f.node),
        "node id: {:?}",
        header.node()
    );
    ensure!(header.parent == dir.node, "parent: {}", header.parent);
    ensure!(
        header.version == written.version,
        "version differs from the create's"
    );
    let state = header.state().ok_or("no state")?;
    ensure!(state.name == "a.txt", "name: {}", state.name);
    let content = state.content.ok_or("no file content")?;
    ensure!(
        content.hash == hash_bytes(b"hello", HashAlgorithm::Blake3),
        "hash: {:?}",
        content.hash
    );
    ensure!(content.size == 5, "size: {}", content.size);
    ensure!(
        content.mtime_ms == 1_700_000_000_123,
        "mtime: {}",
        content.mtime_ms
    );
    ensure!(b.read(written.file_id, None)? == b"hello", "payload");

    let feed = b.changes_to(&start, &[dir_ref.file_id, written.file_id])?;
    let ids: Vec<_> = feed.iter().map(|h| h.file_id).collect();
    ensure!(
        ids == [dir_ref.file_id, written.file_id],
        "feed order: {ids:?}"
    );
    ensure!(feed[1] == header, "the feed's header differs from header()");
    Ok(())
}

fn empty_file(b: &Bench) -> Outcome {
    let (_, written) = b.file("empty", b"")?;
    let state = b
        .remote
        .header(written.file_id)?
        .state()
        .ok_or("no state")?;
    ensure!(state.content.map(|c| c.size) == Some(0), "size");
    ensure!(b.read(written.file_id, None)?.is_empty(), "payload");
    Ok(())
}

fn retried_create_and_recreate_after_delete(b: &Bench) -> Outcome {
    let (f, first) = b.file("x", b"1")?;
    ensure_err!(b.create(&f, Some(b"1")), RemoteError::AlreadyExists);
    b.remote.delete(first.file_id, first.version)?;
    // A tombstone frees the node id; the new file gets a new file id.
    let again = b.create(&f, Some(b"1"))?;
    ensure!(
        again.file_id != first.file_id,
        "same file id after re-create"
    );
    ensure!(
        b.remote.header(again.file_id)?.node() == Some(f.node),
        "node id"
    );
    Ok(())
}

fn guarded_updates(b: &Bench) -> Outcome {
    let (dir, _) = b.folder("target")?;
    let (f, w1) = b.file("v.txt", b"v1")?;
    // Rename and move, metadata only: the payload stays.
    let moved = NodeMeta {
        parent: dir.node,
        ..with(
            &f,
            NodeContent::file("renamed.txt", 1_700_000_000_123, 2, false),
        )
    };
    let w2 = b.update(w1, &moved, None)?;
    ensure!(
        w2.file_id == w1.file_id && w2.version != w1.version,
        "new version"
    );
    let header = b.remote.header(w2.file_id)?;
    ensure!(header.node() == Some(f.node), "node id kept");
    ensure!(header.kind == NodeKind::File, "kind kept");
    ensure!(header.parent == dir.node, "moved");
    let state = header
        .state()
        .ok_or("payload info lost on a metadata update")?;
    ensure!(state.name == "renamed.txt", "renamed");
    ensure!(b.read(w2.file_id, None)? == b"v1", "payload kept");
    // Stale version.
    ensure_err!(b.update(w1, &moved, None), RemoteError::VersionMismatch);
    ensure_err!(
        b.update(w1, &moved, Some(b"lost")),
        RemoteError::VersionMismatch
    );
    // Content update.
    let edited = with(
        &moved,
        NodeContent::file("renamed.txt", 1_700_000_000_999, 3, true),
    );
    let w3 = b.update(w2, &edited, Some(b"v2!"))?;
    ensure!(b.read(w3.file_id, None)? == b"v2!", "new payload");
    let content = b
        .remote
        .header(w3.file_id)?
        .state()
        .ok_or("state")?
        .content
        .ok_or("file")?;
    ensure!(
        content.hash == hash_bytes(b"v2!", HashAlgorithm::Blake3) && content.size == 3,
        "new hash and size"
    );
    ensure!(
        content.executable && content.mtime_ms == 1_700_000_000_999,
        "new metadata"
    );
    Ok(())
}

fn content_round_trips(b: &Bench) -> Outcome {
    let mut content = NodeContent::file("Résumé – 履歴書 🗂.txt", -1_000, 4, false);
    content.set_created_ms(Some(-2_000));
    content.set_tags(&Tags::new([Tag::new("Red", 6), Tag::new("Работа 🗂", 0)]));
    content
        .unknown
        .insert("future".into(), serde_json::json!({"k": [1, 2]}));
    let meta = NodeMeta {
        content: content.clone(),
        ..file("", b.scratch, b"data")
    };
    let written = b.create(&meta, Some(b"data"))?;
    let RemoteBody::Live { content: back, .. } = b.remote.header(written.file_id)?.body else {
        return Err("not live".into());
    };
    ensure!(back == content, "content came back as {back:?}");
    ensure!(
        back.tags() == content.tags(),
        "tags came back as {:?}",
        back.tags()
    );
    Ok(())
}

fn deletes(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"1")?;
    let w2 = b.update(w, &with(&f, NodeContent::file("f", 2, 1, false)), None)?;
    // A delete at a version that is no longer current is refused (another client changed it).
    ensure_err!(
        b.remote.delete(w.file_id, w.version),
        RemoteError::VersionMismatch
    );
    ensure!(!b.remote.header(w.file_id)?.is_deleted(), "deleted anyway");
    b.remote.delete(w2.file_id, w2.version)?;
    ensure!(b.remote.header(w.file_id)?.is_deleted(), "not deleted");
    // A retried delete succeeds.
    b.remote.delete(w2.file_id, w2.version)?;
    // An update of a deleted file fails, header only or with new bytes (another client
    // deleted it meanwhile).
    ensure_err!(
        b.update(w2, &f, None),
        RemoteError::NotFound | RemoteError::VersionMismatch
    );
    ensure_err!(
        b.update(w2, &f, Some(b"edited")),
        RemoteError::NotFound | RemoteError::VersionMismatch
    );
    ensure_err!(b.read(w.file_id, None), RemoteError::NotFound);
    Ok(())
}

fn tombstone_in_the_feed(b: &Bench) -> Outcome {
    let (dir, _) = b.folder("d")?;
    let f = file("f", dir.node, b"data");
    let w = b.create(&f, Some(b"data"))?;
    let cursor = b.cursor_now()?;
    b.remote.delete(w.file_id, w.version)?;

    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(feed.len() == 1, "{} feed entries", feed.len());
    let tomb = &feed[0];
    ensure!(
        (tomb.file_id, tomb.parent, tomb.kind) == (w.file_id, dir.node, NodeKind::File),
        "tombstone keeps file id, parent and kind: {tomb:?}"
    );
    ensure!(
        tomb.body == RemoteBody::Deleted && tomb.node().is_none(),
        "{tomb:?}"
    );
    Ok(())
}

/// `meta` removed to Recently deleted (node schema: `archivalStatus` 2 and `rm`).
pub fn removed(meta: &NodeMeta) -> NodeMeta {
    let mut content = meta.content.clone();
    content.set_removal(Some(Removal {
        at_ms: 1_700_000_000_456,
        root: meta.node,
    }));
    with(meta, content)
}

fn removal_keeps_the_bytes_and_restores(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"kept")?;
    let cursor = b.cursor_now()?;
    let gone = b.update(w, &removed(&f), None)?;
    let header = b.remote.header(w.file_id)?;
    ensure!(
        header.removal() == removed(&f).content.removal() && header.state().is_none(),
        "not removed: {header:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"kept", "the bytes went");
    // The node id stays taken: a create of it is a retry that landed.
    ensure_err!(b.create(&f, Some(b"kept")), RemoteError::AlreadyExists);
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        feed.last()
            .is_some_and(|h| h.node() == Some(f.node) && h.removal().is_some()),
        "the feed shows no removal: {feed:?}"
    );
    // A write that keeps the removal keeps it (every write sends the status).
    let renamed = removed(&with(&f, NodeContent::file("f2", 1, 4, false)));
    let gone = b.update(gone, &renamed, None)?;
    ensure!(
        b.remote.header(w.file_id)?.removal().is_some(),
        "a rewrite restored it"
    );
    // Restore: the update back, here with new bytes (an edit that wins over the delete).
    let edited = with(&f, NodeContent::file("f", 1_700_000_000_124, 6, false));
    b.update(gone, &edited, Some(b"edited"))?;
    let back = b.remote.header(w.file_id)?;
    ensure!(
        back.removal().is_none() && back.state().is_some(),
        "not restored: {back:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"edited", "the restore's bytes");
    Ok(())
}

fn a_removed_node_deleted_for_good(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"1")?;
    let gone = b.update(w, &removed(&f), None)?;
    let cursor = b.cursor_now()?;
    b.remote.delete(gone.file_id, gone.version)?;
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        feed.last().is_some_and(RemoteHeader::is_deleted),
        "no tombstone in the feed: {feed:?}"
    );
    ensure_err!(b.read(w.file_id, None), RemoteError::NotFound);
    Ok(())
}

fn hash_mismatch_stores_nothing(b: &Bench) -> Outcome {
    let f = file("f", b.scratch, b"real");
    // A source that isn't what was hashed (other bytes, fewer, more) is refused.
    for changed in [&b"fake"[..], b"rea", b"real, and more"] {
        let mut reader = Cursor::new(changed);
        let lying = Upload {
            hash: hash_bytes(b"real", HashAlgorithm::Blake3),
            size: 4,
            ..upload(&mut reader)
        };
        ensure_err!(b.remote.create(&f, Some(lying)), RemoteError::HashMismatch);
    }
    // Nothing was stored: the node id is still free.
    let at = b.create(&f, Some(b"real"))?;
    // An update whose source changed keeps the version it would have replaced.
    let mut reader = Cursor::new(&b"fake"[..]);
    let lying = Upload {
        hash: hash_bytes(b"new!", HashAlgorithm::Blake3),
        ..upload(&mut reader)
    };
    ensure_err!(
        b.remote
            .update(at.file_id, at.version, None, &f, Some(lying)),
        RemoteError::HashMismatch
    );
    ensure!(b.read(at.file_id, None)? == b"real", "the old bytes stay");
    b.update(at, &f, Some(b"new!"))?;
    Ok(())
}

fn feed_pages_and_resumes(b: &Bench) -> Outcome {
    let start = b.cursor_now()?;
    let nodes: Vec<NodeMeta> = (0..5)
        .map(|i| folder(&format!("f{i}"), b.scratch))
        .collect();
    let written: Vec<RemoteRef> = nodes
        .iter()
        .map(|n| b.create(n, None))
        .collect::<Result<_, _>>()?;
    let ids: Vec<RemoteFileId> = written.iter().map(|w| w.file_id).collect();
    std::thread::sleep(b.settle);

    // Small pages, resumed by cursor, see each file once and in order.
    let mut cursor = start.clone();
    let mut seen = Vec::new();
    for _ in 0..100 {
        let page = b.remote.changes(Some(&cursor), 2)?;
        ensure!(page.entries.len() <= 2, "page of {}", page.entries.len());
        seen.extend(
            page.entries
                .iter()
                .map(|h| h.file_id)
                .filter(|id| ids.contains(id)),
        );
        cursor = page.cursor;
        if !page.has_more {
            break;
        }
    }
    ensure!(seen == ids, "paged feed: {seen:?}");
    // At the end, the cursor stays usable: nothing new, then only what is new.
    ensure!(b.changes_to(&cursor, &ids)?.is_empty(), "changes repeated");
    let renamed = with(&nodes[0], NodeContent::folder("f0-renamed"));
    b.update(written[0], &renamed, None)?;
    let again = b.changes_to(&cursor, &ids)?;
    ensure!(
        again.len() == 1 && again[0].file_id == written[0].file_id,
        "after an update: {again:?}"
    );
    // From the start, each file appears once, at its latest position.
    let all: Vec<_> = b
        .changes_to(&start, &ids)?
        .iter()
        .map(|h| h.file_id)
        .collect();
    ensure!(all.len() == 5 && all[4] == written[0].file_id, "{all:?}");
    Ok(())
}

/// A client that reads the feed right after a write (its own, or one it was told about)
/// must see it within moments, every time: a feed answer cached from before the write must
/// not hide it (odin-core's query cache did, until the next write to the drive).
fn every_write_shows_up_promptly(b: &Bench) -> Outcome {
    let mut cursor = b.cursor_now()?;
    for i in 0..30 {
        let (_, w) = b.folder(&format!("w{i}"))?;
        let started = std::time::Instant::now();
        loop {
            let page = b.remote.changes(Some(&cursor), 100)?;
            cursor = page.cursor;
            if page.entries.iter().any(|h| h.file_id == w.file_id) {
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(2),
                "write {i} not in the feed after {:?}",
                started.elapsed()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    Ok(())
}

fn ranged_reads(b: &Bench) -> Outcome {
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let (_, w) = b.file("ranged.bin", &data)?;
    for range in [0..1, 1..3, 65_535..65_537, 299_999..300_000, 0..300_000] {
        let got = b.read(w.file_id, Some(range.clone()))?;
        ensure!(
            got == data[range.start as usize..range.end as usize],
            "range {range:?}: {} bytes",
            got.len()
        );
    }
    ensure!(b.read(w.file_id, None)? == data, "whole payload");
    // A range that runs past the end stops there (sizes on and off a 16-byte block).
    for len in [100usize, 112] {
        let (_, w) = b.file(&format!("short-{len}.bin"), &data[..len])?;
        for range in [
            90..110u64,
            96..200,
            0..len as u64 + 1,
            len as u64 - 1..len as u64 + 16,
        ] {
            let got = b.read(w.file_id, Some(range.clone()))?;
            let there = range.end.min(len as u64) as usize;
            ensure!(
                got == data[range.start as usize..there],
                "{len} bytes, range {range:?}: {} bytes",
                got.len()
            );
        }
    }
    Ok(())
}
