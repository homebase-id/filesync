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
//! - notifications: every write is announced to the subscribers as its file's row as each
//!   one's view sees it ([`FakeRemote::subscribe`]), except to those that exclude their own
//!   client's writes (`excludeOwnWrites`, odin-core #1971): each [`FakeRemote::new`] is a
//!   client registration (a token) of its own. A write not made through a client (a test's,
//!   on the server) is announced to all, as Homebase does when it can't tell whose write it is.
//!
//! [`FakeRemote`] is a client of it implementing [`Remote`] the way the Homebase adapter does:
//! a delete refused for its version (4160) is read once more and counts as done when the file
//! is deleted (a retry). Faults and latency are injected per client.
//!
//! [`FakeRemote::member`] is a client of the member the drive's owner shares files with,
//! reading them as the Homebase adapter reads another identity's share (`PeerRemote`;
//! homebase-api-notes, "Shared with you"): only the files shared with it, a share withdrawn as
//! a tombstone without a version, read-only.

use std::cell::OnceCell;
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
    Planned, RefsPage, Refusal, Remote, RemoteBody, RemoteError, RemoteHeader, Result, Skipped,
    Upload,
};

#[derive(Debug, Clone)]
struct StoredFile {
    /// Its `changed_at` is its version's time (Homebase's `versionUpdated`): `modified` as
    /// the write that made the version left it.
    header: RemoteHeader,
    /// `None` for folders and tombstones.
    bytes: Option<Arc<Vec<u8>>>,
    /// When the file last changed (Homebase's `updated`), once written: its version's write,
    /// or a share of it given or withdrawn since, which keeps the version and its time.
    modified: Option<i64>,
    /// Feed position: (`modified`, row id). The row id never changes.
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

/// A notification subscription: what it is told, whose writes it leaves out, and whose view
/// of the drive it hears them in.
#[derive(Debug)]
struct Subscriber {
    events: Sender<ChangeRef>,
    excluded: Option<ClientId>,
    view: View,
}

/// The shared in-memory drive.
#[derive(Debug)]
pub struct FakeServer {
    config: ServerConfig,
    files: HashMap<RemoteFileId, StoredFile>,
    /// Live files only, removed ones included: their node id stays taken, as on Homebase.
    by_node: HashMap<NodeId, RemoteFileId>,
    /// Feed index: (the file's `modified`, row) → file.
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
    /// The member's share rows ([`FakeRemote::member`]): live (`true`) or withdrawn. A hard
    /// delete takes a file's row with it.
    shares: HashMap<RemoteFileId, bool>,
}

/// Whose view of the drive a client has: its owner's, or the member's ([`FakeRemote::member`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Owner,
    Member,
}

/// How a view sees a file's row ([`seen_by`], [`seen`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// The owner: as it is.
    Owner,
    /// The member, shared: as it is, without the file's token.
    Shared,
    /// The member, a share withdrawn: a tombstone without a version.
    Unshared,
}

impl Seen {
    /// Whether the view reads the file: not a share withdrawn.
    fn reads(self) -> bool {
        self != Seen::Unshared
    }
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
            shares: HashMap::new(),
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

    /// A write of `file_id` by `writer` (`None`: not through a client) that made its version
    /// landed: its time, its new place in the feed, and its announcement
    /// ([`FakeServer::changed`]). The version's time is the write's (**[verified]** odin-core
    /// PR #2143 at 5e4cd493f: `versionTagModified` is set to `modified` by every write that
    /// sets the version tag).
    fn touch(&mut self, file_id: RemoteFileId, writer: Option<ClientId>) -> i64 {
        self.changed(file_id, writer, true)
    }

    /// A change of `file_id` by `writer` that keeps its version landed (a share given or
    /// withdrawn, odin-core's `TouchKeepingVersionTagAsync`): [`FakeServer::changed`], the
    /// version's time staying.
    fn touch_keeping_version(&mut self, file_id: RemoteFileId, writer: ClientId) {
        self.changed(file_id, Some(writer), false);
    }

    /// A change of `file_id` by `writer` landed, a write that made its version (`version`) or
    /// one that keeps it (a share given or withdrawn): its new place in the feed, and its
    /// announcement. Returns the change's time: at least one past the file's last change, as
    /// every write on Homebase sets `modified = MAX(modified+1, now)` (**[verified]** odin-core
    /// `TableDriveMainIndex`: upsert, the local app data, a share's touch), so a file's time
    /// never goes back, and a write in the millisecond of the last one still moves it and its
    /// place in the feed. The header's `updated` is that value (**[verified]** `FileMetadata`:
    /// `Updated = record.modified`); its `versionUpdated` moves only with the version.
    fn changed(&mut self, file_id: RemoteFileId, writer: Option<ClientId>, version: bool) -> i64 {
        let mut modified = self.clock_ms;
        if let Some(f) = self.files.get_mut(&file_id) {
            if let Some(last) = f.modified {
                modified = (last + 1).max(modified);
                self.feed.remove(&(last, f.row));
            }
            f.modified = Some(modified);
            if version {
                f.header.changed_at = Some(modified);
            }
            self.feed.insert((modified, f.row), file_id);
            self.announce(file_id, writer);
        }
        if self.config.auto_tick {
            self.tick();
        }
        modified
    }

    /// Announces `file_id`'s row as it is now, written by `writer`. Each subscriber hears it as
    /// its view's feed has it: odin-core sends a member's socket the answer its query gets, the
    /// member nothing of a file no share row names it on (**[verified]**
    /// `DriveEventMessage.MessageForAsync`, `NoLongerSharedViewOf`). Each view's row is built
    /// for its first subscriber.
    fn announce(&mut self, file_id: RemoteFileId, writer: Option<ClientId>) {
        let Some(f) = self.files.get(&file_id) else {
            return;
        };
        let (header, modified, shares) = (&f.header, f.modified, &self.shares);
        let (owner, member) = (OnceCell::new(), OnceCell::new());
        // A subscriber that went away is dropped.
        self.subscribers.retain(|s| {
            if writer.is_some() && s.excluded == writer {
                return true;
            }
            let change = match s.view {
                View::Owner => Some(*owner.get_or_init(|| ChangeRef::of(header))),
                View::Member => *member.get_or_init(|| {
                    seen_by(shares, file_id, View::Member)
                        .map(|how| seen_ref(header, modified, how))
                }),
            };
            change.is_none_or(|c| s.events.send(c).is_ok())
        });
    }

    /// A new client registration.
    fn new_client(&mut self) -> ClientId {
        self.next_client += 1;
        ClientId(self.next_client)
    }

    /// Subscribes to the drive's notifications: every write from now on, as its file's row
    /// as `view` sees it, but those of `excluded` (a client's own writes, with
    /// `excludeOwnWrites`).
    fn subscribe(&mut self, excluded: Option<ClientId>, view: View) -> Receiver<ChangeRef> {
        let (events, rx) = channel();
        self.subscribers.push(Subscriber {
            events,
            excluded,
            view,
        });
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
                let hash = hash_bytes(&bytes, upload.hash.algorithm());
                if !upload.hash.takes(hash) {
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
                    hash,
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
    /// token and the version's time (`versionUpdated`, odin-core #1971 and PR #2143).
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
        let sent = payload.as_ref().map(|(_, info)| info.hash);
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
            // Its time and place in the feed come with the write ([`FakeServer::touch`]).
            changed_at: None,
            token: Some(token_of(file_id)),
        };
        self.files.insert(
            file_id,
            StoredFile {
                header,
                bytes: payload.map(|(b, _)| b),
                modified: None,
                row,
            },
        );
        self.by_node.insert(node.node, file_id);
        let at = self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, at, sent))
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
        let sent = new_payload.as_ref().map(|(_, info)| info.hash);
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.parent = meta.parent;
        f.header.removed = meta.removed;
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
        let at = self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, at, sent))
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
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.body = RemoteBody::Deleted;
        f.header.removed = false;
        f.header.token = None;
        f.bytes = None;
        Ok(Some(self.touch(file_id, writer)))
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
                content: None,
            });
        }
        let version = VersionTag(self.fresh_uuid());
        let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
        f.header.version = version;
        f.header.removed = removed;
        let now = self.touch(file_id, writer);
        Ok(written(RemoteRef { file_id, version }, now, None))
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
        self.shares.remove(&file_id);
        Ok(())
    }

    /// Shares `file_id` with the member (`live`), or withdraws its share: its version and that
    /// version's time stay, its `updated` and place in the feed move, and it is announced as
    /// any write
    /// (**[verified]** odin-core `MainIndexMeta.WriteSharesAsync`,
    /// `TableDriveMainIndex.TouchKeepingVersionTagAsync`, `FileMetadata`'s `Updated`,
    /// `DriveShareService.PublishChangedAsync`). Each refusal is a 400 there, which the adapters
    /// read as fatal (**[verified]** `DriveShareService`): sharing a file not on the drive ("No
    /// file … on the drive") or a tombstone (`CheckFileAndLift`, "deleted or isn't a file"), and
    /// withdrawing a share never given or gone with a file deleted for good ("isn't shared with
    /// circle"). A withdrawal changes nothing of one withdrawn already (`WithdrawShareAsync`). A
    /// deleted file's share is withdrawn, the member reading it no more, but its tombstone keeps
    /// its time, its place in the feed and the age it is purged by; the change is announced at
    /// that time (odin-core PR #2141: `WithdrawShareAsync`, `FileShares.MovesModified`,
    /// `PublishChangedAsync`; **[verified]** in odin-core at aa560aea9).
    fn set_share(&mut self, writer: ClientId, file_id: RemoteFileId, live: bool) -> Result<()> {
        let refused = |why: String| Err(RemoteError::Fatal(why));
        let deleted = match (live, self.files.get(&file_id), self.shares.get(&file_id)) {
            (true, None, _) => return refused(format!("No file {file_id:?} on the drive")),
            (true, Some(f), _) if f.header.is_deleted() => {
                return refused(format!("File {file_id:?} is deleted or isn't a file"));
            }
            (false, _, None) => return refused(format!("{file_id:?} isn't shared with circle")),
            (false, _, Some(false)) => return Ok(()),
            (_, f, _) => f.is_some_and(|f| f.header.is_deleted()),
        };
        self.shares.insert(file_id, live);
        match deleted {
            true => self.announce(file_id, Some(writer)),
            false => self.touch_keeping_version(file_id, writer),
        }
        Ok(())
    }

    /// `file_id`, if `view` may read it, or how it is refused. A file not on the drive (deleted
    /// for good too: the member's share row went with it) is not found, as the header is looked
    /// up before the share rows (**[verified]** odin-core
    /// `DriveStorageServiceBase.ReadForCallerAsync`: no header, no share looked at; the peer
    /// header routes answer none 404, `V2PeerAppDriveReadControllers`). The member
    /// is refused a file not shared with it as Homebase refuses it, with a 403 that has no code
    /// (**[verified]** `DriveStorageServiceBase`, "No access permitted to the file"; the adapter
    /// reads it as fatal).
    fn access(&self, file_id: RemoteFileId, view: View) -> Result<(&StoredFile, Seen)> {
        let Some(file) = self.files.get(&file_id) else {
            return Err(RemoteError::NotFound);
        };
        match seen_by(&self.shares, file_id, view) {
            Some(how) if how.reads() => Ok((file, how)),
            _ => Err(RemoteError::Fatal(format!(
                "{file_id:?}: not shared with you"
            ))),
        }
    }

    /// `file_id`'s header as `view` reads it ([`FakeServer::access`]), refused first if it is
    /// unreadable (a test's).
    fn header_as(&self, file_id: RemoteFileId, view: View) -> Result<RemoteHeader> {
        self.readable(file_id)?;
        let (file, how) = self.access(file_id, view)?;
        Ok(seen(file.header.clone(), file.modified, how))
    }

    /// Refuses a header read of a file made unreadable (a test's).
    fn readable(&self, file_id: RemoteFileId) -> Result<()> {
        if self.unreadable.contains(&file_id) {
            return Err(RemoteError::Fatal(format!(
                "{file_id:?}: unreadable (test)"
            )));
        }
        Ok(())
    }

    /// `file_id`'s payload as `view` reads it ([`FakeServer::access`]). A tombstone's is deleted,
    /// for the owner and the member alike (Homebase's `fileDeleted`, odin-core PR #2141:
    /// homebase-api-notes, Errors). A file without a payload (a folder) has none now for the
    /// member, whose adapter reads the header first, for the key (`PeerRemote::read`,
    /// `payload_key`); the owner's goes by the payload: not there.
    fn payload_as(&self, file_id: RemoteFileId, view: View) -> Result<Arc<Vec<u8>>> {
        let (file, how) = self.access(file_id, view)?;
        match how {
            _ if file.header.is_deleted() => Err(RemoteError::Deleted),
            Seen::Owner => self.payload(file_id),
            _ => file.bytes.clone().ok_or_else(|| {
                RemoteError::StaleRange(format!("file {file_id:?}: no payload now"))
            }),
        }
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
        self.header_as(file_id, View::Owner)
    }

    /// The name of file `file_id`, unless it is a tombstone or missing.
    fn live_name(&self, file_id: RemoteFileId) -> Option<&str> {
        match &self.files.get(&file_id)?.header.body {
            RemoteBody::Live { content, .. } => Some(&content.name),
            RemoteBody::Deleted => None,
        }
    }

    /// Which of `folders` hold a file not deleted that `holds` counts (unlisted ones too), as
    /// `view` sees them: a share withdrawn is a tombstone to the member, holding nothing.
    fn holding(&self, folders: &[NodeId], holds: Holds<'_>, view: View) -> HashSet<NodeId> {
        self.files
            .values()
            .map(|f| &f.header)
            .filter(|h| {
                !h.is_deleted()
                    && holds.counts(h.at(), h.removed)
                    && seen_by(&self.shares, h.file_id, view).is_some_and(Seen::reads)
            })
            .map(|h| h.parent)
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
    fn read(&mut self, cursor: Option<&str>, limit: usize, view: View) -> Result<ChangesPage> {
        let page = self.page(cursor, limit, view)?;
        if page.has_more && page.entries.len() < limit {
            self.tick();
        }
        Ok(page)
    }

    pub fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        self.page(cursor, limit, View::Owner)
    }

    /// A page of the feed as `view` sees it: the rows it doesn't see aren't in it.
    fn page(&self, cursor: Option<&str>, limit: usize, view: View) -> Result<ChangesPage> {
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
        // A file the feed leaves out (unlisted), or one the view doesn't see, isn't in it.
        .filter(|(_, file_id)| !self.unlisted.contains(*file_id))
        .filter_map(|(key, file_id)| Some((key, file_id, seen_by(&self.shares, *file_id, view)?)));
        let mut entries = Vec::new();
        let mut skipped = Vec::new();
        let mut last = after;
        let mut has_more = false;
        for (key, file_id, how) in rows {
            // A row of the current millisecond is held back, with more rows to come (the
            // feed is in time order: so is everything after it).
            if entries.len() + skipped.len() == limit || key.0 >= self.clock_ms {
                has_more = true;
                break;
            }
            let file = &self.files[file_id];
            let header = &file.header;
            if how.reads() && self.unreadable.contains(file_id) && !header.is_deleted() {
                skipped.push(Skipped {
                    at: Some(header.at()),
                    node: header.node(),
                    parent: Some(header.parent),
                    why: format!("{file_id:?}: unreadable (test)"),
                });
            } else {
                entries.push(seen(header.clone(), file.modified, how));
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
            hash: Planned::Known(hash),
            size: data.len() as u64,
            reader: &mut std::io::Cursor::new(data),
            pace: None,
        };
        Ok((node, self.create(&new, Some(upload))?))
    }

    /// Another writer's write as it sent it, its content unchecked (for tests: a buggy or
    /// hostile writer; Homebase stores the header a writer sends **[inferred]**): `meta`, with
    /// `payload`'s bytes stored under its descriptor whatever they are. Over live file `over` (a
    /// full update: the node id and kind change as the header says; without an upload the
    /// payload stays, as [`FakeServer::update`] keeps it), or a new file. Refused as this fake
    /// models Homebase (`update_by`, `receive`): a file deleted answers so first
    /// (`AssertActiveForUpdate`), then a node id another live file has (`AlreadyExists`), and a
    /// new file without a payload where hashes are required (`HashRequired`).
    pub fn write_unchecked(
        &mut self,
        over: Option<RemoteFileId>,
        meta: &NodeMeta,
        payload: Option<(Vec<u8>, PayloadInfo)>,
    ) -> Result<RemoteRef> {
        if let Some(file_id) = over {
            match self.files.get(&file_id).map(|f| f.header.is_deleted()) {
                None => return Err(RemoteError::NotFound),
                Some(true) => return Err(RemoteError::Deleted),
                Some(false) => {}
            }
        }
        if self
            .by_node
            .get(&meta.node)
            .is_some_and(|f| Some(*f) != over)
        {
            return Err(RemoteError::AlreadyExists);
        }
        if over.is_none()
            && meta.kind == NodeKind::File
            && payload.is_none()
            && self.config.require_hashes
        {
            return Err(RemoteError::HashRequired);
        }
        let version = VersionTag(self.fresh_uuid());
        // Without an upload the payload stays (a file made a folder keeps its bytes).
        let kept = over
            .and_then(|f| self.files.get(&f))
            .and_then(|f| match &f.header.body {
                RemoteBody::Live { payload, .. } => payload.map(|info| (f.bytes.clone(), info)),
                RemoteBody::Deleted => None,
            });
        let (bytes, info) = match (payload, kept) {
            (Some((bytes, info)), _) => (Some(Arc::new(bytes)), Some(info)),
            (None, Some((bytes, info))) => (bytes, Some(info)),
            (None, None) => (None, None),
        };
        let header = RemoteHeader {
            file_id: RemoteFileId(Uuid::nil()),
            version,
            parent: meta.parent,
            kind: meta.kind,
            body: RemoteBody::Live {
                node: meta.node,
                content: meta.content.clone(),
                payload: info,
            },
            removed: meta.removed,
            // Its time comes with the write ([`FakeServer::touch`]).
            changed_at: None,
            token: None,
        };
        let file_id = match over {
            Some(file_id) => {
                let f = self.files.get_mut(&file_id).ok_or(RemoteError::NotFound)?;
                if let Some(old) = f.header.node() {
                    self.by_node.remove(&old);
                }
                f.header = RemoteHeader {
                    file_id,
                    token: Some(token_of(file_id)),
                    ..header
                };
                f.bytes = bytes;
                file_id
            }
            None => {
                let file_id = RemoteFileId(self.fresh_uuid());
                let row = self.next_row;
                self.next_row += 1;
                let header = RemoteHeader {
                    file_id,
                    token: Some(token_of(file_id)),
                    ..header
                };
                self.files.insert(
                    file_id,
                    StoredFile {
                        header,
                        bytes,
                        modified: None,
                        row,
                    },
                );
                file_id
            }
        };
        self.by_node.insert(meta.node, file_id);
        self.touch(file_id, None);
        Ok(RemoteRef { file_id, version })
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

    /// The files with a share row for the member ([`FakeRemote::member`]), live or withdrawn.
    pub fn shared(&self) -> impl Iterator<Item = RemoteFileId> + '_ {
        self.shares.keys().copied()
    }

    /// Whether the member reads `file_id`: shared with it, the share not withdrawn.
    pub fn shared_live(&self, file_id: RemoteFileId) -> bool {
        self.shares.get(&file_id) == Some(&true)
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
    /// Creates, updates, deletes and share changes of nodes with this name fail transiently (a
    /// write that keeps failing).
    pub fail_writes_named: Option<String>,
    /// Every request is refused with [`RemoteError::Denied`] (credentials revoked).
    pub denied: bool,
    /// What `denied` refuses for: the drive private and this login without its key
    /// ([`Refusal::NoKey`]), not the credentials.
    pub deny_key: bool,
    /// What `denied` refuses for instead, where it is neither: a sharer's server refusing
    /// its token ([`Refusal::Token`]), the app wanting the owner's approval
    /// ([`Refusal::Permission`]).
    pub deny_as: Option<Refusal>,
    /// Every request is put off with [`RemoteError::Later`]: its reason, and after how long to
    /// ask again (a sharer whose device hasn't opened the share to this identity yet, say).
    pub later: Option<(String, Duration)>,
    /// Creates, updates, deletes and share changes are refused with [`RemoteError::Denied`]
    /// (the app lost write access to the drive); reads still work.
    pub deny_writes: bool,
    /// Creates, updates, deletes and share changes are refused with [`RemoteError::Fatal`] (a
    /// drive that won't take them, read-only); reads still work.
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
    /// Bytes of the payloads taken in whole, for assertions (what was read to send them).
    pub upload_bytes: u64,
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
    /// Whose client it is: the owner's, or the member's ([`FakeRemote::member`]).
    view: View,
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
            view: View::Owner,
        }
    }

    /// A client of the member `server`'s owner shares files with, reading them as the Homebase
    /// adapter reads another identity's share: the feed, headers and payloads of what is shared
    /// with it, a share withdrawn as a tombstone without a version
    /// ([`RemoteHeader::is_unshared`]). Every write refuses before it is a request, as
    /// `PeerRemote`'s do.
    pub fn member(server: Arc<Mutex<FakeServer>>) -> Self {
        FakeRemote {
            view: View::Member,
            ..FakeRemote::new(server)
        }
    }

    /// Another client on this one's registration (a copied credentials file): faults of its
    /// own, the same token.
    pub fn sharing_token(&self) -> Self {
        FakeRemote {
            server: Arc::clone(&self.server),
            faults: Arc::default(),
            client: self.client,
            view: self.view,
        }
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// Subscribes to the drive's notifications: every write from now on, as its file's row as
    /// this client's view sees it (the member's: only what is shared with it, a share withdrawn
    /// as a tombstone without a version), leaving out the writes made with this client's token
    /// if `exclude_own_writes`.
    pub fn subscribe(&self, exclude_own_writes: bool) -> Receiver<ChangeRef> {
        self.server()
            .subscribe(exclude_own_writes.then_some(self.client), self.view)
    }

    pub fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The server it is a client of, to make another client of it. Its own reads
    /// ([`FakeServer::header`], [`FakeServer::changes`], [`FakeServer::payload`],
    /// [`FakeServer::live`]) are the owner's view, whosever client handed it out: a member's
    /// view is read through a [`FakeRemote::member`].
    pub fn server_handle(&self) -> Arc<Mutex<FakeServer>> {
        Arc::clone(&self.server)
    }

    /// The server it is a client of, locked: its reads are the owner's view, as
    /// [`FakeRemote::server_handle`]'s are, on a member's client too.
    pub fn server(&self) -> MutexGuard<'_, FakeServer> {
        self.server.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Refuses a write of the member's: what is shared with it is read-only.
    fn writable(&self) -> Result<()> {
        match self.view {
            View::Owner => Ok(()),
            View::Member => Err(RemoteError::read_only("the owner")),
        }
    }

    /// Runs the per-request fault logic. Returns an error if this request should fail.
    fn gate(&self) -> Result<()> {
        let latency = {
            let mut f = self.faults();
            f.requests += 1;
            if f.denied {
                let refusal = match f.deny_as {
                    Some(refusal) => refusal,
                    None if f.deny_key => Refusal::NoKey,
                    None => Refusal::Credentials,
                };
                return Err(RemoteError::Denied(
                    "injected: not authorized".into(),
                    refusal,
                ));
            }
            if let Some((why, retry_after)) = f.later.clone() {
                return Err(RemoteError::Later { why, retry_after });
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
            return Err(RemoteError::Denied(
                "injected: no write access".into(),
                Refusal::Credentials,
            ));
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
                // What was sent, checked as Homebase checks it once the payload is in: the
                // hash planned, or, hashed on send, a source that didn't change.
                let sent = hash_bytes(&bytes, upload.hash.algorithm());
                if !upload.hash.takes(sent) {
                    return Err(RemoteError::HashMismatch);
                }
                self.faults().upload_bytes += bytes.len() as u64;
                return Ok(Some((sent, bytes)));
            }
            if let Some(pace) = upload.pace {
                pace.take(n);
            }
            bytes.extend_from_slice(&buf[..n]);
        }
    }
}

/// The file written at `at` at server time `changed_at`, with its token and the hash of the
/// bytes it took, if any.
fn written(at: RemoteRef, changed_at: i64, content: Option<hbsync_types::ContentHash>) -> Written {
    Written {
        target: Target {
            at,
            token: Some(token_of(at.file_id)),
        },
        changed_at: Some(changed_at),
        content,
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
        hash: Planned::Known(hash),
        size: bytes.len() as u64,
        reader: &mut std::io::Cursor::new(bytes.as_slice()),
        pace: None,
    }))
}

impl Remote for FakeRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        self.gate()?;
        self.faults().full_pages += 1;
        self.server().read(cursor, limit, self.view)
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
            ..RefsPage::of(&self.server().read(cursor, limit, self.view)?)
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
        self.server().header_as(file_id, self.view)
    }

    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        self.gate()?;
        Ok(self.server().holding(folders, holds, self.view))
    }

    fn shared(&self) -> bool {
        self.view == View::Member
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Written> {
        self.writable()?;
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
        self.writable()?;
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
        self.writable()?;
        Answers::in_batches(files, BATCH_FILES, |batch| self.delete_batch(batch))
    }

    /// A request per [`BATCH_FILES`] files, as Homebase's `hard-delete-batch`, refused by the
    /// faults on every write as a delete is: Homebase checks the caller may write to the drive
    /// first (**[verified]** odin-core
    /// `V2DriveBatchFileWriteOperationsController.HardDeleteBatch`: `AssertCanWriteToDrive`).
    fn delete_for_good(&self, files: &[Doomed]) -> Result<ForGood> {
        self.writable()?;
        Answers::in_batches(files, BATCH_FILES, |batch| {
            self.gate()?;
            batch_of(batch.iter().map(|f| f.at().file_id))?;
            // The faults on every write (no file is named so).
            self.gate_name("")?;
            let failing = self.faults().fail_writes_named.clone();
            let mut server = self.server();
            Ok(batch
                .iter()
                .map(|f| {
                    let RemoteRef { file_id, version } = f.at();
                    match failing.is_some() && server.live_name(file_id) == failing.as_deref() {
                        true => Err(RemoteError::Transient("injected write failure".into())),
                        false => server.hard_delete(file_id, version),
                    }
                })
                .collect())
        })
    }

    /// One request: a refusal of all writes, or an answer lost (after the server wrote the
    /// rest), is the request's. A write failure injected for a name
    /// ([`Faults::fail_writes_named`]) fails that file alone, unwritten: a lever of the fake's
    /// (Homebase answers a file only with a client error) to hold one file back.
    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Written>> {
        self.writable()?;
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
        let bytes = self.server().payload_as(file_id, self.view)?;
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

/// The sharer's share rows for the member, through its own client.
impl crate::contract::Shares for FakeRemote {
    fn share(&self, file_id: RemoteFileId) -> Result<()> {
        self.set_share(file_id, true)
    }

    fn withdraw(&self, file_id: RemoteFileId) -> Result<()> {
        self.set_share(file_id, false)
    }
}

impl FakeRemote {
    /// Shares `file_id` (`live`) or withdraws its share, refused by the faults on every write
    /// first, as Homebase checks the caller may write to the drive before it looks at the file
    /// (**[verified]** odin-core `DriveShareService.WriteSharesAsync`, `WithdrawShareAsync`:
    /// `AssertCallerMayWrite`).
    fn set_share(&self, file_id: RemoteFileId, live: bool) -> Result<()> {
        self.writable()?;
        self.gate()?;
        let name = self
            .server()
            .live_name(file_id)
            .unwrap_or_default()
            .to_owned();
        self.gate_name(&name)?;
        self.server().set_share(self.client, file_id, live)
    }
}

/// How `view` sees `file_id`'s row, if at all, by the member's share rows `shares`: the owner
/// every row as it is, the member only the rows shared with it.
fn seen_by(
    shares: &HashMap<RemoteFileId, bool>,
    file_id: RemoteFileId,
    view: View,
) -> Option<Seen> {
    match view {
        View::Owner => Some(Seen::Owner),
        View::Member => shares
            .get(&file_id)
            .map(|&live| if live { Seen::Shared } else { Seen::Unshared }),
    }
}

/// `header` of a file last changed at `modified`, as it is seen `how` ([`seen_by`]): the member
/// sees no file's token, and a share withdrawn as a tombstone without a version, `groupId`,
/// tags or key (the root its parent, as the Homebase adapter reads a tombstone without a
/// `groupId`), at the file's last change (the withdrawal: it has no version, so no time of
/// one).
fn seen(header: RemoteHeader, modified: Option<i64>, how: Seen) -> RemoteHeader {
    match how {
        Seen::Owner => header,
        Seen::Shared => RemoteHeader {
            token: None,
            ..header
        },
        Seen::Unshared => RemoteHeader {
            version: VersionTag(Uuid::nil()),
            parent: NodeId::ROOT,
            body: RemoteBody::Deleted,
            removed: false,
            changed_at: modified,
            token: None,
            ..header
        },
    }
}

/// The ref of `header` as it is seen `how` ([`seen_by`]): [`ChangeRef::of`] its [`seen`]
/// header, without building that header (a notification's, built for every write).
fn seen_ref(header: &RemoteHeader, modified: Option<i64>, how: Seen) -> ChangeRef {
    let as_is = ChangeRef::of(header);
    match how {
        Seen::Owner | Seen::Shared => as_is,
        Seen::Unshared => ChangeRef {
            version: VersionTag(Uuid::nil()),
            deleted: true,
            parent: None,
            removed: false,
            changed_at: modified,
            ..as_is
        },
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

    /// A notice of a file is the ref of the header its view's feed has, however it is seen.
    #[test]
    fn a_members_ref_is_its_headers() {
        let header = RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(1)),
            version: VersionTag(Uuid::from_u128(2)),
            parent: NodeId::new(),
            kind: NodeKind::File,
            body: RemoteBody::Deleted,
            removed: true,
            changed_at: Some(1_000),
            token: None,
        };
        let live = RemoteHeader {
            body: RemoteBody::Live {
                node: NodeId::new(),
                content: NodeContent::file("f", 1, 1, false),
                payload: None,
            },
            token: Some(token_of(header.file_id)),
            ..header.clone()
        };
        for h in [header, live] {
            for how in [Seen::Owner, Seen::Shared, Seen::Unshared] {
                assert_eq!(
                    seen_ref(&h, Some(2_000), how),
                    ChangeRef::of(&seen(h.clone(), Some(2_000), how)),
                    "{how:?} {h:?}"
                );
            }
        }
    }

    /// Every write in the millisecond of the file's last change still moves its time and its
    /// place in the feed, one past it, and the header and the answer carry that one time:
    /// odin-core's `MAX(modified+1, now)` (**[verified]** `TableDriveMainIndex`). A share
    /// given moves the place and keeps the version's time.
    #[test]
    fn a_write_in_the_last_changes_millisecond_moves_the_file() -> Result<()> {
        let server = FakeServer::new(ServerConfig {
            auto_tick: false,
            ..ServerConfig::default()
        });
        let sharer = FakeRemote::new(server.clone());
        let mut s = server.lock().unwrap_or_else(|p| p.into_inner());
        let folder = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            content: NodeContent::folder("shared"),
            removed: false,
        };
        let created = s.create_by(None, &folder, None)?;
        let file_id = created.at().file_id;
        // The file's last change, and its place in the feed.
        let at = |s: &FakeServer| {
            let modified = s.files[&file_id].modified;
            let row = s.files[&file_id].row;
            let place = s.feed.iter().find(|(_, f)| **f == file_id).map(|(k, _)| *k);
            (modified, place == modified.map(|t| (t, row)))
        };
        let version_at = |s: &FakeServer| s.files[&file_id].header.changed_at;
        let mut time = s.clock_ms;
        assert_eq!(created.changed_at, Some(time));
        assert_eq!(at(&s), (Some(time), true));
        let updated = s.update_by(None, file_id, created.at().version, &folder, None)?;
        time += 1;
        assert_eq!(
            (updated.changed_at, at(&s)),
            (Some(time), (Some(time), true))
        );
        let removed = s.set_removed_by(None, file_id, updated.at().version, true)?;
        time += 1;
        assert_eq!(
            (removed.changed_at, at(&s)),
            (Some(time), (Some(time), true))
        );
        s.set_share(sharer.client(), file_id, true)?;
        time += 1;
        assert_eq!(at(&s), (Some(time), true));
        assert_eq!(
            version_at(&s),
            removed.changed_at,
            "the version's time stays"
        );
        let deleted = s.delete_by(None, file_id, removed.at().version)?;
        time += 1;
        assert_eq!((deleted, at(&s)), (Some(time), (Some(time), true)));
        // Withdrawing a deleted file's share keeps the tombstone's time (odin-core PR #2141).
        s.set_share(sharer.client(), file_id, false)?;
        assert_eq!(at(&s), (Some(time), true));
        assert_eq!(
            s.shares.get(&file_id),
            Some(&false),
            "withdrawn all the same"
        );
        assert_eq!(s.feed.keys().last().map(|k| k.0), Some(time));
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
