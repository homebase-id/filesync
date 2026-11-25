//! The behavior every [`Remote`] must have, as runnable scenarios: the fake runs them in CI,
//! the Homebase adapter against a live identity.
//!
//! The scenarios only use the trait, and only inside a scratch folder of their own, so they
//! can run on a drive that holds other files. Everything they create is deleted by
//! [`Bench::cleanup`].
//!
//! A read-only remote (another identity's share, as a member reads it) runs a subset
//! ([`Reach`], [`Bench::shared`]): the sharer writes and shares, the member reads.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;

use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_types::schema::NodeContent;
use hbsync_types::{
    Doomed, FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef, Tag, Tags, Target,
};

use crate::{
    ChangeRef, ChangesPage, Holds, NodeMeta, Planned, RefsPage, Remote, RemoteBody, RemoteError,
    RemoteHeader, Upload,
};

/// `files` to delete, unchecked ([`Doomed::unchecked`]): what a test wrote itself.
pub fn doomed(files: &[RemoteRef]) -> Vec<Doomed> {
    files.iter().copied().map(Doomed::unchecked).collect()
}

pub type Outcome = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A page of the feed, whole ([`ChangesPage`]) or slim ([`RefsPage`]), for [`rows_after`].
pub trait FeedPage {
    type Row;

    /// Its rows, cursor and whether more follow; an error when it left rows out (skipped
    /// whole, unread slim): a scenario would miss them.
    fn rows(self) -> Result<(Vec<Self::Row>, String, bool), RemoteError>;
}

impl FeedPage for ChangesPage {
    type Row = RemoteHeader;

    fn rows(self) -> Result<(Vec<RemoteHeader>, String, bool), RemoteError> {
        if !self.skipped.is_empty() {
            return Err(RemoteError::Fatal(format!(
                "feed rows skipped: {:?}",
                self.skipped
            )));
        }
        Ok((self.entries, self.cursor, self.has_more))
    }
}

impl FeedPage for RefsPage {
    type Row = ChangeRef;

    fn rows(self) -> Result<(Vec<ChangeRef>, String, bool), RemoteError> {
        if self.unread != 0 {
            return Err(RemoteError::Fatal(format!(
                "{} feed rows not read slim",
                self.unread
            )));
        }
        Ok((self.refs, self.cursor, self.has_more))
    }
}

/// Every row of the feed after `cursor` (from the start without one) once writes `settle`d,
/// read by `page` (a cursor and a limit) 100 at a time until no more follow, and the cursor
/// after them.
pub fn rows_after<P: FeedPage>(
    settle: Duration,
    cursor: Option<&str>,
    mut page: impl FnMut(Option<&str>, usize) -> Result<P, RemoteError>,
) -> Result<(Vec<P::Row>, String), RemoteError> {
    std::thread::sleep(settle);
    let mut cursor = cursor.map(str::to_owned);
    let mut rows = Vec::new();
    loop {
        let (more, next, has_more) = page(cursor.as_deref(), 100)?.rows()?;
        rows.extend(more);
        if !has_more {
            return Ok((rows, next));
        }
        cursor = Some(next);
    }
}

/// A sharer's share rows for the member a shared bench reads as ([`Bench::shared`]).
pub trait Shares: Sync {
    /// Shares `file_id` with the member for reading, or shares it again; its version stays.
    fn share(&self, file_id: RemoteFileId) -> Result<(), RemoteError>;

    /// Shares `file_id` with the member for editing (or shares it again so): it may change
    /// the file, and add to it where it is a folder.
    fn share_to_edit(&self, file_id: RemoteFileId) -> Result<(), RemoteError>;

    /// Withdraws the member's share of `file_id`; its version stays.
    fn withdraw(&self, file_id: RemoteFileId) -> Result<(), RemoteError>;
}

/// A sharer: one client that both writes the drive and shares its files, so a shared bench
/// can't write to one drive and share on another ([`Bench::shared`]).
pub trait Sharer: Remote + Shares {}

impl<T: Remote + Shares + ?Sized> Sharer for T {}

/// The member a shared bench reads as, and the sharer's share rows for it.
#[derive(Clone, Copy)]
struct Shared<'a> {
    member: &'a dyn Remote,
    shares: &'a dyn Shares,
    /// What the bench creates is shared for editing, and the member writes
    /// ([`Bench::editing`]).
    editing: bool,
}

/// A remote under test and the scratch folder the scenarios work in.
pub struct Bench<'a> {
    /// What the scenarios write through.
    pub remote: &'a dyn Remote,
    /// On a shared bench, the member the scenarios read as and the share rows of `remote` (the
    /// sharer) for it: every file the bench creates is shared.
    shared: Option<Shared<'a>>,
    scratch: NodeId,
    /// How long a write takes to show up in the feed (Homebase: rows of the current
    /// millisecond are held back).
    settle: Duration,
    created: Mutex<Vec<RemoteFileId>>,
    /// Each file's token from its last write, as the engine keeps it.
    tokens: Mutex<HashMap<RemoteFileId, FileToken>>,
    /// The feed's end before the scratch folder was made: every change of the run comes after it.
    start: String,
    /// The furthest feed end read so far, so finding the end reads only what came since (a
    /// shared drive's whole feed can be long).
    end: Mutex<String>,
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

/// Which benches a scenario runs on ([`Bench::run_all`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// Every bench: what a member reads of what is shared with it as the owner reads it.
    Both,
    /// A bench of the drive's owner only (writes, and what only a writer sees).
    OwnerOnly,
    /// A shared bench only ([`Bench::shared`]): what sharing does.
    SharedOnly,
    /// A bench whose member writes only ([`Bench::editing`]): what it may change of what is
    /// shared with it for editing.
    EditingOnly,
}

pub const SCENARIOS: &[(&str, Reach, Scenario)] = &[
    ("create_read_and_feed", Reach::Both, create_read_and_feed),
    ("empty_file", Reach::Both, empty_file),
    (
        "retried_create_and_recreate_after_delete",
        Reach::OwnerOnly,
        retried_create_and_recreate_after_delete,
    ),
    ("guarded_updates", Reach::OwnerOnly, guarded_updates),
    ("content_round_trips", Reach::Both, content_round_trips),
    ("deletes", Reach::OwnerOnly, deletes),
    (
        "deletes_in_one_request",
        Reach::OwnerOnly,
        deletes_in_one_request,
    ),
    ("deletes_for_good", Reach::OwnerOnly, deletes_for_good),
    ("folder_holdings", Reach::Both, folder_holdings),
    ("tombstone_in_the_feed", Reach::Both, tombstone_in_the_feed),
    (
        "removal_keeps_the_bytes_and_restores",
        Reach::Both,
        removal_keeps_the_bytes_and_restores,
    ),
    (
        "removals_in_one_request",
        Reach::OwnerOnly,
        removals_in_one_request,
    ),
    (
        "a_removed_node_deleted_for_good",
        Reach::Both,
        a_removed_node_deleted_for_good,
    ),
    (
        "hash_mismatch_stores_nothing",
        Reach::OwnerOnly,
        hash_mismatch_stores_nothing,
    ),
    ("hashed_on_send", Reach::OwnerOnly, hashed_on_send),
    (
        "feed_pages_and_resumes",
        Reach::Both,
        feed_pages_and_resumes,
    ),
    (
        "slim_pages_match_whole_ones",
        Reach::Both,
        slim_pages_match_whole_ones,
    ),
    (
        "every_write_shows_up_promptly",
        Reach::Both,
        every_write_shows_up_promptly,
    ),
    ("ranged_reads", Reach::Both, ranged_reads),
    (
        "the_sharer_adds_to_a_shared_folder",
        Reach::SharedOnly,
        the_sharer_adds_to_a_shared_folder,
    ),
    (
        "a_child_of_a_folder_not_shared",
        Reach::SharedOnly,
        a_child_of_a_folder_not_shared,
    ),
    (
        "what_isnt_shared_is_refused",
        Reach::SharedOnly,
        what_isnt_shared_is_refused,
    ),
    (
        "a_withdrawn_share_is_a_tombstone_without_a_version",
        Reach::SharedOnly,
        a_withdrawn_share_is_a_tombstone_without_a_version,
    ),
    (
        "a_share_again_comes_back_at_its_version",
        Reach::SharedOnly,
        a_share_again_comes_back_at_its_version,
    ),
    (
        "a_file_deleted_for_good_is_gone",
        Reach::SharedOnly,
        a_file_deleted_for_good_is_gone,
    ),
    (
        "the_member_writes_nothing",
        Reach::SharedOnly,
        the_member_writes_nothing,
    ),
    (
        "a_member_holds_only_what_is_shared",
        Reach::SharedOnly,
        a_member_holds_only_what_is_shared,
    ),
    (
        "a_member_changes_what_it_may_edit",
        Reach::EditingOnly,
        a_member_changes_what_it_may_edit,
    ),
    (
        "a_member_adds_to_a_folder_it_may_edit",
        Reach::EditingOnly,
        a_member_adds_to_a_folder_it_may_edit,
    ),
    (
        "a_member_removes_and_puts_back",
        Reach::EditingOnly,
        a_member_removes_and_puts_back,
    ),
    (
        "a_member_deletes_nothing",
        Reach::EditingOnly,
        a_member_deletes_nothing,
    ),
    (
        "what_is_shared_to_read_is_not_changed",
        Reach::EditingOnly,
        what_is_shared_to_read_is_not_changed,
    ),
];

impl<'a> Bench<'a> {
    /// Creates a scratch folder named `name` at the top of the drive.
    pub fn new(remote: &'a dyn Remote, name: &str, settle: Duration) -> Result<Self, RemoteError> {
        Bench::of(remote, None, name, settle)
    }

    /// A bench where `sharer` writes and shares, and `member` (a member of what it shares)
    /// reads: the scenarios not [`Reach::OwnerOnly`]. The scratch folder is shared.
    pub fn shared(
        sharer: &'a dyn Sharer,
        member: &'a dyn Remote,
        name: &str,
        settle: Duration,
    ) -> Result<Self, RemoteError> {
        Bench::shared_as(sharer, member, false, name, settle)
    }

    /// A bench where `sharer` writes and shares for editing, and `member` writes to what is
    /// shared with it: the [`Reach::EditingOnly`] scenarios. The scratch folder is shared for
    /// editing.
    pub fn editing(
        sharer: &'a dyn Sharer,
        member: &'a dyn Remote,
        name: &str,
        settle: Duration,
    ) -> Result<Self, RemoteError> {
        Bench::shared_as(sharer, member, true, name, settle)
    }

    fn shared_as(
        sharer: &'a dyn Sharer,
        member: &'a dyn Remote,
        editing: bool,
        name: &str,
        settle: Duration,
    ) -> Result<Self, RemoteError> {
        let shared = Shared {
            member,
            shares: sharer,
            editing,
        };
        Bench::of(sharer, Some(shared), name, settle)
    }

    fn of(
        remote: &'a dyn Remote,
        shared: Option<Shared<'a>>,
        name: &str,
        settle: Duration,
    ) -> Result<Self, RemoteError> {
        let scratch = folder(name, NodeId::ROOT);
        // The feed's end as the scenarios read it ([`Bench::reader`]).
        let start = feed_end(shared.map_or(remote, |s| s.member), None)?;
        let bench = Bench {
            remote,
            shared,
            scratch: scratch.node,
            settle,
            created: Mutex::default(),
            tokens: Mutex::default(),
            end: Mutex::new(start.clone()),
            start,
        };
        let at = bench.create_unshared(&scratch, None)?;
        // A scratch folder that can't be shared is deleted again: nothing left on the drive.
        if let Err(e) = bench.share(at.file_id) {
            if let Err(left) = bench.cleanup() {
                eprintln!("cleanup of the scratch folder not shared: {left}");
            }
            return Err(e);
        }
        Ok(bench)
    }

    /// The feed's end before the scratch folder was made, as the scenarios read it.
    pub fn start(&self) -> &str {
        &self.start
    }

    /// Runs every scenario that reaches this bench ([`Reach`]); returns the names and errors of
    /// the ones that failed.
    pub fn run_all(&self) -> Vec<(&'static str, String)> {
        let reached = |reach: Reach| match self.shared {
            None => matches!(reach, Reach::Both | Reach::OwnerOnly),
            Some(shared) if shared.editing => reach == Reach::EditingOnly,
            Some(_) => matches!(reach, Reach::Both | Reach::SharedOnly),
        };
        SCENARIOS
            .iter()
            .filter(|(_, reach, _)| reached(*reach))
            .filter_map(|(name, _, run)| run(self).err().map(|e| (*name, e.to_string())))
            .collect()
    }

    /// What the scenarios read through: `remote`, or on a shared bench the member reading what
    /// it shares.
    pub fn reader(&self) -> &'a dyn Remote {
        self.shared.map_or(self.remote, |s| s.member)
    }

    /// Deletes for good everything the scenarios (and the scratch folder) left, tombstones
    /// too: nothing of them stays in the feed. Each file at the version its header has: one
    /// answered as written since (a header read that raced a write still landing, as under a
    /// whole live suite's load) is read and deleted again, a few times.
    pub fn cleanup(&self) -> Result<(), RemoteError> {
        let mut files =
            std::mem::take(&mut *self.created.lock().unwrap_or_else(|e| e.into_inner()));
        for round in 0..4 {
            let mut left = Vec::new();
            for file_id in files {
                match self.remote.header(file_id) {
                    Ok(h) => left.push(RemoteRef {
                        file_id,
                        version: h.version,
                    }),
                    Err(RemoteError::NotFound) => {}
                    Err(e) => return Err(e),
                }
            }
            if left.is_empty() {
                return Ok(());
            }
            let answers = self
                .remote
                .delete_for_good(&doomed(&left))?
                .into_answers()?;
            files = Vec::new();
            for (at, result) in left.iter().zip(answers) {
                match result {
                    Ok(()) | Err(RemoteError::NotFound) => {}
                    Err(RemoteError::VersionMismatch) if round < 3 => files.push(at.file_id),
                    Err(e) => return Err(e),
                }
            }
            if files.is_empty() {
                return Ok(());
            }
            eprintln!(
                "cleanup: {} files written since their header was read; reading them again",
                files.len()
            );
            std::thread::sleep(Duration::from_secs(1));
        }
        Ok(())
    }

    /// A create, remembered for cleanup; on a shared bench, shared.
    pub fn create(&self, meta: &NodeMeta, data: Option<&[u8]>) -> Result<RemoteRef, RemoteError> {
        let at = self.create_unshared(meta, data)?;
        self.share(at.file_id)?;
        Ok(at)
    }

    /// A create, remembered for cleanup, not shared by the bench: shared all the same where
    /// it goes into a shared folder, as the sharer's client shares what it adds like the
    /// folder it adds to ([`the_sharer_adds_to_a_shared_folder`]).
    fn create_unshared(
        &self,
        meta: &NodeMeta,
        data: Option<&[u8]>,
    ) -> Result<RemoteRef, RemoteError> {
        let written = with_upload(data, |upload| self.remote.create(meta, upload))?;
        self.track(written.at().file_id);
        Ok(self.keep(written.target))
    }

    /// Shares `file_id` with the member, on a shared bench: for editing where the member
    /// writes ([`Bench::editing`]).
    fn share(&self, file_id: RemoteFileId) -> Result<(), RemoteError> {
        match self.shared {
            Some(s) if s.editing => s.shares.share_to_edit(file_id),
            Some(s) => s.shares.share(file_id),
            None => Ok(()),
        }
    }

    /// Keeps a write's token for the next update of its file.
    fn keep(&self, written: Target) -> RemoteRef {
        let mut tokens = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
        match written.token {
            Some(t) => tokens.insert(written.at.file_id, t),
            None => tokens.remove(&written.at.file_id),
        };
        written.at
    }

    /// The token file `file_id`'s last write returned.
    fn token(&self, file_id: RemoteFileId) -> Option<FileToken> {
        let tokens = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
        tokens.get(&file_id).cloned()
    }

    /// The file at `at` to write again, with the token its last write returned.
    pub fn target(&self, at: RemoteRef) -> Target {
        Target {
            at,
            token: self.token(at.file_id),
        }
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

    /// An update with the token of the file's last write, as the engine has it.
    fn update(
        &self,
        at: RemoteRef,
        meta: &NodeMeta,
        data: Option<&[u8]>,
    ) -> Result<RemoteRef, RemoteError> {
        let target = self.target(at);
        let written = with_upload(data, |upload| self.remote.update(&target, meta, upload))?;
        Ok(self.keep(written.target))
    }

    /// Deletes one file at `at` ([`Remote::delete`]): its answer, the tombstone's time.
    pub fn delete(&self, at: RemoteRef) -> Result<Option<i64>, RemoteError> {
        self.remote.delete(&doomed(&[at]))?.single()
    }

    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Vec<u8>, RemoteError> {
        read_all(self.reader(), file_id, range)
    }

    /// A create by the member, remembered for cleanup: made wrongly, it is still cleaned up.
    fn member_create(
        &self,
        meta: &NodeMeta,
        data: Option<&[u8]>,
    ) -> Result<hbsync_types::Written, RemoteError> {
        let made = with_upload(data, |upload| self.reader().create(meta, upload));
        if let Ok(written) = &made {
            self.track(written.at().file_id);
        }
        made
    }

    /// The feed's end: a cursor after every change so far.
    pub fn cursor_now(&self) -> Result<String, RemoteError> {
        std::thread::sleep(self.settle);
        let mut end = self.end.lock().unwrap_or_else(|e| e.into_inner());
        *end = feed_end(self.reader(), Some(&end))?;
        Ok(end.clone())
    }

    /// Every change after `cursor` (all pages) and the cursor after them.
    pub fn changes_after(
        &self,
        cursor: Option<&str>,
    ) -> Result<(Vec<RemoteHeader>, String), RemoteError> {
        rows_after(self.settle, cursor, |at, limit| {
            self.reader().changes(at, limit)
        })
    }

    /// Every change after `cursor` read slim ([`Remote::change_refs`], all pages).
    fn refs_after(&self, cursor: &str) -> Result<Vec<ChangeRef>, RemoteError> {
        rows_after(self.settle, Some(cursor), |at, limit| {
            self.reader().change_refs(at, limit)
        })
        .map(|(refs, _)| refs)
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

pub use crate::feed_end;

/// `data` as an upload with its BLAKE3 hash.
pub fn upload<'a>(reader: &'a mut Cursor<&[u8]>) -> Upload<'a> {
    let data: &[u8] = reader.get_ref();
    Upload {
        hash: Planned::Known(hash_bytes(data, HashAlgorithm::Blake3)),
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
        removed: false,
    }
}

pub fn file(name: &str, parent: NodeId, data: &[u8]) -> NodeMeta {
    NodeMeta {
        node: NodeId::new(),
        parent,
        kind: NodeKind::File,
        content: NodeContent::file(name, 1_700_000_000_123, data.len() as u64, false),
        removed: false,
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

    let header = b.reader().header(written.file_id)?;
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
    // A write returns the token the writer's header gives for its version, so the engine has
    // it at once: one written, one updated. The member's header has none: the writer's is read.
    let token = match b.shared {
        Some(_) => b.remote.header(written.file_id)?.token,
        None => header.token,
    };
    ensure!(
        token.is_some() && token == b.token(written.file_id),
        "the create's token isn't the header's"
    );
    let renamed = b.update(
        written,
        &with(&f, NodeContent::file("b.txt", 1, 5, false)),
        None,
    )?;
    ensure!(
        b.remote.header(renamed.file_id)?.token == b.token(renamed.file_id),
        "the update's token isn't the header's"
    );
    Ok(())
}

fn empty_file(b: &Bench) -> Outcome {
    let (_, written) = b.file("empty", b"")?;
    let state = b
        .reader()
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
    b.delete(first)?;
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
    let RemoteBody::Live { content: back, .. } = b.reader().header(written.file_id)?.body else {
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
    ensure_err!(b.delete(w), RemoteError::VersionMismatch);
    ensure!(!b.remote.header(w.file_id)?.is_deleted(), "deleted anyway");
    b.delete(w2)?;
    ensure!(b.remote.header(w.file_id)?.is_deleted(), "not deleted");
    // A retried delete succeeds.
    b.delete(w2)?;
    // An update of a deleted file fails, header only or with new bytes (another client
    // deleted it meanwhile).
    ensure_err!(
        b.update(w2, &f, None),
        RemoteError::NotFound | RemoteError::Deleted | RemoteError::VersionMismatch
    );
    ensure_err!(
        b.update(w2, &f, Some(b"edited")),
        RemoteError::NotFound | RemoteError::Deleted | RemoteError::VersionMismatch
    );
    ensure_err!(b.read(w.file_id, None), RemoteError::Deleted);
    Ok(())
}

/// Deletes in one request ([`Remote::delete`]): each file at its version, an answer per file
/// in order, one file's answer never stopping the others. A live file goes, answering its
/// tombstone's time as the feed shows it; one written since stays; a tombstone answers as
/// deleted whatever the version asked, with its own time, left as it is; a file the drive
/// doesn't have answers not found. A batch naming a file twice is refused whole, nothing
/// deleted.
fn deletes_in_one_request(b: &Bench) -> Outcome {
    let (_, w) = b.file("f", b"1")?;
    let (g, v) = b.file("g", b"2")?;
    let v2 = b.update(v, &with(&g, NodeContent::file("g", 2, 1, false)), None)?;
    let (_, t) = b.file("t", b"3")?;
    let tomb_at = b.delete(t)?;
    let tomb = b.remote.header(t.file_id)?;
    let (_, x) = b.file("x", b"4")?;
    ensure!(
        b.remote.delete(&doomed(&[x, x])).is_err() && !b.remote.header(x.file_id)?.is_deleted(),
        "a batch naming a file twice went through"
    );
    let missing = RemoteRef {
        file_id: RemoteFileId(NodeId::new().0),
        version: w.version,
    };
    let cursor = b.cursor_now()?;
    let answers = b
        .remote
        .delete(&doomed(&[w, v, t, missing, x]))?
        .into_answers()?;
    ensure!(answers.len() == 5, "{} answers for 5 files", answers.len());
    ensure_err!(&answers[1], RemoteError::VersionMismatch);
    ensure_err!(&answers[3], RemoteError::NotFound);
    let (Ok(gone_w), Ok(again_t), Ok(gone_x)) = (&answers[0], &answers[2], &answers[4]) else {
        return Err(format!("not deleted: {answers:?}").into());
    };
    let header = b.remote.header(v.file_id)?;
    ensure!(
        !header.is_deleted() && header.version == v2.version,
        "the file written since was deleted: {header:?}"
    );
    let header = b.remote.header(t.file_id)?;
    ensure!(
        header.version == tomb.version && *again_t == tomb_at && tomb_at == tomb.changed_at,
        "the tombstone moved, or answered another time: {again_t:?} for {tomb:?}, now {header:?}"
    );
    let feed = b.changes_to(&cursor, &[w.file_id, x.file_id])?;
    ensure!(feed.len() == 2, "{} feed rows for 2 deletes", feed.len());
    for (at, answered) in [(w, gone_w), (x, gone_x)] {
        let header = b.remote.header(at.file_id)?;
        ensure!(header.is_deleted(), "not deleted: {header:?}");
        let row = feed.iter().find(|h| h.file_id == at.file_id);
        ensure!(
            row.is_some_and(|row| row.is_deleted()
                && row.version == header.version
                && answered.is_some()
                && row.changed_at == *answered),
            "the answer isn't the feed's: {answered:?}, {row:?}"
        );
    }
    Ok(())
}

/// A folder holds a live file while a live, not removed file is in it: a removed one or a
/// tombstone doesn't count, nor one in another folder. It holds any file while one that isn't
/// deleted is in it, removed too, a tombstone not; but the files named at their versions (one
/// written since counts). Several folders are asked at once.
fn folder_holdings(b: &Bench) -> Outcome {
    let (dir, _) = b.folder("children")?;
    let (other, _) = b.folder("other")?;
    let (empty, _) = b.folder("empty")?;
    let folders = [dir.node, other.node, empty.node];
    let none = HashSet::new();
    let ask = |holds: Holds<'_>, expected: &[NodeId], why: &str| -> Outcome {
        let mut got: Vec<NodeId> = b.reader().holding(&folders, holds)?.into_iter().collect();
        got.sort();
        let mut want = expected.to_vec();
        want.sort();
        ensure!(got == want, "{why}, {holds:?}: {got:?}");
        Ok(())
    };
    let holding = |live: &[NodeId], any: &[NodeId], why: &str| -> Outcome {
        std::thread::sleep(b.reader().hold_back());
        ask(Holds::Live, live, why)?;
        ask(Holds::AnyBut(&none), any, why)
    };
    holding(&[], &[], "empty folders")?;
    let f = file("f", dir.node, b"1");
    let w = b.create(&f, Some(b"1"))?;
    let g = b.create(&file("g", other.node, b"2"), Some(b"2"))?;
    let both = [dir.node, other.node];
    holding(&both, &both, "a live file in each")?;
    ask(Holds::AnyBut(&HashSet::from([g])), &[dir.node], "all but g")?;
    let gone = b.update(w, &removed(&f), None)?;
    holding(&[other.node], &both, "only a removed one in dir")?;
    ask(
        Holds::AnyBut(&HashSet::from([gone])),
        &[other.node],
        "all but f",
    )?;
    // Named at the version it had before: written since, so it counts.
    ask(
        Holds::AnyBut(&HashSet::from([w])),
        &both,
        "all but f's last version",
    )?;
    let w = b.update(gone, &f, None)?;
    holding(&both, &both, "restored")?;
    b.delete(w)?;
    holding(&[other.node], &[other.node], "only a tombstone in dir")?;
    Ok(())
}

/// A delete for good leaves nothing, in the feed either, each file only at its version: a live
/// file and a tombstone alike; one written since stays; a retry finds it gone.
fn deletes_for_good(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"1")?;
    let w2 = b.update(w, &with(&f, NodeContent::file("f", 2, 1, false)), None)?;
    let (_, t) = b.file("t", b"2")?;
    b.delete(t)?;
    let tomb = RemoteRef {
        file_id: t.file_id,
        version: b.remote.header(t.file_id)?.version,
    };
    let (_, o) = b.file("o", b"3")?;
    let results = b
        .remote
        .delete_for_good(&doomed(&[w, tomb, o]))?
        .into_answers()?;
    ensure!(results.len() == 3, "{} results for 3 files", results.len());
    ensure_err!(&results[0], RemoteError::VersionMismatch);
    ensure!(
        results[1].is_ok() && results[2].is_ok(),
        "the tombstone and the live file: {results:?}"
    );
    ensure!(!b.remote.header(w.file_id)?.is_deleted(), "deleted anyway");
    ensure_err!(b.remote.header(o.file_id), RemoteError::NotFound);
    ensure_err!(b.remote.header(t.file_id), RemoteError::NotFound);
    let (feed, _) = b.changes_after(Some(&b.start))?;
    ensure!(
        !feed
            .iter()
            .any(|h| h.file_id == o.file_id || h.file_id == t.file_id),
        "still in the feed"
    );
    // A retry: gone already; the file at its current version goes too.
    let again = b
        .remote
        .delete_for_good(&doomed(&[o, w2]))?
        .into_answers()?;
    ensure_err!(&again[0], RemoteError::NotFound);
    ensure!(again[1].is_ok(), "{:?}", again[1]);
    ensure_err!(b.remote.header(w.file_id), RemoteError::NotFound);
    Ok(())
}

fn tombstone_in_the_feed(b: &Bench) -> Outcome {
    let (dir, _) = b.folder("d")?;
    let f = file("f", dir.node, b"data");
    let w = b.create(&f, Some(b"data"))?;
    let cursor = b.cursor_now()?;
    b.delete(w)?;

    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(feed.len() == 1, "{} feed entries", feed.len());
    let tomb = &feed[0];
    ensure!(
        (tomb.file_id, tomb.parent, tomb.kind) == (w.file_id, dir.node, NodeKind::File),
        "tombstone keeps file id, parent and kind: {tomb:?}"
    );
    ensure!(
        tomb.body == RemoteBody::Deleted && tomb.node().is_none() && !tomb.is_unshared(),
        "{tomb:?}"
    );
    Ok(())
}

/// `meta` removed to Recently deleted (node schema: `archivalStatus` 2; the time is the
/// server's).
pub fn removed(meta: &NodeMeta) -> NodeMeta {
    NodeMeta {
        removed: true,
        ..meta.clone()
    }
}

fn removal_keeps_the_bytes_and_restores(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"kept")?;
    let (g, v) = b.file("g", b"later")?;
    let cursor = b.cursor_now()?;
    let gone = b.update(w, &removed(&f), None)?;
    let header = b.reader().header(w.file_id)?;
    ensure!(
        header.removed && header.state().is_none(),
        "not removed: {header:?}"
    );
    // The time is the server's, of the write: one removed later is no earlier.
    b.update(v, &removed(&g), None)?;
    let (first, then) = (
        header.removed_at(),
        b.reader().header(v.file_id)?.removed_at(),
    );
    ensure!(
        first.zip(then).is_some_and(|(f, t)| f <= t),
        "removal times out of order: {first:?}, {then:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"kept", "the bytes went");
    // The node id stays taken: a create of it is a retry that landed.
    ensure_err!(b.create(&f, Some(b"kept")), RemoteError::AlreadyExists);
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        feed.last()
            .is_some_and(|h| h.node() == Some(f.node) && h.removed),
        "the feed shows no removal: {feed:?}"
    );
    // A write that keeps the removal keeps it (every write sends the status).
    let renamed = removed(&with(&f, NodeContent::file("f2", 1, 4, false)));
    let gone = b.update(gone, &renamed, None)?;
    ensure!(
        b.reader().header(w.file_id)?.removed,
        "a rewrite restored it"
    );
    // Restore: the update back, here with new bytes (an edit that wins over the delete).
    let edited = with(&f, NodeContent::file("f", 1_700_000_000_124, 6, false));
    b.update(gone, &edited, Some(b"edited"))?;
    let back = b.reader().header(w.file_id)?;
    ensure!(
        !back.removed && back.state().is_some(),
        "not restored: {back:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"edited", "the restore's bytes");
    Ok(())
}

/// Removals and restores in one request ([`Remote::set_removed`]): each file at its version,
/// its header kept but for the removal, an answer per file in order. A file changed since stays
/// as it is; a tombstone is deleted for good; one already at the status asked for
/// is left alone (its version and removal time stay).
fn removals_in_one_request(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"kept")?;
    let (g, v) = b.file("g", b"stale")?;
    let (_, x) = b.file("x", b"later")?;
    let v2 = b.update(v, &with(&g, NodeContent::file("g", 2, 5, false)), None)?;
    let (_, t) = b.file("t", b"tomb")?;
    b.delete(t)?;
    let tomb = b.remote.header(t.file_id)?.version;
    let cursor = b.cursor_now()?;
    let asked = [
        b.target(w),
        b.target(v),
        b.target(x),
        b.target(RemoteRef {
            file_id: t.file_id,
            version: tomb,
        }),
    ];
    let answers = b.remote.set_removed(&asked, true)?.into_answers()?;
    ensure!(answers.len() == 4, "{} answers for 4 files", answers.len());
    ensure_err!(&answers[1], RemoteError::VersionMismatch);
    ensure_err!(&answers[3], RemoteError::Deleted);
    let (Ok(gone_f), Ok(gone_x)) = (&answers[0], &answers[2]) else {
        return Err(format!("not removed: {answers:?}").into());
    };
    for (gone, at) in [(gone_f, w), (gone_x, x)] {
        let header = b.remote.header(at.file_id)?;
        ensure!(
            gone.at().file_id == at.file_id && gone.at().version != at.version,
            "{gone:?} for {at:?}"
        );
        ensure!(
            header.version == gone.at().version
                && header.removed_at().is_some()
                && header.removed_at() == gone.changed_at,
            "the answer isn't the header: {gone:?}, {header:?}"
        );
    }
    // Only the removal changed: the name, the place and the bytes stay.
    let header = b.remote.header(w.file_id)?;
    ensure!(
        header.parent == f.parent && header.state().is_none(),
        "{header:?}"
    );
    ensure!(
        matches!(&header.body, RemoteBody::Live { node, content, payload: Some(_) }
            if *node == f.node && content.name == "f"),
        "the header changed: {header:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"kept", "the bytes went");
    let header = b.remote.header(v.file_id)?;
    ensure!(
        header.version == v2.version && !header.removed,
        "the file written since was removed: {header:?}"
    );
    let feed = b.changes_to(&cursor, &[w.file_id, x.file_id])?;
    ensure!(
        feed.iter().filter(|h| h.removed).count() == 2,
        "the feed shows no removals: {feed:?}"
    );
    // Again at its new version: already removed, left alone.
    let again = b
        .remote
        .set_removed(std::slice::from_ref(&gone_f.target), true)?
        .into_answers()?;
    ensure!(
        matches!(&again[0], Ok(t) if t.at() == gone_f.at() && t.changed_at == gone_f.changed_at),
        "an unchanged removal moved: {again:?}"
    );
    // Restored: live again, as it was.
    let back = b
        .remote
        .set_removed(&[gone_f.target.clone(), gone_x.target.clone()], false)?
        .into_answers()?;
    ensure!(back.iter().all(Result::is_ok), "{back:?}");
    let header = b.remote.header(w.file_id)?;
    ensure!(
        !header.removed && header.state().is_some(),
        "not restored: {header:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"kept", "the restore's bytes");
    Ok(())
}

/// The feed slim ([`Remote::change_refs`]) has the rows of the feed whole, page by page, with
/// the same cursor: what lets the engine read a page whole only when it holds something new.
fn slim_pages_match_whole_ones(b: &Bench) -> Outcome {
    let cursor = b.cursor_now()?;
    let (d, dw) = b.folder("d")?;
    let (f, fw) = b.file("f", b"one")?;
    let (g, gw) = b.file("g", b"two")?;
    let (_, hw) = b.file("h", b"three")?;
    b.update(fw, &with(&f, NodeContent::file("f2", 1, 3, false)), None)?;
    b.update(gw, &removed(&g), None)?;
    b.delete(hw)?;
    b.update(dw, &removed(&d), None)?;
    std::thread::sleep(b.settle);
    let mut at = Some(cursor);
    let mut rows = 0;
    loop {
        let refs = b.reader().change_refs(at.as_deref(), 2)?;
        let page = b.reader().changes(at.as_deref(), 2)?;
        ensure!(page.skipped.is_empty(), "rows skipped: {:?}", page.skipped);
        let whole: Vec<_> = page.entries.iter().map(ChangeRef::of).collect();
        ensure!(refs.refs == whole, "slim {:?}, whole {whole:?}", refs.refs);
        ensure!(
            (&refs.cursor, refs.has_more) == (&page.cursor, page.has_more),
            "slim pages on to {:?}, whole to {:?}",
            (&refs.cursor, refs.has_more),
            (&page.cursor, page.has_more)
        );
        rows += whole.len();
        if !page.has_more {
            break;
        }
        at = Some(page.cursor);
    }
    ensure!(rows >= 4, "{rows} rows for 4 files");
    Ok(())
}

fn a_removed_node_deleted_for_good(b: &Bench) -> Outcome {
    let (f, w) = b.file("f", b"1")?;
    let gone = b.update(w, &removed(&f), None)?;
    let cursor = b.cursor_now()?;
    b.delete(gone)?;
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        feed.last().is_some_and(RemoteHeader::is_deleted),
        "no tombstone in the feed: {feed:?}"
    );
    // One answer for the owner and a member, whole or a range of it: deleted, not "not there"
    // (the tombstone is proof on its own; a file not there is asked about again).
    ensure_err!(b.read(w.file_id, None), RemoteError::Deleted);
    ensure_err!(b.read(w.file_id, Some(0..1)), RemoteError::Deleted);
    Ok(())
}

fn hash_mismatch_stores_nothing(b: &Bench) -> Outcome {
    let f = file("f", b.scratch, b"real");
    // A source that isn't what was hashed (other bytes, fewer, more) is refused.
    for changed in [&b"fake"[..], b"rea", b"real, and more"] {
        let mut reader = Cursor::new(changed);
        let lying = Upload {
            hash: Planned::Known(hash_bytes(b"real", HashAlgorithm::Blake3)),
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
        hash: Planned::Known(hash_bytes(b"new!", HashAlgorithm::Blake3)),
        ..upload(&mut reader)
    };
    ensure_err!(
        b.remote
            .update(&Target { at, token: None }, &f, Some(lying)),
        RemoteError::HashMismatch
    );
    ensure!(b.read(at.file_id, None)? == b"real", "the old bytes stay");
    b.update(at, &f, Some(b"new!"))?;
    Ok(())
}

/// An upload hashed as it is sent (sync semantics §3): the answer gives the hash of the bytes
/// sent, which the drive keeps; a source that changed while it was read stores nothing.
fn hashed_on_send(b: &Bench) -> Outcome {
    let data = b"sent unread";
    let sent = hash_bytes(data, HashAlgorithm::Blake3);
    let on_send = |unchanged: &'static (dyn Fn() -> bool + Sync)| Planned::OnSend {
        algorithm: HashAlgorithm::Blake3,
        unchanged,
    };
    let f = file("on send", b.scratch, data);
    let mut reader = Cursor::new(&data[..]);
    let unread = Upload {
        hash: on_send(&|| true),
        ..upload(&mut reader)
    };
    let written = b.remote.create(&f, Some(unread))?;
    b.track(written.at().file_id);
    ensure!(
        written.content == Some(sent),
        "the answer's hash: {:?}",
        written.content
    );
    let at = written.at();
    ensure!(b.read(at.file_id, None)? == data, "the bytes sent");
    let kept = b.remote.header(at.file_id)?.state().and_then(|s| s.content);
    ensure!(
        kept.map(|c| c.hash) == Some(sent),
        "the drive's hash: {kept:?}"
    );
    // Changed while it was read: refused, nothing stored (the node id is still free).
    let g = file("changed", b.scratch, data);
    let mut reader = Cursor::new(&data[..]);
    let changed = Upload {
        hash: on_send(&|| false),
        ..upload(&mut reader)
    };
    ensure_err!(
        b.remote.create(&g, Some(changed)),
        RemoteError::HashMismatch
    );
    b.create(&g, Some(data))?;
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
        let page = b.reader().changes(Some(&cursor), 2)?;
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
            let page = b.reader().changes(Some(&cursor), 100)?;
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

/// The sharer's share rows, which only a shared bench has.
fn shares<'a>(b: &Bench<'a>) -> Result<&'a dyn Shares, &'static str> {
    b.shared
        .map(|s| s.shares)
        .ok_or("not a shared bench: no member to share with")
}

/// A name for a node made at the top of the drive, outside the scratch folder: its own on a
/// drive that keeps what earlier runs left there.
fn unshared_name(what: &str) -> String {
    format!("{what} {}", NodeId::new().0.simple())
}

/// What the sharer adds to a shared folder is shared as the folder is, with the write itself:
/// the member reads it at once, a folder made there and what goes into that too, and may
/// change it where it may change the folder. What is added once the folder's share is
/// withdrawn isn't shared.
fn the_sharer_adds_to_a_shared_folder(b: &Bench) -> Outcome {
    let (member, editing) = (b.reader(), b.shared.is_some_and(|s| s.editing));
    let cursor = b.cursor_now()?;
    let added = file("added", b.scratch, b"1");
    let dir = folder("a new folder", b.scratch);
    let inner = file("inner", dir.node, b"2");
    let at = [
        b.create_unshared(&added, Some(b"1"))?,
        b.create_unshared(&dir, None)?,
        b.create_unshared(&inner, Some(b"2"))?,
    ];
    let ids: Vec<RemoteFileId> = at.iter().map(|at| at.file_id).collect();
    let feed = b.changes_to(&cursor, &ids)?;
    ensure!(feed.len() == 3, "the member's feed: {feed:?}");
    for (at, meta) in at.iter().zip([&added, &dir, &inner]) {
        let header = member.header(at.file_id)?;
        ensure!(
            header.version == at.version && header.parent == meta.parent,
            "the member reads {header:?}"
        );
        ensure!(
            header.writable == editing,
            "shared as its folder is: {header:?}"
        );
    }
    ensure!(b.read(at[0].file_id, None)? == b"1", "payload");
    ensure!(b.read(at[2].file_id, None)? == b"2", "payload");

    shares(b)?.withdraw(at[1].file_id)?;
    let late = b.create_unshared(&file("late", dir.node, b"3"), Some(b"3"))?;
    ensure_err!(member.header(late.file_id), RemoteError::Fatal(_));
    Ok(())
}

/// A file shared in a folder that isn't is listed with its parent (`groupId`), the folder not at
/// all, nor read.
fn a_child_of_a_folder_not_shared(b: &Bench) -> Outcome {
    let cursor = b.cursor_now()?;
    // At the top: in the scratch folder it would be shared as that is.
    let dir = folder(&unshared_name("not shared"), NodeId::ROOT);
    let dir_ref = b.create_unshared(&dir, None)?;
    let f = file("shared", dir.node, b"in it");
    let w = b.create(&f, Some(b"in it"))?;
    let feed = b.changes_to(&cursor, &[dir_ref.file_id, w.file_id])?;
    ensure!(
        feed.len() == 1 && feed[0].file_id == w.file_id && feed[0].parent == dir.node,
        "the feed: {feed:?}"
    );
    ensure!(b.read(w.file_id, None)? == b"in it", "payload");
    ensure_err!(b.reader().header(dir_ref.file_id), RemoteError::Fatal(_));
    Ok(())
}

/// A file not shared isn't in the feed, and its header and payload are refused: not as gone
/// (the file is there), nor as a refusal of the member (the rest is still shared).
fn what_isnt_shared_is_refused(b: &Bench) -> Outcome {
    let cursor = b.cursor_now()?;
    // At the top: in the scratch folder it would be shared as that is.
    let private = file(&unshared_name("private"), NodeId::ROOT, b"mine");
    let w = b.create_unshared(&private, Some(b"mine"))?;
    ensure!(
        b.changes_to(&cursor, &[w.file_id])?.is_empty(),
        "a file not shared is in the feed"
    );
    ensure_err!(b.reader().header(w.file_id), RemoteError::Fatal(_));
    ensure_err!(b.read(w.file_id, None), RemoteError::Fatal(_));
    Ok(())
}

/// A share withdrawn comes as a tombstone without a version, a parent (the root, as the adapter
/// reads a tombstone without a `groupId`) or a node id, whole and slim alike
/// ([`RemoteHeader::is_unshared`]), at the withdrawal's time: later than the file's last write.
/// The file isn't read any more. The sharer's file keeps its version and that version's time:
/// a share change is no write.
fn a_withdrawn_share_is_a_tombstone_without_a_version(b: &Bench) -> Outcome {
    let (_, w) = b.file("withdrawn", b"data")?;
    let before = b.reader().header(w.file_id)?.changed_at;
    ensure!(before.is_some(), "no time on the shared file");
    let cursor = b.cursor_now()?;
    shares(b)?.withdraw(w.file_id)?;
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(feed.len() == 1, "{} feed entries", feed.len());
    let tomb = &feed[0];
    ensure!(
        tomb.is_unshared() && tomb.node().is_none() && !tomb.removed,
        "not a tombstone without a version: {tomb:?}"
    );
    ensure!(
        tomb.kind == NodeKind::File && tomb.parent == NodeId::ROOT,
        "kind kept, the root its parent: {tomb:?}"
    );
    ensure!(
        tomb.changed_at > before,
        "not at the withdrawal's time (the file's last write {before:?}): {tomb:?}"
    );
    let slim: Vec<_> = b
        .refs_after(&cursor)?
        .into_iter()
        .filter(|r| r.file_id == w.file_id)
        .collect();
    ensure!(
        slim == [ChangeRef::of(tomb)] && slim[0].is_unshared() && slim[0].parent.is_none(),
        "slim {slim:?}, whole {tomb:?}"
    );
    ensure_err!(b.reader().header(w.file_id), RemoteError::Fatal(_));
    ensure_err!(b.read(w.file_id, None), RemoteError::Fatal(_));
    let kept = b.remote.header(w.file_id)?;
    ensure!(
        kept.version == w.version && !kept.is_deleted() && kept.changed_at == before,
        "the sharer's file changed, or its version's time moved (from {before:?}): {kept:?}"
    );
    Ok(())
}

/// A share withdrawn and given again comes back live at the version it had.
fn a_share_again_comes_back_at_its_version(b: &Bench) -> Outcome {
    let (_, w) = b.file("again", b"back")?;
    shares(b)?.withdraw(w.file_id)?;
    let cursor = b.cursor_now()?;
    shares(b)?.share(w.file_id)?;
    let feed = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        feed.len() == 1 && feed[0].version == w.version && feed[0].state().is_some(),
        "not back as it was: {feed:?}"
    );
    ensure!(
        b.reader().header(w.file_id)? == feed[0],
        "the header isn't the feed's"
    );
    ensure!(b.read(w.file_id, None)? == b"back", "payload");
    Ok(())
}

/// A file deleted for good is just gone: not in the feed (no tombstone), its header not found,
/// as its share row went with it (**[verified]** odin-core: `MainIndexMeta.WriteSharesAsync`
/// locks the file's row so a hard delete takes its shares; `ReadForCallerAsync` finds no header
/// before it looks at shares, and the peer header routes answer that 404).
fn a_file_deleted_for_good_is_gone(b: &Bench) -> Outcome {
    let cursor = b.cursor_now()?;
    let (_, w) = b.file("gone", b"1")?;
    b.remote.delete_for_good(&doomed(&[w]))?.single()?;
    ensure!(
        b.changes_to(&cursor, &[w.file_id])?.is_empty(),
        "a file deleted for good is in the feed"
    );
    ensure_err!(b.reader().header(w.file_id), RemoteError::NotFound);
    Ok(())
}

/// Every write of the member's refuses (what is shared is read-only), and changes nothing.
fn the_member_writes_nothing(b: &Bench) -> Outcome {
    let (f, w) = b.file("theirs", b"1")?;
    let cursor = b.cursor_now()?;
    let target = Target { at: w, token: None };
    let new = file("mine", b.scratch, b"2");
    ensure_err!(b.member_create(&new, Some(b"2")), RemoteError::ReadOnly(_));
    ensure_err!(
        b.reader().update(
            &target,
            &with(&f, NodeContent::file("x", 1, 1, false)),
            None
        ),
        RemoteError::ReadOnly(_)
    );
    ensure_err!(b.reader().delete(&doomed(&[w])), RemoteError::ReadOnly(_));
    ensure_err!(
        b.reader().delete_for_good(&doomed(&[w])),
        RemoteError::ReadOnly(_)
    );
    ensure_err!(
        b.reader().set_removed(std::slice::from_ref(&target), true),
        RemoteError::ReadOnly(_)
    );
    ensure!(
        b.changes_to(&cursor, &[w.file_id])?.is_empty(),
        "the file changed"
    );
    let header = b.reader().header(w.file_id)?;
    ensure!(header.version == w.version, "the file changed");
    ensure!(!header.writable, "said to be the member's to change");
    Ok(())
}

/// A folder holds, to the member, only children shared with it: one not shared, or whose share
/// was withdrawn (a tombstone to the member), isn't counted, live or any; shared, it is.
fn a_member_holds_only_what_is_shared(b: &Bench) -> Outcome {
    let (dir, dir_at) = b.folder("held")?;
    let (other, _) = b.folder("withdrawn")?;
    // Made while its folder isn't shared (in a shared one it would be shared as that is):
    // a folder shared later doesn't share what is in it.
    shares(b)?.withdraw(dir_at.file_id)?;
    let private = b.create_unshared(&file("private", dir.node, b"1"), Some(b"1"))?;
    b.share(dir_at.file_id)?;
    let w = b.create(&file("w", other.node, b"2"), Some(b"2"))?;
    shares(b)?.withdraw(w.file_id)?;
    let folders = [dir.node, other.node];
    let none = HashSet::new();
    let holding = |expected: &[NodeId], why: &str| -> Outcome {
        std::thread::sleep(b.reader().hold_back());
        for holds in [Holds::Live, Holds::AnyBut(&none)] {
            let got = b.reader().holding(&folders, holds)?;
            ensure!(
                got == expected.iter().copied().collect(),
                "{why}, {holds:?}: {got:?}"
            );
        }
        Ok(())
    };
    holding(&[], "a child not shared and one withdrawn")?;
    shares(b)?.share(private.file_id)?;
    shares(b)?.share(w.file_id)?;
    holding(&folders, "both shared")?;
    Ok(())
}

/// `range` of `file_id`'s bytes (all of them without one) as `reader` reads them.
fn read_all(
    reader: &dyn Remote,
    file_id: RemoteFileId,
    range: Option<std::ops::Range<u64>>,
) -> Result<Vec<u8>, RemoteError> {
    let mut out = Vec::new();
    let n = reader.read(file_id, range, &mut out)?;
    if n != out.len() as u64 {
        return Err(RemoteError::Fatal(format!(
            "read returned {n} but wrote {} bytes",
            out.len()
        )));
    }
    Ok(out)
}

/// A member changes a file shared with it for editing: its bytes and its header, and its
/// header alone (the bytes stay). The owner and the member read what it wrote, at the version
/// its write answered, which carries no token. A write at the version before is refused.
fn a_member_changes_what_it_may_edit(b: &Bench) -> Outcome {
    let (f, w) = b.file("theirs", b"1")?;
    let member = b.reader();
    // Its header says so to the member (nothing asks it of the owner's own).
    ensure!(
        member.header(w.file_id)?.writable && !b.remote.header(w.file_id)?.writable,
        "shared for editing, and not said so to the member alone"
    );
    let target = Target { at: w, token: None };
    let edited = with(&f, NodeContent::file("theirs", 2, 2, false));
    let written = with_upload(Some(b"22"), |upload| {
        member.update(&target, &edited, upload)
    })?;
    ensure!(
        written.target.token.is_none(),
        "a member's write came with a token"
    );
    let at = written.at();
    ensure!(
        at.file_id == w.file_id && at.version != w.version,
        "written at {at:?}, the file was {w:?}"
    );
    for (who, reader) in [("the owner", b.remote), ("the member", member)] {
        let header = reader.header(w.file_id)?;
        ensure!(header.version == at.version, "{who} reads {header:?}");
        ensure!(
            matches!(&header.body, RemoteBody::Live { content, .. } if *content == edited.content),
            "{who} reads {header:?}"
        );
        ensure!(
            read_all(reader, w.file_id, None)? == b"22",
            "{who} reads other bytes"
        );
    }
    ensure_err!(
        member.update(&target, &edited, None),
        RemoteError::VersionMismatch
    );
    // Its header alone: a rename. The bytes stay.
    let renamed = with(&f, NodeContent::file("renamed", 2, 2, false));
    let target = Target { at, token: None };
    let again = member.update(&target, &renamed, None)?.at();
    let header = b.remote.header(w.file_id)?;
    ensure!(
        header.version == again.version
            && matches!(&header.body, RemoteBody::Live { content, .. } if content.name == "renamed"),
        "the owner reads {header:?}"
    );
    ensure!(
        read_all(b.remote, w.file_id, None)? == b"22",
        "the bytes changed"
    );
    Ok(())
}

/// A member adds a file, a folder, and a file in the folder it added, to a folder shared with
/// it for editing: each is the owner's to read and shared with the member as the folder is.
/// One added again is there already. Nothing is added outside what is shared.
fn a_member_adds_to_a_folder_it_may_edit(b: &Bench) -> Outcome {
    let member = b.reader();
    let add = |meta: &NodeMeta, data: Option<&[u8]>| b.member_create(meta, data);
    let new = file("mine", b.scratch, b"2");
    let made = add(&new, Some(b"2"))?;
    ensure!(
        made.target.token.is_none(),
        "a member's write came with a token"
    );
    let dir = folder("my folder", b.scratch);
    let made_dir = add(&dir, None)?;
    let inner = file("inner", dir.node, b"3");
    let made_inner = add(&inner, Some(b"3"))?;
    for (meta, written, data) in [
        (&new, &made, Some(&b"2"[..])),
        (&dir, &made_dir, None),
        (&inner, &made_inner, Some(&b"3"[..])),
    ] {
        let at = written.at();
        ensure!(
            member.header(at.file_id)?.writable,
            "what the member added isn't its to change"
        );
        for (who, reader) in [("the owner", b.remote), ("the member", member)] {
            let header = reader.header(at.file_id)?;
            ensure!(
                header.version == at.version && header.parent == meta.parent,
                "{who} reads {header:?}"
            );
            ensure!(
                matches!(&header.body, RemoteBody::Live { node, .. } if *node == meta.node),
                "{who} reads {header:?}"
            );
            if let Some(data) = data {
                ensure!(
                    read_all(reader, at.file_id, None)? == data,
                    "{who} reads other bytes"
                );
            }
        }
    }
    ensure_err!(add(&new, Some(b"2")), RemoteError::AlreadyExists);
    ensure_err!(
        add(&folder("at the top", NodeId::ROOT), None),
        RemoteError::Fatal(_)
    );
    Ok(())
}

/// A member moves a file shared with it for editing to Recently deleted, and back: the owner
/// reads it removed, then not. One already so is left as it is, its version too; one written
/// since is refused.
fn a_member_removes_and_puts_back(b: &Bench) -> Outcome {
    let (_, w) = b.file("theirs", b"1")?;
    let member = b.reader();
    let target = |at: RemoteRef| Target { at, token: None };
    let removed = member.set_removed(&[target(w)], true)?.single()?;
    ensure!(
        removed.target.token.is_none(),
        "a member's write came with a token"
    );
    let at = removed.at();
    ensure!(at.version != w.version, "removed at the version before");
    for (who, reader) in [("the owner", b.remote), ("the member", member)] {
        let header = reader.header(w.file_id)?;
        ensure!(
            header.removed && header.version == at.version,
            "{who} reads {header:?}"
        );
    }
    ensure!(
        read_all(b.remote, w.file_id, None)? == b"1",
        "the bytes changed"
    );
    let again = member.set_removed(&[target(at)], true)?.single()?;
    ensure!(again.at().version == at.version, "removed twice");
    ensure_err!(
        member.set_removed(&[target(w)], false)?.single(),
        RemoteError::VersionMismatch
    );
    let back = member.set_removed(&[target(at)], false)?.single()?.at();
    let header = b.remote.header(w.file_id)?;
    ensure!(
        !header.removed && header.version == back.version,
        "the owner reads {header:?}"
    );
    Ok(())
}

/// A member that writes still deletes nothing of the owner's, a tombstone or for good.
fn a_member_deletes_nothing(b: &Bench) -> Outcome {
    let (_, w) = b.file("theirs", b"1")?;
    let member = b.reader();
    ensure_err!(member.delete(&doomed(&[w])), RemoteError::ReadOnly(_));
    ensure_err!(
        member.delete_for_good(&doomed(&[w])),
        RemoteError::ReadOnly(_)
    );
    let header = b.remote.header(w.file_id)?;
    ensure!(
        header.version == w.version && !header.is_deleted(),
        "the owner reads {header:?}"
    );
    Ok(())
}

/// What is shared with the member for reading alone it doesn't change, though it writes
/// elsewhere: a file's change and removal refuse, and nothing is added to such a folder.
fn what_is_shared_to_read_is_not_changed(b: &Bench) -> Outcome {
    let shares = shares(b)?;
    let (f, w) = b.file("to read", b"1")?;
    let (dir, d) = b.folder("to read too")?;
    shares.share(w.file_id)?;
    shares.share(d.file_id)?;
    let member = b.reader();
    let target = Target { at: w, token: None };
    ensure_err!(
        member.update(
            &target,
            &with(&f, NodeContent::file("x", 1, 1, false)),
            None
        ),
        RemoteError::ReadOnly(_)
    );
    ensure_err!(
        member
            .set_removed(std::slice::from_ref(&target), true)?
            .single(),
        RemoteError::ReadOnly(_)
    );
    let new = file("mine", dir.node, b"2");
    ensure_err!(b.member_create(&new, Some(b"2")), RemoteError::ReadOnly(_));
    let header = b.remote.header(w.file_id)?;
    ensure!(
        header.version == w.version && !header.removed,
        "the owner reads {header:?}"
    );
    // Shared for editing again, it is changed: the feed tells the member so, of the same
    // version.
    ensure!(
        !member.header(w.file_id)?.writable,
        "said to be the member's to change"
    );
    let cursor = b.cursor_now()?;
    shares.share_to_edit(w.file_id)?;
    let told = b.changes_to(&cursor, &[w.file_id])?;
    ensure!(
        matches!(told.as_slice(), [row] if row.writable && row.version == w.version),
        "a share turned to editing reads {told:?}"
    );
    member.update(
        &target,
        &with(&f, NodeContent::file("x", 1, 1, false)),
        None,
    )?;
    Ok(())
}
