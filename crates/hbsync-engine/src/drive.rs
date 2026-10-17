//! One account's drive, shared by the engines of its folders on this device: the remote, the
//! change feed (pulled once for all of them), and what is the whole drive's rather than a
//! folder's: Recently deleted, restoring and emptying it, the purge.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hbsync_remote::{ChangeRef, NodeMeta, Remote, RemoteError};
use hbsync_state::{StateDb, StateError, keys};
use std::collections::HashMap;

use hbsync_state::MirrorRow;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, RemoteFileId, Role, VersionTag};

use crate::deleted;
use crate::engine::EngineError;
use crate::feed::{self, FeedError, FeedReport};
use crate::root::{RootError, SyncRoot};

/// Rows of the change feed read per request, at most: each read asks for a few less than the
/// last ([`Drive::page_size`]).
pub const FEED_PAGE: usize = 500;
/// How many page sizes the reads go through.
const PAGE_SIZES: u64 = 64;
/// Unreadable files remembered at most ([`Drive::unseen`]).
const MAX_UNREADABLE: usize = 100_000;
/// Skipped feed rows kept until a folder's runner reports them, at most.
const MAX_SKIPPED: usize = 1000;

pub struct Drive {
    /// The account's connection (it sees no folder: a change fans out to those it concerns).
    account: Mutex<StateDb>,
    remote: Arc<dyn Remote>,
    /// Pulls asked for so far.
    asked: AtomicU64,
    /// Held while pulling: how many of the asks the last finished pull answered.
    answered: Mutex<u64>,
    /// Folders to wake when the drive changed.
    wakes: Mutex<Vec<(i64, Sender<()>)>>,
    /// Feed rows the client can't read, whoever pulled them, until taken.
    skipped: Mutex<Vec<String>>,
    /// The drive refused an upload, out of storage: until when uploads are held.
    full: Mutex<Option<Instant>>,
    /// The identity announces the drive's changes: the notifications are connected, and the
    /// read that followed their connecting worked.
    announcing: AtomicBool,
    /// Counts the connections the notifications lost: a read asked for at a connection that
    /// was lost since says nothing about announcements ([`Drive::announcing_from`]).
    connections: AtomicU64,
    /// The account's picture may lack something (a pull failed, a write was refused for the
    /// drive's state, an announced change never showed): a count of such doubts, and the count
    /// a read that worked has answered. Behind while the doubts are ahead: a read clears only
    /// the doubts from before it started.
    doubts: AtomicU64,
    answered_doubts: AtomicU64,
    /// Files whose row the feed holds but the mirror doesn't take (not readable as a node, live
    /// without a payload), with that row's version: an announced change of that version is no
    /// change to wait for. One per file, at most [`MAX_UNREADABLE`].
    unreadable: Mutex<HashMap<RemoteFileId, VersionTag>>,
    /// Feed reads so far, from a random start: the next read's page size.
    reads: AtomicU64,
}

/// How long uploads are held after the drive said it is full: then one is tried again.
const FULL_HOLD: Duration = Duration::from_secs(600);

impl std::fmt::Debug for Drive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Drive").finish_non_exhaustive()
    }
}

impl Drive {
    /// The drive of `identity`'s account, whose state DB is at `db` (made if new; refused if
    /// another's).
    pub fn open(
        db: &Path,
        identity: &str,
        remote: Arc<dyn Remote>,
    ) -> Result<Arc<Drive>, RootError> {
        Ok(Drive::new(SyncRoot::open_account(db, identity)?, remote))
    }

    /// The drive of the account whose state DB `account` is ([`StateDb::open_account`]).
    pub fn new(account: StateDb, remote: Arc<dyn Remote>) -> Arc<Drive> {
        Arc::new(Drive {
            account: Mutex::new(account),
            remote,
            asked: AtomicU64::new(0),
            answered: Mutex::new(0),
            wakes: Mutex::default(),
            skipped: Mutex::default(),
            full: Mutex::default(),
            announcing: AtomicBool::new(false),
            connections: AtomicU64::new(0),
            doubts: AtomicU64::new(0),
            answered_doubts: AtomicU64::new(0),
            unreadable: Mutex::default(),
            reads: AtomicU64::new(NodeId::new().0.as_u128() as u64 % PAGE_SIZES),
        })
    }

    /// The notifications connected: the mark of this connection, for
    /// [`Drive::announcing_from`] once the read that follows worked.
    pub fn connection(&self) -> u64 {
        self.connections.load(Ordering::SeqCst)
    }

    /// The read asked for when the notifications connected (`connection`) worked: unless that
    /// connection was lost since, the drive's changes are announced from now on.
    pub fn announcing_from(&self, connection: u64) {
        if self.connections.load(Ordering::SeqCst) == connection {
            self.announcing.store(true, Ordering::SeqCst);
        }
    }

    /// The notifications are lost or refused: changes go unannounced, and what was announced
    /// meanwhile may not have been taken, so the account is behind.
    pub fn not_announcing(&self) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        self.announcing.store(false, Ordering::SeqCst);
        self.set_behind();
    }

    /// The account's picture may lack something: the next sync reads the feed.
    pub fn set_behind(&self) {
        self.doubts.fetch_add(1, Ordering::SeqCst);
    }

    fn behind(&self) -> bool {
        self.answered_doubts.load(Ordering::SeqCst) < self.doubts.load(Ordering::SeqCst)
    }

    /// Whether the account's picture of the drive is as current as the announcements make it:
    /// they are connected and the last pull worked. Then nothing needs reading the feed to
    /// sync (this device's own writes are recorded from their answers); else a sync reads it.
    pub fn current(&self) -> bool {
        self.announcing.load(Ordering::SeqCst) && !self.behind()
    }

    /// The page size of the read of the feed numbered `read`: a little smaller each time,
    /// round after round. odin-core keeps a query's answer by the query (cursor and page size,
    /// not who asks: `QueryBatchCached`), and an answer computed while a write commits can be
    /// kept after it, hiding that write from the same query until the next write to the drive
    /// or for hours (`homebase-api-notes.md`, the change feed). A read never asks what the last
    /// one did, nor as another client most likely just did. A workaround for odin-core #1909
    /// (tracked in filesync #90): once that is fixed, every read asks for [`FEED_PAGE`].
    fn page_size(read: u64) -> usize {
        FEED_PAGE - (read % PAGE_SIZES) as usize
    }

    /// The page size the next read of the feed will use (tests: to keep its answer).
    #[doc(hidden)]
    pub fn next_page_size(&self) -> usize {
        Drive::page_size(self.reads.load(Ordering::Relaxed))
    }

    /// Of announced `refs`, the live ones the account's picture doesn't show yet: neither that
    /// version nor a later write of the file (by the server's time) is in it, the file wasn't
    /// deleted for good, and the feed didn't hold its row unreadable. A read of the feed that
    /// should have brought them didn't: the server may hold a row back (its write is in this
    /// millisecond, or kept out by a stale answer, odin-core #1909, filesync #90, see
    /// [`Drive::page_size`]); they are read for again a moment later.
    pub fn unseen(&self, refs: &[ChangeRef]) -> Result<Vec<ChangeRef>, StateError> {
        let live: Vec<&ChangeRef> = {
            let mut unreadable = self.unreadable.lock().unwrap_or_else(|e| e.into_inner());
            refs.iter()
                .filter(|r| !r.deleted)
                .filter(|r| match unreadable.get(&r.file_id) {
                    Some(v) if *v == r.version => false,
                    // A newer version of a file once unreadable: awaited as any, and the
                    // entry forgotten.
                    Some(_) => {
                        unreadable.remove(&r.file_id);
                        true
                    }
                    None => true,
                })
                .collect()
        };
        let versions: Vec<_> = live
            .iter()
            .map(|r| {
                (
                    r.at(),
                    r.removed_at.is_some(),
                    r.changed_at.or(r.removed_at),
                )
            })
            .collect();
        let unseen = self.account().read(|s| s.unseen(&versions))?;
        Ok(unseen.into_iter().map(|i| *live[i]).collect())
    }

    /// Reads the feed ([`Drive::pull`]) unless the picture is [`Drive::current`].
    pub fn catch_up(&self, from: Option<i64>) -> Result<FeedReport, FeedError> {
        if self.current() {
            return Ok(FeedReport::default());
        }
        self.pull(from)
    }

    /// Whether uploads are held: the drive said it is full lately.
    pub fn full(&self) -> bool {
        let full = self.full.lock().unwrap_or_else(|e| e.into_inner());
        full.is_some_and(|until| Instant::now() < until)
    }

    /// Tries uploads again at once (the user asked for a sync: they may have made room).
    pub fn clear_full(&self) {
        *self.full.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// The drive refused an upload, out of storage: hold uploads for a while, in every folder.
    pub fn set_full(&self) {
        *self.full.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now() + FULL_HOLD);
    }

    fn remote(&self) -> &dyn Remote {
        self.remote.as_ref()
    }

    pub fn shared_remote(&self) -> Arc<dyn Remote> {
        Arc::clone(&self.remote)
    }

    /// Whether `db` is a connection to the drive's DB file.
    pub(crate) fn same_file(&self, db: &StateDb) -> bool {
        self.account().same_file(db)
    }

    fn account(&self) -> MutexGuard<'_, StateDb> {
        self.account.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reads the feed until it has no more rows, into the mirror. One pull at a time: a caller
    /// that asked while another pull ran waits for it and pulls again only if that pull had
    /// started before it asked. Either way, what the drive had when it asked is in the mirror
    /// when it returns. Wakes the engines of the folders the changes concern, but `from`'s.
    /// The account's connection is free for others while a page is on its way.
    pub fn pull(&self, from: Option<i64>) -> Result<FeedReport, FeedError> {
        let ticket = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let mut answered = self.answered.lock().unwrap_or_else(|e| e.into_inner());
        if *answered >= ticket {
            return Ok(FeedReport::default());
        }
        // Every ask so far came before this pull starts: it answers them all.
        let covers = self.asked.load(Ordering::SeqCst);
        let page = Drive::page_size(self.reads.fetch_add(1, Ordering::Relaxed));
        let doubts = self.doubts.load(Ordering::SeqCst);
        let pulled = feed::pull(|| self.account(), self.remote(), page);
        match pulled {
            // What was in doubt when it started is answered; a doubt since isn't.
            Ok(_) => {
                self.answered_doubts.fetch_max(doubts, Ordering::SeqCst);
                *answered = covers;
            }
            Err(_) => self.set_behind(),
        }
        // Still one at a time: what it touched is its to wake, the pages it committed before a
        // failure too.
        self.wake_touched(&mut self.account(), from);
        drop(answered);
        let mut report = pulled?;
        let mut unreadable = self.unreadable.lock().unwrap_or_else(|e| e.into_inner());
        if unreadable.len() + report.unreadable.len() > MAX_UNREADABLE {
            unreadable.clear(); // forgotten: an announced change of one costs a few reads
        }
        unreadable.extend(
            report
                .unreadable
                .drain(..)
                .map(|at| (at.file_id, at.version)),
        );
        drop(unreadable);
        let mut skipped = self.skipped.lock().unwrap_or_else(|e| e.into_inner());
        let room = MAX_SKIPPED.saturating_sub(skipped.len());
        skipped.extend(std::mem::take(&mut report.skipped).into_iter().take(room));
        Ok(report)
    }

    /// Takes changes the notifications announced ([`feed::announced`]): whether that was all,
    /// or the feed has to be read. Wakes the folders they concern. One at a time with
    /// [`Drive::pull`] (its lock): a pull's page fetched before a hard delete must not commit
    /// after the delete was taken (it would put the row back), and the folders a running pull
    /// touched are its to wake.
    pub fn announced(&self, refs: &[ChangeRef]) -> Result<bool, StateError> {
        if refs.is_empty() {
            return Ok(true);
        }
        let _one_at_a_time = self.answered.lock().unwrap_or_else(|e| e.into_inner());
        let mut db = self.account();
        let all = db.write(|s| feed::announced(s, refs))?;
        self.wake_touched(&mut db, None);
        Ok(all)
    }

    /// The feed rows the client couldn't read (not encrypted, another key, not a sync node)
    /// since the last call, each with why; a thousand at most.
    pub fn take_skipped(&self) -> Vec<String> {
        std::mem::take(&mut *self.skipped.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// A channel woken when the drive changed for `folder`'s engine (another engine's pull, or
    /// its writes).
    pub fn subscribe(&self, folder: i64) -> Receiver<()> {
        let (tx, rx) = channel();
        self.wakes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((folder, tx));
        rx
    }

    /// Wakes every folder's engine: the drive changed. Folders gone are forgotten.
    pub fn wake_all(&self) {
        self.wake_where(|_| true);
    }

    /// Wakes the engines of the folders the writes through `db` marked nodes dirty in
    /// ([`StateDb::take_touched`]), but `except`'s.
    pub fn wake_touched(&self, db: &mut StateDb, except: Option<i64>) {
        let folders = db.take_touched();
        self.wake_where(|folder| Some(folder) != except && folders.contains(&folder));
    }

    fn wake_where(&self, wanted: impl Fn(i64) -> bool) {
        self.wakes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(folder, tx)| !wanted(*folder) || tx.send(()).is_ok());
    }

    /// Makes sure the drive has `role`'s folder, just pulled: kept if it is there, back from
    /// Recently deleted (with what was deleted with it), else made at the drive's top (whichever
    /// device syncing it gets there first). Before every sync of the role's folder: nothing may
    /// be synced into a folder that isn't on the drive, where it would land in Recently deleted.
    pub fn ensure_role(&self, role: Role) -> Result<(), EngineError> {
        let top = role.node();
        let live = |drive: &Drive| -> Result<bool, StateError> {
            drive.account().read(|s| Ok(s.get_mirror(top)?.is_some()))
        };
        if live(self)? {
            return Ok(());
        }
        if self.restore(&[top])? == 0 {
            let meta = NodeMeta {
                node: top,
                parent: NodeId::ROOT,
                kind: NodeKind::Folder,
                content: NodeContent::folder(role.name()),
                removed: false,
            };
            match self.remote().create(&meta, None) {
                // Recorded from the answer, as the executor records its writes.
                Ok(written) => {
                    let row = MirrorRow::written(
                        written,
                        meta.node,
                        meta.parent,
                        meta.kind,
                        &meta.content,
                        None,
                    );
                    let mut db = self.account();
                    // As the executor records a folder it made: out of Recently deleted too.
                    db.write(|s| {
                        s.delete_removed(top)?;
                        s.put_mirror(&row)
                    })?;
                    self.wake_touched(&mut db, None);
                }
                // Another device made it (or deleted it) meanwhile: read it.
                Err(RemoteError::AlreadyExists) => {
                    self.pull(None)?;
                }
                Err(e) => return Err(EngineError::Remote(e)),
            }
        }
        if !live(self)? {
            return Err(EngineError::Remote(RemoteError::Transient(format!(
                "{} isn't on the drive yet",
                role.name()
            ))));
        }
        Ok(())
    }

    /// Records which login (`by`, a fingerprint of its key) the remote seals file tokens under
    /// from now on; if another one sealed those stored, they are forgotten (a token doesn't open
    /// under another login's key: the next update of each file reads its header first, and the
    /// token it writes is the new login's). Whether they were.
    pub fn tokens_sealed_by(&self, by: &str) -> Result<bool, StateError> {
        self.account().write(|s| {
            let changed = s.account_kv(keys::TOKENS_SEALED_BY)?.as_deref() != Some(by);
            if changed {
                s.clear_tokens()?;
                s.set_account_kv(keys::TOKENS_SEALED_BY, by)?;
            }
            Ok(changed)
        })
    }

    /// Restores these items of Recently deleted (each with what was deleted with it); the
    /// next syncs bring them back. Returns how many items came back.
    /// The user's to ask for: it reads the feed first, whatever the notifications say. A write
    /// the drive refused for its state, or one whose answer didn't come, leaves the account
    /// behind.
    pub fn restore(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.pull(None)?;
        let (mut restored, mut moved) = (0, false);
        let done = nodes.iter().try_for_each(|&node| {
            restored += deleted::restore(&mut self.account(), self.remote(), node, &mut moved)?;
            Ok::<_, EngineError>(())
        });
        if moved || matches!(&done, Err(EngineError::Remote(e)) if e.is_transient()) {
            self.set_behind();
        }
        self.wake_all();
        done.map(|()| restored)
    }

    /// Deletes these items of Recently deleted for good, each with what was deleted with it.
    pub fn empty(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.catch_up(None)?;
        deleted::empty(&mut self.account(), self.remote(), nodes)
    }

    /// Deletes for good what a listing of Recently deleted saw ([`deleted::empty_listed`]),
    /// nothing recorded since.
    pub fn empty_listed(&self, listed: &deleted::Listed) -> Result<usize, EngineError> {
        self.catch_up(None)?;
        deleted::empty_listed(&mut self.account(), self.remote(), listed)
    }

    /// Deletes for good what was deleted before `before_ms` (Unix ms): Recently deleted, and
    /// the trash of every folder.
    pub fn purge(&self, before_ms: i64) -> Result<usize, EngineError> {
        self.catch_up(None)?;
        deleted::purge(&mut self.account(), self.remote(), before_ms)
    }

    /// Once a day: deletes for good what Recently deleted has kept `keep_days` (and the trash
    /// of the same age). A failure waits for the next day's try... or the next call: it is only
    /// recorded as done when it worked.
    pub fn purge_when_due(&self, keep_days: u32) -> Result<Option<usize>, EngineError> {
        const DAY: u64 = 24 * 60 * 60;
        let now = hbsync_types::unix_now();
        let last = self
            .account()
            .read(|s| s.account_kv(keys::PURGED_AT))?
            .and_then(|v| v.parse::<u64>().ok());
        if last.is_some_and(|last| now < last + DAY) {
            return Ok(None);
        }
        let before_ms = now.saturating_sub(u64::from(keep_days) * DAY) as i64 * 1000;
        let gone = self.purge(before_ms)?;
        self.account()
            .write(|s| s.set_account_kv(keys::PURGED_AT, &now.to_string()))?;
        Ok(Some(gone))
    }
}
