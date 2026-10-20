//! One account's drive, shared by the engines of its folders on this device: the remote, the
//! change feed (pulled once for all of them), and what is the whole drive's rather than a
//! folder's: Recently deleted, restoring and emptying it, the purge.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use hbsync_remote::{ChangeRef, NodeMeta, Remote, RemoteError};
use hbsync_state::{StateDb, StateError, keys};

use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, Role};

use crate::deleted;
use crate::error::EngineError;
use crate::feed::{self, FeedError, FeedReport};
use crate::mirror;
use crate::root::{RootError, SyncRoot};

/// Rows of the change feed read per request, at most: whole pages (decrypted headers), and slim
/// ones (which file changed, a quarter of the bytes; most of a first read of a drive with many
/// deletes), which grow to their most in a run of them. Fewer requests save their round trips,
/// and a page is one commit.
pub const FEED_PAGES: feed::Pages = feed::Pages {
    whole: 500,
    slim: 2000,
};
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
    wakes: Wakes,
    /// Feed rows the client can't read, whoever pulled them, until taken.
    skipped: Mutex<Vec<String>>,
    /// The drive refused an upload, out of storage: until when uploads are held.
    full: Mutex<Option<Instant>>,
    /// Whether the account's picture is current.
    freshness: Freshness,
    /// The server's clock at the last page of the feed read, Unix ms (0: none yet).
    server_time: AtomicI64,
    /// A re-read of the whole feed on its way, between pulls ([`feed::Reread`]).
    reread: Mutex<Option<feed::Reread>>,
}

/// Wakes a folder's engine ([`Drive::subscribe_with`]).
type Wake = Box<dyn Fn() -> bool + Send>;

/// The folders to wake when the drive changed: each one's call, false once it is gone.
#[derive(Default)]
struct Wakes(Mutex<Vec<(i64, Wake)>>);

impl Wakes {
    fn add(&self, folder: i64, wake: Wake) {
        lock(&self.0).push((folder, wake));
    }

    /// Wakes the folders `wanted` takes; those gone are forgotten.
    fn wake_where(&self, wanted: impl Fn(i64) -> bool) {
        lock(&self.0).retain(|(folder, wake)| !wanted(*folder) || wake());
    }
}

/// Whether the account's picture of the drive is current: the notifications announce the
/// drive's changes, and nothing put it in doubt since the last read that worked.
#[derive(Debug, Default)]
struct Freshness {
    /// The identity announces the drive's changes: the notifications are connected, and the
    /// read that followed their connecting worked.
    announcing: AtomicBool,
    /// Counts the connections the notifications lost: a read asked for at a connection that
    /// was lost since says nothing about announcements ([`Drive::announcing_from`]).
    connections: AtomicU64,
    /// The account's picture may lack something (a pull failed, a write was refused for the
    /// drive's state or its answer was lost, the notifications were lost): a count of such
    /// doubts, and the count a read that worked has answered. Behind while the doubts are
    /// ahead: a read clears only the doubts from before it started.
    doubts: AtomicU64,
    answered_doubts: AtomicU64,
}

impl Freshness {
    /// The mark of the notifications' current connection ([`Self::announcing_from`]).
    fn connection(&self) -> u64 {
        self.connections.load(Ordering::SeqCst)
    }

    /// The read asked for at `connection` worked: unless it was lost since, the drive's
    /// changes are announced from now on.
    fn announcing_from(&self, connection: u64) {
        if self.connections.load(Ordering::SeqCst) == connection {
            self.announcing.store(true, Ordering::SeqCst);
        }
    }

    /// The notifications are lost or refused: changes go unannounced, and what was announced
    /// meanwhile may not have been taken.
    fn not_announcing(&self) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        self.announcing.store(false, Ordering::SeqCst);
        self.doubt();
    }

    /// The picture may lack something.
    fn doubt(&self) {
        self.doubts.fetch_add(1, Ordering::SeqCst);
    }

    /// The doubts so far: what a read starting now answers once it worked ([`Self::answered`]).
    fn doubts(&self) -> u64 {
        self.doubts.load(Ordering::SeqCst)
    }

    /// A read that worked answered the doubts there were when it started.
    fn answered(&self, doubts: u64) {
        self.answered_doubts.fetch_max(doubts, Ordering::SeqCst);
    }

    fn current(&self) -> bool {
        self.announcing.load(Ordering::SeqCst)
            && self.answered_doubts.load(Ordering::SeqCst) >= self.doubts.load(Ordering::SeqCst)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

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
            wakes: Wakes::default(),
            skipped: Mutex::default(),
            full: Mutex::default(),
            freshness: Freshness::default(),
            server_time: AtomicI64::new(0),
            reread: Mutex::default(),
        })
    }

    /// The notifications connected: the mark of this connection, for
    /// [`Drive::announcing_from`] once the read that follows worked.
    pub fn connection(&self) -> u64 {
        self.freshness.connection()
    }

    /// The read asked for when the notifications connected (`connection`) worked: unless that
    /// connection was lost since, the drive's changes are announced from now on.
    pub fn announcing_from(&self, connection: u64) {
        self.freshness.announcing_from(connection);
    }

    /// The notifications are lost or refused: changes go unannounced, and what was announced
    /// meanwhile may not have been taken, so the account is behind.
    pub fn not_announcing(&self) {
        self.freshness.not_announcing();
    }

    /// The account's picture may lack something: the next sync reads the feed.
    pub fn set_behind(&self) {
        self.freshness.doubt();
    }

    /// Behind after deleting for good when the drive refused something for its state
    /// (`moved`), or a request failed on the way.
    fn behind_if<T>(&self, moved: bool, done: &Result<T, EngineError>) {
        if moved || matches!(done, Err(EngineError::Remote(e)) if e.is_transient()) {
            self.set_behind();
        }
    }

    /// Whether the account's picture of the drive is as current as the announcements make it:
    /// they are connected and the last pull worked. Then nothing needs reading the feed to
    /// sync (this device's own writes are recorded from their answers); else a sync reads it.
    pub fn current(&self) -> bool {
        self.freshness.current()
    }

    /// Reads the feed ([`Drive::pull`]) unless the picture is [`Drive::current`].
    pub fn catch_up(&self, from: Option<i64>) -> Result<FeedReport, FeedError> {
        if self.current() {
            return Ok(FeedReport::default());
        }
        self.pull(from)
    }

    /// Whether uploads are held: the drive said it is full lately.
    pub(crate) fn full(&self) -> bool {
        let full = lock(&self.full);
        full.is_some_and(|until| Instant::now() < until)
    }

    /// Tries uploads again at once (the user asked for a sync: they may have made room).
    pub fn clear_full(&self) {
        *lock(&self.full) = None;
    }

    /// The drive refused an upload, out of storage: hold uploads for a while, in every folder.
    pub(crate) fn set_full(&self) {
        *lock(&self.full) = Some(Instant::now() + crate::time::FULL_HOLD);
    }

    fn remote(&self) -> &dyn Remote {
        self.remote.as_ref()
    }

    pub(crate) fn shared_remote(&self) -> Arc<dyn Remote> {
        Arc::clone(&self.remote)
    }

    /// Whether `db` is a connection to the drive's DB file.
    pub(crate) fn same_file(&self, db: &StateDb) -> bool {
        self.account().same_file(db)
    }

    fn account(&self) -> MutexGuard<'_, StateDb> {
        lock(&self.account)
    }

    /// Runs `op`, one of Recently deleted's operations on the account's DB and the drive; the
    /// account is behind if the drive refused a write for its state or an answer was lost.
    fn with_ops<T>(
        &self,
        op: impl FnOnce(&mut deleted::AccountOps<'_>) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let (done, behind) = deleted::AccountOps::run(&self.account, self.remote(), op);
        self.behind_if(behind, &done);
        done
    }

    /// Reads the feed until it has no more rows, into the mirror. One pull at a time: a caller
    /// that asked while another pull ran waits for it and pulls again only if that pull had
    /// started before it asked. Either way, what the drive had when it asked is in the mirror
    /// when it returns. Wakes the engines of the folders the changes concern, but `from`'s.
    /// The account's connection is free for others while a page is on its way.
    pub fn pull(&self, from: Option<i64>) -> Result<FeedReport, FeedError> {
        let ticket = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let mut answered = lock(&self.answered);
        if *answered >= ticket {
            return Ok(FeedReport::default());
        }
        // Every ask so far came before this pull starts: it answers them all.
        let covers = self.asked.load(Ordering::SeqCst);
        let doubts = self.freshness.doubts();
        let pulled = {
            let mut reread = lock(&self.reread);
            feed::pull(|| self.account(), self.remote(), FEED_PAGES, &mut reread)
        };
        match &pulled {
            // What was in doubt when it started is answered; a doubt since isn't.
            Ok(report) => {
                self.freshness.answered(doubts);
                *answered = covers;
                if let Some(now) = report.server_time {
                    self.server_time.store(now, Ordering::SeqCst);
                }
            }
            Err(_) => self.set_behind(),
        }
        // Still one at a time: what it touched is its to wake, the pages it committed before a
        // failure too.
        self.wake_touched(&mut self.account(), from);
        drop(answered);
        let mut report = pulled?;
        let mut skipped = lock(&self.skipped);
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
        let _one_at_a_time = lock(&self.answered);
        let mut db = self.account();
        let all = db.write(|s| feed::announced(s.account(), refs))?;
        self.wake_touched(&mut db, None);
        Ok(all)
    }

    /// The feed rows the client couldn't read (not encrypted, another key, not a sync node)
    /// since the last call, each with why; a thousand at most.
    pub fn take_skipped(&self) -> Vec<String> {
        std::mem::take(&mut *lock(&self.skipped))
    }

    /// A channel woken when the drive changed for `folder`'s engine (another engine's pull, or
    /// its writes).
    pub fn subscribe(&self, folder: i64) -> Receiver<()> {
        let (tx, rx) = channel();
        self.subscribe_with(folder, move || tx.send(()).is_ok());
        rx
    }

    /// Calls `wake` when the drive changed for `folder`'s engine, as [`Drive::subscribe`]
    /// does, until it returns false (its folder is gone).
    pub fn subscribe_with(&self, folder: i64, wake: impl Fn() -> bool + Send + 'static) {
        self.wakes.add(folder, Box::new(wake));
    }

    /// Wakes every folder's engine: the drive changed. Folders gone are forgotten.
    pub fn wake_all(&self) {
        self.wakes.wake_where(|_| true);
    }

    /// Wakes the engines of the folders the writes through `db` marked nodes dirty in
    /// ([`StateDb::take_touched`]), but `except`'s.
    pub(crate) fn wake_touched(&self, db: &mut StateDb, except: Option<i64>) {
        let folders = db.take_touched();
        self.wakes
            .wake_where(|folder| Some(folder) != except && folders.contains(&folder));
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
                    let row = mirror::written(&meta, None, written);
                    let mut db = self.account();
                    // As the executor records a folder it made.
                    db.write(|s| mirror::live(s.account(), &row))?;
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
                mirror::clear_tokens(s.account())?;
                s.account().set_account_kv(keys::TOKENS_SEALED_BY, by)?;
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
        let restored = self.with_ops(|ops| {
            nodes
                .iter()
                .try_fold(0, |n, &node| Ok(n + ops.restore(node)?))
        });
        self.wake_all();
        restored
    }

    /// Deletes these items of Recently deleted for good as `listed` showed them, each with
    /// what was in it then ([`deleted::AccountOps::empty`]).
    pub fn empty(&self, nodes: &[NodeId], listed: &deleted::Listed) -> Result<usize, EngineError> {
        self.catch_up(None)?;
        self.with_ops(|ops| ops.empty(nodes, listed))
    }

    /// Deletes for good what a listing of Recently deleted saw
    /// ([`deleted::AccountOps::empty_listed`]), nothing recorded since.
    pub fn empty_listed(&self, listed: &deleted::Listed) -> Result<usize, EngineError> {
        self.catch_up(None)?;
        self.with_ops(|ops| ops.empty_listed(listed))
    }

    /// Deletes for good what was deleted `keep_ms` ago or more: Recently deleted, by the
    /// server's clock (as read just now: a pull, whatever the announcements say, so a restore
    /// elsewhere is in the picture), and the trash of every folder.
    pub fn purge(&self, keep_ms: i64) -> Result<usize, EngineError> {
        self.pull(None)?;
        let now = self.server_time.load(Ordering::SeqCst);
        self.with_ops(|ops| ops.purge(keep_ms, now))
    }

    /// Once a day: deletes for good what Recently deleted has kept `keep_days`, never less
    /// than [`crate::time::MIN_KEEP_DAYS`] (and the trash of the same age). A failure waits for the
    /// next day's try... or the next call: it is only recorded as done when it worked.
    pub fn purge_when_due(&self, keep_days: u32) -> Result<Option<usize>, EngineError> {
        let keep_days = keep_days.max(crate::time::MIN_KEEP_DAYS);
        let now = hbsync_types::unix_now();
        let last = self
            .account()
            .read(|s| s.account_kv(keys::PURGED_AT))?
            .and_then(|v| v.parse::<u64>().ok());
        if last.is_some_and(|last| now < last + crate::time::DAY_S) {
            return Ok(None);
        }
        let gone = self.purge(i64::from(keep_days) * crate::time::DAY_MS)?;
        self.account().write(|s| {
            s.account()
                .set_account_kv(keys::PURGED_AT, &now.to_string())
        })?;
        Ok(Some(gone))
    }
}
