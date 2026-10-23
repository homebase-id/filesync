//! One account's drive, shared by the engines of its folders on this device: the remote, the
//! change feed (pulled once for all of them), and what is the whole drive's rather than a
//! folder's: Recently deleted, restoring and emptying it, the purge.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hbsync_remote::{ChangeRef, ErrorClass, NodeMeta, Remote, RemoteError, Skipped};
use hbsync_state::{Answer, StateDb, StateError, keys};

use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, Role};

use crate::deleted;
use crate::error::EngineError;
use crate::feed::{self, FeedError, FeedReport};
use crate::lock;
use crate::mirror;
pub use crate::news::DriveNews;
use crate::root::{RootError, SyncRoot};

/// Rows of the change feed read per request, at most: whole pages (decrypted headers), and slim
/// ones (which file changed, a quarter of the bytes; most of a first read of a drive with many
/// deletes), which grow to their most in a run of them. Fewer requests save their round trips,
/// and a page is one commit.
pub const FEED_PAGES: feed::Pages = feed::Pages {
    whole: 500,
    slim: 2000,
};
/// Skipped feed rows one pull tells, at most.
const MAX_SKIPPED: usize = 1000;

/// Where the feed rows the client can't read go ([`Drive::tell_skipped`]).
type SkippedTo = Arc<dyn Fn(Vec<Skipped>) + Send + Sync>;

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
    /// Told the feed rows the client can't read, whoever pulled them; logged without it.
    skipped_to: Mutex<Option<SkippedTo>>,
    /// The drive refused an upload, out of storage: until when uploads are held.
    full: Mutex<Option<Instant>>,
    /// The identity refused this computer (401, 403), and why: nothing asks it again until
    /// the user acts or the notifications connect ([`Drive::deny`]).
    denied: Mutex<Option<String>>,
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
            skipped_to: Mutex::default(),
            full: Mutex::default(),
            denied: Mutex::default(),
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

    /// Takes in what a folder's run or an operation of Recently deleted learned of the drive
    /// ([`DriveNews`]): behind, and uploads held while it is full.
    pub(crate) fn absorb(&self, news: DriveNews) {
        if news.behind {
            self.set_behind();
        }
        if let Some(hold) = news.full {
            self.set_full(hold);
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

    /// The identity refused this computer's credentials (401, 403): the account's folders and
    /// its puller don't ask the drive again (each would be refused alike) until the user acts
    /// (Sync now, resume), the notifications connect again (the credentials work), or new
    /// credentials make a new drive. Whether it wasn't denied before.
    pub fn deny(&self, why: &str) -> bool {
        let mut denied = lock(&self.denied);
        if denied.is_none() {
            tracing::warn!(
                "the identity refused this computer ({why}): asking no more until a login or Sync now"
            );
        }
        denied.replace(why.to_owned()).is_none()
    }

    /// A failure of the account's work on the drive, of `class` (why: `e`): one the identity
    /// refused latches the account ([`Drive::deny`]). Every pull, every operation of Recently
    /// deleted ([`Drive::with_ops`]) and every folder's sync ([`crate::Engine`]) comes here as
    /// it fails.
    pub(crate) fn latch(&self, class: ErrorClass, e: &dyn std::fmt::Display) {
        if class == ErrorClass::Denied {
            self.deny(&e.to_string());
        }
    }

    /// Why the identity refused this computer, while that holds ([`Drive::deny`]).
    pub fn denied(&self) -> Option<String> {
        lock(&self.denied).clone()
    }

    /// Whether the identity refuses this computer ([`Drive::deny`]).
    pub fn is_denied(&self) -> bool {
        lock(&self.denied).is_some()
    }

    /// The drive is asked again ([`Drive::deny`]).
    pub fn allow(&self) {
        *lock(&self.denied) = None;
    }

    /// The drive refused an upload, out of storage: hold uploads in every folder for `hold`
    /// ([`crate::time::full_hold`]).
    pub(crate) fn set_full(&self, hold: Duration) {
        *lock(&self.full) = Some(Instant::now() + hold);
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

    /// Runs `op`, one of Recently deleted's operations on the account's DB and the drive, and
    /// takes in what it learned of the drive ([`deleted::AccountOps::run`]); a failure the
    /// identity refused latches the account.
    fn with_ops<T>(
        &self,
        op: impl FnOnce(&mut deleted::AccountOps<'_>) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let (done, news) = deleted::AccountOps::run(&self.account, self.remote(), op);
        self.absorb(news);
        if let Err(e) = &done {
            self.latch(e.class(), e);
        }
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
            Err(e) => {
                self.set_behind();
                self.latch(e.class(), e);
            }
        }
        // Still one at a time: what it touched is its to wake, the pages it committed before a
        // failure too.
        self.wake_touched(&mut self.account(), from);
        drop(answered);
        let mut report = pulled?;
        let mut skipped = std::mem::take(&mut report.skipped);
        if !skipped.is_empty() {
            skipped.truncate(MAX_SKIPPED);
            // Told without the lock: pulls on other threads don't wait for it.
            let tell = lock(&self.skipped_to).clone();
            match tell {
                Some(tell) => tell(skipped),
                None => {
                    for row in &skipped {
                        tracing::warn!("a row of the drive can't be read: {}", row.why);
                    }
                }
            }
        }
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

    /// Tells `tell` the feed rows the client can't read (not encrypted, another key, not a sync
    /// node), each with why, as a pull (anyone's) finds them: a thousand a pull at most. Told on
    /// the pulling thread.
    pub fn tell_skipped(&self, tell: impl Fn(Vec<Skipped>) + Send + Sync + 'static) {
        *lock(&self.skipped_to) = Some(Arc::new(tell));
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
                    let answer = Answer::live(None, mirror::written(&meta, None, written));
                    let mut db = self.account();
                    // As the executor records a folder it made.
                    let behind = db.write(|s| mirror::answered(s.account(), &[answer]))?;
                    self.absorb(DriveNews { behind, full: None });
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
                s.account().clear_tokens()?;
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
        self.restore_told(nodes).map(|r| r.back)
    }

    /// [`Drive::restore`], with what stayed in Recently deleted and what went for good
    /// ([`deleted::Restored`]): the user's restore, to tell them.
    pub fn restore_told(&self, nodes: &[NodeId]) -> Result<deleted::Restored, EngineError> {
        self.pull(None)?;
        let restored = self.with_ops(|ops| ops.restore_told(nodes));
        self.wake_all();
        restored
    }

    /// [`Drive::restore`] for a caller that has just read the feed itself.
    pub(crate) fn restore_read(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        let restored = self.with_ops(|ops| ops.restore(nodes));
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
    /// elsewhere is in the picture).
    pub fn purge(&self, keep_ms: i64) -> Result<usize, EngineError> {
        self.pull(None)?;
        let now = self.server_time.load(Ordering::SeqCst);
        self.with_ops(|ops| ops.purge(keep_ms, now))
    }

    /// Asks again about the files of Recently deleted the identity said once weren't there, a
    /// minute to an hour ago ([`deleted::AccountOps::ask_again`]): a missed delete for good (a
    /// hard delete leaves no trace in the feed) is confirmed so, its row forgotten, without
    /// waiting for the next day's purge. Returns how many went for good.
    pub fn ask_again(&self) -> Result<usize, EngineError> {
        let now = hbsync_types::unix_now_ms();
        self.with_ops(|ops| ops.ask_again(now))
    }

    /// Whether Recently deleted holds a file the identity said once wasn't there, within the
    /// hour (one [`Drive::ask_again`] would ask about, now or later). One query.
    pub fn doubted(&self) -> Result<bool, EngineError> {
        let now = hbsync_types::unix_now_ms();
        let suspects = self.account().read(|s| s.removed_suspects())?;
        Ok(suspects
            .iter()
            .any(|(_, first)| crate::gone::still_doubted(first, now)))
    }

    /// Once a day: deletes for good what Recently deleted has kept
    /// [`crate::time::KEEP_DAYS`]. A failure waits for the next day's try... or the next call:
    /// it is only recorded as done when it worked.
    pub fn purge_when_due(&self) -> Result<Option<usize>, EngineError> {
        let now = hbsync_types::unix_now();
        let last = self
            .account()
            .read(|s| s.account_kv(keys::PURGED_AT))?
            .and_then(|v| v.parse::<u64>().ok());
        if last.is_some_and(|last| now < last + crate::time::DAY_S) {
            return Ok(None);
        }
        let gone = self.purge(i64::from(crate::time::KEEP_DAYS) * crate::time::DAY_MS)?;
        self.account().write(|s| {
            s.account()
                .set_account_kv(keys::PURGED_AT, &now.to_string())
        })?;
        Ok(Some(gone))
    }
}
