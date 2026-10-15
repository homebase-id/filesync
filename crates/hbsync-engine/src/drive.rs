//! One account's drive, shared by the engines of its folders on this device: the remote, the
//! change feed (pulled once for all of them), and what is the whole drive's rather than a
//! folder's: Recently deleted, restoring and emptying it, the purge.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{StateDb, StateError, keys};
use hbsync_types::NodeId;
use hbsync_types::activity::Deleted;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeKind, Role};

use crate::deleted;
use crate::engine::EngineError;
use crate::feed::{self, FeedError, FeedReport};

/// Rows of the change feed read per request.
pub const FEED_PAGE: usize = 500;
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
}

/// How long uploads are held after the drive said it is full: then one is tried again.
const FULL_HOLD: Duration = Duration::from_secs(600);

impl std::fmt::Debug for Drive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Drive").finish_non_exhaustive()
    }
}

impl Drive {
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
        })
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
        let mut report = feed::pull(|| self.account(), self.remote(), FEED_PAGE)?;
        *answered = covers;
        drop(answered);
        self.wake_touched(&mut self.account(), from);
        let mut skipped = self.skipped.lock().unwrap_or_else(|e| e.into_inner());
        let room = MAX_SKIPPED.saturating_sub(skipped.len());
        skipped.extend(std::mem::take(&mut report.skipped).into_iter().take(room));
        Ok(report)
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
            };
            match self.remote().create(&meta, None) {
                // Another device made it (or deleted it) meanwhile: seen below.
                Ok(_) | Err(RemoteError::AlreadyExists) => {}
                Err(e) => return Err(EngineError::Remote(e)),
            }
            self.pull(None)?;
        }
        if !live(self)? {
            return Err(EngineError::Remote(RemoteError::Transient(format!(
                "{} isn't on the drive yet",
                role.name()
            ))));
        }
        Ok(())
    }

    /// Recently deleted, newest first, each item with the folder it would come back to.
    pub fn recently_deleted(&self, limit: usize) -> Result<Vec<Deleted>, StateError> {
        self.account().read(|s| deleted::list(s, limit))
    }

    /// Restores these items of Recently deleted (each with what was deleted with it); the
    /// next syncs bring them back. Returns how many items came back.
    pub fn restore(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.pull(None)?;
        let mut restored = 0;
        for &node in nodes {
            restored += deleted::restore(&mut self.account(), self.remote(), node)?;
        }
        self.wake_all();
        Ok(restored)
    }

    /// Deletes these items of Recently deleted for good, each with what was deleted with it.
    pub fn empty(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.pull(None)?;
        deleted::empty(&mut self.account(), self.remote(), nodes)
    }

    /// Deletes for good everything removed until `at_ms` (Unix ms): what the user was shown.
    pub fn empty_until(&self, at_ms: i64) -> Result<usize, EngineError> {
        self.pull(None)?;
        deleted::empty_until(&mut self.account(), self.remote(), at_ms)
    }

    /// Deletes for good what was deleted before `before_ms` (Unix ms): Recently deleted, and
    /// the trash of every folder.
    pub fn purge(&self, before_ms: i64) -> Result<usize, EngineError> {
        self.pull(None)?;
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
