//! One account's drive, shared by the engines of its folders on this device: the remote, the
//! change feed (pulled once for all of them), and what is the whole drive's rather than a
//! folder's: Recently deleted, restoring and emptying it, the purge.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};

use hbsync_remote::Remote;
use hbsync_state::{StateDb, StateError};
use hbsync_types::NodeId;
use hbsync_types::activity::Deleted;

use crate::deleted;
use crate::engine::EngineError;
use crate::feed::{self, FeedError, FeedReport};

/// Rows of the change feed read per request.
pub const FEED_PAGE: usize = 500;

/// The state DB key of the last purge of Recently deleted (Unix seconds).
const PURGED_KEY: &str = "deleted.purged_at";

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
}

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
        })
    }

    pub fn remote(&self) -> &dyn Remote {
        self.remote.as_ref()
    }

    fn account(&self) -> MutexGuard<'_, StateDb> {
        self.account.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reads the feed until it has no more rows, into the mirror. One pull at a time: a caller
    /// that asked while another pull ran waits for it and pulls again only if that pull had
    /// started before it asked. Either way, what the drive had when it asked is in the mirror
    /// when it returns. Wakes the other folders' engines when something changed.
    pub fn pull(&self, from: i64) -> Result<FeedReport, FeedError> {
        let ticket = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let mut answered = self.answered.lock().unwrap_or_else(|e| e.into_inner());
        if *answered >= ticket {
            return Ok(FeedReport::default());
        }
        // Every ask so far came before this pull starts: it answers them all.
        let covers = self.asked.load(Ordering::SeqCst);
        let report = feed::pull(&mut self.account(), self.remote(), FEED_PAGE)?;
        *answered = covers;
        drop(answered);
        if report.live + report.deleted > 0 {
            self.wake(Some(from));
        }
        Ok(report)
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

    /// Wakes every folder's engine but `except`'s: the drive changed. Folders gone are
    /// forgotten.
    pub fn wake(&self, except: Option<i64>) {
        self.wakes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(folder, tx)| Some(*folder) == except || tx.send(()).is_ok());
    }

    /// Recently deleted, newest first, each item with the folder it would come back to.
    pub fn recently_deleted(&self, limit: usize) -> Result<Vec<Deleted>, StateError> {
        deleted::list(&mut self.account(), limit)
    }

    /// Restores these items of Recently deleted (each with what was deleted with it); the
    /// next syncs bring them back. Returns how many items came back.
    pub fn restore(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.pull(0)?;
        let mut restored = 0;
        for &node in nodes {
            restored += deleted::restore(&mut self.account(), self.remote(), node)?;
        }
        self.wake(None);
        Ok(restored)
    }

    /// Deletes these items of Recently deleted for good, each with what was deleted with it.
    pub fn empty(&self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.pull(0)?;
        deleted::empty(&mut self.account(), self.remote(), nodes)
    }

    /// Deletes for good everything removed until `at_ms` (Unix ms): what the user was shown.
    pub fn empty_until(&self, at_ms: i64) -> Result<usize, EngineError> {
        self.pull(0)?;
        deleted::empty_until(&mut self.account(), self.remote(), at_ms)
    }

    /// Deletes for good what was deleted before `before_ms` (Unix ms): Recently deleted, and
    /// the trash of every folder.
    pub fn purge(&self, before_ms: i64) -> Result<usize, EngineError> {
        self.pull(0)?;
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
            .read(|s| s.account_kv(PURGED_KEY))?
            .and_then(|v| v.parse::<u64>().ok());
        if last.is_some_and(|last| now < last + DAY) {
            return Ok(None);
        }
        let before_ms = now.saturating_sub(u64::from(keep_days) * DAY) as i64 * 1000;
        let gone = self.purge(before_ms)?;
        self.account()
            .write(|s| s.set_account_kv(PURGED_KEY, &now.to_string()))?;
        Ok(Some(gone))
    }
}
