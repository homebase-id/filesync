//! Recently deleted (node schema): what a delete took out of the tree, kept on the drive with
//! its bytes until the purge. A restore puts it back in the tree, where the next sync brings it
//! to every device; the purge deletes it for good.
//!
//! It is the whole drive's, kept with the account ([`crate::drive::Drive`]): each item comes
//! back to the folder of this device whose part of the drive it was in, if any.
//!
//! - `listing`: what Recently deleted holds, as listed, and the listing's token ([`Listed`]).
//! - `rules`: the pure rules: what a delete took along, what emptying deletes and in which
//!   order, and a restore's waves.
//! - `restore` and `purge`: the operations on the drive ([`AccountOps`]).

use std::sync::{Mutex, MutexGuard};

use hbsync_remote::{Remote, RemoteError};
use hbsync_state::StateDb;

use crate::error::EngineError;
use crate::news::DriveNews;
use crate::policy::{InRun, policy};

mod listing;
mod purge;
mod restore;
mod rules;
#[cfg(test)]
mod tests;

pub use listing::{DeletedSummary, Listed, deletes, listed_here, listed_now};
pub use restore::{RestorePlan, RestoreSource, Restored, plan_restore_from};
pub use rules::deleted_with_at;
pub(crate) use rules::item_by;
pub use rules::{
    Chosen, Holdings, RestoreWave, before_folders_go, inside, passes, restore_wave,
    take_restored_name, taken_in,
};

use crate::lock;

/// A delete for good as it goes (emptying Recently deleted, the purge), for its caller to stop
/// and show: asked between batches whether to stop, told after each how far it got.
#[derive(Clone, Copy)]
pub struct Going<'a> {
    /// Whether to stop, asked before each pass and each batch (many files a request): it then
    /// ends with [`Stopped::Requested`](crate::exec::Stopped::Requested). What went is written
    /// batch by batch, the rest stays in Recently deleted, and no later pass runs, so nothing
    /// in a folder is left behind by it.
    pub stop: &'a dyn Fn() -> bool,
    /// Told how many items went, of how many it set out to delete: as it starts, and after
    /// each batch is written.
    pub told: &'a dyn Fn(usize, usize),
}

impl Going<'static> {
    /// Never stopped, telling no one.
    pub const UNWATCHED: Going<'static> = Going {
        stop: &|| false,
        told: &|_, _| {},
    };
}

impl Going<'_> {
    /// Asked to stop: [`Stopped::Requested`](crate::exec::Stopped::Requested).
    fn check(&self) -> Result<(), EngineError> {
        match (self.stop)() {
            true => Err(EngineError::Stopped(crate::exec::Stopped::Requested)),
            false => Ok(()),
        }
    }
}

impl std::fmt::Debug for Going<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Going").finish_non_exhaustive()
    }
}

/// Recently deleted's operations on the account's drive: restoring, emptying, the purge. The
/// account's DB is held only while it is read or written, never while the server answers.
pub struct AccountOps<'a> {
    account: &'a Mutex<StateDb>,
    remote: &'a dyn Remote,
    /// The drive refused a write for its state (changed, restored, or deleted for good
    /// meanwhile), or a write may have landed unanswered: the account's picture lacks that.
    /// [`AccountOps::run`] returns it.
    behind: bool,
}

impl std::fmt::Debug for AccountOps<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountOps")
            .field("behind", &self.behind)
            .finish_non_exhaustive()
    }
}

impl<'a> AccountOps<'a> {
    /// Runs `op`, operations on the account's DB and `remote`: its result, and what it learned
    /// of the drive ([`DriveNews`]), which the caller must take in: behind if the drive refused
    /// a write for its state, an answer was lost, or a request failed on the way (it may have
    /// landed).
    #[must_use = "what the drive said must be taken in"]
    pub fn run<T>(
        account: &'a Mutex<StateDb>,
        remote: &'a dyn Remote,
        op: impl FnOnce(&mut AccountOps<'a>) -> Result<T, EngineError>,
    ) -> (Result<T, EngineError>, DriveNews) {
        let mut ops = AccountOps::new(account, remote);
        let done = op(&mut ops);
        let failed = |e: &RemoteError| policy(e.class()).in_run == InRun::Backoff;
        let news = DriveNews {
            behind: ops.behind || matches!(&done, Err(EngineError::Remote(e)) if failed(e)),
            full: None,
        };
        (done, news)
    }

    fn new(account: &'a Mutex<StateDb>, remote: &'a dyn Remote) -> AccountOps<'a> {
        AccountOps {
            account,
            remote,
            behind: false,
        }
    }

    /// The account's DB, locked: held only while it is read or written, never while the server
    /// answers.
    fn db(&self) -> MutexGuard<'a, StateDb> {
        lock(self.account)
    }
}
