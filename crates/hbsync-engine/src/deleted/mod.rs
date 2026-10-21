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

use crate::drive::DriveNews;
use crate::error::EngineError;
use crate::policy::{InRun, policy};

mod listing;
mod purge;
mod restore;
mod rules;
#[cfg(test)]
mod tests;

pub use listing::{DeletedSummary, Listed, deletes, listed_here, listed_now};
pub use rules::{
    Chosen, Holdings, RestoreWave, before_folders_go, inside, passes, restore_wave,
    take_restored_name, taken_in,
};

/// The account's DB, held only while it is read or written: never while the server answers.
fn locked(account: &Mutex<StateDb>) -> MutexGuard<'_, StateDb> {
    account.lock().unwrap_or_else(|e| e.into_inner())
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

    /// The account's DB ([`locked`]).
    fn db(&self) -> MutexGuard<'a, StateDb> {
        locked(self.account)
    }
}
