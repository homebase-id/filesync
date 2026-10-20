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

use hbsync_remote::Remote;
use hbsync_state::StateDb;

use crate::error::EngineError;

mod listing;
mod purge;
mod restore;
mod rules;
#[cfg(test)]
mod tests;

pub use listing::{DeletedSummary, Listed, deletes, listed_here, listed_now};
pub use rules::{Chosen, Holdings, RestoreWave, before_folders_go, inside, passes, restore_wave};

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
    /// Runs `op`, operations on the account's DB and `remote`: its result, and whether the
    /// account is behind after it (the drive refused a write for its state, or an answer was
    /// lost), which the caller must take in.
    #[must_use = "whether the account fell behind must be taken in"]
    pub fn run<T>(
        account: &'a Mutex<StateDb>,
        remote: &'a dyn Remote,
        op: impl FnOnce(&mut AccountOps<'a>) -> Result<T, EngineError>,
    ) -> (Result<T, EngineError>, bool) {
        let mut ops = AccountOps::new(account, remote);
        let done = op(&mut ops);
        (done, ops.behind)
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
