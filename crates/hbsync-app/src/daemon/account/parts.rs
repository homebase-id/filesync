//! An account's parts once its identity answers, made one way for the daemon's account and the
//! dev tool `hbsync-sync`.

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::Instant;

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_remote::Remote;

use super::feed::{Notifications, Puller};
use crate::runner::RunnerError;

/// An account's drive and what brings it the drive's changes.
#[derive(Debug)]
pub struct AccountParts {
    pub drive: Arc<Drive>,
    /// Its notifications, sending their events on; an error when they couldn't start (the
    /// daemon then reads the drive on a timer: [`Notifications`]).
    pub notifier: std::io::Result<Notifier>,
    /// Takes the notifications' events: changes announced are pulled by its puller.
    pub notifications: Notifications,
}

impl AccountParts {
    /// The account `identity`, its state DB at `db`, on `remote`, its notifications' events
    /// sent to `events`.
    pub fn open(
        identity: &str,
        remote: Arc<HomebaseRemote>,
        db: &Path,
        events: Sender<Event>,
    ) -> Result<Self, RunnerError> {
        let drive = Self::open_drive(identity, Arc::clone(&remote), db)?;
        let notifier = Notifier::spawn(remote.client().clone(), remote.drive(), events);
        Ok(AccountParts {
            notifier,
            notifications: Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now()),
            drive,
        })
    }

    /// Only the drive of [`AccountParts::open`], for a sync that doesn't listen. A DB that
    /// synced with a drive made at another time than `remote`'s is refused
    /// ([`RunnerError::OtherDrive`]): its account sets it aside. The files' tokens it kept for
    /// another login are forgotten: they don't open under `remote`'s.
    pub fn open_drive(
        identity: &str,
        remote: Arc<HomebaseRemote>,
        db: &Path,
    ) -> Result<Arc<Drive>, RunnerError> {
        let (login, made) = (remote.client().login_id().to_owned(), remote.made());
        let drive = drive_made(identity, remote, made, db)?;
        // Tokens are sealed under the login's shared secret: another login's don't open.
        if drive.tokens_sealed_by(&login)? {
            tracing::info!("a new login: the files' tokens are read again as they're written");
        }
        Ok(drive)
    }
}

/// The account's drive on `remote`, made at `made` (its creation time), from its DB at `db`:
/// refused if the DB synced with another ([`RunnerError::OtherDrive`]), taken by one that knows
/// none yet.
pub(super) fn drive_made(
    identity: &str,
    remote: Arc<dyn Remote>,
    made: i64,
    db: &Path,
) -> Result<Arc<Drive>, RunnerError> {
    let drive = Drive::open(db, identity, remote)?;
    if drive.other_drive(made)? {
        return Err(RunnerError::OtherDrive);
    }
    Ok(drive)
}
