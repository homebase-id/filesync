//! An account's parts once its identity answers, made one way for the daemon's account and the
//! dev tool `hbsync-sync`.

use std::path::{Path, PathBuf};
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
    /// What other identities share with the account, each a drive of its own: synced into a
    /// folder of its own in the main folder's folder of shares (sync semantics §10, §13).
    /// Whoever makes them leaves out a share whose drive or DB can't be opened (told), and
    /// tries it again: that is never the account's error, whose own folders sync on.
    pub shares: Vec<Share>,
    /// Whose shares are synced, as the identity's share markers say, and how each is opened:
    /// read whenever a marker changed ([`hbsync_homebase::notify::Event::Markers`]) and
    /// whenever the drive is read for what wasn't announced (as the notifications connect;
    /// docs/spec/daemon.md, "Whose shares are synced"). `None`: only `shares`, for good.
    pub sharers: Option<Box<dyn Sharers>>,
}

/// Whose shares an account syncs, and how each is opened.
pub trait Sharers: Send + std::fmt::Debug {
    /// What the identity's share markers say now. `synced`: whose shares are synced or
    /// being opened at present, for [`Synced::unsure`].
    fn told(&self, synced: &[crate::Identity]) -> Result<Synced, RunnerError>;

    /// What `sharer` shares, opened to sync: its drive as the account may read it, on a
    /// state DB of its own.
    fn open(&self, sharer: &crate::Identity) -> Result<Share, RunnerError>;
}

/// What an identity's share markers say ([`Sharers::told`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Synced {
    /// Whose shares to sync.
    pub on: Vec<crate::Identity>,
    /// Those of the ones synced or being opened of whom the markers don't say (one that may
    /// be theirs didn't read): each stays as it is, never taken for off, nor for on again.
    pub unsure: Vec<crate::Identity>,
}

/// What another identity shares with the account: its drive as the account may read it.
#[derive(Debug)]
pub struct Share {
    /// Whose drive it is.
    pub sharer: crate::Identity,
    /// The drive, on a state DB of its own ([`crate::Paths::shared_db`]).
    pub drive: Arc<Drive>,
    /// That DB.
    pub db: PathBuf,
    /// What brings it the drive's changes: read on a timer until they are announced.
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
            // By the identity's share markers, once each share can be opened with a token
            // of its own that is kept (not yet: none is synced).
            shares: Vec::new(),
            sharers: None,
        })
    }

    /// Only the drive of [`AccountParts::open`], for a sync that doesn't listen. A DB that
    /// synced with a drive made at another time than `remote`'s is refused
    /// ([`RunnerError::OtherDrive`]): the daemon's account sets it aside; `hbsync-sync` stops.
    /// The files' tokens it kept for another login are forgotten: they don't open under
    /// `remote`'s.
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
