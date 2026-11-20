//! An account's parts once its identity answers, made one way for the daemon's account and the
//! dev tool `hbsync-sync`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

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
    /// being opened at present, for [`Synced::unsure`] (with those it finds [`Synced::kept`]:
    /// at a start nothing is synced yet).
    fn told(&self, synced: &[crate::Identity]) -> Result<Synced, RunnerError>;

    /// What `sharer` shares, opened to sync: its drive as the account may read it, on a
    /// state DB of its own. Or (`to_end`) only to take its copy off this device: nothing is
    /// asked of the sharer for that, so it is to open without them (a share that ended
    /// gives no access any more).
    fn open(&self, sharer: &crate::Identity, to_end: bool) -> Result<Share, RunnerError>;

    /// How long after a read that says a share is off a second one is to say so too, before
    /// it is taken for off ([`OFF_CONFIRM`]).
    fn confirm_after(&self) -> Duration {
        OFF_CONFIRM
    }
}

/// A share the markers turn off is off once two reads, this long apart at least, say so: its
/// copy leaves this device then, all of it, and one answer that left a marker out (a listing
/// from before it was written) would take it off only to bring it back. A minute at least,
/// as for a file found gone (sync semantics §8b); unlike there, no longest time, and a
/// restart starts anew. It costs one more read of the markers each time a share is turned
/// off.
pub(super) const OFF_CONFIRM: Duration = Duration::from_secs(60);

/// What an identity's share markers say ([`Sharers::told`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Synced {
    /// Whose shares to sync.
    pub on: Vec<crate::Identity>,
    /// Those of the ones synced, being opened or [`Synced::kept`] of whom the markers don't
    /// say (one that may be theirs didn't read): each stays as it is, never taken for off
    /// (a kept one's copy stays), nor for on again.
    pub unsure: Vec<crate::Identity>,
    /// Whose shares have a state DB on this device: synced here before. One of them that
    /// isn't `on` (turned off while the daemon didn't run, or its ending cut short) has
    /// its copy taken off this device.
    pub kept: Vec<crate::Identity>,
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

impl Share {
    /// What `sharer` shares, as `drive` on its state DB at `db`: read on a timer from now,
    /// by a puller of its own.
    pub fn new(sharer: &crate::Identity, drive: Arc<Drive>, db: PathBuf) -> Share {
        Share {
            sharer: sharer.clone(),
            notifications: Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now()),
            drive,
            db,
        }
    }
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
            // By the identity's share markers, where the caller sets `sharers` (the daemon's
            // account does: `sharers::ByMarkers`).
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
