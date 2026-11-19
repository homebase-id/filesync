//! The account's folders' runners, on the account's thread: one that failed starts again
//! after a pause, backing off; one whose folder was removed stops for good.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_state::Unusable;

use std::path::Path;

use super::parts::{Share, Sharers};
use crate::runner::{Backoff, Command, Handle, RunnerError};
use crate::{Identity, Root};

/// A folder's runner, on the account's thread.
pub(super) struct Running {
    /// Its folder, as the config lists it; a share's ([`Running::sharer`]) isn't listed: its
    /// place in the main folder's folder of shares.
    pub(super) root: Root,
    /// Whose drive it syncs, when another identity's ([`Sharing`]): not one of the account's
    /// folders (its main folder, the ones it readies and sets up again).
    pub(super) sharer: Option<Identity>,
    pub(super) handle: Handle,
    pub(super) life: Life,
    /// Its thread is ending: joining it waits for no more than that.
    pub(super) done: Arc<AtomicBool>,
    pub(super) started: Instant,
    pub(super) backoff: Backoff,
    /// The main folder's: started to hold a folder of shares (the account had a share to
    /// sync then).
    pub(super) shared: bool,
}

pub(super) enum Life {
    Syncing(JoinHandle<Result<(), RunnerError>>),
    /// Asked to stop for good (its folder removed): it no longer shows.
    Stopping(JoinHandle<Result<(), RunnerError>>),
    /// Failed: started again at this time (not while the account ends).
    Waiting(Instant),
    /// Ended, to be dropped.
    Gone,
}

/// What another identity shares with the account, on the account's thread: its runner
/// ([`Running::sharer`]) syncs it into `<the folder of shares>/<local>` while the main
/// folder's runner shows and has told where the folder of shares is.
pub(super) struct Sharing {
    pub(super) share: Share,
    /// The sharer's name as a folder's here.
    pub(super) local: String,
    /// Its runner couldn't start: tried again at this time.
    pub(super) retry: Option<Instant>,
    /// The pauses between such tries, from the first again once it started.
    pub(super) backoff: Backoff,
    /// The identity's share markers say it isn't synced any more: its runner stops, and it
    /// goes with it.
    pub(super) off: bool,
}

/// What the identity's share markers say, followed on the account's thread: read when due,
/// each share they turn on opened (tried again while it can't be).
pub(super) struct Markers {
    source: Option<Box<dyn Sharers>>,
    /// When to read them (again): at the start, as one changed, after a read that failed.
    pub(super) read_at: Option<Instant>,
    read_backoff: Backoff,
    /// Shares to open: turned on, and not opened yet.
    opening: Vec<Opening>,
}

struct Opening {
    sharer: Identity,
    at: Instant,
    backoff: Backoff,
    /// Why it can't be opened was told.
    told: bool,
}

/// What following the markers changed ([`Markers::follow`]).
#[derive(Default)]
pub(super) struct Followed {
    /// The shares to run changed.
    pub(super) changed: bool,
    /// For the account's status: a share that can't be opened, said once.
    pub(super) notes: Vec<String>,
}

impl Markers {
    pub(super) fn new(source: Option<Box<dyn Sharers>>, now: Instant) -> Markers {
        Markers {
            read_at: source.as_ref().map(|_| now),
            source,
            read_backoff: Backoff::new(),
            opening: Vec::new(),
        }
    }

    /// A marker changed, or may have (the notifications connected again): read at once.
    pub(super) fn changed(&mut self, now: Instant) {
        if self.source.is_some() {
            self.read_at = Some(now);
        }
    }

    /// When to [`Markers::follow`] at the latest: a read, or a share to try opening again.
    pub(super) fn deadline(&self) -> Option<Instant> {
        let opening = self.opening.iter().map(|o| o.at);
        self.read_at.into_iter().chain(opening).min()
    }

    /// Reads the markers if due, and has `shares` follow them: one they turn off is marked
    /// ([`Sharing::off`]: its runner stops, and it goes), one they are unsure of stays, one
    /// they turn on is opened and added (now, or at a later call while it can't be). While
    /// they can't be read, all stays as it is.
    pub(super) fn follow(&mut self, shares: &mut Vec<Sharing>, now: Instant) -> Followed {
        let mut followed = Followed::default();
        let Some(source) = &self.source else {
            return followed;
        };
        if self.read_at.is_some_and(|at| now >= at) {
            let synced: Vec<Identity> = shares.iter().map(|s| s.share.sharer.clone()).collect();
            match source.told(&synced) {
                Ok(told) => {
                    (self.read_at, self.read_backoff) = (None, Backoff::new());
                    for share in shares.iter_mut() {
                        let sharer = &share.share.sharer;
                        let off = !told.on.contains(sharer) && !told.unsure.contains(sharer);
                        if off != share.off {
                            let now_is = if off {
                                "no longer synced"
                            } else {
                                "synced again"
                            };
                            tracing::info!("what {sharer} shares is {now_is}, by its marker");
                            (share.off, followed.changed) = (off, true);
                        }
                    }
                    self.opening.retain(|o| told.on.contains(&o.sharer));
                    for sharer in told.on {
                        let known = shares.iter().any(|s| s.share.sharer == sharer)
                            || self.opening.iter().any(|o| o.sharer == sharer);
                        if !known {
                            let (backoff, told) = (Backoff::new(), false);
                            let at = now;
                            self.opening.push(Opening {
                                sharer,
                                at,
                                backoff,
                                told,
                            });
                        }
                    }
                }
                Err(e) => {
                    let pause = self.read_backoff.next();
                    tracing::warn!(
                        "the share markers can't be read ({e}): what is synced stays as it \
                         is; read again in {pause:?}"
                    );
                    self.read_at = Some(now + pause);
                }
            }
        }
        // Each share to open that is due; one that can't be is tried again, the others and
        // the account's own folders syncing on.
        let mut waiting = Vec::new();
        for mut opening in std::mem::take(&mut self.opening) {
            if now < opening.at {
                waiting.push(opening);
                continue;
            }
            match source.open(&opening.sharer) {
                Ok(share) => {
                    if let Some(share) = Sharing::new(share).filter(|new| new.fits(shares)) {
                        shares.push(share);
                        followed.changed = true;
                    }
                }
                Err(e) => {
                    let pause = opening.backoff.next();
                    let sharer = &opening.sharer;
                    tracing::warn!(
                        "what {sharer} shares can't be opened ({e}): trying again in {pause:?}"
                    );
                    if !std::mem::replace(&mut opening.told, true) {
                        followed
                            .notes
                            .push(crate::words::share_unopened(sharer, &e));
                    }
                    opening.at = now + pause;
                    waiting.push(opening);
                }
            }
        }
        self.opening = waiting;
        followed
    }
}

impl Sharing {
    /// `share` to run; `None` for a sharer whose name no folder here can have (told once).
    pub(super) fn new(share: Share) -> Option<Sharing> {
        let os = hbsync_platform::name::Os::current();
        let local = match hbsync_namemap::to_local(share.sharer.as_str(), os) {
            Ok(local) => local.name,
            Err(e) => {
                tracing::error!(
                    "what {} shares isn't synced: no folder here can have its name ({e:?})",
                    share.sharer
                );
                return None;
            }
        };
        Some(Sharing {
            share,
            local,
            retry: None,
            backoff: Backoff::new(),
            off: false,
        })
    }

    /// Whether it can run beside `others`: its sharer isn't among them, nor its folder's
    /// name (as names compare here). Two runners on one folder would each take the other's
    /// files for changes made here. Said where not.
    fn fits(&self, others: &[Sharing]) -> bool {
        let key = hbsync_platform::name_key(&self.local);
        let taken = |other: &Sharing| {
            other.share.sharer == self.share.sharer
                || hbsync_platform::name_key(&other.local) == key
        };
        let fits = !others.iter().any(taken);
        if !fits {
            tracing::error!(
                "what {} shares isn't synced: its folder {} is another share's",
                self.share.sharer,
                self.local
            );
        }
        fits
    }

    /// `shares` to run, each sharer once, each under a folder's name of its own: one more of
    /// a sharer, or of a name another has (as names compare here), is left out, told once.
    /// Two runners on one folder would each take the other's files for changes made here.
    pub(super) fn each(shares: Vec<Share>) -> Vec<Sharing> {
        let mut each: Vec<Sharing> = Vec::new();
        for share in shares.into_iter().filter_map(Sharing::new) {
            if share.fits(&each) {
                each.push(share);
            }
        }
        each
    }

    /// Whether `folder` is its folder in the folder of shares `dir`.
    pub(super) fn is_at(&self, dir: &Path, folder: &Path) -> bool {
        folder.parent() == Some(dir) && folder.file_name().is_some_and(|name| name == &*self.local)
    }
}

/// A runner that ran this long without failing starts over with short pauses.
const RAN_WELL: Duration = Duration::from_secs(3600);

impl Running {
    /// Whether the status shows it: not once its folder was removed.
    pub(super) fn shows(&self) -> bool {
        matches!(self.life, Life::Syncing(_) | Life::Waiting(_))
    }

    /// Its folder was removed: it stops between operations and isn't started again.
    pub(super) fn stop_for_good(&mut self) {
        self.handle.send(Command::Stop);
        self.life = match std::mem::replace(&mut self.life, Life::Gone) {
            Life::Syncing(t) | Life::Stopping(t) => Life::Stopping(t),
            Life::Waiting(_) | Life::Gone => Life::Gone,
        };
    }

    /// When it is to start again, if it waits to.
    pub(super) fn restart_at(&self) -> Option<Instant> {
        match self.life {
            Life::Waiting(at) => Some(at),
            _ => None,
        }
    }

    /// Whether it is to start again at `now`.
    pub(super) fn due(&self, now: Instant) -> bool {
        matches!(self.life, Life::Waiting(at) if now >= at)
    }

    /// Joins the runner if it ended: one that failed is to start again after a pause, unless
    /// its folder was removed or the account is `ending`; one waiting for that goes when the
    /// account ends. Why the account's DB can't be used, if that is why it failed (not while
    /// `ending`).
    pub(super) fn ended(&mut self, ending: bool) -> Option<Unusable> {
        let ending_now = self.done.load(Ordering::SeqCst);
        match &self.life {
            Life::Syncing(t) | Life::Stopping(t) if !t.is_finished() && !ending_now => {
                return None;
            }
            Life::Waiting(_) if !ending => return None,
            _ => {}
        }
        let failed = match std::mem::replace(&mut self.life, Life::Gone) {
            Life::Syncing(t) => t.join().unwrap_or(Err(RunnerError::Panicked)).err(),
            Life::Stopping(t) => {
                let _ = t.join();
                None
            }
            Life::Waiting(_) | Life::Gone => None,
        };
        let failed = failed.filter(|_| !ending)?;
        if self.started.elapsed() >= RAN_WELL {
            self.backoff = Backoff::new();
        }
        let pause = self.backoff.next();
        tracing::info!("syncing {} again in {pause:?}", self.root.folder.display());
        self.life = Life::Waiting(Instant::now() + pause);
        failed.unusable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::Drive;
    use hbsync_engine::root::SyncRoot;
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    use crate::protocol::State;
    use crate::runner::{Runner, RunnerConfig};

    /// A runner that fails is to start again after a pause, unless its folder was removed or
    /// the account is ending.
    #[test]
    fn a_failed_runner_starts_again_unless_its_folder_went()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(&folder)?;
        // Set up for another identity: its runner fails at once.
        SyncRoot::create(&home.path().join("other.db"), "other.example", &folder)?;
        let db = home.path().join("state.db");
        let drive = Drive::open(
            &db,
            "a.example",
            Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default()))),
        )?;
        let failed = |removed: bool, ending: bool| -> Result<Running, Box<dyn std::error::Error>> {
            let cfg = RunnerConfig::new(&folder, &db, &"a.example".into(), "pc");
            let (handle, thread) = Runner::spawn(cfg, Arc::clone(&drive))?;
            let mut r = Running {
                root: Root::main(&"a.example".into(), &folder),
                sharer: None,
                handle,
                life: Life::Syncing(thread),
                done: Arc::default(),
                started: Instant::now(),
                backoff: Backoff::new(),
                shared: false,
            };
            if removed {
                r.stop_for_good();
            }
            while matches!(r.life, Life::Syncing(_) | Life::Stopping(_)) {
                std::thread::sleep(Duration::from_millis(10));
                r.ended(ending);
            }
            Ok(r)
        };
        let waiting = |r: &Running| matches!(r.life, Life::Waiting(_));
        let r = failed(false, false)?;
        assert!(waiting(&r));
        let status = r.handle.status();
        assert_eq!(status.state, State::Failing);
        assert!(status.error.is_some());
        assert!(!waiting(&failed(true, false)?), "removed");
        assert!(!waiting(&failed(false, true)?), "the account ends");
        Ok(())
    }
}
