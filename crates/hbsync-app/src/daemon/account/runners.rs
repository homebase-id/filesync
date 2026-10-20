//! The account's folders' runners, on the account's thread: one that failed starts again
//! after a pause, backing off; one whose folder was removed stops for good.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::Root;
use crate::protocol::State;
use crate::runner::{Backoff, Command, Handle, RunnerError};

/// A folder's runner, on the account's thread.
pub(super) struct Running {
    /// Its folder, as the config lists it.
    pub(super) root: Root,
    pub(super) handle: Handle,
    pub(super) life: Life,
    /// Its thread is ending: joining it waits for no more than that.
    pub(super) done: Arc<AtomicBool>,
    pub(super) started: Instant,
    pub(super) backoff: Backoff,
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
    /// account ends.
    pub(super) fn ended(&mut self, ending: bool) {
        let ending_now = self.done.load(Ordering::SeqCst);
        match &self.life {
            Life::Syncing(t) | Life::Stopping(t) if !t.is_finished() && !ending_now => return,
            Life::Waiting(_) if !ending => return,
            _ => {}
        }
        let failed = match std::mem::replace(&mut self.life, Life::Gone) {
            Life::Syncing(t) => t.join().unwrap_or(Err(RunnerError::Panicked)).is_err(),
            Life::Stopping(t) => {
                let _ = t.join();
                false
            }
            Life::Waiting(_) | Life::Gone => false,
        };
        if !failed || ending {
            return;
        }
        if self.started.elapsed() >= RAN_WELL {
            self.backoff = Backoff::new();
        }
        let pause = self.backoff.after(State::Failing);
        tracing::info!("syncing {} again in {pause:?}", self.root.folder.display());
        self.life = Life::Waiting(Instant::now() + pause);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::Drive;
    use hbsync_engine::root::SyncRoot;
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

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
                handle,
                life: Life::Syncing(thread),
                done: Arc::default(),
                started: Instant::now(),
                backoff: Backoff::new(),
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
