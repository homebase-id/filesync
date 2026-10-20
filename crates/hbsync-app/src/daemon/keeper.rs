//! Recently deleted's work, on a thread of its own: the daily purge, and restoring and
//! emptying as asked. The account's loop only asks: it never waits for the state DB or the
//! drive meanwhile (a purge waits on the DB while the folders' first syncs hold it, and deletes
//! many files), so the notifications, folders added or removed and pauses are taken at once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread::JoinHandle;

use hbsync_engine::deleted::Listed;
use hbsync_engine::{Drive, EngineError};

use crate::protocol::AccountControl;

/// What the keeper is asked to do.
enum Job {
    /// The purge, if a day has passed since the last one ([`Drive::purge_when_due`]): what was
    /// kept this many days goes.
    Purge(u32),
    Control(AccountControl),
}

/// Does Recently deleted's work one job at a time, in the order asked, never on the asker's
/// thread: a thread that ended (or couldn't start) is started again, as the puller's is.
pub(crate) struct Keeper {
    drive: Arc<Drive>,
    worker: Option<(Sender<Job>, JoinHandle<()>)>,
    /// A purge is asked for and hasn't started: asked again meanwhile, it is that one.
    purging: Arc<AtomicBool>,
    /// The account is paused, or ends: a purge not started yet doesn't start (it is asked
    /// again after the pause).
    paused: Arc<AtomicBool>,
    ending: Arc<AtomicBool>,
    /// Jobs done, for tests.
    done: crate::TestCount,
}

impl Keeper {
    pub(super) fn spawn(drive: Arc<Drive>) -> Keeper {
        let mut keeper = Keeper {
            drive,
            worker: None,
            purging: Arc::default(),
            paused: Arc::default(),
            ending: Arc::default(),
            done: crate::TestCount::default(),
        };
        keeper.start();
        keeper
    }

    fn start(&mut self) {
        let (tx, rx) = channel::<Job>();
        let drive = Arc::clone(&self.drive);
        let (purging, paused, ending, done) = (
            Arc::clone(&self.purging),
            Arc::clone(&self.paused),
            Arc::clone(&self.ending),
            self.done.clone(),
        );
        let thread = std::thread::Builder::new()
            .name("hbsync-deleted".into())
            .spawn(move || {
                for job in rx {
                    match job {
                        Job::Purge(keep_days) => {
                            purging.store(false, Ordering::SeqCst);
                            let held =
                                paused.load(Ordering::SeqCst) || ending.load(Ordering::SeqCst);
                            if !held {
                                purge_now(&drive, keep_days);
                            }
                        }
                        Job::Control(control) => run_request(&drive, control),
                    }
                    done.add();
                }
            });
        self.worker = match thread {
            Ok(thread) => Some((tx, thread)),
            Err(e) => {
                tracing::error!("can't start Recently deleted's thread ({e}): trying again");
                None
            }
        };
    }

    /// The purge, once a day: what Recently deleted has kept `keep_days` goes. Asked while one
    /// waits to start, it is that one.
    pub(super) fn purge(&mut self, keep_days: u32) {
        if !self.purging.swap(true, Ordering::SeqCst) {
            self.send(Job::Purge(keep_days));
        }
    }

    /// The account is paused (or resumed): no purge starts while it is.
    pub(super) fn paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
    }

    /// Restores or empties as `control` asks.
    pub(super) fn request(&mut self, control: AccountControl) {
        self.send(Job::Control(control));
    }

    /// Hands `job` to the thread, starting one again if it ended. Lost (logged) only while no
    /// thread starts: a purge is asked again later, a request by its user.
    fn send(&mut self, job: Job) {
        let job = match &self.worker {
            Some((jobs, _)) => match jobs.send(job) {
                Ok(()) => return,
                Err(e) => e.0,
            },
            None => job,
        };
        self.start();
        if let Some((jobs, _)) = &self.worker
            && jobs.send(job).is_ok()
        {
            return;
        }
        self.purging.store(false, Ordering::SeqCst);
        tracing::warn!("Recently deleted's thread isn't running: a request was dropped");
    }

    /// The account ends: what was asked of it is done (but a purge not started yet), then its
    /// thread ends.
    pub(super) fn finish(self) {
        self.ending.store(true, Ordering::SeqCst);
        if let Some((jobs, thread)) = self.worker {
            drop(jobs);
            let _ = thread.join();
        }
    }

    /// Jobs done so far.
    #[cfg(test)]
    fn done(&self) -> u64 {
        self.done.get()
    }
}

fn purge_now(drive: &Drive, keep_days: u32) {
    match drive.purge_when_due(keep_days) {
        Ok(Some(n)) if n > 0 => tracing::info!("{n} items deleted for good after {keep_days} days"),
        Ok(_) => {}
        Err(e) => tracing::warn!("purging Recently deleted failed: {e}"),
    }
}

/// Runs a request for Recently deleted; the folders it concerns sync next.
fn run_request(drive: &Drive, control: AccountControl) {
    match control {
        AccountControl::Restore { nodes } => match drive.restore(&nodes) {
            Ok(n) => tracing::info!("{n} items restored from Recently deleted"),
            Err(e) => tracing::warn!("restoring from Recently deleted failed: {e}"),
        },
        AccountControl::EmptyDeleted { nodes, db, seq } => {
            report_emptied(drive.empty(&nodes, &Listed { db, seq }))
        }
        AccountControl::EmptyListed { db, seq } => {
            report_emptied(drive.empty_listed(&Listed { db, seq }))
        }
    }
}

fn report_emptied(result: Result<usize, EngineError>) {
    match result {
        Ok(n) => tracing::info!("{n} items deleted for good"),
        Err(e) => tracing::warn!("emptying Recently deleted failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
    use hbsync_state::{StateDb, keys};

    use super::*;

    type R = Result<(), Box<dyn std::error::Error>>;

    fn wait_for(what: &str, done: impl Fn() -> bool) -> R {
        let until = Instant::now() + Duration::from_secs(20);
        while !done() {
            if Instant::now() > until {
                return Err(format!("{what} didn't happen").into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }

    /// A purge that waits (here on the drive, as the first one waits on the state DB while the
    /// folders' first syncs hold it) keeps no one who asks waiting: asking returns at once, and
    /// a request asked meanwhile runs once the purge is done. Asked again meanwhile, it is the
    /// same purge.
    #[test]
    fn asking_never_waits_for_a_purge() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let mut keeper = Keeper::spawn(Arc::clone(&drive));
        let purged = || -> Result<bool, Box<dyn std::error::Error>> {
            let mut account = StateDb::open_account(&db)?;
            Ok(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_some())
        };
        // The purge reads the drive first: it waits for the server meanwhile.
        let server = remote.server();
        let asked = Instant::now();
        keeper.purge(30);
        keeper.purge(30);
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        assert!(asked.elapsed() < Duration::from_secs(1), "asking waited");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(keeper.done(), 0, "nothing was done while the purge waited");
        assert!(!purged()?);
        drop(server);
        wait_for("the purge and the request", || keeper.done() == 2)?;
        assert!(purged()?);
        keeper.finish();
        Ok(())
    }

    /// A purge asked for once the account ends doesn't start; a request still runs.
    #[test]
    fn an_ending_account_starts_no_purge() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let mut keeper = Keeper::spawn(drive);
        let done = keeper.done.clone();
        let server = remote.server();
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        keeper.purge(30);
        keeper.ending.store(true, Ordering::SeqCst);
        drop(server);
        keeper.finish();
        assert_eq!(done.get(), 2);
        let mut account = StateDb::open_account(&db)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_none());
        Ok(())
    }

    /// A purge asked for before a pause doesn't start during it.
    #[test]
    fn no_purge_starts_while_paused() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let mut keeper = Keeper::spawn(drive);
        let server = remote.server();
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        keeper.purge(30);
        keeper.paused(true);
        drop(server);
        wait_for("the request and the purge", || keeper.done() == 2)?;
        let mut account = StateDb::open_account(&db)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_none());
        keeper.paused(false);
        keeper.purge(30);
        wait_for("the purge", || keeper.done() == 3)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_some());
        keeper.finish();
        Ok(())
    }
}
