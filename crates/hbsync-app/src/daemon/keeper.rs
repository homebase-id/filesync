//! Recently deleted's work, on a thread of its own: the daily purge, and restoring and
//! emptying as asked. The account's loop only asks: it never waits for the state DB or the
//! drive meanwhile (a purge waits on the DB while the folders' first syncs hold it, and deletes
//! many files), so the notifications, folders added or removed and pauses are taken at once.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hbsync_engine::deleted::Listed;
use hbsync_engine::{Drive, EngineError};

use crate::protocol::AccountControl;
use crate::runner::lock;

/// What the keeper is asked to do.
enum Job {
    /// The purge, if a day has passed since the last one: what was kept this many days goes.
    Purge(u32),
    Control(AccountControl),
}

/// Does Recently deleted's work one job at a time, in the order asked. A purge asked while one
/// is still to run is that one. A thread it can't start leaves the work to whoever asks, as
/// before it had one.
pub(crate) struct Keeper {
    drive: Arc<Drive>,
    jobs: Mutex<Option<Sender<Job>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// A purge is asked for and hasn't ended yet.
    purging: Arc<AtomicBool>,
    /// The account ends: a purge not started yet doesn't start.
    ending: Arc<AtomicBool>,
    /// Jobs done, for tests.
    #[cfg_attr(not(test), expect(dead_code, reason = "read by tests"))]
    done: Arc<AtomicUsize>,
}

impl Keeper {
    pub fn spawn(drive: Arc<Drive>) -> Keeper {
        let (tx, rx) = channel::<Job>();
        let purging = Arc::new(AtomicBool::new(false));
        let ending = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicUsize::new(0));
        let (mine, purge, end, count) = (
            Arc::clone(&drive),
            Arc::clone(&purging),
            Arc::clone(&ending),
            Arc::clone(&done),
        );
        let thread = std::thread::Builder::new()
            .name("hbsync-deleted".into())
            .spawn(move || {
                for job in rx {
                    match job {
                        Job::Purge(keep_days) => {
                            if !end.load(Ordering::SeqCst) {
                                purge_now(&mine, keep_days);
                            }
                            purge.store(false, Ordering::SeqCst);
                        }
                        Job::Control(control) => run_request(&mine, control),
                    }
                    count.fetch_add(1, Ordering::SeqCst);
                }
            });
        let (jobs, thread) = match thread {
            Ok(thread) => (Some(tx), Some(thread)),
            Err(e) => {
                tracing::error!("can't start Recently deleted's thread ({e}): its work waits");
                (None, None)
            }
        };
        Keeper {
            drive,
            jobs: Mutex::new(jobs),
            thread: Mutex::new(thread),
            purging,
            ending,
            done,
        }
    }

    /// The purge, once a day: what Recently deleted has kept `keep_days` goes.
    pub fn purge(&self, keep_days: u32) {
        if !self.purging.swap(true, Ordering::SeqCst) {
            self.send(Job::Purge(keep_days));
        }
    }

    /// Restores or empties as `control` asks.
    pub fn request(&self, control: AccountControl) {
        self.send(Job::Control(control));
    }

    fn send(&self, job: Job) {
        let left = match &*lock(&self.jobs) {
            Some(jobs) => jobs.send(job).err().map(|e| e.0),
            None => Some(job),
        };
        // No thread: done here, as before there was one.
        match left {
            Some(Job::Purge(keep_days)) => {
                purge_now(&self.drive, keep_days);
                self.purging.store(false, Ordering::SeqCst);
            }
            Some(Job::Control(control)) => run_request(&self.drive, control),
            None => {}
        }
    }

    /// The account ends: what was asked of it is done (but a purge not started yet), then its
    /// thread ends.
    pub fn finish(&self) {
        self.ending.store(true, Ordering::SeqCst);
        lock(&self.jobs).take();
        if let Some(thread) = lock(&self.thread).take() {
            let _ = thread.join();
        }
    }

    /// Jobs done so far.
    #[cfg(test)]
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
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
        AccountControl::EmptyDeleted { nodes } => report_emptied(drive.empty(&nodes)),
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
    /// a request asked meanwhile runs once the purge is done. Asked again while it waits, it is
    /// the same purge.
    #[test]
    fn asking_never_waits_for_a_purge() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let keeper = Keeper::spawn(Arc::clone(&drive));
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
        let keeper = Keeper::spawn(drive);
        let server = remote.server();
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        keeper.purge(30);
        keeper.ending.store(true, Ordering::SeqCst);
        drop(server);
        keeper.finish();
        assert_eq!(keeper.done(), 2);
        let mut account = StateDb::open_account(&db)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_none());
        Ok(())
    }
}
