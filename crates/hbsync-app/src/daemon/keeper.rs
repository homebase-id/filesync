//! Recently deleted's work, on a thread of its own: the daily purge, and restoring and
//! emptying as asked. The account's loop only asks: it never waits for the state DB or the
//! drive meanwhile (a purge waits on the DB while the folders' first syncs hold it, and deletes
//! many files), so the notifications, folders added or removed and pauses are taken at once.
//! What a purge found not there once is asked again a minute on ([`Drive::ask_again`]); a
//! restore that found something not there once is run once more a minute on.
//!
//! Deleting for good (emptying, the purge) goes a batch at a time, each written as it is
//! answered, and is shown on the account's status meanwhile; once the account ends, it stops
//! after the batch in flight, and what it didn't reach stays in Recently deleted.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_engine::deleted::{Going, Listed};
use hbsync_engine::exec::Stopped;
use hbsync_engine::{Drive, EngineError};

use hbsync_types::NodeId;

use crate::protocol::{AccountControl, Emptying};

/// What the keeper is asked to do.
enum Job {
    /// The purge, if a day has passed since the last one ([`Drive::purge_when_due`]).
    Purge,
    Control(AccountControl),
}

/// Tells the user a note (the account's: [`Keeper::spawn`]).
pub(super) type Tell = Arc<dyn Fn(String) + Send + Sync>;

/// Shows how far deleting for good has got on the account's status, `None` once it ended
/// ([`Keeper::spawn`]).
pub(super) type Show = Arc<dyn Fn(Option<Emptying>) + Send + Sync>;

/// Does Recently deleted's work one job at a time, in the order asked, never on the asker's
/// thread: a thread that ended (or couldn't start) is started again, as the puller's is.
pub(crate) struct Keeper {
    drive: Arc<Drive>,
    /// How a request that failed is told.
    tell: Tell,
    /// How far deleting for good has got is shown with it.
    show: Show,
    worker: Option<(Sender<Job>, JoinHandle<()>)>,
    /// A purge is asked for and hasn't started: asked again meanwhile, it is that one.
    purging: Arc<AtomicBool>,
    /// The account is paused, or ends: a purge not started yet doesn't start (it is asked
    /// again after the pause). Ending, deleting for good stops after its batch in flight, and
    /// an emptying not started yet doesn't start ([`Keeper::stop`]).
    paused: Arc<AtomicBool>,
    ending: Arc<AtomicBool>,
    /// How long after a purge what it found not there once is asked again.
    ask_after: Duration,
    /// Jobs done (and asks again), for tests.
    done: crate::TestCount,
}

impl Keeper {
    /// The keeper of `drive`'s Recently deleted; a request that fails is told with `tell`, and
    /// how far deleting for good has got is shown with `show`.
    pub(super) fn spawn(drive: Arc<Drive>, tell: Tell, show: Show) -> Keeper {
        Self::spawn_with(drive, tell, show, hbsync_engine::gone::ASK_AGAIN)
    }

    /// [`Keeper::spawn`], asking again `ask_after` after a purge.
    fn spawn_with(drive: Arc<Drive>, tell: Tell, show: Show, ask_after: Duration) -> Keeper {
        let mut keeper = Keeper {
            drive,
            tell,
            show,
            worker: None,
            purging: Arc::default(),
            paused: Arc::default(),
            ending: Arc::default(),
            ask_after,
            done: crate::TestCount::default(),
        };
        keeper.start();
        keeper
    }

    fn start(&mut self) {
        let (tx, rx) = channel::<Job>();
        let drive = Arc::clone(&self.drive);
        let tell = Arc::clone(&self.tell);
        let deleting = Deleting {
            ending: Arc::clone(&self.ending),
            show: Arc::clone(&self.show),
        };
        let (purging, paused, done, ask_after) = (
            Arc::clone(&self.purging),
            Arc::clone(&self.paused),
            self.done.clone(),
            self.ask_after,
        );
        let thread = std::thread::Builder::new()
            .name("hbsync-deleted".into())
            .spawn(move || {
                // When to ask again what the last purge found not there once (a delete for
                // good this device missed): once, a minute on, not paused.
                let mut again: Option<Instant> = None;
                // What restores found not there once: restored once more a minute on, each
                // with its name (while the daemon runs).
                let mut restore_again: Option<(Instant, Vec<(NodeId, String)>)> = None;
                let held = || paused.load(Ordering::SeqCst) || deleting.ending();
                loop {
                    let next = match (again, restore_again.as_ref().map(|(at, _)| *at)) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    let got = match next {
                        Some(at) => rx.recv_timeout(at.saturating_duration_since(Instant::now())),
                        None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                    };
                    let job = match got {
                        Ok(job) => Some(job),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    match job {
                        Some(Job::Purge) => {
                            purging.store(false, Ordering::SeqCst);
                            if !held()
                                && purge_now(&drive, &deleting)
                                && drive.doubted().unwrap_or(true)
                            {
                                again = Some(Instant::now() + ask_after);
                            }
                        }
                        Some(Job::Control(control)) => {
                            let rest = run_request(&drive, control, &*tell, &deleting, None);
                            if !rest.is_empty() {
                                // Another one pending too: both, a minute after this one.
                                let mut all =
                                    restore_again.take().map(|(_, r)| r).unwrap_or_default();
                                all.extend(rest);
                                all.sort();
                                all.dedup_by_key(|(n, _)| *n);
                                restore_again = Some((Instant::now() + ask_after, all));
                            }
                        }
                        // The restore to run once more, due (paused or not: the user asked for
                        // it; refused, not: asking no more till a login).
                        None if restore_again
                            .as_ref()
                            .is_some_and(|(at, _)| *at <= Instant::now()) =>
                        {
                            if let Some((_, rest)) = restore_again.take()
                                && !deleting.ending()
                                && !drive.is_denied()
                            {
                                let nodes = rest.iter().map(|(n, _)| *n).collect();
                                let control = AccountControl::Restore { nodes };
                                run_request(&drive, control, &*tell, &deleting, Some(&rest));
                            }
                        }
                        // Paused: again a minute on, while what it would ask can still be
                        // confirmed (within the hour); ending: not again.
                        None if held() => {
                            again = (!deleting.ending() && drive.doubted().unwrap_or(true))
                                .then(|| Instant::now() + ask_after);
                        }
                        None => {
                            again = None;
                            ask_again(&drive);
                        }
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

    /// The purge, once a day: what Recently deleted has kept 30 days goes. Asked while one
    /// waits to start, it is that one.
    pub(super) fn purge(&mut self) {
        if !self.purging.swap(true, Ordering::SeqCst) {
            self.send(Job::Purge);
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

    /// The account ends: deleting for good stops after its batch in flight (what it didn't
    /// reach stays in Recently deleted), and no purge or emptying starts.
    pub(super) fn stop(&self) {
        self.ending.store(true, Ordering::SeqCst);
    }

    /// The account ends ([`Keeper::stop`]): what else was asked of it is done, then its thread
    /// ends.
    pub(super) fn finish(self) {
        self.stop();
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

/// What deleting for good is stopped by and shown on: the account ending, its status.
struct Deleting {
    ending: Arc<AtomicBool>,
    show: Show,
}

impl Deleting {
    /// Whether the account ends: no deleting for good starts.
    fn ending(&self) -> bool {
        self.ending.load(Ordering::SeqCst)
    }

    /// Runs `run`, deleting for good (the daily purge if `purge`, else the user's emptying):
    /// stopped between batches once the account ends (asked as it ends, it doesn't start),
    /// shown while it runs, and logged as it starts and as it stops or fails part way.
    fn run<T>(
        &self,
        purge: bool,
        run: impl FnOnce(Going<'_>) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let what = if purge { PURGE } else { EMPTY };
        let far = Cell::new((0, 0));
        let stop = || self.ending();
        let told = |done: usize, total: usize| {
            if far.get().1 == 0 && total > 0 {
                tracing::info!("{what}: {total} items to delete for good, in the background");
            }
            far.set((done, total));
            if total > 0 {
                (self.show)(Some(Emptying {
                    done: done as u64,
                    total: total as u64,
                    purge,
                }));
            }
        };
        // Shown no more once it ends, however it ends (a panic too).
        let _shown = Shown(&self.show, &far);
        let result = run(Going {
            stop: &stop,
            told: &told,
        });
        let (done, total) = far.get();
        match &result {
            Err(EngineError::Stopped(Stopped::Requested)) if total == 0 => {
                tracing::info!("{what}: not started, as the account ends");
            }
            Err(EngineError::Stopped(Stopped::Requested)) => tracing::info!(
                "{what} stopped as the account ends: {done} of {total} items deleted for good, \
                 the rest stay in Recently deleted"
            ),
            Err(_) if done > 0 => {
                tracing::info!("{what}: {done} of {total} items deleted for good, then it failed");
            }
            _ => {}
        }
        result
    }
}

/// What deleting for good showed, cleared when dropped: how far it got (of none, nothing was
/// shown).
struct Shown<'a>(&'a Show, &'a Cell<(usize, usize)>);

impl Drop for Shown<'_> {
    fn drop(&mut self) {
        if self.1.get().1 > 0 {
            (self.0)(None);
        }
    }
}

/// What emptying Recently deleted is called in the log and in notes.
const EMPTY: &str = "Emptying Recently deleted";
/// What the daily purge is called in the log.
const PURGE: &str = "Purging Recently deleted";

/// The purge, if due: whether it ran (what it found not there once is to be asked again).
fn purge_now(drive: &Drive, deleting: &Deleting) -> bool {
    // Refused: asked again once the identity lets this computer in.
    if drive.is_denied() {
        return false;
    }
    match deleting.run(true, |going| drive.purge_when_due(going)) {
        Ok(Some(n)) => {
            if n > 0 {
                let days = crate::KEEP_DAYS;
                tracing::info!("{n} items deleted for good, kept {days} days or more");
            }
            true
        }
        Ok(None) => false,
        // Not recorded as done: it runs again at the account's next start.
        Err(EngineError::Stopped(Stopped::Requested)) => false,
        Err(e) => {
            tracing::warn!("{PURGE} failed: {e}");
            true
        }
    }
}

/// Asks again what the purge found not there once ([`Drive::ask_again`]).
fn ask_again(drive: &Drive) {
    if drive.is_denied() {
        return;
    }
    match drive.ask_again() {
        Ok(n) if n > 0 => tracing::info!("{n} items in Recently deleted were deleted for good"),
        Ok(_) => {}
        Err(e) => tracing::warn!("asking again about Recently deleted failed: {e}"),
    }
}

/// Runs a request for Recently deleted; the folders it concerns sync next. One that fails is
/// told to the user (`tell`): it was answered before it ran. A restore that found something
/// not there once (§8b) returns what of it is still in Recently deleted, with names, to restore
/// once more a minute on; `again`: this is that once more, of these (no more after it). What
/// a restore left in Recently deleted, or found deleted for good, is told. Emptying goes as
/// `deleting` has it: once the account ends, it doesn't start, or stops between batches.
fn run_request(
    drive: &Drive,
    control: AccountControl,
    tell: &dyn Fn(String),
    deleting: &Deleting,
    again: Option<&[(NodeId, String)]>,
) -> Vec<(NodeId, String)> {
    const RESTORE: &str = "Restoring from Recently deleted";
    let result = match control {
        AccountControl::Restore { nodes } => {
            match drive.restore_told(&nodes) {
                Ok(restored) => {
                    tracing::info!("{RESTORE}: {} items restored", restored.back);
                    // Asked once more: what went for good since (another path found it so)
                    // is named from the first time.
                    let mut went = restored.went.clone();
                    if let Some(again) = again {
                        went.extend(
                            again
                                .iter()
                                .filter(|(n, _)| restored.missing.contains(n))
                                .map(|(_, name)| name.clone()),
                        );
                    }
                    for note in restore_notes(&restored.left, &went) {
                        tell(note);
                    }
                }
                Err(EngineError::NotThereOnce { files, rest, went }) if again.is_none() => {
                    let what = match files {
                        1 => "one item isn't".to_owned(),
                        n => format!("{n} items aren't"),
                    };
                    tell(format!(
                        "{RESTORE}: Homebase says {what} there; asking again in a minute"
                    ));
                    for note in restore_notes(&[], &went) {
                        tell(note);
                    }
                    return rest;
                }
                Err(e) => tell(failed(RESTORE, &e)),
            }
            return Vec::new();
        }
        AccountControl::EmptyDeleted { nodes, db, seq } => deleting.run(false, |going| {
            drive.empty(&nodes, &Listed { db, seq }, going)
        }),
        AccountControl::EmptyListed { db, seq } => deleting.run(false, |going| {
            drive.empty_listed(&Listed { db, seq }, going)
        }),
    };
    match result {
        Ok(n) => tracing::info!("{EMPTY}: {n} items deleted for good"),
        // The account ends (logged): what is left stays in Recently deleted.
        Err(EngineError::Stopped(Stopped::Requested)) => {}
        // Refused, the drive latched it: the account asks nothing more ([`Drive::deny`]).
        Err(e) => tell(failed(EMPTY, &e)),
    }
    Vec::new()
}

/// The notes of a restore: what stayed in Recently deleted (`left`), and what was deleted for
/// good meanwhile (`went`), one note each naming the first five.
fn restore_notes(left: &[String], went: &[String]) -> Vec<String> {
    let names = |names: &[String]| {
        const SHOWN: usize = 5;
        let mut shown: Vec<String> = names
            .iter()
            .take(SHOWN)
            .map(|n| format!("\u{201c}{n}\u{201d}"))
            .collect();
        if names.len() > SHOWN {
            shown.push(format!("{} more", names.len() - SHOWN));
        }
        shown.join(", ")
    };
    let mut notes = Vec::new();
    if !left.is_empty() {
        notes.push(format!(
            "Restoring from Recently deleted: {} stayed there (a name taken where it goes, or \
             a folder that didn't come back): try again, or rename what holds the name",
            names(left)
        ));
    }
    if !went.is_empty() {
        let was = if went.len() == 1 { "was" } else { "were" };
        notes.push(format!(
            "Restoring from Recently deleted: {} {was} deleted for good meanwhile and can't \
             come back",
            names(went)
        ));
    }
    notes
}

/// The note for `what` that failed with `e`: what to do about it.
fn failed(what: &str, e: &EngineError) -> String {
    // Not a network fault: the identity said it isn't there (twice too soon).
    if let EngineError::NotThereOnce { .. } = e {
        return format!("{what} failed ({e}): try again later");
    }
    let then = match e.class() {
        hbsync_remote::ErrorClass::Transient | hbsync_remote::ErrorClass::RateLimited => {
            "Homebase can't be reached now: try again later"
        }
        hbsync_remote::ErrorClass::Denied => {
            "Homebase no longer accepts this computer: log in again"
        }
        hbsync_remote::ErrorClass::Full => {
            "the identity's storage is full: make room, then try again"
        }
        _ => "try again",
    };
    format!("{what} failed ({e}): {then}")
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
        let mut keeper = Keeper::spawn(Arc::clone(&drive), Arc::new(|_| {}), Arc::new(|_| {}));
        let purged = || -> Result<bool, Box<dyn std::error::Error>> {
            let mut account = StateDb::open_account(&db)?;
            Ok(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_some())
        };
        // The purge reads the drive first: it waits for the server meanwhile.
        let server = remote.server();
        let asked = Instant::now();
        keeper.purge();
        keeper.purge();
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
        let mut keeper = Keeper::spawn(drive, Arc::new(|_| {}), Arc::new(|_| {}));
        let done = keeper.done.clone();
        let server = remote.server();
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        keeper.purge();
        keeper.ending.store(true, Ordering::SeqCst);
        drop(server);
        keeper.finish();
        assert_eq!(done.get(), 2);
        let mut account = StateDb::open_account(&db)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_none());
        Ok(())
    }

    /// A request that fails is told to the user (it was answered before it ran), with what to
    /// do: a refused one, log in again (the drive latched it: the account asks no more).
    #[test]
    fn a_failed_request_is_told() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let told = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let notes = Arc::clone(&told);
        let tell: Tell =
            Arc::new(move |note| notes.lock().unwrap_or_else(|e| e.into_inner()).push(note));
        let mut keeper = Keeper::spawn(Arc::clone(&drive), tell, Arc::new(|_| {}));
        remote.faults().denied = true;
        keeper.request(AccountControl::EmptyListed {
            db: "x".into(),
            seq: 0,
        });
        wait_for("the request", || keeper.done() == 1)?;
        keeper.finish();
        assert!(drive.is_denied(), "the drive latched the refusal");
        let told = told.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(told.len(), 1, "{told:?}");
        assert!(
            told[0].starts_with("Emptying Recently deleted failed"),
            "{told:?}"
        );
        assert!(told[0].ends_with("log in again"), "{told:?}");
        Ok(())
    }

    /// The purge finds a file of Recently deleted not there (deleted for good elsewhere, a
    /// delete this device didn't hear of): once, a suspicion. A minute on (here less), the
    /// keeper asks again, and the row goes. Before, it waited for the next day's purge, a first
    /// again, and stayed for good (review 14 R2).
    #[test]
    fn a_purge_asks_again_a_minute_on_what_it_found_not_there() -> R {
        use hbsync_remote::Remote;
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let algorithm = hbsync_fileio::HashAlgorithm::Blake3;
        let (_, at) =
            remote
                .server()
                .seed_file(hbsync_types::NodeId::ROOT, "x.txt", b"x", algorithm)?;
        let target = |at| hbsync_types::Target {
            at,
            token: Some(hbsync_remote::fake::token_of(at.file_id)),
        };
        let (answers, _) = remote.set_removed(&[target(at)], true)?.padded(1);
        let removed = answers.into_iter().next().ok_or("an answer")??;
        drive.pull(None)?;
        {
            let mut server = remote.server();
            server.advance(40 * 86_400_000);
            server.hard_delete(at.file_id, removed.at().version)?;
        }
        let sql = rusqlite::Connection::open(&db)?;
        let count = |table: &str| -> Result<i64, rusqlite::Error> {
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        };
        assert_eq!(count("removed")?, 1);
        let ask_after = Duration::from_secs(2);
        let mut keeper = Keeper::spawn_with(
            Arc::clone(&drive),
            Arc::new(|_| {}),
            Arc::new(|_| {}),
            ask_after,
        );
        keeper.purge();
        wait_for("the purge", || keeper.done() == 1)?;
        assert_eq!(count("removed")?, 1, "not there once: kept");
        assert_eq!(count("gone_suspects")?, 1);
        // As if a minute had passed since.
        sql.execute("UPDATE gone_suspects SET at = at - 60000", [])?;
        wait_for("asked again", || keeper.done() == 2)?;
        assert_eq!(count("removed")?, 0, "gone for good");
        keeper.finish();
        Ok(())
    }

    /// What a callback was given, gathered: the notes told, what was shown.
    #[allow(clippy::type_complexity)]
    fn gathered<T: Send + 'static>() -> (Arc<dyn Fn(T) + Send + Sync>, Arc<std::sync::Mutex<Vec<T>>>)
    {
        let all = Arc::new(std::sync::Mutex::new(Vec::new()));
        let into = Arc::clone(&all);
        let gather = Arc::new(move |t| into.lock().unwrap_or_else(|e| e.into_inner()).push(t));
        (gather, all)
    }

    /// The notes told, gathered.
    fn notes() -> (Tell, Arc<std::sync::Mutex<Vec<String>>>) {
        gathered()
    }

    /// `name` seeded at the drive's top and removed, as the device's store has it: its node.
    fn removed_file(
        remote: &FakeRemote,
        drive: &Drive,
        name: &str,
    ) -> Result<(hbsync_types::NodeId, hbsync_types::RemoteRef), Box<dyn std::error::Error>> {
        use hbsync_remote::Remote;
        let algorithm = hbsync_fileio::HashAlgorithm::Blake3;
        let (node, at) =
            remote
                .server()
                .seed_file(hbsync_types::NodeId::ROOT, name, b"x", algorithm)?;
        let target = hbsync_types::Target {
            at,
            token: Some(hbsync_remote::fake::token_of(at.file_id)),
        };
        let (answers, _) = remote.set_removed(&[target], true)?.padded(1);
        let removed = answers.into_iter().next().ok_or("an answer")??;
        drive.pull(None)?;
        Ok((node, removed.at()))
    }

    /// A restore finds its item not there, the identity's first word on it (§8b): told so, not
    /// as a network fault, and run once more a minute on (here less), without the user asking
    /// again. Gone for good by then: told it can't come back. Before, the note said Homebase
    /// couldn't be reached, and nothing asked again (the scan of 2026-10-23, A3).
    #[test]
    fn a_restore_said_not_there_once_runs_once_more() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let (node, at) = removed_file(&remote, &drive, "x.txt")?;
        // Deleted for good elsewhere, unheard of: the drive says not there, no tombstone.
        remote.server().hard_delete(at.file_id, at.version)?;
        let (tell, told) = notes();
        let mut keeper = Keeper::spawn_with(
            Arc::clone(&drive),
            tell,
            Arc::new(|_| {}),
            Duration::from_secs(2),
        );
        keeper.request(AccountControl::Restore { nodes: vec![node] });
        wait_for("the restore", || keeper.done() == 1)?;
        let first = told.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(first.len(), 1, "{first:?}");
        assert!(
            first[0].contains("isn't there; asking again in a minute"),
            "{first:?}"
        );
        assert!(!first[0].contains("can't be reached"), "{first:?}");
        // As if a minute had passed since.
        let sql = rusqlite::Connection::open(&db)?;
        sql.execute("UPDATE gone_suspects SET at = at - 60000", [])?;
        wait_for("run once more", || keeper.done() == 2)?;
        keeper.finish();
        let told = told.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(told.len(), 2, "{told:?}");
        assert!(
            told[1].contains("\u{201c}x.txt\u{201d} was deleted for good meanwhile"),
            "{told:?}"
        );
        let removed: i64 = sql.query_row("SELECT count(*) FROM removed", [], |r| r.get(0))?;
        assert_eq!(removed, 0, "forgotten");
        Ok(())
    }

    /// A restore that leaves a row in Recently deleted with no error (here: every name it may
    /// take is taken at the top) says which, once: before, it only logged "0 items restored".
    #[test]
    fn a_restore_that_leaves_a_row_says_which() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let (node, _) = removed_file(&remote, &drive, "x.txt")?;
        {
            let algorithm = hbsync_fileio::HashAlgorithm::Blake3;
            let mut server = remote.server();
            for name in hbsync_engine::tree::restored_names("x.txt") {
                server.seed_file(hbsync_types::NodeId::ROOT, &name, b"y", algorithm)?;
            }
        }
        let (tell, told) = notes();
        let mut keeper = Keeper::spawn(Arc::clone(&drive), tell, Arc::new(|_| {}));
        keeper.request(AccountControl::Restore { nodes: vec![node] });
        keeper.finish();
        let told = told.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(told.len(), 1, "{told:?}");
        assert!(
            told[0]
                .starts_with("Restoring from Recently deleted: \u{201c}x.txt\u{201d} stayed there"),
            "{told:?}"
        );
        Ok(())
    }

    /// A purge asked for before a pause doesn't start during it.
    #[test]
    fn no_purge_starts_while_paused() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let mut keeper = Keeper::spawn(drive, Arc::new(|_| {}), Arc::new(|_| {}));
        let server = remote.server();
        keeper.request(AccountControl::Restore { nodes: Vec::new() });
        keeper.purge();
        keeper.paused(true);
        drop(server);
        wait_for("the request and the purge", || keeper.done() == 2)?;
        let mut account = StateDb::open_account(&db)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_none());
        keeper.paused(false);
        keeper.purge();
        wait_for("the purge", || keeper.done() == 3)?;
        assert!(account.read(|s| s.account_kv(keys::PURGED_AT))?.is_some());
        keeper.finish();
        Ok(())
    }

    /// `n` files seeded at the drive's top and removed, as the device's store has them; and
    /// a request to empty Recently deleted as listed now.
    fn emptying(
        remote: &FakeRemote,
        drive: &Drive,
        db: &std::path::Path,
        n: usize,
    ) -> Result<AccountControl, Box<dyn std::error::Error>> {
        use hbsync_remote::Remote;
        let algorithm = hbsync_fileio::HashAlgorithm::Blake3;
        let mut targets = Vec::new();
        for i in 0..n {
            let name = format!("{i}.txt");
            let (_, at) = remote
                .server()
                .seed_file(NodeId::ROOT, &name, b"x", algorithm)?;
            let token = Some(hbsync_remote::fake::token_of(at.file_id));
            targets.push(hbsync_types::Target { at, token });
        }
        for batch in targets.chunks(hbsync_remote::BATCH_FILES) {
            remote.set_removed(batch, true)?;
        }
        drive.pull(None)?;
        let listed = StateDb::open_account(db)?.read(hbsync_engine::deleted::listed_now)?;
        Ok(AccountControl::EmptyListed {
            db: listed.db,
            seq: listed.seq,
        })
    }

    /// The rows of Recently deleted in the account's DB at `db`.
    fn removed_rows(db: &std::path::Path) -> Result<i64, Box<dyn std::error::Error>> {
        let sql = rusqlite::Connection::open(db)?;
        Ok(sql.query_row("SELECT count(*) FROM removed", [], |r| r.get(0))?)
    }

    /// Emptying shows how far it got on the account's status while it runs, a batch at a
    /// time, and nothing once it ended; nothing is told (it didn't fail).
    #[test]
    fn emptying_shows_how_far_it_got() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let n = hbsync_engine::deleted::EMPTYING_BATCH + 1;
        let control = emptying(&remote, &drive, &db, n)?;
        let (tell, told) = notes();
        let (show, seen) = gathered();
        let mut keeper = Keeper::spawn(Arc::clone(&drive), tell, show);
        keeper.request(control);
        wait_for("the emptying", || keeper.done() == 1)?;
        keeper.finish();
        let at = |done: usize| {
            Some(Emptying {
                done: done as u64,
                total: n as u64,
                purge: false,
            })
        };
        let seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(seen, [at(0), at(n - 1), at(n), None]);
        assert!(told.lock().unwrap_or_else(|e| e.into_inner()).is_empty());
        assert_eq!(removed_rows(&db)?, 0);
        Ok(())
    }

    /// The account ending (`hbsync stop`, quitting the tray, SIGTERM) stops emptying after
    /// the batch in flight, its result written: what it didn't reach stays in Recently deleted
    /// for the next time, and nothing is told (it didn't fail). Before, the daemon's stop waited
    /// for the whole job.
    #[test]
    fn the_account_ending_stops_emptying_after_the_batch_in_flight() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let batch = hbsync_engine::deleted::EMPTYING_BATCH;
        let n = 2 * batch + 1;
        let control = emptying(&remote, &drive, &db, n)?;
        let (tell, told) = notes();
        // Stopped ([`Keeper::stop`]) while the first batch is answered.
        let ending: Arc<std::sync::OnceLock<Arc<AtomicBool>>> = Arc::default();
        let stopping = Arc::clone(&ending);
        let show: Show = Arc::new(move |e| {
            if e.is_some_and(|e| e.done > 0)
                && let Some(ending) = stopping.get()
            {
                ending.store(true, Ordering::SeqCst);
            }
        });
        let mut keeper = Keeper::spawn(Arc::clone(&drive), tell, show);
        ending
            .set(Arc::clone(&keeper.ending))
            .map_err(|_| "set once")?;
        keeper.request(control);
        wait_for("the stop", || keeper.done() == 1)?;
        keeper.finish();
        assert_eq!(
            removed_rows(&db)?,
            (n - batch) as i64,
            "the first batch went"
        );
        assert!(told.lock().unwrap_or_else(|e| e.into_inner()).is_empty());
        Ok(())
    }

    /// Emptying asked once the account ends doesn't start: Recently deleted keeps it all.
    #[test]
    fn emptying_asked_as_the_account_ends_doesnt_start() -> R {
        let home = tempfile::tempdir()?;
        let db = home.path().join("state.db");
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&db, "a.example", remote.clone())?;
        let control = emptying(&remote, &drive, &db, 2)?;
        let (show, seen) = gathered();
        let mut keeper = Keeper::spawn(Arc::clone(&drive), Arc::new(|_| {}), show);
        keeper.stop();
        keeper.request(control);
        keeper.finish();
        assert_eq!(removed_rows(&db)?, 2);
        assert!(seen.lock().unwrap_or_else(|e| e.into_inner()).is_empty());
        Ok(())
    }
}
