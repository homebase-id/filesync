//! The account's folders' runners, on the account's thread: one that failed starts again
//! after a pause, backing off; one whose folder was removed stops for good. And whose
//! shares are synced ([`Sharing`]), as the identity's share markers say ([`Markers`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_state::Unusable;

use std::path::Path;

use super::Msg;
use super::parts::{Share, Sharers, Synced};
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
    /// The main folder's: it makes a folder of shares where none is on record (started so,
    /// the account having a share to sync then, or told so since: `Command::HoldShares`).
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
/// each share they turn on opened (tried again while it can't be). The reading and the
/// opening ask the identity, and another's: done by a thread of their own
/// ([`Sharers`]' calls), whose answers come to the account as messages
/// ([`Msg::Sharers`]): its thread never waits for them.
pub(super) struct Markers {
    /// What the worker is asked, and its thread; `None`: nothing says whose shares are
    /// synced (or the thread ended: [`Markers::ask`]).
    worker: Option<(Sender<Job>, JoinHandle<()>)>,
    /// Told apart from another account run's, whose answers may still come.
    id: u64,
    /// When to read them (again): as one changed or may have, after a read that failed.
    read_at: Option<Instant>,
    /// A read is under way; `again`: and something changed since it was asked.
    reading: bool,
    again: bool,
    read_backoff: Backoff,
    /// Shares to open: turned on, and not opened yet.
    opening: Vec<Opening>,
    /// Shares turned on that can't be synced here (no folder can have the name, or another
    /// share has it): said, and not opened, while their markers say so.
    left_out: Vec<Identity>,
    /// Shares not synced any more whose copy couldn't be taken off this device (all of it):
    /// not tried again in this run, unless their markers turn them on.
    unended: Vec<Identity>,
}

struct Opening {
    sharer: Identity,
    /// The name of its folder here.
    local: String,
    /// When to try (again); `None`: being tried.
    at: Option<Instant>,
    backoff: Backoff,
    /// Why it can't be opened was told.
    told: bool,
    /// Opened to take its copy off this device, not to sync it ([`Synced::kept`]).
    end: bool,
}

enum Job {
    Read(Vec<Identity>),
    /// A share to open: to sync, or (`true`) to take its copy off this device.
    Open(Identity, bool),
}

/// What the worker found ([`Markers::answer`]).
pub(crate) enum Answer {
    Told(Result<Synced, RunnerError>),
    Opened(Identity, Result<Share, RunnerError>),
}

impl Markers {
    /// `source`'s markers, followed; its answers sent by `tell`. First read at `first_read`,
    /// or sooner where the account says they may have changed ([`Markers::changed`]).
    pub(super) fn new(
        source: Option<Box<dyn Sharers>>,
        tell: Sender<Msg>,
        first_read: Instant,
    ) -> Markers {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let worker = source.and_then(|source| {
            let (jobs, asked) = channel::<Job>();
            let work = move || {
                for job in asked {
                    let answer = match job {
                        Job::Read(synced) => Answer::Told(source.told(&synced)),
                        Job::Open(sharer, to_end) => {
                            let opened = source.open(&sharer, to_end);
                            Answer::Opened(sharer, opened)
                        }
                    };
                    if tell.send(Msg::Sharers(id, Box::new(answer))).is_err() {
                        break;
                    }
                }
            };
            let spawned = std::thread::Builder::new()
                .name("hbsync-sharers".into())
                .spawn(work);
            match spawned {
                Ok(thread) => Some((jobs, thread)),
                Err(e) => {
                    tracing::error!("no thread to read the share markers on ({e}): none synced");
                    None
                }
            }
        });
        Markers {
            read_at: worker.as_ref().map(|_| first_read),
            worker,
            id,
            reading: false,
            again: false,
            read_backoff: Backoff::new(),
            opening: Vec::new(),
            left_out: Vec::new(),
            unended: Vec::new(),
        }
    }

    /// The copy of what `sharer` shared was taken off this device, or (`!all`) not all of
    /// it: not tried again in this run then. The markers are read again either way: turned
    /// on meanwhile, the share is opened anew.
    pub(super) fn ended(&mut self, sharer: Identity, all: bool, now: Instant) {
        if !all {
            self.unended.push(sharer);
        }
        self.changed(now);
    }

    /// Whether anything says whose shares are synced.
    pub(super) fn any(&self) -> bool {
        self.worker.is_some()
    }

    /// A marker changed, or may have (the drive is read for what wasn't announced, as its
    /// notifications connect): read; and what waits to be opened is tried now.
    pub(super) fn changed(&mut self, now: Instant) {
        match self.reading {
            true => self.again = true,
            false if self.worker.is_some() => self.read_at = Some(now),
            false => {}
        }
        self.hurry(now);
    }

    /// A share waiting to be tried again is tried now: the user asked to sync, the machine
    /// woke, or the identity can be reached again (what kept it may be mended).
    pub(super) fn hurry(&mut self, now: Instant) {
        for opening in &mut self.opening {
            opening.at = opening.at.map(|at| at.min(now));
        }
    }

    /// When to [`Markers::ask`] at the latest: a read, or a share to try opening again.
    /// Never while `paused` (nothing is asked then), nor while an answer is awaited (its
    /// coming has the account ask for the next).
    pub(super) fn deadline(&self, paused: bool) -> Option<Instant> {
        let opening = self.opening.iter().filter_map(|o| o.at);
        let due = self.read_at.into_iter().chain(opening).min();
        let asked = self.reading || self.opening.iter().any(|o| o.at.is_none());
        due.filter(|_| !paused && !asked && self.worker.is_some())
    }

    /// Asks the worker for what is due, one thing at a time (the next once its answer came:
    /// nothing waits in its queue that a pause, or the account's end, should hold back): a
    /// read of the markers (told whose shares are synced or being opened: `shares`, and those
    /// to open), else a share to open. A worker whose thread ended (the source panicked) is
    /// said, once, and nothing is asked any more: what is synced stays as it is, and
    /// nothing is due.
    pub(super) fn ask(&mut self, shares: &[Sharing], now: Instant) {
        let Some((jobs, thread)) = &self.worker else {
            return;
        };
        let mut sent = !thread.is_finished();
        let asked = self.reading || self.opening.iter().any(|o| o.at.is_none());
        if sent && !asked && self.read_at.is_some_and(|at| now >= at) {
            let synced = shares.iter().map(|s| &s.share.sharer);
            let synced = synced.chain(self.opening.iter().map(|o| &o.sharer));
            sent = jobs.send(Job::Read(synced.cloned().collect())).is_ok();
            (self.reading, self.read_at, self.again) = (sent, None, false);
        } else if sent && !asked {
            let due = |o: &&mut Opening| o.at.is_some_and(|at| now >= at);
            if let Some(opening) = self.opening.iter_mut().find(due) {
                sent = jobs
                    .send(Job::Open(opening.sharer.clone(), opening.end))
                    .is_ok();
                opening.at = None;
            }
        }
        if !sent {
            tracing::error!(
                "the thread that reads the share markers ended: whose shares are synced stays \
                 as it is until the account starts again"
            );
            (self.worker, self.read_at, self.reading) = (None, None, false);
            self.opening.clear();
        }
    }

    /// Takes the worker's `answer` (of the run `id`), and has `shares` follow it: one the
    /// markers turn off is marked ([`Sharing::off`]: its runner stops, and it goes), one
    /// they are unsure of stays, one they turn on is to be opened, and added once it is.
    /// While they can't be read, all stays as it is. `ending`: whose copies are being taken
    /// off this device now, none of them opened meanwhile. What to tell on the account's
    /// status.
    pub(super) fn answer(
        &mut self,
        id: u64,
        answer: Answer,
        shares: &mut Vec<Sharing>,
        ending: &[Identity],
        now: Instant,
    ) -> Vec<String> {
        // (Another run's: its shares aren't this one's to sync. Or one that comes after its
        // thread was found ended: all stays as it is from then on, as was said.)
        if id != self.id || self.worker.is_none() {
            return Vec::new();
        }
        match answer {
            Answer::Told(Ok(told)) => {
                (self.reading, self.read_backoff) = (false, Backoff::new());
                self.read_at = self.again.then_some(now);
                self.told(told, shares, ending, now);
            }
            Answer::Told(Err(e)) => {
                let pause = wait_after(&mut self.read_backoff, &e);
                tracing::warn!(
                    "the share markers can't be read ({e}): what is synced stays as it is; \
                     read again in {pause:?}"
                );
                (self.reading, self.read_at) = (false, Some(now + pause));
            }
            Answer::Opened(sharer, opened) => {
                // (Turned off since it was asked for: not synced.)
                let Some(i) = self.opening.iter().position(|o| o.sharer == sharer) else {
                    return Vec::new();
                };
                match opened {
                    Ok(share) => {
                        let opening = self.opening.swap_remove(i);
                        if let Some(mut new) = Sharing::at(share, opening.local, shares) {
                            new.off = opening.end;
                            shares.push(new);
                        }
                    }
                    Err(e) => {
                        let opening = &mut self.opening[i];
                        let pause = wait_after(&mut opening.backoff, &e);
                        tracing::warn!(
                            "what {sharer} shares can't be opened ({e}): trying again in {pause:?}"
                        );
                        opening.at = Some(now + pause);
                        if !std::mem::replace(&mut opening.told, true) {
                            return vec![crate::words::share_unopened(&sharer, &e)];
                        }
                    }
                }
            }
        }
        Vec::new()
    }

    /// What the markers say, `told`, followed by `shares` and by what is to be opened: to
    /// sync, or (one with a state DB here that isn't synced any more) to take its copy off
    /// this device. None of `ending` meanwhile.
    fn told(&mut self, told: Synced, shares: &mut [Sharing], ending: &[Identity], now: Instant) {
        // (One they are unsure of stays as it is: synced, being opened, or ending.)
        let unsure = |sharer: &Identity| told.unsure.contains(sharer);
        let on = |sharer: &Identity| told.on.contains(sharer);
        for share in shares.iter_mut() {
            let sharer = &share.share.sharer;
            let off = match unsure(sharer) {
                true => share.off,
                false => !on(sharer),
            };
            if off != share.off {
                let now_is = if off {
                    "no longer synced"
                } else {
                    "synced again"
                };
                tracing::info!("what {sharer} shares is {now_is}, by its marker");
                share.off = off;
            }
        }
        self.opening.retain(|o| match o.end {
            true => !on(&o.sharer) && told.kept.contains(&o.sharer),
            false => on(&o.sharer) || unsure(&o.sharer),
        });
        self.left_out.retain(on);
        self.unended.retain(|sharer| !on(sharer));
        // Those to sync, then those to end: with a state DB here, and neither synced nor
        // unsure (one of those stays as it is).
        let to_end = (told.kept.iter()).filter(|sharer| !on(sharer) && !unsure(sharer));
        let wanted = told.on.iter().map(|sharer| (sharer, false));
        for (sharer, end) in wanted.chain(to_end.map(|sharer| (sharer, true))) {
            let known = shares.iter().any(|s| s.share.sharer == *sharer)
                || self.opening.iter().any(|o| o.sharer == *sharer)
                || self.left_out.contains(sharer)
                || ending.contains(sharer)
                || (end && self.unended.contains(sharer));
            if known {
                continue;
            }
            // Before it is opened: one that can't be synced here is never asked for.
            let names = shares.iter().map(|s| &s.local);
            let names = names.chain(self.opening.iter().map(|o| &o.local));
            match Sharing::local(sharer).filter(|local| free(local, sharer, names)) {
                Some(local) => self.opening.push(Opening {
                    sharer: sharer.clone(),
                    local,
                    at: Some(now),
                    backoff: Backoff::new(),
                    told: false,
                    end,
                }),
                None => self.left_out.push(sharer.clone()),
            }
        }
    }
}

/// The wait before what failed with `e` is tried again: an hour where the identity refused
/// the login (the user's to mend, as for the account's own connecting); else by its class,
/// and not sooner than the server asked.
fn wait_after(backoff: &mut Backoff, e: &RunnerError) -> Duration {
    let asked = match e {
        RunnerError::Engine(e) => e.retry_after(),
        _ => None,
    };
    match e.class() {
        hbsync_remote::ErrorClass::Denied => super::run::NEEDS_USER_AGAIN,
        class => backoff.after(class, asked),
    }
}

/// Whether `sharer`'s folder can be named `local`: none of `names` is that name, as names
/// compare here. Two runners on one folder would each take the other's files for changes
/// made here. Said where not.
fn free<'a>(local: &str, sharer: &Identity, mut names: impl Iterator<Item = &'a String>) -> bool {
    let key = hbsync_platform::name_key(local);
    let free = !names.any(|name| hbsync_platform::name_key(name) == key);
    if !free {
        tracing::error!("what {sharer} shares isn't synced: its folder {local} is another share's");
    }
    free
}

impl Sharing {
    /// The name of `sharer`'s folder here; `None` where no folder here can have its name
    /// (said).
    fn local(sharer: &Identity) -> Option<String> {
        let os = hbsync_platform::name::Os::current();
        match hbsync_namemap::to_local(sharer.as_str(), os) {
            Ok(local) => Some(local.name),
            Err(e) => {
                tracing::error!(
                    "what {sharer} shares isn't synced: no folder here can have its name ({e:?})"
                );
                None
            }
        }
    }

    /// `share` to run in a folder named `local`, beside `others`; `None` where one of them
    /// is its sharer's, or has that name (said: [`free`]).
    fn at(share: Share, local: String, others: &[Sharing]) -> Option<Sharing> {
        let taken = others
            .iter()
            .any(|other| other.share.sharer == share.sharer);
        if taken {
            let sharer = &share.sharer;
            tracing::error!("what {sharer} shares is synced already: not a second time");
        }
        let names = others.iter().map(|other| &other.local);
        (!taken && free(&local, &share.sharer, names)).then(|| Sharing {
            share,
            local,
            retry: None,
            backoff: Backoff::new(),
            off: false,
        })
    }

    /// `shares` to run, each sharer once, each under a folder's name of its own: the others
    /// are left out ([`Sharing::at`]).
    pub(super) fn each(shares: Vec<Share>) -> Vec<Sharing> {
        let mut each: Vec<Sharing> = Vec::new();
        for share in shares {
            let local = Sharing::local(&share.sharer);
            if let Some(new) = local.and_then(|local| Sharing::at(share, local, &each)) {
                each.push(new);
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

    /// A source whose reads wait to be let through, counted; no share of its opens.
    #[derive(Debug)]
    struct Held {
        reads: Arc<std::sync::atomic::AtomicUsize>,
        through: std::sync::Mutex<std::sync::mpsc::Receiver<Synced>>,
    }

    impl Sharers for Held {
        fn told(&self, _: &[Identity]) -> Result<Synced, RunnerError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let through = self.through.lock().map_err(|_| RunnerError::Panicked)?;
            through.recv().map_err(|_| RunnerError::Panicked)
        }

        fn open(&self, _: &Identity, _: bool) -> Result<Share, RunnerError> {
            Err(std::io::Error::other("not here").into())
        }
    }

    /// The markers are read off the account's thread, one read at a time: a change told
    /// while one is under way has them read once more after it; the answer of another run
    /// is passed over; a share that can't be opened is said once and tried again.
    #[test]
    fn markers_are_read_one_at_a_time_and_again_for_a_change_meanwhile()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reads, tom): (Arc<std::sync::atomic::AtomicUsize>, Identity) =
            (Arc::default(), "tom.example".into());
        let (let_through, through) = channel();
        let source = Held {
            reads: Arc::clone(&reads),
            through: std::sync::Mutex::new(through),
        };
        let (tell, told) = channel();
        let start = Instant::now();
        let mut markers = Markers::new(Some(Box::new(source)), tell, start);
        let mut shares = Vec::new();
        let wait = Duration::from_secs(10);
        let answer = |markers: &mut Markers, shares: &mut Vec<Sharing>| {
            let Msg::Sharers(id, answer) = told.recv_timeout(wait)? else {
                return Err("another message".into());
            };
            Ok::<_, Box<dyn std::error::Error>>(markers.answer(id, *answer, shares, &[], start))
        };

        // One read, however often it is asked for meanwhile.
        assert_eq!(markers.deadline(false), Some(start));
        assert_eq!(
            markers.deadline(true),
            None,
            "nothing is asked while paused"
        );
        markers.ask(&shares, start);
        markers.changed(start);
        markers.ask(&shares, start);
        assert_eq!(markers.deadline(false), None, "a read is under way");
        // Another run's answer: passed over.
        let other = Answer::Told(Ok(Synced::default()));
        assert!(
            markers
                .answer(markers.id + 1, other, &mut shares, &[], start)
                .is_empty()
        );
        assert!(markers.reading);

        // Its answer turns a share on; and it is read once more, for the change meanwhile.
        let on = Synced {
            on: vec![tom.clone()],
            unsure: Vec::new(),
            kept: Vec::new(),
        };
        let_through.send(on.clone())?;
        assert!(answer(&mut markers, &mut shares)?.is_empty());
        assert_eq!(markers.opening.len(), 1);
        // One thing asked at a time: the second read, then the share's opening.
        markers.ask(&shares, start);
        assert!(markers.reading && markers.opening[0].at.is_some());
        let_through.send(on)?;
        assert!(answer(&mut markers, &mut shares)?.is_empty());
        markers.ask(&shares, start);
        assert_eq!(markers.deadline(false), None, "its answer is awaited");
        let said = answer(&mut markers, &mut shares)?;
        assert_eq!(said, [crate::words::share_unopened(&tom, &"not here")]);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(!markers.reading && markers.read_at.is_none());

        // The share is tried again after a pause, or at once where asked to hurry, and not
        // said again.
        let again = markers.deadline(false).ok_or("no retry")?;
        assert!(again > start);
        markers.hurry(start);
        assert_eq!(markers.deadline(false), Some(start));
        markers.ask(&shares, start);
        assert!(answer(&mut markers, &mut shares)?.is_empty());
        assert!(shares.is_empty() && markers.opening.len() == 1);
        Ok(())
    }

    /// A source that panics as it is read.
    #[derive(Debug)]
    struct Broken;

    impl Sharers for Broken {
        fn told(&self, _: &[Identity]) -> Result<Synced, RunnerError> {
            panic!("as a source with a bug would (expected in this test)")
        }

        fn open(&self, _: &Identity, _: bool) -> Result<Share, RunnerError> {
            Err(RunnerError::Panicked)
        }
    }

    /// The thread that reads the markers ended (its source panicked): nothing is asked any
    /// more and nothing is due, so the account's loop doesn't turn for it.
    #[test]
    fn a_reader_that_ended_asks_nothing_more() {
        let (tell, told) = channel();
        let start = Instant::now();
        let mut markers = Markers::new(Some(Box::new(Broken)), tell, start);
        markers.ask(&[], start);
        assert!(markers.reading);
        // (Its end: the sender it held is gone.)
        assert!(told.recv_timeout(Duration::from_secs(10)).is_err());
        let ended = |markers: &Markers| {
            let thread = markers.worker.as_ref().map(|(_, thread)| thread);
            thread.is_none_or(|thread| thread.is_finished())
        };
        let until = Instant::now() + Duration::from_secs(10);
        while !ended(&markers) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        markers.changed(start);
        markers.ask(&[], start);
        assert!(!markers.any() && !markers.reading);
        assert_eq!(markers.deadline(false), None);
        markers.changed(start);
        assert_eq!(markers.deadline(false), None);
        // An answer it had sent before, taken only now: passed over, nothing due by it.
        let on = Synced {
            on: vec!["tom.example".into()],
            unsure: Vec::new(),
            kept: Vec::new(),
        };
        let mut shares = Vec::new();
        for late in [Ok(on), Err(RunnerError::Panicked)] {
            let said = markers.answer(markers.id, Answer::Told(late), &mut shares, &[], start);
            assert!(said.is_empty() && markers.opening.is_empty());
            assert_eq!(markers.deadline(false), None);
        }
    }

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
