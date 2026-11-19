//! The account's loop, on its own thread: it waits for the credentials and the identity, wires
//! the account up (the notifications, the [`Drive`], the puller, Recently deleted's keeper),
//! starts its folders' runners, and serves its channel ([`Msg`]) until it ends.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::Event;
use hbsync_homebase::{Client, HomebaseError};
use hbsync_remote::ErrorClass;
use hbsync_state::Unusable;

use super::ending::{self, Ending};
use super::parts::{AccountParts, Share};
use super::runners::{Life, Markers, Running, Sharing};
use super::{Account, Msg, Request, Try, Waiting};
use crate::daemon::DaemonError;
use crate::daemon::keeper::Keeper;
use crate::folders::{heal_account, ready_account};
use crate::protocol::{AccountStatus, Hints, Pause, State};
use crate::runner::{Backoff, Command, Runner, RunnerConfig, RunnerError, SharesTold, Showing};
use crate::sync::{OnDrop, lock, wait_for};
use crate::words;
use crate::{Config, CredentialError, CredentialStore, Identity, Location, Paths, Root};

/// How an account readies its state DB ([`Account::ready_db`]).
pub(super) enum Readying<'a> {
    /// As it starts ([`ready_account`]), its folders among these (the config's).
    Start(&'a [Root]),
    /// As it was found unusable while the account ran ([`heal_account`], its folders those it
    /// syncs): checked, or with why, set aside unchecked.
    Heal(Option<Unusable>),
}

/// While what only the user can fix keeps an account from starting, how often it looks again.
pub(super) const NEEDS_USER_AGAIN: Duration = Duration::from_secs(3600);

/// How often the purge of Recently deleted is looked at (it runs once a day).
const PURGE_LOOK: Duration = Duration::from_secs(600);
/// While the config can't be read, folders to start wait this long before it is read again.
const CONFIG_AGAIN: Duration = Duration::from_secs(1);
/// The name the folder of shares is made under (sync semantics §10), in the main folder of an
/// account other identities share with; the user may give it another.
const SHARES: &str = "Shared";

impl Account {
    /// Shows that it waits in `to`, for `why`; to connect again when `connecting`, refused for
    /// want of the private drive's key when `lacks_key` ([`Waiting`]).
    fn show(&self, to: State, why: Option<String>, connecting: bool, lacks_key: bool) {
        if let Some(waiting) = &mut lock(&self.shown).waiting {
            waiting.state = to;
            waiting.error = why;
            waiting.connecting = connecting;
            waiting.lacks_key = lacks_key;
        }
        self.changed.raise();
    }

    /// Changes its status.
    fn set_status(&self, f: impl FnOnce(&mut AccountStatus)) {
        f(&mut lock(&self.shown).status);
        self.changed.raise();
    }

    /// Whether it should stop: the daemon stops, or the account ends.
    fn stopping(&self, stop: &AtomicBool) -> bool {
        stop.load(Ordering::Relaxed) || self.ended.load(Ordering::Relaxed)
    }

    /// Waits `time` on `rx` to try again, or less if it should stop, the machine woke up or the
    /// user asked to sync now since `tried` began ([`Clicks::clicked`]): a try then goes at
    /// once. Returns whether it should stop. Requests and the like that come meanwhile are
    /// kept in `early`.
    fn wait(
        &self,
        time: Duration,
        tried: Try,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> bool {
        let until = Instant::now() + time;
        while !self.stopping(stop) && !self.clicks.clicked(tried) {
            match rx.recv_timeout(until.saturating_duration_since(Instant::now())) {
                Ok(Msg::Woke) | Err(_) => break,
                // Looked at again ([`Clicks::clicked`]).
                Ok(Msg::Wake | Msg::SyncNow) => {}
                Ok(msg) => early.push(msg),
            }
        }
        self.stopping(stop)
    }

    /// Connects and syncs its folders until it should stop, then stops them; a folder's runner
    /// that fails starts again after a pause. An error: a runner that couldn't start.
    pub(in crate::daemon) fn run(
        self: &Arc<Self>,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<Msg>,
        stop: &AtomicBool,
    ) -> Result<(), DaemonError> {
        // What came while waiting for the credentials and the identity.
        let mut msgs = Vec::new();
        // Its state DB first, before any request. The config's folders, as read when the
        // account was made: they all are in it.
        let start = Readying::Start(&config.roots);
        if self
            .ready_db(paths, start, stop, requests, &mut msgs)?
            .is_none()
        {
            return Ok(());
        }
        let Some(creds) = self.credentials(paths, stop, requests, &mut msgs)? else {
            return Ok(());
        };
        let client = Client::new(&creds)?;
        let Some(remote) = self.connect(&client, stop, requests, &mut msgs) else {
            return Ok(());
        };
        // The notifications' events reach this thread's channel through a thread of their own,
        // which ends with them.
        let (tx, events) = channel();
        let to_account = self.requests.clone();
        std::thread::Builder::new()
            .name("hbsync-hints".into())
            .spawn(move || {
                for event in events {
                    if to_account.send(Msg::Hint(event)).is_err() {
                        return;
                    }
                }
            })
            .map_err(DaemonError::Ipc)?;
        self.sync(paths, config, requests, stop, &mut msgs, |db| {
            AccountParts::open(&self.identity, Arc::clone(&remote), db, tx.clone())
        })
    }

    /// Readies its state DB as `readying` says: one that can't be used at all (from before 0.7,
    /// damaged) is set aside and its folders set up again, told once. One only the user can
    /// set right (a newer version's) is shown and waited out, asking nothing, the daemon and
    /// the other accounts going on; so is any error while healing, tried again after a pause,
    /// checked (not the daemon started again and again). Whether it set one aside; `None` when
    /// it stops first.
    pub(super) fn ready_db(
        &self,
        paths: &Paths,
        mut readying: Readying<'_>,
        stop: &AtomicBool,
        requests: &Receiver<Msg>,
        msgs: &mut Vec<Msg>,
    ) -> Result<Option<bool>, DaemonError> {
        let healing = matches!(readying, Readying::Heal(_));
        let mut backoff = Backoff::new();
        loop {
            let tried = self.clicks.begin_try();
            let readied = match &mut readying {
                Readying::Start(roots) => ready_account(&self.db, &self.identity, roots),
                // Set aside unchecked once: what is there at a later try is checked.
                Readying::Heal(found) => {
                    heal_account(paths, &self.identity, &self.folders(), found.take())
                }
            };
            let e = match readied {
                Ok(set) => {
                    // Ready: the answers may read it now.
                    self.ready.store(true, Ordering::SeqCst);
                    if let Some(set) = &set {
                        self.note(vec![words::set_aside(set.why)]);
                    }
                    return Ok(Some(set.is_some()));
                }
                Err(e) if !healing && e.class() != ErrorClass::NeedsUser => {
                    return Err(RunnerError::Folder(e).into());
                }
                Err(e) => e,
            };
            let wait_for = if healing {
                backoff.after(e.class(), None)
            } else {
                NEEDS_USER_AGAIN
            };
            tracing::error!("{e}: {} waits for it to be set right", self.identity);
            self.show(State::Failing, Some(e.to_string()), false, false);
            if self.wait(wait_for, tried, stop, requests, msgs) {
                return Ok(None);
            }
        }
    }

    /// Syncs its folders on the account's parts, which `open` makes from its DB, until it
    /// should stop. A DB found unusable as it syncs (a runner ended for it) is looked at with
    /// every folder stopped and every part closed: first checked, as at a start (a passing
    /// fault isn't worth every folder set up again: hashing their files, reading the whole
    /// drive); found so again, set aside unchecked (an index's damage, which the check doesn't
    /// see). Once set aside, not again in this run: found so again, the runners back off.
    pub(super) fn sync(
        self: &Arc<Self>,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<Msg>,
        stop: &AtomicBool,
        msgs: &mut Vec<Msg>,
        open: impl Fn(&Path) -> Result<AccountParts, RunnerError>,
    ) -> Result<(), DaemonError> {
        // Found unusable and checked; set aside.
        let (mut checked, mut healed) = (false, false);
        // Between tries at setting aside a DB of another drive.
        let mut other_drive = Backoff::new();
        // After a heal, the config as it is then: the runners start with its settings.
        let mut now: Option<Config> = None;
        loop {
            let parts = match open(&self.db) {
                // Synced with a drive made again since: set aside before anything syncs,
                // whatever was set aside before in this run (read against the new drive, what
                // it knows would be deleted here).
                // Not the run's one heal of a damaged DB: a new one takes the drive, and isn't
                // set aside for it again. One that wasn't set aside (a try that failed after
                // a check found nothing wrong with it) is opened again after a pause.
                Err(e) if e.unusable() == Some(Unusable::OtherDrive) => {
                    tracing::warn!("{}: {e}", self.identity);
                    let tried = self.clicks.begin_try();
                    match self.heal(paths, Some(Unusable::OtherDrive), stop, requests, msgs)? {
                        Some(true) => {}
                        Some(false) => {
                            let wait_for = other_drive.after(ErrorClass::LocalFault, None);
                            if self.wait(wait_for, tried, stop, requests, msgs) {
                                return Ok(());
                            }
                        }
                        None => return Ok(()),
                    }
                    now = Config::now(paths);
                    continue;
                }
                opened => opened?,
            };
            let config = now.as_ref().unwrap_or(config);
            let found = self.serve(paths, config, requests, stop, msgs, parts, !healed)?;
            let Some(why) = found.filter(|_| !self.stopping(stop)) else {
                return Ok(());
            };
            let force = checked.then_some(why);
            checked = true;
            match self.heal(paths, force, stop, requests, msgs)? {
                Some(set) => healed = set,
                None => return Ok(()),
            }
            now = Config::now(paths);
        }
    }

    /// Readies its DB, which nothing of it has open any more, as found unusable while it ran
    /// ([`Readying::Heal`]): the answers' connection is closed, and not opened again until it is
    /// ready. Whether it set one aside; `None` when it stops first.
    fn heal(
        &self,
        paths: &Paths,
        found: Option<Unusable>,
        stop: &AtomicBool,
        requests: &Receiver<Msg>,
        msgs: &mut Vec<Msg>,
    ) -> Result<Option<bool>, DaemonError> {
        self.ready.store(false, Ordering::SeqCst);
        *lock(&self.reader) = None;
        self.ready_db(paths, Readying::Heal(found), stop, requests, msgs)
    }

    /// Syncs its folders on `parts` until it should stop, or until a runner ended as its DB
    /// can't be used, if it may set it aside (`can_heal`): then its folders wait (they show
    /// starting) once every runner has stopped, and why it can't be used is returned. An error:
    /// a runner that couldn't start.
    #[allow(clippy::too_many_arguments)]
    fn serve(
        self: &Arc<Self>,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<Msg>,
        stop: &AtomicBool,
        msgs: &mut Vec<Msg>,
        parts: AccountParts,
        can_heal: bool,
    ) -> Result<Option<Unusable>, DaemonError> {
        let AccountParts {
            drive,
            notifier,
            mut notifications,
            shares,
            sharers,
        } = parts;
        // What other identities share, each synced by a runner of its own: those the
        // identity's share markers turn on.
        let mut shares = Sharing::each(shares);
        // First read as the notifications connect (they tell what changes from then on), or
        // after a moment where they don't; at once where there are none.
        let first_read = Instant::now() + notifier.as_ref().map_or(Duration::ZERO, |_| FIRST_READ);
        let mut markers = Markers::new(sharers, self.requests.clone(), first_read);
        let mut unannounced_reads = notifications.unannounced_reads();
        // Shares not synced any more, whose copies are to be taken off this device: one at a
        // time (`ending_copy`, told to stop as the account ends), the others waiting their
        // turn.
        let mut to_end: Vec<Sharing> = Vec::new();
        let mut ending_copy: Option<Ending> = None;
        let ending_stop = Arc::new(AtomicBool::new(false));
        // The main folder and its folder of shares, as its runner told last where that is.
        let mut shares_at: Option<(PathBuf, PathBuf)> = None;
        let notifier = match notifier {
            Ok(n) => {
                self.set_status(|s| s.hints = Hints::Connecting);
                Some(n)
            }
            // Not connected, for good: the drive is read on a timer (Notifications).
            Err(e) => {
                tracing::error!(
                    "can't start the notifications ({e}); reading the drive now and then"
                );
                self.set_status(|s| s.hints = Hints::Lost(format!("can't start: {e}")));
                None
            }
        };
        // The account's notes go on its status, from the thread that has them (a weak handle:
        // the drive and the keeper don't keep the account).
        let account = Arc::downgrade(self);
        let unreadable = account.clone();
        drive.tell_skipped(move |rows| {
            if let Some(a) = unreadable.upgrade() {
                a.note_unreadable(rows);
            }
        });
        let shown_on = account.clone();
        let tell = Arc::new(move |note| {
            if let Some(a) = account.upgrade() {
                a.note(vec![note]);
            }
        });
        let show = Arc::new(move |emptying| {
            if let Some(a) = shown_on.upgrade() {
                a.set_status(|s| s.emptying = emptying);
            }
        });
        let mut keeper = Keeper::spawn(Arc::clone(&drive), tell, show);
        // A runner that couldn't start: the account stops its runners and ends with it, the
        // daemon with it (restarted whole). One that ended with an error is started again after a
        // pause, its folder showing why meanwhile; the others sync on.
        let mut result: Result<(), DaemonError> = Ok(());
        {
            // Under the lock: no pause or folder gets lost between waiting and running.
            let mut shown = lock(&self.shown);
            let roots = shown.waiting.take().map(|w| w.roots).unwrap_or_default();
            let main = roots.iter().find(|r| r.is_main()).map(|r| r.folder.clone());
            for root in roots {
                let paused = shown.paused();
                let shared = (shared_by(&shares, &markers), main.as_deref());
                match self.start(&drive, config, &root, paused, can_heal, shared) {
                    Ok(r) => shown.runners.push(r),
                    Err(e) => {
                        result = Err(e.into());
                        break;
                    }
                }
            }
        }
        self.changed.raise();

        // Folders to start once no runner of theirs is left.
        let mut adding: Vec<Root> = Vec::new();
        let mut next_purge = Instant::now();
        // Its DB can't be used: it is set aside once every runner has stopped.
        let mut heal: Option<Unusable> = None;
        loop {
            // Set and ended by the daemon ([`Account::pause`]), which wakes this thread.
            let paused = lock(&self.shown).paused().is_some();
            let mut changed = false;
            for msg in msgs.drain(..) {
                match msg {
                    // Only while it connects ([`Account::try_connect`]).
                    Msg::Connected(_) => {}
                    Msg::Shares(main, at) => {
                        shares_at = at.map(|name| {
                            let dir = main.join(name);
                            (main, dir)
                        });
                        // The folders beside the main one know the folder of shares by its
                        // record, which may say something else now: read again there (with a
                        // watcher nothing else has them look).
                        let shown = lock(&self.shown);
                        for beside in shown.own().filter(|r| !r.root.is_main()) {
                            beside.handle.send(Command::Look);
                        }
                    }
                    Msg::Sharers(id, answer) => {
                        let ending = ending_copy.as_ref().map(|e| &e.sharer);
                        let now = Instant::now();
                        let waiting = (&mut to_end, ending);
                        let notes = markers.answer(id, *answer, &mut shares, waiting, now);
                        self.push_notes(notes);
                    }
                    Msg::Hint(event) => {
                        if event == Event::Markers {
                            markers.changed(Instant::now());
                        }
                        let Some(hints) = notifications.event(event, Instant::now(), paused) else {
                            continue;
                        };
                        let mut shown = lock(&self.shown);
                        // Connected again: what waits for the drive goes now.
                        if hints == Hints::Connected {
                            let _ = shown.send(&self.identity, &Command::Online);
                        }
                        shown.status.hints = hints;
                        changed = true;
                    }
                    Msg::Woke => {
                        markers.hurry(Instant::now());
                        if let Some(n) = &notifier {
                            n.reconnect();
                            notifications.reconnecting();
                        }
                        let _ = lock(&self.shown).send(&self.identity, &Command::Woke);
                    }
                    Msg::Request(Request::Deleted(control)) => keeper.request(control),
                    // While its DB is set aside, its folders wait ([`Account::add_folder`]).
                    Msg::Request(Request::AddFolder(root)) if heal.is_some() => {
                        self.add_folder(&root);
                    }
                    // Started below, once no runner of the folder is left (one removed a moment
                    // ago may still be finishing an operation).
                    Msg::Request(Request::AddFolder(root)) => {
                        let synced = lock(&self.shown)
                            .shown()
                            .any(|r| r.root.folder == root.folder);
                        if !synced && !adding.iter().any(|a| a.folder == root.folder) {
                            adding.push(root);
                        }
                    }
                    Msg::Request(Request::RemoveFolder(folder)) => {
                        if heal.is_some() {
                            self.remove_folder(&folder);
                        }
                        adding.retain(|a| a.folder != folder);
                        // One of its own: what others share isn't in the config to remove (it
                        // stops with the main folder, below).
                        let mut shown = lock(&self.shown);
                        let own = shown.runners.iter_mut().filter(|r| r.sharer.is_none());
                        for r in own.filter(|r| r.root.folder == folder) {
                            r.stop_for_good();
                        }
                        changed = true;
                    }
                    // Asked as its runners started (they sync at their start), or while its DB
                    // was looked at: nothing to do for them. A share waiting to be opened
                    // again is tried now.
                    Msg::SyncNow => markers.hurry(Instant::now()),
                    // Looked at below.
                    Msg::Wake => {}
                }
            }
            notifications.tick(Instant::now(), paused);
            // A marker changed while not connected wasn't announced either: read whenever
            // the drive is, for that.
            let reads = notifications.unannounced_reads();
            if std::mem::replace(&mut unannounced_reads, reads) != reads {
                markers.changed(Instant::now());
            }
            keeper.paused(paused);
            // Not while paused: no deletes for good, no pulls. On the keeper's thread: this one
            // never waits for it.
            if Instant::now() >= next_purge && !paused {
                next_purge = Instant::now() + PURGE_LOOK;
                keeper.purge();
            }
            // A runner that ended takes its folder out, unless it failed: then it starts again
            // after a pause. The account ends with the last. Joined under the lock: a runner's
            // thread is done ([`Running::ended`]) only once it has nothing left to do.
            let ending = self.stopping(stop) || result.is_err() || heal.is_some();
            if ending {
                adding.clear();
                // Deleting for good stops after its batches in flight, while the runners stop.
                keeper.stop();
            }
            let now = Instant::now();
            // One of the account's own folders' that is to start again.
            let own_due = |r: &Running| r.due(now) && r.sharer.is_none();
            let (due, ready) = {
                let mut shown = lock(&self.shown);
                if ending {
                    for r in &shown.runners {
                        r.handle.send(Command::Stop);
                    }
                }
                let before = shown.runners.len();
                // Every runner joined if it ended; the first that ended for the DB.
                // (The account's: a share's DB is its own, and its runner backs off.)
                let unusable = (shown.runners.iter_mut())
                    .filter_map(|r| {
                        let own = r.sharer.is_none();
                        r.ended(ending).filter(|_| own)
                    })
                    .reduce(|first, _| first);
                if let Some(why) = unusable.filter(|_| can_heal) {
                    // Its folders wait while the DB is looked at ([`Account::sync`]), as at the
                    // account's start: they show starting, and one added or removed meanwhile
                    // changes them.
                    tracing::error!("{}: its state DB can't be used: stopping", self.identity);
                    let roots = shown.own().map(|r| r.root.clone());
                    shown.waiting = Some(Waiting::new(
                        roots.chain(adding.drain(..)).collect(),
                        Some("its state DB can't be used: it is being checked".into()),
                    ));
                    heal = Some(why);
                    drop(shown);
                    self.changed.raise();
                    // Stopped at once.
                    continue;
                }
                shown.runners.retain(|r| !matches!(r.life, Life::Gone));
                changed |= shown.runners.len() != before;
                // (A share's starts again with the shares, below.)
                let due: Vec<Root> = shown
                    .runners
                    .iter()
                    .filter(|r| own_due(r))
                    .map(|r| r.root.clone())
                    .collect();
                let ready: Vec<Root> = adding
                    .extract_if(.., |a| {
                        !shown.runners.iter().any(|r| r.root.folder == a.folder)
                    })
                    .collect();
                (due, ready)
            };
            // While the config can't be read: when to read it again for folders to start.
            let mut config_again = None;
            if !due.is_empty() || !ready.is_empty() {
                // Started without the lock (only this thread changes the runners), with the
                // config as it is now (its settings may have changed meanwhile); while it
                // can't be read, restarts wait a while and new folders wait a moment.
                let config = Config::now(paths);
                let paused = lock(&self.shown).paused();
                let main = self.main_folder();
                let start = |root: &Root| {
                    let config = config.as_ref()?;
                    let shared = (shared_by(&shares, &markers), main.as_deref());
                    Some(self.start(&drive, config, root, paused, can_heal, shared))
                };
                let restarted: Vec<_> = due.iter().map(start).collect();
                let started: Vec<_> = ready.iter().map(start).collect();
                let mut shown = lock(&self.shown);
                // The pause changed while they started: they were told nothing of it.
                let missed = shown.missed(paused);
                let tell = |r: &Running| {
                    if let Some(command) = &missed {
                        r.handle.send(command.clone());
                    }
                };
                let restarting = shown.runners.iter_mut().filter(|r| own_due(r));
                for (r, again) in restarting.zip(restarted) {
                    match again {
                        Some(Ok(new)) => {
                            tell(&new);
                            *r = Running {
                                backoff: r.backoff,
                                ..new
                            };
                        }
                        Some(Err(e)) => result = Err(e.into()),
                        None => r.life = Life::Waiting(now + r.backoff.next()),
                    }
                }
                for (root, new) in ready.into_iter().zip(started) {
                    match new {
                        Some(Ok(r)) => {
                            tell(&r);
                            shown.runners.push(r);
                        }
                        Some(Err(e)) => result = Err(e.into()),
                        None => {
                            adding.push(root);
                            config_again = Some(now + CONFIG_AGAIN);
                        }
                    }
                }
                changed = true;
            }
            // (Not while it ends: every runner stops, and nothing of the shares is due.)
            let mut shares_next = None;
            if !ending {
                // (Nothing is asked of the identity while paused.)
                if !paused {
                    markers.ask(&shares, now);
                }
                // The main folder's runner started while nothing was shared makes no folder
                // of shares: with a share to sync now, it is told to. (One waiting to start
                // again starts with it.)
                if sharing(&shares) {
                    let mut shown = lock(&self.shown);
                    let main = |r: &&mut Running| r.sharer.is_none() && r.root.is_main();
                    for r in shown.runners.iter_mut().filter(main) {
                        if matches!(r.life, Life::Syncing(_)) && !r.shared {
                            r.handle.send(Command::HoldShares(SHARES.to_owned()));
                            r.shared = true;
                        }
                    }
                }
            }
            if !ending && !shares.is_empty() {
                let (moved, next, gone) =
                    self.run_shares(paths, &mut shares, shares_at.as_ref(), now);
                (changed, shares_next) = (changed | moved, next);
                to_end.extend(gone);
            }
            // A copy taken off (or not): said, and the markers read again (one turned on
            // meanwhile is opened now; one that was stopped is found again by its records).
            // (A pause holds it as it holds syncing: stopped between two operations, and
            // taken up at the resume, found again by its records.)
            ending_stop.store(ending || paused, Ordering::SeqCst);
            if let Some(ended) = ending_copy.take_if(|e| e.done.load(Ordering::SeqCst)) {
                match self.copy_ended(ended) {
                    Some((sharer, all)) => markers.ended(sharer, all, Instant::now()),
                    None => markers.changed(Instant::now()),
                }
            }
            // The next one's copy, on a thread of its own, where the folder of shares is
            // known (it waits where not).
            if !ending
                && !paused
                && ending_copy.is_none()
                && !to_end.is_empty()
                && let Some(at) = &shares_at
            {
                let share = to_end.remove(0);
                let device = config.device_name();
                ending_copy = self.end_copy(share, paths, device, at, &ending_stop);
            }
            if changed {
                self.changed.raise();
            }
            // Until a message, or the next thing that is due.
            let (paused, restart) = {
                let shown = lock(&self.shown);
                if shown.runners.is_empty() {
                    drop(shown);
                    // (A copy being taken off stops between two operations, and is taken up
                    // at the next start: its records are kept until all of it went.)
                    ending_stop.store(true, Ordering::SeqCst);
                    // (What it had moved out by then is told.)
                    if let Some(ended) = ending_copy.take() {
                        let _ = self.copy_ended(ended);
                    }
                    notifications.finish();
                    for share in &shares {
                        share.share.notifications.finish();
                    }
                    keeper.finish();
                    return result.map(|()| heal);
                }
                let restart = shown.runners.iter().filter_map(Running::restart_at).min();
                (shown.paused().is_some(), restart)
            };
            let now = Instant::now();
            let deadlines = [
                notifications.deadline(now, paused),
                (!paused).then_some(next_purge),
                config_again,
                restart,
                shares_next,
                markers.deadline(paused).filter(|_| !ending),
            ];
            let wait = wait_for(now, deadlines.into_iter().flatten());
            msgs.extend(requests.recv_timeout(wait).ok());
            msgs.extend(requests.try_iter());
        }
    }

    /// Starts syncing the folder `root` on the account's `drive`, with `config`'s settings,
    /// paused when `paused`; ending when the DB can't be used if the account `heals` it. The
    /// main folder of an account other identities share with, or may ([`Shared`], with the
    /// account's main folder where it syncs one), tells the account where its folder of
    /// shares is, and makes one where a share is synced; a folder beside it knows that one
    /// by the main folder's record.
    fn start(
        &self,
        drive: &Arc<Drive>,
        config: &Config,
        root: &Root,
        paused: Option<Pause>,
        heals: bool,
        (shared, main): (Shared, Option<&Path>),
    ) -> Result<Running, RunnerError> {
        let mut cfg = RunnerConfig::new(
            &root.folder,
            &self.db,
            &self.identity,
            &config.device_name(),
        );
        cfg.mass_delete = config.mass_delete;
        // The config's entry has the say, as when it was read for the account's start.
        let listed = config.roots.iter().find(|r| r.folder == root.folder);
        let root = listed.unwrap_or(root).clone();
        cfg.kind = root.kind;
        cfg.paused = paused;
        cfg.end_if_unusable = heals;
        // A folder beside the main one knows the folder of shares by the main folder's
        // record: dragged in here, it is left alone as it is below the main folder's top.
        if !root.is_main() {
            cfg.shares_main = main.map(Path::to_path_buf);
        }
        // The main folder of an account shares may come to gives its watcher to theirs and
        // tells where its folder of shares is, from its start; it makes one where a share
        // is synced (told later for one that comes later: [`Command::HoldShares`]).
        if shared != Shared::No && root.is_main() {
            cfg.feed = Some(hbsync_local::Feed::Gives(self.feeds.clone()));
            cfg.shares = (shared == Shared::Now).then(|| SHARES.to_owned());
            let (folder, tell) = (root.folder.clone(), self.requests.clone());
            cfg.shares_told = Some(SharesTold {
                tell: Arc::new(move |at| {
                    let _ = tell.send(Msg::Shares(folder.clone(), at));
                }),
                last: Arc::clone(&self.shares_told),
            });
        }
        self.spawn(cfg, drive, root)
    }

    /// Starts syncing what `share`'s identity shares into its folder in `dir`, the folder of
    /// shares of the main folder `main`: one way, what changes there moving out to the main
    /// folder's top (sync semantics §13). The folder is made where missing; its runner sets
    /// it up in the share's own DB as it opens it (found there again when the folder of
    /// shares was renamed). Never where something else has its place, a link to a folder
    /// neither: what that leads to isn't the share's.
    fn start_share(
        &self,
        share: &Sharing,
        config: &Config,
        (main, dir): (&Path, &Path),
        paused: Option<Pause>,
    ) -> Result<Running, RunnerError> {
        let folder = dir.join(&share.local);
        match std::fs::create_dir(&folder) {
            Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => return Err(e.into()),
            _ => {}
        }
        if !std::fs::symlink_metadata(&folder)?.is_dir() {
            let taken = format!("{} isn't a folder", folder.display());
            return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, taken).into());
        }
        let Share {
            sharer, drive, db, ..
        } = &share.share;
        let device = config.device_name();
        let mut cfg = RunnerConfig::new(&folder, db, &self.identity, &device);
        cfg.mass_delete = config.mass_delete;
        cfg.sharer = Some(sharer.clone());
        cfg.read_only = Some(main.to_path_buf());
        // Its folder lies in the main folder: watched by that one's watcher.
        cfg.feed = Some(hbsync_local::Feed::Takes(self.feeds.clone()));
        cfg.paused = paused;
        self.spawn(cfg, drive, Root::main(&self.identity, &folder))
    }

    /// Starts the runner of `root` with `cfg` on `drive`: it wakes the account as it ends.
    fn spawn(
        &self,
        mut cfg: RunnerConfig,
        drive: &Arc<Drive>,
        root: Root,
    ) -> Result<Running, RunnerError> {
        cfg.changed = Arc::clone(&self.changed);
        let (sharer, shared) = (cfg.sharer.clone(), cfg.shares.is_some());
        let done = Arc::new(AtomicBool::new(false));
        let (ending, tell) = (Arc::clone(&done), self.requests.clone());
        let (handle, thread) = Runner::spawn_with(cfg, Arc::clone(drive), move || {
            ending.store(true, Ordering::SeqCst);
            let _ = tell.send(Msg::Wake);
        })?;
        Ok(Running {
            root,
            sharer,
            handle,
            life: Life::Syncing(thread),
            done,
            started: Instant::now(),
            backoff: Backoff::new(),
            shared,
        })
    }

    /// Has each of `shares` synced by a runner in the folder of shares the main folder's
    /// runner told (`at`: the main folder, and the folder of shares in it), while that runner
    /// shows: started there, started again after it failed (its folder gone: made again), and
    /// stopped where that isn't its place any more (the folder of shares has another name or
    /// is gone, the main folder is removed), to start in the new place once it has ended. One
    /// that can't start is tried again after a pause; the account's own folders sync on. One
    /// its marker turned off ([`Sharing::off`]) has its runner stopped, and is taken out of
    /// `shares` once that has ended. A share's drive is read (its notifications' timer) only
    /// while a runner syncs it.
    ///
    /// Whether the folders shown changed, when to look again at the latest (a start to try
    /// again, a drive to read), and the shares that aren't synced any more, taken out of
    /// `shares`: each one's copy is to be taken off this device.
    fn run_shares(
        &self,
        paths: &Paths,
        shares: &mut Vec<Sharing>,
        at: Option<&(PathBuf, PathBuf)>,
        now: Instant,
    ) -> (bool, Option<Instant>, Vec<Sharing>) {
        let mut changed = false;
        // The shares to start, each with the pauses of the runner it starts again for.
        let mut starting: Vec<(usize, Option<Backoff>)> = Vec::new();
        let (at, paused) = {
            let mut shown = lock(&self.shown);
            let paused = shown.paused();
            // While the main folder that holds the folder of shares is synced.
            let at = at.filter(|(main, _)| shown.own().any(|r| r.root.folder == *main));
            for (i, share) in shares.iter_mut().enumerate() {
                let sharer = share.share.sharer.clone();
                let theirs = |r: &&mut Running| r.sharer.as_ref() == Some(&sharer);
                // One at most: the next starts once the last has ended.
                let again = match shown.runners.iter_mut().find(theirs) {
                    // Not its place any more, or its marker turned it off: stopped.
                    Some(r)
                        if share.off
                            || !at.is_some_and(|(_, dir)| share.is_at(dir, &r.root.folder)) =>
                    {
                        if r.shows() {
                            r.stop_for_good();
                            changed = true;
                        }
                        // Still ending: started in the new place once it has (it wakes the
                        // account). One that only waited to start again is gone at once.
                        if !matches!(r.life, Life::Gone) {
                            continue;
                        }
                        None
                    }
                    // Started again where it was, below: this one goes.
                    Some(r) if r.due(now) => {
                        r.life = Life::Gone;
                        Some(r.backoff)
                    }
                    Some(_) => {
                        share.share.notifications.tick(now, paused.is_some());
                        continue;
                    }
                    None => None,
                };
                match at.filter(|_| !share.off) {
                    Some(_) if share.retry.is_none_or(|at| now >= at) => starting.push((i, again)),
                    Some(_) => {}
                    // No place to start in: nothing to try again.
                    None => share.retry = None,
                }
            }
            let before = shown.runners.len();
            shown.runners.retain(|r| !matches!(r.life, Life::Gone));
            changed |= shown.runners.len() != before;
            (at, paused)
        };
        if let Some((main, dir)) = at.filter(|_| !starting.is_empty()) {
            // Started without the lock, as the account's own. Never into a folder of that
            // name that isn't the folder of shares (gone, or renamed, since its runner told):
            // what the identities share would be the account's own there, and sent.
            let config = Config::now(paths);
            let within = ending::is_shares_folder(main, dir);
            for (i, again) in starting {
                let share = &mut shares[i];
                let started = match (&config, &within) {
                    (None, _) => {
                        share.retry = Some(now + CONFIG_AGAIN);
                        continue;
                    }
                    (Some(config), Ok(true)) => {
                        self.start_share(share, config, (main, dir), paused)
                    }
                    (Some(_), _) => {
                        let gone = format!("{} isn't the folder of shares", dir.display());
                        Err(std::io::Error::new(std::io::ErrorKind::NotFound, gone).into())
                    }
                };
                match started {
                    Ok(new) => {
                        (share.retry, share.backoff) = (None, Backoff::new());
                        let mut shown = lock(&self.shown);
                        if let Some(command) = shown.missed(paused) {
                            new.handle.send(command);
                        }
                        let backoff = again.unwrap_or(new.backoff);
                        shown.runners.push(Running { backoff, ..new });
                        changed = true;
                    }
                    Err(e) => {
                        let pause = share.backoff.next();
                        tracing::warn!(
                            "what {} shares can't be synced into {} ({e}): trying again in {pause:?}",
                            share.share.sharer,
                            dir.display()
                        );
                        share.retry = Some(now + pause);
                    }
                }
            }
        }
        // One its marker turned off goes once its runner has ended, where its place is known
        // (it waits where not): its copy is the caller's to take off this device, off this
        // thread and after the lock is let go (its puller's thread may be in a request).
        let shown = lock(&self.shown);
        let ended = |share: &mut Sharing| {
            let theirs = |r: &Running| r.sharer.as_ref() == Some(&share.share.sharer);
            share.off && at.is_some() && !shown.runners.iter().any(theirs)
        };
        let gone: Vec<Sharing> = shares.extract_if(.., ended).collect();
        changed |= !gone.is_empty();
        // A start to try again (only where there is a place: cleared above otherwise), and
        // the drives of those a runner syncs.
        let synced = |s: &&Sharing| {
            shown
                .shown()
                .any(|r| r.sharer.as_ref() == Some(&s.share.sharer))
        };
        let reads = shares
            .iter()
            .filter(synced)
            .filter_map(|s| s.share.notifications.deadline(now, paused.is_some()));
        let next = shares.iter().filter_map(|s| s.retry).chain(reads).min();
        (changed, next, gone)
    }

    /// Takes the copy of `share`, which isn't synced any more, off this device
    /// ([`ending::end`]): from its folder in the folder of shares `dir` of the main folder
    /// `main`, where what isn't the sharer's moves out to. On a thread of its own, which
    /// wakes the account as it ends; given up at once where `stop` is set. `None` where no
    /// thread starts (said: its copy stays, taken up when the markers are next read).
    fn end_copy(
        &self,
        share: Sharing,
        paths: &Paths,
        device: String,
        (main, dir): &(PathBuf, PathBuf),
        stop: &Arc<AtomicBool>,
    ) -> Option<Ending> {
        let sharer = share.share.sharer.clone();
        let place = ending::Place {
            identity: self.identity.clone(),
            device,
            folder: dir.join(&share.local),
            out: main.clone(),
            records: paths.shared_db(&self.identity, &sharer),
        };
        let (at, stop, tell) = (place.clone(), Arc::clone(stop), self.requests.clone());
        let done = Arc::new(AtomicBool::new(false));
        let finished = Arc::clone(&done);
        let end = move || {
            // Done, then the wake, also where it panics: the account, woken, finds it done
            // (its thread may not have ended yet as the account looks, and nothing would
            // wake it again), and takes what it came to.
            let _done = OnDrop(Some(move || {
                finished.store(true, Ordering::SeqCst);
                let _ = tell.send(Msg::Wake);
            }));
            ending::end(share.share, &at, stop)
        };
        let spawned = std::thread::Builder::new()
            .name("hbsync-share-end".into())
            .spawn(end);
        match spawned {
            Ok(thread) => Some(Ending {
                sharer,
                place,
                done,
                thread,
            }),
            Err(e) => {
                tracing::error!(
                    "no thread to take the copy of what {sharer} shared off this computer \
                     ({e}): it stays in {}",
                    place.folder.display()
                );
                None
            }
        }
    }

    /// What the ending of a share's copy came to, told on the account's status: whose it
    /// was, and whether all of it went. `None` for one that was stopped (a pause, or the
    /// account ending): it is taken up again, found by its records, and only what had
    /// moved out by then is said.
    fn copy_ended(&self, ending: Ending) -> Option<(Identity, bool)> {
        let Ending {
            sharer,
            place,
            thread,
            ..
        } = ending;
        let ended = thread.join().unwrap_or_else(|_| ending::Ended {
            failed: Some(RunnerError::Panicked.to_string()),
            ..Default::default()
        });
        if ended.stopped && ended.failed.is_none() {
            // (What moved out before it was stopped is said now: the removal that goes on
            // later has none of it to tell.)
            if ended.moved_out > 0 {
                let moved = ended.moved_out;
                let note = words::share_moved_out(&sharer, &place.folder, &place.out, moved);
                self.note(vec![note]);
            }
            return None;
        }
        let counts = (ended.moved_out, ended.left.len());
        let why = ended.failed.as_deref();
        let note = words::share_ended(&sharer, &place.folder, &place.out, counts, why);
        self.note(vec![note]);
        Some((sharer, ended.all()))
    }

    /// The credentials, waiting while the keychain can't be read; `None` when stopped first.
    fn credentials(
        &self,
        paths: &Paths,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> Result<Option<hbsync_homebase::Credentials>, DaemonError> {
        let store = CredentialStore::open(paths);
        let mut backoff = Backoff::new();
        let (creds, location) = loop {
            let tried = self.clicks.begin_try();
            match store.load(&self.identity) {
                Ok(Some(found)) => break found,
                Ok(None) => {
                    return Err(DaemonError::NotSetUp(format!(
                        "no credentials for {}",
                        self.identity
                    )));
                }
                Err(CredentialError::Keychain(why)) => {
                    let wait_for = backoff.next();
                    tracing::warn!("can't read the keychain ({why}); trying again in {wait_for:?}");
                    let why = format!("can't read the keychain: {why}");
                    self.show(State::Failing, Some(why), false, false);
                    if self.wait(wait_for, tried, stop, rx, early) {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        };
        if let Location::File { path, why } = location {
            tracing::warn!("credentials in {} (no keychain: {why})", path.display());
            let warning = format!("kept in {} (no keychain: {why})", path.display());
            self.set_status(|s| s.credentials_warning = Some(warning));
        }
        let without_key = creds.without_zk_key();
        self.set_status(|s| s.login_without_key = without_key);
        Ok(Some(creds))
    }

    /// One try at connecting, on a thread of its own that answers on the account's channel:
    /// an identity that takes long to answer, or never does (a resolver or a server that
    /// hangs, up to the request's timeouts), doesn't keep the account from stopping. `None`:
    /// it stops, the try left to end by itself (its answer, or the process's end). What else
    /// comes meanwhile is kept in `early`.
    fn try_connect(
        &self,
        client: &Client,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> Option<Result<HomebaseRemote, HomebaseError>> {
        if self.stopping(stop) {
            return None;
        }
        let (trying, to_account) = (client.clone(), self.requests.clone());
        let spawned = std::thread::Builder::new()
            .name("hbsync-connect".into())
            .spawn(move || {
                let tried = HomebaseRemote::connect(trying);
                let _ = to_account.send(Msg::Connected(Box::new(tried)));
            });
        if let Err(e) = spawned {
            // Tried again after the backoff, as one that got no answer.
            return Some(Err(HomebaseError::Network(format!("can't try: {e}"))));
        }
        while !self.stopping(stop) {
            match rx.recv() {
                Ok(Msg::Connected(tried)) => return Some(*tried),
                // Sync now: counted ([`Clicks`]), for the wait after this try.
                Ok(Msg::Wake | Msg::Woke | Msg::SyncNow) => {}
                Ok(msg) => early.push(msg),
                Err(_) => return None,
            }
        }
        None
    }

    /// The drive, once the identity answers (finding it takes a request); `None` when stopped
    /// first.
    fn connect(
        &self,
        client: &Client,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> Option<Arc<HomebaseRemote>> {
        let tried = |early: &mut Vec<Msg>| self.try_connect(client, stop, rx, early);
        self.connect_by(tried, stop, rx, early).map(Arc::new)
    }

    /// [`Account::connect`] by `try_once`, until a try gets through. While it waits to try
    /// again, Sync now tries at once, as a folder's does: what a try asked for found counts as
    /// any try's. A refusal stays shown through tries that would show offline, till one goes
    /// well ([`Showing::failed`]).
    fn connect_by<R>(
        &self,
        mut try_once: impl FnMut(&mut Vec<Msg>) -> Option<Result<R, HomebaseError>>,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> Option<R> {
        let mut backoff = Backoff::new();
        // The last refusal.
        let mut refused = None;
        loop {
            let tried = self.clicks.begin_try();
            let e = match try_once(early)? {
                Ok(r) => {
                    // Connected: no longer refused, nor waiting to connect, while its folders
                    // start (a check of its DB may come first).
                    self.show(State::Starting, None, false, false);
                    return Some(r);
                }
                Err(e) => e,
            };
            let class = e.class();
            // Refused: the credentials won't work sooner; a login starts it over.
            let asked = match class {
                ErrorClass::Denied => Some(NEEDS_USER_AGAIN),
                _ => e.retry_after(),
            };
            let wait_for = backoff.after(class, asked);
            tracing::warn!(
                "can't connect to {} ({e}); trying again in {wait_for:?}",
                self.identity
            );
            let refusal = e.refusal();
            let showing = Showing::failed(class, refusal, e.to_string(), refused.clone());
            if let Some(refusal) = refusal {
                refused = Some((showing.why.clone(), refusal));
            }
            self.show(showing.state, Some(showing.why), true, showing.lacks_key);
            if self.wait(wait_for, tried, stop, rx, early) {
                return None;
            }
        }
    }
}

/// Whether any of `shares` is to be synced: the main folder holds a folder of shares then.
fn sharing(shares: &[Sharing]) -> bool {
    shares.iter().any(|share| !share.off)
}

/// Whether an account's main folder is to hold a folder of shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shared {
    /// A share is synced: one is made where none is on record.
    Now,
    /// None is, but its markers may turn one on: none is made, one on record is told.
    Maybe,
    /// Nothing is shared with it, nor can be.
    No,
}

/// How long after an account's start its share markers are first read where its
/// notifications haven't connected by then (a connect reads them, and tells their changes
/// from then on: read before it, they would be read twice at every start).
const FIRST_READ: Duration = Duration::from_secs(10);

fn shared_by(shares: &[Sharing], markers: &Markers) -> Shared {
    match (sharing(shares), markers.any()) {
        (true, _) => Shared::Now,
        (false, true) => Shared::Maybe,
        (false, false) => Shared::No,
    }
}

#[cfg(test)]
mod tests {
    use super::super::Clicks;
    use super::*;

    /// An account of `identity`, its folder in a temporary home.
    fn account(identity: &str) -> (tempfile::TempDir, Arc<Account>, Receiver<Msg>) {
        let home = tempfile::tempdir().expect("a temporary folder");
        let paths = Paths::under(home.path());
        let root = Root::main(&identity.into(), &home.path().join("Homebase"));
        let (account, rx) = Account::new(&paths, &root, None, Arc::default());
        (home, account, rx)
    }

    /// One try a click: a click before a try began adds nothing to the wait after it (that try
    /// was the one asked for), one during it or the wait ends that wait; one a wait ended for
    /// (a heal's, within the try) doesn't end the next too.
    #[test]
    fn one_try_a_click() {
        let clicks = Clicks::default();
        clicks.click();
        let tried = clicks.begin_try();
        assert!(!clicks.clicked(tried), "before the try");
        clicks.click();
        assert!(clicks.clicked(tried), "during the try");
        assert!(!clicks.clicked(tried), "taken");

        let (_home, account, rx) = account("a.example");
        let stop = AtomicBool::new(false);
        let tried = account.clicks.begin_try();
        let clicked = Arc::clone(&account);
        let click = std::thread::spawn(move || clicked.sync_now());
        let (long, began) = (Duration::from_secs(20), Instant::now());
        assert!(!account.wait(long, tried, &stop, &rx, &mut Vec::new()));
        assert!(began.elapsed() < long / 2, "waited it out");
        click.join().expect("clicked").expect_err("not running yet");
    }

    /// Sync now asked during a slow try at connecting (taken by the try, as it waits for its
    /// answer) ends the wait after it at once.
    #[test]
    fn sync_now_during_a_slow_try_tries_again_after_it() -> Result<(), Box<dyn std::error::Error>> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let identity = format!("127.0.0.1:{}", listener.local_addr()?.port());
        let (_home, account, rx) = account(&identity);
        let client = Client::new(&crate::secrets::sample(&identity))?;
        let stop = Arc::new(AtomicBool::new(false));
        // Clicked while it holds the first try, then drops it: how long until the next try
        // comes. The wait after a failed try is 5 s.
        let (clicked, stopping) = (Arc::clone(&account), Arc::clone(&stop));
        let server = std::thread::spawn(move || -> std::io::Result<Duration> {
            let (first, _) = listener.accept()?;
            let began = Instant::now();
            let _ = clicked.sync_now();
            drop(first);
            let (_second, _) = listener.accept()?;
            stopping.store(true, Ordering::Relaxed);
            clicked.wake();
            Ok(began.elapsed())
        });
        let connected = account.connect(&client, &stop, &rx, &mut Vec::new());
        assert!(connected.is_none());
        let between = server.join().map_err(|_| "panicked")??;
        assert!(
            between < Duration::from_secs(3),
            "waited it out: {between:?}"
        );
        Ok(())
    }

    /// While its DB waits for the user (a newer version's: looked at again hourly), Sync now
    /// looks again at once.
    #[test]
    fn sync_now_ends_the_wait_for_its_db() -> Result<(), Box<dyn std::error::Error>> {
        let (home, account, rx) = account("a.example");
        let db = Paths::under(home.path()).account_db(&"a.example".into());
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        rusqlite::Connection::open(&db)?.pragma_update(None, "user_version", 9999)?;
        let stop = Arc::new(AtomicBool::new(false));
        let readying = Arc::clone(&account);
        let (paths, stopping) = (Paths::under(home.path()), Arc::clone(&stop));
        let ready = std::thread::spawn(move || {
            let mut msgs = Vec::new();
            let readied =
                readying.ready_db(&paths, Readying::Start(&[]), &stopping, &rx, &mut msgs);
            readied.map_err(|e| e.to_string())
        });
        let shown = || account.status().1[0].clone();
        let until = Instant::now() + Duration::from_secs(10);
        while shown().state != State::Failing && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            shown().error.is_some_and(|e| e.contains("newer")),
            "{:?}",
            shown()
        );
        // Set right by the user (here: gone, made again), then asked.
        std::fs::remove_file(&db)?;
        account.sync_now().expect_err("not running yet");
        while !ready.is_finished() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, Ordering::Relaxed);
        account.wake();
        let readied = ready.join().map_err(|_| "panicked")??;
        assert_eq!(readied, Some(false), "waited for the next look");
        Ok(())
    }

    /// While it connects, a refusal shows, and why where the error tells it (the private
    /// drive's key), through tries that would show offline (no answer, a 503), Sync now saying
    /// it tries again; another answer shows as itself. Connected, it shows none of it.
    #[test]
    fn a_refusal_shows_until_it_connects() {
        let (_home, account, rx) = account("a.example");
        let (stop, mut early) = (AtomicBool::new(false), Vec::new());
        let network = || Err(HomebaseError::Network("failed to lookup address".into()));
        let busy = Err(HomebaseError::Api {
            what: "drive".into(),
            error: hbsync_homebase::ApiError {
                status: 503,
                code: None,
                title: "busy".into(),
                from_identity: true,
                retry_after: None,
            },
        });
        let mut answers = vec![
            Err(HomebaseError::NoDriveKey),
            network(),
            Err(HomebaseError::Denied("401".into())),
            busy,
            Err(HomebaseError::Protocol("odd".into())),
            network(),
        ]
        .into_iter();
        // What each try found shown, and what Sync now said then (its click ends the wait).
        let mut tries = Vec::new();
        let tried = |_: &mut Vec<Msg>| {
            let f = account.status().1[0].clone();
            let said = account.sync_now().expect_err("not running yet");
            tries.push((f.state, f.error, f.lacks_key, said));
            Some(answers.next().unwrap_or(Ok(())))
        };
        assert_eq!(account.connect_by(tried, &stop, &rx, &mut early), Some(()));
        let (key, login) = (HomebaseError::NoDriveKey.to_string(), "denied: 401");
        let shown: Vec<_> = tries
            .iter()
            .map(|(state, error, key, _)| (*state, error.as_deref(), *key))
            .collect();
        assert_eq!(
            shown,
            [
                (State::Starting, None, false),
                (State::Denied, Some(key.as_str()), true),
                (State::Denied, Some(key.as_str()), true),
                (State::Denied, Some(login), false),
                (State::Denied, Some(login), false),
                (State::Failing, Some("protocol: odd"), false),
                (State::Denied, Some(login), false),
            ]
        );
        let waiting = "not connected to a.example yet";
        let trying = words::trying_to_connect(&"a.example".into());
        assert_eq!(tries[0].3, waiting);
        assert!(tries[1..].iter().all(|t| t.3 == trying), "{tries:?}");
        let f = account.status().1[0].clone();
        assert_eq!(
            (f.state, f.error, f.lacks_key),
            (State::Starting, None, false)
        );
        assert_eq!(account.sync_now().expect_err("not running yet"), waiting);
    }

    /// Refused, then asked to sync now: a try that gets no answer waits as briefly as any
    /// that got none (not refused for an hour), and the next try goes then; meanwhile the
    /// refusal stays shown.
    #[test]
    fn a_try_asked_for_while_refused_that_gets_no_answer_waits_briefly() {
        let (_home, account, rx) = account("a.example");
        let (stop, mut early) = (Arc::new(AtomicBool::new(false)), Vec::new());
        // Not an hour, failing: stopped by then.
        let (stopping, waking) = (Arc::clone(&stop), Arc::clone(&account));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(60));
            stopping.store(true, Ordering::Relaxed);
            waking.wake();
        });
        let mut answers = vec![
            Err(HomebaseError::Denied("the drive is private now".into())),
            Err(HomebaseError::Network("failed to lookup address".into())),
        ]
        .into_iter();
        let mut tries = Vec::new();
        let tried = |_: &mut Vec<Msg>| {
            let shown = account.status().1[0].state;
            tries.push((Instant::now(), shown));
            // Asked as it is refused: the wait after ends.
            if tries.len() == 1 {
                let _ = account.sync_now();
            }
            Some(answers.next().unwrap_or(Ok(())))
        };
        assert_eq!(account.connect_by(tried, &stop, &rx, &mut early), Some(()));
        let shown: Vec<_> = tries.iter().map(|(_, state)| *state).collect();
        assert_eq!(shown, [State::Starting, State::Denied, State::Denied]);
        assert!(
            tries[1].0 - tries[0].0 < Duration::from_secs(5),
            "asked at once"
        );
        assert!(
            tries[2].0 - tries[1].0 < Duration::from_secs(60),
            "waited an hour"
        );
    }

    /// Sync now taken while a try at connecting is under way isn't kept as a message: it is
    /// counted ([`Clicks`]).
    #[test]
    fn sync_now_during_a_try_is_not_kept() -> Result<(), Box<dyn std::error::Error>> {
        // A port nothing listens on: the try is refused at once.
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let identity = format!("127.0.0.1:{port}");
        let (_home, account, rx) = account(&identity);
        let client = Client::new(&crate::secrets::sample(&identity))?;
        let (stop, mut early) = (AtomicBool::new(false), Vec::new());
        account.sync_now().expect_err("not running yet");
        let tried = account.try_connect(&client, &stop, &rx, &mut early);
        assert!(matches!(tried, Some(Err(_))), "{tried:?}");
        assert!(early.is_empty(), "kept for later");
        assert!(rx.try_recv().is_err(), "left for the next wait");
        Ok(())
    }
}
