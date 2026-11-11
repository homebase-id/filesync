//! The account's loop, on its own thread: it waits for the credentials and the identity, wires
//! the account up (the notifications, the [`Drive`], the puller, Recently deleted's keeper),
//! starts its folders' runners, and serves its channel ([`Msg`]) until it ends.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, HomebaseError};
use hbsync_remote::ErrorClass;
use hbsync_state::Unusable;

use super::parts::AccountParts;
use super::runners::{Life, Running};
use super::{Account, Msg, Request, Waiting};
use crate::daemon::DaemonError;
use crate::daemon::keeper::Keeper;
use crate::folders::{heal_account, ready_account};
use crate::protocol::{AccountStatus, Hints, Pause, State};
use crate::runner::{Backoff, Command, Runner, RunnerConfig, RunnerError, shown};
use crate::sync::{lock, wait_for};
use crate::words;
use crate::{Config, CredentialError, CredentialStore, Location, Paths, Root};

/// How an account readies its state DB ([`Account::ready_db`]).
pub(super) enum Readying<'a> {
    /// As it starts ([`ready_account`]), its folders among these (the config's).
    Start(&'a [Root]),
    /// As it was found unusable while the account ran ([`heal_account`], its folders those it
    /// syncs): checked, or with why, set aside unchecked.
    Heal(Option<Unusable>),
}

/// While what only the user can fix keeps an account from starting, how often it looks again.
const NEEDS_USER_AGAIN: Duration = Duration::from_secs(3600);

/// How often the purge of Recently deleted is looked at (it runs once a day).
const PURGE_LOOK: Duration = Duration::from_secs(600);
/// While the config can't be read, folders to start wait this long before it is read again.
const CONFIG_AGAIN: Duration = Duration::from_secs(1);

impl Account {
    /// Shows that it waits in `to`, for `why`.
    fn show_waiting(&self, to: State, why: String) {
        if let Some(waiting) = &mut lock(&self.shown).waiting {
            waiting.state = to;
            waiting.error = Some(why);
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

    /// Waits `time` on `rx`, or less if it should stop, the machine woke up meanwhile or, when
    /// `asks`, the user asked to sync now (a try then goes at once). Returns whether it should
    /// stop. Requests and the like that come meanwhile are kept in `early`.
    fn wait(
        &self,
        time: Duration,
        asks: bool,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> bool {
        let until = Instant::now() + time;
        while !self.stopping(stop) {
            match rx.recv_timeout(until.saturating_duration_since(Instant::now())) {
                Ok(Msg::Woke) | Err(_) => break,
                Ok(Msg::SyncNow) if asks => break,
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
            self.show_waiting(State::Failing, e.to_string());
            if self.wait(wait_for, false, stop, requests, msgs) {
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
                    match self.heal(paths, Some(Unusable::OtherDrive), stop, requests, msgs)? {
                        Some(true) => {}
                        Some(false) => {
                            let wait_for = other_drive.after(ErrorClass::LocalFault, None);
                            if self.wait(wait_for, false, stop, requests, msgs) {
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
        } = parts;
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
            for root in roots {
                match self.start(&drive, config, &root, shown.paused(), can_heal) {
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
                    Msg::Hint(event) => {
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
                        if let Some(n) = &notifier {
                            n.reconnect();
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
                        let mut shown = lock(&self.shown);
                        for r in shown.runners.iter_mut().filter(|r| r.root.folder == folder) {
                            r.stop_for_good();
                        }
                        changed = true;
                    }
                    // Asked as its runners started (they sync at their start), or while its DB
                    // was looked at: nothing to do.
                    Msg::SyncNow => {}
                    // Looked at below.
                    Msg::Wake => {}
                }
            }
            notifications.tick(Instant::now(), paused);
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
            let (due, ready) = {
                let mut shown = lock(&self.shown);
                if ending {
                    for r in &shown.runners {
                        r.handle.send(Command::Stop);
                    }
                }
                let before = shown.runners.len();
                // Every runner joined if it ended; the first that ended for the DB.
                let unusable = (shown.runners.iter_mut())
                    .filter_map(|r| r.ended(ending))
                    .reduce(|first, _| first);
                if let Some(why) = unusable.filter(|_| can_heal) {
                    // Its folders wait while the DB is looked at ([`Account::sync`]), as at the
                    // account's start: they show starting, and one added or removed meanwhile
                    // changes them.
                    tracing::error!("{}: its state DB can't be used: stopping", self.identity);
                    let roots = shown.shown().map(|r| r.root.clone());
                    shown.waiting = Some(Waiting {
                        roots: roots.chain(adding.drain(..)).collect(),
                        state: State::Starting,
                        error: Some("its state DB can't be used: it is being checked".into()),
                    });
                    heal = Some(why);
                    drop(shown);
                    self.changed.raise();
                    // Stopped at once.
                    continue;
                }
                shown.runners.retain(|r| !matches!(r.life, Life::Gone));
                changed |= shown.runners.len() != before;
                let due: Vec<Root> = shown
                    .runners
                    .iter()
                    .filter(|r| r.due(now))
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
                let start = |root: &Root| {
                    let config = config.as_ref()?;
                    Some(self.start(&drive, config, root, paused, can_heal))
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
                let restarting = shown.runners.iter_mut().filter(|r| r.due(now));
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
            if changed {
                self.changed.raise();
            }
            // Until a message, or the next thing that is due.
            let (paused, restart) = {
                let shown = lock(&self.shown);
                if shown.runners.is_empty() {
                    drop(shown);
                    notifications.finish();
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
            ];
            let wait = wait_for(now, deadlines.into_iter().flatten());
            msgs.extend(requests.recv_timeout(wait).ok());
            msgs.extend(requests.try_iter());
        }
    }

    /// Starts syncing the folder `root` on the account's `drive`, with `config`'s settings,
    /// paused when `paused`; ending when the DB can't be used if the account `heals` it.
    fn start(
        &self,
        drive: &Arc<Drive>,
        config: &Config,
        root: &Root,
        paused: Option<Pause>,
        heals: bool,
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
        cfg.changed = Arc::clone(&self.changed);
        let done = Arc::new(AtomicBool::new(false));
        let (ending, tell) = (Arc::clone(&done), self.requests.clone());
        let (handle, thread) = Runner::spawn_with(cfg, Arc::clone(drive), move || {
            ending.store(true, Ordering::SeqCst);
            let _ = tell.send(Msg::Wake);
        })?;
        Ok(Running {
            root,
            handle,
            life: Life::Syncing(thread),
            done,
            started: Instant::now(),
            backoff: Backoff::new(),
        })
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
                    self.show_waiting(State::Failing, format!("can't read the keychain: {why}"));
                    if self.wait(wait_for, false, stop, rx, early) {
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
                // Sync now: the try under way is the one asked for.
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
    /// any try's.
    fn connect_by<R>(
        &self,
        mut try_once: impl FnMut(&mut Vec<Msg>) -> Option<Result<R, HomebaseError>>,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> Option<R> {
        let mut backoff = Backoff::new();
        loop {
            let e = match try_once(early)? {
                Ok(r) => return Some(r),
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
            self.show_waiting(shown(class), e.to_string());
            if self.wait(wait_for, true, stop, rx, early) {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An account of `identity`, its folder in a temporary home.
    fn account(identity: &str) -> (tempfile::TempDir, Arc<Account>, Receiver<Msg>) {
        let home = tempfile::tempdir().expect("a temporary folder");
        let paths = Paths::under(home.path());
        let root = Root::main(&identity.into(), &home.path().join("Homebase"));
        let (account, rx) = Account::new(&paths, &root, None, Arc::default());
        (home, account, rx)
    }

    /// Sync now ends a wait to connect again, one try for one ask; the account's other waits
    /// (its DB, the keychain) take it, and change nothing.
    #[test]
    fn sync_now_ends_only_a_wait_to_connect() {
        let (_home, account, rx) = account("a.example");
        let (stop, mut early) = (AtomicBool::new(false), Vec::new());
        let (long, short) = (Duration::from_secs(20), Duration::from_millis(200));
        account.sync_now().expect_err("not running yet");
        let began = Instant::now();
        assert!(!account.wait(long, true, &stop, &rx, &mut early));
        assert!(began.elapsed() < long / 2, "waited it out");
        let began = Instant::now();
        assert!(!account.wait(short, true, &stop, &rx, &mut early));
        assert!(began.elapsed() >= short, "one try for one ask");

        account.sync_now().expect_err("not running yet");
        let began = Instant::now();
        assert!(!account.wait(short, false, &stop, &rx, &mut early));
        assert!(began.elapsed() >= short, "another wait");
        assert!(rx.try_recv().is_err(), "taken");
        assert!(early.is_empty(), "not kept for later");
    }

    /// Refused, then asked to sync now: a try that gets no answer waits as briefly as any
    /// that got none (offline, not refused for an hour), and the next try goes then.
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
        account.sync_now().expect_err("not running yet");
        let tried = |_: &mut Vec<Msg>| {
            let shown = account.status().1[0].state;
            tries.push((Instant::now(), shown));
            Some(answers.next().unwrap_or(Ok(())))
        };
        assert_eq!(account.connect_by(tried, &stop, &rx, &mut early), Some(()));
        let shown: Vec<_> = tries.iter().map(|(_, state)| *state).collect();
        assert_eq!(shown, [State::Starting, State::Denied, State::Offline]);
        assert!(
            tries[1].0 - tries[0].0 < Duration::from_secs(5),
            "asked at once"
        );
        assert!(
            tries[2].0 - tries[1].0 < Duration::from_secs(60),
            "waited an hour"
        );
    }

    /// Sync now while a try at connecting is under way adds nothing: that try is the one asked
    /// for, and nothing is kept for later.
    #[test]
    fn sync_now_during_a_try_adds_nothing() -> Result<(), Box<dyn std::error::Error>> {
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
