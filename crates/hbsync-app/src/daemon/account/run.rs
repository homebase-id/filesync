//! The account's loop, on its own thread: it waits for the credentials and the identity, wires
//! the account up (the notifications, the [`Drive`], the puller, Recently deleted's keeper),
//! starts its folders' runners, and serves its channel ([`Msg`]) until it ends.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, HomebaseError};
use hbsync_remote::ErrorClass;

use super::parts::AccountParts;
use super::runners::{Life, Running};
use super::{Account, Msg, Request};
use crate::daemon::DaemonError;
use crate::daemon::keeper::Keeper;
use crate::folders::ready_account;
use crate::protocol::{AccountStatus, Hints, Pause, State};
use crate::runner::{Backoff, Command, Runner, RunnerConfig, RunnerError, shown};
use crate::sync::{lock, wait_for};
use crate::words;
use crate::{Config, CredentialError, CredentialStore, Location, Paths, Root};

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

    /// Waits `time` on `rx`, or less if it should stop or the machine woke up meanwhile (a
    /// try then goes at once). Returns whether it should stop. Requests and the like that come
    /// meanwhile are kept in `early`.
    fn wait(
        &self,
        time: Duration,
        stop: &AtomicBool,
        rx: &Receiver<Msg>,
        early: &mut Vec<Msg>,
    ) -> bool {
        let until = Instant::now() + time;
        while !self.stopping(stop) {
            match rx.recv_timeout(until.saturating_duration_since(Instant::now())) {
                Ok(Msg::Woke) | Err(_) => break,
                Ok(Msg::Wake) => {}
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
        // Its state DB first, before any request: one that can't be used at all (from before
        // 0.7, damaged) is set aside and its folders set up again, told once; one only the user
        // can set right (a newer version's) is shown and waited out, asking nothing, the daemon
        // and the other accounts going on.
        let set = loop {
            // The config's folders, as read when the account was made: they all are in it.
            match ready_account(&self.db, &self.identity, &config.roots) {
                Ok(set) => break set,
                Err(e) if e.class() != ErrorClass::NeedsUser => {
                    return Err(RunnerError::Folder(e).into());
                }
                Err(e) => {
                    tracing::error!("{e}: {} waits for it to be set right", self.identity);
                    self.show_waiting(State::Failing, e.to_string());
                    if self.wait(NEEDS_USER_AGAIN, stop, requests, &mut msgs) {
                        return Ok(());
                    }
                }
            }
        };
        // Ready: the answers may read it now.
        self.ready.store(true, Ordering::SeqCst);
        if let Some(set) = set {
            self.note(vec![words::set_aside(set.why)]);
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
        let AccountParts {
            drive,
            notifier,
            mut notifications,
        } = AccountParts::open(&self.identity, remote, &self.db, tx)?;
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
                match self.start(&drive, config, &root, shown.paused()) {
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
                        adding.retain(|a| a.folder != folder);
                        let mut shown = lock(&self.shown);
                        for r in shown.runners.iter_mut().filter(|r| r.root.folder == folder) {
                            r.stop_for_good();
                        }
                        changed = true;
                    }
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
            let ending = self.stopping(stop) || result.is_err();
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
                shown.runners.iter_mut().for_each(|r| r.ended(ending));
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
                let config = Self::config(paths);
                let paused = lock(&self.shown).paused();
                let start = |root: &Root| {
                    let config = config.as_ref()?;
                    Some(self.start(&drive, config, root, paused))
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
                    return result;
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

    /// The config as it is now; `None` (logged) while it can't be read (being edited, say):
    /// what needs it waits.
    fn config(paths: &Paths) -> Option<Config> {
        match Config::load(&paths.config_file()) {
            Ok(c) => Some(c.unwrap_or_default()),
            Err(e) => {
                tracing::warn!("can't read the config: {e}");
                None
            }
        }
    }

    /// Starts syncing the folder `root` on the account's `drive`, with `config`'s settings,
    /// paused when `paused`.
    fn start(
        &self,
        drive: &Arc<Drive>,
        config: &Config,
        root: &Root,
        paused: Option<Pause>,
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
        let role = listed.map_or(root.role, |r| r.role);
        cfg.role = role;
        cfg.paused = paused;
        cfg.changed = Arc::clone(&self.changed);
        let done = Arc::new(AtomicBool::new(false));
        let (ending, tell) = (Arc::clone(&done), self.requests.clone());
        let (handle, thread) = Runner::spawn_with(cfg, Arc::clone(drive), move || {
            ending.store(true, Ordering::SeqCst);
            let _ = tell.send(Msg::Wake);
        })?;
        Ok(Running {
            root: Root {
                role,
                ..root.clone()
            },
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
                    if self.wait(wait_for, stop, rx, early) {
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
                Ok(Msg::Wake | Msg::Woke) => {}
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
        let mut backoff = Backoff::new();
        loop {
            match self.try_connect(client, stop, rx, early)? {
                Ok(r) => return Some(Arc::new(r)),
                Err(e) => {
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
                    if self.wait(wait_for, stop, rx, early) {
                        return None;
                    }
                }
            }
        }
    }
}
