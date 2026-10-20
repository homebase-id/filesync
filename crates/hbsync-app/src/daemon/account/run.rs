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
use hbsync_homebase::notify::Notifier;
use hbsync_homebase::{Client, HomebaseError};
use hbsync_types::unix_now;

use super::feed::{Notifications, Puller};
use super::runners::{Life, Running, shown};
use super::{Account, Msg, Phase, Request};
use crate::daemon::DaemonError;
use crate::daemon::keeper::Keeper;
use crate::protocol::{AccountStatus, Hints, State, Until};
use crate::runner::{Backoff, Command, Runner, RunnerConfig, RunnerError, pause_end};
use crate::sync::{lock, wait_for};
use crate::{Config, CredentialError, CredentialStore, Location, Paths};

/// How often the purge of Recently deleted is looked at (it runs once a day).
const PURGE_LOOK: Duration = Duration::from_secs(600);
/// While the config can't be read, folders to start wait this long before it is read again.
const CONFIG_AGAIN: Duration = Duration::from_secs(1);

impl Account {
    fn waiting(&self, to: State, why: String) {
        if let Phase::Waiting { state, error, .. } = &mut *lock(&self.phase) {
            *state = to;
            *error = Some(why);
        }
        self.changed.raise();
    }

    /// Changes its status.
    fn set_status(&self, f: impl FnOnce(&mut AccountStatus)) {
        f(&mut lock(&self.status));
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
        &self,
        paths: &Paths,
        config: &Config,
        requests: &Receiver<Msg>,
        stop: &AtomicBool,
    ) -> Result<(), DaemonError> {
        // What came while waiting for the credentials and the identity.
        let mut msgs = Vec::new();
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
        let hints = self.requests.clone();
        std::thread::Builder::new()
            .name("hbsync-hints".into())
            .spawn(move || {
                for event in events {
                    if hints.send(Msg::Hint(event)).is_err() {
                        return;
                    }
                }
            })
            .map_err(DaemonError::Ipc)?;
        let notifier = match Notifier::spawn(client, remote.drive(), tx) {
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
        let drive = Drive::open(&self.db, &self.identity, remote)
            .map_err(|e| DaemonError::Runner(e.into()))?;
        // Tokens are sealed under the login's shared secret: another login's don't open.
        let login = format!(
            "{:016x}",
            hbsync_platform::fnv::fnv1a64(creds.shared_secret.as_bytes())
        );
        if drive
            .tokens_sealed_by(&login)
            .map_err(|e| DaemonError::Runner(e.into()))?
        {
            tracing::info!("a new login: the files' tokens are read again as they're written");
        }
        let mut notifications =
            Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now());
        let mut keeper = Keeper::spawn(Arc::clone(&drive));
        let mut runners: Vec<Running> = Vec::new();
        // A runner that couldn't start: the account stops its runners and ends with it, the
        // daemon with it (restarted whole). One that ended with an error is started again after a
        // pause, its folder showing why meanwhile; the others sync on.
        let mut result: Result<(), DaemonError> = Ok(());
        {
            // Under the lock: no pause or folder gets lost between the two phases.
            let mut phase = lock(&self.phase);
            let folders = match &*phase {
                Phase::Waiting { folders, .. } => folders.clone(),
                Phase::Running(_) => Vec::new(),
            };
            for folder in folders {
                match self.start(&drive, config, &folder) {
                    Ok(r) => runners.push(r),
                    Err(e) => {
                        result = Err(e.into());
                        break;
                    }
                }
            }
            *phase = Phase::Running(shown(&runners));
            self.changed.raise();
        }

        // Folders to start once no runner of theirs is left.
        let mut adding: Vec<PathBuf> = Vec::new();
        let keep_days = config.keep_deleted_days.days();
        let mut next_purge = Instant::now();
        loop {
            if self.stopping(stop) || result.is_err() {
                for r in &runners {
                    r.handle.send(Command::Stop);
                }
            }
            if pause_ran_out(&mut lock(&self.paused), unix_now()) {
                tracing::info!("the pause is over");
                self.changed.raise();
            }
            let paused = lock(&self.paused).is_some();
            let mut changed = false;
            for msg in msgs.drain(..) {
                match msg {
                    Msg::Hint(event) => {
                        let Some(hints) = notifications.event(event, Instant::now(), paused) else {
                            continue;
                        };
                        // Connected again: what waits for the drive goes now.
                        if hints == Hints::Connected {
                            for r in &runners {
                                r.handle.send(Command::Online);
                            }
                        }
                        self.set_status(|s| s.hints = hints);
                    }
                    Msg::Woke => {
                        if let Some(n) = &notifier {
                            n.reconnect();
                        }
                        for r in &runners {
                            r.handle.send(Command::Woke);
                        }
                    }
                    Msg::Request(Request::Deleted(control)) => keeper.request(control),
                    // Started below, once no runner of the folder is left (one removed a moment
                    // ago may still be finishing an operation).
                    Msg::Request(Request::AddFolder(folder)) => {
                        let synced = runners.iter().any(|r| r.path == folder && r.shows());
                        if !synced && !adding.contains(&folder) {
                            adding.push(folder);
                        }
                    }
                    Msg::Request(Request::RemoveFolder(folder)) => {
                        adding.retain(|f| *f != folder);
                        for r in runners.iter_mut().filter(|r| r.path == folder) {
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
                keeper.purge(keep_days);
            }
            // A runner that ended takes its folder out, unless it failed: then it starts again
            // after a pause. The account ends with the last.
            let before = runners.len();
            let ending = self.stopping(stop) || result.is_err();
            runners.iter_mut().for_each(|r| r.ended(ending));
            if ending {
                adding.clear();
            }
            runners.retain(|r| !matches!(r.life, Life::Gone));
            let now = Instant::now();
            let due = runners.iter().any(|r| r.due(now));
            let ready: Vec<PathBuf> = adding
                .extract_if(.., |f| !runners.iter().any(|r| r.path == *f))
                .collect();
            // The config as it is now (its settings may have changed meanwhile); while it can't
            // be read, restarts wait a while and new folders wait a moment.
            let config = (due || !ready.is_empty())
                .then(|| Self::config(paths))
                .flatten();
            // While the config can't be read: when to read it again for folders to start.
            let mut config_again = None;
            for r in runners.iter_mut().filter(|r| r.due(now)) {
                match config.as_ref().map(|c| self.start(&drive, c, &r.path)) {
                    Some(Ok(new)) => {
                        *r = Running {
                            backoff: r.backoff,
                            ..new
                        };
                        changed = true;
                    }
                    Some(Err(e)) => result = Err(e.into()),
                    None => r.life = Life::Waiting(now + r.backoff.after(State::Failing)),
                }
            }
            for folder in ready {
                let Some(config) = &config else {
                    adding.push(folder);
                    config_again = Some(now + CONFIG_AGAIN);
                    continue;
                };
                match self.start(&drive, config, &folder) {
                    Ok(r) => runners.push(r),
                    Err(e) => result = Err(e.into()),
                }
                changed = true;
            }
            if changed || runners.len() != before {
                *lock(&self.phase) = Phase::Running(shown(&runners));
                self.changed.raise();
            }
            if runners.is_empty() {
                notifications.finish();
                keeper.finish();
                return result;
            }

            // Until a message, or the next thing that is due.
            let now = Instant::now();
            let pause = *lock(&self.paused);
            let paused = pause.is_some();
            let pause_end = pause.and_then(|until| pause_end(until, now, unix_now()));
            let restarts = runners.iter().filter_map(|r| match r.life {
                Life::Waiting(at) => Some(at),
                _ => None,
            });
            let deadlines = [
                notifications.deadline(now, paused),
                (!paused).then_some(next_purge),
                pause_end,
                config_again,
            ];
            let wait = wait_for(now, deadlines.into_iter().flatten().chain(restarts));
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

    /// Starts syncing `folder` (as `config` lists it) on the account's `drive`.
    fn start(
        &self,
        drive: &Arc<Drive>,
        config: &Config,
        folder: &Path,
    ) -> Result<Running, RunnerError> {
        let mut cfg = RunnerConfig::new(folder, &self.db, &self.identity, &config.device_name());
        cfg.mass_delete = config.mass_delete;
        cfg.role = config
            .roots
            .iter()
            .find(|r| r.folder == folder)
            .and_then(|r| r.role);
        cfg.paused = *lock(&self.paused);
        cfg.changed = Arc::clone(&self.changed);
        let done = Arc::new(AtomicBool::new(false));
        let (ending, tell) = (Arc::clone(&done), self.requests.clone());
        let (handle, thread) = Runner::spawn_with(cfg, Arc::clone(drive), move || {
            ending.store(true, Ordering::SeqCst);
            let _ = tell.send(Msg::Wake);
        })?;
        Ok(Running {
            path: folder.to_path_buf(),
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
                    let wait_for = backoff.after(State::Failing);
                    tracing::warn!("can't read the keychain ({why}); trying again in {wait_for:?}");
                    self.waiting(State::Failing, format!("can't read the keychain: {why}"));
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
            match HomebaseRemote::connect(client.clone()) {
                Ok(r) => return Some(Arc::new(r)),
                Err(e) => {
                    let state = match &e {
                        HomebaseError::Denied(_) => State::Denied,
                        HomebaseError::Network(_) => State::Offline,
                        _ => State::Failing,
                    };
                    let wait_for = backoff.after(state);
                    tracing::warn!(
                        "can't connect to {} ({e}); trying again in {wait_for:?}",
                        self.identity
                    );
                    self.waiting(state, e.to_string());
                    if self.wait(wait_for, stop, rx, early) {
                        return None;
                    }
                }
            }
        }
    }
}

/// Whether the account's timed pause (`paused`) ran out at `unix` (seconds): then it is
/// over, as each folder's is ([`crate::runner`]).
fn pause_ran_out(paused: &mut Option<Until>, unix: u64) -> bool {
    if matches!(*paused, Some(Some(until)) if unix >= until) {
        *paused = None;
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The account's timed pause runs out as each folder's does; one without an end doesn't.
    #[test]
    fn the_accounts_timed_pause_runs_out() {
        let mut paused: Option<Until> = Some(Some(100));
        assert!(!pause_ran_out(&mut paused, 99));
        assert_eq!(paused, Some(Some(100)));
        assert!(pause_ran_out(&mut paused, 100));
        assert_eq!(paused, None);
        let mut for_good: Option<Until> = Some(None);
        assert!(!pause_ran_out(&mut for_good, u64::MAX));
        assert_eq!(for_good, Some(None));
        assert!(!pause_ran_out(&mut None, 100));
    }
}
