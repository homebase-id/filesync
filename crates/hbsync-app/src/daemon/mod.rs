//! The daemon: one per user, syncing the configured folders until it is stopped.
//!
//! It takes the one-daemon lock, reads the config, answers on the control socket
//! ([`answer`]), and runs each account ([`account`]: its credentials, connection, drive and
//! folders) until a stop request or signal. Its threads block until there is something to do:
//! nothing wakes it on a timer while nothing happens (but [`sleep`]'s poll off Linux). It holds
//! the pause, every account's, and ends a timed one: the one timer for it.

mod account;
mod answer;
mod keeper;
mod sleep;
mod watch;

pub use account::{AccountParts, Notifications, Puller, UNANNOUNCED_READ};

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use hbsync_homebase::HomebaseError;
use hbsync_remote::ErrorClass;
use hbsync_types::unix_now;

use crate::config::ConfigLock;
use crate::ipc;
use crate::lock::{InstanceLock, LockError};
use crate::protocol::Pause;
use crate::runner::{Command, RunnerError};
use crate::sync::{OnDrop, Wakeup, lock, wait_for};
use crate::{Config, ConfigError, CredentialError, Identity, Paths, Root};
use account::Account;

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// Nothing to run: `hbsync setup` hasn't been run (or was undone).
    #[error("{0}: run `hbsync setup` first")]
    NotSetUp(String),
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error(transparent)]
    Homebase(#[from] HomebaseError),
    #[error(transparent)]
    Runner(#[from] RunnerError),
    #[error("the control socket: {0}")]
    Ipc(std::io::Error),
}

impl DaemonError {
    /// What it means ([`ErrorClass`]): a daemon that ends with one only the user can fix
    /// (`NeedsUser`) isn't started again by the service manager.
    pub fn class(&self) -> ErrorClass {
        match self {
            DaemonError::Lock(LockError::Io(e)) | DaemonError::Config(ConfigError::Io(_, e)) => {
                ErrorClass::of_io(e)
            }
            DaemonError::Credentials(CredentialError::Keychain(_)) => ErrorClass::LocalFault,
            // A credentials file that can't be read now; one that doesn't parse is the user's.
            DaemonError::Credentials(CredentialError::File(HomebaseError::Io(e))) => {
                ErrorClass::of_io(e)
            }
            DaemonError::NotSetUp(_)
            | DaemonError::Lock(LockError::Held { .. })
            | DaemonError::Config(_)
            | DaemonError::Credentials(_) => ErrorClass::NeedsUser,
            DaemonError::Homebase(e) => e.class(),
            DaemonError::Runner(e) => e.class(),
            DaemonError::Ipc(e) => ErrorClass::of_io(e),
        }
    }
}

/// Stops the daemon from another thread (a signal handler's).
#[derive(Debug, Clone, Default)]
pub struct Stop {
    flag: Arc<AtomicBool>,
    wakeup: Arc<Wakeup>,
}

impl Stop {
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
        self.wakeup.raise();
    }

    fn stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

/// Runs the daemon until a stop request, or until `stop` (a signal). It ends when its last
/// account has; one stopped by what only the user can fix ends alone, the others sync on, and
/// one failing otherwise stops them all (the daemon is restarted whole).
pub fn run(paths: &Paths, stop: Stop) -> Result<(), DaemonError> {
    let (_lock, config, listener) = {
        let held = ConfigLock::take(&paths.config_file())?;
        claim(paths, &held)?
    };
    let daemon = Arc::new(Daemon {
        paths: paths.clone(),
        stop,
        // A pause that ended while the daemon wasn't running is over.
        pause: Mutex::new(config.pause(unix_now())),
        pausing: Mutex::default(),
        accounts: Mutex::default(),
        starting_over: AtomicUsize::new(0),
        changed: Arc::default(),
    });
    {
        let daemon = Arc::downgrade(&daemon);
        std::thread::Builder::new()
            .name("hbsync-sleep".into())
            .spawn(move || {
                sleep::watch(|| {
                    let Some(daemon) = daemon.upgrade() else {
                        return false;
                    };
                    tracing::info!("the machine woke up");
                    daemon.accounts().iter().for_each(|a| a.woke());
                    // A timed pause's end may have passed meanwhile (a wait doesn't count
                    // the sleep).
                    if lock(&daemon.pause).is_some() {
                        daemon.stop.wakeup.raise();
                    }
                    true
                })
            })
            .map_err(DaemonError::Ipc)?;
    }
    for root in &config.roots {
        daemon.add_folder(root)?;
        tracing::info!(
            "daemon {} started for {} at {}",
            crate::VERSION,
            root.identity,
            root.folder.display()
        );
    }
    let watchers = Arc::new(watch::Watchers::new(Arc::clone(&daemon.changed)));
    {
        let (weak, watchers) = (Arc::downgrade(&daemon), Arc::clone(&watchers));
        std::thread::Builder::new()
            .name("hbsync-status".into())
            .spawn(move || watch::push(weak, watchers))
            .map_err(DaemonError::Ipc)?;
    }
    {
        let daemon = Arc::clone(&daemon);
        let watch = move |stream| watchers.add(stream);
        std::thread::Builder::new()
            .name("hbsync-control".into())
            .spawn(move || ipc::serve(listener, move |r| answer::answer(r, &daemon), watch))
            .map_err(DaemonError::Ipc)?;
    }
    let mut result: Result<(), DaemonError> = Ok(());
    let (mut rung, mut stopping) = (0, false);
    loop {
        let (ended, left) = daemon.reap();
        // What only the user can fix ends that account alone: the others sync on, and the
        // daemon ends with the last. Any other failure stops them all: the daemon is started
        // again whole.
        if let Err(e) = ended {
            tracing::error!("an account stopped: {e}");
            let fault = e.class() != ErrorClass::NeedsUser;
            if fault {
                daemon.stop.stop();
            }
            // The exit tells a fault rather than what only the user can fix.
            let told_fault = matches!(&result, Err(f) if f.class() != ErrorClass::NeedsUser);
            if result.is_ok() || (fault && !told_fault) {
                result = Err(e);
            }
        }
        if daemon.stop.stopped() && !stopping {
            stopping = true;
            daemon.stop_all();
        }
        if left == 0 && daemon.starting_over.load(Ordering::SeqCst) == 0 {
            break;
        }
        let now = Instant::now();
        // Stopping, the pause no longer changes.
        let pause_end = match stopping {
            true => None,
            false => daemon.end_pause_when_due(now),
        };
        rung = daemon.stop.wakeup.wait(rung, wait_for(now, pause_end));
    }
    tracing::info!("daemon stopped");
    result
}

/// The daemon's lock, its config and its socket, taken holding the config's lock (`held`): a
/// program that finds no daemon running changes the config holding it
/// (`folders::add_synced`), and the daemon reads that change; one that finds it running finds
/// its socket bound too. With nothing to sync, its lock goes first, before `held`: that program
/// finds none running.
fn claim(
    paths: &Paths,
    held: &ConfigLock,
) -> Result<(InstanceLock, Config, ipc::Listener), DaemonError> {
    let lock = InstanceLock::acquire(paths)?;
    let config = match held.load()? {
        Some(config) if !config.roots.is_empty() => config,
        other => {
            drop(lock);
            let why = if other.is_some() {
                "no folder to sync"
            } else {
                "no config"
            };
            return Err(DaemonError::NotSetUp(why.into()));
        }
    };
    let listener = ipc::listen(paths).map_err(DaemonError::Ipc)?;
    Ok((lock, config, listener))
}

/// The daemon's accounts, each on a thread of its own; folders and accounts come and go while
/// it runs.
pub(crate) struct Daemon {
    paths: Paths,
    /// Set to stop; its wakeup is raised when an account ends too, or the pause changes.
    stop: Stop,
    /// Every account's pause (kept in the config too); an account starts with it. Held only
    /// to read or set it, never while taking another lock.
    pause: Mutex<Option<Pause>>,
    /// One change of the pause at a time, in the config, here and in every account: the
    /// accounts are told the changes in their order.
    pausing: Mutex<()>,
    accounts: Mutex<Vec<(Arc<Account>, AccountThread)>>,
    /// Accounts starting over ([`Daemon::start_over`]): the daemon doesn't end meanwhile.
    starting_over: AtomicUsize,
    /// Raised when the status may have changed ([`watch`]).
    changed: Arc<Wakeup>,
}

type AccountThread = JoinHandle<Result<(), DaemonError>>;

impl Daemon {
    /// The accounts running.
    fn accounts(&self) -> Vec<Arc<Account>> {
        lock(&self.accounts)
            .iter()
            .filter(|(_, thread)| !thread.is_finished())
            .map(|(a, _)| Arc::clone(a))
            .collect()
    }

    /// Syncs the folder `root` (set up, in the config): in its identity's account, or in a new
    /// one. Under one lock: two requests for a new identity start one account.
    fn add_folder(&self, root: &Root) -> Result<(), DaemonError> {
        let identity = &root.identity;
        let mut accounts = lock(&self.accounts);
        if let Some((account, _)) = accounts
            .iter()
            .find(|(a, thread)| a.identity == *identity && !thread.is_finished())
        {
            account.add_folder(root);
            return Ok(());
        }
        let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
        // Read holding `accounts`: a pause set meanwhile tells this account too
        // ([`Daemon::set_pause`] sets it before it looks at the accounts).
        let pause = *lock(&self.pause);
        let (account, requests) = Account::new(&self.paths, root, pause, Arc::clone(&self.changed));
        let thread = {
            let (account, paths, stop) =
                (Arc::clone(&account), self.paths.clone(), self.stop.clone());
            std::thread::Builder::new()
                .name(format!("hbsync-account-{identity}"))
                .spawn(move || {
                    // Told also when it panics.
                    let _ended = OnDrop(Some(|| {
                        account.done.store(true, Ordering::SeqCst);
                        stop.wakeup.raise();
                    }));
                    account.run(&paths, &config, &requests, &stop.flag)
                })
                .map_err(DaemonError::Ipc)?
        };
        accounts.push((account, thread));
        self.changed.raise();
        Ok(())
    }

    /// Pauses (`Some`) or resumes every account, also those started later; kept in the config
    /// first, so it survives a restart. Refused when the config can't be written.
    fn set_pause(&self, pause: Option<Pause>) -> Result<(), ConfigError> {
        {
            let _one = lock(&self.pausing);
            Config::update(&self.paths.config_file(), |c| c.set_pause(pause))?;
            *lock(&self.pause) = pause;
            for a in self.accounts() {
                a.pause(pause);
            }
        }
        // Its end, if timed, is looked at from now; rung once the change is done, so a look
        // that found it being changed looks again.
        self.stop.wakeup.raise();
        Ok(())
    }

    /// Ends a timed pause that has run out at `now`: every account syncs again (the config
    /// keeps it, read as over). Returns when the pause in effect runs out (`None`: not paused,
    /// or until resumed). While the pause is being changed, it isn't looked at (the main loop
    /// never waits for the config's lock): the change rings once done.
    fn end_pause_when_due(&self, now: Instant) -> Option<Instant> {
        let _one = match self.pausing.try_lock() {
            Ok(one) => one,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        let unix = unix_now();
        let ended = lock(&self.pause).take_if(|p| p.ran_out(unix)).is_some();
        if ended {
            tracing::info!("the pause is over");
            for a in self.accounts() {
                a.pause_ended();
            }
        }
        lock(&self.pause).and_then(|p| p.end_at(now, unix))
    }

    /// Starts the account `identity` over (its credentials changed): it stops its folders
    /// between operations, then syncs them again with the new credentials.
    fn start_over(&self, identity: &Identity) -> Result<(), DaemonError> {
        let found = {
            let mut accounts = lock(&self.accounts);
            let at = accounts.iter().position(|(a, _)| a.identity == *identity);
            at.map(|at| accounts.remove(at))
        };
        let Some((account, thread)) = found else {
            return Ok(());
        };
        self.starting_over.fetch_add(1, Ordering::SeqCst);
        // Its folders as they are now: those of an ending account go as their runners stop.
        let folders = account.folders();
        account.end();
        let ended = thread
            .join()
            .unwrap_or(Err(DaemonError::Runner(RunnerError::Panicked)));
        // Its DB is the new account's to open as it starts, which may set it aside.
        account.close_reader();
        let again = folders.iter().try_for_each(|root| self.add_folder(root));
        self.starting_over.fetch_sub(1, Ordering::SeqCst);
        self.stop.wakeup.raise();
        ended.and(again)
    }

    /// Stops every account (the stop flag set): their runners stop between operations.
    fn stop_all(&self) {
        for a in self.accounts() {
            // One not running yet stops as it is woken.
            let _ = a.send_all(&Command::Stop);
            a.wake();
        }
    }

    /// Joins the accounts that ended (or are ending: their thread is about to): the first error
    /// among them, and how many still run.
    fn reap(&self) -> (Result<(), DaemonError>, usize) {
        let mut accounts = lock(&self.accounts);
        let ended = |(a, t): &(Arc<Account>, AccountThread)| {
            t.is_finished() || a.done.load(Ordering::SeqCst)
        };
        let before = accounts.len();
        let result = join_finished(&mut accounts, ended, |(_, t)| t);
        if accounts.len() != before {
            self.changed.raise();
        }
        (result, accounts.len())
    }
}

/// Joins the threads of `items` that ended (`finished` tells, `thread` takes an item's out)
/// and keeps the rest: the first error among them.
pub(crate) fn join_finished<T, E: From<RunnerError>>(
    items: &mut Vec<T>,
    finished: impl Fn(&T) -> bool,
    thread: impl Fn(T) -> JoinHandle<Result<(), E>>,
) -> Result<(), E> {
    let (ended, running): (Vec<_>, Vec<_>) = std::mem::take(items).into_iter().partition(finished);
    *items = running;
    let mut result = Ok(());
    for item in ended {
        let ended = thread(item)
            .join()
            .unwrap_or_else(|_| Err(RunnerError::Panicked.into()));
        if result.is_ok() {
            result = ended;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hbsync_engine::root::RootError;
    use hbsync_state::StateError;

    use super::*;

    /// What only the user can fix ends the daemon as no failure: the service manager doesn't
    /// start it again every few seconds. A fault of its own does.
    #[test]
    fn what_only_the_user_can_fix_isnt_a_failure() {
        let folder = |e| DaemonError::Runner(RunnerError::Folder(RootError::State(e)));
        let too_new = folder(StateError::TooNew {
            found: 99,
            supported: 1,
        });
        assert_eq!(too_new.class(), ErrorClass::NeedsUser);
        let corrupt = folder(StateError::Corrupt("x".into()));
        assert_eq!(corrupt.class(), ErrorClass::NeedsUser);
        assert_eq!(
            DaemonError::NotSetUp("x".into()).class(),
            ErrorClass::NeedsUser
        );
        assert_eq!(
            DaemonError::Runner(RunnerError::Panicked).class(),
            ErrorClass::Bug
        );
    }

    /// The daemon's pause: kept in the config as set; a timed one ends once its time has come
    /// (its end the one deadline until then), one until resumed never; while the pause is being
    /// changed, its end isn't looked at.
    #[test]
    fn the_daemon_ends_a_timed_pause() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        std::fs::create_dir_all(&paths.config)?;
        let daemon = Daemon {
            paths: paths.clone(),
            stop: Stop::default(),
            pause: Mutex::default(),
            pausing: Mutex::default(),
            accounts: Mutex::default(),
            starting_over: AtomicUsize::new(0),
            changed: Arc::default(),
        };
        let (now, unix) = (Instant::now(), unix_now());
        let saved = || Config::load(&paths.config_file()).map(|c| c.and_then(|c| c.pause(unix)));

        daemon.set_pause(Some(Pause::ending_at(unix + 60)))?;
        assert_eq!(saved()?, Some(Pause::ending_at(unix + 60)));
        let end = daemon.end_pause_when_due(now).ok_or("no deadline")?;
        assert!(end > now + Duration::from_secs(58), "{:?}", end - now);
        assert!(lock(&daemon.pause).is_some(), "not over yet");

        // Being changed: not looked at.
        daemon.set_pause(Some(Pause::ending_at(unix)))?;
        let changing = lock(&daemon.pausing);
        assert_eq!(daemon.end_pause_when_due(now), None);
        assert!(lock(&daemon.pause).is_some());
        drop(changing);
        assert_eq!(daemon.end_pause_when_due(now), None);
        assert_eq!(*lock(&daemon.pause), None, "ran out: over");

        daemon.set_pause(Some(Pause::UNTIL_RESUMED))?;
        assert_eq!(daemon.end_pause_when_due(now), None);
        assert_eq!(*lock(&daemon.pause), Some(Pause::UNTIL_RESUMED));
        daemon.set_pause(None)?;
        assert_eq!(saved()?, None);
        Ok(())
    }

    /// A daemon starting with nothing to sync lets its lock go while it still holds the
    /// config's: a program waiting for that lock finds no daemon running, and writes the
    /// config itself. One with a folder holds its lock and has bound its socket.
    #[test]
    fn a_daemon_with_nothing_to_sync_is_gone_before_the_config_is_free()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        let held = ConfigLock::take(&paths.config_file())?;
        assert!(matches!(
            claim(&paths, &held),
            Err(DaemonError::NotSetUp(_))
        ));
        assert!(!crate::lock::daemon_runs(&paths)?, "no config");
        held.update(|c| c.device_name = Some("pc".into()))?;
        assert!(matches!(
            claim(&paths, &held),
            Err(DaemonError::NotSetUp(_))
        ));
        assert!(!crate::lock::daemon_runs(&paths)?, "no folder");

        let root = crate::Root::main(&"a.example".into(), &home.path().join("Homebase"));
        held.update(|c| c.roots.push(root))?;
        let claimed = claim(&paths, &held)?;
        assert!(crate::lock::daemon_runs(&paths)?);
        drop(claimed);
        assert!(!crate::lock::daemon_runs(&paths)?);
        Ok(())
    }
}
