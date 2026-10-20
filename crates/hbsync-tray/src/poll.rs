//! Watching the daemon from a thread of its own: its status as the daemon sends it when it
//! changes (a connection kept open, read on a thread of its own: [`control::watch`]), the
//! recently synced list when a sync ended, and starting the daemon when it isn't running. It
//! asks once more when woken (after an action); once a minute it shows what it was sent again
//! (the menu's "2 min ago") and looks whether this program was replaced on disk. While the daemon
//! isn't running it looks every second (it is starting), or every ten (nothing set up yet). The
//! tray keeps it running while a folder is set up.

use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use hbsync_app::Paths;
use hbsync_app::control;
use hbsync_app::protocol::{DaemonStatus, RecentItem};

use crate::model::{Daemon, RECENT_SHOWN};
use crate::{Event, Sender};

/// While the daemon is watched: how often to look again anyway (the menu's relative times,
/// whether this program was replaced on disk).
const LOOK_FOR_UPDATE: Duration = Duration::from_secs(60);
/// While the daemon isn't watched: how often to look (it is starting, or a watch just ended).
const STARTING: Duration = Duration::from_secs(1);
/// While nothing is set up (`hbsync setup` in a terminal, say), or the daemon runs but can't be
/// watched: how often to look.
const UNWATCHED: Duration = Duration::from_secs(10);
/// After a failed start, the next try waits this long.
const START_AGAIN: Duration = Duration::from_secs(30);

/// What wakes the watcher's thread.
pub(crate) enum Poke {
    /// Look now (after an action).
    Look,
    /// Show what was seen last again (the minute's look while watching: relative times).
    Again,
    /// The daemon sent its status.
    Status(Box<DaemonStatus>),
    /// The watched daemon's connection ended (it stopped).
    Gone,
}

/// Starts watching; [`Poke::Look`] sent on the returned channel has it look at once.
pub(crate) fn spawn(paths: Paths, send: Sender) -> std::io::Result<mpsc::Sender<Poke>> {
    let (wake, woken) = mpsc::channel();
    let me = wake.clone();
    std::thread::Builder::new()
        .name("hbsync-tray-poll".into())
        .spawn(move || Watcher::default().run(&paths, &send, &woken, &me))?;
    Ok(wake)
}

#[derive(Default)]
struct Watcher {
    recent: Vec<RecentItem>,
    /// The daemon's `synced_added` when the recent list was read.
    read_at: Option<u64>,
    next_start: Option<Instant>,
    start_failed: Option<String>,
    /// Restarted a daemon of another version once (after an update).
    restarted: bool,
    /// The program that replaced this one on disk ([`crate::os::replacement`]): once found,
    /// it stays; until then, looked for once a minute.
    replaced: Option<PathBuf>,
    replaced_looked: Option<Instant>,
    /// The daemon's status comes as it changes ([`Watcher::watch`]).
    watching: bool,
    /// A watch that just ended isn't started again before then.
    watch_after: Option<Instant>,
    /// The status the daemon sent last.
    pushed: Option<DaemonStatus>,
}

impl Watcher {
    fn run(
        mut self,
        paths: &Paths,
        send: &Sender,
        woken: &mpsc::Receiver<Poke>,
        me: &mpsc::Sender<Poke>,
    ) {
        let mut poke = Poke::Look;
        loop {
            if self.replaced.is_none()
                && self
                    .replaced_looked
                    .is_none_or(|at| at.elapsed() >= LOOK_FOR_UPDATE)
            {
                self.replaced_looked = Some(Instant::now());
                self.replaced = crate::os::replacement();
            }
            let daemon = match poke {
                Poke::Status(status) => {
                    self.pushed = Some((*status).clone());
                    self.show(paths, Some(*status))
                }
                Poke::Again if self.watching && self.pushed.is_some() => {
                    let status = self.pushed.clone();
                    self.show(paths, status)
                }
                Poke::Gone => {
                    (self.watching, self.pushed) = (false, None);
                    // Not at once again: a daemon refusing the watch would be asked in a loop.
                    self.watch_after = Some(Instant::now() + STARTING);
                    self.look(paths, send)
                }
                Poke::Look | Poke::Again => self.look(paths, send),
            };
            if !self.watching
                && matches!(daemon, Daemon::Running(_))
                && self.watch_after.is_none_or(|at| Instant::now() >= at)
            {
                self.watch(paths, me);
            }
            let watch_soon = self.watch_after.is_some_and(|at| Instant::now() < at);
            let wait = match &daemon {
                _ if self.watching => LOOK_FOR_UPDATE,
                // A watch just ended: watched again in a moment.
                Daemon::Running(_) if watch_soon => STARTING,
                // Running, but it can't be watched: looked at now and then, not every second.
                Daemon::NotSetUp | Daemon::Running(_) => UNWATCHED,
                Daemon::Starting(_) => STARTING,
            };
            send(Event::Daemon(
                daemon,
                self.recent.clone(),
                self.replaced.clone(),
            ));
            poke = match woken.recv_timeout(wait) {
                Ok(poke) => poke,
                Err(RecvTimeoutError::Timeout) => Poke::Again,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            // Statuses that came while this thread was busy: only the newest counts; an end of
            // the watch is never skipped.
            for next in woken.try_iter() {
                if !matches!(poke, Poke::Gone) {
                    poke = next;
                }
            }
        }
    }

    /// Has the daemon send its status as it changes, read on a thread of its own that pokes
    /// `me` with each, and with [`Poke::Gone`] when the connection ends.
    fn watch(&mut self, paths: &Paths, me: &mpsc::Sender<Poke>) {
        let mut watch = match control::watch(paths) {
            Ok(Some(watch)) => watch,
            Ok(None) => return,
            Err(e) => {
                tracing::debug!("can't watch the daemon: {e}");
                return;
            }
        };
        let me = me.clone();
        let reading = std::thread::Builder::new()
            .name("hbsync-tray-watch".into())
            .spawn(move || {
                while let Ok(Some(status)) = watch.status() {
                    if me.send(Poke::Status(Box::new(status))).is_err() {
                        return;
                    }
                }
                let _ = me.send(Poke::Gone);
            });
        self.watching = reading.is_ok();
    }

    /// Looks at the daemon once.
    fn look(&mut self, paths: &Paths, send: &Sender) -> Daemon {
        let status = match control::status(paths) {
            Ok(Some(status)) => Some(status),
            // Not running: started if a folder is set up.
            Ok(None) => {
                return match control::configured(paths) {
                    Ok(Some(_)) => self.start(paths, send),
                    Ok(None) => {
                        self.recent.clear();
                        self.read_at = None;
                        Daemon::NotSetUp
                    }
                    Err(e) => Daemon::Starting(Some(e.to_string())),
                };
            }
            // One too old to tell its version.
            Err(e) if e.is::<control::OlderDaemon>() => None,
            Err(e) => return Daemon::Starting(Some(e.to_string())),
        };
        self.show(paths, status)
    }

    /// What the daemon's `status` shows (`None`: it is too old to tell); a daemon of another
    /// version is restarted once.
    fn show(&mut self, paths: &Paths, status: Option<DaemonStatus>) -> Daemon {
        self.start_failed = None;
        // Updated beside a daemon still running the old version. Not when this tray is the old
        // one (replaced on disk): the daemon may be the new one, and this tray goes first.
        let version = status.as_ref().map(|s| s.version.as_str());
        if version != Some(hbsync_app::VERSION) && !self.restarted && self.replaced.is_none() {
            tracing::info!(
                "the daemon runs {:?}, this is {}: restarting it",
                version,
                hbsync_app::VERSION
            );
            self.restarted = true;
            if let Err(e) = control::stop(paths) {
                tracing::warn!("stopping the daemon: {e}");
            }
            return Daemon::Starting(None);
        }
        let Some(status) = status else {
            return Daemon::Starting(Some(control::OlderDaemon.to_string()));
        };
        let synced_added = status.folders.iter().map(|f| f.synced_added).sum();
        if self.read_at != Some(synced_added)
            && let Ok(Some(list)) = control::recent(paths, RECENT_SHOWN)
        {
            self.recent = list;
            self.read_at = Some(synced_added);
        }
        Daemon::Running(Box::new(status))
    }

    fn start(&mut self, paths: &Paths, send: &Sender) -> Daemon {
        if self.next_start.is_some_and(|at| Instant::now() < at) {
            return Daemon::Starting(self.start_failed.clone());
        }
        send(Event::Daemon(
            Daemon::Starting(None),
            self.recent.clone(),
            self.replaced.clone(),
        ));
        tracing::info!("starting the daemon");
        match control::start(paths) {
            Ok(()) => {
                self.next_start = None;
                self.start_failed = None;
                Daemon::Starting(None)
            }
            Err(e) => {
                tracing::warn!("the daemon didn't start: {e}");
                self.next_start = Some(Instant::now() + START_AGAIN);
                self.start_failed = Some(e.to_string());
                Daemon::Starting(self.start_failed.clone())
            }
        }
    }
}
