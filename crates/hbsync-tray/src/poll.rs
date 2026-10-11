//! Watching the daemon from a thread of its own: its status every second (sooner when woken),
//! the recently synced list when a sync ended, and starting the daemon when it isn't running.
//! The tray keeps it running while a folder is set up.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use hbsync_app::Paths;
use hbsync_app::control;
use hbsync_app::ipc::{Ask, IpcError, Reply};
use hbsync_types::activity::Synced;

use crate::model::{Daemon, RECENT_SHOWN};
use crate::{Event, Sender};

const EVERY: Duration = Duration::from_secs(1);
/// After a failed start, the next try waits this long.
const START_AGAIN: Duration = Duration::from_secs(30);

/// Starts watching; whatever is sent on the returned channel wakes the watcher at once.
pub fn spawn(paths: Paths, send: Sender) -> std::io::Result<mpsc::Sender<()>> {
    let (wake, woken) = mpsc::channel();
    std::thread::Builder::new()
        .name("hbsync-tray-poll".into())
        .spawn(move || Watcher::default().run(&paths, &send, &woken))?;
    Ok(wake)
}

#[derive(Default)]
struct Watcher {
    recent: Vec<Synced>,
    /// The last sync the recent list was read after.
    read_after: Option<u64>,
    next_start: Option<Instant>,
    start_failed: Option<String>,
    /// Restarted a daemon of another version once (after an update).
    restarted: bool,
}

impl Watcher {
    fn run(mut self, paths: &Paths, send: &Sender, woken: &mpsc::Receiver<()>) {
        loop {
            let daemon = self.look(paths, send);
            send(Event::Daemon(daemon, self.recent.clone()));
            match woken.recv_timeout(EVERY) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn look(&mut self, paths: &Paths, send: &Sender) -> Daemon {
        match control::configured(paths) {
            Ok(None) => {
                self.recent.clear();
                self.read_after = None;
                return Daemon::NotSetUp;
            }
            Ok(Some(_)) => {}
            Err(e) => return Daemon::Starting(Some(e.to_string())),
        }
        match control::ask(paths, Ask::Status) {
            Ok(Reply::Status(status)) => {
                self.start_failed = None;
                if status.version != env!("CARGO_PKG_VERSION") && !self.restarted {
                    // Updated beside a daemon still running the old version.
                    tracing::info!(
                        "the daemon runs {}, this is {}: restarting it",
                        status.version,
                        env!("CARGO_PKG_VERSION")
                    );
                    self.restarted = true;
                    if let Err(e) = control::stop(paths) {
                        tracing::warn!("stopping the daemon: {e}");
                    }
                    return Daemon::Starting(None);
                }
                let last = status.last_sync.as_ref().map(|l| l.at);
                if self.read_after != last || self.recent.is_empty() {
                    if let Ok(Reply::Recent(list)) = control::ask(
                        paths,
                        Ask::Recent {
                            limit: RECENT_SHOWN,
                        },
                    ) {
                        self.recent = list;
                    }
                    self.read_after = last;
                }
                Daemon::Running(status)
            }
            Ok(other) => Daemon::Starting(Some(format!("unexpected answer: {other:?}"))),
            Err(IpcError::NotRunning) => self.start(paths, send),
            Err(e) => Daemon::Starting(Some(e.to_string())),
        }
    }

    fn start(&mut self, paths: &Paths, send: &Sender) -> Daemon {
        if self.next_start.is_some_and(|at| Instant::now() < at) {
            return Daemon::Starting(self.start_failed.clone());
        }
        send(Event::Daemon(Daemon::Starting(None), self.recent.clone()));
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
