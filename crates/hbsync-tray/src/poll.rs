//! Watching the daemon from a thread of its own: its status every second (sooner when woken),
//! the recently synced list when a sync ended, and starting the daemon when it isn't running.
//! The tray keeps it running while a folder is set up.

use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use hbsync_app::Paths;
use hbsync_app::control;
use hbsync_types::activity::Synced;

use crate::model::{Daemon, RECENT_SHOWN};
use crate::{Event, Sender};

const EVERY: Duration = Duration::from_secs(1);
/// How often to look whether this program was replaced on disk.
const LOOK_FOR_UPDATE: Duration = Duration::from_secs(60);
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
}

impl Watcher {
    fn run(mut self, paths: &Paths, send: &Sender, woken: &mpsc::Receiver<()>) {
        loop {
            if self.replaced.is_none()
                && self
                    .replaced_looked
                    .is_none_or(|at| at.elapsed() >= LOOK_FOR_UPDATE)
            {
                self.replaced_looked = Some(Instant::now());
                self.replaced = crate::os::replacement();
            }
            let daemon = self.look(paths, send);
            send(Event::Daemon(
                daemon,
                self.recent.clone(),
                self.replaced.clone(),
            ));
            match woken.recv_timeout(EVERY) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn look(&mut self, paths: &Paths, send: &Sender) -> Daemon {
        let status = match control::status(paths) {
            Ok(Some(status)) => status,
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
            Err(e) => return Daemon::Starting(Some(e.to_string())),
        };
        self.start_failed = None;
        // Updated beside a daemon still running the old version. Not when this tray is the old
        // one (replaced on disk): the daemon may be the new one, and this tray goes first.
        if status.version != hbsync_app::VERSION && !self.restarted && self.replaced.is_none() {
            tracing::info!(
                "the daemon runs {:?}, this is {}: restarting it",
                status.version,
                hbsync_app::VERSION
            );
            self.restarted = true;
            if let Err(e) = control::stop(paths) {
                tracing::warn!("stopping the daemon: {e}");
            }
            return Daemon::Starting(None);
        }
        if self.read_at != Some(status.synced_added)
            && let Ok(Some(list)) = control::recent(paths, RECENT_SHOWN)
        {
            self.recent = list;
            self.read_at = Some(status.synced_added);
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
