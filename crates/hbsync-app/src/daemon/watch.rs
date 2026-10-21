//! The status pushed to the clients watching it ([`Ask::Watch`], the tray): sent when it
//! changed, never on a timer while nothing happens; at most once a second while a folder syncs
//! (its progress changes without telling).
//!
//! [`Ask::Watch`]: crate::protocol::Ask::Watch

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use interprocess::local_socket::Stream;

use super::Daemon;
use super::answer::status;
use crate::ipc;
use crate::protocol::{DaemonStatus, Reply, State};
use crate::sync::{WAIT_AT_MOST, Wakeup, lock};

/// While a folder syncs, the status is sent this often at most.
const EVERY: Duration = Duration::from_secs(1);

/// The clients watching the status.
pub(super) struct Watchers {
    list: Mutex<Vec<Watcher>>,
    /// Raised when the status may have changed, and when a watcher comes.
    changed: Arc<Wakeup>,
}

/// A watching client. A thread of its own writes to it: one that is stuck (a named pipe can't
/// time out a write) holds back no other.
struct Watcher {
    out: Arc<Outbox>,
    /// What it was sent last.
    last: Option<DaemonStatus>,
}

/// The line a watcher's thread is to write next (the newest replaces one not written yet),
/// and whether the thread ended (the client is gone).
#[derive(Default)]
struct Outbox {
    next: Mutex<(Option<String>, bool)>,
    ready: Condvar,
}

impl Outbox {
    fn put(&self, line: String) {
        lock(&self.next).0 = Some(line);
        self.ready.notify_one();
    }

    fn gone(&self) -> bool {
        lock(&self.next).1
    }

    /// Writes each line put to `stream`, until that fails.
    fn write_to(&self, mut stream: Stream) {
        loop {
            let line = {
                let next = lock(&self.next);
                let mut next = self
                    .ready
                    .wait_while(next, |(line, _)| line.is_none())
                    .unwrap_or_else(|e| e.into_inner());
                next.0.take().unwrap_or_default()
            };
            if ipc::send(&mut stream, &line).is_err() {
                lock(&self.next).1 = true;
                return;
            }
        }
    }
}

impl Watchers {
    pub(super) fn new(changed: Arc<Wakeup>) -> Watchers {
        Watchers {
            list: Mutex::default(),
            changed,
        }
    }

    /// Keeps a new watcher: it is sent the status at once ([`push`]).
    pub(super) fn add(&self, stream: Stream) {
        if let Err(e) = ipc::watched(&stream) {
            tracing::debug!("a watcher's connection: {e}");
            return;
        }
        let out = Arc::new(Outbox::default());
        let writer = Arc::clone(&out);
        let thread = std::thread::Builder::new()
            .name("hbsync-watcher".into())
            .spawn(move || writer.write_to(stream));
        if thread.is_ok() {
            lock(&self.list).push(Watcher { out, last: None });
            self.changed.raise();
        }
    }

    /// Sends `now` to the watchers it is news to; those that are gone are dropped.
    fn send(&self, now: &DaemonStatus) {
        let mut list = lock(&self.list);
        list.retain(|w| !w.out.gone());
        let mut line = None;
        for w in list.iter_mut().filter(|w| w.last.as_ref() != Some(now)) {
            if line.is_none() {
                match ipc::line(&Reply::Status(Box::new(now.clone()))) {
                    Ok(l) => line = Some(l),
                    Err(_) => return,
                }
            }
            w.out.put(line.clone().unwrap_or_default());
            w.last = Some(now.clone());
        }
    }

    fn is_empty(&self) -> bool {
        lock(&self.list).is_empty()
    }
}

/// Whether a folder of `status` syncs: its progress changes without telling.
fn busy(status: &DaemonStatus) -> bool {
    status
        .folders
        .iter()
        .any(|f| matches!(f.state, State::Starting | State::Syncing))
}

/// Sends the status to `watchers` each time `changed` is raised (a new watcher raises it too)
/// and it is news to them, until the daemon is gone.
pub(super) fn push(daemon: Weak<Daemon>, watchers: Arc<Watchers>) {
    let changed = Arc::clone(&watchers.changed);
    send_changes(
        || daemon.upgrade().map(|d| status(&d.accounts())),
        &changed,
        &watchers,
    );
}

/// Sends `now()` to `watchers` each time `changed` is raised and it did change, until it is
/// `None`.
fn send_changes(now: impl Fn() -> Option<DaemonStatus>, changed: &Wakeup, watchers: &Watchers) {
    let (mut seen, mut syncing, mut sent) = (0, false, Instant::now());
    loop {
        seen = changed.wait(seen, if syncing { EVERY } else { WAIT_AT_MOST });
        if syncing {
            std::thread::sleep((sent + EVERY).saturating_duration_since(Instant::now()));
        }
        if watchers.is_empty() {
            syncing = false;
            continue;
        }
        let Some(now) = now() else {
            return;
        };
        syncing = busy(&now);
        watchers.send(&now);
        sent = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::Paths;
    use crate::protocol::FolderStatus;

    fn status(pending: u64, state: State) -> DaemonStatus {
        let mut folder = FolderStatus::new(&"a.example".into(), Path::new("/home/a/Homebase"));
        (folder.state, folder.pending) = (state, pending);
        DaemonStatus {
            version: crate::VERSION.into(),
            accounts: Vec::new(),
            folders: vec![folder],
        }
    }

    /// A client watching through the control socket: what it receives, as it comes.
    fn watching(paths: &Paths) -> crate::Result<Receiver<DaemonStatus>> {
        let mut watch = crate::control::watch(paths)?.ok_or("not running")?;
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            while let Ok(Some(s)) = watch.status() {
                if tx.send(s).is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }

    /// A watcher gets the status at once, then only when it changed: nothing while it doesn't,
    /// raised or not. One that went away is dropped.
    #[test]
    fn the_status_is_sent_when_it_changes_and_only_then() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        let listener = ipc::listen(&paths)?;
        let changed = Arc::new(Wakeup::default());
        let watchers = Arc::new(Watchers::new(Arc::clone(&changed)));
        let current = Arc::new(Mutex::new(status(0, State::Idle)));
        let adding = Arc::clone(&watchers);
        std::thread::spawn(move || ipc::serve(listener, |_| Reply::Done, move |s| adding.add(s)));
        let (pushing, raised, now) = (
            Arc::clone(&watchers),
            Arc::clone(&changed),
            Arc::clone(&current),
        );
        std::thread::spawn(move || send_changes(|| Some(lock(&now).clone()), &raised, &pushing));

        let rx = watching(&paths)?;
        let quiet = Duration::from_millis(300);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))?.folders[0].pending,
            0
        );
        assert!(rx.recv_timeout(quiet).is_err(), "nothing changed");
        changed.raise();
        assert!(
            rx.recv_timeout(quiet).is_err(),
            "raised, but nothing changed"
        );

        lock(&current).folders[0].pending = 3;
        changed.raise();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))?.folders[0].pending,
            3
        );
        assert!(rx.recv_timeout(quiet).is_err(), "sent once");

        // Gone: dropped once a send to it failed.
        drop(rx);
        let until = Instant::now() + Duration::from_secs(10);
        for pending in 4.. {
            if watchers.is_empty() {
                break;
            }
            assert!(Instant::now() < until, "never dropped");
            lock(&current).folders[0].pending = pending;
            watchers.send(&lock(&current).clone());
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }
}
