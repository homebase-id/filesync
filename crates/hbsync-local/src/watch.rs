//! Live watching and the local observer that ties all local sources together.
//!
//! [`LiveWatcher`] turns OS notifications (inotify, FSEvents, ReadDirectoryChangesW via
//! `notify`) into debounced batches of touched folders. An overflow or a watcher error asks for
//! a full rescan, since events were lost.
//!
//! [`LocalObserver`] starts the watcher *before* catch-up, so a change made while catch-up runs
//! is seen by the watcher (and at worst handled twice, which is harmless).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use hbsync_platform::journal::ChangeJournal;
use hbsync_platform::{RelPath, journal::open_journal};
use hbsync_state::{StateDb, StateError};
use notify::event::{AccessKind, AccessMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::journal::{CURSOR_KEY, CatchUp, catch_up};
use crate::scan::{ScanOptions, ScanReport, rescan_dirs, scan};

/// A coalesced set of changes from the watcher.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchBatch {
    pub dirs: BTreeSet<RelPath>,
    /// Events were lost (queue overflow, watcher error): rescan everything.
    pub rescan_all: bool,
    /// The batch ended with a quiet period, so no delivered event was left behind (one closed
    /// by `max_delay` may leave events queued for the next batch).
    pub drained: bool,
}

impl WatchBatch {
    fn is_empty(&self) -> bool {
        self.dirs.is_empty() && !self.rescan_all
    }
}

/// Timing of the debounce.
#[derive(Debug, Clone, Copy)]
pub struct Debounce {
    /// A batch is closed after this long without new events...
    pub quiet: Duration,
    /// ...or at the latest this long after its first event (a busy folder can't starve sync).
    pub max_delay: Duration,
}

impl Default for Debounce {
    fn default() -> Self {
        Debounce {
            quiet: Duration::from_millis(300),
            max_delay: Duration::from_secs(3),
        }
    }
}

/// OS change notifications for one sync root.
#[derive(Debug)]
pub struct LiveWatcher {
    _watcher: RecommendedWatcher,
    rx: Receiver<notify::Result<Event>>,
    root: PathBuf,
    exclude_at_root: Vec<String>,
}

impl LiveWatcher {
    pub fn start(root: &Path, exclude_at_root: &[String]) -> notify::Result<Self> {
        let root = root.canonicalize().map_err(notify::Error::io)?;
        let (tx, rx) = channel();
        let mut watcher = notify::recommended_watcher(move |res| {
            // The receiver is gone only when the watcher is being dropped.
            let _ = tx.send(res);
        })?;
        watcher.watch(&root, RecursiveMode::Recursive)?;
        Ok(LiveWatcher {
            _watcher: watcher,
            rx,
            root,
            exclude_at_root: exclude_at_root.to_vec(),
        })
    }

    /// Waits up to `timeout` for a first event, then collects until the batch is quiet or
    /// `debounce.max_delay` has passed. `None` if nothing happened.
    pub fn next_batch(&self, timeout: Duration, debounce: Debounce) -> Option<WatchBatch> {
        let mut batch = WatchBatch::default();
        let first = match self.rx.recv_timeout(timeout) {
            Ok(event) => event,
            Err(RecvTimeoutError::Timeout) => return None,
            Err(RecvTimeoutError::Disconnected) => {
                batch.rescan_all = true;
                return Some(batch);
            }
        };
        self.add(&mut batch, first);
        let started = Instant::now();
        loop {
            let left = debounce.max_delay.saturating_sub(started.elapsed());
            if left.is_zero() {
                break;
            }
            match self.rx.recv_timeout(debounce.quiet.min(left)) {
                Ok(event) => self.add(&mut batch, event),
                Err(RecvTimeoutError::Timeout) => {
                    batch.drained = debounce.quiet <= left;
                    break;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    batch.rescan_all = true;
                    break;
                }
            }
        }
        (!batch.is_empty()).then_some(batch)
    }

    fn add(&self, batch: &mut WatchBatch, event: notify::Result<Event>) {
        add_event(&self.root, &self.exclude_at_root, batch, event);
    }
}

/// Folds one notification into a batch: every path touches its parent folder.
fn add_event(
    root: &Path,
    exclude_at_root: &[String],
    batch: &mut WatchBatch,
    event: notify::Result<Event>,
) {
    let event = match event {
        Ok(event) => event,
        Err(_) => {
            batch.rescan_all = true;
            return;
        }
    };
    if event.need_rescan() {
        batch.rescan_all = true;
    }
    // Opening or reading changes nothing, and the rescan itself opens what it lists: counting
    // those would make every batch trigger the next. Closing after a write does count.
    if let EventKind::Access(kind) = event.kind
        && kind != AccessKind::Close(AccessMode::Write)
    {
        return;
    }
    for path in &event.paths {
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        let mut components = rel.components().filter_map(|c| match c {
            std::path::Component::Normal(n) => n.to_str(),
            _ => None,
        });
        let names: Vec<&str> = components.by_ref().collect();
        if names
            .first()
            .is_some_and(|first| exclude_at_root.iter().any(|e| e == first))
        {
            continue;
        }
        // The parent of the changed entry; a name that isn't valid ends the path early, which
        // rescans a folder higher up.
        let mut dir = RelPath::root();
        for name in names.iter().take(names.len().saturating_sub(1)) {
            match dir.join(name) {
                Ok(next) => dir = next,
                Err(_) => break,
            }
        }
        batch.dirs.insert(dir);
    }
}

/// All local observation for one sync root: catch-up at start, then live batches.
#[derive(Debug)]
pub struct LocalObserver {
    root: PathBuf,
    opts: ScanOptions,
    journal: Option<Box<dyn ChangeJournal>>,
    watcher: Option<LiveWatcher>,
    /// Why live watching isn't available (the caller then rescans periodically).
    pub watcher_error: Option<String>,
    /// A batch was recorded without storing the journal position (see [`LocalObserver::poll`]).
    unsaved: bool,
}

impl LocalObserver {
    /// Starts watching. Call [`LocalObserver::catch_up`] next.
    pub fn start(root: &Path, opts: ScanOptions) -> Self {
        let (watcher, watcher_error) = match LiveWatcher::start(root, &opts.exclude_at_root) {
            Ok(w) => (Some(w), None),
            // Typically inotify's watch limit on very large trees.
            Err(e) => (None, Some(e.to_string())),
        };
        LocalObserver {
            root: root.to_path_buf(),
            journal: open_journal(root, opts.fs_family),
            opts,
            watcher,
            watcher_error,
            unsaved: false,
        }
    }

    pub fn catch_up(&mut self, db: &mut StateDb) -> Result<CatchUp, StateError> {
        catch_up(db, &self.root, self.journal.as_deref_mut(), &self.opts)
    }

    /// Waits up to `timeout` for live changes and records them. `None` if nothing happened
    /// (or there is no watcher; then [`LocalObserver::catch_up`] should run periodically).
    pub fn poll(
        &mut self,
        db: &mut StateDb,
        timeout: Duration,
        debounce: Debounce,
    ) -> Result<Option<ScanReport>, StateError> {
        let Some(watcher) = &self.watcher else {
            std::thread::sleep(timeout);
            return Ok(None);
        };
        // The journal position is taken before waiting. Once the queue has been drained (a
        // quiet period, or no event at all, for longer than the OS takes to deliver one),
        // every change made before that position has been delivered and recorded, in this
        // batch or an earlier one, so the position can be stored; anything later is replayed
        // after a crash. A batch cut off by `max_delay` may leave events queued: it stores
        // nothing, and the next drained poll catches up.
        let position = self.journal.as_mut().and_then(|j| j.now().ok());
        let Some(batch) = watcher.next_batch(timeout, debounce) else {
            if self.unsaved
                && let Some(position) = position
            {
                db.write(|s| s.set_kv(CURSOR_KEY, &position))?;
                self.unsaved = false;
            }
            return Ok(None);
        };
        let dirs: Vec<RelPath> = batch.dirs.into_iter().collect();
        let report = if batch.rescan_all {
            scan(db, &self.root, &self.opts)?
        } else {
            rescan_dirs(db, &self.root, &dirs, &self.opts)?
        };
        match position {
            Some(position) if batch.drained => {
                db.write(|s| s.set_kv(CURSOR_KEY, &position))?;
                self.unsaved = false;
            }
            _ => self.unsaved = true,
        }
        Ok(Some(report))
    }

    pub fn has_watcher(&self) -> bool {
        self.watcher.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, Flag, ModifyKind, RenameMode};
    use std::fs;
    use std::sync::{Arc, Mutex};

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).expect("valid path")
    }

    #[test]
    fn events_touch_parent_folders() {
        let root = Path::new("/sync");
        let mut batch = WatchBatch::default();
        let excl = vec![".hbsync".to_owned()];
        let ev = |kind, paths: &[&str]| {
            Ok(Event {
                kind,
                paths: paths.iter().map(PathBuf::from).collect(),
                attrs: Default::default(),
            })
        };
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Create(CreateKind::File), &["/sync/a/b/new.txt"]),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                &["/sync/x/f", "/sync/y/f"],
            ),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Create(CreateKind::File), &["/sync/top.txt"]),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Any, &["/sync/.hbsync/state.db"]),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Any, &["/elsewhere/z"]),
        );
        // Reads (such as the rescan's own) touch nothing; a finished write does.
        add_event(
            root,
            &excl,
            &mut batch,
            ev(
                EventKind::Access(AccessKind::Open(AccessMode::Read)),
                &["/sync/r/f"],
            ),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(
                EventKind::Access(AccessKind::Close(AccessMode::Read)),
                &["/sync/r/f"],
            ),
        );
        add_event(
            root,
            &excl,
            &mut batch,
            ev(
                EventKind::Access(AccessKind::Close(AccessMode::Write)),
                &["/sync/w/f"],
            ),
        );
        let expected: BTreeSet<_> = [rp("a/b"), rp("x"), rp("y"), rp("w"), RelPath::root()].into();
        assert_eq!(
            batch,
            WatchBatch {
                dirs: expected,
                rescan_all: false,
                drained: false,
            }
        );
    }

    #[test]
    fn overflow_and_errors_ask_for_full_rescan() {
        let mut batch = WatchBatch::default();
        let overflow = Event::new(EventKind::Other).set_flag(Flag::Rescan);
        add_event(Path::new("/r"), &[], &mut batch, Ok(overflow));
        assert!(batch.rescan_all);
        let mut batch = WatchBatch::default();
        add_event(
            Path::new("/r"),
            &[],
            &mut batch,
            Err(notify::Error::generic("lost")),
        );
        assert!(batch.rescan_all);
    }

    #[test]
    fn observer_picks_up_live_changes() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join("a/b"))?;
        let opts = ScanOptions {
            algorithm: hbsync_fileio::HashAlgorithm::Blake3,
            fs_family: hbsync_platform::fskind::detect(&root)?,
            exclude_at_root: vec![crate::STATE_DIR.into()],
        };
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        assert!(observer.has_watcher(), "{:?}", observer.watcher_error);
        observer.catch_up(&mut db)?;

        fs::write(root.join("a/b/live.txt"), b"hello")?;
        let debounce = Debounce {
            quiet: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
        };
        let mut added = 0;
        let deadline = Instant::now() + Duration::from_secs(20);
        while added == 0 && Instant::now() < deadline {
            if let Some(report) = observer.poll(&mut db, Duration::from_secs(2), debounce)? {
                added += report.added;
            }
        }
        assert_eq!(added, 1);
        let path = rp("a/b/live.txt");
        assert!(db.read(|s| s.get_local(&path))?.is_some());
        Ok(())
    }

    /// A journal whose position is the time since the test started.
    #[derive(Debug)]
    struct ClockJournal(Instant);

    impl ChangeJournal for ClockJournal {
        fn now(&mut self) -> Result<String, hbsync_platform::journal::JournalError> {
            Ok(format!("{:020}", self.0.elapsed().as_nanos()))
        }
        fn since(
            &mut self,
            _: &str,
        ) -> Result<hbsync_platform::journal::JournalDelta, hbsync_platform::journal::JournalError>
        {
            Err(hbsync_platform::journal::JournalError::Unusable(
                "test".into(),
            ))
        }
    }

    /// The stored journal position must never pass a change that isn't recorded yet: after a
    /// crash, catch-up replays only what comes after it.
    #[test]
    fn stored_position_never_passes_an_unrecorded_change() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let opts = ScanOptions {
            algorithm: hbsync_fileio::HashAlgorithm::Blake3,
            fs_family: hbsync_platform::fskind::detect(&root)?,
            exclude_at_root: vec![crate::STATE_DIR.into()],
        };
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        let clock = Instant::now();
        observer.journal = Some(Box::new(ClockJournal(clock)));
        // The OS events come from a delay line instead: each one arrives 20 ms after its
        // change (well within the quiet period), as FSEvents-style delivery can.
        let (tx, rx) = channel();
        let _keep_connected = tx.clone();
        if let Some(w) = observer.watcher.as_mut() {
            w.rx = rx;
        }
        // Known folders, so each event leads to a rescan of its own folder only.
        for i in 0..400 {
            fs::create_dir(root.join(format!("d{i}")))?;
        }
        scan(&mut db, &root, &observer.opts)?;

        let (to_delay, delayed) = channel::<(Instant, PathBuf)>();
        let delay_line = std::thread::spawn(move || {
            for (at, path) in delayed {
                std::thread::sleep(
                    (at + Duration::from_millis(20)).saturating_duration_since(Instant::now()),
                );
                let event = Event::new(EventKind::Create(CreateKind::File)).add_path(path);
                let _ = tx.send(Ok(event));
            }
        });
        // A busy writer: a new file every 2 ms, each in another folder, for about a second.
        let written: Arc<Mutex<Vec<(u128, String)>>> = Arc::default();
        let (writer_root, log) = (root.clone(), written.clone());
        let writer = std::thread::spawn(move || -> std::io::Result<()> {
            for i in 0..400 {
                let rel = format!("d{i}/f");
                let before = clock.elapsed().as_nanos();
                fs::write(writer_root.join(&rel), b"x")?;
                log.lock()
                    .map_err(|_| std::io::Error::other("poisoned"))?
                    .push((before, rel.clone()));
                let _ = to_delay.send((Instant::now(), writer_root.join(&rel)));
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(())
        });
        // Checked after every poll: the process could die at any of those points.
        let check = |db: &mut StateDb| -> Result<(), String> {
            let Some(stored) = db
                .read(|s| s.get_kv(CURSOR_KEY))
                .map_err(|e| e.to_string())?
            else {
                return Ok(());
            };
            let stored: u128 = stored.parse().map_err(|_| "bad position")?;
            let written = written.lock().map_err(|_| "poisoned")?.clone();
            for (at, rel) in written.iter().filter(|(at, _)| *at < stored) {
                let path = rp(rel);
                if db
                    .read(|s| s.get_local(&path))
                    .map_err(|e| e.to_string())?
                    .is_none()
                {
                    return Err(format!(
                        "{rel} (written at {at}) is before {stored}, unrecorded"
                    ));
                }
            }
            Ok(())
        };
        // While the writer runs, batches are cut off by max_delay.
        let busy = Debounce {
            quiet: Duration::from_millis(100),
            max_delay: Duration::from_millis(50),
        };
        while !writer.is_finished() {
            observer.poll(&mut db, Duration::from_millis(500), busy)?;
            check(&mut db)?;
        }
        writer.join().map_err(|_| "writer panicked")??;
        // Then the queue drains and the position catches up.
        let _ = delay_line.join();
        let quiet = Debounce {
            quiet: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        };
        while observer
            .poll(&mut db, Duration::from_millis(300), quiet)?
            .is_some()
        {
            check(&mut db)?;
        }
        check(&mut db)?;
        assert_eq!(
            db.read(|s| s.local_count())?,
            800,
            "every folder and file recorded"
        );
        assert!(
            db.read(|s| s.get_kv(CURSOR_KEY))?.is_some(),
            "a position was stored"
        );
        Ok(())
    }
}
