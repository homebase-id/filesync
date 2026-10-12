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
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use hbsync_platform::journal::ChangeJournal;
use hbsync_platform::{RelPath, journal::open_journal};
use hbsync_state::{StateDb, StateError};
use notify::event::{AccessKind, AccessMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::filter::Filter;
use crate::journal::{CURSOR_KEY, CatchUp, catch_up, relpath_prefix};
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
}

impl LiveWatcher {
    pub fn start(root: &Path, filter: Arc<Filter>) -> notify::Result<Self> {
        let root = root.canonicalize().map_err(notify::Error::io)?;
        let (tx, rx) = channel();
        let filter_root = root.clone();
        let mut watcher = notify::recommended_watcher(move |res| {
            // Only changes are queued, and only their paths that sync: reads (a viewer, a
            // backup tool, the rescan itself) and filtered items (the client's own state,
            // junk, ignored folders) would otherwise flood the channel.
            if let Some(res) = keep(&filter_root, &filter, res) {
                // The receiver is gone only when the watcher is being dropped.
                let _ = tx.send(res);
            }
        })?;
        watcher.watch(&root, RecursiveMode::Recursive)?;
        Ok(LiveWatcher {
            _watcher: watcher,
            rx,
            root,
        })
    }

    /// Waits up to `timeout` for a first change, then collects until no change came for
    /// `debounce.quiet` (the queue is then drained) or `debounce.max_delay` has passed.
    /// `None` if nothing changed. Events that change nothing neither start nor extend a batch.
    pub fn next_batch(&self, timeout: Duration, debounce: Debounce) -> Option<WatchBatch> {
        let mut batch = WatchBatch::default();
        let deadline = Instant::now() + timeout;
        loop {
            match self
                .rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(event) => {
                    if self.add(&mut batch, event) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => return None,
                Err(RecvTimeoutError::Disconnected) => {
                    batch.rescan_all = true;
                    return Some(batch);
                }
            }
        }
        let close_by = Instant::now() + debounce.max_delay;
        let mut quiet_at = Instant::now() + debounce.quiet;
        loop {
            let now = Instant::now();
            if now >= close_by {
                break;
            }
            match self
                .rx
                .recv_timeout(quiet_at.min(close_by).saturating_duration_since(now))
            {
                Ok(event) => {
                    if self.add(&mut batch, event) {
                        quiet_at = Instant::now() + debounce.quiet;
                    }
                }
                // A timeout means the queue is empty: drained, once quiet long enough.
                Err(RecvTimeoutError::Timeout) => {
                    if Instant::now() >= quiet_at {
                        batch.drained = true;
                        break;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    batch.rescan_all = true;
                    break;
                }
            }
        }
        Some(batch)
    }

    /// Folds `event` (one [`keep`] let through) into `batch`; `true` as it counts.
    fn add(&self, batch: &mut WatchBatch, event: notify::Result<Event>) -> bool {
        fold(&self.root, batch, event);
        true
    }
}

/// The notification as far as it counts, or `None`: a change in the tree, or a call for a full
/// rescan, with only the paths in the root that sync. Opening or reading changes nothing, and
/// the rescan itself opens what it lists: counting those would make every batch trigger the
/// next. Closing after a write does count.
fn keep(
    root: &Path,
    filter: &Filter,
    event: notify::Result<Event>,
) -> Option<notify::Result<Event>> {
    let Ok(mut event) = event else {
        return Some(event);
    };
    let rescan = event.need_rescan();
    if let EventKind::Access(kind) = event.kind
        && kind != AccessKind::Close(AccessMode::Write)
        && !rescan
    {
        return None;
    }
    event.paths.retain(|path| {
        path.strip_prefix(root)
            .is_ok_and(|rel| !excluded(rel, filter))
    });
    (rescan || !event.paths.is_empty()).then_some(Ok(event))
}

/// Whether `rel` is filtered or lies in a filtered folder. Whether it is a folder itself is
/// unknown (it may be gone), so it must be filtered either way: a folder that only rules for
/// folders let through (`/*` then `!/docs/`) would otherwise lose its creation event, and with
/// it what was written into it before it was watched. An event kept too many only costs a
/// rescan, which judges with the kind known.
fn excluded(rel: &Path, filter: &Filter) -> bool {
    let names: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
    names.is_some_and(|names| {
        !filter.names(names.iter().copied(), false).is_sync()
            && !filter.names(names.iter().copied(), true).is_sync()
    })
}

/// Folds one notification into a batch, if it counts (see [`keep`]): every changed path
/// touches its parent folder. Returns whether it counts.
#[cfg(test)]
fn add_event(
    root: &Path,
    filter: &Filter,
    batch: &mut WatchBatch,
    event: notify::Result<Event>,
) -> bool {
    keep(root, filter, event).is_some_and(|event| {
        fold(root, batch, event);
        true
    })
}

/// Folds a notification [`keep`] let through into `batch`.
fn fold(root: &Path, batch: &mut WatchBatch, event: notify::Result<Event>) {
    let Ok(event) = event else {
        batch.rescan_all = true;
        return;
    };
    batch.rescan_all |= event.need_rescan();
    for path in &event.paths {
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        // The parent of the changed entry; a name that isn't valid ends the path early, which
        // rescans a folder higher up.
        batch
            .dirs
            .insert(relpath_prefix(rel.parent().unwrap_or(Path::new(""))));
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
    /// When to look into new folders once more, and which (see [`RECHECK`]).
    recheck: Option<(Instant, Vec<RelPath>)>,
}

/// How long after a new folder is found it is rescanned once more. A watcher that watches
/// folder by folder (inotify) starts watching a new one only after its creation event: what is
/// written into it before that sends no event, and the rescan that found the folder may have
/// come too early to see it. By then the watch is long in place. Done on every OS: one more
/// listing of a new folder is cheap, and it covers any watcher that drops an event.
const RECHECK: Duration = Duration::from_secs(1);

impl LocalObserver {
    /// Starts watching. Call [`LocalObserver::catch_up`] next.
    pub fn start(root: &Path, opts: ScanOptions) -> Self {
        let (watcher, watcher_error) = match LiveWatcher::start(root, Arc::clone(&opts.filter)) {
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
            recheck: None,
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
        // Without a watcher nothing is ever due for a recheck.
        if self
            .recheck
            .as_ref()
            .is_some_and(|(at, _)| Instant::now() >= *at)
            && let Some((_, dirs)) = self.recheck.take()
        {
            let report = rescan_dirs(db, &self.root, &dirs, &self.opts)?;
            self.note_new_folders(&report);
            if report.added + report.removed + report.modified + report.moved > 0 {
                return Ok(Some(report));
            }
        }
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
        self.note_new_folders(&report);
        Ok(Some(report))
    }

    /// Looks into the new folders of `report` once more after [`RECHECK`].
    fn note_new_folders(&mut self, report: &ScanReport) {
        if !report.new_folders.is_empty() {
            self.recheck
                .get_or_insert_with(|| (Instant::now() + RECHECK, Vec::new()))
                .1
                .extend(report.new_folders.iter().cloned());
        }
    }

    pub fn has_watcher(&self) -> bool {
        self.watcher.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, Flag, ModifyKind, RenameMode};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{Arc, Mutex};

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).expect("valid path")
    }

    #[test]
    fn events_touch_parent_folders() {
        let root = Path::new("/sync");
        let mut batch = WatchBatch::default();
        let excl = Filter::from_rules("node_modules/\n", false).expect("valid rules");
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
        // The client's own state, junk, ignored folders, paths elsewhere, and reads (such as
        // the rescan's own) touch nothing and don't count; a finished write does.
        for ignored in [
            ev(EventKind::Any, &["/sync/.hbsync/state.db"]),
            ev(EventKind::Any, &["/sync/a/.DS_Store"]),
            ev(EventKind::Any, &["/sync/a/.notes.txt.swp"]),
            ev(EventKind::Any, &["/sync/app/node_modules/x/y.js"]),
            ev(EventKind::Any, &["/elsewhere/z"]),
            ev(
                EventKind::Access(AccessKind::Open(AccessMode::Read)),
                &["/sync/r/f"],
            ),
            ev(
                EventKind::Access(AccessKind::Close(AccessMode::Read)),
                &["/sync/r/f"],
            ),
        ] {
            assert!(!add_event(root, &excl, &mut batch, ignored));
        }
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
        add_event(
            Path::new("/r"),
            &Filter::default(),
            &mut batch,
            Ok(overflow),
        );
        assert!(batch.rescan_all);
        let mut batch = WatchBatch::default();
        add_event(
            Path::new("/r"),
            &Filter::default(),
            &mut batch,
            Err(notify::Error::generic("lost")),
        );
        assert!(batch.rescan_all);
    }

    /// Reads all the time (a user's viewer, a backup tool) must not hold a batch open: the
    /// sync loop waits on `next_batch` and can't react to anything else meanwhile.
    #[test]
    fn events_that_change_nothing_neither_start_nor_extend_a_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let mut watcher = LiveWatcher::start(&root, Arc::default())?;
        let (tx, rx) = channel();
        watcher.rx = rx;
        let reading = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let reader = {
            let (tx, reading, path) = (tx.clone(), reading.clone(), root.join("viewed.txt"));
            let root_in_thread = root.clone();
            std::thread::spawn(move || {
                while reading.load(std::sync::atomic::Ordering::Relaxed) {
                    let read = Event::new(EventKind::Access(AccessKind::Open(AccessMode::Read)))
                        .add_path(path.clone());
                    let own = Event::new(EventKind::Modify(ModifyKind::Any))
                        .add_path(path.parent().unwrap_or(&path).join(".hbsync/state.db"));
                    // As the OS callback queues them.
                    let filter = Filter::default();
                    let mut queue = [Ok(read), Ok(own)]
                        .into_iter()
                        .filter_map(|e| keep(&root_in_thread, &filter, e));
                    if queue.any(|e| tx.send(e).is_err()) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
        };
        let debounce = Debounce {
            quiet: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        };
        let started = Instant::now();
        assert_eq!(
            watcher.next_batch(Duration::from_millis(200), debounce),
            None
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );

        // A real change closes after the quiet period, reads or not.
        let created = Event::new(EventKind::Create(CreateKind::File)).add_path(root.join("new"));
        tx.send(Ok(created))?;
        let started = Instant::now();
        let batch = watcher
            .next_batch(Duration::from_millis(200), debounce)
            .ok_or("no batch")?;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert!(batch.drained);
        assert_eq!(batch.dirs, [RelPath::root()].into());

        reading.store(false, std::sync::atomic::Ordering::Relaxed);
        let _ = reader.join();
        Ok(())
    }

    /// A batch is only drained when the queue is empty: a change queued behind a backlog of
    /// events that don't count must land in this batch, or the stored journal position could
    /// pass it.
    #[test]
    fn a_change_behind_a_backlog_of_reads_is_not_left_out() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let mut watcher = LiveWatcher::start(&root, Arc::default())?;
        let (tx, rx) = channel();
        watcher.rx = rx;
        // As the OS callback queues them.
        let filter = Filter::default();
        let send = |kind, path: &str| -> Result<(), Box<dyn std::error::Error>> {
            let event = Ok(Event::new(kind).add_path(root.join(path)));
            if let Some(event) = keep(&root, &filter, event) {
                tx.send(event)?;
            }
            Ok(())
        };
        send(EventKind::Create(CreateKind::File), "a/first")?;
        for _ in 0..200_000 {
            send(
                EventKind::Access(AccessKind::Open(AccessMode::Read)),
                "r/read",
            )?;
        }
        send(EventKind::Create(CreateKind::File), "b/behind")?;
        // A quiet period far shorter than working through the backlog.
        let debounce = Debounce {
            quiet: Duration::from_micros(100),
            max_delay: Duration::from_secs(30),
        };
        let batch = watcher
            .next_batch(Duration::from_secs(1), debounce)
            .ok_or("no batch")?;
        assert_eq!(batch.dirs, [rp("a"), rp("b")].into());
        assert!(batch.drained);
        Ok(())
    }

    /// Rules for folders only decide on a path whose kind an event doesn't tell: the event
    /// is kept if the path syncs as a folder or as a file.
    #[test]
    fn an_event_for_a_folder_only_rules_let_through_is_kept() -> Result<(), crate::FilterError> {
        let root = Path::new("/sync");
        let filter = Filter::from_rules("/*\n!/docs/\n", false)?;
        let event =
            |p: &str| Ok(Event::new(EventKind::Create(CreateKind::Folder)).add_path(root.join(p)));
        assert!(keep(root, &filter, event("docs")).is_some());
        assert!(keep(root, &filter, event("docs/a.txt")).is_some());
        assert!(keep(root, &filter, event("other")).is_none());
        assert!(keep(root, &filter, event("other/a.txt")).is_none());
        Ok(())
    }

    /// A file written into a folder just created, before the watcher watches the folder,
    /// sends no event; the folder is looked into once more a moment later.
    #[test]
    fn a_file_written_into_a_new_folder_before_its_watch_is_found()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        observer.catch_up(&mut db)?;
        // Only the events this test sends arrive.
        let (tx, rx) = channel();
        observer.watcher.as_mut().ok_or("no watcher")?.rx = rx;

        fs::create_dir(root.join("new"))?;
        let created = Event::new(EventKind::Create(CreateKind::Folder)).add_path(root.join("new"));
        tx.send(Ok(created))?;
        let quick = Debounce {
            quiet: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
        };
        let report = observer
            .poll(&mut db, Duration::from_secs(1), quick)?
            .ok_or("no batch")?;
        assert_eq!(report.new_folders, [rp("new")]);
        // Written before the watch: no event.
        fs::write(root.join("new/early.txt"), "early")?;

        let until = Instant::now() + Duration::from_secs(10);
        while db.read(|s| s.get_local(&rp("new/early.txt")))?.is_none() {
            assert!(Instant::now() < until, "new/early.txt was never found");
            observer.poll(&mut db, Duration::from_millis(100), quick)?;
        }
        Ok(())
    }

    #[test]
    fn observer_picks_up_live_changes() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join("a/b"))?;
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
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

    /// Tagging a file (in Finder, in Dolphin) changes nothing but an attribute: the watcher
    /// still reports it, and the rescan reads the new tags and marks the node dirty.
    #[test]
    fn observer_picks_up_a_tag_change() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_platform::tags::{self, Tag, Tags};
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join("a"))?;
        fs::write(root.join("a/doc.txt"), b"hello")?;
        if !tags::CAN_TAG || tags::write(&root.join("a"), &Tags::default())?.is_none() {
            eprintln!("no tags here; skipped");
            return Ok(());
        }
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        assert!(observer.has_watcher(), "{:?}", observer.watcher_error);
        observer.catch_up(&mut db)?;
        let path = rp("a/doc.txt");
        let node = db
            .read(|s| s.get_local(&path))?
            .map(|r| r.node)
            .ok_or("not scanned")?;
        db.write(|s| s.clear_dirty(node))?;

        std::thread::sleep(Duration::from_millis(20)); // past a coarse clock tick
        let red = Tags::new([Tag::new("Red", 6)]);
        tags::write(&root.join("a/doc.txt"), &red)?;
        let debounce = Debounce {
            quiet: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while !db.read(|s| s.is_dirty(node))? && Instant::now() < deadline {
            observer.poll(&mut db, Duration::from_secs(2), debounce)?;
        }
        assert!(
            db.read(|s| s.is_dirty(node))?,
            "the tag change went unnoticed"
        );
        let read = db.read(|s| s.get_local(&path))?.and_then(|r| r.tags);
        assert_eq!(
            read.map(|t| t.iter().map(|t| t.name.clone()).collect::<Vec<_>>()),
            Some(vec!["Red".to_owned()])
        );
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
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        let clock = Instant::now();
        observer.journal = Some(Box::new(ClockJournal(clock)));
        // The OS events come from a delay line instead: each one arrives DELAY after its
        // change, as FSEvents-style delivery can. That is longer than a busy batch lasts
        // (MAX_DELAY), so batches are cut off with older events still queued, and only the
        // drained rule keeps the position back; and shorter than the quiet period, the bound
        // the guarantee rests on. When the delay line itself falls behind by a whole quiet
        // period (a stalled thread on a busy machine), the OS it plays broke that bound, and a
        // change whose event came that late proves nothing.
        const DELAY: Duration = Duration::from_millis(60);
        const MAX_DELAY: Duration = Duration::from_millis(30);
        const QUIET: Duration = Duration::from_millis(100);
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

        let (to_delay, delayed) = channel::<(Instant, PathBuf, String)>();
        // When each change's event was actually delivered.
        let delivered: Arc<Mutex<HashMap<String, u128>>> = Arc::default();
        let sent = delivered.clone();
        let delay_line = std::thread::spawn(move || {
            for (at, path, rel) in delayed {
                std::thread::sleep((at + DELAY).saturating_duration_since(Instant::now()));
                if let Ok(mut sent) = sent.lock() {
                    sent.insert(rel, clock.elapsed().as_nanos());
                }
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
                let _ = to_delay.send((Instant::now(), writer_root.join(&rel), rel.clone()));
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(())
        });
        // Checked after every poll: the process could die at any of those points.
        let late = std::cell::RefCell::new(std::collections::HashSet::new());
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
                    let arrived = delivered.lock().map_err(|_| "poisoned")?.get(rel).copied();
                    let latency = arrived.unwrap_or_else(|| clock.elapsed().as_nanos()) - at;
                    if latency >= QUIET.as_nanos() {
                        late.borrow_mut().insert(rel.clone());
                        continue;
                    }
                    return Err(format!(
                        "{rel} (written at {at}) is before {stored}, unrecorded"
                    ));
                }
            }
            Ok(())
        };
        // While the writer runs, batches are cut off by max_delay.
        let busy = Debounce {
            quiet: QUIET,
            max_delay: MAX_DELAY,
        };
        while !writer.is_finished() {
            observer.poll(&mut db, Duration::from_millis(500), busy)?;
            check(&mut db)?;
        }
        writer.join().map_err(|_| "writer panicked")??;
        // Then the queue drains and the position catches up.
        let _ = delay_line.join();
        let quiet = Debounce {
            quiet: QUIET,
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
        let late = late.borrow().len();
        if late > 0 {
            eprintln!(
                "{late} events came later than the quiet period (a busy machine): not counted"
            );
        }
        Ok(())
    }
}
