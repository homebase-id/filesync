//! Live watching and the local observer that ties all local sources together.
//!
//! [`LiveWatcher`] turns OS notifications (inotify, FSEvents, ReadDirectoryChangesW via
//! `notify`) into debounced batches of touched folders. An overflow or a watcher error asks for
//! a full rescan, since events were lost.
//!
//! [`LocalObserver`] starts the watcher *before* catch-up, so a change made while catch-up runs
//! is seen by the watcher (and at worst handled twice, which is harmless). It rings a bell when
//! there is something to record, so its owner can block until then instead of polling.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use hbsync_platform::journal::ChangeJournal;
use hbsync_platform::{RelPath, journal::open_journal};
use hbsync_state::{StateDb, StateError, keys};
use notify::event::{AccessKind, AccessMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::filter::Filter;
use crate::journal::{CatchUp, catch_up, relpath_prefix};
use crate::scan::{ScanOptions, ScanReport, Settle, rescan_entries, scan};

/// A coalesced set of changes from the watcher.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WatchBatch {
    /// Folders to rescan: one an event named itself, or the folder of an entry whose name
    /// isn't valid.
    pub dirs: BTreeSet<RelPath>,
    /// Entries the events named, each looked at by itself where that is enough
    /// ([`crate::scan::rescan_entries`]).
    pub entries: BTreeSet<RelPath>,
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

/// Called from the watcher's thread when there is something to record (or the ignore rules
/// were touched): at most once until the next [`LocalObserver::poll`], however many events
/// come meanwhile.
pub type Bell = Arc<dyn Fn() + Send + Sync>;

/// OS change notifications for one sync root.
pub(crate) struct LiveWatcher {
    _watcher: RecommendedWatcher,
    rx: Receiver<notify::Result<Event>>,
    root: PathBuf,
    /// The bell rang since the queue was last drained: it won't ring again until then.
    rung: Arc<AtomicBool>,
    /// The ignore rules' file was touched.
    rules: Arc<AtomicBool>,
    /// The OS watcher stopped sending (its channel closed).
    gone: AtomicBool,
}

impl std::fmt::Debug for LiveWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveWatcher")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl LiveWatcher {
    /// Starts watching; `bell` rings when an event is queued or the ignore rules' file was
    /// changed.
    pub(crate) fn start(
        root: &Path,
        filter: Arc<Filter>,
        bell: Option<Bell>,
    ) -> notify::Result<Self> {
        let root = root.canonicalize().map_err(notify::Error::io)?;
        #[cfg(feature = "testkit")]
        if testkit::take(&testkit::FAIL, &root) {
            return Err(notify::Error::io(std::io::Error::from_raw_os_error(24)));
        }
        let (tx, rx) = channel();
        let (rung, rules): (Arc<AtomicBool>, Arc<AtomicBool>) = Default::default();
        let (filter_root, rang, touched) = (root.clone(), Arc::clone(&rung), Arc::clone(&rules));
        let rules_file = crate::layout::ignore_file(&root);
        let rules_dir = rules_file.parent().unwrap_or(&root).to_path_buf();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            // The rules' file is the client's own (filtered below), but a change to it counts
            // (or may hide in an event that lost its paths); reading it (as the runner does to
            // compare them) doesn't.
            let rules_touched = res.as_ref().map_or(true, |e| {
                e.need_rescan()
                    || !reads_only(&e.kind)
                        && e.paths.iter().any(|p| *p == rules_file || *p == rules_dir)
            });
            if rules_touched {
                touched.store(true, Ordering::SeqCst);
            }
            // Only changes are queued, and only their paths that sync: reads (a viewer, a
            // backup tool, the rescan itself) and filtered items (the client's own state,
            // junk, ignored folders) would otherwise flood the channel.
            let kept = keep(&filter_root, &filter, res);
            let queued = kept.is_some();
            if let Some(res) = kept {
                // The receiver is gone only when the watcher is being dropped.
                let _ = tx.send(res);
            }
            // After queueing: whoever the bell wakes finds the event.
            if (queued || rules_touched)
                && !rang.load(Ordering::Relaxed)
                && !rang.swap(true, Ordering::SeqCst)
                && let Some(bell) = &bell
            {
                bell();
            }
        })?;
        watcher.watch(&root, RecursiveMode::Recursive)?;
        Ok(LiveWatcher {
            _watcher: watcher,
            rx,
            root,
            rung,
            rules,
            gone: AtomicBool::new(false),
        })
    }

    /// Waits up to `timeout` for a first change, then collects until no change came for
    /// `debounce.quiet` (the queue is then drained) or `debounce.max_delay` has passed.
    /// `None` if nothing changed. Events that change nothing neither start nor extend a batch.
    /// `first` is called once the first change came, before the rest is collected.
    pub(crate) fn next_batch(
        &self,
        timeout: Duration,
        debounce: Debounce,
        first: impl FnOnce(),
    ) -> Option<WatchBatch> {
        let mut batch = WatchBatch::default();
        let deadline = Instant::now() + timeout;
        loop {
            match self
                .rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(event) => {
                    if self.add(&mut batch, event) {
                        first();
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => return None,
                Err(RecvTimeoutError::Disconnected) => {
                    self.gone.store(true, Ordering::Relaxed);
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
                    self.gone.store(true, Ordering::Relaxed);
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
    if reads_only(&event.kind) && !rescan {
        return None;
    }
    event.paths.retain(|path| {
        path.strip_prefix(root)
            .is_ok_and(|rel| !excluded(rel, filter))
    });
    (rescan || !event.paths.is_empty()).then_some(Ok(event))
}

/// Whether an event of `kind` only says something was opened or read.
fn reads_only(kind: &EventKind) -> bool {
    matches!(kind, EventKind::Access(a) if *a != AccessKind::Close(AccessMode::Write))
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
        // The changed entry. A name that isn't valid ends the path early: the folder above is
        // rescanned (the root, for an event on the root itself).
        let entry = relpath_prefix(rel);
        if !entry.is_root() && entry.depth() == rel.components().count() {
            batch.entries.insert(entry);
        } else {
            batch.dirs.insert(entry);
        }
    }
}

/// A watcher's bell, kept for a watcher started again.
struct Ringer(Option<Bell>);

impl std::fmt::Debug for Ringer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "Ringer(Some)"
        } else {
            "Ringer(None)"
        })
    }
}

/// All local observation for one sync root: catch-up at start, then live batches.
#[derive(Debug)]
pub struct LocalObserver {
    root: PathBuf,
    opts: ScanOptions,
    journal: Option<Box<dyn ChangeJournal>>,
    watcher: Option<LiveWatcher>,
    /// Why live watching isn't available (the caller then rescans periodically, and asks for
    /// it again: [`LocalObserver::rewatch`]).
    pub watcher_error: Option<String>,
    /// What a watcher rings, and how it lets files being written settle: kept for one started
    /// again.
    bell: Ringer,
    settle: Settle,
    /// A batch was recorded without storing the journal position (see [`LocalObserver::poll`]).
    unsaved: bool,
    /// While `unsaved`: a position taken, stored if no change came until the instant.
    settling: Option<(String, Instant)>,
    /// The last batch was cut off by `max_delay`: events may still be queued.
    more: bool,
    /// When to look into new folders once more, and which (see [`RECHECK`]).
    recheck: Option<(Instant, Vec<RelPath>)>,
    /// Large files left unhashed while they are written ([`Settle`]), each
    /// looked at again once it may have been quiet long enough.
    young: BTreeMap<RelPath, Waiting>,
}

/// A file being written, to look at again.
#[derive(Debug, Clone, Copy)]
struct Waiting {
    at: Instant,
    /// The wait after a look that finds it written to since the last ([`FIRST_WAIT`],
    /// doubling up to [`MAX_WAIT`]).
    wait: Duration,
    /// Its modification time when last seen.
    mtime_ns: i64,
    /// Since when it has been left (or since it was last read anyway, [`READ_ANYWAY`]).
    since: Instant,
}

impl Waiting {
    /// A file's first look, `at`.
    fn first(at: Instant, mtime_ns: i64) -> Self {
        Waiting {
            at,
            wait: FIRST_WAIT,
            mtime_ns,
            since: Instant::now(),
        }
    }
}

/// A file still being written when it is looked at again waits this long for the next look,
/// twice as long each time it is still written to, up to [`MAX_WAIT`]: a file written for
/// minutes is looked at a few times, not at every batch, and is read at most this long after
/// it went quiet.
const FIRST_WAIT: Duration = Duration::from_secs(3);
const MAX_WAIT: Duration = Duration::from_secs(60);

/// A file written to without a quiet moment for this long is read all the same, at its next
/// look, and again after as long each time: one that is never quiet (a log, a running VM's
/// disk) still syncs where its upload fits between two writes.
const READ_ANYWAY: Duration = Duration::from_secs(600);

/// How long after a new folder is found it is rescanned once more. A watcher that watches
/// folder by folder (inotify) starts watching a new one only after its creation event: what is
/// written into it before that sends no event, and the rescan that found the folder may have
/// come too early to see it. By then the watch is long in place. Done on every OS: one more
/// listing of a new folder is cheap, and it covers any watcher that drops an event.
const RECHECK: Duration = Duration::from_secs(1);

impl LocalObserver {
    /// Starts watching. Call [`LocalObserver::catch_up`] next.
    pub fn start(root: &Path, opts: ScanOptions) -> Self {
        Self::ringing(root, opts, None)
    }

    /// [`LocalObserver::start`], ringing `bell` when there is something to record
    /// ([`LocalObserver::poll`]) or the ignore rules' file was touched
    /// ([`LocalObserver::rules_touched`]). Poll at [`LocalObserver::deadline`] too.
    ///
    /// With a watcher, large files being written are hashed once they are quiet
    /// ([`ScanOptions::settle`]: `opts`' own, else the default), and looked at again for it.
    pub fn ringing(root: &Path, mut opts: ScanOptions, bell: Option<Bell>) -> Self {
        let filter = Arc::clone(&opts.filter);
        let (watcher, watcher_error) = match LiveWatcher::start(root, filter, bell.clone()) {
            Ok(w) => (Some(w), None),
            // inotify's limits: its watches on very large trees, its instances (128 a user)
            // when other programs hold them.
            Err(e) => (None, Some(e.to_string())),
        };
        let settle = opts.settle.unwrap_or_default();
        // Without a watcher, the caller's periodic scans hash what they find.
        opts.settle = watcher.as_ref().map(|_| settle);
        LocalObserver {
            root: root.to_path_buf(),
            journal: open_journal(root, opts.fs_family),
            opts,
            watcher,
            watcher_error,
            bell: Ringer(bell),
            settle,
            unsaved: false,
            settling: None,
            more: false,
            recheck: None,
            young: BTreeMap::new(),
        }
    }

    /// Brings the rows up to date with the disk ([`catch_up`]), reading every changed file it
    /// finds, being written or not: a run that catches up, syncs once and stops leaves none
    /// behind. The files left unhashed before (being written when the client stopped, locked)
    /// that it didn't try to read are looked at again at the first poll: a journal names only
    /// what changed since, and nothing else would.
    pub fn catch_up(&mut self, db: &mut StateDb) -> Result<CatchUp, StateError> {
        let opts = ScanOptions {
            settle: None,
            ..self.opts.clone()
        };
        let caught = catch_up(db, &self.root, self.journal.as_deref_mut(), &opts)?;
        let tried: HashSet<&RelPath> = caught.report.unhashed.iter().map(|(p, _)| p).collect();
        let unhashed = db.read(|s| {
            let mut rows = Vec::new();
            for node in s.unhashed_files()? {
                rows.extend(s.local_by_node(node)?);
            }
            Ok(rows)
        })?;
        let now = Instant::now();
        for row in unhashed.into_iter().filter(|r| !tried.contains(&r.path)) {
            self.young
                .entry(row.path)
                .or_insert(Waiting::first(now, row.fingerprint.mtime_ns));
        }
        Ok(caught)
    }

    /// Waits up to `timeout` for live changes and records them. `None` if nothing happened
    /// (or there is no watcher; then [`LocalObserver::catch_up`] should run periodically).
    /// With a zero `timeout` it only takes what is queued, and costs nothing when nothing is.
    pub fn poll(
        &mut self,
        db: &mut StateDb,
        timeout: Duration,
        debounce: Debounce,
    ) -> Result<Option<ScanReport>, StateError> {
        // Before draining: an event queued from now on rings again.
        if let Some(w) = &self.watcher {
            w.rung.store(false, Ordering::SeqCst);
        }
        self.more = false;
        // New folders and files being written, due for another look (none without a watcher,
        // but those a watcher that stopped left).
        let now = Instant::now();
        let dirs = self
            .recheck
            .take_if(|(at, _)| now >= *at)
            .map(|(_, dirs)| dirs)
            .unwrap_or_default();
        let due = self.young.iter().filter(|(_, w)| now >= w.at);
        // Those left for long are read now, whether quiet or not (the rest at the next poll).
        let forced: Vec<RelPath> = due
            .clone()
            .filter(|(_, w)| now.duration_since(w.since) >= READ_ANYWAY)
            .map(|(path, _)| path.clone())
            .collect();
        let forcing = !forced.is_empty();
        let anyway;
        let (files, opts) = match forcing {
            false => (due.map(|(path, _)| path.clone()).collect(), &self.opts),
            true => {
                anyway = ScanOptions {
                    settle: self.opts.settle.map(|s| Settle {
                        quiet: Duration::ZERO,
                        ..s
                    }),
                    ..self.opts.clone()
                };
                (forced, &anyway)
            }
        };
        if !dirs.is_empty() || !files.is_empty() {
            let report = rescan_entries(db, &self.root, &dirs, &files, opts)?;
            self.note_new_folders(&report);
            self.note_young(&report, &files);
            if forcing {
                self.more = true;
                for path in &files {
                    if let Some(w) = self.young.get_mut(path) {
                        w.since = now;
                    }
                }
            }
            if report.added + report.removed + report.modified + report.moved > 0 {
                // The queue wasn't looked at: poll again at once.
                self.more = true;
                return Ok(Some(report));
            }
        }
        let Some(watcher) = &self.watcher else {
            std::thread::sleep(timeout);
            return Ok(None);
        };
        // A batch's journal position is taken when its first change came, before the rest is
        // collected. Once the queue has been drained (a quiet period longer than the OS takes
        // to deliver an event), every change made before that position has been delivered and
        // recorded, in this batch or an earlier one, so the position can be stored; anything
        // later is replayed after a crash. A batch cut off by `max_delay` may leave events
        // queued: it stores nothing. Then a position taken while nothing is recorded is stored
        // once no change came for a quiet period after it (`settling`), as the OS has
        // delivered everything before it by then. Nothing is taken while nothing changes.
        if self.unsaved
            && self.settling.is_none()
            && let Some(journal) = self.journal.as_mut()
        {
            match journal.now() {
                Ok(position) => self.settling = Some((position, Instant::now() + debounce.quiet)),
                // An older position is always safe: it only replays more.
                Err(_) => self.unsaved = false,
            }
        }
        let mut position = None;
        let journal = &mut self.journal;
        let first = || position = journal.as_mut().and_then(|j| j.now().ok());
        let Some(batch) = watcher.next_batch(timeout, debounce, first) else {
            if let Some((_, at)) = &self.settling
                && Instant::now() >= *at
                && let Some((position, _)) = self.settling.take()
            {
                db.write(|s| s.folder().set_kv(keys::JOURNAL_CURSOR, &position))?;
                self.unsaved = false;
            }
            return Ok(None);
        };
        self.settling = None;
        self.more = !batch.drained;
        #[cfg(feature = "testkit")]
        let stopped = testkit::take(&testkit::STOP, &self.root);
        #[cfg(not(feature = "testkit"))]
        let stopped = false;
        if stopped || watcher.gone.load(Ordering::Relaxed) {
            // Left to the look's rescans from now on, as without a watcher.
            self.watcher_error = Some("the live watcher stopped".to_owned());
            self.watcher = None;
            self.opts.settle = None;
            self.more = false;
            // No position is stored without a watcher; the one stored is older, which is safe.
            self.unsaved = false;
        }
        let mut dirs = batch.dirs;
        let mut entries: Vec<RelPath> = batch.entries.into_iter().collect();
        // A batch cut off by `max_delay` may hold half of a rename: its folders are listed,
        // which sees the whole of it.
        if !batch.drained {
            dirs.extend(entries.drain(..).map(|e| e.parent().unwrap_or_default()));
        }
        let dirs: Vec<RelPath> = dirs.into_iter().collect();
        let report = if batch.rescan_all {
            scan(db, &self.root, &self.opts)?
        } else {
            rescan_entries(db, &self.root, &dirs, &entries, &self.opts)?
        };
        // Before anything else can fail: the scan's rows are written.
        self.note_young(&report, &[]);
        match position {
            Some(position) if batch.drained => {
                db.write(|s| s.folder().set_kv(keys::JOURNAL_CURSOR, &position))?;
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

    /// Keeps the files `report` left unhashed while they are written ([`ScanReport::young`])
    /// for another look, and lets go of those of `looked` (the ones due, just looked at) it
    /// read or found gone. One found written to since it was last seen waits longer
    /// ([`Waiting::wait`]); one found quiet waits only till it has been quiet long enough, and
    /// one first found waits that long. One a batch saw keeps the look it has.
    fn note_young(&mut self, report: &ScanReport, looked: &[RelPath]) {
        let now = Instant::now();
        let still: HashSet<&RelPath> = report.young.iter().map(|y| &y.path).collect();
        self.young
            .retain(|path, _| !looked.contains(path) || still.contains(path));
        for young in &report.young {
            match self.young.get_mut(&young.path) {
                Some(w) if looked.contains(&young.path) => {
                    if young.mtime_ns == w.mtime_ns {
                        w.at = now + young.left;
                    } else {
                        w.at = now + w.wait.max(young.left);
                        w.wait = (w.wait * 2).min(MAX_WAIT);
                        w.mtime_ns = young.mtime_ns;
                    }
                }
                Some(w) => w.mtime_ns = young.mtime_ns,
                None => {
                    let first = Waiting::first(now + young.left, young.mtime_ns);
                    self.young.insert(young.path.clone(), first);
                }
            }
        }
    }

    pub fn has_watcher(&self) -> bool {
        self.watcher.is_some()
    }

    /// Without a watcher (it couldn't start, or it stopped), tries to start one again: whether
    /// there is one now. What changed meanwhile is the caller's rescan's, after this.
    pub fn rewatch(&mut self) -> bool {
        if self.watcher.is_some() {
            return true;
        }
        let filter = Arc::clone(&self.opts.filter);
        match LiveWatcher::start(&self.root, filter, self.bell.0.clone()) {
            Ok(w) => {
                self.watcher = Some(w);
                self.watcher_error = None;
                self.opts.settle = Some(self.settle);
                true
            }
            Err(e) => {
                self.watcher_error = Some(e.to_string());
                false
            }
        }
    }

    /// When to [`LocalObserver::poll`] even if the bell didn't ring: a recheck of new
    /// folders or of a file being written, a journal position to store, or events a cut-off
    /// batch left queued.
    pub fn deadline(&self) -> Option<Instant> {
        let journal_due = self.unsaved
            && self.journal.is_some()
            && self.watcher.is_some()
            && self.settling.is_none();
        [
            self.recheck.as_ref().map(|(at, _)| *at),
            self.young.values().map(|w| w.at).min(),
            self.settling.as_ref().map(|(_, at)| *at),
            (self.more || journal_due).then(Instant::now),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Whether the ignore rules' file was touched since the last call (only seen with a
    /// watcher).
    pub fn rules_touched(&self) -> bool {
        self.watcher
            .as_ref()
            .is_some_and(|w| w.rules.swap(false, Ordering::SeqCst))
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

    /// The OS watcher answers with nothing happening on disk: watching, configuring,
    /// unwatching, and dropping it. On Windows its thread waits without a timeout
    /// (`vendor/notify/PATCHED.md`), so a call it isn't woken for would hang here.
    #[test]
    fn an_idle_watcher_answers_every_call() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let dir = tempfile::tempdir().expect("temp dir");
            let mut watcher =
                notify::recommended_watcher(|_: notify::Result<Event>| {}).expect("a watcher");
            watcher
                .watch(dir.path(), RecursiveMode::Recursive)
                .expect("watched");
            let _ = watcher.configure(notify::Config::default());
            watcher.unwatch(dir.path()).expect("unwatched");
            drop(watcher);
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("every call answered");
    }

    #[test]
    fn events_name_their_entries() {
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
        // The root itself, and a name that isn't valid: their folders.
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Modify(ModifyKind::Any), &["/sync"]),
        );
        let long = format!("/sync/v/{}", "n".repeat(300));
        add_event(
            root,
            &excl,
            &mut batch,
            ev(EventKind::Create(CreateKind::File), &[long.as_str()]),
        );
        let entries: BTreeSet<_> = [
            rp("a/b/new.txt"),
            rp("x/f"),
            rp("y/f"),
            rp("top.txt"),
            rp("w/f"),
        ]
        .into();
        assert_eq!(
            batch,
            WatchBatch {
                dirs: [RelPath::root(), rp("v")].into(),
                entries,
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
        let mut watcher = LiveWatcher::start(&root, Arc::default(), None)?;
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
            watcher.next_batch(Duration::from_millis(200), debounce, || {}),
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
            .next_batch(Duration::from_millis(200), debounce, || {})
            .ok_or("no batch")?;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert!(batch.drained);
        assert_eq!(batch.entries, [rp("new")].into());

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
        let mut watcher = LiveWatcher::start(&root, Arc::default(), None)?;
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
            .next_batch(Duration::from_secs(1), debounce, || {})
            .ok_or("no batch")?;
        assert_eq!(batch.entries, [rp("a/first"), rp("b/behind")].into());
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

    /// A large file being written is left unhashed at each batch, and read by a look of the
    /// observer's own once it has been quiet long enough: no event comes for that.
    #[test]
    fn a_file_being_written_is_read_once_it_is_quiet() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let mut opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        opts.settle = Some(Settle {
            from_size: 1,
            quiet: Duration::from_millis(500),
        });
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        observer.catch_up(&mut db)?;
        // Only the events this test sends arrive.
        let (tx, rx) = channel();
        observer.watcher.as_mut().ok_or("no watcher")?.rx = rx;
        let quick = Debounce {
            quiet: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
        };
        let path = root.join("big");
        let mut file = fs::File::create(&path)?;
        for step in 0..3 {
            std::io::Write::write_all(&mut file, b"some more bytes")?;
            let event = Event::new(EventKind::Modify(ModifyKind::Any)).add_path(path.clone());
            tx.send(Ok(event))?;
            let report = observer
                .poll(&mut db, Duration::from_secs(1), quick)?
                .ok_or("no batch")?;
            assert_eq!(report.hashed, 0, "step {step}: {report:?}");
            assert_eq!(report.young.len(), 1, "step {step}: {report:?}");
            assert!(observer.deadline().is_some());
        }
        let row = db.read(|s| s.get_local(&rp("big")))?.ok_or("no row")?;
        assert_eq!(row.hash, None);

        // No more writes, and no event: read by the observer's look.
        let until = Instant::now() + Duration::from_secs(10);
        let mut hashed = 0;
        while db
            .read(|s| s.get_local(&rp("big")))?
            .and_then(|r| r.hash)
            .is_none()
        {
            assert!(Instant::now() < until, "never read");
            if let Some(report) = observer.poll(&mut db, Duration::from_millis(50), quick)? {
                hashed += report.hashed;
            }
        }
        assert_eq!(hashed, 1);
        assert!(observer.young.is_empty());
        Ok(())
    }

    /// The looks at a file being written: the first once it may have been quiet long enough;
    /// one finding it written to since waits 3, 6, 12 … 60 s for the next, one finding it
    /// unchanged only till it has been quiet long enough, and a batch moves none. Read or gone,
    /// it is let go.
    #[test]
    fn a_look_that_finds_a_file_still_written_waits_longer()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let opts = ScanOptions::for_root(dir.path(), hbsync_fileio::HashAlgorithm::Blake3);
        let mut observer = LocalObserver::start(dir.path(), opts);
        let a = rp("a");
        let seen = |mtime_ns: i64, left: u64| ScanReport {
            young: vec![crate::scan::Young {
                path: rp("a"),
                mtime_ns,
                left: Duration::from_secs(left),
            }],
            ..ScanReport::default()
        };
        let batch: &[RelPath] = &[];
        let looked = [a.clone()];
        // How long till the next look, in whole seconds, and the wait after it.
        let next = |o: &LocalObserver| -> Option<(u64, u64)> {
            let w = o.young.get(&rp("a"))?;
            let left = w.at.saturating_duration_since(Instant::now());
            Some((
                (left + Duration::from_millis(500)).as_secs(),
                w.wait.as_secs(),
            ))
        };
        observer.note_young(&seen(1, 4), batch);
        assert_eq!(next(&observer), Some((4, 3)));
        // A batch sees it again, written to: its look stays.
        observer.note_young(&seen(2, 5), batch);
        assert_eq!(next(&observer), Some((4, 3)));
        observer.note_young(&seen(3, 5), &looked);
        assert_eq!(next(&observer), Some((5, 6)));
        observer.note_young(&seen(4, 5), &looked);
        assert_eq!(next(&observer), Some((6, 12)));
        observer.note_young(&seen(5, 5), &looked);
        assert_eq!(next(&observer), Some((12, 24)));
        // Unchanged since the last look: only till it is quiet.
        observer.note_young(&seen(5, 2), &looked);
        assert_eq!(next(&observer), Some((2, 24)));
        // A batch saw it written to; quiet since: only till it is quiet.
        observer.note_young(&seen(6, 5), batch);
        observer.note_young(&seen(6, 3), &looked);
        assert_eq!(next(&observer), Some((3, 24)));
        for mtime in 7..11 {
            observer.note_young(&seen(mtime, 5), &looked);
        }
        assert_eq!(next(&observer), Some((60, 60)));
        // Read (or gone): let go.
        observer.note_young(&ScanReport::default(), &looked);
        assert_eq!(next(&observer), None);
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
        db.write(|s| s.folder().clear_dirty(node))?;

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

    /// Idle, the observer neither rings nor asks to be polled; a change rings once however
    /// many follow before the poll, and so does a touch of the ignore rules.
    #[test]
    fn the_bell_rings_once_per_poll_and_never_while_idle() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::sync::atomic::AtomicUsize;
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(".hbsync"))?;
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let rang = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&rang);
        let bell: Bell = Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        let mut observer = LocalObserver::ringing(&root, opts, Some(bell));
        observer.journal = None;
        observer.catch_up(&mut db)?;
        let quick = Debounce {
            quiet: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        };
        let wait_for = |what: &str, done: &dyn Fn() -> bool| -> Result<(), String> {
            let until = Instant::now() + Duration::from_secs(10);
            while !done() {
                if Instant::now() > until {
                    return Err(format!("timed out waiting until {what}"));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        };
        // Whatever starting the watch delivered.
        std::thread::sleep(Duration::from_millis(300));
        while observer.poll(&mut db, Duration::ZERO, quick)?.is_some() {}
        rang.store(0, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(rang.load(Ordering::SeqCst), 0, "idle: no ring");
        assert_eq!(observer.deadline(), None, "idle: nothing to poll for");
        assert!(observer.poll(&mut db, Duration::ZERO, quick)?.is_none());

        fs::write(root.join("a.txt"), "a")?;
        wait_for("a change rings", &|| rang.load(Ordering::SeqCst) == 1)?;
        fs::write(root.join("b.txt"), "b")?;
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(rang.load(Ordering::SeqCst), 1, "once until the poll");
        assert!(observer.poll(&mut db, Duration::ZERO, quick)?.is_some());
        for name in ["a.txt", "b.txt"] {
            assert!(db.read(|s| s.get_local(&rp(name)))?.is_some(), "{name}");
        }
        // FSEvents can deliver `.hbsync`'s creation late, or fold it into a rescan, so on
        // macOS a stray touch may show here (it costs one read of the rules, no sync). Taken
        // either way, so the rules' own write below is what sets it.
        let touched = observer.rules_touched();
        if !cfg!(target_os = "macos") {
            assert!(!touched, "a file's change touched the rules");
        }

        fs::write(root.join(".hbsync").join("ignore"), "*.tmp\n")?;
        wait_for("the rules ring", &|| rang.load(Ordering::SeqCst) == 2)?;
        assert!(observer.rules_touched());
        assert!(!observer.rules_touched(), "taken");

        // Reading them (as the runner does to compare them) is no change. The write's last
        // events may still come first.
        std::thread::sleep(Duration::from_millis(300));
        while observer.poll(&mut db, Duration::ZERO, quick)?.is_some() {}
        observer.rules_touched();
        let before = rang.load(Ordering::SeqCst);
        fs::read_to_string(root.join(".hbsync").join("ignore"))?;
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(rang.load(Ordering::SeqCst), before, "a read rang");
        assert!(!observer.rules_touched());
        Ok(())
    }

    /// A watcher whose OS side stopped is dropped after one full rescan: the folder is left
    /// to the look, not rescanned in a loop.
    #[test]
    fn a_watcher_that_stopped_is_dropped() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        // As on macOS and Windows: a journal.
        observer.journal = Some(Box::new(ClockJournal(Instant::now())));
        observer.watcher.as_mut().ok_or("no watcher")?.rx = channel().1;
        assert!(
            observer
                .poll(&mut db, Duration::ZERO, Debounce::default())?
                .is_some()
        );
        assert!(!observer.has_watcher());
        assert!(observer.watcher_error.is_some());
        assert_eq!(observer.deadline(), None);
        Ok(())
    }

    /// A batch whose journal position wasn't stored asks to be polled: a position is taken
    /// then, and stored once a quiet period has passed with no change. Polled as the runner
    /// does, at the deadline the observer gives; only the events the test sends arrive (an OS
    /// may still deliver some of its own, FSEvents a moment late).
    #[test]
    fn an_unsaved_position_is_stored_after_a_quiet_period() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        let mut observer = LocalObserver::start(&root, opts);
        observer.journal = Some(Box::new(ClockJournal(Instant::now())));
        let (_tx, rx) = channel();
        observer.watcher.as_mut().ok_or("no watcher")?.rx = rx;
        let quiet = Debounce {
            quiet: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        };
        let stored = |db: &mut StateDb| db.read(|s| s.get_kv(keys::JOURNAL_CURSOR));
        observer.unsaved = true;
        assert!(observer.deadline().is_some_and(|at| at <= Instant::now()));
        let taken = Instant::now();
        assert!(observer.poll(&mut db, Duration::ZERO, quiet)?.is_none());
        let at = observer.deadline().ok_or("no deadline")?;
        assert!(at >= taken + quiet.quiet, "a quiet period later");
        // Until it is stored, as the runner polls: never before the quiet period.
        let until = Instant::now() + Duration::from_secs(10);
        while stored(&mut db)?.is_none() {
            assert!(Instant::now() < until, "never stored");
            let polled = Instant::now();
            assert!(observer.poll(&mut db, Duration::ZERO, quiet)?.is_none());
            if stored(&mut db)?.is_some() {
                assert!(polled >= at, "stored before the quiet period");
                break;
            }
            let next = observer.deadline().ok_or("no deadline while unsaved")?;
            std::thread::sleep(next.saturating_duration_since(Instant::now()));
        }
        assert_eq!(observer.deadline(), None, "nothing more to do");
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

    /// A file never quiet long enough is read all the same once it has been left for
    /// [`READ_ANYWAY`], and left as long again before the next such read.
    #[test]
    fn a_file_never_quiet_is_read_anyway() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let mut opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        opts.settle = Some(Settle {
            from_size: 1,
            quiet: Duration::from_secs(3600),
        });
        let mut db = StateDb::open_in_memory()?;
        fs::write(root.join("log"), b"appended all the time")?;
        let mut observer = LocalObserver::start(&root, opts.clone());
        assert_eq!(scan(&mut db, &root, &opts)?.young.len(), 1);
        let mtime_ns = hbsync_platform::stat(&root.join("log"))?.mtime_ns;
        let long_ago = Instant::now()
            .checked_sub(READ_ANYWAY + Duration::from_secs(1))
            .ok_or("clock")?;
        let mut waiting = Waiting::first(Instant::now(), mtime_ns);
        waiting.since = long_ago;
        observer.young.insert(rp("log"), waiting);
        let report = observer
            .poll(&mut db, Duration::ZERO, Debounce::default())?
            .ok_or("not read")?;
        assert_eq!((report.hashed, report.young.len()), (1, 0), "{report:?}");
        assert!(observer.young.is_empty());
        Ok(())
    }

    /// A journal that names nothing: no change since the stored position.
    #[derive(Debug)]
    struct StillJournal;

    impl ChangeJournal for StillJournal {
        fn now(&mut self) -> Result<String, hbsync_platform::journal::JournalError> {
            Ok("still".into())
        }
        fn since(
            &mut self,
            _: &str,
        ) -> Result<hbsync_platform::journal::JournalDelta, hbsync_platform::journal::JournalError>
        {
            Ok(hbsync_platform::journal::JournalDelta {
                cursor: "still".into(),
                ..Default::default()
            })
        }
    }

    /// A file a scan left unhashed while it was written, the client stopped before it looked
    /// again: the catch-up after a restart reads only what the journal names, which isn't it
    /// (its last change came before the stored position). The observer looks at it again
    /// itself, at its first poll.
    #[test]
    fn a_file_left_unhashed_before_a_restart_is_looked_at_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let mut opts = ScanOptions::for_root(&root, hbsync_fileio::HashAlgorithm::Blake3);
        let mut db = StateDb::open_in_memory()?;
        fs::write(root.join("big"), b"being written")?;
        opts.settle = Some(Settle {
            from_size: 1,
            quiet: Duration::from_secs(60),
        });
        let report = scan(&mut db, &root, &opts)?;
        assert_eq!(report.young.len(), 1, "{report:?}");
        db.write(|s| s.folder().set_kv(keys::JOURNAL_CURSOR, "still"))?;
        // Quiet since.
        let old =
            hbsync_platform::meta::system_time_ns(std::time::SystemTime::now()) - 120_000_000_000;
        hbsync_platform::ops::set_mtime(&root.join("big"), old)?;

        let mut observer = LocalObserver::start(&root, opts);
        observer.journal = Some(Box::new(StillJournal));
        let caught = observer.catch_up(&mut db)?;
        assert_eq!(caught.tier, crate::Tier::Journal { dirs: 0 });
        assert!(observer.deadline().is_some_and(|at| at <= Instant::now()));
        let report = observer
            .poll(&mut db, Duration::ZERO, Debounce::default())?
            .ok_or("not looked at")?;
        assert_eq!(report.hashed, 1, "{report:?}");
        let row = db.read(|s| s.get_local(&rp("big")))?.ok_or("no row")?;
        assert!(row.hash.is_some());
        Ok(())
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
                .read(|s| s.get_kv(keys::JOURNAL_CURSOR))
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
            db.read(|s| s.get_kv(keys::JOURNAL_CURSOR))?.is_some(),
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

/// Tests: a watcher that can't start, or stops (per folder: tests run side by side).
#[cfg(feature = "testkit")]
pub mod testkit {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(crate) static FAIL: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    pub(crate) static STOP: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    /// Whether any is asked for: the hook costs a batch nothing otherwise.
    static ANY: AtomicBool = AtomicBool::new(false);

    /// The next watcher started on `root` fails as out of inotify instances (`EMFILE`).
    pub fn fail_next_start(root: &Path) {
        add(&FAIL, root);
    }

    /// The watcher on `root` stops at its next batch, as one whose thread ended.
    pub fn stop(root: &Path) {
        add(&STOP, root);
    }

    fn add(list: &Mutex<Vec<PathBuf>>, root: &Path) {
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        list.lock().unwrap_or_else(|e| e.into_inner()).push(root);
        ANY.store(true, Ordering::SeqCst);
    }

    pub(crate) fn take(list: &Mutex<Vec<PathBuf>>, root: &Path) -> bool {
        if !ANY.load(Ordering::SeqCst) {
            return false;
        }
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        let mut list = list.lock().unwrap_or_else(|e| e.into_inner());
        match list.iter().position(|r| *r == root) {
            Some(i) => {
                list.swap_remove(i);
                true
            }
            None => false,
        }
    }
}
