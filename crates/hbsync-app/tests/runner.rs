//! The runner on real folders and the fake server: syncing on its own, pause and resume, sync
//! now, confirming held deletes, ignore rules edited while it runs.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_app::daemon::{Notifications, Puller, UNANNOUNCED_READ};
use hbsync_app::protocol::Pause;
use hbsync_app::runner::RunnerError;
use hbsync_app::{Command, FolderStatus, Handle, HeldAnswer, Runner, RunnerConfig, State};
use hbsync_engine::Drive;
use hbsync_engine::root::{AccountReader, RootError, SyncRoot};
use hbsync_homebase::notify::Event;
use hbsync_remote::ChangeRef;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_types::RemoteFileId;

type R = Result<(), Box<dyn std::error::Error>>;

const PATIENCE: Duration = Duration::from_secs(30);

struct Device {
    root: PathBuf,
    handle: Handle,
    thread: Option<JoinHandle<Result<(), RunnerError>>>,
    drive: Arc<Drive>,
    /// The drive's client of the fake server: its own registration, and its counters.
    remote: FakeRemote,
    /// Last: removed after the drive has closed the state DB in it (Windows can't remove an
    /// open file).
    _dir: tempfile::TempDir,
}

impl Device {
    fn start(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
        let mut cfg = RunnerConfig::new(&root, &db_of(&root), &"test.example".into(), name);
        cfg.tweak = Some(|c| c.exec.retry_pause = Duration::from_millis(1));
        let remote = FakeRemote::new(server.clone());
        let drive = Drive::open(&cfg.db, &cfg.identity, Arc::new(remote.clone()))?;
        let (handle, thread) = Runner::spawn(cfg, Arc::clone(&drive))?;
        Ok(Device {
            root,
            handle,
            thread: Some(thread),
            drive,
            remote,
            _dir: dir,
        })
    }

    /// The account's puller, as the daemon runs it.
    fn puller(&self) -> Puller {
        Puller::spawn(Arc::clone(&self.drive))
    }

    /// The account's notifications as the daemon takes them, from the server's announcements
    /// to this device's client, its own writes left out (`excludeOwnWrites`). Connected from
    /// the start; stops when dropped.
    fn listen(&self) -> Listening {
        let events = self.remote.subscribe(true);
        let puller = Puller::spawn(Arc::clone(&self.drive));
        let heard = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (log, stopped) = (Arc::clone(&heard), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut n = Notifications::new(puller, Instant::now());
            n.event(Event::Connected, Instant::now(), false);
            while !stopped.load(Ordering::Relaxed) {
                match events.recv_timeout(Duration::from_millis(50)) {
                    Ok(change) => {
                        log.lock().unwrap_or_else(|e| e.into_inner()).push(change);
                        n.event(Event::Changed(Some(change)), Instant::now(), false);
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                }
                n.tick(Instant::now(), false);
            }
        });
        Listening { heard, stop }
    }

    /// The account's notifications, as the daemon runs them (none come in these tests).
    fn notifications(&self, now: Instant) -> Notifications {
        Notifications::new(self.puller(), now)
    }

    /// Waits until it is idle with nothing pending.
    fn wait_idle(&self) -> R {
        wait_until("idle", || {
            let s = self.status();
            s.state == State::Idle && s.pending == 0
        })
    }

    /// The feed pages read so far: slim and whole.
    fn feed_reads(&self) -> usize {
        let faults = self.remote.faults();
        faults.ref_pages + faults.full_pages
    }

    /// What the account does when its notifications connect (again).
    fn connected(&self) {
        let now = Instant::now();
        self.notifications(now).event(Event::Connected, now, false);
    }

    fn write(&self, rel: &str, data: &str) -> std::io::Result<()> {
        let path = self.root.join(rel);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, data)
    }

    fn has(&self, rel: &str) -> bool {
        self.root.join(rel).exists()
    }

    fn status(&self) -> FolderStatus {
        self.handle.status()
    }

    fn stop(&mut self) -> R {
        assert!(self.handle.send(Command::Stop));
        if let Some(t) = self.thread.take() {
            t.join().map_err(|_| "the runner panicked")??;
        }
        assert_eq!(self.status().state, State::Stopped);
        Ok(())
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.handle.send(Command::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// A device's notifications ([`Device::listen`]).
struct Listening {
    /// The changes announced to it so far.
    heard: Arc<Mutex<Vec<ChangeRef>>>,
    stop: Arc<AtomicBool>,
}

impl Listening {
    /// The files announced so far.
    fn files(&self) -> HashSet<RemoteFileId> {
        let heard = self.heard.lock().unwrap_or_else(|e| e.into_inner());
        heard.iter().map(|c| c.file_id).collect()
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

fn live_names(server: &Arc<Mutex<FakeServer>>) -> Vec<String> {
    server.lock().map(|s| s.live_names()).unwrap_or_default()
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) -> R {
    let until = Instant::now() + PATIENCE;
    while !done() {
        if Instant::now() > until {
            return Err(format!("timed out waiting until {what}").into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn on_server(server: &Arc<Mutex<FakeServer>>, name: &str) -> bool {
    live_names(server).iter().any(|n| n == name)
}

/// No poll: an idle runner with a watcher doesn't wake up at all, and a change wakes it at
/// once.
#[test]
fn an_idle_runner_never_wakes_up() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    // Ignore rules, read when they change: reading them is no change.
    a.write(".hbsync/ignore", "*.log\n")?;
    wait_until("the rule is counted", || a.status().rules == 1)?;
    a.wait_idle()?;
    // Whatever its first sync left (a recheck of new folders, events of its own writes).
    std::thread::sleep(Duration::from_secs(2));
    let woken = a.handle.woken();
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(a.handle.woken(), woken, "woke up while idle");

    a.write("new.txt", "new")?;
    wait_until("the change is uploaded", || on_server(&server, "new.txt"))?;
    assert!(a.handle.woken() > woken);
    Ok(())
}

#[test]
fn syncs_by_itself_and_pauses_and_resumes() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let mut b = Device::start(&server, "b")?;
    a.write("one.txt", "1")?;
    wait_until("one.txt is up", || on_server(&server, "one.txt"))?;
    // b's account hears of it (no notifications in these tests).
    b.connected();
    wait_until("b has one.txt", || b.has("one.txt"))?;
    wait_until("b is idle", || b.status().state == State::Idle)?;
    assert!(b.status().last_sync.is_some());

    b.handle.send(Command::Pause(Pause::UNTIL_RESUMED));
    assert_eq!(b.status().state, State::Paused, "shown at once");
    a.write("two.txt", "2")?;
    wait_until("two.txt is up", || on_server(&server, "two.txt"))?;
    std::thread::sleep(Duration::from_secs(1));
    assert!(!b.has("two.txt"), "b synced while paused");
    // Local changes on a paused device wait too.
    b.write("three.txt", "3")?;
    std::thread::sleep(Duration::from_secs(1));
    assert!(!on_server(&server, "three.txt"));

    b.handle.send(Command::Resume);
    wait_until("b has two.txt", || b.has("two.txt"))?;
    wait_until("three.txt is up", || on_server(&server, "three.txt"))?;
    b.stop()?;
    Ok(())
}

/// Each computer is told of the other's writes and not of its own (`excludeOwnWrites`): a
/// change made on one reaches the other through its announcement alone, and the writer's own
/// syncs read no feed at all (each write, an own removal's time included, is recorded from
/// its answer); the feed is read for the other's news.
#[test]
fn another_computers_changes_are_announced_and_own_ones_not() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    let (heard_a, heard_b) = (a.listen(), b.listen());
    wait_until("both started", || {
        a.status().last_sync.is_some() && b.status().last_sync.is_some()
    })?;
    let file_of = |name: &str| -> Option<RemoteFileId> {
        let s = server.lock().ok()?;
        s.live()
            .chain(s.removed())
            .find(|h| matches!(&h.body, hbsync_remote::RemoteBody::Live { content, .. } if content.name == name))
            .map(|h| h.file_id)
    };
    // The reads at connecting done.
    std::thread::sleep(Duration::from_millis(300));
    a.wait_idle()?;
    let quiet = a.feed_reads();
    a.write("from-a.txt", "a")?;
    wait_until("b has from-a.txt", || b.has("from-a.txt"))?;
    a.wait_idle()?;
    assert_eq!(a.feed_reads(), quiet, "a's own upload read the feed");
    b.write("from-b.txt", "b")?;
    wait_until("a has from-b.txt", || a.has("from-b.txt"))?;
    a.wait_idle()?;
    let news = a.feed_reads();
    assert!(news > quiet, "b's news was read");
    std::fs::remove_file(a.root.join("from-a.txt"))?;
    wait_until("b lost from-a.txt", || !b.has("from-a.txt"))?;
    a.wait_idle()?;
    assert_eq!(a.feed_reads(), news, "a's own removal read the feed");
    for d in [&a, &b] {
        d.wait_idle()?;
    }
    let (ours, theirs) = (
        file_of("from-a.txt").ok_or("from-a.txt")?,
        file_of("from-b.txt").ok_or("from-b.txt")?,
    );
    assert_eq!(heard_a.files(), HashSet::from([theirs]), "a heard only b's");
    assert_eq!(heard_b.files(), HashSet::from([ours]), "b heard only a's");
    // a's removal has the server's time, from its answer.
    let deleted: Vec<_> = AccountReader::open(&db_of(&a.root), "test.example")?
        .ok_or("no state DB")?
        .deletes(100)?
        .0
        .into_iter()
        .map(|(d, _)| d)
        .collect();
    assert!(
        deleted
            .iter()
            .any(|d| d.path == "from-a.txt" && d.removed_at_ms.is_some()),
        "{deleted:?}"
    );
    Ok(())
}

#[test]
fn sync_now_reads_the_drive_unannounced() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    wait_until("b started", || b.status().last_sync.is_some())?;
    a.write("news.txt", "hot")?;
    wait_until("news.txt is up", || on_server(&server, "news.txt"))?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !b.has("news.txt"),
        "not announced, and no poll: b can't know yet"
    );
    b.handle.send(Command::SyncNow);
    wait_until("b has news.txt", || b.has("news.txt"))?;
    Ok(())
}

/// Changes made while the notifications were down weren't announced: the reconnect catches
/// up on all of them from the feed's cursor, as at start.
#[test]
fn a_reconnect_catches_up_on_what_wasnt_announced() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    wait_until("b started", || b.status().last_sync.is_some())?;
    for name in ["x.txt", "y.txt", "z.txt"] {
        a.write(name, name)?;
    }
    wait_until("all three are up", || live_names(&server).len() == 3)?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!b.has("x.txt"), "nothing announced yet");
    // The notifications connect again: the account pulls, which wakes the folder.
    b.connected();
    wait_until("b has all three", || {
        ["x.txt", "y.txt", "z.txt"].iter().all(|n| b.has(n))
    })?;
    Ok(())
}

/// The drive is read on a timer only while the notifications aren't connected (a proxy, a
/// refusal), and not while paused; never while they are connected.
#[test]
fn the_drive_is_read_on_a_timer_only_while_not_connected() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    wait_until("b started", || b.status().last_sync.is_some())?;
    let t0 = Instant::now();
    let mut n = b.notifications(t0);
    a.write("x.txt", "x")?;
    wait_until("x.txt is up", || on_server(&server, "x.txt"))?;
    n.tick(t0 + UNANNOUNCED_READ - Duration::from_millis(1), false);
    n.tick(t0 + UNANNOUNCED_READ, true);
    std::thread::sleep(Duration::from_millis(500));
    assert!(!b.has("x.txt"), "too soon, or paused");
    n.tick(t0 + UNANNOUNCED_READ, false);
    wait_until("b read the drive", || b.has("x.txt"))?;
    wait_until("b is idle", || b.status().state == State::Idle)?;

    // Connected while paused: the read that follows connecting waits for the resume.
    n.event(Event::Connected, t0, true);
    a.write("y.txt", "y")?;
    wait_until("y.txt is up", || on_server(&server, "y.txt"))?;
    std::thread::sleep(Duration::from_millis(300));
    assert!(!b.has("y.txt"), "read while paused");
    n.tick(t0, false);
    wait_until("the read after connecting", || b.has("y.txt"))?;
    b.wait_idle()?;
    // Connected: no timer any more.
    a.write("z.txt", "z")?;
    wait_until("z.txt is up", || on_server(&server, "z.txt"))?;
    n.tick(t0 + UNANNOUNCED_READ * 10, false);
    std::thread::sleep(Duration::from_millis(500));
    assert!(!b.has("z.txt"), "read on a timer while connected");
    Ok(())
}

/// A sync's own writes come back from the watcher, changing nothing: they start no sync (one
/// would read the feed unannounced).
#[test]
fn a_syncs_own_writes_start_no_sync() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    wait_until("b started", || b.status().last_sync.is_some())?;
    a.write("x.txt", "x")?;
    wait_until("x.txt is up", || on_server(&server, "x.txt"))?;
    b.handle.send(Command::SyncNow);
    wait_until("b has x.txt", || b.has("x.txt"))?;
    wait_until("b is idle", || b.status().state == State::Idle)?;
    let reads = b.feed_reads();
    // The watcher's quiet period and a few of the runner's ticks.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(b.feed_reads(), reads, "the download's echo started a sync");
    Ok(())
}

/// A change announced that the drive has already (its own write coming back, from a server
/// that doesn't leave them out) brings no read of the feed; one it doesn't have is read and
/// synced.
#[test]
fn announced_changes_are_read_only_when_new() -> R {
    use hbsync_remote::{ChangeRef, Remote, RemoteBody};
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    // As the server announces a file: a minimal event's row.
    let announce = |name: &str| -> Result<Event, Box<dyn std::error::Error>> {
        let page = FakeRemote::new(Arc::clone(&server)).changes(None, 10_000)?;
        let header = page
            .entries
            .iter()
            .find(|h| matches!(&h.body, RemoteBody::Live { content, .. } if content.name == name))
            .ok_or(format!("{name} isn't on the server"))?;
        Ok(Event::Changed(Some(ChangeRef::of(header))))
    };
    b.write("mine.txt", "m")?;
    wait_until("mine.txt is up", || on_server(&server, "mine.txt"))?;
    b.wait_idle()?;
    let t0 = Instant::now();
    let puller = b.puller();
    let mut n = Notifications::new(puller.clone(), t0);
    n.event(Event::Connected, t0, false);
    wait_until("the read after connecting", || puller.rounds() == 1)?;
    a.write("x.txt", "x")?;
    wait_until("x.txt is up", || on_server(&server, "x.txt"))?;
    let reads = b.feed_reads();
    n.event(announce("mine.txt")?, t0, false);
    n.tick(t0 + Duration::from_secs(4), false);
    wait_until("the puller took it", || puller.rounds() == 2)?;
    assert_eq!(b.feed_reads(), reads, "the feed was read for b's own write");
    assert!(!b.has("x.txt"));

    n.event(announce("x.txt")?, t0, false);
    n.tick(t0 + Duration::from_secs(6), false);
    wait_until("b read the drive", || b.has("x.txt"))?;
    assert!(b.feed_reads() > reads, "x.txt is new: read");
    Ok(())
}

/// A hard delete announced just before the notifications reconnect is still taken: the read
/// at the reconnect can't bring it (it is in no feed).
#[test]
fn a_hard_delete_announced_before_a_reconnect_is_taken() -> R {
    use hbsync_remote::{ChangeRef, RemoteBody};
    let server = server();
    let a = Device::start(&server, "a")?;
    let b = Device::start(&server, "b")?;
    a.write("keep.txt", "k")?;
    a.write("z.txt", "z")?;
    wait_until("both are up", || live_names(&server).len() == 2)?;
    b.connected();
    wait_until("b has z.txt", || b.has("z.txt"))?;
    // Its sync that brought z.txt done (the delete isn't to race with that sync).
    b.wait_idle()?;
    let gone = {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        let header = s
            .live()
            .find(
                |h| matches!(&h.body, RemoteBody::Live { content, .. } if content.name == "z.txt"),
            )
            .cloned()
            .ok_or("z.txt")?;
        s.hard_delete(header.file_id, header.version)?;
        ChangeRef::of(&header).tombstone()
    };
    let now = Instant::now();
    let mut n = b.notifications(now);
    n.event(Event::Changed(Some(gone)), now, false);
    n.event(Event::Connected, now, false);
    wait_until("b let z.txt go", || !b.has("z.txt"))
        .map_err(|e| format!("{e}; b: {:?}", b.status()))?;
    assert!(b.has("keep.txt"));
    Ok(())
}

#[test]
fn held_deletes_go_through_once_confirmed_as_shown() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    for i in 0..60 {
        a.write(&format!("f{i:02}.txt"), &i.to_string())?;
    }
    wait_until("all 60 are up", || live_names(&server).len() == 60)?;
    a.wait_idle()?;
    for i in 0..60 {
        std::fs::remove_file(a.root.join(format!("f{i:02}.txt")))?;
    }
    wait_until("the deletes are held", || {
        a.status().held_deletes.is_some_and(|h| h.here == 60)
    })?;
    let held = a.status().held_deletes.ok_or("held")?;
    assert_eq!(held.remotely, 0);
    assert_eq!(held.examples.len(), 10);
    assert!(
        held.examples
            .iter()
            .all(|p| p.starts_with('f') && p.ends_with(".txt"))
    );
    assert_eq!(live_names(&server).len(), 60, "nothing was deleted");

    // A confirmation of something else (the held deletes changed since) is refused.
    let refused = a.handle.answer(HeldAnswer::Delete, "0000000000000000-60");
    assert!(refused.is_err_and(|why| why.contains("changed since")));

    a.handle.answer(HeldAnswer::Delete, &held.token)?;
    // One answer: a second one to the same (answered already, or gone since) is refused.
    assert!(a.handle.answer(HeldAnswer::PutBack, &held.token).is_err());
    wait_until("the drive is empty", || live_names(&server).is_empty())?;
    wait_until("nothing is held", || a.status().held_deletes.is_none())?;
    Ok(())
}

/// Held deletes put back as shown (sync semantics §7): the files come down again, nothing on
/// the drive changed. Answered while paused, they come back once syncing resumes.
#[test]
fn held_deletes_put_back_come_back() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    for i in 0..60 {
        a.write(&format!("f{i:02}.txt"), &i.to_string())?;
    }
    wait_until("all 60 are up", || live_names(&server).len() == 60)?;
    a.wait_idle()?;
    for i in 0..60 {
        std::fs::remove_file(a.root.join(format!("f{i:02}.txt")))?;
    }
    wait_until("the deletes are held", || {
        a.status().held_deletes.is_some_and(|h| h.here == 60)
    })?;
    let held = a.status().held_deletes.ok_or("held")?;
    a.handle.send(Command::Pause(Pause::UNTIL_RESUMED));
    a.handle.answer(HeldAnswer::PutBack, &held.token)?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!a.root.join("f59.txt").exists(), "put back while paused");
    a.handle.send(Command::Resume);
    wait_until("they are back", || a.root.join("f59.txt").exists())?;
    wait_until("nothing is held", || a.status().held_deletes.is_none())?;
    assert_eq!(live_names(&server).len(), 60, "nothing was deleted");
    assert_eq!(std::fs::read_to_string(a.root.join("f07.txt"))?, "7");
    Ok(())
}

/// An answer taken while a sync runs that holds more deletes still goes, for what the user
/// saw: those go from the drive, and the ones held since stay held, shown again.
#[test]
fn an_answer_during_a_sync_that_holds_more_covers_what_was_shown() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
    let mut cfg = RunnerConfig::new(&root, &db_of(&root), &"test.example".into(), "a");
    cfg.tweak = Some(|c| c.exec.transfer.max_bytes_per_sec = Some(300_000));
    let (handle, _thread) = spawn(cfg, &server)?;
    for i in 0..150 {
        std::fs::write(root.join(format!("f{i:03}.txt")), i.to_string())?;
    }
    wait_until("all 150 are up", || live_names(&server).len() == 150)?;
    for i in 0..60 {
        std::fs::remove_file(root.join(format!("f{i:03}.txt")))?;
    }
    wait_until("the deletes are held", || {
        handle.status().held_deletes.is_some_and(|h| h.here == 60)
    })?;
    let seen = handle.status().held_deletes.ok_or("held")?;
    // 60 more deleted, and a slow upload: one sync takes both, and holds 120.
    handle.send(Command::Pause(Pause::UNTIL_RESUMED));
    for i in 60..120 {
        std::fs::remove_file(root.join(format!("f{i:03}.txt")))?;
    }
    std::fs::write(root.join("big.bin"), vec![7u8; 600_000])?;
    // Taken in while paused (the watcher's batches are recorded as they come).
    std::thread::sleep(Duration::from_millis(1500));
    handle.send(Command::Resume);
    wait_until("the upload runs", || {
        handle.status().progress.is_some_and(|p| p.bytes_done > 0)
    })?;
    assert_eq!(
        handle.status().held_deletes,
        Some(seen.clone()),
        "not recorded yet"
    );
    handle.answer(HeldAnswer::Delete, &seen.token)?;
    wait_until("the 60 seen are gone from the drive", || {
        live_names(&server).len() == 150 - 60 + 1
    })?;
    wait_until("the 60 since are held", || {
        handle
            .status()
            .held_deletes
            .is_some_and(|h| h.here == 60 && h.token != seen.token)
    })?;
    let live = live_names(&server);
    assert!(live.iter().any(|n| n == "f060.txt"), "one not seen went");
    assert!(!live.iter().any(|n| n == "f059.txt"));
    handle.send(Command::Stop);
    Ok(())
}

#[test]
fn an_edited_ignore_file_applies_without_a_restart() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    a.write("notes.txt", "keep")?;
    a.write("debug.log", "noise")?;
    a.write("other.txt", "keep")?;
    wait_until("all three are up", || live_names(&server).len() == 3)?;
    assert_eq!(a.status().rules, 0);

    a.write(".hbsync/ignore", "*.log\n")?;
    a.handle.send(Command::SyncNow);
    wait_until("debug.log left the drive", || {
        !on_server(&server, "debug.log")
    })?;
    assert!(a.has("debug.log"), "an ignored file stays on its device");
    assert!(on_server(&server, "notes.txt"));
    wait_until("the rule is counted", || a.status().rules == 1)?;

    // Seen by the watcher: no sync now needed, nor a minute's look.
    a.write("x.tmp", "scratch")?;
    a.write(".hbsync/ignore", "*.log\n*.tmp\n")?;
    wait_until("the second rule is counted", || a.status().rules == 2)?;
    wait_until("x.tmp left the drive", || !on_server(&server, "x.tmp"))?;
    Ok(())
}

/// The account DB of a test folder: beside it.
fn db_of(root: &Path) -> PathBuf {
    root.with_file_name("state.db")
}

/// What setting the folder at `root` up does: its marker, and its row in the account DB.
fn set_up(root: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    SyncRoot::create(&db_of(root), "test.example", root)?;
    Ok(root.to_path_buf())
}

fn spawn_at(
    server: &Arc<Mutex<FakeServer>>,
    root: &Path,
) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
    spawn(
        RunnerConfig::new(root, &db_of(root), &"test.example".into(), "a"),
        server,
    )
}

/// The runner of `root` on `server`, its transfers at 300 kB/s: 600 kB take two seconds.
fn spawn_slow(
    server: &Arc<Mutex<FakeServer>>,
    root: &Path,
) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
    let mut cfg = RunnerConfig::new(root, &db_of(root), &"test.example".into(), "a");
    cfg.tweak = Some(|c| c.exec.transfer.max_bytes_per_sec = Some(300_000));
    spawn(cfg, server)
}

/// The runner of `cfg`, on its account's drive on `server`, without notifications.
fn spawn(
    cfg: RunnerConfig,
    server: &Arc<Mutex<FakeServer>>,
) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
    let drive = drive(&cfg, server)?;
    Runner::spawn(cfg, drive)
}

/// The account's drive of `cfg` on `server`.
fn drive(cfg: &RunnerConfig, server: &Arc<Mutex<FakeServer>>) -> Result<Arc<Drive>, RunnerError> {
    Ok(Drive::open(
        &cfg.db,
        &cfg.identity,
        Arc::new(FakeRemote::new(server.clone())),
    )?)
}

#[test]
fn a_folder_whose_disk_is_not_mounted_is_left_alone() -> R {
    let server = server();
    let parent = tempfile::tempdir()?;
    let root = set_up(&parent.path().canonicalize()?.join("sync"))?;
    let (handle, thread) = spawn_at(&server, &root)?;
    std::fs::write(root.join("a.txt"), "a")?;
    wait_until("a.txt is up", || on_server(&server, "a.txt"))?;
    handle.send(Command::Stop);
    thread.join().map_err(|_| "panicked")??;

    // The disk isn't mounted at the next start: an empty mount point where the folder was.
    let disk = parent.path().join("disk");
    rename_with_retries(&root, &disk)?;
    std::fs::create_dir(&root)?;
    let mut cfg = RunnerConfig::new(&root, &db_of(&root), &"test.example".into(), "a");
    // Looks for the folder again this often.
    cfg.look = Duration::from_millis(300);
    let (handle, thread) = spawn(cfg, &server)?;
    wait_until("the runner says why it waits", || {
        handle
            .status()
            .error
            .is_some_and(|e| e.contains("the folder is gone"))
    })?;
    std::thread::sleep(Duration::from_secs(1));
    assert!(on_server(&server, "a.txt"), "nothing was deleted");
    assert_eq!(
        std::fs::read_dir(&root)?.count(),
        0,
        "nothing was created in the mount point"
    );

    // Mounted again: syncing picks up where it was.
    std::fs::remove_dir(&root)?;
    rename_with_retries(&disk, &root)?;
    std::fs::write(root.join("b.txt"), "b")?;
    wait_until("b.txt is up", || on_server(&server, "b.txt"))?;
    assert!(on_server(&server, "a.txt"));
    handle.send(Command::Stop);
    thread.join().map_err(|_| "panicked")??;
    Ok(())
}

/// Renames a folder, retrying while Windows still has a handle open in it (the watcher).
fn rename_with_retries(from: &Path, to: &Path) -> std::io::Result<()> {
    let mut tries = 0;
    loop {
        match std::fs::rename(from, to) {
            Err(e) if tries < 50 && cfg!(windows) => {
                tries += 1;
                let _ = e;
                std::thread::sleep(Duration::from_millis(100));
            }
            other => return other,
        }
    }
}

#[test]
fn a_folder_synced_with_another_identity_is_refused() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?.join("sync");
    std::fs::create_dir_all(&root)?;
    hbsync_local::Marker::new("other.example").write(&root)?;
    std::fs::write(root.join("theirs.txt"), "not for test.example")?;
    let (handle, thread) = spawn_at(&server, &root)?;
    let err = thread
        .join()
        .map_err(|_| "panicked")?
        .expect_err("the runner refused the folder");
    assert!(
        matches!(err, RunnerError::Folder(RootError::OtherIdentity { .. })),
        "{err}"
    );
    assert_eq!(
        handle.status().state,
        State::Failing,
        "its account tries again later"
    );
    assert!(
        handle
            .status()
            .error
            .is_some_and(|e| e.contains("other.example"))
    );
    assert!(live_names(&server).is_empty(), "nothing was uploaded");
    Ok(())
}

/// A folder whose row in the DB can't be read isn't opened: its runner fails with nothing
/// synced (its account starts it again later), and a runner started once the read works syncs.
#[test]
fn a_folder_whose_row_cant_be_read_syncs_once_it_can() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
    std::fs::write(root.join("a.txt"), "a")?;
    let cfg = RunnerConfig::new(&root, &db_of(&root), &"test.example".into(), "a");
    let drive = drive(&cfg, &server)?;
    let folder = SyncRoot::find(&cfg.db, &cfg.identity, &root)?.folder();
    let raw = rusqlite::Connection::open(&cfg.db)?;
    let set_top = |top: &[u8]| {
        raw.execute(
            "UPDATE folders SET top = ?1 WHERE id = ?2",
            rusqlite::params![top, folder],
        )
    };
    set_top(&[0])?;
    let (handle, thread) = Runner::spawn(cfg.clone(), Arc::clone(&drive))?;
    assert!(thread.join().map_err(|_| "panicked")?.is_err());
    assert_eq!(handle.status().state, State::Failing);
    assert!(live_names(&server).is_empty(), "nothing was uploaded");

    set_top(hbsync_types::NodeId::ROOT.0.as_bytes())?;
    let (handle, thread) = Runner::spawn(cfg, drive)?;
    wait_until("a.txt is on the drive", || on_server(&server, "a.txt"))?;
    assert!(handle.send(Command::Stop));
    thread.join().map_err(|_| "panicked")??;
    Ok(())
}

/// A folder set up again while its runner runs (a new folder in the DB, the open one removed)
/// is opened again at the next sync: its old engine, whose folder is gone, would only fail.
#[test]
fn a_folder_set_up_again_while_it_runs_is_opened_again() -> R {
    let server = server();
    let mut a = Device::start(&server, "a")?;
    a.write("a.txt", "a")?;
    wait_until("a.txt is on the drive", || on_server(&server, "a.txt"))?;
    a.wait_idle()?;
    SyncRoot::create(&db_of(&a.root), "test.example", &a.root)?;
    assert!(a.handle.send(Command::SyncNow));
    // The new folder's start-up adopts what is in it.
    wait_until("the folder is opened again", || {
        a.status().last_sync.is_some_and(|s| s.adopted == 1)
    })?;
    assert_eq!(a.status().error, None);
    a.stop()
}

/// A timed pause shows its end, and holds past it until it is ended: the daemon's one timer
/// sends [`Command::PauseEnded`], not a timer of each folder's.
#[test]
fn a_timed_pause_holds_until_resumed() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    wait_until("a is idle", || a.status().state == State::Idle)?;
    // Its end already passed.
    let until = hbsync_types::unix_now() - 1;
    a.handle.send(Command::Pause(Pause::ending_at(until)));
    assert_eq!(a.status().state, State::Paused);
    wait_until("the pause shows its end", || {
        a.status().paused_until == Some(until)
    })?;
    a.write("later.txt", "x")?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!on_server(&server, "later.txt"), "synced while paused");
    assert_eq!(a.status().state, State::Paused, "past its end");
    a.handle.send(Command::PauseEnded);
    wait_until("later.txt is up", || on_server(&server, "later.txt"))?;
    let status = a.status();
    assert_ne!(status.state, State::Paused);
    assert_eq!(status.paused_until, None);
    Ok(())
}

/// Pause, Resume, Pause queued while a sync runs: the last one holds, and the folder shows it
/// (the Resume lowered the flag the second Pause raised when it was sent).
#[test]
fn pause_resume_pause_queued_during_a_sync_shows_paused() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
    // The runner is busy for two seconds, and the commands queue.
    let (handle, _thread) = spawn_slow(&server, &root)?;
    std::fs::write(root.join("big.bin"), vec![7u8; 600_000])?;
    wait_until("the transfer runs", || {
        handle.status().progress.is_some_and(|p| p.bytes_done > 0)
    })?;
    // The last one timed, so its end shows once the runner took it.
    let until = hbsync_types::unix_now() + 3600;
    handle.send(Command::Pause(Pause::UNTIL_RESUMED));
    handle.send(Command::Resume);
    handle.send(Command::Pause(Pause::ending_at(until)));
    wait_until("the runner took the three", || {
        handle.status().paused_until == Some(until)
    })?;
    assert_eq!(handle.status().state, State::Paused);
    std::fs::write(root.join("later.txt"), "x")?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!on_server(&server, "later.txt"), "synced while paused");
    handle.send(Command::Resume);
    wait_until("later.txt is up", || on_server(&server, "later.txt"))?;
    handle.send(Command::Stop);
    Ok(())
}

#[test]
fn the_recently_synced_list_follows_the_syncs() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    a.write("Docs/one.txt", "1")?;
    a.write("two.txt", "22")?;
    wait_until("both are listed", || a.status().synced_added == 2)?;
    // The list is the folder's state DB's.
    let folder = SyncRoot::find(&db_of(&a.root), "test.example", &a.root)?.folder();
    let recent = AccountReader::open(&db_of(&a.root), "test.example")?
        .ok_or("no DB")?
        .recent_synced(folder, 10)?;
    assert_eq!(recent.len(), 2);
    assert!(
        recent
            .iter()
            .all(|s| s.kind == hbsync_types::activity::SyncedKind::Uploaded)
    );
    // The status shows no progress once idle.
    wait_until("a is idle", || a.status().state == State::Idle)?;
    assert_eq!(a.status().progress, None);
    Ok(())
}

/// The first sync runs inside start-up: its progress shows while the state is still `Starting`.
#[test]
fn progress_shows_during_the_first_sync() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
    // There before the daemon starts: uploaded by start-up's first sync.
    std::fs::write(root.join("big.bin"), vec![7u8; 600_000])?;
    let (handle, _thread) = spawn_slow(&server, &root)?;
    let mut seen = None;
    wait_until("progress shows", || {
        let status = handle.status();
        seen = status.progress.map(|p| (status.state, p));
        seen.is_some_and(|(_, p)| p.bytes_done > 0)
    })?;
    let (state, p) = seen.ok_or("no progress")?;
    assert_eq!((state, p.bytes_total), (State::Starting, 600_000));
    wait_until("big.bin is up", || on_server(&server, "big.bin"))?;
    handle.send(Command::Stop);
    Ok(())
}

#[test]
fn progress_shows_while_a_transfer_runs() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
    // Two seconds to watch.
    let (handle, _thread) = spawn_slow(&server, &root)?;
    std::fs::write(root.join("big.bin"), vec![7u8; 600_000])?;
    let mut seen = None;
    wait_until("progress shows", || {
        seen = handle.status().progress;
        seen.is_some_and(|p| p.bytes_done > 0)
    })?;
    let p = seen.ok_or("no progress")?;
    assert_eq!((p.files_total, p.bytes_total), (1, 600_000));
    assert!(p.bytes_done < p.bytes_total, "{p:?}");
    wait_until("big.bin is up", || on_server(&server, "big.bin"))?;
    handle.send(Command::Stop);
    Ok(())
}

/// A computer the identity refuses (401) asks it nothing more: not on a change here, not on a
/// timer, not the account's puller, until the user acts (Sync now) or the notifications
/// connect again; then it syncs as before.
#[test]
fn a_refused_computer_asks_nothing_until_the_user_acts() -> R {
    let server = server();
    let a = Device::start(&server, "a")?;
    a.wait_idle()?;
    a.remote.faults().denied = true;
    a.write("one.txt", "1")?;
    wait_until("refused", || a.status().state == State::Denied)?;
    assert!(a.drive.denied().is_some(), "the account latched");
    let asked = a.remote.faults().requests;
    a.write("two.txt", "2")?;
    let puller = a.puller();
    puller.ask();
    // The machine woke up: a sync at once, but not one that asks a refusing identity.
    a.handle.send(Command::Woke);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        a.remote.faults().requests,
        asked,
        "nothing asked while refused"
    );
    assert_eq!(a.status().state, State::Denied);
    // Let in again, and the user asks.
    a.remote.faults().denied = false;
    a.handle.send(Command::SyncNow);
    wait_until("synced again", || {
        on_server(&server, "one.txt") && on_server(&server, "two.txt")
    })?;
    assert!(a.drive.denied().is_none());
    Ok(())
}
