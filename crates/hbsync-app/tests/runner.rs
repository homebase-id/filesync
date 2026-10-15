//! The runner on real folders and the fake server: syncing on its own, pause and resume, sync
//! now, confirming held deletes, ignore rules edited while it runs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_app::runner::RunnerError;
use hbsync_app::{Command, FolderStatus, Handle, Runner, RunnerConfig, State};
use hbsync_engine::Drive;
use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

type R = Result<(), Box<dyn std::error::Error>>;

const PATIENCE: Duration = Duration::from_secs(30);

struct Device {
    _dir: tempfile::TempDir,
    root: PathBuf,
    handle: Handle,
    thread: Option<JoinHandle<Result<(), RunnerError>>>,
}

impl Device {
    fn start(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
        poll: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = set_up(&dir.path().canonicalize()?.join("sync"))?;
        let mut cfg = RunnerConfig::new(&root, &db_of(&root), "test.example", name);
        cfg.poll = poll;
        cfg.tweak = Some(|c| c.exec.retry_pause = Duration::from_millis(1));
        let (handle, thread) = spawn(cfg, server)?;
        Ok(Device {
            _dir: dir,
            root,
            handle,
            thread: Some(thread),
        })
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

/// Long enough that only local changes and requests make a device sync.
const NO_POLL: Duration = Duration::from_secs(3600);

#[test]
fn syncs_by_itself_and_pauses_and_resumes() -> R {
    let server = server();
    let a = Device::start(&server, "a", Duration::from_millis(300))?;
    let mut b = Device::start(&server, "b", Duration::from_millis(300))?;
    a.write("one.txt", "1")?;
    wait_until("b has one.txt", || b.has("one.txt"))?;
    wait_until("b is idle", || b.status().state == State::Idle)?;
    assert!(b.status().last_sync.is_some());

    b.handle.send(Command::Pause { until: None });
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

#[test]
fn sync_now_reads_the_drive_without_waiting_for_the_poll() -> R {
    let server = server();
    let a = Device::start(&server, "a", NO_POLL)?;
    let b = Device::start(&server, "b", NO_POLL)?;
    wait_until("b started", || b.status().last_sync.is_some())?;
    a.write("news.txt", "hot")?;
    wait_until("news.txt is up", || on_server(&server, "news.txt"))?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!b.has("news.txt"), "no hints, no poll: b can't know yet");
    b.handle.send(Command::SyncNow);
    wait_until("b has news.txt", || b.has("news.txt"))?;
    Ok(())
}

#[test]
fn held_deletes_go_through_once_confirmed_as_shown() -> R {
    let server = server();
    let a = Device::start(&server, "a", Duration::from_millis(300))?;
    for i in 0..60 {
        a.write(&format!("f{i:02}.txt"), &i.to_string())?;
    }
    wait_until("all 60 are up", || live_names(&server).len() == 60)?;
    wait_until("a is idle", || {
        let s = a.status();
        s.state == State::Idle && s.pending == 0
    })?;
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

    // A confirmation of something else (the held deletes changed since) is ignored.
    a.handle.send(Command::ConfirmDeletes {
        token: "0000000000000000-60".into(),
    });
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(live_names(&server).len(), 60, "nothing was deleted");

    a.handle.send(Command::ConfirmDeletes { token: held.token });
    wait_until("the drive is empty", || live_names(&server).is_empty())?;
    wait_until("nothing is held", || a.status().held_deletes.is_none())?;
    Ok(())
}

#[test]
fn an_edited_ignore_file_applies_without_a_restart() -> R {
    let server = server();
    let a = Device::start(&server, "a", NO_POLL)?;
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
    let mut cfg = RunnerConfig::new(root, &db_of(root), "test.example", "a");
    cfg.poll = Duration::from_millis(300);
    spawn(cfg, server)
}

/// The runner of `cfg`, on its account's drive on `server`, without notifications.
fn spawn(
    cfg: RunnerConfig,
    server: &Arc<Mutex<FakeServer>>,
) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
    let account = SyncRoot::open_account(&cfg.db, &cfg.identity)?;
    let drive = Drive::new(account, Arc::new(FakeRemote::new(server.clone())));
    Runner::spawn(cfg, drive)
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
    let (handle, thread) = spawn_at(&server, &root)?;
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
    assert_eq!(handle.status().state, State::Stopped);
    assert!(
        handle
            .status()
            .error
            .is_some_and(|e| e.contains("other.example"))
    );
    assert!(live_names(&server).is_empty(), "nothing was uploaded");
    Ok(())
}

/// A folder an older Filesync synced (its state DB in .hbsync, no marker) isn't gone: the
/// runner stops and says to set it up again.
#[test]
fn a_folder_of_an_older_filesync_is_set_up_again() -> R {
    let server = server();
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?.join("sync");
    std::fs::create_dir_all(root.join(".hbsync"))?;
    std::fs::write(root.join(".hbsync/state.db"), "an older Filesync's")?;
    let (handle, thread) = spawn_at(&server, &root)?;
    let err = thread
        .join()
        .map_err(|_| "panicked")?
        .expect_err("the runner stopped");
    assert!(err.to_string().contains("set it up again"), "{err}");
    assert!(
        handle
            .status()
            .error
            .is_some_and(|e| e.contains("older Filesync"))
    );
    Ok(())
}

#[test]
fn a_timed_pause_ends_by_itself() -> R {
    let server = server();
    let a = Device::start(&server, "a", NO_POLL)?;
    wait_until("a is idle", || a.status().state == State::Idle)?;
    let until = hbsync_types::unix_now() + 2;
    a.handle.send(Command::Pause { until: Some(until) });
    assert_eq!(a.status().state, State::Paused);
    wait_until("the pause shows its end", || {
        a.status().paused_until == Some(until)
    })?;
    a.write("later.txt", "x")?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(!on_server(&server, "later.txt"), "synced while paused");
    // Over by itself, and what waited goes up.
    wait_until("later.txt is up", || on_server(&server, "later.txt"))?;
    let status = a.status();
    assert_ne!(status.state, State::Paused);
    assert_eq!(status.paused_until, None);
    Ok(())
}

#[test]
fn the_recently_synced_list_follows_the_syncs() -> R {
    let server = server();
    let a = Device::start(&server, "a", NO_POLL)?;
    a.write("Docs/one.txt", "1")?;
    a.write("two.txt", "22")?;
    wait_until("both are listed", || a.status().synced_added == 2)?;
    // The list is the folder's state DB's.
    let recent = SyncRoot::open(&db_of(&a.root), "test.example", &a.root)?
        .state()?
        .read(|s| s.recent_synced(10))?;
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
    let mut cfg = RunnerConfig::new(&root, &db_of(&root), "test.example", "a");
    cfg.poll = NO_POLL;
    cfg.tweak = Some(|c| c.exec.transfer.max_bytes_per_sec = Some(300_000));
    let (handle, _thread) = spawn(cfg, &server)?;
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
    let mut cfg = RunnerConfig::new(&root, &db_of(&root), "test.example", "a");
    cfg.poll = NO_POLL;
    // 600 kB at 300 kB/s: two seconds to watch.
    cfg.tweak = Some(|c| c.exec.transfer.max_bytes_per_sec = Some(300_000));
    let (handle, _thread) = spawn(cfg, &server)?;
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
