//! The runner on real folders and the fake server: syncing on its own, pause and resume, sync
//! now, confirming held deletes, ignore rules edited while it runs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_app::runner::RunnerError;
use hbsync_app::{Control, Handle, HintSource, Runner, RunnerConfig, State, Status};
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
        let root = dir.path().canonicalize()?;
        // What setting a folder up does.
        std::fs::create_dir_all(root.join(".hbsync"))?;
        let mut cfg = RunnerConfig::new(&root, "test.example", name);
        cfg.poll = poll;
        cfg.tweak = Some(|c| c.exec.retry_pause = Duration::from_millis(1));
        let remote = Arc::new(FakeRemote::new(server.clone()));
        let (handle, thread) = Runner::spawn(cfg, remote, HintSource::none())?;
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

    fn status(&self) -> Status {
        self.handle.status()
    }

    fn stop(&mut self) -> R {
        assert!(self.handle.send(Control::Stop));
        if let Some(t) = self.thread.take() {
            t.join().map_err(|_| "the runner panicked")??;
        }
        assert_eq!(self.status().state, State::Stopped);
        Ok(())
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.handle.send(Control::Stop);
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

    b.handle.send(Control::Pause);
    wait_until("b is paused", || b.status().state == State::Paused)?;
    a.write("two.txt", "2")?;
    wait_until("two.txt is up", || on_server(&server, "two.txt"))?;
    std::thread::sleep(Duration::from_secs(1));
    assert!(!b.has("two.txt"), "b synced while paused");
    // Local changes on a paused device wait too.
    b.write("three.txt", "3")?;
    std::thread::sleep(Duration::from_secs(1));
    assert!(!on_server(&server, "three.txt"));

    b.handle.send(Control::Resume);
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
    b.handle.send(Control::SyncNow);
    wait_until("b has news.txt", || b.has("news.txt"))?;
    Ok(())
}

#[test]
fn held_deletes_go_through_once_confirmed() -> R {
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

    a.handle.send(Control::ConfirmDeletes {
        here: held.here,
        remotely: held.remotely,
    });
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
    a.handle.send(Control::SyncNow);
    wait_until("debug.log left the drive", || {
        !on_server(&server, "debug.log")
    })?;
    assert!(a.has("debug.log"), "an ignored file stays on its device");
    assert!(on_server(&server, "notes.txt"));
    wait_until("the rule is counted", || a.status().rules == 1)?;
    Ok(())
}

fn spawn_at(
    server: &Arc<Mutex<FakeServer>>,
    root: &Path,
) -> Result<(Handle, JoinHandle<Result<(), RunnerError>>), RunnerError> {
    let mut cfg = RunnerConfig::new(root, "test.example", "a");
    cfg.poll = Duration::from_millis(300);
    Runner::spawn(
        cfg,
        Arc::new(FakeRemote::new(server.clone())),
        HintSource::none(),
    )
}

#[test]
fn a_folder_whose_disk_is_not_mounted_is_left_alone() -> R {
    let server = server();
    let parent = tempfile::tempdir()?;
    let root = parent.path().canonicalize()?.join("sync");
    std::fs::create_dir_all(root.join(".hbsync"))?;
    let (handle, thread) = spawn_at(&server, &root)?;
    std::fs::write(root.join("a.txt"), "a")?;
    wait_until("a.txt is up", || on_server(&server, "a.txt"))?;
    handle.send(Control::Stop);
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
    handle.send(Control::Stop);
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
