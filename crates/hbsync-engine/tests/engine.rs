//! The executor and engine end to end: real folders, a real state DB, the fake remote.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::{Engine, EngineConfig, SyncReport};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::StateDb;

type R = Result<(), Box<dyn std::error::Error>>;

struct Client {
    _dir: tempfile::TempDir,
    root: PathBuf,
    engine: Engine,
    remote: FakeRemote,
}

impl Client {
    fn new(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(".hbsync"))?;
        let db = StateDb::open(&root.join(".hbsync").join("state.db"))?;
        let remote = FakeRemote::new(server.clone());
        let engine = Engine::new(db, Box::new(remote.clone()), EngineConfig::new(&root, name));
        Ok(Client {
            _dir: dir,
            root,
            engine,
            remote,
        })
    }

    /// Scan, then sync until idle. The report accumulates every pass.
    fn sync(&mut self) -> Result<SyncReport, Box<dyn std::error::Error>> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            self.engine.scan_local()?;
            let pass = self.engine.sync_once("2026-10-08")?;
            total.ops_done += pass.ops_done;
            total.conflicts.extend(pass.conflicts);
            total.failed.extend(pass.failed);
            total.skipped.extend(pass.skipped);
            total.held = pass.held;
            total.pending = pass.pending;
            if pass.ops_done == 0 {
                break;
            }
        }
        Ok(total)
    }

    fn write(&self, rel: &str, data: &str) -> std::io::Result<()> {
        let p = self.root.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, data)
    }

    fn read(&self, rel: &str) -> std::io::Result<String> {
        fs::read_to_string(self.root.join(rel))
    }

    /// Every synced path with its content (folders end in `/`), excluding `.hbsync`.
    fn tree(&self) -> Vec<String> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, &mut out);
        out.sort();
        out
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if rel == ".hbsync" {
            continue;
        }
        if path.is_dir() {
            out.push(format!("{rel}/"));
            walk(root, &path, out);
        } else {
            out.push(format!(
                "{rel} = {}",
                fs::read_to_string(&path).unwrap_or_default()
            ));
        }
    }
}

fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

#[test]
fn upload_then_download() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("Docs/deep/nested.bin", "bytes")?;
    a.write("top.txt", "top")?;
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 5);
    // A second sync has nothing to do.
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());

    b.sync()?;
    assert_eq!(b.tree(), a.tree());
    assert_eq!(b.read("Docs/deep/nested.bin")?, "bytes");
    Ok(())
}

#[test]
fn edits_moves_and_deletes_propagate() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("x/one.txt", "1")?;
    a.write("x/two.txt", "2")?;
    a.write("y/three.txt", "3")?;
    a.sync()?;
    b.sync()?;

    a.write("x/one.txt", "1 edited")?;
    fs::rename(a.root.join("x/two.txt"), a.root.join("y/two-moved.txt"))?;
    fs::remove_dir_all(b.root.join("y"))?; // b deletes y (and three.txt) concurrently
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree(), b.tree());
    // two-moved.txt went into y, which b deleted: the live file kept y alive (edit/move wins).
    assert_eq!(b.read("y/two-moved.txt")?, "2");
    assert_eq!(b.read("x/one.txt")?, "1 edited");
    assert!(
        b.read("y/three.txt").is_err(),
        "the untouched file stays deleted"
    );
    // b's delete of three.txt went to a's trash, not into the void.
    let trash = fs::read_dir(a.root.join(".hbsync/trash"))?.count();
    assert!(trash >= 1);
    Ok(())
}

#[test]
fn concurrent_edits_keep_both_versions() -> R {
    let server = server();
    let mut a = Client::new(&server, "laptop")?;
    let mut b = Client::new(&server, "desktop")?;
    a.write("notes.txt", "base")?;
    a.sync()?;
    b.sync()?;
    a.write("notes.txt", "from laptop")?;
    b.write("notes.txt", "from desktop!")?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.conflicts.is_empty());
    a.sync()?;
    assert_eq!(a.tree(), b.tree());
    let tree = b.tree();
    assert!(
        tree.contains(&"notes.txt = from laptop".to_owned()),
        "{tree:?}"
    );
    assert!(
        tree.contains(&"notes (conflict desktop 2026-10-08).txt = from desktop!".to_owned()),
        "{tree:?}"
    );
    Ok(())
}

#[test]
fn name_swap_uses_a_parked_name() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("left", "L")?;
    a.write("right", "R")?;
    a.sync()?;
    b.sync()?;
    // a swaps the two names.
    fs::rename(a.root.join("left"), a.root.join("tmp"))?;
    fs::rename(a.root.join("right"), a.root.join("left"))?;
    fs::rename(a.root.join("tmp"), a.root.join("right"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("left")?, "R");
    assert_eq!(b.read("right")?, "L");
    assert_eq!(a.tree(), b.tree());
    Ok(())
}

#[test]
fn transient_failures_are_retried() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("f.txt", "data")?;
    a.remote.faults().fail_next = 2;
    a.sync()?;
    b.remote.faults().cut_download_after = Some(2);
    b.sync()?;
    assert_eq!(b.read("f.txt")?, "data");
    Ok(())
}

#[test]
fn restart_after_an_interrupted_sync_converges() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..12 {
        a.write(&format!("batch/f{i:02}.txt"), &format!("content {i}"))?;
    }
    // A long outage partway through: every request after the first few fails.
    a.remote.faults().script = std::iter::repeat_n(false, 5)
        .chain(std::iter::repeat_n(true, 400))
        .collect();
    a.engine.scan_local()?;
    let _ = a.engine.sync_once("2026-10-08"); // fails or leaves work pending
    a.remote.faults().script.clear();

    // Restart: a new engine on the same state DB.
    let db = StateDb::open(&a.root.join(".hbsync").join("state.db"))?;
    a.engine = Engine::new(
        db,
        Box::new(a.remote.clone()),
        EngineConfig::new(&a.root, "a"),
    );
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(a.tree(), b.tree());
    assert_eq!(b.tree().len(), 13);
    Ok(())
}

#[test]
fn parked_item_from_a_crash_is_put_back() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("doc.txt", "keep me")?;
    a.sync()?;
    // Simulate a crash right after parking: the file sits under its temporary name.
    let node = a
        .engine
        .db()
        .read(|s| {
            s.get_local(
                &hbsync_platform::RelPath::new("doc.txt")
                    .map_err(|e| hbsync_state::StateError::Corrupt(e.to_string()))?,
            )
        })?
        .ok_or("row")?
        .node;
    let parked = format!(".hbsync-move-{}", node.0.simple());
    fs::rename(a.root.join("doc.txt"), a.root.join(&parked))?;
    let (from, to) = (
        hbsync_platform::RelPath::new("doc.txt")?,
        hbsync_platform::RelPath::new(&parked)?,
    );
    a.engine.db().write(|s| {
        s.move_local_subtree(&from, &to)?;
        s.put_parked(node, &to, &from)
    })?;
    a.sync()?;
    assert_eq!(a.read("doc.txt")?, "keep me");
    assert_eq!(a.tree().len(), 1);
    let names: Vec<String> = server
        .lock()
        .map_err(|_| "poisoned")?
        .live()
        .filter_map(|h| h.state().map(|s| s.name))
        .collect();
    assert_eq!(
        names,
        ["doc.txt"],
        "the temporary name never reached the remote"
    );
    Ok(())
}
