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

#[test]
fn parked_item_in_a_folder_the_user_renamed_is_put_back_there() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("A/doc.txt", "keep me")?;
    a.sync()?;
    // A crash right after parking, then the user renames the folder before the next run.
    let from = hbsync_platform::RelPath::new("A/doc.txt")?;
    let node = a
        .engine
        .db()
        .read(|s| s.get_local(&from))?
        .ok_or("row")?
        .node;
    let to = hbsync_platform::RelPath::new(&format!("A/.hbsync-move-{}", node.0.simple()))?;
    fs::rename(from.to_native(&a.root), to.to_native(&a.root))?;
    a.engine.db().write(|s| {
        s.move_local_subtree(&from, &to)?;
        s.put_parked(node, &to, &from)
    })?;
    fs::rename(a.root.join("A"), a.root.join("B"))?;
    a.sync()?;
    assert_eq!(a.read("B/doc.txt")?, "keep me");
    let mut names = server.lock().map_err(|_| "poisoned")?.live_names();
    names.sort();
    assert_eq!(
        names,
        ["B", "doc.txt"],
        "the temporary name never reached the remote"
    );
    Ok(())
}

#[test]
fn first_run_adopts_an_existing_copy_without_downloading() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for (path, data) in [
        ("Photos/2024/img1.jpg", "jpeg1"),
        ("Photos/2024/img2.jpg", "jpeg2"),
        ("notes.md", "# notes"),
    ] {
        a.write(path, data)?;
    }
    a.sync()?;

    // b starts with the same files already on disk (restored from a backup, say).
    let mut b = Client::new(&server, "b")?;
    for (path, data) in [
        ("Photos/2024/img1.jpg", "jpeg1"),
        ("Photos/2024/img2.jpg", "jpeg2"),
        ("notes.md", "# notes"),
    ] {
        b.write(path, data)?;
    }
    let report = b.sync()?;
    assert_eq!(b.remote.faults().served, 0, "nothing was downloaded");
    assert!(report.conflicts.is_empty(), "{:?}", report.conflicts);
    assert_eq!(b.tree(), a.tree());
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        5,
        "no duplicates were uploaded"
    );
    Ok(())
}

#[test]
fn a_differing_copy_keeps_both_versions() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("doc.txt", "remote version")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.write("doc.txt", "local version")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree(), b.tree());
    let contents: Vec<String> = b
        .tree()
        .into_iter()
        .map(|e| e.split(" = ").nth(1).unwrap_or_default().to_owned())
        .collect();
    assert!(
        contents.contains(&"remote version".to_owned())
            && contents.contains(&"local version".to_owned()),
        "{:?}",
        b.tree()
    );
    Ok(())
}

#[test]
fn start_up_catches_up_both_sides_in_parallel() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("from-a/one.txt", "1")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.write("from-b/two.txt", "2")?;
    let opts = b.engine.scan_options().clone();
    let mut observer = hbsync_local::LocalObserver::start(&b.root, opts);
    let db_path = b.root.join(".hbsync").join("state.db");
    let report = b.engine.start_up(&mut observer, &db_path, "2026-10-08")?;
    assert!(report.feed.live >= 2, "{:?}", report.feed);
    assert_eq!(report.local.report.added, 2);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree(), b.tree());
    assert_eq!(a.read("from-b/two.txt")?, "2");
    Ok(())
}

#[test]
fn crossing_moves_park_the_occupant_in_one_pass() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("a", "A")?;
    a.write("b", "B")?;
    a.write("sub/keep", "k")?;
    a.sync()?;
    b.sync()?;
    // On a: b moves into sub, then a takes the name b. On client b, the move of a lands on a
    // name still held by b, which is about to leave: b gets parked first.
    // Two separate syncs on a, so both are moves (in one pass, a name taken over at the same
    // path counts as an edit of the node that was there).
    fs::rename(a.root.join("b"), a.root.join("sub/b"))?;
    a.sync()?;
    fs::rename(a.root.join("a"), a.root.join("b"))?;
    a.sync()?;
    // File-system clocks are coarse: let ctime move past b's recorded fingerprints.
    std::thread::sleep(std::time::Duration::from_millis(50));
    b.engine.scan_local()?;
    let pass = b.engine.sync_once("2026-10-08")?;
    assert!(
        pass.skipped.is_empty(),
        "the parked file's own move must not trip on its ctime: {:?}",
        pass.skipped
    );
    assert_eq!(b.read("b")?, "A");
    assert_eq!(b.read("sub/b")?, "B");
    assert_eq!(a.tree(), b.tree());
    Ok(())
}

#[test]
fn case_only_rename_converges() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("report.pdf", "r")?;
    a.write("photos/p.jpg", "p")?;
    a.sync()?;
    b.sync()?;
    fs::rename(a.root.join("report.pdf"), a.root.join("Report.pdf"))?;
    fs::rename(a.root.join("photos"), a.root.join("Photos"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert_eq!(b.tree(), ["Photos/", "Photos/p.jpg = p", "Report.pdf = r"]);
    assert_eq!(a.tree(), b.tree());
    Ok(())
}

#[test]
fn new_folder_differing_only_in_case_from_a_leaving_one() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("photos/old.jpg", "old")?;
    a.sync()?;
    b.sync()?;
    // a renames photos away and creates a new Photos, in separate syncs.
    fs::rename(a.root.join("photos"), a.root.join("archive"))?;
    a.sync()?;
    a.write("Photos/new.jpg", "new")?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert_eq!(
        b.tree(),
        [
            "Photos/",
            "Photos/new.jpg = new",
            "archive/",
            "archive/old.jpg = old"
        ]
    );
    assert_eq!(a.tree(), b.tree());
    Ok(())
}

#[test]
fn crash_right_after_a_fork_rename_is_completed_not_published() -> R {
    use hbsync_platform::RelPath;
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("notes.txt", "base")?;
    a.sync()?;
    b.sync()?;
    a.write("notes.txt", "a's edit")?;
    a.sync()?;
    b.write("notes.txt", "b's edit")?;
    b.engine.scan_local()?;
    // b starts the fork and "crashes" right after the rename, before the DB update.
    let from = RelPath::new("notes.txt")?;
    let to = RelPath::new("notes (conflict b 2026-10-08).txt")?;
    let node = b
        .engine
        .db()
        .read(|s| s.get_local(&from))?
        .ok_or("row")?
        .node;
    let copy = hbsync_types::NodeId::new();
    b.engine
        .db()
        .write(|s| s.put_fork(node, copy, &from, &to))?;
    fs::rename(b.root.join("notes.txt"), b.root.join(to.as_str()))?;

    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    a.sync()?;
    assert_eq!(a.tree(), b.tree());
    assert_eq!(b.read("notes.txt")?, "a's edit");
    assert_eq!(b.read(to.as_str())?, "b's edit");
    let srv = server.lock().map_err(|_| "poisoned")?;
    let original = srv.file_id_of(node).ok_or("original node is still live")?;
    assert_eq!(
        srv.header(original)?.state().map(|s| s.name).as_deref(),
        Some("notes.txt")
    );
    Ok(())
}

#[test]
fn a_second_conflict_with_the_same_bytes_gets_its_own_copy() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("f.txt", "base")?;
    a.sync()?;
    b.sync()?;
    for remote_edit in ["a1", "a2"] {
        a.write("f.txt", remote_edit)?;
        a.sync()?;
        b.write("f.txt", "same local bytes")?;
        let report = b.sync()?;
        assert!(
            report.failed.is_empty() && report.skipped.is_empty(),
            "{report:?}"
        );
        a.sync()?;
    }
    assert_eq!(a.tree(), b.tree());
    let copies = b
        .tree()
        .iter()
        .filter(|e| e.ends_with("= same local bytes"))
        .count();
    assert_eq!(copies, 2, "{:?}", b.tree());
    assert_eq!(b.read("f.txt")?, "a2");
    Ok(())
}

#[test]
fn adopting_a_copy_of_a_sha256_file_downloads_nothing() -> R {
    let server = server();
    // Another client (say, mobile) uploaded with SHA-256.
    server.lock().map_err(|_| "poisoned")?.seed_file(
        hbsync_types::NodeId::ROOT,
        "photo.jpg",
        b"photo bytes",
        hbsync_fileio::HashAlgorithm::Sha256,
    )?;

    // This client already has the same file.
    let mut b = Client::new(&server, "b")?;
    b.write("photo.jpg", "photo bytes")?;
    let report = b.sync()?;
    assert!(report.conflicts.is_empty(), "{:?}", report.conflicts);
    assert_eq!(b.remote.faults().served, 0, "nothing was downloaded");
    assert_eq!(b.tree(), ["photo.jpg = photo bytes"]);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        1,
        "nothing was uploaded"
    );
    Ok(())
}

#[test]
fn adopting_a_wide_folder_takes_linear_time() -> R {
    const N: usize = 3000;
    let server = server();
    {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        for i in 0..N {
            s.seed_file(
                hbsync_types::NodeId::ROOT,
                &format!("f{i}.txt"),
                format!("data {i}").as_bytes(),
                hbsync_fileio::HashAlgorithm::Blake3,
            )?;
        }
    }
    let dir = tempfile::tempdir()?;
    for i in 0..N {
        fs::write(dir.path().join(format!("f{i}.txt")), format!("data {i}"))?;
    }
    // Adoption alone, on an in-memory DB: no disk syncs in the measurement.
    let mut db = StateDb::open_in_memory()?;
    let opts = hbsync_local::ScanOptions {
        algorithm: hbsync_fileio::HashAlgorithm::Blake3,
        fs_family: hbsync_platform::FsFamily::Other,
        exclude_at_root: Vec::new(),
    };
    hbsync_local::scan(&mut db, dir.path(), &opts)?;
    hbsync_engine::feed::pull(&mut db, &FakeRemote::new(server.clone()), 500)?;
    let started = std::time::Instant::now();
    let adopted = hbsync_engine::adopt::adopt(&mut db, dir.path(), hbsync_platform::Os::current())?;
    let took = started.elapsed();
    assert_eq!(adopted, N);
    // Matching each file against all its siblings took 50 s here (debug); grouped, under 1 s.
    assert!(took.as_secs() < 10, "adoption took {took:?}");
    Ok(())
}

#[test]
fn temp_files_no_download_wants_are_swept() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("doc.txt", "v1")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    let tmp = b.root.join(".hbsync").join("tmp");
    fs::create_dir_all(&tmp)?;
    // Left by a crash or a failed placement: an unrelated file, a partial download of a node
    // that is in sync, and one of content that has been replaced since.
    let synced = server
        .lock()
        .map_err(|_| "poisoned")?
        .live()
        .find_map(|h| match &h.body {
            hbsync_remote::RemoteBody::Live { node, .. } => Some(*node),
            _ => None,
        })
        .ok_or("doc.txt is on the server")?;
    fs::write(tmp.join("junk"), "x")?;
    fs::write(
        tmp.join(format!("{}-0123456789abcdef.partial", synced.0.simple())),
        "v1",
    )?;
    a.write("doc.txt", "v2")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("doc.txt")?, "v2");
    let left: Vec<_> = fs::read_dir(&tmp)?
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(left.is_empty(), "{left:?}");
    Ok(())
}

#[test]
fn nothing_is_created_under_a_folder_the_server_does_not_have() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("docs/report.txt", "r")?;
    a.write("docs/sub/deep.txt", "d")?;
    // The folder's create keeps failing during this sync.
    a.remote.faults().fail_writes_named = Some("docs".into());
    let report = a.sync()?;
    assert!(!report.failed.is_empty(), "{report:?}");
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        0,
        "nothing was created under the missing folder"
    );
    // Once the server takes the folder, everything follows.
    a.remote.faults().fail_writes_named = None;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    let report = b.sync()?;
    assert!(report.conflicts.is_empty(), "{report:?}");
    assert_eq!(b.tree(), a.tree());
    Ok(())
}

#[test]
fn a_folder_is_not_deleted_remotely_while_a_child_delete_failed() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/keep.txt", "k")?;
    a.write("d/other.txt", "o")?;
    a.write("stay.txt", "s")?; // not everything is deleted: no mass-delete hold
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    // The delete of one file keeps failing.
    a.remote.faults().fail_writes_named = Some("keep.txt".into());
    a.sync()?;
    {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let mut names = srv.live_names();
        names.sort();
        assert_eq!(
            names,
            ["d", "keep.txt", "stay.txt"],
            "the folder stays while it has a child"
        );
    }
    a.remote.faults().fail_writes_named = None;
    a.sync()?;
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["stay.txt"]
    );
    Ok(())
}
