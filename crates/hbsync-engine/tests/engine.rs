//! The executor and engine end to end: real folders, a real state DB, the fake remote.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use hbsync_engine::Engine;
use hbsync_engine::exec::CrashPoint;
use hbsync_remote::fake::FakeRemote;
use hbsync_remote::{ChangesPage, NodeMeta, Remote, RemoteHeader, Result as RemoteResult, Upload};
use hbsync_state::StateDb;
use hbsync_types::{RemoteFileId, RemoteRef, VersionTag};

mod common;

use common::{Client, server};

type R = Result<(), Box<dyn std::error::Error>>;

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
    assert_eq!(b.tree_text(), a.tree_text());
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
    assert_eq!(a.tree_text(), b.tree_text());
    // two-moved.txt went into y, which b deleted: the live file kept y alive (edit/move wins).
    assert_eq!(b.read("y/two-moved.txt")?, "2");
    assert_eq!(b.read("x/one.txt")?, "1 edited");
    assert!(
        b.read("y/three.txt").is_err(),
        "the untouched file stays deleted"
    );
    // b's delete of three.txt went to Recently deleted on the drive, not into the void: a
    // needs no copy of its own.
    let removed: Vec<_> = server
        .lock()
        .map_err(|_| "poisoned")?
        .removed()
        .filter_map(|h| h.removal().map(|r| (h.node(), r.root)))
        .collect();
    assert_eq!(removed.len(), 1, "{removed:?}");
    assert!(!a.root.join(".hbsync/trash").exists());
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
    assert_eq!(a.tree_text(), b.tree_text());
    let tree = b.tree_text();
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
    assert_eq!(a.tree_text(), b.tree_text());
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
fn a_denied_remote_stops_the_sync_and_loses_nothing() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("kept.txt", "v1")?;
    a.sync()?;
    a.write("kept.txt", "v2")?;
    a.write("new/one.txt", "one")?;
    // The owner revoked the app: every request is refused.
    a.remote.faults().denied = true;
    let before = a.remote.faults().requests;
    a.engine.scan_local()?;
    let err = a
        .engine
        .sync_once("2026-10-09")
        .err()
        .ok_or("sync went through")?;
    assert!(err.is_denied(), "{err}");
    let requests = a.remote.faults().requests - before;
    assert!(
        requests <= 2,
        "stopped at the first refusal, made {requests} requests"
    );
    assert_eq!(a.read("kept.txt")?, "v2");
    assert_eq!(a.read("new/one.txt")?, "one");

    // Reads work again but writes are refused: the executor stops at the first write.
    a.remote.faults().denied = false;
    a.remote.faults().deny_writes = true;
    let err = a
        .engine
        .sync_once("2026-10-09")
        .err()
        .ok_or("sync went through")?;
    assert!(err.is_denied(), "{err}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 1);

    // Authorized again: the pending work goes through.
    a.remote.faults().deny_writes = false;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(), a.tree_text());
    assert_eq!(b.read("kept.txt")?, "v2");
    Ok(())
}

/// A remote that asks the engine to stop after a number of creates (a pause or a shutdown
/// arriving in the middle of a sync).
struct StopAfterCreates {
    inner: FakeRemote,
    creates: AtomicUsize,
    after: usize,
    stop: Arc<AtomicBool>,
}

impl Remote for StopAfterCreates {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<ChangesPage> {
        self.inner.changes(cursor, limit)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<RemoteRef> {
        let made = self.inner.create(node, payload);
        if self.creates.fetch_add(1, Ordering::SeqCst) + 1 == self.after {
            self.stop.store(true, Ordering::SeqCst);
        }
        made
    }
    fn update(
        &self,
        file_id: RemoteFileId,
        expected: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<RemoteRef> {
        self.inner.update(file_id, expected, meta, payload)
    }
    fn delete(&self, file_id: RemoteFileId, expected: VersionTag) -> RemoteResult<()> {
        self.inner.delete(file_id, expected)
    }
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> RemoteResult<u64> {
        self.inner.read(file_id, range, sink)
    }
}

#[test]
fn a_stopped_sync_keeps_what_it_did_and_the_next_one_finishes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..10 {
        fs::create_dir_all(a.root.join(format!("d{i}")))?;
    }
    for i in 0..5 {
        a.write(&format!("f{i}.txt"), format!("{i}"))?;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let remote = StopAfterCreates {
        inner: a.remote.clone(),
        creates: AtomicUsize::new(0),
        after: 3,
        stop: Arc::clone(&stop),
    };
    a.engine = Engine::new(
        StateDb::open(&a.db_path())?,
        Box::new(remote),
        common::config(&a.root, "a"),
    );
    a.engine.set_stop(Arc::clone(&stop));
    a.engine.scan_local()?;
    let report = a.engine.sync_once("2026-10-09")?;
    assert!(report.stopped, "{report:?}");
    let live = server.lock().map_err(|_| "poisoned")?.live().count();
    assert!((3..15).contains(&live), "stopped part way: {live} of 15 up");
    assert_eq!(report.ops_done, live, "what was done is reported");
    assert!(report.pending > 0);

    // While the flag is set, a sync stops before doing anything.
    let report = a.engine.sync_once("2026-10-09")?;
    assert!(report.stopped && report.ops_done == 0, "{report:?}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), live);

    // Resumed: the rest goes up, and another device gets everything.
    stop.store(false, Ordering::SeqCst);
    a.sync()?;
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 15);
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(), a.tree_text());
    Ok(())
}

/// The progress total holds every byte of the plan from the start, not only the batches that
/// have started.
#[test]
fn progress_knows_the_whole_plan_up_front() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..5 {
        a.write(&format!("d{i}/f.txt"), "0123456789")?;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let remote = StopAfterCreates {
        inner: a.remote.clone(),
        creates: AtomicUsize::new(0),
        after: 2,
        stop: Arc::clone(&stop),
    };
    a.engine = Engine::new(
        StateDb::open(&a.db_path())?,
        Box::new(remote),
        common::config(&a.root, "a"),
    );
    a.engine.set_stop(Arc::clone(&stop));
    a.engine.scan_local()?;
    let report = a.engine.sync_once("2026-10-11")?;
    assert!(report.stopped, "{report:?}");
    let progress = a.engine.progress().get();
    assert_eq!(progress.bytes_total, 50, "{progress:?}");
    assert!(progress.bytes_done < 50, "stopped part way: {progress:?}");
    Ok(())
}

/// When the remote stops answering mid-plan, the run gives up after a few transient errors in
/// a row (pausing between them) instead of trying every queued operation four times.
#[test]
fn a_dead_remote_ends_the_run_early_and_loses_nothing() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..60 {
        a.write(&format!("f{i:02}.txt"), format!("content {i}"))?;
    }
    a.engine.scan_local()?;
    // The feed answers, then nothing does.
    a.remote.faults().script = std::iter::once(false)
        .chain(std::iter::repeat_n(true, 10_000))
        .collect();
    let before = a.remote.faults().requests;
    let err = a
        .engine
        .sync_once("2026-10-09")
        .err()
        .ok_or("sync went through")?;
    assert!(
        matches!(
            err,
            hbsync_engine::EngineError::Stopped(hbsync_engine::exec::Stopped::Unreachable(_))
        ),
        "{err}"
    );
    // Trying every op four times would be 240 requests. Workers that fail instantly can run
    // ahead of the commits until the run stops, so up to one try per upload may happen.
    let requests = a.remote.faults().requests - before;
    assert!(requests <= 70, "gave up after {requests} requests");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 0);

    // Back up: everything goes through.
    a.remote.faults().script.clear();
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(), a.tree_text());
    assert_eq!(b.tree_text().len(), 60);
    Ok(())
}

#[test]
fn restart_after_an_interrupted_sync_converges() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..12 {
        a.write(&format!("batch/f{i:02}.txt"), format!("content {i}"))?;
    }
    // A long outage partway through: every request after the first few fails.
    a.remote.faults().script = std::iter::repeat_n(false, 5)
        .chain(std::iter::repeat_n(true, 400))
        .collect();
    a.engine.scan_local()?;
    let _ = a.engine.sync_once("2026-10-08"); // fails or leaves work pending
    a.remote.faults().script.clear();

    // Restart: a new engine on the same state DB.
    restart(&mut a, "a")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(a.tree_text(), b.tree_text());
    assert_eq!(b.tree_text().len(), 13);
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
        s.put_parked(node, &from)
    })?;
    a.sync()?;
    assert_eq!(a.read("doc.txt")?, "keep me");
    assert_eq!(a.tree_text().len(), 1);
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

/// A fresh engine for `c` on its state DB, as after a restart.
fn restart(c: &mut Client, name: &str) -> R {
    c.engine = Engine::new(
        StateDb::open(&c.db_path())?,
        Box::new(c.remote.clone()),
        common::config(&c.root, name),
    );
    Ok(())
}

#[test]
fn crash_right_after_parking_leaks_no_temporary_name() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.exec.crash_at = Some(CrashPoint::AfterParkRename)
    })?;
    a.write("x", "old file")?;
    a.sync()?;
    b.sync()?;
    // On a, the file x gives way to a folder x. On b the folder is created before the file is
    // trashed, so the file is parked first; b's executor "crashes" right after that rename on
    // disk, before the row follows. The file's own place on the server didn't change (it was
    // deleted there), so only the parked record stops its temporary name from being uploaded.
    fs::remove_file(a.root.join("x"))?;
    a.write("x/inside", "new")?;
    a.sync()?;
    assert!(b.sync().is_err(), "b stops at the crash point");
    assert!(
        fs::read_dir(&b.root)?
            .flatten()
            .any(|e| e.file_name().to_string_lossy().starts_with(".hbsync-move-")),
        "the crash left a parked file behind"
    );
    restart(&mut b, "b")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(b.read("x/inside")?, "new");
    assert_eq!(a.tree_text(), b.tree_text());
    let mut names = server.lock().map_err(|_| "poisoned")?.live_names();
    names.sort();
    assert_eq!(
        names,
        ["inside", "x"],
        "the temporary name never reached the remote"
    );
    Ok(())
}

#[test]
fn crash_right_after_a_fork_rename_survives_a_folder_rename() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.exec.crash_at = Some(CrashPoint::AfterForkRename)
    })?;
    a.write("docs/notes.txt", "base")?;
    a.sync()?;
    b.sync()?;
    a.write("docs/notes.txt", "a's edit")?;
    a.sync()?;
    b.write("docs/notes.txt", "b's edit")?;
    assert!(b.sync().is_err(), "b stops right after the conflict rename");
    // The user renames the folder before b runs again.
    fs::rename(b.root.join("docs"), b.root.join("papers"))?;
    restart(&mut b, "b")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(), b.tree_text());
    assert_eq!(b.read("papers/notes.txt")?, "a's edit");
    assert_eq!(
        b.read("papers/notes (conflict b 2026-10-08).txt")?,
        "b's edit"
    );
    let mut names = server.lock().map_err(|_| "poisoned")?.live_names();
    names.sort();
    assert_eq!(
        names,
        ["notes (conflict b 2026-10-08).txt", "notes.txt", "papers"]
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
        s.put_parked(node, &from)
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
    assert_eq!(b.tree_text(), a.tree_text());
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
    assert_eq!(a.tree_text(), b.tree_text());
    let contents: Vec<String> = b
        .tree_text()
        .into_iter()
        .map(|e| e.split(" = ").nth(1).unwrap_or_default().to_owned())
        .collect();
    assert!(
        contents.contains(&"remote version".to_owned())
            && contents.contains(&"local version".to_owned()),
        "{:?}",
        b.tree_text()
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
    let db_path = b.db_path();
    let report = b.engine.start_up(&mut observer, &db_path, "2026-10-08")?;
    assert!(report.feed.live >= 2, "{:?}", report.feed);
    assert_eq!(report.local.report.added, 2);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(), b.tree_text());
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
    assert_eq!(a.tree_text(), b.tree_text());
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
    assert_eq!(
        b.tree_text(),
        ["Photos/", "Photos/p.jpg = p", "Report.pdf = r"]
    );
    assert_eq!(a.tree_text(), b.tree_text());
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
        b.tree_text(),
        [
            "Photos/",
            "Photos/new.jpg = new",
            "archive/",
            "archive/old.jpg = old"
        ]
    );
    assert_eq!(a.tree_text(), b.tree_text());
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
    b.engine.db().write(|s| s.put_fork(node, copy, &to))?;
    fs::rename(b.root.join("notes.txt"), b.root.join(to.as_str()))?;

    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    a.sync()?;
    assert_eq!(a.tree_text(), b.tree_text());
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
    assert_eq!(a.tree_text(), b.tree_text());
    let copies = b
        .tree_text()
        .iter()
        .filter(|e| e.ends_with("= same local bytes"))
        .count();
    assert_eq!(copies, 2, "{:?}", b.tree_text());
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
    assert_eq!(b.tree_text(), ["photo.jpg = photo bytes"]);
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
        filter: Default::default(),
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
    assert_eq!(b.tree_text(), a.tree_text());
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

// ---- §10 filters ----

fn ignoring(rules: &'static str) -> impl FnOnce(&mut hbsync_engine::EngineConfig) {
    move |cfg| {
        cfg.exec.filter = std::sync::Arc::new(
            hbsync_local::Filter::from_rules(rules, false).expect("valid rules"),
        );
    }
}

#[test]
fn junk_never_syncs_and_goes_to_the_trash_with_its_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("proj/doc.txt", "v1")?;
    a.write("proj/.DS_Store", "finder")?;
    a.write("~$doc.docx", "lock")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(), ["proj/", "proj/doc.txt = v1"]);

    // b's own junk in the folder doesn't hold up a's delete of it.
    b.write("proj/Thumbs.db", "thumbs")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    assert!(b.tree_text().is_empty(), "{:?}", b.tree_text());
    assert_eq!(a.tree_text(), ["~$doc.docx = lock"]);
    Ok(())
}

#[test]
fn ignored_items_stay_on_their_device_and_keep_their_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("node_modules/\n*.log\n"))?;
    a.write("proj/main.js", "m")?;
    a.write("proj/run.log", "from a")?;
    a.write("readme.txt", "r")?; // so deleting proj isn't deleting everything
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(),
        ["proj/", "proj/main.js = m", "readme.txt = r"]
    );

    b.write("proj/node_modules/x/y.js", "dep")?;
    b.write("proj/debug.log", "b's log")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(),
        [
            "proj/",
            "proj/main.js = m",
            "proj/run.log = from a",
            "readme.txt = r"
        ]
    );

    // a deletes the project: b keeps what it ignores, and the folder holding it, which goes
    // back up as a new, empty folder.
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(),
        [
            "proj/",
            "proj/debug.log = b's log",
            "proj/node_modules/",
            "proj/node_modules/x/",
            "proj/node_modules/x/y.js = dep",
            "readme.txt = r"
        ]
    );
    a.sync()?;
    assert_eq!(a.tree_text(), ["proj/", "readme.txt = r"]);
    Ok(())
}

#[test]
fn a_file_moved_into_a_folder_one_device_ignores_leaves_that_device_only() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("notes.txt", "n")?;
    a.write("draft.txt", "d1")?;
    a.sync()?;
    b.sync()?;
    // a moves both into build/; b edited the draft meanwhile.
    fs::create_dir(a.root.join("build"))?;
    fs::rename(a.root.join("notes.txt"), a.root.join("build/notes.txt"))?;
    fs::rename(a.root.join("draft.txt"), a.root.join("build/draft.txt"))?;
    b.write("draft.txt", "d2 from b")?;
    a.sync()?;
    b.sync()?;
    // The unchanged file went with the move; b's edit stays, as a new file everyone gets.
    assert_eq!(b.tree_text(), ["draft.txt = d2 from b"]);
    a.sync()?;
    assert_eq!(
        a.tree_text(),
        [
            "build/",
            "build/draft.txt = d1",
            "build/notes.txt = n",
            "draft.txt = d2 from b"
        ]
    );
    Ok(())
}

/// `c` restarted with the ignore rules `rules`.
fn restart_ignoring(c: &mut Client, name: &str, rules: &'static str) -> R {
    let mut cfg = common::config(&c.root, name);
    ignoring(rules)(&mut cfg);
    c.engine = Engine::new(
        StateDb::open(&c.db_path())?,
        Box::new(c.remote.clone()),
        cfg,
    );
    assert!(c.engine.apply_filter_change()?, "the filter changed");
    assert!(!c.engine.apply_filter_change()?, "and is recorded");
    Ok(())
}

#[test]
fn a_new_ignore_rule_takes_files_off_the_drive_but_not_off_the_disk() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("src/main.rs", "fn main")?;
    a.write("build/out.o", "obj")?;
    a.write("readme", "r")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("build/out.o")?, "obj");

    // a starts ignoring build/: it leaves the drive and b (for b's trash), and stays on a.
    restart_ignoring(&mut a, "a", "build/\n")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(a.read("build/out.o")?, "obj");
    assert_eq!(
        b.tree_text(),
        ["readme = r", "src/", "src/main.rs = fn main"]
    );

    // The rule goes: build/ is new on a again, and goes back up.
    restart_ignoring(&mut a, "a", "")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("build/out.o")?, "obj");
    Ok(())
}

#[test]
fn a_parked_folder_kept_for_what_it_ignores_gets_a_real_name() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("node_modules/\n"))?;
    a.write("proj/main.js", "m")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    b.sync()?;
    b.write("proj/node_modules/x.js", "dep")?;
    b.sync()?;

    // a starts the project over: b's old folder makes way for the new one, and keeps what b
    // ignores under a real name, never the temporary one.
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    a.write("proj/new.js", "n")?;
    a.sync()?;
    b.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        b.tree_text(),
        [
            "proj (conflict 1)/",
            "proj (conflict 1)/node_modules/",
            "proj (conflict 1)/node_modules/x.js = dep",
            "proj/",
            "proj/new.js = n",
            "readme.txt = r"
        ]
    );
    assert_eq!(
        a.tree_text(),
        [
            "proj (conflict 1)/",
            "proj/",
            "proj/new.js = n",
            "readme.txt = r"
        ]
    );
    Ok(())
}

#[test]
fn a_parked_file_whose_name_became_ignored_and_taken_moves_to_a_numbered_one() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("app.log", "keep me")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    // A crash right after parking app.log; then a new app.log appears, and a rule ignores
    // every numbered name too.
    let from = hbsync_platform::RelPath::new("app.log")?;
    let node = a
        .engine
        .db()
        .read(|s| s.get_local(&from))?
        .ok_or("row")?
        .node;
    let to = hbsync_platform::RelPath::new(&format!(".hbsync-move-{}", node.0.simple()))?;
    fs::rename(from.to_native(&a.root), to.to_native(&a.root))?;
    a.engine.db().write(|s| {
        s.move_local_subtree(&from, &to)?;
        s.put_parked(node, &from)
    })?;
    a.write("app.log", "new log")?;
    restart_ignoring(&mut a, "a", "*.log\n")?;
    a.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(),
        [
            "app (conflict 1).log = keep me",
            "app.log = new log",
            "readme.txt = r"
        ]
    );
    // Ignored now: it left the drive.
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["readme.txt"]
    );
    Ok(())
}

/// Another device moves a folder into a name this one ignores: trashing it here is held (more
/// than the guard allows), and the confirmation has to last until the trash ran, although the
/// drive keeps the folder.
#[test]
fn a_confirmed_trash_of_a_folder_moved_into_an_ignored_name_goes_through() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", |cfg| {
        ignoring("build/\n")(cfg);
        cfg.mass_delete_min = 2;
    })?;
    for i in 0..4 {
        a.write(&format!("src/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::rename(a.root.join("src"), a.root.join("build"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert!(b.root.join("src/f0.txt").exists(), "held");

    b.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(b.tree_text(), ["keep.txt = k"]);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 6);
    Ok(())
}

#[test]
fn what_was_synced_is_listed_and_kept() -> R {
    use hbsync_types::activity::SyncedKind::*;

    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    let kinds = |r: &hbsync_engine::SyncReport| -> Vec<_> {
        r.synced
            .iter()
            .map(|s| (s.kind, s.path.clone(), s.files, s.bytes))
            .collect()
    };
    a.write("Docs/report.txt", "quarterly")?;
    a.write("old.txt", "old")?;
    a.engine.scan_local()?;
    let mut up = a.engine.sync_once("2026-10-11")?;
    // That sync's progress: both files, all their bytes.
    assert_eq!(
        a.engine.progress().get(),
        hbsync_types::activity::Progress {
            files_done: 2,
            files_total: 2,
            bytes_done: 12,
            bytes_total: 12
        }
    );
    up.absorb(a.sync()?);
    let mut listed = kinds(&up);
    listed.sort();
    assert_eq!(
        listed,
        [
            (Uploaded, "Docs/report.txt".into(), 1, 9),
            (Uploaded, "old.txt".into(), 1, 3)
        ]
    );
    let down = b.sync()?;
    assert_eq!(kinds(&down).len(), 2);
    assert!(kinds(&down).iter().all(|k| k.0 == Downloaded));

    // A move and a delete here; the other device follows.
    fs::rename(a.root.join("Docs/report.txt"), a.root.join("report.txt"))?;
    fs::remove_file(a.root.join("old.txt"))?;
    let mut here = kinds(&a.sync()?);
    here.sort();
    assert_eq!(
        here,
        [
            (MovedRemotely, "report.txt".into(), 1, 0),
            (DeletedRemotely, "old.txt".into(), 1, 0)
        ]
    );
    let mut there = kinds(&b.sync()?);
    there.sort();
    assert_eq!(
        there,
        [
            (MovedLocally, "report.txt".into(), 1, 0),
            (TrashedLocally, "old.txt".into(), 1, 0)
        ]
    );

    // Many files in one folder come down as one entry.
    for i in 0..12 {
        a.write(&format!("Photos/p{i}.jpg"), "jpeg")?;
    }
    a.sync()?;
    let photos = kinds(&b.sync()?);
    assert_eq!(photos, [(Downloaded, "Photos".into(), 12, 48)]);

    // The list survives a restart, newest first.
    let recent = StateDb::open(&b.db_path())?.read(|s| s.recent_synced(3))?;
    assert_eq!(recent[0].path, "Photos");
    assert_eq!(recent.len(), 3);
    Ok(())
}

#[test]
fn recently_deleted_restores_and_purges() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("d/x.txt", "x")?;
    a.write("d/sub/y.txt", "y")?;
    a.write("z.txt", "z")?;
    a.write("keep.txt", "k")?; // deleting every file would be held (the mass-delete guard)
    a.sync()?;
    b.sync()?;

    // a deletes a folder (with everything in it) and a file: gone on b too, kept on the drive.
    fs::remove_dir_all(a.root.join("d"))?;
    fs::remove_file(a.root.join("z.txt"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(), ["keep.txt = k"]);
    let listed = b.engine.recently_deleted(10)?;
    let mut paths: Vec<_> = listed.iter().map(|d| d.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, ["d", "d/sub", "d/sub/y.txt", "d/x.txt", "z.txt"]);
    let d = listed.iter().find(|i| i.path == "d").ok_or("d")?.node;
    assert!(
        listed
            .iter()
            .filter(|i| i.path.starts_with("d"))
            .all(|i| i.root == d),
        "the folder's delete took its contents along: {listed:?}"
    );

    // b restores the folder: it comes back whole, on both; z.txt stays deleted.
    assert_eq!(b.engine.restore(&[d])?, 4);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(), b.tree_text());
    assert_eq!(a.read("d/sub/y.txt")?, "y");
    assert!(a.read("z.txt").is_err());

    // A restore onto a name taken meanwhile gets another.
    a.write("z.txt", "new z")?;
    a.sync()?;
    let z = a
        .engine
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "z.txt")
        .ok_or("z")?
        .node;
    assert_eq!(a.engine.restore(&[z])?, 1);
    a.sync()?;
    assert_eq!(a.read("z (restored).txt")?, "z");
    assert_eq!(a.read("z.txt")?, "new z");

    // The purge leaves nothing restorable.
    fs::remove_file(a.root.join("z.txt"))?;
    a.sync()?;
    assert_eq!(a.engine.purge(i64::MAX)?, 1);
    b.sync()?;
    assert!(b.engine.recently_deleted(10)?.is_empty());
    assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 0);
    Ok(())
}

#[test]
fn an_edit_restores_what_another_client_deleted() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("notes.txt", "1")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("notes.txt"))?;
    b.write("notes.txt", "1 and 2")?;
    a.sync()?; // removed
    b.sync()?; // the edit wins: restored with b's bytes
    a.sync()?;
    assert_eq!(a.read("notes.txt")?, "1 and 2");
    assert!(a.engine.recently_deleted(10)?.is_empty());
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!((srv.live().count(), srv.removed().count()), (1, 0));
    Ok(())
}
