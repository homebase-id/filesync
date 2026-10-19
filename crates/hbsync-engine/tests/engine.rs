//! The executor and engine end to end: real folders, a real state DB, the fake remote.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hbsync_engine::exec::{CrashPoint, FailPoint};
use hbsync_engine::walk;
use hbsync_remote::fake::FakeRemote;
use hbsync_remote::{
    ChangesPage, NodeMeta, RefsPage, Remote, RemoteHeader, Result as RemoteResult, Upload,
};
use hbsync_state::StateDb;
use hbsync_state::testkit::Commit;
use hbsync_types::{NodeId, RemoteFileId, RemoteRef, Target};

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
    assert_eq!(b.tree_text(""), a.tree_text(""));
    assert_eq!(b.read_text("Docs/deep/nested.bin")?, "bytes");
    Ok(())
}

/// A file's updates carry its token (on Homebase: one request, no header read first), from the
/// write that made it or from the feed, whether this device or another wrote it; the fake
/// refuses another file's token.
#[test]
fn updates_carry_the_files_token() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("x/one.txt", "1")?;
    a.write("x/two.txt", "2")?;
    a.write("three.txt", "3")?;
    a.sync()?;
    b.sync()?;
    a.remote.faults().updates_without_token = 0;
    b.remote.faults().updates_without_token = 0;

    fs::rename(a.root.join("three.txt"), a.root.join("four.txt"))?;
    a.write("x/one.txt", "1 edited")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(b.root.join("x"))?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(b.read_text("four.txt")?, "3");
    assert!(b.read_text("x/one.txt").is_err());
    assert_eq!(a.remote.faults().updates_without_token, 0);
    assert_eq!(b.remote.faults().updates_without_token, 0);
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
    assert_eq!(a.tree_text(""), b.tree_text(""));
    // two-moved.txt went into y, which b deleted: the live file kept y alive (edit/move wins).
    assert_eq!(b.read_text("y/two-moved.txt")?, "2");
    assert_eq!(b.read_text("x/one.txt")?, "1 edited");
    assert!(
        b.read_text("y/three.txt").is_err(),
        "the untouched file stays deleted"
    );
    // b's delete of three.txt went to Recently deleted on the drive, not into the void.
    let removed: Vec<_> = server
        .lock()
        .map_err(|_| "poisoned")?
        .removed()
        .filter_map(|h| h.removed_at.map(|at| (h.node(), at)))
        .collect();
    assert_eq!(removed.len(), 1, "{removed:?}");
    // And here, the local copy went to the trash (emptied by the purge).
    assert!(fs::read_dir(a.root.join(".hbsync/trash"))?.count() >= 1);
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
    assert_eq!(a.tree_text(""), b.tree_text(""));
    let tree = b.tree_text("");
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
    assert_eq!(b.read_text("left")?, "R");
    assert_eq!(b.read_text("right")?, "L");
    assert_eq!(a.tree_text(""), b.tree_text(""));
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
    assert_eq!(b.read_text("f.txt")?, "data");
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
    assert_eq!(a.read_text("kept.txt")?, "v2");
    assert_eq!(a.read_text("new/one.txt")?, "one");

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
    assert_eq!(b.tree_text(""), a.tree_text(""));
    assert_eq!(b.read_text("kept.txt")?, "v2");
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
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn holding_live(
        &self,
        folders: &[hbsync_types::NodeId],
    ) -> RemoteResult<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding_live(folders)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<Target> {
        let made = self.inner.create(node, payload);
        if self.creates.fetch_add(1, Ordering::SeqCst) + 1 == self.after {
            self.stop.store(true, Ordering::SeqCst);
        }
        made
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<Target> {
        self.inner.update(target, meta, payload)
    }
    fn delete(&self, files: &[RemoteRef]) -> RemoteResult<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(&self, files: &[RemoteRef]) -> RemoteResult<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Target>> {
        self.inner.set_removed(files, removed)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
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

/// A remote whose writes take a while, counting how many run at once, and the writes after
/// which the drive held a cycle, and the requests that removed files. Writes ending before
/// `down_until` fail (an outage).
struct SlowWrites {
    inner: FakeRemote,
    running: AtomicUsize,
    most: AtomicUsize,
    cycles: AtomicUsize,
    removals: AtomicUsize,
    down_until: Mutex<Option<std::time::Instant>>,
}

impl SlowWrites {
    fn new(inner: FakeRemote) -> Arc<Self> {
        Arc::new(SlowWrites {
            inner,
            running: AtomicUsize::new(0),
            most: AtomicUsize::new(0),
            cycles: AtomicUsize::new(0),
            removals: AtomicUsize::new(0),
            down_until: Mutex::new(None),
        })
    }

    /// Fails with an outage while one lasts, else runs `f`.
    fn unless_down<T>(&self, f: impl FnOnce() -> RemoteResult<T>) -> RemoteResult<T> {
        let down = self.down_until.lock().map_or(true, |d| {
            d.is_some_and(|until| std::time::Instant::now() < until)
        });
        if down {
            return Err(hbsync_remote::RemoteError::Transient("outage".into()));
        }
        f()
    }

    fn write<T>(&self, f: impl FnOnce() -> T) -> T {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(20));
        let done = f();
        if self.has_cycle() {
            self.cycles.fetch_add(1, Ordering::SeqCst);
        }
        self.running.fetch_sub(1, Ordering::SeqCst);
        done
    }

    /// Whether some live node on the drive is in a cycle.
    fn has_cycle(&self) -> bool {
        let server = self.inner.server();
        let parents: HashMap<NodeId, NodeId> = server
            .live()
            .filter_map(|h| Some((h.node()?, h.parent)))
            .collect();
        parents.keys().any(|&start| {
            let up = walk::up::<_, ()>(start, |at| Ok(parents.get(&at).map(|&p| ((), p))));
            matches!(
                up,
                Ok(walk::Up {
                    end: walk::End::Cycle,
                    ..
                })
            )
        })
    }
}

impl Remote for SlowWrites {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<ChangesPage> {
        self.inner.changes(cursor, limit)
    }
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn holding_live(
        &self,
        folders: &[hbsync_types::NodeId],
    ) -> RemoteResult<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding_live(folders)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<Target> {
        self.write(|| self.unless_down(|| self.inner.create(node, payload)))
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<Target> {
        self.write(|| self.unless_down(|| self.inner.update(target, meta, payload)))
    }
    fn delete(&self, files: &[RemoteRef]) -> RemoteResult<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(&self, files: &[RemoteRef]) -> RemoteResult<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Target>> {
        self.removals.fetch_add(1, Ordering::SeqCst);
        self.write(|| self.unless_down(|| self.inner.set_removed(files, removed)))
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
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

/// Folder creates and renames run on the worker pool, several at once, and deletes together in
/// one request, each still after what it depends on: a folder before what goes into it, and
/// before what is in it when it goes.
#[test]
fn remote_writes_run_together_in_dependency_order() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let slow = SlowWrites::new(a.remote.clone());
    a.restart_with(Box::new(Arc::clone(&slow)), |_| {})?;
    a.write("keep.txt", "k")?; // deleting every file would be held (the mass-delete guard)
    for i in 0..12 {
        fs::create_dir_all(a.root.join(format!("d{i}/sub/deeper")))?;
        a.write(&format!("d{i}/sub/f.txt"), format!("{i}"))?;
    }
    let mut b = Client::new(&server, "b")?;
    let mut check = |a: &mut Client, what: &str| -> R {
        let report = a.sync()?;
        assert!(report.failed.is_empty(), "{what}: {report:?}");
        let again = a.engine.sync_once("2026-10-08")?;
        assert!(again.is_idle(), "{what}: all done: {again:?}");
        let most = slow.most.swap(0, Ordering::SeqCst);
        assert!(most > 1 || what == "deletes", "{what}: one write at a time");
        assert_eq!(slow.cycles.load(Ordering::SeqCst), 0, "{what}");
        b.sync()?;
        assert_eq!(b.tree_text(""), a.tree_text(""), "{what}");
        Ok(())
    };
    check(&mut a, "creates")?;
    for i in 0..12 {
        fs::rename(a.root.join(format!("d{i}")), a.root.join(format!("e{i}")))?;
    }
    check(&mut a, "renames")?;
    for i in 0..12 {
        fs::remove_dir_all(a.root.join(format!("e{i}")))?;
    }
    check(&mut a, "deletes")?;
    // 48 removals, a request for each level: the folders, then `sub`, then what is in that.
    assert_eq!(slow.removals.load(Ordering::SeqCst), 3);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 1);
    Ok(())
}

/// Two moves that turn a nesting around (B out of A, A into B's child C) don't run together:
/// the drive never holds a cycle, not even for a moment.
#[test]
fn moves_that_turn_a_nesting_around_never_make_a_cycle() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let slow = SlowWrites::new(a.remote.clone());
    a.restart_with(Box::new(Arc::clone(&slow)), |_| {})?;
    fs::create_dir_all(a.root.join("A/B/C"))?;
    a.write("keep.txt", "k")?;
    a.sync()?;
    fs::rename(a.root.join("A/B"), a.root.join("B"))?;
    fs::rename(a.root.join("A"), a.root.join("B/C/A"))?;
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(
        slow.cycles.load(Ordering::SeqCst),
        0,
        "a cycle on the drive"
    );
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));
    assert!(b.root.join("B/C/A").is_dir());
    Ok(())
}

/// A folder's delete waits for an edited file moving out of it in the same batch: one sync
/// does both.
#[test]
fn a_folder_goes_after_an_edited_file_moves_out_of_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("F/c.txt", "v1")?;
    a.sync()?;
    // Moved, then edited (seen apart: one node, moved with new bytes).
    fs::rename(a.root.join("F/c.txt"), a.root.join("c.txt"))?;
    a.engine.scan_local()?;
    a.write("c.txt", "version 2")?;
    fs::remove_dir_all(a.root.join("F"))?;
    a.engine.scan_local()?;
    let report = a.engine.sync_once("2026-10-09")?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert!(report.pending == 0, "{report:?}");
    let live = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(live, ["c.txt", "keep.txt"]);
    Ok(())
}

/// A short outage that fails many writes at once on the pool counts as one failure: the run
/// pauses, and the rest goes through in the same sync.
#[test]
fn a_short_outage_during_many_writes_pauses_and_goes_on() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let slow = SlowWrites::new(a.remote.clone());
    a.restart_with(Box::new(Arc::clone(&slow)), |_| {})?;
    a.write("keep.txt", "k")?;
    for i in 0..20 {
        fs::create_dir_all(a.root.join(format!("d{i:02}")))?;
    }
    a.sync()?;
    for i in 0..20 {
        fs::rename(
            a.root.join(format!("d{i:02}")),
            a.root.join(format!("e{i:02}")),
        )?;
    }
    a.engine.scan_local()?;
    // Down for 50 ms: the eight renames in flight fail together, and the next round too.
    // Counted one by one, they would end the run as unreachable.
    *slow.down_until.lock().map_err(|_| "poisoned")? =
        Some(std::time::Instant::now() + std::time::Duration::from_millis(50));
    let report = a.engine.sync_once("2026-10-09")?;
    assert!(report.failed.is_empty(), "{report:?}");
    let mut expected: Vec<String> = (0..20).map(|i| format!("e{i:02}")).collect();
    expected.push("keep.txt".into());
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        expected
    );
    Ok(())
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
    a.restart_with(Box::new(remote), |_| {})?;
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
    assert_eq!(b.tree_text(""), a.tree_text(""));
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
    a.restart_with(Box::new(remote), |_| {})?;
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
    // Trying every op four times would be 240 requests. An outage stops the pool, and the
    // writes in flight count as one failure: a pool's worth (8) at most for each of the 6.
    let requests = a.remote.faults().requests - before;
    assert!(requests <= 50, "gave up after {requests} requests");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 0);

    // Back up: everything goes through.
    a.remote.faults().script.clear();
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));
    assert_eq!(b.tree_text("").len(), 60);
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
    a.restart()?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(b.tree_text("").len(), 13);
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
    assert_eq!(a.read_text("doc.txt")?, "keep me");
    assert_eq!(a.tree_text("").len(), 1);
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

/// b deletes what a changes meanwhile (`gone`), so b's next sync puts it back (a folder with
/// only what was added to it), and a adds g. b's placement then fails its commit write once
/// its item is in place: the run stops, the record kept. The next sync's recovery, before any
/// scan, looks at the disk first: the item put in place but not committed is there to settle,
/// though no scan has seen it. Taken for absent, its record went, and the next scan found the
/// item as new: a node with a base it isn't paired with, so it went up as a duplicate.
fn a_placement_whose_commit_failed_is_kept(
    setup: impl Fn(&Client) -> std::io::Result<()>,
    gone: Option<&str>,
    change: impl Fn(&Client) -> std::io::Result<()>,
    placed: &str,
    names: &[&str],
) -> R {
    let server = server();
    let faults = Arc::new(hbsync_engine::exec::Faults::default());
    let mut a = Client::new(&server, "a")?;
    let failing = Arc::clone(&faults);
    let mut b = Client::with_config(&server, "b", move |c| {
        c.batch = 1;
        c.exec.faults = Arc::clone(&failing);
    })?;
    setup(&a)?;
    a.sync()?;
    b.sync()?;
    if let Some(gone) = gone {
        let path = b.root.join(gone);
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    change(&a)?;
    a.write("g", "new")?;
    a.sync()?;
    faults.fail(FailPoint::Commit, usize::MAX);
    b.engine.scan_local()?;
    assert!(b.engine.sync_once("2026-10-08").is_err(), "the run stops");
    faults.fail(FailPoint::Commit, 0);
    // The next sync, before any scan: its recovery looks at the disk itself, and takes the
    // item for its node.
    b.engine.sync_once("2026-10-08")?;
    let path = hbsync_platform::RelPath::new(placed)?;
    let taken = b.engine.db().read(|s| {
        Ok(match s.get_local(&path)? {
            Some(row) => s.get_base(row.node)?.is_some(),
            None => false,
        })
    })?;
    assert!(taken, "{placed} isn't its node's");
    b.sync()?;
    a.sync()?;
    b.sync()?;
    let mut live = server.lock().map_err(|_| "poisoned")?.live_names();
    live.sort();
    assert_eq!(live, names, "a duplicate went up");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

#[test]
fn a_download_whose_commit_failed_leaves_no_duplicate() -> R {
    a_placement_whose_commit_failed_is_kept(
        |a| a.write("f", "one"),
        Some("f"),
        |a| a.write("f", "two"),
        "f",
        &["f", "g"],
    )
}

/// The drive moves a file to another folder and edits it: b's download puts the new version in
/// place there and the old one in the trash, from the folder it was in. That folder is
/// rescanned too: the node's row there was stale, and recovery took the new file for another
/// item's.
#[test]
fn a_download_into_another_folder_whose_commit_failed_leaves_no_duplicate() -> R {
    a_placement_whose_commit_failed_is_kept(
        |a| {
            a.write("p1/f", "one")?;
            a.write("p2/k", "k")
        },
        None,
        |a| {
            fs::rename(a.root.join("p1/f"), a.root.join("p2/f"))?;
            a.write("p2/f", "two")
        },
        "p2/f",
        &["f", "g", "k", "p1", "p2"],
    )
}

#[test]
fn a_folder_whose_commit_failed_leaves_no_duplicate() -> R {
    a_placement_whose_commit_failed_is_kept(
        |a| a.write("d/x", "x"),
        Some("d"),
        |a| a.write("d/y", "y"),
        "d",
        &["d", "g", "y"],
    )
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
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.crash_at = None)?;
    b.sync()?;
    a.sync()?;
    assert_eq!(b.read_text("x/inside")?, "new");
    assert_eq!(a.tree_text(""), b.tree_text(""));
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
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.crash_at = None)?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(b.read_text("papers/notes.txt")?, "a's edit");
    assert_eq!(
        b.read_text("papers/notes (conflict b 2026-10-08).txt")?,
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
    assert_eq!(a.read_text("B/doc.txt")?, "keep me");
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
    assert_eq!(b.tree_text(""), a.tree_text(""));
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
    assert_eq!(a.tree_text(""), b.tree_text(""));
    let contents: Vec<String> = b
        .tree_text("")
        .into_iter()
        .map(|e| e.split(" = ").nth(1).unwrap_or_default().to_owned())
        .collect();
    assert!(
        contents.contains(&"remote version".to_owned())
            && contents.contains(&"local version".to_owned()),
        "{:?}",
        b.tree_text("")
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
    let report = b.engine.start_up(&mut observer, "2026-10-08")?;
    assert!(report.sync.feed.live >= 2, "{:?}", report.sync.feed);
    assert_eq!(report.local.report.added, 2);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(a.read_text("from-b/two.txt")?, "2");
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
    assert_eq!(b.read_text("b")?, "A");
    assert_eq!(b.read_text("sub/b")?, "B");
    assert_eq!(a.tree_text(""), b.tree_text(""));
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
        b.tree_text(""),
        ["Photos/", "Photos/p.jpg = p", "Report.pdf = r"]
    );
    assert_eq!(a.tree_text(""), b.tree_text(""));
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
        b.tree_text(""),
        [
            "Photos/",
            "Photos/new.jpg = new",
            "archive/",
            "archive/old.jpg = old"
        ]
    );
    assert_eq!(a.tree_text(""), b.tree_text(""));
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
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(b.read_text("notes.txt")?, "a's edit");
    assert_eq!(b.read_text(to.as_str())?, "b's edit");
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
    assert_eq!(a.tree_text(""), b.tree_text(""));
    let copies = b
        .tree_text("")
        .iter()
        .filter(|e| e.ends_with("= same local bytes"))
        .count();
    assert_eq!(copies, 2, "{:?}", b.tree_text(""));
    assert_eq!(b.read_text("f.txt")?, "a2");
    Ok(())
}

/// Two computers with the same bytes against the drive's other version each make their own
/// conflict copy, under its own id: two copies, both named, nothing lost, and quiet after a
/// few rounds. (They shared one id: the second made its copy under the first's, with the
/// drive's node as its state, and could name it "".)
#[test]
fn two_computers_with_the_same_conflict_make_their_own_copies() -> R {
    let server = server();
    let mut phone = Client::new(&server, "phone")?;
    let mut laptop = Client::new(&server, "laptop")?;
    let mut desk = Client::new(&server, "desk")?;
    phone.write("report.txt", "base")?;
    phone.sync()?;
    laptop.sync()?;
    desk.sync()?;
    phone.write("report.txt", "theirs")?;
    phone.sync()?;
    laptop.write("report.txt", "mine")?;
    desk.write("report.txt", "mine")?;
    laptop.sync()?;
    desk.sync()?;
    let mut quiet = false;
    for _ in 0..6 {
        let done = phone.sync()?.ops_done + laptop.sync()?.ops_done + desk.sync()?.ops_done;
        if done == 0 {
            quiet = true;
            break;
        }
    }
    assert!(quiet, "still syncing after 6 rounds");
    let tree = desk.tree_text("");
    assert_eq!(
        tree,
        [
            "report (conflict desk 2026-10-08).txt = mine",
            "report (conflict laptop 2026-10-08).txt = mine",
            "report.txt = theirs",
        ]
    );
    assert_eq!(laptop.tree_text(""), tree);
    assert_eq!(phone.tree_text(""), tree);
    let server = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(server.live().count(), 3);
    assert!(
        server.live_names().iter().all(|n| !n.is_empty()),
        "{:?}",
        server.live_names()
    );
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
    assert_eq!(b.tree_text(""), ["photo.jpg = photo bytes"]);
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
    let cell = std::cell::RefCell::new(db);
    hbsync_engine::feed::pull(
        || cell.borrow_mut(),
        &FakeRemote::new(server.clone()),
        500,
        &mut None,
    )?;
    let mut db = cell.into_inner();
    let started = std::time::Instant::now();
    let adopted = hbsync_engine::adopt::adopt(
        &mut db,
        dir.path(),
        hbsync_platform::Os::current(),
        &hbsync_engine::scope::Scope::main(),
    )?;
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
    assert_eq!(b.read_text("doc.txt")?, "v2");
    let left: Vec<_> = fs::read_dir(&tmp)?
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(left.is_empty(), "{left:?}");
    Ok(())
}

/// A folder's removal that fails holds back the removals of what is in it, so nothing is
/// removed before its folder (node schema, Recently deleted): once the folder goes, what was in
/// it goes with it, and is deleted with it.
#[test]
fn a_folder_that_isnt_removed_keeps_what_is_in_it() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    a.write("Photos/x.jpg", "x")?;
    a.write("Photos/2024/y.jpg", "y")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    // The folder's removal keeps failing during this sync.
    a.remote.faults().fail_writes_named = Some("Photos".into());
    let report = a.sync()?;
    assert!(!report.failed.is_empty(), "{report:?}");
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.removed().count(),
        0,
        "nothing went before its folder"
    );
    a.remote.faults().fail_writes_named = None;
    a.sync()?;
    let listed = a.recently_deleted(10)?;
    let photos = listed
        .iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    assert_eq!(listed.len(), 4, "{listed:?}");
    assert!(
        listed.iter().all(|i| i.root == photos.node),
        "all deleted with the folder: {listed:?}"
    );
    Ok(())
}

/// A folder's removal took along a version another device never had (its own removal still
/// on its way): that device's rename of its older copy doesn't write over it. The version stays,
/// and goes with the folder into Recently deleted; the renamed copy goes up as a new item.
#[test]
fn a_rename_doesnt_write_over_a_version_taken_along() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Photos/x.txt", "old")?;
    a.sync()?;
    b.sync()?;
    a.write("Photos/x.txt", "new")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    // The folder goes; the file's own removal keeps failing during this sync.
    a.remote.faults().fail_writes_named = Some("x.txt".into());
    a.sync()?;
    a.remote.faults().fail_writes_named = None;
    // b never had "new": it renames its "old".
    b.engine.scan_local()?;
    fs::rename(b.root.join("Photos/x.txt"), b.root.join("Photos/y.txt"))?;
    b.sync()?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("Photos/y.txt")?, "old");
    let removed: Vec<String> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert!(removed.contains(&"Photos/x.txt".to_owned()), "{removed:?}");
    let x = server
        .lock()
        .map_err(|_| "poisoned")?
        .removed()
        .find(|h| matches!(&h.body, hbsync_remote::RemoteBody::Live { content, .. } if content.name == "x.txt"))
        .map(|h| h.file_id)
        .ok_or("x.txt not in Recently deleted")?;
    let bytes = server.lock().map_err(|_| "poisoned")?.payload(x)?;
    assert_eq!(bytes.as_slice(), b"new", "the version taken along stays");
    Ok(())
}

/// A device deleting a folder stops half way and never comes back: what is in it is still live,
/// taken along by the folder's removal. Another device's purge, once the folder is kept long
/// enough, removes them first (to Recently deleted, still restorable), and they go for good
/// with the folder once their own removal is kept as long. Nothing else goes.
#[test]
fn the_purge_finishes_a_delete_that_stopped_half_way() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    a.write("Photos/x.txt", "x")?;
    a.write("Photos/2024/y.txt", "y")?;
    a.write("keep.txt", "k")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    a.remote.faults().fail_writes_named = Some("2024".into());
    a.sync()?;
    let count = || {
        server
            .lock()
            .map(|s| (s.live().count(), s.removed().count()))
            .unwrap_or_default()
    };
    assert_eq!(count(), (3, 2), "2024, y.txt and keep.txt are live");
    // a is gone for good. b, which doesn't sync (a phone, say, or a computer syncing another
    // part: one syncing it finishes the delete), purges, the folder kept long enough: 2024 and
    // y.txt are removed, a folder first, and are restorable; nothing goes for good yet.
    drop(a);
    let mut b = Client::new(&server, "b")?;
    assert_eq!(b.engine.drive().purge(0)?, 0);
    assert_eq!(count(), (1, 4));
    let photos = b
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    assert!(!photos.stays, "nothing live holds it any more");
    // Their own removal kept long enough: all of it goes.
    assert_eq!(b.engine.drive().purge(0)?, 4);
    assert_eq!(count(), (1, 0));
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    Ok(())
}

/// The device deleting a folder stops after the folder's removal: what is in it is still live,
/// unmarked, taken along by the folder's removal (node schema, Recently deleted). Emptying the
/// folder from Recently deleted, on another device, deletes for good only what is removed: the
/// folder stays while it holds live nodes, which emptying never deletes for good (the purge
/// removes them first: [`the_purge_finishes_a_delete_that_stopped_half_way`]). Taken along,
/// they show nowhere.
#[test]
fn emptying_a_folder_never_deletes_what_is_live_in_it() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    a.write("Photos/x.txt", "x")?;
    a.write("Photos/2024/y.txt", "y")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    // The folder goes; 2024's removal fails, holding back y.txt's; a stops there.
    a.remote.faults().fail_writes_named = Some("2024".into());
    a.sync()?;
    let live = |server: &std::sync::Arc<std::sync::Mutex<hbsync_remote::fake::FakeServer>>| {
        server
            .lock()
            .map(|s| s.live().count())
            .unwrap_or(usize::MAX)
    };
    assert_eq!(live(&server), 2, "2024 and y.txt are still live");
    let photos = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    // Listed as staying: emptying it leaves it.
    assert!(photos.stays);
    let mut reader =
        hbsync_engine::root::AccountReader::open(&a.db_path(), "test.example")?.ok_or("no DB")?;
    let (items, summary) = reader.deletes(10)?;
    assert!(items[0].0.stays);
    assert_eq!(
        (summary.stays, summary.items),
        (1, 1),
        "x.txt goes, Photos stays"
    );
    // Another device, which never synced, empties it.
    let mut b = Client::new(&server, "b")?;
    assert_eq!(b.engine.drive().empty(&[photos.node])?, 1, "x.txt");
    assert_eq!(live(&server), 2, "2024 and y.txt stay");
    let left: Vec<String> = b
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert_eq!(left, ["Photos"]);
    b.sync()?;
    assert_eq!(b.tree_text(""), Vec::<String>::new());
    Ok(())
}

/// A folder emptied from Recently deleted takes with it what was deleted from it before, listed
/// on its own (a sub-folder deleted first): all removed, so it may go. The listing counts that
/// as in it, and nothing of it stays.
#[test]
fn emptying_a_folder_takes_what_was_deleted_from_it_before() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    a.write("other.txt", "o")?;
    a.write("run/keep.txt", "k")?;
    a.write("run/data/x.txt", "x")?;
    a.write("run/data/sub/y.txt", "y")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("run/data"))?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("run"))?;
    a.sync()?;
    let mut reader =
        hbsync_engine::root::AccountReader::open(&a.db_path(), "test.example")?.ok_or("no DB")?;
    let (items, summary) = reader.deletes(10)?;
    let listed: Vec<(String, usize, bool)> = items
        .iter()
        .map(|(d, along)| (d.path.clone(), *along, d.stays))
        .collect();
    assert_eq!(
        listed,
        [
            ("run".to_owned(), 5, false),
            ("run/data".to_owned(), 3, false)
        ],
        "run holds keep.txt, data and data's three"
    );
    assert_eq!(summary.items, 6);
    let run = items[0].0.node;
    assert_eq!(a.engine.drive().empty(&[run])?, 6);
    assert!(a.recently_deleted(10)?.is_empty());
    let s = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(
        (s.removed().count(), s.live_names()),
        (0, vec!["other.txt".to_owned()])
    );
    drop(s);
    b.sync()?;
    assert!(b.recently_deleted(10)?.is_empty());
    assert_eq!(b.tree_text(""), ["other.txt = o"]);
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
    assert_eq!(b.tree_text(""), a.tree_text(""));
    Ok(())
}

/// A folder's delete removes the folder first, then what is in it (node schema, Recently
/// deleted). A child whose removal failed follows later; meanwhile another client doesn't bring
/// the folder back for it (§6.3), and the folder's restore brings both back.
#[test]
fn a_folder_goes_first_and_a_child_that_failed_follows() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("d/keep.txt", "k")?;
    a.write("d/other.txt", "o")?;
    a.write("stay.txt", "s")?; // not everything is deleted: no mass-delete hold
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    // The delete of one file keeps failing.
    a.remote.faults().fail_writes_named = Some("keep.txt".into());
    a.sync()?;
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["keep.txt", "stay.txt"],
        "the folder went first"
    );
    // b sees the delete under way: its copies go, the folder stays deleted.
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = s"]);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["stay.txt"],
        "b removed the rest of it"
    );
    a.remote.faults().fail_writes_named = None;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["stay.txt = s"]);
    // Everything was deleted with the folder: its restore brings it all back.
    let d = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "d")
        .ok_or("d")?;
    assert_eq!(d.root, d.node);
    assert_eq!(a.engine.drive().restore(&[d.node])?, 3);
    a.sync()?;
    b.sync()?;
    assert_eq!(a.read_text("d/keep.txt")?, "k");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A folder's delete and restore go to the drive a level at a time, each level in one request
/// (`update-archival-status-batch`), the folder before what is in it. A name a live sibling took
/// meanwhile is a rewrite of its own.
#[test]
fn deletes_and_restores_go_a_level_at_a_time() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    for i in 0..20 {
        a.write(&format!("d/f{i:02}.txt"), format!("{i}"))?;
    }
    a.write("d/sub/g.txt", "g")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    let before = a.remote.faults().requests;
    a.sync()?;
    assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 23);
    let delete = a.remote.faults().requests - before;
    // The sync reads the feed too; one request per removal would be 23.
    assert!(delete <= 6, "{delete} requests");
    // Restore it all: d, then its 21 children, then g.txt in sub: three levels.
    let d = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "d")
        .ok_or("d")?;
    let before = a.remote.faults().requests;
    assert_eq!(a.engine.drive().restore(&[d.node])?, 23);
    // The feed read first, then a request per level.
    assert_eq!(a.remote.faults().requests - before, 1 + 3);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 0);
    a.sync()?;
    assert_eq!(a.read_text("d/sub/g.txt")?, "g");
    assert_eq!(a.tree_text("").len(), 23);
    Ok(())
}

/// A removal request whose answer is lost (after the drive wrote it) is never sent again blind:
/// every file in it waits for the feed, which shows them removed.
#[test]
fn a_removal_request_whose_answer_is_lost_is_not_sent_again() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..5 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("stay.txt", "s")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.remote.faults().lose_answer_named = Some("f2.txt".into());
    a.sync()?;
    a.remote.faults().lose_answer_named = None;
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 6);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["stay.txt"]
    );
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = s"]);
    // Deleted with d: its item.
    let items = a.recently_deleted(10)?;
    let d = items.iter().find(|i| i.path == "d").ok_or("d")?;
    assert!(items.iter().all(|i| i.root == d.node), "{items:?}");
    Ok(())
}

/// The Photos example: on Monday `Photos/a.jpg` is deleted on its own; on Tuesday the folder
/// `Photos`, still holding `b.jpg` and a subfolder with a file. Restoring `Photos` brings back
/// `b.jpg` and the subfolder's file, not `a.jpg`: that stays in Recently deleted, an item of its
/// own, and comes back on its own, into `Photos`.
#[test]
fn photos_restored_without_what_was_deleted_from_it_before() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Photos/a.jpg", "a")?;
    a.write("Photos/b.jpg", "b")?;
    a.write("Photos/2024/c.jpg", "c")?;
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("Photos/a.jpg"))?; // Monday
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?; // Tuesday
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    let listed = b.recently_deleted(10)?;
    let item = |path: &str| {
        listed
            .iter()
            .find(|i| i.path == path)
            .ok_or(format!("{path} not listed"))
    };
    let (photos, a_jpg, b_jpg, c_jpg) = (
        item("Photos")?,
        item("Photos/a.jpg")?,
        item("Photos/b.jpg")?,
        item("Photos/2024/c.jpg")?,
    );
    assert_eq!(a_jpg.root, a_jpg.node, "deleted on its own");
    assert_eq!(
        (b_jpg.root, c_jpg.root),
        (photos.node, photos.node),
        "deleted with the folder"
    );
    assert!(a_jpg.removed_at_ms < photos.removed_at_ms);
    assert!(
        photos.removed_at_ms <= b_jpg.removed_at_ms,
        "the folder went first"
    );

    // b restores Photos: b.jpg and 2024/c.jpg come back, on both; a.jpg doesn't.
    assert_eq!(b.engine.drive().restore(&[photos.node])?, 4);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read_text("Photos/b.jpg")?, "b");
    assert_eq!(a.read_text("Photos/2024/c.jpg")?, "c");
    assert!(a.read_text("Photos/a.jpg").is_err());
    let left: Vec<String> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert_eq!(left, ["Photos/a.jpg"]);

    // a.jpg on its own: back into Photos.
    assert_eq!(a.engine.drive().restore(&[a_jpg.node])?, 1);
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("Photos/a.jpg")?, "a");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert!(a.recently_deleted(10)?.is_empty());
    Ok(())
}

// ---- §10 filters ----

fn ignoring(rules: &'static str) -> impl Fn(&mut hbsync_engine::EngineConfig) {
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
    assert_eq!(b.tree_text(""), ["proj/", "proj/doc.txt = v1"]);

    // b's own junk in the folder doesn't hold up a's delete of it.
    b.write("proj/Thumbs.db", "thumbs")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    assert_eq!(a.tree_text(""), ["~$doc.docx = lock"]);
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
        b.tree_text(""),
        ["proj/", "proj/main.js = m", "readme.txt = r"]
    );

    b.write("proj/node_modules/x/y.js", "dep")?;
    b.write("proj/debug.log", "b's log")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
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
        b.tree_text(""),
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
    assert_eq!(a.tree_text(""), ["proj/", "readme.txt = r"]);
    Ok(())
}

/// A device whose rules leave it one top-level folder of the drive (`/*`, `!/mine/`) takes
/// nothing else down and writes nothing else: not another folder's files, nor the name clash
/// in it (two files of one name, which a device that sees them renames on the drive). How the
/// live end-to-end tests keep to their own folder on a drive other tests share.
#[test]
fn a_device_kept_to_one_folder_touches_nothing_else_on_the_drive() -> R {
    use hbsync_remote::Remote;
    let server = server();
    let other = FakeRemote::new(server.clone());
    let elsewhere = hbsync_remote::contract::folder("elsewhere", NodeId::ROOT);
    other.create(&elsewhere, None)?;
    for data in [b"one", b"two"] {
        server.lock().map_err(|_| "poisoned")?.seed_file(
            elsewhere.node,
            "f",
            data,
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
    }
    let versions = || -> Result<Vec<_>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let mut v: Vec<_> = srv.live().map(|h| (h.file_id, h.version)).collect();
        v.sort_by_key(|(f, _)| f.0);
        Ok(v)
    };
    let before = versions()?;

    let mut a = Client::with_config(&server, "a", ignoring("/*\n!/mine/\n"))?;
    let mut b = Client::with_config(&server, "b", ignoring("/*\n!/mine/\n"))?;
    a.write("mine/x.txt", "x")?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    for c in [&a, &b] {
        assert_eq!(c.tree_text(""), ["mine/", "mine/x.txt = x"], "{}", c.name);
    }
    let after: Vec<_> = versions()?
        .into_iter()
        .filter(|(f, _)| before.iter().any(|(g, _)| g == f))
        .collect();
    assert_eq!(after, before, "nothing else on the drive was written");

    // A device that sees the folder resolves the clash on the drive.
    let mut c = Client::new(&server, "c")?;
    c.sync()?;
    assert!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .live_names()
            .iter()
            .any(|n| n.starts_with("f (")),
        "renamed"
    );
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
    assert_eq!(b.tree_text(""), ["draft.txt = d2 from b"]);
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "build/",
            "build/draft.txt = d1",
            "build/notes.txt = n",
            "draft.txt = d2 from b"
        ]
    );
    Ok(())
}

/// `c` restarted with its config changed by `tweak`, as at a start-up: returns whether the
/// filter changed.
fn restart_with(
    c: &mut Client,
    tweak: impl FnOnce(&mut hbsync_engine::EngineConfig),
) -> Result<bool, Box<dyn std::error::Error>> {
    let remote = Box::new(c.remote.clone());
    c.restart_with(remote, tweak)?;
    Ok(c.engine.apply_filter_change()?)
}

/// `c` restarted with the ignore rules `rules`.
fn restart_ignoring(c: &mut Client, rules: &'static str) -> R {
    assert!(restart_with(c, ignoring(rules))?, "the filter changed");
    assert!(!c.engine.apply_filter_change()?, "and is recorded");
    Ok(())
}

#[test]
fn a_folder_no_longer_ignored_comes_with_everything_in_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("readme.txt", "r")?;
    a.write("build/out.txt", "o")?;
    a.write("build/deep/more.txt", "m")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["readme.txt = r"]);

    restart_ignoring(&mut b, "")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "build/",
            "build/deep/",
            "build/deep/more.txt = m",
            "build/out.txt = o",
            "readme.txt = r"
        ]
    );
    Ok(())
}

#[test]
fn a_folder_moved_out_of_an_ignored_one_comes_with_everything_in_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("readme.txt", "r")?;
    a.write("build/sub/out.txt", "o")?;
    a.write("build/sub/deep/more.txt", "m")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["readme.txt = r"]);

    fs::rename(a.root.join("build/sub"), a.root.join("sub"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "readme.txt = r",
            "sub/",
            "sub/deep/",
            "sub/deep/more.txt = m",
            "sub/out.txt = o"
        ]
    );
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
    assert_eq!(b.read_text("build/out.o")?, "obj");

    // a starts ignoring build/: it leaves the drive and b (for b's trash), and stays on a.
    restart_ignoring(&mut a, "build/\n")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(a.read_text("build/out.o")?, "obj");
    assert_eq!(
        b.tree_text(""),
        ["readme = r", "src/", "src/main.rs = fn main"]
    );

    // The rule goes: build/ is new on a again, and goes back up.
    restart_ignoring(&mut a, "")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("build/out.o")?, "obj");
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
        b.tree_text(""),
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
        a.tree_text(""),
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
    restart_ignoring(&mut a, "*.log\n")?;
    a.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
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
    assert!(
        !report.unfinished,
        "held deletes aren't retried: {report:?}"
    );

    b.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 6);
    Ok(())
}

/// Deletes held for a confirmation leave nodes pending that another sync can't do anything
/// about: the sync isn't unfinished, however many nodes one held delete covers.
#[test]
fn held_deletes_leave_no_unfinished_work() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.mass_delete_min = 2)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert!(report.pending > report.held.len() as u64, "{report:?}");
    assert!(
        !report.unfinished,
        "held deletes aren't retried: {report:?}"
    );
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

/// Recently deleted by deletes: a folder with what it took along is one entry with its count;
/// any of its items can still be found, by node or (all of them) to pick by place.
#[test]
fn recently_deleted_lists_deletes_with_counts_and_finds_items() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    for i in 0..30 {
        a.write(&format!("d/f{i}.txt"), "x")?;
    }
    a.write("z.txt", "z")?;
    a.write("keep.txt", "k")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    fs::remove_file(a.root.join("z.txt"))?;
    a.sync()?;

    let mut reader =
        hbsync_engine::root::AccountReader::open(&a.db_path(), "test.example")?.ok_or("no db")?;
    let (deletes, summary) = reader.deletes(10)?;
    let mut shown: Vec<_> = deletes.iter().map(|(d, n)| (d.path.as_str(), *n)).collect();
    shown.sort();
    assert_eq!(shown, [("d", 30), ("z.txt", 0)]);
    assert_eq!((summary.deletes, summary.items), (2, 32));
    assert!(summary.listed.seq >= 32, "{summary:?}");
    let (cut, summary) = reader.deletes(1)?;
    assert_eq!(cut.len(), 1, "cut by deletes");
    assert_eq!(summary.items, 32, "the summary counts everything");

    let all = reader.all_deleted()?;
    assert_eq!(all.len(), 32);
    let child = all
        .iter()
        .find(|(d, _)| d.path == "d/f7.txt")
        .ok_or("f7")?
        .0
        .node;
    let folder = all.iter().find(|(d, _)| d.path == "d").ok_or("d")?.0.node;
    let found = reader.find_deleted(&[child, folder])?;
    let found: Vec<_> = found.iter().map(|(d, n)| (d.path.as_str(), *n)).collect();
    assert_eq!(found, [("d/f7.txt", 0), ("d", 30)]);
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
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    let listed = b.recently_deleted(10)?;
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
    assert_eq!(b.engine.drive().restore(&[d])?, 4);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert_eq!(a.read_text("d/sub/y.txt")?, "y");
    assert!(a.read_text("z.txt").is_err());

    // A restore onto a name taken meanwhile gets another.
    a.write("z.txt", "new z")?;
    a.sync()?;
    let z = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "z.txt")
        .ok_or("z")?
        .node;
    assert_eq!(a.engine.drive().restore(&[z])?, 1);
    a.sync()?;
    assert_eq!(a.read_text("z (restored).txt")?, "z");
    assert_eq!(a.read_text("z.txt")?, "new z");

    // The purge leaves nothing restorable.
    fs::remove_file(a.root.join("z.txt"))?;
    a.sync()?;
    assert_eq!(a.engine.drive().purge(0)?, 1);
    b.sync()?;
    assert!(b.recently_deleted(10)?.is_empty());
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
    assert_eq!(a.read_text("notes.txt")?, "1 and 2");
    assert!(a.recently_deleted(10)?.is_empty());
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!((srv.live().count(), srv.removed().count()), (1, 0));
    Ok(())
}

#[test]
fn an_edit_keeps_a_deleted_version_it_never_had() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("keep.txt", "k")?;
    a.write("notes.txt", "1")?;
    a.sync()?;
    b.sync()?;
    a.write("notes.txt", "1, a's")?;
    a.sync()?;
    fs::remove_file(a.root.join("notes.txt"))?;
    a.sync()?; // a's version, removed
    b.write("notes.txt", "1, b's")?; // b edits the version before, never having a's
    b.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read_text("notes.txt")?, "1, b's");
    let srv = server.lock().map_err(|_| "poisoned")?;
    let kept: Vec<_> = srv.removed().filter_map(|h| h.node()).collect();
    assert_eq!(kept.len(), 1, "a's version stays in Recently deleted");
    Ok(())
}

/// A restore brings nodes back under their old ids: a confirmation of their earlier deletes
/// must not let a later delete of them through unasked.
#[test]
fn a_restored_item_is_guarded_again() -> R {
    let server = server();
    let guard = |cfg: &mut hbsync_engine::EngineConfig| cfg.mass_delete_min = 2;
    let mut a = Client::with_config(&server, "a", guard)?;
    let mut b = Client::with_config(&server, "b", guard)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), "x")?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(b.root.join("d"))?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    b.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    b.sync()?;
    let report = a.sync()?;
    assert!(!report.held.is_empty(), "a holds b's delete: {report:?}");
    a.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    a.sync()?;
    assert!(!a.root.join("d").exists());

    let d = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "d")
        .ok_or("d")?
        .node;
    assert_eq!(a.engine.drive().restore(&[d])?, 5);
    a.sync()?;
    b.sync()?;
    assert!(b.root.join("d/f3.txt").exists());

    fs::remove_dir_all(a.root.join("d"))?;
    let report = a.sync()?;
    assert!(
        !report.held.is_empty(),
        "a holds the second delete: {report:?}"
    );
    a.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "b holds it too: {report:?}");
    assert!(b.root.join("d/f3.txt").exists());
    Ok(())
}

#[test]
fn a_subfolder_restores_with_what_was_in_it_and_empty_spares_later_deletes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("P/E/x.txt", "x")?;
    a.write("P/y.txt", "y")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("P"))?;
    a.sync()?;
    let find = |a: &mut common::Client, path: &str| -> Result<_, Box<dyn std::error::Error>> {
        Ok(a.recently_deleted(10)?
            .into_iter()
            .find(|i| i.path == path)
            .ok_or(format!("{path} not listed"))?)
    };
    // P/E was taken along by P's delete: restoring it brings back P (alone) and E's content.
    let e = find(&mut a, "P/E")?.node;
    assert_eq!(a.engine.drive().restore(&[e])?, 3);
    a.sync()?;
    assert_eq!(a.read_text("P/E/x.txt")?, "x");
    assert!(
        a.read_text("P/y.txt").is_err(),
        "the rest of P stays deleted"
    );

    // Empty as listed: what was deleted after the listing stays.
    find(&mut a, "P/y.txt")?;
    let listed = a.listed()?;
    fs::remove_file(a.root.join("keep.txt"))?;
    a.write("other.txt", "o")?; // not every file deleted (the guard)
    a.sync()?;
    assert_eq!(a.engine.drive().empty_listed(&listed)?, 1);
    let left: Vec<_> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert_eq!(left, ["keep.txt"]);
    Ok(())
}

/// Finder tags go both ways between two folders, on files and folders, and a download that
/// replaces a file keeps them. (Linux holds no colours; how they are kept for the Macs is the
/// simulator's.)
#[test]
fn tags_sync_between_folders() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Docs/report.txt", "quarterly")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("Docs"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let names = |c: &Client, rel: &str| -> Vec<String> {
        tags::read(&c.root.join(rel))
            .unwrap_or_default()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    };
    let tick = || std::thread::sleep(std::time::Duration::from_millis(20));
    let drive_tags = |name: &str| -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let content = srv
            .live()
            .find_map(|h| match &h.body {
                hbsync_remote::RemoteBody::Live { content, .. } if content.name == name => {
                    Some(content.clone())
                }
                _ => None,
            })
            .ok_or("not on the drive")?;
        Ok(content
            .tags()
            .iter()
            .map(|t| format!("{}:{}", t.name, t.colour))
            .collect())
    };
    let red = if cfg!(target_os = "macos") {
        "Red:6"
    } else {
        "Red:0"
    };
    a.sync()?;
    b.sync()?;

    // Tagged on a: a file (with a colour) and a folder.
    tick();
    tags::write(
        &a.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Red", 6)]),
    )?;
    tags::write(&a.root.join("Docs"), &Tags::new([Tag::new("Work", 0)]))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(names(&b, "Docs/report.txt"), ["Red"]);
    assert_eq!(names(&b, "Docs"), ["Work"]);
    assert_eq!(drive_tags("report.txt")?, [red]);

    // b adds one.
    tick();
    tags::write(
        &b.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Red", 6), Tag::new("Todo", 0)]),
    )?;
    b.sync()?;
    a.sync()?;
    assert_eq!(drive_tags("report.txt")?, [red, "Todo:0"]);
    assert_eq!(names(&a, "Docs/report.txt"), ["Red", "Todo"]);

    // A new version from a replaces b's file: the tags stay.
    a.write("Docs/report.txt", "quarterly, revised")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("Docs/report.txt")?, "quarterly, revised");
    assert_eq!(names(&b, "Docs/report.txt"), ["Red", "Todo"]);

    // A folder gets a new child and a tag in one go: the tag lands though the folder changed.
    tick();
    a.write("Docs/new.txt", "new")?;
    tags::write(
        &a.root.join("Docs"),
        &Tags::new([Tag::new("Work", 0), Tag::new("Q3", 0)]),
    )?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(names(&b, "Docs"), ["Q3", "Work"]);
    assert_eq!(b.read_text("Docs/new.txt")?, "new");

    // Removed on b, removed on a.
    tick();
    tags::write(
        &b.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Todo", 0)]),
    )?;
    b.sync()?;
    a.sync()?;
    assert_eq!(names(&a, "Docs/report.txt"), ["Todo"]);
    assert_eq!(drive_tags("report.txt")?, ["Todo:0"]);
    // Nothing left to do on either side.
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// A copy proven equal only across hash algorithms gets its base though the tags differ (the
/// drive's are tagged, the local copy isn't): no conflict copy, and the tags come down.
#[test]
fn adopting_a_sha256_copy_whose_tags_differ_keeps_one_file() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    let server = server();
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let (node, written) = srv.seed_file(
            hbsync_types::NodeId::ROOT,
            "photo.jpg",
            b"photo bytes",
            hbsync_fileio::HashAlgorithm::Sha256,
        )?;
        let mut content =
            hbsync_types::schema::NodeContent::file("photo.jpg", 1_700_000_000_000, 11, false);
        content.set_tags(&Tags::new([Tag::new("Blue", 4)]));
        let meta = NodeMeta {
            node,
            parent: hbsync_types::NodeId::ROOT,
            kind: hbsync_types::NodeKind::File,
            content,
            removed: false,
        };
        srv.update(written.file_id, written.version, &meta, None)?;
    }
    let mut b = Client::new(&server, "b")?;
    b.write("photo.jpg", "photo bytes")?;
    let report = b.sync()?;
    assert!(report.conflicts.is_empty(), "{:?}", report.conflicts);
    assert_eq!(b.tree_text(""), ["photo.jpg = photo bytes"]);
    if tags::CAN_TAG && tags::read(&b.root.join("photo.jpg")).is_some() {
        let names: Vec<String> = tags::read(&b.root.join("photo.jpg"))
            .unwrap_or_default()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(names, ["Blue"]);
    }
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

// ---- §12 scopes ----

/// The role folder `role` on the drive, made by another device, with one file in it.
fn seed_role(
    server: &Arc<std::sync::Mutex<hbsync_remote::fake::FakeServer>>,
    role: hbsync_types::Role,
    file: &str,
    data: &str,
) -> R {
    let mut srv = server.lock().map_err(|_| "poisoned")?;
    let folder = NodeMeta {
        node: role.node(),
        ..hbsync_remote::contract::folder(role.name(), hbsync_types::NodeId::ROOT)
    };
    srv.create(&folder, None)?;
    srv.seed_file(
        role.node(),
        file,
        data.as_bytes(),
        hbsync_fileio::HashAlgorithm::Blake3,
    )?;
    Ok(())
}

fn unscoped(cfg: &mut hbsync_engine::EngineConfig) {
    cfg.exec.scope = Arc::new(hbsync_engine::scope::Scope {
        top: hbsync_types::NodeId::ROOT,
        outside: Default::default(),
    });
}

#[test]
fn role_folders_stay_out_of_the_main_folder() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "mine")?;
    let mut a = Client::new(&server, "a")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["readme.txt = r"]);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Documents", "cv.txt", "readme.txt"]);
    Ok(())
}

#[test]
fn a_plain_documents_folder_beside_the_role_folder_keeps_its_name() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("Documents/mine.txt", "plain")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    // A role folder holds no name for the main folder: two at the top, told apart by id.
    let plain = ["Documents/", "Documents/mine.txt = plain"];
    assert_eq!(a.tree_text(""), plain);
    assert_eq!(b.tree_text(""), plain);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Documents", "Documents", "cv.txt", "mine.txt"]);
    Ok(())
}

#[test]
fn a_plain_documents_folder_keeps_its_name_when_the_role_folder_comes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Documents/mine.txt", "plain")?;
    a.sync()?;
    b.sync()?;
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    a.write("other.txt", "o")?;
    a.sync()?;
    b.sync()?;
    let mine = ["Documents/", "Documents/mine.txt = plain", "other.txt = o"];
    assert_eq!(a.tree_text(""), mine);
    assert_eq!(b.tree_text(""), mine);
    Ok(())
}

#[test]
fn a_role_folder_no_longer_left_out_comes_with_everything_in_it() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["readme.txt = r"]);

    restart_with(&mut a, unscoped)?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        ["Documents/", "Documents/cv.txt = role", "readme.txt = r"]
    );
    Ok(())
}

/// Recently deleted is the account's: the main folder lists the role folder's removals too,
/// each with the folder it comes back to (none here for a role folder this device doesn't
/// sync), and emptying what was shown takes both.
#[test]
fn recently_deleted_names_the_folder_each_item_comes_back_to() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("mine.txt", "m")?;
    a.sync()?;
    // Both removed elsewhere: one in the role folder, one at the top.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let files: Vec<_> = srv
            .live()
            .filter(|h| h.kind == hbsync_types::NodeKind::File)
            .cloned()
            .collect();
        assert_eq!(files.len(), 2);
        for h in files {
            if let hbsync_remote::RemoteBody::Live { node, content, .. } = h.body {
                let meta = hbsync_remote::contract::removed(&NodeMeta {
                    node,
                    parent: h.parent,
                    kind: h.kind,
                    content,
                    removed: false,
                });
                srv.update(h.file_id, h.version, &meta, None)?;
            }
        }
    }
    a.sync()?;
    let mut listed: Vec<(String, Option<String>)> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|d| (d.path, d.folder))
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        [
            ("cv.txt".into(), None),
            (
                "mine.txt".into(),
                Some(a.root.to_string_lossy().into_owned())
            )
        ]
    );
    let listed = a.listed()?;
    a.engine.drive().empty_listed(&listed)?;
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(srv.removed().count(), 0);
    Ok(())
}

#[test]
fn a_role_folder_moved_into_a_folder_keeps_it_alive() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("Archive/old.txt", "o")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    // A client that breaks the schema moves the role folder into Archive.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let archive = srv
            .live()
            .find_map(|h| match &h.body {
                hbsync_remote::RemoteBody::Live { node, content, .. }
                    if content.name == "Archive" =>
                {
                    Some(*node)
                }
                _ => None,
            })
            .ok_or("Archive")?;
        let docs = srv
            .live()
            .find(|h| {
                matches!(&h.body, hbsync_remote::RemoteBody::Live { node, .. }
                if *node == hbsync_types::Role::Documents.node())
            })
            .cloned()
            .ok_or("the role folder")?;
        let meta = NodeMeta {
            parent: archive,
            ..hbsync_remote::contract::folder("Documents", archive)
        };
        let meta = NodeMeta {
            node: hbsync_types::Role::Documents.node(),
            ..meta
        };
        srv.update(docs.file_id, docs.version, &meta, None)?;
    }
    a.sync()?;
    fs::remove_dir_all(a.root.join("Archive"))?;
    a.sync()?;
    // Archive holds what this device doesn't see: it stays on the drive, and comes back here.
    assert_eq!(a.tree_text(""), ["Archive/", "readme.txt = r"]);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Archive", "Documents", "cv.txt", "readme.txt"]);
    Ok(())
}

/// A full drive holds what goes up, without a note per file; the rest of the sync goes on, and
/// once there is room the held file goes up.
#[test]
fn a_full_drive_holds_uploads_and_lets_the_rest_through() -> R {
    let server = server();
    let live_names = || -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        Ok(srv
            .live()
            .filter_map(|h| match &h.body {
                hbsync_remote::RemoteBody::Live { content, .. } => Some(content.name.clone()),
                _ => None,
            })
            .collect())
    };
    let mut a = Client::new(&server, "a")?;
    a.write("old.txt", vec![b'o'; 600])?;
    a.sync()?;
    server.lock().map_err(|_| "poisoned")?.set_quota(Some(1000));
    a.write("new.txt", vec![b'n'; 600])?;
    std::fs::remove_file(a.root.join("old.txt"))?;
    let report = a.sync()?;
    assert!(report.full, "{report:?}");
    // Held, not told: no note per file.
    assert!(
        report.failed.is_empty()
            && report
                .skipped
                .iter()
                .all(|(_, why)| matches!(why, hbsync_engine::exec::Skip::Waiting(_))),
        "{report:?}"
    );
    assert!(report.pending > 0);
    assert_eq!(
        live_names()?,
        Vec::<String>::new(),
        "the delete went through"
    );

    // Held a while, whatever the drive says now, in a new sync too: no upload just to be
    // refused again.
    server.lock().map_err(|_| "poisoned")?.set_quota(None);
    let report = a.sync()?;
    assert!(report.full, "{report:?}");
    assert_eq!(live_names()?, Vec::<String>::new());
    // Until the user asks for a sync (they may have made room).
    a.engine.drive().clear_full();
    let report = a.sync()?;
    assert!(!report.full, "{report:?}");
    assert_eq!(live_names()?, ["new.txt"]);
    Ok(())
}

/// A download put in place whose commit a crash lost is the node again on the next start,
/// where it was put, even after the drive moved the node meanwhile: no second copy.
#[test]
fn a_download_placed_but_not_recorded_is_found_again() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("x.txt", "the bytes")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    // b crashes just before the commit that records x.txt in place.
    let placed = b.root.join("x.txt");
    let crashed = common::crash(&mut b, common::Kept::All, move |at, _| {
        at == Commit::Before && placed.exists()
    })?;
    assert!(crashed && b.read_text("x.txt")? == "the bytes");
    // Meanwhile a renames it.
    fs::rename(a.root.join("x.txt"), a.root.join("y.txt"))?;
    a.sync()?;
    b.restart()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = k", "y.txt = the bytes"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A folder deleted on the drive and a new one made under its name: here the old one is parked
/// for the new one, then trashed, in one sync. A power cut losing the parking's commit must not
/// leave the DB taking the new folder for the old one.
#[test]
fn a_power_cut_after_a_folder_was_replaced_keeps_the_new_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("F/old.txt", "old")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("F"))?;
    a.sync()?;
    a.write("F/new.txt", "new")?;
    a.sync()?;
    // b's power goes just before the first commit once the new folder is there.
    let (gone, f) = (std::sync::atomic::AtomicBool::new(false), b.root.join("F"));
    let crashed = common::crash(&mut b, common::Kept::Durable(None), move |at, _| {
        if !f.exists() {
            gone.store(true, Ordering::SeqCst);
        }
        at == Commit::Before && gone.load(Ordering::SeqCst) && f.exists()
    })?;
    assert!(crashed);
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["F/", "F/new.txt = new", "keep.txt = k"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// An item detached here (edited here, moved where this device ignores) goes out under its
/// new id once: a power cut after its upload doesn't make the next sync pick another id.
#[test]
fn a_power_cut_after_a_detach_uploads_the_item_once() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("keep.txt", "k")?;
    a.write("a.txt", "v1")?;
    a.sync()?;
    b.sync()?;
    fs::create_dir_all(a.root.join("build"))?;
    fs::rename(a.root.join("a.txt"), a.root.join("build/a.txt"))?;
    a.sync()?;
    b.write("a.txt", "b's edit")?;
    // b's power goes right after the commit that follows its upload of the detached item.
    let drive = server.clone();
    let uploaded = move || drive.lock().is_ok_and(|s| s.live().count() == 4);
    let crashed = common::crash(&mut b, common::Kept::Durable(None), move |at, _| {
        at == Commit::After && uploaded()
    })?;
    assert!(crashed);
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["a.txt = b's edit", "keep.txt = k"]);
    assert_eq!(
        a.tree_text(""),
        [
            "a.txt = b's edit",
            "build/",
            "build/a.txt = v1",
            "keep.txt = k"
        ]
    );
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 4);
    Ok(())
}

/// A folder deleted here and renamed elsewhere comes back here (the change wins): a power cut
/// right after it is made doesn't make it a second folder.
#[test]
fn a_power_cut_after_a_folder_came_back_keeps_one() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    fs::create_dir_all(a.root.join("F"))?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    fs::remove_dir(b.root.join("F"))?;
    fs::rename(a.root.join("F"), a.root.join("G"))?;
    a.sync()?;
    // Just before the commit that records it.
    let g = b.root.join("G");
    let crashed = common::crash(&mut b, common::Kept::Durable(None), move |at, _| {
        at == Commit::Before && g.exists()
    })?;
    assert!(crashed);
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["G/", "keep.txt = k"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// The feed comes slim first (odin-core #1967): this device's own writes coming back are
/// versions it knows, so its pages are never read whole; its removals reach Recently deleted
/// from the slim rows, with the server's times. Another device, to which they are new, reads
/// them whole.
#[test]
fn own_writes_come_back_slim() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    for i in 0..20 {
        a.write(&format!("Photos/{i}.jpg"), format!("{i}"))?;
    }
    a.write("Photos/2024/y.jpg", "y")?;
    a.sync()?;
    // The first read has no cursor: all of it is new, so it is read whole.
    let first = a.remote.faults().full_pages;
    assert_eq!(first, 1, "the first read, of an empty drive");
    fs::rename(a.root.join("Photos/0.jpg"), a.root.join("Photos/zero.jpg"))?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    a.sync()?;
    {
        let faults = a.remote.faults();
        assert_eq!(faults.full_pages, first, "its own writes, slim");
        assert!(faults.ref_pages > 0);
    }
    let listed = a.recently_deleted(100)?;
    assert_eq!(listed.len(), 23, "{listed:?}");
    let photos = listed
        .iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    assert!(listed.iter().all(|i| i.root == photos.node), "{listed:?}");
    let server_times: Vec<i64> = server
        .lock()
        .map_err(|_| "poisoned")?
        .removed()
        .filter_map(|h| h.removed_at)
        .collect();
    assert!(
        listed
            .iter()
            .all(|i| i.removed_at_ms.is_some_and(|t| server_times.contains(&t))),
        "the server's times: {listed:?}"
    );

    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert!(b.remote.faults().full_pages > 0, "new to b: read whole");
    assert_eq!(b.recently_deleted(100)?.len(), 23);
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    Ok(())
}

/// The feed pages `c` read so far, slim and whole.
fn feed_reads(c: &Client) -> usize {
    let f = c.remote.faults();
    f.ref_pages + f.full_pages
}

/// A folder made by another client, as `b` writes it.
fn folder_meta(name: &str) -> NodeMeta {
    hbsync_remote::contract::folder(name, NodeId::ROOT)
}

/// With the drive's changes announced and this device's own writes left out of them
/// (`excludeOwnWrites`, odin-core #1971), its syncs read no feed at all: each write is
/// recorded from its answer, a version's server time and a removal's alike, and nothing of its
/// own comes back to it. Another computer's change, announced and unknown, is what reads it.
#[test]
fn own_writes_are_recorded_from_their_answers() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    a.sync()?; // start: reads the feed, as at every start
    a.announced();
    let heard = a.remote.subscribe(true);
    let before = feed_reads(&a);
    a.write("doc.txt", "x")?;
    a.write("dir/in.txt", "y")?;
    a.sync_unasked()?;
    fs::rename(a.root.join("doc.txt"), a.root.join("renamed.txt"))?;
    a.sync_unasked()?;
    assert_eq!(feed_reads(&a), before, "a's own writes read the feed");

    // Each version's time is the server's.
    let mut db = StateDb::open_account(&a.db_path())?;
    let headers: Vec<RemoteHeader> = {
        let s = server.lock().map_err(|_| "poisoned")?;
        s.live().cloned().collect()
    };
    assert_eq!(headers.len(), 3, "dir, in.txt, renamed.txt: {headers:?}");
    for h in &headers {
        let row = db
            .read(|s| s.mirror_by_file(h.file_id))?
            .ok_or("not in the mirror")?;
        assert_eq!(row.version, h.version);
        assert_eq!(row.changed_at, h.changed_at, "{h:?}");
    }

    // A removal's time too, without a feed read.
    fs::remove_dir_all(a.root.join("dir"))?;
    fs::remove_file(a.root.join("renamed.txt"))?;
    a.sync_unasked()?;
    assert_eq!(feed_reads(&a), before, "a's own removals read the feed");
    let on_server: HashMap<RemoteFileId, Option<i64>> = {
        let s = server.lock().map_err(|_| "poisoned")?;
        s.removed().map(|h| (h.file_id, h.removed_at)).collect()
    };
    assert_eq!(on_server.len(), 3);
    let deleted = a.recently_deleted(10)?;
    assert_eq!(deleted.len(), 3, "{deleted:?}");
    for d in deleted {
        let row = db
            .read(|s| s.removed_by_node(d.node))?
            .ok_or("not removed")?;
        assert!(d.removed_at_ms.is_some(), "{} open", d.path);
        assert_eq!(d.removed_at_ms, on_server[&row.row.file_id], "{}", d.path);
    }
    assert_eq!(heard.try_iter().count(), 0, "a heard its own writes");

    // Another computer's write, announced: news, so the feed is read for it.
    let b = FakeRemote::new(server.clone());
    b.create(&folder_meta("from b"), None)?;
    let announced: Vec<_> = heard.try_iter().collect();
    assert_eq!(announced.len(), 1);
    assert!(!a.engine.drive().announced(&announced)?, "news: read");
    a.engine.drive().pull(None)?;
    assert!(feed_reads(&a) > before, "the news is read");
    a.sync_unasked()?;
    assert!(a.root.join("from b").is_dir());
    Ok(())
}

/// A write the drive refuses for its state (another device changed the file, unannounced to
/// this one) leaves the drive behind: the sync reads the feed at once, though the
/// notifications run and nothing else would read, and the next try settles it.
#[test]
fn a_write_refused_for_the_drives_state_reads_the_feed() -> R {
    let server = server();
    let (mut a, mut b) = (Client::new(&server, "a")?, Client::new(&server, "b")?);
    a.write("doc.txt", "a1")?;
    a.sync()?;
    b.sync()?;
    a.announced();
    // b's edit, never announced to a.
    b.write("doc.txt", "b2")?;
    b.sync()?;
    a.write("doc.txt", "a2")?;
    // The refused write reads the feed; the retry of the unfinished work (the runner's, a few
    // seconds on) settles it, with no read of its own.
    let refused = a.sync_unasked()?;
    assert!(refused.unfinished && refused.feed.live > 0, "{refused:?}");
    let reads = feed_reads(&a);
    a.sync_unasked()?;
    assert_eq!(feed_reads(&a), reads, "the retry reads nothing");
    let texts: Vec<String> = a
        .entries("")
        .into_iter()
        .map(|(_, t)| String::from_utf8_lossy(&t).into_owned())
        .collect();
    assert!(
        texts.contains(&"b2".to_owned()) && texts.contains(&"a2".to_owned()),
        "{texts:?}"
    );
    Ok(())
}

/// A write whose answer didn't come (it landed: a timeout after the server wrote) isn't sent
/// again blind, a large upload twice: the drive is behind, the feed is read first, and the
/// file settles as uploaded.
#[test]
fn a_write_whose_answer_was_lost_isnt_sent_again() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.sync()?;
    a.announced();
    a.remote.faults().lose_answer_named = Some("big.bin".into());
    a.write("big.bin", vec![7u8; 1 << 20])?;
    a.sync_unasked()?;
    a.remote.faults().lose_answer_named = None;
    a.sync_unasked()?;
    assert_eq!(a.remote.faults().uploads, 1, "sent once");
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["big.bin"]
    );
    assert_eq!(a.tree_text(""), ["big.bin = <1048576 bytes>"]);
    assert!(a.sync_unasked()?.ops_done == 0, "settled");
    Ok(())
}

/// A file renamed, moved or tagged here and edited on the drive: the plan writes the rename,
/// move or tags to the drive, then downloads the edit. The download acts on the version that
/// write made (the bytes the same), so it is fetched once, in the same round: acting only on
/// the version the plan saw, it went stale behind the run's own write and came again a round
/// later.
#[test]
fn a_download_after_the_runs_own_write_is_fetched_once() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    type Change = fn(&Client) -> std::io::Result<()>;
    let rename: Change = |c| fs::rename(c.root.join("d/f.txt"), c.root.join("d/g.txt"));
    let mv: Change = |c| fs::rename(c.root.join("d/f.txt"), c.root.join("e/f.txt"));
    let tag: Change = |c| {
        let at = ["d/f.txt", "d/g.txt"].map(|p| c.root.join(p));
        let file = at
            .iter()
            .find(|p| p.exists())
            .ok_or(std::io::ErrorKind::NotFound)?;
        tags::write(file, &Tags::new([Tag::new("Red", 6)])).map(|_| ())
    };
    const EDITED: &str = "edited on the drive";
    let edit: Change = |c| c.write("d/f.txt", EDITED);
    let blue: Change =
        |c| tags::write(&c.root.join("d/f.txt"), &Tags::new([Tag::new("Blue", 4)])).map(|_| ());
    // (case, changed here, changed on the drive)
    let cases: [(&str, &[Change], &[Change]); 6] = [
        ("rename / edit", &[rename], &[edit]),
        ("move / edit", &[mv], &[edit]),
        ("tag / edit", &[tag], &[edit]),
        ("rename / edit+tag", &[rename], &[edit, blue]),
        ("tag / edit+tag", &[tag], &[edit, blue]),
        ("rename+tag / edit", &[rename, tag], &[edit]),
    ];
    for (case, here, there) in cases {
        let server = server();
        let mut a = Client::new(&server, "a")?;
        let mut b = Client::new(&server, "b")?;
        a.write("d/f.txt", "base")?;
        a.write("e/other.txt", "other")?;
        let tagless =
            !tags::CAN_TAG || tags::write(&a.root.join("d/f.txt"), &Tags::default())?.is_none();
        if tagless && case.contains("tag") {
            eprintln!("{case}: no tags here; skipped");
            continue;
        }
        a.sync()?;
        b.sync()?;
        there.iter().try_for_each(|change| change(&b))?;
        b.sync()?;
        here.iter().try_for_each(|change| change(&a))?;
        a.remote.faults().served = 0;
        let report = a.sync()?;
        let stale: Vec<_> = report
            .skipped
            .iter()
            .filter(|(_, why)| matches!(why, hbsync_engine::exec::Skip::Stale(_)))
            .collect();
        assert!(stale.is_empty(), "{case}: {stale:?}");
        assert!(report.failed.is_empty(), "{case}: {:?}", report.failed);
        let served = a.remote.faults().served;
        assert_eq!(served, EDITED.len() as u64, "{case}: downloaded once");
        b.sync()?;
        assert_eq!(a.tree_text(""), b.tree_text(""), "{case}");
    }
    Ok(())
}

/// A folder's delete, its removals answered a request at a time and recorded a request at a
/// time: a crash or a power cut at any of its commits loses nothing, and the next sync finishes
/// the delete. Every node of the folder ends removed on the drive (restorable, as one item),
/// none live, and the other device has the folder gone.
#[test]
fn a_crash_at_any_commit_of_a_folders_delete_finishes_it() -> R {
    for at in 0..8 {
        for before in [true, false] {
            for kept in [common::Kept::All, common::Kept::Durable(None)] {
                let server = server();
                let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
                a.write("keep.txt", "k")?;
                for i in 0..5 {
                    a.write(&format!("F/f{i}.txt"), format!("{i}"))?;
                }
                a.write("F/G/g.txt", "g")?;
                a.sync()?;
                let mut b = Client::new(&server, "b")?;
                b.sync()?;
                fs::remove_dir_all(a.root.join("F"))?;
                let case = format!("at {at}, before {before}, {kept:?}");
                common::crash(&mut a, kept, move |now, n| {
                    n == at && (now == Commit::Before) == before
                })?;
                a.sync()?;
                b.sync()?;
                let s = server.lock().map_err(|_| "poisoned")?;
                let live: Vec<String> = s.live_names();
                assert_eq!(live, ["keep.txt"], "{case}");
                assert_eq!(s.removed().count(), 8, "{case}");
                drop(s);
                assert_eq!(b.tree_text(""), ["keep.txt = k"], "{case}");
                // One item, with all of the folder in it.
                let deleted = a.recently_deleted(10)?;
                let items = deleted.iter().filter(|d| d.root == d.node).count();
                assert_eq!((items, deleted.len()), (1, 8), "{case}: {deleted:?}");
            }
        }
    }
    Ok(())
}

/// A folder's delete that its device never finished (the folder removed on the drive, what is
/// in it still live, and the device never back) is finished by another device that syncs it:
/// what the removal took along goes to Recently deleted with the folder, and off its disk.
#[test]
fn another_device_finishes_a_folders_delete_left_half_done() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    for i in 0..5 {
        a.write(&format!("F/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    let mut b = Client::with_config(&server, "b", |c| c.allow_mass_delete = true)?;
    b.sync()?;
    // a removes the folder alone, and is never seen again.
    let f = {
        let s = server.lock().map_err(|_| "poisoned")?;
        let h = s
            .live()
            .find(|h| h.state().is_some_and(|st| st.name == "F"))
            .ok_or("no F")?
            .clone();
        Target {
            at: RemoteRef {
                file_id: h.file_id,
                version: h.version,
            },
            token: Some(hbsync_remote::fake::token_of(h.file_id)),
            changed_at: None,
        }
    };
    a.remote.set_removed(&[f], true)?.into_answers()?;
    drop(a);
    b.sync()?;
    let s = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(s.live_names(), ["keep.txt"]);
    assert_eq!(s.removed().count(), 6);
    drop(s);
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    let deleted = b.recently_deleted(10)?;
    let items = deleted.iter().filter(|d| d.root == d.node).count();
    assert_eq!((items, deleted.len()), (1, 6), "{deleted:?}");
    Ok(())
}

/// A file removed here that another device deleted for good meanwhile (a tombstone, not in the
/// picture yet): the removal's answer says so, and its header isn't read to make sure. It is
/// gone here too, out of Recently deleted.
#[test]
fn a_removal_answered_as_a_tombstone_reads_no_header() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    a.announced();
    // Deleted for good elsewhere, leaving a tombstone a hasn't heard of.
    {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        let h = s
            .live()
            .find(|h| h.state().is_some_and(|st| st.name == "x.txt"))
            .ok_or("no x")?
            .clone();
        s.delete(h.file_id, h.version)?;
    }
    fs::remove_file(a.root.join("x.txt"))?;
    a.sync_unasked()?;
    assert_eq!(a.remote.faults().headers, 0);
    assert!(a.recently_deleted(10)?.is_empty());
    assert_eq!(a.tree_text(""), ["keep.txt = k"]);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["keep.txt"]
    );
    Ok(())
}

/// A delete taken back: the files the other device set aside in its trash when the delete
/// came are taken from there, not over the network, each only once its bytes read as the
/// drive's. A trash file with other bytes (changed there) stays, and its file is downloaded.
#[test]
fn a_restore_takes_the_bytes_from_the_trash() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
    let (x, y, z) = ("x".repeat(1000), "y".repeat(2000), "z".repeat(3000));
    a.write("run/x.txt", &x)?;
    a.write("run/sub/y.txt", &y)?;
    a.write("run/z.txt", &z)?;
    a.sync()?;
    let mut b = Client::with_config(&server, "b", |c| c.allow_mass_delete = true)?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("run"))?;
    a.sync()?;
    b.sync()?;
    assert!(b.read_text("run/x.txt").is_err(), "deleted on b");
    let entry = fs::read_dir(b.root.join(".hbsync/trash"))?
        .next()
        .ok_or("no trash entry")??
        .path();
    // Changed in the trash, its size kept: not the drive's bytes.
    let changed = "Z".repeat(3000);
    fs::write(entry.join("run/z.txt"), &changed)?;
    let run = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "run")
        .ok_or("run not listed")?;
    assert_eq!(a.engine.drive().restore(&[run.node])?, 5);
    let served = b.remote.faults().served;
    b.sync()?;
    assert_eq!(
        b.remote.faults().served - served,
        3000,
        "only z.txt over the network"
    );
    assert_eq!(b.read_text("run/x.txt")?, x);
    assert_eq!(b.read_text("run/sub/y.txt")?, y);
    assert_eq!(b.read_text("run/z.txt")?, z);
    assert!(!entry.join("run/x.txt").exists(), "taken from the trash");
    assert_eq!(fs::read_to_string(entry.join("run/z.txt"))?, changed);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A restore taken from the trash, with a crash or a power cut at any of the sync's commits:
/// the next sync finishes it, every file whole and in place, from the trash or the drive.
#[test]
fn a_crash_while_taking_from_the_trash_loses_nothing() -> R {
    for at in 0..8 {
        for before in [true, false] {
            for kept in [common::Kept::All, common::Kept::Durable(None)] {
                let case = format!("at {at}, before {before}, {kept:?}");
                let server = server();
                let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
                for i in 0..4 {
                    a.write(&format!("F/f{i}.txt"), format!("{i}").repeat(100))?;
                }
                a.write("F/G/g.txt", "g".repeat(100))?;
                a.sync()?;
                let mut b = Client::with_config(&server, "b", |c| c.allow_mass_delete = true)?;
                b.sync()?;
                fs::remove_dir_all(a.root.join("F"))?;
                a.sync()?;
                b.sync()?;
                let f = a
                    .recently_deleted(10)?
                    .into_iter()
                    .find(|i| i.path == "F")
                    .ok_or("F not listed")?;
                a.engine.drive().restore(&[f.node])?;
                common::crash(&mut b, kept, move |now, n| {
                    n == at && (now == Commit::Before) == before
                })?;
                b.sync()?;
                for i in 0..4 {
                    assert_eq!(
                        b.read_text(&format!("F/f{i}.txt"))?,
                        format!("{i}").repeat(100),
                        "{case}"
                    );
                }
                assert_eq!(b.read_text("F/G/g.txt")?, "g".repeat(100), "{case}");
                a.sync()?;
                assert_eq!(a.tree_text(""), b.tree_text(""), "{case}");
            }
        }
    }
    Ok(())
}
