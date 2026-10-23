//! A remote that fails, refuses, stops or slows a run: retries, stops, order of writes, progress.

use super::*;

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
    assert_eq!(err.class(), hbsync_remote::ErrorClass::Denied, "{err}");
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
    assert_eq!(err.class(), hbsync_remote::ErrorClass::Denied, "{err}");
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
    fn holding(
        &self,
        folders: &[hbsync_types::NodeId],
        holds: hbsync_remote::Holds<'_>,
    ) -> RemoteResult<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<hbsync_types::Written> {
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
    ) -> RemoteResult<hbsync_types::Written> {
        self.inner.update(target, meta, payload)
    }
    fn delete(&self, files: &[hbsync_types::Doomed]) -> RemoteResult<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::Doomed],
    ) -> RemoteResult<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Written>> {
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
    fn holding(
        &self,
        folders: &[hbsync_types::NodeId],
        holds: hbsync_remote::Holds<'_>,
    ) -> RemoteResult<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<hbsync_types::Written> {
        self.write(|| self.unless_down(|| self.inner.create(node, payload)))
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<hbsync_types::Written> {
        self.write(|| self.unless_down(|| self.inner.update(target, meta, payload)))
    }
    fn delete(&self, files: &[hbsync_types::Doomed]) -> RemoteResult<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::Doomed],
    ) -> RemoteResult<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Written>> {
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
