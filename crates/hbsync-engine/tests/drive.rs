//! The account's drive, shared by its folders' engines: one feed for all, wakes, the purge.

use std::sync::{Arc, Barrier};
use std::time::Duration;

use hbsync_engine::Drive;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{NodeMeta, Remote};
use hbsync_state::StateDb;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, Role};

type R = Result<(), Box<dyn std::error::Error>>;

fn folder(name: &str) -> NodeMeta {
    NodeMeta {
        node: NodeId::new(),
        parent: NodeId::ROOT,
        kind: NodeKind::Folder,
        content: NodeContent::folder(name),
        removed: false,
    }
}

/// A drive on a fresh account DB in `dir`, and the fake server behind it.
fn drive(
    dir: &std::path::Path,
) -> Result<(Arc<Drive>, Arc<FakeRemote>), Box<dyn std::error::Error>> {
    let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
    let account = StateDb::open_account(&dir.join("state.db"))?;
    Ok((Drive::new(account, remote.clone()), remote))
}

/// Engines asking at once share a pull: one at a time, and one that started after an ask
/// answers it; what the drive had when each asked is in the mirror when it returns.
#[test]
fn pulls_asked_at_once_are_merged() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let made = remote.create(&folder("Docs"), None)?;
    // Each feed read a little slow, so the asks overlap.
    let before = {
        let mut faults = remote.faults();
        faults.latency = Duration::from_millis(100);
        faults.requests
    };
    let start = Barrier::new(8);
    std::thread::scope(|scope| {
        let pulls: Vec<_> = (0..8)
            .map(|engine| {
                let (drive, start) = (&drive, &start);
                scope.spawn(move || {
                    start.wait();
                    drive.pull(Some(engine)).map(drop)
                })
            })
            .collect();
        pulls
            .into_iter()
            .try_for_each(|p| p.join().expect("no panic"))
    })?;
    let reads = {
        let mut faults = remote.faults();
        faults.latency = Duration::ZERO;
        faults.requests - before
    };
    assert!((1..=2).contains(&reads), "{reads} feed reads for 8 asks");
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    assert!(db.read(|s| s.mirror_by_file(made.at.file_id))?.is_some());
    // An ask after a pull finished pulls again: the drive may have changed since.
    let later = remote.create(&folder("Later"), None)?;
    drive.pull(None)?;
    assert!(db.read(|s| s.mirror_by_file(later.at.file_id))?.is_some());
    Ok(())
}

/// Changes the notifications announce: versions the store has (this device's own writes
/// coming back) are taken without reading the feed, an own removal getting the server's time;
/// a version it doesn't know needs the feed; a file deleted for good (a hard delete leaves no
/// row in the feed) is gone from the mirror at once.
#[test]
fn announced_changes_are_taken_without_reading_what_is_known() -> R {
    use hbsync_remote::ChangeRef;
    use hbsync_remote::contract::removed;
    use hbsync_state::RemovedRow;

    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let docs = folder("Docs");
    let made = remote.create(&docs, None)?;
    drive.pull(None)?;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    let known = ChangeRef::of(&remote.header(made.at.file_id)?);
    assert!(
        drive.announced(&[known])?,
        "an own write coming back: no read"
    );

    // This device removed it: its row in Recently deleted, the time open until the server's.
    let gone_at = remote.update(&made, &removed(&docs), None)?;
    db.write(|s| {
        let mut row = s.mirror_by_file(made.at.file_id)?.expect("in the mirror");
        s.delete_mirror(made.at.file_id)?;
        row.version = gone_at.at.version;
        s.put_removed(&RemovedRow {
            row,
            removed_at: None,
        })
    })?;
    let echo = ChangeRef::of(&remote.header(made.at.file_id)?);
    assert!(
        drive.announced(&[echo])?,
        "an own removal coming back: no read"
    );
    let row = db
        .read(|s| s.removed_by_node(docs.node))?
        .expect("in Recently deleted");
    assert_eq!(
        row.removed_at, echo.removed_at,
        "the server's time, confirmed"
    );

    let other = remote.create(&folder("Other"), None)?;
    let unknown = ChangeRef::of(&remote.header(other.at.file_id)?);
    assert!(
        !drive.announced(&[echo, unknown])?,
        "a version not known: read the feed"
    );

    // Deleted for good without a row in the feed (a hard delete): gone at once.
    let hard = echo.tombstone();
    assert!(drive.announced(&[hard])?);
    assert!(db.read(|s| s.removed_by_node(docs.node))?.is_none());
    assert!(db.read(|s| s.mirror_by_file(made.at.file_id))?.is_none());
    Ok(())
}

/// Announcements that arrive late change nothing they shouldn't: a version the mirror has moved
/// on from, or a removal undone since, isn't known (the feed is read, the rows stay), and a
/// file deleted for good touches only its own file id, not the node made again since.
#[test]
fn late_announcements_dont_move_the_mirror_back() -> R {
    use hbsync_remote::ChangeRef;
    use hbsync_remote::contract::removed;

    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    let docs = folder("Docs");
    let v1 = remote.create(&docs, None)?;
    let file = v1.at.file_id;
    let stale = ChangeRef::of(&remote.header(file)?);
    let v2 = remote.update(&v1, &docs, None)?;
    drive.pull(None)?;
    let version = |db: &mut StateDb| -> Result<_, Box<dyn std::error::Error>> {
        Ok(db.read(|s| s.mirror_by_file(file))?.map(|r| r.version))
    };
    assert!(!drive.announced(&[stale])?, "v1 after v2: not known");
    assert_eq!(
        version(&mut db)?,
        Some(v2.at.version),
        "the mirror stays at v2"
    );

    // Removed, then restored: the removal's announcement comes late.
    let gone = remote.update(&v2, &removed(&docs), None)?;
    let late_removal = ChangeRef::of(&remote.header(file)?);
    let back = remote.update(&gone, &docs, None)?;
    drive.pull(None)?;
    assert!(
        !drive.announced(&[late_removal])?,
        "a removal undone since: not known"
    );
    assert_eq!(
        version(&mut db)?,
        Some(back.at.version),
        "live, as restored"
    );
    assert!(db.read(|s| s.removed_by_node(docs.node))?.is_none());

    // Deleted for good, the node made again (a new file id), then the delete is announced.
    let late_delete = ChangeRef::of(&remote.header(file)?).tombstone();
    remote.delete(file, back.at.version)?;
    let again = remote.create(&docs, None)?;
    assert_ne!(again.at.file_id, file, "a new file id");
    drive.pull(None)?;
    assert!(drive.announced(&[late_delete])?);
    let row = db
        .read(|s| s.mirror_by_file(again.at.file_id))?
        .expect("the node made again stays");
    assert_eq!(row.version, again.at.version);
    Ok(())
}

/// A remote whose next whole feed page (or with `hold_header`, header read), once fetched,
/// waits for the test before it is handed back: a page on its way while the drive changes.
struct HeldPage {
    inner: Arc<FakeRemote>,
    hold: std::sync::atomic::AtomicBool,
    hold_header: std::sync::atomic::AtomicBool,
    fetched: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
    go: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Remote for HeldPage {
    fn changes(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::ChangesPage> {
        let page = self.inner.changes(cursor, limit)?;
        if self.hold.swap(false, std::sync::atomic::Ordering::SeqCst) {
            let _ = self.fetched.lock().map(|tx| tx.send(()));
            let _ = self.go.lock().map(|rx| rx.recv());
        }
        Ok(page)
    }
    fn change_refs(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn header(
        &self,
        file_id: hbsync_types::RemoteFileId,
    ) -> hbsync_remote::Result<hbsync_remote::RemoteHeader> {
        let header = self.inner.header(file_id);
        if self
            .hold_header
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let _ = self.fetched.lock().map(|tx| tx.send(()));
            let _ = self.go.lock().map(|rx| rx.recv());
        }
        header
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn holding_live(
        &self,
        folders: &[hbsync_types::NodeId],
    ) -> hbsync_remote::Result<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding_live(folders)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::RemoteRef],
    ) -> hbsync_remote::Result<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Target>> {
        self.inner.set_removed(files, removed)
    }
    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.create(node, payload)
    }
    fn update(
        &self,
        target: &hbsync_types::Target,
        meta: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.update(target, meta, payload)
    }
    fn delete(
        &self,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> hbsync_remote::Result<()> {
        self.inner.delete(file_id, expected)
    }
    fn read(
        &self,
        file_id: hbsync_types::RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> hbsync_remote::Result<u64> {
        self.inner.read(file_id, range, sink)
    }
}

/// A hard delete announced while a pull's page is on its way waits for that page: the page
/// (fetched before the delete) must not put the file back after the delete was taken.
#[test]
fn a_page_fetched_before_a_hard_delete_doesnt_bring_it_back() -> R {
    use hbsync_remote::ChangeRef;
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir()?;
    let inner = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
    let (fetched_tx, fetched) = std::sync::mpsc::channel();
    let (go, go_rx) = std::sync::mpsc::channel();
    let remote = Arc::new(HeldPage {
        inner: Arc::clone(&inner),
        hold: AtomicBool::new(false),
        hold_header: AtomicBool::new(false),
        fetched: std::sync::Mutex::new(fetched_tx),
        go: std::sync::Mutex::new(go_rx),
    });
    let drive = Drive::new(
        StateDb::open_account(&dir.path().join("state.db"))?,
        remote.clone(),
    );
    let docs = folder("Docs");
    let v1 = inner.create(&docs, None)?;
    drive.pull(None)?;
    // A change the next pull reads whole; its page waits once fetched.
    let v2 = inner.update(&v1, &docs, None)?;
    remote.hold.store(true, Ordering::SeqCst);
    let pulling = {
        let drive = Arc::clone(&drive);
        std::thread::spawn(move || drive.pull(None).map(drop).map_err(|e| e.to_string()))
    };
    fetched.recv_timeout(Duration::from_secs(10))?;
    // Deleted for good meanwhile: no feed row; the notification says so.
    let gone = ChangeRef::of(&inner.header(v2.at.file_id)?).tombstone();
    inner.server().hard_delete(v2.at.file_id, v2.at.version)?;
    let taking = {
        let drive = Arc::clone(&drive);
        std::thread::spawn(move || drive.announced(&[gone]).map_err(|e| e.to_string()))
    };
    // Give the delete every chance to go first (it must wait for the page).
    let until = std::time::Instant::now() + Duration::from_millis(500);
    while !taking.is_finished() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    go.send(())?;
    pulling.join().map_err(|_| "the pull panicked")??;
    taking.join().map_err(|_| "the take panicked")??;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    assert!(
        db.read(|s| s.mirror_by_file(v2.at.file_id))?.is_none(),
        "the page fetched before the delete brought it back"
    );
    Ok(())
}

/// A re-read asks the server about a file the feed didn't show, and writes the answer, in the
/// same pull: a pull meanwhile that brings the file's removal waits for it, so the header read
/// before the removal can't write over it (the delete lost, the file brought back).
#[test]
fn a_reread_answer_doesnt_write_over_a_later_removal() -> R {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir()?;
    let inner = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
    let (fetched_tx, fetched) = std::sync::mpsc::channel();
    let (go, go_rx) = std::sync::mpsc::channel();
    let remote = Arc::new(HeldPage {
        inner: Arc::clone(&inner),
        hold: AtomicBool::new(false),
        hold_header: AtomicBool::new(false),
        fetched: std::sync::Mutex::new(fetched_tx),
        go: std::sync::Mutex::new(go_rx),
    });
    let drive = Drive::new(
        StateDb::open_account(&dir.path().join("state.db"))?,
        remote.clone(),
    );
    let meta = folder("Docs");
    let docs = inner.create(&meta, None)?;
    drive.pull(None)?;
    // Away a month: the next pull reads the whole feed again, which leaves Docs out; it asks
    // about it, and its answer (Docs live) waits.
    inner.server().unlist(docs.at.file_id);
    inner
        .server()
        .advance(hbsync_engine::feed::TRACE_MS + 24 * 60 * 60 * 1000);
    remote.hold_header.store(true, Ordering::SeqCst);
    let rereading = {
        let drive = Arc::clone(&drive);
        std::thread::spawn(move || drive.pull(None).map(drop).map_err(|e| e.to_string()))
    };
    fetched.recv_timeout(Duration::from_secs(10))?;
    // Meanwhile Docs is removed, in the feed again, and another pull asked for.
    let removed = NodeMeta {
        removed: true,
        ..meta
    };
    inner.update(&docs, &removed, None)?;
    inner.server().relist(docs.at.file_id);
    let pulling = {
        let drive = Arc::clone(&drive);
        std::thread::spawn(move || drive.pull(None).map(drop).map_err(|e| e.to_string()))
    };
    let until = std::time::Instant::now() + Duration::from_millis(500);
    while !pulling.is_finished() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    go.send(())?;
    rereading.join().map_err(|_| "the re-read panicked")??;
    pulling.join().map_err(|_| "the pull panicked")??;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    let removed: Vec<_> = db
        .read(|s| s.list_removed(10))?
        .into_iter()
        .map(|r| r.row.file_id)
        .collect();
    assert_eq!(removed, [docs.at.file_id], "the removal stands");
    Ok(())
}

/// A file deleted for good stays gone whatever comes after: here a page fetched before the
/// delete and committed after it, past the pulls' lock (the store refuses the file id, as it
/// does a write the executor commits after it).
#[test]
fn a_row_fetched_before_a_hard_delete_is_refused_after_it() -> R {
    use hbsync_remote::ChangeRef;

    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let v1 = remote.create(&folder("Docs"), None)?;
    drive.pull(None)?;
    let fetched = remote.header(v1.at.file_id)?;
    remote.server().hard_delete(v1.at.file_id, v1.at.version)?;
    drive.announced(&[ChangeRef::of(&fetched).tombstone()])?;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    db.write(|s| hbsync_engine::feed::apply(s, &fetched).map(drop))?;
    assert!(db.read(|s| s.mirror_by_file(v1.at.file_id))?.is_none());
    Ok(())
}

/// A remote whose feed reads fail once a whole page was read: a pull that stops partway.
struct FailsAfterAPage {
    inner: Arc<FakeRemote>,
    whole: std::sync::atomic::AtomicUsize,
}

impl FailsAfterAPage {
    fn failing(&self) -> hbsync_remote::Result<()> {
        match self.whole.load(std::sync::atomic::Ordering::SeqCst) {
            0 => Ok(()),
            _ => Err(hbsync_remote::RemoteError::Transient(
                "the line went down".into(),
            )),
        }
    }
}

impl Remote for FailsAfterAPage {
    fn changes(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::ChangesPage> {
        self.failing()?;
        let page = self.inner.changes(cursor, limit);
        self.whole.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        page
    }
    fn change_refs(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::RefsPage> {
        self.failing()?;
        self.inner.change_refs(cursor, limit)
    }
    fn header(
        &self,
        file_id: hbsync_types::RemoteFileId,
    ) -> hbsync_remote::Result<hbsync_remote::RemoteHeader> {
        self.inner.header(file_id)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn holding_live(
        &self,
        folders: &[hbsync_types::NodeId],
    ) -> hbsync_remote::Result<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding_live(folders)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::RemoteRef],
    ) -> hbsync_remote::Result<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Target>> {
        self.inner.set_removed(files, removed)
    }
    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.create(node, payload)
    }
    fn update(
        &self,
        target: &hbsync_types::Target,
        meta: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.update(target, meta, payload)
    }
    fn delete(
        &self,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> hbsync_remote::Result<()> {
        self.inner.delete(file_id, expected)
    }
    fn read(
        &self,
        file_id: hbsync_types::RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> hbsync_remote::Result<u64> {
        self.inner.read(file_id, range, sink)
    }
}

/// A pull that fails partway still wakes the folders the pages it committed concern.
#[test]
fn a_pull_that_fails_partway_wakes_what_it_took() -> R {
    let dir = tempfile::tempdir()?;
    let inner = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
    let remote = Arc::new(FailsAfterAPage {
        inner: Arc::clone(&inner),
        whole: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut account = StateDb::open_account(&dir.path().join("state.db"))?;
    let main = account.write(|s| s.add_folder(NodeId::ROOT, "/h/Homebase", "m1"))?;
    drop(account);
    let drive = Drive::new(
        StateDb::open_account(&dir.path().join("state.db"))?,
        remote.clone(),
    );
    let woken = drive.subscribe(main);
    // More than a page: the second read fails.
    for i in 0..=hbsync_engine::drive::FEED_PAGE {
        inner.create(&folder(&format!("f{i}")), None)?;
    }
    assert!(drive.pull(None).is_err(), "the second page fails");
    assert!(
        woken.try_recv().is_ok(),
        "the first page's folders are woken"
    );
    Ok(())
}

/// Thousands of announced versions, more than SQLite takes variables in one query, are asked
/// in parts.
#[test]
fn many_announced_versions_are_asked_in_parts() -> R {
    use hbsync_remote::ChangeRef;
    use hbsync_types::{RemoteFileId, VersionTag};

    let dir = tempfile::tempdir()?;
    let (drive, _) = drive(dir.path())?;
    let refs: Vec<ChangeRef> = (0..12_000)
        .map(|_| ChangeRef {
            file_id: RemoteFileId(uuid::Uuid::new_v4()),
            version: VersionTag(uuid::Uuid::new_v4()),
            deleted: false,
            parent: None,
            removed_at: None,
            changed_at: Some(1),
        })
        .collect();
    assert!(!drive.announced(&refs)?, "unknown: the feed is read");
    Ok(())
}

/// One ref per file: this device's own write made twice, or made then deleted for good, needs
/// no read of the feed.
#[test]
fn an_own_double_write_or_write_then_delete_needs_no_read() -> R {
    use hbsync_remote::ChangeRef;

    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let docs = folder("Docs");
    let v1 = remote.create(&docs, None)?;
    let first = ChangeRef::of(&remote.header(v1.at.file_id)?);
    let v2 = remote.update(&v1, &docs, None)?;
    let second = ChangeRef::of(&remote.header(v1.at.file_id)?);
    drive.pull(None)?;
    assert!(
        drive.announced(&[first, second])?,
        "v1 then v2 of an own write: v2 is known, v1 is past"
    );

    let notes = folder("Notes");
    let n1 = remote.create(&notes, None)?;
    let written = ChangeRef::of(&remote.header(n1.at.file_id)?);
    remote.server().hard_delete(n1.at.file_id, n1.at.version)?;
    assert!(
        drive.announced(&[written, written.tombstone()])?,
        "written, then deleted for good: nothing left to read"
    );
    let _ = v2;
    Ok(())
}

/// What happens to the drive in the property below, to item `0..ITEMS`.
#[derive(Debug, Clone)]
enum Step {
    Write(usize),
    Remove(usize),
    Restore(usize),
    HardDelete(usize),
    Pull,
    /// Announcements delivered: these of the queued ones (by index, wrapped), late, out of
    /// order and some twice.
    Announce(Vec<usize>),
    /// A round of the daemon's puller: what it gathered of the queued ones, the hard deletes
    /// taken first and on their own, then the rest.
    Round(Vec<usize>),
}

const ITEMS: usize = 4;

fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    prop_oneof![
        (0..ITEMS).prop_map(Step::Write),
        (0..ITEMS).prop_map(Step::Remove),
        (0..ITEMS).prop_map(Step::Restore),
        (0..ITEMS).prop_map(Step::HardDelete),
        Just(Step::Pull),
        proptest::collection::vec(0..64usize, 0..6).prop_map(Step::Announce),
        proptest::collection::vec(0..64usize, 0..24).prop_map(Step::Round),
    ]
}

proptest::proptest! {
    /// Announcements, however late, out of order or repeated, and pulls in between, leave the
    /// mirror as the drive is once every hard delete was announced and the feed read: no row a
    /// hard delete took comes back, none is left, and nothing moves back to an older version.
    #[test]
    fn announcements_and_pulls_leave_the_mirror_as_the_drive_is(
        steps in proptest::collection::vec(step(), 1..40)
    ) {
        mirror_follows_the_drive(steps)?;
    }
}

/// An error as a failed test case.
fn ok<T, E: std::fmt::Display>(r: Result<T, E>) -> Result<T, proptest::test_runner::TestCaseError> {
    r.map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))
}

fn mirror_follows_the_drive(steps: Vec<Step>) -> Result<(), proptest::test_runner::TestCaseError> {
    use hbsync_remote::ChangeRef;
    let dir = ok(tempfile::tempdir())?;
    let (drive, remote) = ok(drive(dir.path()))?;
    let mut db = ok(StateDb::open_account(&dir.path().join("state.db")))?;
    // Per item: its node, and its file now (`None`: none, or deleted for good).
    let metas: Vec<NodeMeta> = (0..ITEMS).map(|i| folder(&format!("f{i}"))).collect();
    let mut files: Vec<Option<hbsync_types::Target>> = vec![None; ITEMS];
    // Every file id an item had: one deleted for good must stay gone.
    let mut ever: Vec<Vec<hbsync_types::RemoteFileId>> = vec![Vec::new(); ITEMS];
    let mut queued: Vec<ChangeRef> = Vec::new();
    let mut hard: Vec<ChangeRef> = Vec::new();
    let announce = |remote: &FakeRemote, at: &hbsync_types::Target| {
        ok(remote.header(at.at.file_id)).map(|h| ChangeRef::of(&h))
    };
    for step in steps {
        match step {
            Step::Write(i) => {
                let done = match &files[i] {
                    None => remote.create(&metas[i], None),
                    Some(at) => remote.update(at, &metas[i], None),
                };
                if let Ok(at) = done {
                    queued.push(announce(&remote, &at)?);
                    if !ever[i].contains(&at.at.file_id) {
                        ever[i].push(at.at.file_id);
                    }
                    files[i] = Some(at);
                }
            }
            Step::Remove(i) | Step::Restore(i) => {
                let meta = match step {
                    Step::Remove(_) => hbsync_remote::contract::removed(&metas[i]),
                    _ => metas[i].clone(),
                };
                if let Some(at) = &files[i]
                    && let Ok(now) = remote.update(at, &meta, None)
                {
                    queued.push(announce(&remote, &now)?);
                    files[i] = Some(now);
                }
            }
            Step::HardDelete(i) => {
                if let Some(at) = files[i].take() {
                    let was = announce(&remote, &at)?;
                    ok(remote.server().hard_delete(at.at.file_id, at.at.version))?;
                    queued.push(was.tombstone());
                    hard.push(was.tombstone());
                }
            }
            Step::Pull => {
                ok(drive.pull(None))?;
            }
            Step::Announce(picks) => {
                if queued.is_empty() {
                    continue;
                }
                let refs: Vec<ChangeRef> = picks.iter().map(|k| queued[k % queued.len()]).collect();
                ok(drive.announced(&refs))?;
            }
            Step::Round(picks) => {
                if queued.is_empty() {
                    continue;
                }
                let refs = picks.iter().map(|k| queued[k % queued.len()]);
                let (gone, live): (Vec<ChangeRef>, Vec<ChangeRef>) = refs.partition(|r| r.deleted);
                for refs in [gone, live] {
                    if !refs.is_empty() {
                        ok(drive.announced(&refs))?;
                    }
                }
            }
        }
    }
    // Every hard delete is announced at least once (a notification), and the feed read.
    ok(drive.announced(&hard))?;
    ok(drive.pull(None))?;
    for (i, file) in files.iter().enumerate() {
        let now = file.as_ref().map(|at| at.at.file_id);
        for gone in ever[i].iter().filter(|id| Some(**id) != now) {
            let back = ok(db.read(|s| s.mirror_by_file(*gone)))?;
            proptest::prop_assert!(back.is_none(), "f{} came back: {:?}", i, back);
        }
        let removed = ok(db.read(|s| s.removed_by_node(metas[i].node)))?;
        let Some(at) = file else {
            proptest::prop_assert!(removed.is_none(), "f{} left in Recently deleted", i);
            continue;
        };
        let h = ok(remote.header(at.at.file_id))?;
        let mirror = ok(db.read(|s| s.mirror_by_file(at.at.file_id)))?;
        if h.removed_at.is_some() {
            proptest::prop_assert!(mirror.is_none());
            proptest::prop_assert_eq!(removed.map(|r| r.row.version), Some(h.version));
        } else {
            proptest::prop_assert_eq!(mirror.map(|r| r.version), Some(h.version));
            proptest::prop_assert!(removed.is_none());
        }
    }
    Ok(())
}

/// A change pulled by one folder's engine wakes the folders it concerns: not itself, and not
/// one whose part of the drive it isn't in.
#[test]
fn a_change_wakes_the_folders_it_concerns() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let docs_top = Role::Documents.node();
    let (main_id, docs_id) = StateDb::open_account(&dir.path().join("state.db"))?.write(|s| {
        Ok((
            s.add_folder(NodeId::ROOT, "/h/Homebase", "m1")?,
            s.add_folder(docs_top, "/h/Documents", "m2")?,
        ))
    })?;
    let (main, docs) = (drive.subscribe(main_id), drive.subscribe(docs_id));
    let documents = NodeMeta {
        node: docs_top,
        ..folder("Documents")
    };
    remote.create(&documents, None)?;
    remote.create(&folder("Projects"), None)?;
    drive.pull(Some(docs_id))?;
    assert!(main.try_recv().is_ok(), "Projects is the main folder's");
    assert!(docs.try_recv().is_err(), "the puller isn't woken");

    let letter = NodeMeta {
        parent: docs_top,
        ..folder("Letters")
    };
    remote.create(&letter, None)?;
    drive.pull(None)?;
    assert!(docs.try_recv().is_ok(), "Letters is in Documents");
    assert!(main.try_recv().is_err(), "nothing for the main folder");
    drive.pull(None)?;
    assert!(docs.try_recv().is_err(), "nothing new: nobody is woken");
    Ok(())
}

/// The purge empties the trash of every folder: what went there since it began, before the
/// cut-off.
#[test]
fn the_purge_empties_every_folders_trash() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, _) = drive(dir.path())?;
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let mut account = StateDb::open_account(&dir.path().join("state.db"))?;
    account.write(|s| {
        s.add_folder(NodeId::ROOT, &a.to_string_lossy(), "ma")?;
        s.add_folder(NodeId::new(), &b.to_string_lossy(), "mb")
    })?;
    // The first purge starts the clock: what was in the trash before stays.
    drive.purge(0)?;
    let now = hbsync_types::unix_now();
    let entry = |folder: &std::path::Path, at: u64| {
        let path = folder
            .join(".hbsync/trash")
            .join(format!("{at}-{}", NodeId::new().0))
            .join("f.txt");
        std::fs::create_dir_all(path.parent().expect("parent"))?;
        std::fs::write(&path, "x")?;
        std::io::Result::Ok(path)
    };
    let (in_a, in_b) = (entry(&a, now + 1)?, entry(&b, now + 1)?);
    let young = entry(&b, now + 100)?;
    // Kept less than nothing: what goes there in the next 10 s goes too.
    drive.purge(-10_000)?;
    assert!(!in_a.exists() && !in_b.exists(), "both trashes emptied");
    assert!(young.exists(), "what is younger stays");
    Ok(())
}

/// The files' tokens are sealed under a login's key: another login forgets them, the same one
/// keeps them.
#[test]
fn a_new_login_forgets_the_tokens() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let made = remote.create(&folder("Docs"), None)?;
    drive.pull(None)?;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    let mut token = || -> Result<_, hbsync_state::StateError> {
        db.read(|s| s.mirror_by_file(made.at.file_id))
            .map(|r| r.and_then(|r| r.token))
    };
    assert!(token()?.is_some(), "the feed's");
    assert!(drive.tokens_sealed_by("one")?, "none recorded: forgotten");
    assert_eq!(token()?, None);
    let header = remote.header(made.at.file_id)?;
    let mut db2 = StateDb::open_account(&dir.path().join("state.db"))?;
    db2.write(|s| {
        let row = hbsync_engine::feed::mirror_row(&header).expect("a folder's row");
        s.refresh(&row, false)
    })?;
    assert!(token()?.is_some());
    assert!(!drive.tokens_sealed_by("one")?, "the same login");
    assert!(token()?.is_some());
    assert!(drive.tokens_sealed_by("two")?, "another login");
    assert_eq!(token()?, None);
    Ok(())
}

/// A role folder this device makes takes its place out of Recently deleted, as any folder the
/// executor makes does: one there that couldn't come back (deleted for good meanwhile) isn't
/// left beside the new one.
#[test]
fn a_role_folder_made_leaves_recently_deleted() -> R {
    use hbsync_remote::contract::removed;
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let top = Role::Documents.node();
    let meta = NodeMeta {
        node: top,
        parent: NodeId::ROOT,
        kind: NodeKind::Folder,
        content: NodeContent::folder(Role::Documents.name()),
        removed: false,
    };
    let made = remote.create(&meta, None)?;
    let gone = remote.update(&made, &removed(&meta), None)?;
    drive.pull(None)?;
    let mut db = StateDb::open_account(&dir.path().join("state.db"))?;
    assert!(db.read(|s| s.removed_by_node(top))?.is_some());
    // Deleted for good, not yet read: the restore finds nothing, and the folder is made anew.
    remote.delete(gone.at.file_id, gone.at.version)?;
    drive.announcing_from(drive.connection());
    drive.ensure_role(Role::Documents)?;
    assert!(db.read(|s| s.get_mirror(top))?.is_some(), "made");
    assert!(
        db.read(|s| s.removed_by_node(top))?.is_none(),
        "out of Recently deleted"
    );
    Ok(())
}

/// A doubt that comes while a read of the feed runs isn't cleared by that read: the account
/// stays behind until a read that started after it.
#[test]
fn a_read_clears_only_the_doubts_from_before_it() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    drive.announcing_from(drive.connection());
    assert!(drive.current());
    remote.faults().latency = Duration::from_millis(300);
    std::thread::scope(|scope| -> R {
        let reading = scope.spawn(|| drive.pull(None).map(drop));
        std::thread::sleep(Duration::from_millis(100));
        drive.set_behind();
        reading.join().map_err(|_| "panicked")??;
        Ok(())
    })?;
    assert!(!drive.current(), "the doubt came during the read");
    drive.pull(None)?;
    assert!(drive.current());
    Ok(())
}

/// A restore is the user's: it reads the feed first, whatever the notifications say; a write
/// of it whose answer didn't come (it may have landed) leaves the account behind.
#[test]
fn a_restore_reads_first_and_a_lost_answer_is_behind() -> R {
    use hbsync_remote::contract::removed;
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let docs = folder("Docs");
    let made = remote.create(&docs, None)?;
    remote.update(&made, &removed(&docs), None)?;
    drive.pull(None)?;
    drive.announcing_from(drive.connection());
    let pages = || {
        let f = remote.faults();
        f.ref_pages + f.full_pages
    };
    let before = pages();
    remote.faults().fail_writes_named = Some("Docs".into());
    assert!(drive.restore(&[docs.node]).is_err(), "the write failed");
    assert!(pages() > before, "read first");
    assert!(!drive.current(), "its answer didn't come: behind");
    Ok(())
}
