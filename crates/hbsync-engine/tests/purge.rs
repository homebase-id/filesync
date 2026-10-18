//! Deleting for good: without a trace once the feed had the delete long enough, else a
//! tombstone; and a device away longer than that reading the whole feed again. Time passes on
//! the fake server's clock: every time involved is the server's.

mod common;

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Client, server};
use hbsync_engine::deleted;
use hbsync_engine::feed::{self, TRACE_MS};
use hbsync_remote::RemoteError;
use hbsync_remote::fake::{FakeRemote, FakeServer};
use hbsync_state::StateDb;
use hbsync_types::{NodeId, RemoteFileId};

/// A day more than the feed keeps a trace of a delete for good.
const LATER: i64 = TRACE_MS + 24 * 60 * 60 * 1000;

fn live(server: &Arc<Mutex<FakeServer>>) -> Vec<String> {
    let mut names = server.lock().expect("server").live_names();
    names.sort();
    names
}

/// A young removal leaves a tombstone, which a device away now still sees; the tombstone goes
/// hard once it is old enough, and nothing of the file is left on the server.
#[test]
fn a_young_removal_leaves_a_tombstone_that_goes_hard_later() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    let x = server
        .lock()
        .map_err(|_| "poisoned")?
        .live()
        .find(|h| h.state().is_some_and(|s| s.name == "x.txt"))
        .map(|h| h.file_id)
        .ok_or("x on the server")?;
    fs::remove_file(a.root.join("x.txt"))?;
    a.sync()?;

    assert_eq!(a.engine.drive().purge(0)?, 1);
    let header = server.lock().map_err(|_| "poisoned")?.header(x)?;
    assert!(header.is_deleted(), "a tombstone: {header:?}");
    a.sync()?;

    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    a.engine.drive().purge(0)?;
    assert!(
        matches!(
            server.lock().map_err(|_| "poisoned")?.header(x),
            Err(RemoteError::NotFound)
        ),
        "the tombstone went hard"
    );
    let feed = server.lock().map_err(|_| "poisoned")?.changes(None, 1000)?;
    assert!(
        feed.entries.iter().all(|h| h.file_id != x),
        "nothing in the feed"
    );
    assert_eq!(live(&server), ["y.txt"]);
    Ok(())
}

/// An old removal goes hard at once, a folder with what was deleted with it; a device that saw
/// it removed and comes back reads the feed again, and forgets it.
#[test]
fn an_old_removal_goes_hard_at_once() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("d/x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.recently_deleted(10)?.len(), 2, "b saw the delete");

    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(0)?, 2);
    let gone = server
        .lock()
        .map_err(|_| "poisoned")?
        .changes(None, 1000)?
        .entries
        .iter()
        .all(|h| h.state().is_some_and(|s| s.name == "y.txt"));
    assert!(gone, "only y is left in the feed, no tombstones");
    assert!(a.recently_deleted(10)?.is_empty());

    b.sync()?;
    assert!(b.recently_deleted(10)?.is_empty(), "b forgot it");
    assert_eq!(live(&server), ["y.txt"]);
    Ok(())
}

/// A device away longer than the feed keeps a trace of a delete for good doesn't see it in the
/// feed: it reads the whole feed again, and what the feed no longer has is deleted here (to the
/// folder's trash), never uploaded again. Back from that, it reads the feed as before.
#[test]
fn a_device_away_longer_than_the_trace_reads_the_feed_again() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("d/x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    b.sync()?;

    // While b is away, x is deleted and, the removal old enough, deleted for good.
    fs::remove_file(a.root.join("d/x.txt"))?;
    a.sync()?;
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(0)?, 1);

    b.sync()?;
    assert!(b.read_text("d/x.txt").is_err(), "x went from b");
    assert_eq!(b.read_text("y.txt")?, "y");
    assert_eq!(live(&server), ["d", "y.txt"], "nothing came back");

    let pages = |c: &Client| {
        let f = c.remote.faults();
        f.ref_pages + f.full_pages
    };
    let before = pages(&b);
    b.sync()?;
    assert!(pages(&b) - before <= 2, "no second re-read");
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// The header of the file named `name` on the server, live or removed.
fn header_named(server: &Arc<Mutex<FakeServer>>, name: &str) -> hbsync_remote::RemoteHeader {
    let server = server.lock().expect("server");
    server
        .live()
        .chain(server.removed())
        .find(|h| match &h.body {
            hbsync_remote::RemoteBody::Live { content, .. } => content.name == name,
            hbsync_remote::RemoteBody::Deleted => false,
        })
        .cloned()
        .expect("on the server")
}

/// The feed leaves out some rows the server has (one changed in its last moment, one the
/// caller may not see): a re-read keeps a file it didn't show once the server says it is there.
#[test]
fn a_reread_keeps_what_the_feed_leaves_out() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    b.sync()?;
    let y = header_named(&server, "y.txt").file_id;
    {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        server.advance(LATER);
        server.unlist(y);
    }
    b.sync()?;
    assert_eq!(b.read_text("y.txt")?, "y", "kept: the server has it");
    assert_eq!(live(&server), ["x.txt", "y.txt"]);
    Ok(())
}

/// A restore of an item deleted for good meanwhile (by another device, leaving no trace in the
/// feed) forgets it: it leaves Recently deleted.
#[test]
fn a_restore_of_what_went_for_good_forgets_it() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    fs::remove_file(a.root.join("x.txt"))?;
    a.sync()?;
    let x = header_named(&server, "x.txt");
    server
        .lock()
        .map_err(|_| "poisoned")?
        .hard_delete(x.file_id, x.version)?;
    let item = a
        .recently_deleted(10)?
        .pop()
        .ok_or("x in Recently deleted")?;
    assert_eq!(a.engine.drive().restore(&[item.node])?, 0);
    assert!(a.recently_deleted(10)?.is_empty(), "forgotten");
    Ok(())
}

/// Hard by each node's own removal: in a folder's group, one removed lately (another device's
/// mark, long after the folder's) leaves a tombstone while the rest goes without a trace.
#[test]
fn a_young_member_of_an_old_group_leaves_a_tombstone() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/x.txt", "x")?;
    a.write("d/y.txt", "y")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let y = header_named(&server, "y.txt");
    {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        server.advance(LATER);
        // y removed again, now: after the folder's removal, so still deleted with it.
        let hbsync_remote::RemoteBody::Live { node, content, .. } = &y.body else {
            return Err("y is live".into());
        };
        let meta = hbsync_remote::NodeMeta {
            node: *node,
            parent: y.parent,
            kind: y.kind,
            content: content.clone(),
            removed: true,
        };
        server.update(y.file_id, y.version, &meta, None)?;
    }
    a.sync()?;
    assert_eq!(a.engine.drive().purge(0)?, 3);
    let server = server.lock().map_err(|_| "poisoned")?;
    assert!(server.header(y.file_id)?.is_deleted(), "y: a tombstone");
    let left = server.live().chain(server.removed()).count();
    assert_eq!(left, 0, "x and d went without a trace");
    Ok(())
}

/// Marks the node of `header` removed on the server, as another device's delete does, without
/// what it holds (their removals are still on their way).
fn remove_on_server(server: &Arc<Mutex<FakeServer>>, header: &hbsync_remote::RemoteHeader) {
    let hbsync_remote::RemoteBody::Live { node, content, .. } = &header.body else {
        panic!("live: {header:?}");
    };
    let meta = hbsync_remote::NodeMeta {
        node: *node,
        parent: header.parent,
        kind: header.kind,
        content: content.clone(),
        removed: true,
    };
    let mut server = server.lock().expect("server");
    server
        .update(header.file_id, header.version, &meta, None)
        .expect("removed");
}

fn node_of(header: &hbsync_remote::RemoteHeader) -> NodeId {
    match &header.body {
        hbsync_remote::RemoteBody::Live { node, .. } => *node,
        hbsync_remote::RemoteBody::Deleted => panic!("live: {header:?}"),
    }
}

/// The purge never writes a live node: a removed folder still holding one (another device's
/// delete, stopped before it removed what is in it) stays in Recently deleted, purge after
/// purge, what it holds untouched; and it can be restored.
#[test]
fn a_removed_folder_holding_a_live_node_stays_and_nothing_live_is_touched() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/e/x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    let d = header_named(&server, "d");
    // The purge reads the feed itself, before a sync would carry the removal on to e and x.
    remove_on_server(&server, &d);
    let (e, x) = (header_named(&server, "e"), header_named(&server, "x.txt"));
    for _ in 0..2 {
        server.lock().map_err(|_| "poisoned")?.advance(LATER);
        assert_eq!(a.engine.drive().purge(TRACE_MS)?, 0, "d stays");
    }
    {
        let server = server.lock().map_err(|_| "poisoned")?;
        assert!(
            server.header(d.file_id)?.removed_at.is_some(),
            "d is removed"
        );
        for held in [&e, &x] {
            let now = server.header(held.file_id)?;
            assert_eq!(now.version, held.version, "untouched: {now:?}");
        }
        assert_eq!(server.payload(x.file_id)?.as_slice(), b"x");
    }
    let items = a.recently_deleted(10)?;
    assert!(
        items.iter().any(|i| i.node == node_of(&d)),
        "d in Recently deleted: {items:?}"
    );
    assert_eq!(a.engine.drive().restore(&[node_of(&d)])?, 1);
    a.sync()?;
    assert_eq!(a.read_text("d/e/x.txt")?, "x");
    assert_eq!(live(&server), ["d", "e", "x.txt", "y.txt"]);
    Ok(())
}

/// An item is due once all of it is as old as the purge keeps: a file removed long after its
/// folder (a delete that stopped half way, the file live in the folder until then) has its own
/// 30 days in Recently deleted, and the folder waits with it.
#[test]
fn a_member_removed_later_keeps_its_own_time() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("m/f.txt", "f")?;
    a.sync()?;
    let m = header_named(&server, "m");
    remove_on_server(&server, &m);
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(TRACE_MS)?, 0, "m holds f");
    // The delete carries on: f removed now, with m.
    let f = header_named(&server, "f.txt");
    remove_on_server(&server, &f);
    assert_eq!(a.engine.drive().purge(TRACE_MS)?, 0, "f has its own time");
    {
        let server = server.lock().map_err(|_| "poisoned")?;
        assert!(server.header(m.file_id)?.removed_at.is_some(), "m");
        assert!(server.header(f.file_id)?.removed_at.is_some(), "f");
        assert_eq!(server.payload(f.file_id)?.as_slice(), b"f");
    }
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(TRACE_MS)?, 2);
    assert!(live(&server).is_empty());
    Ok(())
}

/// A folder holding a live node this device doesn't read (not in its mirror: the feed left it
/// out) isn't deleted for good: the server is asked first.
#[test]
fn a_folder_holding_what_this_device_doesnt_read_stays() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/x.txt", "x")?;
    a.sync()?;
    let (d, x) = (header_named(&server, "d"), header_named(&server, "x.txt"));
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let z = {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        let (_, z) = server.seed_file(
            node_of(&d),
            "z.txt",
            b"z",
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
        server.unlist(z.file_id);
        z
    };
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(0)?, 1, "x went, d stays");
    a.sync()?;
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(0)?, 0);
    let server = server.lock().map_err(|_| "poisoned")?;
    let d = server.header(d.file_id)?;
    assert!(d.removed_at.is_some(), "d stays: {d:?}");
    assert!(server.header(z.file_id)?.state().is_some(), "z is live");
    assert!(matches!(
        server.header(x.file_id),
        Err(RemoteError::NotFound)
    ));
    Ok(())
}

/// A file deleted for good elsewhere without a trace and edited here: the update finds it gone,
/// which forgets it, and the next plan puts it on the drive anew.
#[test]
fn an_edit_of_a_file_gone_for_good_puts_it_back_anew() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    let x = header_named(&server, "x.txt");
    server
        .lock()
        .map_err(|_| "poisoned")?
        .hard_delete(x.file_id, x.version)?;
    a.write("x.txt", "x, edited")?;
    a.sync()?;
    a.sync()?;
    assert_eq!(live(&server), ["x.txt"]);
    let again = header_named(&server, "x.txt");
    assert_ne!(again.file_id, x.file_id, "a new file");
    Ok(())
}

/// A node written into a folder after its removal isn't in its delete, and holds it: the
/// folder stays, in Recently deleted, while the node is live.
#[test]
fn a_node_written_into_a_removed_folder_later_holds_it() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/x.txt", "x")?;
    a.sync()?;
    let d = header_named(&server, "d");
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    server.lock().map_err(|_| "poisoned")?.seed_file(
        node_of(&d),
        "z.txt",
        b"z",
        hbsync_fileio::HashAlgorithm::Blake3,
    )?;

    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(a.engine.drive().purge(0)?, 1, "x went, d stays");
    let server = server.lock().map_err(|_| "poisoned")?;
    let d = server.header(d.file_id)?;
    assert!(d.removed_at.is_some(), "d stays: {d:?}");
    assert_eq!(server.live_names(), ["z.txt"]);
    Ok(())
}

/// A restore whose folder went for good meanwhile (another device's purge) brings the item
/// back at the top in the same restore.
#[test]
fn a_restore_whose_folder_went_for_good_comes_back_at_the_top() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/x.txt", "x")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let d = header_named(&server, "d");
    server
        .lock()
        .map_err(|_| "poisoned")?
        .hard_delete(d.file_id, d.version)?;
    let x = header_named(&server, "x.txt");
    assert_eq!(a.engine.drive().restore(&[node_of(&x)])?, 1);
    a.sync()?;
    assert_eq!(a.read_text("x.txt")?, "x");
    Ok(())
}

/// The account's DB with what the server has, read in one pull.
fn pulled(remote: &FakeRemote) -> common::Res<std::cell::RefCell<StateDb>> {
    let db = std::cell::RefCell::new(StateDb::open_in_memory()?);
    feed::pull(|| db.borrow_mut(), remote, 1000, &mut None)?;
    Ok(db)
}

fn seed(server: &Arc<Mutex<FakeServer>>, name: &str) -> common::Res<RemoteFileId> {
    let (_, at) = server.lock().map_err(|_| "poisoned")?.seed_file(
        NodeId::ROOT,
        name,
        name.as_bytes(),
        hbsync_fileio::HashAlgorithm::Blake3,
    )?;
    Ok(at.file_id)
}

/// A re-read asks the server about what the feed didn't show; one it can't tell about now (a
/// broken connection) is kept and asked again at the next pull, the re-read open until then.
#[test]
fn a_reread_asks_again_what_the_server_couldnt_tell_about() -> common::Res<()> {
    let server = server();
    seed(&server, "x.txt")?;
    let y = seed(&server, "y.txt")?;
    let remote = FakeRemote::new(server.clone());
    let db = pulled(&remote)?;
    db.borrow_mut().write(|s| s.start_reread())?;
    server.lock().map_err(|_| "poisoned")?.unlist(y);
    // The page reads; y's header doesn't.
    remote.faults().script = [false, true].into();
    let mut reread = None;
    feed::pull(|| db.borrow_mut(), &remote, 1000, &mut reread)?;
    assert!(
        db.borrow_mut().read(|s| s.mirror_by_file(y))?.is_some(),
        "y kept"
    );
    assert!(db.borrow_mut().read(|s| s.rereading())?, "open for y");
    assert!(reread.is_some());
    // Gone meanwhile: the next pull asks again, and finds out.
    {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        let version = server.header(y)?.version;
        server.hard_delete(y, version)?;
    }
    feed::pull(|| db.borrow_mut(), &remote, 1000, &mut reread)?;
    assert!(
        db.borrow_mut().read(|s| s.mirror_by_file(y))?.is_none(),
        "y went"
    );
    assert!(
        !db.borrow_mut().read(|s| s.rereading())?,
        "the re-read ended"
    );
    assert!(reread.is_none());
    Ok(())
}

/// A re-read cut short and taken up again after as long an absence starts over: what it had
/// seen may have gone for good since, without a trace.
#[test]
fn a_reread_taken_up_after_long_starts_over() -> common::Res<()> {
    let server = server();
    let x = seed(&server, "x.txt")?;
    seed(&server, "y.txt")?;
    seed(&server, "z.txt")?;
    let remote = FakeRemote::new(server.clone());
    let db = pulled(&remote)?;
    db.borrow_mut().write(|s| s.start_reread())?;
    // The first page (x) reads, the next doesn't.
    remote.faults().script = [false, true, true, true, true].into();
    let mut reread = None;
    assert!(feed::pull(|| db.borrow_mut(), &remote, 1, &mut reread).is_err());
    assert!(reread.is_some(), "held");
    {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        let version = server.header(x)?.version;
        server.hard_delete(x, version)?;
        server.advance(LATER);
    }
    feed::pull(|| db.borrow_mut(), &remote, 1000, &mut reread)?;
    assert!(
        db.borrow_mut().read(|s| s.mirror_by_file(x))?.is_none(),
        "x is gone here too"
    );
    Ok(())
}

/// A write answered not found, though the file is there (a read past a payload's end, say):
/// asked by its header, the file stays known, and a later write goes to it, not a new file.
#[test]
fn a_not_found_the_header_denies_forgets_nothing() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    let x = header_named(&server, "x.txt");
    a.remote.faults().not_found_writes = true;
    a.write("x.txt", "x, edited")?;
    let _ = a.sync();
    a.remote.faults().not_found_writes = false;
    a.sync()?;
    let now = header_named(&server, "x.txt");
    assert_eq!(now.file_id, x.file_id, "the same file");
    assert_eq!(live(&server), ["x.txt"]);
    assert_eq!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .payload(x.file_id)?
            .as_slice(),
        b"x, edited"
    );
    Ok(())
}

/// A removal answered not found, though the file is there: the header says so, so the delete
/// isn't taken as done; it reaches the drive at a later sync, and the file doesn't come back.
#[test]
fn a_removal_the_header_denies_is_sent_again() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.write("y.txt", "y")?;
    a.sync()?;
    let x = header_named(&server, "x.txt");
    a.remote.faults().not_found_writes = true;
    fs::remove_file(a.root.join("x.txt"))?;
    let _ = a.sync();
    assert!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .header(x.file_id)?
            .removed_at
            .is_none(),
        "not removed yet"
    );
    a.remote.faults().not_found_writes = false;
    a.sync()?;
    a.sync()?;
    assert!(
        removed_on_server(&server, "x.txt"),
        "the delete reached the drive"
    );
    assert!(a.read_text("x.txt").is_err(), "x didn't come back");
    assert_eq!(live(&server), ["y.txt"]);
    Ok(())
}

fn removed_on_server(server: &Arc<Mutex<FakeServer>>, name: &str) -> bool {
    header_named(server, name).removed_at.is_some()
}

/// A remote that watches the purge: whether the account was free at each request, and how
/// long each question about folders came after the request before it. Its queries hold back
/// a moment. With `cut`, a delete for good stops after that many files with that error, as a
/// failed batch after others went; with `answer`, deletes answer that for each file, deleting
/// nothing; with `soft_fails`, a soft delete fails as a broken connection; with `headers_fail`,
/// so does a header read (`headers` counts them).
struct Watched {
    inner: FakeRemote,
    account: Arc<Mutex<StateDb>>,
    hold: Duration,
    cut: Option<(usize, fn() -> RemoteError)>,
    answer: Option<fn() -> RemoteError>,
    soft_fails: bool,
    headers_fail: bool,
    headers: AtomicUsize,
    /// Requests made while the account was locked.
    locked: AtomicUsize,
    last: Mutex<Option<Instant>>,
    /// How long after the request before it each question about folders came.
    waited: Mutex<Vec<Duration>>,
}

impl Watched {
    fn new(
        server: &Arc<Mutex<FakeServer>>,
        account: &Arc<Mutex<StateDb>>,
        hold: Duration,
    ) -> Watched {
        Watched {
            inner: FakeRemote::new(server.clone()),
            account: account.clone(),
            hold,
            cut: None,
            answer: None,
            soft_fails: false,
            headers_fail: false,
            headers: AtomicUsize::default(),
            locked: AtomicUsize::default(),
            last: Mutex::default(),
            waited: Mutex::default(),
        }
    }

    /// A request: was the account free, and how long since the one before.
    fn request(&self) -> Option<Duration> {
        if self.account.try_lock().is_err() {
            self.locked.fetch_add(1, Ordering::SeqCst);
        }
        let mut last = self.last.lock().expect("last");
        last.replace(Instant::now()).map(|t| t.elapsed())
    }
}

impl hbsync_remote::Remote for Watched {
    fn changes(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::ChangesPage> {
        self.request();
        self.inner.changes(cursor, limit)
    }
    fn change_refs(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::RefsPage> {
        self.request();
        self.inner.change_refs(cursor, limit)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn header(&self, file_id: RemoteFileId) -> hbsync_remote::Result<hbsync_remote::RemoteHeader> {
        self.request();
        self.headers.fetch_add(1, Ordering::SeqCst);
        if self.headers_fail {
            return Err(RemoteError::Transient("cut".into()));
        }
        self.inner.header(file_id)
    }
    fn holding_live(&self, folders: &[NodeId]) -> hbsync_remote::Result<HashSet<NodeId>> {
        if let Some(since) = self.request() {
            self.waited.lock().expect("waited").push(since);
        }
        self.inner.holding_live(folders)
    }
    fn hold_back(&self) -> Duration {
        self.hold
    }
    fn create(
        &self,
        node: &hbsync_remote::NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.request();
        self.inner.create(node, payload)
    }
    fn update(
        &self,
        target: &hbsync_types::Target,
        meta: &hbsync_remote::NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.request();
        self.inner.update(target, meta, payload)
    }
    fn delete(
        &self,
        file_id: RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> hbsync_remote::Result<()> {
        self.request();
        if let Some(answer) = self.answer {
            return Err(answer());
        }
        if self.soft_fails {
            return Err(RemoteError::Transient("cut".into()));
        }
        self.inner.delete(file_id, expected)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::RemoteRef],
    ) -> hbsync_remote::Result<hbsync_remote::ForGood> {
        self.request();
        if let Some(answer) = self.answer {
            let answers = files.iter().map(|_| Err(answer())).collect();
            return Ok(hbsync_remote::ForGood::all(answers));
        }
        match self.cut {
            Some((n, stop)) if n < files.len() => {
                let mut done = self.inner.delete_for_good(&files[..n])?;
                done.stopped = Some(stop());
                Ok(done)
            }
            _ => self.inner.delete_for_good(files),
        }
    }
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> hbsync_remote::Result<u64> {
        self.request();
        self.inner.read(file_id, range, sink)
    }
}

/// Reads the feed into `account`: the server's time.
fn pull_into(account: &Mutex<StateDb>, remote: &dyn hbsync_remote::Remote) -> common::Res<i64> {
    let report = feed::pull(|| account.lock().expect("account"), remote, 1000, &mut None)?;
    Ok(report.server_time.ok_or("a time")?)
}

/// The purge holds the account only while it reads or writes it, never while the drive
/// answers; and it asks the drive about a pass's folders no sooner than the drive's hold-back
/// after the request before (by then, the drive's queries show what was written up to it).
#[test]
fn the_purge_holds_the_account_only_for_its_own_reads_and_writes() -> common::Res<()> {
    let server = server();
    let folder = |parent: NodeId, name: &str| hbsync_remote::NodeMeta {
        node: NodeId::new(),
        parent,
        kind: hbsync_types::NodeKind::Folder,
        content: hbsync_types::schema::NodeContent::folder(name),
        removed: false,
    };
    {
        let d = folder(NodeId::ROOT, "d");
        let e = folder(d.node, "e");
        let mut server = server.lock().map_err(|_| "poisoned")?;
        server.create(&d, None)?;
        server.create(&e, None)?;
        server.seed_file(e.node, "x.txt", b"x", hbsync_fileio::HashAlgorithm::Blake3)?;
    }
    // A delete, the folder first, then what is in it.
    let d = header_named(&server, "d");
    for name in ["d", "e", "x.txt"] {
        remove_on_server(&server, &header_named(&server, name));
    }
    let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
    let remote = Watched::new(&server, &account, Duration::from_millis(300));
    pull_into(&account, &remote)?;
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    let now = pull_into(&account, &remote)?;
    assert_eq!(
        deleted::purge(&account, &remote, TRACE_MS, now, &mut false)?,
        3
    );
    assert!(matches!(
        server.lock().map_err(|_| "poisoned")?.header(d.file_id),
        Err(RemoteError::NotFound)
    ));
    assert_eq!(
        remote.locked.load(Ordering::SeqCst),
        0,
        "no request while the account was locked"
    );
    let waited = remote.waited.lock().map_err(|_| "poisoned")?.clone();
    assert_eq!(waited.len(), 2, "e's pass, then d's: {waited:?}");
    assert!(
        waited.iter().all(|t| *t >= remote.hold),
        "asked after the hold-back: {waited:?}"
    );
    Ok(())
}

/// A delete for good stopped part way (a batch failed after others went): what went is
/// recorded gone; what the failed request didn't answer stays in Recently deleted, whatever
/// its error (a not found too: only the drive's answer for a file says it is gone), and the
/// purge reports the error, a fatal one too. The next purge takes the rest.
#[test]
fn a_purge_cut_short_keeps_what_went() -> common::Res<()> {
    let stops: [fn() -> RemoteError; 3] = [
        || RemoteError::Transient("cut".into()),
        || RemoteError::NotFound,
        || RemoteError::Fatal("refused".into()),
    ];
    for stop in stops {
        let server = server();
        let files = [seed(&server, "x.txt")?, seed(&server, "y.txt")?];
        for name in ["x.txt", "y.txt"] {
            remove_on_server(&server, &header_named(&server, name));
        }
        let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
        let mut remote = Watched::new(&server, &account, Duration::ZERO);
        remote.cut = Some((1, stop));
        pull_into(&account, &remote)?;
        server.lock().map_err(|_| "poisoned")?.advance(LATER);
        let now = pull_into(&account, &remote)?;
        let mut moved = false;
        let why = stop();
        assert!(
            deleted::purge(&account, &remote, TRACE_MS, now, &mut moved).is_err(),
            "{why:?}"
        );
        let gone = |f: &RemoteFileId| {
            matches!(
                server.lock().expect("server").header(*f),
                Err(RemoteError::NotFound)
            )
        };
        let (went, left): (Vec<RemoteFileId>, Vec<RemoteFileId>) =
            files.into_iter().partition(|f| gone(f));
        assert_eq!((went.len(), left.len()), (1, 1), "one went: {why:?}");
        {
            let mut account = account.lock().map_err(|_| "poisoned")?;
            let removed: Vec<RemoteFileId> = account
                .read(|s| s.list_removed(10))?
                .iter()
                .map(|r| r.row.file_id)
                .collect();
            assert_eq!(removed, left, "what went left Recently deleted: {why:?}");
            assert!(account.read(|s| s.file_gone(went[0]))?, "recorded gone");
            assert!(!account.read(|s| s.file_gone(left[0]))?, "{why:?}");
        }
        assert!(!moved, "nothing refused: {why:?}");

        remote.cut = None;
        assert_eq!(
            deleted::purge(&account, &remote, TRACE_MS, now, &mut moved)?,
            1
        );
        assert!(files.iter().all(gone));
        assert!(!moved);
    }
    Ok(())
}

/// A delete answered not found, of a file the drive still has (its header says so): it isn't
/// recorded gone, it stays in Recently deleted, and the account is behind. Hard or soft alike.
#[test]
fn a_not_found_the_header_denies_keeps_the_item() -> common::Res<()> {
    let server = server();
    let x = seed(&server, "x.txt")?;
    let y = seed(&server, "y.txt")?;
    let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
    let mut remote = Watched::new(&server, &account, Duration::ZERO);
    pull_into(&account, &remote)?;
    // x removed long ago (it goes without a trace), y lately (it leaves a tombstone).
    remove_on_server(&server, &header_named(&server, "x.txt"));
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    remove_on_server(&server, &header_named(&server, "y.txt"));
    server.lock().map_err(|_| "poisoned")?.advance(1_000);
    let now = pull_into(&account, &remote)?;
    remote.answer = Some(|| RemoteError::NotFound);
    let mut moved = false;
    assert_eq!(deleted::purge(&account, &remote, 0, now, &mut moved)?, 0);
    assert!(moved, "behind");
    {
        let mut account = account.lock().map_err(|_| "poisoned")?;
        assert_eq!(account.read(|s| s.list_removed(10))?.len(), 2, "both kept");
        for f in [x, y] {
            assert!(!account.read(|s| s.file_gone(f))?, "not recorded gone");
        }
    }
    remote.answer = None;
    assert_eq!(deleted::purge(&account, &remote, 0, now, &mut false)?, 2);
    Ok(())
}

/// A pass without a folder asks the drive nothing, and doesn't wait for its hold-back.
#[test]
fn a_pass_of_files_doesnt_wait() -> common::Res<()> {
    let server = server();
    seed(&server, "x.txt")?;
    remove_on_server(&server, &header_named(&server, "x.txt"));
    let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
    let remote = Watched::new(&server, &account, Duration::from_secs(2));
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    let now = pull_into(&account, &remote)?;
    let began = Instant::now();
    assert_eq!(
        deleted::purge(&account, &remote, TRACE_MS, now, &mut false)?,
        1
    );
    assert!(began.elapsed() < remote.hold, "{:?}", began.elapsed());
    assert!(remote.waited.lock().map_err(|_| "poisoned")?.is_empty());
    Ok(())
}

/// The account, with the server's files `names` removed now: their removals a moment old.
fn removed_now(
    server: &Arc<Mutex<FakeServer>>,
    names: &[&str],
) -> common::Res<(Arc<Mutex<StateDb>>, Vec<RemoteFileId>)> {
    let mut files = Vec::new();
    for name in names {
        files.push(seed(server, name)?);
        remove_on_server(server, &header_named(server, name));
    }
    server.lock().map_err(|_| "poisoned")?.advance(1_000);
    Ok((Arc::new(Mutex::new(StateDb::open_in_memory()?)), files))
}

/// A part of the purge that fails keeps none of the others from their work: deleting the
/// items fails here (the connection breaks), and the old tombstone is deleted hard all the
/// same; the purge fails, so it runs again.
#[test]
fn a_failing_part_of_the_purge_doesnt_stop_the_others() -> common::Res<()> {
    let server = server();
    let z = seed(&server, "z.txt")?;
    {
        let mut server = server.lock().map_err(|_| "poisoned")?;
        let version = server.header(z)?.version;
        server.delete(z, version)?;
        server.advance(LATER);
    }
    let (account, _) = removed_now(&server, &["y.txt"])?;
    let mut remote = Watched::new(&server, &account, Duration::ZERO);
    let now = pull_into(&account, &remote)?;
    assert_eq!(
        account
            .lock()
            .map_err(|_| "poisoned")?
            .read(|s| s.tombstones_before(now))?
            .len(),
        1
    );
    remote.soft_fails = true;
    assert!(deleted::purge(&account, &remote, 0, now, &mut false).is_err());
    assert!(
        matches!(
            server.lock().map_err(|_| "poisoned")?.header(z),
            Err(RemoteError::NotFound)
        ),
        "the tombstone went hard"
    );
    assert!(
        removed_on_server(&server, "y.txt"),
        "y stays, for the next purge"
    );
    Ok(())
}

/// Not found, and the header can't be read (the connection broke): the asking stops there,
/// the rest left for the next purge, which fails meanwhile.
#[test]
fn asking_after_not_found_stops_at_a_failed_request() -> common::Res<()> {
    let server = server();
    let (account, _) = removed_now(&server, &["x.txt", "y.txt"])?;
    let mut remote = Watched::new(&server, &account, Duration::ZERO);
    let now = pull_into(&account, &remote)?;
    remote.answer = Some(|| RemoteError::NotFound);
    remote.headers_fail = true;
    let mut moved = false;
    assert!(deleted::purge(&account, &remote, 0, now, &mut moved).is_err());
    assert_eq!(remote.headers.load(Ordering::SeqCst), 1, "one asked");
    assert!(moved);
    assert_eq!(
        account
            .lock()
            .map_err(|_| "poisoned")?
            .read(|s| s.list_removed(10))?
            .len(),
        2
    );
    Ok(())
}

/// A soft delete is recorded gone in its own write: a page read before it (the removed row)
/// doesn't bring the item back to Recently deleted.
#[test]
fn a_page_read_before_a_soft_delete_doesnt_bring_it_back() -> common::Res<()> {
    let server = server();
    let (account, files) = removed_now(&server, &["x.txt"])?;
    let remote = Watched::new(&server, &account, Duration::ZERO);
    let now = pull_into(&account, &remote)?;
    let before = header_named(&server, "x.txt");
    assert_eq!(deleted::purge(&account, &remote, 0, now, &mut false)?, 1);
    assert!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .header(files[0])?
            .is_deleted(),
        "a tombstone"
    );
    let mut account = account.lock().map_err(|_| "poisoned")?;
    account.write(|s| feed::apply(s, &before).map(drop))?;
    assert!(account.read(|s| s.list_removed(10))?.is_empty(), "not back");
    Ok(())
}

/// A delete for good answered with a code the client doesn't know: the file stays, in Recently
/// deleted, and the purge fails (it runs again later).
#[test]
fn an_unknown_answer_fails_the_purge() -> common::Res<()> {
    let server = server();
    let x = seed(&server, "x.txt")?;
    remove_on_server(&server, &header_named(&server, "x.txt"));
    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
    let mut remote = Watched::new(&server, &account, Duration::ZERO);
    let now = pull_into(&account, &remote)?;
    remote.answer = Some(|| RemoteError::Fatal("someNewCode".into()));
    assert!(deleted::purge(&account, &remote, TRACE_MS, now, &mut false).is_err());
    let mut db = account.lock().map_err(|_| "poisoned")?;
    assert_eq!(db.read(|s| s.list_removed(10))?.len(), 1);
    assert!(!db.read(|s| s.file_gone(x))?);
    assert!(removed_on_server(&server, "x.txt"));
    Ok(())
}

/// While deleting the items fails, every folder's trash keeps its old entries too (the local
/// copies of what was deleted): emptied by a purge that went through.
#[test]
fn the_trash_waits_for_the_items() -> common::Res<()> {
    let server = server();
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("a");
    let account = Arc::new(Mutex::new(StateDb::open_in_memory()?));
    account.lock().map_err(|_| "poisoned")?.write(|s| {
        s.add_folder(NodeId::new(), &folder.to_string_lossy(), "ma")
            .map(drop)
    })?;
    let mut remote = Watched::new(&server, &account, Duration::ZERO);
    // The first purge starts the trash's clock.
    let now = pull_into(&account, &remote)?;
    deleted::purge(&account, &remote, 0, now, &mut false)?;
    let entry = folder
        .join(".hbsync/trash")
        .join(format!(
            "{}-{}",
            hbsync_types::unix_now() + 1,
            NodeId::new().0
        ))
        .join("f.txt");
    fs::create_dir_all(entry.parent().ok_or("parent")?)?;
    fs::write(&entry, "x")?;
    seed(&server, "y.txt")?;
    remove_on_server(&server, &header_named(&server, "y.txt"));
    let now = pull_into(&account, &remote)?;
    remote.soft_fails = true;
    // Kept less than nothing: the entry is old enough, and so is y.
    assert!(deleted::purge(&account, &remote, -10_000, now, &mut false).is_err());
    assert!(entry.exists(), "the trash waits");
    remote.soft_fails = false;
    deleted::purge(&account, &remote, -10_000, now, &mut false)?;
    assert!(!entry.exists(), "emptied");
    Ok(())
}
