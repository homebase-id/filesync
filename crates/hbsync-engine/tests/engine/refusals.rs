//! Refusals, a full drive, and this device's own writes recorded from their answers (more
//! of the last in the `answers` test binary).

use super::*;

/// An upload the drive refuses as it is isn't sent again while it is the same: not on the next
/// sync, and it leaves no work unfinished; an edit is sent (and here refused again); Sync now
/// sends it once more.
#[test]
fn a_refused_upload_is_sent_again_only_once_it_changes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.remote.faults().refuse_writes = true;
    a.write("doc.txt", "v1")?;
    let report = a.sync()?;
    let refused = |r: &hbsync_engine::SyncReport| {
        r.skipped
            .iter()
            .filter(|(_, why)| matches!(why, hbsync_engine::exec::Skip::Refused(_)))
            .count()
    };
    assert_eq!(refused(&report), 1, "{report:?}");

    let sent = writes_refused(&a);
    assert_eq!(sent, 1);

    // The same: not sent, told as refused, nothing left that another try would do.
    let report = a.sync()?;
    assert_eq!(writes_refused(&a), sent, "{report:?}");
    assert_eq!(report.refused.len(), 1, "{report:?}");
    assert!(!report.unfinished, "{report:?}");
    assert!(report.pending > 0, "still to sync: {report:?}");

    // A change is another upload.
    a.write("doc.txt", "v2")?;
    let report = a.sync()?;
    assert_eq!(writes_refused(&a), sent + 1, "{report:?}");
    assert_eq!(refused(&report), 1, "{report:?}");

    // Taken now, but held back until Sync now (or an hour).
    a.remote.faults().refuse_writes = false;
    a.sync()?;
    assert_eq!(writes_refused(&a), sent + 1);
    a.engine.clear_refused()?;
    let report = a.sync()?;
    assert!(
        report.refused.is_empty() && report.pending == 0,
        "{report:?}"
    );
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["doc.txt"]
    );

    Ok(())
}

/// A refused upload made executable is another upload: sent.
#[cfg(unix)]
#[test]
fn a_refused_upload_made_executable_is_sent_again() -> R {
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.remote.faults().refuse_writes = true;
    a.write("run.sh", "echo")?;
    a.sync()?;
    a.sync()?;
    let sent = writes_refused(&a);
    assert_eq!(sent, 1);
    let path = a.root.join("run.sh");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
    a.sync()?;
    assert_eq!(
        writes_refused(&a),
        sent + 1,
        "the executable bit is another request"
    );
    Ok(())
}

fn writes_refused(c: &Client) -> usize {
    c.remote.faults().refused
}

/// A download whose bytes don't match what the drive says they are is read again once (the
/// drive may have been written meanwhile); a second time at that version it is refused, not
/// downloaded again every sync.
#[test]
fn a_download_that_doesnt_match_twice_is_refused() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("doc.txt", "on the drive")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.remote.faults().corrupt_downloads = true;
    b.sync()?;
    assert_eq!(b.remote.faults().reads, 1);
    let report = b.sync()?;
    assert_eq!(b.remote.faults().reads, 2, "read again once: {report:?}");
    let report = b.sync()?;
    assert_eq!(b.remote.faults().reads, 2, "not read again: {report:?}");
    assert_eq!(report.refused.len(), 1, "{report:?}");
    assert!(!b.root.join("doc.txt").exists());
    // Once the drive gives the right bytes and the user asks.
    b.remote.faults().corrupt_downloads = false;
    b.engine.clear_refused()?;
    b.sync()?;
    assert_eq!(b.read_text("doc.txt")?, "on the drive");
    Ok(())
}

/// Once the drive said it is full, the uploads queued behind the refused one don't send their
/// bytes only to hear so too.
#[test]
fn a_full_drive_stops_the_queued_uploads() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.exec.transfer.workers = 1)?;
    server.lock().map_err(|_| "poisoned")?.set_quota(Some(1000));
    for i in 0..10 {
        a.write(&format!("f{i}.txt"), vec![b'x'; 600])?;
    }
    a.remote.faults().uploads = 0;
    let report = a.sync()?;
    assert!(report.full, "{report:?}");
    assert_eq!(
        a.remote.faults().uploads,
        2,
        "one landed, one refused, none after"
    );
    assert_eq!(report.pending, 9, "{report:?}");
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

/// The feed comes slim first (odin-core #1967): this device's own writes coming back are
/// versions it knows, so its pages are never read whole; its removals reach Recently deleted
/// from the slim rows, with the server's times. Another device, to which they are new, reads
/// them whole.
#[test]
fn own_writes_come_back_slim() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
        .filter_map(|h| h.removed_at())
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
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
        s.removed().map(|h| (h.file_id, h.removed_at())).collect()
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
