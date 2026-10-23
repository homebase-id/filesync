//! Syncing between devices: transfers, conflicts, names, parking, adoption and start-up.

use super::*;

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
        .filter_map(|h| h.removed_at().map(|at| (h.node(), at)))
        .collect();
    assert_eq!(removed.len(), 1, "{removed:?}");
    // And here, the local copy was deleted outright: nothing is kept (sync semantics §7).
    assert!(nothing_kept(&a), "kept");
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

/// A sync that makes a conflict copy and is then refused keeps what it did for the telling:
/// the conflict is told although the sync failed.
#[test]
fn a_conflict_made_before_a_refusal_is_still_told() -> R {
    let server = server();
    let mut a = Client::new(&server, "laptop")?;
    let mut b = Client::new(&server, "desktop")?;
    a.write("notes.txt", "base")?;
    a.sync()?;
    b.sync()?;
    a.write("notes.txt", "from laptop")?;
    b.write("notes.txt", "from desktop!")?;
    a.sync()?;
    b.remote.faults().deny_writes = true;
    assert!(b.sync().is_err(), "refused");
    let partial = b
        .engine
        .take_unreported()
        .ok_or("what the sync did is kept")?;
    assert_eq!(partial.conflicts.len(), 1, "{partial:?}");
    assert!(b.engine.take_unreported().is_none(), "taken once");
    Ok(())
}

/// A change in a folder is planned from the few rows that may clash with its names; a clash
/// is then judged against the whole folder: the numbered name taken already is skipped. (The
/// test kit checks every plan against one made from whole folders.)
#[test]
fn a_clash_in_a_large_folder_takes_a_name_free_in_all_of_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..40 {
        a.write(&format!("big/f{i:02}.txt"), format!("{i}"))?;
    }
    a.write("big/x (conflict 1).txt", "taken")?;
    a.write("big/X.md", "other extension")?;
    a.sync()?;
    b.sync()?;
    a.write("big/X.txt", "from a")?;
    b.write("big/x.txt", "from b")?;
    a.sync()?;
    let report = b.sync()?;
    assert_eq!(report.conflicts.len(), 1, "{report:?}");
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    let tree = b.tree_text("big");
    assert!(
        tree.contains(&"x (conflict 1).txt = taken".to_owned()),
        "{tree:?}"
    );
    let kept = tree
        .iter()
        .filter(|e| e.ends_with("= from a") || e.ends_with("= from b"));
    let mut names: Vec<&str> = kept.map(|e| e.split(" = ").next().unwrap_or("")).collect();
    names.sort_by_key(|n| n.to_lowercase());
    assert_eq!(names.len(), 2, "{tree:?}");
    assert!(
        names[0].eq_ignore_ascii_case("x (conflict 2).txt"),
        "{tree:?}"
    );
    assert!(names[1].eq_ignore_ascii_case("x.txt"), "{tree:?}");
    Ok(())
}

/// Downloads are put in place only once their bytes are durable, together: a group whose
/// barrier fails places none of them, and they come in a later try.
#[test]
fn downloads_wait_for_their_bytes_to_be_durable() -> R {
    let server = server();
    let faults = Arc::new(hbsync_engine::exec::Faults::default());
    let mut a = Client::new(&server, "a")?;
    let failing = Arc::clone(&faults);
    let mut b = Client::with_config(&server, "b", move |c| {
        c.exec.hooks.faults = Arc::clone(&failing);
    })?;
    for i in 0..20 {
        a.write(&format!("d/f{i}"), format!("{i}"))?;
    }
    a.sync()?;
    faults.fail(FailPoint::Barrier, usize::MAX);
    b.sync()?;
    assert!(
        b.entries("d").is_empty(),
        "nothing placed: {:?}",
        b.entries("d")
    );
    faults.fail(FailPoint::Barrier, 1);
    b.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));
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
        exact_names: false,
        settle: None,
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
    let folder = hbsync_engine::FolderCtx::new(dir.path());
    let started = std::time::Instant::now();
    let adopted = hbsync_engine::adopt::adopt(&mut db, &folder)?;
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
            (DeletedLocally, "old.txt".into(), 1, 0)
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

/// A pull that commits between a round's plan and its clearing of the marks the plan found
/// clean (another folder's engine, or an announcement) marks a node again that is dirty
/// already: the clear leaves that mark, so the change it brought is planned.
#[test]
fn a_change_pulled_between_plan_and_clear_is_planned() -> R {
    use hbsync_engine::testkit::AfterPlan;

    type Then = Box<dyn FnOnce() + Send>;
    // f.txt's header on the drive, live.
    fn f_txt(server: &Mutex<hbsync_remote::fake::FakeServer>) -> RemoteHeader {
        let server = server.lock().expect("server");
        let live = server.live().find(|h| {
            matches!(&h.body, hbsync_remote::RemoteBody::Live { content, .. }
                if content.name == "f.txt")
        });
        live.cloned().expect("f.txt")
    }

    let server = server();
    // What runs once, after the next plan.
    let then: Arc<Mutex<Option<Then>>> = Arc::default();
    let hook = {
        let then = Arc::clone(&then);
        AfterPlan(Arc::new(move || {
            let then = then.lock().ok().and_then(|mut t| t.take());
            if let Some(then) = then {
                then();
            }
        }))
    };
    let mut a = Client::with_config(&server, "a", move |cfg| {
        cfg.after_plan = Some(hook.clone());
        // The pull lands between the narrow plan and the whole one too.
        cfg.check_narrow = false;
    })?;
    a.write("keep.txt", "k")?;
    a.write("f.txt", "one")?;
    a.sync()?;
    // f.txt dirty, with nothing to do: the plan finds it clean.
    let node = f_txt(&server).node().ok_or("removed")?;
    a.engine.db().write(|s| s.folder().mark_dirty(node))?;
    // Then another computer moves f.txt to Recently deleted, and this account pulls that.
    let (b, drive, at) = (
        FakeRemote::new(server.clone()),
        Arc::clone(a.engine.drive()),
        f_txt(&server),
    );
    *then.lock().map_err(|_| "poisoned")? = Some(Box::new(move || {
        let target = Target {
            at: at.at(),
            token: at.token.clone(),
        };
        b.set_removed(&[target], true).expect("b removes it");
        drive.pull(None).expect("the pull");
    }));
    a.sync_unasked()?;
    assert!(
        then.lock().map_err(|_| "poisoned")?.is_none(),
        "the pull came"
    );
    assert!(
        a.engine.db().read(|s| s.is_dirty(node))?,
        "the pull's mark stays"
    );
    // Planned at the next sweep of the dirty set.
    a.sync_unasked()?;
    assert!(!a.root.join("f.txt").exists(), "{:?}", a.tree_text(""));
    Ok(())
}
