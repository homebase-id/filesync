//! Recently deleted: removals, restores, emptying, the purge, and orphans.

use super::*;

/// A folder's removal that fails holds back the removals of what is in it, so nothing is
/// removed before its folder (node schema, Recently deleted): once the folder goes, what was in
/// it goes with it, and is deleted with it.
#[test]
fn a_folder_that_isnt_removed_keeps_what_is_in_it() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    a.write("Photos/x.jpg", "x")?;
    a.write("Photos/2024/y.jpg", "y")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    // The folder's removal keeps failing during this sync.
    a.remote.faults().fail_writes_named = Some("Photos".into());
    let report = a.sync()?;
    // What is in it waits for the next plan: not given up as failed.
    assert!(report.failed.is_empty(), "{report:?}");
    let waiting = report
        .skipped
        .iter()
        .filter(|(_, why)| why.as_str() == "its folder wasn't removed")
        .count();
    assert_eq!(waiting, 3, "{report:?}");
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
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
    assert_eq!(b.engine.drive().purge(0, Going::UNWATCHED)?, 0);
    assert_eq!(count(), (1, 4));
    let photos = b
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    assert!(!photos.stays, "nothing live holds it any more");
    // Their own removal kept long enough: all of it goes.
    assert_eq!(b.engine.drive().purge(0, Going::UNWATCHED)?, 4);
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
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
    // Another device, which never synced, lists it and empties it.
    let mut b = Client::new(&server, "b")?;
    b.engine.drive().catch_up(None)?;
    let listed = b.listed()?;
    assert_eq!(
        b.engine
            .drive()
            .empty(&[photos.node], &listed, Going::UNWATCHED)?,
        1,
        "x.txt"
    );
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

/// Emptying one item takes only what the user was shown: a removal recorded inside it after the
/// listing (its delete finished meanwhile, or another device deleted something in it) stays in
/// Recently deleted, and so does the folder holding it.
#[test]
fn emptying_an_item_takes_only_what_was_listed() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    a.write("Photos/a.txt", "a")?;
    a.write("Photos/x.txt", "x")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Photos"))?;
    // The folder and a.txt go; x.txt's removal fails, and a stops there.
    a.remote.faults().fail_writes_named = Some("x.txt".into());
    a.sync()?;
    let photos = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "Photos")
        .ok_or("Photos not listed")?;
    let listed = a.listed()?;
    // Then x.txt's removal goes through: recorded after the listing.
    a.remote.faults().fail_writes_named = None;
    a.sync()?;
    assert!(
        a.recently_deleted(10)?
            .iter()
            .any(|i| i.path == "Photos/x.txt"),
        "x.txt is in Recently deleted now"
    );
    assert_eq!(
        a.engine
            .drive()
            .empty(&[photos.node], &listed, Going::UNWATCHED)?,
        1,
        "a.txt only"
    );
    let left: Vec<String> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert_eq!(
        left,
        ["Photos/x.txt", "Photos"],
        "x.txt and its folder stay"
    );
    // Listed again, it all goes.
    let listed = a.listed()?;
    assert_eq!(
        a.engine
            .drive()
            .empty(&[photos.node], &listed, Going::UNWATCHED)?,
        2
    );
    assert!(a.recently_deleted(10)?.is_empty());
    // A listing this DB never gave is refused.
    let other = hbsync_engine::deleted::Listed {
        db: "another".into(),
        seq: listed.seq,
    };
    assert!(
        a.engine
            .drive()
            .empty(&[photos.node], &other, Going::UNWATCHED)
            .is_err()
    );
    Ok(())
}

/// A folder emptied from Recently deleted takes with it what was deleted from it before, listed
/// on its own (a sub-folder deleted first): all removed, so it may go. The listing counts that
/// as in it, and nothing of it stays.
#[test]
fn emptying_a_folder_takes_what_was_deleted_from_it_before() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
    assert_eq!(
        a.engine
            .drive()
            .empty(&[run], &summary.listed, Going::UNWATCHED)?,
        6
    );
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

/// Another app deletes a folder for good outright (a hard delete), leaving the files in it
/// live with no folder. A device that never had the folder puts them into Recovered files on
/// the drive, made there at the top beside a plain folder of the same name, which keeps its
/// name; another device finds them there and joins the folder. Nothing lost.
#[test]
fn files_whose_folder_is_gone_for_good_are_recovered() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("docs/report.txt", "r")?;
    a.write("docs/sub/deep.txt", "d")?;
    a.write("Recovered files/mine.txt", "m")?;
    a.sync()?;
    {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        let docs = s
            .live()
            .find(|h| h.state().is_some_and(|st| st.name == "docs"))
            .map(|h| h.at())
            .ok_or("docs")?;
        s.hard_delete(docs.file_id, docs.version)?;
    }
    let mut b = Client::new(&server, "b")?;
    let report = b.sync()?;
    let orphans = report
        .conflicts
        .iter()
        .filter(|c| matches!(c, hbsync_engine::Conflict::Orphan { .. }))
        .count();
    assert_eq!(orphans, 2, "docs/report.txt and docs/sub: {report:?}");
    let recovered = [
        "Recovered files (conflict 1)/",
        "Recovered files (conflict 1)/report.txt = r",
        "Recovered files (conflict 1)/sub/",
        "Recovered files (conflict 1)/sub/deep.txt = d",
        "Recovered files/",
        "Recovered files/mine.txt = m",
    ];
    assert_eq!(b.tree_text(""), recovered);
    let s = server.lock().map_err(|_| "poisoned")?;
    let folder = s
        .live()
        .find(|h| h.node() == Some(NodeId::RECOVERED))
        .and_then(|h| h.state())
        .ok_or("Recovered files on the drive")?;
    assert_eq!(folder.parent, NodeId::ROOT);
    drop(s);
    let mut c = Client::new(&server, "c")?;
    let report = c.sync()?;
    assert!(report.conflicts.is_empty(), "{report:?}");
    assert_eq!(c.tree_text(""), recovered);
    Ok(())
}

/// Another app deletes folders `names` for good (a hard delete), leaving what is in them live,
/// and `c` hears it from the notification, as the daemon does: gone from its mirror at once.
fn deleted_for_good_announced(
    server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>,
    c: &Client,
    names: &[&str],
) -> R {
    let mut refs = Vec::new();
    for name in names {
        let at = {
            let s = server.lock().map_err(|_| "poisoned")?;
            s.live()
                .find(|h| h.state().is_some_and(|st| st.name == *name))
                .map(|h| h.at())
                .ok_or("no such folder")?
        };
        let header = c.remote.header(at.file_id)?;
        let mut s = server.lock().map_err(|_| "poisoned")?;
        s.hard_delete(at.file_id, at.version)?;
        refs.push(hbsync_remote::ChangeRef::of(&header).tombstone());
    }
    c.engine.drive().announced(&refs)?;
    Ok(())
}

/// The names of what is live on the server, sorted; and whether anything is in Recently
/// deleted.
fn server_names(
    server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>,
) -> Result<(Vec<String>, bool), Box<dyn std::error::Error>> {
    let s = server.lock().map_err(|_| "poisoned")?;
    let mut live: Vec<String> = s
        .live()
        .filter_map(|h| h.state().map(|st| st.name.clone()))
        .collect();
    live.sort();
    Ok((live, s.removed().next().is_some()))
}

/// a syncs only its run folder (`/*` but `!/run/`), as the live tests do; b's folders x/d/f0..f3
/// are on the drive. Another app deletes x and d for good, and b, not hearing of it, makes
/// x/d/f9. What was in d is an orphan with nowhere a can put it: Recovered files and the top
/// are places it doesn't sync (§10). It leaves them to the devices that do, and plans alike in
/// its narrow view and the whole one. Brought in at the top, they read as items a stopped
/// syncing, and f9 left the drive (b's, in Recently deleted).
#[test]
fn an_orphan_where_a_device_syncs_nothing_stays_on_the_drive() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", ignoring("/*\n!/run/\n"))?;
    let mut b = Client::new(&server, "b")?;
    a.write("run/readme.txt", "r")?;
    a.sync()?;
    for i in 0..4 {
        b.write(&format!("x/d/f{i}/keep.txt"), "k")?;
    }
    b.sync()?;
    a.sync()?;
    deleted_for_good_announced(&server, &a, &["x", "d"])?;
    fs::create_dir_all(b.root.join("x/d/f9"))?;
    b.sync()?;
    let report = a.sync()?;
    assert_eq!(report.ops_done, 0, "{report:?}");
    a.sync()?;
    assert_eq!(a.tree_text(""), ["run/", "run/readme.txt = r"]);
    let (live, removed) = server_names(&server)?;
    let mut expected = vec!["f0", "f1", "f2", "f3", "f9", "readme.txt", "run"];
    expected.extend(["keep.txt"; 4]);
    expected.sort();
    assert_eq!(live, expected);
    assert!(!removed, "nothing in Recently deleted");
    Ok(())
}

/// As above, with a syncing the top (it ignores x/ only): every folder of d is an orphan when
/// d goes for good, dirty or not, and a recovers them all into Recovered files, but for what it
/// ignores in them (`*.log`, edited meanwhile: planned with them), which stays on the drive,
/// untouched. Before, a recovered none until one changed: only the folder that went was
/// planned again.
#[test]
fn every_orphan_of_a_folder_deleted_for_good_is_recovered() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", ignoring("x/\n*.log\n"))?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..3 {
        fs::create_dir_all(b.root.join(format!("x/d/f{i}")))?;
    }
    b.write("x/d/f0/a.txt", "t")?;
    b.write("x/d/f0/a.log", "l")?;
    b.sync()?;
    a.sync()?;
    assert!(a.tree_text("").is_empty());
    deleted_for_good_announced(&server, &a, &["x", "d"])?;
    // b, not hearing of it, edits a.log: planned in the same plan as its folder's recovery.
    b.write("x/d/f0/a.log", "l2")?;
    b.sync()?;
    let report = a.sync()?;
    assert_eq!(report.conflicts.len(), 3, "f0, f1, f2: {report:?}");
    assert_eq!(
        a.tree_text(""),
        [
            "Recovered files/",
            "Recovered files/f0/",
            "Recovered files/f0/a.txt = t",
            "Recovered files/f1/",
            "Recovered files/f2/",
        ]
    );
    a.sync()?;
    let (live, removed) = server_names(&server)?;
    assert_eq!(
        live,
        ["Recovered files", "a.log", "a.txt", "f0", "f1", "f2"],
        "a.log stays"
    );
    assert!(!removed, "nothing in Recently deleted");
    Ok(())
}

/// As above, with Recovered files in Recently deleted (b made it, then deleted it): it comes
/// back for the orphans, and what a ignores in them (`*.log`) still stays on the drive,
/// untouched, though Recovered files' place is known only once it is back.
#[test]
fn an_orphan_into_recovered_files_coming_back_leaves_what_is_ignored() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", ignoring("x/\n*.log\n"))?;
    let mut b = Client::new(&server, "b")?;
    b.write("Recovered files/old.txt", "o")?;
    b.write("x/d/f0/a.txt", "t")?;
    b.write("x/d/f0/a.log", "l")?;
    b.sync()?;
    fs::remove_dir_all(b.root.join("Recovered files"))?;
    b.sync()?;
    a.sync()?;
    assert!(a.tree_text("").is_empty());
    deleted_for_good_announced(&server, &a, &["x", "d"])?;
    a.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "Recovered files/",
            "Recovered files/f0/",
            "Recovered files/f0/a.txt = t"
        ]
    );
    let (live, _) = server_names(&server)?;
    assert_eq!(
        live,
        ["Recovered files", "a.log", "a.txt", "f0"],
        "a.log stays"
    );
    Ok(())
}

/// The server, a, b and P's node id ([`written_into_a_removed_ignored_folder`]).
type WrittenInto = (
    Arc<Mutex<hbsync_remote::fake::FakeServer>>,
    Client,
    Client,
    NodeId,
);

/// a ignores x/; b syncs x/P/f.txt; another app removes P (to Recently deleted, f.txt taken
/// along, live), then makes x/P/new in it. With P's node id.
fn written_into_a_removed_ignored_folder() -> Result<WrittenInto, Box<dyn std::error::Error>> {
    let server = server();
    let mut a = Client::with_config(&server, "a", ignoring("x/\n"))?;
    let mut b = Client::new(&server, "b")?;
    b.write("keep.txt", "k")?;
    b.write("x/P/f.txt", "f")?;
    b.sync()?;
    a.sync()?;
    remove_on_drive(&server, &|name| name == "P")?;
    let p = {
        let s = server.lock().map_err(|_| "poisoned")?;
        s.removed().find_map(|h| h.node()).ok_or("P removed")?
    };
    FakeRemote::new(server.clone()).create(&hbsync_remote::contract::folder("new", p), None)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok((server, a, b, p))
}

/// a ignores x/; b syncs x/P/f.txt. Another app removes P (to Recently deleted, f.txt taken
/// along, live) and then makes x/P/new in it: a live child, which keeps P (§6.3). For a, new
/// is in a folder it doesn't sync, not an orphan: it leaves it. It moved new into Recovered
/// files on the drive, and P stayed deleted for b, f.txt with it. b brings P back with both.
#[test]
fn what_is_written_into_a_removed_folder_a_device_ignores_is_left() -> R {
    let (server, mut a, mut b, _) = written_into_a_removed_ignored_folder()?;
    let report = a.sync()?;
    assert_eq!(report.ops_done, 0, "{report:?}");
    assert_eq!(a.tree_text(""), ["keep.txt = k"]);
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["keep.txt = k", "x/", "x/P/", "x/P/f.txt = f", "x/P/new/"]
    );
    let (_, removed) = server_names(&server)?;
    assert!(!removed, "nothing in Recently deleted");
    Ok(())
}

/// As above, with no computer syncing x/ (b made x/P/f.txt and is gone): new stays in P, which
/// stays in Recently deleted. The purge (here as a) removes what the delete took along and
/// deletes it for good, never P: it holds a live node, written after its removal (§10).
#[test]
fn what_no_device_syncs_stays_in_its_removed_folder_through_the_purge() -> R {
    let (server, mut a, _b, p) = written_into_a_removed_ignored_folder()?;
    a.sync()?;
    // The purge, twice, a day apart: the first removes what the delete took along, the second
    // deletes it for good.
    for _ in 0..2 {
        a.engine.drive().purge(0, Going::UNWATCHED)?;
        a.sync()?;
        server
            .lock()
            .map_err(|_| "poisoned")?
            .advance(24 * 60 * 60 * 1000);
    }
    assert_eq!(a.tree_text(""), ["keep.txt = k"]);
    let s = server.lock().map_err(|_| "poisoned")?;
    let mut live = s.live_names();
    live.sort();
    assert_eq!(
        live,
        ["keep.txt", "new", "x"],
        "f.txt went for good, new stays"
    );
    let removed: Vec<_> = s.removed().filter_map(|h| h.node()).collect();
    assert_eq!(removed, [p], "P stays in Recently deleted, holding new");
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
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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

/// Recently deleted by deletes: a folder with what it took along is one entry with its count;
/// any of its items can still be found, by node or (all of them) to pick by place.
#[test]
fn recently_deleted_lists_deletes_with_counts_and_finds_items() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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
    let (found, _) = reader.find_deleted(&[child, folder])?;
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
    assert_eq!(a.engine.drive().purge(0, Going::UNWATCHED)?, 1);
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
    let guard = |cfg: &mut hbsync_engine::EngineConfig| cfg.guard.min = 2;
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
    assert!(report.held.is_empty(), "a follows b's delete: {report:?}");
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

    // b, which confirmed the first delete of these nodes, deletes them again: held anew.
    fs::remove_dir_all(b.root.join("d"))?;
    let report = b.sync()?;
    assert!(
        !report.held.is_empty(),
        "b holds the second delete: {report:?}"
    );
    // Unconfirmed, nothing went: a still has it.
    a.sync()?;
    assert!(a.root.join("d/f3.txt").exists());
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
    assert_eq!(a.engine.drive().empty_listed(&listed, Going::UNWATCHED)?, 1);
    let left: Vec<_> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|i| i.path)
        .collect();
    assert_eq!(left, ["keep.txt"]);
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
    let mut b = Client::with_config(&server, "b", |c| c.guard.allow = true)?;
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

/// A file removed here that the drive answers is deleted for good (a tombstone the feed doesn't
/// show): it is known gone here, as any file deleted for good, so no page read before brings
/// it back.
#[test]
fn a_removal_answered_gone_for_good_keeps_the_file_gone() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    let x = {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        let h = s
            .live()
            .find(|h| h.state().is_some_and(|st| st.name == "x.txt"))
            .ok_or("no x")?
            .clone();
        s.delete(h.file_id, h.version)?;
        s.unlist(h.file_id);
        h.file_id
    };
    fs::remove_file(a.root.join("x.txt"))?;
    a.sync_unasked()?;
    assert!(a.engine.db().read(|s| s.file_gone(x))?, "known gone");
    assert_eq!(a.tree_text(""), ["keep.txt = k"]);
    Ok(())
}

/// A delete taken back comes down again: the other device kept nothing of what it deleted
/// (sync semantics §7), so every file of it is downloaded.
#[test]
fn a_restore_downloads_again() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let (x, y, z) = ("x".repeat(1000), "y".repeat(2000), "z".repeat(3000));
    a.write("run/x.txt", &x)?;
    a.write("run/sub/y.txt", &y)?;
    a.write("run/z.txt", &z)?;
    a.sync()?;
    let mut b = Client::with_config(&server, "b", |c| c.guard.allow = true)?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("run"))?;
    a.sync()?;
    b.sync()?;
    assert!(b.read_text("run/x.txt").is_err(), "deleted on b");
    assert!(nothing_kept(&b), "kept");
    let run = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "run")
        .ok_or("run not listed")?;
    assert_eq!(a.engine.drive().restore(&[run.node])?, 5);
    let served = b.remote.faults().served;
    b.sync()?;
    assert_eq!(b.remote.faults().served - served, 6000, "all of it again");
    assert_eq!(b.read_text("run/x.txt")?, x);
    assert_eq!(b.read_text("run/sub/y.txt")?, y);
    assert_eq!(b.read_text("run/z.txt")?, z);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// Another writer's `.HBSYNC` at the top of the drive, and a `.hbsync` at the top of a role
/// folder (each computer's state folder there, in any case), deleted into Recently deleted,
/// the role folder with it: restored, the role folder first, each comes back under another
/// name (`.HBSYNC (restored)`), never as the state folder no computer syncs; here too.
#[test]
fn a_restore_numbers_around_the_state_folder_name_at_a_sync_roots_top() -> R {
    use hbsync_remote::NodeMeta;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeKind, Role};

    let server = server();
    let mut a = Client::new(&server, "a")?;
    let docs = Role::Documents.node();
    let folder = |node, parent, name: &str| NodeMeta {
        node,
        parent,
        kind: NodeKind::Folder,
        content: NodeContent::folder(name),
        removed: false,
    };
    let (top, in_docs) = (NodeId::new(), NodeId::new());
    a.sync()?; // reading the feed from here: removals come in as such
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        // Into Recently deleted (`archivalStatus` 2), as a client removes them: Documents
        // too, after what is in it.
        let names = [
            (top, NodeId::ROOT, ".HBSYNC"),
            (in_docs, docs, ".hbsync"),
            (docs, NodeId::ROOT, "Documents"),
        ];
        let made: Vec<_> = names
            .iter()
            .map(|(node, parent, name)| srv.create(&folder(*node, *parent, name), None))
            .collect::<Result<_, _>>()?;
        for ((node, parent, name), at) in names.iter().zip(made) {
            let removed = NodeMeta {
                removed: true,
                ..folder(*node, *parent, name)
            };
            srv.update(at.file_id, at.version, &removed, None)?;
        }
    }
    a.sync()?;
    // Documents comes back with what is in it, first.
    assert_eq!(a.engine.drive().restore(&[top, in_docs])?, 3);
    a.sync()?;
    assert!(
        a.root.join(".HBSYNC (restored)").is_dir(),
        "{:?}",
        a.tree_text("")
    );
    let srv = server.lock().map_err(|_| "poisoned")?;
    let names: Vec<(NodeId, String)> = srv
        .live()
        .filter_map(|h| match &h.body {
            hbsync_remote::RemoteBody::Live { node, content, .. } => {
                Some((*node, content.name.clone()))
            }
            hbsync_remote::RemoteBody::Deleted => None,
        })
        .collect();
    for (node, want) in [
        (top, ".HBSYNC (restored)"),
        (in_docs, ".hbsync (restored)"),
        (docs, "Documents"),
    ] {
        let name = names
            .iter()
            .find(|(n, _)| *n == node)
            .map(|(_, name)| name.as_str());
        assert_eq!(name, Some(want), "{names:?}");
    }
    Ok(())
}

/// Another writer gave Recovered files' id to a file (a row no device reads, #170): no device
/// can make Recovered files on the drive. An orphan (a file another writer put in a file) goes
/// to the top instead, at once; it waited for good, its move tried again at every sync, and
/// the sync left unfinished, a retry every few minutes for ever.
#[test]
fn an_orphan_goes_to_the_top_when_recovered_files_id_is_taken() -> R {
    use hbsync_remote::{NodeMeta, RemoteBody, Upload};
    use hbsync_types::NodeKind;
    use hbsync_types::schema::NodeContent;

    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x", "x1")?;
    a.write("keep", "keep")?;
    a.sync()?;
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let bytes = b"rf".to_vec();
        let hash = hbsync_fileio::hash_bytes(&bytes, hbsync_fileio::HashAlgorithm::Blake3);
        let taken = NodeMeta {
            node: NodeId::RECOVERED,
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file("fixed", 1, 2, false),
            removed: false,
        };
        let mut reader: &[u8] = &bytes;
        let upload = Upload {
            hash: hbsync_remote::Planned::Known(hash),
            size: 2,
            reader: &mut reader,
            pace: None,
        };
        srv.create(&taken, Some(upload))?;
        let headers: Vec<_> = srv.live().cloned().collect();
        let of = |name: &str| {
            headers.iter().find_map(|h| match &h.body {
                RemoteBody::Live { node, content, .. } if content.name == name => {
                    Some((*node, h.clone()))
                }
                _ => None,
            })
        };
        let (keep, _) = of("keep").ok_or("keep")?;
        let (x, h) = of("x").ok_or("x")?;
        let RemoteBody::Live { content, .. } = &h.body else {
            return Err("x".into());
        };
        let into_file = NodeMeta {
            node: x,
            parent: keep,
            kind: NodeKind::File,
            content: content.clone(),
            removed: false,
        };
        srv.update(h.file_id, h.version, &into_file, None)?;
    }
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert!(!report.unfinished, "{report:?}");
    assert_eq!(a.read_text("x")?, "x1", "{:?}", a.tree_text(""));
    let again = a.engine.sync("2026-10-08", true)?;
    assert_eq!(again.ops_done, 0, "{again:?}");
    assert!(again.failed.is_empty(), "{:?}", again.failed);
    Ok(())
}

/// A file being written is left unhashed (sync semantics §6.3) and holds a delete of its
/// folder from the drive. Once it is quiet the observer's own look reads it, with no event to
/// bring it, and the edit keeps the folder: it comes back on the drive with it.
#[test]
fn a_file_left_while_it_is_written_ends_its_wait_once_quiet() -> R {
    use std::time::{Duration, Instant};

    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("p/big", "v1")?;
    a.sync()?;
    b.sync()?;
    let mut opts = a.engine.scan_options().clone();
    opts.settle = Some(hbsync_local::Settle {
        from_size: 1,
        quiet: Duration::from_secs(1),
    });
    let mut observer = hbsync_local::LocalObserver::start(&a.root, opts);
    if !observer.has_watcher() {
        return Ok(());
    }
    a.engine.start_up(&mut observer, "2026-10-08")?;
    fs::remove_dir_all(b.root.join("p"))?;
    b.sync()?;

    // A batch leaves it unhashed.
    a.write("p/big", "v2, being written")?;
    let until = Instant::now() + Duration::from_secs(10);
    while !a
        .engine
        .observe(&mut observer, Duration::from_millis(100))?
        .is_some_and(|r| !r.young.is_empty())
    {
        assert!(Instant::now() < until, "never left");
    }
    // Written to again since: the round doesn't read it either, and the delete waits.
    a.write("p/big", "v2, written")?;
    let report = a.engine.sync("2026-10-08", true)?;
    assert!(
        report.waiting.iter().any(|(p, _)| p == "p/big"),
        "{report:?}"
    );
    assert!(a.root.join("p/big").exists());

    // No more writes: read by the observer once quiet.
    let mut hashed = 0;
    while hashed == 0 {
        assert!(Instant::now() < until, "never read");
        if let Some(r) = a
            .engine
            .observe(&mut observer, Duration::from_millis(100))?
        {
            hashed += r.hashed;
        }
    }
    a.engine.sync("2026-10-08", true)?;
    b.sync()?;
    assert_eq!(b.read_text("p/big")?, "v2, written");
    assert_eq!(a.read_text("p/big")?, "v2, written");
    Ok(())
}

/// Two items of one name in one folder restored together come back as when restored one after
/// the other: the first asked for under its name, the other under a free restored one.
#[test]
fn two_items_of_one_name_restored_together_both_come_back() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    for v in ["one", "two"] {
        a.write("x.txt", v)?;
        a.sync()?;
        fs::remove_file(a.root.join("x.txt"))?;
        a.sync()?;
    }
    let nodes: Vec<NodeId> = a.recently_deleted(10)?.iter().map(|d| d.node).collect();
    assert_eq!(nodes.len(), 2, "{nodes:?}");
    assert_eq!(a.engine.drive().restore(&nodes)?, 2);
    a.sync()?;
    let tree = a.tree_text("");
    let mut texts: Vec<&str> = tree
        .iter()
        .map(|l| l.split(" = ").nth(1).unwrap_or_default())
        .collect();
    texts.sort();
    assert_eq!(texts, ["one", "two"], "{tree:?}");
    // The newest first, as listed: it keeps the name.
    assert!(tree.iter().any(|l| l == "x.txt = two"), "{tree:?}");
    Ok(())
}

/// b keeps what it ignores in a folder a deleted, so its delete moves the rest one item at a
/// time; one of them can't move (its folder is read-only). What did move left its base with
/// it: a restore made meanwhile won't read as b deleting it.
#[cfg(unix)]
#[test]
fn a_delete_that_stops_half_way_drops_the_bases_of_what_went() -> R {
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        ignoring("*.log\n")(c);
        c.guard.allow = true;
    })?;
    a.write("proj/0.txt", "0")?;
    a.write("proj/sub/b.txt", "b")?;
    a.sync()?;
    b.sync()?;
    b.write("proj/sub/k.log", "kept")?;
    let path = hbsync_platform::RelPath::new("proj/0.txt")?;
    let moved = b
        .engine
        .db()
        .read(|s| s.get_local(&path))?
        .ok_or("0.txt")?
        .node;
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    let sub = b.root.join("proj/sub");
    fs::set_permissions(&sub, fs::Permissions::from_mode(0o555))?;
    let _ = b.sync();
    fs::set_permissions(&sub, fs::Permissions::from_mode(0o755))?;
    assert!(!b.root.join("proj/0.txt").exists(), "deleted");
    assert!(b.root.join("proj/sub/b.txt").exists(), "couldn't move");
    assert!(
        b.engine.db().read(|s| s.get_base(moved))?.is_none(),
        "its base stayed"
    );
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["proj/", "proj/sub/", "proj/sub/k.log = kept"]
    );
    Ok(())
}
