//! Crashes and power cuts at every step: what recovery finds and finishes.

use super::*;

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
    let parked = hbsync_local::layout::parked_name(node);
    fs::rename(a.root.join("doc.txt"), a.root.join(&parked))?;
    let (from, to) = (
        hbsync_platform::RelPath::new("doc.txt")?,
        hbsync_platform::RelPath::new(&parked)?,
    );
    a.engine.db().write(|s| {
        s.folder().move_local_subtree(&from, &to)?;
        s.folder().put_parked(node, &from)
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
        c.exec.hooks.faults = Arc::clone(&failing);
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
/// place there and moves the old one aside, from the folder it was in. That folder is
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
        c.exec.hooks.crash_at = Some(CrashPoint::AfterParkRename)
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
    b.restart_with(remote, |c| c.exec.hooks.crash_at = None)?;
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
        c.exec.hooks.crash_at = Some(CrashPoint::AfterForkRename)
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
    b.restart_with(remote, |c| c.exec.hooks.crash_at = None)?;
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
    let to =
        hbsync_platform::RelPath::new(&format!("A/{}", hbsync_local::layout::parked_name(node)))?;
    fs::rename(from.to_native(&a.root), to.to_native(&a.root))?;
    a.engine.db().write(|s| {
        s.folder().move_local_subtree(&from, &to)?;
        s.folder().put_parked(node, &from)
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
        .write(|s| s.folder().put_fork(node, copy, &to))?;
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
                let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
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

/// A restore coming down, with a crash or a power cut at any of the sync's commits: the next
/// sync finishes it, every file whole and in place.
#[test]
fn a_crash_while_a_restore_comes_down_loses_nothing() -> R {
    for at in 0..8 {
        for before in [true, false] {
            for kept in [common::Kept::All, common::Kept::Durable(None)] {
                let case = format!("at {at}, before {before}, {kept:?}");
                let server = server();
                let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
                for i in 0..4 {
                    a.write(&format!("F/f{i}.txt"), format!("{i}").repeat(100))?;
                }
                a.write("F/G/g.txt", "g".repeat(100))?;
                a.sync()?;
                let mut b = Client::with_config(&server, "b", |c| c.guard.allow = true)?;
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

/// b's delete of a file a deleted is cut short by a crash at `at`: right after the file went
/// aside (before it was checked), or after it was unlinked (before the commit); with
/// `restored`, a restores the file before b runs again. Returns b's tree and the drive's live
/// names once both synced, and checks that b keeps nothing of it.
#[allow(clippy::type_complexity)]
fn crash_in_a_delete(
    at: CrashPoint,
    restored: bool,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", move |c| {
        c.guard.allow = true;
        c.exec.hooks.crash_at = Some(at);
    })?;
    a.write("x", "x")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("x"))?;
    a.sync()?;
    assert!(b.sync().is_err(), "b stops in the delete");
    assert!(!b.root.join("x").exists(), "aside, or gone");
    if restored {
        let x = a
            .recently_deleted(10)?
            .into_iter()
            .find(|i| i.path == "x")
            .ok_or("x")?;
        assert_eq!(a.engine.drive().restore(&[x.node])?, 1);
    }
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.crash_at = None)?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert!(nothing_kept(&b), "kept");
    let mut names = server.lock().map_err(|_| "poisoned")?.live_names();
    names.sort();
    Ok((b.tree_text(""), names))
}

/// A crash anywhere in a delete loses it not: what is aside goes back at the next sync and is
/// deleted again; what was unlinked stays so.
#[test]
fn a_crash_in_a_delete_keeps_the_delete() -> R {
    for at in [CrashPoint::AfterAside, CrashPoint::AfterUnlink] {
        let (tree, names) = crash_in_a_delete(at, false)?;
        assert!(
            tree.is_empty() && names.is_empty(),
            "{at:?}: {tree:?} {names:?}"
        );
    }
    Ok(())
}

/// Nor a restore made before the next run, where the crash left the file aside: recovery finds
/// it the synced version and deletes it (the delete the run was making), its base with it; the
/// restore stands, and the file comes down again.
#[test]
fn a_crash_with_a_delete_aside_keeps_a_restore() -> R {
    let (tree, names) = crash_in_a_delete(CrashPoint::AfterAside, true)?;
    assert_eq!(tree, ["x = x"]);
    assert_eq!(names, ["x"]);
    Ok(())
}

/// Where the crash came after the unlink, it doesn't yet: the scan reads the missing item
/// against the base the lost commit would have dropped, as a local delete, and the plan removes
/// the file again, back to Recently deleted (nothing lost). Accepted for now (#171, Q4): a
/// write-ahead record of the run's deletes would close it.
#[test]
#[ignore = "known limit: a delete's lost commit undoes a restore made before the next run"]
fn a_crash_after_an_unlink_keeps_a_restore() -> R {
    let (tree, names) = crash_in_a_delete(CrashPoint::AfterUnlink, true)?;
    assert_eq!(tree, ["x = x"]);
    assert_eq!(names, ["x"]);
    Ok(())
}

/// An app that had x open writes to it after b moved it aside to delete it (a deleted it): the
/// check finds it changed and puts it back, and the edit wins: x comes back on the drive with
/// its new bytes. Unlinked, the edit was lost.
#[test]
fn an_edit_while_a_delete_is_aside_is_kept() -> R {
    use hbsync_engine::exec::Aside;
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.guard.allow = true;
        c.exec.hooks.aside = Some(Aside(Arc::new(|at| {
            if at.is_file() {
                let _ = fs::write(at, "edited aside, longer");
            }
        })));
    })?;
    a.write("x", "x")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("x"))?;
    a.sync()?;
    b.sync()?;
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.aside = None)?;
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["x = edited aside, longer"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert!(nothing_kept(&b), "kept");
    Ok(())
}

/// On a volume whose mtimes can't prove the bytes (FAT, HFS+, ext3: a tick of a second or
/// more), an edit of the same size that keeps the mtime is caught by the hash: put back, the
/// edit wins.
#[test]
fn a_same_size_edit_aside_is_caught_by_the_hash_on_a_coarse_volume() -> R {
    use hbsync_engine::exec::Aside;
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.guard.allow = true;
        let _ = c.folder.mtime_proves_bytes.set(false);
        c.exec.hooks.aside = Some(Aside(Arc::new(|at| {
            let Ok(meta) = hbsync_platform::stat(at) else {
                return;
            };
            let _ = fs::write(at, "X");
            let _ = hbsync_platform::ops::set_mtime(at, meta.mtime_ns);
        })));
    })?;
    a.write("x", "x")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("x"))?;
    a.sync()?;
    b.sync()?;
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.aside = None)?;
    b.sync()?;
    a.sync()?;
    assert_eq!(b.tree_text(""), ["x = X"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A crash leaves b's delete of d/x aside, and the user then deletes folder d on b. The next
/// sync decides what is aside by what it is: still the synced x, it goes (the drive's delete
/// stands) though its folder is gone; it isn't brought back at the top.
#[test]
fn what_is_aside_as_synced_goes_though_its_folder_went() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.guard.allow = true;
        c.exec.hooks.crash_at = Some(CrashPoint::AfterAside);
    })?;
    a.write("d/x", "x")?;
    a.write("d/y", "y")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("d/x"))?;
    a.sync()?;
    assert!(b.sync().is_err(), "b stops with x aside");
    fs::remove_dir_all(b.root.join("d"))?;
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.crash_at = None)?;
    b.sync()?;
    a.sync()?;
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert!(nothing_kept(&b), "kept");
    Ok(())
}

/// b parks its old b (the drive deleted it) for the new b a made in its place, then moves it
/// aside to delete it; an app writes to it there. Put back, it takes a real name, never the
/// temporary one: the edit wins, and both go up.
#[test]
fn a_parked_item_put_back_takes_a_real_name() -> R {
    use hbsync_engine::exec::Aside;
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.guard.allow = true;
        c.exec.hooks.aside = Some(Aside(Arc::new(|at| {
            if at.is_file() {
                let _ = fs::write(at, "edited aside");
            }
        })));
    })?;
    a.write("b", "old")?;
    a.sync()?;
    b.sync()?;
    fs::remove_file(a.root.join("b"))?;
    a.sync()?;
    a.write("b", "new")?;
    a.sync()?;
    b.sync()?;
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.aside = None)?;
    b.sync()?;
    a.sync()?;
    b.sync()?;
    let tree = b.tree_text("");
    assert!(!tree.iter().any(|t| t.contains(".hbsync-move")), "{tree:?}");
    // Both kept: the new b and the edit, one of them renamed for the clash (§6.1).
    assert!(tree.iter().any(|t| t.ends_with("= new")), "{tree:?}");
    assert!(
        tree.iter().any(|t| t.ends_with("= edited aside")),
        "{tree:?}"
    );
    assert_eq!(a.tree_text(""), tree);
    assert!(nothing_kept(&b), "kept");
    Ok(())
}

/// Something in `.hbsync/aside` that no synced item here is (its state DB lost since, say) is
/// left there, never deleted, and told; the sync goes on.
#[test]
fn an_unknown_item_aside_is_left() -> R {
    let server = server();
    let mut b = Client::with_config(&server, "b", |c| c.guard.allow = true)?;
    b.write("y", "y")?;
    b.sync()?;
    let aside = b.root.join(".hbsync/aside");
    fs::create_dir_all(&aside)?;
    let name = hbsync_types::NodeId::new().0.simple().to_string();
    let stranger = aside.join(&name);
    fs::write(&stranger, "only copy")?;
    fs::write(aside.join("not an entry"), "nor this")?;
    // Junk the OS leaves there is no delete's, and not told.
    fs::write(aside.join(".DS_Store"), "junk")?;
    let told = b.sync()?.left_aside;
    let mut expected = [
        format!(".hbsync/aside/{name}"),
        ".hbsync/aside/not an entry".to_string(),
    ];
    expected.sort();
    assert_eq!(told, expected);
    assert_eq!(fs::read_to_string(&stranger)?, "only copy");
    assert_eq!(fs::read_to_string(aside.join("not an entry"))?, "nor this");
    assert_eq!(
        fs::read_to_string(aside.join(".DS_Store"))?,
        "junk",
        "left too"
    );
    assert_eq!(b.tree_text(""), ["y = y"]);
    Ok(())
}

/// An app saves a file into folder run while b has it aside to delete it (a deleted it): the
/// check finds it there and puts the folder back; the new file keeps the folder alive,
/// restored on the drive, and what was synced in it goes.
#[test]
fn a_file_saved_into_a_folder_aside_is_kept() -> R {
    use hbsync_engine::exec::Aside;
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |c| {
        c.guard.allow = true;
        c.exec.hooks.aside = Some(Aside(Arc::new(|at| {
            if at.is_dir() {
                let _ = fs::write(at.join("new.txt"), "new");
            }
        })));
    })?;
    a.write("run/old.txt", "old")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("run"))?;
    a.sync()?;
    b.sync()?;
    let remote = Box::new(b.remote.clone());
    b.restart_with(remote, |c| c.exec.hooks.aside = None)?;
    b.sync()?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["run/", "run/new.txt = new"]);
    assert_eq!(a.tree_text(""), b.tree_text(""));
    assert!(nothing_kept(&b), "kept");
    Ok(())
}
