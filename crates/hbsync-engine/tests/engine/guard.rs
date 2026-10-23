//! The mass-delete guard: held deletes, confirmations and Put them back (§7).

use super::*;

/// Deletes held for a confirmation leave nodes pending that another sync can't do anything
/// about: the sync isn't unfinished, however many nodes one held delete covers.
#[test]
fn held_deletes_leave_no_unfinished_work() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    let report = a.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert!(report.pending >= report.held.len() as u64, "{report:?}");
    assert!(
        !report.unfinished,
        "held deletes aren't retried: {report:?}"
    );
    Ok(())
}

/// Another device removed every synced file (held here, §7), and the user confirmed; a folder
/// holds a file this device can't read yet: the folder's delete waits for it (§6.3), and the
/// confirmation lasts while it does, so once the file is read the delete goes through without
/// asking again.
#[cfg(unix)]
#[test]
fn a_confirmation_lasts_while_its_delete_waits_for_a_file() -> R {
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..6 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    b.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    // Three of b's files can't be read now, unchanged (a mode change): the folder's delete
    // waits for them, and after, their deletes alone trip the guard.
    let unread: Vec<_> = (0..3).map(|i| b.root.join(format!("d/f{i}.txt"))).collect();
    for f in &unread {
        fs::set_permissions(f, fs::Permissions::from_mode(0o000))?;
    }
    if fs::read(&unread[0]).is_ok() {
        return Ok(()); // root reads anything: nothing to test
    }
    b.engine.scan_local()?;
    let report = b.engine.sync("2026-10-08", true)?;
    assert!(report.held.is_empty(), "nothing held again: {report:?}");
    assert!(b.root.join("d").is_dir(), "the folder's delete waits");
    for f in &unread {
        fs::set_permissions(f, fs::Permissions::from_mode(0o644))?;
    }
    let report = b.sync()?;
    assert!(report.held.is_empty(), "not asked again: {report:?}");
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    Ok(())
}

/// This device deletes a folder over the guard's limit: held here (§7), nothing sent, so the
/// drive and the other devices keep it. A new folder of its name made meanwhile syncs under a
/// numbered one (the drive still has the old). "Put them back": the folder comes down again,
/// beside the new one.
#[test]
fn a_delete_here_is_held_and_put_back_brings_it_back() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    b.sync()?;
    assert_eq!(b.read_text("Projects/f3.txt")?, "3", "nothing sent");
    // A new Projects meanwhile: numbered, the old one's name still taken on the drive.
    a.write("Projects/new.txt", "n")?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    b.sync()?;
    assert_eq!(b.read_text("Projects (conflict 1)/new.txt")?, "n");
    assert_eq!(a.read_text("Projects (conflict 1)/new.txt")?, "n");
    // Put them back.
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    assert_eq!(a.engine.put_back(&held)?, 5);
    let report = a.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(a.read_text("Projects/f0.txt")?, "0");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A held delete here, then another device removes the folder too: "Put them back" doesn't
/// bring back what the drive removed since (its state wins).
#[test]
fn put_back_never_undoes_a_removal_on_the_drive_since() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.allow = true)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    let report = a.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    fs::remove_dir_all(b.root.join("d"))?;
    b.sync()?;
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    a.engine.put_back(&held)?;
    a.sync()?;
    assert!(!a.root.join("d").exists(), "{:?}", a.tree_text(""));
    assert_eq!(a.tree_text(""), ["keep.txt = k"]);
    Ok(())
}

/// Another device removes every synced file (over the limit): held here (a broken device, a
/// wiped drive, §7). "Put them back" keeps them here and restores them on the drive, for every
/// device.
#[test]
fn every_file_removed_elsewhere_is_held_and_put_back_restores_it() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(b.read_text("d/f2.txt")?, "2", "held");
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    assert!(b.engine.put_back(&held)? > 0);
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    a.sync()?;
    assert_eq!(a.read_text("d/f2.txt")?, "2", "back for every device");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// Removed folders in a cycle, each deleted with the other (two open removals, both the
/// newest): "Put them back" walks up to the item their delete took along and ends, as Recently
/// deleted's listing does, instead of walking round the cycle for good.
#[test]
fn put_back_ends_at_a_cycle_of_removed_folders() -> R {
    use hbsync_state::testkit::RawPicture;
    use hbsync_types::{Uuid, VersionTag};
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    // `d` now in a removed folder `q` that is in `d`; `d`, `q` and what `d` held all removed
    // open, so each counts as deleted with its folder.
    let d = b
        .engine
        .db()
        .read(|s| match s.removed_named(&["d"])?.first() {
            Some(&n) => s.removed_by_node(n),
            None => Ok(None),
        })?
        .ok_or("d in Recently deleted")?
        .row;
    let q = NodeId::new();
    b.engine.db().write(|s| {
        let open = |row: &hbsync_state::MirrorRow| hbsync_state::RemovedRow {
            row: hbsync_state::MirrorRow {
                version: VersionTag(Uuid::new_v4()),
                ..row.clone()
            },
            removed_at: None,
        };
        let q_row = hbsync_state::MirrorRow {
            file_id: RemoteFileId(Uuid::new_v4()),
            node: q,
            parent: d.node,
            name: "q".into(),
            ..d.clone()
        };
        s.account().put_removed(&open(&q_row))?;
        s.account().put_removed(&open(&hbsync_state::MirrorRow {
            parent: q,
            ..d.clone()
        }))?;
        for row in s.removed_children(d.node)? {
            s.account().put_removed(&open(&row.row))?;
        }
        Ok(())
    })?;
    let cycle = b.engine.db().read(|s| {
        let parent =
            |n| Ok::<_, hbsync_state::StateError>(s.removed_by_node(n)?.map(|r| r.row.parent));
        Ok((parent(d.node)?, parent(q)?))
    })?;
    assert_eq!(
        cycle,
        (Some(q), Some(d.node)),
        "a cycle in Recently deleted"
    );
    // A walk round the cycle never returns: the test process ends instead. Set however
    // put_back ends.
    struct Done(Arc<AtomicBool>);
    impl Drop for Done {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let done = Done(Arc::new(AtomicBool::new(false)));
    let watched = done.0.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(300));
        if !watched.load(Ordering::SeqCst) {
            eprintln!("put back walked round the cycle");
            std::process::exit(1);
        }
    });
    let put = b.engine.put_back(&held);
    drop(done);
    assert!(put? > 0);
    Ok(())
}

/// Removals elsewhere short of every synced file are followed unasked, however many (§7):
/// the drive keeps them in Recently deleted.
#[test]
fn removals_elsewhere_are_followed_unasked() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..6 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    Ok(())
}

/// A synced folder moved here into an ignored name leaves the drive (§10, rule 1): held as any
/// removal from here over the limit. "Put them back" keeps it on the drive, synced here again
/// where the drive has it (masking goes by the drive's path); the ignored copy stays as it is,
/// and nothing more is asked.
#[test]
fn a_newly_ignored_folder_put_back_is_synced_again() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| {
        ignoring("build/\n")(cfg);
        cfg.guard.min = 2;
    })?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..4 {
        a.write(&format!("src/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::rename(a.root.join("src"), a.root.join("build"))?;
    let report = a.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    a.engine.put_back(&held)?;
    let report = a.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    let again = a.sync()?;
    assert_eq!(again.ops_done, 0, "{again:?}");
    assert_eq!(a.read_text("build/f0.txt")?, "0", "kept here");
    assert_eq!(a.read_text("src/f0.txt")?, "0", "synced again");
    b.sync()?;
    assert_eq!(b.read_text("src/f0.txt")?, "0", "kept on the drive");
    Ok(())
}

/// Every synced file removed elsewhere, but few (under the guard's floor: a small folder
/// emptied from the phone): followed unasked.
#[test]
fn a_few_files_all_removed_elsewhere_are_followed_unasked() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..3 {
        a.write(&format!("f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    for i in 0..3 {
        fs::remove_file(a.root.join(format!("f{i}.txt")))?;
    }
    a.sync()?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    Ok(())
}

/// Every synced file removed elsewhere, held; one of them can't be read here yet, then is
/// edited back to the drive: the rest stays held (it isn't every file any more, but nobody
/// answered), across a restart too, until the user puts them back.
#[cfg(unix)]
#[test]
fn a_hold_of_every_file_lasts_until_answered() -> R {
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..8 {
        a.write(&format!("f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    for i in 0..8 {
        fs::remove_file(a.root.join(format!("f{i}.txt")))?;
    }
    a.sync()?;
    let f3 = b.root.join("f3.txt");
    fs::write(&f3, "3, b's")?;
    fs::set_permissions(&f3, fs::Permissions::from_mode(0o000))?;
    if fs::read(&f3).is_ok() {
        return Ok(()); // root reads anything: nothing to test
    }
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    fs::set_permissions(&f3, fs::Permissions::from_mode(0o644))?;
    b.sync()?;
    assert_eq!(b.read_text("f3.txt")?, "3, b's");
    let report = b.sync()?;
    assert_eq!(report.held.len(), 7, "still held: {report:?}");
    b.restart()?;
    let report = b.sync()?;
    assert_eq!(report.held.len(), 7, "held across a restart: {report:?}");
    assert_eq!(b.read_text("f5.txt")?, "5");

    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    b.engine.put_back(&held)?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    a.sync()?;
    assert_eq!(a.read_text("f5.txt")?, "5", "back on the drive");
    assert_eq!(a.read_text("f3.txt")?, "3, b's");
    Ok(())
}

/// Every synced file removed elsewhere, some of a folder's files removed apart before the
/// folder: "Put them back" restores those too, not only the folder and what went with it.
#[test]
fn put_back_restores_what_was_removed_apart_before() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..6 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    for i in 0..3 {
        fs::remove_file(a.root.join(format!("d/f{i}.txt")))?;
    }
    a.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    fs::remove_file(a.root.join("keep.txt"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    b.engine.put_back(&held)?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    b.sync()?;
    for i in 0..6 {
        assert_eq!(b.read_text(&format!("d/f{i}.txt"))?, format!("{i}"));
    }
    a.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A file moved here out of a folder whose delete is then held, the move not sent yet: "Put
/// them back" brings the folder back and leaves the move as it is.
#[test]
fn put_back_keeps_a_move_out_of_the_folder() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    // The move and the delete in one scan; the move keeps failing meanwhile.
    fs::create_dir(a.root.join("Other"))?;
    fs::rename(a.root.join("Projects/f0.txt"), a.root.join("Other/f0.txt"))?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    a.remote.faults().fail_writes_named = Some("f0.txt".into());
    let report = a.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    a.remote.faults().fail_writes_named = None;
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    a.engine.put_back(&held)?;
    a.sync()?;
    assert_eq!(a.read_text("Other/f0.txt")?, "0");
    assert!(
        !a.root.join("Projects/f0.txt").exists(),
        "{:?}",
        a.tree_text("")
    );
    assert_eq!(a.read_text("Projects/f1.txt")?, "1");
    Ok(())
}

/// Another device moves every synced file into a name this one ignores: they leave this
/// device unasked, a move and not a removal (the drive keeps them live, §7).
#[test]
fn every_file_moved_out_of_view_elsewhere_is_followed() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| {
        ignoring("build/\n")(cfg);
        cfg.guard.min = 2;
    })?;
    for i in 0..6 {
        a.write(&format!("src/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::rename(a.root.join("src"), a.root.join("build"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    Ok(())
}

/// A folder held as every synced file; another device restores one file of it, and the sync
/// that sees it fails after its plan (an upload refused): the rest is still held next time.
#[test]
fn a_hold_of_every_file_outlasts_a_failed_sync() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..8 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let f3 = a
        .recently_deleted(20)?
        .into_iter()
        .find(|i| i.path == "d/f3.txt")
        .ok_or("f3")?
        .node;
    assert_eq!(a.engine.drive().restore(&[f3])?, 2, "f3 and its folder");
    b.write("x.txt", "x")?;
    b.remote.faults().deny_writes = true;
    b.engine.scan_local()?;
    assert!(b.engine.sync_once("2026-10-09").is_err());
    b.remote.faults().deny_writes = false;
    for _ in 0..2 {
        let report = b.sync()?;
        assert!(!report.held.is_empty(), "{report:?}");
        assert_eq!(b.read_text("d/f5.txt")?, "5", "held: {:?}", b.tree_text(""));
    }
    Ok(())
}

/// A folder held as every synced file; one file of it back on the drive meanwhile, and a
/// subfolder's delete waiting for a file in it not hashed yet: once it's read, the subfolder's
/// delete (taking held files along) is held too.
#[cfg(unix)]
#[test]
fn a_hold_of_every_file_holds_a_delete_taking_it_along() -> R {
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("d/s/f{i}.txt"), format!("{i}"))?;
        a.write(&format!("d/g{i}.txt"), format!("g{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let g0 = a
        .recently_deleted(20)?
        .into_iter()
        .find(|i| i.path == "d/g0.txt")
        .ok_or("g0")?
        .node;
    a.engine.drive().restore(&[g0])?;
    let f1 = b.root.join("d/s/f1.txt");
    fs::set_permissions(&f1, fs::Permissions::from_mode(0o000))?;
    if fs::read(&f1).is_ok() {
        return Ok(()); // root reads anything: nothing to test
    }
    b.sync()?;
    fs::set_permissions(&f1, fs::Permissions::from_mode(0o644))?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(
        b.read_text("d/s/f2.txt")?,
        "2",
        "held: {:?}",
        b.tree_text("")
    );
    Ok(())
}

/// Held file by file (plans in pages), then a restart plans the folder's delete whole, one file
/// back on the drive: still held.
#[test]
fn a_hold_of_every_file_paged_holds_the_whole_delete() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| {
        cfg.guard.min = 2;
        cfg.batch = 2;
        cfg.max_rounds = 1000;
    })?;
    for i in 0..12 {
        a.write(&format!("d/s{}/f{i}.txt", i % 3), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    let f0 = a
        .recently_deleted(20)?
        .into_iter()
        .find(|i| i.path == "d/s0/f0.txt")
        .ok_or("f0")?
        .node;
    a.engine.drive().restore(&[f0])?;
    let remote = b.remote.clone();
    b.restart_with(Box::new(remote), |cfg| cfg.batch = 20_000)?;
    let report = b.sync()?;
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(
        b.read_text("d/s1/f1.txt")?,
        "1",
        "held: {:?}",
        b.tree_text("")
    );
    Ok(())
}

/// Put back of a removal held file by file (plans in pages) brings back what it does held as
/// one folder. Held whole it costs one restore (what was removed with the folder comes back
/// with it); file by file, the folders were kept on the drive meanwhile (they hold what is
/// held), so each file is a removal of its own, restored with the others in one go.
#[test]
fn put_back_held_file_by_file_comes_back_alike() -> R {
    let requests = |batch: usize| -> hbsync_engine::testkit::Res<(usize, Vec<String>)> {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.min = 2;
            cfg.batch = batch;
            cfg.max_rounds = 1000;
        })?;
        for i in 0..12 {
            a.write(&format!("d/s{}/f{i}.txt", i % 3), format!("{i}"))?;
        }
        a.sync()?;
        b.sync()?;
        fs::remove_dir_all(a.root.join("d"))?;
        a.sync()?;
        let report = b.sync()?;
        assert!(!report.held.is_empty(), "{report:?}");
        let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
        let before = b.remote.faults().requests;
        b.engine.put_back(&held)?;
        let requests = b.remote.faults().requests - before;
        b.sync()?;
        a.sync()?;
        Ok((requests, a.tree_text("")))
    };
    let (whole, tree) = requests(20_000)?;
    let (_, paged_tree) = requests(2)?;
    assert_eq!(tree, paged_tree);
    assert_eq!(tree.len(), 16, "{tree:?}");
    assert!(whole <= 4, "one feed read, one restore: {whole} requests");
    Ok(())
}

/// Held deletes leave the dirty set till the user answers (sync semantics §7): a sync with
/// nothing new plans nothing, one with an edit elsewhere plans that only, and each still shows
/// what is held, with it pending. A new engine (a restart) plans them again, still held; a
/// later delete here is held with them (the guard counts them); Delete lets them all through.
#[test]
fn held_deletes_are_planned_again_only_when_needed() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.write("other/o.txt", "o")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    let held_db = |a: &mut Client| a.engine.db().read(|s| s.held_delete_count());
    assert_eq!(held_db(&mut a)?, 5, "out of the dirty set");
    // Nothing new: nothing planned, the same shown.
    let report = a.engine.sync("2026-10-08", false)?;
    assert_eq!((report.rounds, report.held.len()), (0, 5), "{report:?}");
    assert_eq!(report.pending, 5);
    // An edit elsewhere: that alone is dirty, and planned.
    a.write("other/o.txt", "o2")?;
    a.engine.scan_local()?;
    assert_eq!(a.engine.db().read(|s| s.dirty_count())?, 1);
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    assert_eq!(held_db(&mut a)?, 5);
    // A new engine plans them again, still held.
    a.restart()?;
    let report = a.sync()?;
    assert!(report.rounds > 0, "{report:?}");
    assert_eq!(report.held.len(), 5, "{report:?}");
    // One file more deleted here: held with them (5 files, over the limit of 2).
    fs::remove_file(a.root.join("keep.txt"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 6, "{report:?}");
    b.sync()?;
    assert_eq!(b.read_text("keep.txt")?, "k", "nothing sent");
    assert_eq!(b.read_text("Projects/f0.txt")?, "0", "nothing sent");
    // Delete: all of them go.
    let nodes: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    a.engine.confirm_deletes(nodes);
    let report = a.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(held_db(&mut a)?, 0);
    b.sync()?;
    assert_eq!(b.tree_text(""), ["other/", "other/o.txt = o2"]);
    Ok(())
}

/// A change to a held item plans every held delete again, with it (its view reaches them):
/// as when they stayed dirty. Here a file of a held folder is edited elsewhere, so the folder
/// comes back here with it, and that delete is no longer held.
#[test]
fn a_change_to_a_held_item_plans_them_again() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    let mut b = Client::new(&server, "b")?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    b.write("Projects/f1.txt", "edited")?;
    b.sync()?;
    let report = a.sync()?;
    assert!(report.rounds > 0, "{report:?}");
    assert_eq!(
        a.read_text("Projects/f1.txt")?,
        "edited",
        "{:?}",
        a.tree_text("")
    );
    b.sync()?;
    assert_eq!(b.read_text("Projects/f1.txt")?, "edited");
    Ok(())
}

/// This device's own removals held out of the dirty set go through once the guard would let
/// them (more files synced since: the limit is a share of them), as they would have had they
/// stayed dirty.
#[test]
fn held_deletes_here_go_once_the_limit_lets_them() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| {
        cfg.guard.min = 2;
        cfg.guard.fraction = 0.25;
    })?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    for i in 0..4 {
        a.write(&format!("keep/k{i}.txt"), "k")?;
    }
    a.sync()?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    // 24 more: 32 synced, a limit of 8 for 4 files.
    for i in 0..24 {
        a.write(&format!("more/m{i}.txt"), "m")?;
    }
    a.sync()?;
    let report = a.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    let live = server.lock().map_err(|_| "poisoned")?.live().count();
    assert_eq!(live, 1 + 4 + 1 + 24, "keep, its files, more, its files");
    Ok(())
}

/// A held delete planned over several pages leaves the dirty set whole at the end of the sync
/// (each page sees the others' nodes as context, as before), and stays out. (Loose files: a
/// folder's delete over several pages is review 14's R1, apart.)
#[test]
fn a_held_delete_over_several_pages_stays_out() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| {
        cfg.guard.min = 2;
        cfg.batch = 2;
    })?;
    for i in 0..4 {
        a.write(&format!("f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    for i in 0..4 {
        fs::remove_file(a.root.join(format!("f{i}.txt")))?;
    }
    let report = a.sync()?;
    assert_eq!(report.held.len(), 4, "{report:?}");
    assert_eq!(a.engine.db().read(|s| s.held_delete_count())?, 4);
    for _ in 0..2 {
        let report = a.engine.sync("2026-10-08", false)?;
        assert_eq!((report.rounds, report.held.len()), (0, 4), "{report:?}");
    }
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 5);
    Ok(())
}

/// Something else left dirty (a file that can't be read yet) keeps held deletes in the dirty
/// set only if its view reaches them: planned alone at the end of the sync, this one doesn't,
/// so they leave it, and it stays dirty, planned every sync.
#[cfg(unix)]
#[test]
fn held_deletes_leave_the_dirty_set_beside_a_file_that_waits() -> R {
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("other/locked.txt", "l")?;
    a.sync()?;
    let locked = a.root.join("other/locked.txt");
    a.write("other/locked.txt", "changed")?;
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    let counts = a
        .engine
        .db()
        .read(|s| Ok((s.held_delete_count()?, s.dirty_count()?)))?;
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o644))?;
    assert_eq!(counts, (5, 1), "the held ones out, the locked file dirty");
    Ok(())
}

/// More than a thousand other nodes left dirty (here files that can't be read yet) keep held
/// deletes in the dirty set (`LEFTOVERS_MAX`): checking that the view of so many doesn't reach
/// them, planned alone at the end of every sync, would cost about what planning them with it
/// does. A thousand exactly, they leave it. (The scan of 2026-10-23, G7.)
#[cfg(unix)]
#[test]
fn held_deletes_stay_dirty_beside_more_than_a_thousand_left_over() -> R {
    use std::os::unix::fs::PermissionsExt;
    for (locked, out) in [(1_000, 5), (1_001, 0)] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| {
            cfg.guard.min = 2;
            cfg.guard.fraction = 0.0;
        })?;
        for i in 0..4 {
            a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
        }
        for i in 0..locked {
            a.write(&format!("other/l{i}.txt"), "l")?;
        }
        a.sync()?;
        let paths: Vec<_> = (0..locked)
            .map(|i| a.root.join(format!("other/l{i}.txt")))
            .collect();
        for path in &paths {
            fs::write(path, "changed")?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o000))?;
        }
        fs::remove_dir_all(a.root.join("Projects"))?;
        let report = a.sync();
        let counts = a
            .engine
            .db()
            .read(|s| Ok((s.held_delete_count()?, s.dirty_count()?)));
        for path in &paths {
            fs::set_permissions(path, fs::Permissions::from_mode(0o644))?;
        }
        assert_eq!(report?.held.len(), 5);
        let (held_out, dirty) = counts?;
        println!("ZZ locked {locked}: held out {held_out}, dirty {dirty}");
        assert_eq!(held_out, out, "{locked} left over");
        assert_eq!(
            dirty as usize,
            locked + 5 - out as usize,
            "{locked} left over"
        );
    }
    Ok(())
}

/// Another device removes every synced file, and this device hears of it (an announcement,
/// another folder's pull) just after a sync read the guard's counts with nothing dirty: the
/// removal is still judged against the synced files, and held (§7), not deleted with the
/// every-file rule off.
#[test]
fn a_wipe_marked_right_after_the_counts_is_still_held() -> R {
    use hbsync_engine::testkit::AfterPlan;
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    // The drive to pull from when the hook runs, once: set once b is up.
    let drive: Arc<Mutex<Option<Arc<hbsync_engine::Drive>>>> = Arc::default();
    let hook = {
        let drive = drive.clone();
        AfterPlan(Arc::new(move || {
            if let Some(drive) = drive.lock().ok().and_then(|mut d| d.take()) {
                drive.pull(None).expect("pull");
            }
        }))
    };
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.min = 2;
        cfg.after_counts = Some(hook.clone());
    })?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    // b takes the drive's changes as announced: its next sync reads no feed; the hook pulls
    // a's removal right after the counts.
    b.announced();
    *drive.lock().map_err(|_| "poisoned")? = Some(b.engine.drive().clone());
    let report = b.engine.sync("2026-10-08", false)?;
    assert!(
        drive.lock().map_err(|_| "poisoned")?.is_none(),
        "the hook ran"
    );
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(b.read_text("d/f2.txt")?, "2", "held, not deleted");
    Ok(())
}

/// The same with something else dirty when the counts are read (a file that can't be read
/// yet) and the wipe planned over several pages: the pages are judged against the totals
/// counted again with the wipe in them, not those read before it was marked.
#[cfg(unix)]
#[test]
fn a_wipe_marked_after_the_counts_beside_a_dirty_file_is_still_held() -> R {
    use hbsync_engine::testkit::AfterPlan;
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let drive: Arc<Mutex<Option<Arc<hbsync_engine::Drive>>>> = Arc::default();
    let hook = {
        let drive = drive.clone();
        AfterPlan(Arc::new(move || {
            if let Some(drive) = drive.lock().ok().and_then(|mut d| d.take()) {
                drive.pull(None).expect("pull");
            }
        }))
    };
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.min = 2;
        cfg.batch = 2;
        cfg.after_counts = Some(hook.clone());
    })?;
    for i in 0..4 {
        a.write(&format!("d{i}/f.txt"), format!("{i}"))?;
    }
    a.write("x.txt", "x")?;
    a.sync()?;
    b.sync()?;
    for i in 0..4 {
        fs::remove_dir_all(a.root.join(format!("d{i}")))?;
    }
    fs::remove_file(a.root.join("x.txt"))?;
    a.sync()?;
    // x changed here and can't be read yet: dirty when b's sync counts.
    let x = b.root.join("x.txt");
    b.write("x.txt", "changed")?;
    fs::set_permissions(&x, fs::Permissions::from_mode(0o000))?;
    b.engine.scan_local()?;
    b.announced();
    *drive.lock().map_err(|_| "poisoned")? = Some(b.engine.drive().clone());
    let report = b.engine.sync("2026-10-08", false);
    fs::set_permissions(&x, fs::Permissions::from_mode(0o644))?;
    let report = report?;
    assert!(
        drive.lock().map_err(|_| "poisoned")?.is_none(),
        "the hook ran"
    );
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(b.read_text("d2/f.txt")?, "2", "held, not deleted");
    Ok(())
}

/// A wipe held as every synced file in one pass of a sync stays held in the next pass, though
/// the counts are read again meanwhile (another connection committed: here one of the files
/// restored elsewhere, pulled) and no longer add up to every synced file: what is held so stays
/// held till answered (§7).
#[test]
fn a_wipe_held_in_one_pass_stays_held_after_the_counts_are_read_again() -> R {
    use hbsync_engine::testkit::AfterPlan;
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    type Then = Box<dyn FnOnce() + Send>;
    let then: Arc<Mutex<Option<Then>>> = Arc::default();
    let hook = {
        let then = then.clone();
        AfterPlan(Arc::new(move || {
            if let Some(then) = then.lock().ok().and_then(|mut t| t.take()) {
                then();
            }
        }))
    };
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.min = 2;
        cfg.after_plan = Some(hook.clone());
        cfg.check_narrow = false;
    })?;
    for i in 0..4 {
        a.write(&format!("d/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.sync()?;
    // b pulls the wipe, and has a new file of its own: its first pass uploads it (so a second
    // pass follows) and holds the wipe; meanwhile f0 is restored elsewhere and pulled.
    b.engine.drive().pull(None)?;
    b.write("new.txt", "n")?;
    b.engine.scan_local()?;
    let (restorer, drive, f0) = (
        FakeRemote::new(server.clone()),
        Arc::clone(b.engine.drive()),
        common::header_named(&server, "f0.txt"),
    );
    *then.lock().map_err(|_| "poisoned")? = Some(Box::new(move || {
        let target = Target {
            at: f0.at(),
            token: f0.token.clone(),
        };
        restorer.set_removed(&[target], false).expect("restored");
        drive.pull(None).expect("the pull");
    }));
    b.announced();
    let report = b.engine.sync("2026-10-08", false)?;
    assert!(
        then.lock().map_err(|_| "poisoned")?.is_none(),
        "the hook ran"
    );
    assert!(!report.held.is_empty(), "{report:?}");
    assert_eq!(b.read_text("d/f2.txt")?, "2", "still held");
    Ok(())
}

/// A wipe held as every synced file, planned over several pages: one of its files restored
/// elsewhere (and pulled) after the first page doesn't let the later pages' part of it
/// through: the counts read again are never less than those the first page was judged by.
#[test]
fn a_wipe_over_pages_stays_held_after_a_restore_elsewhere() -> R {
    use hbsync_engine::testkit::AfterPlan;
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    type Then = Box<dyn FnOnce() + Send>;
    let then: Arc<Mutex<Option<Then>>> = Arc::default();
    let hook = {
        let then = then.clone();
        AfterPlan(Arc::new(move || {
            if let Some(then) = then.lock().ok().and_then(|mut t| t.take()) {
                then();
            }
        }))
    };
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.min = 2;
        cfg.batch = 2;
        cfg.after_plan = Some(hook.clone());
        cfg.check_narrow = false;
    })?;
    for i in 0..4 {
        a.write(&format!("d{i}/f{i}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    for i in 0..4 {
        fs::remove_dir_all(a.root.join(format!("d{i}")))?;
    }
    a.sync()?;
    b.engine.drive().pull(None)?;
    let (restorer, drive, f0) = (
        FakeRemote::new(server.clone()),
        Arc::clone(b.engine.drive()),
        common::header_named(&server, "f0.txt"),
    );
    *then.lock().map_err(|_| "poisoned")? = Some(Box::new(move || {
        let target = Target {
            at: f0.at(),
            token: f0.token.clone(),
        };
        restorer.set_removed(&[target], false).expect("restored");
        drive.pull(None).expect("the pull");
    }));
    b.announced();
    let report = b.engine.sync("2026-10-08", false)?;
    assert!(
        then.lock().map_err(|_| "poisoned")?.is_none(),
        "the hook ran"
    );
    for i in 1..4 {
        assert_eq!(
            b.read_text(&format!("d{i}/f{i}.txt"))?,
            i.to_string(),
            "held: {report:?}"
        );
    }
    Ok(())
}

/// Putting back many loose files removed elsewhere (held as every synced file, §7) restores
/// them together: a feed read and a request per [`hbsync_remote::BATCH_FILES`] restored, not a
/// request each.
#[test]
fn putting_back_many_loose_files_takes_few_requests() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", |cfg| cfg.guard.min = 2)?;
    for i in 0..300 {
        a.write(&format!("f{i:03}.txt"), format!("{i}"))?;
    }
    a.sync()?;
    b.sync()?;
    for i in 0..300 {
        fs::remove_file(a.root.join(format!("f{i:03}.txt")))?;
    }
    a.sync()?;
    let report = b.sync()?;
    assert_eq!(report.held.len(), 300, "{report:?}");
    let held: Vec<NodeId> = report.held.iter().map(|op| op.node()).collect();
    let before = b.remote.faults().requests;
    assert_eq!(b.engine.put_back(&held)?, 300);
    let requests = b.remote.faults().requests - before;
    assert!(requests <= 3, "{requests} requests");
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read_text("f123.txt")?, "123", "back for every device");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// The user's Delete outlives a sync that fails before it judges anything: the next sync lets
/// the confirmed deletes through, not held and asked about again.
#[test]
fn a_confirmation_outlives_a_sync_that_failed_at_its_start() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("Projects/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    fs::remove_dir_all(a.root.join("Projects"))?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 5, "{report:?}");
    a.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    // The next sync fails where it judges: the store's id is missing (a damaged DB).
    let conn = rusqlite::Connection::open(a.db_path())?;
    let id: String =
        conn.query_row("SELECT value FROM kv WHERE key = 'db.id'", [], |r| r.get(0))?;
    conn.execute("UPDATE kv SET value = '' WHERE key = 'db.id'", [])?;
    assert!(a.engine.sync("2026-10-08", true).is_err());
    conn.execute("UPDATE kv SET value = ?1 WHERE key = 'db.id'", [&id])?;
    let report = a.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        1,
        "only keep.txt"
    );
    Ok(())
}
