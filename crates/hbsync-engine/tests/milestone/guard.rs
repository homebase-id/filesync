//! The mass-delete guard on real folders: what is held, and what a confirmation lets through.

use super::*;

#[test]
fn deleting_everything_is_held() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    for i in 0..60 {
        a.write(&format!("f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..60 {
        fs::remove_file(a.root.join(format!("f{i:02}")))?;
    }
    let report = a.sync()?;
    assert_eq!(report.held.len(), 60);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        60,
        "nothing was deleted remotely"
    );
    Ok(())
}

#[test]
fn a_confirmation_lets_through_exactly_the_deletes_the_user_saw() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    for i in 0..120 {
        a.write(&format!("f{i:03}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    let live = |server: &std::sync::Arc<std::sync::Mutex<hbsync_remote::fake::FakeServer>>| {
        server
            .lock()
            .map(|s| s.live().count())
            .map_err(|_| "poisoned")
    };
    let gone = |a: &Client, range: std::ops::Range<usize>| -> std::io::Result<()> {
        range
            .into_iter()
            .try_for_each(|i| fs::remove_file(a.root.join(format!("f{i:03}"))))
    };
    gone(&a, 0..60)?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 60);
    // Held, out of the dirty set: counted apart.
    let counts = a
        .engine
        .db()
        .read(|s| Ok((s.pending_file_deletes()?, s.held_file_deletes()?)))?;
    assert_eq!(counts, ((0, 0), (60, 0)));
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();

    // 55 more deletes after the user looked: not part of what they confirm. Alone they are
    // over the limit (max(50, 25 % of 120)), so they are held; the confirmed 60 go through.
    gone(&a, 60..115)?;
    a.engine.confirm_deletes(seen.iter().copied());
    let report = a.sync()?;
    assert_eq!(report.held.len(), 55, "{report:?}");
    assert!(report.held.iter().all(|op| !seen.contains(&op.node())));
    assert_eq!(live(&server)?, 60);

    // Confirming those too lets them through.
    a.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    assert!(a.sync()?.held.is_empty());
    assert_eq!(live(&server)?, 5);

    // A confirmation doesn't outlive its deletes: deleting the last five, now every file, is
    // held again.
    gone(&a, 115..120)?;
    assert_eq!(a.sync()?.held.len(), 5);
    assert_eq!(live(&server)?, 5);
    Ok(())
}

#[test]
fn a_confirmed_delete_that_was_undone_is_forgotten() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("f{i}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..4 {
        fs::remove_file(a.root.join(format!("f{i}")))?;
    }
    let report = a.sync()?;
    assert_eq!(report.held.len(), 4);
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();
    // The user puts them back (the deletes undone), then confirms what they were shown.
    a.engine.put_back(&seen)?;
    a.sync()?;
    assert_eq!(a.read("f3")?, b"3");
    a.engine.confirm_deletes(seen);
    a.sync()?;
    // The same files deleted again later: a new delete, held again.
    for i in 0..4 {
        fs::remove_file(a.root.join(format!("f{i}")))?;
    }
    assert_eq!(a.sync()?.held.len(), 4);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 4);
    Ok(())
}

/// A confirmed delete of a folder takes the files under it along: they are the user's, and
/// don't count against other deletes. A small delete that comes in with it isn't held.
#[test]
fn a_confirmed_folder_delete_does_not_hold_other_deletes() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    for i in 0..60 {
        a.write(&format!("dir/f{i:02}"), format!("{i}").as_bytes())?;
        a.write(&format!("g{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.entries("").len(), 121);

    // Deleted here: removing it from the drive is held (60 of 120 files, over the limit of
    // 50).
    fs::remove_dir_all(b.root.join("dir"))?;
    let report = b.sync()?;
    assert!(!report.held.is_empty());
    a.sync()?;
    assert!(a.root.join("dir").exists());
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();

    // The user confirms; meanwhile one more file is deleted here, well under the limit. One
    // sync (as the daemon runs it) does both.
    fs::remove_file(b.root.join("g00"))?;
    b.engine.confirm_deletes(seen);
    b.engine.scan_local()?;
    let report = b.engine.sync_once("2026-10-08")?;
    assert!(report.held.is_empty(), "{:?}", report.held);
    a.sync()?;
    assert!(!a.root.join("dir").exists());
    assert!(!a.root.join("g00").exists());
    Ok(())
}

#[test]
fn held_deletes_do_not_hold_up_other_changes() -> R {
    let server = server();
    // 5 nodes per round, 2 rounds per sync: one sync sees 10 of the 61 dirty nodes.
    let mut a = Client::with_config(&server, "a", |c| {
        c.batch = 5;
        c.max_rounds = 2;
    })?;
    for i in 0..60 {
        a.write(&format!("f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..60 {
        fs::remove_file(a.root.join(format!("f{i:02}")))?;
    }
    a.write("new.txt", b"new")?;
    a.engine.scan_local()?;
    let mut held = std::collections::BTreeSet::new();
    for _ in 0..8 {
        let report = a.engine.sync_once("2026-10-08")?;
        held.extend(report.held.iter().map(|op| format!("{op:?}")));
    }
    assert_eq!(held.len(), 60, "every delete was seen and held");
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(
        srv.live().count(),
        61,
        "the deletes are held, the new file is up"
    );
    assert!(srv.live_names().iter().any(|n| n == "new.txt"));
    Ok(())
}

#[test]
fn a_mass_delete_split_over_batches_is_still_held() -> R {
    let server = server();
    let small_batches = |c: &mut EngineConfig| {
        c.batch = 10;
        c.guard.min = 20;
    };
    let mut a = Client::with_config(&server, "a", small_batches)?;
    for i in 0..30 {
        a.write(&format!("dir/f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    // 30 deletes, planned 10 at a time: each batch alone is under the limit of 20.
    fs::remove_dir_all(a.root.join("dir"))?;
    let report = a.sync()?;
    assert!(!report.held.is_empty());
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        31,
        "nothing was deleted remotely"
    );
    Ok(())
}
