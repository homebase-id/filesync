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
    let hard = ChangeRef {
        deleted: true,
        parent: None,
        removed_at: None,
        changed_at: None,
        ..echo
    };
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
    let late_delete = ChangeRef {
        deleted: true,
        parent: None,
        removed_at: None,
        changed_at: None,
        ..ChangeRef::of(&remote.header(file)?)
    };
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
    drive.purge(((now + 10) * 1000) as i64)?;
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
