//! The account's drive, shared by its folders' engines: one feed for all, wakes, the purge.

use std::sync::{Arc, Barrier};
use std::time::Duration;

use hbsync_engine::Drive;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{NodeMeta, Remote};
use hbsync_state::StateDb;
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind};

type R = Result<(), Box<dyn std::error::Error>>;

fn folder(name: &str) -> NodeMeta {
    NodeMeta {
        node: NodeId::new(),
        parent: NodeId::ROOT,
        kind: NodeKind::Folder,
        content: NodeContent::folder(name),
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
    assert!(db.read(|s| s.mirror_by_file(made.file_id))?.is_some());
    // An ask after a pull finished pulls again: the drive may have changed since.
    let later = remote.create(&folder("Later"), None)?;
    drive.pull(None)?;
    assert!(db.read(|s| s.mirror_by_file(later.file_id))?.is_some());
    Ok(())
}

/// A change pulled by one folder's engine wakes the others, not itself.
#[test]
fn a_change_wakes_the_other_folders() -> R {
    let dir = tempfile::tempdir()?;
    let (drive, remote) = drive(dir.path())?;
    let (main, docs) = (drive.subscribe(1), drive.subscribe(2));
    remote.create(&folder("Docs"), None)?;
    drive.pull(Some(1))?;
    assert!(docs.try_recv().is_ok());
    assert!(main.try_recv().is_err(), "the puller isn't woken");
    drive.pull(Some(1))?;
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
