//! An account's state DB that can't be used (from before 0.7, or damaged) is set aside as the
//! account starts, the daemon's or setup's, and its folder set up again as a folder with files:
//! what it holds is adopted (nothing sent or fetched again), a file changed meanwhile is kept
//! beside the drive's, nothing is deleted; a folder made anew fetches the drive's files.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hbsync_app::folders::{self, Taken};
use hbsync_app::setup::{self, check_state};
use hbsync_app::{Command, Identity, Paths, Root, Runner, RunnerConfig, State};
use hbsync_engine::Drive;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::Unusable;

type E = Box<dyn std::error::Error + Send + Sync>;
type R = Result<(), E>;

const ID: &str = "test.example";

/// A folder's files as `(path, contents)`, its state folder left out.
fn files(root: &Path) -> Result<BTreeMap<String, String>, E> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) -> Result<(), E> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            let rel = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            if rel == ".hbsync" {
                continue;
            }
            if path.is_dir() {
                walk(root, &path, out)?;
            } else {
                out.insert(rel, std::fs::read_to_string(&path)?);
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out)?;
    Ok(out)
}

fn write_files(root: &Path) -> R {
    std::fs::create_dir_all(root.join("sub"))?;
    std::fs::write(root.join("a.txt"), "a")?;
    std::fs::write(root.join("sub/b.txt"), "b")?;
    std::fs::write(root.join("c.txt"), "the drive's")?;
    Ok(())
}

/// Syncs the folder `root` (set up in `db`) once with `server` and stops: what it sent and
/// fetched (uploads, bytes served).
fn sync_once(server: &Arc<Mutex<FakeServer>>, root: &Path, db: &Path) -> Result<(usize, u64), E> {
    let remote = FakeRemote::new(server.clone());
    let cfg = RunnerConfig::new(root, db, &ID.into(), "a");
    let drive = Drive::open(&cfg.db, &cfg.identity, Arc::new(remote.clone()))?;
    let (handle, thread) = Runner::spawn(cfg, drive)?;
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let s = handle.status();
        if s.last_sync.is_some() && s.state == State::Idle && s.pending == 0 {
            break;
        }
        if let Some(e) = s.error.filter(|_| s.state == State::Failing) {
            return Err(e.into());
        }
        if Instant::now() > until {
            return Err("timed out waiting for the sync".into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    handle.send(Command::Stop);
    thread.join().map_err(|_| "the runner panicked")??;
    let faults = remote.faults();
    Ok((faults.uploads, faults.served))
}

/// Makes the DB at `db` one this version can't use: from before 0.7, or garbage (its log too,
/// which would hold its pages otherwise).
fn spoil(db: &Path, why: Unusable) -> R {
    match why {
        Unusable::TooOld => {
            rusqlite::Connection::open(db)?.pragma_update(None, "user_version", 6)?
        }
        Unusable::Corrupt => {
            for suffix in ["", "-wal"] {
                let mut path = db.as_os_str().to_owned();
                path.push(suffix);
                std::fs::write(path, [0x5a_u8; 8192])?;
            }
        }
        Unusable::OtherDrive => return Err("nothing to spoil: the drive was made again".into()),
    }
    Ok(())
}

/// What was set aside beside `db`.
fn set_aside(db: &Path) -> Result<Vec<PathBuf>, E> {
    let name = db.file_name().ok_or("name")?.to_string_lossy().into_owned();
    let mut found = Vec::new();
    for entry in std::fs::read_dir(db.parent().ok_or("parent")?)? {
        let entry = entry?;
        let file = entry.file_name().to_string_lossy().into_owned();
        if file.starts_with(&format!("{name}."))
            && !file.ends_with("-wal")
            && !file.ends_with("-shm")
        {
            found.push(entry.path());
        }
    }
    Ok(found)
}

/// A folder synced with a drive: its files up, its DB beside it.
struct Synced {
    server: Arc<Mutex<FakeServer>>,
    root: PathBuf,
    db: PathBuf,
    /// What the folder holds.
    files: BTreeMap<String, String>,
    _dir: tempfile::TempDir,
}

fn synced() -> Result<Synced, E> {
    let server = FakeServer::new(ServerConfig::default());
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?.join("Homebase");
    let db = root.with_file_name("state.db");
    std::fs::create_dir_all(&root)?;
    hbsync_engine::root::SyncRoot::create(&db, ID, &root)?;
    write_files(&root)?;
    sync_once(&server, &root, &db)?;
    Ok(Synced {
        server,
        files: files(&root)?,
        root,
        db,
        _dir: dir,
    })
}

/// A folder synced, then a computer whose DB became unusable, as its account starts again (the
/// daemon's start): the DB set aside, kept, and the folder adopted, nothing sent or fetched.
#[test]
fn the_daemon_sets_an_unusable_db_aside_and_adopts_the_folder() -> R {
    for why in [Unusable::TooOld, Unusable::Corrupt] {
        let Synced {
            server,
            root,
            db,
            files: synced,
            _dir,
        } = synced()?;
        let on_drive = server.lock().map_err(|_| "poisoned")?.live_names();

        spoil(&db, why)?;
        let roots = [Root::main(&ID.into(), &root)];
        let set = folders::ready_account(&db, &ID.into(), &roots)?.ok_or("not set aside")?;
        assert_eq!(set.why, why);
        assert_eq!(set_aside(&db)?, vec![set.to.clone()], "{why:?}: kept");
        assert_eq!(sync_once(&server, &root, &db)?, (0, 0), "{why:?}: adopted");
        assert_eq!(files(&root)?, synced, "{why:?}");
        let server = server.lock().map_err(|_| "poisoned")?;
        assert_eq!(server.live_names(), on_drive, "{why:?}");
        assert_eq!(server.removed().count(), 0, "{why:?}: nothing deleted");
    }
    Ok(())
}

/// A folder not there when the DB is set aside (its disk unplugged) is left alone then, and
/// set up again at a later start once it is back: adopted, nothing sent or fetched.
#[test]
fn a_folder_back_later_is_set_up_again_at_the_next_start() -> R {
    let Synced {
        server,
        root,
        db,
        files: synced,
        _dir,
    } = synced()?;
    spoil(&db, Unusable::TooOld)?;
    let away = root.with_file_name("unplugged");
    std::fs::rename(&root, &away)?;
    let roots = [Root::main(&ID.into(), &root)];
    folders::ready_account(&db, &ID.into(), &roots)?.ok_or("not set aside")?;
    assert!(!root.exists(), "nothing made where it was");
    std::fs::rename(&away, &root)?;
    assert_eq!(folders::ready_account(&db, &ID.into(), &roots)?, None);
    assert_eq!(sync_once(&server, &root, &db)?, (0, 0));
    assert_eq!(files(&root)?, synced);
    Ok(())
}

/// A DB set aside, the new one's key saying so lost to a crash: knowing no folder beside one
/// set aside, it sets the config's folders up again all the same, adopted, nothing moved. A DB
/// that knows no folder without one set aside (the state folder wiped) doesn't: that folder is
/// set up again by hand, merging its files.
#[test]
fn a_new_db_beside_one_set_aside_sets_them_up_again() -> R {
    let Synced {
        server,
        root,
        db,
        files: synced,
        _dir,
    } = synced()?;
    let remove = |db: &Path| {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = db.as_os_str().to_owned();
            path.push(suffix);
            drop(std::fs::remove_file(path));
        }
    };
    remove(&db);
    let roots = [Root::main(&ID.into(), &root)];
    assert_eq!(folders::ready_account(&db, &ID.into(), &roots)?, None);
    assert!(matches!(
        hbsync_engine::root::SyncRoot::find(&db, ID, &root),
        Err(hbsync_engine::root::RootError::Unknown(_))
    ));

    remove(&db);
    std::fs::write(db.with_file_name("state.db.too-old-1"), "the old one")?;
    assert_eq!(folders::ready_account(&db, &ID.into(), &roots)?, None);
    assert_eq!(sync_once(&server, &root, &db)?, (0, 0));
    assert_eq!(files(&root)?, synced);
    Ok(())
}

/// A file changed here while the DB was unusable (an edit never sent) is kept beside the
/// drive's version after the folder is set up again, as a conflict copy; nothing is deleted.
#[test]
fn an_edit_never_sent_is_kept_beside_the_drives() -> R {
    let Synced {
        server,
        root,
        db,
        _dir,
        ..
    } = synced()?;
    spoil(&db, Unusable::TooOld)?;
    std::fs::write(root.join("c.txt"), "changed here")?;
    let roots = [Root::main(&ID.into(), &root)];
    folders::ready_account(&db, &ID.into(), &roots)?.ok_or("not set aside")?;
    sync_once(&server, &root, &db)?;
    let here: Vec<String> = files(&root)?.into_values().collect();
    for kept in ["a", "b", "the drive's", "changed here"] {
        assert!(here.iter().any(|f| f == kept), "{kept:?} not in {here:?}");
    }
    let server = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(server.live_names().len(), 5, "the conflict copy is up");
    assert_eq!(server.removed().count(), 0, "nothing deleted");
    Ok(())
}

/// Setup on a computer whose DB is unusable, no daemon running: the DB is set aside without
/// asking to merge, and the folder set up again. One that holds the files adopts them; an empty
/// one without its marker (the folder deleted meanwhile, or a disk's mount point) is set up only
/// when asked to merge, and then fetches them.
#[test]
fn setup_sets_an_unusable_db_aside() -> R {
    for (why, emptied) in [(Unusable::TooOld, false), (Unusable::Corrupt, true)] {
        let server = FakeServer::new(ServerConfig::default());
        let home = tempfile::tempdir()?;
        let paths = Paths::under(&home.path().canonicalize()?);
        let identity = Identity::new(ID);
        let root = setup::resolve(&home.path().join("Homebase"));
        let db = paths.account_db(&identity);
        std::fs::create_dir_all(&root)?;
        assert_eq!(
            folders::add_synced(&paths, &identity, &root, None)?,
            (Taken::AtStart, None)
        );
        write_files(&root)?;
        sync_once(&server, &root, &db)?;
        let synced = files(&root)?;

        spoil(&db, why)?;
        if emptied {
            std::fs::remove_dir_all(&root)?;
            std::fs::create_dir_all(&root)?;
        }
        let set = check_state(&paths, &identity, &root, false);
        if emptied {
            let said = set.as_ref().err().map(ToString::to_string);
            assert!(said.is_some_and(|e| e.contains("plug it in")), "{set:?}");
            assert_eq!(check_state(&paths, &identity, &root, true)?, (false, None));
        } else {
            assert_eq!(set?, (false, Some(why)));
        }
        assert_eq!(set_aside(&db)?.len(), 1, "{why:?}: kept");
        folders::add_synced(&paths, &identity, &root, None)?;
        let (uploads, served) = sync_once(&server, &root, &db)?;
        assert_eq!(uploads, 0, "{why:?}");
        assert_eq!(
            served > 0,
            emptied,
            "{why:?}: fetched only into the empty folder"
        );
        assert_eq!(files(&root)?, synced, "{why:?}");
        assert_eq!(server.lock().map_err(|_| "poisoned")?.removed().count(), 0);
    }
    Ok(())
}

/// Setup of a folder whose disk isn't plugged in (an empty mount point where it was) is
/// refused after the DB was set aside, as before: the DB made again doesn't know the folder
/// yet, the config does. Nothing is made there; merging its files is still asked for.
#[test]
fn setup_after_a_set_aside_refuses_an_empty_mount_point() -> R {
    for heal_first in [false, true] {
        let server = FakeServer::new(ServerConfig::default());
        let home = tempfile::tempdir()?;
        let paths = Paths::under(&home.path().canonicalize()?);
        let identity = Identity::new(ID);
        let root = setup::resolve(&home.path().join("Homebase"));
        let db = paths.account_db(&identity);
        std::fs::create_dir_all(&root)?;
        folders::add_synced(&paths, &identity, &root, None)?;
        write_files(&root)?;
        sync_once(&server, &root, &db)?;

        spoil(&db, Unusable::Corrupt)?;
        // Unplugged: its mount point is left, empty.
        std::fs::rename(&root, root.with_file_name("unplugged"))?;
        std::fs::create_dir_all(&root)?;
        let roots = [Root::main(&identity, &root)];
        if heal_first {
            // Set aside as the daemon started, before setup.
            folders::ready_account(&db, &identity, &roots)?.ok_or("not set aside")?;
        }
        let refused = check_state(&paths, &identity, &root, false);
        let said = refused.as_ref().err().map(ToString::to_string);
        assert!(
            said.as_ref().is_some_and(|e| e.contains("plug it in")),
            "{heal_first}: {refused:?}"
        );
        // Set aside by setup itself: told with the refusal.
        let told = said.is_some_and(|e| e.contains("set aside"));
        assert_eq!(told, !heal_first, "{refused:?}");
        assert_eq!(set_aside(&db)?.len(), 1, "{heal_first}");
        assert!(files(&root)?.is_empty(), "{heal_first}: nothing made there");
        assert!(!root.join(".hbsync").exists(), "{heal_first}: not set up");
        assert!(
            check_state(&paths, &identity, &root, true).is_ok(),
            "merging"
        );
    }
    Ok(())
}

/// A DB of a newer version is refused, by setup and as the account starts, and left as it is.
#[test]
fn a_newer_db_is_refused_and_kept() -> R {
    let home = tempfile::tempdir()?;
    let paths = Paths::under(&home.path().canonicalize()?);
    let identity = Identity::new(ID);
    let root = setup::resolve(&home.path().join("Homebase"));
    std::fs::create_dir_all(&root)?;
    folders::add_synced(&paths, &identity, &root, None)?;
    let db = paths.account_db(&identity);
    rusqlite::Connection::open(&db)?.pragma_update(None, "user_version", 9999)?;
    let before = std::fs::read(&db)?;
    let config = hbsync_app::Config::load(&paths.config_file())?.ok_or("no config")?;
    let refused = check_state(&paths, &identity, &root, false);
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("newer")),
        "{refused:?}"
    );
    assert!(folders::ready_account(&db, &identity, &config.roots).is_err());
    assert_eq!(std::fs::read(&db)?, before);
    assert!(set_aside(&db)?.is_empty());
    Ok(())
}
