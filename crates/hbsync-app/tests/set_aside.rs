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

type R = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const ID: &str = "test.example";

/// A folder's files as `(path, contents)`, its state folder left out.
fn files(
    root: &Path,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut BTreeMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
fn sync_once(
    server: &Arc<Mutex<FakeServer>>,
    root: &Path,
    db: &Path,
) -> Result<(usize, u64), Box<dyn std::error::Error + Send + Sync>> {
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
    }
    Ok(())
}

/// What was set aside beside `db`.
fn set_aside(db: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
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

/// A folder synced, then a computer whose DB became unusable, as its account starts again (the
/// daemon's start): the DB set aside, kept, and the folder adopted, nothing sent or fetched.
#[test]
fn the_daemon_sets_an_unusable_db_aside_and_adopts_the_folder() -> R {
    for why in [Unusable::TooOld, Unusable::Corrupt] {
        let server = FakeServer::new(ServerConfig::default());
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?.join("Homebase");
        let db = root.with_file_name("state.db");
        std::fs::create_dir_all(&root)?;
        hbsync_engine::root::SyncRoot::create(&db, ID, &root)?;
        write_files(&root)?;
        sync_once(&server, &root, &db)?;
        let synced = files(&root)?;
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

/// A file changed here while the DB was unusable (an edit never sent) is kept beside the
/// drive's version after the folder is set up again, as a conflict copy; nothing is deleted.
#[test]
fn an_edit_never_sent_is_kept_beside_the_drives() -> R {
    let server = FakeServer::new(ServerConfig::default());
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?.join("Homebase");
    let db = root.with_file_name("state.db");
    std::fs::create_dir_all(&root)?;
    hbsync_engine::root::SyncRoot::create(&db, ID, &root)?;
    write_files(&root)?;
    sync_once(&server, &root, &db)?;

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
/// one (the folder deleted meanwhile) fetches them.
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
            Taken::AtStart
        );
        write_files(&root)?;
        sync_once(&server, &root, &db)?;
        let synced = files(&root)?;

        spoil(&db, why)?;
        if emptied {
            std::fs::remove_dir_all(&root)?;
            std::fs::create_dir_all(&root)?;
        }
        let config = hbsync_app::Config::load(&paths.config_file())?.ok_or("no config")?;
        let set = check_state(&paths, &config, &identity, &root, false, false)?;
        assert_eq!(set, Some(why));
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
    let refused = check_state(&paths, &config, &identity, &root, false, false);
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
