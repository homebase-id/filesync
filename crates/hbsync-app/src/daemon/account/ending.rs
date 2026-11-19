//! A share that isn't synced here any more (its marker turned it off, or it ended): its copy
//! taken off this device, on a thread of its own ([`end`]), under the engine's guards
//! (sync semantics §13, "A share that isn't synced here any more"): only in the folder set up
//! for this share's state DB (its marker says so), what is there as it was synced deleted,
//! everything else moved out to the account's main folder, no link followed. Then the folder
//! itself goes, if nothing is left in it, and the share's state DB.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hbsync_engine::root::{RootError, SyncRoot};
use hbsync_engine::{Engine, EngineConfig};
use hbsync_local::{Filter, STATE_DIR, Verdict};

use super::parts::Share;
use crate::Identity;
use crate::runner::RunnerError;

/// What became of a share's copy ([`end`]).
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Ended {
    /// Its folder is gone from the folder of shares (or was no more).
    pub(super) removed: bool,
    /// How many items moved out to the main folder: changed or put there, or never synced.
    pub(super) moved_out: usize,
    /// What is left in its folder, by name: what couldn't be deleted or moved.
    pub(super) left: Vec<String>,
}

/// Takes `share`'s copy in `folder` off this device, what isn't the sharer's moving out to
/// `out` (the account's main folder), and removes the share's state DB. Stopped between
/// operations once `stop` is set: what is left then is taken up at the next start.
///
/// Never in a folder that isn't set up for this share's DB: one whose marker names another
/// (a folder renamed into its place) is left as it is, an error. A folder that is gone
/// leaves only the DB to remove.
pub(super) fn end(
    share: Share,
    identity: &Identity,
    device: &str,
    folder: &Path,
    out: &Path,
    stop: Arc<AtomicBool>,
) -> Result<Ended, RunnerError> {
    // Its puller's thread ends first (joined here, not on the account's thread).
    share.notifications.finish();
    let Share { drive, db, .. } = share;
    let mut ended = Ended::default();
    match SyncRoot::open(&db, identity, folder) {
        Ok(root) => {
            let mut cfg = EngineConfig::new(folder, device);
            let filter = Filter::load(folder, cfg.folder.case_insensitive)?;
            cfg.folder.filter = Arc::new(filter);
            cfg.folder.read_only = Some(out.to_path_buf());
            let filter = Arc::clone(&cfg.folder.filter);
            let mut engine = Engine::open(&root, Arc::clone(&drive), cfg)?;
            engine.set_stop(stop);
            let (report, unsynced) = engine.end_share(&crate::today())?;
            ended.moved_out = report.moved_out.len() + unsynced.len();
            drop(engine);
            ended.left = left_in(folder, &filter)?;
            if ended.left.is_empty() {
                // (The client's own, in a folder its marker proved ours: its marker, what a
                // transfer left. Then the folder, which holds nothing now.)
                remove(&folder.join(STATE_DIR), true)?;
                remove(folder, false)?;
                ended.removed = true;
            }
        }
        // Gone already (removed by hand, or by an ending cut short after its last step).
        Err(RootError::NoMarker(_)) if !folder.exists() => ended.removed = true,
        Err(e) => return Err(e.into()),
    }
    // Its records go only with its folder: one that stays is taken up again with them.
    if ended.removed {
        drop(drive);
        if let Some(dir) = db.parent() {
            remove(dir, true)?;
        }
    }
    Ok(ended)
}

/// What `folder` still holds at its top, by name, junk files deleted on the way (they go
/// with a folder, as in any delete: sync semantics §7): nothing but the client's own state
/// when all of the copy went.
fn left_in(folder: &Path, filter: &Filter) -> Result<Vec<String>, RunnerError> {
    let mut left = Vec::new();
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == STATE_DIR {
            continue;
        }
        let is_dir = entry.file_type()?.is_dir();
        let junk = !is_dir && filter.in_folder("", &name, false) == Verdict::Junk;
        if !junk || std::fs::remove_file(entry.path()).is_err() {
            left.push(name);
        }
    }
    left.sort();
    Ok(left)
}

/// Removes the folder at `path`, with all in it when `all` (no link in it is followed);
/// one that isn't there is removed.
fn remove(path: &Path, all: bool) -> std::io::Result<()> {
    let removed = match all {
        true => std::fs::remove_dir_all(path),
        false => std::fs::remove_dir(path),
    };
    match removed {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// A share's copy being taken off this device, on the account's thread.
pub(super) struct Ending {
    pub(super) sharer: Identity,
    /// Where its folder is.
    pub(super) folder: PathBuf,
    pub(super) thread: std::thread::JoinHandle<Result<Ended, RunnerError>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::Drive;
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    use super::super::feed::{Notifications, Puller};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// What `me` reads of a sharer's (empty) drive, on the state DB `db`.
    fn share(me: &Identity, db: &Path) -> Result<Share, Box<dyn std::error::Error>> {
        let server = FakeServer::new(ServerConfig::default());
        let drive = Drive::open(db, me, Arc::new(FakeRemote::member(server)))?;
        let puller = Puller::spawn(Arc::clone(&drive));
        Ok(Share {
            sharer: "tom.example".into(),
            drive,
            db: db.to_path_buf(),
            notifications: Notifications::new(puller, std::time::Instant::now()),
        })
    }

    /// Only the folder set up for the share's own records is touched: one that carries
    /// another's marker (renamed into its place) is left as it is, with all in it, and the
    /// share's records stay.
    #[test]
    fn a_folder_of_other_records_is_never_emptied() -> R {
        let home = tempfile::tempdir()?;
        let me: Identity = "me.example".into();
        let (folder, out) = (
            home.path().join("Shared/tom.example"),
            home.path().join("out"),
        );
        std::fs::create_dir_all(&folder)?;
        std::fs::create_dir_all(&out)?;
        // Set up for other records than the share's.
        let (others, db) = (
            home.path().join("other/state.db"),
            home.path().join("tom/state.db"),
        );
        for db in [&others, &db] {
            std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        }
        drop(share(&me, &others)?);
        SyncRoot::create(&others, &me, &folder)?;
        std::fs::write(folder.join("theirs.txt"), "of another share")?;

        let stop = Arc::new(AtomicBool::new(false));
        let ended = end(share(&me, &db)?, &me, "pc", &folder, &out, stop);
        assert!(ended.is_err(), "{ended:?}");
        assert_eq!(
            std::fs::read_to_string(folder.join("theirs.txt"))?,
            "of another share"
        );
        assert!(folder.join(STATE_DIR).is_dir() && db.exists());
        assert!(out.read_dir()?.next().is_none(), "nothing moved out");
        Ok(())
    }

    /// A folder that is gone already leaves only the share's records to remove.
    #[test]
    fn a_copy_gone_already_leaves_its_records_to_remove() -> R {
        let home = tempfile::tempdir()?;
        let me: Identity = "me.example".into();
        let (folder, out) = (
            home.path().join("Shared/tom.example"),
            home.path().join("out"),
        );
        let db = home.path().join("tom/state.db");
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        let stop = Arc::new(AtomicBool::new(false));
        let ended = end(share(&me, &db)?, &me, "pc", &folder, &out, stop)?;
        assert_eq!(
            ended,
            Ended {
                removed: true,
                ..Ended::default()
            }
        );
        assert!(!db.exists());
        Ok(())
    }
}
