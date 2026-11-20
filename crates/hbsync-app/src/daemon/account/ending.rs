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
use hbsync_local::{Filter, is_junk, is_state_dir, layout};

use super::parts::Share;
use crate::Identity;

/// What became of a share's copy ([`end`]).
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Ended {
    /// How many items moved out to the main folder: changed or put there, or never synced.
    pub(super) moved_out: usize,
    /// What is left in its folder, by name: what couldn't be deleted or moved.
    pub(super) left: Vec<String>,
    /// Stopped before the end: taken up again later.
    pub(super) stopped: bool,
    /// Why it couldn't be done, or not all of it.
    pub(super) failed: Option<String>,
    /// Its folder is gone from the folder of shares (removed now, or no more there): what
    /// failed after that left only the share's records.
    pub(super) folder_gone: bool,
}

impl Ended {
    /// Whether all of the copy went, the share's records with it.
    pub(super) fn all(&self) -> bool {
        self.left.is_empty() && !self.stopped && self.failed.is_none()
    }
}

/// Where a share's copy is, and what its ending goes by ([`end`]).
#[derive(Debug, Clone)]
pub(super) struct Place {
    pub(super) identity: Identity,
    pub(super) device: String,
    /// The share's folder, in the folder of shares.
    pub(super) folder: PathBuf,
    /// The account's main folder: where what isn't the sharer's moves out to.
    pub(super) out: PathBuf,
    /// Where this account keeps the share's state DB ([`crate::Paths::shared_db`]).
    pub(super) records: PathBuf,
}

/// Takes `share`'s copy at `place` off this device, what isn't the sharer's moving out to
/// the account's main folder, and removes the share's state DB. Stopped between operations
/// once `stop` is set: what is left then is taken up later.
///
/// Before anything is touched: the share's drive is another identity's and its DB is where
/// this account keeps that sharer's (nothing else is ever removed as "its records"); its
/// folder is in the folder of shares on the main folder's record; and it is a folder, not a
/// link to one. Then only a folder set up for this share's DB (its marker) is emptied: one
/// whose marker names another is left as it is. A folder that isn't there leaves only the
/// DB to remove. What was done, and why not all of it, where it wasn't.
pub(super) fn end(share: Share, place: &Place, stop: Arc<AtomicBool>) -> Ended {
    let mut ended = Ended::default();
    match take_off(share, place, stop, &mut ended) {
        Ok(()) => {}
        Err(why) => ended.failed = Some(why),
    }
    ended
}

fn take_off(
    share: Share,
    place: &Place,
    stop: Arc<AtomicBool>,
    ended: &mut Ended,
) -> Result<(), String> {
    // Its puller's thread ends first (joined here, not on the account's thread), and lets
    // go of the drive: the share's DB is closed once `drive` is dropped below, which
    // removing its file takes on Windows.
    let Share {
        drive,
        db,
        notifications,
        ..
    } = share;
    notifications.finish();
    drop(notifications);
    let (folder, out) = (place.folder.as_path(), place.out.as_path());
    let said = |e: &dyn std::fmt::Display| e.to_string();
    if !drive.shared() || db != place.records {
        return Err("its records aren't those of what is shared with this account".into());
    }
    let dir = folder.parent().unwrap_or(folder);
    if !is_shares_folder(out, dir).map_err(|e| said(&e))? {
        return Err(format!("{} isn't the folder of shares now", dir.display()));
    }
    match std::fs::symlink_metadata(folder) {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => return Err("it isn't a folder (a link to one is never followed)".into()),
        // Gone already (removed by hand, or by an ending cut short after its last step):
        // only its records are left.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            ended.folder_gone = true;
            drop(drive);
            return remove_records(&db);
        }
        Err(e) => return Err(said(&e)),
    }
    // Its state folder is a folder too: through a link there the marker would be read, and
    // what is the client's own removed, wherever it leads.
    match std::fs::symlink_metadata(layout::state_dir(folder)) {
        Ok(meta) if !meta.is_dir() => {
            return Err("its state folder isn't a folder (a link to one is never followed)".into());
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(said(&e)),
    }
    // Whether the folder is proven this share's by its marker (and the engine went over it).
    let mut proven = false;
    match SyncRoot::open(&db, &place.identity, folder) {
        Ok(root) => {
            proven = true;
            let mut cfg = EngineConfig::new(folder, &place.device);
            let filter = Filter::load(folder, cfg.folder.case_insensitive);
            cfg.folder.filter = Arc::new(filter.map_err(|e| said(&e))?);
            cfg.folder.read_only = Some(out.to_path_buf());
            let mut engine = Engine::open(&root, Arc::clone(&drive), cfg).map_err(|e| said(&e))?;
            engine.set_stop(stop);
            let done = engine.end_share(&crate::today());
            // (What it did before it failed moved out all the same: told.)
            let report = match &done {
                Ok(report) => Some(report.moved_out.len()),
                Err(_) => engine.take_unreported().map(|r| r.moved_out.len()),
            };
            ended.moved_out = report.unwrap_or(0);
            ended.stopped = done.map_err(|e| said(&e))?.stopped;
        }
        // No marker: its own went with the last step of an ending that was cut short
        // there, or the account made the folder and its runner never set it up, or another
        // folder is at its place. Nothing proves it this share's: nothing in it is deleted
        // below, and only an empty folder goes.
        Err(RootError::NoMarker(_)) => {}
        Err(e) => return Err(said(&e)),
    }
    if ended.stopped {
        return Ok(());
    }
    let (left, mut junk) = left_in(folder, proven).map_err(|e| said(&e))?;
    ended.left = left;
    // (Without its marker nothing in it is deleted, junk neither: only an empty folder
    // goes.)
    if !proven {
        let names = junk.drain(..).filter_map(|path| {
            let name = path.file_name()?.to_string_lossy().into_owned();
            Some(name)
        });
        ended.left.extend(names);
        ended.left.sort();
    }
    if !ended.left.is_empty() {
        return Ok(());
    }
    // Nothing is left: junk goes with the folder (as in any delete: sync semantics §7),
    // then the client's own state, piece by piece (nothing in it is removed that wasn't
    // looked at above), then the folder. Something put there in this moment keeps the
    // folder, without its junk and its marker: found again by its DB, which stays.
    for path in junk {
        gone(std::fs::remove_file(path)).map_err(|e| said(&e))?;
    }
    remove_own_state(folder, proven).map_err(|e| said(&e))?;
    remove(folder, false).map_err(|e| said(&e))?;
    ended.folder_gone = true;
    drop(drive);
    remove_records(&db)
}

/// The client's own in the state folder of `folder`, which is `proven` this share's by its
/// marker: the marker, what a transfer left, and where it sets aside.
fn own_state(folder: &Path) -> [PathBuf; 3] {
    [
        layout::marker_file(folder),
        layout::tmp_dir(folder),
        layout::aside_dir(folder),
    ]
}

/// Removes the state folder of `folder`, which [`left_in`] found to hold nothing but the
/// client's own (its junk and probes are gone by now): what a transfer left (with all in
/// it: partial downloads), the place it sets aside in (empty, or it would have counted as
/// left: removed only as an empty folder), and last its marker, so that what fails or is
/// cut short before leaves the folder proven this share's, and the next try finishes it;
/// then the state folder itself, empty: what appeared in it since keeps it. Without the
/// marker (`!proven`) it holds nothing, and only goes empty.
fn remove_own_state(folder: &Path, proven: bool) -> std::io::Result<()> {
    if proven {
        let [marker, tmp, aside] = own_state(folder);
        gone(std::fs::remove_dir_all(tmp))?;
        gone(std::fs::remove_dir(aside))?;
        gone(std::fs::remove_file(marker))?;
    }
    remove(&layout::state_dir(folder), false)
}

/// Whether `name`, in a state folder, is a probe of the volume the client made there (one
/// a crash left: `hbsync_platform::fskind`'s).
fn own_probe(name: &str) -> bool {
    name.starts_with(".hbsync-case-probe-") || name.starts_with(".hbsync-mtime-probe-")
}

/// `removed`, where what was to go is gone: also where it wasn't there.
fn gone(removed: std::io::Result<()>) -> std::io::Result<()> {
    match removed {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Whether `dir` is the folder of shares of the main folder `main`, by that one's record,
/// and a folder itself: not a link to it (where a volume tells no file ids the record goes
/// by the marker in it, which a link would lead to). What a share is synced into, and what
/// its copy is taken out of, is asked here.
pub(super) fn is_shares_folder(main: &Path, dir: &Path) -> std::io::Result<bool> {
    if !std::fs::symlink_metadata(dir).is_ok_and(|meta| meta.is_dir()) {
        return Ok(false);
    }
    hbsync_local::shares::Record::read(main)?.is(dir)
}

/// Removes a share's state DB at `db`, with the directory this account keeps it in
/// ([`crate::Paths::shared_dir`]: the sharer's token, kept beside it, goes with it).
fn remove_records(db: &Path) -> Result<(), String> {
    let dir = db.parent().ok_or("its records have no place")?;
    remove(dir, true).map_err(|e| e.to_string())
}

/// What `folder` still holds, by name: at its top; what the client set aside there (§8a,
/// left for the user: every entry, read with its errors told, as what isn't seen here is
/// removed with the folder); and what is in its state folder that isn't the client's own
/// (the user's ignore rules where they couldn't move out, a backup an editor left beside
/// them): with the folder `proven` this share's by its marker, the client's own are
/// [`own_state`]; without, nothing is. And the junk files at its top and in its state
/// folder (a `.DS_Store` a file manager left, a probe of the volume a crash left), which go
/// with the folder when nothing else is left.
fn left_in(folder: &Path, proven: bool) -> std::io::Result<(Vec<String>, Vec<PathBuf>)> {
    let (mut left, mut junk) = (Vec::new(), Vec::new());
    let state = layout::state_dir(folder);
    // (The folder's own state, under whatever spelling the volume shows it: another folder
    // of such a name, where names differ by case, is the user's.)
    let is_state = |path: &Path| match (path.canonicalize(), state.canonicalize()) {
        (Ok(path), Ok(state)) => path == state,
        _ => false,
    };
    // A folder's entries, or none where it isn't there; any other error is told.
    let entries = |dir: &Path| match std::fs::read_dir(dir) {
        Ok(entries) => Ok(Some(entries)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    };
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        // (A name that isn't text is nobody's junk: the user's, left.)
        let name = entry.file_name();
        let text = name.to_str();
        if text.is_some_and(is_state_dir) && is_state(&entry.path()) {
            continue;
        }
        match text.is_some_and(is_junk) && !entry.file_type()?.is_dir() {
            true => junk.push(entry.path()),
            false => left.push(name.to_string_lossy().into_owned()),
        }
    }
    for entry in entries(&layout::aside_dir(folder))?.into_iter().flatten() {
        left.push(layout::aside_shown(&entry?.file_name().to_string_lossy()));
    }
    let own = own_state(folder);
    for entry in entries(&state)?.into_iter().flatten() {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if proven && own.contains(&entry.path()) {
            continue;
        }
        // (Junk a file manager left there, a probe a crash left: with the folder.)
        let disposable = is_junk(&name) || own_probe(&name);
        match proven && disposable && !entry.file_type()?.is_dir() {
            true => junk.push(entry.path()),
            false => left.push(format!("{}/{name}", hbsync_local::STATE_DIR)),
        }
    }
    left.sort();
    left.dedup();
    Ok((left, junk))
}

/// Removes the folder at `path`, with all in it when `all` (no link in it is followed);
/// one that isn't there is removed.
fn remove(path: &Path, all: bool) -> std::io::Result<()> {
    gone(match all {
        true => std::fs::remove_dir_all(path),
        false => std::fs::remove_dir(path),
    })
}

/// A share's copy being taken off this device, on the account's thread.
pub(super) struct Ending {
    pub(super) sharer: Identity,
    pub(super) place: Place,
    /// Set by its thread before it wakes the account: its result is ready to take (the
    /// thread itself may not have ended yet as the account looks).
    pub(super) done: Arc<AtomicBool>,
    pub(super) thread: std::thread::JoinHandle<Ended>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_engine::Drive;
    use hbsync_local::shares::{Made, make};
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// What `me` reads of a sharer's (empty) drive, on the state DB `db`; or (`own`) its own.
    fn share(me: &Identity, db: &Path, own: bool) -> Result<Share, Box<dyn std::error::Error>> {
        let server = FakeServer::new(ServerConfig::default());
        let remote = match own {
            true => FakeRemote::new(server),
            false => FakeRemote::member(server),
        };
        std::fs::create_dir_all(db.parent().ok_or("no parent")?)?;
        let drive = Drive::open(db, me, Arc::new(remote))?;
        Ok(Share::new(&"tom.example".into(), drive, db.to_path_buf()))
    }

    /// The share's records made (its DB opened once), with nothing left open.
    fn made(me: &Identity, db: &Path) -> R {
        let share = share(me, db, false)?;
        share.notifications.finish();
        Ok(())
    }

    /// A main folder under `home` with its folder of shares, and where tom's share is.
    fn place(home: &Path) -> Result<Place, Box<dyn std::error::Error>> {
        let out = home.join("Homebase");
        std::fs::create_dir_all(&out)?;
        assert_eq!(make(&out, "Shared")?, Made::Made);
        Ok(Place {
            identity: "me.example".into(),
            device: "pc".into(),
            folder: out.join("Shared").join("tom.example"),
            out,
            records: home.join("state/shared/tom/state.db"),
        })
    }

    fn end_now(share: Share, place: &Place) -> Ended {
        end(share, place, Arc::new(AtomicBool::new(false)))
    }

    /// Only the folder set up for the share's own records is touched: one that carries
    /// another's marker (renamed into its place) is left as it is, with all in it, and the
    /// share's records stay.
    #[test]
    fn a_folder_of_other_records_is_never_emptied() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        std::fs::create_dir_all(&place.folder)?;
        // Set up for other records than the share's.
        let others = home.path().join("state/shared/other/state.db");
        made(&place.identity, &others)?;
        SyncRoot::create(&others, &place.identity, &place.folder)?;
        std::fs::write(place.folder.join("theirs.txt"), "of another share")?;

        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.failed.is_some() && !ended.all(), "{ended:?}");
        let kept = std::fs::read_to_string(place.folder.join("theirs.txt"))?;
        assert_eq!(kept, "of another share");
        assert!(layout::state_dir(&place.folder).is_dir() && place.records.exists());
        assert!(!place.out.join("theirs.txt").exists(), "nothing moved out");
        Ok(())
    }

    /// A folder that is gone already leaves only the share's records to remove.
    #[test]
    fn a_copy_gone_already_leaves_its_records_to_remove() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        let gone = Ended {
            folder_gone: true,
            ..Ended::default()
        };
        assert_eq!(ended, gone);
        assert!(ended.all() && !place.records.exists());
        Ok(())
    }

    /// Nothing is removed as a share's records that isn't: a drive that is the account's
    /// own, or a DB that isn't where this account keeps that sharer's, is refused before
    /// anything is touched, the folder missing or not.
    #[test]
    fn records_that_arent_a_shares_are_never_removed() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        // The account's own drive, at the place of a share's records.
        let own = end_now(share(&place.identity, &place.records, true)?, &place);
        assert!(own.failed.is_some(), "{own:?}");
        assert!(place.records.exists());
        // A share's drive, but its DB somewhere else (beside something that isn't ours).
        let elsewhere = home.path().join("state/accounts/me/state.db");
        std::fs::create_dir_all(elsewhere.parent().ok_or("no parent")?)?;
        let beside = elsewhere.with_file_name("precious.txt");
        std::fs::write(&beside, "not a share's")?;
        let other = end_now(share(&place.identity, &elsewhere, false)?, &place);
        assert!(other.failed.is_some(), "{other:?}");
        assert!(elsewhere.exists() && beside.exists());
        Ok(())
    }

    /// A folder at the share's place that holds no marker (its own went with an ending cut
    /// short, or the user put another folder there) has nothing in it deleted, junk
    /// neither: it stays, with its records. Empty, it goes.
    #[test]
    fn nothing_is_deleted_from_a_folder_without_its_marker() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        std::fs::create_dir_all(&place.folder)?;
        // Set up, then its state gone: the DB still has the folder there.
        made(&place.identity, &place.records)?;
        SyncRoot::create(&place.records, &place.identity, &place.folder)?;
        std::fs::remove_dir_all(layout::state_dir(&place.folder))?;
        let download = place.folder.join("report.pdf.crdownload");
        std::fs::write(&download, "half of it")?;

        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert_eq!(ended.left, ["report.pdf.crdownload"], "{ended:?}");
        assert!(download.exists() && place.records.exists());

        std::fs::remove_file(&download)?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.all(), "{ended:?}");
        assert!(!place.folder.exists() && !place.records.exists());
        Ok(())
    }

    /// What is in the folder's state folder and isn't the client's own (a backup an editor
    /// left beside the ignore rules) is never deleted with it: it counts as left, and the
    /// folder stays, marker and all.
    #[test]
    fn the_users_own_in_the_state_folder_keeps_the_folder() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        std::fs::create_dir_all(&place.folder)?;
        made(&place.identity, &place.records)?;
        SyncRoot::create(&place.records, &place.identity, &place.folder)?;
        let backup = layout::state_dir(&place.folder).join("ignore~");
        std::fs::write(&backup, "*.log")?;

        // (And junk a file manager left there: the client's to take with the folder.)
        let finder = layout::state_dir(&place.folder).join(".DS_Store");
        std::fs::write(&finder, "junk")?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert_eq!(ended.left, [".hbsync/ignore~"], "{ended:?}");
        assert_eq!(std::fs::read_to_string(&backup)?, "*.log");
        assert!(layout::marker_file(&place.folder).exists() && place.records.exists());
        assert!(finder.exists(), "junk stays with a folder that stays");

        // What was set aside there is the user's too, whatever it is named.
        std::fs::remove_file(&backup)?;
        let aside = layout::aside_dir(&place.folder);
        std::fs::create_dir_all(&aside)?;
        std::fs::write(aside.join("kept"), "set aside")?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert_eq!(ended.left, [layout::aside_shown("kept")], "{ended:?}");
        assert_eq!(std::fs::read_to_string(aside.join("kept"))?, "set aside");

        // With neither, all of it goes, the junk with it.
        std::fs::remove_file(aside.join("kept"))?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.all(), "{ended:?}");
        assert!(!place.folder.exists() && !place.records.exists());
        Ok(())
    }

    /// A link at the place of the folder's state is never followed either: what it leads to
    /// (the state folder moved away, with this share's marker in it) is left as it is.
    #[cfg(unix)]
    #[test]
    fn a_link_at_the_state_folders_place_is_never_followed() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        std::fs::create_dir_all(&place.folder)?;
        made(&place.identity, &place.records)?;
        SyncRoot::create(&place.records, &place.identity, &place.folder)?;
        let (state, moved) = (
            layout::state_dir(&place.folder),
            home.path().join("state away"),
        );
        std::fs::rename(&state, &moved)?;
        std::fs::create_dir_all(moved.join("tmp"))?;
        std::fs::write(moved.join("tmp/half.partial"), "half")?;
        std::os::unix::fs::symlink(&moved, &state)?;

        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.failed.is_some(), "{ended:?}");
        assert!(moved.join("tmp/half.partial").exists() && place.records.exists());
        assert!(state.symlink_metadata()?.is_symlink());
        Ok(())
    }

    /// A folder the account made at the share's place that was never set up (no marker, and
    /// its DB knows no folder there) goes when it is empty, the share's records with it;
    /// with something in it, it is left as it is.
    #[test]
    fn a_folder_never_set_up_goes_only_empty() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        std::fs::create_dir_all(&place.folder)?;
        std::fs::write(place.folder.join("mine.txt"), "put here")?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert_eq!(ended.left, ["mine.txt"], "{ended:?}");
        assert!(place.folder.join("mine.txt").exists() && place.records.exists());

        std::fs::remove_file(place.folder.join("mine.txt"))?;
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.all(), "{ended:?}");
        assert!(!place.folder.exists() && !place.records.exists());
        Ok(())
    }

    /// A link at the share's place is never followed: what it leads to is left as it is,
    /// though it is the share's own folder, moved away, with its marker.
    #[cfg(unix)]
    #[test]
    fn a_link_at_the_shares_place_is_never_followed() -> R {
        let home = tempfile::tempdir()?;
        let place = place(home.path())?;
        let moved = home.path().join("moved away");
        std::fs::create_dir_all(&moved)?;
        made(&place.identity, &place.records)?;
        SyncRoot::create(&place.records, &place.identity, &moved)?;
        std::fs::write(moved.join("kept.txt"), "kept")?;
        std::os::unix::fs::symlink(&moved, &place.folder)?;

        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.failed.is_some(), "{ended:?}");
        assert_eq!(std::fs::read_to_string(moved.join("kept.txt"))?, "kept");
        assert!(layout::state_dir(&moved).is_dir() && place.records.exists());
        Ok(())
    }

    /// A folder that isn't in the folder of shares on record (that one renamed since) is
    /// neither emptied nor taken for gone: its records stay.
    #[test]
    fn a_place_that_isnt_in_the_folder_of_shares_is_left_alone() -> R {
        let home = tempfile::tempdir()?;
        let mut place = place(home.path())?;
        place.folder = place.out.join("Not shares").join("tom.example");
        let ended = end_now(share(&place.identity, &place.records, false)?, &place);
        assert!(ended.failed.is_some(), "{ended:?}");
        assert!(place.records.exists());
        Ok(())
    }
}
