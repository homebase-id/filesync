//! The sync root's one writer: every change the executor (and recovery) makes to a folder's
//! entries goes through [`Disk`], in the order the state DB needs with its durable commits (its
//! docs on durability), and so do the tags and creation times it writes on items (a
//! download's in its [`Disk::put`]). Those aren't
//! ordered: one lost reads as a change of those only. It also owns the run's durable commits,
//! which order the DB against the disk and the drive: write-ahead records before what they
//! record, and the scan's new ids before anything goes to the drive.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hbsync_local::layout;
use hbsync_platform::{RelPath, ops, tags};
use hbsync_state::{FolderTx, StateDb, StateError};
use hbsync_types::Tags;

use super::{FailPoint, Hooks};
use crate::folder::FolderCtx;

/// What an item to unlink is ([`Disk::unlink`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Unlink {
    File,
    /// Empty by then: one that isn't stays.
    Folder,
    /// Disposable (§10): with whatever is in it.
    Junk,
}

/// What a tag write left on an item ([`Disk::write_tags`]).
#[derive(Debug, Clone)]
pub(super) struct TagsLeft {
    /// What read back, for a placement's record to claim (`None`: not known: the write failed,
    /// read back as nothing to go by, or the volume keeps none).
    pub(super) claimed: Option<Tags>,
    /// What the item holds: what read back, or where the write failed, what it holds as read
    /// (`None` where that can't be read either: its row takes what it held before then,
    /// [`Disk::on_item_after`]). Taken for not known while it can be read, the item read as
    /// holding the drive's tags, and the next scan took their absence for the user removing
    /// them.
    pub(super) on_item: Option<Tags>,
    pub(super) failed: bool,
}

/// The disk of one sync root, as a run (or recovery) writes it.
#[derive(Debug)]
pub(super) struct Disk<'a> {
    root: &'a Path,
    /// The folder (its root, whether its volume keeps tags, once a tag write found out).
    folder: &'a FolderCtx,
    /// Test faults.
    hooks: &'a Hooks,
    /// The ids the scan gave new items are durable: before anything goes to the drive, they
    /// are (a power cut must not make the scan give others to what went out).
    ids_durable: bool,
    /// The aside folder is there ([`Disk::move_aside`]), as far as this run knows.
    aside_ready: bool,
    /// What the check of the folder things move out to found, this run ([`Disk::out_folder`]).
    out_checked: Option<Result<(), String>>,
}

impl<'a> Disk<'a> {
    pub(super) fn new(folder: &'a FolderCtx, hooks: &'a Hooks) -> Self {
        Disk {
            root: &folder.root,
            folder,
            hooks,
            ids_durable: false,
            aside_ready: false,
            out_checked: None,
        }
    }

    /// Whether test fault `point` fails now.
    fn fails(&self, point: FailPoint) -> bool {
        self.hooks.fails(point)
    }

    pub(super) fn root(&self) -> &'a Path {
        self.root
    }

    pub(super) fn folder(&self) -> &'a FolderCtx {
        self.folder
    }

    /// The native path of `path`.
    pub(super) fn abs(&self, path: &RelPath) -> PathBuf {
        path.to_native(self.root)
    }

    // ---- durable commits ----

    /// A durable commit (with every commit before): a write-ahead record of what is about to
    /// happen on disk or on the drive, or a change that must not be lost.
    pub(super) fn write_durable(
        &mut self,
        db: &mut StateDb,
        f: impl FnOnce(FolderTx<'_>) -> Result<(), StateError>,
    ) -> Result<(), StateError> {
        db.write_durable(|s| f(s.folder()))?;
        self.ids_durable = true;
        Ok(())
    }

    /// Makes the ids the scan gave new items durable, once a run: before anything goes to the
    /// drive.
    pub(super) fn flush_ids(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        if !self.ids_durable {
            db.flush()?;
            self.ids_durable = true;
        }
        Ok(())
    }

    // ---- the folder's entries ----

    /// Puts something at `path` on disk with `put` ([`put_ordered`]).
    pub(super) fn put<T>(
        &self,
        db: &mut StateDb,
        path: &RelPath,
        put: impl FnOnce(&Path) -> T,
    ) -> Result<T, StateError> {
        put_ordered(db, &self.abs(path), put)
    }

    /// Renames `from` to `to` on disk, never over anything ([`rename_ordered`]).
    pub(super) fn rename(
        &self,
        db: &mut StateDb,
        from: &RelPath,
        to: &RelPath,
    ) -> Result<io::Result<()>, StateError> {
        rename_ordered(db, &self.abs(from), &self.abs(to))
    }

    /// The folder a read-only folder's changes move out to (§13), checked once a run before
    /// it is made where it is missing: a full path without `..`, outside the root as the file
    /// system resolves both (else its scan would find what moved out as new items here), on
    /// the root's volume. Why not, where it isn't one.
    pub(super) fn out_folder(&mut self, db: &mut StateDb) -> Result<&'a Path, String> {
        let out = self
            .folder
            .read_only
            .as_deref()
            .ok_or("the folder isn't read-only")?;
        let checked = self
            .out_checked
            .get_or_insert_with(|| check_out(out, self.root, db));
        checked.clone().map(|()| out)
    }

    /// Moves `from` out of the sync root, to `to`, never over anything ([`rename_ordered`]).
    pub(super) fn move_out(
        &self,
        db: &mut StateDb,
        from: &RelPath,
        to: &Path,
    ) -> Result<io::Result<()>, StateError> {
        // Never onto what is there, even the same file (a hard link of it), which a rename
        // keeps as it is.
        if to.symlink_metadata().is_ok() {
            return Ok(Err(io::ErrorKind::AlreadyExists.into()));
        }
        rename_ordered(db, &self.abs(from), to)
    }

    // ---- what is written on items: tags, creation times ----

    /// Whether the root's volume keeps tags, as far as known (this OS keeps some, and no tag
    /// write found it doesn't).
    pub(super) fn tags_kept(&self) -> bool {
        tags::CAN_TAG && self.folder.tags_kept.get() != Some(&false)
    }

    /// Writes `tags` on the item at `path` ([`tags::write_on`]), noting whether the volume
    /// keeps tags, and says what that left ([`TagsLeft`]).
    pub(super) fn write_tags(&self, path: &Path, tags: &Tags) -> TagsLeft {
        let written = match self.fails(FailPoint::TagWrite) {
            true => Err(io::Error::other("its tag write failed (a test)")),
            false => tags::write_on(path, tags),
        };
        match written {
            Ok(tags::Written::Unsupported) => {
                let _ = self.folder.tags_kept.set(false);
                TagsLeft {
                    claimed: None,
                    on_item: None,
                    failed: false,
                }
            }
            Ok(tags::Written::Done(read)) => {
                let _ = self.folder.tags_kept.set(true);
                TagsLeft {
                    claimed: read.clone(),
                    on_item: read,
                    failed: false,
                }
            }
            Err(_) => TagsLeft {
                claimed: None,
                on_item: (self.tags_kept() && !self.fails(FailPoint::TagRead))
                    .then(|| tags::read(path))
                    .flatten(),
                failed: true,
            },
        }
    }

    /// Whether a tag write failed and what it left couldn't be read back, on a volume that
    /// keeps tags.
    pub(super) fn unread(&self, left: &TagsLeft) -> bool {
        left.failed && left.on_item.is_none() && self.tags_kept()
    }

    /// What a tag write left on an item that held `before`, for its row: what read back, or
    /// where the write failed and that read failed too, `before`, as a failed write changes
    /// nothing. Not known read as the drive's tags: a commit settled its base on them, and the
    /// scan that then read the item without them sent their removal up.
    pub(super) fn on_item_after(&self, left: TagsLeft, before: Tags) -> Option<Tags> {
        if self.unread(&left) {
            Some(before)
        } else {
            left.on_item
        }
    }

    /// Sets the creation time of the item at `path` (Unix ms), where the system lets it: only
    /// shown, never synced on, so a file system that won't take it is no failure.
    pub(super) fn set_created(&self, path: &Path, created_ms: i64) {
        let _ = ops::set_created(path, created_ms);
    }

    // ---- items being deleted, aside ----

    /// Moves the item at `path` into the folder's aside folder ([`layout::aside_dir`]) as
    /// `entry`, one rename on its volume, and returns where it is now. One of that name there
    /// already (left by a crash, for the next sync's recovery) is never renamed over: the
    /// rename fails ([`io::ErrorKind::AlreadyExists`]).
    pub(super) fn move_aside(
        &mut self,
        db: &mut StateDb,
        path: &RelPath,
        entry: layout::AsideEntry,
    ) -> Result<io::Result<PathBuf>, StateError> {
        let dir = layout::aside_dir(self.root);
        // Made with the first item it takes (once a run asks): then the state folder changed.
        if !self.aside_ready {
            match fs::create_dir(&dir) {
                Ok(()) => db.changed(&dir),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Ok(Err(e)),
            }
            self.aside_ready = true;
        }
        let to = dir.join(entry.name());
        Ok(rename_ordered(db, &self.abs(path), &to)?.map(|()| to))
    }

    /// Renames `from` (an item aside) to `path`, never over anything.
    pub(super) fn rename_from(
        &self,
        db: &mut StateDb,
        from: &Path,
        path: &RelPath,
    ) -> Result<io::Result<()>, StateError> {
        rename_ordered(db, from, &self.abs(path))
    }

    /// Unlinks what is at `at`, as `what` says it is. The caller notes the folder it was in as
    /// changed ([`StateDb::changed`]).
    pub(super) fn unlink(&self, at: &Path, what: Unlink) -> io::Result<()> {
        match what {
            Unlink::File => fs::remove_file(at),
            Unlink::Folder => fs::remove_dir(at),
            Unlink::Junk => match fs::symlink_metadata(at)?.is_dir() {
                true => fs::remove_dir_all(at),
                false => fs::remove_file(at),
            },
        }
    }
}

/// The volume `path` itself is on (a link not followed), where it can be read.
fn volume_of(path: &Path) -> Option<u64> {
    hbsync_platform::stat(path).map(|m| m.file_id.volume).ok()
}

/// Why the out folder is refused, where it is in the folder (or not a full path).
const OUT_INSIDE: &str = "the folder changes move out to is inside this one, or not a full path";

/// Why the out folder is refused, where it is on another volume than the folder.
const OUT_ELSEWHERE: &str = "the folder changes move out to is on another volume";

/// [`Disk::out_folder`]'s check of `out` against the sync root `root`, made where it is
/// missing once it passed (the folders made noted in `db` for the durable commits).
fn check_out(out: &Path, root: &Path, db: &mut StateDb) -> Result<(), String> {
    let plain = out.is_absolute()
        && !out
            .components()
            .any(|c| c == std::path::Component::ParentDir);
    if !plain || resolved(out).starts_with(resolved(root)) {
        return Err(OUT_INSIDE.into());
    }
    // On the root's volume (a move out is one rename), as its nearest folder that is there,
    // links followed.
    let volume = |p: &Path| volume_of(&resolved(p));
    let there = out.ancestors().find(|p| p.exists()).unwrap_or(out);
    if volume(there) != volume(root) {
        return Err(OUT_ELSEWHERE.into());
    }
    // Nor is the root one of its folders by another path (a bind mount): told by file id.
    let id = |p: &Path| hbsync_platform::stat(p).map(|m| m.file_id).ok();
    let root_id = id(&resolved(root));
    if root_id.is_some() && there.ancestors().any(|p| id(p) == root_id) {
        return Err(OUT_INSIDE.into());
    }
    // Each folder it makes is noted as it is made, so its entry is synced before a commit
    // that relies on what moved into it (a failure part way keeps those made noted).
    let mut missing: Vec<&Path> = out.ancestors().take_while(|p| !p.exists()).collect();
    missing.reverse();
    for made in missing {
        match fs::create_dir(made) {
            Ok(()) => db.changed(made),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && made.is_dir() => {}
            Err(e) => return Err(format!("{}: {e}", made.display())),
        }
    }
    if !out.is_dir() {
        return Err(format!("{}: not a folder", out.display()));
    }
    // A rename into it works: one on another mount of the root's file system (a bind mount
    // shares its volume) can't take one. Tried once a run, with a folder made in the root's
    // tmp, renamed there and removed (where none can be made, it is taken as working).
    let probe = format!(".hbsync-probe-{}", hbsync_types::NodeId::new());
    let (from, to) = (layout::tmp_dir(root).join(&probe), out.join(&probe));
    let made = fs::create_dir_all(layout::tmp_dir(root)).and_then(|()| fs::create_dir(&from));
    if made.is_ok() {
        let moved = fs::rename(&from, &to);
        let _ = fs::remove_dir(if moved.is_ok() { &to } else { &from });
        if moved.is_err_and(|e| e.kind() == io::ErrorKind::CrossesDevices) {
            return Err(OUT_ELSEWHERE.into());
        }
    }
    Ok(())
}

/// `path` as the file system resolves it: its nearest folder that is there resolved (links,
/// `..`, the volume's own spelling), the rest as given.
fn resolved(path: &Path) -> PathBuf {
    let mut rest = Vec::new();
    for at in path.ancestors() {
        if let Ok(real) = fs::canonicalize(at) {
            return rest
                .iter()
                .rev()
                .fold(real, |p: PathBuf, name| p.join(name));
        }
        if let Some(name) = at.file_name() {
            rest.push(name.to_owned());
        }
    }
    path.to_path_buf()
}

/// Puts something at `at` on disk with `put`: once the place can take it
/// ([`StateDb::filling`]), its folder noted as changed after.
fn put_ordered<T>(
    db: &mut StateDb,
    at: &Path,
    put: impl FnOnce(&Path) -> T,
) -> Result<T, StateError> {
    db.filling(at)?;
    let out = put(at);
    db.changed(at);
    Ok(out)
}

/// Renames `from` to `to` on disk, never over anything ([`put_ordered`]); `from` is noted as
/// emptied.
fn rename_ordered(db: &mut StateDb, from: &Path, to: &Path) -> Result<io::Result<()>, StateError> {
    let renamed = put_ordered(db, to, |to| ops::rename_noreplace(from, to))?;
    if renamed.is_ok() {
        db.emptied(from);
    }
    Ok(renamed)
}
