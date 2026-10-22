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
use hbsync_types::{NodeId, Tags};

use super::{FailPoint, Hooks};
use crate::folder::FolderCtx;
use crate::trash::Trash;

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
    /// [`Disk::held_after`]). Taken for not known while it can be read, the item read as
    /// holding the drive's tags, and the next scan took their absence for the user removing
    /// them.
    pub(super) held: Option<Tags>,
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
    /// The folder's trash, read when a download first asks this run ([`Disk::in_trash`]).
    trash: Option<Trash>,
    /// The aside folder is there ([`Disk::move_aside`]), as far as this run knows.
    aside_ready: bool,
}

impl<'a> Disk<'a> {
    pub(super) fn new(folder: &'a FolderCtx, hooks: &'a Hooks) -> Self {
        Disk {
            root: &folder.root,
            folder,
            hooks,
            ids_durable: false,
            trash: None,
            aside_ready: false,
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
                    held: None,
                    failed: false,
                }
            }
            Ok(tags::Written::Done(read)) => {
                let _ = self.folder.tags_kept.set(true);
                TagsLeft {
                    claimed: read.clone(),
                    held: read,
                    failed: false,
                }
            }
            Err(_) => TagsLeft {
                claimed: None,
                held: (self.tags_kept() && !self.fails(FailPoint::TagRead))
                    .then(|| tags::read(path))
                    .flatten(),
                failed: true,
            },
        }
    }

    /// Whether a tag write failed and what it left couldn't be read back, on a volume that
    /// keeps tags.
    pub(super) fn unread(&self, left: &TagsLeft) -> bool {
        left.failed && left.held.is_none() && self.tags_kept()
    }

    /// What a tag write left on an item that held `before`, for its row: what read back, or
    /// where the write failed and that read failed too, `before`, as a failed write changes
    /// nothing. Not known read as the drive's tags: a commit settled its base on them, and the
    /// scan that then read the item without them sent their removal up.
    pub(super) fn held_after(&self, left: TagsLeft, before: Tags) -> Option<Tags> {
        if self.unread(&left) {
            Some(before)
        } else {
            left.held
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

    // ---- the folder's trash ----

    /// Moves a local item into `.hbsync/trash/<time>-<node>/`.
    pub(super) fn move_to_trash(
        &mut self,
        db: &mut StateDb,
        path: &RelPath,
        node: NodeId,
    ) -> Result<io::Result<PathBuf>, StateError> {
        let trash_dir = layout::trash_dir(self.root);
        // The trash folder too, made with the first item in it.
        db.changed(&trash_dir);
        // Its own entry folder, named by when and by the node (one name: a native path). The
        // node trashed again within that second (the old version a download kept, then the
        // file itself, deleted) finds its entry taken: it takes the next second's, so the
        // newer stays the newer. Only the entry itself taken counts (`Ok(false)`).
        let mut at = hbsync_types::unix_now();
        let dir = loop {
            let dir = trash_dir.join(layout::trash_entry(at, node));
            let made = put_ordered(db, &dir, |dir| match fs::create_dir(dir) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir_all(dir).map(|()| true)
                }
                Err(e) => Err(e),
            })?;
            match made {
                Ok(true) => break dir,
                Ok(false) => at += 1,
                Err(e) => return Ok(Err(e)),
            }
        };
        let to = dir.join(path.name().unwrap_or("item"));
        let moved = rename_ordered(db, &self.abs(path), &to)?.map(|()| to);
        match (&moved, &mut self.trash) {
            (Ok(_), Some(trash)) => trash.add(node, dir),
            (Ok(_), None) => {}
            // Its entry, empty, goes (best effort: the purge takes one left).
            (Err(_), _) => drop(fs::remove_dir(&dir)),
        }
        Ok(moved)
    }

    /// Files of the folder's trash that may hold the bytes a download of `node` (in `parent`)
    /// to `target` wants: where its own entry, or the entry of a folder above it, holds it
    /// under the names `target` has (a delete taken back); its own entries first, then each
    /// folder's above it, each newest first. Only candidates: the download takes one only once
    /// it read exactly the bytes wanted ([`hbsync_fileio::CompletedFile::adopt`]). Nothing to
    /// look up while the trash is empty, or for a node one was refused for this run
    /// ([`Disk::refuse_from_trash`]).
    pub(super) fn in_trash(
        &mut self,
        db: &mut StateDb,
        node: NodeId,
        parent: NodeId,
        target: &RelPath,
    ) -> Result<Vec<PathBuf>, StateError> {
        let root = self.root;
        let trash = self
            .trash
            .get_or_insert_with(|| Trash::read(&layout::trash_dir(root)));
        if trash.is_empty() || trash.refused(node) {
            return Ok(Vec::new());
        }
        let names: Vec<&str> = target.components().collect();
        // The folders above it, as far as `target` names them.
        let mut left = names.len().saturating_sub(1);
        let above = db.read(|s| {
            crate::walk::up::<_, StateError>(parent, |at| {
                if left == 0 {
                    return Ok(None);
                }
                left -= 1;
                Ok(s.get_mirror(at)?.map(|m| (at, m.parent)))
            })
        })?;
        let mut found = Vec::new();
        for (i, at) in (0..names.len())
            .rev()
            .zip(std::iter::once(node).chain(above.items))
        {
            let entries = trash.entries(at);
            if entries.is_empty() {
                continue;
            }
            // Its place in the entry: its name there, and the names below it to the item.
            let Ok(place) = RelPath::new(&names[i..].join("/")) else {
                continue;
            };
            found.extend(entries.iter().map(|e| place.to_native(e)));
        }
        Ok(found)
    }

    /// A file taken from the trash didn't do for `node` (its tags weren't written): its
    /// download fetches the bytes for the rest of the run.
    pub(super) fn refuse_from_trash(&mut self, node: NodeId) {
        if let Some(trash) = &mut self.trash {
            trash.refuse(node);
        }
    }

    /// Brings an old version a download just put in the trash (at `entry`) back to `path`, and
    /// removes its entry's folder, empty then (one that can't be removed, not empty after
    /// all, stays for the daily purge).
    pub(super) fn restore_from_trash(
        &self,
        db: &mut StateDb,
        entry: &Path,
        path: &RelPath,
    ) -> Result<io::Result<()>, StateError> {
        let back = rename_ordered(db, entry, &self.abs(path))?;
        if back.is_ok()
            && let Some(dir) = entry.parent()
        {
            drop(fs::remove_dir(dir));
        }
        Ok(back)
    }
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
