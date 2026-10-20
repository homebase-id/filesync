//! Parking an item under a temporary name, and the names it gets back.

use super::*;

/// The tracked item that holds `target` on disk: the row at that exact path, or, on a
/// case-insensitive volume, a sibling whose name differs only in case or normalization.
pub(super) fn occupant(
    db: &mut StateDb,
    case_insensitive: bool,
    target: &RelPath,
) -> Result<Option<LocalRow>, StateError> {
    if let Some(row) = db.read(|s| s.get_local(target))? {
        return Ok(Some(row));
    }
    if !case_insensitive {
        return Ok(None);
    }
    let (Some(name), parent) = (target.name(), target.parent().unwrap_or_default()) else {
        return Ok(None);
    };
    let key = name_key(name);
    let siblings = db.read(|s| s.local_children(&parent))?;
    Ok(siblings
        .into_iter()
        .find(|r| r.path.name().is_some_and(|n| name_key(n) == key)))
}

/// Whether nothing holds `path`, on disk or in the DB.
pub(super) fn is_free(
    db: &mut StateDb,
    folder: &FolderCtx,
    path: &RelPath,
) -> Result<bool, StateError> {
    Ok(fs::symlink_metadata(path.to_native(&folder.root)).is_err()
        && occupant(db, folder.case_insensitive, path)?.is_none())
}

/// The first free numbered variant of `target`, one this device syncs if any is (§10). `None`
/// if all that are tried are taken.
pub(super) fn numbered_place(
    db: &mut StateDb,
    folder: &FolderCtx,
    target: &RelPath,
    is_dir: bool,
) -> Result<Option<RelPath>, StateError> {
    let (Some(name), dir) = (target.name(), target.parent().unwrap_or_default()) else {
        return Ok(None);
    };
    // Up to the first name the folder can't join (too long): the later ones are longer.
    let places = crate::tree::numbered_names(name).map_while(|n| dir.join(&n).ok());
    crate::tree::first_free(
        places,
        |p| is_free(db, folder, p),
        |p| folder.filter.path(p, is_dir).is_sync(),
    )
}

/// A real name for a parked item that leaves its temporary one: its `original` place if that is
/// free, else a numbered one ([`numbered_place`]).
pub(super) fn release_place(
    db: &mut StateDb,
    folder: &FolderCtx,
    original: &RelPath,
    is_dir: bool,
) -> Result<Option<RelPath>, StateError> {
    if is_free(db, folder, original)? {
        return Ok(Some(original.clone()));
    }
    numbered_place(db, folder, original, is_dir)
}

/// Moves the parked item `row` to `to` and drops its parking record. After a crash in between,
/// the scan finds it at `to`, which voids the record.
pub(super) fn restore_parked(
    db: &mut StateDb,
    disk: &Disk<'_>,
    row: &LocalRow,
    to: &RelPath,
) -> Result<io::Result<LocalRow>, StateError> {
    if let Err(e) = disk.rename(db, &row.path, to)? {
        return Ok(Err(e));
    }
    let refreshed = renamed_row(disk.root(), row, to);
    db.write(|s| {
        s.folder().move_local_subtree(&row.path, to)?;
        if let Some(r) = &refreshed {
            s.folder().put_local(r)?;
        }
        s.folder().delete_parked(row.node)
    })?;
    Ok(Ok(refreshed.unwrap_or_else(|| LocalRow {
        path: to.clone(),
        ..row.clone()
    })))
}

/// Puts parked items back where they came from when that is possible, and drops records that
/// no longer apply (the item was renamed, by us or by the user). Run before each plan.
/// An original name this device now filters (§10) may be held by an item it doesn't track, which
/// would keep the parked item away for good: it takes the first free numbered name instead.
pub(super) fn unpark(db: &mut StateDb, folder: &FolderCtx) -> Result<(), StateError> {
    let disk = Disk::new(&folder.root);
    for node in db.read(|s| s.all_parked())? {
        let place = db.read(|s| {
            let Some(row) = s.local_by_node(node)? else {
                return Ok(None);
            };
            Ok(view::parked_place(s, &row)?.map(|original| (row, original)))
        })?;
        let Some((current, original)) = place else {
            db.write(|s| s.folder().delete_parked(node))?;
            continue;
        };
        let is_dir = current.kind == NodeKind::Folder;
        let to = if folder.filter.path(&original, is_dir).is_sync() {
            // Taken by something synced: that changes, wait for it.
            is_free(db, folder, &original)?.then_some(original)
        } else {
            release_place(db, folder, &original, is_dir)?
        };
        if let Some(to) = to {
            // One that can't move stays parked for the next time.
            let _ = restore_parked(db, &disk, &current, &to)?;
        }
    }
    Ok(())
}
