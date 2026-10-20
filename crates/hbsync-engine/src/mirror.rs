//! The drive's picture: what this device knows the drive holds, recorded from the feed, from the
//! answers to its own writes, and from Recently deleted's operations. The one writer of the
//! mirror (`remote`), Recently deleted (`removed`), the files deleted for good and their
//! tombstones, mostly on the store's named operations, which keep a node live, in Recently
//! deleted, or its file gone, never two of them ([`AccountTx::put_live`],
//! [`AccountTx::move_to_removed`], [`AccountTx::forget_file`]). The answers to this device's own
//! renames and removals are recorded as they always were ([`updated`], [`removed`]): a pull
//! may have recorded a newer state of the node meanwhile, which they must not undo.

use hbsync_remote::{ChangeRef, NodeMeta, Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{AccountTx, MirrorRow, RemovedRow, StateError};
use hbsync_types::removal_time;
use hbsync_types::{ContentHash, FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef, Target};

// ---- facts the feed brings ----

/// Applies one header to the mirror. Returns `true` for a node in the tree, `false` for one
/// that left it (deleted, or removed to Recently deleted).
pub fn apply(s: AccountTx<'_>, header: &RemoteHeader) -> Result<bool, StateError> {
    if header.is_deleted() {
        tombstone(s, header.at(), header.changed_at)?;
        return Ok(false);
    }
    // A live file without its payload changes nothing: it must never read as a delete.
    let Some(row) = mirror_row(header) else {
        return Ok(true);
    };
    // The store marks the nodes that changed dirty where that concerns.
    let removed_at = header.removed_at;
    if s.has_version(row.remote_ref(), removed_at.is_some())? {
        return apply_known(s, &ChangeRef::of(header), header.token.as_ref());
    }
    if let Some(removed_at) = removed_at {
        // Out of the tree for the reconciler, as a tombstone is; restorable meanwhile.
        let removed = RemovedRow {
            row,
            removed_at: known_time(removed_at),
        };
        s.move_to_removed(&removed)?;
        return Ok(false);
    }
    s.put_live(&row)?;
    Ok(true)
}

/// A version the store has already (this device's own write coming back, or a page applied
/// again) changes nothing but what the server says better: the file's `token` where the
/// header gives one (an earlier login's may not open), a removal's time, and the time of a
/// version in a removed folder, where it decides whether it went with it (node schema,
/// Recently deleted). No change of the node, so nothing to reconcile. Returns `true` for a node
/// in the tree.
pub(crate) fn apply_known(
    s: AccountTx<'_>,
    r: &ChangeRef,
    token: Option<&FileToken>,
) -> Result<bool, StateError> {
    if let Some(token) = token {
        s.set_token(r.file_id, r.removed_at.is_some(), token)?;
    }
    if let Some(removed_at) = r.removed_at {
        if let Some(at) = known_time(removed_at) {
            s.confirm_removed(r.at(), at)?;
        }
        return Ok(false);
    }
    if let (Some(changed_at), Some(parent)) = (r.changed_at, r.parent) {
        s.refresh_time(r.file_id, parent, changed_at)?;
    }
    Ok(true)
}

/// The mirror row of a live header (removed or not); `None` for tombstones and for files
/// without a payload.
fn mirror_row(header: &RemoteHeader) -> Option<MirrorRow> {
    let RemoteBody::Live {
        node,
        content,
        payload,
    } = &header.body
    else {
        return None;
    };
    let payload = match header.kind {
        NodeKind::Folder => None,
        // `Remote`s refuse such rows.
        NodeKind::File => Some(payload.as_ref()?),
    };
    let at = header.at();
    let hash = payload.map(|p| p.hash);
    let mut row = MirrorRow {
        token: header.token.clone(),
        changed_at: header.changed_at,
        ..MirrorRow::from_content(at, *node, header.parent, header.kind, content, hash)
    };
    // A content without its size (another client's) has the payload's.
    if let (Some(file), Some(p), None) = (&mut row.file, payload, content.size) {
        file.size = p.bytes;
    }
    Some(row)
}

/// A removal's time, unless the remote didn't say ([`removal_time`]`(None)`: the newest, open
/// in the store until a version that says).
fn known_time(removed_at: i64) -> Option<i64> {
    (removed_at != removal_time(None)).then_some(removed_at)
}

/// A tombstone in the feed, written at `at` (the server's time): the file is deleted for good,
/// and the tombstone itself goes hard once old enough ([`crate::deleted::AccountOps::purge`]).
pub(crate) fn tombstone(
    s: AccountTx<'_>,
    tombstone: RemoteRef,
    at: Option<i64>,
) -> Result<(), StateError> {
    s.tombstone(tombstone, at)
}

/// A file deleted for good with nothing left (a hard delete, announced): a tombstone this device
/// holds at that version has nothing left to delete, and the file is forgotten ([`forget`]).
pub(crate) fn hard_deleted(s: AccountTx<'_>, at: RemoteRef) -> Result<(), StateError> {
    s.delete_tombstone_at(at)?;
    forget(s, at.file_id, None)
}

/// File `file_id` deleted for good (a tombstone, its header not found, this device's delete):
/// out of Recently deleted and the mirror, and never back in them ([`AccountTx::forget_file`]);
/// gone since `at` (the server's: the tombstone a delete of this device left, by its answer;
/// the tombstone itself, at its version, comes with the feed's row), if known.
pub(crate) fn forget(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    at: Option<i64>,
) -> Result<(), StateError> {
    s.forget_file(file_id, at)
}

// ---- this device's own writes, from their answers ----

/// A live version this device wrote (or put back), as the drive answered: out of Recently
/// deleted, into the mirror ([`AccountTx::put_live`]).
pub(crate) fn live(s: AccountTx<'_>, row: &MirrorRow) -> Result<(), StateError> {
    s.put_live(row)
}

/// A removal written (`row`, its answer; `None`: the drive no longer had the file): out of the
/// mirror, into Recently deleted at the answer's time (open if it gave none, until the feed
/// brings it).
pub(crate) fn removed(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    row: Option<MirrorRow>,
) -> Result<(), StateError> {
    s.delete_mirror(file_id)?;
    match row {
        Some(row) => s.put_removed(&RemovedRow {
            removed_at: row.changed_at,
            row,
        }),
        None => Ok(()),
    }
}

/// A rename, move or tag write this device made, as the drive answered: into the mirror. Unlike
/// [`live`] it leaves Recently deleted as it is: a newer removal of the node, which another
/// pull may have recorded since the write landed, must not go.
pub(crate) fn updated(s: AccountTx<'_>, row: &MirrorRow) -> Result<(), StateError> {
    s.put_mirror(row)
}

/// The mirror's row of `meta` as written (`w`, the drive's answer).
pub(crate) fn written(meta: &NodeMeta, hash: Option<ContentHash>, w: Target) -> MirrorRow {
    MirrorRow::written(w, meta.node, meta.parent, meta.kind, &meta.content, hash)
}

/// A mirror row's node as written back: at `parent` under `name`, removed or not.
pub(crate) fn meta_at(row: &MirrorRow, parent: NodeId, name: &str, removed: bool) -> NodeMeta {
    NodeMeta {
        node: row.node,
        parent,
        kind: row.kind,
        content: row.content(name),
        removed,
    }
}

/// [`meta_at`] where and as the row has it.
pub(crate) fn meta(row: &MirrorRow, removed: bool) -> NodeMeta {
    meta_at(row, row.parent, &row.name, removed)
}

// ---- Recently deleted's upkeep and the login ----

/// A tombstone at `at` (its version) deleted hard by this device's purge: nothing left of it.
pub(crate) fn tombstone_deleted(s: AccountTx<'_>, at: RemoteRef) -> Result<(), StateError> {
    s.delete_tombstone_at(at).map(drop)
}

/// Forgets the files known gone since before `before_ms` ([`AccountTx::prune_gone`]); how many.
pub(crate) fn prune(s: AccountTx<'_>, before_ms: i64, now_ms: i64) -> Result<usize, StateError> {
    s.prune_gone(before_ms, now_ms)
}

/// Forgets the file tokens stored: another login seals them from now on.
pub(crate) fn clear_tokens(s: AccountTx<'_>) -> Result<(), StateError> {
    s.clear_tokens()
}

// ---- asking the drive ----

/// Whether a file a write was refused with `e` (not found, or a tombstone) is deleted for good:
/// a tombstone's answer says so, else its header is read ([`gone_for_good`]).
pub(crate) fn gone_after(
    remote: &dyn Remote,
    e: &RemoteError,
    file_id: RemoteFileId,
) -> Result<bool, RemoteError> {
    match e {
        RemoteError::Deleted => Ok(true),
        _ => gone_for_good(remote, file_id),
    }
}

/// Whether the drive has file `file_id` no more (not found, or only its tombstone), by its
/// header: a write or read answered not found may have other reasons.
pub(crate) fn gone_for_good(
    remote: &dyn Remote,
    file_id: RemoteFileId,
) -> Result<bool, RemoteError> {
    match remote.header(file_id) {
        Ok(header) => Ok(header.is_deleted()),
        Err(RemoteError::NotFound) => Ok(true),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use hbsync_fileio::{HashAlgorithm, hash_bytes};
    use hbsync_remote::PayloadInfo;
    use hbsync_state::StateDb;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{Uuid, VersionTag};

    use super::*;

    #[test]
    fn a_live_file_without_its_payload_is_no_delete() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let mut header = RemoteHeader {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            body: RemoteBody::Live {
                node: NodeId::new(),
                content: NodeContent::file("f", 0, 1, false),
                payload: Some(PayloadInfo {
                    hash: hash_bytes(b"x", HashAlgorithm::Blake3),
                    bytes: 1,
                }),
            },
            removed_at: None,
            changed_at: None,
            token: None,
        };
        assert!(db.write(|s| apply(s.account(), &header))?);
        if let RemoteBody::Live { payload, .. } = &mut header.body {
            *payload = None;
        }
        assert!(db.write(|s| apply(s.account(), &header))?);
        assert_eq!(
            db.read(|s| s.mirror_count())?,
            1,
            "the mirror keeps the file"
        );
        Ok(())
    }

    /// Another client's file without `size` in its content has its payload's.
    #[test]
    fn a_file_without_a_size_has_its_payloads() {
        let mut content = NodeContent::file("f", 0, 1, false);
        content.size = None;
        let header = RemoteHeader {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            body: RemoteBody::Live {
                node: NodeId::new(),
                content,
                payload: Some(PayloadInfo {
                    hash: hash_bytes(b"xyz", HashAlgorithm::Blake3),
                    bytes: 3,
                }),
            },
            removed_at: None,
            changed_at: None,
            token: None,
        };
        let size = mirror_row(&header).and_then(|r| r.file).map(|f| f.size);
        assert_eq!(size, Some(3));
    }

    /// The feed brings this device's own writes back: a version the mirror has is no change.
    #[test]
    fn a_known_version_marks_nothing() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let header = |version: u128, removed_at: Option<i64>| RemoteHeader {
            removed_at,
            ..folder(1, version, node, NodeId::ROOT)
        };
        let dirty_after = |db: &mut StateDb, h: &RemoteHeader| {
            db.write(|s| {
                s.folder().clear_dirty(node)?;
                apply(s.account(), h)?;
                s.dirty_count()
            })
        };
        for (version, removed_at) in [(1, None), (2, Some(9))] {
            let h = header(version, removed_at);
            assert_eq!(dirty_after(&mut db, &h)?, 1, "version {version} is news");
            assert_eq!(
                dirty_after(&mut db, &h)?,
                0,
                "version {version} again isn't"
            );
        }
        Ok(())
    }

    #[test]
    fn a_removal_leaves_the_tree_and_a_restore_returns() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let live = |removed_at: Option<i64>| RemoteHeader {
            version: VersionTag(Uuid::new_v4()),
            removed_at,
            ..folder(1, 0, node, NodeId::ROOT)
        };
        assert!(db.write(|s| apply(s.account(), &live(None)))?);
        assert!(!db.write(|s| apply(s.account(), &live(Some(9))))?);
        db.read(|s| {
            assert_eq!(s.get_mirror(node)?, None);
            let at = s.removed_by_node(node)?.and_then(|r| r.removed_at);
            assert_eq!(at, Some(9));
            Ok(())
        })?;
        assert!(db.write(|s| apply(s.account(), &live(None)))?);
        db.read(|s| {
            assert!(s.get_mirror(node)?.is_some() && s.removed_by_node(node)?.is_none());
            Ok(())
        })
    }

    /// A version the mirror has already, again, changes nothing; its echo takes the token (an
    /// earlier login's may not open), and nothing needs reconciling. In Recently deleted too.
    #[test]
    fn a_known_version_takes_only_its_token() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let without = folder(1, 1, node, NodeId::ROOT);
        let token = |b: u8| Some(FileToken(vec![b; 16].into()));
        let with = |b: u8| RemoteHeader {
            token: token(b),
            ..without.clone()
        };
        db.write(|s| {
            apply(s.account(), &without)?;
            s.folder().clear_dirty(node)
        })?;
        let held =
            |db: &mut StateDb| db.read(|s| s.get_mirror(node).map(|m| m.and_then(|m| m.token)));
        db.write(|s| apply(s.account(), &with(0xAA)))?;
        assert_eq!(held(&mut db)?, token(0xAA), "the token taken");
        assert!(!db.read(|s| s.is_dirty(node))?);
        // An earlier login's is replaced; none keeps the one there.
        db.write(|s| apply(s.account(), &with(0xEE)))?;
        db.write(|s| apply(s.account(), &without))?;
        assert_eq!(held(&mut db)?, token(0xEE));

        // In Recently deleted, the same; its time stays the one it has.
        let removed = RemoteHeader {
            version: VersionTag(Uuid::from_u128(2)),
            removed_at: Some(5),
            ..without.clone()
        };
        db.write(|s| {
            apply(s.account(), &removed)?;
            s.folder().clear_dirty(node)
        })?;
        let again = RemoteHeader {
            removed_at: Some(9),
            token: token(0xAA),
            ..removed
        };
        db.write(|s| apply(s.account(), &again))?;
        let back = db.read(|s| s.removed_by_node(node))?.expect("removed");
        assert_eq!((back.row.token, back.removed_at), (token(0xAA), Some(5)));
        assert!(!db.read(|s| s.is_dirty(node))?);
        Ok(())
    }

    /// This device's own write has no server time until the feed brings it back, and the echo
    /// records it only where it decides something: in a folder in Recently deleted.
    #[test]
    fn an_own_writes_time_is_kept_in_a_removed_folder_only() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let top = folder(1, 1, NodeId::new(), NodeId::ROOT);
        let inner = folder(2, 2, NodeId::new(), top.node().expect("live"));
        let leaf = folder(3, 3, NodeId::new(), inner.node().expect("live"));
        let node = leaf.node().expect("live");
        db.write(|s| {
            [&top, &inner, &leaf]
                .into_iter()
                .try_for_each(|h| apply(s.account(), h).map(drop))
        })?;
        let echo = RemoteHeader {
            changed_at: Some(7),
            ..leaf.clone()
        };
        let time = |db: &mut StateDb| {
            db.read(|s| s.get_mirror(node).map(|m| m.and_then(|m| m.changed_at)))
        };
        db.write(|s| apply(s.account(), &echo))?;
        assert_eq!(time(&mut db)?, None, "nothing it decides");
        let removed = RemoteHeader {
            version: VersionTag(Uuid::from_u128(4)),
            removed_at: Some(5),
            ..top
        };
        db.write(|s| apply(s.account(), &removed))?;
        db.write(|s| apply(s.account(), &echo))?;
        assert_eq!(
            time(&mut db)?,
            Some(7),
            "in a removed folder, two levels up"
        );
        Ok(())
    }

    /// A folder's header at `version` (no token, no time): edit the fields a test needs.
    fn folder(file_id: u128, version: u128, node: NodeId, parent: NodeId) -> RemoteHeader {
        RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(file_id)),
            version: VersionTag(Uuid::from_u128(version)),
            parent,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content: NodeContent::folder("d"),
                payload: None,
            },
            removed_at: None,
            changed_at: None,
            token: None,
        }
    }
}
