//! The drive's picture: what this device knows the drive holds, recorded from the feed, from the
//! answers to its own writes, and from Recently deleted's operations. The one writer of the
//! mirror (`remote`), Recently deleted (`removed`), the files deleted for good and their
//! tombstones, mostly on the store's named operations, which keep a node live, in Recently
//! deleted, or its file gone, never two of them ([`AccountTx::put_live`],
//! [`AccountTx::move_to_removed`], [`AccountTx::forget_file`]), and the answers to this device's
//! own writes ([`AccountTx::answered`]): a pull may have recorded a newer state of the node
//! since the write landed, which they must not undo.

use hbsync_remote::{ChangeRef, NodeMeta, Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{AccountTx, Answer, Answered, MirrorRow, RemovedRow, StateError};
use hbsync_types::Role;
use hbsync_types::{ContentHash, FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef, Written};

// ---- facts the feed brings ----

/// What [`apply`] made of a header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// A node in the tree.
    Live,
    /// One that left it: deleted, or removed to Recently deleted.
    Left,
    /// Not a node this client takes (a fixed id out of place, [`misplaced`]; a change of kind),
    /// with why: recorded as a row it can't read ([`AccountTx::put_unreadable`]).
    Refused(String),
}

/// Applies one header to the mirror ([`Applied`]).
pub fn apply(s: AccountTx<'_>, header: &RemoteHeader) -> Result<Applied, StateError> {
    if header.is_deleted() {
        tombstone(s, header.at(), header.changed_at)?;
        return Ok(Applied::Left);
    }
    if let Some(why) = misplaced(header) {
        // There all the same; its ids are none this device takes.
        s.put_unreadable(header.at(), None, None)?;
        return Ok(Applied::Refused(why));
    }
    // A live file without its payload changes nothing: it must never read as a delete.
    let Some(row) = mirror_row(header) else {
        return Ok(Applied::Live);
    };
    // The store marks the nodes that changed dirty where that concerns.
    let known = s.known_file(row.file_id)?;
    if known.is_some_and(|(v, _, removed)| v == row.version && removed == header.removed) {
        return apply_known(s, &ChangeRef::of(header), header.token.as_ref());
    }
    // A node is one folder or one file for its life (node schema, Identity and tombstones).
    // Taken as a delete and a create, a change of kind would trash this device's copy on
    // another writer's mistake: left out, the drive keeps every byte, and the node is held as
    // this device had it (its writes fork; what is in it waits).
    if let Some((_, had, _)) = known
        && had != row.kind
    {
        s.put_unreadable(row.remote_ref(), Some(row.node), Some(row.parent))?;
        return Ok(Applied::Refused(format!(
            "{:?}: node {:?} was a {had:?}, now a {:?}",
            row.file_id, row.node, row.kind
        )));
    }
    if header.removed {
        // Out of the tree for the reconciler, as a tombstone is; restorable meanwhile; its time
        // open in the store until a version that says.
        let removed = RemovedRow {
            row,
            removed_at: header.removed_at(),
        };
        s.move_to_removed(&removed)?;
        return Ok(Applied::Left);
    }
    s.put_live(&row)?;
    Ok(Applied::Live)
}

/// Why the live (or removed) node `header` brings uses a fixed id out of place, if it does:
/// the root's or the rest of the drive's, which are never nodes, or a role folder's or
/// Recovered files' on a file. Such a version is left out, as a row this client can't read.
fn misplaced(header: &RemoteHeader) -> Option<String> {
    let node = header.node()?;
    let (kind, file) = (header.kind, header.file_id);
    let folder_only = node == NodeId::RECOVERED || Role::of(node).is_some();
    if node.is_root() || node == NodeId::OUTSIDE {
        Some(format!("{file:?}: node id {node:?} is never a node"))
    } else if header.parent == NodeId::OUTSIDE {
        Some(format!(
            "{file:?}: folder {:?} is never a node",
            header.parent
        ))
    } else if folder_only && kind != NodeKind::Folder {
        Some(format!(
            "{file:?}: a {kind:?} with the fixed folder id {node:?}"
        ))
    } else {
        None
    }
}

/// A version the store has already (this device's own write coming back, or a page applied
/// again) changes nothing but what the server says better: the file's `token` where the
/// header gives one (an earlier login's may not open), a removal's time, and the time of a
/// version in a removed folder, where it decides whether it went with it (node schema,
/// Recently deleted). No change of the node, so nothing to reconcile.
pub(crate) fn apply_known(
    s: AccountTx<'_>,
    r: &ChangeRef,
    token: Option<&FileToken>,
) -> Result<Applied, StateError> {
    if let Some(token) = token {
        s.set_token(r.file_id, r.removed, token)?;
    }
    if r.removed {
        if let Some(at) = r.removed_at() {
            s.confirm_removed(r.at(), at)?;
        }
        return Ok(Applied::Left);
    }
    if let (Some(changed_at), Some(parent)) = (r.changed_at, r.parent) {
        s.refresh_time(r.file_id, parent, changed_at)?;
    }
    Ok(Applied::Live)
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

/// Records the answers to this device's writes made outside a folder's run (a restore, the
/// purge's, the role folder), each as [`AccountTx::answered`] does (the executor's settle
/// theirs too); whether a pull superseded one since its write landed: the account reads the
/// feed again then.
pub(crate) fn answered(s: AccountTx<'_>, answers: &[Answer]) -> Result<bool, StateError> {
    let mut superseded = false;
    for answer in answers {
        superseded |= s.answered(answer)? == Answered::Superseded;
    }
    Ok(superseded)
}

/// The mirror's row of `meta` as written (`w`, the drive's answer).
pub(crate) fn written(meta: &NodeMeta, hash: Option<ContentHash>, w: Written) -> MirrorRow {
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

/// What the drive says of a file a request found not there ([`gone_after`], [`look`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Said {
    /// A tombstone: deleted for good, proof on its own.
    Gone,
    /// The identity's own "not there" (a 404): a suspicion until it says so again
    /// ([`not_there`]).
    NotThere,
    /// There after all.
    There,
}

/// What the drive says of a file a write was refused for with `e` (not found, or a tombstone):
/// a tombstone's answer says it, else its header is read ([`look`]).
pub(crate) fn gone_after(
    remote: &dyn Remote,
    e: &RemoteError,
    file_id: RemoteFileId,
) -> Result<Said, RemoteError> {
    match e {
        RemoteError::Deleted => Ok(Said::Gone),
        _ => look(remote, file_id),
    }
}

/// What the drive says of file `file_id`, by its header: a write or read answered not found
/// may have other reasons.
pub(crate) fn look(remote: &dyn Remote, file_id: RemoteFileId) -> Result<Said, RemoteError> {
    match remote.header(file_id) {
        Ok(header) if header.is_deleted() => Ok(Said::Gone),
        Ok(_) => Ok(Said::There),
        Err(RemoteError::Deleted) => Ok(Said::Gone),
        Err(RemoteError::NotFound) => Ok(Said::NotThere),
        Err(e) => Err(e),
    }
}

/// The identity said file `file_id` isn't there (its own 404), now: whether that makes it
/// deleted for good ([`crate::gone::judge`]: said twice, a minute apart, at the version known).
/// Else the suspicion is kept, for the next time.
pub(crate) fn not_there(s: AccountTx<'_>, file_id: RemoteFileId) -> Result<bool, StateError> {
    let prior = s.gone_suspect(file_id)?;
    let version = s.known_version(file_id)?;
    match crate::gone::judge(prior, version, hbsync_types::unix_now_ms()) {
        crate::gone::NotThere::Gone => {
            s.clear_gone_suspect(file_id)?;
            Ok(true)
        }
        crate::gone::NotThere::Suspect(suspect) => {
            if prior != Some(suspect) {
                tracing::warn!("{file_id:?}: the identity says it isn't there; asked again later");
                s.put_gone_suspect(file_id, &suspect)?;
            }
            Ok(false)
        }
    }
}

/// File `file_id` is there after all: a "not there" of it is forgotten.
pub(crate) fn there(s: AccountTx<'_>, file_id: RemoteFileId) -> Result<(), StateError> {
    s.clear_gone_suspect(file_id)
}

/// Whether what the drive `said` of file `file_id` ([`look`]) makes it deleted for good: a
/// tombstone does, the identity's "not there" the second time a minute on ([`not_there`]);
/// there after all clears the doubt.
pub(crate) fn judged(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    said: Said,
) -> Result<bool, StateError> {
    match said {
        Said::Gone => Ok(true),
        Said::NotThere => not_there(s, file_id),
        Said::There => there(s, file_id).map(|()| false),
    }
}

/// [`judged`], and a file deleted for good is forgotten. Whether it was.
pub(crate) fn heard(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    said: Said,
) -> Result<bool, StateError> {
    let gone = judged(s, file_id, said)?;
    if gone {
        forget(s, file_id, None)?;
    }
    Ok(gone)
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
            removed: false,
            changed_at: None,
            token: None,
        };
        assert_eq!(db.write(|s| apply(s.account(), &header))?, Applied::Live);
        if let RemoteBody::Live { payload, .. } = &mut header.body {
            *payload = None;
        }
        assert_eq!(db.write(|s| apply(s.account(), &header))?, Applied::Live);
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
            removed: false,
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
            removed: removed_at.is_some(),
            changed_at: removed_at,
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
            removed: removed_at.is_some(),
            changed_at: removed_at,
            ..folder(1, 0, node, NodeId::ROOT)
        };
        assert_eq!(
            db.write(|s| apply(s.account(), &live(None)))?,
            Applied::Live
        );
        assert_eq!(
            db.write(|s| apply(s.account(), &live(Some(9))))?,
            Applied::Left
        );
        db.read(|s| {
            assert_eq!(s.get_mirror(node)?, None);
            let at = s.removed_by_node(node)?.and_then(|r| r.removed_at);
            assert_eq!(at, Some(9));
            Ok(())
        })?;
        assert_eq!(
            db.write(|s| apply(s.account(), &live(None)))?,
            Applied::Live
        );
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
            removed: true,
            changed_at: Some(5),
            ..without.clone()
        };
        db.write(|s| {
            apply(s.account(), &removed)?;
            s.folder().clear_dirty(node)
        })?;
        let again = RemoteHeader {
            removed: true,
            changed_at: Some(9),
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
            removed: true,
            changed_at: Some(5),
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

    /// The fixed ids out of place are left out as unreadable rows: nothing is recorded, and
    /// nothing is marked to plan.
    #[test]
    fn fixed_ids_out_of_place_and_kind_changes_are_refused() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let file = |file_id: u128, version: u128, node: NodeId| RemoteHeader {
            kind: NodeKind::File,
            body: RemoteBody::Live {
                node,
                content: NodeContent::file("f", 0, 1, false),
                payload: Some(PayloadInfo {
                    hash: hash_bytes(b"x", HashAlgorithm::Blake3),
                    bytes: 1,
                }),
            },
            ..folder(file_id, version, node, NodeId::ROOT)
        };
        let refused = |db: &mut StateDb, h: &RemoteHeader| -> Result<bool, StateError> {
            let dirty = db.read(|s| s.dirty_count())?;
            let applied = db.write(|s| apply(s.account(), h))?;
            Ok(matches!(applied, Applied::Refused(_)) && db.read(|s| s.dirty_count())? == dirty)
        };
        let docs = Role::Documents.node();
        for header in [
            folder(1, 1, NodeId::ROOT, NodeId::ROOT),
            folder(2, 2, NodeId::OUTSIDE, NodeId::ROOT),
            folder(3, 3, NodeId::new(), NodeId::OUTSIDE),
            file(4, 4, docs),
            file(5, 5, NodeId::RECOVERED),
        ] {
            assert!(refused(&mut db, &header)?, "{header:?}");
        }
        assert_eq!(db.read(|s| s.mirror_count())?, 0);
        // As folders, at the top or anywhere, they are nodes.
        for header in [
            folder(6, 6, docs, NodeId::new()),
            folder(7, 7, NodeId::RECOVERED, NodeId::ROOT),
        ] {
            assert_eq!(db.write(|s| apply(s.account(), &header))?, Applied::Live);
        }
        // Each is there all the same: a slim page holding it again needs no whole read.
        let at = |f: u128, v: u128| hbsync_types::RemoteRef {
            file_id: RemoteFileId(Uuid::from_u128(f)),
            version: VersionTag(Uuid::from_u128(v)),
        };
        assert!(db.read(|s| s.known(&[(at(1, 1), false), (at(4, 4), false)]))?);

        // A file that becomes a folder, live or in Recently deleted, is held at the version it
        // had: what it is now doesn't read.
        let node = NodeId::new();
        db.write(|s| {
            apply(s.account(), &file(8, 8, node))?;
            s.folder().clear_dirty(node)
        })?;
        assert!(matches!(
            db.write(|s| apply(s.account(), &folder(8, 9, node, NodeId::ROOT)))?,
            Applied::Refused(_)
        ));
        let kept = db
            .read(|s| s.get_mirror(node))?
            .map(|m| (m.kind, m.version));
        assert_eq!(kept, Some((NodeKind::File, VersionTag(Uuid::from_u128(8)))));
        assert!(
            db.read(|s| s.held_files())?
                .contains(&RemoteFileId(Uuid::from_u128(8)))
        );
        assert!(db.read(|s| s.unreadable_nodes())?.contains(&node));
        let removed = RemoteHeader {
            removed: true,
            changed_at: Some(5),
            ..file(8, 10, node)
        };
        db.write(|s| apply(s.account(), &removed).map(drop))?;
        assert!(
            !db.read(|s| s.held_files())?
                .contains(&RemoteFileId(Uuid::from_u128(8))),
            "read again"
        );
        let back = RemoteHeader {
            removed: true,
            changed_at: Some(5),
            ..folder(8, 11, node, NodeId::ROOT)
        };
        assert!(matches!(
            db.write(|s| apply(s.account(), &back))?,
            Applied::Refused(_)
        ));
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
            removed: false,
            changed_at: None,
            token: None,
        }
    }
}
