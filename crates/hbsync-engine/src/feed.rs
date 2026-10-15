//! Pulling the remote change feed into the mirror.
//!
//! Pages are applied in feed order, each in one transaction together with the cursor, so a
//! crash replays at most one page (and applying a page twice is harmless). Every node a page
//! changes is marked dirty; a version the mirror has already (this device's own write coming
//! back) changes nothing. The feed is drained completely before planning, which is what
//! makes the mirror complete enough to call an unknown parent truly gone.

use hbsync_remote::{Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::{NodeId, NodeKind, RemoteRef};

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error(transparent)]
    State(#[from] StateError),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FeedReport {
    pub pages: usize,
    pub live: usize,
    pub deleted: usize,
}

impl std::ops::AddAssign for FeedReport {
    fn add_assign(&mut self, other: FeedReport) {
        self.pages += other.pages;
        self.live += other.live;
        self.deleted += other.deleted;
    }
}

/// Reads the feed from the stored cursor until it has no more rows.
pub fn pull(
    db: &mut StateDb,
    remote: &dyn Remote,
    page_size: usize,
) -> Result<FeedReport, FeedError> {
    let mut report = FeedReport::default();
    loop {
        let cursor = db.read(|s| s.account_kv(keys::REMOTE_CURSOR))?;
        let page = with_retries(|| remote.changes(cursor.as_deref(), page_size))?;
        db.write(|s| {
            for header in &page.entries {
                if apply(s, header)? {
                    report.live += 1;
                } else {
                    report.deleted += 1;
                }
            }
            s.set_account_kv(keys::REMOTE_CURSOR, &page.cursor)
        })?;
        report.pages += 1;
        if !page.has_more {
            return Ok(report);
        }
    }
}

/// Retries transient errors a few times with exponential backoff (50, 100, 200 ms).
pub fn with_retries<T>(mut f: impl FnMut() -> Result<T, RemoteError>) -> Result<T, RemoteError> {
    let mut delay = std::time::Duration::from_millis(50);
    for _ in 0..3 {
        match f() {
            Err(e) if e.is_transient() => {
                std::thread::sleep(delay);
                delay *= 2;
            }
            other => return other,
        }
    }
    f()
}

/// Applies one header to the mirror. Returns `true` for a node in the tree, `false` for one
/// that left it (deleted, or removed to Recently deleted).
pub fn apply(s: &Store<'_>, header: &RemoteHeader) -> Result<bool, StateError> {
    if header.is_deleted() {
        s.delete_removed_file(header.file_id)?;
        gone(s, header)?;
        return Ok(false);
    }
    // A live file without its payload changes nothing: it must never read as a delete.
    let Some(row) = mirror_row(header) else {
        return Ok(true);
    };
    // A version the store has already (this device's own write coming back, or a page applied
    // again) changes nothing; the store marks the nodes that changed dirty where that concerns.
    let removal = header.removal();
    if s.has_version(row.remote_ref(), removal.is_some())? {
        return Ok(removal.is_none());
    }
    if let Some(removal) = removal {
        // Out of the tree for the reconciler, as a tombstone is; restorable meanwhile.
        gone(s, header)?;
        s.put_removed(&RemovedRow { row, removal })?;
        return Ok(false);
    }
    s.delete_removed(row.node)?;
    s.put_mirror(&row)?;
    Ok(true)
}

/// The mirror without the file of `header`, its node marked to plan again where it was
/// held. A tombstone has no node id: the node is found by file id.
fn gone(s: &Store<'_>, header: &RemoteHeader) -> Result<(), StateError> {
    if s.delete_mirror(header.file_id)?.is_none()
        && let Some(node) = s.node_of_file(header.file_id)?
    {
        // Its place went with its row: the folders holding it are the ones concerned.
        s.fan_out(node, NodeId::ROOT)?;
    }
    Ok(())
}

/// The mirror row of a live header (removed or not); `None` for tombstones and for files
/// without a payload.
pub fn mirror_row(header: &RemoteHeader) -> Option<MirrorRow> {
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
    let at = RemoteRef {
        file_id: header.file_id,
        version: header.version,
    };
    let hash = payload.map(|p| p.hash);
    let mut row = MirrorRow::from_content(at, *node, header.parent, header.kind, content, hash);
    // A content without its size (another client's) has the payload's.
    if let (Some(file), Some(p), None) = (&mut row.file, payload, content.size) {
        file.size = p.bytes;
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use hbsync_fileio::{HashAlgorithm, hash_bytes};
    use hbsync_remote::PayloadInfo;
    use hbsync_types::Uuid;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, RemoteFileId, VersionTag};

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
        };
        assert!(db.write(|s| apply(s, &header))?);
        if let RemoteBody::Live { payload, .. } = &mut header.body {
            *payload = None;
        }
        assert!(db.write(|s| apply(s, &header))?);
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
        };
        let size = mirror_row(&header).and_then(|r| r.file).map(|f| f.size);
        assert_eq!(size, Some(3));
    }

    /// The feed brings this device's own writes back: a version the mirror has is no change.
    #[test]
    fn a_known_version_marks_nothing() -> Result<(), StateError> {
        use hbsync_types::schema::Removal;
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let header = |version: u128, content: NodeContent| RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(1)),
            version: VersionTag(Uuid::from_u128(version)),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content,
                payload: None,
            },
        };
        let dirty_after = |db: &mut StateDb, h: &RemoteHeader| {
            db.write(|s| {
                s.clear_dirty(node)?;
                apply(s, h)?;
                s.dirty_count()
            })
        };
        let mut gone = NodeContent::folder("d");
        gone.set_removal(Some(Removal {
            at_ms: 9,
            root: node,
        }));
        for (version, content) in [(1, NodeContent::folder("d")), (2, gone)] {
            let h = header(version, content);
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
        use hbsync_types::schema::Removal;
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let live = |content: NodeContent| RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(1)),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content,
                payload: None,
            },
        };
        let mut gone = NodeContent::folder("d");
        let removal = Removal {
            at_ms: 9,
            root: node,
        };
        gone.set_removal(Some(removal));
        assert!(db.write(|s| apply(s, &live(NodeContent::folder("d"))))?);
        assert!(!db.write(|s| apply(s, &live(gone)))?);
        db.read(|s| {
            assert_eq!(s.get_mirror(node)?, None);
            let removed = s
                .removed_by_node(node)?
                .map(|r| (r.removal, r.row.content_extra));
            assert_eq!(
                removed,
                Some((removal, None)),
                "kept, without rm in the extra keys"
            );
            Ok(())
        })?;
        assert!(db.write(|s| apply(s, &live(NodeContent::folder("d"))))?);
        db.read(|s| {
            assert!(s.get_mirror(node)?.is_some() && s.removed_by_node(node)?.is_none());
            Ok(())
        })
    }
}
