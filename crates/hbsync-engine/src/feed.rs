//! Pulling the remote change feed into the mirror.
//!
//! Pages are applied in feed order, each in one transaction together with the cursor, so a
//! crash replays at most one page (and applying a page twice is harmless). Every node a page
//! touches is marked dirty. The feed is drained completely before planning, which is what
//! makes the mirror complete enough to call an unknown parent truly gone.

use hbsync_remote::{Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{
    MirrorFile, MirrorRow, REMOTE_CURSOR_KEY, RemovedRow, StateDb, StateError, Store,
};
use hbsync_types::NodeKind;
use hbsync_types::schema::{NodeContent, REMOVAL_KEY};

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
        let cursor = db.read(|s| s.get_kv(REMOTE_CURSOR_KEY))?;
        let page = with_retries(|| remote.changes(cursor.as_deref(), page_size))?;
        db.write(|s| {
            for header in &page.entries {
                if apply(s, header)? {
                    report.live += 1;
                } else {
                    report.deleted += 1;
                }
            }
            s.set_kv(REMOTE_CURSOR_KEY, &page.cursor)
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
    if let Some(removal) = header.removal() {
        // Out of the tree for the reconciler, as a tombstone is; restorable meanwhile.
        gone(s, header)?;
        s.put_removed(&RemovedRow { row, removal })?;
        return Ok(false);
    }
    s.delete_removed(row.node)?;
    s.put_mirror(&row)?;
    s.mark_dirty(row.node)?;
    Ok(true)
}

/// The mirror without the file of `header`, its node marked to plan again. A tombstone has
/// no node id: the node is found by file id.
fn gone(s: &Store<'_>, header: &RemoteHeader) -> Result<(), StateError> {
    if let Some(row) = s.delete_mirror(header.file_id)? {
        s.mark_dirty(row.node)?;
    } else if let Some(base) = s.base_by_remote_file(header.file_id)? {
        s.mark_dirty(base.node)?;
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
    let file = match header.kind {
        NodeKind::Folder => None,
        NodeKind::File => {
            let info = payload.as_ref()?; // `Remote`s refuse such rows

            Some(MirrorFile {
                hash: info.hash,
                size: content.size.unwrap_or(info.bytes),
                mtime_ms: content.mtime.unwrap_or(0),
                executable: content.x,
            })
        }
    };
    Some(MirrorRow {
        file_id: header.file_id,
        version: header.version,
        node: *node,
        parent: header.parent,
        name: content.name.clone(),
        kind: header.kind,
        file,
        content_extra: content_extra(content),
    })
}

/// What a mirror row keeps of `content` beyond its columns: the keys sync carries without
/// acting on them (unknown ones, and the creation time), read back by [`unknown_keys`]. Not
/// the removal: the `removed` table has it in columns.
pub(crate) fn content_extra(content: &NodeContent) -> Option<String> {
    let mut unknown = content.unknown.clone();
    unknown.remove(REMOVAL_KEY);
    (!unknown.is_empty()).then(|| serde_json::Value::Object(unknown).to_string())
}

/// The content keys of `row` this client carries without acting on them (stored by
/// [`mirror_row`]), to be written back unchanged.
pub(crate) fn unknown_keys(row: &MirrorRow) -> serde_json::Map<String, serde_json::Value> {
    row.content_extra
        .as_deref()
        .and_then(|extra| serde_json::from_str(extra).ok())
        .unwrap_or_default()
}

/// The creation time `row`'s node carries, Unix ms.
pub(crate) fn created_ms(row: &MirrorRow) -> Option<i64> {
    NodeContent::created_ms_in(&unknown_keys(row))
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
