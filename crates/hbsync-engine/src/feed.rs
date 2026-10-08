//! Pulling the remote change feed into the mirror.
//!
//! Pages are applied in feed order, each in one transaction together with the cursor, so a
//! crash replays at most one page (and applying a page twice is harmless). Every node a page
//! touches is marked dirty. The feed is drained completely before planning, which is what
//! makes the mirror complete enough to call an unknown parent truly gone.

use hbsync_remote::{PayloadInfo, Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{MirrorFile, MirrorRow, REMOTE_CURSOR_KEY, StateDb, StateError, Store};
use hbsync_types::NodeKind;

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

/// Applies one header to the mirror. Returns `true` for a live node, `false` for a tombstone.
pub fn apply(s: &Store<'_>, header: &RemoteHeader) -> Result<bool, StateError> {
    match mirror_row(header) {
        Some(row) => {
            s.put_mirror(&row)?;
            s.mark_dirty(row.node)?;
            Ok(true)
        }
        None => {
            // A tombstone has no node id: find the node by file id.
            if let Some(row) = s.delete_mirror(header.file_id)? {
                s.mark_dirty(row.node)?;
            } else if let Some(base) = s.base_by_remote_file(header.file_id)? {
                s.mark_dirty(base.node)?;
            }
            Ok(false)
        }
    }
}

/// The mirror row of a live header; `None` for tombstones.
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
            let info = payload.unwrap_or(PayloadInfo {
                hash: None,
                bytes: 0,
            });
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
        content_extra: (!content.unknown.is_empty())
            .then(|| serde_json::Value::Object(content.unknown.clone()).to_string()),
    })
}
