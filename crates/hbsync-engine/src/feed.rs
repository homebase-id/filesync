//! Pulling the remote change feed into the mirror.
//!
//! Pages are applied in feed order, each in one transaction together with the cursor, so a
//! crash replays at most one page (and applying a page twice is harmless). Every node a page
//! changes is marked dirty; a version the mirror has already (this device's own write coming
//! back) changes nothing. The feed is drained completely before planning, which is what
//! makes the mirror complete enough to call an unknown parent truly gone.

use std::collections::HashMap;
use std::ops::DerefMut;

use hbsync_remote::{ChangeRef, Remote, RemoteBody, RemoteError, RemoteHeader};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::{FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef};

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error(transparent)]
    State(#[from] StateError),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FeedReport {
    pub pages: usize,
    pub live: usize,
    pub deleted: usize,
    /// Rows the client can't read (not encrypted, another key, not a sync node), with why.
    pub skipped: Vec<String>,
    /// The rows the feed holds but the mirror doesn't take, by file and version: those it
    /// couldn't read (where they say which), and live files without a payload.
    pub unreadable: Vec<RemoteRef>,
}

impl std::ops::AddAssign for FeedReport {
    fn add_assign(&mut self, other: FeedReport) {
        self.pages += other.pages;
        self.live += other.live;
        self.deleted += other.deleted;
        self.skipped.extend(other.skipped);
        self.unreadable.extend(other.unreadable);
    }
}

/// Reads the feed from the stored cursor until it has no more rows. The DB is taken from `db`
/// for each step, and let go while a page is on its way: others use it meanwhile.
///
/// A page is read slim first ([`Remote::change_refs`]: which version of which file changed).
/// If the store knows every row (a version it has: this device's own writes coming back; a
/// tombstone), the page is applied from them alone (this device's own writes are known, with
/// their times, from their answers; a time an answer didn't give comes from the row). A page with anything new is
/// read again whole from the same cursor, and the feed continues from that page's cursor (a
/// file changed in between has moved further along the feed). Changes from elsewhere come in
/// runs, so after a page with something new the next is read whole at once, as is a first read
/// (no cursor: all of it is new), until a whole page holds nothing new.
pub fn pull<G: DerefMut<Target = StateDb>>(
    db: impl Fn() -> G,
    remote: &dyn Remote,
    page_size: usize,
) -> Result<FeedReport, FeedError> {
    let mut report = FeedReport::default();
    let mut slim = None;
    loop {
        let cursor = db().read(|s| s.account_kv(keys::REMOTE_CURSOR))?;
        let cursor = cursor.as_deref();
        let applied = if *slim.get_or_insert(cursor.is_some()) {
            read_slim(&db, remote, cursor, page_size, &mut report)?
        } else {
            None
        };
        let (has_more, nothing_new) = match applied {
            Some(has_more) => (has_more, true),
            None => read_whole(&db, remote, cursor, page_size, &mut report)?,
        };
        slim = Some(nothing_new);
        report.pages += 1;
        if !has_more {
            return Ok(report);
        }
    }
}

/// One page slim, applied if it holds nothing new: whether more follow; `None` if it holds
/// something new (nothing applied).
fn read_slim<G: DerefMut<Target = StateDb>>(
    db: &impl Fn() -> G,
    remote: &dyn Remote,
    cursor: Option<&str>,
    page_size: usize,
    report: &mut FeedReport,
) -> Result<Option<bool>, FeedError> {
    let page = with_retries(|| remote.change_refs(cursor, page_size))?;
    // Checked where it is applied: a commit in between may have changed what is known.
    let applied = db().write(|s| {
        if page.unread > 0 || !known(s, &page.refs)? {
            return Ok(false);
        }
        for r in &page.refs {
            report.count(apply_ref(s, r)?);
        }
        save_cursor(s, cursor, !page.refs.is_empty(), &page.cursor)?;
        Ok(true)
    })?;
    Ok(applied.then_some(page.has_more))
}

/// One page whole, applied: whether more follow, and whether it held nothing new.
fn read_whole<G: DerefMut<Target = StateDb>>(
    db: &impl Fn() -> G,
    remote: &dyn Remote,
    cursor: Option<&str>,
    page_size: usize,
    report: &mut FeedReport,
) -> Result<(bool, bool), FeedError> {
    let mut page = with_retries(|| remote.changes(cursor, page_size))?;
    report.skipped.append(&mut page.skipped);
    report.unreadable.append(&mut page.skipped_files);
    report.unreadable.extend(
        page.entries
            .iter()
            .filter(|h| !h.is_deleted() && mirror_row(h).is_none())
            .map(|h| RemoteRef {
                file_id: h.file_id,
                version: h.version,
            }),
    );
    let nothing_new = db().write(|s| {
        let refs: Vec<ChangeRef> = page.entries.iter().map(ChangeRef::of).collect();
        let nothing_new = known(s, &refs)?;
        for header in &page.entries {
            report.count(apply(s, header)?);
        }
        save_cursor(s, cursor, !page.entries.is_empty(), &page.cursor)?;
        Ok(nothing_new)
    })?;
    Ok((page.has_more, nothing_new))
}

impl FeedReport {
    fn count(&mut self, live: bool) {
        if live {
            self.live += 1;
        } else {
            self.deleted += 1;
        }
    }
}

/// Whether the store knows every one of `refs`: a tombstone needs nothing, the rest are
/// versions it has, in Recently deleted or else in the mirror.
fn known(s: &Store<'_>, refs: &[ChangeRef]) -> Result<bool, StateError> {
    let versions: Vec<(RemoteRef, bool)> = refs
        .iter()
        .filter(|r| !r.deleted)
        .map(|r| (r.at(), r.removed_at.is_some()))
        .collect();
    s.known(&versions)
}

/// Stores the cursor after a page; unchanged on an idle pull: no write, so nothing to sync.
fn save_cursor(s: &Store<'_>, old: Option<&str>, rows: bool, new: &str) -> Result<(), StateError> {
    if rows || old != Some(new) {
        s.set_account_kv(keys::REMOTE_CURSOR, new)?;
    }
    Ok(())
}

/// Changes the notifications announced (rows as a slim page has them): a file deleted for good
/// is gone at once (a hard delete leaves no row in the feed), and when the store knows every
/// other version (this device's own writes coming back) they are applied as a slim page's,
/// without reading the feed. Whether that was all; else the feed has to be read.
pub(crate) fn announced(s: &Store<'_>, refs: &[ChangeRef]) -> Result<bool, StateError> {
    let refs = latest(refs);
    for r in refs.iter().filter(|r| r.deleted) {
        apply_tombstone(s, r.file_id)?;
    }
    if !known(s, &refs)? {
        return Ok(false);
    }
    for r in refs.iter().filter(|r| !r.deleted) {
        apply_ref(s, r)?;
    }
    Ok(true)
}

/// One ref per file: the last a file had. A tombstone ends a file (its id never comes back,
/// `homebase-api-notes.md`); of the others the newest by the server's time, the later
/// announced on a tie. An own write made twice, or made then deleted, needs no read.
fn latest(refs: &[ChangeRef]) -> Vec<ChangeRef> {
    let time = |r: &ChangeRef| r.changed_at.max(r.removed_at);
    let mut last: HashMap<RemoteFileId, ChangeRef> = HashMap::new();
    for r in refs {
        match last.get(&r.file_id) {
            Some(had) if had.deleted || (!r.deleted && time(had) > time(r)) => {}
            _ => {
                last.insert(r.file_id, *r);
            }
        }
    }
    last.into_values().collect()
}

/// Applies a known ref ([`known`]) as [`apply`] would its header: only what the server says
/// better, its times. Returns `true` for a node in the tree.
fn apply_ref(s: &Store<'_>, r: &ChangeRef) -> Result<bool, StateError> {
    if r.deleted {
        apply_tombstone(s, r.file_id)?;
        return Ok(false);
    }
    apply_known(s, r, None)
}

/// A version the store has already (this device's own write coming back, or a page applied
/// again) changes nothing but what the server says better: the file's `token` where the
/// header gives one (an earlier login's may not open), a removal's time, and the time of a
/// version in a removed folder, where it decides whether it went with it (node schema,
/// Recently deleted). No change of the node, so nothing to reconcile. Returns `true` for a node
/// in the tree.
fn apply_known(
    s: &Store<'_>,
    r: &ChangeRef,
    token: Option<&FileToken>,
) -> Result<bool, StateError> {
    if let Some(token) = token {
        s.set_token(r.file_id, r.removed_at.is_some(), token)?;
    }
    if let Some(removed_at) = r.removed_at {
        s.confirm_removed(r.at(), removed_at)?;
        return Ok(false);
    }
    if let (Some(changed_at), Some(parent)) = (r.changed_at, r.parent) {
        s.refresh_time(r.file_id, parent, changed_at)?;
    }
    Ok(true)
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
        apply_tombstone(s, header.file_id)?;
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
        gone(s, header.file_id)?;
        s.put_removed(&RemovedRow {
            row,
            removed_at: Some(removed_at),
        })?;
        return Ok(false);
    }
    s.delete_removed(row.node)?;
    s.put_mirror(&row)?;
    Ok(true)
}

/// A file deleted for good: out of Recently deleted and the mirror, and never back in them
/// ([`Store::mark_file_gone`]).
fn apply_tombstone(s: &Store<'_>, file_id: RemoteFileId) -> Result<(), StateError> {
    s.mark_file_gone(file_id)?;
    s.delete_removed_file(file_id)?;
    gone(s, file_id)
}

/// The mirror without file `file_id`, its node marked to plan again where it was
/// held. A tombstone has no node id: the node is found by file id.
fn gone(s: &Store<'_>, file_id: RemoteFileId) -> Result<(), StateError> {
    if s.delete_mirror(file_id)?.is_none()
        && let Some(node) = s.node_of_file(file_id)?
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

#[cfg(test)]
mod tests {
    use hbsync_fileio::{HashAlgorithm, hash_bytes};
    use hbsync_remote::PayloadInfo;
    use hbsync_types::Uuid;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, RemoteFileId, VersionTag};

    use super::*;

    /// A first read (no cursor) is read whole; a page of nothing new costs one slim read; a run
    /// of changes from elsewhere is read slim once, then whole page by page.
    #[test]
    fn news_is_read_whole_and_the_rest_slim() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::NodeMeta;
        use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
        let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
        let folder = |name: String| NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            content: NodeContent::folder(&name),
            removed: false,
        };
        for i in 0..4 {
            remote.create(&folder(format!("a{i}")), None)?;
        }
        let db = std::sync::Mutex::new(StateDb::open_in_memory()?);
        let lock = || db.lock().unwrap_or_else(|e| e.into_inner());
        let reads = |r: &FakeRemote| {
            let f = r.faults();
            (f.ref_pages, f.full_pages)
        };
        pull(lock, &remote, 3)?;
        assert_eq!(reads(&remote), (0, 2), "a first read: whole");
        pull(lock, &remote, 3)?;
        assert_eq!(reads(&remote), (1, 2), "nothing new: slim");
        for i in 0..7 {
            remote.create(&folder(format!("b{i}")), None)?;
        }
        pull(lock, &remote, 3)?;
        assert_eq!(reads(&remote), (2, 5), "news: slim once, then whole");
        assert_eq!(lock().read(|s| s.mirror_count())?, 11);
        // A slim row that can't be read: the page is read whole, the cursor goes on.
        remote.faults().unread_refs = true;
        pull(lock, &remote, 3)?;
        assert_eq!(reads(&remote), (3, 6), "an unread row: whole");
        Ok(())
    }

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
            file_id: RemoteFileId(Uuid::from_u128(1)),
            version: VersionTag(Uuid::from_u128(version)),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content: NodeContent::folder("d"),
                payload: None,
            },
            removed_at,
            changed_at: None,
            token: None,
        };
        let dirty_after = |db: &mut StateDb, h: &RemoteHeader| {
            db.write(|s| {
                s.clear_dirty(node)?;
                apply(s, h)?;
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
            file_id: RemoteFileId(Uuid::from_u128(1)),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content: NodeContent::folder("d"),
                payload: None,
            },
            removed_at,
            changed_at: None,
            token: None,
        };
        assert!(db.write(|s| apply(s, &live(None)))?);
        assert!(!db.write(|s| apply(s, &live(Some(9))))?);
        db.read(|s| {
            assert_eq!(s.get_mirror(node)?, None);
            let at = s.removed_by_node(node)?.and_then(|r| r.removed_at);
            assert_eq!(at, Some(9));
            Ok(())
        })?;
        assert!(db.write(|s| apply(s, &live(None)))?);
        db.read(|s| {
            assert!(s.get_mirror(node)?.is_some() && s.removed_by_node(node)?.is_none());
            Ok(())
        })
    }
}
