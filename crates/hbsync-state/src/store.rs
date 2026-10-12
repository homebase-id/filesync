//! Typed access to the tables, inside one transaction.
//!
//! Every node has a small integer id (`nodes`, never reused), and every table is keyed by it;
//! the UUIDs appear once. The local side is a tree like the other two: each row holds its
//! parent and its name, and paths are derived. Moving a folder rewrites one row.

use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_types::activity::Synced;
use hbsync_types::schema::{Removal, tags_from_str, tags_in_keys, tags_to_string};
use hbsync_types::{
    ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Tags,
    VersionTag,
};
use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::codec;
use crate::{Result, StateError};

/// The agreed state of one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseRow {
    pub node: NodeId,
    pub state: NodeState,
    pub remote: RemoteRef,
    pub content_state: ContentState,
    /// The local spelling, when the name map says it must be recorded.
    pub local_name: Option<String>,
    /// Hash of the local bytes when the content was agreed (`state.content.hash` is the
    /// remote's representation, which may use another algorithm).
    pub local_hash: Option<hbsync_types::ContentHash>,
}

/// What the last observation saw at one local path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRow {
    pub path: RelPath,
    pub node: NodeId,
    pub kind: NodeKind,
    pub fingerprint: Fingerprint,
    pub executable: Option<bool>,
    /// `None` until hashed; always `None` for folders.
    pub hash: Option<hbsync_types::ContentHash>,
    /// `None` if not known: no tags on this system or file system, or not read yet.
    pub tags: Option<Tags>,
}

/// The client's copy of one live remote node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRow {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
    pub node: NodeId,
    pub parent: NodeId,
    pub name: String,
    pub kind: NodeKind,
    /// Files only.
    pub file: Option<MirrorFile>,
    /// Content keys sync carries without acting on them (JSON object: unknown keys, the
    /// creation time `ct` and the tags), written back unchanged.
    pub content_extra: Option<String>,
}

/// A remote node in Recently deleted: its row as it was, and its removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedRow {
    pub row: MirrorRow,
    pub removal: Removal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorFile {
    pub hash: hbsync_types::ContentHash,
    pub size: u64,
    pub mtime_ms: i64,
    pub executable: bool,
}

/// Where a sweep over the dirty set stopped (see [`Store::dirty_page`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirtyCursor(i64);

impl MirrorRow {
    pub fn remote_ref(&self) -> RemoteRef {
        RemoteRef {
            file_id: self.file_id,
            version: self.version,
        }
    }

    /// The node state, if complete (files need their [`MirrorFile`]).
    pub fn state(&self) -> Option<NodeState> {
        let content = match (self.kind, &self.file) {
            (NodeKind::Folder, _) => None,
            (NodeKind::File, Some(f)) => Some(FileContent {
                hash: f.hash,
                size: f.size,
                mtime_ms: f.mtime_ms,
                executable: f.executable,
            }),
            (NodeKind::File, None) => return None,
        };
        Some(NodeState {
            parent: self.parent,
            name: self.name.clone(),
            kind: self.kind,
            content,
            tags: self.tags(),
        })
    }

    /// The tags, from the content keys.
    pub fn tags(&self) -> Tags {
        self.content_extra
            .as_deref()
            .map(tags_in_keys)
            .unwrap_or_default()
    }
}

/// The tables, seen through one open transaction.
#[derive(Debug)]
pub struct Store<'a> {
    conn: &'a Connection,
}

const BASE_SELECT: &str = "SELECT n.uuid, p.uuid, b.name, b.kind, b.hash, b.size, b.mtime_ms, \
     b.executable, b.remote_file_id, b.version_tag, b.content_state, b.local_name, b.local_hash, \
     b.tags FROM base b JOIN nodes n ON n.id = b.id JOIN nodes p ON p.id = b.parent";
const REMOVED_SELECT: &str = "SELECT file_id, version_tag, node, parent, kind, name, hash, size, \
     mtime_ms, executable, content_extra, removed_at, root FROM removed";
const MIRROR_SELECT: &str = "SELECT m.file_id, m.version_tag, n.uuid, p.uuid, m.kind, m.name, \
     m.hash, m.size, m.mtime_ms, m.executable, m.content_extra \
     FROM remote m JOIN nodes n ON n.id = m.id JOIN nodes p ON p.id = m.parent";
/// The columns [`RawLocal`] reads, from `local l JOIN nodes n`.
const LOCAL_COLS: &str = "l.id, n.uuid, l.kind, l.size, l.mtime_ns, l.ctime_ns, l.volume, \
     l.file_id, l.file_id_hi, l.executable, l.hash, l.parent, l.name, l.tags";
/// Columns in [`LOCAL_COLS`].
const LOCAL_COL_COUNT: usize = 14;
/// Deeper than any real tree (Windows' 32k-character paths hold about 16k levels); reaching it
/// means a broken parent chain.
const MAX_DEPTH: usize = 16_384;
const LOCAL_FROM: &str = "FROM local l JOIN nodes n ON n.id = l.id";
/// A node's integer id, by UUID.
const ID_OF: &str = "(SELECT id FROM nodes WHERE uuid = ?1)";

/// A dirty node `d` with base `b` is a synced file gone locally (online-only files have no
/// local copy by design), or gone remotely: what the mass-delete guard counts.
const GONE_LOCALLY: &str = "b.kind = 0 AND b.content_state != 2 \
    AND NOT EXISTS (SELECT 1 FROM local l WHERE l.id = d.id)";
const GONE_REMOTELY: &str = "b.kind = 0 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = d.id)";

impl<'a> Store<'a> {
    pub(crate) fn new(conn: &'a Connection) -> Self {
        Store { conn }
    }

    // ---- node ids ----

    fn key(&self, node: NodeId) -> Result<Option<i64>> {
        Ok(self
            .conn
            .prepare_cached("SELECT id FROM nodes WHERE uuid = ?1")?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?)
    }

    /// The integer id of `node`, assigned on first use.
    fn key_or_insert(&self, node: NodeId) -> Result<i64> {
        if let Some(id) = self.key(node)? {
            return Ok(id);
        }
        self.conn
            .prepare_cached("INSERT INTO nodes (uuid) VALUES (?1)")?
            .execute([node.0.as_bytes()])?;
        Ok(self.conn.last_insert_rowid())
    }

    fn uuid_of(&self, id: i64) -> Result<NodeId> {
        let bytes: Vec<u8> = self
            .conn
            .prepare_cached("SELECT uuid FROM nodes WHERE id = ?1")?
            .query_row([id], |r| r.get(0))?;
        codec::node(&bytes)
    }

    /// Forgets the ids of nodes nothing refers to any more (gone everywhere, nothing pending).
    /// Only ids that lost a reference since the last call are checked (triggers note them in
    /// `gone`). Ids are never handed out again, so an id held anywhere can't come to mean
    /// another node.
    pub fn collect_nodes(&self) -> Result<usize> {
        let n = self
            .conn
            .prepare_cached(
                "DELETE FROM nodes WHERE id IN (SELECT id FROM gone) \
             AND NOT EXISTS (SELECT 1 FROM local WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM local WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM base WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM base WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM remote WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM remote WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM dirty WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM parked WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM forks WHERE id = nodes.id OR new_node = nodes.id)",
            )?
            .execute([])?;
        self.conn.prepare_cached("DELETE FROM gone")?.execute([])?;
        Ok(n)
    }

    pub fn node_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM nodes")
    }

    // ---- base ----

    pub fn get_base(&self, node: NodeId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("{BASE_SELECT} WHERE n.uuid = ?1"),
            node.0.as_bytes(),
            base_row,
        )
    }

    pub fn base_by_remote_file(&self, file_id: RemoteFileId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("{BASE_SELECT} WHERE b.remote_file_id = ?1"),
            file_id.0.as_bytes(),
            base_row,
        )
    }

    pub fn base_children(&self, parent: NodeId) -> Result<Vec<BaseRow>> {
        self.many(
            &format!("{BASE_SELECT} WHERE b.parent = {ID_OF}"),
            [parent.0.as_bytes()],
            base_row,
        )
    }

    /// Inserts or replaces the base row of `row.node`.
    pub fn put_base(&self, row: &BaseRow) -> Result<()> {
        let c = row.state.content.as_ref();
        let (id, parent) = (
            self.key_or_insert(row.node)?,
            self.key_or_insert(row.state.parent)?,
        );
        self.conn
            .prepare_cached(
                "INSERT INTO base (id, parent, name, kind, hash, size, mtime_ms, \
                 executable, remote_file_id, version_tag, content_state, local_name, local_hash, \
                 tags) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
                 ON CONFLICT (id) DO UPDATE SET parent = ?2, name = ?3, kind = ?4, hash = ?5, \
                 size = ?6, mtime_ms = ?7, executable = ?8, remote_file_id = ?9, \
                 version_tag = ?10, content_state = ?11, local_name = ?12, local_hash = ?13, \
                 tags = ?14",
            )?
            .execute(params![
                id,
                parent,
                row.state.name,
                codec::kind_to(row.state.kind),
                c.map(|c| codec::hash_to(&c.hash)),
                c.map(|c| c.size as i64),
                c.map(|c| c.mtime_ms),
                c.is_some_and(|c| c.executable),
                row.remote.file_id.0.as_bytes(),
                row.remote.version.0.as_bytes(),
                codec::content_state_to(row.content_state),
                row.local_name,
                row.local_hash.as_ref().map(codec::hash_to),
                (!row.state.tags.is_empty()).then(|| tags_to_string(&row.state.tags)),
            ])?;
        Ok(())
    }

    pub fn delete_base(&self, node: NodeId) -> Result<bool> {
        self.change(
            &format!("DELETE FROM base WHERE id = {ID_OF}"),
            node.0.as_bytes(),
        )
    }

    pub fn base_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM base")
    }

    /// Number of file (not folder) nodes in the base; the mass-delete guard's denominator.
    /// A full table scan: callers take it once per sync.
    pub fn base_file_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM base WHERE kind = 0")
    }

    // ---- local observations ----

    /// The id of the local row at `path`; for the root, the root's id (it has no row).
    fn local_id_at(&self, path: &RelPath) -> Result<Option<i64>> {
        let Some(mut id) = self.key(NodeId::ROOT)? else {
            return Ok(None);
        };
        let mut stmt = self
            .conn
            .prepare_cached("SELECT id FROM local WHERE parent = ?1 AND name = ?2")?;
        for name in path.components() {
            match stmt.query_row(params![id, name], |r| r.get(0)).optional()? {
                Some(child) => id = child,
                None => return Ok(None),
            }
        }
        Ok(Some(id))
    }

    /// The id of the folder at `path`, to write a row into: the root's id is assigned on first
    /// use.
    fn folder_id_for_write(&self, path: &RelPath) -> Result<Option<i64>> {
        if path.is_root() {
            return self.key_or_insert(NodeId::ROOT).map(Some);
        }
        self.local_id_at(path)
    }

    /// The path of the folder `id` (the root's id gives the root), walking up its parents
    /// until a folder already in `cache`; every folder on the way is cached.
    fn folder_path(&self, id: i64, cache: &mut HashMap<i64, RelPath>) -> Result<RelPath> {
        if cache.is_empty()
            && let Some(root) = self.key(NodeId::ROOT)?
        {
            cache.insert(root, RelPath::root());
        }
        let mut stmt = self
            .conn
            .prepare_cached("SELECT parent, name FROM local WHERE id = ?1")?;
        let mut chain: Vec<(i64, String)> = Vec::new();
        let mut at = id;
        let mut path = loop {
            if let Some(p) = cache.get(&at) {
                break p.clone();
            }
            if chain.len() >= MAX_DEPTH {
                return Err(StateError::Corrupt(format!(
                    "local row {id}: parent chain too deep, or a cycle"
                )));
            }
            let (parent, name): (i64, String) = stmt
                .query_row([at], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?
                .ok_or_else(|| {
                    StateError::Corrupt(format!("local row {id} is not connected to the root"))
                })?;
            chain.push((at, name));
            at = parent;
        };
        for (folder, name) in chain.into_iter().rev() {
            path = path
                .join(&name)
                .map_err(|e| StateError::Corrupt(format!("local name {name:?}: {e}")))?;
            cache.insert(folder, path.clone());
        }
        Ok(path)
    }

    fn raw_local(&self, sql: &str, params: impl rusqlite::Params) -> Result<Vec<RawLocal>> {
        let mut stmt = self.conn.prepare_cached(sql)?;
        let rows = stmt.query_map(params, |r| RawLocal::read(r, 0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Rows found without their path: each path is its folder's (derived once per folder)
    /// plus its name.
    fn with_paths(&self, raw: Vec<RawLocal>) -> Result<Vec<LocalRow>> {
        let mut cache = HashMap::new();
        raw.into_iter()
            .map(|r| {
                let path = self
                    .folder_path(r.parent, &mut cache)?
                    .join(&r.name)
                    .map_err(|e| StateError::Corrupt(format!("local name {:?}: {e}", r.name)))?;
                r.into_row(path)
            })
            .collect()
    }

    /// The folder `node` is in locally: the root for a top-level item, `None` without a row.
    pub fn local_parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        let parent: Option<Vec<u8>> = self
            .conn
            .prepare_cached(
                "SELECT p.uuid FROM local l JOIN nodes n ON n.id = l.id \
                 JOIN nodes p ON p.id = l.parent WHERE n.uuid = ?1",
            )?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?;
        parent.as_deref().map(codec::node).transpose()
    }

    pub fn get_local(&self, path: &RelPath) -> Result<Option<LocalRow>> {
        if path.is_root() {
            return Ok(None);
        }
        let Some(id) = self.local_id_at(path)? else {
            return Ok(None);
        };
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE l.id = ?1"),
            [id],
        )?;
        raw.into_iter()
            .next()
            .map(|r| r.into_row(path.clone()))
            .transpose()
    }

    pub fn local_by_node(&self, node: NodeId) -> Result<Option<LocalRow>> {
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE n.uuid = ?1"),
            [node.0.as_bytes()],
        )?;
        Ok(self.with_paths(raw)?.into_iter().next())
    }

    /// The node of the folder at `path`: the root for the empty path, `None` if no row is
    /// recorded there.
    pub fn local_node_at(&self, path: &RelPath) -> Result<Option<NodeId>> {
        if path.is_root() {
            return Ok(Some(NodeId::ROOT));
        }
        self.local_id_at(path)?
            .map(|id| self.uuid_of(id))
            .transpose()
    }

    pub fn local_by_file_id(&self, id: &FileId) -> Result<Vec<LocalRow>> {
        let (lo, hi) = codec::file_id_parts(id);
        let raw = self.raw_local(
            &format!(
                "SELECT {LOCAL_COLS} {LOCAL_FROM} \
                 WHERE l.file_id = ?1 AND l.file_id_hi IS ?2 AND l.volume = ?3"
            ),
            params![lo, hi, id.volume as i64],
        )?;
        self.with_paths(raw)
    }

    /// Inserts or updates the row of `row.node` at `row.path`. Fails with
    /// [`StateError::Conflict`] if the node is recorded at another path (move it, or delete
    /// that row, first), if another node holds the path, or if the parent folder has no row.
    pub fn put_local(&self, row: &LocalRow) -> Result<()> {
        let (Some(parent_path), Some(name)) = (row.path.parent(), row.path.name()) else {
            return Err(StateError::Conflict("the root has no row".into()));
        };
        let parent = self.folder_id_for_write(&parent_path)?.ok_or_else(|| {
            StateError::Conflict(format!("{} has no recorded parent folder", row.path))
        })?;
        let id = self.key_or_insert(row.node)?;
        let place: Option<(i64, String)> = self
            .conn
            .prepare_cached("SELECT parent, name FROM local WHERE id = ?1")?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        if let Some((p, n)) = place
            && (p != parent || n != name)
        {
            let at = self.local_by_node(row.node)?.map(|r| r.path);
            return Err(StateError::Conflict(format!(
                "{:?} is already at {at:?}",
                row.node
            )));
        }
        let holder: Option<i64> = self
            .conn
            .prepare_cached("SELECT id FROM local WHERE parent = ?1 AND name = ?2")?
            .query_row(params![parent, name], |r| r.get(0))
            .optional()?;
        if holder.is_some_and(|h| h != id) {
            return Err(StateError::Conflict(format!("{} is taken", row.path)));
        }
        let fp = &row.fingerprint;
        let (lo, hi) = codec::file_id_parts(&fp.file_id);
        self.conn
            .prepare_cached(
                "INSERT INTO local (id, parent, name, kind, size, mtime_ns, ctime_ns, \
                 volume, file_id, file_id_hi, executable, hash, tags) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
                 ON CONFLICT (id) DO UPDATE SET parent = ?2, name = ?3, kind = ?4, size = ?5, \
                 mtime_ns = ?6, ctime_ns = ?7, volume = ?8, file_id = ?9, file_id_hi = ?10, \
                 executable = ?11, hash = ?12, tags = ?13",
            )?
            .execute(params![
                id,
                parent,
                name,
                codec::kind_to(row.kind),
                fp.size as i64,
                fp.mtime_ns,
                fp.ctime_ns,
                fp.file_id.volume as i64,
                lo,
                hi,
                row.executable,
                row.hash.as_ref().map(codec::hash_to),
                row.tags.as_ref().map(tags_to_string),
            ])?;
        Ok(())
    }

    /// Records the tags of `node`'s local item, as read (the rest of its row stays).
    pub fn set_local_tags(&self, node: NodeId, tags: &Tags) -> Result<bool> {
        let n = self
            .conn
            .prepare_cached(&format!("UPDATE local SET tags = ?2 WHERE id = {ID_OF}"))?
            .execute(params![node.0.as_bytes(), tags_to_string(tags)])?;
        Ok(n > 0)
    }

    /// Deletes the row of `node`. A folder's contents must go first (or use
    /// [`Store::delete_local_subtree`]): rows below a deleted folder would have no path.
    pub fn delete_local(&self, node: NodeId) -> Result<bool> {
        let Some(id) = self.key(node)? else {
            return Ok(false);
        };
        let has_children = self
            .conn
            .prepare_cached("SELECT 1 FROM local WHERE parent = ?1 LIMIT 1")?
            .query_row([id], |_| Ok(()))
            .optional()?
            .is_some();
        if has_children {
            return Err(StateError::Conflict(format!(
                "{node:?} still has rows below it"
            )));
        }
        Ok(self
            .conn
            .prepare_cached("DELETE FROM local WHERE id = ?1")?
            .execute([id])?
            > 0)
    }

    /// The item recorded for `old` is node `new` from now on (adoption): same place, same
    /// observation, and its contents stay inside it.
    pub fn rekey_local(&self, old: NodeId, new: NodeId) -> Result<()> {
        let old_id = self
            .key(old)?
            .ok_or_else(|| StateError::Conflict(format!("{old:?} has no row")))?;
        let new_id = self.key_or_insert(new)?;
        self.conn
            .prepare_cached("UPDATE local SET id = ?2 WHERE id = ?1")?
            .execute([old_id, new_id])?;
        self.conn
            .prepare_cached("UPDATE local SET parent = ?2 WHERE parent = ?1")?
            .execute([old_id, new_id])?;
        Ok(())
    }

    /// `path` and everything below it with their ids, unsorted.
    fn subtree_raw(&self, path: &RelPath) -> Result<Vec<(RelPath, RawLocal)>> {
        let Some(start) = self.local_id_at(path)? else {
            return Ok(Vec::new());
        };
        let mut stmt = self.conn.prepare_cached(&format!(
            "WITH RECURSIVE sub(id, path, depth) AS ( \
                 VALUES (?1, ?2, 0) \
                 UNION ALL \
                 SELECT l.id, CASE WHEN sub.path = '' THEN l.name ELSE sub.path || '/' || l.name END, \
                 sub.depth + 1 \
                 FROM local l JOIN sub ON l.parent = sub.id WHERE sub.depth < ?3) \
             SELECT {LOCAL_COLS}, sub.path, sub.depth FROM sub JOIN local l ON l.id = sub.id \
             JOIN nodes n ON n.id = l.id"
        ))?;
        let rows = stmt.query_map(params![start, path.as_str(), MAX_DEPTH as i64], |r| {
            Ok((
                r.get::<_, String>(LOCAL_COL_COUNT)?,
                r.get::<_, i64>(LOCAL_COL_COUNT + 1)?,
                RawLocal::read(r, 0)?,
            ))
        })?;
        rows.map(|r| {
            let (p, depth, raw) = r?;
            if depth as usize >= MAX_DEPTH {
                return Err(StateError::Corrupt(format!(
                    "below {path}: too deep, or a cycle"
                )));
            }
            Ok((codec::rel_path(&p)?, raw))
        })
        .collect()
    }

    /// `path` and everything below it, in walk order.
    pub fn local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        let mut rows = self
            .subtree_raw(path)?
            .into_iter()
            .map(|(p, raw)| raw.into_row(p))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    /// The direct children of the folder at `parent` (the root for an empty path), in walk
    /// order.
    pub fn local_children(&self, parent: &RelPath) -> Result<Vec<LocalRow>> {
        match self.local_id_at(parent)? {
            Some(id) => self.children_of(id, parent),
            None => Ok(Vec::new()),
        }
    }

    /// The direct children of `folder` (a row read in this transaction), in walk order.
    pub fn local_children_of(&self, folder: &LocalRow) -> Result<Vec<LocalRow>> {
        match self.key(folder.node)? {
            Some(id) => self.children_of(id, &folder.path),
            None => Ok(Vec::new()),
        }
    }

    fn children_of(&self, id: i64, path: &RelPath) -> Result<Vec<LocalRow>> {
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE l.parent = ?1"),
            [id],
        )?;
        let mut rows = raw
            .into_iter()
            .map(|r| {
                let child = path
                    .join(&r.name)
                    .map_err(|e| StateError::Corrupt(format!("local name {:?}: {e}", r.name)))?;
                r.into_row(child)
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    /// Moves the row at `from`, and with it everything below, to `to` (a folder was moved or
    /// renamed): one row changes. Fails if `to` is taken or its folder has no row.
    pub fn move_local_subtree(&self, from: &RelPath, to: &RelPath) -> Result<()> {
        let conflict = || StateError::Conflict(format!("can't move {from} to {to}"));
        if from.is_root() || to.starts_with(from) {
            return Err(conflict());
        }
        let (Some(to_parent), Some(name)) = (to.parent(), to.name()) else {
            return Err(conflict());
        };
        let id = self.local_id_at(from)?.ok_or_else(conflict)?;
        let parent = self.folder_id_for_write(&to_parent)?.ok_or_else(conflict)?;
        if self.local_id_at(to)?.is_some() {
            return Err(conflict());
        }
        self.conn
            .prepare_cached("UPDATE local SET parent = ?2, name = ?3 WHERE id = ?1")?
            .execute(params![id, parent, name])?;
        Ok(())
    }

    /// Deletes the rows of `path` and everything below it and returns them.
    pub fn delete_local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        let raw = self.subtree_raw(path)?;
        let mut stmt = self
            .conn
            .prepare_cached("DELETE FROM local WHERE id = ?1")?;
        for (_, r) in &raw {
            stmt.execute([r.id])?;
        }
        let mut rows = raw
            .into_iter()
            .map(|(p, r)| r.into_row(p))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    pub fn local_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM local")
    }

    // ---- parked items ----

    /// Records that `node`, at `original`, is being parked under a temporary name in the same
    /// folder. Only the original name is kept: the place follows from the node's row (see the
    /// engine's `parked_place`).
    pub fn put_parked(&self, node: NodeId, original: &RelPath) -> Result<()> {
        let name = original
            .name()
            .ok_or_else(|| StateError::Conflict("the root can't be parked".into()))?;
        let id = self.key_or_insert(node)?;
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO parked (id, original_name) VALUES (?1, ?2)")?
            .execute(params![id, name])?;
        Ok(())
    }

    /// The name a parked node had.
    pub fn get_parked(&self, node: NodeId) -> Result<Option<String>> {
        Ok(self
            .conn
            .prepare_cached(&format!(
                "SELECT original_name FROM parked WHERE id = {ID_OF}"
            ))?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?)
    }

    /// Every parked node.
    pub fn all_parked(&self) -> Result<Vec<NodeId>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT n.uuid FROM parked k JOIN nodes n ON n.id = k.id")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    pub fn delete_parked(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM parked WHERE id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }

    // ---- forks in progress ----

    /// Records, before the rename, that `node`'s file becomes the conflict copy `new_node`
    /// under the name of `to`, in the same folder. Only the name is kept, so the record holds
    /// wherever the folder moves meanwhile.
    pub fn put_fork(&self, node: NodeId, new_node: NodeId, to: &RelPath) -> Result<()> {
        let name = to
            .name()
            .ok_or_else(|| StateError::Conflict("the root can't be forked".into()))?;
        let (id, new_id) = (self.key_or_insert(node)?, self.key_or_insert(new_node)?);
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO forks (id, new_node, to_name) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![id, new_id, name])?;
        Ok(())
    }

    /// `(node, new node, conflict-copy name)` of every fork in progress.
    pub fn all_forks(&self) -> Result<Vec<(NodeId, NodeId, String)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT n.uuid, m.uuid, f.to_name FROM forks f \
             JOIN nodes n ON n.id = f.id JOIN nodes m ON m.id = f.new_node",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|r| {
            let (node, new_node, name) = r?;
            Ok((codec::node(&node)?, codec::node(&new_node)?, name))
        })
        .collect()
    }

    pub fn delete_fork(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM forks WHERE id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }

    // ---- remote mirror ----

    pub fn get_mirror(&self, node: NodeId) -> Result<Option<MirrorRow>> {
        self.one(
            &format!("{MIRROR_SELECT} WHERE n.uuid = ?1"),
            node.0.as_bytes(),
            mirror_row,
        )
    }

    pub fn mirror_by_file(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        self.one(
            &format!("{MIRROR_SELECT} WHERE m.file_id = ?1"),
            file_id.0.as_bytes(),
            mirror_row,
        )
    }

    pub fn mirror_children(&self, parent: NodeId) -> Result<Vec<MirrorRow>> {
        self.many(
            &format!("{MIRROR_SELECT} WHERE m.parent = {ID_OF}"),
            [parent.0.as_bytes()],
            mirror_row,
        )
    }

    /// Records a live remote node. A row for the same node under an older file id (the node
    /// was deleted and re-created) is replaced.
    pub fn put_mirror(&self, row: &MirrorRow) -> Result<()> {
        let (id, parent) = (
            self.key_or_insert(row.node)?,
            self.key_or_insert(row.parent)?,
        );
        let f = row.file.as_ref();
        self.conn
            .prepare_cached(
                "INSERT INTO remote (id, file_id, version_tag, parent, kind, name, \
                 hash, size, mtime_ms, executable, content_extra) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
                 ON CONFLICT (id) DO UPDATE SET file_id = ?2, version_tag = ?3, parent = ?4, \
                 kind = ?5, name = ?6, hash = ?7, size = ?8, mtime_ms = ?9, executable = ?10, \
                 content_extra = ?11",
            )?
            .execute(params![
                id,
                row.file_id.0.as_bytes(),
                row.version.0.as_bytes(),
                parent,
                codec::kind_to(row.kind),
                row.name,
                f.map(|f| codec::hash_to(&f.hash)),
                f.map(|f| f.size as i64),
                f.map(|f| f.mtime_ms),
                f.is_some_and(|f| f.executable),
                row.content_extra,
            ])?;
        Ok(())
    }

    /// Removes the mirror row of a deleted file and returns it.
    pub fn delete_mirror(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        let row = self.mirror_by_file(file_id)?;
        if row.is_some() {
            self.change(
                "DELETE FROM remote WHERE file_id = ?1",
                file_id.0.as_bytes(),
            )?;
        }
        Ok(row)
    }

    // ---- Recently deleted ----

    /// Records a removed node (replacing an older record of it).
    pub fn put_removed(&self, removed: &RemovedRow) -> Result<()> {
        let (row, f) = (&removed.row, removed.row.file.as_ref());
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO removed (node, file_id, version_tag, parent, kind, name, \
                 hash, size, mtime_ms, executable, content_extra, removed_at, root) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )?
            .execute(params![
                row.node.0.as_bytes(),
                row.file_id.0.as_bytes(),
                row.version.0.as_bytes(),
                row.parent.0.as_bytes(),
                codec::kind_to(row.kind),
                row.name,
                f.map(|f| codec::hash_to(&f.hash)),
                f.map(|f| f.size as i64),
                f.map(|f| f.mtime_ms),
                f.is_some_and(|f| f.executable),
                row.content_extra,
                removed.removal.at_ms,
                removed.removal.root.0.as_bytes(),
            ])?;
        Ok(())
    }

    pub fn removed_by_node(&self, node: NodeId) -> Result<Option<RemovedRow>> {
        self.one(
            &format!("{REMOVED_SELECT} WHERE node = ?1"),
            node.0.as_bytes(),
            removed_row,
        )
    }

    /// The removed nodes in folder `parent`.
    pub fn removed_children(&self, parent: NodeId) -> Result<Vec<RemovedRow>> {
        self.many(
            &format!("{REMOVED_SELECT} WHERE parent = ?1"),
            [parent.0.as_bytes()],
            removed_row,
        )
    }

    /// Recently deleted, newest first.
    pub fn list_removed(&self, limit: usize) -> Result<Vec<RemovedRow>> {
        self.many(
            &format!("{REMOVED_SELECT} ORDER BY removed_at DESC, name LIMIT ?1"),
            [i64::try_from(limit).unwrap_or(i64::MAX)],
            removed_row,
        )
    }

    /// What was removed before `at_ms` (Unix ms), with the delete that took it along (its
    /// root, if still removed): a folder's delete goes as a whole. Files before folders.
    pub fn removed_before(&self, at_ms: i64) -> Result<Vec<RemovedRow>> {
        self.many(
            &format!(
                "{REMOVED_SELECT} WHERE removed_at < ?1 AND NOT EXISTS \
                 (SELECT 1 FROM removed r WHERE r.node = removed.root AND r.removed_at >= ?1) \
                 ORDER BY kind, removed_at"
            ),
            [at_ms],
            removed_row,
        )
    }

    /// Forgets a node's removal (it was restored, or deleted for good); whether it had one.
    pub fn delete_removed(&self, node: NodeId) -> Result<bool> {
        self.change("DELETE FROM removed WHERE node = ?1", node.0.as_bytes())
    }

    /// [`Store::delete_removed`] by file id (a tombstone carries no node id).
    pub fn delete_removed_file(&self, file_id: RemoteFileId) -> Result<bool> {
        self.change(
            "DELETE FROM removed WHERE file_id = ?1",
            file_id.0.as_bytes(),
        )
    }

    pub fn mirror_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM remote")
    }

    // ---- dirty set ----

    pub fn mark_dirty(&self, node: NodeId) -> Result<()> {
        let id = self.key_or_insert(node)?;
        self.conn
            .prepare_cached("INSERT OR IGNORE INTO dirty (id) VALUES (?1)")?
            .execute([id])?;
        Ok(())
    }

    /// Marks every node with a base or a remote row dirty: all of them are planned again.
    pub fn mark_synced_dirty(&self) -> Result<()> {
        self.conn
            .prepare_cached(
                "INSERT OR IGNORE INTO dirty (id) SELECT id FROM base UNION SELECT id FROM remote",
            )?
            .execute([])?;
        Ok(())
    }

    pub fn clear_dirty(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM dirty WHERE id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }

    pub fn is_dirty(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .conn
            .prepare_cached(&format!("SELECT 1 FROM dirty WHERE id = {ID_OF}"))?
            .query_row([node.0.as_bytes()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Up to `limit` dirty nodes in id order, starting after `after`, and the cursor for the
    /// next page: successive pages visit the whole set, so nodes that stay dirty can't keep the
    /// others waiting. Ids are never reused, so a cursor stays valid whatever happens to its
    /// node.
    pub fn dirty_page(
        &self,
        after: Option<DirtyCursor>,
        limit: usize,
    ) -> Result<(Vec<NodeId>, Option<DirtyCursor>)> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT d.id, n.uuid FROM dirty d JOIN nodes n ON n.id = d.id \
             WHERE d.id > ?1 ORDER BY d.id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after.map_or(0, |c| c.0), limit as i64], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let mut nodes = Vec::new();
        let mut last = None;
        for row in rows {
            let (id, uuid) = row?;
            nodes.push(codec::node(&uuid)?);
            last = Some(DirtyCursor(id));
        }
        Ok((nodes, last))
    }

    /// Local rows of dirty nodes that have neither a base nor a mirror row: what adoption may
    /// match against remote nodes.
    pub fn unpaired_local(&self) -> Result<Vec<LocalRow>> {
        let raw = self.raw_local(
            &format!(
                "SELECT {LOCAL_COLS} {LOCAL_FROM} \
                 WHERE l.id IN (SELECT id FROM dirty) \
                 AND NOT EXISTS (SELECT 1 FROM base b WHERE b.id = l.id) \
                 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = l.id)"
            ),
            [],
        )?;
        self.with_paths(raw)
    }

    pub fn dirty_nodes(&self, limit: usize) -> Result<Vec<NodeId>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT n.uuid FROM dirty d JOIN nodes n ON n.id = d.id LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be trashed locally). Online-only files have
    /// no local copy by design and don't count. Used by the mass-delete guard, which must see
    /// the whole delete, not one planning batch.
    pub fn pending_file_deletes(&self) -> Result<(u64, u64)> {
        let gone = |predicate: &str| {
            self.count(&format!(
                "SELECT COUNT(*) FROM dirty d JOIN base b ON b.id = d.id WHERE {predicate}"
            ))
        };
        Ok((gone(GONE_LOCALLY)?, gone(GONE_REMOTELY)?))
    }

    /// What a confirmed delete of dirty `node` covers of the
    /// [`pending_file_deletes`](Self::pending_file_deletes): whether `node` is a file gone
    /// locally (deleted remotely, one node at a time), and the files gone remotely in its local
    /// subtree (a trash takes everything under it along). `None` when `node` isn't dirty.
    pub fn confirmed_file_deletes(&self, node: NodeId) -> Result<Option<(bool, Vec<NodeId>)>> {
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "WITH RECURSIVE top(id) AS (SELECT {ID_OF}), \
                 sub(id) AS (SELECT id FROM top \
                   UNION SELECT l.id FROM local l JOIN sub ON l.parent = sub.id) \
                 SELECT n.uuid, d.id = (SELECT id FROM top), \
                   coalesce({GONE_LOCALLY}, 0), coalesce({GONE_REMOTELY}, 0) \
                 FROM sub JOIN dirty d ON d.id = sub.id JOIN nodes n ON n.id = d.id \
                 LEFT JOIN base b ON b.id = d.id"
            )
        });
        let mut stmt = self.conn.prepare_cached(&SQL)?;
        let rows = stmt.query_map([node.0.as_bytes()], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, bool>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, bool>(3)?,
            ))
        })?;
        let (mut dirty, mut gone_here, mut gone_there) = (false, false, Vec::new());
        for row in rows {
            let (uuid, top, here, there) = row?;
            if top {
                dirty = true;
                gone_here = here;
            }
            if there {
                gone_there.push(codec::node(&uuid)?);
            }
        }
        Ok(dirty.then_some((gone_here, gone_there)))
    }

    pub fn dirty_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM dirty")
    }

    // ---- key/value ----

    pub fn get_kv(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")?
            .query_row([key], |r| r.get(0))
            .optional()?)
    }

    pub fn set_kv(&self, key: &str, value: &str) -> Result<()> {
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")?
            .execute([key, value])?;
        Ok(())
    }

    pub fn delete_kv(&self, key: &str) -> Result<()> {
        self.change("DELETE FROM kv WHERE key = ?1", key)?;
        Ok(())
    }

    // ---- what was synced ----

    /// Adds `entries` (oldest first) and keeps only the newest `keep`.
    pub fn add_synced(&self, entries: &[Synced], keep: usize) -> Result<()> {
        let entries = &entries[entries.len().saturating_sub(keep)..];
        if entries.is_empty() {
            return Ok(());
        }
        let mut insert = self.conn.prepare_cached(
            "INSERT INTO synced (at, kind, path, files, bytes) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for e in entries {
            insert.execute(params![
                e.at as i64,
                codec::synced_kind_to(e.kind),
                e.path,
                e.files as i64,
                e.bytes as i64
            ])?;
        }
        self.conn
            .prepare_cached("DELETE FROM synced WHERE id <= (SELECT MAX(id) FROM synced) - ?1")?
            .execute([keep as i64])?;
        Ok(())
    }

    /// The newest `limit` entries, newest first.
    pub fn recent_synced(&self, limit: usize) -> Result<Vec<Synced>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT at, kind, path, files, bytes FROM synced ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (at, kind, path, files, bytes) = row?;
            Ok(Synced {
                at: at as u64,
                kind: codec::synced_kind_from(kind)?,
                path,
                files: files as u64,
                bytes: bytes as u64,
            })
        })
        .collect()
    }

    // ---- statement helpers (all through the statement cache) ----

    /// The row a one-parameter query finds, if any.
    fn one<T>(
        &self,
        sql: &str,
        param: impl rusqlite::ToSql,
        decode: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Option<T>> {
        self.conn
            .prepare_cached(sql)?
            .query_row([param], |r| Ok(decode(r)))
            .optional()?
            .transpose()
    }

    fn many<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        decode: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut stmt = self.conn.prepare_cached(sql)?;
        let rows = stmt.query_map(params, |r| Ok(decode(r)))?;
        rows.map(|r| r?).collect()
    }

    /// Runs a one-parameter statement; whether it changed a row.
    fn change(&self, sql: &str, param: impl rusqlite::ToSql) -> Result<bool> {
        Ok(self.conn.prepare_cached(sql)?.execute([param])? > 0)
    }

    fn count(&self, sql: &str) -> Result<u64> {
        let n: i64 = self.conn.prepare_cached(sql)?.query_row([], |r| r.get(0))?;
        Ok(n as u64)
    }
}

/// A local row as stored, before its path is known.
struct RawLocal {
    id: i64,
    uuid: Vec<u8>,
    kind: i64,
    size: i64,
    mtime_ns: i64,
    ctime_ns: i64,
    volume: i64,
    file_id: i64,
    file_id_hi: Option<i64>,
    executable: Option<bool>,
    hash: Option<Vec<u8>>,
    parent: i64,
    name: String,
    tags: Option<String>,
}

impl RawLocal {
    /// Reads [`LOCAL_COLS`] starting at column `at`.
    fn read(r: &Row<'_>, at: usize) -> rusqlite::Result<Self> {
        Ok(RawLocal {
            id: r.get(at)?,
            uuid: r.get(at + 1)?,
            kind: r.get(at + 2)?,
            size: r.get(at + 3)?,
            mtime_ns: r.get(at + 4)?,
            ctime_ns: r.get(at + 5)?,
            volume: r.get(at + 6)?,
            file_id: r.get(at + 7)?,
            file_id_hi: r.get(at + 8)?,
            executable: r.get(at + 9)?,
            hash: r.get(at + 10)?,
            parent: r.get(at + 11)?,
            name: r.get(at + 12)?,
            tags: r.get(at + 13)?,
        })
    }

    fn into_row(self, path: RelPath) -> Result<LocalRow> {
        Ok(LocalRow {
            path,
            node: codec::node(&self.uuid)?,
            kind: codec::kind_from(self.kind)?,
            fingerprint: codec::fingerprint_from(
                self.size,
                self.mtime_ns,
                self.ctime_ns,
                self.volume,
                self.file_id,
                self.file_id_hi,
            ),
            executable: self.executable,
            hash: self.hash.as_deref().map(codec::hash_from).transpose()?,
            tags: self.tags.as_deref().map(tags_from_str),
        })
    }
}

fn base_row(r: &Row<'_>) -> Result<BaseRow> {
    let kind = codec::kind_from(r.get(3)?)?;
    let content = match kind {
        NodeKind::Folder => None,
        NodeKind::File => {
            let hash: Vec<u8> = r
                .get::<_, Option<Vec<u8>>>(4)?
                .ok_or_else(|| StateError::Corrupt("file without hash".into()))?;
            Some(FileContent {
                hash: codec::hash_from(&hash)?,
                size: r.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
                mtime_ms: r.get::<_, Option<i64>>(6)?.unwrap_or(0),
                executable: r.get(7)?,
            })
        }
    };
    Ok(BaseRow {
        node: codec::node(&r.get::<_, Vec<u8>>(0)?)?,
        state: NodeState {
            parent: codec::node(&r.get::<_, Vec<u8>>(1)?)?,
            name: r.get(2)?,
            kind,
            content,
            tags: r
                .get::<_, Option<String>>(13)?
                .as_deref()
                .map(tags_from_str)
                .unwrap_or_default(),
        },
        remote: RemoteRef {
            file_id: codec::remote_file(&r.get::<_, Vec<u8>>(8)?)?,
            version: codec::version(&r.get::<_, Vec<u8>>(9)?)?,
        },
        content_state: codec::content_state_from(r.get(10)?)?,
        local_name: r.get(11)?,
        local_hash: r
            .get::<_, Option<Vec<u8>>>(12)?
            .as_deref()
            .map(codec::hash_from)
            .transpose()?,
    })
}

fn removed_row(r: &Row<'_>) -> Result<RemovedRow> {
    Ok(RemovedRow {
        row: mirror_row(r)?,
        removal: Removal {
            at_ms: r.get(11)?,
            root: codec::node(&r.get::<_, Vec<u8>>(12)?)?,
        },
    })
}

fn mirror_row(r: &Row<'_>) -> Result<MirrorRow> {
    let kind = codec::kind_from(r.get(4)?)?;
    let file = match kind {
        NodeKind::Folder => None,
        NodeKind::File => Some(MirrorFile {
            hash: codec::hash_from(&r.get::<_, Vec<u8>>(6)?)?,
            size: r.get::<_, Option<i64>>(7)?.unwrap_or(0) as u64,
            mtime_ms: r.get::<_, Option<i64>>(8)?.unwrap_or(0),
            executable: r.get(9)?,
        }),
    };
    Ok(MirrorRow {
        file_id: codec::remote_file(&r.get::<_, Vec<u8>>(0)?)?,
        version: codec::version(&r.get::<_, Vec<u8>>(1)?)?,
        node: codec::node(&r.get::<_, Vec<u8>>(2)?)?,
        parent: codec::node(&r.get::<_, Vec<u8>>(3)?)?,
        name: r.get(5)?,
        kind,
        file,
        content_extra: r.get(10)?,
    })
}
