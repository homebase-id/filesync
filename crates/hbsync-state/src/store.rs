//! Typed access to the tables, inside one transaction.

use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_types::{
    ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, VersionTag,
};
use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::codec::{self, file_id_bytes};
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
    /// Content keys this client doesn't know (JSON object), written back unchanged.
    pub content_extra: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorFile {
    /// `None` if the writer sent no hash: the content must be downloaded to be compared.
    pub hash: Option<hbsync_types::ContentHash>,
    pub size: u64,
    pub mtime_ms: i64,
    pub executable: bool,
}

impl MirrorRow {
    pub fn remote_ref(&self) -> RemoteRef {
        RemoteRef {
            file_id: self.file_id,
            version: self.version,
        }
    }

    /// The node state, if complete (files need a hash).
    pub fn state(&self) -> Option<NodeState> {
        let content = match (self.kind, &self.file) {
            (NodeKind::Folder, _) => None,
            (NodeKind::File, Some(f)) => Some(FileContent {
                hash: f.hash?,
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
        })
    }
}

/// The tables, seen through one open transaction.
#[derive(Debug)]
pub struct Store<'a> {
    conn: &'a Connection,
}

const BASE_COLS: &str = "node_id, parent_id, name, kind, hash, size, mtime_ms, executable, \
                         remote_file_id, version_tag, content_state, local_name, local_hash";
const MIRROR_COLS: &str = "file_id, version_tag, node_id, parent_id, kind, name, hash, size, \
                           mtime_ms, executable, content_extra";
const LOCAL_COLS: &str =
    "path, node_id, kind, size, mtime_ns, ctime_ns, volume, file_id, executable, hash";

impl<'a> Store<'a> {
    pub(crate) fn new(conn: &'a Connection) -> Self {
        Store { conn }
    }

    // ---- base ----

    pub fn get_base(&self, node: NodeId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("SELECT {BASE_COLS} FROM base WHERE node_id = ?1"),
            node.0.as_bytes(),
            base_row,
        )
    }

    pub fn base_by_remote_file(&self, file_id: RemoteFileId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("SELECT {BASE_COLS} FROM base WHERE remote_file_id = ?1"),
            file_id.0.as_bytes(),
            base_row,
        )
    }

    pub fn base_children(&self, parent: NodeId) -> Result<Vec<BaseRow>> {
        self.many(
            &format!("SELECT {BASE_COLS} FROM base WHERE parent_id = ?1"),
            [parent.0.as_bytes()],
            base_row,
        )
    }

    /// Inserts or replaces the base row of `row.node`.
    pub fn put_base(&self, row: &BaseRow) -> Result<()> {
        let c = row.state.content.as_ref();
        self.conn
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO base ({BASE_COLS}) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
            ))?
            .execute(params![
                row.node.0.as_bytes(),
                row.state.parent.0.as_bytes(),
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
            ])?;
        Ok(())
    }

    pub fn delete_base(&self, node: NodeId) -> Result<bool> {
        self.change("DELETE FROM base WHERE node_id = ?1", node.0.as_bytes())
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

    pub fn get_local(&self, path: &RelPath) -> Result<Option<LocalRow>> {
        self.one(
            &format!("SELECT {LOCAL_COLS} FROM local_items WHERE path = ?1"),
            path.as_str(),
            local_row,
        )
    }

    pub fn local_by_node(&self, node: NodeId) -> Result<Option<LocalRow>> {
        self.one(
            &format!("SELECT {LOCAL_COLS} FROM local_items WHERE node_id = ?1"),
            node.0.as_bytes(),
            local_row,
        )
    }

    /// The node of the folder at `path`: the root for the empty path, `None` if no row is
    /// recorded there.
    pub fn local_node_at(&self, path: &RelPath) -> Result<Option<NodeId>> {
        if path.is_root() {
            return Ok(Some(NodeId::ROOT));
        }
        Ok(self.get_local(path)?.map(|r| r.node))
    }

    pub fn local_by_file_id(&self, id: &FileId) -> Result<Vec<LocalRow>> {
        self.many(
            &format!("SELECT {LOCAL_COLS} FROM local_items WHERE volume = ?1 AND file_id = ?2"),
            params![id.volume as i64, file_id_bytes(id)],
            local_row,
        )
    }

    /// Inserts or updates the row at `row.path`. Fails with [`StateError::Conflict`] if the node
    /// is already recorded at another path (the caller must move or delete that row first).
    pub fn put_local(&self, row: &LocalRow) -> Result<()> {
        if let Some(existing) = self.local_by_node(row.node)?
            && existing.path != row.path
        {
            return Err(StateError::Conflict(format!(
                "{:?} is already at {}",
                row.node, existing.path
            )));
        }
        let fp = &row.fingerprint;
        self.conn
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO local_items ({LOCAL_COLS}, parent_path) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
            ))?
            .execute(params![
                row.path.as_str(),
                row.node.0.as_bytes(),
                codec::kind_to(row.kind),
                fp.size as i64,
                fp.mtime_ns,
                fp.ctime_ns,
                fp.file_id.volume as i64,
                file_id_bytes(&fp.file_id),
                row.executable,
                row.hash.as_ref().map(codec::hash_to),
                row.path.parent().unwrap_or_default().as_str(),
            ])?;
        Ok(())
    }

    pub fn delete_local(&self, path: &RelPath) -> Result<bool> {
        self.change("DELETE FROM local_items WHERE path = ?1", path.as_str())
    }

    /// Up to `limit` rows strictly after `after` (or from the start), in walk order.
    pub fn local_page(&self, after: Option<&RelPath>, limit: usize) -> Result<Vec<LocalRow>> {
        // Two statements: an `?1 IS NULL OR path > ?1` filter would defeat the key seek and
        // make every page re-read the rows before it.
        match after {
            None => self.many(
                &format!("SELECT {LOCAL_COLS} FROM local_items ORDER BY path LIMIT ?1"),
                [limit as i64],
                local_row,
            ),
            Some(after) => self.many(
                &format!(
                    "SELECT {LOCAL_COLS} FROM local_items WHERE path > ?1 ORDER BY path LIMIT ?2"
                ),
                params![after.as_str(), limit as i64],
                local_row,
            ),
        }
    }

    /// `path` and everything below it, in walk order.
    pub fn local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        if path.is_root() {
            return self.local_page(None, usize::MAX >> 1);
        }
        // Under `hbpath`, every "p/…" sorts in ["p/", "p\x01"), and nothing else does.
        let (lo, hi) = (
            format!("{}/", path.as_str()),
            format!("{}\u{1}", path.as_str()),
        );
        self.many(
            &format!(
                "SELECT {LOCAL_COLS} FROM local_items \
             WHERE path = ?1 OR (path >= ?2 AND path < ?3) ORDER BY path"
            ),
            params![path.as_str(), lo, hi],
            local_row,
        )
    }

    /// The direct children of the folder at `parent` (the root for an empty path).
    pub fn local_children(&self, parent: &RelPath) -> Result<Vec<LocalRow>> {
        self.many(
            &format!("SELECT {LOCAL_COLS} FROM local_items WHERE parent_path = ?1 ORDER BY path"),
            [parent.as_str()],
            local_row,
        )
    }

    /// Re-roots the rows of `from` and everything below it at `to` (a folder was moved).
    /// Returns the number of rows moved. Fails if a target path is taken.
    pub fn move_local_subtree(&self, from: &RelPath, to: &RelPath) -> Result<usize> {
        if from.is_root() || to.starts_with(from) {
            return Err(StateError::Conflict(format!("can't move {from} to {to}")));
        }
        let to_parent = to.parent().unwrap_or_default();
        // Lengths in characters, as SQLite's substr counts them.
        let n = self.conn.execute(
            "UPDATE local_items SET \
               parent_path = CASE WHEN path = ?1 THEN ?3 \
                             ELSE ?2 || substr(parent_path, length(?1) + 1) END, \
               path = ?2 || substr(path, length(?1) + 1) \
             WHERE path = ?1 OR (path >= ?1 || '/' AND path < ?1 || char(1))",
            params![from.as_str(), to.as_str(), to_parent.as_str()],
        )?;
        Ok(n)
    }

    /// Deletes the rows of `path` and everything below it and returns them.
    pub fn delete_local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        let rows = self.local_subtree(path)?;
        if path.is_root() {
            self.conn.execute("DELETE FROM local_items", [])?;
        } else {
            self.conn
                .prepare_cached(
                    "DELETE FROM local_items WHERE path = ?1 OR (path >= ?1 || '/' AND path < ?1 || char(1))",
                )?
                .execute([path.as_str()])?;
        }
        Ok(rows)
    }

    // ---- parked items ----

    /// Records that `node` was parked under a temporary name, away from `original`. Where it
    /// is parked follows from its row (see the engine's `parked_place`).
    pub fn put_parked(&self, node: NodeId, original: &RelPath) -> Result<()> {
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO parked (node_id, original_path) VALUES (?1, ?2)",
            )?
            .execute(params![node.0.as_bytes(), original.as_str()])?;
        Ok(())
    }

    /// The original path of a parked node.
    pub fn get_parked(&self, node: NodeId) -> Result<Option<RelPath>> {
        let original = self
            .conn
            .prepare_cached("SELECT original_path FROM parked WHERE node_id = ?1")?
            .query_row([node.0.as_bytes()], |r| r.get::<_, String>(0))
            .optional()?;
        original.as_deref().map(codec::rel_path).transpose()
    }

    /// Every parked node.
    pub fn all_parked(&self) -> Result<Vec<NodeId>> {
        let mut stmt = self.conn.prepare_cached("SELECT node_id FROM parked")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    // ---- forks in progress ----

    /// Records, before renaming, that `node`'s file at `from` becomes `new_node` at `to`.
    pub fn put_fork(
        &self,
        node: NodeId,
        new_node: NodeId,
        from: &RelPath,
        to: &RelPath,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO forks (node_id, new_node, from_path, to_path) VALUES (?1, ?2, ?3, ?4)",
            params![node.0.as_bytes(), new_node.0.as_bytes(), from.as_str(), to.as_str()],
        )?;
        Ok(())
    }

    /// `(node, new node, from, to)` of every fork in progress.
    pub fn all_forks(&self) -> Result<Vec<(NodeId, NodeId, RelPath, RelPath)>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT node_id, new_node, from_path, to_path FROM forks")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        rows.map(|r| {
            let (node, new_node, from, to) = r?;
            Ok((
                codec::node(&node)?,
                codec::node(&new_node)?,
                codec::rel_path(&from)?,
                codec::rel_path(&to)?,
            ))
        })
        .collect()
    }

    pub fn delete_fork(&self, node: NodeId) -> Result<()> {
        self.change("DELETE FROM forks WHERE node_id = ?1", node.0.as_bytes())?;
        Ok(())
    }

    pub fn delete_parked(&self, node: NodeId) -> Result<()> {
        self.change("DELETE FROM parked WHERE node_id = ?1", node.0.as_bytes())?;
        Ok(())
    }

    pub fn local_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM local_items")
    }

    // ---- remote mirror ----

    pub fn get_mirror(&self, node: NodeId) -> Result<Option<MirrorRow>> {
        self.one(
            &format!("SELECT {MIRROR_COLS} FROM remote WHERE node_id = ?1"),
            node.0.as_bytes(),
            mirror_row,
        )
    }

    pub fn mirror_by_file(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        self.one(
            &format!("SELECT {MIRROR_COLS} FROM remote WHERE file_id = ?1"),
            file_id.0.as_bytes(),
            mirror_row,
        )
    }

    pub fn mirror_children(&self, parent: NodeId) -> Result<Vec<MirrorRow>> {
        self.many(
            &format!("SELECT {MIRROR_COLS} FROM remote WHERE parent_id = ?1"),
            [parent.0.as_bytes()],
            mirror_row,
        )
    }

    /// Records a live remote node. A row for the same node under an older file id (the node
    /// was deleted and re-created) is replaced.
    pub fn put_mirror(&self, row: &MirrorRow) -> Result<()> {
        self.conn
            .prepare_cached("DELETE FROM remote WHERE node_id = ?1 AND file_id != ?2")?
            .execute(params![row.node.0.as_bytes(), row.file_id.0.as_bytes()])?;
        let f = row.file.as_ref();
        self.conn
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO remote ({MIRROR_COLS}) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
            ))?
            .execute(params![
                row.file_id.0.as_bytes(),
                row.version.0.as_bytes(),
                row.node.0.as_bytes(),
                row.parent.0.as_bytes(),
                codec::kind_to(row.kind),
                row.name,
                f.and_then(|f| f.hash.as_ref().map(codec::hash_to)),
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

    pub fn mirror_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM remote")
    }

    // ---- dirty set ----

    pub fn mark_dirty(&self, node: NodeId) -> Result<()> {
        self.conn
            .prepare_cached("INSERT OR IGNORE INTO dirty (node_id) VALUES (?1)")?
            .execute([node.0.as_bytes()])?;
        Ok(())
    }

    pub fn clear_dirty(&self, node: NodeId) -> Result<()> {
        self.conn
            .prepare_cached("DELETE FROM dirty WHERE node_id = ?1")?
            .execute([node.0.as_bytes()])?;
        Ok(())
    }

    pub fn is_dirty(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .conn
            .prepare_cached("SELECT 1 FROM dirty WHERE node_id = ?1")?
            .query_row([node.0.as_bytes()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Up to `limit` dirty nodes in node id order, starting after `after`: successive pages
    /// visit the whole set, so nodes that stay dirty can't keep the others waiting.
    pub fn dirty_page(&self, after: Option<NodeId>, limit: usize) -> Result<Vec<NodeId>> {
        let after = after.map(|n| n.0.as_bytes().to_vec()).unwrap_or_default();
        let mut stmt = self.conn.prepare_cached(
            "SELECT node_id FROM dirty WHERE node_id > ?1 ORDER BY node_id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after, limit as i64], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    /// Local rows of dirty nodes that have neither a base nor a mirror row: what adoption may
    /// match against remote nodes.
    pub fn unpaired_local(&self) -> Result<Vec<LocalRow>> {
        self.many(
            &format!(
                "SELECT {LOCAL_COLS} FROM local_items l \
             WHERE node_id IN (SELECT node_id FROM dirty) \
             AND NOT EXISTS (SELECT 1 FROM base b WHERE b.node_id = l.node_id) \
             AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.node_id = l.node_id)"
            ),
            [],
            local_row,
        )
    }

    pub fn dirty_nodes(&self, limit: usize) -> Result<Vec<NodeId>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT node_id FROM dirty LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be trashed locally). Online-only files have
    /// no local copy by design and don't count. Used by the mass-delete guard, which must see
    /// the whole delete, not one planning batch.
    pub fn pending_file_deletes(&self) -> Result<(u64, u64)> {
        let local_gone: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM dirty d JOIN base b ON b.node_id = d.node_id \
             WHERE b.kind = 0 AND b.content_state != 2 \
             AND NOT EXISTS (SELECT 1 FROM local_items l WHERE l.node_id = d.node_id)",
            [],
            |r| r.get(0),
        )?;
        let remote_gone: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM dirty d JOIN base b ON b.node_id = d.node_id \
             WHERE b.kind = 0 \
             AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.node_id = d.node_id)",
            [],
            |r| r.get(0),
        )?;
        Ok((local_gone as u64, remote_gone as u64))
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

fn local_row(r: &Row<'_>) -> Result<LocalRow> {
    Ok(LocalRow {
        path: codec::rel_path(&r.get::<_, String>(0)?)?,
        node: codec::node(&r.get::<_, Vec<u8>>(1)?)?,
        kind: codec::kind_from(r.get(2)?)?,
        fingerprint: codec::fingerprint_from(
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
            &r.get::<_, Vec<u8>>(7)?,
        )?,
        executable: r.get(8)?,
        hash: r
            .get::<_, Option<Vec<u8>>>(9)?
            .as_deref()
            .map(codec::hash_from)
            .transpose()?,
    })
}

fn mirror_row(r: &Row<'_>) -> Result<MirrorRow> {
    let kind = codec::kind_from(r.get(4)?)?;
    let file = match kind {
        NodeKind::Folder => None,
        NodeKind::File => Some(MirrorFile {
            hash: r
                .get::<_, Option<Vec<u8>>>(6)?
                .as_deref()
                .map(codec::hash_from)
                .transpose()?,
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
