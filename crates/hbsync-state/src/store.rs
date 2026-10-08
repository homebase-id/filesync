//! Typed access to the tables, inside one transaction.

use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_types::{
    ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, VersionTag,
};
use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::codec::{self, file_id_bytes};
use crate::{Result, StateError};

/// Where a node lives on the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RemoteRef {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
}

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
        self.conn
            .query_row(
                &format!("SELECT {BASE_COLS} FROM base WHERE node_id = ?1"),
                [node.0.as_bytes()],
                |r| Ok(base_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn base_by_remote_file(&self, file_id: RemoteFileId) -> Result<Option<BaseRow>> {
        self.conn
            .query_row(
                &format!("SELECT {BASE_COLS} FROM base WHERE remote_file_id = ?1"),
                [file_id.0.as_bytes()],
                |r| Ok(base_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn base_children(&self, parent: NodeId) -> Result<Vec<BaseRow>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {BASE_COLS} FROM base WHERE parent_id = ?1"
        ))?;
        let rows = stmt.query_map([parent.0.as_bytes()], |r| Ok(base_row(r)))?;
        rows.map(|r| r?).collect()
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
        Ok(self
            .conn
            .execute("DELETE FROM base WHERE node_id = ?1", [node.0.as_bytes()])?
            > 0)
    }

    pub fn base_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM base", [], |r| r.get::<_, i64>(0))? as u64)
    }

    /// Number of file (not folder) nodes in the base; the mass-delete guard's denominator.
    pub fn base_file_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM base WHERE kind = 0", [], |r| {
                r.get::<_, i64>(0)
            })? as u64)
    }

    /// Every base row. For tests and small trees; production code walks by parent.
    pub fn all_base(&self) -> Result<Vec<BaseRow>> {
        let mut stmt = self
            .conn
            .prepare_cached(&format!("SELECT {BASE_COLS} FROM base"))?;
        let rows = stmt.query_map([], |r| Ok(base_row(r)))?;
        rows.map(|r| r?).collect()
    }

    // ---- local observations ----

    pub fn get_local(&self, path: &RelPath) -> Result<Option<LocalRow>> {
        self.conn
            .query_row(
                &format!("SELECT {LOCAL_COLS} FROM local_items WHERE path = ?1"),
                [path.as_str()],
                |r| Ok(local_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn local_by_node(&self, node: NodeId) -> Result<Option<LocalRow>> {
        self.conn
            .query_row(
                &format!("SELECT {LOCAL_COLS} FROM local_items WHERE node_id = ?1"),
                [node.0.as_bytes()],
                |r| Ok(local_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn local_by_file_id(&self, id: &FileId) -> Result<Vec<LocalRow>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {LOCAL_COLS} FROM local_items WHERE volume = ?1 AND file_id = ?2"
        ))?;
        let rows = stmt.query_map(params![id.volume as i64, file_id_bytes(id)], |r| {
            Ok(local_row(r))
        })?;
        rows.map(|r| r?).collect()
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
        Ok(self
            .conn
            .execute("DELETE FROM local_items WHERE path = ?1", [path.as_str()])?
            > 0)
    }

    /// Up to `limit` rows strictly after `after` (or from the start), in walk order.
    pub fn local_page(&self, after: Option<&RelPath>, limit: usize) -> Result<Vec<LocalRow>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {LOCAL_COLS} FROM local_items WHERE ?1 IS NULL OR path > ?1 \
             ORDER BY path LIMIT ?2"
        ))?;
        let rows = stmt.query_map(params![after.map(RelPath::as_str), limit as i64], |r| {
            Ok(local_row(r))
        })?;
        rows.map(|r| r?).collect()
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
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {LOCAL_COLS} FROM local_items \
             WHERE path = ?1 OR (path >= ?2 AND path < ?3) ORDER BY path"
        ))?;
        let rows = stmt.query_map(params![path.as_str(), lo, hi], |r| Ok(local_row(r)))?;
        rows.map(|r| r?).collect()
    }

    /// The direct children of the folder at `parent` (the root for an empty path).
    pub fn local_children(&self, parent: &RelPath) -> Result<Vec<LocalRow>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {LOCAL_COLS} FROM local_items WHERE parent_path = ?1 ORDER BY path"
        ))?;
        let rows = stmt.query_map([parent.as_str()], |r| Ok(local_row(r)))?;
        rows.map(|r| r?).collect()
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
        for row in &rows {
            self.delete_local(&row.path)?;
        }
        Ok(rows)
    }

    // ---- parked items ----

    /// Records that `node` was moved from `original` to the temporary `parked` path.
    pub fn put_parked(&self, node: NodeId, parked: &RelPath, original: &RelPath) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO parked (node_id, parked_path, original_path) VALUES (?1, ?2, ?3)",
            params![node.0.as_bytes(), parked.as_str(), original.as_str()],
        )?;
        Ok(())
    }

    /// `(parked path, original path)` of a parked node.
    pub fn get_parked(&self, node: NodeId) -> Result<Option<(RelPath, RelPath)>> {
        let row = self
            .conn
            .prepare_cached("SELECT parked_path, original_path FROM parked WHERE node_id = ?1")?
            .query_row([node.0.as_bytes()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .optional()?;
        row.map(|(parked, original)| {
            let path = |p: &str| {
                RelPath::new(p).map_err(|e| StateError::Corrupt(format!("parked path {p:?}: {e}")))
            };
            Ok((path(&parked)?, path(&original)?))
        })
        .transpose()
    }

    /// `(node, parked path, original path)` of every parked item.
    pub fn all_parked(&self) -> Result<Vec<(NodeId, RelPath, RelPath)>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT node_id, parked_path, original_path FROM parked")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|r| {
            let (node, parked, original) = r?;
            let path = |p: &str| {
                RelPath::new(p).map_err(|e| StateError::Corrupt(format!("parked path {p:?}: {e}")))
            };
            Ok((codec::node(&node)?, path(&parked)?, path(&original)?))
        })
        .collect()
    }

    pub fn delete_parked(&self, node: NodeId) -> Result<()> {
        self.conn
            .execute("DELETE FROM parked WHERE node_id = ?1", [node.0.as_bytes()])?;
        Ok(())
    }

    pub fn local_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM local_items", [], |r| {
                r.get::<_, i64>(0)
            })? as u64)
    }

    // ---- remote mirror ----

    pub fn get_mirror(&self, node: NodeId) -> Result<Option<MirrorRow>> {
        self.conn
            .query_row(
                &format!("SELECT {MIRROR_COLS} FROM remote WHERE node_id = ?1"),
                [node.0.as_bytes()],
                |r| Ok(mirror_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn mirror_by_file(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        self.conn
            .query_row(
                &format!("SELECT {MIRROR_COLS} FROM remote WHERE file_id = ?1"),
                [file_id.0.as_bytes()],
                |r| Ok(mirror_row(r)),
            )
            .optional()?
            .transpose()
    }

    pub fn mirror_children(&self, parent: NodeId) -> Result<Vec<MirrorRow>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT {MIRROR_COLS} FROM remote WHERE parent_id = ?1"
        ))?;
        let rows = stmt.query_map([parent.0.as_bytes()], |r| Ok(mirror_row(r)))?;
        rows.map(|r| r?).collect()
    }

    pub fn all_mirror(&self) -> Result<Vec<MirrorRow>> {
        let mut stmt = self
            .conn
            .prepare_cached(&format!("SELECT {MIRROR_COLS} FROM remote"))?;
        let rows = stmt.query_map([], |r| Ok(mirror_row(r)))?;
        rows.map(|r| r?).collect()
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
            self.conn.execute(
                "DELETE FROM remote WHERE file_id = ?1",
                [file_id.0.as_bytes()],
            )?;
        }
        Ok(row)
    }

    pub fn mirror_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM remote", [], |r| r.get::<_, i64>(0))?
            as u64)
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
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM dirty", [], |r| r.get::<_, i64>(0))? as u64)
    }

    // ---- key/value ----

    pub fn get_kv(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM kv WHERE key = ?1", [key], |r| r.get(0))
            .optional()?)
    }

    pub fn set_kv(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            [key, value],
        )?;
        Ok(())
    }

    pub fn delete_kv(&self, key: &str) -> Result<()> {
        self.conn.execute("DELETE FROM kv WHERE key = ?1", [key])?;
        Ok(())
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
    let path: String = r.get(0)?;
    Ok(LocalRow {
        path: RelPath::new(&path)
            .map_err(|e| StateError::Corrupt(format!("path {path:?}: {e}")))?,
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
