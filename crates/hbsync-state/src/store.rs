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

/// The tables, seen through one open transaction.
#[derive(Debug)]
pub struct Store<'a> {
    conn: &'a Connection,
}

const BASE_COLS: &str = "node_id, parent_id, name, kind, hash, size, mtime_ms, executable, \
                         remote_file_id, version_tag, content_state, local_name";
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
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
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
                "INSERT OR REPLACE INTO local_items ({LOCAL_COLS}) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
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

    pub fn local_count(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM local_items", [], |r| {
                r.get::<_, i64>(0)
            })? as u64)
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
