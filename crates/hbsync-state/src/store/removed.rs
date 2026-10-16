//! Recently deleted: the drive's removed nodes.

use super::*;

impl Store<'_> {
    // ---- Recently deleted ----

    /// Records a removed node (replacing an older record of it), and marks it dirty where that
    /// concerns ([`Store::fan_out`]); the same version again changes nothing.
    pub fn put_removed(&self, removed: &RemovedRow) -> Result<()> {
        if self.has_version(removed.row.remote_ref(), true)? {
            return Ok(());
        }
        let Some(row) = &self.on_drive(&removed.row)? else {
            return Ok(());
        };
        self.moved_folder(row.node, row.kind);
        let f = row.file.as_ref();
        self.prep(
            "INSERT OR REPLACE INTO removed (node, file_id, version_tag, parent, kind, name, \
                 hash, size, mtime_ms, executable, content_extra, removed_at, token) \
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
            removed.removed_at,
            row.token.as_ref().map(|t| &t.0[..]),
        ])?;
        self.fan_out(row.node, row.parent)
    }

    /// Records the server's time of a removal this device made, open until now
    /// ([`RemovedRow::removed_at`]); a time already known stays.
    pub fn confirm_removed(&self, at: RemoteRef, removed_at: i64) -> Result<()> {
        self.prep(
            "UPDATE removed SET removed_at = ?3 \
             WHERE file_id = ?1 AND version_tag = ?2 AND removed_at IS NULL",
        )?
        .execute(params![
            at.file_id.0.as_bytes(),
            at.version.0.as_bytes(),
            removed_at
        ])?;
        Ok(())
    }

    /// Whether the store has every one of `versions`, each `(version, removed)` in Recently
    /// deleted or else the mirror: one query.
    pub fn known(&self, versions: &[(RemoteRef, bool)]) -> Result<bool> {
        if versions.is_empty() {
            return Ok(true);
        }
        let rows = vec!["(?, ?, ?)"; versions.len()].join(", ");
        let sql = format!(
            "WITH q(f, v, r) AS (VALUES {rows}) SELECT NOT EXISTS (SELECT 1 FROM q WHERE \
             CASE WHEN q.r THEN NOT EXISTS (SELECT 1 FROM removed \
                 WHERE file_id = q.f AND version_tag = q.v) \
             ELSE NOT EXISTS (SELECT 1 FROM remote \
                 WHERE file_id = q.f AND version_tag = q.v) END)"
        );
        let params = versions.iter().flat_map(|(at, removed)| {
            [
                rusqlite::types::Value::Blob(at.file_id.0.as_bytes().to_vec()),
                rusqlite::types::Value::Blob(at.version.0.as_bytes().to_vec()),
                rusqlite::types::Value::Integer(i64::from(*removed)),
            ]
        });
        Ok(self
            .prep(&sql)?
            .query_row(rusqlite::params_from_iter(params), |r| r.get(0))?)
    }

    pub fn removed_by_node(&self, node: NodeId) -> Result<Option<RemovedRow>> {
        match self.one(
            &format!("{REMOVED_SELECT} WHERE node = ?1"),
            node.0.as_bytes(),
            removed_row,
        )? {
            Some(r) => self.seen_removed(r),
            None => Ok(None),
        }
    }

    /// Rows of Recently deleted as this folder sees them.
    pub(super) fn removed_rows(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<RemovedRow>> {
        let mut seen = Vec::new();
        for r in self.many(sql, params, removed_row)? {
            seen.extend(self.seen_removed(r)?);
        }
        Ok(seen)
    }

    /// The removed nodes in folder `parent`.
    pub fn removed_children(&self, parent: NodeId) -> Result<Vec<RemovedRow>> {
        let parent = self.coords()?.to_drive(parent);
        self.removed_rows(
            &format!("{REMOVED_SELECT} WHERE parent = ?1"),
            [parent.0.as_bytes()],
        )
    }

    /// Recently deleted, newest first (an open time is the newest).
    pub fn list_removed(&self, limit: usize) -> Result<Vec<RemovedRow>> {
        self.removed_rows(
            &format!(
                "{REMOVED_SELECT} ORDER BY removed_at IS NULL DESC, removed_at DESC, name LIMIT ?1"
            ),
            [i64::try_from(limit).unwrap_or(i64::MAX)],
        )
    }

    /// What was removed before `at_ms` (Unix ms), oldest first; an open time isn't.
    pub fn removed_before(&self, at_ms: i64) -> Result<Vec<RemovedRow>> {
        self.removed_rows(
            &format!("{REMOVED_SELECT} WHERE removed_at < ?1 ORDER BY removed_at"),
            [at_ms],
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
}
