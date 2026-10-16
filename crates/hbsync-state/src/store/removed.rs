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
        // The same removal recorded again (its server time confirmed, its row as the feed has
        // it) keeps its place; another one (a restore, then a delete again) gets the next.
        let recorded: Option<(i64, Option<i64>)> = self
            .prep("SELECT seq, removed_at FROM removed WHERE node = ?1")?
            .query_row([row.node.0.as_bytes()], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let seq = match recorded {
            Some((seq, at)) if same_removal(at, removed.removed_at) => seq,
            _ => self.next_removed_seq()?,
        };
        let f = row.file.as_ref();
        self.prep(
            "INSERT OR REPLACE INTO removed (node, file_id, version_tag, parent, kind, name, \
                 hash, size, mtime_ms, executable, content_extra, removed_at, token, seq) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
            seq,
        ])?;
        self.fan_out(row.node, row.parent)
    }

    /// The largest [`RemovedRow::seq`] given so far (0: none): no row has a larger one.
    pub fn removed_seq(&self) -> Result<u64> {
        Ok(self
            .account_kv(crate::keys::REMOVED_SEQ)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    fn next_removed_seq(&self) -> Result<i64> {
        let next = self.removed_seq()? + 1;
        self.set_account_kv(crate::keys::REMOVED_SEQ, &next.to_string())?;
        Ok(next as i64)
    }

    /// Recently deleted's rows recorded up to `seq` ([`RemovedRow::seq`]): what a listing that
    /// saw up to it saw.
    pub fn removed_upto(&self, seq: u64) -> Result<Vec<RemovedRow>> {
        self.removed_rows(
            &format!("{REMOVED_SELECT} WHERE seq <= ?1"),
            [i64::try_from(seq).unwrap_or(i64::MAX)],
        )
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

    /// Forgets every file's token, in the mirror and Recently deleted: they were sealed under
    /// a login that is no longer this one's. The next update of each reads its header first.
    pub fn clear_tokens(&self) -> Result<()> {
        self.prep("UPDATE remote SET token = NULL WHERE token IS NOT NULL")?
            .execute([])?;
        self.prep("UPDATE removed SET token = NULL WHERE token IS NOT NULL")?
            .execute([])?;
        Ok(())
    }

    /// Whether the store has every one of `versions`, each `(version, removed)` in Recently
    /// deleted or else the mirror: one query (its text differs by length: not cached).
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
            .conn
            .prepare(&sql)?
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

    /// Recently deleted, newest first: NULL (an open time) first, as
    /// [`hbsync_types::activity::removal_time`] has it.
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

/// Whether a removal recorded with time `recorded` and one arriving with `arriving` are the
/// same: an open time (this device's own, before the server's comes back) or the same time.
fn same_removal(recorded: Option<i64>, arriving: Option<i64>) -> bool {
    recorded.is_none() || arriving.is_none() || recorded == arriving
}
