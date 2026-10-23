//! Recently deleted: the drive's removed nodes.

use super::*;

/// Whether the version `q.(f, v)` (removed: `q.r`) isn't in the store, nor recorded as one this
/// client can't read.
const UNKNOWN: &str = "CASE WHEN q.r THEN NOT EXISTS (SELECT 1 FROM removed \
     WHERE file_id = q.f AND version_tag = q.v) \
     ELSE NOT EXISTS (SELECT 1 FROM remote WHERE file_id = q.f AND version_tag = q.v) END \
     AND NOT EXISTS (SELECT 1 FROM unreadable WHERE file_id = q.f AND version_tag = q.v)";

/// A version's query variables: file id, version tag, removed.
fn version_params(at: &RemoteRef, removed: bool) -> [rusqlite::types::Value; 3] {
    [
        rusqlite::types::Value::Blob(at.file_id.0.as_bytes().to_vec()),
        rusqlite::types::Value::Blob(at.version.0.as_bytes().to_vec()),
        rusqlite::types::Value::Integer(i64::from(removed)),
    ]
}

/// Versions [`ReadTx::known`] and [`ReadTx::first_unknown`] ask in one query: three or four
/// variables each, under SQLite's limit (32,766 in the bundled SQLite, 999 in old ones).
const KNOWN_CHUNK: usize = 240;

impl ReadTx<'_> {
    // ---- Recently deleted ----

    /// The largest [`Recorded::seq`] given so far (0: none): no row has a larger one, and a row
    /// recorded later gets a larger one.
    pub fn removed_seq(&self) -> Result<u64> {
        if let Some((seq, _)) = self.removed_seq.get() {
            return Ok(seq);
        }
        let seq = self
            .account_kv(crate::keys::REMOVED_SEQ)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        self.removed_seq.set(Some((seq, false)));
        Ok(seq)
    }

    /// The next [`Recorded::seq`]: the counter is written once, when the transaction commits.
    fn next_removed_seq(&self) -> Result<i64> {
        let next = self.removed_seq()? + 1;
        self.removed_seq.set(Some((next, true)));
        Ok(next as i64)
    }

    /// How many nodes folder `parent` holds on the drive: live, or in Recently deleted.
    pub fn child_count(&self, parent: NodeId) -> Result<u64> {
        let parent = self.coords()?.to_drive(parent);
        let n: i64 = self
            .prep(&format!(
                "SELECT (SELECT COUNT(*) FROM remote m WHERE m.parent = {ID_OF}) \
                     + (SELECT COUNT(*) FROM removed WHERE parent = ?1)"
            ))?
            .query_row([parent.0.as_bytes()], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// The nodes of Recently deleted that hold a live node: one query.
    pub fn removed_holding_live(&self) -> Result<std::collections::HashSet<NodeId>> {
        let c = self.coords()?;
        let nodes = self.many(
            "SELECT r.node FROM removed r JOIN nodes n ON n.uuid = r.node \
             WHERE EXISTS (SELECT 1 FROM remote m WHERE m.parent = n.id)",
            [],
            |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
        )?;
        Ok(nodes.into_iter().map(|n| c.from_drive(n)).collect())
    }

    /// The folders of Recently deleted holding a live node written no later than their removal,
    /// both times known (the server's): those whose delete may have left live what it took
    /// along (node schema, The purge, 2.; a file takes nothing along). One query.
    pub fn removed_holding_taken(&self) -> Result<std::collections::HashSet<NodeId>> {
        let c = self.coords()?;
        let nodes = self.many(
            "SELECT r.node FROM removed r JOIN nodes n ON n.uuid = r.node \
             WHERE r.kind = 1 AND r.removed_at IS NOT NULL AND EXISTS (SELECT 1 FROM remote m \
                 WHERE m.parent = n.id AND m.changed_at <= r.removed_at)",
            [],
            |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
        )?;
        Ok(nodes.into_iter().map(|n| c.from_drive(n)).collect())
    }

    /// How many nodes of Recently deleted each folder holds: one grouped query.
    pub fn removed_per_folder(&self) -> Result<HashMap<NodeId, u64>> {
        let c = self.coords()?;
        let counts = self.many(
            "SELECT parent, COUNT(*) FROM removed GROUP BY parent",
            [],
            |r| {
                Ok((
                    codec::node(&r.get::<_, Vec<u8>>(0)?)?,
                    r.get::<_, i64>(1)? as u64,
                ))
            },
        )?;
        Ok(counts
            .into_iter()
            .map(|(n, count)| (c.from_drive(n), count))
            .collect())
    }

    /// This DB's own id (random, made with it): a listing of its Recently deleted names it, so
    /// one of another DB (of the same identity, made again) is told apart.
    pub fn db_id(&self) -> Result<String> {
        Ok(self.account_kv(crate::keys::DB_ID)?.unwrap_or_default())
    }

    /// Recently deleted's rows recorded up to `seq`: what a listing that read it saw.
    pub fn recorded_upto(&self, seq: u64) -> Result<Vec<Recorded>> {
        self.recorded_rows(
            &format!("{REMOVED_SELECT} WHERE seq <= ?1 ORDER BY seq"),
            [i64::try_from(seq).unwrap_or(i64::MAX)],
        )
    }

    /// The row of Recently deleted that is `node`, with its [`Recorded::seq`]
    /// ([`ReadTx::removed_by_node`]).
    pub fn recorded_by_node(&self, node: NodeId) -> Result<Option<Recorded>> {
        Ok(self
            .recorded_rows(
                &format!("{REMOVED_SELECT} WHERE node = ?1"),
                [node.0.as_bytes()],
            )?
            .pop())
    }

    /// The removed nodes in folder `parent`, with their [`Recorded::seq`]
    /// ([`ReadTx::removed_children`]).
    pub fn recorded_children(&self, parent: NodeId) -> Result<Vec<Recorded>> {
        let parent = self.coords()?.to_drive(parent);
        self.recorded_rows(
            &format!("{REMOVED_SELECT} WHERE parent = ?1"),
            [parent.0.as_bytes()],
        )
    }

    fn recorded_rows(&self, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Recorded>> {
        let mut seen = Vec::new();
        for r in self.many(sql, params, recorded_row)? {
            if let Some(removed) = self.seen_removed(r.removed)? {
                seen.push(Recorded {
                    removed,
                    seq: r.seq,
                });
            }
        }
        Ok(seen)
    }

    /// Whether the store has every one of `versions`, each `(version, removed)` in Recently
    /// deleted or else the mirror: a query per [`KNOWN_CHUNK`] (SQLite allows only so many
    /// variables in one; its text differs by length: not cached), each ending at the first one
    /// missing.
    pub fn known(&self, versions: &[(RemoteRef, bool)]) -> Result<bool> {
        for chunk in versions.chunks(KNOWN_CHUNK) {
            if !self.known_chunk(chunk)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn known_chunk(&self, versions: &[(RemoteRef, bool)]) -> Result<bool> {
        let rows = vec!["(?, ?, ?)"; versions.len()].join(", ");
        let sql = format!(
            "WITH q(f, v, r) AS (VALUES {rows}) SELECT NOT EXISTS (SELECT 1 FROM q WHERE {UNKNOWN})"
        );
        let params = versions
            .iter()
            .flat_map(|(at, removed)| version_params(at, *removed));
        Ok(self
            .conn
            .prepare(&sql)?
            .query_row(rusqlite::params_from_iter(params), |r| r.get(0))?)
    }

    /// The first of `versions` the store doesn't have ([`ReadTx::known`]), by its index.
    pub fn first_unknown(&self, versions: &[(RemoteRef, bool)]) -> Result<Option<usize>> {
        for (n, chunk) in versions.chunks(KNOWN_CHUNK).enumerate() {
            if let Some(i) = self.first_unknown_in(chunk)? {
                return Ok(Some(n * KNOWN_CHUNK + i));
            }
        }
        Ok(None)
    }

    fn first_unknown_in(&self, versions: &[(RemoteRef, bool)]) -> Result<Option<usize>> {
        let rows = vec!["(?, ?, ?, ?)"; versions.len()].join(", ");
        let sql =
            format!("WITH q(i, f, v, r) AS (VALUES {rows}) SELECT MIN(q.i) FROM q WHERE {UNKNOWN}");
        let params = versions.iter().enumerate().flat_map(|(i, (at, removed))| {
            std::iter::once(rusqlite::types::Value::Integer(i as i64))
                .chain(version_params(at, *removed))
        });
        let first: Option<i64> = self
            .conn
            .prepare(&sql)?
            .query_row(rusqlite::params_from_iter(params), |r| r.get(0))?;
        Ok(first.map(|i| i as usize))
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
    /// [`hbsync_types::removal_time`] has it.
    pub fn list_removed(&self, limit: usize) -> Result<Vec<RemovedRow>> {
        self.removed_rows(
            &format!(
                "{REMOVED_SELECT} ORDER BY removed_at IS NULL DESC, removed_at DESC, name LIMIT ?1"
            ),
            [i64::try_from(limit).unwrap_or(i64::MAX)],
        )
    }

    /// The nodes in Recently deleted named one of `names`: one read.
    pub fn removed_named(&self, names: &[&str]) -> Result<Vec<NodeId>> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let c = self.coords()?;
        let marks = vec!["?"; names.len()].join(", ");
        let nodes = self.many(
            &format!("SELECT node FROM removed WHERE name IN ({marks})"),
            rusqlite::params_from_iter(names),
            |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
        )?;
        Ok(nodes.into_iter().filter(|n| !c.hides(*n)).collect())
    }

    /// What was removed before `at_ms` (Unix ms), oldest first; an open time isn't, nor a 0 (an
    /// earlier build's for a header without one).
    pub fn removed_before(&self, at_ms: i64) -> Result<Vec<RemovedRow>> {
        self.removed_rows(
            &format!(
                "{REMOVED_SELECT} WHERE removed_at > 0 AND removed_at < ?1 ORDER BY removed_at"
            ),
            [at_ms],
        )
    }

    pub fn mirror_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM remote")
    }
}

impl AccountTx<'_> {
    /// Records a removed node (replacing an older record of it), and marks it dirty where that
    /// concerns (`fan_out`); the same version again changes nothing, and keeps its
    /// [`Recorded::seq`], and so does a file id deleted for good ([`ReadTx::file_gone`]).
    /// Another version is another removal (a restore, an edit, then a delete again, perhaps
    /// elsewhere): it gets the next.
    pub(crate) fn put_removed(&self, removed: &RemovedRow) -> Result<()> {
        if self.has_version(removed.row.remote_ref(), true)?
            || self.file_gone(removed.row.file_id)?
        {
            return Ok(());
        }
        // A new row of it: it is there, whatever the identity said once.
        self.clear_file_marks(removed.row.file_id)?;
        let Some(row) = &self.on_drive(&removed.row)? else {
            return Ok(());
        };
        self.moved_folder(row.node, row.kind);
        let seq = self.next_removed_seq()?;
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
            f.and_then(|f| f.mtime_ms),
            f.is_some_and(|f| f.executable),
            row.content_extra,
            removed.removed_at,
            row.token.as_ref().map(|t| &t.0[..]),
            seq,
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

    /// Forgets every file's token, in the mirror and Recently deleted: they were sealed under
    /// a login that is no longer this one's. The next update of each reads its header first.
    pub fn clear_tokens(&self) -> Result<()> {
        self.prep("UPDATE remote SET token = NULL WHERE token IS NOT NULL")?
            .execute([])?;
        self.prep("UPDATE removed SET token = NULL WHERE token IS NOT NULL")?
            .execute([])?;
        Ok(())
    }

    /// Forgets a node's removal (it was restored, or deleted for good); whether it had one.
    pub(crate) fn delete_removed(&self, node: NodeId) -> Result<bool> {
        self.change("DELETE FROM removed WHERE node = ?1", node.0.as_bytes())
    }

    /// `delete_removed` by file id (a tombstone carries no node id).
    pub(crate) fn delete_removed_file(&self, file_id: RemoteFileId) -> Result<bool> {
        self.change(
            "DELETE FROM removed WHERE file_id = ?1",
            file_id.0.as_bytes(),
        )
    }
}

impl Store<'_> {
    /// What this transaction leaves for its commit: the counter, if it gave a [`Recorded::seq`].
    pub(crate) fn finish(&self) -> Result<()> {
        if let Some((seq, true)) = self.removed_seq.get() {
            self.account()
                .set_account_kv(crate::keys::REMOVED_SEQ, &seq.to_string())?;
            self.removed_seq.set(Some((seq, false)));
        }
        Ok(())
    }
}
