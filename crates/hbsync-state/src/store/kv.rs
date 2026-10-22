//! Settings, and what was synced.

use super::*;

impl ReadTx<'_> {
    // ---- key/value ----

    /// This folder's setting `key`.
    pub fn get_kv(&self, key: FolderKey) -> Result<Option<String>> {
        Ok(self
            .prep("SELECT value FROM folder_kv WHERE folder = ?1 AND key = ?2")?
            .query_row(params![self.folder, key.0], |r| r.get(0))
            .optional()?)
    }

    /// The account's setting `key` (the feed's cursor, the identity).
    pub fn account_kv(&self, key: AccountKey) -> Result<Option<String>> {
        Ok(self
            .prep("SELECT value FROM kv WHERE key = ?1")?
            .query_row([key.0], |r| r.get(0))
            .optional()?)
    }

    // ---- what was synced ----

    /// The newest `limit` entries, newest first.
    pub fn recent_synced(&self, limit: usize) -> Result<Vec<Synced>> {
        let mut stmt = self.prep(
            "SELECT at, kind, path, files, bytes FROM synced WHERE folder = $F \
             ORDER BY id DESC LIMIT ?1",
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
}

impl FolderTx<'_> {
    pub fn set_kv(&self, key: FolderKey, value: &str) -> Result<()> {
        self.prep("INSERT OR REPLACE INTO folder_kv (folder, key, value) VALUES (?1, ?2, ?3)")?
            .execute(params![self.folder, key.0, value])?;
        Ok(())
    }

    pub fn delete_kv(&self, key: FolderKey) -> Result<()> {
        self.change(
            "DELETE FROM folder_kv WHERE folder = $F AND key = ?1",
            key.0,
        )?;
        Ok(())
    }

    /// Adds `entries` (oldest first) and keeps only the newest `keep`.
    pub fn add_synced(&self, entries: &[Synced], keep: usize) -> Result<()> {
        let entries = &entries[entries.len().saturating_sub(keep)..];
        if entries.is_empty() {
            return Ok(());
        }
        let mut insert = self.prep(
            "INSERT INTO synced (folder, at, kind, path, files, bytes) \
             VALUES ($F, ?1, ?2, ?3, ?4, ?5)",
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
        self.prep(
            "DELETE FROM synced WHERE folder = $F AND id NOT IN \
                 (SELECT id FROM synced WHERE folder = $F ORDER BY id DESC LIMIT ?1)",
        )?
        .execute([keep as i64])?;
        Ok(())
    }
}

impl AccountTx<'_> {
    pub(super) fn delete_account_kv(&self, key: AccountKey) -> Result<()> {
        self.change("DELETE FROM kv WHERE key = ?1", key.0)?;
        Ok(())
    }

    pub fn set_account_kv(&self, key: AccountKey, value: &str) -> Result<()> {
        self.prep("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")?
            .execute([key.0, value])?;
        Ok(())
    }
}

impl AccountTx<'_> {
    /// Counts a [`crate::StateDb::flush`]: the write its commit syncs.
    pub(crate) fn count_flush(&self) -> Result<()> {
        self.prep(
            "INSERT INTO kv (key, value) VALUES (?1, '1') \
             ON CONFLICT (key) DO UPDATE SET value = CAST(value AS INTEGER) + 1",
        )?
        .execute([crate::keys::FLUSHES.0])?;
        Ok(())
    }
}
