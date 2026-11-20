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

    /// [`ReadTx::account_kv`] read as a `T` (a time, a count); none where it doesn't read as
    /// one.
    pub fn account_kv_as<T: std::str::FromStr>(&self, key: AccountKey) -> Result<Option<T>> {
        Ok(self.account_kv(key)?.and_then(|v| v.parse().ok()))
    }

    /// Whether this DB is of another identity's drive, read as a member of what they share
    /// with this one ([`AccountTx::set_shared`]): its feed lists only what is shared, so the
    /// mirror may hold a node without its folder (shown at the top, [`ReadTx::mirror_children`]),
    /// and a tombstone without a version is a share withdrawn ([`ReadTx::withdrawn`]). Read
    /// once per transaction.
    pub fn shared(&self) -> Result<bool> {
        if let Some(shared) = self.shared.get() {
            return Ok(shared);
        }
        let shared = self.account_kv(crate::keys::SHARED)?.is_some();
        self.shared.set(Some(shared));
        Ok(shared)
    }

    /// Whether a deleted file's `at` says its share was withdrawn, not that it is gone: on
    /// another identity's drive ([`ReadTx::shared`]), a tombstone without a version (verified,
    /// API notes "Shared with you": the server's answer for a file this identity may no longer
    /// read, whose version it doesn't tell). The sharer's own delete keeps its version. With
    /// its time or without: the wire's `is_unshared` asks for the time too (a delete for good
    /// announced by file id alone has neither), but the server names no delete for good to a
    /// member, and a withdrawal taken for a delete would never come back, so here the version
    /// alone decides.
    pub fn withdrawn(&self, at: RemoteRef) -> Result<bool> {
        Ok(at.version.0.is_nil() && self.shared()?)
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
    pub fn delete_account_kv(&self, key: AccountKey) -> Result<()> {
        self.change("DELETE FROM kv WHERE key = ?1", key.0)?;
        Ok(())
    }

    pub fn set_account_kv(&self, key: AccountKey, value: &str) -> Result<()> {
        self.prep("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")?
            .execute([key.0, value])?;
        Ok(())
    }

    /// Says whose drive this DB is of ([`ReadTx::shared`]): kept in the DB, as every
    /// connection reading its mirror goes by it. Set before the drive is first read, by
    /// whoever knows the remote, and refused once the DB read a drive of the other kind (it
    /// knows when its drive was made or how far it read the feed, or the picture holds a
    /// row): read by the other's rules, a withdrawal would be a delete for good, and an
    /// orphan an item at the top.
    pub fn set_shared(&self, shared: bool) -> Result<()> {
        use crate::keys::{DRIVE_MADE, READ_AT, REMOTE_CURSOR};
        if self.shared()? == shared {
            return Ok(());
        }
        let mut read = self
            .count("SELECT EXISTS (SELECT 1 FROM remote) OR EXISTS (SELECT 1 FROM removed)")?
            > 0;
        for key in [DRIVE_MADE, REMOTE_CURSOR, READ_AT] {
            read = read || self.account_kv(key)?.is_some();
        }
        if read {
            return Err(StateError::Conflict(format!(
                "this DB holds {} drive already",
                if shared {
                    "the identity's own"
                } else {
                    "another identity's"
                }
            )));
        }
        if shared {
            self.set_account_kv(crate::keys::SHARED, "1")?;
        } else {
            self.delete_account_kv(crate::keys::SHARED)?;
        }
        self.0.shared.set(Some(shared));
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
