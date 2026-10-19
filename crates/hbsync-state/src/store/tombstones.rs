//! What the feed said is gone: the tombstones of files deleted for good, deleted hard once they
//! are old enough, and how long a file is known gone; and reading the whole feed again after a
//! long absence.

use super::*;

impl Store<'_> {
    // ---- tombstones ----

    /// Records a tombstone the feed brought, written at `at` (Unix ms, the server's): its file
    /// is gone ([`Store::mark_file_gone`]), and the tombstone is still on the drive, at that
    /// version, until deleted hard.
    pub fn put_tombstone(&self, tombstone: RemoteRef, at: i64) -> Result<()> {
        self.prep(
            "INSERT INTO gone_files (file_id, version_tag, at) VALUES (?1, ?2, ?3)
             ON CONFLICT (file_id) DO UPDATE SET version_tag = excluded.version_tag,
                 at = excluded.at",
        )?
        .execute(params![
            tombstone.file_id.0.as_bytes(),
            tombstone.version.0.as_bytes(),
            at
        ])?;
        Ok(())
    }

    /// Records the tombstone a delete of this device left, written at `at` (Unix ms, the
    /// server's: the delete's answer): its file is gone ([`Store::mark_file_gone`]), and the
    /// tombstone is on the drive from then on. Its version, which a hard delete of it needs,
    /// comes with the feed's row ([`Store::put_tombstone`]); a row the feed brought already
    /// stays as it is.
    pub fn put_own_tombstone(&self, file_id: RemoteFileId, at: i64) -> Result<()> {
        self.prep(
            "INSERT INTO gone_files (file_id, at) VALUES (?1, ?2)
             ON CONFLICT (file_id) DO UPDATE SET at = excluded.at WHERE version_tag IS NULL",
        )?
        .execute(params![file_id.0.as_bytes(), at])?;
        Ok(())
    }

    /// The tombstones still on the drive written before `at_ms` (Unix ms), oldest first: those
    /// whose version is known ([`Store::put_own_tombstone`]).
    pub fn tombstones_before(&self, at_ms: i64) -> Result<Vec<RemoteRef>> {
        self.many(
            "SELECT file_id, version_tag FROM gone_files
             WHERE at IS NOT NULL AND at < ?1 AND version_tag IS NOT NULL ORDER BY at",
            [at_ms],
            |r| {
                Ok(RemoteRef {
                    file_id: RemoteFileId(codec::uuid_from(&r.get::<_, Vec<u8>>(0)?)?),
                    version: VersionTag(codec::uuid_from(&r.get::<_, Vec<u8>>(1)?)?),
                })
            },
        )
    }

    /// The tombstone at `at` (its version) was deleted hard, by this device or another: its file
    /// stays gone. A delete of another version changes nothing.
    pub fn delete_tombstone_at(&self, at: RemoteRef) -> Result<bool> {
        Ok(self
            .prep(
                "UPDATE gone_files SET version_tag = NULL, at = NULL
                 WHERE file_id = ?1 AND version_tag = ?2",
            )?
            .execute(params![at.file_id.0.as_bytes(), at.version.0.as_bytes()])?
            > 0)
    }

    /// Forgets the files gone since before `before_ms` (Unix ms, the server's) that left
    /// nothing on the drive this device can delete: no page read since can show them any more
    /// (node schema, Recently deleted). Those whose time is not known yet are gone from `now_ms`
    /// on; a tombstone of this device's whose version the feed never brought
    /// ([`Store::put_own_tombstone`]), from its own time. Returns how many went.
    pub fn prune_gone(&self, before_ms: i64, now_ms: i64) -> Result<usize> {
        self.prep("UPDATE gone_files SET gone_at = ?1 WHERE gone_at IS NULL AND at IS NULL")?
            .execute([now_ms])?;
        Ok(self
            .prep(
                "DELETE FROM gone_files
                 WHERE version_tag IS NULL AND COALESCE(at, gone_at) < ?1",
            )?
            .execute([before_ms])?)
    }

    // ---- reading the whole feed again ----

    /// Starts reading the whole feed again, from its beginning: the cursor goes, and
    /// [`keys::REREADING`] says so until [`Store::end_reread`] (a re-read a crash cut short
    /// starts over).
    pub fn start_reread(&self) -> Result<()> {
        self.delete_account_kv(crate::keys::REMOTE_CURSOR)?;
        self.set_account_kv(crate::keys::REREADING, "1")
    }

    /// Whether the whole feed is being read again.
    pub fn rereading(&self) -> Result<bool> {
        Ok(self.account_kv(crate::keys::REREADING)?.is_some())
    }

    pub fn end_reread(&self) -> Result<()> {
        self.delete_account_kv(crate::keys::REREADING)
    }

    /// Every file of the drive the store has, in the mirror or Recently deleted.
    pub fn drive_file_ids(&self) -> Result<Vec<RemoteFileId>> {
        self.many(
            "SELECT file_id FROM remote UNION SELECT file_id FROM removed",
            [],
            |r| Ok(RemoteFileId(codec::uuid_from(&r.get::<_, Vec<u8>>(0)?)?)),
        )
    }
}
