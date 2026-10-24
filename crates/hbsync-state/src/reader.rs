//! A connection that only reads.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::{BUSY_TIMEOUT, NO_FOLDER, ReadTx, Result, StateError, migrations};

/// A reader's page cache: 256 KiB (SQLite's negative `cache_size`).
const READER_CACHE_KIB: i64 = -256;

/// A connection to a state DB that only reads, for what is answered or checked while others
/// write it (the daemon's engines, another process): it makes no DB, writes no row, brings no
/// schema up to date ([`StateError::Behind`]: a writer does) and never copies the log into the
/// DB file (which would put commits on the disk out of order, the crate's docs on
/// durability).
#[derive(Debug)]
pub struct Reader {
    conn: Connection,
}

impl Reader {
    /// The DB at `path`; `None` if there is none (or nothing in it yet).
    pub fn open(path: &Path) -> Result<Option<Reader>> {
        if !path.exists() {
            return Ok(None);
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // It couldn't (its file is read-only here), but it doesn't try either.
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
        // What it reads is answered and let go: a small cache, not the default 2 MB a listing
        // of Recently deleted would fill and keep.
        conn.pragma_update(None, "cache_size", READER_CACHE_KIB)?;
        // A sort larger than that cache (a listing of Recently deleted: every row, ordered by
        // time) stays in memory: on disk it wrote ~3 MB of temporary files per listing of 10k
        // rows. The writer does the same.
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        match migrations::user_version(&conn)? {
            0 => Ok(None),
            migrations::VERSION => Ok(Some(Reader { conn })),
            found => {
                migrations::check(found)?;
                Err(StateError::Behind {
                    found,
                    supported: migrations::VERSION,
                })
            }
        }
    }

    /// Runs `f` in a read transaction (a consistent snapshot), seeing no folder.
    pub fn read<T>(&mut self, f: impl FnOnce(&ReadTx<'_>) -> Result<T>) -> Result<T> {
        self.read_folder(NO_FOLDER, f)
    }

    /// Runs `f` in a read transaction seeing folder `folder`.
    pub fn read_folder<T>(
        &mut self,
        folder: i64,
        f: impl FnOnce(&ReadTx<'_>) -> Result<T>,
    ) -> Result<T> {
        crate::read_in(&mut self.conn, folder, None, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StateDb, keys};

    /// What a reader reads is answered and let go: its page cache is small. What it sorts stays
    /// in memory, as the writer's does: no temporary file.
    #[test]
    fn a_reader_keeps_a_small_cache() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        StateDb::open_account(&path)?
            .write(|s| s.account().set_account_kv(keys::IDENTITY, "a.example"))?;
        let reader = Reader::open(&path)?.ok_or(StateError::Corrupt("no DB".into()))?;
        let size: i64 = reader
            .conn
            .pragma_query_value(None, "cache_size", |r| r.get(0))?;
        assert_eq!(size, READER_CACHE_KIB);
        // 2: MEMORY.
        let temp: i64 = reader
            .conn
            .pragma_query_value(None, "temp_store", |r| r.get(0))?;
        let db = StateDb::open_account(&path)?;
        let writer: i64 = db
            .conn
            .pragma_query_value(None, "temp_store", |r| r.get(0))?;
        assert_eq!((temp, writer), (2, 2));
        Ok(())
    }

    #[test]
    fn a_reader_reads_what_was_written_and_writes_nothing() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        assert!(Reader::open(&path)?.is_none());
        assert!(!path.exists(), "no DB is made");

        let mut db = StateDb::open_account(&path)?;
        db.write(|s| s.account().set_account_kv(keys::IDENTITY, "a.example"))?;
        let mut reader = Reader::open(&path)?.ok_or(StateError::Corrupt("no DB".into()))?;
        assert_eq!(
            reader.read(|s| s.account_kv(keys::IDENTITY))?.as_deref(),
            Some("a.example")
        );
        // A later commit of the writer is seen by the next read.
        db.write(|s| s.account().set_account_kv(keys::IDENTITY, "b.example"))?;
        assert_eq!(
            reader.read(|s| s.account_kv(keys::IDENTITY))?.as_deref(),
            Some("b.example")
        );
        // A reader can't write: what it hands out is a `ReadTx`, which has no public write
        // methods, and its connection is read-only.
        assert!(
            reader
                .conn
                .execute("INSERT INTO kv (key, value) VALUES ('x', 'y')", [])
                .is_err(),
            "a reader can't write"
        );

        // An older schema isn't brought up to date.
        let older = migrations::VERSION - 1;
        db.write(|_| Ok(()))?;
        rusqlite::Connection::open(&path)?.pragma_update(None, "user_version", older)?;
        assert!(matches!(
            Reader::open(&path),
            Err(StateError::Behind { found, .. }) if found == older
        ));
        let version =
            rusqlite::Connection::open(&path)?
                .pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))?;
        assert_eq!(version, older);
        rusqlite::Connection::open(&path)?.pragma_update(
            None,
            "user_version",
            migrations::VERSION,
        )?;

        // The last connection to close doesn't copy the log into the DB file.
        drop(db);
        drop(reader);
        let log = dir.path().join("state.db-wal");
        assert!(std::fs::metadata(&log).is_ok_and(|m| m.len() > 0));
        Ok(())
    }
}
