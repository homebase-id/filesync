//! A connection that only reads.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::{BUSY_TIMEOUT, NO_FOLDER, Result, StateDb, Store, migrations};

/// A connection to a state DB that only reads, for what is answered or checked while others
/// write it (the daemon's engines, another process): it makes no DB, writes no row and never
/// copies the log into the DB file (which would put commits on the disk out of order, the
/// crate's docs on durability). A DB of an older schema is brought up to date first, as by
/// any connection.
#[derive(Debug)]
pub struct Reader {
    conn: Connection,
}

impl Reader {
    /// The DB at `path`; `None` if there is none.
    pub fn open(path: &Path) -> Result<Option<Reader>> {
        if !path.exists() {
            return Ok(None);
        }
        let open = || -> Result<Connection> {
            let conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            conn.busy_timeout(BUSY_TIMEOUT)?;
            Ok(conn)
        };
        let mut conn = open()?;
        if migrations::user_version(&conn)? != migrations::VERSION {
            drop(conn);
            drop(StateDb::open_account(path)?);
            conn = open()?;
        }
        Ok(Some(Reader { conn }))
    }

    /// Runs `f` in a read transaction (a consistent snapshot), seeing no folder.
    pub fn read<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.read_folder(NO_FOLDER, f)
    }

    /// Runs `f` in a read transaction seeing folder `folder`.
    pub fn read_folder<T>(
        &mut self,
        folder: i64,
        f: impl FnOnce(&Store<'_>) -> Result<T>,
    ) -> Result<T> {
        crate::read_in(&mut self.conn, folder, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StateError, keys};

    #[test]
    fn a_reader_reads_what_was_written_and_writes_nothing() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        assert!(Reader::open(&path)?.is_none());
        assert!(!path.exists(), "no DB is made");

        let mut db = StateDb::open_account(&path)?;
        db.write(|s| s.set_account_kv(keys::IDENTITY, "a.example"))?;
        let mut reader = Reader::open(&path)?.ok_or(StateError::Corrupt("no DB".into()))?;
        assert_eq!(
            reader.read(|s| s.account_kv(keys::IDENTITY))?.as_deref(),
            Some("a.example")
        );
        // A later commit of the writer is seen by the next read.
        db.write(|s| s.set_account_kv(keys::IDENTITY, "b.example"))?;
        assert_eq!(
            reader.read(|s| s.account_kv(keys::IDENTITY))?.as_deref(),
            Some("b.example")
        );
        assert!(
            reader
                .read(|s| s.set_account_kv(keys::IDENTITY, "c.example"))
                .is_err(),
            "a reader can't write"
        );

        // The last connection to close doesn't copy the log into the DB file.
        drop(db);
        drop(reader);
        let log = dir.path().join("state.db-wal");
        assert!(std::fs::metadata(&log).is_ok_and(|m| m.len() > 0));
        Ok(())
    }
}
