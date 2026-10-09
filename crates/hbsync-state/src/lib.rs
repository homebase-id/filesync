//! The state DB: everything the client knows between runs, in one SQLite file.
//!
//! - **nodes**: one small integer id per node; every other table is keyed by it.
//! - **base**: the last state both sides agreed on, per node (the three-way merge's ancestor).
//! - **local**: what the last observation saw on disk, as a tree of (parent, name) rows.
//! - **remote**: the client's copy of the live remote nodes.
//! - **dirty**: nodes whose local or remote side changed and still need reconciling. Written
//!   in the same transaction as the change, so a crash never loses one.
//! - **parked**, **forks**: write-ahead records of the executor's temporary renames.
//! - **kv**: cursors and small settings.
//!
//! Every access runs inside a transaction ([`StateDb::read`], [`StateDb::write`]); a write
//! closure that returns an error rolls everything back.

mod codec;
mod migrations;
mod store;

use std::path::Path;

use rusqlite::{Connection, TransactionBehavior};

pub use store::{BaseRow, DirtyCursor, LocalRow, MirrorFile, MirrorRow, Store};

/// The state DB key of the remote change-feed cursor.
pub const REMOTE_CURSOR_KEY: &str = "remote.cursor";

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("state DB is corrupt: {0}")]
    Corrupt(String),
    #[error("state DB schema version {found} is newer than this client ({supported})")]
    TooNew { found: u32, supported: u32 },
    #[error("conflict: {0}")]
    Conflict(String),
}

pub type Result<T, E = StateError> = std::result::Result<T, E>;

/// An open state DB.
#[derive(Debug)]
pub struct StateDb {
    conn: Connection,
}

impl StateDb {
    /// Opens (or creates) the DB at `path` and brings its schema up to date.
    pub fn open(path: &Path) -> Result<Self> {
        Self::setup(Connection::open(path)?)
    }

    /// An in-memory DB, for tests.
    pub fn open_in_memory() -> Result<Self> {
        Self::setup(Connection::open_in_memory()?)
    }

    fn setup(mut conn: Connection) -> Result<Self> {
        // Start-up runs the local catch-up and the feed on two connections; a long scan
        // transaction must not make the other writer give up.
        conn.busy_timeout(std::time::Duration::from_secs(120))?;
        // WAL keeps readers off the writer's back. NORMAL syncs the WAL at checkpoints, not at
        // every commit: a commit survives an app crash at once, and a power loss can only undo
        // the last few, never corrupt the file. That is safe here because every step after a
        // disk or remote change is redone by the next scan and plan; the records that must be
        // on disk before the change they describe use `write_durable`.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Enough for every statement the store prepares, so hot lookups are never re-parsed.
        conn.set_prepared_statement_cache_capacity(64);
        migrations::apply(&mut conn)?;
        Ok(StateDb { conn })
    }

    /// Runs `f` in a read transaction (a consistent snapshot).
    pub fn read<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let out = f(&Store::new(&tx))?;
        tx.finish()?;
        Ok(out)
    }

    /// Runs `f` in a write transaction, committed only if `f` succeeds.
    pub fn write<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let out = f(&Store::new(&tx))?;
        tx.commit()?;
        Ok(out)
    }

    /// [`StateDb::write`], with the commit on disk before this returns, even across a power
    /// loss. For write-ahead records: ones that must exist before the disk change they
    /// describe (forks, parked items).
    pub fn write_durable<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.conn.pragma_update(None, "synchronous", "FULL")?;
        let out = self.write(f);
        self.conn.pragma_update(None, "synchronous", "NORMAL")?;
        out
    }

    /// The schema version in use.
    pub fn schema_version(&self) -> Result<u32> {
        Ok(self
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))?)
    }
}
