//! The state DB: everything the client knows between runs, in one SQLite file.
//!
//! - **base**: the last state both sides agreed on, per node (the three-way merge's ancestor).
//! - **local_items**: what the last observation saw on disk, keyed by local path and sorted
//!   with the `hbpath` collation, so a sorted walk can be merged against it in one pass.
//! - **dirty**: nodes whose local or remote side changed and still need reconciling. Written
//!   in the same transaction as the change, so a crash never loses one.
//! - **kv**: cursors and small settings.
//!
//! Every access runs inside a transaction ([`StateDb::read`], [`StateDb::write`]); a write
//! closure that returns an error rolls everything back.

mod codec;
mod migrations;
mod store;

use std::path::Path;

use rusqlite::{Connection, TransactionBehavior};

pub use store::{BaseRow, LocalRow, RemoteRef, Store};

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

    /// An in-memory DB, for tests and the simulator.
    pub fn open_in_memory() -> Result<Self> {
        Self::setup(Connection::open_in_memory()?)
    }

    fn setup(mut conn: Connection) -> Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        // WAL keeps readers off the writer's back; FULL fsyncs each commit so a committed base
        // update survives power loss.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.create_collation("hbpath", hbsync_platform::path_cmp)?;
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

    /// The schema version in use.
    pub fn schema_version(&self) -> Result<u32> {
        Ok(self
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))?)
    }
}
