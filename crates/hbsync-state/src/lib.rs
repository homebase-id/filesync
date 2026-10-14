//! The state DB: everything the client knows between runs about one account, in one SQLite
//! file, for every folder it syncs on this device.
//!
//! - **nodes**: one small integer id per node; every other table is keyed by it.
//! - **folders**: the folders synced here, each holding a part of the drive.
//! - **remote**: the client's copy of the drive's live nodes; **removed**: Recently deleted.
//! - Per folder:
//!   - **base**: the last state both sides agreed on, per node (the three-way merge's ancestor).
//!   - **local**: what the last observation saw on disk, as a tree of (parent, name) rows.
//!   - **dirty**: nodes whose local or remote side changed and still need reconciling. Written
//!     in the same transaction as the change, so a crash never loses one.
//!   - **parked**, **forks**: write-ahead records of the executor's temporary renames.
//!   - **folder_kv**: the folder's cursors and settings; **kv** has the account's.
//!
//! Every access runs inside a transaction ([`StateDb::read`], [`StateDb::write`]); a write
//! closure that returns an error rolls everything back.

mod codec;
mod coords;
mod migrations;
mod store;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, TransactionBehavior};

pub use coords::Coords;
pub use store::{
    BaseRow, DirtyCursor, FolderRow, LocalRow, MAX_DEPTH, MirrorFile, MirrorRow, RemovedRow, Store,
};

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
    #[error(
        "state DB schema version {found} is from an older Filesync (one DB per folder) and isn't upgraded: set the folder up again"
    )]
    TooOld { found: u32 },
    #[error("conflict: {0}")]
    Conflict(String),
}

pub type Result<T, E = StateError> = std::result::Result<T, E>;

/// How long a connection waits for another one's write.
const BUSY_TIMEOUT: Duration = Duration::from_secs(120);

/// Puts the DB in WAL mode. Switching a new DB needs it to itself, and SQLite answers busy at
/// once instead of waiting: tried again while another connection is creating it.
fn wal(conn: &Connection) -> Result<()> {
    let until = Instant::now() + BUSY_TIMEOUT;
    loop {
        match conn.pragma_update(None, "journal_mode", "WAL") {
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::DatabaseBusy && Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            done => return Ok(done?),
        }
    }
}

/// An open state DB, seen as one of its folders.
#[derive(Debug)]
pub struct StateDb {
    conn: Connection,
    folder: i64,
    /// `None` in memory.
    path: Option<PathBuf>,
    /// Shared by every connection of this process to the same file: writes take turns here
    /// instead of in SQLite's busy wait (which stays for other processes).
    gate: Arc<Mutex<()>>,
}

/// The write gate of the DB file at `path`, one per file in this process.
fn gate(path: Option<&Path>) -> Arc<Mutex<()>> {
    static GATES: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
        LazyLock::new(Default::default);
    let Some(path) = path else {
        return Arc::default();
    };
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut gates = GATES.lock().unwrap_or_else(|e| e.into_inner());
    Arc::clone(gates.entry(key).or_default())
}

/// The folder a DB opened with [`StateDb::open`] sees: the first one, made on first use for the
/// drive's root.
pub const FIRST_FOLDER: i64 = 1;
/// What [`StateDb::open_account`] sees: no folder.
const NO_FOLDER: i64 = 0;

impl StateDb {
    /// Opens (or creates) the DB at `path`, brings its schema up to date, and sees its first
    /// folder (made for the drive's root if there is none).
    pub fn open(path: &Path) -> Result<Self> {
        Self::first(Self::setup(Connection::open(path)?, Some(path))?)
    }

    /// The DB at `path` as the account's: no folder made, none seen (a folder's rows can't be
    /// written through it). [`StateDb::with_folder`] picks one.
    pub fn open_account(path: &Path) -> Result<Self> {
        let mut db = Self::setup(Connection::open(path)?, Some(path))?;
        db.folder = NO_FOLDER;
        Ok(db)
    }

    /// The DB at `path` seen as folder `folder` (one of its `folders`).
    pub fn open_folder(path: &Path, folder: i64) -> Result<Self> {
        let mut db = Self::setup(Connection::open(path)?, Some(path))?;
        db.folder = folder;
        if !db.read(|s| Ok(s.folders()?.iter().any(|f| f.id == folder)))? {
            return Err(StateError::Conflict(format!(
                "no folder {folder} in this DB"
            )));
        }
        Ok(db)
    }

    /// An in-memory DB, for tests: its first folder holds the drive's root.
    pub fn open_in_memory() -> Result<Self> {
        Self::first(Self::setup(Connection::open_in_memory()?, None)?)
    }

    fn first(mut db: StateDb) -> Result<Self> {
        db.write(|s| {
            if s.folders()?.is_empty() {
                s.add_folder(hbsync_types::NodeId::ROOT, "", "")?;
            }
            Ok(())
        })?;
        Ok(db)
    }

    /// The folder this handle sees.
    pub fn folder(&self) -> i64 {
        self.folder
    }

    /// The DB's file (`None` in memory).
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// A second connection to the same DB, seeing the same folder (for work on another thread).
    pub fn another(&self) -> Result<Self> {
        let path = self.path.as_deref().ok_or_else(|| {
            StateError::Conflict("an in-memory DB has no second connection".into())
        })?;
        Self::open_folder(path, self.folder)
    }

    /// This DB seen as folder `folder`: the same connection.
    pub fn with_folder(mut self, folder: i64) -> Self {
        self.folder = folder;
        self
    }

    fn setup(mut conn: Connection, path: Option<&Path>) -> Result<Self> {
        // Start-up runs the local catch-up and the feed on two connections; a long scan
        // transaction must not make the other writer give up.
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // WAL keeps readers off the writer's back; FULL syncs each commit, so after a power loss
        // the DB is never more than the one step in flight behind the disk. The executor's
        // crash handling (write-ahead records, the scan's identity rules) relies on that, so
        // speed comes from fewer commits (an operation and its settling are one), not from
        // weaker ones.
        wal(&conn)?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Enough for every statement the store prepares, so hot lookups are never re-parsed.
        conn.set_prepared_statement_cache_capacity(64);
        migrations::apply(&mut conn)?;
        Ok(StateDb {
            conn,
            folder: FIRST_FOLDER,
            path: path.map(Path::to_path_buf),
            gate: gate(path),
        })
    }

    /// Runs `f` in a read transaction (a consistent snapshot).
    pub fn read<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let out = f(&Store::new(&tx, self.folder))?;
        tx.finish()?;
        Ok(out)
    }

    /// Runs `f` in a write transaction, committed only if `f` succeeds.
    pub fn write<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let _turn = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let out = f(&Store::new(&tx, self.folder))?;
        tx.commit()?;
        Ok(out)
    }

    /// The schema version in use.
    pub fn schema_version(&self) -> Result<u32> {
        migrations::user_version(&self.conn)
    }
}
