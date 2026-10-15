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
//!   - **parked**, **forks**, **placing**: write-ahead records of the executor's temporary
//!     renames, conflict copies and downloads.
//!   - **folder_kv**: the folder's cursors and settings; **kv** has the account's.
//!
//! Every access runs inside a transaction ([`StateDb::read`], [`StateDb::write`]); a write
//! closure that returns an error rolls everything back.
//!
//! **Durability.** A commit reaches the disk soon, not at once: an OS crash or a power cut can
//! lose the last ones, in order, never one in the middle. A durable commit
//! ([`StateDb::write_durable`], [`StateDb::flush`]) is on the disk when it returns, with every
//! commit before it. What the client does on disk is ordered against that here, for every
//! connection of the process to the DB file:
//! - the folders whose entries changed ([`StateDb::changed`], [`StateDb::emptied`]) are synced
//!   before any commit is made durable, so the DB never durably holds a change the disk lost;
//! - a place emptied since the last durable commit is made durable before something else is
//!   put there ([`StateDb::filling`]), so a power cut can't leave the DB holding the old item
//!   where the new one is;
//! - SQLite checkpoints (which put every commit on the disk) only right after a durable commit.
//!
//! Losing the commits since the last durable one is then like a crash just after it, with what
//! was done on disk since still there: the executor's write-ahead records and the scan's rules
//! (sync semantics §8a) take it from there.

mod codec;
mod coords;
pub mod keys;
mod migrations;
mod store;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, TransactionBehavior};

pub use coords::Coords;
pub use store::{
    BaseRow, DirtyCursor, FolderRow, LocalRow, MAX_DEPTH, MirrorFile, MirrorRow, Placing,
    RemovedRow, Store,
};

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
    gate: Arc<Mutex<Gate>>,
    /// Places on disk emptied through this connection since its last durable commit, by
    /// [`place_key`]. Its own: another connection's durable commit may come before this one's
    /// commit that records the emptying.
    emptied: HashSet<String>,
    /// The folders writes through this connection marked a node dirty in
    /// ([`StateDb::take_touched`]).
    touched: std::collections::BTreeSet<i64>,
    #[cfg(feature = "testkit")]
    hook: Option<testkit::Hook>,
}

/// The DB file at `path` as one key, whatever the spelling.
fn file_key(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// What a DB file's next durable commit waits for (see the crate's docs on durability).
#[derive(Debug, Default)]
struct Gate {
    /// A commit since the last durable one may not be on the disk yet.
    unsynced: bool,
    /// Commits since the last checkpoint.
    commits: u32,
    /// Folders whose entries changed since the last durable commit.
    dirs: HashSet<PathBuf>,
}

impl Gate {
    fn sync_dirs(&mut self) {
        if !self.dirs.is_empty() {
            hbsync_platform::ops::sync_dirs(self.dirs.drain());
        }
    }
}

/// A place on disk as [`StateDb::emptied`] knows it (any case).
fn place_key(path: &Path) -> String {
    hbsync_platform::name::name_key(&path.to_string_lossy())
}

/// The folder holding `path`.
fn folder_of(path: &Path) -> PathBuf {
    path.parent().unwrap_or(path).to_path_buf()
}

/// Copies the WAL into the DB file, which puts every commit in it on the disk: only while none
/// is waiting for a durable commit (`gate` is held, and not unsynced).
fn checkpoint(conn: &Connection, gate: &mut Gate) {
    // Best effort: a reader in the way leaves the rest for the next one.
    if conn
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
        .is_ok()
    {
        gate.commits = 0;
    }
}

/// Commits after which a durable one checkpoints (SQLite's own default is 1000 pages).
const CHECKPOINT_AFTER: u32 = 1000;

/// The write gate of the DB file `key` ([`file_key`]), one per file in this process.
fn gate(key: Option<&Path>) -> Arc<Mutex<Gate>> {
    static GATES: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<Gate>>>>> =
        LazyLock::new(Default::default);
    let Some(key) = key else {
        return Arc::default();
    };
    let mut gates = GATES.lock().unwrap_or_else(|e| e.into_inner());
    Arc::clone(gates.entry(key.to_path_buf()).or_default())
}

impl Drop for StateDb {
    fn drop(&mut self) {
        // The last connection to the file checkpoints as it closes: the folders changed since
        // the last durable commit go first. A gate in use has another connection open, so this
        // one isn't the last.
        let gate = match self.gate.try_lock() {
            Ok(gate) => Some(gate),
            Err(std::sync::TryLockError::Poisoned(e)) => Some(e.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(mut gate) = gate {
            gate.sync_dirs();
        }
    }
}

/// Watching commits, for tests that crash a client at one (feature `testkit`).
#[cfg(feature = "testkit")]
pub mod testkit {
    use super::*;

    /// Called around each commit to a DB file, in its turn (no other connection of this process
    /// writes meanwhile).
    #[derive(Clone)]
    pub struct Hook(pub Arc<dyn Fn(Commit) + Send + Sync>);

    /// When a [`Hook`] is called.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Commit {
        Before,
        After,
        /// After a durable one ([`StateDb::write_durable`]).
        Durable,
    }

    impl std::fmt::Debug for Hook {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Hook")
        }
    }

    static HOOKS: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(Default::default);

    /// Connections opened from now on to the DB at `path` call `hook` around each commit (none
    /// with `None`).
    pub fn on_commit(path: &Path, hook: Option<Hook>) {
        let mut hooks = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        match hook {
            Some(hook) => hooks.insert(file_key(path), hook),
            None => hooks.remove(&file_key(path)),
        };
    }

    impl StateDb {
        /// Copies the DB as of its last commit to a new file at `to`.
        pub fn copy_to(&mut self, to: &Path) -> Result<()> {
            self.conn
                .execute("VACUUM INTO ?1", [to.to_string_lossy().as_ref()])?;
            Ok(())
        }

        /// Puts back every row of a copy made with [`StateDb::copy_to`], in one transaction:
        /// the DB as a power cut leaves it, its last commits lost. Every engine on the DB is to
        /// be started again after it.
        pub fn restore_from(&mut self, copy: &Path) -> Result<()> {
            let mut turn = self.gate.lock().unwrap_or_else(|e| e.into_inner());
            // What the lost commits did on disk is still there; nothing waits any more.
            *turn = Gate::default();
            self.emptied.clear();
            // No cascades while tables are emptied one by one.
            self.conn.pragma_update(None, "foreign_keys", false)?;
            self.conn
                .execute("ATTACH ?1 AS copy", [copy.to_string_lossy().as_ref()])?;
            let restored = (|| -> Result<()> {
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                let tables: Vec<String> = tx
                    .prepare(
                        "SELECT name FROM main.sqlite_master WHERE type = 'table' \
                         AND (name NOT LIKE 'sqlite_%' OR name = 'sqlite_sequence')",
                    )?
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                // `gone` last: the triggers of the others write to it.
                let (gone, others): (Vec<String>, Vec<String>) =
                    tables.into_iter().partition(|t| t == "gone");
                for t in others.into_iter().chain(gone) {
                    tx.execute_batch(&format!(
                        "DELETE FROM main.\"{t}\"; \
                         INSERT INTO main.\"{t}\" SELECT * FROM copy.\"{t}\";"
                    ))?;
                }
                Ok(tx.commit()?)
            })();
            self.conn.execute("DETACH copy", [])?;
            self.conn.pragma_update(None, "foreign_keys", true)?;
            restored
        }
    }

    /// The hook of the DB file `key` ([`file_key`]).
    pub(crate) fn hook(key: Option<&Path>) -> Option<Hook> {
        let hooks = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        hooks.get(key?).cloned()
    }
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

    /// From now on this connection sees folder `folder`.
    pub fn see_folder(&mut self, folder: i64) {
        self.folder = folder;
    }

    /// From now on this connection sees no folder: the account's ([`StateDb::open_account`]).
    pub fn see_account(&mut self) {
        self.folder = NO_FOLDER;
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
        // WAL keeps readers off the writer's back. NORMAL syncs the WAL only at checkpoints,
        // which run only after durable commits (the crate's docs on durability). A commit costs
        // ~0.03 ms this way, 2 ms (NVMe) to 90 ms (a spinning disk) with each one synced.
        wal(&conn)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        // A sync on macOS stops at the drive's cache unless it is a full one.
        #[cfg(target_os = "macos")]
        conn.pragma_update(None, "fullfsync", true)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Enough for every statement the store prepares, so hot lookups are never re-parsed.
        conn.set_prepared_statement_cache_capacity(128);
        migrations::apply(&mut conn)?;
        let key = path.map(file_key);
        Ok(StateDb {
            conn,
            folder: FIRST_FOLDER,
            path: path.map(Path::to_path_buf),
            gate: gate(key.as_deref()),
            emptied: HashSet::new(),
            touched: Default::default(),
            #[cfg(feature = "testkit")]
            hook: testkit::hook(key.as_deref()),
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

    /// Runs `f` in a write transaction, committed only if `f` succeeds. The commit reaches the
    /// disk soon, not at once: an OS crash or a power cut can lose it (the crate's docs on
    /// durability).
    pub fn write<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.commit(false, f)
    }

    /// [`StateDb::write`], on the disk when it returns, with every commit before it.
    pub fn write_durable<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.commit(true, f)
    }

    /// Puts every commit so far on the disk (nothing to do when they are), and into the DB
    /// file.
    pub fn flush(&mut self) -> Result<()> {
        if self.turn().unsynced {
            // A commit syncs only when it writes something.
            self.write_durable(|s| s.count_flush())?;
        }
        let mut gate = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        if !gate.unsynced {
            self.emptied.clear();
            if gate.commits > 0 {
                checkpoint(&self.conn, &mut gate);
            }
        }
        Ok(())
    }

    /// The item at `path` on disk just left it (moved away or trashed); the commit recording
    /// that is to come through this connection. See [`StateDb::filling`].
    pub fn emptied(&mut self, path: &Path) {
        self.emptied.insert(place_key(path));
        self.changed(path);
    }

    /// An entry at `path` on disk was just made, replaced or moved there: its folder is synced
    /// before any commit is made durable.
    pub fn changed(&self, path: &Path) {
        self.turn().dirs.insert(folder_of(path));
    }

    /// Before something is put at `path` on disk: if that place was emptied through this
    /// connection since its last durable commit, every commit is made durable first. Call
    /// [`StateDb::changed`] once it is there.
    pub fn filling(&mut self, path: &Path) -> Result<()> {
        if self.emptied.contains(&place_key(path)) {
            self.flush()?;
        }
        Ok(())
    }

    fn turn(&self) -> std::sync::MutexGuard<'_, Gate> {
        self.gate.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn commit<T>(&mut self, durable: bool, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let mut gate = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        if durable {
            gate.sync_dirs();
            self.conn.pragma_update(None, "synchronous", "FULL")?;
        }
        // A commit that writes nothing syncs nothing.
        let changes = self.conn.total_changes();
        let committed = (|| -> Result<_> {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let store = Store::new(&tx, self.folder);
            let out = f(&store)?;
            let touched = store.take_touched();
            #[cfg(feature = "testkit")]
            if let Some(hook) = &self.hook {
                (hook.0)(testkit::Commit::Before);
            }
            tx.commit()?;
            Ok((out, touched))
        })();
        if durable {
            // Left on FULL if this fails: slower, never wrong.
            let _ = self.conn.pragma_update(None, "synchronous", "NORMAL");
        }
        let (out, touched) = committed?;
        if self.conn.total_changes() != changes {
            gate.unsynced = !durable;
            gate.commits += 1;
        }
        if !gate.unsynced {
            self.emptied.clear();
            if gate.commits >= CHECKPOINT_AFTER {
                checkpoint(&self.conn, &mut gate);
            }
        }
        #[cfg(feature = "testkit")]
        if let Some(hook) = &self.hook {
            (hook.0)(if durable {
                testkit::Commit::Durable
            } else {
                testkit::Commit::After
            });
        }
        self.touched.extend(touched);
        Ok(out)
    }

    /// The folders whose dirty sets the writes since the last call marked nodes in, from
    /// changes of the drive ([`Store::fan_out`]): the engines to wake.
    pub fn take_touched(&mut self) -> std::collections::BTreeSet<i64> {
        std::mem::take(&mut self.touched)
    }

    /// The schema version in use.
    pub fn schema_version(&self) -> Result<u32> {
        migrations::user_version(&self.conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flushes(db: &mut StateDb) -> Result<Option<String>> {
        db.read(|s| s.account_kv(keys::FLUSHES))
    }

    fn write(db: &mut StateDb, value: &str) -> Result<()> {
        db.write(|s| s.set_kv(keys::FILTER, value))
    }

    /// A place emptied since the last durable commit is made durable before something is put
    /// there; one that wasn't, or was made durable since, isn't.
    #[test]
    fn filling_an_emptied_place_makes_the_commits_durable_first() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        let (mut a, mut b) = (StateDb::open(&path)?, StateDb::open(&path)?);
        let (place, other) = (dir.path().join("Place"), dir.path().join("other"));
        a.emptied(&place);
        write(&mut a, "1")?;
        a.filling(&other)?;
        assert_eq!(flushes(&mut a)?, None);
        // Any case: the same place on a volume that ignores it.
        a.filling(&dir.path().join("place"))?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("1"));
        a.filling(&place)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("1"));

        // Another connection's durable commit can't vouch for a commit still to come here.
        a.emptied(&place);
        b.write_durable(|s| s.set_kv(keys::SCOPE, "2"))?;
        write(&mut a, "3")?;
        a.filling(&place)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("2"));
        Ok(())
    }

    /// Commits reach the DB file only by a checkpoint right after a durable one: SQLite's own
    /// would put commits on the disk before the folders changed are.
    #[test]
    fn only_a_flush_or_many_commits_with_a_durable_one_checkpoint() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        let mut db = StateDb::open(&path)?;
        db.flush()?;
        let len = || std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let before = len();
        for i in 0..2 * CHECKPOINT_AFTER {
            write(&mut db, &format!("{i:0>4000}"))?;
        }
        assert_eq!(len(), before);
        db.flush()?;
        assert!(len() > before);
        let before = len();
        for i in 0..CHECKPOINT_AFTER {
            db.write(|s| s.set_kv(keys::SCOPE, &format!("{i:0>4000}")))?;
        }
        assert_eq!(len(), before);
        db.write_durable(|s| s.set_kv(keys::SCOPE, "done"))?;
        assert!(len() > before);
        Ok(())
    }
}
