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
//! closure that returns an error rolls everything back. What only reads while others write
//! (answers, checks) uses a [`Reader`], which can't write.
//!
//! **Durability.** A commit reaches the disk soon, not at once: an OS crash or a power cut can
//! lose the last ones, in order, never one in the middle. A durable commit
//! ([`StateDb::write_durable`], [`StateDb::flush`]) is on the disk when it returns, with every
//! commit before it. What the client does on disk is ordered against that here, for every
//! connection of the process to the DB file:
//! - the folders whose entries changed ([`StateDb::changed`], [`StateDb::emptied`]) are synced
//!   before any commit is made durable, so a durable commit never holds a change the disk lost;
//! - a place emptied since the last durable commit is made durable before something else is
//!   put there ([`StateDb::filling`]), so a power cut can't leave the DB holding the old item
//!   where the new one is;
//! - SQLite checkpoints (which put every commit on the disk) only after those folders are synced
//!   too: once the log holds [`CHECKPOINT_PAGES`], not by itself or as a connection closes;
//! - a process starts as if its commits weren't durable: one that died may have left some.
//!
//! What isn't ordered: the system writing the log out on its own, between syncs. A power cut
//! may then keep a commit whose change on disk it lost (rare: file systems mostly write their
//! journal of folder changes first); the scan finds the item where it is, as a local change.
//!
//! Losing the commits since the last durable one is then like a crash just after it, with what
//! was done on disk since still there: the executor's write-ahead records and the scan's rules
//! (sync semantics §8a) take it from there.

mod codec;
mod coords;
pub mod keys;
mod migrations;
mod reader;
mod store;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, TransactionBehavior};

pub use coords::Coords;
pub use reader::Reader;
pub use store::{
    BaseRow, DirtyCursor, FolderRow, LocalRow, MAX_DEPTH, MirrorFile, MirrorRow, Placing,
    PlannedBase, Recorded, RemovedRow, Store,
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
    /// Older than this client's, and a [`Reader`] can't bring it up to date: a writer does
    /// (the daemon of this version as it starts).
    #[error(
        "state DB schema version {found} is older than this client's ({supported}): it is brought up to date when this version's daemon starts (restart a running one)"
    )]
    Behind { found: u32, supported: u32 },
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
    /// Shared by every connection of this process to the same file.
    shared: Arc<Shared>,
    /// Places on disk emptied through this connection (by [`place_key`]) since its last commit
    /// that wrote, and those it committed since its last durable commit. Its own: another
    /// connection's durable commit can't vouch for a commit still to come here.
    emptied: Emptied,
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

/// What every connection of this process to one DB file shares.
#[derive(Debug, Default)]
struct Shared {
    /// Writes take turns here instead of in SQLite's busy wait (which stays for other
    /// processes).
    gate: Mutex<Gate>,
    /// Durable commits and checkpoints take turns here: they sync the folders changed before
    /// they take the gate, so plain writes don't wait for that.
    durable: Mutex<()>,
}

/// What a DB file's next durable commit waits for (see the crate's docs on durability).
#[derive(Debug, Default)]
struct Gate {
    /// A commit since the last durable one may not be on the disk yet.
    unsynced: bool,
    /// Folders whose entries changed since the last durable commit or checkpoint.
    dirs: HashSet<PathBuf>,
    /// The log's size at which the next checkpoint is tried, past [`CHECKPOINT_PAGES`] while a
    /// reader keeps the last one from emptying it.
    checkpoint_at: i32,
}

/// See [`StateDb::emptied`].
#[derive(Debug, Default)]
struct Emptied {
    /// Not recorded by a commit yet.
    fresh: HashSet<String>,
    /// Recorded, not durable yet.
    committed: HashSet<String>,
}

/// What a commit is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Durable,
    /// Durable, writing only to be one: it records no emptied place.
    Flush,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A place on disk as [`StateDb::emptied`] knows it (any case).
fn place_key(path: &Path) -> String {
    hbsync_platform::name::name_key(&path.to_string_lossy())
}

/// The log's size (pages) at which it is copied into the DB file, 8 MB.
pub const CHECKPOINT_PAGES: i32 = 2000;

/// What the log's file is cut back to once it is copied (it keeps its largest size otherwise).
const LOG_LIMIT: i64 = 16 << 20;

thread_local! {
    /// The log's size after the last commit on this thread ([`wal_size`]).
    static WAL_PAGES: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

/// SQLite's log hook, called on the committing thread right after each commit.
fn wal_size(_: &rusqlite::hooks::Wal, pages: std::ffi::c_int) -> rusqlite::Result<()> {
    WAL_PAGES.set(pages);
    Ok(())
}

/// What the connections of this process to the DB file `key` ([`file_key`]) share.
fn shared(key: Option<&Path>) -> Arc<Shared> {
    static SHARED: LazyLock<Mutex<HashMap<PathBuf, Arc<Shared>>>> = LazyLock::new(Default::default);
    let Some(key) = key else {
        return Arc::default();
    };
    Arc::clone(lock(&SHARED).entry(key.to_path_buf()).or_insert_with(|| {
        // A process that used the file before may have died with commits not durable.
        let gate = Gate {
            unsynced: true,
            ..Gate::default()
        };
        Arc::new(Shared {
            gate: Mutex::new(gate),
            durable: Mutex::new(()),
        })
    }))
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
        let mut hooks = lock(&HOOKS);
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
            let mut turn = lock(&self.shared.gate);
            // What the lost commits did on disk is still there; nothing waits any more.
            *turn = Gate::default();
            self.emptied = Emptied::default();
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
        let hooks = lock(&HOOKS);
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
        // which this crate makes, never SQLite (the crate's docs on durability): the log hook
        // takes the place of its own, and closing makes none. A commit costs ~0.03 ms this way,
        // 2 ms (NVMe) to 90 ms (a spinning disk) with each one synced.
        wal(&conn)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.wal_hook(Some(wal_size));
        conn.pragma_update(None, "journal_size_limit", LOG_LIMIT)?;
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
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
            shared: shared(key.as_deref()),
            emptied: Emptied::default(),
            touched: Default::default(),
            #[cfg(feature = "testkit")]
            hook: testkit::hook(key.as_deref()),
        })
    }

    /// Runs `f` in a read transaction (a consistent snapshot).
    pub fn read<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        read_in(&mut self.conn, self.folder, f)
    }

    /// Runs `f` in a write transaction, committed only if `f` succeeds. The commit reaches the
    /// disk soon, not at once: an OS crash or a power cut can lose it (the crate's docs on
    /// durability).
    pub fn write<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.commit(Kind::Plain, f)
    }

    /// [`StateDb::write`], on the disk when it returns, with every commit before it.
    pub fn write_durable<T>(&mut self, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        self.commit(Kind::Durable, f)
    }

    /// Puts every commit so far on the disk (nothing to do when they are).
    pub fn flush(&mut self) -> Result<()> {
        if lock(&self.shared.gate).unsynced {
            // A commit syncs only when it writes something.
            self.commit(Kind::Flush, |s| s.count_flush())?;
        } else {
            self.emptied.committed.clear();
        }
        Ok(())
    }

    /// The item at `path` on disk just left it (moved away or trashed), which the next write
    /// through this connection records. Before something else is put there
    /// ([`StateDb::filling`]), that write is made durable.
    pub fn emptied(&mut self, path: &Path) {
        self.emptied.fresh.insert(place_key(path));
        self.changed(path);
    }

    /// An entry at `path` on disk was just made, replaced or moved there: its folder is synced
    /// before any commit is made durable.
    pub fn changed(&self, path: &Path) {
        let folder = path.parent().unwrap_or(path).to_path_buf();
        lock(&self.shared.gate).dirs.insert(folder);
    }

    /// Before something is put at `path` on disk: if that place was emptied through this
    /// connection since its last durable commit, every commit is made durable first. Call
    /// [`StateDb::changed`] once it is there.
    pub fn filling(&mut self, path: &Path) -> Result<()> {
        // One emptied and not recorded yet can't be helped here: its record is still to come.
        if !self.emptied.committed.is_empty() && self.emptied.committed.contains(&place_key(path)) {
            self.commit(Kind::Flush, |s| s.count_flush())?;
        }
        Ok(())
    }

    /// Runs `f` once every folder changed so far is synced, with the gate held so that no
    /// commit comes in between: before anything that puts commits on the disk. The folders are
    /// synced before the gate is taken (taking turns with other durable commits instead), so
    /// plain writes don't wait for that.
    fn with_folders_synced<T>(&mut self, f: impl FnOnce(&mut Self, &mut Gate) -> T) -> T {
        let shared = Arc::clone(&self.shared);
        let _turn = lock(&shared.durable);
        let dirs = std::mem::take(&mut lock(&shared.gate).dirs);
        hbsync_platform::ops::sync_dirs(dirs);
        let mut gate = lock(&shared.gate);
        // Those changed meanwhile, if any.
        hbsync_platform::ops::sync_dirs(std::mem::take(&mut gate.dirs));
        f(self, &mut gate)
    }

    fn commit<T>(&mut self, kind: Kind, f: impl FnOnce(&Store<'_>) -> Result<T>) -> Result<T> {
        let (out, pages) = if kind == Kind::Plain {
            let shared = Arc::clone(&self.shared);
            let mut gate = lock(&shared.gate);
            self.commit_in(&mut gate, kind, f)?
        } else {
            self.with_folders_synced(|db, gate| db.commit_in(gate, kind, f))?
        };
        if pages >= CHECKPOINT_PAGES {
            // Copies the log into the DB file, which puts every commit on the disk. Best
            // effort: a reader in the way leaves the rest for a later try.
            self.with_folders_synced(|db, gate| {
                if pages >= gate.checkpoint_at {
                    let _ = db
                        .conn
                        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
                    gate.checkpoint_at = pages + CHECKPOINT_PAGES;
                }
            });
        }
        Ok(out)
    }

    /// Commits `f` with the gate held. Returns its result and the log's size after it.
    fn commit_in<T>(
        &mut self,
        gate: &mut Gate,
        kind: Kind,
        f: impl FnOnce(&Store<'_>) -> Result<T>,
    ) -> Result<(T, i32)> {
        let durable = kind != Kind::Plain;
        if durable {
            self.conn.pragma_update(None, "synchronous", "FULL")?;
        }
        // A commit that writes nothing syncs nothing.
        let changes = self.conn.total_changes();
        // Unknown unless the commit writes to the log.
        WAL_PAGES.set(-1);
        let committed = (|| -> Result<_> {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let store = Store::new(&tx, self.folder);
            let out = f(&store)?;
            store.finish()?;
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
            if kind != Kind::Flush {
                let fresh = std::mem::take(&mut self.emptied.fresh);
                self.emptied.committed.extend(fresh);
            }
        }
        if !gate.unsynced {
            self.emptied.committed.clear();
        }
        let pages = WAL_PAGES.get();
        if (0..CHECKPOINT_PAGES).contains(&pages) {
            // The log started over: a checkpoint emptied it.
            gate.checkpoint_at = CHECKPOINT_PAGES;
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
        Ok((out, pages))
    }

    /// The folders whose dirty sets the writes since the last call marked nodes in, from
    /// changes of the drive ([`Store::fan_out`]): the engines to wake.
    pub fn take_touched(&mut self) -> std::collections::BTreeSet<i64> {
        std::mem::take(&mut self.touched)
    }

    /// Whether `other` is a connection to the same DB file.
    pub fn same_file(&self, other: &StateDb) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// The schema version in use.
    pub fn schema_version(&self) -> Result<u32> {
        migrations::user_version(&self.conn)
    }
}

/// Runs `f` in a read transaction on `conn`, seeing folder `folder`.
fn read_in<T>(
    conn: &mut Connection,
    folder: i64,
    f: impl FnOnce(&Store<'_>) -> Result<T>,
) -> Result<T> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let out = f(&Store::new(&tx, folder))?;
    tx.finish()?;
    Ok(out)
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
        write(&mut a, "x")?;
        a.filling(&other)?;
        assert_eq!(flushes(&mut a)?, None);
        // Any case: the same place on a volume that ignores it.
        a.filling(&dir.path().join("place"))?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("1"));
        a.filling(&place)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("1"));

        // Another connection's durable commit can't vouch for a commit still to come here.
        a.emptied(&place);
        b.write_durable(|s| s.set_kv(keys::SCOPE, "x"))?;
        write(&mut a, "y")?;
        a.filling(&place)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("2"));

        // Nor can the flush before filling another place, made between emptying this one and
        // recording that.
        a.emptied(&other);
        write(&mut a, "z")?;
        a.emptied(&place);
        a.filling(&other)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("3"));
        write(&mut a, "w")?;
        a.filling(&place)?;
        assert_eq!(flushes(&mut a)?.as_deref(), Some("4"));
        Ok(())
    }

    /// Commits reach the DB file only by this crate's checkpoints, once the log is big enough:
    /// not by SQLite's own (at 1000 pages), nor as the last connection closes.
    #[test]
    fn the_log_is_copied_into_the_db_file_only_once_it_is_big() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| StateError::Conflict(e.to_string()))?;
        let path = dir.path().join("state.db");
        let len = || std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut db = StateDb::open(&path)?;
        let before = len();
        let mut i = 0;
        while WAL_PAGES.get() < 1500 {
            write(&mut db, &format!("{i:0>8000}"))?;
            i += 1;
        }
        db.flush()?;
        drop(db);
        assert_eq!(len(), before);
        let mut db = StateDb::open(&path)?;
        while len() == before && WAL_PAGES.get() < 2 * CHECKPOINT_PAGES {
            write(&mut db, &format!("{i:0>8000}"))?;
            i += 1;
        }
        assert!(len() > before);
        Ok(())
    }
}
