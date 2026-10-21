//! The state DB's schema. `PRAGMA user_version` holds its version: a new DB is made at
//! [`VERSION`] by [`SCHEMA`]; one of version [`BASE`] or later is brought up to it by
//! [`MIGRATIONS`], each adding one. A DB older than `BASE` (before 0.7) isn't upgraded: its
//! account is set up again.
//!
//! Changing the schema:
//! - a migration is appended, and [`SCHEMA`] makes the same change: a new DB never runs
//!   migrations. `the_schema_is_what_an_upgrade_makes` checks that they agree;
//! - a migration's data steps (filling a column, a kv row) are for the DBs it upgrades: what a
//!   new DB needs too goes in [`SCHEMA`] or in creating it ([`apply`]);
//! - migrations are append-only once released: never edit one that has shipped;
//! - a release that ships a [`SCHEMA`] the first time freezes its schema, as SQLite keeps it, in
//!   `v<N>.sql` beside `v13.sql`, for the test to bring up to date too.

use rusqlite::{Connection, TransactionBehavior};

use crate::{Result, StateError};

/// The oldest version upgraded: 0.7.0's and 0.7.1's.
const BASE: u32 = 13;

/// The whole schema, as of [`VERSION`]: one DB per account. The drive's side (`nodes`, `remote`,
/// `removed`, `kv`) is the account's; each synced folder has its rows in the other tables,
/// with `folder` last in every key and index: a node's rows are found by id, a folder's listings
/// by parent or file id, and a folder alone matches no index, so the planner (which has no
/// statistics) never scans a whole folder for a lookup.
const SCHEMA: &str = r#"
    -- Every node gets a small integer id once; every other table is keyed by it, so the
    -- 16-byte UUID is stored a single time. AUTOINCREMENT: an id is never handed out again,
    -- even after its node is forgotten.
    CREATE TABLE nodes (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid BLOB NOT NULL UNIQUE
    );

    -- The folders this account syncs on this device. `top` is the drive's node a folder
    -- holds (the drive's root for the main folder); its rows below are in its own coordinates,
    -- its top being the root. `marker` matches the marker file in the folder's `.hbsync`.
    CREATE TABLE folders (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        top      BLOB NOT NULL UNIQUE,
        path     TEXT NOT NULL,
        marker   TEXT NOT NULL,
        added_at INTEGER NOT NULL                     -- Unix seconds
    );

    -- Per folder: the last state both sides agreed on (the three-way merge's ancestor).
    CREATE TABLE base (
        folder         INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id             INTEGER NOT NULL,              -- nodes.id
        parent         INTEGER NOT NULL,              -- nodes.id
        name           TEXT NOT NULL,                 -- remote spelling
        kind           INTEGER NOT NULL,              -- 0 file, 1 folder
        hash           BLOB,                          -- remote hash: algorithm byte + 32 bytes
        size           INTEGER,
        mtime_ms       INTEGER,
        executable     INTEGER NOT NULL DEFAULT 0,
        remote_file_id BLOB NOT NULL,                 -- finds the node of a tombstone
        version_tag    BLOB NOT NULL,                 -- the version agreed on (conflict ids)
        content_state  INTEGER NOT NULL DEFAULT 0,    -- 0 local, 1 pinned, 2 online-only
        local_name     TEXT,                          -- recorded local spelling, if any
        local_hash     BLOB,                          -- hash of the local bytes then
        tags           TEXT,                          -- Finder tags (JSON, node schema `tags`); NULL: none
        name_key       INTEGER NOT NULL DEFAULT 0,    -- sibling hash of name (below)
        PRIMARY KEY (id, folder),
        UNIQUE (remote_file_id, folder)
    ) WITHOUT ROWID;
    -- Each side's rows by their name's sibling hash (`hbsync_namemap::sibling_hash`) with their
    -- folder: planning a change reads the rows that may clash with its name, not the whole
    -- folder.
    CREATE INDEX base_parent ON base(parent, name_key, folder);

    -- Per folder: what the last observation saw on disk, as a tree: each row holds its folder
    -- and its on-disk name, so a path is derived and a moved folder changes one row.
    CREATE TABLE local (
        folder     INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id         INTEGER NOT NULL,                  -- nodes.id
        parent     INTEGER NOT NULL,                  -- nodes.id of the folder (the root's id at the top)
        name       TEXT NOT NULL,                     -- local spelling
        kind       INTEGER NOT NULL,
        size       INTEGER NOT NULL,
        mtime_ns   INTEGER NOT NULL,
        ctime_ns   INTEGER NOT NULL,
        volume     INTEGER NOT NULL,                  -- part of the fingerprint: a root that moves
                                                      -- to another volume rehashes
        file_id    INTEGER NOT NULL,                  -- low 64 bits
        file_id_hi INTEGER,                           -- high 64 bits, only when not zero
        executable INTEGER,                           -- NULL where the OS has no x bit
        hash       BLOB,                              -- NULL until hashed (files)
        tags       TEXT,                              -- Finder tags (JSON); NULL: not known (none on
                                                      -- this system, or the read failed)
        name_key   INTEGER NOT NULL DEFAULT 0,        -- sibling hash of name
        PRIMARY KEY (id, folder),
        UNIQUE (parent, name, folder)
    ) WITHOUT ROWID;
    CREATE INDEX local_file_id ON local(file_id, folder);
    CREATE INDEX local_key ON local(parent, name_key, folder);

    -- The client's copy of the live remote nodes (the remote side of the merge), the drive's
    -- whole tree.
    CREATE TABLE remote (
        id            INTEGER PRIMARY KEY,            -- nodes.id
        file_id       BLOB NOT NULL UNIQUE,
        version_tag   BLOB NOT NULL,
        parent        INTEGER NOT NULL,               -- nodes.id
        kind          INTEGER NOT NULL,
        name          TEXT NOT NULL,
        hash          BLOB,                           -- files only
        size          INTEGER,
        mtime_ms      INTEGER,
        executable    INTEGER NOT NULL DEFAULT 0,
        content_extra TEXT,                           -- unknown content keys (JSON), kept on rewrite
        token         BLOB,                           -- what the remote needs to write the file
                                                      -- again without reading it; NULL: not known
        changed_at    INTEGER,                        -- the server's time of the version; NULL for
                                                      -- this device's own write until the feed
                                                      -- brings it where it matters
        name_key      INTEGER NOT NULL DEFAULT 0      -- sibling hash of name
    );
    CREATE INDEX remote_parent ON remote(parent, name_key);

    -- Per folder: nodes whose local or remote side changed and still need reconciling.
    CREATE TABLE dirty (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id     INTEGER NOT NULL,
        seq    INTEGER NOT NULL DEFAULT 0,            -- its mark's version, moved on by a change
                                                      -- of the drive's picture: a round clears
                                                      -- only the one it read
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;

    -- Per folder: items parked under a temporary name in their folder, with the name they had.
    CREATE TABLE parked (
        folder        INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id            INTEGER NOT NULL,
        original_name TEXT NOT NULL,
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;

    -- Per folder: conflict-copy forks in progress (written before the rename, in the same
    -- folder), so a crash in between can be completed instead of misread.
    CREATE TABLE forks (
        folder   INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id       INTEGER NOT NULL,
        new_node INTEGER NOT NULL,
        to_name  TEXT NOT NULL,
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;

    -- Node ids that lost a reference since the last collection (see AccountTx::collect_nodes).
    -- Not unique: a trigger's conflict handling yields to the statement that fired it, and a
    -- repeated id costs nothing (the table is emptied at each collection).
    CREATE TABLE gone (id INTEGER NOT NULL);
    CREATE TRIGGER local_gone AFTER DELETE ON local
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;
    CREATE TRIGGER local_regone AFTER UPDATE OF id, parent ON local
        WHEN old.id IS NOT new.id OR old.parent IS NOT new.parent
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;
    CREATE TRIGGER base_gone AFTER DELETE ON base
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;
    CREATE TRIGGER base_regone AFTER UPDATE OF parent ON base
        WHEN old.parent IS NOT new.parent
        BEGIN INSERT INTO gone VALUES (old.parent); END;
    CREATE TRIGGER remote_gone AFTER DELETE ON remote
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;
    CREATE TRIGGER remote_regone AFTER UPDATE OF parent ON remote
        WHEN old.parent IS NOT new.parent
        BEGIN INSERT INTO gone VALUES (old.parent); END;
    CREATE TRIGGER dirty_gone AFTER DELETE ON dirty
        BEGIN INSERT INTO gone VALUES (old.id); END;
    CREATE TRIGGER parked_gone AFTER DELETE ON parked
        BEGIN INSERT INTO gone VALUES (old.id); END;
    CREATE TRIGGER forks_gone AFTER DELETE ON forks
        BEGIN INSERT INTO gone VALUES (old.id), (old.new_node); END;

    -- The account's cursors and settings (the feed's cursor, the identity).
    CREATE TABLE kv (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;

    -- Per folder: cursors and settings (the journal's cursor, the filter's fingerprint).
    CREATE TABLE folder_kv (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        key    TEXT NOT NULL,
        value  TEXT NOT NULL,
        PRIMARY KEY (folder, key)
    ) WITHOUT ROWID;

    -- Per folder: what was synced, for people to see (the writer keeps the newest few hundred).
    CREATE TABLE synced (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        at     INTEGER NOT NULL,                      -- Unix seconds
        kind   INTEGER NOT NULL,
        path   TEXT NOT NULL,
        files  INTEGER NOT NULL,
        bytes  INTEGER NOT NULL
    );
    CREATE INDEX synced_folder ON synced(folder, id);

    -- Per folder: downloads about to be put in place (written before a batch of them runs),
    -- each with the base its commit writes: one whose commit a crash lost is still known as its
    -- node, as of that version.
    CREATE TABLE placing (
        folder         INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id             INTEGER NOT NULL,              -- nodes.id
        path           TEXT NOT NULL,                 -- where it goes
        parent         INTEGER NOT NULL,              -- nodes.id; the rest as in base
        name           TEXT NOT NULL,
        kind           INTEGER NOT NULL,
        hash           BLOB,
        size           INTEGER,
        mtime_ms       INTEGER,
        executable     INTEGER NOT NULL DEFAULT 0,
        remote_file_id BLOB NOT NULL,
        version_tag    BLOB NOT NULL,
        tags           TEXT,
        placed_tags    TEXT,                          -- what the item holds of tags; NULL: not known
        prior_tags     TEXT,                          -- what it held before; NULL: not known
        base_at        BLOB,                          -- the base's version its plan saw (empty: no
                                                      -- base; NULL: a record not checked)
        held_ms        INTEGER,                       -- since when recovery found its file there
                                                      -- but not hashed yet; NULL: not so far
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
    CREATE TRIGGER placing_gone AFTER DELETE ON placing
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;

    -- Per folder: operations the drive refused for good (a request it won't take as it is),
    -- not sent again while what they would send is the same, for an hour at most, and until
    -- Sync now (sync semantics §8a). Read whole before each plan: few rows, if any.
    CREATE TABLE refused (
        folder  INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id      INTEGER NOT NULL,                     -- nodes.id
        op      TEXT NOT NULL,                        -- the operation's kind
        key     TEXT NOT NULL,                        -- what it sends or fetches, as the engine
                                                      -- keys it: another is another operation
        refused INTEGER NOT NULL,                     -- 0: one strike (a download whose bytes
                                                      -- didn't match once), 1: refused
        why     TEXT NOT NULL,
        at      INTEGER NOT NULL,                     -- Unix ms
        PRIMARY KEY (id, folder, op)
    ) WITHOUT ROWID;
    CREATE TRIGGER refused_gone AFTER DELETE ON refused
        BEGIN INSERT INTO gone VALUES (old.id); END;

    -- Recently deleted: the removed remote nodes (node schema, archivalStatus 2), restorable
    -- until purged. By UUID, not nodes.id: these nodes are out of the tree and its collection.
    CREATE TABLE removed (
        node          BLOB PRIMARY KEY,
        file_id       BLOB NOT NULL UNIQUE,
        version_tag   BLOB NOT NULL,
        parent        BLOB NOT NULL,
        kind          INTEGER NOT NULL,
        name          TEXT NOT NULL,
        hash          BLOB,                           -- files only
        size          INTEGER,
        mtime_ms      INTEGER,
        executable    INTEGER NOT NULL DEFAULT 0,
        content_extra TEXT,                           -- as in remote, without rm
        removed_at    INTEGER,                        -- Unix ms; NULL: open (this device's own
                                                      -- removal, until the feed brings its time)
        token         BLOB,                           -- as in remote
        seq           INTEGER NOT NULL DEFAULT 0      -- its place in this device's order of
                                                      -- recording removals (kv `deleted.seq`):
                                                      -- emptying as listed takes the rows up to
                                                      -- the counter the listing read; no index
    ) WITHOUT ROWID;
    CREATE INDEX removed_at ON removed(removed_at);
    CREATE INDEX removed_parent ON removed(parent);

    -- File ids deleted for good (a tombstone, or an announced hard delete taken): Homebase never
    -- gives one back, so no row may put one back in the mirror or Recently deleted. With the
    -- tombstone it left while that is still on the drive (its version and time, the server's:
    -- deleted hard once old enough), and since when it is known gone (the server's time; NULL:
    -- not known yet, the next purge's). No index: the daily purge reads them all once.
    CREATE TABLE gone_files (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB,
        at          INTEGER,
        gone_at     INTEGER
    ) WITHOUT ROWID;

    -- Files the identity said once aren't there (its own 404), at the version this device knew
    -- then: gone for good only when it says so again, a minute on at least, at that version
    -- (sync semantics §8b). Any row of the file, or an answer that it is there, clears it.
    -- Few rows, if any.
    CREATE TABLE gone_suspects (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB,                             -- the version this device knew; NULL: none
        at          INTEGER NOT NULL                  -- Unix ms, this device's clock
    ) WITHOUT ROWID;

    -- Rows of the drive this client can't read as nodes (a version that doesn't decrypt,
    -- changes a node's kind, a newer writer's), there all the same: a folder that is one keeps
    -- what is in it from being an orphan, and a node whose newer version is one is held (its
    -- writes fork). Any later row of the file, or its delete, clears it. Few rows, if any.
    CREATE TABLE unreadable (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB NOT NULL,                    -- the version that didn't read
        node        BLOB,                             -- its uniqueId and groupId, where they read
        parent      BLOB
    ) WITHOUT ROWID;
"#;

/// Migrations after [`BASE`]: the first takes a DB from `BASE` to `BASE + 1`.
const MIGRATIONS: &[&str] = &[
    // 14: each row's sibling hash (`hbsync_namemap::sibling_hash` of its name, which `apply`
    // gives SQL as `sibling_hash`), indexed with its folder on every side: planning a change
    // reads the rows that may clash with its name, not the whole folder.
    "ALTER TABLE base ADD COLUMN name_key INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE local ADD COLUMN name_key INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE remote ADD COLUMN name_key INTEGER NOT NULL DEFAULT 0;
    UPDATE base SET name_key = sibling_hash(name);
    UPDATE local SET name_key = sibling_hash(name);
    UPDATE remote SET name_key = sibling_hash(name);
    DROP INDEX base_parent;
    CREATE INDEX base_parent ON base(parent, name_key, folder);
    DROP INDEX remote_parent;
    CREATE INDEX remote_parent ON remote(parent, name_key);
    CREATE INDEX local_key ON local(parent, name_key, folder);",
    // 15: operations the drive refused for good.
    "CREATE TABLE refused (
        folder  INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id      INTEGER NOT NULL,                     -- nodes.id
        op      TEXT NOT NULL,                        -- the operation's kind
        key     TEXT NOT NULL,                        -- what it sends or fetches, as the engine
                                                      -- keys it: another is another operation
        refused INTEGER NOT NULL,                     -- 0: one strike (a download whose bytes
                                                      -- didn't match once), 1: refused
        why     TEXT NOT NULL,
        at      INTEGER NOT NULL,                     -- Unix ms
        PRIMARY KEY (id, folder, op)
    ) WITHOUT ROWID;
    CREATE TRIGGER refused_gone AFTER DELETE ON refused
        BEGIN INSERT INTO gone VALUES (old.id); END;",
    // 16: files the identity said once aren't there.
    "CREATE TABLE gone_suspects (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB,                             -- the version this device knew; NULL: none
        at          INTEGER NOT NULL                  -- Unix ms, this device's clock
    ) WITHOUT ROWID;",
    // 17: rows this client can't read as nodes.
    "CREATE TABLE unreadable (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB NOT NULL,                    -- the version that didn't read
        node        BLOB,                             -- its uniqueId and groupId, where they read
        parent      BLOB
    ) WITHOUT ROWID;",
    // 18: each dirty mark's version: a plan clears only the marks it read.
    "ALTER TABLE dirty ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;",
];

/// The version this client makes and reads.
pub(crate) const VERSION: u32 = BASE + MIGRATIONS.len() as u32;

pub(crate) fn user_version(conn: &Connection) -> Result<u32> {
    Ok(conn.pragma_query_value(None, "user_version", |r| r.get(0))?)
}

/// Refuses a DB this client can't use (0 is a new one).
pub(crate) fn check(version: u32) -> Result<()> {
    if version > VERSION {
        Err(StateError::TooNew {
            found: version,
            supported: VERSION,
        })
    } else if version != 0 && version < BASE {
        Err(StateError::TooOld { found: version })
    } else {
        Ok(())
    }
}

/// `sibling_hash(name)` in SQL: what migration 14 fills the new columns with.
fn sibling_hash_in_sql(conn: &Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;
    conn.create_scalar_function(
        "sibling_hash",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| Ok(hbsync_namemap::sibling_hash(&ctx.get::<String>(0)?)),
    )?;
    Ok(())
}

/// Makes a new DB, or brings one of [`BASE`] or later up to [`VERSION`].
pub(crate) fn apply(conn: &mut Connection) -> Result<()> {
    let seen = user_version(conn)?;
    check(seen)?;
    if seen == VERSION {
        return Ok(());
    }
    // Another connection may be creating or migrating the same DB: decide again under the
    // write lock.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = user_version(&tx)?;
    check(current)?;
    if current == VERSION {
        return Ok(());
    }
    if current == 0 {
        tx.execute_batch(SCHEMA)?;
        // Its own random id: a listing of its Recently deleted names it, so one of another DB
        // of the identity (made again) is told apart.
        tx.execute(
            "INSERT INTO kv (key, value) VALUES ('db.id', lower(hex(randomblob(16))))",
            [],
        )?;
    } else {
        sibling_hash_in_sql(&tx)?;
        for sql in &MIGRATIONS[(current - BASE) as usize..] {
            tx.execute_batch(sql)?;
        }
    }
    tx.pragma_update(None, "user_version", VERSION)?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applying_twice_changes_nothing() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        apply(&mut conn)?;
        conn.execute("INSERT INTO kv (key, value) VALUES ('k', 'v')", [])?;
        apply(&mut conn)?;
        assert_eq!(user_version(&conn)?, VERSION);
        let value: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'k'", [], |r| r.get(0))?;
        assert_eq!(value, "v");
        Ok(())
    }

    /// A DB has its own id from its first open on: one at the latest version without one (or
    /// with an empty one) gets one; one that has one keeps it.
    #[test]
    fn every_db_has_its_own_id() -> Result<()> {
        let id = |conn: &Connection| -> Result<String> {
            Ok(conn.query_row("SELECT value FROM kv WHERE key = 'db.id'", [], |r| r.get(0))?)
        };
        let mut conn = Connection::open_in_memory()?;
        apply(&mut conn)?;
        let first = id(&conn)?;
        assert_eq!(first.len(), 32);
        apply(&mut conn)?;
        assert_eq!(id(&conn)?, first, "kept");
        let mut other = Connection::open_in_memory()?;
        apply(&mut other)?;
        assert_ne!(id(&other)?, first);
        Ok(())
    }

    /// A DB at version 13 gets each row's sibling hash, on every side.
    #[test]
    fn version_13_gets_sibling_hashes() -> Result<()> {
        let mut conn = version_13()?;
        conn.execute_batch(
            "INSERT INTO folders (top, path, marker, added_at) VALUES (x'00', '/f', 'm', 0);
             INSERT INTO base (folder, id, parent, name, kind, remote_file_id, version_tag)
                 VALUES (1, 2, 1, 'Report.pdf', 0, x'01', x'02');
             INSERT INTO local (folder, id, parent, name, kind, size, mtime_ns, ctime_ns,
                 volume, file_id) VALUES (1, 2, 1, 'a\u{F022}b', 0, 0, 0, 0, 0, 0);
             INSERT INTO remote (id, file_id, version_tag, parent, kind, name)
                 VALUES (2, x'01', x'02', 1, 0, 'REPORT.pdf');",
        )?;
        apply(&mut conn)?;
        let key = |table: &str| -> Result<i32> {
            Ok(conn.query_row(&format!("SELECT name_key FROM {table}"), [], |r| r.get(0))?)
        };
        let report = hbsync_namemap::sibling_hash("report.pdf");
        assert_eq!(key("base")?, report);
        assert_eq!(key("remote")?, report);
        assert_eq!(key("local")?, hbsync_namemap::sibling_hash("a:b"));
        Ok(())
    }

    /// One from before 0.7 (6: 0.6.0's; 12: the last before 0.7; 4: one DB per folder) isn't
    /// upgraded.
    #[test]
    fn a_db_from_before_0_7_is_refused() -> Result<()> {
        for found in [6, 12, 4] {
            let mut conn = Connection::open_in_memory()?;
            conn.pragma_update(None, "user_version", found)?;
            assert!(matches!(
                apply(&mut conn),
                Err(StateError::TooOld { found: f }) if f == found
            ));
        }
        Ok(())
    }

    /// A DB of 0.7.0 or 0.7.1 (version 13), as SQLite keeps its schema.
    fn version_13() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(include_str!("v13.sql"))?;
        conn.pragma_update(None, "user_version", 13)?;
        Ok(conn)
    }

    /// Everything SQLite knows of a schema: each table's columns (type, NOT NULL, default,
    /// key), its foreign keys, whether it has rowids and whether they are AUTOINCREMENT (never
    /// handed out again); each index's table, columns, order, uniqueness and origin; each
    /// trigger's SQL. Not how the SQL was spelled: a table a migration changed keeps its first
    /// `CREATE` with the changes appended.
    fn shape(conn: &Connection) -> Result<Vec<String>> {
        // Each row's values, in Debug form (NULL apart from 0, text apart from numbers).
        let rows = |sql: &str| -> Result<Vec<String>> {
            let mut st = conn.prepare(sql)?;
            let n = st.column_count();
            let all = st.query_map([], |r| {
                (0..n)
                    .map(|i| Ok(format!("{:?}", r.get::<_, rusqlite::types::Value>(i)?)))
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map(|v| v.join(" "))
            })?;
            Ok(all.collect::<rusqlite::Result<_>>()?)
        };
        let names = |sql: &str| -> Result<Vec<String>> {
            let mut st = conn.prepare(sql)?;
            let all = st.query_map([], |r| r.get::<_, String>(0))?;
            Ok(all.collect::<rusqlite::Result<_>>()?)
        };
        let mut out = Vec::new();
        for t in names("SELECT name FROM pragma_table_list WHERE schema = 'main' ORDER BY name")? {
            out.push(format!("table {t} (type, ncol, wr, strict): {:?}", rows(&format!(
                "SELECT type, ncol, wr, strict FROM pragma_table_list('{t}') WHERE schema = 'main'"
            ))?));
            out.push(format!(
                "  columns {:?}",
                rows(&format!("SELECT * FROM pragma_table_xinfo('{t}')"))?
            ));
            out.push(format!(
                "  keys {:?}",
                rows(&format!("SELECT * FROM pragma_foreign_key_list('{t}')"))?
            ));
            out.push(format!(
                "  autoincrement {:?}",
                rows(&format!(
                    "SELECT sql LIKE '%AUTOINCREMENT%' FROM sqlite_master \
                     WHERE type = 'table' AND name = '{t}'"
                ))?
            ));
            for i in names(&format!(
                "SELECT name FROM pragma_index_list('{t}') ORDER BY name"
            ))? {
                out.push(format!(
                    "  index {i} {:?} {:?}",
                    rows(&format!(
                        "SELECT \"unique\", origin, partial FROM pragma_index_list('{t}') WHERE name = '{i}'"
                    ))?,
                    rows(&format!("SELECT * FROM pragma_index_xinfo('{i}')"))?
                ));
            }
        }
        // Without comments or layout.
        for sql in names("SELECT sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")? {
            let code = sql
                .lines()
                .map(|l| l.split("--").next().unwrap_or_default());
            out.push(
                code.flat_map(str::split_whitespace)
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
        Ok(out)
    }

    /// A new DB and one of 0.7.1 brought up to date have the same schema: [`SCHEMA`] is what
    /// the migrations from it made.
    #[test]
    fn the_schema_is_what_an_upgrade_makes() -> Result<()> {
        let mut new = Connection::open_in_memory()?;
        apply(&mut new)?;
        let mut upgraded = version_13()?;
        apply(&mut upgraded)?;
        assert_eq!(user_version(&upgraded)?, VERSION);
        let (new, upgraded) = (shape(&new)?, shape(&upgraded)?);
        assert!(new.len() > 20, "{new:#?}");
        assert_eq!(new, upgraded);
        Ok(())
    }
}
