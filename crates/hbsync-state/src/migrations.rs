//! The state DB's schema. `PRAGMA user_version` holds its version: [`SCHEMA`] is version
//! [`BASE`], and each migration after it adds one. Migrations are append-only once released:
//! never edit one that has shipped. A DB older than `BASE` (one per folder, before 0.6) isn't
//! upgraded: its folder is set up again.

use rusqlite::{Connection, TransactionBehavior};

use crate::{Result, StateError};

/// The version [`SCHEMA`] creates: one DB per account (0.6).
const BASE: u32 = 5;

/// The whole schema, as of [`BASE`]: one DB per account. The drive's side (`nodes`, `remote`,
/// `removed`, `kv`) is the account's; each synced folder has its rows in the other tables,
/// with `folder` last in every key and index: a node's rows are found by id, a folder's listings
/// by parent or file id, and a folder alone matches no index, so the planner (which has no
/// statistics) never scans a whole folder for a lookup. `the_schema_is_pinned` pins it.
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
        PRIMARY KEY (id, folder),
        UNIQUE (remote_file_id, folder)
    ) WITHOUT ROWID;
    CREATE INDEX base_parent ON base(parent, folder);

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
        PRIMARY KEY (id, folder),
        UNIQUE (parent, name, folder)
    ) WITHOUT ROWID;
    CREATE INDEX local_file_id ON local(file_id, folder);

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
        content_extra TEXT                            -- unknown content keys (JSON), kept on rewrite
    );
    CREATE INDEX remote_parent ON remote(parent);

    -- Per folder: nodes whose local or remote side changed and still need reconciling.
    CREATE TABLE dirty (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id     INTEGER NOT NULL,
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

    -- Node ids that lost a reference since the last collection (see Store::collect_nodes).
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
        removed_at    INTEGER NOT NULL,               -- Unix ms
        root          BLOB NOT NULL                   -- the node whose delete took this one
    ) WITHOUT ROWID;
    CREATE INDEX removed_at ON removed(removed_at);
    CREATE INDEX removed_root ON removed(root);
    CREATE INDEX removed_parent ON removed(parent);
"#;

/// Migrations after [`BASE`]: the first takes a DB from `BASE` to `BASE + 1`.
const MIGRATIONS: &[&str] = &[
    // 6: per folder, downloads about to be put in place (written before a batch of them
    // runs), each with the base its commit writes: one whose commit a crash lost is still
    // known as its node, as of that version.
    "CREATE TABLE placing (
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
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
    CREATE TRIGGER placing_gone AFTER DELETE ON placing
        BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END;",
    // 7: each file's token (what the remote needs to write it again without reading it
    // first), NULL until known; the server's time of each version (NULL for this device's own
    // writes until the feed brings it where it matters); what was deleted with what goes by
    // these times (node schema, Recently deleted), so the root goes.
    "ALTER TABLE remote ADD COLUMN token BLOB;
    ALTER TABLE remote ADD COLUMN changed_at INTEGER;
    ALTER TABLE removed ADD COLUMN token BLOB;
    DROP INDEX removed_root;
    ALTER TABLE removed DROP COLUMN root;",
    // 8: a removal's time may be open: this device's own removal is in Recently deleted from
    // its write on, its time NULL until the feed brings the server's (so the feed knows that
    // version without reading it whole). SQLite drops a NOT NULL only by making the table again.
    "CREATE TABLE removed_new (
        node          BLOB PRIMARY KEY,
        file_id       BLOB NOT NULL UNIQUE,
        version_tag   BLOB NOT NULL,
        parent        BLOB NOT NULL,
        kind          INTEGER NOT NULL,
        name          TEXT NOT NULL,
        hash          BLOB,
        size          INTEGER,
        mtime_ms      INTEGER,
        executable    INTEGER NOT NULL DEFAULT 0,
        content_extra TEXT,
        removed_at    INTEGER,                        -- Unix ms; NULL: open
        token         BLOB
    ) WITHOUT ROWID;
    INSERT INTO removed_new (node, file_id, version_tag, parent, kind, name, hash, size,
        mtime_ms, executable, content_extra, removed_at, token)
        SELECT node, file_id, version_tag, parent, kind, name, hash, size, mtime_ms, executable,
            content_extra, removed_at, token FROM removed;
    DROP TABLE removed;
    ALTER TABLE removed_new RENAME TO removed;
    CREATE INDEX removed_at ON removed(removed_at);
    CREATE INDEX removed_parent ON removed(parent);",
    // 9: each removal's place in this device's order of recording them (`seq`), and the
    // counter (kv `deleted.seq`): emptying Recently deleted as listed takes exactly the rows up
    // to the largest one the listing saw. The rows there get theirs in a stable order.
    "ALTER TABLE removed ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
    CREATE TEMP TABLE removed_order AS
        SELECT node FROM removed ORDER BY removed_at IS NULL, removed_at, node;
    CREATE INDEX temp.removed_order_node ON removed_order(node);
    UPDATE removed SET seq = (SELECT o.rowid FROM temp.removed_order o WHERE o.node = removed.node);
    INSERT OR REPLACE INTO kv (key, value)
        SELECT 'deleted.seq', CAST(COUNT(*) AS TEXT) FROM removed;
    DROP TABLE temp.removed_order;
    CREATE INDEX removed_seq ON removed(seq);",
];

/// Migration 8, which [`repair_early_8`] runs again.
const MIGRATION_8: &str = MIGRATIONS[(8 - BASE - 1) as usize];

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

pub fn apply(conn: &mut Connection) -> Result<()> {
    let seen = user_version(conn)?;
    check(seen)?;
    if seen >= 8 {
        repair_early_8(conn)?;
    }
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
    let from = if current == 0 {
        tx.execute_batch(SCHEMA)?;
        BASE
    } else {
        current
    };
    for sql in &MIGRATIONS[(from - BASE) as usize..] {
        tx.execute_batch(sql)?;
    }
    tx.pragma_update(None, "user_version", VERSION)?;
    tx.commit()?;
    Ok(())
}

/// Version 8 as an earlier build of it made it (a branch, never released): a table `pending` of
/// this device's own removals, and `removed.removed_at` still NOT NULL. Made what version 8 is:
/// those removals come back through the feed as anyone else's (the table goes, unread).
fn repair_early_8(conn: &mut Connection) -> Result<()> {
    let early = |c: &Connection| -> Result<bool> {
        Ok(c.query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pending')",
            [],
            |r| r.get(0),
        )?)
    };
    if !early(conn)? {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if early(&tx)? {
        tx.execute_batch("DROP TABLE pending;")?;
        tx.execute_batch(MIGRATION_8)?;
    }
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

    /// A DB of 0.6.0 (version 6) gets the token columns, its rows without one, and loses the
    /// roots of its removals.
    #[test]
    fn version_6_gets_tokens_and_loses_roots() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(MIGRATIONS[0])?;
        conn.pragma_update(None, "user_version", 6)?;
        conn.execute(
            "INSERT INTO remote (id, file_id, version_tag, parent, kind, name) \
             VALUES (1, x'01', x'02', 0, 1, 'd')",
            [],
        )?;
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name, removed_at, \
             root) VALUES (x'01', x'02', x'03', x'04', 1, 'd', 5, x'01')",
            [],
        )?;
        apply(&mut conn)?;
        assert_eq!(user_version(&conn)?, VERSION);
        let token: Option<Vec<u8>> =
            conn.query_row("SELECT token FROM remote WHERE id = 1", [], |r| r.get(0))?;
        assert_eq!(token, None);
        let at: i64 = conn.query_row("SELECT removed_at FROM removed", [], |r| r.get(0))?;
        assert_eq!(at, 5);
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name, removed_at, \
             token) VALUES (x'05', x'06', x'07', x'04', 1, 'e', 6, x'00')",
            [],
        )?;
        Ok(())
    }

    /// A DB at version 7 keeps its removals and their times, and takes an open one.
    #[test]
    fn version_7_takes_open_removal_times() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(MIGRATIONS[0])?;
        conn.execute_batch(MIGRATIONS[1])?;
        conn.pragma_update(None, "user_version", 7)?;
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name, removed_at, \
             token) VALUES (x'01', x'02', x'03', x'04', 1, 'd', 5, x'09')",
            [],
        )?;
        apply(&mut conn)?;
        assert_eq!(user_version(&conn)?, VERSION);
        let (at, token): (i64, Vec<u8>) =
            conn.query_row("SELECT removed_at, token FROM removed", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        assert_eq!((at, token), (5, vec![9]));
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name) \
             VALUES (x'05', x'06', x'07', x'04', 1, 'e')",
            [],
        )?;
        Ok(())
    }

    /// A DB an earlier build took to version 8 (a `pending` table, `removed_at` NOT NULL) is
    /// made version 8 as it is now when opened: its removals and their times stay.
    #[test]
    fn an_early_version_8_is_repaired() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(MIGRATIONS[0])?;
        conn.execute_batch(MIGRATIONS[1])?;
        conn.execute_batch(
            "CREATE TABLE pending (file_id BLOB PRIMARY KEY, node BLOB NOT NULL, \
             version_tag BLOB NOT NULL, parent BLOB NOT NULL, kind INTEGER NOT NULL, \
             name TEXT NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, \
             executable INTEGER NOT NULL DEFAULT 0, content_extra TEXT, token BLOB) WITHOUT ROWID;",
        )?;
        conn.pragma_update(None, "user_version", 8)?;
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name, removed_at) \
             VALUES (x'01', x'02', x'03', x'04', 1, 'd', 5)",
            [],
        )?;
        apply(&mut conn)?;
        assert_eq!(user_version(&conn)?, VERSION);
        let pending: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'pending')",
            [],
            |r| r.get(0),
        )?;
        assert!(!pending);
        let at: i64 = conn.query_row("SELECT removed_at FROM removed", [], |r| r.get(0))?;
        assert_eq!(at, 5);
        conn.execute(
            "INSERT INTO removed (node, file_id, version_tag, parent, kind, name) \
             VALUES (x'05', x'06', x'07', x'04', 1, 'e')",
            [],
        )?;
        // Opened again: nothing more to do.
        apply(&mut conn)?;
        Ok(())
    }

    /// A DB at version 8 gives each removal there a place in its order of recording (oldest
    /// removal first, an open one last), and its counter the largest.
    #[test]
    fn version_8_numbers_its_removals() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        for sql in &MIGRATIONS[..3] {
            conn.execute_batch(sql)?;
        }
        conn.pragma_update(None, "user_version", 8)?;
        for (node, at) in [("01", Some(9)), ("02", None), ("03", Some(4))] {
            conn.execute(
                &format!(
                    "INSERT INTO removed (node, file_id, version_tag, parent, kind, name, \
                     removed_at) VALUES (x'{node}', x'11{node}', x'22{node}', x'00', 1, 'n', ?1)"
                ),
                [at],
            )?;
        }
        apply(&mut conn)?;
        assert_eq!(user_version(&conn)?, VERSION);
        let seqs: Vec<(Vec<u8>, i64)> = conn
            .prepare("SELECT node, seq FROM removed ORDER BY seq")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(seqs, [(vec![3], 1), (vec![1], 2), (vec![2], 3)]);
        let counter: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'deleted.seq'", [], |r| {
                r.get(0)
            })?;
        assert_eq!(counter, "3");
        Ok(())
    }

    #[test]
    fn a_db_of_one_folder_is_refused() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "user_version", 4)?;
        assert!(matches!(
            apply(&mut conn),
            Err(StateError::TooOld { found: 4 })
        ));
        Ok(())
    }

    /// The SQL of every table, index and trigger [`SCHEMA`] makes, without comments or layout.
    #[test]
    fn the_schema_is_pinned() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        let mut objects = conn
            .prepare("SELECT sql FROM sqlite_master WHERE sql NOT NULL ORDER BY type DESC, name")?;
        let made: Vec<String> = objects
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|sql| {
                let sql = sql?;
                let code = sql
                    .lines()
                    .map(|l| l.split("--").next().unwrap_or_default());
                Ok(code
                    .flat_map(str::split_whitespace)
                    .collect::<Vec<_>>()
                    .join(" "))
            })
            .collect::<Result<_>>()?;
        assert_eq!(made.join("\n"), PINNED);
        Ok(())
    }

    /// What [`SCHEMA`] makes. Once released, this changes only through [`MIGRATIONS`].
    const PINNED: &str = "\
        CREATE TRIGGER base_gone AFTER DELETE ON base BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER base_regone AFTER UPDATE OF parent ON base WHEN old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.parent); END\n\
        CREATE TRIGGER dirty_gone AFTER DELETE ON dirty BEGIN INSERT INTO gone VALUES (old.id); END\n\
        CREATE TRIGGER forks_gone AFTER DELETE ON forks BEGIN INSERT INTO gone VALUES (old.id), (old.new_node); END\n\
        CREATE TRIGGER local_gone AFTER DELETE ON local BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER local_regone AFTER UPDATE OF id, parent ON local WHEN old.id IS NOT new.id OR old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER parked_gone AFTER DELETE ON parked BEGIN INSERT INTO gone VALUES (old.id); END\n\
        CREATE TRIGGER remote_gone AFTER DELETE ON remote BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER remote_regone AFTER UPDATE OF parent ON remote WHEN old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.parent); END\n\
        CREATE TABLE base ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, id INTEGER NOT NULL, parent INTEGER NOT NULL, name TEXT NOT NULL, kind INTEGER NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, remote_file_id BLOB NOT NULL, version_tag BLOB NOT NULL, content_state INTEGER NOT NULL DEFAULT 0, local_name TEXT, local_hash BLOB, tags TEXT, PRIMARY KEY (id, folder), UNIQUE (remote_file_id, folder) ) WITHOUT ROWID\n\
        CREATE TABLE dirty ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, id INTEGER NOT NULL, PRIMARY KEY (id, folder) ) WITHOUT ROWID\n\
        CREATE TABLE folder_kv ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (folder, key) ) WITHOUT ROWID\n\
        CREATE TABLE folders ( id INTEGER PRIMARY KEY AUTOINCREMENT, top BLOB NOT NULL UNIQUE, path TEXT NOT NULL, marker TEXT NOT NULL, added_at INTEGER NOT NULL )\n\
        CREATE TABLE forks ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, id INTEGER NOT NULL, new_node INTEGER NOT NULL, to_name TEXT NOT NULL, PRIMARY KEY (id, folder) ) WITHOUT ROWID\n\
        CREATE TABLE gone (id INTEGER NOT NULL)\n\
        CREATE TABLE kv (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID\n\
        CREATE TABLE local ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, id INTEGER NOT NULL, parent INTEGER NOT NULL, name TEXT NOT NULL, kind INTEGER NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL, volume INTEGER NOT NULL, file_id INTEGER NOT NULL, file_id_hi INTEGER, executable INTEGER, hash BLOB, tags TEXT, PRIMARY KEY (id, folder), UNIQUE (parent, name, folder) ) WITHOUT ROWID\n\
        CREATE TABLE nodes ( id INTEGER PRIMARY KEY AUTOINCREMENT, uuid BLOB NOT NULL UNIQUE )\n\
        CREATE TABLE parked ( folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, id INTEGER NOT NULL, original_name TEXT NOT NULL, PRIMARY KEY (id, folder) ) WITHOUT ROWID\n\
        CREATE TABLE remote ( id INTEGER PRIMARY KEY, file_id BLOB NOT NULL UNIQUE, version_tag BLOB NOT NULL, parent INTEGER NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, content_extra TEXT )\n\
        CREATE TABLE removed ( node BLOB PRIMARY KEY, file_id BLOB NOT NULL UNIQUE, version_tag BLOB NOT NULL, parent BLOB NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, content_extra TEXT, removed_at INTEGER NOT NULL, root BLOB NOT NULL ) WITHOUT ROWID\n\
        CREATE TABLE sqlite_sequence(name,seq)\n\
        CREATE TABLE synced ( id INTEGER PRIMARY KEY AUTOINCREMENT, folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE, at INTEGER NOT NULL, kind INTEGER NOT NULL, path TEXT NOT NULL, files INTEGER NOT NULL, bytes INTEGER NOT NULL )\n\
        CREATE INDEX base_parent ON base(parent, folder)\n\
        CREATE INDEX local_file_id ON local(file_id, folder)\n\
        CREATE INDEX remote_parent ON remote(parent)\n\
        CREATE INDEX removed_at ON removed(removed_at)\n\
        CREATE INDEX removed_parent ON removed(parent)\n\
        CREATE INDEX removed_root ON removed(root)\n\
        CREATE INDEX synced_folder ON synced(folder, id)";
}
