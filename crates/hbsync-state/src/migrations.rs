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
const MIGRATIONS: &[&str] = &[];

/// The version this client makes and reads.
const VERSION: u32 = BASE + MIGRATIONS.len() as u32;

pub(crate) fn user_version(conn: &Connection) -> Result<u32> {
    Ok(conn.pragma_query_value(None, "user_version", |r| r.get(0))?)
}

/// Refuses a DB this client can't use (0 is a new one).
fn check(version: u32) -> Result<()> {
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
