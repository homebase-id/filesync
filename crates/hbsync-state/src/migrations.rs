//! Schema migrations. `PRAGMA user_version` holds the number of migrations applied. Migrations
//! are append-only once released: never edit one that has shipped.

use rusqlite::Connection;

use crate::{Result, StateError};

const MIGRATIONS: &[&str] = &[
    // 1: the whole schema.
    r#"
    -- Every node gets a small integer id once; every other table is keyed by it, so the
    -- 16-byte UUID is stored a single time. AUTOINCREMENT: an id is never handed out again,
    -- even after its node is forgotten.
    CREATE TABLE nodes (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid BLOB NOT NULL UNIQUE
    );

    -- The last state both sides agreed on (the three-way merge's ancestor).
    CREATE TABLE base (
        id             INTEGER PRIMARY KEY,           -- nodes.id
        parent         INTEGER NOT NULL,              -- nodes.id
        name           TEXT NOT NULL,                 -- remote spelling
        kind           INTEGER NOT NULL,              -- 0 file, 1 folder
        hash           BLOB,                          -- remote hash: algorithm byte + 32 bytes
        size           INTEGER,
        mtime_ms       INTEGER,
        executable     INTEGER NOT NULL DEFAULT 0,
        remote_file_id BLOB NOT NULL UNIQUE,          -- finds the node of a tombstone
        version_tag    BLOB NOT NULL,                 -- the version agreed on (conflict ids)
        content_state  INTEGER NOT NULL DEFAULT 0,    -- 0 local, 1 pinned, 2 online-only
        local_name     TEXT,                          -- recorded local spelling, if any
        local_hash     BLOB                           -- hash of the local bytes then
    );
    CREATE INDEX base_parent ON base(parent);

    -- What the last observation saw on disk, as a tree: each row holds its folder and its
    -- on-disk name, so a path is derived and a moved folder changes one row.
    CREATE TABLE local (
        id         INTEGER PRIMARY KEY,               -- nodes.id
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
        hash       BLOB                               -- NULL until hashed (files)
    );
    CREATE UNIQUE INDEX local_child ON local(parent, name);
    CREATE INDEX local_file_id ON local(file_id);

    -- The client's copy of the live remote nodes (the remote side of the merge).
    CREATE TABLE remote (
        id            INTEGER PRIMARY KEY,            -- nodes.id
        file_id       BLOB NOT NULL UNIQUE,
        version_tag   BLOB NOT NULL,
        parent        INTEGER NOT NULL,               -- nodes.id
        kind          INTEGER NOT NULL,
        name          TEXT NOT NULL,
        hash          BLOB,                           -- NULL: file written without a hash
        size          INTEGER,
        mtime_ms      INTEGER,
        executable    INTEGER NOT NULL DEFAULT 0,
        content_extra TEXT                            -- unknown content keys (JSON), kept on rewrite
    );
    CREATE INDEX remote_parent ON remote(parent);

    -- Nodes whose local or remote side changed and still need reconciling.
    CREATE TABLE dirty (id INTEGER PRIMARY KEY);

    -- Items parked under a temporary name in their folder, with the name they had.
    CREATE TABLE parked (id INTEGER PRIMARY KEY, original_name TEXT NOT NULL);

    -- Conflict-copy forks in progress (written before the rename, in the same folder), so a
    -- crash in between can be completed instead of misread.
    CREATE TABLE forks (
        id       INTEGER PRIMARY KEY,
        new_node INTEGER NOT NULL,
        to_name  TEXT NOT NULL
    );

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

    -- Cursors and small settings.
    CREATE TABLE kv (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
    "#,
];

pub fn apply(conn: &mut Connection) -> Result<()> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let supported = MIGRATIONS.len() as u32;
    if current > supported {
        return Err(StateError::TooNew {
            found: current,
            supported,
        });
    }
    for (i, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", i as u32 + 1)?;
        tx.commit()?;
    }
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
        let version: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        assert_eq!(version, MIGRATIONS.len() as u32);
        let value: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'k'", [], |r| r.get(0))?;
        assert_eq!(value, "v");
        Ok(())
    }
}
