//! The state DB's schema. `PRAGMA user_version` holds its version: [`SCHEMA`] is version
//! [`BASE`], and each migration after it adds one. Migrations are append-only once released:
//! never edit one that has shipped. A DB older than `BASE` (made before 0.3) isn't upgraded: its
//! folder is set up again.

use rusqlite::{Connection, TransactionBehavior};

use crate::{Result, StateError};

/// The version [`SCHEMA`] creates: 0.3.0's.
const BASE: u32 = 4;

/// The whole schema, as of [`BASE`]. It has shipped: change it only through [`MIGRATIONS`] (0.3
/// DBs are at `BASE` and never run it again). `the_schema_is_0_3s` pins it.
const SCHEMA: &str = r#"
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
        local_hash     BLOB,                          -- hash of the local bytes then
        tags           TEXT                           -- Finder tags (JSON, node schema `tags`); NULL: none
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
        hash       BLOB,                              -- NULL until hashed (files)
        tags       TEXT                               -- Finder tags (JSON); NULL: not known (none on
                                                      -- this system, or the read failed)
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
        hash          BLOB,                           -- files only
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

    -- What was synced, for people to see (the writer keeps the newest few hundred).
    CREATE TABLE synced (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        at    INTEGER NOT NULL,                       -- Unix seconds
        kind  INTEGER NOT NULL,
        path  TEXT NOT NULL,
        files INTEGER NOT NULL,
        bytes INTEGER NOT NULL
    );

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

pub fn apply(conn: &mut Connection) -> Result<()> {
    let supported = BASE + MIGRATIONS.len() as u32;
    let version =
        |c: &Connection| c.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0));
    if version(conn)? == supported {
        return Ok(());
    }
    // Another connection may be creating or migrating the same DB: decide under the write lock.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = version(&tx)?;
    if current > supported {
        return Err(StateError::TooNew {
            found: current,
            supported,
        });
    }
    if current == 0 {
        tx.execute_batch(SCHEMA)?;
    } else if current < BASE {
        return Err(StateError::TooOld { found: current });
    }
    for sql in &MIGRATIONS[(current.max(BASE) - BASE) as usize..] {
        tx.execute_batch(sql)?;
    }
    tx.pragma_update(None, "user_version", supported)?;
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
        let version: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        assert_eq!(version, BASE + MIGRATIONS.len() as u32);
        let value: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'k'", [], |r| r.get(0))?;
        assert_eq!(value, "v");
        Ok(())
    }

    #[test]
    fn a_db_from_before_0_3_is_refused() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "user_version", 3)?;
        assert!(matches!(
            apply(&mut conn),
            Err(StateError::TooOld { found: 3 })
        ));
        Ok(())
    }

    /// Triggers and indexes by their SQL, tables by their columns (type, NOT NULL, default,
    /// key position) and UNIQUE constraints: what the code relies on, not comments or layout.
    fn describe(conn: &Connection) -> Result<String> {
        let mut out = String::new();
        let mut objects = conn.prepare(
            "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' \
             ORDER BY type DESC, name",
        )?;
        let objects: Vec<(String, String, Option<String>)> = objects
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (kind, name, sql) in objects {
            if kind == "table" {
                let mut cols = conn.prepare(&format!("PRAGMA table_info({name})"))?;
                let cols: Vec<String> = cols
                    .query_map([], |r| {
                        let (col, ty, not_null, default, pk): (
                            String,
                            String,
                            bool,
                            Option<String>,
                            u32,
                        ) = (r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?);
                        let mut c = format!("{col} {ty}");
                        if not_null {
                            c += " NOT NULL";
                        }
                        if let Some(d) = default {
                            c += &format!(" DEFAULT {d}");
                        }
                        if pk > 0 {
                            c += &format!(" PK{pk}");
                        }
                        Ok(c)
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                let mut uniques = conn.prepare(&format!(
                    "SELECT group_concat(c.name, ', ') FROM pragma_index_list('{name}') l, \
                     pragma_index_info(l.name) c WHERE l.origin = 'u' GROUP BY l.name ORDER BY l.name"
                ))?;
                let uniques: Vec<String> = uniques
                    .query_map([], |r| Ok(format!("UNIQUE({})", r.get::<_, String>(0)?)))?
                    .collect::<rusqlite::Result<_>>()?;
                let all: Vec<String> = cols.into_iter().chain(uniques).collect();
                out += &format!("{name}: {}\n", all.join(", "));
            } else {
                let sql = sql.unwrap_or_default();
                out += &format!("{}\n", sql.split_whitespace().collect::<Vec<_>>().join(" "));
            }
        }
        Ok(out)
    }

    #[test]
    fn the_schema_is_0_3s() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        apply(&mut conn)?;
        assert_eq!(describe(&conn)?, PINNED_0_3);
        Ok(())
    }

    /// What 0.3.0's four migrations made (version 4), checked against them when the schema was
    /// squashed.
    const PINNED_0_3: &str = "\
        CREATE TRIGGER base_gone AFTER DELETE ON base BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER base_regone AFTER UPDATE OF parent ON base WHEN old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.parent); END\n\
        CREATE TRIGGER dirty_gone AFTER DELETE ON dirty BEGIN INSERT INTO gone VALUES (old.id); END\n\
        CREATE TRIGGER forks_gone AFTER DELETE ON forks BEGIN INSERT INTO gone VALUES (old.id), (old.new_node); END\n\
        CREATE TRIGGER local_gone AFTER DELETE ON local BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER local_regone AFTER UPDATE OF id, parent ON local WHEN old.id IS NOT new.id OR old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER parked_gone AFTER DELETE ON parked BEGIN INSERT INTO gone VALUES (old.id); END\n\
        CREATE TRIGGER remote_gone AFTER DELETE ON remote BEGIN INSERT INTO gone VALUES (old.id), (old.parent); END\n\
        CREATE TRIGGER remote_regone AFTER UPDATE OF parent ON remote WHEN old.parent IS NOT new.parent BEGIN INSERT INTO gone VALUES (old.parent); END\n\
        base: id INTEGER PK1, parent INTEGER NOT NULL, name TEXT NOT NULL, kind INTEGER NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, remote_file_id BLOB NOT NULL, version_tag BLOB NOT NULL, content_state INTEGER NOT NULL DEFAULT 0, local_name TEXT, local_hash BLOB, tags TEXT, UNIQUE(remote_file_id)\n\
        dirty: id INTEGER PK1\n\
        forks: id INTEGER PK1, new_node INTEGER NOT NULL, to_name TEXT NOT NULL\n\
        gone: id INTEGER NOT NULL\n\
        kv: key TEXT NOT NULL PK1, value TEXT NOT NULL\n\
        local: id INTEGER PK1, parent INTEGER NOT NULL, name TEXT NOT NULL, kind INTEGER NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL, volume INTEGER NOT NULL, file_id INTEGER NOT NULL, file_id_hi INTEGER, executable INTEGER, hash BLOB, tags TEXT\n\
        nodes: id INTEGER PK1, uuid BLOB NOT NULL, UNIQUE(uuid)\n\
        parked: id INTEGER PK1, original_name TEXT NOT NULL\n\
        remote: id INTEGER PK1, file_id BLOB NOT NULL, version_tag BLOB NOT NULL, parent INTEGER NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, content_extra TEXT, UNIQUE(file_id)\n\
        removed: node BLOB NOT NULL PK1, file_id BLOB NOT NULL, version_tag BLOB NOT NULL, parent BLOB NOT NULL, kind INTEGER NOT NULL, name TEXT NOT NULL, hash BLOB, size INTEGER, mtime_ms INTEGER, executable INTEGER NOT NULL DEFAULT 0, content_extra TEXT, removed_at INTEGER NOT NULL, root BLOB NOT NULL, UNIQUE(file_id)\n\
        synced: id INTEGER PK1, at INTEGER NOT NULL, kind INTEGER NOT NULL, path TEXT NOT NULL, files INTEGER NOT NULL, bytes INTEGER NOT NULL\n\
        CREATE INDEX base_parent ON base(parent)\n\
        CREATE UNIQUE INDEX local_child ON local(parent, name)\n\
        CREATE INDEX local_file_id ON local(file_id)\n\
        CREATE INDEX remote_parent ON remote(parent)\n\
        CREATE INDEX removed_at ON removed(removed_at)\n\
        CREATE INDEX removed_parent ON removed(parent)\n\
        CREATE INDEX removed_root ON removed(root)\n\
    ";
}
