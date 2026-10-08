//! Schema migrations. `PRAGMA user_version` holds the number of migrations applied. Migrations
//! are append-only: never edit one that has shipped.

use rusqlite::{Connection, Transaction};

use crate::{Result, StateError};

/// One schema step: SQL, or code for what SQL can't express.
enum Migration {
    Sql(&'static str),
    Code(fn(&Transaction<'_>) -> Result<()>),
}

use Migration::{Code, Sql};

const MIGRATIONS: &[Migration] = &[
    // 1: base, local observations, dirty set, key/value.
    Sql(r#"
    CREATE TABLE base (
        node_id        BLOB PRIMARY KEY NOT NULL,
        parent_id      BLOB NOT NULL,
        name           TEXT NOT NULL,
        kind           INTEGER NOT NULL,          -- 0 file, 1 folder
        hash           BLOB,                      -- algorithm byte + 32 bytes (files)
        size           INTEGER,
        mtime_ms       INTEGER,
        executable     INTEGER NOT NULL DEFAULT 0,
        remote_file_id BLOB NOT NULL,
        version_tag    BLOB NOT NULL,
        content_state  INTEGER NOT NULL DEFAULT 0, -- 0 local, 1 pinned, 2 online-only
        local_name     TEXT                        -- recorded local spelling, if any
    ) WITHOUT ROWID;
    CREATE INDEX base_parent ON base(parent_id);
    CREATE UNIQUE INDEX base_remote_file ON base(remote_file_id);

    CREATE TABLE local_items (
        path        TEXT PRIMARY KEY NOT NULL COLLATE hbpath,
        node_id     BLOB NOT NULL UNIQUE,
        kind        INTEGER NOT NULL,
        size        INTEGER NOT NULL,
        mtime_ns    INTEGER NOT NULL,
        ctime_ns    INTEGER NOT NULL,
        volume      INTEGER NOT NULL,
        file_id     BLOB NOT NULL,
        executable  INTEGER,                      -- NULL where the OS has no x bit
        hash        BLOB                          -- NULL until hashed (files)
    ) WITHOUT ROWID;
    CREATE INDEX local_file_id ON local_items(volume, file_id);

    CREATE TABLE dirty (
        node_id BLOB PRIMARY KEY NOT NULL
    ) WITHOUT ROWID;

    CREATE TABLE kv (
        key   TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
    ) WITHOUT ROWID;
    "#),
    // 2: the remote mirror (live nodes only; the remote side of the three-way merge).
    Sql(r#"
    CREATE TABLE remote (
        file_id       BLOB PRIMARY KEY NOT NULL,
        version_tag   BLOB NOT NULL,
        node_id       BLOB NOT NULL UNIQUE,
        parent_id     BLOB NOT NULL,
        kind          INTEGER NOT NULL,
        name          TEXT NOT NULL,
        hash          BLOB,                       -- NULL: file written without a hash
        size          INTEGER,
        mtime_ms      INTEGER,
        executable    INTEGER NOT NULL DEFAULT 0,
        content_extra TEXT                        -- unknown content keys (JSON), kept on rewrite
    ) WITHOUT ROWID;
    CREATE INDEX remote_parent ON remote(parent_id);
    "#),
    // 3: the base's local hash, local parent paths (folder listings without subtree scans),
    //    and parked items (temporary names the executor uses for swaps).
    Sql(r#"
    ALTER TABLE base ADD COLUMN local_hash BLOB;
    ALTER TABLE local_items ADD COLUMN parent_path TEXT;
    CREATE INDEX local_parent ON local_items(parent_path);
    CREATE TABLE parked (
        node_id       BLOB PRIMARY KEY NOT NULL,
        parked_path   TEXT NOT NULL,
        original_path TEXT NOT NULL
    ) WITHOUT ROWID;
    "#),
    Code(backfill_parent_paths),
];

fn backfill_parent_paths(tx: &Transaction<'_>) -> Result<()> {
    let paths: Vec<String> = {
        let mut stmt = tx.prepare("SELECT path FROM local_items WHERE parent_path IS NULL")?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?
    };
    let mut update = tx.prepare("UPDATE local_items SET parent_path = ?2 WHERE path = ?1")?;
    for path in paths {
        let parent = path.rsplit_once('/').map_or("", |(p, _)| p).to_owned();
        update.execute([&path, &parent])?;
    }
    Ok(())
}

pub fn apply(conn: &mut Connection) -> Result<()> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let supported = MIGRATIONS.len() as u32;
    if current > supported {
        return Err(StateError::TooNew {
            found: current,
            supported,
        });
    }
    for (i, migration) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        let tx = conn.transaction()?;
        match migration {
            Sql(sql) => tx.execute_batch(sql)?,
            Code(f) => f(&tx)?,
        }
        tx.pragma_update(None, "user_version", i as u32 + 1)?;
        tx.commit()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_from_v1_keeps_data() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.create_collation("hbpath", hbsync_platform::path_cmp)?;
        // A DB as the first release left it: only migration 1.
        let Sql(first) = MIGRATIONS[0] else {
            unreachable!("migration 1 is SQL")
        };
        conn.execute_batch(first)?;
        conn.pragma_update(None, "user_version", 1)?;
        conn.execute("INSERT INTO kv (key, value) VALUES ('k', 'v')", [])?;
        conn.execute(
            "INSERT INTO local_items (path, node_id, kind, size, mtime_ns, ctime_ns, volume, file_id) \
             VALUES ('a/b', x'00', 0, 0, 0, 0, 0, x'00')",
            [],
        )?;

        apply(&mut conn)?;
        let version: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        assert_eq!(version, MIGRATIONS.len() as u32);
        let value: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'k'", [], |r| r.get(0))?;
        assert_eq!(value, "v");
        let mirrored: i64 = conn.query_row("SELECT COUNT(*) FROM remote", [], |r| r.get(0))?;
        assert_eq!(mirrored, 0);
        let parent: String = conn.query_row(
            "SELECT parent_path FROM local_items WHERE path = 'a/b'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(parent, "a");
        Ok(())
    }
}
