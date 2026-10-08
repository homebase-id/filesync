//! Schema migrations. `PRAGMA user_version` holds the number of migrations applied. Migrations
//! are append-only: never edit one that has shipped.

use rusqlite::Connection;

use crate::{Result, StateError};

const MIGRATIONS: &[&str] = &[
    // 1: base, local observations, dirty set, key/value.
    r#"
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
