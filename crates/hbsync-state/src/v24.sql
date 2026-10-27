-- The schema of version 24 (0.9.0 and 0.9.1) as SQLite keeps it in a DB those made: what
-- migration 25 starts from. Frozen; the tests only.
CREATE TABLE nodes (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid BLOB NOT NULL UNIQUE
    );
CREATE TABLE folders (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        top      BLOB NOT NULL UNIQUE,
        path     TEXT NOT NULL,
        marker   TEXT NOT NULL,
        added_at INTEGER NOT NULL                     -- Unix seconds
    );
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
CREATE INDEX base_parent ON base(parent, name_key, folder);
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
        send_hash  INTEGER NOT NULL DEFAULT 0,        -- 1: a new file hashed as it is sent (hash
                                                      -- NULL by design; sync semantics §3)
        PRIMARY KEY (id, folder),
        -- A name once in a folder; and the rows that may clash with a name, by its sibling
        -- hash (one index for both: a row in costs one page fewer than two would). name_key
        -- is always sibling_hash(name), as on every side (the narrow views rely on it too).
        UNIQUE (parent, name_key, name, folder)
    ) WITHOUT ROWID;
CREATE INDEX local_file_id ON local(file_id, folder);
CREATE INDEX local_unhashed ON local(folder) WHERE hash IS NULL AND kind = 0 AND send_hash = 0;
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
CREATE TABLE dirty (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id     INTEGER NOT NULL,
        seq    INTEGER NOT NULL DEFAULT 0,            -- its mark's version, moved on by a change
                                                      -- of the drive's picture: a round clears
                                                      -- only the one it read
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
CREATE TABLE parked (
        folder        INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id            INTEGER NOT NULL,
        original_name TEXT NOT NULL,
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
CREATE TABLE forks (
        folder   INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id       INTEGER NOT NULL,
        new_node INTEGER NOT NULL,
        to_name  TEXT NOT NULL,
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
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
CREATE TABLE kv (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE folder_kv (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        key    TEXT NOT NULL,
        value  TEXT NOT NULL,
        PRIMARY KEY (folder, key)
    ) WITHOUT ROWID;
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
CREATE TABLE gone_files (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB,
        at          INTEGER,
        gone_at     INTEGER
    ) WITHOUT ROWID;
CREATE TABLE gone_suspects (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB,                             -- the version this device knew; NULL: none
        at          INTEGER NOT NULL                  -- Unix ms, this device's clock
    ) WITHOUT ROWID;
CREATE TABLE unreadable (
        file_id     BLOB PRIMARY KEY,
        version_tag BLOB NOT NULL,                    -- the version that didn't read
        node        BLOB,                             -- its uniqueId and groupId, where they read
        parent      BLOB
    ) WITHOUT ROWID;
CREATE TABLE held_deletes (
        folder INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
        id     INTEGER NOT NULL,
        PRIMARY KEY (id, folder)
    ) WITHOUT ROWID;
