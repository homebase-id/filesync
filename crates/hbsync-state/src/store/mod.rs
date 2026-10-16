//! Typed access to the tables, inside one transaction.
//!
//! Every node has a small integer id (`nodes`, never reused), and every table is keyed by it;
//! the UUIDs appear once. The local side is a tree like the other two: each row holds its
//! parent and its name, and paths are derived. Moving a folder rewrites one row.

use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_types::activity::Synced;
use hbsync_types::schema::{
    Keys, NodeContent, keys_from_str, tags_from_str, tags_in_keys, tags_to_string,
};
use hbsync_types::{
    ContentHash, ContentState, FileContent, FileToken, NodeId, NodeKind, NodeState, RemoteFileId,
    RemoteRef, Tags, Target, VersionTag,
};
use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::codec;
use crate::coords::Coords;
use crate::keys::{AccountKey, FolderKey};
use crate::{Result, StateError};

mod base;
mod dirty;
mod folders;
mod kv;
mod local;
mod mirror;
mod parked;
mod removed;

/// The agreed state of one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseRow {
    pub node: NodeId,
    pub state: NodeState,
    pub remote: RemoteRef,
    pub content_state: ContentState,
    /// The local spelling, when the name map says it must be recorded.
    pub local_name: Option<String>,
    /// Hash of the local bytes when the content was agreed (`state.content.hash` is the
    /// remote's representation, which may use another algorithm).
    pub local_hash: Option<hbsync_types::ContentHash>,
}

/// A download about to be put in place (written before its batch runs): the base its commit
/// writes (the node as the drive has it at the version fetched), and where it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placing {
    pub base: BaseRow,
    pub target: RelPath,
}

impl Placing {
    /// The record of `mirror`'s node put at `target` (`None` for a file the mirror lacks the
    /// content of).
    pub fn of(mirror: &MirrorRow, target: RelPath) -> Option<Placing> {
        Some(Placing {
            base: BaseRow::new(mirror.node, mirror.state()?, mirror.remote_ref()),
            target,
        })
    }
}

impl BaseRow {
    /// A base of local content holding `state`, agreed at `remote`.
    pub fn new(node: NodeId, state: NodeState, remote: RemoteRef) -> BaseRow {
        BaseRow {
            node,
            state,
            remote,
            content_state: ContentState::Local,
            local_name: None,
            local_hash: None,
        }
    }
}

/// What the last observation saw at one local path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRow {
    pub path: RelPath,
    pub node: NodeId,
    pub kind: NodeKind,
    pub fingerprint: Fingerprint,
    pub executable: Option<bool>,
    /// `None` until hashed; always `None` for folders.
    pub hash: Option<hbsync_types::ContentHash>,
    /// `None` if not known: no tags on this system or file system, or the read failed.
    pub tags: Option<Tags>,
}

/// The client's copy of one live remote node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRow {
    pub file_id: RemoteFileId,
    pub version: VersionTag,
    pub node: NodeId,
    pub parent: NodeId,
    pub name: String,
    pub kind: NodeKind,
    /// Files only.
    pub file: Option<MirrorFile>,
    /// Content keys sync carries without acting on them (JSON object: unknown keys, the
    /// creation time `ct` and the tags), written back unchanged.
    pub content_extra: Option<String>,
    /// The file's [`FileToken`], once a write or the feed has given it.
    pub token: Option<FileToken>,
    /// When this version was written, the server's time (Unix ms, the feed's header): whether
    /// the node went with a removed folder it is in depends on it (node schema, Recently
    /// deleted). `None` for this device's own write until the feed brings it back where that
    /// matters ([`Store::refresh`]); a removed node's is its removal time.
    pub changed_at: Option<i64>,
}

/// A remote node in Recently deleted: its row as it was, and when it went there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedRow {
    pub row: MirrorRow,
    /// Unix ms, the server's time of the removal. Open (`None`) for this device's own removal
    /// until the feed brings it back: never this device's clock, and the newest of all where
    /// times are compared ([`RemovedRow::time`]).
    pub removed_at: Option<i64>,
}

impl RemovedRow {
    /// The removal's time for comparing: an open one is the newest
    /// ([`hbsync_types::activity::removal_time`]).
    pub fn time(&self) -> i64 {
        hbsync_types::activity::removal_time(self.removed_at)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorFile {
    pub hash: hbsync_types::ContentHash,
    pub size: u64,
    pub mtime_ms: i64,
    pub executable: bool,
}

/// Where a sweep over the dirty set stopped (see [`Store::dirty_page`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirtyCursor(i64);

impl MirrorRow {
    /// The row of a node written as `content`, now at `at`. A file has its payload's content
    /// hash; a file without one has no [`MirrorFile`].
    pub fn from_content(
        at: RemoteRef,
        node: NodeId,
        parent: NodeId,
        kind: NodeKind,
        content: &NodeContent,
        hash: Option<ContentHash>,
    ) -> MirrorRow {
        MirrorRow {
            file_id: at.file_id,
            version: at.version,
            node,
            parent,
            name: content.name.clone(),
            kind,
            file: hash
                .filter(|_| kind == NodeKind::File)
                .map(|hash| MirrorFile {
                    hash,
                    size: content.size.unwrap_or(0),
                    mtime_ms: content.mtime.unwrap_or(0),
                    executable: content.x,
                }),
            content_extra: content.carried_json(),
            token: None,
            changed_at: None,
        }
    }

    /// The node's content as this row has it, under `name`, with the keys sync carries along.
    pub fn content(&self, name: &str) -> NodeContent {
        let mut content = match &self.file {
            Some(f) => NodeContent::file(name, f.mtime_ms, f.size, f.executable),
            None => NodeContent::folder(name),
        };
        content.unknown = self.carried_keys();
        content
    }

    /// The content keys this client carries without acting on them, to write back unchanged.
    pub fn carried_keys(&self) -> Keys {
        self.content_extra
            .as_deref()
            .map(keys_from_str)
            .unwrap_or_default()
    }

    /// The creation time the node carries, Unix ms.
    pub fn created_ms(&self) -> Option<i64> {
        NodeContent::created_ms_in(&self.carried_keys())
    }

    pub fn remote_ref(&self) -> RemoteRef {
        RemoteRef {
            file_id: self.file_id,
            version: self.version,
        }
    }

    /// This file to write at `version` (the one a plan saw), with its token.
    pub fn target_at(&self, version: VersionTag) -> Target {
        Target {
            at: RemoteRef {
                file_id: self.file_id,
                version,
            },
            token: self.token.clone(),
        }
    }

    /// This file to write at the version this row has.
    pub fn target(&self) -> Target {
        self.target_at(self.version)
    }

    /// The node state, if complete (files need their [`MirrorFile`]).
    pub fn state(&self) -> Option<NodeState> {
        let content = match (self.kind, &self.file) {
            (NodeKind::Folder, _) => None,
            (NodeKind::File, Some(f)) => Some(FileContent {
                hash: f.hash,
                size: f.size,
                mtime_ms: f.mtime_ms,
                executable: f.executable,
            }),
            (NodeKind::File, None) => return None,
        };
        Some(NodeState {
            parent: self.parent,
            name: self.name.clone(),
            kind: self.kind,
            content,
            tags: self.tags(),
        })
    }

    /// The tags, from the content keys.
    pub fn tags(&self) -> Tags {
        self.content_extra
            .as_deref()
            .map(tags_in_keys)
            .unwrap_or_default()
    }
}

/// The tables, seen through one open transaction, for one folder: the account's tables (the
/// mirror, Recently deleted, the account's kv) are shared, the others hold this folder's rows.
#[derive(Debug)]
pub struct Store<'a> {
    conn: &'a Connection,
    /// `folders.id`, which `$F` in the SQL stands for.
    folder: i64,
    /// [`Store::folders`], read once per transaction (a change through this store forgets it).
    folders: std::cell::RefCell<Option<std::rc::Rc<[FolderRow]>>>,
    /// How this folder sees the drive, once known.
    coords: std::cell::Cell<Option<Coords>>,
    /// [`Store::fan_out`]: the folder whose part of the drive a drive node is in, for the
    /// nodes walked through in this transaction (not those whose place isn't known).
    places: std::cell::RefCell<HashMap<NodeId, Place>>,
    /// The folders [`Store::fan_out`] marked a node dirty in.
    touched: std::cell::RefCell<std::collections::BTreeSet<i64>>,
    /// The ids of local folders by path, and their paths by id, found in this transaction
    /// ([`Store::local_id_at`], [`Store::folder_path`]); any local write but
    /// [`Store::put_local`] forgets them.
    local_ids: std::cell::RefCell<HashMap<String, i64>>,
    local_paths: std::cell::RefCell<HashMap<i64, RelPath>>,
    /// [`Store::prep`]: each SQL text with this store's folder put in.
    sqls: std::cell::RefCell<HashMap<Box<str>, Box<str>>>,
}

/// A folder this account syncs here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderRow {
    pub id: i64,
    /// The drive's node the folder holds: the drive's root for the main folder.
    pub top: NodeId,
    pub path: String,
    pub marker: String,
}

const BASE_SELECT: &str = "SELECT n.uuid, p.uuid, b.name, b.kind, b.hash, b.size, b.mtime_ms, \
     b.executable, b.remote_file_id, b.version_tag, b.content_state, b.local_name, b.local_hash, \
     b.tags FROM base b JOIN nodes n ON n.id = b.id AND b.folder = $F \
     JOIN nodes p ON p.id = b.parent";
/// A removed row has no `changed_at` of its own: its removal is its last write.
const REMOVED_SELECT: &str = "SELECT file_id, version_tag, node, parent, kind, name, hash, size, \
     mtime_ms, executable, content_extra, token, NULL, removed_at FROM removed";
const MIRROR_SELECT: &str = "SELECT m.file_id, m.version_tag, n.uuid, p.uuid, m.kind, m.name, \
     m.hash, m.size, m.mtime_ms, m.executable, m.content_extra, m.token, m.changed_at \
     FROM remote m JOIN nodes n ON n.id = m.id JOIN nodes p ON p.id = m.parent";
/// The columns [`RawLocal`] reads, from `local l JOIN nodes n`.
const LOCAL_COLS: &str = "l.id, n.uuid, l.kind, l.size, l.mtime_ns, l.ctime_ns, l.volume, \
     l.file_id, l.file_id_hi, l.executable, l.hash, l.parent, l.name, l.tags";
/// Columns in [`LOCAL_COLS`].
const LOCAL_COL_COUNT: usize = 14;
/// Where a drive node is, for [`Store::fan_out`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// In this folder's part of the drive.
    Folder(i64),
    /// In a part no folder here holds (the drive's top with no main folder here).
    Unheld,
}
/// The parent of a mirror node, by UUID.
const MIRROR_PARENT: &str = "SELECT p.uuid FROM remote m JOIN nodes p ON p.id = m.parent WHERE m.id = (SELECT id FROM nodes WHERE uuid = ?1)";
/// Deeper than any real tree (Windows' 32k-character paths hold about 16k levels); reaching it
/// means a broken parent chain.
pub const MAX_DEPTH: usize = 16_384;
const LOCAL_FROM: &str = "FROM local l JOIN nodes n ON n.id = l.id AND l.folder = $F";
/// A node's integer id, by UUID.
const ID_OF: &str = "(SELECT id FROM nodes WHERE uuid = ?1)";

/// A dirty node `d` with base `b` is a synced file gone locally (online-only files have no
/// local copy by design), or gone remotely: what the mass-delete guard counts.
const GONE_LOCALLY: &str = "b.kind = 0 AND b.content_state != 2 \
    AND NOT EXISTS (SELECT 1 FROM local l WHERE l.folder = $F AND l.id = d.id)";
const GONE_REMOTELY: &str = "b.kind = 0 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = d.id)";

impl<'a> Store<'a> {
    pub(crate) fn new(conn: &'a Connection, folder: i64) -> Self {
        Store {
            conn,
            folder,
            folders: Default::default(),
            coords: Default::default(),
            places: Default::default(),
            touched: Default::default(),
            local_ids: Default::default(),
            local_paths: Default::default(),
            sqls: Default::default(),
        }
    }

    /// `sql` prepared through the statement cache, `$F` standing for this store's folder.
    fn prep(&self, sql: &str) -> rusqlite::Result<rusqlite::CachedStatement<'a>> {
        if !sql.contains("$F") {
            return self.conn.prepare_cached(sql);
        }
        let mut sqls = self.sqls.borrow_mut();
        if let Some(text) = sqls.get(sql) {
            return self.conn.prepare_cached(text);
        }
        let text = sql.replace("$F", &self.folder.to_string());
        let stmt = self.conn.prepare_cached(&text);
        sqls.insert(sql.into(), text.into());
        stmt
    }

    // ---- statement helpers (all through the statement cache) ----

    /// The row a one-parameter query finds, if any.
    fn one<T>(
        &self,
        sql: &str,
        param: impl rusqlite::ToSql,
        decode: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Option<T>> {
        self.prep(sql)?
            .query_row([param], |r| Ok(decode(r)))
            .optional()?
            .transpose()
    }

    fn many<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        decode: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut stmt = self.prep(sql)?;
        let rows = stmt.query_map(params, |r| Ok(decode(r)))?;
        rows.map(|r| r?).collect()
    }

    /// Runs a one-parameter statement; whether it changed a row.
    fn change(&self, sql: &str, param: impl rusqlite::ToSql) -> Result<bool> {
        Ok(self.prep(sql)?.execute([param])? > 0)
    }

    fn count(&self, sql: &str) -> Result<u64> {
        let n: i64 = self.prep(sql)?.query_row([], |r| r.get(0))?;
        Ok(n as u64)
    }
}

fn base_row(r: &Row<'_>) -> Result<BaseRow> {
    let kind = codec::kind_from(r.get(3)?)?;
    let content = match kind {
        NodeKind::Folder => None,
        NodeKind::File => {
            let hash: Vec<u8> = r
                .get::<_, Option<Vec<u8>>>(4)?
                .ok_or_else(|| StateError::Corrupt("file without hash".into()))?;
            Some(FileContent {
                hash: codec::hash_from(&hash)?,
                size: r.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
                mtime_ms: r.get::<_, Option<i64>>(6)?.unwrap_or(0),
                executable: r.get(7)?,
            })
        }
    };
    Ok(BaseRow {
        node: codec::node(&r.get::<_, Vec<u8>>(0)?)?,
        state: NodeState {
            parent: codec::node(&r.get::<_, Vec<u8>>(1)?)?,
            name: r.get(2)?,
            kind,
            content,
            tags: r
                .get::<_, Option<String>>(13)?
                .as_deref()
                .map(tags_from_str)
                .unwrap_or_default(),
        },
        remote: RemoteRef {
            file_id: codec::remote_file(&r.get::<_, Vec<u8>>(8)?)?,
            version: codec::version(&r.get::<_, Vec<u8>>(9)?)?,
        },
        content_state: codec::content_state_from(r.get(10)?)?,
        local_name: r.get(11)?,
        local_hash: r
            .get::<_, Option<Vec<u8>>>(12)?
            .as_deref()
            .map(codec::hash_from)
            .transpose()?,
    })
}

fn removed_row(r: &Row<'_>) -> Result<RemovedRow> {
    Ok(RemovedRow {
        row: mirror_row(r)?,
        removed_at: r.get(13)?,
    })
}

fn mirror_row(r: &Row<'_>) -> Result<MirrorRow> {
    let kind = codec::kind_from(r.get(4)?)?;
    let file = match kind {
        NodeKind::Folder => None,
        NodeKind::File => Some(MirrorFile {
            hash: codec::hash_from(&r.get::<_, Vec<u8>>(6)?)?,
            size: r.get::<_, Option<i64>>(7)?.unwrap_or(0) as u64,
            mtime_ms: r.get::<_, Option<i64>>(8)?.unwrap_or(0),
            executable: r.get(9)?,
        }),
    };
    Ok(MirrorRow {
        file_id: codec::remote_file(&r.get::<_, Vec<u8>>(0)?)?,
        version: codec::version(&r.get::<_, Vec<u8>>(1)?)?,
        node: codec::node(&r.get::<_, Vec<u8>>(2)?)?,
        parent: codec::node(&r.get::<_, Vec<u8>>(3)?)?,
        name: r.get(5)?,
        kind,
        file,
        content_extra: r.get(10)?,
        token: r
            .get::<_, Option<Vec<u8>>>(11)?
            .map(|b| FileToken(b.into())),
        changed_at: r.get(12)?,
    })
}
