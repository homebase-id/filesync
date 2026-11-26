//! Node schema v1 on the wire (`docs/spec/node-schema-v1.md`): Homebase file headers as
//! [`RemoteHeader`]s, and the upload parts that write a [`NodeMeta`].
//!
//! Verified in odin-core `SharedSecretEncryptedFileHeader`, `ClientFileMetadata`,
//! `UploadInstructionSet`, `FileUpdateInstructionSet`, `UploadFileDescriptor` (branch
//! `payload-hashes` @ 467d0b531), and chat-kmp's `UploadDescriptorBuilder`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hbsync_remote::{ChangeRef, NodeMeta, PayloadInfo, RemoteBody, RemoteHeader, Skipped};
use hbsync_types::schema::{
    ARCHIVAL_REMOVED, FILE_TYPE_FILE, FILE_TYPE_FOLDER, NodeContent, PAYLOAD_KEY,
};
use hbsync_types::{
    ContentHash, HashAlgorithm, NodeId, NodeKind, RemoteFileId, RemoteRef, VersionTag,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::HomebaseError;
use crate::crypto::{Key, WriteOnlyKey, aes_cbc_encrypt};
use crate::drive::sealed::{KeyHeader, SealedKeyHeader, checked_sealed_len, iv16, sealed_len};
use crate::drive::share_keys::CircleKey;
use crate::drive::shared_as::{Fed, Share};

pub(crate) const CONTENT_TYPE: &str = "application/octet-stream";

/// The keys a file's key header is locked with: the shared secret, or on a private
/// (zero-knowledge) drive the drive's own key, once this computer holds it; a file another
/// identity shares with this one, the key of the circle it is shared through
/// ([`FileHeader::share_circle`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keys<'a> {
    pub shared: &'a Key,
    pub drive: Option<&'a Key>,
    pub circle: Option<&'a CircleKey>,
    /// The drive's write-only key, held only to open a file sealed to it
    /// ([`SealedKeyHeader::sealed_to_drive`]).
    pub write_only: Option<&'a WriteOnlyKey>,
}

/// A file header as query-batch and the header routes return it (the fields the client
/// reads).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileHeader {
    pub file_id: Uuid,
    pub file_state: String,
    pub file_metadata: FileMetadata,
    /// The file key and header IV under the shared secret, or the drive's key on a private
    /// drive. Not read on a tombstone: one that isn't usable (null fields, say) is taken as
    /// none, so it can't make that row unreadable.
    #[serde(default, deserialize_with = "lenient")]
    pub shared_secret_encrypted_key_header: Option<SealedKeyHeader>,
    /// To a member, the live shares the file reaches them through, by circle id: its key header
    /// is the first one's (**[verified]** odin-core `CallerShares.LiveRowsOf`,
    /// `DriveFileUtility.CreateClientFileHeader`). None on this identity's own drive, where
    /// only a folder asked about with its shares has them, read strictly ([`shared_folder`]).
    #[serde(default, deserialize_with = "share_rows")]
    pub shares: Option<Vec<ShareRow>>,
}

/// A header's `shares`, each row read on its own: one that doesn't read (not an object, say)
/// names no circle, and leaves the others.
fn share_rows<'de, D>(d: D) -> Result<Option<Vec<ShareRow>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let rows: Option<Vec<Value>> = lenient(d)?;
    Ok(rows.map(|rows| {
        rows.into_iter()
            .map(|row| serde_json::from_value(row).unwrap_or_default())
            .collect()
    }))
}

/// One of a shared file's live shares, as a member reads it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShareRow {
    /// None in a row that doesn't read ([`share_rows`]), or whose circle doesn't.
    #[serde(default, deserialize_with = "lenient")]
    pub circle_id: Option<Uuid>,
    /// When the share ends, Unix ms by the server's clock (`null` where it has no end). One
    /// that doesn't read is taken for no end: the file is kept past it, not taken off this
    /// device on a guess. The row still names its circle, and a row whose circle doesn't
    /// read still says when it ends. The server writes nothing as it
    /// passes (**[verified]** odin-core 139c0f618: `ShareRow.ExpiresAt`, a number of ms,
    /// `UnixTimeUtcConverter`; `TableDriveAclIndex.IsLive`).
    #[serde(default, deserialize_with = "lenient")]
    pub expires_at: Option<i64>,
    /// What the share gives its circle on the file ([`permissions`]): nothing where it
    /// doesn't read.
    #[serde(default, deserialize_with = "permissions")]
    pub permissions: u32,
}

/// A share row's `permissions` read strictly ([`shared_folder`]): a number, or names this
/// client knows every one of. A name it doesn't know fails the row, where [`permissions`]
/// leaves the name out: a new file would be given less than its folder's row gives.
fn all_permissions<'de, D>(d: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let sent = Value::deserialize(d)?;
    let unknown = |name: &str| D::Error::custom(format!("a permission not known: {name:?}"));
    match &sent {
        Value::Number(n) => {
            (n.as_u64().and_then(|n| u32::try_from(n).ok())).ok_or_else(|| unknown(&n.to_string()))
        }
        Value::String(names) => names.split(',').try_fold(0, |all, name| {
            let flags = permissions(json!(name.trim())).map_err(|_| unknown(name))?;
            // (No bits: a name [`permissions`] doesn't know. The server names no flag so.)
            (flags != 0)
                .then_some(all | flags)
                .ok_or_else(|| unknown(name))
        }),
        other => Err(unknown(&other.to_string())),
    }
}

/// A share row's `permissions` as odin-core's `DrivePermission` flags ([`crate::app`]'s
/// `READ`, `WRITE`, `REACT`, `COMMENT`): the server names them (`"read"`,
/// `"read, write"`, `"readWrite"` for all four; **[verified]** live, odin-core 3ace44b69, and
/// `DrivePermission.cs`), and a number is taken as it is. A name this client doesn't know
/// gives nothing.
fn permissions<'de, D>(d: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use crate::app::{COMMENT, REACT, READ, WRITE};
    const NAMED: [(&str, u32); 7] = [
        ("read", READ),
        ("write", WRITE),
        ("react", REACT),
        ("comment", COMMENT),
        ("writeReactionsAndComments", REACT | COMMENT),
        ("readWrite", READ | WRITE | REACT | COMMENT),
        ("all", READ | WRITE | REACT | COMMENT),
    ];
    Ok(match Value::deserialize(d)? {
        Value::Number(n) => n.as_u64().and_then(|n| u32::try_from(n).ok()).unwrap_or(0),
        Value::String(names) => names
            .split(',')
            .filter_map(|name| {
                let named = |(known, _): &&(&str, u32)| known.eq_ignore_ascii_case(name.trim());
                NAMED.iter().find(named).map(|(_, flags)| *flags)
            })
            .fold(0, |all, one| all | one),
        _ => 0,
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub app_data: AppData,
    pub version_tag: Uuid,
    #[serde(default)]
    pub is_encrypted: bool,
    pub payloads: Option<Vec<Payload>>,
    /// Unix ms of the last change; `None` if the header doesn't say. Read only where there
    /// is no version ([`Times::of`]).
    #[serde(default)]
    pub updated: Option<i64>,
    /// Unix ms of the write that gave it its `versionTag` ([`Times::changed_at`]).
    #[serde(default)]
    pub version_updated: Option<i64>,
    /// A shared file's key may be bound to it rather than to its file id
    /// (`CircleKey::file_key`).
    #[serde(default, deserialize_with = "lenient")]
    pub global_transit_id: Option<Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub unique_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub file_type: i32,
    pub content: Option<String>,
    /// [`ARCHIVAL_REMOVED`] for a node in Recently deleted.
    #[serde(default)]
    pub archival_status: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub key: String,
    #[serde(default)]
    pub bytes_written: u64,
    /// Base64: the payload IV.
    #[serde(default)]
    pub iv: Option<String>,
    pub hash: Option<PayloadHash>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadHash {
    pub algorithm: String,
    pub stored_hash: String,
    pub content_hash: String,
}

/// One page of `query-batch`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryBatchResponse {
    pub cursor_state: Option<String>,
    #[serde(default)]
    pub has_more_rows: bool,
    /// Raw, so one row the client can't read doesn't fail the page (and fields the client
    /// ignores are skipped, not built into a tree).
    #[serde(default)]
    pub search_results: Vec<Box<serde_json::value::RawValue>>,
    /// The rows of a [`Shape::Minimal`] page, which has no `searchResults`; absent from a
    /// server without odin-core #1967. Raw, as `searchResults`: one row the client can't read
    /// doesn't fail the page.
    pub file_refs: Option<Vec<Box<serde_json::value::RawValue>>>,
    /// The server's clock when it answered, Unix ms (odin-core `QueryBatchResponse.QueryTime`,
    /// **[verified]** `DriveQueryServiceBase.cs`).
    pub query_time: i64,
}

/// The file a feed row is about, if its `fileId` reads (a row that doesn't read as a node).
pub fn file_id_of(row: &str) -> Option<Uuid> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Id {
        file_id: Uuid,
    }
    serde_json::from_str::<Id>(row).ok().map(|id| id.file_id)
}

/// What of a row's `appData` still reads when its header doesn't.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ids {
    #[serde(default, deserialize_with = "lenient")]
    unique_id: Option<Uuid>,
    #[serde(default, deserialize_with = "lenient")]
    group_id: Option<Uuid>,
    #[serde(default, deserialize_with = "lenient")]
    file_type: Option<i32>,
}

/// What of a row still reads when its header doesn't: its state, and its `appData`'s ids.
#[derive(Debug, Default)]
pub(crate) struct RowIds {
    pub file_state: Option<String>,
    pub unique_id: Option<Uuid>,
    pub file_type: Option<i32>,
}

/// A row's [`RowIds`], read leniently: none that don't read.
pub(crate) fn row_ids(row: &Value) -> RowIds {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Row {
        #[serde(default, deserialize_with = "lenient")]
        file_state: Option<String>,
        #[serde(default, deserialize_with = "lenient")]
        file_metadata: Option<Meta>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Meta {
        #[serde(default, deserialize_with = "lenient")]
        app_data: Option<Ids>,
    }
    let Ok(row) = Row::deserialize(row) else {
        return RowIds::default();
    };
    let ids = row.file_metadata.and_then(|m| m.app_data);
    RowIds {
        file_state: row.file_state,
        unique_id: ids.as_ref().and_then(|i| i.unique_id),
        file_type: ids.and_then(|i| i.file_type),
    }
}

/// Whether `file_type` is a sync node's.
fn node_type(file_type: i32) -> bool {
    file_type == FILE_TYPE_FOLDER || file_type == FILE_TYPE_FILE
}

/// What of a feed row that isn't a usable node ([`to_remote`] said why) still reads: its file
/// and version, `uniqueId` and `groupId`, for a sync node's file type; nothing for another
/// app's file.
pub(crate) fn skipped_row(row: &str, why: String) -> Skipped {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Row {
        file_id: Uuid,
        file_metadata: Meta,
        #[serde(default, deserialize_with = "share_rows")]
        shares: Option<Vec<ShareRow>>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Meta {
        version_tag: Uuid,
        #[serde(default, deserialize_with = "lenient")]
        app_data: Option<Ids>,
    }
    let Ok(row) = serde_json::from_str::<Row>(row) else {
        return Skipped::why(why);
    };
    let Some(Ids {
        unique_id,
        group_id,
        file_type: Some(t),
    }) = row.file_metadata.app_data
    else {
        return Skipped::why(why);
    };
    if !node_type(t) {
        return Skipped::why(why);
    }
    Skipped {
        at: Some(RemoteRef {
            file_id: RemoteFileId(row.file_id),
            version: VersionTag(row.file_metadata.version_tag),
        }),
        node: unique_id.map(NodeId),
        parent: group_id.map(NodeId),
        expires: share_ends(row.shares.as_deref()),
        why,
    }
}

/// The time of the last change a feed cursor stands after: `cursorState` is odin-core's
/// `QueryBatchCursor` as JSON, `{"paging": {"time": <Unix ms>, "row": …}, …}` (**[verified]**
/// `QueryBatchCursor.cs`, `QueryBatchResponse.cs`).
pub(crate) fn cursor_time(cursor: &str) -> Option<i64> {
    let state: Value = serde_json::from_str(cursor).ok()?;
    state["paging"]["time"].as_i64()
}

/// One row of a minimal `query-batch` page (odin-core #1967 `QueryBatchFileRef`, **[verified]**):
/// the fields the client reads; the others (created, …) are skipped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileRef {
    pub file_id: Uuid,
    pub version_tag: Uuid,
    pub file_state: String,
    #[serde(default)]
    pub archival_status: i32,
    /// The parent node, as the header's: omitted when empty.
    pub group_id: Option<Uuid>,
    /// The node, as the header's `uniqueId`.
    #[serde(default)]
    pub unique_id: Option<Uuid>,
    /// Unix ms, as the header's `updated` (read as that one is); `None` if the row doesn't
    /// say.
    #[serde(default)]
    pub updated: Option<i64>,
    /// Unix ms, as the header's `versionUpdated`.
    #[serde(default)]
    pub version_updated: Option<i64>,
    /// A sync node's ([`FILE_TYPE_FOLDER`], [`FILE_TYPE_FILE`]), or another app's file on the
    /// drive.
    #[serde(default)]
    pub file_type: Option<i32>,
}

impl FileRef {
    /// What the row says of its folder's shares ([`Fed`]): none for a row that isn't a
    /// folder's.
    pub(crate) fn fed(&self) -> Option<Fed> {
        let node = (self.unique_id).filter(|_| self.file_type == Some(FILE_TYPE_FOLDER));
        Fed::of(node, self.version_tag, self.updated, self.version_updated)
    }

    /// Whether the row is another app's file, not a sync node (its type says so).
    pub(crate) fn not_a_node(&self) -> bool {
        self.file_type.is_some_and(|t| !node_type(t))
    }

    /// Whether the row is a share marker (node schema, "Sync markers").
    pub(crate) fn is_marker(&self) -> bool {
        self.file_type == Some(hbsync_types::schema::FILE_TYPE_MARKER)
    }
}

impl FileRef {
    /// As [`to_remote`] reads the same row's header ([`Times`]).
    pub(crate) fn to_ref(&self) -> ChangeRef {
        let times = Times::of(
            &self.file_state,
            self.archival_status,
            self.version_tag,
            self.version_updated,
            self.updated,
        );
        ChangeRef {
            file_id: RemoteFileId(self.file_id),
            version: VersionTag(self.version_tag),
            deleted: times.deleted,
            parent: self.group_id.map(NodeId).filter(|_| !times.deleted),
            removed: times.removed,
            changed_at: times.changed_at,
        }
    }
}

/// What a row's state, status and times say, read alike from a header and a minimal row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Times {
    pub deleted: bool,
    /// In Recently deleted: the status says whether it's removed, [`Times::changed_at`] when.
    pub removed: bool,
    /// [`RemoteHeader::changed_at`]: the header's `versionUpdated` (odin-core PR #2143,
    /// **[verified]** at 5e4cd493f: `DriveFileUtility.VersionUpdatedOf`, `FileRef`,
    /// `ClientFileMetadata`). A share withdrawn, as its member reads it, is a tombstone
    /// without a version and so without one (**[verified]** `NoLongerSharedTombstoneOf`): its
    /// time is its `updated`, the withdrawal's.
    pub changed_at: Option<i64>,
}

impl Times {
    /// A row that doesn't say when has no time: removed, its removal's time is unknown (the
    /// newest when compared: never old enough to go without a trace).
    pub(crate) fn of(
        file_state: &str,
        archival_status: i32,
        version_tag: Uuid,
        version_updated: Option<i64>,
        updated: Option<i64>,
    ) -> Times {
        let deleted = file_state == "deleted";
        Times {
            deleted,
            removed: !deleted && archival_status == ARCHIVAL_REMOVED,
            changed_at: if deleted && version_tag.is_nil() {
                updated
            } else {
                version_updated
            },
        }
    }
}

/// What each row of a feed page carries (`resultShape`, odin-core #1967).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// The full header.
    Full,
    /// Only what identifies the change: a [`FileRef`].
    Minimal,
}

/// The order of a `query-batch` listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Order {
    /// Every change, oldest first: the server holds back what changed in its last second
    /// (homebase-api-notes).
    Changes,
    /// When each file was made, oldest first: a write doesn't move a file, and nothing is
    /// held back (a file made while the pages are read may be missed by them).
    Created,
}

/// A `query-batch` body: `params`, a page of `limit` rows in `shape` and `order`, from
/// `cursor`.
pub(crate) fn query(
    params: Value,
    cursor: Option<&str>,
    limit: usize,
    shape: Shape,
    order: Order,
) -> Value {
    let mut options = json!({
        "maxRecords": limit.max(1),
        "resultShape": match shape {
            Shape::Full => "full",
            Shape::Minimal => "minimal",
        },
        "sorting": match order {
            Order::Changes => "anyChangeDate",
            Order::Created => "createdDate",
        },
        "ordering": "oldestFirst",
    });
    if let Some(cursor) = cursor.filter(|c| !c.is_empty()) {
        options["cursorState"] = json!(cursor);
    }
    json!({ "queryParams": params, "resultOptionsRequest": options })
}

/// The `query-batch` body for one page of the change feed: every change to a sync node,
/// tombstones included, oldest first. Both shapes page alike: the same rows and cursor.
pub fn feed_query(cursor: Option<&str>, limit: usize, shape: Shape) -> Value {
    let params = json!({ "fileType": [FILE_TYPE_FOLDER, FILE_TYPE_FILE] });
    query(params, cursor, limit, shape, Order::Changes)
}

/// A folder of this identity's own drive, as far as its shares go ([`shared_folder`]).
#[derive(Debug)]
pub(crate) struct SharedFolder {
    pub at: RemoteRef,
    /// Deleted for good: its tombstone still lists the rows it had (**[verified]** live).
    pub deleted: bool,
    /// The live shares that read: its manager is sent the live rows alone (**[verified]**
    /// odin-core `DriveStorageServiceBase.AttachSharesAsync`), and one without the right to
    /// read is a withdrawn one.
    pub shares: Vec<Share>,
    /// The rows that don't read: not given to what is added to the folder.
    pub unread: usize,
}

/// A folder's row of [`shares_query`], read for its shares alone, each row strictly: one
/// whose circle, permissions or end doesn't read is counted, not guessed at (a share with
/// an end that doesn't read would be given without one). A row with no `shares` at all has
/// none: the server sets them only on a Selective Share drive, for a caller that manages its
/// shares (**[verified]** odin-core `DriveStorageServiceBase.AttachSharesAsync`), so a login
/// that isn't the owning app's reads every folder as unshared, without an error to tell it
/// by (this client's is that app's: **[verified]** live).
pub(crate) fn shared_folder(row: &str) -> Result<SharedFolder, serde_json::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Row {
        file_id: Uuid,
        file_state: String,
        file_metadata: Meta,
        #[serde(default)]
        shares: Option<Vec<Value>>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Meta {
        version_tag: Uuid,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Shared {
        circle_id: Uuid,
        #[serde(deserialize_with = "all_permissions")]
        permissions: u32,
        #[serde(default)]
        expires_at: Option<i64>,
    }
    let row: Row = serde_json::from_str(row)?;
    let rows = row.shares.unwrap_or_default();
    let read: Vec<Shared> = (rows.iter())
        .filter_map(|row| Shared::deserialize(row).ok())
        .collect();
    let unread = rows.len() - read.len();
    let shares = (read.into_iter())
        .filter(|share| share.permissions & crate::app::READ != 0)
        .map(|share| Share {
            circle: share.circle_id,
            permissions: share.permissions,
            expires_at: share.expires_at,
        })
        .collect();
    Ok(SharedFolder {
        at: RemoteRef {
            file_id: RemoteFileId(row.file_id),
            version: VersionTag(row.file_metadata.version_tag),
        },
        deleted: row.file_state == "deleted",
        shares,
        unread,
    })
}

/// The `query-batch` body for the folder `node` alone, with its live shares (`includeShares`:
/// **[verified]** odin-core `QueryBatchResultOptionsRequest`, chat-kmp `FilesyncSharing.inherit`).
pub(crate) fn shares_query(node: NodeId) -> Value {
    json!({
        "queryParams": { "clientUniqueIdAtLeastOne": [node.0], "fileType": [FILE_TYPE_FOLDER] },
        "resultOptionsRequest": { "maxRecords": 1, "includeShares": true },
    })
}

/// The `query-batch` body for the sync node `node` alone, whole, with its live shares.
pub(crate) fn node_query(node: NodeId) -> Value {
    json!({
        "queryParams": {
            "clientUniqueIdAtLeastOne": [node.0],
            "fileType": [FILE_TYPE_FOLDER, FILE_TYPE_FILE],
        },
        "resultOptionsRequest": { "maxRecords": 1, "includeShares": true },
    })
}

/// The `query-batch` body for a page of the active sync nodes in `folders` (their
/// `groupId`), whole, with their live shares: what a move reads of what is in the folder
/// moved ([`HomebaseRemote::moving`](super::HomebaseRemote)). By when they were made: nothing
/// is held back.
pub(crate) fn nodes_in_query(folders: &[NodeId], cursor: Option<&str>, limit: usize) -> Value {
    let params = json!({
        "groupId": folders.iter().map(|f| f.0).collect::<Vec<_>>(),
        "fileState": ["active"],
        "fileType": [FILE_TYPE_FOLDER, FILE_TYPE_FILE],
    });
    let mut body = query(params, cursor, limit, Shape::Full, Order::Created);
    body["resultOptionsRequest"]["includeShares"] = json!(true);
    body
}

/// The `query-batch` body for a page of the active files in `folders` (their `groupId`), of
/// any type, minimal rows: what [`HomebaseRemote::holding`](super::HomebaseRemote) asks.
/// odin-core filters on `groupId`, `fileState` and `archivalStatus` (**[verified]**
/// `QueryBatch.cs:130-142, 351`); live is any status but removed, so that is read per row.
pub(crate) fn children_query(folders: &[NodeId], cursor: Option<&str>, limit: usize) -> Value {
    let params = json!({
        "groupId": folders.iter().map(|f| f.0).collect::<Vec<_>>(),
        "fileState": ["active"],
    });
    query(params, cursor, limit, Shape::Minimal, Order::Changes)
}

/// A header as a [`RemoteHeader`], decrypted with `keys`; `Err` says why it isn't a usable
/// sync node.
pub(crate) fn to_remote(header: FileHeader, keys: Keys<'_>) -> Result<RemoteHeader, String> {
    let id = header.file_id;
    let expires = header.share_ends();
    let meta = &header.file_metadata;
    let kind = match meta.app_data.file_type {
        FILE_TYPE_FOLDER => NodeKind::Folder,
        FILE_TYPE_FILE => NodeKind::File,
        other => return Err(format!("file {id}: file type {other} isn't a sync node")),
    };
    let group = meta.app_data.group_id.map(NodeId);
    let times = Times::of(
        &header.file_state,
        meta.app_data.archival_status,
        meta.version_tag,
        meta.version_updated,
        meta.updated,
    );
    let (parent, body, token) = match header.file_state.as_str() {
        // A tombstone always comes through: dropping it would keep a deleted node alive. Its
        // parent isn't used, only its file id.
        "deleted" => (group.unwrap_or(NodeId::ROOT), RemoteBody::Deleted, None),
        "active" => {
            let parent = group.ok_or_else(|| format!("file {id}: no parent (groupId)"))?;
            let node = meta
                .app_data
                .unique_id
                .ok_or_else(|| format!("file {id}: no node id (uniqueId)"))?;
            let key = header.key(keys).map_err(|e| format!("file {id}: {e}"))?;
            let text = key
                .open_text(meta.app_data.content.as_deref().unwrap_or_default())
                .map_err(|e| format!("file {id}: content: {e}"))?;
            // A newer schema's node is read-only here (node schema, "v"): unreadable, as a
            // newer writer's version is (§8a), so this client never renames, moves, retags or
            // writes over it, and loses none of its newer fields. Its `v` is looked at alone
            // only where the content doesn't read (a newer one may change a field's type).
            let newer = |v: u64| format!("file {id}: node schema v{v}, newer than this client's");
            let content = NodeContent::from_json(&text).map_err(|e| {
                match NodeContent::newer_version(&text) {
                    Some(v) => newer(v),
                    None => format!("file {id}: content isn't node schema v1: {e}"),
                }
            })?;
            if !content.is_writable() {
                return Err(newer(u64::from(content.v)));
            }
            let payload = match kind {
                NodeKind::Folder => None,
                NodeKind::File => {
                    // The stored size of an encrypted payload isn't the file's: only `size`
                    // tells it.
                    let size = content
                        .size
                        .ok_or_else(|| format!("file {id}: no size (node schema v1)"))?;
                    let p = meta
                        .sync_payload()
                        .ok_or_else(|| format!("file {id}: no payload"))?;
                    // Bytes that can't be the file's would fail every download, in full.
                    if checked_sealed_len(size) != Some(p.bytes_written) {
                        return Err(format!(
                            "file {id}: size {size} doesn't fit its payload's {} bytes",
                            p.bytes_written
                        ));
                    }
                    Some(payload_info(p, &key).map_err(|e| format!("file {id}: {e}"))?)
                }
            };
            let body = RemoteBody::Live {
                node: NodeId(node),
                content,
                payload,
            };
            // The key opened: the header it came in is the file's token, one under the shared
            // secret, as every token is (one locked with the drive's key, or sealed to its
            // write-only key, is sealed so here).
            let token = match header.shared_secret_encrypted_key_header.as_ref() {
                // Another identity's file: nothing is written to it from here.
                Some(sealed) if sealed.locked_for_share() => None,
                Some(sealed) if sealed.locked_with_drive_key() || sealed.sealed_to_drive() => {
                    Some(key.token(keys.shared))
                }
                sealed => sealed.and_then(SealedKeyHeader::to_token),
            };
            (parent, body, token)
        }
        other => return Err(format!("file {id}: file state {other:?}")),
    };
    // (A tombstone is nobody's to change; an identity's own drive has no rows.)
    let writable = header.shared_for_writing() && matches!(body, RemoteBody::Live { .. });
    Ok(RemoteHeader {
        file_id: RemoteFileId(id),
        version: VersionTag(meta.version_tag),
        parent,
        kind,
        body,
        removed: times.removed,
        changed_at: times.changed_at,
        token,
        expires,
        writable,
    })
}

/// The latest end of the share `rows` of a header, none where one of them has no end (or
/// there are none): [`FileHeader::share_ends`].
fn share_ends(rows: Option<&[ShareRow]>) -> Option<i64> {
    let mut rows = rows?.iter();
    rows.try_fold(None, |latest: Option<i64>, row| {
        Some(latest.max(Some(row.expires_at?)))
    })?
}

/// `T` if the value is one, else `None`.
pub(crate) fn lenient<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_value(Value::deserialize(d)?).ok())
}

impl FileHeader {
    /// What the header says of its folder's shares ([`Fed`]), as its slim row does
    /// ([`FileRef::fed`]): none for one that isn't a folder's.
    pub(crate) fn fed(&self) -> Option<Fed> {
        let (meta, app) = (&self.file_metadata, &self.file_metadata.app_data);
        let node = (app.unique_id).filter(|_| app.file_type == FILE_TYPE_FOLDER);
        Fed::of(node, meta.version_tag, meta.updated, meta.version_updated)
    }

    /// The file key and header IV, opened with `keys`. Every node is encrypted.
    pub(crate) fn key(&self, keys: Keys<'_>) -> Result<KeyHeader, HomebaseError> {
        if !self.file_metadata.is_encrypted {
            return Err(HomebaseError::Crypto("not encrypted".into()));
        }
        let sealed = self
            .shared_secret_encrypted_key_header
            .as_ref()
            .ok_or_else(|| HomebaseError::Crypto("no key header".into()))?;
        if sealed.sealed_to_drive() {
            let key = keys.write_only.ok_or_else(|| {
                HomebaseError::Crypto(
                    "sealed to the drive's write-only key, which isn't here".into(),
                )
            })?;
            return KeyHeader::open_sealed(sealed, key);
        }
        if sealed.locked_for_share() {
            // The circle is bound into the locked key: another circle's doesn't open it.
            let key = keys.circle.ok_or_else(|| {
                HomebaseError::Crypto("shared through a circle whose key isn't here".into())
            })?;
            return KeyHeader::open_share(
                sealed,
                key,
                self.file_id,
                self.file_metadata.global_transit_id,
            );
        }
        let key = match sealed.locked_with_drive_key() {
            true => keys.drive.ok_or_else(|| {
                HomebaseError::Crypto("locked with the drive's key, which isn't here".into())
            })?,
            false => keys.shared,
        };
        KeyHeader::open(sealed, key)
    }

    /// A live node's, shared with this identity: [`to_remote`] opens it with the key of its
    /// [`FileHeader::share_circle`].
    pub(crate) fn needs_circle_key(&self) -> bool {
        self.live_with(SealedKeyHeader::locked_for_share)
    }

    /// When a member stops reading the file ([`RemoteHeader::expires`]): the latest end of
    /// the live shares it reaches them through, none where one of them has no end (or on
    /// this identity's own drive, which has no rows).
    pub(crate) fn share_ends(&self) -> Option<i64> {
        share_ends(self.shares.as_deref())
    }

    /// Whether a share gives this identity `Write` on the file.
    pub(crate) fn shared_for_writing(&self) -> bool {
        let mut rows = self.shares.iter().flatten();
        rows.any(|row| row.permissions & crate::app::WRITE != 0)
    }

    /// The circle a shared file's key header is locked for: the one its first share names.
    pub(crate) fn share_circle(&self) -> Option<Uuid> {
        self.shares.as_ref()?.first()?.circle_id
    }

    /// A live sync node's header (not a tombstone, nor another app's file).
    fn live_node(&self) -> bool {
        self.file_state == "active" && node_type(self.file_metadata.app_data.file_type)
    }

    /// Its key header is locked with the drive's key (a private drive's file).
    pub(crate) fn locked_with_drive_key(&self) -> bool {
        self.shared_secret_encrypted_key_header
            .as_ref()
            .is_some_and(SealedKeyHeader::locked_with_drive_key)
    }

    /// A live node's, locked with the drive's key: [`to_remote`] opens it with that.
    pub(crate) fn needs_drive_key(&self) -> bool {
        self.live_with(SealedKeyHeader::locked_with_drive_key)
    }

    /// A live node's, sealed to the drive's write-only key: [`to_remote`] opens it with that.
    pub(crate) fn needs_write_only_key(&self) -> bool {
        self.live_with(SealedKeyHeader::sealed_to_drive)
    }

    /// A live node's, whose key header is of the `kind` asked.
    fn live_with(&self, kind: fn(&SealedKeyHeader) -> bool) -> bool {
        self.live_node()
            && self
                .shared_secret_encrypted_key_header
                .as_ref()
                .is_some_and(kind)
    }
}

impl FileMetadata {
    /// The `sync_data` payload.
    pub fn sync_payload(&self) -> Option<&Payload> {
        self.payloads
            .iter()
            .flatten()
            .find(|p| p.key == PAYLOAD_KEY)
    }
}

/// The IV of a payload.
pub(crate) fn payload_iv(p: &Payload) -> Result<[u8; 16], String> {
    let iv = p.iv.as_deref().ok_or("no payload IV")?;
    iv16(iv).ok_or_else(|| format!("payload IV {iv:?}"))
}

/// The payload's size and content hash, sealed with the file `key` under the payload IV.
fn payload_info(p: &Payload, key: &KeyHeader) -> Result<PayloadInfo, String> {
    let h = p.hash.as_ref().ok_or("no content hash")?;
    let algorithm: HashAlgorithm = h
        .algorithm
        .parse()
        .map_err(|_| format!("hash algorithm {:?}", h.algorithm))?;
    let hash = key
        .open_hash(&payload_iv(p)?, algorithm, &h.content_hash)
        .map_err(|e| format!("content hash: {e}"))?;
    Ok(PayloadInfo {
        hash,
        bytes: p.bytes_written,
    })
}

/// What an upload says about the `sync_data` payload it sends: its hashes follow it, in its
/// `payloadHash` part (odin-core #1935).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadDescriptor {
    pub algorithm: HashAlgorithm,
    /// The payload IV.
    pub iv: [u8; 16],
}

/// What a deferred hash's values are in the descriptor: every byte this, as many as the value
/// has.
const DEFERRED: u8 = 0x11;

/// A hash's length.
const HASH_LEN: usize = 32;

/// A sealed content hash's length: a hash, AES-CBC with its padding.
const SEALED_HASH_LEN: usize = sealed_len(HASH_LEN as u64) as usize;

/// The `payloadHash` part: the values of a deferred descriptor, for a payload encrypted with
/// `key` under `iv` whose plaintext hashes to `content` and ciphertext to `stored`.
pub(crate) fn payload_hash(
    key: &KeyHeader,
    iv: &[u8; 16],
    content: &ContentHash,
    stored: &ContentHash,
) -> Vec<u8> {
    hash_part(&stored.to_base64(), &key.seal_hash(iv, content))
}

/// A `payloadHash` part no bytes hash to: the server refuses the payload and stores nothing
/// (`payloadHashMismatch`).
pub(crate) fn refused_payload_hash() -> Vec<u8> {
    hash_part(
        &STANDARD.encode([0; HASH_LEN]),
        &STANDARD.encode([0; SEALED_HASH_LEN]),
    )
}

fn hash_part(stored: &str, content: &str) -> Vec<u8> {
    json!({ "storedHash": stored, "contentHash": content })
        .to_string()
        .into_bytes()
}

/// Every `payloadHash` part's length: base64 of values of fixed length.
pub(crate) const PAYLOAD_HASH_LEN: usize = r#"{"storedHash":"","contentHash":""}"#.len()
    + HASH_LEN.div_ceil(3) * 4
    + SEALED_HASH_LEN.div_ceil(3) * 4;

/// The `instructions` part of a create (`update: false`) or update. With `payload`, the
/// `sync_data` payload is (over)written.
pub(crate) fn instructions(
    transfer_iv: &[u8; 16],
    payload: Option<&PayloadDescriptor>,
    update: bool,
) -> Value {
    let descriptors: Vec<Value> = payload
        .map(|p| {
            let mut d = json!({
                "payloadKey": PAYLOAD_KEY,
                "contentType": CONTENT_TYPE,
                "iv": STANDARD.encode(p.iv),
                // Deferred: the values follow the payload.
                "hash": {
                    "algorithm": p.algorithm.as_str(),
                    "storedHash": STANDARD.encode([DEFERRED; HASH_LEN]),
                    "contentHash": STANDARD.encode([DEFERRED; SEALED_HASH_LEN]),
                },
            });
            if update {
                d["payloadUpdateOperationType"] = json!("appendOrOverwrite");
            }
            d
        })
        .into_iter()
        .collect();
    let mut instructions = json!({
        "transferIv": STANDARD.encode(transfer_iv),
        "manifest": { "payloadDescriptors": descriptors },
    });
    if update {
        // Without `locale` the server fails with 500 (`FileSystemUpdateWriterBase`).
        instructions["locale"] = json!("local");
        instructions["recipients"] = json!([]);
    }
    instructions
}

/// The `metadata` part: the header JSON, AES-CBC under the shared secret with `transfer_iv`.
/// Homebase replaces the whole header on update, so every field is always sent: node id,
/// parent, kind, content, and the owner-only ACL. The file is encrypted: its `key` sealed with
/// the shared secret (locked with the drive's key on a private drive), the content with the key.
/// A new file in a shared folder goes up `shared` as the folder is ([`NewShares`]): the
/// server writes the header and its shares as one (**[verified]** odin-core
/// `StandardFileStreamWriter.ProcessNewFileUpload`), and refuses shares on any other write.
pub(crate) fn metadata(
    meta: &NodeMeta,
    version: Option<VersionTag>,
    keys: Keys<'_>,
    transfer_iv: &[u8; 16],
    key: &KeyHeader,
    shared: Option<&NewShares>,
) -> Vec<u8> {
    let key_header = match keys.drive {
        Some(drive_key) => key.lock(drive_key),
        None => key.seal(keys.shared, transfer_iv),
    };
    // Owner-only, and not sent on: every write of the owner's own computers (a share lifts
    // the group, on the server).
    let mut plain = descriptor(meta, version, key, key_header, "owner", false);
    if let Some(shared) = shared {
        shared.sent_with(&mut plain);
    }
    aes_cbc_encrypt(keys.shared, transfer_iv, plain.to_string().as_bytes())
}

/// The shares a new file goes up with ([`share_of_new`]), and the global transit id picked
/// for it, which its keys are locked to: it has no file id yet.
#[derive(Debug)]
pub(crate) struct NewShares {
    pub global_transit_id: Uuid,
    pub shares: Vec<Value>,
}

impl NewShares {
    /// Named in a new file's descriptor `plain`.
    fn sent_with(&self, plain: &mut Value) {
        plain["shares"] = json!(self.shares);
        plain["globalTransitId"] = json!(self.global_transit_id);
    }
}

/// The identity's transient temp drive (its alias and its type): where its server stages
/// what it sends on to another identity's drive (**[verified]** odin-core
/// `SystemDriveConstants.TransientTempDrive`).
pub(crate) const TRANSIENT_DRIVE: Uuid = Uuid::from_u128(0x90f5e74a_b7f9_efda_0ac2_98373a32ad8c);

/// The `instructions` part of a change to a file another identity (`owner`) shares with this
/// one, sent through this identity's own server (`locale: "peer"`: nothing is stored here).
pub(crate) fn member_update_instructions(
    transfer_iv: &[u8; 16],
    payload: Option<&PayloadDescriptor>,
    owner: &str,
) -> Value {
    let mut instructions = instructions(transfer_iv, payload, true);
    instructions["locale"] = json!("peer");
    instructions["recipients"] = json!([owner]);
    instructions
}

/// The `metadata` part of that change: the header as [`metadata`] sends it, but for a file
/// that is shared, to be sent on (`allowDistribution`), with `key_header` as this identity
/// may send it ([`KeyHeader::kept`], [`KeyHeader::sealed_to`]). Sealed with this identity's
/// own shared secret: its own server opens it.
pub(crate) fn member_update(
    meta: &NodeMeta,
    version: VersionTag,
    shared_secret: &Key,
    transfer_iv: &[u8; 16],
    key: &KeyHeader,
    key_header: Value,
) -> Vec<u8> {
    // As a file that is shared (**[verified]** chat-kmp `FilesyncMemberWrites.replace`).
    let plain = descriptor(meta, Some(version), key, key_header, "connected", true);
    aes_cbc_encrypt(shared_secret, transfer_iv, plain.to_string().as_bytes())
}

/// The `instructions` part of a new file for `owner`'s `drive`, added to a folder they share
/// with this identity: staged on this identity's transient temp drive and sent on, not kept
/// here (**[verified]** live, odin-core 3ace44b69; chat-kmp `FilesyncMemberWrites.add`).
pub(crate) fn deposit_instructions(
    transfer_iv: &[u8; 16],
    payload: Option<&PayloadDescriptor>,
    owner: &str,
    drive: &super::TargetDrive,
) -> Value {
    let mut instructions = instructions(transfer_iv, payload, false);
    instructions["storageOptions"] =
        json!({ "drive": { "alias": TRANSIENT_DRIVE, "type": TRANSIENT_DRIVE } });
    instructions["transitOptions"] = json!({
        "recipients": [owner],
        "isTransient": true,
        "remoteTargetDrive": { "alias": drive.alias, "type": drive.drive_type },
    });
    instructions
}

/// One share of a new file, as its adder sends it along: as the folder it goes into is shared
/// with `circle` (the same `permissions` and end), with the file's key `locked` under that
/// circle's key ([`CircleKey::lock_file_key`]). The file has no id yet.
pub(crate) fn share_of_new(
    circle: Uuid,
    permissions: u32,
    expires_at: Option<i64>,
    locked: &[u8],
) -> Value {
    share_of(Uuid::nil(), circle, permissions, expires_at, locked)
}

/// One share of the file `file_id`, as `PUT /drives/{d}/shares` takes it (its body:
/// `{"shares": [...]}`, 100 at most, all or none: **[verified]** odin-core
/// `V2DriveSharesController`, chat-kmp `DriveShareProvider`), with the file's key `locked`
/// under the circle's key ([`CircleKey::lock_key_of`]); a new file's names no file.
pub(crate) fn share_of(
    file_id: Uuid,
    circle: Uuid,
    permissions: u32,
    expires_at: Option<i64>,
    locked: &[u8],
) -> Value {
    json!({
        "fileId": file_id,
        "circleId": circle,
        "permissions": permissions,
        "expiresAt": expires_at,
        "lockedFileKey": STANDARD.encode(locked),
    })
}

/// The `metadata` part of that new file: its header, the key sealed to the owner's drive
/// (`key_header`: [`KeyHeader::sealed_to`]), the global transit id picked for it and its
/// `shares` ([`share_of_new`]). Owner-only as sent: its shares lift it there.
pub(crate) fn deposit(
    meta: &NodeMeta,
    shared_secret: &Key,
    transfer_iv: &[u8; 16],
    key: &KeyHeader,
    key_header: Value,
    shares: &NewShares,
) -> Vec<u8> {
    let mut plain = descriptor(meta, None, key, key_header, "owner", true);
    shares.sent_with(&mut plain);
    aes_cbc_encrypt(shared_secret, transfer_iv, plain.to_string().as_bytes())
}

/// A header as an upload sends it, before it is sealed: `key_header` as its writer locks the
/// file `key`, the content under that key; who may read the file (`group`: its ACL's security
/// group), and whether the server may send it on to another identity (`sent_on`).
fn descriptor(
    meta: &NodeMeta,
    version: Option<VersionTag>,
    key: &KeyHeader,
    key_header: Value,
    group: &str,
    sent_on: bool,
) -> Value {
    let file_type = match meta.kind {
        NodeKind::Folder => FILE_TYPE_FOLDER,
        NodeKind::File => FILE_TYPE_FILE,
    };
    json!({
        "encryptedKeyHeader": key_header,
        "fileMetadata": {
            "isEncrypted": true,
            "allowDistribution": sent_on,
            "accessControlList": { "requiredSecurityGroup": group },
            "appData": {
                "uniqueId": meta.node.0,
                "groupId": meta.parent.0,
                "fileType": file_type,
                "dataType": 0,
                "content": key.seal_text(&meta.content.to_json()),
                // Homebase sets a missing one to 0: a write that left it out would restore.
                "archivalStatus": if meta.removed { ARCHIVAL_REMOVED } else { 0 },
            },
            "versionTag": version.map(|v| v.0),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aes_cbc_decrypt;
    use crate::drive::sealed;

    /// The shared secret of the tests.
    const SS: Key = [3; 16];
    const SHARED: Keys<'static> = Keys {
        shared: &SS,
        drive: None,
        circle: None,
        write_only: None,
    };

    fn fixture_value(name: &str) -> Value {
        let path = format!(
            "{}/../../docs/spec/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    fn fixture(name: &str) -> FileHeader {
        serde_json::from_value(fixture_value(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
    }

    /// Fixture `name` with each key header that doesn't parse (taken as none, through the
    /// same lenient decoding as rows), marked `encrypted` or not.
    fn with_unusable_key_headers(name: &str, encrypted: bool) -> Vec<(Value, FileHeader)> {
        let nulls = json!({ "iv": null, "encryptedAesKey": null, "encryptionVersion": null });
        [nulls, json!(null), json!("junk")]
            .into_iter()
            .map(|key_header| {
                let mut row = fixture_value(name);
                row["fileMetadata"]["isEncrypted"] = json!(encrypted);
                row["sharedSecretEncryptedKeyHeader"] = key_header.clone();
                let row: FileHeader =
                    serde_json::from_value(row).unwrap_or_else(|e| panic!("{name}: {e}"));
                assert!(row.shared_secret_encrypted_key_header.is_none());
                (key_header, row)
            })
            .collect()
    }

    /// `header` (a spec fixture, plain) as Homebase stores it: content, payload IV and content
    /// hash sealed with the file key `kh`, the key under the shared secret.
    fn as_stored_with(
        mut header: FileHeader,
        kh: &KeyHeader,
    ) -> Result<FileHeader, Box<dyn std::error::Error>> {
        let meta = &mut header.file_metadata;
        meta.is_encrypted = true;
        if let Some(plain) = &meta.app_data.content {
            meta.app_data.content = Some(kh.seal_text(plain));
        }
        for payload in meta.payloads.iter_mut().flatten() {
            payload.bytes_written = sealed_len(payload.bytes_written);
            let payload_iv = sealed::strong()?;
            payload.iv = Some(STANDARD.encode(payload_iv));
            if let Some(hash) = payload.hash.as_mut() {
                let plain = ContentHash::from_base64(HashAlgorithm::Blake3, &hash.content_hash)?;
                hash.content_hash = kh.seal_hash(&payload_iv, &plain);
            }
        }
        header.shared_secret_encrypted_key_header =
            Some(serde_json::from_value(kh.seal(&SS, &sealed::strong()?))?);
        Ok(header)
    }

    fn as_stored(header: FileHeader) -> Result<FileHeader, Box<dyn std::error::Error>> {
        as_stored_with(header, &KeyHeader::random()?)
    }

    type R = Result<(), Box<dyn std::error::Error>>;

    /// A private drive's file: its key header locked with the drive's key opens with it (and
    /// with nothing else), and its token is sealed under the shared secret as every token is;
    /// an upload to it locks the key header with the drive's key.
    #[test]
    fn a_private_drives_file_opens_with_the_drive_key() -> R {
        let (kh, drive_key) = (KeyHeader::random()?, [9; 16]);
        let mut header = as_stored_with(fixture("file-node.json"), &kh)?;
        header.shared_secret_encrypted_key_header =
            Some(serde_json::from_value(kh.lock(&drive_key))?);
        let private = Keys {
            shared: &SS,
            drive: Some(&drive_key),
            circle: None,
            write_only: None,
        };
        let file = to_remote(header.clone(), private)?;
        let token = file.token.as_ref().ok_or("token")?;
        let sealed = SealedKeyHeader::from_token(token).ok_or("not a token")?;
        assert_eq!(KeyHeader::open(&sealed, &SS)?, kh);
        let why = to_remote(header.clone(), SHARED).err().ok_or("opened")?;
        assert!(why.contains("drive's key"), "{why}");
        let wrong = Keys {
            shared: &SS,
            drive: Some(&[8; 16]),
            circle: None,
            write_only: None,
        };
        assert!(to_remote(header, wrong).is_err());

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            content: NodeContent::folder("Documents"),
            removed: false,
        };
        let transfer_iv = sealed::strong()?;
        let sent = metadata(&meta, None, private, &transfer_iv, &kh, None);
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sent)?)?;
        let key: SealedKeyHeader = serde_json::from_value(plain["encryptedKeyHeader"].clone())?;
        assert!(key.locked_with_drive_key());
        assert_eq!(KeyHeader::open(&key, &drive_key)?, kh);
        Ok(())
    }

    /// A file someone added to a folder shared with them, as the owner's own computer reads it:
    /// its key header is sealed to the drive's write-only key (odin-core's vector), which opens
    /// it; its token is under the shared secret, as any file's, so the next write of it here
    /// names its key. Without that key it doesn't open, and says why. A tombstone with such a
    /// key header needs no key.
    #[test]
    fn a_file_a_member_added_opens_with_the_drives_write_only_key() -> R {
        use crate::crypto::WriteOnlyKey;
        use crate::drive::share_keys::tests as v;
        let write_only = WriteOnlyKey::from_pkcs8(&v::bytes(v::WRITE_ONLY_PKCS8))?;
        let (kh, sent) = v::sealed_key_header();
        let mut header = as_stored_with(fixture("file-node.json"), &kh)?;
        header.shared_secret_encrypted_key_header = Some(serde_json::from_value(sent)?);
        assert!(header.needs_write_only_key() && !header.needs_drive_key());
        let keys = Keys {
            write_only: Some(&write_only),
            ..SHARED
        };
        assert_eq!(header.key(keys)?, kh);
        let file = to_remote(header.clone(), keys)?;
        let plain = to_remote(as_stored_with(fixture("file-node.json"), &kh)?, SHARED)?;
        assert_eq!(file.body, plain.body);
        let token = file.token.as_ref().ok_or("token")?;
        let sealed = SealedKeyHeader::from_token(token).ok_or("not a token")?;
        assert_eq!(KeyHeader::open(&sealed, &SS)?, kh);

        let why = to_remote(header.clone(), SHARED).err().ok_or("opened")?;
        assert!(why.contains("write-only key"), "{why}");
        // The drive's key doesn't open it.
        let with_drive = Keys {
            drive: Some(&[9; 16]),
            ..SHARED
        };
        assert!(to_remote(header.clone(), with_drive).is_err());

        header.file_state = "deleted".into();
        assert!(!header.needs_write_only_key());
        assert_eq!(to_remote(header, SHARED)?.body, RemoteBody::Deleted);
        Ok(())
    }

    /// A file another identity shares, as their server sends it to a member: its key header is
    /// the file's IV and the file key locked with the circle's key (odin-core's golden vector),
    /// which opens it for the circle the first share names and the file it is bound to (its id,
    /// or its global transit id), and with nothing else. It carries no token: nothing is written
    /// to it from here.
    #[test]
    fn a_shared_file_opens_with_its_circles_key() -> R {
        use crate::crypto::WriteOnlyKey;
        use crate::drive::share_keys::tests as v;
        let write_only = WriteOnlyKey::from_pkcs8(&v::bytes(v::WRITE_ONLY_PKCS8))?;
        let circle = v::unseal_with(&write_only, v::MEMBER)?;
        let kh = KeyHeader {
            iv: sealed::strong()?,
            key: v::bytes(v::FILE_KEY).try_into().map_err(|_| "a key")?,
        };
        let shared =
            |locked: &str, kind: Value| -> Result<FileHeader, Box<dyn std::error::Error>> {
                let mut header = as_stored_with(fixture("file-node.json"), &kh)?;
                header.file_id = v::id(v::FILE);
                header.file_metadata.global_transit_id = Some(v::id(v::GTID));
                header.shares = Some(vec![ShareRow {
                    circle_id: Some(v::id(v::CIRCLE)),
                    ..ShareRow::default()
                }]);
                header.shared_secret_encrypted_key_header = Some(serde_json::from_value(json!({
                    "encryptionVersion": 1, "type": kind,
                    "iv": STANDARD.encode(kh.iv), "encryptedAesKey": locked,
                }))?);
                Ok(header)
            };
        let keys = Keys {
            shared: &SS,
            drive: None,
            circle: Some(&circle),
            write_only: None,
        };
        for header in [
            shared(v::LOCKED, json!("share"))?,
            shared(v::LOCKED, json!(14))?,
            shared(v::LOCKED_BY_GTID, json!("share"))?,
        ] {
            assert!(header.needs_circle_key());
            assert_eq!(header.share_circle(), Some(v::id(v::CIRCLE)));
            assert_eq!(header.key(keys)?, kh);
            let file = to_remote(header.clone(), keys)?;
            assert_eq!(file.state().ok_or("state")?.name, "Résumé.txt");
            assert!(file.token.is_none());
            // Shared without an end; with one, the header read says until when.
            assert_eq!(file.expires, None);
            let mut until = header.clone();
            for row in until.shares.iter_mut().flatten() {
                row.expires_at = Some(1_700_000_000_000);
            }
            assert_eq!(to_remote(until, keys)?.expires, Some(1_700_000_000_000));
            // Shared for reading: not this identity's to change. A share that gives `Write`
            // makes it so, while the file is live: a tombstone is nobody's to change.
            assert!(!file.writable);
            let mut to_edit = header.clone();
            for row in to_edit.shares.iter_mut().flatten() {
                row.permissions = crate::app::WRITE | 1;
            }
            assert!(to_remote(to_edit.clone(), keys)?.writable);
            to_edit.file_state = "deleted".into();
            let gone = to_remote(to_edit, keys)?;
            assert!(gone.is_deleted() && !gone.writable, "{gone:?}");
            let why = to_remote(header, SHARED)
                .err()
                .ok_or("opened without the key")?;
            assert!(why.contains("circle"), "{why}");
        }

        // Through another circle: that one's key is asked for (one of another circle wouldn't
        // open it, the circle being bound into the locked key). Bound to another file: not
        // opened.
        let mut other = shared(v::LOCKED, json!("share"))?;
        let elsewhere = Uuid::new_v4();
        other.shares = Some(vec![
            ShareRow {
                circle_id: Some(elsewhere),
                expires_at: Some(2_000),
                ..ShareRow::default()
            },
            ShareRow {
                circle_id: Some(v::id(v::CIRCLE)),
                expires_at: Some(5_000),
                ..ShareRow::default()
            },
        ]);
        assert_eq!(other.share_circle(), Some(elsewhere));
        // Read until the last of them ends: the file reaches the member through each.
        assert_eq!(other.share_ends(), Some(5_000));
        let mut moved = shared(v::LOCKED, json!("share"))?;
        moved.file_id = Uuid::new_v4();
        assert!(to_remote(moved, keys).is_err());
        let mut unbound = shared(v::LOCKED_BY_GTID, json!("share"))?;
        unbound.file_metadata.global_transit_id = None;
        assert!(to_remote(unbound, keys).is_err());

        // A share row that doesn't read leaves the others; with the first unread, no circle is
        // named.
        let rows: FileHeader = serde_json::from_value({
            let mut row = fixture_value("file-node.json");
            row["shares"] = json!([{ "circleId": null }, "junk", { "circleId": v::CIRCLE }]);
            row
        })?;
        let shares = rows.shares.as_ref().ok_or("shares")?;
        assert_eq!(shares.len(), 3);
        assert_eq!(shares[2].circle_id, Some(v::id(v::CIRCLE)));
        assert_eq!(rows.share_circle(), None);
        // One of them has no end (the rows that don't read are taken so): neither has the file.
        assert_eq!(rows.share_ends(), None);
        let ending: FileHeader = serde_json::from_value({
            let mut row = fixture_value("file-node.json");
            row["shares"] = json!([{ "circleId": v::CIRCLE, "expiresAt": 1_700_000_000_000_i64 }]);
            row
        })?;
        assert_eq!(ending.share_ends(), Some(1_700_000_000_000));
        // A time that doesn't read is no end, and the row names its circle all the same.
        let odd: FileHeader = serde_json::from_value({
            let mut row = fixture_value("file-node.json");
            row["shares"] = json!([{ "circleId": v::CIRCLE, "expiresAt": "soon" }]);
            row
        })?;
        assert_eq!(
            (odd.share_ends(), odd.share_circle()),
            (None, Some(v::id(v::CIRCLE)))
        );
        // This identity's own drive has no rows, and no end.
        let own: FileHeader = serde_json::from_value(fixture_value("file-node.json"))?;
        assert_eq!(own.share_ends(), None);

        // A share withdrawn: a tombstone without its version, parent or key, read as gone.
        let gone: FileHeader = serde_json::from_value(json!({
            "fileId": v::FILE, "fileState": "deleted",
            "fileMetadata": {
                "isEncrypted": false, "versionTag": Uuid::nil(), "updated": 1_759_920_009_000_i64,
                "appData": { "fileType": FILE_TYPE_FILE, "content": "" },
            },
            "sharedSecretEncryptedKeyHeader": {
                "encryptionVersion": 1, "type": "aes", "iv": "", "encryptedAesKey": "",
            },
        }))?;
        assert!(!gone.needs_circle_key());
        let gone = to_remote(gone, keys)?;
        assert!(matches!(gone.body, RemoteBody::Deleted));
        assert!(gone.is_unshared());
        Ok(())
    }

    #[test]
    fn spec_fixtures_map_to_remote_headers() -> R {
        let file = to_remote(as_stored(fixture("file-node.json"))?, SHARED)?;
        assert_eq!(file.kind, NodeKind::File);
        let state = file.state().ok_or("file state")?;
        assert_eq!(state.name, "Résumé.txt");
        let content = state.content.ok_or("content")?;
        assert_eq!((content.size, content.mtime_ms), (1025, 1_759_920_000_123));
        assert_eq!(
            content.hash.to_hex(),
            "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"
        );

        let folder = to_remote(as_stored(fixture("folder-node.json"))?, SHARED)?;
        assert_eq!(folder.kind, NodeKind::Folder);
        assert!(folder.state().is_some());

        // An older writer's removal: the time is the header's, its `rm` is dropped.
        let removed = to_remote(as_stored(fixture("removed-node.json"))?, SHARED)?;
        assert_eq!(removed.removed_at(), Some(1_759_920_005_000));
        assert!(removed.state().is_none());
        let RemoteBody::Live { content, .. } = &removed.body else {
            return Err("not live".into());
        };
        assert!(content.unknown.is_empty(), "{content:?}");

        let tomb = to_remote(fixture("tombstone.json"), SHARED)?;
        assert!(tomb.is_deleted());
        assert_eq!((tomb.file_id, tomb.parent), (file.file_id, file.parent));
        Ok(())
    }

    #[test]
    fn rows_that_are_not_sync_nodes_are_refused() -> R {
        let refused = |row: FileHeader, why: &str| -> R {
            let error = to_remote(as_stored(row)?, SHARED).err().ok_or("decoded")?;
            assert!(error.contains(why), "{error}");
            Ok(())
        };
        let mut other = fixture("file-node.json");
        other.file_metadata.app_data.file_type = 100;
        refused(other, "isn't a sync node")?;
        let mut bad = fixture("file-node.json");
        bad.file_metadata.app_data.content = Some("{\"name\": 3}".into());
        refused(bad, "content isn't node schema v1")?;
        let mut orphan = fixture("file-node.json");
        orphan.file_metadata.app_data.unique_id = None;
        refused(orphan, "no node id")?;
        let mut no_parent = fixture("file-node.json");
        no_parent.file_metadata.app_data.group_id = None;
        refused(no_parent, "no parent")?;
        // The file fixture with content `size`.
        let with_size = |size: Option<u64>| -> Result<FileHeader, Box<dyn std::error::Error>> {
            let mut row = fixture("file-node.json");
            let meta = &mut row.file_metadata.app_data;
            let mut content = NodeContent::from_json(meta.content.as_deref().unwrap_or_default())?;
            content.size = size;
            meta.content = Some(content.to_json());
            Ok(row)
        };
        // Without its size, the stored (padded) size would be taken for the file's.
        refused(with_size(None)?, "no size")?;
        // A newer schema's node is read-only: unreadable here (node schema, "v"), also where
        // that schema changed a field's type.
        for json in [
            r#"{"v":2,"name":"a","mtime":1,"size":3}"#,
            r#"{"v":2,"name":"a","mtime":"2026-10-23","size":3}"#,
        ] {
            let mut newer = fixture("file-node.json");
            newer.file_metadata.app_data.content = Some(json.into());
            refused(newer, "node schema v2, newer than this client's")?;
        }
        // A size its stored bytes can't hold, or one near u64::MAX (whose sealed size would
        // overflow): every download would fail its check, in full.
        for (size, stored) in [(1000, None), (1040, None), (u64::MAX - 3, Some(u64::MAX))] {
            let mut odd = as_stored(with_size(Some(size))?)?;
            for payload in odd.file_metadata.payloads.iter_mut().flatten() {
                payload.bytes_written = stored.unwrap_or(payload.bytes_written);
            }
            let error = to_remote(odd, SHARED).err().ok_or("decoded")?;
            assert!(error.contains("doesn't fit its payload"), "{error}");
        }
        // A file is its payload, and its content hash is what sync compares.
        let mut bare = fixture("file-node.json");
        bare.file_metadata.payloads = None;
        refused(bare, "no payload")?;
        let mut unhashed = fixture("file-node.json");
        for payload in unhashed.file_metadata.payloads.iter_mut().flatten() {
            payload.hash = None;
        }
        refused(unhashed, "no content hash")?;
        // Every node is encrypted.
        let error = to_remote(fixture("file-node.json"), SHARED)
            .err()
            .ok_or("decoded")?;
        assert!(error.contains("not encrypted"), "{error}");

        // A tombstone is never dropped, even without a parent.
        let mut tomb = fixture("tombstone.json");
        tomb.file_metadata.app_data.group_id = None;
        assert!(to_remote(tomb, SHARED).is_ok_and(|h| h.is_deleted()));
        Ok(())
    }

    /// A sync node's row that doesn't read is kept as there: its file, version and ids; another
    /// app's file is only a reason.
    #[test]
    fn a_skipped_row_keeps_what_reads() -> R {
        let mut row = fixture_value("file-node.json");
        row["fileMetadata"]["appData"]["content"] = json!("not sealed");
        // (Shared until a time: the row that doesn't read says so all the same; so does a
        // row whose circle doesn't read.)
        row["shares"] = json!([{ "circleId": "not an id", "expiresAt": 1_700_000_000_000_i64 }]);
        let skipped = skipped_row(&row.to_string(), "why".into());
        assert_eq!(skipped.expires, Some(1_700_000_000_000));
        let header: FileHeader = serde_json::from_value(row.clone())?;
        let meta = &header.file_metadata;
        assert_eq!(
            skipped.at,
            Some(RemoteRef {
                file_id: RemoteFileId(header.file_id),
                version: VersionTag(meta.version_tag),
            })
        );
        assert_eq!(
            (skipped.node, skipped.parent),
            (
                meta.app_data.unique_id.map(NodeId),
                meta.app_data.group_id.map(NodeId)
            )
        );
        row["fileMetadata"]["appData"]["fileType"] = json!(100);
        assert_eq!(
            skipped_row(&row.to_string(), "why".into()),
            Skipped::why("why")
        );
        assert_eq!(skipped_row("{]", "why".into()), Skipped::why("why"));
        Ok(())
    }

    #[test]
    fn rows_that_do_not_decrypt_are_refused() -> R {
        let kh = KeyHeader::random()?;
        let header = as_stored_with(fixture("file-node.json"), &kh)?;
        to_remote(header.clone(), SHARED)?;
        // Another shared secret, a damaged content, a missing key header or payload IV.
        assert!(
            to_remote(
                header.clone(),
                Keys {
                    shared: &[4; 16],
                    drive: None,
                    circle: None,
                    write_only: None,
                }
            )
            .is_err()
        );
        let mut damaged = header.clone();
        damaged.file_metadata.app_data.content = Some(kh.seal_text("{not json"));
        assert!(to_remote(damaged, SHARED).is_err());
        for (key_header, keyless) in with_unusable_key_headers("file-node.json", true) {
            let why = to_remote(keyless, SHARED).err().ok_or("decoded")?;
            assert!(why.contains("no key header"), "{key_header}: {why}");
        }
        let mut no_iv = header;
        no_iv
            .file_metadata
            .payloads
            .iter_mut()
            .flatten()
            .for_each(|p| p.iv = None);
        assert!(to_remote(no_iv, SHARED).is_err());
        Ok(())
    }

    /// A tombstone's key header isn't read: an unusable one (null fields, as a tombstone may
    /// carry) doesn't make the row unreadable.
    #[test]
    fn tombstones_need_no_key_header() -> R {
        for (_, row) in with_unusable_key_headers("tombstone.json", false) {
            assert!(to_remote(row, SHARED)?.is_deleted());
        }
        Ok(())
    }

    #[test]
    fn the_status_decides_removal() -> R {
        // A removal without `rm` (this client's, another app's): from its version's time (the
        // fixture's `updated` is later: a share changed since).
        let mut other = fixture("file-node.json");
        other.file_metadata.app_data.archival_status = ARCHIVAL_REMOVED;
        let header = to_remote(as_stored(other)?, SHARED)?;
        assert_eq!(header.removed_at(), Some(1_759_920_001_000));
        // A restore by a writer that kept `rm`: the status wins.
        let mut restored = fixture("removed-node.json");
        restored.file_metadata.app_data.archival_status = 0;
        let header = to_remote(as_stored(restored)?, SHARED)?;
        assert!(!header.removed && header.state().is_some());
        Ok(())
    }

    /// A minimal page's rows as odin-core #1967 sends them read as the same rows' headers do:
    /// removed from the status at `versionUpdated` (a later `updated`, a share changed since,
    /// isn't it), a tombstone with its own.
    #[test]
    fn minimal_rows_read_as_their_headers() -> R {
        let page: QueryBatchResponse = serde_json::from_str(
            r#"{"cursorState": "c", "hasMoreRows": true, "queryTime": 1, "fileRefs": [
                {"fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                 "fileType": 1, "dataType": 0, "archivalStatus": 2,
                 "versionTag": "22222222-2222-2222-2222-222222222222",
                 "created": 5, "updated": 12, "versionUpdated": 9, "fileByteCount": 3,
                 "uniqueId": "33333333-3333-3333-3333-333333333333"},
                {"fileId": "44444444-4444-4444-4444-444444444444", "fileState": "deleted",
                 "fileType": 1, "dataType": 0, "archivalStatus": 0,
                 "versionTag": "55555555-5555-5555-5555-555555555555",
                 "created": 5, "updated": 10, "versionUpdated": 10, "fileByteCount": 0},
                {"fileId": "66666666-6666-6666-6666-666666666666", "fileState": "deleted",
                 "fileType": 1, "dataType": 0, "archivalStatus": 0,
                 "versionTag": "00000000-0000-0000-0000-000000000000",
                 "created": 5, "updated": 11, "fileByteCount": 0}
            ]}"#,
        )?;
        assert!(page.search_results.is_empty());
        let refs = page
            .file_refs
            .ok_or("no fileRefs")?
            .iter()
            .map(|row| serde_json::from_str::<FileRef>(row.get()).map(|r| r.to_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            (refs[0].deleted, refs[0].removed_at(), refs[0].changed_at),
            (false, Some(9), Some(9))
        );
        assert_eq!(
            (refs[1].deleted, refs[1].removed_at(), refs[1].changed_at),
            (true, None, Some(10)),
            "a tombstone's time: when it may go hard"
        );
        assert!(!refs[1].is_unshared(), "a delete keeps its version");
        // A share withdrawn, as odin-core's `FileRef.FromHeader` makes the member's row of
        // `NoLongerSharedTombstoneOf`: no version (all zeros), no `groupId`, no
        // `versionUpdated`: its time is its own `updated`, the withdrawal's.
        assert!(refs[2].is_unshared() && refs[2].parent.is_none());
        assert_eq!(refs[2].changed_at, Some(11));
        // A row that doesn't say when: no time, never old enough to go without a trace.
        let unknown: FileRef = serde_json::from_str(
            r#"{"fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                "archivalStatus": 2, "versionTag": "22222222-2222-2222-2222-222222222222"}"#,
        )?;
        let unknown = unknown.to_ref();
        assert_eq!((unknown.removed, unknown.changed_at), (true, None));
        // A server without #1967 answers headers: no refs, not an empty page.
        let old: QueryBatchResponse =
            serde_json::from_str(r#"{"cursorState": "c", "queryTime": 1, "searchResults": []}"#)?;
        assert!(old.file_refs.is_none());
        Ok(())
    }

    /// A cursor's time is its paging point's, as odin-core writes `cursorState`.
    #[test]
    fn a_cursor_tells_its_time() {
        let cursor = r#"{"paging":{"time":1792134971335,"row":42},"stop":null,"next":null}"#;
        assert_eq!(cursor_time(cursor), Some(1_792_134_971_335));
        assert_eq!(cursor_time("not json"), None);
        assert_eq!(cursor_time(r#"{"paging":null}"#), None);
    }

    /// What someone a file is shared with sends for a change to it: through their own server
    /// to its owner (`locale: "peer"`), the header sealed with their own shared secret, for a
    /// shared file that is sent on, naming the version it changes, the key header as given.
    #[test]
    fn a_members_change_as_it_is_sent() -> R {
        let kh = KeyHeader::random()?;
        let transfer_iv = sealed::strong()?;
        let p = PayloadDescriptor {
            algorithm: HashAlgorithm::Blake3,
            iv: sealed::strong()?,
        };
        let sent = member_update_instructions(&transfer_iv, Some(&p), "pippin.dotyou.cloud");
        assert_eq!(sent["locale"], "peer");
        assert_eq!(sent["recipients"], json!(["pippin.dotyou.cloud"]));
        assert_eq!(
            sent["manifest"]["payloadDescriptors"][0]["payloadUpdateOperationType"],
            "appendOrOverwrite"
        );
        let header_only = member_update_instructions(&transfer_iv, None, "pippin.dotyou.cloud");
        assert_eq!(header_only["manifest"]["payloadDescriptors"], json!([]));

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::new(),
            kind: NodeKind::File,
            content: NodeContent::file("Notes.txt", 1, 1, false),
            removed: true,
        };
        let version = VersionTag(Uuid::new_v4());
        let sealed = member_update(&meta, version, &SS, &transfer_iv, &kh, kh.kept());
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sealed)?)?;
        assert_eq!(plain["encryptedKeyHeader"], kh.kept());
        let file = &plain["fileMetadata"];
        assert_eq!(file["allowDistribution"], true);
        assert_eq!(
            file["accessControlList"]["requiredSecurityGroup"],
            "connected"
        );
        assert_eq!(file["versionTag"], json!(version.0));
        assert_eq!(file["appData"]["uniqueId"], json!(meta.node.0));
        assert_eq!(file["appData"]["groupId"], json!(meta.parent.0));
        assert_eq!(file["appData"]["archivalStatus"], ARCHIVAL_REMOVED);
        let content = file["appData"]["content"].as_str().ok_or("content")?;
        assert_eq!(kh.open_text(content)?, meta.content.to_json());
        assert!(plain.get("shares").is_none() && plain.get("globalTransitId").is_none());
        Ok(())
    }

    /// What someone adding a file to a folder shared with them sends: staged on their own
    /// transient drive and sent on to the owner's, with the global transit id picked for it
    /// and its shares, no version, owner-only as sent.
    #[test]
    fn a_members_new_file_as_it_is_sent() -> R {
        let kh = KeyHeader::random()?;
        let transfer_iv = sealed::strong()?;
        let drive = crate::drive::TargetDrive {
            alias: Uuid::new_v4(),
            drive_type: Uuid::new_v4(),
        };
        let sent = deposit_instructions(&transfer_iv, None, "pippin.dotyou.cloud", &drive);
        assert!(sent.get("locale").is_none());
        let staged = &sent["storageOptions"]["drive"];
        assert_eq!(staged["alias"], "90f5e74a-b7f9-efda-0ac2-98373a32ad8c");
        assert_eq!(staged["type"], staged["alias"]);
        let transit = &sent["transitOptions"];
        assert_eq!(transit["recipients"], json!(["pippin.dotyou.cloud"]));
        assert_eq!(transit["isTransient"], true);
        assert_eq!(transit["remoteTargetDrive"]["alias"], json!(drive.alias));
        assert_eq!(
            transit["remoteTargetDrive"]["type"],
            json!(drive.drive_type)
        );

        let row: ShareRow = serde_json::from_value(json!({
            "circleId": Uuid::new_v4(), "permissions": "readWrite", "expiresAt": 5_000,
        }))?;
        let circle = row.circle_id.ok_or("circle")?;
        let share = share_of_new(circle, row.permissions, row.expires_at, b"locked");
        assert_eq!(share["fileId"], json!(Uuid::nil()));
        assert_eq!(share["circleId"], json!(circle));
        assert_eq!(
            (share["permissions"].as_u64(), share["expiresAt"].as_i64()),
            (Some(15), Some(5_000))
        );
        assert_eq!(share["lockedFileKey"], STANDARD.encode(b"locked"));

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::new(),
            kind: NodeKind::Folder,
            content: NodeContent::folder("From tom"),
            removed: false,
        };
        let gtid = Uuid::new_v4();
        let key_header = json!({ "type": "sealed" });
        let shares = NewShares {
            global_transit_id: gtid,
            shares: vec![share.clone()],
        };
        let sealed = deposit(&meta, &SS, &transfer_iv, &kh, key_header.clone(), &shares);
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sealed)?)?;
        assert_eq!(plain["encryptedKeyHeader"], key_header);
        assert_eq!(plain["globalTransitId"], json!(gtid));
        assert_eq!(plain["shares"], json!([share]));
        let file = &plain["fileMetadata"];
        assert_eq!(file["allowDistribution"], true);
        assert_eq!(file["accessControlList"]["requiredSecurityGroup"], "owner");
        assert_eq!(file["versionTag"], Value::Null);
        assert_eq!(file["appData"]["fileType"], FILE_TYPE_FOLDER);
        Ok(())
    }

    /// A new node in a shared folder goes up with its shares and the global transit id its
    /// keys are locked to; any other write names neither (the server refuses them there).
    #[test]
    fn a_new_node_goes_up_with_the_shares_it_is_given() -> R {
        let transfer_iv = [7u8; 16];
        let kh = KeyHeader::random()?;
        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::new(),
            kind: NodeKind::File,
            content: NodeContent::file("added.txt", 1, 1, false),
            removed: false,
        };
        let open = |sealed: Vec<u8>| -> Result<Value, Box<dyn std::error::Error>> {
            let plain = aes_cbc_decrypt(&SS, &transfer_iv, &sealed)?;
            Ok(serde_json::from_slice(&plain)?)
        };
        let share = share_of_new(Uuid::from_u128(9), 3, None, b"locked");
        let shared = NewShares {
            global_transit_id: Uuid::from_u128(4),
            shares: vec![share.clone()],
        };
        let plain = open(metadata(
            &meta,
            None,
            SHARED,
            &transfer_iv,
            &kh,
            Some(&shared),
        ))?;
        assert_eq!(plain["shares"], json!([share]));
        assert_eq!(plain["globalTransitId"], json!(Uuid::from_u128(4)));
        // As any write of the owner's: its group is lifted by the shares, on the server.
        let file = &plain["fileMetadata"];
        assert_eq!(file["accessControlList"]["requiredSecurityGroup"], "owner");
        assert_eq!(file["allowDistribution"], false);

        let plain = open(metadata(&meta, None, SHARED, &transfer_iv, &kh, None))?;
        let sent = plain.as_object().ok_or("an object")?;
        assert!(!sent.contains_key("shares") && !sent.contains_key("globalTransitId"));
        Ok(())
    }

    /// A folder's shares are read strictly: the live ones that read are its shares, a
    /// withdrawn one is none, and one that doesn't read is counted, never guessed at.
    #[test]
    fn a_folders_shares_are_read_strictly() -> R {
        const FILE: &str = "11111111-1111-1111-1111-111111111111";
        const VERSION: &str = "22222222-2222-2222-2222-222222222222";
        let circle = |n: u128| Uuid::from_u128(n);
        let folder = |state: &str, shares: Value| {
            let row = json!({
                "fileId": FILE, "fileState": state, "shares": shares,
                "fileMetadata": { "versionTag": VERSION, "appData": { "content": 7 } },
            });
            shared_folder(&row.to_string())
        };
        let read = folder(
            "active",
            json!([
                { "circleId": circle(1), "permissions": "readWrite", "expiresAt": 5_000 },
                { "circleId": circle(2), "permissions": "read", "expiresAt": null },
                { "circleId": circle(3), "permissions": 1 },
                // Withdrawn: no bits.
                { "circleId": circle(4), "permissions": 0 },
                // These don't read: an end that isn't a time, no circle, no permissions, a
                // permission this client doesn't know.
                { "circleId": circle(5), "permissions": "read", "expiresAt": "soon" },
                { "circleId": "x", "permissions": "read" },
                { "circleId": circle(6) },
                { "circleId": circle(7), "permissions": "read, someLaterOne" },
                { "circleId": circle(8), "permissions": null },
                "a row",
            ]),
        )?;
        assert_eq!(
            (read.at.file_id.0, read.at.version.0),
            (FILE.parse()?, VERSION.parse()?)
        );
        assert!(!read.deleted);
        let shares: Vec<_> = (read.shares.iter())
            .map(|s| (s.circle, s.permissions, s.expires_at))
            .collect();
        assert_eq!(
            shares,
            [
                (circle(1), 15, Some(5_000)),
                (circle(2), 1, None),
                (circle(3), 1, None)
            ]
        );
        assert_eq!(read.unread, 6);
        // No shares at all on the row: none, and nothing unread.
        let none = folder("active", Value::Null)?;
        assert!(none.shares.is_empty() && none.unread == 0);
        assert!(folder("deleted", json!([]))?.deleted);
        assert!(shared_folder("{}").is_err(), "not a folder's row");
        Ok(())
    }

    /// What a feed row says of a folder's shares, slim and whole alike: a folder as it was
    /// written, one changed since (a share given or withdrawn moves `updated` alone), and
    /// nothing for a file's row or one that doesn't name its node.
    #[test]
    fn a_feed_row_says_whether_a_folder_is_as_it_was_written() -> R {
        const NODE: &str = "33333333-3333-3333-3333-333333333333";
        const VERSION: &str = "22222222-2222-2222-2222-222222222222";
        let slim = |file_type: i32, updated: Option<i64>, node: Option<&str>| {
            let row = json!({
                "fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                "fileType": file_type, "versionTag": VERSION, "uniqueId": node,
                "updated": updated, "versionUpdated": 9,
            });
            serde_json::from_value::<FileRef>(row).map(|row| row.fed())
        };
        let whole = |file_type: i32, updated: Option<i64>, node: Option<&str>| {
            let row = json!({
                "fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                "fileMetadata": {
                    "versionTag": VERSION, "updated": updated, "versionUpdated": 9,
                    "appData": { "uniqueId": node, "fileType": file_type, "content": "x" },
                },
            });
            serde_json::from_value::<FileHeader>(row).map(|row| row.fed())
        };
        let (node, version): (Uuid, Uuid) = (NODE.parse()?, VERSION.parse()?);
        let said = |fed: Option<Fed>| fed.map(|f| (f.node.0, f.version, f.as_written));
        for (updated, as_written) in [(Some(9), true), (Some(12), false), (None, false)] {
            let folder = Some((node, version, as_written));
            assert_eq!(said(slim(FILE_TYPE_FOLDER, updated, Some(NODE))?), folder);
            assert_eq!(said(whole(FILE_TYPE_FOLDER, updated, Some(NODE))?), folder);
        }
        assert_eq!(said(slim(FILE_TYPE_FILE, Some(12), Some(NODE))?), None);
        assert_eq!(said(whole(FILE_TYPE_FILE, Some(12), Some(NODE))?), None);
        assert_eq!(said(slim(FILE_TYPE_FOLDER, Some(12), None)?), None);
        assert_eq!(said(whole(FILE_TYPE_FOLDER, Some(12), None)?), None);
        Ok(())
    }

    /// A share row's permissions read from the names the server sends (and a number), and
    /// say whether the file is shared for writing; what doesn't read gives nothing.
    #[test]
    fn a_share_rows_permissions_are_read_by_name() -> R {
        let row = |permissions: Value| -> ShareRow {
            serde_json::from_value(
                json!({ "circleId": Uuid::new_v4(), "permissions": permissions }),
            )
            .unwrap_or_default()
        };
        for (sent, flags) in [
            (json!("read"), 1),
            (json!("read, write"), 3),
            (json!("Read,Comment"), 9),
            (json!("readWrite"), 15),
            (json!("all"), 15),
            (json!("writeReactionsAndComments"), 12),
            (json!("read, somethingNew"), 1),
            (json!(7), 7),
            (json!("none"), 0),
            (json!(null), 0),
            (json!(["read"]), 0),
            (json!(-1), 0),
        ] {
            assert_eq!(row(sent.clone()).permissions, flags, "{sent}");
            // A row that says nothing else still names its circle.
            assert!(row(sent).circle_id.is_some());
        }
        let mut header = fixture("file-node.json");
        assert!(!header.shared_for_writing());
        header.shares = Some(vec![row(json!("read")), row(json!("read, react"))]);
        assert!(!header.shared_for_writing());
        header.shares = Some(vec![row(json!("read")), row(json!("readWrite"))]);
        assert!(header.shared_for_writing());
        Ok(())
    }

    #[test]
    fn upload_parts() -> R {
        let kh = KeyHeader::random()?;
        let (payload_iv, transfer_iv) = (sealed::strong()?, sealed::strong()?);
        let content = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let stored = hbsync_fileio::hash_bytes(b"ciphertext", HashAlgorithm::Blake3);
        let p = PayloadDescriptor {
            algorithm: HashAlgorithm::Blake3,
            iv: payload_iv,
        };
        let create = instructions(&transfer_iv, Some(&p), false);
        assert_eq!(create["transferIv"], STANDARD.encode(transfer_iv));
        let d = &create["manifest"]["payloadDescriptors"][0];
        assert_eq!(
            (d["payloadKey"].as_str(), d["hash"]["algorithm"].as_str()),
            (Some("sync_data"), Some("blake3"))
        );
        assert!(create.get("locale").is_none() && d.get("payloadUpdateOperationType").is_none());
        assert_eq!(d["iv"], STANDARD.encode(payload_iv));
        // Deferred: the sentinel, as long as each value.
        assert_eq!(d["hash"]["storedHash"], STANDARD.encode([0x11; 32]));
        assert_eq!(d["hash"]["contentHash"], STANDARD.encode([0x11; 48]));
        assert_eq!((HASH_LEN, SEALED_HASH_LEN), (32, 48));
        // The values follow, in a part of a length known up front.
        let part: Value =
            serde_json::from_slice(&payload_hash(&kh, &payload_iv, &content, &stored))?;
        assert_eq!(part["storedHash"], stored.to_base64());
        let sealed_hash = part["contentHash"].as_str().ok_or("hash")?;
        assert_eq!(
            kh.open_hash(&payload_iv, HashAlgorithm::Blake3, sealed_hash)?,
            content
        );
        assert_eq!(part.to_string().len(), PAYLOAD_HASH_LEN);
        assert_eq!(refused_payload_hash().len(), PAYLOAD_HASH_LEN);
        let update = instructions(&transfer_iv, Some(&p), true);
        let d = &update["manifest"]["payloadDescriptors"][0];
        assert_eq!(update["locale"], "local");
        assert_eq!(d["payloadUpdateOperationType"], "appendOrOverwrite");
        let header_only = instructions(&transfer_iv, None, true);
        assert_eq!(header_only["manifest"]["payloadDescriptors"], json!([]));

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file("Résumé.txt", 1, 1, false),
            removed: false,
        };
        let version = VersionTag(Uuid::from_u128(5));
        let sealed = metadata(&meta, Some(version), SHARED, &transfer_iv, &kh, None);
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sealed)?)?;
        let file = &plain["fileMetadata"];
        assert_eq!(file["isEncrypted"], true);
        assert_eq!(file["versionTag"], json!(version.0));
        let app = &file["appData"];
        assert_eq!(app["uniqueId"], json!(meta.node.0));
        assert_eq!(app["groupId"], json!(NodeId::ROOT.0));
        assert_eq!(app["fileType"], FILE_TYPE_FILE);
        assert_eq!(app["archivalStatus"], 0);
        // A removal is the status alone: the content goes as it is.
        let gone = NodeMeta {
            removed: true,
            ..meta.clone()
        };
        let sealed_gone = metadata(&gone, Some(version), SHARED, &transfer_iv, &kh, None);
        let plain_gone: Value =
            serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sealed_gone)?)?;
        let app_gone = &plain_gone["fileMetadata"]["appData"];
        assert_eq!(app_gone["archivalStatus"], ARCHIVAL_REMOVED);
        let content_gone = app_gone["content"].as_str().ok_or("content")?;
        assert_eq!(kh.open_text(content_gone)?, meta.content.to_json());
        let key: SealedKeyHeader = serde_json::from_value(plain["encryptedKeyHeader"].clone())?;
        assert_eq!(KeyHeader::open(&key, &SS)?, kh);
        let content = app["content"].as_str().ok_or("content")?;
        assert_eq!(kh.open_text(content)?, meta.content.to_json());
        Ok(())
    }
}
