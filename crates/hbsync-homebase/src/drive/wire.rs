//! Node schema v1 on the wire (`docs/spec/node-schema-v1.md`): Homebase file headers as
//! [`RemoteHeader`]s, and the upload parts that write a [`NodeMeta`].
//!
//! Verified in odin-core `SharedSecretEncryptedFileHeader`, `ClientFileMetadata`,
//! `UploadInstructionSet`, `FileUpdateInstructionSet`, `UploadFileDescriptor` (branch
//! `payload-hashes` @ 467d0b531), and chat-kmp's `UploadDescriptorBuilder`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hbsync_remote::{ChangeRef, NodeMeta, PayloadInfo, RemoteBody, RemoteHeader};
use hbsync_types::schema::{
    ARCHIVAL_REMOVED, FILE_TYPE_FILE, FILE_TYPE_FOLDER, NodeContent, PAYLOAD_KEY,
};
use hbsync_types::{ContentHash, HashAlgorithm, NodeId, NodeKind, RemoteFileId, VersionTag};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::HomebaseError;
use crate::crypto::{Key, aes_cbc_encrypt};
use crate::drive::sealed::{KeyHeader, SealedKeyHeader, checked_sealed_len, sealed_len};

pub(crate) const CONTENT_TYPE: &str = "application/octet-stream";

/// A file header as query-batch and the header routes return it (the fields the client
/// reads).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileHeader {
    pub file_id: Uuid,
    pub file_state: String,
    pub file_metadata: FileMetadata,
    /// The file key and header IV under the shared secret. Not read on a tombstone: one that
    /// isn't usable (null fields, say) is taken as none, so it can't make that row unreadable.
    #[serde(default, deserialize_with = "lenient")]
    pub shared_secret_encrypted_key_header: Option<SealedKeyHeader>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub app_data: AppData,
    pub version_tag: Uuid,
    #[serde(default)]
    pub is_encrypted: bool,
    pub payloads: Option<Vec<Payload>>,
    /// Unix ms of the last change; `None` if the header doesn't say.
    #[serde(default)]
    pub updated: Option<i64>,
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

/// The time of the last change a feed cursor stands after: `cursorState` is odin-core's
/// `QueryBatchCursor` as JSON, `{"paging": {"time": <Unix ms>, "row": …}, …}` (**[verified]**
/// `QueryBatchCursor.cs`, `QueryBatchResponse.cs`).
pub(crate) fn cursor_time(cursor: &str) -> Option<i64> {
    let state: Value = serde_json::from_str(cursor).ok()?;
    state["paging"]["time"].as_i64()
}

/// One row of a minimal `query-batch` page (odin-core #1967 `QueryBatchFileRef`, **[verified]**):
/// the fields the client reads; the others (created, uniqueId, …) are skipped.
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
    /// Unix ms, as the header's `updated`; `None` if the row doesn't say.
    #[serde(default)]
    pub updated: Option<i64>,
    /// A sync node's ([`FILE_TYPE_FOLDER`], [`FILE_TYPE_FILE`]), or another app's file on the
    /// drive.
    #[serde(default)]
    pub file_type: Option<i32>,
}

impl FileRef {
    /// Whether the row is another app's file, not a sync node (its type says so).
    pub(crate) fn not_a_node(&self) -> bool {
        self.file_type
            .is_some_and(|t| t != FILE_TYPE_FOLDER && t != FILE_TYPE_FILE)
    }
}

impl FileRef {
    /// As [`to_remote`] reads the same row's header ([`Times`]).
    pub(crate) fn to_ref(&self) -> ChangeRef {
        let times = Times::of(&self.file_state, self.archival_status, self.updated);
        ChangeRef {
            file_id: RemoteFileId(self.file_id),
            version: VersionTag(self.version_tag),
            deleted: times.deleted,
            parent: self.group_id.map(NodeId).filter(|_| !times.deleted),
            removed_at: times.removed_at,
            changed_at: times.changed_at,
        }
    }
}

/// What a row's state, status and `updated` say, read alike from a header and a minimal row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Times {
    pub deleted: bool,
    /// In Recently deleted since: the status says whether it's removed, the header's `updated`
    /// when, the server's time of the last write (odin-core's `record.modified`, **[verified]**
    /// `FileMetadata.cs`). Nobody writes to a removed node (node schema: also no sharing,
    /// reactions or local app data, which move it too).
    pub removed_at: Option<i64>,
    /// The server's time of this version, a tombstone's too (when it may be deleted hard).
    pub changed_at: Option<i64>,
}

impl Times {
    /// A row whose `updated` is unknown has no time: removed, it counts as removed now
    /// (`removal_time(None)`, the newest: never old enough to go without a trace).
    pub(crate) fn of(file_state: &str, archival_status: i32, updated: Option<i64>) -> Times {
        let deleted = file_state == "deleted";
        Times {
            deleted,
            removed_at: (!deleted && archival_status == ARCHIVAL_REMOVED)
                .then(|| updated.unwrap_or(hbsync_types::removal_time(None))),
            changed_at: updated,
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

/// The `query-batch` body for one page of the change feed: every change to a sync node,
/// tombstones included, oldest first. Both shapes page alike: the same rows and cursor.
pub fn feed_query(cursor: Option<&str>, limit: usize, shape: Shape) -> Value {
    let mut options = json!({
        "maxRecords": limit.max(1),
        "resultShape": match shape {
            Shape::Full => "full",
            Shape::Minimal => "minimal",
        },
        "sorting": "anyChangeDate",
        "ordering": "oldestFirst",
    });
    if let Some(cursor) = cursor.filter(|c| !c.is_empty()) {
        options["cursorState"] = json!(cursor);
    }
    json!({
        "queryParams": { "fileType": [FILE_TYPE_FOLDER, FILE_TYPE_FILE] },
        "resultOptionsRequest": options,
    })
}

/// The `query-batch` body for a page of the active files in `folders` (their `groupId`), of
/// any type, minimal rows: what [`HomebaseRemote::holding`](super::HomebaseRemote) asks.
/// odin-core filters on `groupId`, `fileState` and `archivalStatus` (**[verified]**
/// `QueryBatch.cs:130-142, 351`); live is any status but removed, so that is read per row.
pub(crate) fn children_query(folders: &[NodeId], cursor: Option<&str>, limit: usize) -> Value {
    let mut options = json!({
        "maxRecords": limit.max(1),
        "resultShape": "minimal",
        "sorting": "anyChangeDate",
        "ordering": "oldestFirst",
    });
    if let Some(cursor) = cursor.filter(|c| !c.is_empty()) {
        options["cursorState"] = json!(cursor);
    }
    json!({
        "queryParams": {
            "groupId": folders.iter().map(|f| f.0).collect::<Vec<_>>(),
            "fileState": ["active"],
        },
        "resultOptionsRequest": options,
    })
}

/// A header as a [`RemoteHeader`], decrypted with `shared_secret`; `Err` says why it isn't a
/// usable sync node.
pub(crate) fn to_remote(header: FileHeader, shared_secret: &Key) -> Result<RemoteHeader, String> {
    let id = header.file_id;
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
            let key = header
                .key(shared_secret)
                .map_err(|e| format!("file {id}: {e}"))?;
            let content = key
                .open_text(meta.app_data.content.as_deref().unwrap_or_default())
                .map_err(|e| format!("file {id}: content: {e}"))?;
            let content = NodeContent::from_json(&content)
                .map_err(|e| format!("file {id}: content isn't node schema v1: {e}"))?;
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
            // The key opened: the header it came in is the file's token.
            let token = header
                .shared_secret_encrypted_key_header
                .as_ref()
                .and_then(SealedKeyHeader::to_token);
            (parent, body, token)
        }
        other => return Err(format!("file {id}: file state {other:?}")),
    };
    Ok(RemoteHeader {
        file_id: RemoteFileId(id),
        version: VersionTag(meta.version_tag),
        parent,
        kind,
        body,
        removed_at: times.removed_at,
        changed_at: times.changed_at,
        token,
    })
}

/// `T` if the value is one, else `None`.
fn lenient<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_value(Value::deserialize(d)?).ok())
}

impl FileHeader {
    /// The file key and header IV, opened with `shared_secret`. Every node is encrypted.
    pub(crate) fn key(&self, shared_secret: &Key) -> Result<KeyHeader, HomebaseError> {
        if !self.file_metadata.is_encrypted {
            return Err(HomebaseError::Crypto("not encrypted".into()));
        }
        let sealed = self
            .shared_secret_encrypted_key_header
            .as_ref()
            .ok_or_else(|| HomebaseError::Crypto("no key header".into()))?;
        KeyHeader::open(sealed, shared_secret)
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
fn payload_iv(p: &Payload) -> Result<[u8; 16], String> {
    let iv = p.iv.as_deref().ok_or("no payload IV")?;
    STANDARD
        .decode(iv)
        .ok()
        .and_then(|iv| iv.try_into().ok())
        .ok_or_else(|| format!("payload IV {iv:?}"))
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
/// the shared secret, the content with the key.
pub(crate) fn metadata(
    meta: &NodeMeta,
    version: Option<VersionTag>,
    shared_secret: &Key,
    transfer_iv: &[u8; 16],
    key: &KeyHeader,
) -> Vec<u8> {
    let file_type = match meta.kind {
        NodeKind::Folder => FILE_TYPE_FOLDER,
        NodeKind::File => FILE_TYPE_FILE,
    };
    let plain = json!({
        "encryptedKeyHeader": key.seal(shared_secret, transfer_iv),
        "fileMetadata": {
            "isEncrypted": true,
            "allowDistribution": false,
            "accessControlList": { "requiredSecurityGroup": "owner" },
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
    });
    aes_cbc_encrypt(shared_secret, transfer_iv, plain.to_string().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aes_cbc_decrypt;
    use crate::drive::sealed;

    /// The shared secret of the tests.
    const SS: Key = [3; 16];

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

    #[test]
    fn spec_fixtures_map_to_remote_headers() -> R {
        let file = to_remote(as_stored(fixture("file-node.json"))?, &SS)?;
        assert_eq!(file.kind, NodeKind::File);
        let state = file.state().ok_or("file state")?;
        assert_eq!(state.name, "Résumé.txt");
        let content = state.content.ok_or("content")?;
        assert_eq!((content.size, content.mtime_ms), (1025, 1_759_920_000_123));
        assert_eq!(
            content.hash.to_hex(),
            "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"
        );

        let folder = to_remote(as_stored(fixture("folder-node.json"))?, &SS)?;
        assert_eq!(folder.kind, NodeKind::Folder);
        assert!(folder.state().is_some());

        // An older writer's removal: the time is the header's, its `rm` is dropped.
        let removed = to_remote(as_stored(fixture("removed-node.json"))?, &SS)?;
        assert_eq!(removed.removed_at, Some(1_759_920_005_000));
        assert!(removed.state().is_none());
        let RemoteBody::Live { content, .. } = &removed.body else {
            return Err("not live".into());
        };
        assert!(content.unknown.is_empty(), "{content:?}");

        let tomb = to_remote(fixture("tombstone.json"), &SS)?;
        assert!(tomb.is_deleted());
        assert_eq!((tomb.file_id, tomb.parent), (file.file_id, file.parent));
        Ok(())
    }

    #[test]
    fn rows_that_are_not_sync_nodes_are_refused() -> R {
        let refused = |row: FileHeader, why: &str| -> R {
            let error = to_remote(as_stored(row)?, &SS).err().ok_or("decoded")?;
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
        // Without its size, the stored (padded) size would be taken for the file's.
        let mut sizeless = fixture("file-node.json");
        let mut content = NodeContent::from_json(
            sizeless
                .file_metadata
                .app_data
                .content
                .as_deref()
                .unwrap_or_default(),
        )?;
        content.size = None;
        sizeless.file_metadata.app_data.content = Some(content.to_json());
        refused(sizeless, "no size")?;
        // A size its stored bytes can't hold, or one near u64::MAX (whose sealed size would
        // overflow): every download would fail its check, in full.
        for (size, stored) in [(1000, None), (1040, None), (u64::MAX - 3, Some(u64::MAX))] {
            let mut odd = fixture("file-node.json");
            let mut content = NodeContent::from_json(
                odd.file_metadata
                    .app_data
                    .content
                    .as_deref()
                    .unwrap_or_default(),
            )?;
            content.size = Some(size);
            odd.file_metadata.app_data.content = Some(content.to_json());
            let mut odd = as_stored(odd)?;
            for payload in odd.file_metadata.payloads.iter_mut().flatten() {
                payload.bytes_written = stored.unwrap_or(payload.bytes_written);
            }
            let error = to_remote(odd, &SS).err().ok_or("decoded")?;
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
        let error = to_remote(fixture("file-node.json"), &SS)
            .err()
            .ok_or("decoded")?;
        assert!(error.contains("not encrypted"), "{error}");

        // A tombstone is never dropped, even without a parent.
        let mut tomb = fixture("tombstone.json");
        tomb.file_metadata.app_data.group_id = None;
        assert!(to_remote(tomb, &SS).is_ok_and(|h| h.is_deleted()));
        Ok(())
    }

    #[test]
    fn rows_that_do_not_decrypt_are_refused() -> R {
        let kh = KeyHeader::random()?;
        let header = as_stored_with(fixture("file-node.json"), &kh)?;
        to_remote(header.clone(), &SS)?;
        // Another shared secret, a damaged content, a missing key header or payload IV.
        assert!(to_remote(header.clone(), &[4; 16]).is_err());
        let mut damaged = header.clone();
        damaged.file_metadata.app_data.content = Some(kh.seal_text("{not json"));
        assert!(to_remote(damaged, &SS).is_err());
        for (key_header, keyless) in with_unusable_key_headers("file-node.json", true) {
            let why = to_remote(keyless, &SS).err().ok_or("decoded")?;
            assert!(why.contains("no key header"), "{key_header}: {why}");
        }
        let mut no_iv = header;
        no_iv
            .file_metadata
            .payloads
            .iter_mut()
            .flatten()
            .for_each(|p| p.iv = None);
        assert!(to_remote(no_iv, &SS).is_err());
        Ok(())
    }

    /// A tombstone's key header isn't read: an unusable one (null fields, as a tombstone may
    /// carry) doesn't make the row unreadable.
    #[test]
    fn tombstones_need_no_key_header() -> R {
        for (_, row) in with_unusable_key_headers("tombstone.json", false) {
            assert!(to_remote(row, &SS)?.is_deleted());
        }
        Ok(())
    }

    #[test]
    fn the_status_decides_removal() -> R {
        // A removal without `rm` (this client's, another app's): from the last change.
        let mut other = fixture("file-node.json");
        other.file_metadata.app_data.archival_status = ARCHIVAL_REMOVED;
        let header = to_remote(as_stored(other)?, &SS)?;
        assert_eq!(header.removed_at, Some(1_759_920_001_000));
        // A restore by a writer that kept `rm`: the status wins.
        let mut restored = fixture("removed-node.json");
        restored.file_metadata.app_data.archival_status = 0;
        let header = to_remote(as_stored(restored)?, &SS)?;
        assert!(header.removed_at.is_none() && header.state().is_some());
        Ok(())
    }

    /// A minimal page's rows as odin-core #1967 sends them read as the same rows' headers do:
    /// removed from the status at `updated`, a tombstone without times.
    #[test]
    fn minimal_rows_read_as_their_headers() -> R {
        let page: QueryBatchResponse = serde_json::from_str(
            r#"{"cursorState": "c", "hasMoreRows": true, "queryTime": 1, "fileRefs": [
                {"fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                 "fileType": 1, "dataType": 0, "archivalStatus": 2,
                 "versionTag": "22222222-2222-2222-2222-222222222222",
                 "created": 5, "updated": 9, "fileByteCount": 3,
                 "uniqueId": "33333333-3333-3333-3333-333333333333"},
                {"fileId": "44444444-4444-4444-4444-444444444444", "fileState": "deleted",
                 "fileType": 1, "dataType": 0, "archivalStatus": 0,
                 "versionTag": "55555555-5555-5555-5555-555555555555",
                 "created": 5, "updated": 10, "fileByteCount": 0}
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
            (refs[0].deleted, refs[0].removed_at, refs[0].changed_at),
            (false, Some(9), Some(9))
        );
        assert_eq!(
            (refs[1].deleted, refs[1].removed_at, refs[1].changed_at),
            (true, None, Some(10)),
            "a tombstone's time: when it may go hard"
        );
        // A row that doesn't say when: no time, never old enough to go without a trace.
        let unknown: FileRef = serde_json::from_str(
            r#"{"fileId": "11111111-1111-1111-1111-111111111111", "fileState": "active",
                "archivalStatus": 2, "versionTag": "22222222-2222-2222-2222-222222222222"}"#,
        )?;
        let unknown = unknown.to_ref();
        assert_eq!(
            (unknown.removed_at, unknown.changed_at),
            (Some(i64::MAX), None)
        );
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
        let sealed = metadata(&meta, Some(version), &SS, &transfer_iv, &kh);
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
        let sealed_gone = metadata(&gone, Some(version), &SS, &transfer_iv, &kh);
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
