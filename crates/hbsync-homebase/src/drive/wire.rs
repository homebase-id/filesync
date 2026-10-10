//! Node schema v1 on the wire (`docs/spec/node-schema-v1.md`): Homebase file headers as
//! [`RemoteHeader`]s, and the upload parts that write a [`NodeMeta`].
//!
//! Verified in odin-core `SharedSecretEncryptedFileHeader`, `ClientFileMetadata`,
//! `UploadInstructionSet`, `FileUpdateInstructionSet`, `UploadFileDescriptor` (branch
//! `payload-hashes` @ 467d0b531), and chat-kmp's `UploadDescriptorBuilder`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hbsync_remote::{NodeMeta, PayloadInfo, RemoteBody, RemoteHeader};
use hbsync_types::schema::{FILE_TYPE_FILE, FILE_TYPE_FOLDER, NodeContent, PAYLOAD_KEY};
use hbsync_types::{ContentHash, HashAlgorithm, NodeId, NodeKind, RemoteFileId, VersionTag};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::crypto::{Key, aes_cbc_encrypt};
use crate::drive::sealed::KeyHeader;

pub const CONTENT_TYPE: &str = "application/octet-stream";

/// A file header as query-batch and the header routes return it (the fields the client
/// reads).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileHeader {
    pub file_id: Uuid,
    pub file_state: String,
    pub file_metadata: FileMetadata,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub app_data: AppData,
    pub version_tag: Uuid,
    #[serde(default)]
    pub is_encrypted: bool,
    pub payloads: Option<Vec<Payload>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppData {
    pub unique_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub file_type: i32,
    pub content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub key: String,
    #[serde(default)]
    pub bytes_written: u64,
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
pub struct QueryBatchResponse {
    pub cursor_state: Option<String>,
    #[serde(default)]
    pub has_more_rows: bool,
    /// Raw, so one row the client can't read doesn't fail the page (and fields the client
    /// ignores are skipped, not built into a tree).
    #[serde(default)]
    pub search_results: Vec<Box<serde_json::value::RawValue>>,
}

/// The `query-batch` body for one page of the change feed: every change to a sync node,
/// tombstones included, oldest first.
pub fn feed_query(cursor: Option<&str>, limit: usize) -> Value {
    let mut options = json!({
        "maxRecords": limit.max(1),
        "includeMetadataHeader": true,
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

/// A header as a [`RemoteHeader`]; `Err` says why it isn't a usable sync node.
pub fn to_remote(header: FileHeader) -> Result<RemoteHeader, String> {
    let id = header.file_id;
    let meta = header.file_metadata;
    let kind = match meta.app_data.file_type {
        FILE_TYPE_FOLDER => NodeKind::Folder,
        FILE_TYPE_FILE => NodeKind::File,
        other => return Err(format!("file {id}: file type {other} isn't a sync node")),
    };
    let group = meta.app_data.group_id.map(NodeId);
    let (parent, body) = match header.file_state.as_str() {
        // A tombstone always comes through: dropping it would keep a deleted node alive. Its
        // parent isn't used, only its file id.
        "deleted" => (group.unwrap_or(NodeId::ROOT), RemoteBody::Deleted),
        "active" => {
            let parent = group.ok_or_else(|| format!("file {id}: no parent (groupId)"))?;
            if meta.is_encrypted {
                return Err(format!("file {id}: encrypted nodes aren't supported yet"));
            }
            let node = meta
                .app_data
                .unique_id
                .ok_or_else(|| format!("file {id}: no node id (uniqueId)"))?;
            let content = meta.app_data.content.as_deref().unwrap_or_default();
            let content = NodeContent::from_json(content)
                .map_err(|e| format!("file {id}: content isn't node schema v1: {e}"))?;
            let payload = match kind {
                NodeKind::Folder => None,
                NodeKind::File => meta
                    .payloads
                    .iter()
                    .flatten()
                    .find(|p| p.key == PAYLOAD_KEY)
                    .map(payload_info)
                    .transpose()
                    .map_err(|e| format!("file {id}: {e}"))?,
            };
            let body = RemoteBody::Live {
                node: NodeId(node),
                content,
                payload,
            };
            (parent, body)
        }
        other => return Err(format!("file {id}: file state {other:?}")),
    };
    Ok(RemoteHeader {
        file_id: RemoteFileId(id),
        version: VersionTag(meta.version_tag),
        parent,
        kind,
        body,
    })
}

fn payload_info(p: &Payload) -> Result<PayloadInfo, String> {
    // Unencrypted: the content hash is the stored hash.
    let hash = p
        .hash
        .as_ref()
        .map(|h| {
            let algorithm: HashAlgorithm = h
                .algorithm
                .parse()
                .map_err(|_| format!("hash algorithm {:?}", h.algorithm))?;
            ContentHash::from_base64(algorithm, &h.content_hash)
                .map_err(|e| format!("content hash: {e}"))
        })
        .transpose()?;
    Ok(PayloadInfo {
        hash,
        bytes: p.bytes_written,
    })
}

/// The `instructions` part of a create (`update: false`) or update. With `payload`, the
/// `sync_data` payload is (over)written with that hash.
pub fn instructions(transfer_iv: &[u8; 16], payload: Option<&ContentHash>, update: bool) -> Value {
    let descriptors: Vec<Value> = payload
        .map(|hash| {
            let b64 = hash.to_base64();
            let mut d = json!({
                "payloadKey": PAYLOAD_KEY,
                "contentType": CONTENT_TYPE,
                "hash": {
                    "algorithm": hash.algorithm.as_str(),
                    "storedHash": b64,
                    "contentHash": b64,
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

/// The plaintext of the `metadata` part. Homebase replaces the whole header on update, so
/// every field is always sent: node id, parent, kind, content, and the owner-only ACL.
fn metadata_json(meta: &NodeMeta, version: Option<VersionTag>, key_header: Value) -> Value {
    let file_type = match meta.kind {
        NodeKind::Folder => FILE_TYPE_FOLDER,
        NodeKind::File => FILE_TYPE_FILE,
    };
    json!({
        "encryptedKeyHeader": key_header,
        "fileMetadata": {
            "isEncrypted": false,
            "allowDistribution": false,
            "accessControlList": { "requiredSecurityGroup": "owner" },
            "appData": {
                "uniqueId": meta.node.0,
                "groupId": meta.parent.0,
                "fileType": file_type,
                "dataType": 0,
                "content": meta.content.to_json(),
            },
            "versionTag": version.map(|v| v.0),
        },
    })
}

/// The `metadata` part: the header JSON, AES-CBC under the shared secret with `transfer_iv`.
/// An unencrypted file still needs a key header; like chat-kmp, an all-zero key encrypted the
/// same way.
pub fn metadata(
    meta: &NodeMeta,
    version: Option<VersionTag>,
    shared_secret: &Key,
    transfer_iv: &[u8; 16],
) -> Vec<u8> {
    let key_header = KeyHeader::seal_none(shared_secret, transfer_iv);
    let plain = metadata_json(meta, version, key_header).to_string();
    aes_cbc_encrypt(shared_secret, transfer_iv, plain.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aes_cbc_decrypt;

    fn fixture(name: &str) -> FileHeader {
        let path = format!(
            "{}/../../docs/spec/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    #[test]
    fn spec_fixtures_map_to_remote_headers() -> Result<(), String> {
        let file = to_remote(fixture("file-node.json"))?;
        assert_eq!(file.kind, NodeKind::File);
        let state = file.state().ok_or("file state")?;
        assert_eq!(state.name, "Résumé.txt");
        let content = state.content.ok_or("content")?;
        assert_eq!((content.size, content.mtime_ms), (1025, 1_759_920_000_123));
        assert_eq!(
            content.hash.to_hex(),
            "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"
        );

        let folder = to_remote(fixture("folder-node.json"))?;
        assert_eq!(folder.kind, NodeKind::Folder);
        assert!(folder.state().is_some());

        let tomb = to_remote(fixture("tombstone.json"))?;
        assert!(tomb.is_deleted());
        assert_eq!((tomb.file_id, tomb.parent), (file.file_id, file.parent));
        Ok(())
    }

    #[test]
    fn rows_that_are_not_sync_nodes_are_refused() {
        let mut other = fixture("file-node.json");
        other.file_metadata.app_data.file_type = 100;
        assert!(to_remote(other).is_err());
        let mut bad = fixture("file-node.json");
        bad.file_metadata.app_data.content = Some("{\"name\": 3}".into());
        assert!(to_remote(bad).is_err());
        let mut orphan = fixture("file-node.json");
        orphan.file_metadata.app_data.unique_id = None;
        assert!(to_remote(orphan).is_err());
        let mut no_parent = fixture("file-node.json");
        no_parent.file_metadata.app_data.group_id = None;
        assert!(to_remote(no_parent).is_err());
        // A tombstone is never dropped, even without a parent.
        let mut tomb = fixture("tombstone.json");
        tomb.file_metadata.app_data.group_id = None;
        assert!(to_remote(tomb).is_ok_and(|h| h.is_deleted()));
        // A file without its payload is kept, without a state (the engine waits for it).
        let mut bare = fixture("file-node.json");
        bare.file_metadata.payloads = None;
        assert_eq!(to_remote(bare).map(|h| h.state()), Ok(None));
    }

    #[test]
    fn upload_parts() -> Result<(), Box<dyn std::error::Error>> {
        let hash = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let iv = [7u8; 16];
        let create = instructions(&iv, Some(&hash), false);
        assert_eq!(create["transferIv"], STANDARD.encode(iv));
        let d = &create["manifest"]["payloadDescriptors"][0];
        assert_eq!(
            (d["payloadKey"].as_str(), d["hash"]["algorithm"].as_str()),
            (Some("sync_data"), Some("blake3"))
        );
        assert!(create.get("locale").is_none() && d.get("payloadUpdateOperationType").is_none());
        let update = instructions(&iv, None, true);
        assert_eq!(update["locale"], "local");
        assert_eq!(update["manifest"]["payloadDescriptors"], json!([]));

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file("a", 1, 1, false),
        };
        let key = [9u8; 16];
        let version = VersionTag(Uuid::from_u128(5));
        let sealed = metadata(&meta, Some(version), &key, &iv);
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&key, &iv, &sealed)?)?;
        let app = &plain["fileMetadata"]["appData"];
        assert_eq!(app["uniqueId"], json!(meta.node.0));
        assert_eq!(app["groupId"], json!(NodeId::ROOT.0));
        assert_eq!(app["fileType"], FILE_TYPE_FILE);
        assert_eq!(app["content"], meta.content.to_json());
        assert_eq!(plain["fileMetadata"]["versionTag"], json!(version.0));
        let wrapped = STANDARD.decode(
            plain["encryptedKeyHeader"]["encryptedAesKey"]
                .as_str()
                .ok_or("key")?,
        )?;
        assert_eq!(aes_cbc_decrypt(&key, &iv, &wrapped)?, [0u8; 32]);
        Ok(())
    }
}
