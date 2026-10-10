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

use crate::HomebaseError;
use crate::crypto::{Key, aes_cbc_encrypt};
use crate::drive::sealed::{KeyHeader, SealedKeyHeader};

pub const CONTENT_TYPE: &str = "application/octet-stream";

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

/// A header as a [`RemoteHeader`], decrypted with `shared_secret`; `Err` says why it isn't a
/// usable sync node.
pub fn to_remote(header: FileHeader, shared_secret: &Key) -> Result<RemoteHeader, String> {
    let id = header.file_id;
    let meta = &header.file_metadata;
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
            // The stored size of an encrypted payload isn't the file's: only `size` tells it.
            if kind == NodeKind::File && content.size.is_none() {
                return Err(format!("file {id}: no size (node schema v1)"));
            }
            let payload = match kind {
                NodeKind::Folder => None,
                NodeKind::File => meta
                    .sync_payload()
                    .map(|p| payload_info(p, &key))
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
    pub fn key(&self, shared_secret: &Key) -> Result<KeyHeader, HomebaseError> {
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
    let hash = p
        .hash
        .as_ref()
        .map(|h| {
            let algorithm: HashAlgorithm = h
                .algorithm
                .parse()
                .map_err(|_| format!("hash algorithm {:?}", h.algorithm))?;
            key.open_hash(&payload_iv(p)?, algorithm, &h.content_hash)
                .map_err(|e| format!("content hash: {e}"))
        })
        .transpose()?;
    Ok(PayloadInfo {
        hash,
        bytes: p.bytes_written,
    })
}

/// What an upload says about the `sync_data` payload it sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadDescriptor {
    pub algorithm: HashAlgorithm,
    /// Base64, over the bytes sent.
    pub stored_hash: String,
    /// Base64: the plaintext hash, sealed.
    pub content_hash: String,
    /// The payload IV.
    pub iv: [u8; 16],
}

impl PayloadDescriptor {
    /// A payload encrypted with `key` under `iv`, whose ciphertext hashes to `stored`.
    pub fn sealed(
        key: &KeyHeader,
        iv: [u8; 16],
        content: &ContentHash,
        stored: &ContentHash,
    ) -> Self {
        PayloadDescriptor {
            algorithm: content.algorithm,
            stored_hash: stored.to_base64(),
            content_hash: key.seal_hash(&iv, content),
            iv,
        }
    }
}

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
                "hash": {
                    "algorithm": p.algorithm.as_str(),
                    "storedHash": p.stored_hash,
                    "contentHash": p.content_hash,
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
        // Every node is encrypted.
        let error = to_remote(fixture("file-node.json"), &SS)
            .err()
            .ok_or("decoded")?;
        assert!(error.contains("not encrypted"), "{error}");

        // A tombstone is never dropped, even without a parent.
        let mut tomb = fixture("tombstone.json");
        tomb.file_metadata.app_data.group_id = None;
        assert!(to_remote(tomb, &SS).is_ok_and(|h| h.is_deleted()));
        // A file without its payload is kept, without a state (the engine waits for it).
        let mut bare = fixture("file-node.json");
        bare.file_metadata.payloads = None;
        assert_eq!(
            to_remote(as_stored(bare)?, &SS).map(|h| h.state()),
            Ok(None)
        );
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
    fn upload_parts() -> R {
        let kh = KeyHeader::random()?;
        let (payload_iv, transfer_iv) = (sealed::strong()?, sealed::strong()?);
        let content = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let stored = hbsync_fileio::hash_bytes(b"ciphertext", HashAlgorithm::Blake3);
        let p = PayloadDescriptor::sealed(&kh, payload_iv, &content, &stored);
        let create = instructions(&transfer_iv, Some(&p), false);
        assert_eq!(create["transferIv"], STANDARD.encode(transfer_iv));
        let d = &create["manifest"]["payloadDescriptors"][0];
        assert_eq!(
            (d["payloadKey"].as_str(), d["hash"]["algorithm"].as_str()),
            (Some("sync_data"), Some("blake3"))
        );
        assert!(create.get("locale").is_none() && d.get("payloadUpdateOperationType").is_none());
        assert_eq!(d["iv"], STANDARD.encode(payload_iv));
        assert_eq!(d["hash"]["storedHash"], stored.to_base64());
        let sealed_hash = d["hash"]["contentHash"].as_str().ok_or("hash")?;
        assert_eq!(
            kh.open_hash(&payload_iv, HashAlgorithm::Blake3, sealed_hash)?,
            content
        );
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
        let key: SealedKeyHeader = serde_json::from_value(plain["encryptedKeyHeader"].clone())?;
        assert_eq!(KeyHeader::open(&key, &SS)?, kh);
        let content = app["content"].as_str().ok_or("content")?;
        assert_eq!(kh.open_text(content)?, meta.content.to_json());
        Ok(())
    }
}
