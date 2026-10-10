//! Node schema v1 on the wire (`docs/spec/node-schema-v1.md`): Homebase file headers as
//! [`RemoteHeader`]s, and the upload parts that write a [`NodeMeta`].
//!
//! Verified in odin-core `SharedSecretEncryptedFileHeader`, `ClientFileMetadata`,
//! `UploadInstructionSet`, `FileUpdateInstructionSet`, `UploadFileDescriptor` (branch
//! `payload-hashes` @ 467d0b531), and chat-kmp's `UploadDescriptorBuilder`.

use std::borrow::Cow;

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
    /// The file key and header IV under the shared secret. Only read when the file is
    /// encrypted: one that isn't usable (null fields on a tombstone, say) is taken as none, so
    /// it can't make the row unreadable.
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
    /// Base64; set when the file is encrypted.
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

/// A header as a [`RemoteHeader`], decrypted with `shared_secret` when encrypted; `Err` says
/// why it isn't a usable sync node.
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
            let content = meta.app_data.content.as_deref().unwrap_or_default();
            let content = match &key {
                Some(key) => Cow::Owned(
                    key.open_text(content)
                        .map_err(|e| format!("file {id}: content: {e}"))?,
                ),
                None => Cow::Borrowed(content),
            };
            let content = NodeContent::from_json(&content)
                .map_err(|e| format!("file {id}: content isn't node schema v1: {e}"))?;
            // The stored size of an encrypted payload isn't the file's: only `size` tells it.
            if kind == NodeKind::File && key.is_some() && content.size.is_none() {
                return Err(format!(
                    "file {id}: encrypted, but no size (node schema v1)"
                ));
            }
            let payload = match kind {
                NodeKind::Folder => None,
                NodeKind::File => meta
                    .sync_payload()
                    .map(|p| payload_info(p, key.as_ref()))
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
    /// The file key and header IV, opened with `shared_secret`; `None` when unencrypted.
    pub fn key(&self, shared_secret: &Key) -> Result<Option<KeyHeader>, HomebaseError> {
        if !self.file_metadata.is_encrypted {
            return Ok(None);
        }
        let sealed = self
            .shared_secret_encrypted_key_header
            .as_ref()
            .ok_or_else(|| HomebaseError::Crypto("no key header".into()))?;
        KeyHeader::open(sealed, shared_secret).map(Some)
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

/// The IV of a payload of an encrypted file.
fn payload_iv(p: &Payload) -> Result<[u8; 16], String> {
    let iv = p.iv.as_deref().ok_or("no payload IV")?;
    STANDARD
        .decode(iv)
        .ok()
        .and_then(|iv| iv.try_into().ok())
        .ok_or_else(|| format!("payload IV {iv:?}"))
}

/// The payload's size and content hash. Unencrypted, the content hash is the stored hash;
/// encrypted, it is sealed with the file `key` under the payload IV.
fn payload_info(p: &Payload, key: Option<&KeyHeader>) -> Result<PayloadInfo, String> {
    let hash = p
        .hash
        .as_ref()
        .map(|h| {
            let algorithm: HashAlgorithm = h
                .algorithm
                .parse()
                .map_err(|_| format!("hash algorithm {:?}", h.algorithm))?;
            match key {
                Some(key) => key
                    .open_hash(&payload_iv(p)?, algorithm, &h.content_hash)
                    .map_err(|e| format!("content hash: {e}")),
                None => ContentHash::from_base64(algorithm, &h.content_hash)
                    .map_err(|e| format!("content hash: {e}")),
            }
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
    /// Base64: the plaintext hash, sealed when encrypted.
    pub content_hash: String,
    /// The payload IV, when encrypted.
    pub iv: Option<[u8; 16]>,
}

impl PayloadDescriptor {
    /// An unencrypted payload: both hashes are the plaintext's.
    pub fn plain(hash: &ContentHash) -> Self {
        PayloadDescriptor {
            algorithm: hash.algorithm,
            stored_hash: hash.to_base64(),
            content_hash: hash.to_base64(),
            iv: None,
        }
    }

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
            iv: Some(iv),
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
                "hash": {
                    "algorithm": p.algorithm.as_str(),
                    "storedHash": p.stored_hash,
                    "contentHash": p.content_hash,
                },
            });
            if let Some(iv) = &p.iv {
                d["iv"] = json!(STANDARD.encode(iv));
            }
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
/// parent, kind, content, and the owner-only ACL. With `key`, the file is encrypted: the key
/// sealed with the shared secret, the content with the key. An unencrypted file still needs a
/// key header; like chat-kmp, an all-zero key.
pub(crate) fn metadata(
    meta: &NodeMeta,
    version: Option<VersionTag>,
    shared_secret: &Key,
    transfer_iv: &[u8; 16],
    key: Option<&KeyHeader>,
) -> Vec<u8> {
    let file_type = match meta.kind {
        NodeKind::Folder => FILE_TYPE_FOLDER,
        NodeKind::File => FILE_TYPE_FILE,
    };
    let none = KeyHeader {
        iv: [0; 16],
        key: [0; 16],
    };
    let content = meta.content.to_json();
    let plain = json!({
        "encryptedKeyHeader": key.unwrap_or(&none).seal(shared_secret, transfer_iv),
        "fileMetadata": {
            "isEncrypted": key.is_some(),
            "allowDistribution": false,
            "accessControlList": { "requiredSecurityGroup": "owner" },
            "appData": {
                "uniqueId": meta.node.0,
                "groupId": meta.parent.0,
                "fileType": file_type,
                "dataType": 0,
                "content": match key {
                    Some(k) => k.seal_text(&content),
                    None => content,
                },
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
        let file = to_remote(fixture("file-node.json"), &SS)?;
        assert_eq!(file.kind, NodeKind::File);
        let state = file.state().ok_or("file state")?;
        assert_eq!(state.name, "Résumé.txt");
        let content = state.content.ok_or("content")?;
        assert_eq!((content.size, content.mtime_ms), (1025, 1_759_920_000_123));
        assert_eq!(
            content.hash.to_hex(),
            "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"
        );

        let folder = to_remote(fixture("folder-node.json"), &SS)?;
        assert_eq!(folder.kind, NodeKind::Folder);
        assert!(folder.state().is_some());

        let tomb = to_remote(fixture("tombstone.json"), &SS)?;
        assert!(tomb.is_deleted());
        assert_eq!((tomb.file_id, tomb.parent), (file.file_id, file.parent));
        Ok(())
    }

    #[test]
    fn rows_that_are_not_sync_nodes_are_refused() {
        let mut other = fixture("file-node.json");
        other.file_metadata.app_data.file_type = 100;
        assert!(to_remote(other, &SS).is_err());
        let mut bad = fixture("file-node.json");
        bad.file_metadata.app_data.content = Some("{\"name\": 3}".into());
        assert!(to_remote(bad, &SS).is_err());
        let mut orphan = fixture("file-node.json");
        orphan.file_metadata.app_data.unique_id = None;
        assert!(to_remote(orphan, &SS).is_err());
        let mut no_parent = fixture("file-node.json");
        no_parent.file_metadata.app_data.group_id = None;
        assert!(to_remote(no_parent, &SS).is_err());
        // A tombstone is never dropped, even without a parent.
        let mut tomb = fixture("tombstone.json");
        tomb.file_metadata.app_data.group_id = None;
        assert!(to_remote(tomb, &SS).is_ok_and(|h| h.is_deleted()));
        // A file without its payload is kept, without a state (the engine waits for it).
        let mut bare = fixture("file-node.json");
        bare.file_metadata.payloads = None;
        assert_eq!(to_remote(bare, &SS).map(|h| h.state()), Ok(None));
    }

    /// An encrypted node as another Homebase client writes it: content, payload IV and
    /// content hash sealed with the file key, the key under the shared secret.
    fn encrypted(kh: &KeyHeader) -> Result<(FileHeader, ContentHash), Box<dyn std::error::Error>> {
        let mut header = fixture("file-node.json");
        let plain = header
            .file_metadata
            .app_data
            .content
            .clone()
            .ok_or("content")?;
        let meta = &mut header.file_metadata;
        meta.is_encrypted = true;
        meta.app_data.content = Some(kh.seal_text(&plain));
        let payload = meta.payloads.iter_mut().flatten().next().ok_or("payload")?;
        let payload_iv = sealed::strong()?;
        payload.iv = Some(STANDARD.encode(payload_iv));
        let hash = payload.hash.as_mut().ok_or("hash")?;
        let content_hash = ContentHash::from_base64(HashAlgorithm::Blake3, &hash.content_hash)?;
        hash.content_hash = kh.seal_hash(&payload_iv, &content_hash);
        header.shared_secret_encrypted_key_header =
            Some(serde_json::from_value(kh.seal(&SS, &sealed::strong()?))?);
        Ok((header, content_hash))
    }

    #[test]
    fn encrypted_rows_decode_like_plain_ones() -> Result<(), Box<dyn std::error::Error>> {
        let kh = KeyHeader::random()?;
        let (header, content_hash) = encrypted(&kh)?;
        let plain = to_remote(fixture("file-node.json"), &SS)?;
        let decoded = to_remote(header.clone(), &SS)?;
        assert_eq!(decoded.state(), plain.state());
        let state = decoded.state().ok_or("state")?;
        assert_eq!(state.name, "Résumé.txt");
        assert_eq!(state.content.ok_or("content")?.hash, content_hash);

        // Another shared secret, a damaged content or a missing key header: not a node.
        assert!(to_remote(header.clone(), &[4; 16]).is_err());
        let mut damaged = header.clone();
        damaged.file_metadata.app_data.content = Some(kh.seal_text("{not json"));
        assert!(to_remote(damaged, &SS).is_err());
        let mut keyless = header.clone();
        keyless.shared_secret_encrypted_key_header = None;
        assert!(to_remote(keyless, &SS).is_err());
        let mut no_iv = header.clone();
        no_iv
            .file_metadata
            .payloads
            .iter_mut()
            .flatten()
            .for_each(|p| p.iv = None);
        assert!(to_remote(no_iv, &SS).is_err());

        // Without its size, the stored (padded) size would be taken for the file's.
        let mut sizeless = header;
        let sealed = sizeless
            .file_metadata
            .app_data
            .content
            .as_deref()
            .ok_or("content")?;
        let mut content = NodeContent::from_json(&kh.open_text(sealed)?)?;
        content.size = None;
        sizeless.file_metadata.app_data.content = Some(kh.seal_text(&content.to_json()));
        let why = to_remote(sizeless, &SS).err().ok_or("decoded")?;
        assert!(why.contains("no size"), "{why}");
        Ok(())
    }

    /// The key header is read only when the file is encrypted: an unusable one elsewhere
    /// (null fields, as a tombstone may carry) doesn't make the row unreadable.
    #[test]
    fn unusable_key_headers_only_matter_when_encrypted() -> Result<(), Box<dyn std::error::Error>> {
        let with_key_header = |name: &str, key_header: Value| -> Result<FileHeader, String> {
            let path = format!(
                "{}/../../docs/spec/fixtures/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
            let mut row: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            row["sharedSecretEncryptedKeyHeader"] = key_header;
            serde_json::from_value(row).map_err(|e| e.to_string())
        };
        let nulls = json!({ "iv": null, "encryptedAesKey": null, "encryptionVersion": null });
        for name in ["tombstone.json", "file-node.json", "folder-node.json"] {
            for key_header in [nulls.clone(), json!(null), json!("junk")] {
                let row = with_key_header(name, key_header.clone())?;
                assert!(row.shared_secret_encrypted_key_header.is_none());
                to_remote(row, &SS).map_err(|e| format!("{name} with {key_header}: {e}"))?;
            }
        }
        // An encrypted file needs it.
        let mut encrypted = with_key_header("file-node.json", nulls)?;
        encrypted.file_metadata.is_encrypted = true;
        let why = to_remote(encrypted, &SS).err().ok_or("decoded")?;
        assert!(why.contains("no key header"), "{why}");
        Ok(())
    }

    #[test]
    fn upload_parts() -> Result<(), Box<dyn std::error::Error>> {
        let hash = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let iv = [7u8; 16];
        let create = instructions(&iv, Some(&PayloadDescriptor::plain(&hash)), false);
        assert_eq!(create["transferIv"], STANDARD.encode(iv));
        let d = &create["manifest"]["payloadDescriptors"][0];
        assert_eq!(
            (d["payloadKey"].as_str(), d["hash"]["algorithm"].as_str()),
            (Some("sync_data"), Some("blake3"))
        );
        assert!(create.get("locale").is_none() && d.get("payloadUpdateOperationType").is_none());
        assert!(d.get("iv").is_none());
        assert_eq!(d["hash"]["storedHash"], d["hash"]["contentHash"]);
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
        let sealed = metadata(&meta, Some(version), &key, &iv, None);
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
        assert_eq!(plain["fileMetadata"]["isEncrypted"], false);
        Ok(())
    }

    #[test]
    fn encrypted_upload_parts() -> Result<(), Box<dyn std::error::Error>> {
        let kh = KeyHeader::random()?;
        let (payload_iv, transfer_iv) = (sealed::strong()?, sealed::strong()?);
        let content = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let stored = hbsync_fileio::hash_bytes(b"ciphertext", HashAlgorithm::Blake3);
        let p = PayloadDescriptor::sealed(&kh, payload_iv, &content, &stored);
        let d = &instructions(&transfer_iv, Some(&p), true)["manifest"]["payloadDescriptors"][0];
        assert_eq!(d["iv"], STANDARD.encode(payload_iv));
        assert_eq!(d["hash"]["storedHash"], stored.to_base64());
        let sealed_hash = d["hash"]["contentHash"].as_str().ok_or("hash")?;
        assert_eq!(
            kh.open_hash(&payload_iv, HashAlgorithm::Blake3, sealed_hash)?,
            content
        );

        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file("Résumé.txt", 1, 1, false),
        };
        let sealed = metadata(&meta, None, &SS, &transfer_iv, Some(&kh));
        let plain: Value = serde_json::from_slice(&aes_cbc_decrypt(&SS, &transfer_iv, &sealed)?)?;
        assert_eq!(plain["fileMetadata"]["isEncrypted"], true);
        let key: SealedKeyHeader = serde_json::from_value(plain["encryptedKeyHeader"].clone())?;
        assert_eq!(KeyHeader::open(&key, &SS)?, kh);
        let content = plain["fileMetadata"]["appData"]["content"]
            .as_str()
            .ok_or("content")?;
        assert_eq!(kh.open_text(content)?, meta.content.to_json());
        Ok(())
    }
}
