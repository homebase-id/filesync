//! Column encodings.

use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_types::{
    ContentHash, ContentState, HashAlgorithm, NodeId, NodeKind, RemoteFileId, Uuid, VersionTag,
};

use crate::{Result, StateError};

pub fn uuid_from(bytes: &[u8]) -> Result<Uuid> {
    Uuid::from_slice(bytes)
        .map_err(|_| StateError::Corrupt(format!("bad uuid ({} bytes)", bytes.len())))
}

pub fn node(bytes: &[u8]) -> Result<NodeId> {
    uuid_from(bytes).map(NodeId)
}

pub fn remote_file(bytes: &[u8]) -> Result<RemoteFileId> {
    uuid_from(bytes).map(RemoteFileId)
}

pub fn version(bytes: &[u8]) -> Result<VersionTag> {
    uuid_from(bytes).map(VersionTag)
}

pub fn rel_path(s: &str) -> Result<RelPath> {
    RelPath::new(s).map_err(|e| StateError::Corrupt(format!("path {s:?}: {e}")))
}

pub fn kind_to(kind: NodeKind) -> i64 {
    match kind {
        NodeKind::File => 0,
        NodeKind::Folder => 1,
    }
}

pub fn kind_from(v: i64) -> Result<NodeKind> {
    match v {
        0 => Ok(NodeKind::File),
        1 => Ok(NodeKind::Folder),
        _ => Err(StateError::Corrupt(format!("bad kind {v}"))),
    }
}

pub fn content_state_to(s: ContentState) -> i64 {
    match s {
        ContentState::Local => 0,
        ContentState::Pinned => 1,
        ContentState::OnlineOnly => 2,
    }
}

pub fn content_state_from(v: i64) -> Result<ContentState> {
    match v {
        0 => Ok(ContentState::Local),
        1 => Ok(ContentState::Pinned),
        2 => Ok(ContentState::OnlineOnly),
        _ => Err(StateError::Corrupt(format!("bad content state {v}"))),
    }
}

/// Algorithm byte (1 = SHA-256, 2 = BLAKE3, the numeric forms Homebase accepts) + 32 digest
/// bytes.
pub fn hash_to(h: &ContentHash) -> Vec<u8> {
    let mut out = Vec::with_capacity(33);
    out.push(match h.algorithm {
        HashAlgorithm::Sha256 => 1,
        HashAlgorithm::Blake3 => 2,
    });
    out.extend_from_slice(&h.bytes);
    out
}

pub fn hash_from(bytes: &[u8]) -> Result<ContentHash> {
    let (alg, digest) = bytes
        .split_first()
        .ok_or_else(|| StateError::Corrupt("empty hash".into()))?;
    let algorithm = match alg {
        1 => HashAlgorithm::Sha256,
        2 => HashAlgorithm::Blake3,
        _ => return Err(StateError::Corrupt(format!("bad hash algorithm {alg}"))),
    };
    let bytes: [u8; 32] = digest
        .try_into()
        .map_err(|_| StateError::Corrupt("bad hash length".into()))?;
    Ok(ContentHash { algorithm, bytes })
}

pub fn file_id_bytes(id: &FileId) -> [u8; 16] {
    id.file.to_be_bytes()
}

pub fn fingerprint_from(
    size: i64,
    mtime_ns: i64,
    ctime_ns: i64,
    volume: i64,
    file_id: &[u8],
) -> Result<Fingerprint> {
    let file: [u8; 16] = file_id
        .try_into()
        .map_err(|_| StateError::Corrupt("bad file id".into()))?;
    Ok(Fingerprint {
        size: size as u64,
        mtime_ns,
        ctime_ns,
        // u64 ids are stored bit-for-bit in SQLite's signed INTEGER.
        file_id: FileId {
            volume: volume as u64,
            file: u128::from_be_bytes(file),
        },
    })
}
