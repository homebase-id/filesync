//! Streaming file I/O with constant memory: content hashes computed while the bytes flow,
//! downloads into resumable temp files, and an atomic swap into place.

mod file;
mod hash;
mod small;

pub use file::{CompletedFile, FileIoError, HashedFile, PartialFile, hash_file, sync_completed};
pub use hash::{Hasher, hash_bytes, hash_reader};
pub use hbsync_types::{ContentHash, HashAlgorithm};
pub use small::{write_atomic, write_if_changed};
