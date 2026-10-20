//! Streaming file I/O with constant memory: content hashes computed while the bytes flow,
//! downloads into resumable temp files, and an atomic swap into place.

pub mod file;
pub mod hash;
pub mod small;

pub use file::{CompletedFile, FileIoError, HashedFile, PartialFile, hash_file, sync_completed};
pub use hash::{ContentHash, HashAlgorithm, Hasher, hash_bytes, hash_reader};
pub use small::{FNV1A64_OFFSET, fnv1a64, fnv1a64_from, write_atomic, write_if_changed};
