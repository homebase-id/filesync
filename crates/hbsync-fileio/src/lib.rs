//! Streaming file I/O with constant memory: content hashes computed while the bytes flow,
//! downloads into resumable temp files, and an atomic swap into place.

pub mod file;
pub mod hash;

pub use file::{CompletedFile, FileIoError, HashedFile, PartialFile, hash_file};
pub use hash::{ContentHash, HashAlgorithm, Hasher, HashingReader, hash_bytes, hash_reader};
