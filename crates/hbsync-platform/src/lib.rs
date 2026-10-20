//! Platform layer: hides how Windows, macOS and Linux differ.
//!
//! Pure rules (name legality, comparison keys, long paths) take the target [`Os`] as a
//! parameter so they are tested on every OS; code that calls into the OS is `cfg`-gated.

mod errors;
pub mod fnv;
pub mod fskind;
pub mod host;
pub mod journal;
pub mod listdir;
pub mod meta;
pub mod name;
pub mod ops;
mod relpath;
pub mod tags;
pub mod user_dirs;
pub mod winpath;

pub use errors::{IoClass, classify};
pub use fskind::FsFamily;
pub use meta::{EntryKind, FileId, Fingerprint, LocalMeta, nothing_there, stat};
pub use name::{NameIssue, Os, check_local_name, check_remote_name, name_key, nfc};
pub use relpath::RelPath;
