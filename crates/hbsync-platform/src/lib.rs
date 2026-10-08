//! Platform layer: hides how Windows, macOS and Linux differ.
//!
//! Pure rules (name legality, comparison keys, long paths) take the target [`Os`] as a
//! parameter so they are tested on every OS; code that calls into the OS is `cfg`-gated.

pub mod name;
pub mod relpath;
pub mod winpath;

pub use name::{NameIssue, Os, check_local_name, check_remote_name, name_key, nfc};
pub use relpath::{RelPath, path_cmp};
