//! Local observation: what is on disk, and what changed since the last look.
//!
//! All sources (the tier-C walk now; journals, watchers and File Provider later) end in the same
//! place: `local_items` rows in the state DB plus the dirty set.

pub mod scan;
pub mod walk;

pub use scan::{ScanError, ScanOptions, ScanReport, scan};
pub use walk::{SkipReason, WalkEvent, Walker};

/// The client's own folder at the top of the sync root (state, temp files, trash). Never synced.
pub const STATE_DIR: &str = ".hbsync";
