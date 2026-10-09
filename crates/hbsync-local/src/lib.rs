//! Local observation: what is on disk, and what changed since the last look.
//!
//! All sources (the tier-C walk now; journals, watchers and File Provider later) end in the same
//! place: `local` rows in the state DB plus the dirty set.

pub mod journal;
pub mod scan;
pub mod walk;
pub mod watch;

pub use journal::{CatchUp, ChangeJournal, Tier, catch_up, open_journal};
pub use scan::{ScanOptions, ScanReport, rescan_dirs, scan};
pub use walk::{SkipReason, WalkEvent, Walker};
pub use watch::{Debounce, LiveWatcher, LocalObserver, WatchBatch};

/// The client's own folder at the top of the sync root (state, temp files, trash). Never synced.
pub const STATE_DIR: &str = ".hbsync";
