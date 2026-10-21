//! Local observation: what is on disk, and what changed since the last look.
//!
//! All sources (the walk, the OS change journals, the live watcher; File Provider later) end in
//! the same place: `local` rows in the state DB plus the dirty set.

mod filter;
pub mod journal;
pub mod layout;
mod marker;
pub mod scan;
mod walk;
mod watch;

pub use filter::{Filter, FilterError, Verdict, state_dir_key};
pub use journal::{CatchUp, ChangeJournal, Tier, catch_up, open_journal};
pub use layout::IGNORE_FILE;
pub use marker::Marker;
pub use scan::{ScanOptions, ScanReport, rescan_dirs, rescan_entries, scan};
pub use walk::{SkipReason, WalkEvent, Walker};
pub use watch::{Bell, Debounce, LocalObserver};

/// The client's own folder at the top of the sync root (its marker, ignore rules, temp files,
/// trash). Never synced. The state DB is the account's, elsewhere.
pub const STATE_DIR: &str = ".hbsync";
