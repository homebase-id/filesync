//! Local observation: what is on disk, and what changed since the last look.
//!
//! All sources (the tier-C walk now; journals, watchers and File Provider later) end in the same
//! place: `local` rows in the state DB plus the dirty set.

pub mod filter;
pub mod journal;
pub mod layout;
pub mod marker;
pub mod scan;
pub mod walk;
pub mod watch;

pub use filter::{Filter, FilterError, Verdict, is_junk};
pub use journal::{CatchUp, ChangeJournal, Tier, catch_up, open_journal};
pub use layout::IGNORE_FILE;
pub use marker::Marker;
pub use scan::{ScanOptions, ScanReport, rescan_dirs, scan};
pub use walk::{SkipReason, WalkEvent, Walker};
pub use watch::{Debounce, LiveWatcher, LocalObserver, WatchBatch};

/// The client's own folder at the top of the sync root (its marker, ignore rules, temp files,
/// trash). Never synced. The state DB is the account's, elsewhere.
pub const STATE_DIR: &str = ".hbsync";
