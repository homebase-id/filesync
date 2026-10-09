//! The sync engine. `tree` and `reconcile` are pure; `view`, `exec`, `feed` and `engine` are
//! the shell that reads and writes the state DB, the disk and the remote.

pub mod adopt;
pub mod engine;
pub mod exec;
pub mod feed;
pub mod reconcile;
pub mod transfer;
pub mod tree;
pub mod view;

pub use engine::{Engine, EngineConfig, EngineError, StartupReport, SyncReport};
pub use reconcile::{BaseExtra, Conflict, Input, Op, Options, Plan, reconcile};
pub use tree::{Tree, Violation, conflict_copy_name, numbered_conflict_name};
