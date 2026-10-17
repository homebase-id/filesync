//! The sync engine. `tree` and `reconcile` are pure; `view`, `exec`, `feed` and `engine` are
//! the shell that reads and writes the state DB, the disk and the remote.

pub mod activity;
pub mod adopt;
pub mod deleted;
pub mod drive;
pub mod engine;
pub mod exec;
pub mod feed;
pub mod mask;
pub mod plan;
pub mod reconcile;
pub mod root;
pub mod scope;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod transfer;
pub mod tree;
pub mod view;
pub mod walk;

pub use drive::Drive;
pub use engine::{Engine, EngineConfig, EngineError, StartupReport, SyncReport};
pub use plan::{
    Against, Agreement, BaseExtra, Conflict, DriveVersion, Input, Op, Options, Plan, Requirement,
};
pub use reconcile::reconcile;
pub use tree::{Tree, Violation, conflict_copy_name, numbered_conflict_name};
