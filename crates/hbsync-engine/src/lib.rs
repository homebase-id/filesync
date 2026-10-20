//! The sync engine. `tree` and `reconcile` are pure; `view`, `exec`, `feed` and `engine` are
//! the shell that reads and writes the state DB, the disk and the remote.

pub mod activity;
pub mod adopt;
pub mod along;
pub mod deleted;
pub mod drive;
mod engine;
mod error;
pub mod exec;
pub mod feed;
mod id;
pub mod mask;
pub mod plan;
pub mod reconcile;
pub mod root;
pub mod scope;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod transfer;
mod trash;
pub mod tree;
pub mod view;
pub mod walk;

pub use drive::Drive;
pub use engine::{Engine, EngineConfig, StartupReport, SyncReport};
pub use error::EngineError;
pub use plan::{
    Against, Agreement, BaseExtra, Conflict, Coverage, DriveVersion, Input, Op, Options, Plan,
    Requirement,
};
pub use reconcile::reconcile;
pub use tree::{Tree, Violation, conflict_copy_name, numbered_conflict_name};
