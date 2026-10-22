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
mod folder;
pub mod gone;
pub mod held;
mod holding;
mod id;
pub mod mask;
pub mod mirror;
pub mod pages;
pub mod plan;
pub mod policy;
pub mod reconcile;
pub mod refused;
pub mod root;
pub mod scope;
mod settle;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod time;
pub mod transfer;
pub mod tree;
pub mod view;
mod waiting;
pub mod walk;

pub use drive::Drive;
pub use engine::{Engine, EngineConfig, StartupReport, SyncReport};
pub use error::EngineError;
pub use folder::FolderCtx;
pub use plan::{
    Against, Agreement, BaseExtra, Conflict, Coverage, DeleteGuard, DriveVersion, Guarding, Input,
    Masking, Op, Options, Plan, Requirement,
};
pub use reconcile::reconcile;
pub use tree::{Tree, Violation, conflict_copy_name, numbered_conflict_name};

/// `m`, locked: a lock a panic left poisoned is taken all the same (what it guards holds no
/// half-done state a later holder would trip on: the state DB rolls back, the rest are plain
/// values).
pub(crate) fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
