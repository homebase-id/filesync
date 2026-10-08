//! The sync engine. `tree` and `reconcile` are pure; everything that touches the disk, the
//! state DB or the remote sits in the executor and the start-up orchestrator.

pub mod reconcile;
pub mod tree;

pub use reconcile::{BaseExtra, Conflict, Input, Op, Options, Plan, RemoteRef, reconcile};
pub use tree::{Tree, Violation, conflict_copy_name, numbered_conflict_name};
