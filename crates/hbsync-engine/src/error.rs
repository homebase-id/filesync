//! The engine's errors: a folder's sync, and the account's operations on its drive (Recently
//! deleted, the purge, a role's folder).

use hbsync_remote::{ErrorClass, RemoteError};
use hbsync_state::StateError;

use crate::exec::Stopped;
use crate::feed::FeedError;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Feed(#[from] FeedError),
    #[error("{0} panicked")]
    Panic(&'static str),
    /// The executor stopped early: access denied, or the remote unreachable.
    #[error(transparent)]
    Stopped(#[from] Stopped),
    /// A remote call outside a sync (Recently deleted) failed.
    #[error(transparent)]
    Remote(#[from] RemoteError),
    /// A restore found `files` not there, the identity's first word on it (sync semantics
    /// §8b): a suspicion only, the rows stay; asked again a minute on, each is gone for good
    /// (or there). `rest`: the rows of the restore still in Recently deleted, with their names,
    /// to restore again; `went`: the names of those it found deleted for good meanwhile.
    #[error("{}", not_there(*files))]
    NotThereOnce {
        files: usize,
        rest: Vec<(hbsync_types::NodeId, String)>,
        went: Vec<String>,
    },
}

/// "Homebase says one item isn't there", or "… 3 items aren't there".
fn not_there(files: usize) -> String {
    match files {
        1 => "Homebase says one item isn't there".into(),
        n => format!("Homebase says {n} items aren't there"),
    }
}

impl EngineError {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            EngineError::State(e) => e.class(),
            EngineError::Feed(e) => e.class(),
            EngineError::Panic(_) => ErrorClass::Bug,
            EngineError::Stopped(e) => e.class(),
            EngineError::Remote(e) => e.class(),
            EngineError::NotThereOnce { .. } => ErrorClass::Gone,
        }
    }

    /// When the server said to ask again, if it is busy or paused.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            EngineError::Stopped(Stopped::Later(_, after)) => Some(*after),
            EngineError::Feed(FeedError::Remote(e)) | EngineError::Remote(e)
                if e.class() == ErrorClass::RateLimited =>
            {
                e.retry_after()
            }
            _ => None,
        }
    }
}
