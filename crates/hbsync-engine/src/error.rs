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
