//! The Homebase side of the client: registering the Filesync app on an identity and
//! authorizing this device (YouAuth), the shared-secret envelope every API call uses, a small
//! V2 API client, the drive as a `Remote` ([`drive`]), and the notification WebSocket
//! ([`notify`]).
//!
//! Facts about the Homebase API here were verified in `../odin-core` unless a comment says
//! otherwise; see also `docs/spec/homebase-api-notes.md`.

pub mod app;
mod client;
mod connections;
mod crypto;
pub mod drive;
mod multipart;
pub mod notify;
mod problem;
pub mod youauth;

pub use client::Client;
pub use problem::ApiError;
pub use youauth::{Credentials, Pending};

/// Its class and `Retry-After` (`impl HomebaseError` in [`drive`]) sit next to the mapping onto
/// the trait's errors that they follow.
#[derive(Debug, thiserror::Error)]
pub enum HomebaseError {
    /// The request didn't get an answer: no connection, a timeout, a dropped stream. Worth
    /// retrying.
    #[error("network: {0}")]
    Network(String),
    /// The identity answered with an error.
    #[error("{what}: {error}")]
    Api { what: String, error: ApiError },
    /// The owner refused, or the credentials no longer work (401/403).
    #[error("denied: {0}")]
    Denied(String),
    /// The identity has the app but no drive at its address (`invalidDrive`): deleted, say, or
    /// made by another app. The owner can give the app its drive again
    /// ([`app::restore_drive_url`]).
    #[error("Filesync's drive isn't on {0}")]
    NoDrive(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("crypto: {0}")]
    Crypto(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
