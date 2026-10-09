//! The Homebase side of the client: registering the Filesync app on an identity and
//! authorizing this device (YouAuth), the shared-secret envelope every API call uses, and a
//! small V2 API client. The drive adapter behind the `Remote` trait comes next (Layer 5).
//!
//! Facts about the Homebase API here were verified in `../odin-core` unless a comment says
//! otherwise; see also `docs/spec/homebase-api-notes.md`.

pub mod app;
pub mod client;
pub mod crypto;
pub mod youauth;

pub use client::Client;
pub use youauth::{Credentials, Pending, begin};

#[derive(Debug, thiserror::Error)]
pub enum HomebaseError {
    #[error("HTTP: {0}")]
    Http(String),
    #[error("denied: {0}")]
    Denied(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("crypto: {0}")]
    Crypto(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
