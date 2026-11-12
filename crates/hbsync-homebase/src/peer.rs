//! What reading another identity's share asks of this identity's own server: a token for their
//! server, and their circle's key sealed to this identity's drive (`docs/spec/homebase-api-notes.md`,
//! "Shared with you").

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{Client, HomebaseError};

/// A token for one sharer's server, made by it for this identity's connection: it reads there
/// what they share with this identity ([`Client::for_peer`]). Kept: each one asked for stays a
/// row on their server.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerToken {
    /// Sent as `Authorization: Bearer` to their server.
    #[serde(rename = "authenticationToken64")]
    pub token: String,
    /// The key of that server's envelopes, base64.
    pub shared_secret: String,
}

/// Never shown: a secret.
impl std::fmt::Debug for PeerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerToken(..)")
    }
}

/// Whether `error` is the sharer's server refusing a token ([`Client::for_peer`]): unknown, or
/// the connection gone or blocked. Then, and only then, a new one is asked for.
pub fn refused(error: &HomebaseError) -> bool {
    matches!(error, HomebaseError::Api { error, .. } if error.status == 401)
}

impl PeerToken {
    /// A new token for `sharer`'s server, through `own`, this identity's server: `POST
    /// /api/v2/peer/notify/token` (refusals: homebase-api-notes, "Shared with you"). Not
    /// retried blindly: a call whose answer is lost may still have left a row there.
    pub fn new(own: &Client, sharer: &str) -> Result<PeerToken, HomebaseError> {
        own.post("/api/v2/peer/notify/token", &json!({ "identity": sharer }))
    }
}

/// This identity's copy of a sharer's circle key, as their server answers for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealedKey {
    /// Sealed to this identity's drive ([`crate::drive::share_keys`] opens it).
    Sealed(Vec<u8>),
    /// None yet: a device of theirs seals it the next time it is online.
    NotYet,
    /// They haven't reviewed this identity's connection yet.
    NotReviewed,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SealedAnswer {
    sealed_key: Option<String>,
}

impl SealedKey {
    /// `sharer`'s answer for `circle`, through `own`: `GET
    /// /api/v2/peer/{sharer}/circles/{circle}/share-key`. Another refusal (not a member, the
    /// circle off, no longer connected) is an [`HomebaseError::Api`] 403 (homebase-api-notes,
    /// "Shared with you").
    pub fn ask(own: &Client, sharer: &str, circle: Uuid) -> Result<SealedKey, HomebaseError> {
        let route = format!("/api/v2/peer/{sharer}/circles/{circle}/share-key");
        match own.get::<SealedAnswer>(&route, &[]) {
            Ok(SealedAnswer { sealed_key: None }) => Ok(SealedKey::NotYet),
            Ok(SealedAnswer {
                sealed_key: Some(b64),
            }) => match STANDARD.decode(b64) {
                Ok(sealed) if !sealed.is_empty() => Ok(SealedKey::Sealed(sealed)),
                _ => Err(HomebaseError::Protocol(format!(
                    "{sharer}'s circle key isn't a sealed key"
                ))),
            },
            Err(HomebaseError::Api { error, .. }) if error.is("notReviewedByOwner") => {
                Ok(SealedKey::NotReviewed)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_reads_as_the_server_gives_it_and_shows_nothing() {
        let token: PeerToken = serde_json::from_str(
            r#"{"authenticationToken64":"dG9rZW4=","sharedSecret":"AAAAAAAAAAAAAAAAAAAAAA=="}"#,
        )
        .expect("a token");
        assert_eq!(token.token, "dG9rZW4=");
        assert_eq!(format!("{token:?}"), "PeerToken(..)");
    }
}
