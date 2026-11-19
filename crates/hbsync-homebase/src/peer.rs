//! What reading another identity's share asks of this identity's own server: a token for their
//! server, and their circle's key sealed to this identity's drive (`docs/spec/homebase-api-notes.md`,
//! "Shared with you").

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::crypto::Envelope;
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

    /// Keeps the token at `path`, sealed under `own`'s shared secret (this login's, which the
    /// keychain holds): the file opens nothing by itself. Written in one step, readable by
    /// this user only.
    pub fn keep(&self, own: &Client, path: &Path) -> Result<(), HomebaseError> {
        let plain = Zeroizing::new(serde_json::to_vec(self)?);
        let sealed = Envelope::seal(own.key(), &plain)?;
        hbsync_fileio::write_atomic(path, &serde_json::to_vec(&sealed)?)?;
        Ok(())
    }

    /// The token kept at `path` ([`PeerToken::keep`]), if there is one that opens under
    /// `own`'s login. None was kept, it was kept under another login (signed in again since)
    /// or the file is damaged: `None`, and a new one is asked for. Only a file that can't be
    /// read is an error: asking again then would leave a row on their server at every try.
    pub fn kept(own: &Client, path: &Path) -> Result<Option<PeerToken>, HomebaseError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let Ok(sealed) = serde_json::from_slice::<Envelope>(&bytes) else {
            return Ok(None);
        };
        let Ok(plain) = sealed.open(own.key()).map(Zeroizing::new) else {
            return Ok(None);
        };
        Ok(serde_json::from_slice(&plain).ok())
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

    fn login(shared_secret: &str) -> Client {
        Client::new(&crate::Credentials {
            identity: "tom.example".into(),
            client_auth_token: "dG9rZW4=".into(),
            shared_secret: shared_secret.into(),
            zk_app_key: None,
        })
        .expect("a client")
    }

    fn token() -> PeerToken {
        PeerToken {
            token: "dGhlaXIgdG9rZW4=".into(),
            shared_secret: "AQEBAQEBAQEBAQEBAQEBAQ==".into(),
        }
    }

    /// A token kept is read back under the same login, and the file shows nothing of it.
    #[test]
    fn a_token_kept_opens_under_its_login_and_the_file_shows_nothing() -> Result<(), HomebaseError>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("token");
        let own = login("AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(PeerToken::kept(&own, &path)?, None, "none kept yet");
        token().keep(&own, &path)?;
        assert_eq!(PeerToken::kept(&own, &path)?, Some(token()));
        let file = std::fs::read_to_string(&path)?;
        assert!(!file.contains(&token().token) && !file.contains(&token().shared_secret));
        Ok(())
    }

    /// Another login's token, or a damaged file, is none: a new one is asked for.
    #[test]
    fn a_token_of_another_login_or_a_damaged_one_is_none() -> Result<(), HomebaseError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("token");
        token().keep(&login("AAAAAAAAAAAAAAAAAAAAAA=="), &path)?;
        let again = login("AgICAgICAgICAgICAgICAg==");
        assert_eq!(PeerToken::kept(&again, &path)?, None);
        for damaged in ["", "{", "{\"iv\":\"x\",\"data\":\"y\"}", "[]"] {
            std::fs::write(&path, damaged)?;
            assert_eq!(PeerToken::kept(&again, &path)?, None, "{damaged:?}");
        }
        Ok(())
    }

    /// A file that can't be read is an error, not a reason to ask their server again.
    #[test]
    fn a_token_that_cant_be_read_is_an_error() -> Result<(), HomebaseError> {
        let dir = tempfile::tempdir()?;
        // A folder where the file would be.
        let path = dir.path().join("token");
        std::fs::create_dir(&path)?;
        assert!(PeerToken::kept(&login("AAAAAAAAAAAAAAAAAAAAAA=="), &path).is_err());
        Ok(())
    }
}
