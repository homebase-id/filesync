//! Registering the app on an identity and authorizing this device: the YouAuth flow, as
//! odin-core runs it (verified in `YouAuthUnifiedController`, `YouAuthUnifiedService`) and
//! chat-kmp's desktop client does it.
//!
//! 1. Open `https://{identity}/api/owner/v1/youauth/authorize` in the browser, with our P-384
//!    public key and the app's permission request. The owner logs in; a new app goes through
//!    the registration page first (which creates the requested drives), then comes back.
//! 2. The identity redirects to our localhost callback with its own public key and a salt.
//! 3. Both sides derive the exchange key; we prove it with its SHA-256 at
//!    `POST /api/owner/v1/youauth/token` and get the client auth token and the shared secret,
//!    AES-CBC encrypted under the exchange key, and the app's zero-knowledge key, sealed to our
//!    public key (asked for with `zk_app_key=1`: odin-core
//!    `docs/zero-knowledge-drive-api-plan.md`, 3.5).

use std::collections::HashMap;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::HomebaseError;
use crate::crypto::{Exchange, Key, aes_cbc_decrypt, random};

/// What a device keeps after authorizing: who it talks to and how.
#[derive(Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// The identity's domain, e.g. `frodo.dotyou.cloud`.
    pub identity: String,
    /// The client auth token (CAT), base64: sent as `Authorization: Bearer`.
    pub client_auth_token: String,
    /// The shared secret, base64: the key of every request and response envelope.
    pub shared_secret: String,
    /// The app's zero-knowledge key (AppZkK), base64: it opens the drive's key once the drive is
    /// private (zero knowledge). `None` for a login from before this client asked for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zk_app_key: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// Which login these are, without saying anything of the secret: a fingerprint of the
    /// shared secret (FNV-1a, stable across builds). A new login gets a new shared secret, so
    /// what was sealed under the old one (the files' tokens) is known not to open.
    pub fn login_id(&self) -> String {
        format!(
            "{:016x}",
            hbsync_platform::fnv::fnv1a64(self.shared_secret.as_bytes())
        )
    }

    pub(crate) fn shared_secret_key(&self) -> Result<Key, HomebaseError> {
        key16(&self.shared_secret, "the shared secret")
    }

    /// The app's zero-knowledge key, if this login was handed one.
    pub(crate) fn zk_key(&self) -> Result<Option<Key>, HomebaseError> {
        self.zk_app_key
            .as_deref()
            .map(|k| key16(k, "the zero-knowledge key"))
            .transpose()
    }

    pub fn load(path: &Path) -> Result<Option<Self>, HomebaseError> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Saves to `path` in one step (a crash leaves the old file or the new one), readable by
    /// this user only (0600 on Unix). The product keeps these in the OS keychain when there is
    /// one.
    pub fn save(&self, path: &Path) -> Result<(), HomebaseError> {
        hbsync_fileio::write_atomic(path, &serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

/// `what`, base64 of a 16-byte key.
fn key16(b64: &str, what: &str) -> Result<Key, HomebaseError> {
    STANDARD
        .decode(b64)
        .ok()
        .and_then(|k| k.try_into().ok())
        .ok_or_else(|| HomebaseError::Protocol(format!("{what} isn't 16 bytes")))
}

/// A YouAuth flow waiting for the identity's answer, which comes back to `redirect_uri` (a
/// loopback page the caller serves: [`Pending::complete`] takes its query).
#[derive(Debug)]
pub struct Pending {
    identity: String,
    exchange: Exchange,
    state: String,
    /// Open this in the browser.
    pub authorize_url: String,
}

/// Starts authorizing this device for the app on `identity` (a domain such as
/// `frodo.dotyou.cloud`). `friendly_name` names the device in the owner's list; the identity
/// sends the browser back to `redirect_uri`.
pub fn begin(
    identity: &str,
    friendly_name: &str,
    redirect_uri: &str,
) -> Result<Pending, HomebaseError> {
    let exchange = Exchange::new()?;
    let state = uuid::Uuid::from_bytes(random::<16>()?).simple().to_string();
    let request = crate::app::permission_request(friendly_name, redirect_uri);
    let query = crate::query(&[
        ("client_id", &crate::app::APP_ID.simple().to_string()),
        ("client_type", "app"),
        ("client_info", friendly_name),
        ("public_key", &exchange.public_jwk_base64url()),
        ("zk_app_key", "1"),
        ("permission_request", &request.to_string()),
        ("state", &state),
        ("redirect_uri", redirect_uri),
    ]);
    let authorize_url = format!("https://{identity}/api/owner/v1/youauth/authorize?{query}");
    Ok(Pending {
        identity: identity.to_owned(),
        exchange,
        state,
        authorize_url,
    })
}

impl Pending {
    /// The identity's answer, the query of the request it sent the browser back with,
    /// exchanged for credentials.
    pub fn complete(&self, query: &HashMap<String, String>) -> Result<Credentials, HomebaseError> {
        let get = |name: &str| query.get(name).map(String::as_str);
        if get("state") != Some(self.state.as_str()) {
            return Err(HomebaseError::Protocol(
                "callback for another request".into(),
            ));
        }
        if let Some(error) = get("error") {
            return Err(HomebaseError::Denied(format!(
                "{error}: {}",
                get("error_description").unwrap_or("")
            )));
        }
        let (Some(peer), Some(salt)) = (get("public_key"), get("salt")) else {
            return Err(HomebaseError::Protocol(
                "callback without public key or salt".into(),
            ));
        };
        let salt = STANDARD
            .decode(salt)
            .map_err(|_| HomebaseError::Protocol("salt isn't base64".into()))?;
        let key = self.exchange.derive(peer, &salt)?;
        let digest = STANDARD.encode(sha2::Sha256::digest(key));

        let url = format!("https://{}/api/owner/v1/youauth/token", self.identity);
        let body = serde_json::json!({ "secret_digest": digest });
        let answer: TokenResponse = crate::client::http_json(
            crate::client::agent().post(&url).send_json(&body),
            "exchanging the YouAuth token",
        )?;
        if answer.cipher.as_deref().is_some_and(|c| c != "aes-cbc") {
            return Err(HomebaseError::Protocol(format!(
                "token sealed with {:?}, expected aes-cbc",
                answer.cipher
            )));
        }
        let open = |cipher: &str, iv: &str| -> Result<String, HomebaseError> {
            let bad = |_| HomebaseError::Protocol("token response isn't base64".into());
            let plain = aes_cbc_decrypt(
                &key,
                &STANDARD.decode(iv).map_err(bad)?,
                &STANDARD.decode(cipher).map_err(bad)?,
            )?;
            Ok(STANDARD.encode(plain))
        };
        // Asked for: a login without it couldn't read the drive once it is private.
        let sealed = answer.zk_app_key.as_deref().ok_or_else(|| {
            HomebaseError::Protocol(
                "the identity didn't hand over the app's zero-knowledge key".into(),
            )
        })?;
        let zk_app_key = self.exchange.unseal(sealed)?;
        if zk_app_key.len() != 16 {
            return Err(HomebaseError::Protocol(
                "the app's zero-knowledge key isn't 16 bytes".into(),
            ));
        }
        Ok(Credentials {
            identity: self.identity.clone(),
            client_auth_token: open(
                &answer.base64_client_auth_token_cipher,
                &answer.base64_client_auth_token_iv,
            )?,
            shared_secret: open(
                &answer.base64_shared_secret_cipher,
                &answer.base64_shared_secret_iv,
            )?,
            zk_app_key: Some(STANDARD.encode(zk_app_key)),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponse {
    base64_shared_secret_cipher: String,
    base64_shared_secret_iv: String,
    base64_client_auth_token_cipher: String,
    base64_client_auth_token_iv: String,
    #[serde(default)]
    cipher: Option<String>,
    /// The app's zero-knowledge key, sealed to our public key (`ZkSeal`).
    #[serde(default)]
    zk_app_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A login's id is stored (the files' tokens are sealed by it): it never changes between
    /// builds, and a new shared secret gives a new one.
    #[test]
    fn a_logins_id_is_stable() {
        let creds = |secret: &str| Credentials {
            identity: "a.example".into(),
            client_auth_token: "cat".into(),
            shared_secret: secret.into(),
            zk_app_key: None,
        };
        assert_eq!(
            creds("AAAAAAAAAAAAAAAAAAAAAA==").login_id(),
            "475ad89adabbaa9d"
        );
        assert_ne!(
            creds("AAAAAAAAAAAAAAAAAAAAAA==").login_id(),
            creds("AQAAAAAAAAAAAAAAAAAAAA==").login_id()
        );
    }
}
