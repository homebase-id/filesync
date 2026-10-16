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
//!    AES-CBC encrypted under the exchange key.

use std::collections::HashMap;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
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
    /// The app's own circle (slug `filesync`), once found.
    #[serde(default)]
    pub circle_id: Option<uuid::Uuid>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("identity", &self.identity)
            .field("circle_id", &self.circle_id)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    pub fn shared_secret_key(&self) -> Result<Key, HomebaseError> {
        STANDARD
            .decode(&self.shared_secret)
            .ok()
            .and_then(|k| k.try_into().ok())
            .ok_or_else(|| HomebaseError::Protocol("the shared secret isn't 16 bytes".into()))
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
    let params = [
        ("client_id", crate::app::APP_ID.simple().to_string()),
        ("client_type", "app".to_owned()),
        ("client_info", friendly_name.to_owned()),
        ("public_key", exchange.public_jwk_base64url()),
        ("permission_request", request.to_string()),
        ("state", state.clone()),
        ("redirect_uri", redirect_uri.to_owned()),
    ];
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", utf8_percent_encode(v, NON_ALPHANUMERIC)))
        .collect();
    let authorize_url = format!(
        "https://{identity}/api/owner/v1/youauth/authorize?{}",
        query.join("&")
    );
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
            circle_id: None,
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
}
