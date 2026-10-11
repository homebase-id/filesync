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

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::HomebaseError;
use crate::crypto::{Exchange, Key, aes_cbc_decrypt, random};

const CALLBACK_PATH: &str = "/authorization-code-callback";

/// What a device keeps after authorizing: who it talks to and how.
#[derive(Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// The identity's domain, e.g. `frodo.dotyou.cloud`.
    pub identity: String,
    /// The client auth token (CAT), base64: sent as `Authorization: Bearer`.
    pub client_auth_token: String,
    /// The shared secret, base64: the key of every request and response envelope.
    pub shared_secret: String,
    /// The app's own circle, once created.
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
        let dir = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir)?;
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        tmp.write_all(&serde_json::to_vec_pretty(self)?)?;
        tmp.as_file().sync_all()?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }
}

/// A YouAuth flow waiting for the browser to come back.
#[derive(Debug)]
pub struct Pending {
    identity: String,
    exchange: Exchange,
    state: String,
    listener: TcpListener,
    /// Open this in the browser.
    pub authorize_url: String,
    /// Where the browser goes once the identity is back (`?error=` added on failure); else it
    /// shows a line of text.
    return_to: Option<String>,
}

/// Starts authorizing this device for the app on `identity` (a domain such as
/// `frodo.dotyou.cloud`). `friendly_name` names the device in the owner's list.
pub fn begin(identity: &str, friendly_name: &str) -> Result<Pending, HomebaseError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://localhost:{port}{CALLBACK_PATH}");
    let exchange = Exchange::new()?;
    let state = uuid::Uuid::from_bytes(random::<16>()?).simple().to_string();
    let request = crate::app::permission_request(friendly_name, &redirect_uri);
    let params = [
        ("client_id", crate::app::APP_ID.simple().to_string()),
        ("client_type", "app".to_owned()),
        ("client_info", friendly_name.to_owned()),
        ("public_key", exchange.public_jwk_base64url()),
        ("permission_request", request.to_string()),
        ("state", state.clone()),
        ("redirect_uri", redirect_uri),
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
        listener,
        authorize_url,
        return_to: None,
    })
}

impl Pending {
    /// Sends the browser to `url` once the identity is back, instead of showing a line of text
    /// (`?error=<why>` added when connecting failed).
    pub fn return_to(mut self, url: &str) -> Self {
        self.return_to = Some(url.to_owned());
        self
    }

    /// Waits up to `timeout` for the browser's callback, then exchanges it for credentials.
    pub fn finish(self, timeout: Duration) -> Result<Credentials, HomebaseError> {
        let deadline = Instant::now() + timeout;
        self.listener.set_nonblocking(true)?;
        loop {
            let (stream, _) = match self.listener.accept() {
                Ok(conn) => conn,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Err(HomebaseError::Protocol(
                            "no answer from the browser in time".into(),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            stream.set_nonblocking(false)?;
            // Anything else is ignored: a favicon request, or a TLS handshake from a browser
            // that tries https first (Firefox's HTTPS-First) before falling back to http.
            let Some(query) = read_callback(&stream) else {
                let _ = respond(stream, 404, "Not here.");
                continue;
            };
            let result = self.complete(&query);
            match (&self.return_to, &result) {
                (Some(url), Ok(_)) => redirect(stream, url)?,
                (Some(url), Err(e)) => {
                    let why = utf8_percent_encode(&e.to_string(), NON_ALPHANUMERIC).to_string();
                    let sep = if url.contains('?') { '&' } else { '?' };
                    redirect(stream, &format!("{url}{sep}error={why}"))?;
                }
                (None, Ok(_)) => respond(
                    stream,
                    200,
                    "Homebase Filesync is connected. You can close this window.",
                )?,
                (None, Err(e)) => respond(
                    stream,
                    200,
                    &format!("Homebase Filesync could not connect: {e}"),
                )?,
            }
            return result;
        }
    }

    fn complete(&self, query: &[(String, String)]) -> Result<Credentials, HomebaseError> {
        let get = |name: &str| {
            query
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
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

/// The query of a `GET /authorization-code-callback?...` request; `None` for anything else.
fn read_callback(stream: &TcpStream) -> Option<Vec<(String, String)>> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut line = Vec::new();
    BufReader::new(stream).read_until(b'\n', &mut line).ok()?;
    let line = String::from_utf8(line).ok()?;
    let mut parts = line.split_whitespace();
    if parts.next() != Some("GET") {
        return None;
    }
    let target = parts.next().unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if path != CALLBACK_PATH {
        return None;
    }
    let decode = |s: &str| {
        percent_decode_str(&s.replace('+', " "))
            .decode_utf8_lossy()
            .into_owned()
    };
    Some(
        query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (decode(k), decode(v))
            })
            .collect(),
    )
}

fn redirect(mut stream: TcpStream, url: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 303 See Other\r\nLocation: {url}\r\nContent-Length: 0\r\n\
         Connection: close\r\n\r\n"
    )
}

fn respond(mut stream: TcpStream, status: u16, text: &str) -> std::io::Result<()> {
    let body = format!(
        "<!doctype html><meta charset=utf-8><title>Homebase Filesync</title>\
         <p style=\"font:16px system-ui;margin:3em\">{text}</p>"
    );
    write!(
        stream,
        "HTTP/1.1 {status} OK\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
