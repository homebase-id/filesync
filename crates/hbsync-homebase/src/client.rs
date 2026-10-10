//! Calls to an identity's V2 API as an authorized app client. JSON calls go inside the
//! shared-secret envelope (verified in odin-core `SharedSecretEncryptionMiddleware`): a GET's
//! query string becomes one `ss` parameter, even when empty; a JSON body becomes `{iv, data}`;
//! a response marked `X-SSE: 1` comes back the same way. Uploads (multipart) and payload
//! reads aren't (encrypted files carry their own encryption).

use std::io::{Read, Write};
use std::time::Duration;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::de::DeserializeOwned;
use ureq::http::Response;

use crate::HomebaseError;
use crate::crypto::{Envelope, Key};
use crate::multipart::Multipart;
use crate::problem::ApiError;
use crate::youauth::Credentials;

/// The largest JSON answer read (a page of the change feed is far below it).
const MAX_JSON: u64 = 256 * 1024 * 1024;

/// One connection pool for all calls. No overall timeout: an upload of a large file takes as
/// long as it takes.
pub(crate) fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_send_request(Some(Duration::from_secs(60)))
        .timeout_recv_response(Some(Duration::from_secs(300)))
        // A body that stalls (a connection that died without a reset) ends the call instead of
        // blocking a worker forever. Answers are bounded: JSON pages, 8 MiB payload ranges.
        .timeout_recv_body(Some(Duration::from_secs(300)))
        .user_agent(concat!("hbsync/", env!("CARGO_PKG_VERSION")))
        .build()
        .into()
}

/// An authorized client of one identity. Cheap to clone; clones share the connection pool.
#[derive(Clone)]
pub struct Client {
    identity: String,
    /// The client auth token (CAT), base64.
    token: String,
    key: Key,
    agent: ureq::Agent,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

/// How a multipart upload is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadMethod {
    Post,
    Patch,
}

impl Client {
    pub fn new(credentials: &Credentials) -> Result<Self, HomebaseError> {
        Ok(Client {
            identity: credentials.identity.clone(),
            token: credentials.client_auth_token.clone(),
            key: credentials.shared_secret_key()?,
            agent: agent(),
        })
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The shared secret: the key of every envelope and of upload metadata.
    pub(crate) fn key(&self) -> &Key {
        &self.key
    }

    /// The client auth token, base64.
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    fn url(&self, path: &str) -> String {
        format!("https://{}{path}", self.identity)
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.token)
    }

    /// `GET {path}` with `query` encrypted into `ss`.
    pub fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, HomebaseError> {
        let qs: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{k}={}", utf8_percent_encode(v, NON_ALPHANUMERIC)))
            .collect();
        let ss = serde_json::to_string(&Envelope::seal(&self.key, qs.join("&").as_bytes())?)?;
        let url = format!(
            "{}?ss={}",
            self.url(path),
            utf8_percent_encode(&ss, NON_ALPHANUMERIC)
        );
        let response = self
            .agent
            .get(&url)
            .header("Authorization", &self.bearer())
            .call();
        self.decode(response, path)
    }

    /// `POST {path}` with `body` in an envelope.
    pub fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, HomebaseError> {
        let sealed = Envelope::seal(&self.key, &serde_json::to_vec(body)?)?;
        let response = self
            .agent
            .post(self.url(path))
            .header("Authorization", &self.bearer())
            .send_json(&sealed);
        self.decode(response, path)
    }

    /// Sends a multipart form (plaintext; the parts carry their own encryption).
    pub fn upload<T: DeserializeOwned>(
        &self,
        method: UploadMethod,
        path: &str,
        form: Multipart<'_>,
    ) -> Result<T, HomebaseError> {
        let url = self.url(path);
        let request = match method {
            UploadMethod::Post => self.agent.post(&url),
            UploadMethod::Patch => self.agent.patch(&url),
        };
        let (content_type, len) = (form.content_type(), form.content_length());
        let mut body = form.into_reader();
        let response = request
            .header("Authorization", &self.bearer())
            .header("Content-Type", &content_type)
            .header("Content-Length", &len.to_string())
            .send(ureq::SendBody::from_reader(&mut body));
        self.decode(response, path)
    }

    /// `GET {path}` with a `Range` of `start..=last`: what the response says about the payload,
    /// and its bytes to read (encrypted payloads come as ciphertext).
    pub fn fetch(&self, path: &str, start: u64, last: u64) -> Result<Fetched, HomebaseError> {
        let response = self
            .agent
            .get(self.url(path))
            .header("Authorization", &self.bearer())
            .header("Range", &format!("bytes={start}-{last}"))
            .call();
        let response = checked(response, path)?;
        // Anything but the range asked for (a proxy that ignored `Range`) must not reach the
        // sink: it would be written at the wrong offset.
        if response.status().as_u16() != 206 {
            return Err(HomebaseError::Protocol(format!(
                "{path}: asked for bytes {start}-{last}, got HTTP {}",
                response.status().as_u16()
            )));
        }
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        Ok(Fetched {
            encrypted: header("PayloadEncrypted").is_some_and(|v| v.eq_ignore_ascii_case("true")),
            key_header: header("SharedSecretEncryptedHeader64"),
            what: path.to_owned(),
            body: response
                .into_body()
                .into_with_config()
                .limit(u64::MAX)
                .reader(),
        })
    }

    fn decode<T: DeserializeOwned>(
        &self,
        response: Result<Response<ureq::Body>, ureq::Error>,
        what: &str,
    ) -> Result<T, HomebaseError> {
        let mut response = checked(response, what)?;
        let encrypted = response
            .headers()
            .get("X-SSE")
            .is_some_and(|v| v.as_bytes() == b"1");
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_JSON)
            .read_to_vec()
            .map_err(|e| HomebaseError::Network(format!("{what}: {e}")))?;
        let plain = if encrypted {
            serde_json::from_slice::<Envelope>(&body)?.open(&self.key)?
        } else {
            body
        };
        Ok(serde_json::from_slice(&plain)?)
    }
}

/// A payload range being read ([`Client::fetch`]).
pub struct Fetched {
    /// The payload is encrypted (`PayloadEncrypted`).
    pub encrypted: bool,
    /// When encrypted: the key with this payload's IV, under the shared secret
    /// (`SharedSecretEncryptedHeader64`).
    pub key_header: Option<String>,
    what: String,
    body: ureq::BodyReader<'static>,
}

impl std::fmt::Debug for Fetched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetched")
            .field("what", &self.what)
            .field("encrypted", &self.encrypted)
            .finish_non_exhaustive()
    }
}

impl Fetched {
    /// Copies up to `limit` bytes into `sink`; returns how many came.
    pub fn copy_to(&mut self, sink: &mut dyn Write, limit: u64) -> Result<u64, HomebaseError> {
        let mut buf = vec![0u8; 64 * 1024];
        let mut written = 0u64;
        while written < limit {
            let want = (limit - written).min(buf.len() as u64) as usize;
            let n = self
                .body
                .read(&mut buf[..want])
                .map_err(|e| HomebaseError::Network(format!("{}: {e}", self.what)))?;
            if n == 0 {
                break;
            }
            sink.write_all(&buf[..n])?;
            written += n as u64;
        }
        Ok(written)
    }
}

/// A plain JSON exchange (no envelope), for the YouAuth token call.
pub(crate) fn http_json<T: DeserializeOwned>(
    response: Result<Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<T, HomebaseError> {
    let mut response = checked(response, what)?;
    response
        .body_mut()
        .read_json()
        .map_err(|e| HomebaseError::Network(format!("{what}: {e}")))
}

/// The response if it is a success; otherwise the error it carries.
fn checked(
    response: Result<Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<Response<ureq::Body>, HomebaseError> {
    let mut response = response.map_err(|e| HomebaseError::Network(format!("{what}: {e}")))?;
    let status = response.status().as_u16();
    if status < 400 {
        return Ok(response);
    }
    let body = response
        .body_mut()
        .with_config()
        .limit(64 * 1024)
        .read_to_vec()
        .unwrap_or_default();
    let error = ApiError::parse(status, &body);
    Err(match status {
        401 | 403 => HomebaseError::Denied(format!("{what}: {error}")),
        _ => HomebaseError::Api {
            what: what.to_owned(),
            error,
        },
    })
}
