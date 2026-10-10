//! Calls to an identity's V2 API as an authorized app client. JSON calls go inside the
//! shared-secret envelope (verified in odin-core `SharedSecretEncryptionMiddleware`): a GET's
//! query string becomes one `ss` parameter, even when empty; a JSON body becomes `{iv, data}`;
//! a response marked `X-SSE: 1` comes back the same way. Uploads (multipart) and payload
//! reads aren't (encrypted files carry their own encryption).

use std::io::{Read, Write};
use std::time::Duration;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::de::DeserializeOwned;
use ureq::Timeout;
use ureq::http::Response;
use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::transport::{
    Buffers, ConnectionDetails, Connector, DefaultConnector, NextTimeout, Transport,
};

use crate::HomebaseError;
use crate::crypto::{Envelope, Key};
use crate::multipart::Multipart;
use crate::problem::ApiError;
use crate::youauth::Credentials;

/// The largest JSON answer read (a page of the change feed is far below it).
const MAX_JSON: u64 = 256 * 1024 * 1024;

/// One connection pool for all calls. No overall timeout on a body: a large file takes as long
/// as it takes, while [`IDLE`] ends one that stalls.
pub(crate) fn agent() -> ureq::Agent {
    agent_with(IDLE)
}

fn agent_with(idle: Duration) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_send_request(Some(Duration::from_secs(60)))
        .timeout_recv_response(Some(Duration::from_secs(300)))
        // Bodies get an overall timeout no transfer reaches, so that their reads and writes
        // carry it (not `Global`) and `IdleTimeout` caps them.
        .timeout_send_body(Some(BODY))
        .timeout_recv_body(Some(BODY))
        // Well above the transfer workers (`TransferConfig::default`): a connection over the
        // limit is closed when it goes idle, and the next call pays a new TCP and TLS handshake.
        .max_idle_connections_per_host(16)
        .max_idle_connections(16)
        .user_agent(concat!("hbsync/", env!("CARGO_PKG_VERSION")))
        .build();
    ureq::Agent::with_parts(
        config,
        DefaultConnector::new().chain(IdleTimeout(idle)),
        DefaultResolver::default(),
    )
}

/// Longer than any body takes: a week.
const BODY: Duration = Duration::from_secs(7 * 24 * 3600);

/// The longest a request or response body may go without a byte: a connection that died
/// without a reset ends the call instead of blocking a worker forever.
const IDLE: Duration = Duration::from_secs(60);

/// Gives every connection an idle timeout while a body moves. ureq has only overall timeouts;
/// its transports take the time an overall one leaves as each read's or write's socket timeout,
/// so capping that time makes it an idle timeout.
#[derive(Debug)]
struct IdleTimeout(Duration);

impl<In: Transport> Connector<In> for IdleTimeout {
    type Out = Idle<In>;

    fn connect(
        &self,
        _: &ConnectionDetails,
        chained: Option<In>,
    ) -> Result<Option<Idle<In>>, ureq::Error> {
        Ok(chained.map(|inner| Idle {
            inner,
            idle: self.0,
        }))
    }
}

#[derive(Debug)]
struct Idle<T> {
    inner: T,
    idle: Duration,
}

impl<T: Transport> Idle<T> {
    fn cap(&self, timeout: NextTimeout) -> NextTimeout {
        match timeout.reason {
            Timeout::SendBody | Timeout::RecvBody => NextTimeout {
                after: timeout.after.min(self.idle.into()),
                reason: timeout.reason,
            },
            _ => timeout,
        }
    }
}

impl<T: Transport> Transport for Idle<T> {
    fn buffers(&mut self) -> &mut dyn Buffers {
        self.inner.buffers()
    }

    fn transmit_output(&mut self, amount: usize, timeout: NextTimeout) -> Result<(), ureq::Error> {
        let timeout = self.cap(timeout);
        self.inner.transmit_output(amount, timeout)
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, ureq::Error> {
        let timeout = self.cap(timeout);
        self.inner.await_input(timeout)
    }

    fn is_open(&mut self) -> bool {
        self.inner.is_open()
    }

    fn is_tls(&self) -> bool {
        self.inner.is_tls()
    }
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
        let range = header("Content-Range");
        let size = range
            .as_deref()
            .and_then(|r| content_range(r).filter(|(first, _)| *first == start))
            .map(|(_, size)| size)
            .ok_or_else(|| {
                HomebaseError::Protocol(format!(
                    "{path}: asked for bytes {start}-{last}, got Content-Range {range:?}"
                ))
            })?;
        Ok(Fetched {
            encrypted: header("PayloadEncrypted").is_some_and(|v| v.eq_ignore_ascii_case("true")),
            key_header: header("SharedSecretEncryptedHeader64"),
            size,
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
    /// The whole payload's size, as stored (`Content-Range`).
    pub size: u64,
    what: String,
    body: ureq::BodyReader<'static>,
}

/// The first byte and the whole size of a `Content-Range: bytes first-last/size`.
fn content_range(value: &str) -> Option<(u64, u64)> {
    let (range, size) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (first, _) = range.split_once('-')?;
    Some((first.trim().parse().ok()?, size.trim().parse().ok()?))
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    use super::{agent_with, content_range};

    /// A body that stops coming ends the read after the idle time, though the call has no
    /// overall timeout.
    #[test]
    fn a_stalled_body_times_out() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}/", listener.local_addr()?);
        let server = std::thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request)?;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1000\r\n\r\n0123456789")?;
            socket.flush()?;
            // Alive, but sends nothing more.
            std::thread::sleep(Duration::from_millis(1500));
            Ok(())
        });
        let idle = Duration::from_millis(200);
        let began = Instant::now();
        let mut body = agent_with(idle).get(&url).call()?.into_body().into_reader();
        let mut got = Vec::new();
        let result = body.read_to_end(&mut got);
        let took = began.elapsed();
        assert!(result.is_err(), "read {} bytes", got.len());
        assert_eq!(got, b"0123456789");
        assert!(took >= idle && took < Duration::from_secs(1), "{took:?}");
        drop(body);
        server.join().map_err(|_| "server panicked")??;
        Ok(())
    }

    #[test]
    fn content_ranges_parse() {
        assert_eq!(content_range("bytes 16-47/112"), Some((16, 112)));
        assert_eq!(content_range("bytes 0-0/1"), Some((0, 1)));
        for bad in [
            "bytes */112",
            "bytes 16-47/*",
            "items 0-1/2",
            "bytes 16/112",
            "",
        ] {
            assert_eq!(content_range(bad), None, "{bad}");
        }
    }
}
