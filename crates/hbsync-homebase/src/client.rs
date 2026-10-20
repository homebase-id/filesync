//! Calls to an identity's V2 API as an authorized app client. JSON calls go inside the
//! shared-secret envelope (verified in odin-core `SharedSecretEncryptionMiddleware`): a GET's
//! query string becomes one `ss` parameter, even when empty; a JSON body becomes `{iv, data}`;
//! a response marked `X-SSE: 1` comes back the same way. Uploads (multipart) and payload
//! reads aren't (encrypted files carry their own encryption).

use std::io::{Read, Write};
use std::sync::Arc;
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
use crate::connections::{Call, Connections, REUSED_WITHIN, SharedConnector};
use crate::crypto::{Envelope, Key};
use crate::multipart::Multipart;
use crate::problem::{ApiError, Sender};
use crate::youauth::Credentials;

/// The largest JSON answer read (a page of the change feed is far below it).
const MAX_JSON: u64 = 256 * 1024 * 1024;

/// An agent for a few calls (a [`Client`] keeps its own: [`connections`]). No overall timeout
/// on a body: a large file takes as long as it takes, while [`IDLE`] ends one that stalls.
pub(crate) fn agent() -> ureq::Agent {
    agent_with(IDLE)
}

fn agent_with(idle: Duration) -> ureq::Agent {
    agent_on(DefaultConnector::new(), idle, REUSED_WITHIN)
}

/// A client's connections: its agents, each let go once unused for `reused_within`, connect
/// through one connector, which keeps the TLS setup and its sessions.
fn connections(reused_within: Duration) -> Connections {
    let connector = Arc::new(DefaultConnector::new());
    Connections::new(reused_within, move |reused_within| {
        agent_on(SharedConnector(Arc::clone(&connector)), IDLE, reused_within)
    })
}

/// An agent through `connector`, which reuses no connection idle longer than `reused_within`.
fn agent_on(
    connector: impl Connector<Out = Box<dyn Transport>>,
    idle: Duration,
    reused_within: Duration,
) -> ureq::Agent {
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
        // What `Connections` counts on.
        .max_idle_age(reused_within)
        .user_agent(concat!("hbsync/", env!("CARGO_PKG_VERSION")))
        .build();
    ureq::Agent::with_parts(
        config,
        connector.chain(IdleTimeout(idle)),
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
    connections: Arc<Connections>,
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
pub(crate) enum UploadMethod {
    Post,
    Patch,
}

impl Client {
    pub fn new(credentials: &Credentials) -> Result<Self, HomebaseError> {
        Ok(Client {
            identity: credentials.identity.clone(),
            token: credentials.client_auth_token.clone(),
            key: credentials.shared_secret_key()?,
            connections: Arc::new(connections(REUSED_WITHIN)),
        })
    }

    pub(crate) fn identity(&self) -> &str {
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
    pub(crate) fn get<T: DeserializeOwned>(
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
        let call = self.connections.call();
        let response = call
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
        let call = self.connections.call();
        let response = call
            .post(self.url(path))
            .header("Authorization", &self.bearer())
            .send_json(&sealed);
        self.decode(response, path)
    }

    /// Sends a multipart form (plaintext; the parts carry their own encryption).
    pub(crate) fn upload<T: DeserializeOwned>(
        &self,
        method: UploadMethod,
        path: &str,
        form: Multipart<'_>,
    ) -> Result<T, HomebaseError> {
        let url = self.url(path);
        let call = self.connections.call();
        let request = match method {
            UploadMethod::Post => call.post(&url),
            UploadMethod::Patch => call.patch(&url),
        };
        let (content_type, len) = (form.content_type(), form.content_length());
        let mut body = Recording {
            inner: form.into_reader(),
            failed: None,
        };
        let response = request
            .header("Authorization", &self.bearer())
            .header("Content-Type", &content_type)
            .header("Content-Length", &len.to_string())
            .send(ureq::SendBody::from_reader(&mut body));
        // The body's own read failed (the upload source): a local error, not the network's.
        if response.is_err()
            && let Some(e) = body.failed
        {
            return Err(HomebaseError::Io(e));
        }
        self.decode(response, path)
    }

    /// `GET {path}` with a `Range` of `start..=last`: what the response says about the payload,
    /// and its bytes to read (encrypted payloads come as ciphertext).
    pub fn fetch(&self, path: &str, start: u64, last: u64) -> Result<Fetched, HomebaseError> {
        let call = self.connections.call();
        let response = call
            .get(self.url(path))
            .header("Authorization", &self.bearer())
            .header("Range", &format!("bytes={start}-{last}"))
            .call();
        Fetched::new(call, response, path, start, last)
    }

    fn decode<T: DeserializeOwned>(
        &self,
        response: Result<Response<ureq::Body>, ureq::Error>,
        what: &str,
    ) -> Result<T, HomebaseError> {
        let mut response = checked(response, what)?;
        let encrypted = sealed(response.headers());
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
    /// Keeps the agent while the body is read (after the body: its connection is back first).
    _call: Call,
}

/// The first and last byte and the whole size of a `Content-Range: bytes first-last/size`.
fn content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (range, size) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    Some((
        first.trim().parse().ok()?,
        last.trim().parse().ok()?,
        size.trim().parse().ok()?,
    ))
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
    /// A ranged GET's response for `start..=last` of the payload at `path`, made as `call`,
    /// which the body keeps.
    fn new(
        call: Call,
        response: Result<Response<ureq::Body>, ureq::Error>,
        path: &str,
        start: u64,
        last: u64,
    ) -> Result<Fetched, HomebaseError> {
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
            // The range asked for, cut short only at the payload's end.
            .and_then(content_range)
            .filter(|&(first, end, size)| first == start && end == last.min(size.saturating_sub(1)))
            .map(|(_, _, size)| size)
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
            _call: call,
        })
    }

    /// Copies the body into `sink`, at most `limit` bytes, and returns how many came. The body
    /// must end there: bytes past `limit` are an error. Reading it to its end gives the
    /// connection back to the pool (ureq keeps one only once a read has found the body's end).
    pub fn copy_to(&mut self, sink: &mut dyn Write, limit: u64) -> Result<u64, HomebaseError> {
        let mut buf = vec![0u8; limit.clamp(1, 64 * 1024) as usize];
        let mut written = 0u64;
        while written < limit {
            let want = (limit - written).min(buf.len() as u64) as usize;
            let n = self.read(&mut buf[..want])?;
            if n == 0 {
                return Ok(written);
            }
            sink.write_all(&buf[..n])?;
            written += n as u64;
        }
        // Everything asked for came: a failure to read the end only costs the connection.
        if self.read(&mut [0u8; 1]).is_ok_and(|n| n != 0) {
            return Err(HomebaseError::Protocol(format!(
                "{}: more than the {limit} bytes asked for",
                self.what
            )));
        }
        Ok(written)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, HomebaseError> {
        loop {
            match self.body.read(buf) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                result => {
                    return result
                        .map_err(|e| HomebaseError::Network(format!("{}: {e}", self.what)));
                }
            }
        }
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

/// A reader passing on `inner`, keeping its first error (ureq gets a copy and reports only its own).
struct Recording<R> {
    inner: R,
    failed: Option<std::io::Error>,
}

impl<R: Read> Read for Recording<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf).map_err(|e| {
            let copy = std::io::Error::new(e.kind(), e.to_string());
            if e.kind() != std::io::ErrorKind::Interrupted {
                self.failed.get_or_insert(e);
            }
            copy
        })
    }
}

/// Whether an answer is sealed under the shared secret (`X-SSE: 1`).
fn sealed(headers: &ureq::http::HeaderMap) -> bool {
    headers.get("X-SSE").is_some_and(|v| v.as_bytes() == b"1")
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
    let sender = Sender {
        odin: response.headers().contains_key("X-Odin-Version"),
        sealed: sealed(response.headers()),
    };
    let body = response
        .body_mut()
        .with_config()
        .limit(64 * 1024)
        .read_to_vec()
        .unwrap_or_default();
    let error = ApiError::read(status, &body, sender);
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

    use super::{Fetched, REUSED_WITHIN, agent_with, connections, content_range};
    use crate::HomebaseError;
    use crate::connections::Connections;

    /// A keep-alive server answering every request with `body` as bytes `0..len` of a payload
    /// (its `Content-Length` is the body's): the connections it accepted, once the client is done.
    fn ranges_server(
        body: &'static [u8],
        len: usize,
    ) -> std::io::Result<(String, std::thread::JoinHandle<usize>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}/", listener.local_addr()?);
        let server = std::thread::spawn(move || {
            let mut accepted = 0;
            for socket in listener.incoming() {
                let Ok(mut socket) = socket else { break };
                accepted += 1;
                // A connection the client keeps without using it ends the test, not hangs it.
                let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                while let Ok(n) = socket.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                    // A request is answered once its head is in.
                    if !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        continue;
                    }
                    if request.starts_with(b"GET /stop") {
                        return accepted;
                    }
                    request.clear();
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\ncontent-range: bytes 0-{}/{}\r\n\
                         content-length: {}\r\n\r\n",
                        len - 1,
                        body.len().max(len),
                        body.len()
                    );
                    if socket.write_all(head.as_bytes()).is_err() || socket.write_all(body).is_err()
                    {
                        break;
                    }
                }
            }
            accepted
        });
        Ok((url, server))
    }

    fn fetch(connections: &Connections, url: &str, len: u64) -> Result<Fetched, HomebaseError> {
        let call = connections.call();
        let response = call
            .get(url)
            .header("Range", &format!("bytes=0-{}", len - 1))
            .call();
        Fetched::new(call, response, "payload", 0, len - 1)
    }

    /// Stops `ranges_server`: the connections it accepted.
    fn stop(
        connections: &Connections,
        url: &str,
        server: std::thread::JoinHandle<usize>,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let _ = connections.call().get(format!("{url}stop")).call();
        Ok(server.join().map_err(|_| "server panicked")?)
    }

    /// Reading a payload to its end gives its connection back to the pool: many downloads, one
    /// connection.
    #[test]
    fn downloads_reuse_their_connection() -> Result<(), Box<dyn std::error::Error>> {
        const BODY: &[u8] = b"0123456789";
        const LEN: u64 = BODY.len() as u64;
        let (url, server) = ranges_server(BODY, BODY.len())?;
        let connections = connections(REUSED_WITHIN);
        for _ in 0..5 {
            let mut got = Vec::new();
            let n = fetch(&connections, &url, LEN)?.copy_to(&mut got, LEN)?;
            assert_eq!((n, got.as_slice()), (LEN, BODY));
        }
        assert_eq!(stop(&connections, &url, server)?, 1);
        Ok(())
    }

    /// Calls in a row share a connection; once none has run for a while, the agent goes with
    /// its connections, and the next call makes another.
    #[test]
    fn idle_connections_are_let_go() -> Result<(), Box<dyn std::error::Error>> {
        const BODY: &[u8] = b"0123456789";
        const LEN: u64 = BODY.len() as u64;
        let (url, server) = ranges_server(BODY, BODY.len())?;
        let connections = connections(Duration::from_millis(300));
        for _ in 0..3 {
            fetch(&connections, &url, LEN)?.copy_to(&mut Vec::new(), LEN)?;
        }
        let began = Instant::now();
        while connections.kept() {
            assert!(began.elapsed() < Duration::from_secs(5), "the agent stays");
            std::thread::sleep(Duration::from_millis(20));
        }
        // The first connection is closed: the server, which serves one at a time, takes the
        // next at once (not after its 5 s read timeout).
        let began = Instant::now();
        fetch(&connections, &url, LEN)?.copy_to(&mut Vec::new(), LEN)?;
        assert!(
            began.elapsed() < Duration::from_secs(2),
            "{:?}",
            began.elapsed()
        );
        assert_eq!(stop(&connections, &url, server)?, 2);
        Ok(())
    }

    /// A body still being read keeps its agent, however long it takes: its connection comes
    /// back to the pool, and the next call reuses it.
    #[test]
    fn a_call_on_its_way_keeps_the_agent() -> Result<(), Box<dyn std::error::Error>> {
        const BODY: &[u8] = b"0123456789";
        const LEN: u64 = BODY.len() as u64;
        let (url, server) = ranges_server(BODY, BODY.len())?;
        let connections = connections(Duration::from_millis(500));
        let mut fetched = fetch(&connections, &url, LEN)?;
        std::thread::sleep(Duration::from_millis(1500));
        assert!(connections.kept());
        fetched.copy_to(&mut Vec::new(), LEN)?;
        drop(fetched);
        fetch(&connections, &url, LEN)?.copy_to(&mut Vec::new(), LEN)?;
        assert_eq!(stop(&connections, &url, server)?, 1);
        Ok(())
    }

    /// A body longer than the range asked for is refused, not cut short.
    #[test]
    fn bytes_past_the_range_are_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let (url, server) = ranges_server(b"0123456789abc", 10)?;
        let connections = connections(REUSED_WITHIN);
        let mut got = Vec::new();
        let err = fetch(&connections, &url, 10)?.copy_to(&mut got, 10);
        assert!(
            matches!(&err, Err(HomebaseError::Protocol(why)) if why.contains("more than")),
            "{err:?}"
        );
        assert_eq!(got, b"0123456789");
        stop(&connections, &url, server)?;
        Ok(())
    }

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
        assert_eq!(content_range("bytes 16-47/112"), Some((16, 47, 112)));
        assert_eq!(content_range("bytes 0-0/1"), Some((0, 0, 1)));
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
