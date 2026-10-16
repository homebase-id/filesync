//! The identity's notification WebSocket, as a wake-up hint to read the feed from its cursor,
//! the truth: a change announced, and every (re)connect (daemon.md, "No poll").
//!
//! Verified in odin-core `V2NotificationSocketController`, `AppNotificationHandler`,
//! `AppNotificationDispatcher`, and chat-kmp `OdinWebSocketClient`:
//! - `wss://{identity}/api/v2/notify/ws-token`, app tokens only, authenticated by the
//!   subprotocols `odin.notify.v1, odin.bearer.<base64url CAT>` in one header;
//! - client frames are envelopes of `{command, data}` (`data` a JSON string); the drive
//!   subscription is `establishConnectionRequest`, answered by `deviceHandshakeSuccess`;
//! - server frames are `{isEncrypted, payload}`; drive changes are `fileAdded`,
//!   `fileModified`, `fileDeleted` (reactions also send `fileModified`).

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::{Value, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

use crate::client::Client;
use crate::crypto::{Envelope, random};
use crate::drive::TargetDrive;

/// What the notifier tells the sync loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Subscribed (again): changes made while disconnected weren't announced, so read the feed.
    Connected,
    /// A file on the drive changed.
    Changed,
    /// The connection broke; it is retried with backoff.
    Lost(String),
    /// The identity refused the subscription (credentials revoked, no access to the drive):
    /// asked again later, as for a broken connection but not before [`REFUSED_PAUSE`].
    Refused(String),
}

/// How often the socket is checked for a stop request and the keep-alive.
const TICK: Duration = Duration::from_secs(1);
const PING_EVERY: Duration = Duration::from_secs(30);
/// No frame at all (not even a pong) for this long: the connection is dead.
const SILENCE: Duration = Duration::from_secs(75);
const HANDSHAKE: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A refused subscription isn't asked again sooner (the sync's own refusals wait as long).
const REFUSED_PAUSE: Duration = Duration::from_secs(300);

/// The notification connection, on its own thread. Stops when dropped: the thread ends at
/// its next check (it isn't waited for, as it may be in the middle of connecting).
#[derive(Debug)]
pub struct Notifier {
    stop: Arc<AtomicBool>,
    reconnect: Arc<AtomicBool>,
}

impl Notifier {
    /// Connects to `client`'s identity and subscribes to `drive`, reconnecting as needed;
    /// events go to `events` until it is closed or the notifier dropped. An error: its thread
    /// couldn't start (then never connected: the caller reads the drive on a timer).
    pub fn spawn(client: Client, drive: TargetDrive, events: Sender<Event>) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let reconnect = Arc::new(AtomicBool::new(false));
        let (flag, kick) = (stop.clone(), reconnect.clone());
        std::thread::Builder::new()
            .name("hbsync-notify".into())
            .spawn(move || run(&client, drive, &events, &flag, &kick))?;
        Ok(Notifier { stop, reconnect })
    }

    /// Drops the connection and connects again at once, without waiting out a backoff: after
    /// the machine woke up or the network changed, the old connection is most likely dead
    /// but would take a while to look it.
    pub fn reconnect(&self) {
        self.reconnect.store(true, Ordering::Relaxed);
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn run(
    client: &Client,
    drive: TargetDrive,
    events: &Sender<Event>,
    stop: &AtomicBool,
    reconnect: &AtomicBool,
) {
    const FIRST: Duration = Duration::from_secs(1);
    let mut backoff = FIRST;
    while !stop.load(Ordering::Relaxed) {
        let (mut connected, mut refused) = (false, false);
        let result = session(
            client,
            drive,
            events,
            stop,
            reconnect,
            &mut connected,
            &mut refused,
        );
        if connected {
            backoff = FIRST;
        }
        let Err(why) = result else {
            return; // stopped, or nobody listens any more
        };
        if reconnect.swap(false, Ordering::Relaxed) {
            backoff = FIRST;
            continue;
        }
        if !refused && events.send(Event::Lost(why)).is_err() {
            return;
        }
        // Jitter of ±20%, so clients of one identity don't reconnect in lockstep; after a
        // refusal above its floor.
        let jitter = random::<1>().map_or(0, |b| b[0]) as f64 / 255.0;
        let wait = if refused {
            backoff.max(REFUSED_PAUSE).mul_f64(1.0 + 0.2 * jitter)
        } else {
            backoff.mul_f64(0.8 + 0.4 * jitter)
        };
        let until = Instant::now() + wait;
        while Instant::now() < until
            && !stop.load(Ordering::Relaxed)
            && !reconnect.load(Ordering::Relaxed)
        {
            std::thread::sleep(TICK.min(until.saturating_duration_since(Instant::now())));
        }
        backoff = if reconnect.swap(false, Ordering::Relaxed) {
            FIRST
        } else {
            (backoff * 2).min(MAX_BACKOFF)
        };
    }
}

type Socket = WebSocket<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>;

/// One connection, until it breaks or is `refused` (`Err`) or the notifier stops (`Ok`).
fn session(
    client: &Client,
    drive: TargetDrive,
    events: &Sender<Event>,
    stop: &AtomicBool,
    reconnect: &AtomicBool,
    connected: &mut bool,
    refused: &mut bool,
) -> Result<(), String> {
    let mut socket = connect(client)?;
    send(
        &mut socket,
        client,
        "establishConnectionRequest",
        &json!({ "drives": [{
            "alias": drive.alias.simple().to_string(),
            "type": drive.drive_type.simple().to_string(),
        }]})
        .to_string(),
    )?;
    let opened = Instant::now();
    let (mut last_frame, mut last_ping) = (Instant::now(), Instant::now());
    loop {
        if stop.load(Ordering::Relaxed) {
            let _ = socket.close(None);
            return Ok(());
        }
        if reconnect.load(Ordering::Relaxed) {
            let _ = socket.close(None);
            return Err("reconnecting on request".into());
        }
        match socket.read() {
            Ok(Message::Text(text)) => {
                last_frame = Instant::now();
                let event = match notification(client, text.as_str()) {
                    Notification::Handshake if !*connected => {
                        *connected = true;
                        Event::Connected
                    }
                    Notification::Changed => Event::Changed,
                    Notification::Refused(why) => {
                        let _ = socket.close(None);
                        let _ = events.send(Event::Refused(why.clone()));
                        *refused = true;
                        return Err(why);
                    }
                    _ => continue,
                };
                if events.send(event).is_err() {
                    return Ok(());
                }
            }
            Ok(Message::Close(frame)) => {
                return Err(format!("closed by the server: {frame:?}"));
            }
            Ok(_) => last_frame = Instant::now(),
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(e) => return Err(e.to_string()),
        }
        if !*connected && opened.elapsed() > HANDSHAKE {
            return Err("no handshake from the server".into());
        }
        if last_frame.elapsed() > SILENCE {
            return Err("the connection went silent".into());
        }
        if last_ping.elapsed() > PING_EVERY {
            send(&mut socket, client, "ping", "ping")?;
            last_ping = Instant::now();
        }
    }
}

fn connect(client: &Client) -> Result<Socket, String> {
    let host = client.identity();
    // Every address in turn, as the HTTP client does (an IPv6 address may be unreachable).
    let mut failure = format!("{host} has no address");
    let mut connected = None;
    for addr in (host, 443)
        .to_socket_addrs()
        .map_err(|e| format!("resolving {host}: {e}"))?
    {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(10)) {
            Ok(tcp) => {
                connected = Some(tcp);
                break;
            }
            Err(e) => failure = format!("connecting to {host} ({addr}): {e}"),
        }
    }
    let tcp = connected.ok_or(failure)?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| format!("{host}: {e}"))?;
    let tls = rustls::ClientConnection::new(TLS.clone()?, name).map_err(|e| e.to_string())?;
    let stream = rustls::StreamOwned::new(tls, tcp);

    let mut request = format!("wss://{host}/api/v2/notify/ws-token")
        .into_client_request()
        .map_err(|e| e.to_string())?;
    // base64url without padding: a subprotocol must be an HTTP token ('/' and '=' aren't).
    let token = URL_SAFE_NO_PAD.encode(
        STANDARD
            .decode(client.token())
            .map_err(|_| "the token isn't base64".to_owned())?,
    );
    let protocols = format!("odin.notify.v1, odin.bearer.{token}")
        .parse()
        .map_err(|_| "the token can't go in a header".to_owned())?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", protocols);
    let (socket, _) = tungstenite::client(request, stream).map_err(|e| match e {
        tungstenite::HandshakeError::Failure(e) => e.to_string(),
        tungstenite::HandshakeError::Interrupted(_) => "handshake timed out".to_owned(),
    })?;
    // From now on, reads wake up every tick for the stop flag and the keep-alive.
    socket
        .get_ref()
        .get_ref()
        .set_read_timeout(Some(TICK))
        .map_err(|e| e.to_string())?;
    Ok(socket)
}

/// TLS with ring and the webpki roots, as ureq uses them.
static TLS: LazyLock<Result<Arc<rustls::ClientConfig>, String>> = LazyLock::new(|| {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
});

fn send(socket: &mut Socket, client: &Client, command: &str, data: &str) -> Result<(), String> {
    let plain = json!({ "command": command, "data": data }).to_string();
    let sealed = Envelope::seal(client.key(), plain.as_bytes()).map_err(|e| e.to_string())?;
    let text = serde_json::to_string(&sealed).map_err(|e| e.to_string())?;
    socket
        .send(Message::text(text))
        .map_err(|e| format!("sending {command}: {e}"))
}

#[derive(Debug, PartialEq, Eq)]
enum Notification {
    Handshake,
    Changed,
    Refused(String),
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Frame {
    #[serde(default)]
    is_encrypted: bool,
    payload: String,
}

/// What a server frame says; [`Notification::Other`] if it can't be read.
fn notification(client: &Client, text: &str) -> Notification {
    let body = || -> Option<Value> {
        let frame: Frame = serde_json::from_str(text).ok()?;
        if frame.is_encrypted {
            let envelope: Envelope = serde_json::from_str(&frame.payload).ok()?;
            serde_json::from_slice(&envelope.open(client.key()).ok()?).ok()
        } else {
            serde_json::from_str(&frame.payload).ok()
        }
    };
    body().map_or(Notification::Other, |b| classify(&b))
}

fn classify(body: &Value) -> Notification {
    let kind = match &body["notificationType"] {
        Value::String(s) => s.to_ascii_lowercase(),
        _ => return Notification::Other,
    };
    match kind.as_str() {
        "devicehandshakesuccess" => Notification::Handshake,
        "fileadded" | "filemodified" | "filedeleted" => Notification::Changed,
        "authenticationerror" => {
            Notification::Refused(format!("refused: {}", body["data"].as_str().unwrap_or("")))
        }
        _ => Notification::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_are_classified() {
        let n = |v: Value| classify(&v);
        assert_eq!(
            n(json!({"notificationType": "deviceHandshakeSuccess", "stunUrls": []})),
            Notification::Handshake
        );
        assert_eq!(
            n(json!({"notificationType": "fileModified", "data": "{}"})),
            Notification::Changed
        );
        assert_eq!(n(json!({"notificationType": "pong"})), Notification::Other);
        assert!(matches!(
            n(json!({"notificationType": "authenticationError", "data": "no"})),
            Notification::Refused(_)
        ));
    }
}
