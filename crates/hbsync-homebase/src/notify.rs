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
//!   `fileModified`, `fileDeleted` (reactions also send `fileModified`), as minimal rows
//!   (odin-core #1970, `homebase-api-notes.md`);
//! - with `excludeOwnWrites` (odin-core #1971) the socket isn't told of writes made with its
//!   own token: this client records each of its writes from the write's answer, its server
//!   time included (`daemon.md`, the sync loop).

use std::borrow::Cow;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hbsync_remote::ChangeRef;
use serde::Deserialize;
use serde_json::json;
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

use crate::client::Client;
use crate::crypto::{Envelope, random};
use crate::drive::TargetDrive;
use crate::drive::wire::FileRef;

/// What the notifier tells the sync loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Subscribed (again): changes made while disconnected weren't announced, so read the feed.
    Connected,
    /// A file on the drive changed: which, as a slim feed row has it (`deleted` also for a hard
    /// delete, which leaves no row in the feed), when the event says (`None`: read the feed).
    Changed(Option<ChangeRef>),
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

/// Whether the subscription hears of the drive writes this client makes itself (measurements:
/// the `dev` feature).
#[cfg(feature = "dev")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OwnWrites {
    /// Not told (`excludeOwnWrites`, odin-core #1971): the server leaves out the writes made
    /// with this client's token. Another computer's writes (its own client registration) are
    /// still announced; two computers sharing one token would miss each other's.
    #[default]
    Excluded,
    /// Told of every write, this client's own coming back as echoes (for measurements).
    Heard,
}

/// What the connection received so far, over all its connections: server frames, and their
/// bytes as text frames (the encrypted envelope included; TLS and WebSocket framing not).
/// Counted always, read with the `dev` feature.
#[derive(Debug, Default)]
pub struct Received {
    pub frames: AtomicU64,
    pub bytes: AtomicU64,
}

/// The notification connection, on its own thread. Stops when dropped: the thread ends at
/// its next check (it isn't waited for, as it may be in the middle of connecting).
#[derive(Debug)]
pub struct Notifier {
    stop: Arc<AtomicBool>,
    reconnect: Arc<AtomicBool>,
    /// Counted by the connection's thread, read with the `dev` feature.
    #[cfg_attr(not(feature = "dev"), allow(dead_code))]
    received: Arc<Received>,
}

impl Notifier {
    /// Connects to `client`'s identity and subscribes to `drive`, reconnecting as needed;
    /// events go to `events` until it is closed or the notifier dropped. Writes made with
    /// `client`'s own token aren't announced (`excludeOwnWrites`). An error: its thread
    /// couldn't start (then never connected: the caller reads the drive on a timer).
    pub fn spawn(client: Client, drive: TargetDrive, events: Sender<Event>) -> io::Result<Self> {
        Self::subscribe(client, Subscription { drive, own: false }, events)
    }

    /// [`Notifier::spawn`], hearing this client's `own` writes or not (measurements).
    #[cfg(feature = "dev")]
    pub fn spawn_with(
        client: Client,
        drive: TargetDrive,
        own: OwnWrites,
        events: Sender<Event>,
    ) -> io::Result<Self> {
        let own = own == OwnWrites::Heard;
        Self::subscribe(client, Subscription { drive, own }, events)
    }

    fn subscribe(
        client: Client,
        subscription: Subscription,
        events: Sender<Event>,
    ) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let reconnect = Arc::new(AtomicBool::new(false));
        let received = Arc::new(Received::default());
        let (flag, kick, counts) = (stop.clone(), reconnect.clone(), received.clone());
        std::thread::Builder::new()
            .name("hbsync-notify".into())
            .spawn(move || run(&client, subscription, &events, &flag, &kick, &counts))?;
        Ok(Notifier {
            stop,
            reconnect,
            received,
        })
    }

    /// What it received so far (measurements).
    #[cfg(feature = "dev")]
    pub fn received(&self) -> &Received {
        &self.received
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

/// What the connection subscribes to.
#[derive(Debug, Clone, Copy)]
struct Subscription {
    drive: TargetDrive,
    /// Hears this client's own writes too (measurements).
    own: bool,
}

impl Subscription {
    /// The `establishConnectionRequest`'s data.
    fn request(&self) -> serde_json::Value {
        json!({
            "drives": [{
                "alias": self.drive.alias.simple().to_string(),
                "type": self.drive.drive_type.simple().to_string(),
            }],
            "resultShape": "minimal",
            "excludeOwnWrites": !self.own,
        })
    }
}

fn run(
    client: &Client,
    subscription: Subscription,
    events: &Sender<Event>,
    stop: &AtomicBool,
    reconnect: &AtomicBool,
    received: &Received,
) {
    const FIRST: Duration = Duration::from_secs(1);
    let mut backoff = FIRST;
    while !stop.load(Ordering::Relaxed) {
        let (mut connected, mut refused) = (false, false);
        let result = session(
            client,
            subscription,
            events,
            (stop, reconnect),
            received,
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
    subscription: Subscription,
    events: &Sender<Event>,
    (stop, reconnect): (&AtomicBool, &AtomicBool),
    received: &Received,
    connected: &mut bool,
    refused: &mut bool,
) -> Result<(), String> {
    let mut socket = connect(client)?;
    send(
        &mut socket,
        client,
        "establishConnectionRequest",
        &subscription.request().to_string(),
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
                received.frames.fetch_add(1, Ordering::Relaxed);
                received
                    .bytes
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                let event = match notification(client, text.as_str()) {
                    Notification::Handshake if !*connected => {
                        *connected = true;
                        Event::Connected
                    }
                    Notification::Changed(change) => Event::Changed(change),
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
    Changed(Option<ChangeRef>),
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
    let body = || -> Option<Vec<u8>> {
        let frame: Frame = serde_json::from_str(text).ok()?;
        if frame.is_encrypted {
            let envelope: Envelope = serde_json::from_str(&frame.payload).ok()?;
            envelope.open(client.key()).ok()
        } else {
            Some(frame.payload.into_bytes())
        }
    };
    body().map_or(Notification::Other, |b| classify(&b))
}

/// A notification's body: only what is read of it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Body<'a> {
    #[serde(borrow)]
    notification_type: Cow<'a, str>,
    #[serde(borrow, default)]
    data: Option<Cow<'a, str>>,
}

/// A minimal drive event (its `data`): the file as it is now, or as it was before a delete,
/// as rows of a slim feed page.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveEvent {
    #[serde(default)]
    file_ref: Option<FileRef>,
    #[serde(default)]
    previous_file_ref: Option<FileRef>,
}

fn classify(body: &[u8]) -> Notification {
    // By its type alone when the rest isn't what we expect (`data` not a string): a file
    // event then says nothing of which file (the feed is read), a refusal is still one.
    let (kind, data) = match serde_json::from_slice::<Body>(body) {
        Ok(body) => (body.notification_type.into_owned(), body.data),
        Err(_) => match serde_json::from_slice::<KindOnly>(body) {
            Ok(only) => (only.notification_type, None),
            Err(_) => return Notification::Other,
        },
    };
    let kind = |k: &str| kind.eq_ignore_ascii_case(k);
    if kind("deviceHandshakeSuccess") {
        Notification::Handshake
    } else if kind("fileAdded") || kind("fileModified") || kind("fileDeleted") {
        Notification::Changed(data.as_deref().and_then(|d| change(d, kind("fileDeleted"))))
    } else if kind("authenticationError") {
        Notification::Refused(format!("refused: {}", data.as_deref().unwrap_or("")))
    } else {
        Notification::Other
    }
}

/// A server frame's type, whatever the rest of it holds.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KindOnly {
    notification_type: String,
}

/// The file a drive event announces; `None` if the event doesn't say.
fn change(data: &str, deleted: bool) -> Option<ChangeRef> {
    match serde_json::from_str::<DriveEvent>(data) {
        Ok(event) => match (event.file_ref, event.previous_file_ref) {
            (Some(now), _) => Some(now.to_ref()),
            // Deleted, and no file now: deleted for good without a tombstone (a hard delete).
            (None, Some(before)) if deleted => Some(before.to_ref().tombstone()),
            _ => None,
        },
        // A delete is in no feed when it was a hard one: its file id, however the rest reads.
        Err(e) if deleted => {
            let gone = deleted_file(data);
            if gone.is_none() {
                tracing::warn!("a delete announced without a file id that reads ({e})");
            }
            gone
        }
        Err(_) => None,
    }
}

/// The file a delete names, read leniently: `previousFileRef`'s `fileId` (else `fileRef`'s),
/// with its version where that reads (a tombstone is taken by file id).
fn deleted_file(data: &str) -> Option<ChangeRef> {
    let event: serde_json::Value = serde_json::from_str(data).ok()?;
    let file = event
        .get("previousFileRef")
        .filter(|f| f.get("fileId").is_some())
        .or_else(|| event.get("fileRef"))?;
    let id = |key: &str| {
        file.get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(|s| s.parse::<uuid::Uuid>().ok())
    };
    Some(ChangeRef {
        file_id: hbsync_types::RemoteFileId(id("fileId")?),
        version: hbsync_types::VersionTag(id("versionTag").unwrap_or_default()),
        deleted: true,
        parent: None,
        removed_at: None,
        changed_at: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(body: serde_json::Value) -> Notification {
        classify(body.to_string().as_bytes())
    }

    /// The subscription asks for minimal events, without this client's own writes unless it
    /// wants to hear them.
    #[test]
    fn the_subscription_leaves_out_own_writes() {
        let drive = TargetDrive {
            alias: uuid::Uuid::from_u128(1),
            drive_type: uuid::Uuid::from_u128(2),
        };
        let request = |own| Subscription { drive, own }.request();
        let excluded = request(false);
        assert_eq!(excluded["excludeOwnWrites"], true);
        assert_eq!(excluded["resultShape"], "minimal");
        assert_eq!(
            excluded["drives"][0]["alias"],
            "00000000000000000000000000000001"
        );
        assert_eq!(request(true)["excludeOwnWrites"], false);
    }

    #[test]
    fn notifications_are_classified() {
        assert_eq!(
            n(json!({"notificationType": "deviceHandshakeSuccess", "stunUrls": []})),
            Notification::Handshake
        );
        assert_eq!(n(json!({"notificationType": "pong"})), Notification::Other);
        assert!(matches!(
            n(json!({"notificationType": "authenticationError", "data": "no"})),
            Notification::Refused(_)
        ));
    }

    /// A frame whose `data` isn't the string expected still counts by its type: a file event
    /// says nothing of which file (the feed is read), a refusal stays one.
    #[test]
    fn a_frame_counts_by_its_type_whatever_its_data() {
        assert_eq!(
            n(json!({"notificationType": "fileModified", "data": {"fileRef": {}}})),
            Notification::Changed(None)
        );
        assert_eq!(
            n(json!({"notificationType": "fileDeleted", "data": 42})),
            Notification::Changed(None)
        );
        assert!(matches!(
            n(json!({"notificationType": "authenticationError", "data": {"why": "no"}})),
            Notification::Refused(_)
        ));
        assert_eq!(n(json!({"data": "no type"})), Notification::Other);
    }

    /// A delete whose rows don't fully read still names its file (a hard delete is in no
    /// feed): taken as deleted by its file id; without a file id that reads, the feed is read.
    #[test]
    fn a_delete_that_doesnt_fully_read_still_names_its_file() {
        use hbsync_types::RemoteFileId;
        use uuid::Uuid;
        let file = Uuid::new_v4();
        let deleted = |data: serde_json::Value| {
            n(json!({"notificationType": "fileDeleted", "data": data.to_string()}))
        };
        let Notification::Changed(Some(gone)) = deleted(json!({
            "previousFileRef": {"fileId": file, "versionTag": 42, "fileState": "active"}
        })) else {
            panic!("the file isn't named");
        };
        assert_eq!(gone.file_id, RemoteFileId(file));
        assert!(gone.deleted);
        assert_eq!(
            deleted(json!({"previousFileRef": {"fileId": "not an id"}})),
            Notification::Changed(None)
        );
    }

    #[test]
    fn a_minimal_event_says_which_file() {
        use hbsync_types::{NodeId, RemoteFileId, VersionTag};
        use uuid::Uuid;
        let (file, version, parent) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let event = |kind: &str, data: serde_json::Value| {
            n(json!({"notificationType": kind, "data": data.to_string()}))
        };
        let row = |state: &str, archival: i64| {
            json!({"fileId": file, "versionTag": version, "fileState": state,
                "archivalStatus": archival, "groupId": parent, "updated": 7, "fileType": 7101})
        };
        let changed = |deleted, removed_at, changed_at, parent| {
            Notification::Changed(Some(ChangeRef {
                file_id: RemoteFileId(file),
                version: VersionTag(version),
                deleted,
                parent,
                removed_at,
                changed_at,
            }))
        };
        assert_eq!(
            event(
                "fileModified",
                json!({"targetDrive": {}, "fileRef": row("active", 2)})
            ),
            changed(false, Some(7), Some(7), Some(NodeId(parent)))
        );
        // A tombstone, and a hard delete (only the file as it was): deleted either way.
        assert_eq!(
            event(
                "fileDeleted",
                json!({"fileRef": row("deleted", 0), "previousFileRef": row("active", 2)})
            ),
            changed(true, None, None, None)
        );
        assert_eq!(
            event(
                "fileDeleted",
                json!({"targetDrive": {}, "previousFileRef": row("active", 2)})
            ),
            changed(true, None, None, None)
        );
        // An event that doesn't say (full headers, or no row): the feed is read.
        assert_eq!(
            event("fileAdded", json!({"header": {"fileId": file}})),
            Notification::Changed(None)
        );
    }
}
