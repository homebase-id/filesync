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
//!
//! Another identity's server tells a member of what it shares the same way, on a socket of
//! its own ([`Notifier::spawn_shared`]; verified in odin-core
//! `V2PeerNotificationSocketController`, `PeerAppNotificationHandler`,
//! `EstablishConnectionOptions`):
//! - `wss://{sharer}/api/v2/peer/notify/ws-token`, upgraded by anyone: the token it made for
//!   this identity's connection ([`crate::peer::PeerToken`], the one the files are read with)
//!   goes in the first frame, in the clear beside the subscription sealed with its secret:
//!   `{clientAuthToken64, sharedSecretEncryptedOptions}`; a token it doesn't take is answered
//!   by an unsealed `authenticationError`;
//! - every later frame is as on the identity's own socket;
//! - `excludeOwnWrites` is refused there (a member writes nothing as a device), and an event
//!   comes only for a file shared with the member: one whose share was withdrawn or ended as
//!   `fileDeleted` with the file as it now reads to them (`homebase-api-notes.md`, "Shared
//!   with you").

use std::borrow::Cow;
use std::io;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hbsync_remote::ChangeRef;
use serde::Deserialize;
use serde_json::json;
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

use crate::client::{Client, has_odin_header, not_from_identity, refuses};
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
    /// One of the drive's share markers (node schema, "Sync markers") was written or removed:
    /// whose shares are synced is to be read again ([`crate::drive::HomebaseRemote::markers`]).
    /// Not a node: nothing of it is in the feed.
    Markers,
    /// The connection broke; it is retried with backoff.
    Lost(String),
    /// The identity refused the subscription (credentials revoked, no access to the drive):
    /// asked again later, as for a broken connection but not before [`REFUSED_PAUSE`].
    Refused(String),
}

/// The shortest read timeout (a zero one would mean none).
const MIN_WAIT: Duration = Duration::from_millis(10);
/// The longest read timeout. Windows doesn't say whether shutting a socket down ends a read
/// blocked on it: there a stop or reconnect is also seen at this slow poll.
#[cfg(windows)]
const MAX_WAIT: Duration = Duration::from_secs(5);
#[cfg(not(windows))]
const MAX_WAIT: Duration = Duration::MAX;
const PING_EVERY: Duration = Duration::from_secs(30);
/// No frame at all (not even a pong) for this long: the connection is dead.
const SILENCE: Duration = Duration::from_secs(75);
const HANDSHAKE: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A refused subscription isn't asked again sooner, doubling while it stays refused up to
/// [`REFUSED_MAX`]: the account asks the drive nothing meanwhile (its folders wait for a
/// login), so this is what finds out the credentials work again.
const REFUSED_PAUSE: Duration = Duration::from_secs(300);
const REFUSED_MAX: Duration = Duration::from_secs(3600);

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

/// The notification connection, on its own thread. Stops when dropped: its socket is shut
/// down, so the thread ends at once (it isn't waited for: resolving a name can take a while).
/// The thread blocks until a frame comes or the keep-alive is due: no wake-up in between.
#[derive(Debug)]
pub struct Notifier {
    control: Arc<Control>,
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
        Self::subscribe(
            move || client.clone(),
            Subscription { drive, own: false },
            events,
        )
    }

    /// As [`Notifier::spawn`], for what another identity shares: connects to their server as
    /// the client `theirs` gives ([`Client::for_peer`]) and subscribes to `drive`, theirs.
    /// `theirs` is asked at every connect: the token their server takes may have been
    /// replaced since the last. [`Event::Refused`]: their server doesn't take the token.
    pub fn spawn_shared(
        theirs: impl Fn() -> Client + Send + 'static,
        drive: TargetDrive,
        events: Sender<Event>,
    ) -> io::Result<Self> {
        // Their server offers no leaving out of own writes: a member makes none there.
        Self::subscribe(theirs, Subscription { drive, own: true }, events)
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
        Self::subscribe(move || client.clone(), Subscription { drive, own }, events)
    }

    /// `client`: who each connection speaks as, asked as it is made.
    fn subscribe(
        client: impl Fn() -> Client + Send + 'static,
        subscription: Subscription,
        events: Sender<Event>,
    ) -> io::Result<Self> {
        let control = Arc::new(Control::default());
        let received = Arc::new(Received::default());
        let (told, counts) = (control.clone(), received.clone());
        std::thread::Builder::new()
            .name("hbsync-notify".into())
            .spawn(move || run(&client, subscription, &events, &told, &counts))?;
        Ok(Notifier { control, received })
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
        self.control.tell(&self.control.reconnect);
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        self.control.tell(&self.control.stop);
    }
}

/// What the notifier tells its thread, and how it reaches it at once: the open connection's
/// socket is shut down (a read or a TLS handshake blocked on it ends), a backoff's wait is
/// woken.
#[derive(Debug, Default)]
struct Control {
    stop: AtomicBool,
    reconnect: AtomicBool,
    /// A connection was made before: the drive's key was read before the first.
    connected_before: AtomicBool,
    /// The open connection's socket (a clone of it), while there is one.
    socket: Mutex<Option<TcpStream>>,
    /// Wakes a backoff's wait.
    told: Condvar,
}

impl Control {
    /// Sets `flag` (one of its own) and makes the thread see it at once.
    fn tell(&self, flag: &AtomicBool) {
        flag.store(true, Ordering::SeqCst);
        // Its reading side only: the thread can still say goodbye (a Close frame).
        if let Some(socket) = &*self.socket() {
            let _ = socket.shutdown(Shutdown::Read);
        }
        self.told.notify_all();
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Stopping, or to reconnect.
    fn told(&self) -> bool {
        self.stopping() || self.reconnect.load(Ordering::SeqCst)
    }

    /// Keeps a clone of `tcp`, the new connection's socket, to shut it down when told. False
    /// when told already: the connection isn't to be used. Without a clone (the OS refused
    /// one), a stop is seen when the next read times out, at the keep-alive at the latest.
    fn hold(&self, tcp: &TcpStream) -> bool {
        let mut socket = self.socket();
        if self.told() {
            return false;
        }
        *socket = tcp.try_clone().ok();
        true
    }

    /// The connection is closed.
    fn release(&self) {
        *self.socket() = None;
    }

    /// Waits `time`, or until told.
    fn wait(&self, time: Duration) {
        let socket = self.socket();
        let _ = self.told.wait_timeout_while(socket, time, |_| !self.told());
    }

    fn socket(&self) -> MutexGuard<'_, Option<TcpStream>> {
        self.socket.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What the connection subscribes to.
#[derive(Debug, Clone, Copy)]
struct Subscription {
    drive: TargetDrive,
    /// Hears this client's own writes too (measurements; and on another identity's server,
    /// which offers nothing else).
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
    client: &dyn Fn() -> Client,
    subscription: Subscription,
    events: &Sender<Event>,
    control: &Control,
    received: &Received,
) {
    const FIRST: Duration = Duration::from_secs(1);
    let mut backoff = FIRST;
    let mut refused_wait = REFUSED_PAUSE;
    while !control.stopping() {
        let mut came = Came::default();
        let result = session(
            &client(),
            subscription,
            events,
            control,
            received,
            &mut came,
        );
        control.release();
        let Came { connected, refused } = came;
        if connected {
            backoff = FIRST;
            refused_wait = REFUSED_PAUSE;
        }
        let Err(why) = result else {
            return; // stopped, or nobody listens any more
        };
        if control.stopping() {
            return;
        }
        if control.reconnect.swap(false, Ordering::Relaxed) {
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
            let wait = refused_wait.mul_f64(1.0 + 0.2 * jitter);
            refused_wait = (refused_wait * 2).min(REFUSED_MAX);
            wait
        } else {
            backoff.mul_f64(0.8 + 0.4 * jitter)
        };
        control.wait(wait);
        backoff = if control.reconnect.swap(false, Ordering::Relaxed) {
            FIRST
        } else {
            (backoff * 2).min(MAX_BACKOFF)
        };
    }
}

type Socket = WebSocket<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>;

/// How far one connection came.
#[derive(Debug, Default, PartialEq, Eq)]
struct Came {
    /// The server subscribed it.
    connected: bool,
    /// The server refused it: its upgrade, or what it said who it is.
    refused: bool,
}

/// One connection, until it breaks or is `refused` (`Err`) or the notifier stops (`Ok`).
fn session(
    client: &Client,
    subscription: Subscription,
    events: &Sender<Event>,
    control: &Control,
    received: &Received,
    came: &mut Came,
) -> Result<(), String> {
    let mut socket = match connect(client, control) {
        Ok(socket) => socket,
        Err(Unopened::Lost(why)) => return Err(why),
        // The upgrade answered 401 or 403: the identity refused the credentials.
        Err(Unopened::Refused(why)) => {
            let _ = events.send(Event::Refused(why.clone()));
            came.refused = true;
            return Err(why);
        }
    };
    converse(
        &mut socket,
        client,
        subscription,
        events,
        control,
        received,
        came,
    )
}

/// A connection's stream, whose reads can be given a timeout.
trait Timed {
    fn wait_at_most(&self, wait: Duration) -> io::Result<()>;
}

impl Timed for rustls::StreamOwned<rustls::ClientConnection, TcpStream> {
    fn wait_at_most(&self, wait: Duration) -> io::Result<()> {
        self.get_ref().set_read_timeout(Some(wait))
    }
}

/// What is said on an open connection: subscribes, then hears the server until the
/// connection breaks or is `refused` (`Err`) or the notifier stops (`Ok`).
fn converse<S: io::Read + io::Write + Timed>(
    socket: &mut WebSocket<S>,
    client: &Client,
    subscription: Subscription,
    events: &Sender<Event>,
    control: &Control,
    received: &Received,
    came: &mut Came,
) -> Result<(), String> {
    let first = opening(client, &subscription.request().to_string())?;
    socket
        .send(Message::text(first))
        .map_err(|e| format!("subscribing: {e}"))?;
    let opened = Instant::now();
    let (mut last_frame, mut last_ping) = (Instant::now(), Instant::now());
    // Told to stop or reconnect: the socket is shut down, or about to be.
    let told = || match control.stopping() {
        true => Ok(()),
        false => Err("reconnecting on request".to_owned()),
    };
    loop {
        if control.told() {
            let _ = socket.close(None);
            return told();
        }
        // Blocks until a frame comes or the next check is due (the socket is shut down when
        // told).
        let wait = next_check(
            Instant::now(),
            opened,
            came.connected,
            last_frame,
            last_ping,
        );
        socket
            .get_ref()
            .wait_at_most(wait)
            .map_err(|e| e.to_string())?;
        match socket.read() {
            Ok(Message::Text(text)) => {
                last_frame = Instant::now();
                received.frames.fetch_add(1, Ordering::Relaxed);
                received
                    .bytes
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                let event = match notification(client, text.as_str()) {
                    Notification::Handshake if !came.connected => {
                        came.connected = true;
                        if control.connected_before.swap(true, Ordering::SeqCst) {
                            client.connected();
                        }
                        Event::Connected
                    }
                    Notification::Changed(change) => Event::Changed(change),
                    Notification::Markers => Event::Markers,
                    Notification::Refused(why) => {
                        let _ = socket.close(None);
                        let _ = events.send(Event::Refused(why.clone()));
                        came.refused = true;
                        return Err(why);
                    }
                    _ => continue,
                };
                if events.send(event).is_err() {
                    return Ok(());
                }
            }
            Ok(Message::Close(frame)) => {
                let why = format!("closed by the server: {frame:?}");
                // Another identity's server closes a connection whose token it doesn't take
                // without a word (verified live: `homebase-api-notes.md`, "Shared with you";
                // the `authenticationError` it means to send first doesn't arrive).
                if client.peer() && !came.connected {
                    let _ = events.send(Event::Refused(why.clone()));
                    came.refused = true;
                }
                return Err(why);
            }
            Ok(_) => last_frame = Instant::now(),
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) if control.told() => return told(),
            Err(e) => return Err(e.to_string()),
        }
        if !came.connected && opened.elapsed() > HANDSHAKE {
            return Err("no handshake from the server".into());
        }
        if last_frame.elapsed() > SILENCE {
            return Err("the connection went silent".into());
        }
        if last_ping.elapsed() > PING_EVERY {
            send(socket, client, "ping", "ping")?;
            last_ping = Instant::now();
        }
    }
}

/// How long the session may block on a read before it has something to check: the
/// handshake's deadline, the silence's, the next ping.
fn next_check(
    now: Instant,
    opened: Instant,
    connected: bool,
    last_frame: Instant,
    last_ping: Instant,
) -> Duration {
    let mut at = (last_frame + SILENCE).min(last_ping + PING_EVERY);
    if !connected {
        at = at.min(opened + HANDSHAKE);
    }
    at.saturating_duration_since(now).clamp(MIN_WAIT, MAX_WAIT)
}

/// Why no connection was opened.
enum Unopened {
    /// Not reached, or it broke: tried again soon.
    Lost(String),
    /// The identity refused the credentials at the upgrade (401, 403).
    Refused(String),
}

impl From<String> for Unopened {
    fn from(why: String) -> Self {
        Unopened::Lost(why)
    }
}

fn connect(client: &Client, control: &Control) -> Result<Socket, Unopened> {
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
    if !control.hold(&tcp) {
        return Err(Unopened::Lost("told before connecting".into()));
    }
    tcp.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| format!("{host}: {e}"))?;
    let tls = rustls::ClientConnection::new(TLS.clone()?, name).map_err(|e| e.to_string())?;
    let stream = rustls::StreamOwned::new(tls, tcp);

    let (socket, _) = tungstenite::client(upgrade(client)?, stream).map_err(|e| match e {
        tungstenite::HandshakeError::Failure(e) => unopened(host, &e),
        tungstenite::HandshakeError::Interrupted(_) => {
            Unopened::Lost("handshake timed out".to_owned())
        }
    })?;
    Ok(socket)
}

/// The request that upgrades a connection to `client`'s server to the notification socket:
/// the identity's own with the token as a subprotocol, another identity's with nothing of it
/// (it goes in the first frame: [`opening`]).
fn upgrade(client: &Client) -> Result<tungstenite::handshake::client::Request, String> {
    let host = client.identity();
    if client.peer() {
        return format!("wss://{host}/api/v2/peer/notify/ws-token")
            .into_client_request()
            .map_err(|e| e.to_string());
    }
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
    Ok(request)
}

/// Why the upgrade to `host` failed with `e`: refused for the credentials (401, 403) only in the
/// identity's own answer; a hotel's or a proxy's, as any other failure, is a connection lost.
fn unopened(host: &str, e: &tungstenite::Error) -> Unopened {
    match e {
        tungstenite::Error::Http(answer) => {
            let status = answer.status().as_u16();
            if !has_odin_header(answer.headers()) {
                Unopened::Lost(not_from_identity(host, status))
            } else if refuses(status) {
                Unopened::Refused(format!("{host}: HTTP {status}"))
            } else {
                Unopened::Lost(e.to_string())
            }
        }
        _ => Unopened::Lost(e.to_string()),
    }
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

/// `command` with `data`, sealed with `client`'s secret: a client frame.
fn sealed(client: &Client, command: &str, data: &str) -> Result<Envelope, String> {
    let plain = json!({ "command": command, "data": data }).to_string();
    Envelope::seal(client.key(), plain.as_bytes()).map_err(|e| e.to_string())
}

/// A connection's first frame, which subscribes as `request` says: the sealed command, and
/// to another identity's server the token beside it (its upgrade carried none: [`upgrade`]).
fn opening(client: &Client, request: &str) -> Result<String, String> {
    let subscription = sealed(client, "establishConnectionRequest", request)?;
    let frame = match client.peer() {
        true => serde_json::to_string(&json!({
            "clientAuthToken64": client.token(),
            "sharedSecretEncryptedOptions": subscription,
        })),
        false => serde_json::to_string(&subscription),
    };
    frame.map_err(|e| e.to_string())
}

fn send<S: io::Read + io::Write>(
    socket: &mut WebSocket<S>,
    client: &Client,
    command: &str,
    data: &str,
) -> Result<(), String> {
    let text = serde_json::to_string(&sealed(client, command, data)?).map_err(|e| e.to_string())?;
    socket
        .send(Message::text(text))
        .map_err(|e| format!("sending {command}: {e}"))
}

#[derive(Debug, PartialEq, Eq)]
enum Notification {
    Handshake,
    Changed(Option<ChangeRef>),
    Markers,
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
        let data = data.as_deref();
        match data.map(named) {
            // A share marker: who syncs is read again.
            Some(Named::Marker) => Notification::Markers,
            // Another app's file on the drive: not the client's, nothing to read or await.
            Some(Named::Other) => Notification::Other,
            _ => Notification::Changed(data.and_then(|d| change(d, kind("fileDeleted")))),
        }
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

/// What kind of file a drive event names, by its type (now, or before).
enum Named {
    /// A sync node, or the event doesn't say.
    Node,
    /// A share marker.
    Marker,
    /// Another app's file.
    Other,
}

fn named(data: &str) -> Named {
    let Ok(event) = serde_json::from_str::<DriveEvent>(data) else {
        return Named::Node;
    };
    match event.file_ref.as_ref().or(event.previous_file_ref.as_ref()) {
        Some(file) if file.is_marker() => Named::Marker,
        Some(file) if file.not_a_node() => Named::Other,
        _ => Named::Node,
    }
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
        removed: false,
        changed_at: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(body: serde_json::Value) -> Notification {
        classify(body.to_string().as_bytes())
    }

    /// Connected and quiet, a read blocks until the ping is due, never on a short tick;
    /// before the handshake, until its deadline.
    #[test]
    fn a_read_blocks_until_the_next_check() {
        let t0 = Instant::now();
        let s = Duration::from_secs;
        let check = |now, connected, last_frame, last_ping| {
            next_check(now, t0, connected, last_frame, last_ping)
        };
        assert_eq!(check(t0, true, t0, t0), PING_EVERY.min(MAX_WAIT));
        assert_eq!(
            check(t0 + s(10), true, t0, t0),
            (PING_EVERY - s(10)).min(MAX_WAIT)
        );
        assert_eq!(check(t0, false, t0, t0), HANDSHAKE.min(MAX_WAIT));
        // Pinged, but nothing heard for long: the silence's deadline.
        let late = t0 + SILENCE - s(5);
        assert_eq!(check(late, true, t0, late), s(5));
        // Overdue: a short wait, never a zero one (no timeout at all).
        assert_eq!(next_check(t0 + s(100), t0, true, t0, t0), MIN_WAIT);
    }

    /// A local socket pair: what the notifier holds, and the server's end.
    fn pair() -> io::Result<(TcpStream, TcpStream)> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let client = TcpStream::connect(listener.local_addr()?)?;
        Ok((client, listener.accept()?.0))
    }

    /// A stop or a reconnect ends a read blocked on the connection at once. (Not on Windows,
    /// whose blocked reads end by their timeout: `MAX_WAIT`.)
    #[cfg(not(windows))]
    #[test]
    fn a_stop_or_reconnect_ends_a_blocked_read() -> io::Result<()> {
        use std::io::Read;
        for stop in [true, false] {
            let control = Arc::new(Control::default());
            let (mut tcp, _server) = pair()?;
            assert!(control.hold(&tcp));
            // As the session reads: far longer than the test waits.
            tcp.set_read_timeout(Some(Duration::from_secs(60)))?;
            let reading = std::thread::spawn(move || tcp.read(&mut [0u8; 16]).map(drop));
            std::thread::sleep(Duration::from_millis(50));
            let told = Instant::now();
            control.tell(if stop {
                &control.stop
            } else {
                &control.reconnect
            });
            let _ = reading.join();
            assert!(
                told.elapsed() < Duration::from_secs(5),
                "{:?}",
                told.elapsed()
            );
            assert_eq!(control.stopping(), stop);
        }
        Ok(())
    }

    /// Told to stop or reconnect, a backoff isn't waited out, and a connection made since
    /// isn't used; not told, a backoff is waited out.
    #[test]
    fn a_stop_or_reconnect_ends_a_backoff() -> io::Result<()> {
        for stop in [true, false] {
            let control = Control::default();
            control.tell(if stop {
                &control.stop
            } else {
                &control.reconnect
            });
            let waited = Instant::now();
            control.wait(Duration::from_secs(60));
            assert!(
                waited.elapsed() < Duration::from_secs(5),
                "a backoff isn't waited out"
            );
            assert!(!control.hold(&pair()?.0), "told: not used");
        }
        let control = Control::default();
        let waited = Instant::now();
        control.wait(Duration::from_millis(100));
        assert!(waited.elapsed() >= Duration::from_millis(100));
        assert!(control.hold(&pair()?.0));
        Ok(())
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

    fn client_of(identity: &str) -> Client {
        Client::new(&crate::Credentials {
            identity: identity.into(),
            client_auth_token: "dG9rZW4/Pz8+".into(),
            shared_secret: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            zk_app_key: None,
        })
        .expect("a client")
    }

    fn peer_of(identity: &str) -> Client {
        let token = crate::peer::PeerToken {
            token: "dGhlaXIgdG9rZW4/Pg==".into(),
            shared_secret: "AQEBAQEBAQEBAQEBAQEBAQ==".into(),
        };
        Client::for_peer(identity, &token).expect("a client")
    }

    /// The identity's own socket is upgraded with the token as a subprotocol; another
    /// identity's by its peer route with no token at all (its first frame carries it).
    #[test]
    fn a_sharers_socket_is_upgraded_without_the_token() -> Result<(), String> {
        let own = upgrade(&client_of("tom.example"))?;
        assert_eq!(
            own.uri().to_string(),
            "wss://tom.example/api/v2/notify/ws-token"
        );
        let protocols = own.headers()["Sec-WebSocket-Protocol"]
            .to_str()
            .unwrap_or("");
        // base64url of the token's bytes, no padding.
        assert_eq!(protocols, "odin.notify.v1, odin.bearer.dG9rZW4_Pz8-");

        let theirs = upgrade(&peer_of("pippin.example"))?;
        assert_eq!(
            theirs.uri().to_string(),
            "wss://pippin.example/api/v2/peer/notify/ws-token"
        );
        assert!(!theirs.headers().contains_key("Sec-WebSocket-Protocol"));
        assert!(!format!("{theirs:?}").contains("dGhlaXIgdG9rZW4"));
        Ok(())
    }

    /// The first frame: the identity's own server gets the subscription sealed; another
    /// identity's the token in the clear beside it, and the subscription sealed with that
    /// token's secret, asking for every write (their server offers nothing else).
    #[test]
    fn a_sharers_socket_opens_with_the_token_beside_the_subscription()
    -> Result<(), Box<dyn std::error::Error>> {
        let drive = TargetDrive {
            alias: uuid::Uuid::from_u128(1),
            drive_type: uuid::Uuid::from_u128(2),
        };
        let opened = |client: &Client, sealed: &serde_json::Value| {
            let envelope: Envelope = serde_json::from_value(sealed.clone())?;
            let command: serde_json::Value = serde_json::from_slice(&envelope.open(client.key())?)?;
            assert_eq!(command["command"], "establishConnectionRequest");
            let data = command["data"].as_str().ok_or("data isn't a string")?;
            Ok::<serde_json::Value, Box<dyn std::error::Error>>(serde_json::from_str(data)?)
        };

        let own = client_of("tom.example");
        let request = Subscription { drive, own: false }.request().to_string();
        let frame: serde_json::Value = serde_json::from_str(&opening(&own, &request)?)?;
        assert!(frame.get("clientAuthToken64").is_none(), "{frame}");
        assert_eq!(opened(&own, &frame)?["excludeOwnWrites"], true);

        let theirs = peer_of("pippin.example");
        // As `Notifier::spawn_shared` subscribes.
        let request = Subscription { drive, own: true }.request().to_string();
        let frame: serde_json::Value = serde_json::from_str(&opening(&theirs, &request)?)?;
        assert_eq!(frame["clientAuthToken64"], "dGhlaXIgdG9rZW4/Pg==");
        let options = opened(&theirs, &frame["sharedSecretEncryptedOptions"])?;
        assert_eq!(options["excludeOwnWrites"], false);
        assert_eq!(options["resultShape"], "minimal");
        assert_eq!(
            options["drives"][0]["alias"],
            "00000000000000000000000000000001"
        );
        // Sealed with their token's secret, not readable without it.
        let sealed: Envelope =
            serde_json::from_value(frame["sharedSecretEncryptedOptions"].clone())?;
        assert!(
            sealed
                .open(own.key())
                .ok()
                .and_then(|plain| serde_json::from_slice::<serde_json::Value>(&plain).ok())
                .is_none()
        );
        Ok(())
    }

    impl Timed for TcpStream {
        fn wait_at_most(&self, wait: Duration) -> io::Result<()> {
            self.set_read_timeout(Some(wait))
        }
    }

    /// A server frame in the clear.
    fn frame(body: serde_json::Value) -> Message {
        Message::text(json!({"isEncrypted": false, "payload": body.to_string()}).to_string())
    }

    /// What a connection comes to as `client`, against a server on this machine that reads
    /// its first frame, sends `answers` and closes: that first frame, how the connection
    /// ended, how far it came, and what it told.
    #[allow(clippy::type_complexity)]
    fn conversed(
        client: &Client,
        answers: Vec<Message>,
    ) -> Result<(serde_json::Value, Result<(), String>, Came, Vec<Event>), Box<dyn std::error::Error>>
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = std::thread::spawn(move || -> Result<String, String> {
            let (tcp, _) = listener.accept().map_err(|e| e.to_string())?;
            let mut socket = tungstenite::accept(tcp).map_err(|e| e.to_string())?;
            let first = socket.read().map_err(|e| e.to_string())?;
            for answer in answers {
                socket.send(answer).map_err(|e| e.to_string())?;
            }
            socket.close(None).map_err(|e| e.to_string())?;
            // Until the client has answered the close.
            while socket.read().is_ok() {}
            first
                .into_text()
                .map(|t| t.to_string())
                .map_err(|e| e.to_string())
        });
        let tcp = TcpStream::connect(addr)?;
        let (mut socket, _) =
            tungstenite::client(format!("ws://{addr}/"), tcp).map_err(|e| e.to_string())?;
        let (tx, rx) = std::sync::mpsc::channel();
        let drive = TargetDrive {
            alias: uuid::Uuid::from_u128(1),
            drive_type: uuid::Uuid::from_u128(2),
        };
        let subscription = Subscription {
            drive,
            own: client.peer(),
        };
        let mut came = Came::default();
        let ended = converse(
            &mut socket,
            client,
            subscription,
            &tx,
            &Control::default(),
            &Received::default(),
            &mut came,
        );
        // Hung up: the server's last read ends.
        drop(socket);
        drop(tx);
        let first = server.join().map_err(|_| "the server panicked")??;
        Ok((
            serde_json::from_str(&first)?,
            ended,
            came,
            rx.iter().collect(),
        ))
    }

    /// Another identity's server closes a connection whose token it doesn't take without a
    /// word: a refusal, asked again later, not a connection lost and tried again at once. The
    /// identity's own server closing is a connection lost, as before.
    #[test]
    fn a_sharers_server_closing_before_it_subscribes_refuses()
    -> Result<(), Box<dyn std::error::Error>> {
        let (first, ended, came, told) = conversed(&peer_of("pippin.example"), Vec::new())?;
        assert_eq!(first["clientAuthToken64"], "dGhlaXIgdG9rZW4/Pg==");
        assert!(ended.is_err(), "{ended:?}");
        let refused = Came {
            connected: false,
            refused: true,
        };
        assert_eq!(came, refused);
        assert!(matches!(told[..], [Event::Refused(_)]), "{told:?}");

        let (first, ended, came, told) = conversed(&client_of("tom.example"), Vec::new())?;
        assert!(first.get("clientAuthToken64").is_none(), "{first}");
        assert!(ended.is_err(), "{ended:?}");
        assert_eq!(came, Came::default());
        assert!(told.is_empty(), "{told:?}");
        Ok(())
    }

    /// Their server subscribes the connection and tells of a file as the identity's own
    /// does; closing after that is a connection lost, not a refusal.
    #[test]
    fn a_sharers_server_subscribes_and_tells() -> Result<(), Box<dyn std::error::Error>> {
        let file = json!({"fileRef": {"fileId": uuid::Uuid::new_v4(),
            "versionTag": uuid::Uuid::new_v4(), "fileState": "active", "updated": 1,
            "fileType": 7101}});
        let answers = vec![
            frame(json!({"notificationType": "deviceHandshakeSuccess"})),
            frame(json!({"notificationType": "fileModified", "data": file.to_string()})),
        ];
        let (_, ended, came, told) = conversed(&peer_of("pippin.example"), answers)?;
        assert!(ended.is_err(), "{ended:?}");
        let subscribed = Came {
            connected: true,
            refused: false,
        };
        assert_eq!(came, subscribed);
        assert!(
            matches!(told[..], [Event::Connected, Event::Changed(Some(_))]),
            "{told:?}"
        );
        Ok(())
    }

    /// A token their server doesn't take is answered in the clear (it has no secret to seal
    /// with): a refusal.
    #[test]
    fn a_token_refused_in_the_clear_is_a_refusal() {
        let frame = json!({"isEncrypted": false, "payload":
            json!({"notificationType": "authenticationError", "data": "Invalid Token"}).to_string()});
        assert_eq!(
            notification(&peer_of("pippin.example"), &frame.to_string()),
            Notification::Refused("refused: Invalid Token".into())
        );
    }

    /// An event of another app's file on the drive (a type not a sync node's) is no change of
    /// the client's: nothing to read or await. A sync node's is.
    #[test]
    fn another_apps_file_is_not_announced() {
        let event = |file_type: i32| {
            let data = json!({"fileRef": {"fileId": uuid::Uuid::new_v4(),
                "versionTag": uuid::Uuid::new_v4(), "fileState": "active", "updated": 1,
                "fileType": file_type}});
            classify(
                json!({"notificationType": "fileAdded", "data": data.to_string()})
                    .to_string()
                    .as_bytes(),
            )
        };
        assert_eq!(event(100), Notification::Other);
        // A share marker: neither a node nor nothing.
        assert_eq!(event(7103), Notification::Markers);
        assert!(matches!(event(7102), Notification::Changed(Some(_))));
        assert!(matches!(event(7101), Notification::Changed(Some(_))));
    }

    /// An upgrade the identity refused for the credentials is a refusal (the account waits for
    /// a login); any other failure is a connection lost, a hotel's or a proxy's 401 or 403 too.
    #[test]
    fn a_refused_upgrade_is_a_refusal() -> Result<(), Box<dyn std::error::Error>> {
        let answer = |status: u16, odin: bool| -> Result<Unopened, Box<dyn std::error::Error>> {
            let mut response = tungstenite::http::Response::builder().status(status);
            if odin {
                response = response.header("X-Odin-Version", "1.0");
            }
            let e = tungstenite::Error::Http(Box::new(response.body(None)?));
            Ok(unopened("frodo.dotyou.cloud", &e))
        };
        assert!(matches!(answer(401, true)?, Unopened::Refused(_)));
        assert!(matches!(answer(403, true)?, Unopened::Refused(_)));
        assert!(matches!(answer(502, true)?, Unopened::Lost(_)));
        assert!(
            matches!(answer(403, false)?, Unopened::Lost(why) if why.contains("not from the identity"))
        );
        let closed = unopened("frodo.dotyou.cloud", &tungstenite::Error::ConnectionClosed);
        assert!(matches!(closed, Unopened::Lost(_)));
        Ok(())
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
                "archivalStatus": archival, "groupId": parent, "updated": 8, "versionUpdated": 7,
                "fileType": 7101})
        };
        let changed = |deleted, removed, changed_at, parent| {
            Notification::Changed(Some(ChangeRef {
                file_id: RemoteFileId(file),
                version: VersionTag(version),
                deleted,
                parent,
                removed,
                changed_at,
            }))
        };
        assert_eq!(
            event(
                "fileModified",
                json!({"targetDrive": {}, "fileRef": row("active", 2)})
            ),
            changed(false, true, Some(7), Some(NodeId(parent)))
        );
        // A tombstone (with its time: when it may go hard), and a hard delete (only the file as
        // it was): deleted either way.
        assert_eq!(
            event(
                "fileDeleted",
                json!({"fileRef": row("deleted", 0), "previousFileRef": row("active", 2)})
            ),
            changed(true, false, Some(7), None)
        );
        assert_eq!(
            event(
                "fileDeleted",
                json!({"targetDrive": {}, "previousFileRef": row("active", 2)})
            ),
            changed(true, false, None, None)
        );
        // An event that doesn't say (full headers, or no row): the feed is read.
        assert_eq!(
            event("fileAdded", json!({"header": {"fileId": file}})),
            Notification::Changed(None)
        );
    }

    /// A member's socket announces a share withdrawn as a `fileDeleted` of the tombstone
    /// odin-core makes for it (**[verified]** `ClientDriveNotification.MessageForAsync`,
    /// `NoLongerSharedTombstoneOf`, minimal: `FileRef.FromHeader`, no previous): no version, no
    /// `groupId`, its own `updated`, read as unshared. A hard delete's (only the file as it was,
    /// or just its id) has no time: a delete, not a share withdrawn.
    #[test]
    fn a_withdrawn_share_is_heard_as_unshared() {
        use uuid::Uuid;
        let file = Uuid::new_v4();
        let deleted = |data: serde_json::Value| match n(
            json!({"notificationType": "fileDeleted", "data": data.to_string()}),
        ) {
            Notification::Changed(Some(change)) => change,
            other => panic!("{other:?}"),
        };
        let withdrawn = deleted(json!({"targetDrive": {}, "fileRef": {
            "fileId": file, "fileState": "deleted", "fileType": 7101, "dataType": 0,
            "archivalStatus": 0, "versionTag": Uuid::nil(), "created": 5, "updated": 9,
            "fileByteCount": 0}}));
        assert!(
            withdrawn.is_unshared() && withdrawn.parent.is_none(),
            "{withdrawn:?}"
        );
        let before = json!({"fileId": file, "fileState": "active", "fileType": 7101,
            "archivalStatus": 0, "versionTag": Uuid::new_v4(), "groupId": Uuid::new_v4(),
            "updated": 7});
        for hard in [
            deleted(json!({"targetDrive": {}, "previousFileRef": before})),
            deleted(json!({"previousFileRef": {"fileId": file}})),
        ] {
            assert!(hard.deleted && !hard.is_unshared(), "{hard:?}");
        }
    }
}
