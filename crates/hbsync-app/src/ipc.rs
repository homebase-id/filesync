//! The daemon's control socket: how the CLI asks for the status and sends requests.
//!
//! One JSON request per line, one JSON reply per line, then the connection closes; but a
//! [`Ask::Watch`], whose connection is handed over and stays open for replies to come. Each
//! request carries the protocol version; a daemon refuses one it doesn't speak (after an update,
//! until it restarts).
//!
//! Only this user can connect:
//! - Unix: a socket file in a folder only this user can open (`0700`).
//! - Windows: a named pipe whose security descriptor grants only its owner (and SYSTEM). Pipe
//!   names are machine-wide, so the name carries a random token kept in this user's state
//!   folder: another user can't guess it to squat it.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(crate) use interprocess::local_socket::Listener;
use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{ListenerOptions, Name, Stream};

use crate::Paths;
use crate::protocol::{Ask, OTHER_PROTOCOL, PROTOCOL, Reply, Request};

/// How long either side waits for the other's line.
const PATIENCE: Duration = Duration::from_secs(10);
/// Longer lines are refused, both ways. Only this user can connect (see above: a socket in a
/// `0700` folder of this user's, a pipe for its owner and SYSTEM), so a bound only stops a
/// runaway line: requests name tens of thousands of items to restore or empty, and the longest
/// reply is a list (about 300 bytes a delete of Recently deleted, listed by deletes).
const MAX_LINE: u64 = 64 << 20;

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    /// No daemon listens (none was started, or it stopped).
    #[error("the daemon isn't running")]
    NotRunning,
    #[error("talking to the daemon: {0}")]
    Io(#[from] io::Error),
    #[error("the daemon's answer: {0}")]
    Protocol(String),
}

/// Asks the daemon `request` and waits for its reply.
pub(crate) fn call(paths: &Paths, request: &Request) -> Result<Reply, IpcError> {
    let mut stream = open(paths, request)?;
    read_reply(&mut stream)?.ok_or(IpcError::Protocol("no answer".into()))
}

/// Connects to the daemon and sends `request`.
fn open(paths: &Paths, request: &Request) -> Result<BufReader<Stream>, IpcError> {
    let name = endpoint(paths, false).map_err(not_running)?;
    let mut stream = Stream::connect(name).map_err(not_running)?;
    patience(&stream)?;
    write_line(&mut stream, request)?;
    Ok(BufReader::new(stream))
}

/// The other side's next reply; `None` at the end of the stream.
fn read_reply(stream: &mut BufReader<Stream>) -> Result<Option<Reply>, IpcError> {
    let Some(line) = read_line(stream)? else {
        return Ok(None);
    };
    serde_json::from_str(&line)
        .map(Some)
        .map_err(|e| IpcError::Protocol(e.to_string()))
}

/// Writes `value` as a JSON line.
fn write_line(stream: &mut Stream, value: &impl serde::Serialize) -> Result<(), IpcError> {
    let mut out = serde_json::to_string(value).map_err(|e| IpcError::Protocol(e.to_string()))?;
    out.push('\n');
    stream.write_all(out.as_bytes())?;
    Ok(())
}

fn not_running(e: io::Error) -> IpcError {
    match e.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => IpcError::NotRunning,
        _ => IpcError::Io(e),
    }
}

/// Listens on this user's control socket. Call it holding the [`InstanceLock`]: an
/// existing socket file is then left over from a daemon that died, and is replaced.
///
/// [`InstanceLock`]: crate::lock::InstanceLock
pub(crate) fn listen(paths: &Paths) -> io::Result<Listener> {
    let name = endpoint(paths, true)?;
    let options = ListenerOptions::new().name(name);
    #[cfg(windows)]
    let options = {
        use interprocess::os::windows::local_socket::ListenerOptionsExt;
        use interprocess::os::windows::security_descriptor::SecurityDescriptor;
        // Protected DACL: full access for the pipe's owner (this user) and SYSTEM, none for
        // anyone else.
        let sddl = widestring::U16CString::from_str("D:P(A;;GA;;;OW)(A;;GA;;;SY)")
            .map_err(|e| io::Error::other(e.to_string()))?;
        options.security_descriptor(SecurityDescriptor::deserialize(&sddl)?)
    };
    options.create_sync()
}

/// Answers requests on `listener` with `answer`, each connection on its own thread; hands a
/// [`Ask::Watch`]'s connection to `watch`. Returns only if the listener fails.
pub(crate) fn serve(
    listener: Listener,
    answer: impl Fn(Request) -> Reply + Send + Sync + 'static,
    watch: impl Fn(Stream) + Send + Sync + 'static,
) {
    let both = Arc::new((answer, watch));
    for conn in listener.incoming() {
        let Ok(conn) = conn else {
            continue;
        };
        let both = Arc::clone(&both);
        let _ = std::thread::Builder::new()
            .name("hbsync-ipc".into())
            .spawn(move || {
                if let Err(e) = handle(conn, &both.0, &both.1) {
                    tracing::debug!("a control connection failed: {e}");
                }
            });
    }
}

fn handle(
    conn: Stream,
    answer: &(dyn Fn(Request) -> Reply + Sync),
    watch: &(dyn Fn(Stream) + Sync),
) -> Result<(), IpcError> {
    patience(&conn)?;
    let mut conn = BufReader::new(conn);
    let Some(line) = read_line(&mut conn)? else {
        return Ok(());
    };
    // The version first: a request of another protocol may not parse here.
    #[derive(serde::Deserialize)]
    struct Envelope {
        protocol: u32,
        ask: serde_json::Value,
    }
    let not_a_request = |e: serde_json::Error| Reply::Refused(format!("not a request: {e}"));
    let reply = match serde_json::from_str::<Envelope>(&line) {
        // Whatever the version: an update stops the daemon of the build before it.
        Ok(e) if e.ask == "stop" => answer(Request::new(Ask::Stop)),
        Ok(e) if e.protocol != PROTOCOL => Reply::Refused(format!(
            "{OTHER_PROTOCOL}{PROTOCOL}, the request {}: restart the daemon",
            e.protocol
        )),
        Ok(_) => match serde_json::from_str::<Request>(&line) {
            Ok(Request {
                ask: Ask::Watch, ..
            }) => {
                watch(conn.into_inner());
                return Ok(());
            }
            Ok(request) => answer(request),
            Err(e) => not_a_request(e),
        },
        Err(e) => not_a_request(e),
    };
    write_line(conn.get_mut(), &reply)
}

/// Watches the daemon's status ([`Ask::Watch`]).
pub(crate) fn watch(paths: &Paths) -> Result<Watching, IpcError> {
    let stream = open(paths, &Request::new(Ask::Watch))?;
    // Replies come when the status changes: blocks until then.
    unless_unsupported(stream.get_ref().set_recv_timeout(None))?;
    Ok(Watching(stream))
}

/// The daemon's replies to an [`Ask::Watch`], as they come.
#[derive(Debug)]
pub(crate) struct Watching(BufReader<Stream>);

impl Watching {
    /// The next reply, waiting for it; `None` once the daemon closed the connection (it
    /// stopped).
    pub(crate) fn reply(&mut self) -> Result<Option<Reply>, IpcError> {
        read_reply(&mut self.0)
    }
}

/// Readies a watching client's connection ([`Ask::Watch`]): writes wait at most [`PATIENCE`]
/// where the OS can time them out (named pipes can't).
pub(crate) fn watched(stream: &Stream) -> io::Result<()> {
    unless_unsupported(stream.set_send_timeout(Some(PATIENCE)))
}

/// A reply as a line, to send to watching clients ([`send`]).
pub(crate) fn line(reply: &Reply) -> Result<String, IpcError> {
    let mut out = serde_json::to_string(reply).map_err(|e| IpcError::Protocol(e.to_string()))?;
    out.push('\n');
    Ok(out)
}

/// Writes a [`line`] to a watching client.
pub(crate) fn send(stream: &mut Stream, line: &str) -> io::Result<()> {
    stream.write_all(line.as_bytes())
}

/// Ok where the OS can't time out (named pipes).
fn unless_unsupported(set: io::Result<()>) -> io::Result<()> {
    match set {
        Err(e) if e.kind() == io::ErrorKind::Unsupported => Ok(()),
        other => other,
    }
}

/// Waits at most [`PATIENCE`] for the other side, where the OS can time out reads (named pipes
/// can't).
fn patience(stream: &Stream) -> io::Result<()> {
    unless_unsupported(stream.set_recv_timeout(Some(PATIENCE)))
}

/// One line of at most [`MAX_LINE`] bytes, without its end; `None` at the end of the stream.
fn read_line(stream: &mut BufReader<Stream>) -> Result<Option<String>, IpcError> {
    let mut line = String::new();
    let n = stream.by_ref().take(MAX_LINE).read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    if !line.ends_with('\n') {
        return Err(IpcError::Protocol("a line too long or cut off".into()));
    }
    line.pop();
    Ok(Some(line))
}

/// Socket addresses hold 104 bytes on macOS and 108 on Linux, terminator included.
#[cfg(unix)]
const MAX_SOCKET_PATH: usize = 100;

/// The socket file (Unix): in the runtime folder or, when that path is too long for a socket
/// address (a deep `HBSYNC_HOME`), in a private folder of the temp folder named after it.
#[cfg(unix)]
pub(crate) fn socket_path(paths: &Paths) -> PathBuf {
    const SOCKET: &str = "hbsyncd.sock";
    let path = paths.runtime.join(SOCKET);
    if path.as_os_str().len() <= MAX_SOCKET_PATH {
        return path;
    }
    use std::os::unix::ffi::OsStrExt;
    // Stable across builds, so an older daemon and a newer CLI agree.
    let hash = hbsync_platform::fnv::fnv1a64(paths.runtime.as_os_str().as_bytes());
    std::env::temp_dir()
        .join(format!("hbsync-{hash:016x}"))
        .join(SOCKET)
}

/// Where the daemon listens; `serving` prepares it for listening.
#[cfg(unix)]
fn endpoint(paths: &Paths, serving: bool) -> io::Result<Name<'static>> {
    use interprocess::local_socket::GenericFilePath;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let path = socket_path(paths);
    if path.as_os_str().len() > MAX_SOCKET_PATH {
        return Err(io::Error::other(format!(
            "the control socket's path is too long for the OS: {}",
            path.display()
        )));
    }
    let dir = path.parent().unwrap_or(&paths.runtime);
    if serving {
        // Only the runtime folder is created with its parents: the temp folder exists.
        std::fs::DirBuilder::new()
            .recursive(dir == paths.runtime)
            .mode(0o700)
            .create(dir)
            .or_else(|e| match e.kind() {
                io::ErrorKind::AlreadyExists => Ok(()),
                _ => Err(e),
            })?;
        if dir == paths.runtime {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    // A shared temp folder: the socket's folder must be this user's own and private, or
    // someone else could listen in our place.
    if dir != paths.runtime {
        let meta = match std::fs::symlink_metadata(dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound && !serving => {
                return Err(e);
            }
            other => other?,
        };
        // SAFETY: getuid has no preconditions and can't fail.
        let me = unsafe { libc::getuid() };
        if !meta.is_dir() || meta.uid() != me || meta.mode() & 0o777 != 0o700 {
            return Err(io::Error::other(format!(
                "{} isn't a private folder of this user: not using it for the control socket",
                dir.display()
            )));
        }
    }
    if serving {
        crate::paths::remove_if_exists(&path)?;
    }
    path.to_fs_name::<GenericFilePath>()
}

/// The file holding the pipe name's random part (Windows).
#[cfg(windows)]
fn token_path(paths: &Paths) -> PathBuf {
    paths.state.join("hbsyncd.pipe")
}

#[cfg(windows)]
fn endpoint(paths: &Paths, serving: bool) -> io::Result<Name<'static>> {
    use interprocess::local_socket::GenericNamespaced;
    let path = token_path(paths);
    let token = match std::fs::read_to_string(&path) {
        Ok(t) if !t.trim().is_empty() => t.trim().to_owned(),
        Err(e) if !serving => return Err(e),
        _ if !serving => return Err(io::ErrorKind::NotFound.into()),
        _ => {
            let token = uuid::Uuid::new_v4().simple().to_string();
            std::fs::create_dir_all(&paths.state)?;
            std::fs::write(&path, &token)?;
            token
        }
    };
    format!("hbsync-{token}").to_ns_name::<GenericNamespaced>()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use hbsync_types::activity::{Synced, SyncedKind};

    use super::*;
    use crate::protocol::{
        AccountControl, AccountStatus, Ask, Control, DaemonControl, DaemonStatus, FolderControl,
        FolderStatus, Pause, RecentItem, State,
    };

    fn deleted(i: usize) -> crate::protocol::DeletedItem {
        let node = hbsync_types::NodeId(hbsync_types::Uuid::from_u128(i as u128 + 1));
        crate::protocol::DeletedItem {
            identity: "a.example".into(),
            place: None,
            deleted: hbsync_types::activity::Deleted {
                node,
                parent: hbsync_types::NodeId::ROOT,
                path: format!("Photos/2024/a fairly long name of a photo {i}.jpg"),
                kind: hbsync_types::NodeKind::File,
                size: Some(1_234_567),
                removed_at_ms: Some(1_800_000_000_000 - i as i64),
                root: node,
                folder: Some("/home/a/Homebase".into()),
                stays: false,
            },
            along: 0,
            db: "d1".into(),
            seq: 1,
        }
    }

    fn status() -> DaemonStatus {
        let mut folder = FolderStatus::new(&"a.example".into(), Path::new("/home/a/Homebase"));
        folder.state = State::Idle;
        folder.pending = 3;
        DaemonStatus {
            version: crate::VERSION.into(),
            accounts: vec![AccountStatus::new(&"a.example".into())],
            folders: vec![folder],
        }
    }

    #[test]
    fn requests_and_replies_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        assert!(matches!(
            call(&paths, &Request::new(Ask::Status)),
            Err(IpcError::NotRunning)
        ));
        assert!(matches!(watch(&paths), Err(IpcError::NotRunning)));

        let listener = listen(&paths)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = std::sync::Mutex::new(tx);
        std::thread::spawn(move || {
            serve(
                listener,
                move |r| match r.ask {
                    Ask::Status => Reply::Status(Box::new(status())),
                    Ask::Control(c) => {
                        let _ = tx.lock().map(|tx| tx.send(c));
                        Reply::Done
                    }
                    // A large reply: megabytes in one line.
                    Ask::Deleted { limit } => Reply::Deleted(crate::protocol::DeletedList {
                        items: (0..limit).map(deleted).collect(),
                        accounts: Vec::new(),
                    }),
                    Ask::FindDeleted { .. } => Reply::Refused("not here".into()),
                    Ask::AddFolder { .. }
                    | Ask::RemoveFolder { .. }
                    | Ask::StartOver { .. }
                    | Ask::Watch
                    | Ask::Stop => Reply::Refused("not here".into()),
                    Ask::Recent { limit } => Reply::Recent(
                        (0..limit as u64)
                            .map(|i| RecentItem {
                                folder: PathBuf::from("/home/a/Homebase"),
                                main: true,
                                synced: Synced {
                                    at: 1_800_000_000 - i,
                                    kind: SyncedKind::Downloaded,
                                    path: format!("Photos/p{i}.jpg"),
                                    files: 1,
                                    bytes: 1000,
                                },
                            })
                            .collect(),
                    ),
                },
                |mut stream| {
                    // The status now, then as it changes; the connection ends with the daemon.
                    let mut changed = status();
                    changed.folders[0].pending = 4;
                    for s in [status(), changed] {
                        let _ = line(&Reply::Status(Box::new(s))).map(|l| send(&mut stream, &l));
                    }
                },
            )
        });

        let mut watching = watch(&paths)?;
        assert_eq!(watching.reply()?, Some(Reply::Status(Box::new(status()))));
        match watching.reply()? {
            Some(Reply::Status(s)) => assert_eq!(s.folders[0].pending, 4),
            other => panic!("{other:?}"),
        }
        assert_eq!(watching.reply()?, None, "the daemon closed it");

        match call(&paths, &Request::new(Ask::Status))? {
            Reply::Status(s) => assert_eq!(*s, status()),
            other => panic!("{other:?}"),
        }
        let confirm = Control::Folder {
            path: PathBuf::from("/home/a/Homebase"),
            control: FolderControl::ConfirmDeletes {
                token: "0123456789abcdef-60".into(),
            },
        };
        assert_eq!(
            call(&paths, &Request::new(Ask::Control(confirm.clone())))?,
            Reply::Done
        );
        assert_eq!(rx.recv_timeout(PATIENCE)?, confirm);
        match call(&paths, &Request::new(Ask::Recent { limit: 3 }))? {
            Reply::Recent(list) => assert_eq!(list.len(), 3),
            other => panic!("{other:?}"),
        }
        // Thousands of deletes, megabytes in one reply line.
        match call(&paths, &Request::new(Ask::Deleted { limit: 20_000 }))? {
            Reply::Deleted(list) => assert_eq!(list.items.len(), 20_000),
            other => panic!("{other:?}"),
        }

        // Another protocol version is refused, with the reason, but for a stop.
        let other = |ask| Request {
            protocol: PROTOCOL + 1,
            ask,
        };
        assert!(
            matches!(call(&paths, &other(Ask::Status))?, Reply::Refused(why) if why.starts_with(OTHER_PROTOCOL))
        );
        assert!(
            matches!(call(&paths, &other(Ask::Stop))?, Reply::Refused(why) if why == "not here"),
            "a stop reaches the daemon whatever its version"
        );
        Ok(())
    }

    /// The wire format docs/spec/daemon.md documents for protocol 6.
    #[test]
    fn requests_look_as_documented() -> Result<(), serde_json::Error> {
        let json = |ask| serde_json::to_string(&Request::new(ask));
        assert_eq!(json(Ask::Status)?, r#"{"protocol":8,"ask":"status"}"#);
        assert_eq!(json(Ask::Watch)?, r#"{"protocol":8,"ask":"watch"}"#);
        assert_eq!(json(Ask::Stop)?, r#"{"protocol":8,"ask":"stop"}"#);
        assert_eq!(
            json(Ask::Control(Control::Daemon(DaemonControl::SyncNow)))?,
            r#"{"protocol":8,"ask":{"control":{"daemon":"sync_now"}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Daemon(DaemonControl::Pause(
                Pause::ending_at(1_800_000_000)
            ))))?,
            r#"{"protocol":8,"ask":{"control":{"daemon":{"pause":{"until":1800000000}}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Daemon(DaemonControl::Pause(
                Pause::UNTIL_RESUMED
            ))))?,
            r#"{"protocol":8,"ask":{"control":{"daemon":{"pause":{"until":null}}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Folder {
                path: PathBuf::from("/h"),
                control: FolderControl::ConfirmDeletes { token: "t".into() }
            }))?,
            r#"{"protocol":8,"ask":{"control":{"folder":{"path":"/h","control":{"confirm_deletes":{"token":"t"}}}}}}"#
        );
        let node = hbsync_types::NodeId(hbsync_types::Uuid::from_u128(1));
        assert_eq!(
            json(Ask::Control(Control::Account {
                identity: "a.example".into(),
                control: AccountControl::Restore { nodes: vec![node] }
            }))?,
            r#"{"protocol":8,"ask":{"control":{"account":{"identity":"a.example","control":{"restore":{"nodes":["00000000-0000-0000-0000-000000000001"]}}}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Account {
                identity: "a.example".into(),
                control: AccountControl::EmptyListed {
                    db: "d1".to_owned(),
                    seq: 7
                }
            }))?,
            r#"{"protocol":8,"ask":{"control":{"account":{"identity":"a.example","control":{"empty_listed":{"db":"d1","seq":7}}}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Account {
                identity: "a.example".into(),
                control: AccountControl::EmptyDeleted {
                    nodes: vec![node],
                    db: "d1".to_owned(),
                    seq: 7
                }
            }))?,
            r#"{"protocol":8,"ask":{"control":{"account":{"identity":"a.example","control":{"empty_deleted":{"nodes":["00000000-0000-0000-0000-000000000001"],"db":"d1","seq":7}}}}}}"#
        );
        assert_eq!(
            json(Ask::Recent { limit: 10 })?,
            r#"{"protocol":8,"ask":{"recent":{"limit":10}}}"#
        );
        assert_eq!(
            json(Ask::Deleted { limit: 100 })?,
            r#"{"protocol":8,"ask":{"deleted":{"limit":100}}}"#
        );
        assert_eq!(
            json(Ask::FindDeleted {
                items: vec!["Photos/2024".into()]
            })?,
            r#"{"protocol":8,"ask":{"find_deleted":{"items":["Photos/2024"]}}}"#
        );
        assert_eq!(serde_json::to_string(&Reply::Done)?, r#""done""#);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_deep_home_gets_a_short_private_socket_folder() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let deep = dir.path().join("a".repeat(60)).join("b".repeat(60));
        let paths = Paths::under(&deep);
        let path = socket_path(&paths);
        assert!(path.starts_with(std::env::temp_dir()), "{}", path.display());
        assert!(path.as_os_str().len() <= MAX_SOCKET_PATH);
        assert_eq!(path, socket_path(&paths), "the same every time");
        assert!(matches!(
            call(&paths, &Request::new(Ask::Status)),
            Err(IpcError::NotRunning)
        ));

        let listener = listen(&paths)?;
        std::thread::spawn(move || serve(listener, |_| Reply::Done, |_| {}));
        let folder = path.parent().ok_or("folder")?;
        let mode = std::fs::metadata(folder)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        assert_eq!(
            call(
                &paths,
                &Request::new(Ask::Control(Control::Daemon(DaemonControl::SyncNow)))
            )?,
            Reply::Done
        );

        // Opened up by someone: neither side uses it any more.
        std::fs::set_permissions(folder, std::fs::Permissions::from_mode(0o777))?;
        assert!(matches!(
            call(&paths, &Request::new(Ask::Status)),
            Err(IpcError::Io(_))
        ));
        assert!(listen(&paths).is_err());
        std::fs::remove_dir_all(folder)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_left_over_socket_file_is_replaced_and_the_folder_is_private()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(&dir.path().join("run"));
        std::fs::create_dir_all(&paths.runtime)?;
        std::fs::write(socket_path(&paths), "stale")?;
        let listener = listen(&paths)?;
        std::thread::spawn(move || serve(listener, |_| Reply::Done, |_| {}));
        let mode = std::fs::metadata(&paths.runtime)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        assert_eq!(
            call(
                &paths,
                &Request::new(Ask::Control(Control::Daemon(DaemonControl::SyncNow)))
            )?,
            Reply::Done
        );
        Ok(())
    }
}
