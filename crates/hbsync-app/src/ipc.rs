//! The daemon's control socket: how the CLI asks for the status and sends requests.
//!
//! One JSON request per line, one JSON reply per line, then the connection closes. Each request
//! carries the protocol version; a daemon refuses one it doesn't speak (after an update, until
//! it restarts).
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

use hbsync_types::activity::{Deleted, Synced};
use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{Listener, ListenerOptions, Name, Stream};
use serde::{Deserialize, Serialize};

use crate::Paths;
use crate::runner::{Control, Status};

/// The protocol version this build speaks.
pub const PROTOCOL: u32 = 3;

/// How long either side waits for the other's line.
const PATIENCE: Duration = Duration::from_secs(10);
/// Longer lines are refused (a status is a few kilobytes).
const MAX_LINE: u64 = 1 << 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub protocol: u32,
    pub ask: Ask,
}

impl Request {
    pub fn new(ask: Ask) -> Request {
        Request {
            protocol: PROTOCOL,
            ask,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ask {
    Status,
    Control(Control),
    /// The newest entries of the recently synced list.
    Recent {
        limit: usize,
    },
    /// The newest items of Recently deleted.
    Deleted {
        limit: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Status(Box<Status>),
    /// Newest first.
    Recent(Vec<Synced>),
    /// Newest first.
    Deleted(Vec<Deleted>),
    Done,
    Refused(String),
}

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
pub fn call(paths: &Paths, request: &Request) -> Result<Reply, IpcError> {
    let name = endpoint(paths, false).map_err(not_running)?;
    let stream = Stream::connect(name).map_err(not_running)?;
    patience(&stream)?;
    let mut stream = BufReader::new(stream);
    let mut line = serde_json::to_string(request).map_err(|e| IpcError::Protocol(e.to_string()))?;
    line.push('\n');
    stream.get_mut().write_all(line.as_bytes())?;
    let reply = read_line(&mut stream)?.ok_or(IpcError::Protocol("no answer".into()))?;
    serde_json::from_str(&reply).map_err(|e| IpcError::Protocol(e.to_string()))
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
pub fn listen(paths: &Paths) -> io::Result<Listener> {
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

/// Answers requests on `listener` with `answer`, each connection on its own thread. Returns
/// only if the listener fails.
pub fn serve(listener: Listener, answer: impl Fn(Request) -> Reply + Send + Sync + 'static) {
    let answer = Arc::new(answer);
    for conn in listener.incoming() {
        let Ok(conn) = conn else {
            continue;
        };
        let answer = Arc::clone(&answer);
        let _ = std::thread::Builder::new()
            .name("hbsync-ipc".into())
            .spawn(move || {
                if let Err(e) = handle(conn, answer.as_ref()) {
                    tracing::debug!("a control connection failed: {e}");
                }
            });
    }
}

fn handle(conn: Stream, answer: &(dyn Fn(Request) -> Reply + Sync)) -> Result<(), IpcError> {
    patience(&conn)?;
    let mut conn = BufReader::new(conn);
    let Some(line) = read_line(&mut conn)? else {
        return Ok(());
    };
    let reply = match serde_json::from_str::<Request>(&line) {
        Ok(r) if r.protocol == PROTOCOL => answer(r),
        Ok(r) => Reply::Refused(format!(
            "this daemon speaks protocol {PROTOCOL}, the request {}: restart the daemon",
            r.protocol
        )),
        Err(e) => Reply::Refused(format!("not a request: {e}")),
    };
    let mut out = serde_json::to_string(&reply).map_err(|e| IpcError::Protocol(e.to_string()))?;
    out.push('\n');
    conn.get_mut().write_all(out.as_bytes())?;
    Ok(())
}

/// Waits at most [`PATIENCE`] for the other side, where the OS can time out reads (named pipes
/// can't).
fn patience(stream: &Stream) -> io::Result<()> {
    match stream.set_recv_timeout(Some(PATIENCE)) {
        Err(e) if e.kind() == io::ErrorKind::Unsupported => Ok(()),
        other => other,
    }
}

/// One line, without its end; `None` at the end of the stream.
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
pub fn socket_path(paths: &Paths) -> PathBuf {
    const SOCKET: &str = "hbsyncd.sock";
    let path = paths.runtime.join(SOCKET);
    if path.as_os_str().len() <= MAX_SOCKET_PATH {
        return path;
    }
    use std::os::unix::ffi::OsStrExt;
    // FNV-1a: stable across builds, so an older daemon and a newer CLI agree.
    let hash = paths
        .runtime
        .as_os_str()
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
        });
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
    use super::*;
    use crate::runner::State;

    fn status() -> Status {
        let mut s = Status::new("a.example", std::path::Path::new("/home/a/Homebase"));
        s.state = State::Idle;
        s.pending = 3;
        s
    }

    #[test]
    fn requests_and_replies_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        assert!(matches!(
            call(&paths, &Request::new(Ask::Status)),
            Err(IpcError::NotRunning)
        ));

        let listener = listen(&paths)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = std::sync::Mutex::new(tx);
        std::thread::spawn(move || {
            serve(listener, move |r| match r.ask {
                Ask::Status => Reply::Status(Box::new(status())),
                Ask::Control(c) => {
                    let _ = tx.lock().map(|tx| tx.send(c));
                    Reply::Done
                }
                Ask::Deleted { .. } => Reply::Deleted(Vec::new()),
                Ask::Recent { limit } => Reply::Recent(
                    (0..limit as u64)
                        .map(|i| Synced {
                            at: 1_800_000_000 - i,
                            kind: hbsync_types::activity::SyncedKind::Downloaded,
                            path: format!("Photos/p{i}.jpg"),
                            files: 1,
                            bytes: 1000,
                        })
                        .collect(),
                ),
            })
        });

        match call(&paths, &Request::new(Ask::Status))? {
            Reply::Status(s) => assert_eq!(*s, status()),
            other => panic!("{other:?}"),
        }
        let confirm = Control::ConfirmDeletes {
            token: "0123456789abcdef-60".into(),
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

        // Another protocol version is refused, with the reason.
        let old = Request {
            protocol: PROTOCOL + 1,
            ask: Ask::Status,
        };
        assert!(matches!(call(&paths, &old)?, Reply::Refused(why) if why.contains("restart")));
        Ok(())
    }

    /// The wire format docs/spec/daemon.md documents for protocol 3.
    #[test]
    fn requests_look_as_documented() -> Result<(), serde_json::Error> {
        let json = |ask| serde_json::to_string(&Request::new(ask));
        assert_eq!(json(Ask::Status)?, r#"{"protocol":3,"ask":"status"}"#);
        assert_eq!(
            json(Ask::Control(Control::SyncNow))?,
            r#"{"protocol":3,"ask":{"control":"sync_now"}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::ConfirmDeletes { token: "t".into() }))?,
            r#"{"protocol":3,"ask":{"control":{"confirm_deletes":{"token":"t"}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::Pause {
                until: Some(1_800_000_000)
            }))?,
            r#"{"protocol":3,"ask":{"control":{"pause":{"until":1800000000}}}}"#
        );
        assert_eq!(
            json(Ask::Recent { limit: 10 })?,
            r#"{"protocol":3,"ask":{"recent":{"limit":10}}}"#
        );
        let node = hbsync_types::NodeId(hbsync_types::Uuid::from_u128(1));
        assert_eq!(
            json(Ask::Control(Control::Restore { nodes: vec![node] }))?,
            r#"{"protocol":3,"ask":{"control":{"restore":{"nodes":["00000000-0000-0000-0000-000000000001"]}}}}"#
        );
        assert_eq!(
            json(Ask::Control(Control::EmptyDeletedUntil { at_ms: 7 }))?,
            r#"{"protocol":3,"ask":{"control":{"empty_deleted_until":{"at_ms":7}}}}"#
        );
        assert_eq!(
            json(Ask::Deleted { limit: 100 })?,
            r#"{"protocol":3,"ask":{"deleted":{"limit":100}}}"#
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
        std::thread::spawn(move || serve(listener, |_| Reply::Done));
        let folder = path.parent().ok_or("folder")?;
        let mode = std::fs::metadata(folder)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        assert_eq!(
            call(&paths, &Request::new(Ask::Control(Control::SyncNow)))?,
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
        std::thread::spawn(move || serve(listener, |_| Reply::Done));
        let mode = std::fs::metadata(&paths.runtime)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        assert_eq!(
            call(&paths, &Request::new(Ask::Control(Control::SyncNow)))?,
            Reply::Done
        );
        Ok(())
    }
}
