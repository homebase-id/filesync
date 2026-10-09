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

use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{Listener, ListenerOptions, Name, Stream};
use serde::{Deserialize, Serialize};

use crate::Paths;
use crate::runner::{Control, Status};

/// The protocol version this build speaks.
pub const PROTOCOL: u32 = 1;

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Status(Box<Status>),
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
    stream.set_recv_timeout(Some(PATIENCE))?;
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
    conn.set_recv_timeout(Some(PATIENCE))?;
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

/// The socket file (Unix).
#[cfg(unix)]
pub fn socket_path(paths: &Paths) -> PathBuf {
    paths.runtime.join("hbsyncd.sock")
}

/// Where the daemon listens; `serving` prepares it for listening.
#[cfg(unix)]
fn endpoint(paths: &Paths, serving: bool) -> io::Result<Name<'static>> {
    use interprocess::local_socket::GenericFilePath;
    let path = socket_path(paths);
    // `sun_path` holds 104 bytes on macOS and 108 on Linux, terminator included.
    if path.as_os_str().len() > 100 {
        return Err(io::Error::other(format!(
            "the control socket's path is too long for the OS: {} (set HBSYNC_HOME to a \
             shorter folder)",
            path.display()
        )));
    }
    if serving {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&paths.runtime)?;
        std::fs::set_permissions(&paths.runtime, std::fs::Permissions::from_mode(0o700))?;
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
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
        Status {
            state: State::Idle,
            identity: "a.example".into(),
            folder: "/home/a/Homebase".into(),
            error: None,
            retry_at: None,
            last_sync: None,
            pending: 3,
            held_deletes: None,
            recent: Vec::new(),
            watcher_error: None,
            hints: crate::runner::Hints::Off,
            rules: 0,
            credentials_warning: None,
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
            })
        });

        match call(&paths, &Request::new(Ask::Status))? {
            Reply::Status(s) => assert_eq!(*s, status()),
            other => panic!("{other:?}"),
        }
        let confirm = Control::ConfirmDeletes {
            here: 60,
            remotely: 0,
        };
        assert_eq!(
            call(&paths, &Request::new(Ask::Control(confirm.clone())))?,
            Reply::Done
        );
        assert_eq!(rx.recv_timeout(PATIENCE)?, confirm);

        // Another protocol version is refused, with the reason.
        let old = Request {
            protocol: PROTOCOL + 1,
            ask: Ask::Status,
        };
        assert!(matches!(call(&paths, &old)?, Reply::Refused(why) if why.contains("restart")));
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
