//! The daemon's process: whether it runs, starting it (through the service manager or beside
//! the calling program) and stopping it.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::Result;
use super::client::ask;
use crate::ipc::IpcError;
use crate::protocol::Ask;
use crate::{Paths, autostart};

/// Whether a daemon answers.
pub fn running(paths: &Paths) -> std::result::Result<bool, IpcError> {
    match ask(paths, Ask::Status) {
        Ok(_) => Ok(true),
        Err(IpcError::NotRunning) => Ok(false),
        Err(e) => Err(e),
    }
}

/// A private instance (`HBSYNC_HOME`): tests, development. It leaves the user's logon entry
/// alone unless asked by `hbsync autostart`.
pub fn dev_home() -> bool {
    std::env::var_os("HBSYNC_HOME").is_some_and(|h| !h.is_empty())
}

/// Starts the daemon and waits until it answers: through the service manager when it starts
/// the daemon at logon (so that it supervises it), else `hbsyncd` beside this program, on its
/// own (on a Linux desktop, in a scope of the app's: [`crate::unit`]).
pub fn start(paths: &Paths) -> Result<()> {
    if !dev_home() && autostart::installed()? && autostart::start()? {
        return wait_until_running(paths, None);
    }
    let exe = autostart::daemon_exe()?;
    #[cfg(windows)]
    keep_std_handles_from_children();
    let mut process = Command::new(&exe);
    process
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut process, 0);
    #[cfg(windows)]
    {
        // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP: not tied to this console.
        std::os::windows::process::CommandExt::creation_flags(&mut process, 0x8 | 0x200);
    }
    let child = process
        .spawn()
        .map_err(|e| format!("can't start {}: {e}", exe.display()))?;
    #[cfg(target_os = "linux")]
    crate::unit::move_to_scope(child.id());
    wait_until_running(paths, Some(child))
}

/// Windows children inherit every inheritable handle, and ours are when we were started with
/// redirected output: the daemon would hold our stdout open, and whoever reads it would wait
/// until the daemon stops.
#[cfg(windows)]
fn keep_std_handles_from_children() {
    use windows_sys::Win32::Foundation::{
        HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
    };
    use windows_sys::Win32::System::Console::{
        GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: GetStdHandle has no preconditions; SetHandleInformation gets a handle of
        // this process (or fails harmlessly on a missing one).
        unsafe {
            let handle = GetStdHandle(which);
            if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

/// Waits until the daemon answers, or `child` (when we started it) exits.
fn wait_until_running(paths: &Paths, mut child: Option<std::process::Child>) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(15);
    while Instant::now() < until {
        if running(paths)? {
            return Ok(());
        }
        if let Some(status) = child.as_mut().map(|c| c.try_wait()).transpose()?.flatten() {
            return Err(format!(
                "the daemon exited ({status}); its log is in {}",
                paths.logs.display()
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "the daemon didn't answer; its log is in {}",
        paths.logs.display()
    )
    .into())
}

/// Stops the daemon and waits until it's gone. False when it wasn't running.
pub fn stop(paths: &Paths) -> Result<bool> {
    match ask(paths, Ask::Stop) {
        Err(IpcError::NotRunning) => return Ok(false),
        // The daemon may exit before its answer is out: the connection just ends.
        Err(IpcError::Protocol(_)) => {}
        Err(IpcError::Io(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            ) => {}
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let until = Instant::now() + Duration::from_secs(60);
    while Instant::now() < until {
        // A daemon on its way out may drop a connection: that's not an answer.
        if matches!(running(paths), Ok(false)) {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("the daemon is still finishing a transfer; try again in a moment".into())
}

/// Turns starting at logon off, unless this is a private instance. True when it was on.
pub(super) fn autostart_off_unless_dev() -> Result<bool> {
    if !dev_home() && autostart::installed()? {
        autostart::uninstall()?;
        return Ok(true);
    }
    Ok(false)
}
