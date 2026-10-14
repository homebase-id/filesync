//! The Homebase Filesync daemon: syncs the folder `hbsync setup` configured, until stopped
//! (`hbsync stop`, SIGTERM, SIGINT). Autostart runs it at logon; it logs to a file in the log
//! folder (`hbsync log` shows where), and to stderr too when started from a terminal.

// No console window at logon.
#![cfg_attr(windows, windows_subsystem = "windows")]

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hbsync_app::Paths;
use hbsync_app::accounts::FolderError;
use hbsync_app::daemon::DaemonError;
use hbsync_app::lock::LockError;
use hbsync_app::runner::RunnerError;
use hbsync_state::StateError;

fn main() -> ExitCode {
    let paths = match Paths::resolve() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("hbsyncd: {e}");
            return ExitCode::FAILURE;
        }
    };
    let _flush = match hbsync_app::logging::init(&paths, hbsync_app::lock::DAEMON) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("hbsyncd: can't log to {}: {e}", paths.logs.display());
            return ExitCode::FAILURE;
        }
    };
    let stop = Arc::new(AtomicBool::new(false));
    #[cfg(unix)]
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(signal, Arc::clone(&stop)) {
            tracing::warn!("can't handle signal {signal}: {e}");
        }
    }
    let Err(e) = hbsync_app::daemon::run(&paths, stop) else {
        return ExitCode::SUCCESS;
    };
    // Also for whoever started it without a terminal (`hbsync start`, a test).
    if !std::io::stderr().is_terminal() {
        eprintln!("hbsyncd: {e}");
    }
    match e {
        // Nothing to do is no failure: launchd and systemd restart a daemon that failed. Nor
        // is a folder that must be set up again: restarting can't help.
        DaemonError::NotSetUp(_)
        | DaemonError::Lock(LockError::Held { .. })
        | DaemonError::Runner(RunnerError::State(StateError::TooOld { .. }))
        | DaemonError::Runner(RunnerError::Folder(
            FolderError::Older(_) | FolderError::Unknown(_),
        )) => {
            tracing::warn!("{e}");
            ExitCode::SUCCESS
        }
        e => {
            tracing::error!("{e}");
            ExitCode::FAILURE
        }
    }
}
