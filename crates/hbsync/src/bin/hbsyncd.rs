//! The Homebase Filesync daemon: syncs the folder `hbsync setup` configured, until stopped
//! (`hbsync stop`, SIGTERM, SIGINT). Autostart runs it at logon; it logs to a file in the log
//! folder (`hbsync log` shows where), and to stderr too when started from a terminal.

// No console window at logon.
#![cfg_attr(windows, windows_subsystem = "windows")]

use hbsync_app::Paths;
use hbsync_app::daemon::Stop;
use hbsync_types::ErrorClass;
use std::io::IsTerminal;
use std::process::ExitCode;

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
    let stop = Stop::default();
    #[cfg(unix)]
    {
        use signal_hook::consts::{SIGINT, SIGTERM};
        let stopper = stop.clone();
        let handled = signal_hook::iterator::Signals::new([SIGTERM, SIGINT]).and_then(|mut s| {
            std::thread::Builder::new()
                .name("hbsync-signals".into())
                .spawn(move || s.forever().for_each(|_| stopper.stop()))
        });
        if let Err(e) = handled {
            tracing::warn!("can't handle signals: {e}");
        }
    }
    let Err(e) = hbsync_app::daemon::run(&paths, stop) else {
        return ExitCode::SUCCESS;
    };
    // Also for whoever started it without a terminal (`hbsync start`, a test).
    if !std::io::stderr().is_terminal() {
        eprintln!("hbsyncd: {e}");
    }
    // Nothing to do is no failure: launchd and systemd restart a daemon that failed. Nor is
    // what only the user can fix (a folder to set up again, a state DB that is corrupt or from
    // another version, a config that doesn't parse): restarting every few seconds can't help.
    if e.class() == ErrorClass::NeedsUser {
        tracing::warn!("{e}");
        return ExitCode::SUCCESS;
    }
    tracing::error!("{e}");
    ExitCode::FAILURE
}
