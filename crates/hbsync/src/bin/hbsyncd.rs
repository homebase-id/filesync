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
use tracing_subscriber::prelude::*;

fn main() -> ExitCode {
    let paths = match Paths::resolve() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("hbsyncd: {e}");
            return ExitCode::FAILURE;
        }
    };
    let _flush = match logging(&paths) {
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
    match hbsync_app::daemon::run(&paths, stop) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            // Also for whoever started it without a terminal (`hbsync start`, a test).
            if !std::io::stderr().is_terminal() {
                eprintln!("hbsyncd: {e}");
            }
            ExitCode::FAILURE
        }
    }
}

/// A daily log file in the log folder (a week of them kept), and stderr from a terminal.
fn logging(
    paths: &Paths,
) -> Result<tracing_appender::non_blocking::WorkerGuard, Box<dyn std::error::Error>> {
    let file = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("hbsyncd")
        .filename_suffix("log")
        .max_log_files(7)
        .build(&paths.logs)?;
    let (file, guard) = tracing_appender::non_blocking(file);
    let filter = || tracing_subscriber::filter::Targets::new().with_default(tracing::Level::INFO);
    let to_file = tracing_subscriber::fmt::layer()
        .with_writer(file)
        .with_ansi(false)
        .with_filter(filter());
    let terminal = std::io::stderr().is_terminal();
    let to_stderr = terminal.then(|| {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(false)
            .with_filter(filter())
    });
    tracing_subscriber::registry()
        .with(to_file)
        .with(to_stderr)
        .try_init()?;
    Ok(guard)
}
