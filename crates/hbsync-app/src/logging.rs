//! The programs' logs: a daily file in the log folder (a week of them kept), and stderr when
//! run from a terminal.

use std::io::IsTerminal;

use tracing_subscriber::prelude::*;

use crate::Paths;

/// Logs to `<logs>/<program>.<date>.log`. Keep the guard: it flushes the file when dropped.
pub fn init(
    paths: &Paths,
    program: &str,
) -> Result<tracing_appender::non_blocking::WorkerGuard, Box<dyn std::error::Error>> {
    // The appender looks for old files to prune before it creates the folder.
    std::fs::create_dir_all(&paths.logs)?;
    let file = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(program)
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
