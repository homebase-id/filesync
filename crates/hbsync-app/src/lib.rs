//! The product shell around the sync engine: where the client keeps its files, the config,
//! the credentials, setting the app up on an identity, the sync runner, the control socket and
//! the one-daemon lock, and starting the daemon at logon.

pub mod accounts;
pub mod autostart;
pub mod config;
pub mod control;
pub mod daemon;
pub mod install;
pub mod ipc;
pub mod lock;
pub mod logging;
pub mod paths;
pub mod runner;
pub mod secrets;
pub mod setup;
pub mod shell;
#[cfg(windows)]
mod sync_root;
pub mod words;

pub use config::{Config, ConfigError, MassDelete, Root};
pub use hbsync_homebase::app::{APP_NAME, files_online};
/// The local pages' HTTP (login callback, the tray's pages), and `escape` for HTML and XML.
pub use hbsync_homebase::local_http;
pub use paths::{NoHome, Paths};
pub use runner::{Control, Handle, HintSource, Runner, RunnerConfig, State, Status};
pub use secrets::{CredentialError, CredentialStore, Location};

use std::time::{SystemTime, UNIX_EPOCH};

/// This build's version, with the git commit it was built from when known: `0.1.0 (47f0273)`.
/// In the logs, `--version`, About, and the daemon's status (the tray restarts a daemon of
/// another build).
pub const VERSION: &str = env!("HBSYNC_VERSION");

/// Today's date (UTC) as `YYYY-MM-DD`, for conflict-copy names.
pub fn today() -> String {
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_371), (2025, 10, 10));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(today().len(), 10);
    }
}
