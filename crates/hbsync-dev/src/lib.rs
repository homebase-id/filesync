//! Development tools around the sync engine and a live Homebase identity:
//! - `hbsync-register <identity>`: registers the app and authorizes this machine;
//! - `hbsync-sync <identity> <folder>`: syncs a folder with the identity's Filesync drive.
//!
//! Not the product: the daemon, its configuration and the keychain are roadmap Layer 6.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use hbsync_homebase::{Client, Credentials};

/// This machine's name, for conflict copies and the owner's list of devices.
pub fn device_name() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "this computer".into())
}

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

/// Where the development tools keep the credentials of `identity`:
/// `$XDG_CONFIG_HOME/hbsync/dev/<identity>.json` (default `~/.config`). The product keeps them
/// in the OS keychain.
pub fn credentials_path(identity: &str) -> Result<PathBuf, String> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .ok_or("no config folder (HOME unset)")?;
    Ok(base
        .join("hbsync")
        .join("dev")
        .join(format!("{identity}.json")))
}

/// A client from the credentials `hbsync-register` saved for `identity`.
pub fn saved_client(identity: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let path = credentials_path(identity)?;
    let creds = Credentials::load(&path)?.ok_or_else(|| {
        format!(
            "no credentials for {identity} at {}: run hbsync-register {identity} first",
            path.display()
        )
    })?;
    Ok(Client::new(&creds)?)
}

/// The identity the live tests run against (`HBSYNC_LIVE`).
pub fn live_identity() -> Result<String, String> {
    std::env::var("HBSYNC_LIVE")
        .map_err(|_| "set HBSYNC_LIVE to an identity registered with hbsync-register".into())
}

/// A name for one live test run, unique enough to work in a top-level folder of its own.
pub fn run_name(what: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!("hbsync-test {what} {now}")
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
