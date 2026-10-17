//! Development tools around the sync engine and a live Homebase identity:
//! - `hbsync-register <identity>`: registers the app and authorizes this machine;
//! - `hbsync-sync <identity> <folder>`: syncs a folder with the identity's Filesync drive.
//!
//! Not the product: the daemon, its configuration and the keychain are roadmap Layer 6.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hbsync_homebase::{Client, Credentials};
use hbsync_remote::{Remote, RemoteError};

/// Where the development tools keep the credentials of `identity`:
/// `$XDG_CONFIG_HOME/hbsync/dev/<identity>.json` (default `~/.config`). The product keeps them
/// in the OS keychain.
pub fn credentials_path(identity: &str) -> Result<PathBuf, String> {
    credentials_path_as(identity, None)
}

/// [`credentials_path`] of one of several clients of `identity` on this machine (`hbsync-register
/// --as <client>`): `<identity>.<client>.json`; `None` is the default one. Each is a client
/// registration of its own, as on another computer: a notification subscription leaves out
/// only its own token's writes (`excludeOwnWrites`), so test "computers" that should hear each
/// other need one each.
pub fn credentials_path_as(identity: &str, client: Option<&str>) -> Result<PathBuf, String> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .ok_or("no config folder (HOME unset)")?;
    let file = match client {
        None => format!("{identity}.json"),
        Some(c) => format!("{identity}.{c}.json"),
    };
    Ok(base.join("hbsync").join("dev").join(file))
}

/// The second client the live tests use for another computer.
pub const OTHER_CLIENT: &str = "b";

/// A client from the credentials `hbsync-register` saved for `identity`.
pub fn saved_client(identity: &str) -> Result<Client, Box<dyn std::error::Error>> {
    saved_client_as(identity, None)
}

/// The credentials file `hbsync-register` (`--as <client>`) saved for `identity`, or what to
/// run to make it.
pub fn credentials_or_hint(identity: &str, client: Option<&str>) -> Result<PathBuf, String> {
    let path = credentials_path_as(identity, client)?;
    if path.exists() {
        return Ok(path);
    }
    let register = match client {
        None => format!("hbsync-register {identity}"),
        Some(c) => format!("hbsync-register {identity} --as {c}"),
    };
    Err(format!(
        "no credentials for {identity} at {}: run {register} first",
        path.display()
    ))
}

/// A client from the credentials `hbsync-register --as <client>` saved for `identity`.
pub fn saved_client_as(
    identity: &str,
    client: Option<&str>,
) -> Result<Client, Box<dyn std::error::Error>> {
    let path = credentials_or_hint(identity, client)?;
    let creds = Credentials::load(&path)?.ok_or("the credentials file is gone")?;
    Ok(Client::new(&creds)?)
}

/// The value after `name` (say `--as`) in a tool's arguments.
pub fn option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// `work` for each of `0..n` on 8 threads: how many went through, or the first error.
pub fn on_threads<E: std::fmt::Display>(
    n: usize,
    work: impl Fn(usize) -> Result<(), E> + Sync,
) -> Result<usize, String> {
    let (next, done) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let failed = Mutex::new(None::<String>);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let stop = failed.lock().map_or(true, |f| f.is_some());
                    if i >= n || stop {
                        return;
                    }
                    match work(i) {
                        Ok(()) => drop(done.fetch_add(1, Ordering::Relaxed)),
                        Err(e) => {
                            if let Ok(mut f) = failed.lock() {
                                f.get_or_insert(e.to_string());
                            }
                        }
                    }
                }
            });
        }
    });
    match failed.into_inner().unwrap_or_else(|e| e.into_inner()) {
        Some(e) => Err(e),
        None => Ok(done.into_inner()),
    }
}

/// The cursor at the end of the feed, read on from `from` (`None`: the beginning) in slim pages.
pub fn cursor_at_end(remote: &dyn Remote, from: Option<&str>) -> Result<String, RemoteError> {
    let mut cursor = from.map(str::to_owned);
    loop {
        let page = remote.change_refs(cursor.as_deref(), 1000)?;
        cursor = Some(page.cursor);
        if !page.has_more {
            return Ok(cursor.unwrap_or_default());
        }
    }
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
