//! Setting the app up on an identity: authorizing this device (the owner approves in the
//! browser), then the one-time work on the identity (the app's circle) and a first look at the
//! sync drive.

use std::process::Command;
use std::time::Duration;

use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::{Client, Credentials, HomebaseError, app};
use hbsync_remote::Remote;

/// How long the owner has to approve in the browser.
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(1800);

/// Authorizes this device for Filesync on `identity`: opens the authorize page in `browser`
/// (the system's default when `None`), passes its URL to `show` for when no browser opens,
/// and waits for the owner to approve. A new app is registered on the way.
pub fn authorize(
    identity: &str,
    device: &str,
    browser: Option<&str>,
    show: impl FnOnce(&str),
) -> Result<Credentials, HomebaseError> {
    let pending = hbsync_homebase::begin(identity, &format!("Filesync on {device}"))?;
    show(&pending.authorize_url);
    // The URL was shown: a browser that doesn't start isn't an error.
    let _ = open_browser(browser, &pending.authorize_url);
    pending.finish(APPROVAL_TIMEOUT)
}

/// What [`prepare`] found and did.
#[derive(Debug)]
pub struct Prepared {
    pub remote: HomebaseRemote,
    /// The app's circle, created now (`creds` changed, and were saved).
    pub circle_created: Option<uuid::Uuid>,
    /// Changes on the first page of the drive's feed: it answers.
    pub feed_entries: usize,
}

/// Gets the identity ready for syncing once this device is authorized: finds the sync drive,
/// creates the app's circle once, and reads the drive's change feed. `save` keeps `creds` as
/// soon as they change (the circle created), before anything else can fail: otherwise the
/// next try would create a second circle.
pub fn prepare<E: std::fmt::Display>(
    creds: &mut Credentials,
    save: impl FnOnce(&Credentials) -> Result<(), E>,
) -> Result<Prepared, HomebaseError> {
    let remote = HomebaseRemote::connect(Client::new(creds)?)?;
    let circle_created = app::ensure_circle(&remote, creds)?;
    if circle_created.is_some() {
        save(creds).map_err(|e| HomebaseError::Protocol(format!("saving the credentials: {e}")))?;
    }
    let feed_entries = remote
        .changes(None, 10)
        .map_err(|e| HomebaseError::Protocol(format!("the sync drive's feed: {e}")))?
        .entries
        .len();
    Ok(Prepared {
        remote,
        circle_created,
        feed_entries,
    })
}

/// Opens `url` with `command`, or with the system's opener.
pub fn open_browser(command: Option<&str>, url: &str) -> std::io::Result<()> {
    let opener = command.unwrap_or(if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(windows) {
        "explorer"
    } else {
        "xdg-open"
    });
    Command::new(opener).arg(url).spawn().map(drop)
}
