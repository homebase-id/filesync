//! What the tray asks of the OS: opening folders and pages, showing a file in the file manager,
//! notifications.

use hbsync_app::install::Install;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::LazyLock;

/// How this copy was installed.
pub static INSTALL: LazyLock<Install> = LazyLock::new(Install::detect);

/// Opens a folder in the file manager, or a page in the browser.
pub fn open(what: impl AsRef<std::ffi::OsStr>) {
    let what = what.as_ref();
    if let Err(e) = hbsync_app::setup::open_browser(None, what) {
        tracing::warn!("can't open {}: {e}", what.display());
    }
}

/// Shows `path` selected in its folder (or its folder, where that can't be done).
pub fn reveal(path: &Path) {
    if let Err(e) = show_selected(path) {
        tracing::debug!(
            "can't show {} in its folder ({e}): opening the folder",
            path.display()
        );
        if let Some(folder) = path.parent() {
            open(folder);
        }
    }
}

#[cfg(target_os = "macos")]
fn show_selected(path: &Path) -> std::io::Result<()> {
    Command::new("open").arg("-R").arg(path).spawn().map(drop)
}

#[cfg(windows)]
fn show_selected(path: &Path) -> std::io::Result<()> {
    let mut arg = std::ffi::OsString::from("/select,");
    arg.push(path);
    Command::new("explorer").arg(arg).spawn().map(drop)
}

/// The file manager interface Dolphin, Nautilus and others implement.
#[cfg(target_os = "linux")]
fn show_selected(path: &Path) -> std::io::Result<()> {
    let status = Command::new("dbus-send")
        .args([
            "--session",
            "--dest=org.freedesktop.FileManager1",
            "--type=method_call",
            "/org/freedesktop/FileManager1",
            "org.freedesktop.FileManager1.ShowItems",
        ])
        .arg(format!(
            "array:string:{}",
            hbsync_app::shell::file_uri(path)
        ))
        .arg("string:")
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other("no file manager answered"))
    }
}

/// A desktop notification, from a thread of its own (some systems take their time).
pub fn notify(summary: String, body: String) {
    std::thread::spawn(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname("Homebase Filesync")
            .summary(&summary)
            .body(&body);
        // In the MSIX package, as the package (its name and icon), not as PowerShell.
        #[cfg(windows)]
        if let Some(id) = INSTALL.app_user_model_id() {
            notification.app_id(&id);
        }
        let shown = notification.show();
        if let Err(e) = shown {
            tracing::debug!("no notification ({e}): {summary}: {body}");
        }
    });
}

/// Notifications on macOS come from the app bundle, when there is one.
pub fn name_notifications() {
    #[cfg(target_os = "macos")]
    if matches!(*INSTALL, Install::MacApp { .. })
        && let Err(e) = notify_rust::set_application(hbsync_app::autostart::LABEL)
    {
        tracing::debug!("notifications keep the default sender: {e}");
    }
}

/// This program, replaced on disk since it started (a package or `cargo install` update): Linux
/// keeps running the old one until it restarts. Elsewhere the updater restarts it.
pub fn replaced() -> bool {
    cfg!(target_os = "linux")
        && std::fs::read_link("/proc/self/exe")
            .is_ok_and(|exe| exe.to_string_lossy().ends_with(" (deleted)"))
}

/// Starts the program now at this one's path, which waits for this one to end.
pub fn start_again() -> std::io::Result<()> {
    let exe = std::env::current_exe()?;
    // Linux names a replaced program's path with this suffix.
    let exe = match exe.to_str().and_then(|s| s.strip_suffix(" (deleted)")) {
        Some(path) => PathBuf::from(path),
        None => exe,
    };
    Command::new(exe)
        .arg(RESTARTED)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(drop)
}

/// The argument of a tray started by [`start_again`]: it waits for the old one to end.
pub const RESTARTED: &str = "--restarted";
