//! What the tray asks of the OS: opening folders and pages, showing a file in the file manager,
//! notifications.

use std::path::Path;
use std::process::Command;

/// Opens a folder in the file manager, or a page in the browser.
pub fn open(what: &str) {
    if let Err(e) = hbsync_app::setup::open_browser(None, what) {
        tracing::warn!("can't open {what}: {e}");
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
            open(&folder.display().to_string());
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
        .arg(format!("array:string:{}", crate::shell::file_uri(path)))
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
        let shown = notify_rust::Notification::new()
            .appname("Homebase Filesync")
            .summary(&summary)
            .body(&body)
            .show();
        if let Err(e) = shown {
            tracing::debug!("no notification ({e}): {summary}: {body}");
        }
    });
}
