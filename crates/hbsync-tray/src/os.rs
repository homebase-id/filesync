//! What the tray asks of the OS: opening folders and pages, showing a file in the file manager,
//! notifications.

use hbsync_app::install::Install;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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
        if let Some(id) = Install::current().app_user_model_id() {
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
    if matches!(Install::current(), Install::MacApp { .. })
        && let Err(e) = notify_rust::set_application(hbsync_app::autostart::LABEL)
    {
        tracing::debug!("notifications keep the default sender: {e}");
    }
}

/// The program that replaced this one on disk since it started (a package or `cargo install`
/// update): Linux keeps running the old one until it restarts; elsewhere the updater restarts
/// it. Not when it was removed (nothing to restart into).
pub fn replacement() -> Option<PathBuf> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let exe = std::fs::read_link("/proc/self/exe").ok()?;
    let path = PathBuf::from(exe.to_str()?.strip_suffix(" (deleted)")?);
    path.is_file()
        .then(|| Install::current().lasting_path(&path))
}

/// Starts `exe` (the [`replacement`]) on its own, to wait for this tray to end and take over.
pub fn start_again(exe: &Path) -> std::io::Result<()> {
    let mut command = Command::new(exe);
    command
        .arg(RESTARTED)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Its own process group: closing the terminal this tray was started from mustn't end it.
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    command.spawn().map(drop)
}

/// The argument of a tray started by [`start_again`]: it waits for the old one to end.
pub const RESTARTED: &str = "--restarted";
