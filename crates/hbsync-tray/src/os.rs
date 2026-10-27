//! What the tray asks of the OS: opening folders and pages, showing a file in the file manager,
//! choosing a folder, notifications.

use hbsync_app::install::Install;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Opens a folder in the file manager, or a page in the browser.
pub(crate) fn open(what: impl AsRef<std::ffi::OsStr>) {
    let what = what.as_ref();
    if let Err(e) = hbsync_app::setup::open_browser(None, what) {
        tracing::warn!("can't open {}: {e}", what.display());
    }
}

/// Shows `path` selected in its folder (or its folder, where that can't be done).
pub(crate) fn reveal(path: &Path) {
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
    Command::new("open")
        .arg("-R")
        .arg(path)
        .spawn()
        .map(hbsync_app::reap)
}

#[cfg(windows)]
fn show_selected(path: &Path) -> std::io::Result<()> {
    let mut arg = std::ffi::OsString::from("/select,");
    arg.push(path);
    Command::new("explorer")
        .arg(arg)
        .spawn()
        .map(hbsync_app::reap)
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

/// Asks for a folder with the OS's picker, starting in `start`. On the main thread; it waits
/// until the picker closes.
pub(crate) fn pick_folder(title: &str, start: &Path) -> Option<PathBuf> {
    let dialog = rfd::FileDialog::new().set_title(title).set_directory(start);
    // Windows: the tray has no window, and a process in the background can't take the
    // foreground from the browser, so a picker of its own opens behind it. One owned by a
    // topmost window is topmost too: in front, though not focused until clicked. It stays
    // above other windows while open, the price of never opening behind. (macOS's panel floats
    // above every window already.)
    #[cfg(windows)]
    if let Some(owner) = owner::Owner::new() {
        return dialog.set_parent(&owner).pick_folder();
    }
    dialog.pick_folder()
}

#[cfg(windows)]
mod owner {
    use std::num::NonZeroIsize;
    use std::ptr::{null, null_mut};

    use tao::rwh_06::{
        DisplayHandle, HandleError, HasDisplayHandle, HasWindowHandle, RawWindowHandle,
        Win32WindowHandle, WindowHandle,
    };
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::Graphics::Gdi::{
        GetMonitorInfoW, MONITOR_DEFAULTTOPRIMARY, MONITORINFO, MonitorFromWindow,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        CreateWindowExW, DestroyWindow, GetForegroundWindow, WS_EX_TOPMOST, WS_POPUP,
    };

    /// A hidden topmost window that owns one dialog and goes with it. It sits in the middle
    /// of the screen the window in front (the browser) is on: the dialog centres itself on it.
    #[derive(Debug)]
    pub(super) struct Owner(HWND);

    impl Owner {
        pub(super) fn new() -> Option<Owner> {
            let (x, y) = middle_of_the_screen_in_front().unwrap_or((0, 0));
            // SAFETY: a system class (registered, with its own window procedure) and no
            // parent, menu or creation data; null when it fails.
            let hwnd = unsafe {
                CreateWindowExW(
                    WS_EX_TOPMOST,
                    windows_sys::w!("STATIC"),
                    null(),
                    WS_POPUP,
                    x,
                    y,
                    0,
                    0,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    null(),
                )
            };
            if hwnd.is_null() {
                tracing::debug!(
                    "no window to own the folder picker: {}",
                    std::io::Error::last_os_error()
                );
                return None;
            }
            Some(Owner(hwnd))
        }
    }

    /// The middle of the work area of the monitor showing the window in front (where it was
    /// before it was minimized; the main monitor when there's none).
    fn middle_of_the_screen_in_front() -> Option<(i32, i32)> {
        let mut info = MONITORINFO {
            cbSize: size_of::<MONITORINFO>() as u32,
            ..Default::default()
        };
        // SAFETY: queries only; `info` outlives the call and says its size.
        let found = unsafe {
            let monitor = MonitorFromWindow(GetForegroundWindow(), MONITOR_DEFAULTTOPRIMARY);
            !monitor.is_null() && GetMonitorInfoW(monitor, &raw mut info) != 0
        };
        let r = info.rcWork;
        found.then(|| (r.left.midpoint(r.right), r.top.midpoint(r.bottom)))
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            // SAFETY: the window this made, on the thread that made it, once.
            unsafe { DestroyWindow(self.0) };
        }
    }

    impl HasWindowHandle for Owner {
        fn window_handle(&self) -> Result<WindowHandle<'_>, HandleError> {
            let hwnd = NonZeroIsize::new(self.0 as isize).ok_or(HandleError::Unavailable)?;
            let raw = RawWindowHandle::Win32(Win32WindowHandle::new(hwnd));
            // SAFETY: the window lives as long as `self`.
            Ok(unsafe { WindowHandle::borrow_raw(raw) })
        }
    }

    impl HasDisplayHandle for Owner {
        fn display_handle(&self) -> Result<DisplayHandle<'_>, HandleError> {
            Ok(DisplayHandle::windows())
        }
    }
}

/// A desktop notification, from a thread of its own (some systems take their time).
pub(crate) fn notify(summary: String, body: String) {
    std::thread::spawn(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname(hbsync_app::APP_NAME)
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
pub(crate) fn name_notifications() {
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
pub(crate) fn replacement() -> Option<PathBuf> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let exe = std::fs::read_link("/proc/self/exe").ok()?;
    let path = PathBuf::from(exe.to_str()?.strip_suffix(" (deleted)")?);
    // A package's link, which follows the program if the package moved it.
    let next = Install::current().program(hbsync_app::autostart::TRAY, &path);
    next.is_file().then_some(next)
}
