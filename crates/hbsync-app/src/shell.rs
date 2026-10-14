//! The synced folder in the file manager: the Homebase icon on it, and a place in the sidebar
//! (on Windows at the top of Explorer's navigation pane, as a Cloud Files sync root,
//! `sync_root.rs`; the bookmarks of GTK file managers and KDE's places on Linux;
//! Finder's sidebar has no public interface, so not on macOS). Done once per folder (a note in
//! the state folder), so a shortcut the user removed stays removed, except the sync root,
//! registered at every start; undone when the folder is unlinked. All best effort: a file
//! manager that doesn't take it is no failure.

use std::path::{Path, PathBuf};

use crate::Paths;

/// The Homebase icon, as each OS wants it.
#[derive(Debug, Clone, Copy)]
pub struct Icons<'a> {
    /// A PNG (Linux, macOS).
    pub png: &'a [u8],
    /// An ICO (Windows).
    pub ico: &'a [u8],
}

/// The note of the folder already given its icon and shortcut.
const DONE: &str = "folder-decorated";

fn note(paths: &Paths) -> PathBuf {
    paths.state.join(DONE)
}

/// Gives `folder` (synced with `identity`) its icon and shortcut, unless it already had them
/// once.
pub fn decorate(paths: &Paths, folder: &Path, identity: &str, icons: Icons<'_>) {
    // Each time, not once: it also reaches folders decorated before there was one.
    #[cfg(windows)]
    os::sync_root(paths, folder, identity, icons);
    #[cfg(not(windows))]
    let _ = identity;
    let done = note(paths);
    if std::fs::read_to_string(&done).is_ok_and(|f| Path::new(f.trim()) == folder) {
        return;
    }
    tracing::info!("giving {} its icon and a shortcut", folder.display());
    if let Err(e) = os::decorate(paths, folder, icons) {
        tracing::info!("the folder's icon or shortcut: {e}");
    }
    if let Err(e) = std::fs::write(&done, folder.display().to_string()) {
        tracing::warn!("{}: {e}", done.display());
    }
}

/// Takes the icon and shortcut off `folder` again (unlinking it).
pub fn undecorate(paths: &Paths, folder: &Path) {
    if let Err(e) = os::undecorate(paths, folder) {
        tracing::info!("removing the folder's icon or shortcut: {e}");
    }
    let _ = std::fs::remove_file(note(paths));
}

/// `file:///home/anna/My%20Files`, as bookmark files and the file manager interface want it.
#[cfg(any(target_os = "linux", test))]
pub fn file_uri(path: &Path) -> String {
    use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
    const KEEP: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
        .remove(b'/')
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    format!(
        "file://{}",
        percent_encode(path.as_os_str().as_encoded_bytes(), KEEP)
    )
}

/// GTK's bookmarks (`gtk-3.0/bookmarks`): one `URI label` per line. With `uri`, labelled.
#[cfg(any(target_os = "linux", test))]
pub fn gtk_bookmarks_with(text: &str, uri: &str, label: &str) -> Option<String> {
    if text.lines().any(|l| l.split(' ').next() == Some(uri)) {
        return None;
    }
    let mut out = text.to_owned();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!("{uri} {label}\n"));
    Some(out)
}

/// GTK's bookmarks without `uri`.
#[cfg(any(target_os = "linux", test))]
pub fn gtk_bookmarks_without(text: &str, uri: &str) -> Option<String> {
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| l.split(' ').next() != Some(uri))
        .collect();
    (kept.len() != text.lines().count()).then(|| kept.iter().map(|l| format!("{l}\n")).collect())
}

/// KDE's places (`user-places.xbel`) with a bookmark for `uri`, before the closing tag.
#[cfg(any(target_os = "linux", test))]
pub fn kde_places_with(xbel: &str, uri: &str, title: &str) -> Option<String> {
    if xbel.contains(&format!("href=\"{uri}\"")) {
        return None;
    }
    let end = xbel.rfind("</xbel>")?;
    let bookmark = format!(
        " <bookmark href=\"{uri}\">\n  <title>{title}</title>\n  <info>\n   \
         <metadata owner=\"http://freedesktop.org\">\n    \
         <bookmark:icon name=\"folder-cloud\"/>\n   </metadata>\n  </info>\n </bookmark>\n",
        title = crate::words::escape_xml(title),
    );
    Some(format!("{}{bookmark}{}", &xbel[..end], &xbel[end..]))
}

/// KDE's places without the bookmark for `uri`.
#[cfg(any(target_os = "linux", test))]
pub fn kde_places_without(xbel: &str, uri: &str) -> Option<String> {
    let start = xbel.find(&format!("<bookmark href=\"{uri}\""))?;
    let start = xbel[..start].rfind('\n').map_or(start, |nl| nl + 1);
    let end = start + xbel[start..].find("</bookmark>")? + "</bookmark>".len();
    let end = if xbel[end..].starts_with('\n') {
        end + 1
    } else {
        end
    };
    Some(format!("{}{}", &xbel[..start], &xbel[end..]))
}

#[cfg(target_os = "linux")]
mod os {
    use super::*;
    use crate::paths::xdg_dir;
    use std::process::Command;

    /// Rewrites `path` with `change` when it says so; a missing file counts as empty when
    /// `create` (else it's left alone).
    fn edit(path: &Path, create: bool, change: impl FnOnce(&str) -> Option<String>) {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && create => String::new(),
            Err(_) => return,
        };
        if let Some(new) = change(&text) {
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            if let Err(e) = std::fs::write(path, new) {
                tracing::info!("{}: {e}", path.display());
            }
        }
    }

    fn icon(paths: &Paths) -> PathBuf {
        paths.state.join("homebase.png")
    }

    /// Dolphin's folder settings, giving the icon.
    fn dolphin(icon: &Path) -> String {
        format!("[Desktop Entry]\nIcon={}\n", icon.display())
    }

    pub fn decorate(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon(paths);
        std::fs::write(&icon, icons.png)?;
        // GNOME's Files and others: GIO metadata (outside the folder).
        let _ = Command::new("gio")
            .args(["set", "-t", "string"])
            .arg(folder)
            .arg("metadata::custom-icon")
            .arg(file_uri(&icon))
            .output();
        // KDE's Dolphin: `.directory` in the folder (never synced: it is built-in junk).
        let directory = folder.join(".directory");
        if !directory.exists() {
            std::fs::write(&directory, dolphin(&icon))?;
        }
        let uri = file_uri(folder);
        if let Some(config) = xdg_dir("XDG_CONFIG_HOME", ".config") {
            edit(&config.join("gtk-3.0/bookmarks"), true, |t| {
                gtk_bookmarks_with(t, &uri, "Homebase")
            });
        }
        // KDE writes this file on first start; with none, there is no KDE to tell.
        if let Some(data) = xdg_dir("XDG_DATA_HOME", ".local/share") {
            edit(&data.join("user-places.xbel"), false, |t| {
                kde_places_with(t, &uri, "Homebase")
            });
        }
        Ok(())
    }

    pub fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        let _ = Command::new("gio")
            .args(["set", "-d"])
            .arg(folder)
            .arg("metadata::custom-icon")
            .output();
        let directory = folder.join(".directory");
        if std::fs::read_to_string(&directory).is_ok_and(|t| t == dolphin(&icon(paths))) {
            std::fs::remove_file(&directory)?;
        }
        let uri = file_uri(folder);
        if let Some(config) = xdg_dir("XDG_CONFIG_HOME", ".config") {
            edit(&config.join("gtk-3.0/bookmarks"), false, |t| {
                gtk_bookmarks_without(t, &uri)
            });
        }
        if let Some(data) = xdg_dir("XDG_DATA_HOME", ".local/share") {
            edit(&data.join("user-places.xbel"), false, |t| {
                kde_places_without(t, &uri)
            });
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod os {
    use super::*;
    use objc2::AnyThread;
    use objc2_app_kit::{NSImage, NSWorkspace, NSWorkspaceIconCreationOptions};
    use objc2_foundation::{NSData, NSString};

    fn set_icon(folder: &Path, png: Option<&[u8]>) -> std::io::Result<()> {
        let path = NSString::from_str(&folder.display().to_string());
        let image = match png {
            Some(bytes) => Some(
                NSImage::initWithData(NSImage::alloc(), &NSData::with_bytes(bytes))
                    .ok_or_else(|| std::io::Error::other("the icon isn't an image"))?,
            ),
            None => None,
        };
        let set = NSWorkspace::sharedWorkspace().setIcon_forFile_options(
            image.as_deref(),
            &path,
            NSWorkspaceIconCreationOptions(0),
        );
        if set {
            Ok(())
        } else {
            Err(std::io::Error::other("Finder didn't take the icon"))
        }
    }

    pub fn decorate(_paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        set_icon(folder, Some(icons.png))
    }

    pub fn undecorate(_paths: &Paths, folder: &Path) -> std::io::Result<()> {
        set_icon(folder, None)
    }
}

#[cfg(windows)]
mod os {
    use super::*;
    use std::os::windows::ffi::OsStrExt;
    use std::process::Command;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_SYSTEM, GetFileAttributesW,
        INVALID_FILE_ATTRIBUTES, SetFileAttributesW,
    };

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    /// Adds (or with `on` false, removes) attribute bits.
    fn attributes(path: &Path, bits: u32, on: bool) -> std::io::Result<()> {
        let name = wide(path);
        // SAFETY: a NUL-terminated path.
        let now = unsafe { GetFileAttributesW(name.as_ptr()) };
        if now == INVALID_FILE_ATTRIBUTES {
            return Err(std::io::Error::last_os_error());
        }
        let new = if on { now | bits } else { now & !bits };
        // SAFETY: a NUL-terminated path.
        if unsafe { SetFileAttributesW(name.as_ptr(), new) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Takes the folder off Explorer's Quick access, where versions before the sync root
    /// pinned it, through the shell's verbs.
    fn unpin_quick_access(folder: &Path) {
        let path = folder.display().to_string().replace('\'', "''");
        let script = format!(
            "(New-Object -ComObject Shell.Application).Namespace(\
             'shell:::{{679f85cb-0220-4080-b29b-5540cc05aab6}}').Items() | \
             Where-Object {{ $_.Path -eq '{path}' }} | \
             ForEach-Object {{ $_.InvokeVerb('unpinfromhome') }}"
        );
        let _ = Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output();
    }

    fn icon(paths: &Paths) -> PathBuf {
        paths.state.join("homebase.ico")
    }

    /// Explorer's folder settings, giving the icon.
    fn desktop_ini(icon: &Path) -> String {
        format!(
            "[.ShellClassInfo]\r\nIconResource={},0\r\nInfoTip=Synced with Homebase\r\n",
            icon.display()
        )
    }

    /// Registers the folder as a sync root; one pinned to Quick access by an older version
    /// comes off it when the root is new, so it isn't in the navigation pane twice.
    pub fn sync_root(paths: &Paths, folder: &Path, identity: &str, icons: Icons<'_>) {
        let icon = icon(paths);
        let registered = std::fs::write(&icon, icons.ico)
            .and_then(|()| crate::sync_root::register(folder, identity, &icon));
        match registered {
            Ok(true) => unpin_quick_access(folder),
            Ok(false) => {}
            Err(e) => tracing::info!("the folder in Explorer's navigation pane: {e}"),
        }
    }

    pub fn decorate(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon(paths);
        std::fs::write(&icon, icons.ico)?;
        let ini = folder.join("desktop.ini");
        if !ini.exists() {
            std::fs::write(&ini, desktop_ini(&icon))?;
            attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, true)?;
        }
        // Explorer reads desktop.ini of read-only (or system) folders only.
        attributes(folder, FILE_ATTRIBUTE_READONLY, true)?;
        Ok(())
    }

    pub fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        if let Err(e) = crate::sync_root::unregister(folder) {
            tracing::info!("the folder in Explorer's navigation pane: {e}");
        }
        let ini = folder.join("desktop.ini");
        let ours = std::fs::read_to_string(&ini).is_ok_and(|t| t == desktop_ini(&icon(paths)));
        if ours {
            attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, false)?;
            std::fs::remove_file(&ini)?;
            attributes(folder, FILE_ATTRIBUTE_READONLY, false)?;
        }
        unpin_quick_access(folder);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URI: &str = "file:///home/anna/Homebase";

    #[test]
    fn uris() {
        assert_eq!(
            file_uri(Path::new("/home/anna/My Files/ø")),
            "file:///home/anna/My%20Files/%C3%B8"
        );
    }

    #[test]
    fn gtk_bookmarks() {
        let before = "file:///home/anna/Music\nfile:///home/anna/Work Work";
        let with = gtk_bookmarks_with(before, URI, "Homebase").unwrap_or_default();
        assert_eq!(
            with,
            "file:///home/anna/Music\nfile:///home/anna/Work Work\nfile:///home/anna/Homebase Homebase\n"
        );
        assert_eq!(gtk_bookmarks_with(&with, URI, "Homebase"), None, "once");
        assert_eq!(
            gtk_bookmarks_without(&with, URI).as_deref(),
            Some("file:///home/anna/Music\nfile:///home/anna/Work Work\n")
        );
        assert_eq!(gtk_bookmarks_without(before, URI), None);
    }

    #[test]
    fn kde_places() {
        let before = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE xbel>\n\
            <xbel xmlns:bookmark=\"http://www.freedesktop.org/standards/desktop-bookmarks\">\n \
            <bookmark href=\"file:///home/anna\">\n  <title>Home</title>\n </bookmark>\n</xbel>\n";
        let with = kde_places_with(before, URI, "Homebase").unwrap_or_default();
        assert!(with.contains("<bookmark href=\"file:///home/anna/Homebase\">"));
        assert!(with.ends_with(" </bookmark>\n</xbel>\n"), "{with}");
        assert_eq!(kde_places_with(&with, URI, "Homebase"), None, "once");
        assert_eq!(kde_places_without(&with, URI).as_deref(), Some(before));
        assert_eq!(kde_places_without(before, URI), None);
    }
}
