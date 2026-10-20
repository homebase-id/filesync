//! The synced folder in the file manager: the Filesync icon on it, and a place in the sidebar
//! (Windows: Explorer's navigation pane, `sync_root.rs`; Linux: GTK bookmarks and KDE places;
//! Finder's sidebar has no public interface). The shortcut is made once per folder (a note in
//! the state folder for each), so one the user removed stays removed; the icon again when a version
//! brings another. Undone when the folder is unlinked. All best effort: a file manager that
//! doesn't take it is no failure.

use std::path::{Path, PathBuf};

use crate::Paths;

/// The Filesync icon, as each OS wants it.
#[derive(Debug, Clone, Copy)]
pub struct Icons<'a> {
    /// A PNG (Linux, macOS).
    pub png: &'a [u8],
    /// An ICO (Windows).
    pub ico: &'a [u8],
}

/// The note that `folder` is decorated, holding the hash of the icon it got (empty when that
/// failed): `folder-decorated-<hash of the folder's path>`.
fn note(paths: &Paths, folder: &Path) -> PathBuf {
    let key = hbsync_fileio::fnv1a64(folder.to_string_lossy().as_bytes());
    paths.state.join(format!("folder-decorated-{key:016x}"))
}

/// What a folder's note (`None`: none yet) says is still to do with the icon hashed to `key`:
/// (the shortcut too, being a new folder; the icon).
fn to_do(note: Option<&str>, key: &str) -> (bool, bool) {
    match note {
        None => (true, true),
        Some(done) => (false, done.trim() != key),
    }
}

/// The icon this OS gives the folder.
fn icon_bytes(icons: Icons<'_>) -> &[u8] {
    if cfg!(windows) { icons.ico } else { icons.png }
}

/// The icon as a file in the state folder, named after its content: another icon gets another
/// name, so file managers that cache icons by path show it. Written whole or not at all; ours
/// with other names go (one Explorer still holds stays, until a later start).
#[cfg(not(target_os = "macos"))]
fn icon_file(paths: &Paths, bytes: &[u8], ext: &str) -> std::io::Result<PathBuf> {
    let name = format!("homebase-{:016x}.{ext}", hbsync_fileio::fnv1a64(bytes));
    let path = paths.state.join(&name);
    if !path.exists() {
        hbsync_fileio::write_atomic(&path, bytes)?;
    }
    for entry in std::fs::read_dir(&paths.state)?.flatten() {
        let other = entry.file_name();
        let other = other.to_string_lossy();
        if other != name && other.starts_with("homebase") && other.ends_with(&format!(".{ext}")) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(path)
}

/// Gives `folder` (synced with `identity`) its icon and shortcut, unless it already has them.
pub fn decorate(paths: &Paths, folder: &Path, identity: &str, icons: Icons<'_>) {
    // Not for a folder no longer set up: a late call that lost the race with Log out.
    let configured = crate::Config::load(&paths.config_file())
        .ok()
        .flatten()
        .is_some_and(|c| {
            c.roots
                .iter()
                .any(|r| r.folder == folder && r.identity == identity)
        });
    if !configured {
        return;
    }
    let done = note(paths, folder);
    let key = format!("{:016x}", hbsync_fileio::fnv1a64(icon_bytes(icons)));
    let (first, icon) = to_do(std::fs::read_to_string(&done).ok().as_deref(), &key);
    // Each time, not once: it also reaches folders decorated before there was one.
    #[cfg(windows)]
    os::sync_root(paths, folder, identity, icons, first);
    // An icon that failed is tried again at the next start (the note keeps no key).
    let icon_done = !icon || {
        tracing::info!("giving {} its icon", folder.display());
        let done = os::icon(paths, folder, icons);
        if let Err(e) = &done {
            tracing::info!("the folder's icon: {e}");
        }
        done.is_ok()
    };
    #[cfg(not(windows))]
    if first {
        os::shortcut(folder);
    }
    let key = if icon_done { key.as_str() } else { "" };
    if (first || icon)
        && let Err(e) = std::fs::write(&done, format!("{key}\n"))
    {
        tracing::warn!("{}: {e}", done.display());
    }
}

/// Takes the icon and shortcut off `folder` (synced with `identity`) again (unlinking it).
pub fn undecorate(paths: &Paths, folder: &Path, identity: &str) {
    #[cfg(windows)]
    os::unregister(folder, identity);
    #[cfg(not(windows))]
    let _ = identity;
    if let Err(e) = os::undecorate(paths, folder) {
        tracing::info!("removing the folder's icon or shortcut: {e}");
    }
    let _ = std::fs::remove_file(note(paths, folder));
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
pub(crate) fn gtk_bookmarks_with(text: &str, uri: &str, label: &str) -> Option<String> {
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
pub(crate) fn kde_places_with(xbel: &str, uri: &str, title: &str) -> Option<String> {
    if xbel.contains(&format!("href=\"{uri}\"")) {
        return None;
    }
    let end = xbel.rfind("</xbel>")?;
    let bookmark = format!(
        " <bookmark href=\"{uri}\">\n  <title>{title}</title>\n  <info>\n   \
         <metadata owner=\"http://freedesktop.org\">\n    \
         <bookmark:icon name=\"folder-cloud\"/>\n   </metadata>\n  </info>\n </bookmark>\n",
        title = crate::local_http::escape(title),
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

    /// Dolphin's folder settings, giving the icon.
    fn dolphin(icon: &Path) -> String {
        format!("[Desktop Entry]\nIcon={}\n", icon.display())
    }

    /// Whether `.directory` is ours: only the icon, one in the state folder.
    fn our_dolphin(paths: &Paths, text: &str) -> bool {
        text.strip_prefix("[Desktop Entry]\nIcon=")
            .and_then(|rest| rest.strip_suffix('\n'))
            .is_some_and(|icon| Path::new(icon).parent() == Some(&paths.state))
    }

    pub fn icon(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon_file(paths, icons.png, "png")?;
        // GNOME's Files and others: GIO metadata (outside the folder).
        let _ = Command::new("gio")
            .args(["set", "-t", "string"])
            .arg(folder)
            .arg("metadata::custom-icon")
            .arg(file_uri(&icon))
            .output();
        // KDE's Dolphin: `.directory` in the folder (never synced: it is built-in junk).
        let directory = folder.join(".directory");
        match std::fs::read_to_string(&directory) {
            Ok(text) if !our_dolphin(paths, &text) => {}
            _ => std::fs::write(&directory, dolphin(&icon))?,
        }
        Ok(())
    }

    pub fn shortcut(folder: &Path) {
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
    }

    pub fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        let _ = Command::new("gio")
            .args(["set", "-d"])
            .arg(folder)
            .arg("metadata::custom-icon")
            .output();
        let directory = folder.join(".directory");
        if std::fs::read_to_string(&directory).is_ok_and(|t| our_dolphin(paths, &t)) {
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

    pub fn icon(_paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        set_icon(folder, Some(icons.png))
    }

    /// Finder's sidebar has no public interface.
    pub fn shortcut(_folder: &Path) {}

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
    use windows_sys::Win32::UI::Shell::{SHCNE_UPDATEITEM, SHCNF_PATHW, SHChangeNotify};

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

    /// Explorer's Quick access, through the shell's verbs.
    fn quick_access(folder: &Path, pin: bool) {
        let path = folder.display().to_string().replace('\'', "''");
        let script = if pin {
            format!(
                "(New-Object -ComObject Shell.Application).Namespace('{path}').Self.InvokeVerb('pintohome')"
            )
        } else {
            format!(
                "(New-Object -ComObject Shell.Application).Namespace(\
                 'shell:::{{679f85cb-0220-4080-b29b-5540cc05aab6}}').Items() | \
                 Where-Object {{ $_.Path -eq '{path}' }} | \
                 ForEach-Object {{ $_.InvokeVerb('unpinfromhome') }}"
            )
        };
        let _ = Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output();
    }

    /// Explorer's folder settings, giving the icon.
    fn desktop_ini(icon: &Path) -> String {
        format!(
            "[.ShellClassInfo]\r\nIconResource={},0\r\n{OURS}\r\n",
            icon.display()
        )
    }

    /// The line that marks a desktop.ini as ours.
    const OURS: &str = "InfoTip=Synced with Homebase";

    /// Tells Explorer the item changed, so it shows the new icon.
    fn changed(path: &Path) {
        let name = wide(path);
        // SAFETY: a NUL-terminated path, as SHCNF_PATHW says; the second item is unused.
        unsafe {
            SHChangeNotify(
                SHCNE_UPDATEITEM as i32,
                SHCNF_PATHW,
                name.as_ptr().cast(),
                std::ptr::null(),
            )
        };
    }

    /// Makes the folder a sync root, at the top of Explorer's navigation pane. A folder decorated
    /// before (`!first`) may be pinned to Quick access, by an older version or as the fallback
    /// below: it comes off, so it isn't in the pane twice. Where Explorer won't take a root (not
    /// NTFS, a share, inside another provider's root), the first time pins it there instead.
    pub fn sync_root(paths: &Paths, folder: &Path, identity: &str, icons: Icons<'_>, first: bool) {
        let registered = icon_file(paths, icons.ico, "ico")
            .and_then(|icon| crate::sync_root::register(folder, identity, &icon));
        match registered {
            Ok(true) if !first => quick_access(folder, false),
            Ok(_) => {}
            Err(e) => {
                tracing::info!("the folder in Explorer's navigation pane: {e}");
                if first {
                    quick_access(folder, true);
                }
            }
        }
    }

    pub fn unregister(folder: &Path, identity: &str) {
        if let Err(e) = crate::sync_root::unregister(folder, identity) {
            tracing::info!("the folder in Explorer's navigation pane: {e}");
        }
    }

    pub fn icon(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon_file(paths, icons.ico, "ico")?;
        let ini = folder.join("desktop.ini");
        match std::fs::read_to_string(&ini) {
            Ok(text) if !text.contains(OURS) => {}
            Ok(text) if text == desktop_ini(&icon) => {}
            found => {
                if found.is_ok() {
                    attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, false)?;
                }
                std::fs::write(&ini, desktop_ini(&icon))?;
                attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, true)?;
            }
        }
        // Explorer reads desktop.ini of read-only (or system) folders only.
        attributes(folder, FILE_ATTRIBUTE_READONLY, true)?;
        changed(folder);
        Ok(())
    }

    pub fn undecorate(_paths: &Paths, folder: &Path) -> std::io::Result<()> {
        let ini = folder.join("desktop.ini");
        let ours = std::fs::read_to_string(&ini).is_ok_and(|t| t.contains(OURS));
        if ours {
            attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, false)?;
            std::fs::remove_file(&ini)?;
            attributes(folder, FILE_ATTRIBUTE_READONLY, false)?;
        }
        quick_access(folder, false);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URI: &str = "file:///home/anna/Homebase";

    /// Without it, Windows won't let the packaged tray register the sync root (`sync_root.rs`).
    #[test]
    fn the_msix_declares_cloud_files() {
        let conf = include_str!("../../../packaging/conveyor.conf");
        assert!(conf.contains(r#"<desktop3:Extension Category="windows.cloudFiles">"#));
    }

    #[test]
    fn the_note_says_what_is_left_to_do() {
        assert_eq!(to_do(None, "k1"), (true, true));
        assert_eq!(to_do(Some("k1\n"), "k1"), (false, false));
        // Another version's icons, or an icon that failed: the icon again.
        assert_eq!(to_do(Some("k0\n"), "k1"), (false, true));
        assert_eq!(to_do(Some("\n"), "k1"), (false, true));
    }

    #[test]
    fn each_folder_has_a_note_of_its_own() {
        let paths = Paths::under(Path::new("/h"));
        let (a, b) = (Path::new("/h/Homebase"), Path::new("/h/Documents"));
        assert_ne!(note(&paths, a), note(&paths, b));
        assert_eq!(note(&paths, a), note(&paths, a));
    }

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
