//! The synced folder in the file manager: the Filesync icon on it, and a place in the sidebar
//! (Windows: Explorer's navigation pane, `sync_root.rs`; Linux: GTK bookmarks and KDE places;
//! Finder's sidebar has no public interface). The shortcut is made once per folder (a note in
//! the state folder for each), so one the user removed stays removed; the icon again when a version
//! brings another. Its folder of shares (what other identities share, sync semantics §13) has
//! an icon of its own, and no shortcut: it is in the main folder. Undone when the folder is
//! unlinked. All best effort: a file manager that doesn't take it is no failure.

use std::path::{Path, PathBuf};

use crate::{Identity, Paths};

/// A folder's icon, as each OS wants it.
#[derive(Debug, Clone, Copy)]
pub struct Icons<'a> {
    /// What its file in the state folder is named by (`<name>-<hash>.png`, `.ico`): each
    /// icon its own, as a new one of a name takes the place of the old.
    pub name: &'a str,
    /// A PNG (Linux, macOS).
    pub png: &'a [u8],
    /// An ICO (Windows).
    pub ico: &'a [u8],
}

/// The note that `folder` is decorated, holding the hash of the icon it got (empty when that
/// failed): `folder-decorated-<hash of the folder's path>`.
fn note(paths: &Paths, folder: &Path) -> PathBuf {
    let key = hbsync_platform::fnv::fnv1a64(folder.to_string_lossy().as_bytes());
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

/// What a folder's note holds once it has `icons`: a hash of the icon this OS gives it.
fn icon_key(icons: Icons<'_>) -> String {
    let bytes = if cfg!(windows) { icons.ico } else { icons.png };
    format!("{:016x}", hbsync_platform::fnv::fnv1a64(bytes))
}

/// The icon `kind` ([`Icons::name`]) as a file in the state folder, named after its content:
/// another icon gets another name, so file managers that cache icons by path show it. Written
/// whole or not at all; that kind's with other names go (one Explorer still holds stays, until
/// a later start).
#[cfg(any(not(target_os = "macos"), test))]
fn icon_file(paths: &Paths, kind: &str, bytes: &[u8], ext: &str) -> std::io::Result<PathBuf> {
    let name = format!("{kind}-{:016x}.{ext}", hbsync_platform::fnv::fnv1a64(bytes));
    let path = paths.state.join(&name);
    if !path.exists() {
        hbsync_fileio::write_atomic(&path, bytes)?;
    }
    // (By the whole pattern: another kind's name may begin with this one's.)
    let of_kind = |other: &str| {
        other
            .strip_prefix(kind)
            .and_then(|rest| rest.strip_prefix('-'))
            .and_then(|rest| rest.strip_suffix(ext))
            .and_then(|rest| rest.strip_suffix('.'))
            .is_some_and(|hash| hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
    };
    for entry in std::fs::read_dir(&paths.state)?.flatten() {
        let other = entry.file_name();
        let other = other.to_string_lossy();
        if other != name && of_kind(&other) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(path)
}

/// Whether `folder` is set up as the main folder synced with `identity`.
fn is_main(paths: &Paths, folder: &Path, identity: &Identity) -> bool {
    crate::Config::load(&paths.config_file())
        .ok()
        .flatten()
        .is_some_and(|c| {
            c.roots
                .iter()
                .any(|r| r.folder == folder && r.identity == *identity && r.is_main())
        })
}

/// Gives `folder` (synced with `identity`) its icon and shortcut, unless it already has them.
pub fn decorate(paths: &Paths, folder: &Path, identity: &Identity, icons: Icons<'_>) {
    // Not for a folder no longer set up (a late call that lost the race with Log out), nor
    // for one that isn't the main folder (a role folder or the Desktop is the user's own).
    if !is_main(paths, folder, identity) {
        return;
    }
    let done = note(paths, folder);
    let key = icon_key(icons);
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

/// Gives the folder of shares `name` at the top of `main` (the main folder synced with
/// `identity`) its icon, unless it has it: the icon alone, as its place is in the main
/// folder, which has the shortcut. Only the folder on the main folder's record, as it is
/// now (the daemon's status may be a moment old): never another folder that took the name,
/// nor what a link of that name leads to.
pub fn decorate_shares(
    paths: &Paths,
    main: &Path,
    identity: &Identity,
    name: &str,
    icons: Icons<'_>,
) {
    let folder = main.join(name);
    let shares = hbsync_local::shares::is_folder(main, &folder).unwrap_or(false);
    if !shares || !is_main(paths, main, identity) {
        return;
    }
    let done = note(paths, &folder);
    let key = icon_key(icons);
    let said = std::fs::read_to_string(&done).ok();
    if !shares_icon_due(said.as_deref(), &key, folder.join(os::ICON_FILE).exists()) {
        return;
    }
    tracing::info!("giving {} its icon", folder.display());
    let set = os::icon(paths, &folder, icons);
    if let Err(e) = &set {
        tracing::info!("the icon of the folder of shares: {e}");
    }
    // One that failed is tried again at the next start (the note keeps no key).
    let key = if set.is_ok() { key.as_str() } else { "" };
    if let Err(e) = std::fs::write(&done, format!("{key}\n")) {
        tracing::warn!("{}: {e}", done.display());
    }
}

/// What is left of the icon of a folder of shares at `was` in `main` that the daemon no
/// longer tells of there (renamed, moved away, deleted; or nothing is shared any more):
/// nothing while it is the folder of shares still. Else its note goes, and what the file
/// manager keeps by the path, not in the folder: another folder at that path (the drive's own
/// folder of that name, where the folder of shares gave way to it) would show the icon.
pub fn forget_shares(paths: &Paths, main: &Path, was: &Path) {
    if hbsync_local::shares::is_folder(main, was).unwrap_or(true) {
        return;
    }
    os::forget_icon(was);
    let _ = std::fs::remove_file(note(paths, was));
}

/// Whether the folder of shares is to get the icon hashed to `key`, its note saying `note`
/// and the file that carries a folder's icon being in it or not (`has_file`). The note
/// outlives a folder the user removed: one made again in its place has no icon, its file
/// having gone with the folder.
fn shares_icon_due(note: Option<&str>, key: &str, has_file: bool) -> bool {
    to_do(note, key).1 || !has_file
}

/// Takes the icon and shortcut off `folder` (synced with `identity`) again (unlinking it), and
/// the icon off its folder of shares.
pub fn undecorate(paths: &Paths, folder: &Path, identity: &Identity) {
    #[cfg(windows)]
    os::unregister(folder, identity);
    #[cfg(not(windows))]
    let _ = identity;
    for shares in hbsync_local::shares::folders(folder).unwrap_or_default() {
        let shares = folder.join(shares);
        if let Err(e) = os::icon_off(paths, &shares) {
            tracing::info!("removing the icon of the folder of shares: {e}");
        }
        let _ = std::fs::remove_file(note(paths, &shares));
    }
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
pub(crate) fn gtk_bookmarks_without(text: &str, uri: &str) -> Option<String> {
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
pub(crate) fn kde_places_without(xbel: &str, uri: &str) -> Option<String> {
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

    /// The file in a folder that carries its icon (built-in junk: never synced).
    pub(super) const ICON_FILE: &str = ".directory";

    pub(super) fn icon(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon_file(paths, icons.name, icons.png, "png")?;
        // GNOME's Files and others: GIO metadata (outside the folder).
        let _ = Command::new("gio")
            .args(["set", "-t", "string"])
            .arg(folder)
            .arg("metadata::custom-icon")
            .arg(file_uri(&icon))
            .output();
        // KDE's Dolphin: `.directory` in the folder (never synced: it is built-in junk).
        let directory = folder.join(ICON_FILE);
        match std::fs::read_to_string(&directory) {
            Ok(text) if !our_dolphin(paths, &text) => {}
            _ => std::fs::write(&directory, dolphin(&icon))?,
        }
        Ok(())
    }

    pub(super) fn shortcut(folder: &Path) {
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

    /// Forgets the icon given to the folder that was at `path`: GIO keeps it by the path,
    /// and a folder renamed (not through GIO) leaves it there for whatever comes next.
    /// Best effort: GIO is thought to refuse while nothing is at the path (not checked: no
    /// machine with GVfs metadata was at hand), and a folder made there later shows the icon.
    pub(super) fn forget_icon(path: &Path) {
        let _ = Command::new("gio")
            .args(["set", "-d"])
            .arg(path)
            .arg("metadata::custom-icon")
            .output();
    }

    /// Takes the icon off `folder`: what [`icon`] set.
    pub(super) fn icon_off(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        forget_icon(folder);
        let directory = folder.join(ICON_FILE);
        if std::fs::read_to_string(&directory).is_ok_and(|t| our_dolphin(paths, &t)) {
            std::fs::remove_file(&directory)?;
        }
        Ok(())
    }

    pub(super) fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        icon_off(paths, folder)?;
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

    /// The file in a folder that carries its icon (built-in junk: never synced).
    pub(super) const ICON_FILE: &str = "Icon\r";

    pub(super) fn icon(_paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        set_icon(folder, Some(icons.png))
    }

    /// Nothing is kept by a folder's path: its icon is in it, and goes where it goes.
    pub(super) fn forget_icon(_path: &Path) {}

    /// Finder's sidebar has no public interface.
    pub(super) fn shortcut(_folder: &Path) {}

    /// Takes the icon off `folder`: what [`icon`] set.
    pub(super) fn icon_off(_paths: &Paths, folder: &Path) -> std::io::Result<()> {
        set_icon(folder, None)
    }

    pub(super) fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        icon_off(paths, folder)
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
    pub(super) fn sync_root(
        paths: &Paths,
        folder: &Path,
        identity: &str,
        icons: Icons<'_>,
        first: bool,
    ) {
        let registered = icon_file(paths, icons.name, icons.ico, "ico")
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

    pub(super) fn unregister(folder: &Path, identity: &str) {
        if let Err(e) = crate::sync_root::unregister(folder, identity) {
            tracing::info!("the folder in Explorer's navigation pane: {e}");
        }
    }

    /// The file in a folder that carries its icon (built-in junk: never synced).
    pub(super) const ICON_FILE: &str = "desktop.ini";

    /// Nothing is kept by a folder's path: its icon is in it, and goes where it goes.
    pub(super) fn forget_icon(_path: &Path) {}

    pub(super) fn icon(paths: &Paths, folder: &Path, icons: Icons<'_>) -> std::io::Result<()> {
        let icon = icon_file(paths, icons.name, icons.ico, "ico")?;
        let ini = folder.join(ICON_FILE);
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

    /// Takes the icon off `folder`: what [`icon`] set.
    pub(super) fn icon_off(_paths: &Paths, folder: &Path) -> std::io::Result<()> {
        let ini = folder.join(ICON_FILE);
        let ours = std::fs::read_to_string(&ini).is_ok_and(|t| t.contains(OURS));
        if ours {
            attributes(&ini, FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM, false)?;
            std::fs::remove_file(&ini)?;
            attributes(folder, FILE_ATTRIBUTE_READONLY, false)?;
        }
        Ok(())
    }

    pub(super) fn undecorate(paths: &Paths, folder: &Path) -> std::io::Result<()> {
        icon_off(paths, folder)?;
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

    /// Each kind of icon has a file of its own in the state folder: a new one takes the
    /// place of its kind's old one, and leaves the other kind's.
    #[test]
    fn each_kind_of_icon_keeps_its_file() -> std::io::Result<()> {
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        std::fs::create_dir_all(&paths.state)?;
        let main = icon_file(&paths, "homebase", b"main", "png")?;
        let shares = icon_file(&paths, "homebase-shares", b"shares", "png")?;
        assert_ne!(main, shares);
        assert!(main.exists() && shares.exists());
        // A new version's main icon: the old one goes, the folder of shares' stays; and so
        // the other way round. Another format's stays too.
        let ico = icon_file(&paths, "homebase", b"main", "ico")?;
        let newer = icon_file(&paths, "homebase", b"main 2", "png")?;
        assert!(!main.exists() && newer.exists() && shares.exists() && ico.exists());
        let newer_shares = icon_file(&paths, "homebase-shares", b"shares 2", "png")?;
        assert!(!shares.exists() && newer_shares.exists() && newer.exists());
        Ok(())
    }

    /// The folder of shares gets its icon once, again when a version brings another, and
    /// again when the folder was made anew (the note says done; the icon's file isn't there).
    #[test]
    fn the_folder_of_shares_gets_its_icon_when_it_has_none() {
        assert!(shares_icon_due(None, "k1", false));
        assert!(!shares_icon_due(Some("k1\n"), "k1", true));
        assert!(shares_icon_due(Some("k0\n"), "k1", true));
        assert!(shares_icon_due(Some("\n"), "k1", true), "it failed: again");
        assert!(shares_icon_due(Some("k1\n"), "k1", false), "made anew");
    }

    /// Only the folder of shares of a main folder that is set up gets an icon: never what
    /// a link of its name leads to, nor another folder that took its name. And what is
    /// left of the icon by a path it left is forgotten, not while it is there.
    #[test]
    fn nothing_else_gets_the_icon_of_the_folder_of_shares() -> Result<(), Box<dyn std::error::Error>>
    {
        use hbsync_local::shares::{Made, make};
        let home = tempfile::tempdir()?;
        let paths = Paths::under(home.path());
        std::fs::create_dir_all(&paths.state)?;
        let (me, main) = (
            Identity::from("anna.example.com"),
            home.path().join("Homebase"),
        );
        let icons = Icons {
            name: "shares",
            png: b"png",
            ico: b"ico",
        };
        let shared = main.join("Shared");
        let untouched =
            |folder: &Path| !folder.join(os::ICON_FILE).exists() && !note(&paths, &shared).exists();
        // Not set up (logged out meanwhile).
        std::fs::create_dir_all(&main)?;
        assert_eq!(make(&main, "Shared")?, Made::Made);
        decorate_shares(&paths, &main, &me, "Shared", icons);
        assert!(untouched(&shared));
        crate::Config::update(&paths.config_file(), |config| {
            config.roots.push(crate::config::Root::main(&me, &main));
        })?;
        // Set up: a note of it stays while it is the folder of shares.
        std::fs::write(note(&paths, &shared), "k\n")?;
        forget_shares(&paths, &main, &shared);
        assert!(note(&paths, &shared).exists());
        // Renamed, and another folder has its name: that one gets nothing, and the note of
        // the path goes.
        std::fs::rename(&shared, main.join("From others"))?;
        std::fs::create_dir(&shared)?;
        forget_shares(&paths, &main, &shared);
        decorate_shares(&paths, &main, &me, "Shared", icons);
        assert!(untouched(&shared));
        // `Shared` a link to the folder of shares, elsewhere now.
        #[cfg(unix)]
        {
            let elsewhere = home.path().join("elsewhere");
            std::fs::remove_dir(&shared)?;
            std::fs::rename(main.join("From others"), &elsewhere)?;
            std::os::unix::fs::symlink(&elsewhere, &shared)?;
            decorate_shares(&paths, &main, &me, "Shared", icons);
            assert!(untouched(&elsewhere));
        }
        Ok(())
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
