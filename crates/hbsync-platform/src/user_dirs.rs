//! The user's own folders (Documents, Pictures, …) where this OS keeps them, and whether another
//! sync app holds one already: two apps syncing one folder fight over every change.
//!
//! Windows asks the shell for its Known Folders (moved ones included); macOS has them in the
//! home folder; Linux reads XDG's `user-dirs.dirs` (localized names), falling back to the
//! English names in the home folder.

use std::path::{Path, PathBuf};

/// One of the user's own folders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDir {
    Documents,
    Pictures,
    Music,
    Videos,
    Downloads,
}

/// Another app that syncs a folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    OneDrive,
    ICloud,
}

impl Owner {
    pub fn name(self) -> &'static str {
        match self {
            Owner::OneDrive => "OneDrive",
            Owner::ICloud => "iCloud",
        }
    }
}

/// Where this computer keeps `dir`; `None` if it has none (or it is the home folder itself,
/// XDG's way of saying so).
pub fn user_dir(dir: UserDir) -> Option<PathBuf> {
    let found = imp::user_dir(dir)?;
    (Some(&found) != home().as_ref()).then_some(found)
}

/// The app that syncs `path` already, if any.
pub fn owner(path: &Path) -> Option<Owner> {
    owner_among(path, &imp::synced_roots())
}

/// The owner of the first of `roots` that holds `path`.
fn owner_among(path: &Path, roots: &[(Owner, PathBuf)]) -> Option<Owner> {
    roots
        .iter()
        .find(|(_, root)| path.starts_with(root))
        .map(|(owner, _)| *owner)
}

fn home() -> Option<PathBuf> {
    #[cfg(windows)]
    let var = "USERPROFILE";
    #[cfg(not(windows))]
    let var = "HOME";
    std::env::var_os(var)
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

/// The English name a folder has in the home folder (macOS's, and Linux's without XDG).
#[cfg_attr(windows, allow(dead_code))]
fn english(dir: UserDir) -> &'static str {
    match dir {
        UserDir::Documents => "Documents",
        UserDir::Pictures => "Pictures",
        UserDir::Music => "Music",
        UserDir::Videos if cfg!(target_os = "macos") => "Movies",
        UserDir::Videos => "Videos",
        UserDir::Downloads => "Downloads",
    }
}

/// `dir` as `user-dirs.dirs` (its text) gives it: `XDG_DOCUMENTS_DIR="$HOME/Dokumente"`.
/// Relative to `home` unless absolute; `None` when not listed.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn xdg_dir(text: &str, dir: UserDir, home: &Path) -> Option<PathBuf> {
    let key = match dir {
        UserDir::Documents => "XDG_DOCUMENTS_DIR",
        UserDir::Pictures => "XDG_PICTURES_DIR",
        UserDir::Music => "XDG_MUSIC_DIR",
        UserDir::Videos => "XDG_VIDEOS_DIR",
        UserDir::Downloads => "XDG_DOWNLOAD_DIR",
    };
    text.lines().find_map(|line| {
        let value = line
            .trim()
            .strip_prefix(key)?
            .trim_start()
            .strip_prefix('=')?;
        let value = value.trim().strip_prefix('"')?.strip_suffix('"')?;
        let value = value.replace("\\\"", "\"");
        Some(match value.strip_prefix("$HOME") {
            Some(rest) => home.join(rest.trim_start_matches('/')),
            None if value.starts_with('/') => PathBuf::from(value),
            None => home.join(value),
        })
    })
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;

    pub fn user_dir(dir: UserDir) -> Option<PathBuf> {
        let home = home()?;
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .filter(|c| !c.is_empty())
            .map_or_else(|| home.join(".config"), PathBuf::from);
        match std::fs::read_to_string(config.join("user-dirs.dirs")) {
            Ok(text) => xdg_dir(&text, dir, &home),
            Err(_) => Some(home.join(english(dir))),
        }
    }

    pub fn synced_roots() -> Vec<(Owner, PathBuf)> {
        Vec::new()
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;

    pub fn user_dir(dir: UserDir) -> Option<PathBuf> {
        Some(home()?.join(english(dir)))
    }

    /// OneDrive's folders under `~/Library/CloudStorage`; iCloud's Desktop & Documents, which
    /// keeps `~/Documents` (and `~/Desktop`) in iCloud Drive once its folder there exists
    /// (inferred from how macOS lays it out, not from an API).
    pub fn synced_roots() -> Vec<(Owner, PathBuf)> {
        let Some(home) = home() else {
            return Vec::new();
        };
        let mut roots = Vec::new();
        if let Ok(entries) = std::fs::read_dir(home.join("Library/CloudStorage")) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with("OneDrive") {
                    roots.push((Owner::OneDrive, entry.path()));
                }
            }
        }
        let cloud_docs = home.join("Library/Mobile Documents/com~apple~CloudDocs");
        if cloud_docs.join("Documents").is_dir() {
            roots.push((Owner::ICloud, home.join("Documents")));
            roots.push((Owner::ICloud, home.join("Desktop")));
        }
        roots.push((Owner::ICloud, cloud_docs));
        roots
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;

    use windows_sys::Win32::System::Com::CoTaskMemFree;
    use windows_sys::Win32::UI::Shell::{
        FOLDERID_Documents, FOLDERID_Downloads, FOLDERID_Music, FOLDERID_Pictures, FOLDERID_Videos,
        KF_FLAG_DEFAULT, SHGetKnownFolderPath,
    };

    pub fn user_dir(dir: UserDir) -> Option<PathBuf> {
        let id = match dir {
            UserDir::Documents => FOLDERID_Documents,
            UserDir::Pictures => FOLDERID_Pictures,
            UserDir::Music => FOLDERID_Music,
            UserDir::Videos => FOLDERID_Videos,
            UserDir::Downloads => FOLDERID_Downloads,
        };
        let mut path = std::ptr::null_mut();
        // SAFETY: `path` receives a string the shell allocates, freed below whatever the result.
        let hr = unsafe {
            SHGetKnownFolderPath(&id, KF_FLAG_DEFAULT as u32, std::ptr::null_mut(), &mut path)
        };
        let found = (hr >= 0 && !path.is_null()).then(|| {
            // SAFETY: on success `path` is a NUL-terminated wide string.
            let len = (0..).take_while(|&i| unsafe { *path.add(i) } != 0).count();
            let wide = unsafe { std::slice::from_raw_parts(path, len) };
            PathBuf::from(OsString::from_wide(wide))
        });
        // SAFETY: allocated by the shell (or null, which CoTaskMemFree takes).
        unsafe { CoTaskMemFree(path as *const _) };
        found
    }

    /// OneDrive's folders, as it sets them in the environment (personal and work).
    pub fn synced_roots() -> Vec<(Owner, PathBuf)> {
        ["OneDrive", "OneDriveConsumer", "OneDriveCommercial"]
            .into_iter()
            .filter_map(std::env::var_os)
            .filter(|p| !p.is_empty())
            .map(|p| (Owner::OneDrive, PathBuf::from(p)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_dirs_dirs_are_read_as_xdg_writes_them() {
        let home = Path::new("/home/u");
        let text = "# written by xdg-user-dirs-update\n\
                    XDG_DESKTOP_DIR=\"$HOME/Schreibtisch\"\n\
                    XDG_DOCUMENTS_DIR=\"$HOME/Dokumente\"\n\
                    XDG_PICTURES_DIR=\"/data/Bilder\"\n\
                    XDG_DOWNLOAD_DIR=\"$HOME/\"\n";
        assert_eq!(
            xdg_dir(text, UserDir::Documents, home),
            Some(PathBuf::from("/home/u/Dokumente"))
        );
        assert_eq!(
            xdg_dir(text, UserDir::Pictures, home),
            Some(PathBuf::from("/data/Bilder"))
        );
        assert_eq!(xdg_dir(text, UserDir::Music, home), None);
        // The home folder itself: XDG's "none" (user_dir drops it).
        assert_eq!(
            xdg_dir(text, UserDir::Downloads, home),
            Some(PathBuf::from("/home/u/"))
        );
    }

    #[test]
    fn a_folder_inside_another_apps_folder_is_its() {
        let roots = [
            (Owner::OneDrive, PathBuf::from("/u/OneDrive")),
            (Owner::ICloud, PathBuf::from("/u/Documents")),
        ];
        assert_eq!(
            owner_among(Path::new("/u/OneDrive/Documents"), &roots),
            Some(Owner::OneDrive)
        );
        assert_eq!(
            owner_among(Path::new("/u/Documents"), &roots),
            Some(Owner::ICloud)
        );
        assert_eq!(owner_among(Path::new("/u/OneDriveX/a"), &roots), None);
        assert_eq!(owner_among(Path::new("/u/Pictures"), &roots), None);
    }

    #[test]
    fn this_computer_has_its_folders() {
        // Whatever the machine: no panic, and never the home folder itself.
        for dir in [
            UserDir::Documents,
            UserDir::Pictures,
            UserDir::Music,
            UserDir::Videos,
            UserDir::Downloads,
        ] {
            assert_ne!(user_dir(dir), home(), "{dir:?}");
        }
    }
}
