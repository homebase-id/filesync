//! How this copy of Filesync was installed, told from where its program runs (and, on Windows,
//! from the package it runs in). It decides how Filesync starts at logon, how notifications
//! name it, and who updates it: Conveyor's packages update themselves (MSIX, Sparkle, apt), a
//! plain copy doesn't.

use hbsync_platform::Os;
use std::path::{Path, PathBuf};

/// The MSIX application's ID in Conveyor's manifest (its display name without spaces).
pub const MSIX_APP_ID: &str = "HomebaseFilesync";
/// The MSIX startup task Conveyor declares (`start-on-login`).
pub const MSIX_STARTUP_TASK: &str = "HomebaseFilesync.StartupTask";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Install {
    /// A Windows MSIX package; `family` is its package family name.
    Msix { family: String },
    /// A macOS app bundle.
    MacApp { bundle: PathBuf },
    /// A Linux package (.deb, .rpm): under `/usr`, updated by the package manager.
    LinuxPackage,
    /// An archive unpacked anywhere, or a build from source.
    Plain,
}

impl Install {
    /// This program's.
    pub fn detect() -> Install {
        #[cfg(windows)]
        if let Some(family) = package_family() {
            return Install::Msix { family };
        }
        match std::env::current_exe() {
            Ok(exe) => from_path(Os::current(), &exe),
            Err(_) => Install::Plain,
        }
    }

    /// The Windows AppUserModelID that names it in notifications, in a package.
    pub fn app_user_model_id(&self) -> Option<String> {
        match self {
            Install::Msix { family } => Some(format!("{family}!{MSIX_APP_ID}")),
            _ => None,
        }
    }

    /// Why it can't start at logon from where it is: a Mac app run from its disk image, or
    /// from the temporary copy macOS makes of an app it hasn't moved (App Translocation).
    pub fn not_settled(&self) -> Option<&'static str> {
        match self {
            Install::MacApp { bundle } if temporary(bundle) => Some(
                "Move Homebase Filesync to the Applications folder first, then start it from there",
            ),
            _ => None,
        }
    }
}

/// Told from the program's path (an MSIX package is asked from Windows instead).
pub fn from_path(os: Os, exe: &Path) -> Install {
    match os {
        Os::MacOs => {
            let bundle = exe
                .parent()
                .filter(|d| d.ends_with("Contents/MacOS"))
                .and_then(Path::parent)
                .and_then(Path::parent)
                .filter(|b| b.extension().is_some_and(|e| e == "app"));
            match bundle {
                Some(bundle) => Install::MacApp {
                    bundle: bundle.to_path_buf(),
                },
                None => Install::Plain,
            }
        }
        Os::Linux if exe.starts_with("/usr") => Install::LinuxPackage,
        _ => Install::Plain,
    }
}

/// A disk image, or App Translocation's randomized read-only copy.
fn temporary(bundle: &Path) -> bool {
    bundle.starts_with("/Volumes")
        || bundle
            .components()
            .any(|c| c.as_os_str() == "AppTranslocation")
}

/// The package family this process runs in, if it runs in a package.
#[cfg(windows)]
fn package_family() -> Option<String> {
    use windows_sys::Win32::Foundation::ERROR_SUCCESS;
    use windows_sys::Win32::Storage::Packaging::Appx::GetCurrentPackageFamilyName;

    let mut len = 0u32;
    // SAFETY: a null buffer only asks for the length (APPMODEL_ERROR_NO_PACKAGE outside one).
    unsafe { GetCurrentPackageFamilyName(&mut len, std::ptr::null_mut()) };
    if len == 0 {
        return None;
    }
    let mut buf = vec![0u16; len as usize];
    // SAFETY: the buffer holds `len` UTF-16 units, as asked for.
    if unsafe { GetCurrentPackageFamilyName(&mut len, buf.as_mut_ptr()) } != ERROR_SUCCESS {
        return None;
    }
    buf.truncate(len.saturating_sub(1) as usize);
    String::from_utf16(&buf).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn told_from_the_path() {
        let app = "/Applications/Homebase Filesync.app";
        let installed = from_path(
            Os::MacOs,
            Path::new(&format!("{app}/Contents/MacOS/homebase-filesync")),
        );
        assert_eq!(installed, Install::MacApp { bundle: app.into() });
        assert_eq!(installed.not_settled(), None);
        assert_eq!(
            from_path(Os::MacOs, Path::new("/Users/a/bin/homebase-filesync")),
            Install::Plain
        );
        assert_eq!(
            from_path(
                Os::Linux,
                Path::new("/usr/lib/homebase/homebase-filesync/bin/homebase-filesync")
            ),
            Install::LinuxPackage
        );
        assert_eq!(
            from_path(Os::Linux, Path::new("/home/a/.cargo/bin/homebase-filesync")),
            Install::Plain
        );
        assert_eq!(
            from_path(
                Os::Windows,
                Path::new(r"C:\Users\a\hbsync\homebase-filesync.exe")
            ),
            Install::Plain
        );
    }

    #[test]
    fn a_mac_app_must_be_moved_before_it_starts_at_logon() {
        for bundle in [
            "/Volumes/Homebase Filesync/Homebase Filesync.app",
            "/private/var/folders/x/y/T/AppTranslocation/1234-5678/d/Homebase Filesync.app",
        ] {
            let exe = format!("{bundle}/Contents/MacOS/homebase-filesync");
            assert!(
                from_path(Os::MacOs, Path::new(&exe))
                    .not_settled()
                    .is_some(),
                "{bundle}"
            );
        }
    }

    #[test]
    fn notifications_name_the_package() {
        let msix = Install::Msix {
            family: "HomebaseFilesync_abc123".into(),
        };
        assert_eq!(
            msix.app_user_model_id().as_deref(),
            Some("HomebaseFilesync_abc123!HomebaseFilesync")
        );
        assert_eq!(Install::Plain.app_user_model_id(), None);
    }
}
