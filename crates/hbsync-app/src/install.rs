//! How this copy of Filesync was installed, told from where its program runs (and, on Windows,
//! from the package it runs in). It decides how Filesync starts at logon, how notifications
//! name it, and who updates it: Conveyor's packages update themselves (MSIX, Sparkle, apt), a
//! plain copy doesn't.

use hbsync_platform::Os;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The MSIX application's ID in Conveyor's manifest (its display name without spaces).
pub(crate) const MSIX_APP_ID: &str = "HomebaseFilesync";
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

/// Asks Windows to start this program again, with `args`, once an update of its package closed
/// it: the `.appinstaller`'s background update closes the package's running programs and,
/// without this, they stay closed until the next logon. Not after a crash, a hang or a reboot
/// (the startup task starts the tray at logon), and only for the package's programs: Windows
/// would start a private instance's (`HBSYNC_HOME`) without its environment, as the real one.
/// Windows honours it only for a program that ran 60 seconds or more. Unverified on a real PC.
#[cfg(windows)]
pub fn restart_after_update(args: &str) {
    use windows_sys::Win32::System::Recovery::{
        RESTART_NO_CRASH, RESTART_NO_HANG, RESTART_NO_REBOOT, RegisterApplicationRestart,
    };
    if !matches!(Install::current(), Install::Msix { .. }) || crate::control::dev_home() {
        return;
    }
    let args: Vec<u16> = args.encode_utf16().chain(Some(0)).collect();
    // SAFETY: a NUL-terminated string, which Windows copies.
    let result = unsafe {
        RegisterApplicationRestart(
            args.as_ptr(),
            RESTART_NO_CRASH | RESTART_NO_HANG | RESTART_NO_REBOOT,
        )
    };
    if result < 0 {
        tracing::warn!("not started again after an update: HRESULT {result:#x}");
    }
}

/// Takes back [`restart_after_update`]: quitting on purpose, an update closing the program
/// meanwhile mustn't start it again.
#[cfg(windows)]
pub fn no_restart_after_update() {
    // SAFETY: no arguments; it only forgets this process's registration.
    unsafe { windows_sys::Win32::System::Recovery::UnregisterApplicationRestart() };
}

/// This program's path with symlinks resolved (a CLI linked onto the PATH still finds its app),
/// found once. Not resolved on Windows: its canonical form (`\\?\`) isn't wanted, and there
/// are no such links.
pub(crate) fn current_exe() -> std::io::Result<&'static Path> {
    static CURRENT: OnceLock<Option<PathBuf>> = OnceLock::new();
    CURRENT
        .get_or_init(|| {
            let exe = std::env::current_exe().ok()?;
            Some(if cfg!(unix) {
                exe.canonicalize().unwrap_or(exe)
            } else {
                exe
            })
        })
        .as_deref()
        .ok_or_else(|| std::io::Error::other("this program's path is unknown"))
}

impl Install {
    /// This program's, found once.
    pub fn current() -> &'static Install {
        static CURRENT: OnceLock<Install> = OnceLock::new();
        CURRENT.get_or_init(|| {
            #[cfg(windows)]
            if let Some(family) = package_family() {
                return Install::Msix { family };
            }
            match current_exe() {
                Ok(exe) => from_path(Os::current(), exe),
                Err(_) => Install::Plain,
            }
        })
    }

    /// Where its program `name` is, found from the program at `beside`: beside it, except
    /// - in a Mac app the tray is the app's program (`Contents/MacOS`) and the daemon and the
    ///   command line are its helpers (`Contents/Helpers`), so that macOS and the packager know
    ///   which one to launch (a bundle with all three in `Contents/MacOS` still works);
    /// - a Linux package's programs are started by their links in `/usr/bin`, which last
    ///   (where the programs live is the packager's to change).
    pub fn program(&self, name: &str, beside: &Path) -> PathBuf {
        let file = format!("{name}{}", std::env::consts::EXE_SUFFIX);
        let packaged = match self {
            Install::MacApp { bundle } => {
                let dir = if name == crate::autostart::TRAY {
                    "Contents/MacOS"
                } else {
                    "Contents/Helpers"
                };
                Some(bundle.join(dir).join(&file))
            }
            Install::LinuxPackage => Some(Path::new("/usr/bin").join(&file)),
            _ => None,
        };
        packaged
            .filter(|p| p.exists())
            .unwrap_or_else(|| beside.with_file_name(file))
    }

    /// Whether the package lists it among the desktop's applications itself (Linux).
    pub fn lists_itself(&self) -> bool {
        matches!(self, Install::LinuxPackage)
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
            Install::MacApp { bundle }
                if temporary(bundle)
                    || hbsync_platform::fskind::read_only(bundle).unwrap_or(false) =>
            {
                Some(
                    "move Homebase Filesync to the Applications folder and start it from there, then turn on Start at login in its menu",
                )
            }
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
                .filter(|d| d.ends_with("Contents/MacOS") || d.ends_with("Contents/Helpers"))
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
        // /usr/local is where software goes by hand, not by the package manager.
        Os::Linux if exe.starts_with("/usr") && !exe.starts_with("/usr/local") => {
            Install::LinuxPackage
        }
        _ => Install::Plain,
    }
}

/// App Translocation's randomized copy of an app macOS hasn't seen moved.
fn temporary(bundle: &Path) -> bool {
    bundle
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

    /// Windows keeps what the program asked for: these arguments, after an update only.
    #[cfg(windows)]
    #[test]
    fn windows_is_asked_to_start_it_again_after_an_update() {
        use windows_sys::Win32::System::Recovery::{
            GetApplicationRestartSettings, RESTART_NO_CRASH, RESTART_NO_HANG, RESTART_NO_REBOOT,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        // A test runs outside the package: registered directly, as the package's programs are.
        let args: Vec<u16> = "--restarted".encode_utf16().chain(Some(0)).collect();
        let (mut text, mut len, mut flags) = (vec![0u16; 64], 64u32, 0u32);
        // SAFETY: NUL-terminated arguments; `text` holds `len` characters.
        let read = unsafe {
            windows_sys::Win32::System::Recovery::RegisterApplicationRestart(
                args.as_ptr(),
                RESTART_NO_CRASH | RESTART_NO_HANG | RESTART_NO_REBOOT,
            );
            GetApplicationRestartSettings(
                GetCurrentProcess(),
                text.as_mut_ptr(),
                &mut len,
                &mut flags,
            )
        };
        assert_eq!(read, 0);
        assert_eq!(
            String::from_utf16_lossy(&text[..len as usize - 1]),
            "--restarted"
        );
        assert_eq!(
            flags,
            RESTART_NO_CRASH | RESTART_NO_HANG | RESTART_NO_REBOOT
        );
        no_restart_after_update();
        // SAFETY: as above.
        let read = unsafe {
            GetApplicationRestartSettings(
                GetCurrentProcess(),
                text.as_mut_ptr(),
                &mut len,
                &mut flags,
            )
        };
        assert_ne!(read, 0, "forgotten");
    }

    #[test]
    fn told_from_the_path() {
        let app = "/Applications/Homebase Filesync.app";
        let installed = from_path(
            Os::MacOs,
            Path::new(&format!("{app}/Contents/MacOS/homebase-filesync")),
        );
        assert_eq!(installed, Install::MacApp { bundle: app.into() });
        assert_eq!(installed.not_settled(), None);
        let helper = format!("{app}/Contents/Helpers/hbsync");
        assert_eq!(from_path(Os::MacOs, Path::new(&helper)), installed);
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
        for plain in [
            "/home/a/.cargo/bin/homebase-filesync",
            "/usr/local/bin/homebase-filesync",
        ] {
            assert_eq!(from_path(Os::Linux, Path::new(plain)), Install::Plain);
        }
        assert_eq!(
            from_path(
                Os::Windows,
                Path::new(r"C:\Users\a\hbsync\homebase-filesync.exe")
            ),
            Install::Plain
        );
    }

    #[test]
    fn programs_are_found_where_the_app_keeps_them() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let bundle = dir.path().join("Homebase Filesync.app");
        let macos = bundle.join("Contents/MacOS");
        let helpers = bundle.join("Contents/Helpers");
        let exe = |d: &Path, n: &str| d.join(format!("{n}{}", std::env::consts::EXE_SUFFIX));
        let app = Install::MacApp { bundle };
        let tray = exe(&macos, "homebase-filesync");
        // All three in Contents/MacOS (a layout it still finds): beside each other.
        assert_eq!(app.program("hbsyncd", &tray), exe(&macos, "hbsyncd"));
        // The daemon and the CLI as helpers, found from either side.
        std::fs::create_dir_all(&macos)?;
        std::fs::create_dir_all(&helpers)?;
        std::fs::write(exe(&helpers, "hbsyncd"), "")?;
        std::fs::write(&tray, "")?;
        assert_eq!(app.program("hbsyncd", &tray), exe(&helpers, "hbsyncd"));
        let cli = exe(&helpers, "hbsync");
        assert_eq!(app.program("homebase-filesync", &cli), tray);
        assert_eq!(
            Install::Plain.program("hbsyncd", &cli),
            exe(&helpers, "hbsyncd")
        );
        Ok(())
    }

    #[test]
    fn a_translocated_mac_app_must_be_moved_before_it_starts_at_logon() {
        let bundle =
            "/private/var/folders/x/y/T/AppTranslocation/1234-5678/d/Homebase Filesync.app";
        let exe = format!("{bundle}/Contents/MacOS/homebase-filesync");
        assert!(
            from_path(Os::MacOs, Path::new(&exe))
                .not_settled()
                .is_some()
        );
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
