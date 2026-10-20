//! Starting Filesync at logon, as this user (never as a service: it needs the user's keychain
//! and writes the user's files). What starts is the tray app when it is installed with the
//! command line (beside it, or in a Mac app in `Contents/MacOS`; it starts the daemon), else
//! the daemon itself:
//! - macOS: a launchd agent, `~/Library/LaunchAgents/id.homebase.filesync.plist` (the
//!   tray's: `id.homebase.filesync.tray.plist`);
//! - Linux: the daemon as a systemd user unit, `~/.config/systemd/user/hbsync.service`; the tray
//!   as a desktop session autostart entry, `~/.config/autostart/id.homebase.filesync.desktop` (a
//!   GUI needs the session);
//! - Windows: the per-user `Run` key (`HKCU\Software\Microsoft\Windows\CurrentVersion\Run`),
//!   which needs no administrator, unlike a logon task; in the MSIX package, its startup task
//!   (the `Run` key would be virtualized there, and the package's path changes with each
//!   version).
//!
//! launchd and systemd restart the daemon if it crashes, not when it was stopped. The
//! environment overrides (`HBSYNC_HOME`, `HBSYNC_CREDENTIALS`) are passed on where the manager
//! can (not on Windows).

use std::io;
use std::path::{Path, PathBuf};

/// The app identity (as in the packages; chat is `id.homebase.feed`), and the daemon's launchd
/// label (the tray's adds `.tray`).
pub const LABEL: &str = "id.homebase.filesync";
/// The tray's program name.
pub const TRAY: &str = "homebase-filesync";
/// The tray's desktop entry (Linux): in the session's autostart folder to start at logon, and
/// in the applications folder as its launcher. Named after the app ID, as the package's is.
pub const DESKTOP_ENTRY: &str = "id.homebase.filesync.desktop";
/// The tray's icon name in the icon theme (Linux).
pub const ICON: &str = "homebase-filesync";
/// The systemd unit.
pub const UNIT: &str = "hbsync.service";
/// The `Run` value's name.
pub const RUN_VALUE: &str = "Homebase Filesync";

/// The environment variables passed on to the daemon, as they are set now.
/// They decide where the daemon keeps its files ([`crate::Paths`]): the daemon must use the
/// same files as the command line that set it up. (`XDG_RUNTIME_DIR` is the session's own,
/// set by systemd.)
pub fn passed_env() -> Vec<(String, String)> {
    [
        "HBSYNC_HOME",
        "HBSYNC_CREDENTIALS",
        "XDG_CONFIG_HOME",
        "XDG_STATE_HOME",
    ]
    .into_iter()
    // Empty is unset ([`crate::Paths`]).
    .filter_map(|k| {
        Some((
            k.to_owned(),
            std::env::var(k).ok().filter(|v| !v.is_empty())?,
        ))
    })
    .collect()
}

/// What starts at logon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Program {
    Daemon(PathBuf),
    /// The tray app, which starts the daemon.
    Tray(PathBuf),
}

impl Program {
    pub fn path(&self) -> &Path {
        match self {
            Program::Daemon(p) | Program::Tray(p) => p,
        }
    }

    /// Its launchd label.
    fn label(&self) -> String {
        match self {
            Program::Daemon(_) => LABEL.to_owned(),
            Program::Tray(_) => format!("{LABEL}.tray"),
        }
    }
}

/// The launchd agent for `program`.
pub fn launchd_plist(program: &Program, env: &[(String, String)]) -> String {
    let (label, kind) = match program {
        Program::Tray(_) => (program.label(), "Interactive"),
        Program::Daemon(_) => (program.label(), "Background"),
    };
    let esc = crate::local_http::escape;
    let mut env_xml = String::new();
    if !env.is_empty() {
        env_xml.push_str("  <key>EnvironmentVariables</key>\n  <dict>\n");
        for (k, v) in env {
            env_xml.push_str(&format!(
                "    <key>{}</key>\n    <string>{}</string>\n",
                esc(k),
                esc(v)
            ));
        }
        env_xml.push_str("  </dict>\n");
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ProcessType</key>
  <string>{kind}</string>
{env_xml}</dict>
</plist>
"#,
        esc(&program.path().display().to_string())
    )
}

/// The tray's desktop entry, for autostart and the launcher (freedesktop Desktop Entry spec:
/// the environment through `env`, every argument quoted).
pub fn desktop_entry(tray: &Path, env: &[(String, String)]) -> String {
    let quote = |s: &str| {
        let s = s
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('`', "\\`")
            .replace('$', "\\$")
            .replace('%', "%%");
        format!("\"{s}\"")
    };
    let mut exec = String::new();
    if !env.is_empty() {
        exec.push_str("env");
        for (k, v) in env {
            exec.push(' ');
            exec.push_str(&quote(&format!("{k}={v}")));
        }
        exec.push(' ');
    }
    exec.push_str(&quote(&tray.display().to_string()));
    format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=Homebase Filesync\n\
         Comment=Keeps your Homebase folder in sync\n\
         Exec={exec}\n\
         Icon={ICON}\n\
         Categories=Network;FileTransfer;\n\
         Terminal=false\n\
         X-GNOME-Autostart-enabled=true\n"
    )
}

/// The systemd user unit for `daemon`.
pub fn systemd_unit(daemon: &Path, env: &[(String, String)]) -> String {
    // systemd's quoting: double quotes, with backslashes and quotes escaped, and its
    // specifiers (`%`) doubled; `ExecStart` also expands variables (`$`), `Environment` not.
    let quote = |s: &str| {
        let s = s
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%");
        format!("\"{s}\"")
    };
    let mut env_lines = String::new();
    for (k, v) in env {
        env_lines.push_str(&format!("Environment={}\n", quote(&format!("{k}={v}"))));
    }
    let exec = quote(&daemon.display().to_string()).replace('$', "$$");
    format!(
        "[Unit]\n\
         Description=Homebase Filesync\n\
         \n\
         [Service]\n\
         ExecStart={}\n\
         {env_lines}\
         Restart=on-failure\n\
         RestartSec=10\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exec
    )
}

/// The `Run` value for `daemon`: its path, quoted.
pub fn run_value(daemon: &Path) -> String {
    format!("\"{}\"", daemon.display())
}

/// The program `name` of this installation ([`crate::install::Install::program`]).
pub(crate) fn program_of(name: &str) -> io::Result<PathBuf> {
    let exe = crate::install::current_exe()?;
    Ok(crate::install::Install::current().program(name, exe))
}

/// Where the daemon is ([`program_of`]).
pub(crate) fn daemon_exe() -> io::Result<PathBuf> {
    program_of(crate::lock::DAEMON)
}

/// What starts at logon: the tray of this installation if it is there (it starts the daemon),
/// else the daemon.
pub fn logon_program() -> io::Result<Program> {
    if let Some(why) = crate::install::Install::current().not_settled() {
        return Err(io::Error::other(why));
    }
    let tray = program_of(TRAY)?;
    if tray.is_file() {
        return Ok(Program::Tray(tray));
    }
    Ok(Program::Daemon(daemon_exe()?))
}

#[cfg(unix)]
fn home() -> io::Result<PathBuf> {
    crate::paths::home().ok_or_else(|| io::Error::other("HOME isn't set"))
}

/// Runs a service manager command, turning a failure into an error with its output.
#[cfg(unix)]
fn run(program: &str, args: &[&str]) -> io::Result<()> {
    let out = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| io::Error::new(e.kind(), format!("{program}: {e}")))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{program} {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

#[cfg(target_os = "macos")]
mod os {
    use super::*;

    /// The daemon's and the tray's labels.
    fn labels() -> [String; 2] {
        let none = PathBuf::new();
        [
            Program::Daemon(none.clone()).label(),
            Program::Tray(none).label(),
        ]
    }

    fn plist(label: &str) -> io::Result<PathBuf> {
        Ok(home()?
            .join("Library/LaunchAgents")
            .join(format!("{label}.plist")))
    }

    fn domain() -> String {
        // SAFETY: getuid has no preconditions and can't fail.
        format!("gui/{}", unsafe { libc::getuid() })
    }

    pub(super) fn installed() -> io::Result<bool> {
        for label in labels() {
            if plist(&label)?.exists() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn install(program: &Program) -> io::Result<()> {
        uninstall()?;
        let path = plist(&program.label())?;
        hbsync_fileio::write_if_changed(&path, launchd_plist(program, &passed_env()).as_bytes())?;
        // The tray asks for this itself, running: launchd loads the agent at the next logon.
        if matches!(program, Program::Tray(_)) {
            return Ok(());
        }
        run(
            "launchctl",
            &["bootstrap", &domain(), &path.display().to_string()],
        )
        // An agent launchd didn't take mustn't look installed.
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&path);
        })
    }

    pub(super) fn uninstall() -> io::Result<()> {
        for label in labels() {
            // The daemon's agent is unloaded too (not loaded is fine); the tray's isn't: that
            // would end the tray that asks.
            if label == LABEL {
                let _ = run("launchctl", &["bootout", &format!("{}/{label}", domain())]);
            }
            crate::paths::remove_if_exists(&plist(&label)?)?;
        }
        Ok(())
    }

    /// Through launchd only when it starts the daemon (not the tray).
    pub(super) fn start() -> io::Result<bool> {
        if !plist(LABEL)?.exists() {
            return Ok(false);
        }
        run(
            "launchctl",
            &["kickstart", &format!("{}/{LABEL}", domain())],
        )?;
        Ok(true)
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
mod os {
    use super::*;

    /// Where the user's systemd looks by default (its own `XDG_CONFIG_HOME`, not the
    /// shell's, decides; it is rarely set).
    fn unit() -> io::Result<PathBuf> {
        Ok(home()?.join(".config/systemd/user").join(UNIT))
    }

    fn systemctl(args: &[&str]) -> io::Result<()> {
        let mut all = vec!["--user"];
        all.extend_from_slice(args);
        run("systemctl", &all).map_err(|e| {
            io::Error::other(format!(
                "{e} (autostart needs a systemd user session; without one, start hbsyncd \
                 from your session's startup programs)"
            ))
        })
    }

    /// The session's autostart folder (`$XDG_CONFIG_HOME/autostart`).
    fn desktop() -> io::Result<PathBuf> {
        let config = crate::paths::xdg_dir("XDG_CONFIG_HOME", ".config")
            .ok_or_else(|| io::Error::other("HOME isn't set"))?;
        Ok(config.join("autostart").join(DESKTOP_ENTRY))
    }

    pub(super) fn installed() -> io::Result<bool> {
        Ok(unit()?.exists() || desktop()?.exists())
    }

    pub(super) fn install(program: &Program) -> io::Result<()> {
        uninstall()?;
        let daemon = match program {
            Program::Tray(tray) => {
                let entry = desktop_entry(tray, &passed_env());
                return hbsync_fileio::write_if_changed(&desktop()?, entry.as_bytes()).map(drop);
            }
            Program::Daemon(daemon) => daemon,
        };
        let path = unit()?;
        hbsync_fileio::write_if_changed(&path, systemd_unit(daemon, &passed_env()).as_bytes())?;
        systemctl(&["daemon-reload"])
            .and_then(|()| systemctl(&["enable", UNIT]))
            // A unit systemd didn't take mustn't look installed.
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&path);
                let _ = systemctl(&["daemon-reload"]);
            })
    }

    pub(super) fn uninstall() -> io::Result<()> {
        crate::paths::remove_if_exists(&desktop()?)?;
        let path = unit()?;
        if !path.exists() {
            return Ok(());
        }
        let _ = systemctl(&["disable", UNIT]);
        std::fs::remove_file(&path)?;
        systemctl(&["daemon-reload"])
    }

    pub(super) fn add_launcher(icon_png: &[u8]) -> io::Result<()> {
        let data = crate::paths::xdg_dir("XDG_DATA_HOME", ".local/share")
            .ok_or_else(|| io::Error::other("HOME isn't set"))?;
        let packaged = crate::install::Install::current().lists_itself();
        write_launcher(&data, &desktop()?, &program_of(TRAY)?, icon_png, packaged)
    }

    /// Through systemd only when it starts the daemon (not the tray).
    pub(super) fn start() -> io::Result<bool> {
        if !unit()?.exists() {
            return Ok(false);
        }
        systemctl(&["start", UNIT])?;
        Ok(true)
    }
}

#[cfg(windows)]
mod os {
    use super::*;
    use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS};
    use windows_sys::Win32::System::Registry::{
        HKEY_CURRENT_USER, REG_SZ, RRF_RT_REG_SZ, RegDeleteKeyValueW, RegGetValueW, RegSetKeyValueW,
    };

    const RUN_KEY: &str = r"Software\Microsoft\Windows\CurrentVersion\Run";

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(Some(0)).collect()
    }

    fn check(code: u32) -> io::Result<()> {
        if code == ERROR_SUCCESS {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(code as i32))
        }
    }

    /// In the MSIX package, its startup task; else the Run key.
    fn msix() -> bool {
        matches!(
            crate::install::Install::current(),
            crate::install::Install::Msix { .. }
        )
    }

    pub(super) fn installed() -> io::Result<bool> {
        // Unreadable, it counts as off: callers that only consult it (starting the daemon,
        // logging out) mustn't fail on it.
        if msix() {
            return Ok(startup_task::enabled().unwrap_or_else(|e| {
                tracing::warn!("the startup task: {e}");
                false
            }));
        }
        let (key, value) = (wide(RUN_KEY), wide(RUN_VALUE));
        let mut size = 0u32;
        // SAFETY: valid NUL-terminated strings; a null buffer only asks for the size.
        let code = unsafe {
            RegGetValueW(
                HKEY_CURRENT_USER,
                key.as_ptr(),
                value.as_ptr(),
                RRF_RT_REG_SZ,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut size,
            )
        };
        match code {
            ERROR_FILE_NOT_FOUND => Ok(false),
            code => check(code).map(|()| true),
        }
    }

    pub(super) fn install(program: &Program) -> io::Result<()> {
        if msix() {
            return startup_task::enable();
        }
        let (key, value, data) = (
            wide(RUN_KEY),
            wide(RUN_VALUE),
            wide(&run_value(program.path())),
        );
        // SAFETY: valid NUL-terminated strings; the data's size includes its terminator.
        check(unsafe {
            RegSetKeyValueW(
                HKEY_CURRENT_USER,
                key.as_ptr(),
                value.as_ptr(),
                REG_SZ,
                data.as_ptr().cast(),
                (data.len() * 2) as u32,
            )
        })
    }

    pub(super) fn uninstall() -> io::Result<()> {
        if msix() {
            // Also a Run value an earlier, unpackaged copy left (it would start that one too).
            // The package sees the registry through its own layer: try, but don't depend on it.
            if let Err(e) = uninstall_run_value() {
                tracing::debug!("the Run value: {e}");
            }
            return startup_task::disable();
        }
        uninstall_run_value()
    }

    fn uninstall_run_value() -> io::Result<()> {
        let (key, value) = (wide(RUN_KEY), wide(RUN_VALUE));
        // SAFETY: valid NUL-terminated strings.
        match unsafe { RegDeleteKeyValueW(HKEY_CURRENT_USER, key.as_ptr(), value.as_ptr()) } {
            ERROR_FILE_NOT_FOUND => Ok(()),
            code => check(code),
        }
    }

    /// The Run key only starts programs at logon: the caller starts the daemon itself.
    pub(super) fn start() -> io::Result<bool> {
        Ok(false)
    }

    /// The MSIX package's startup task (Conveyor's `start-on-login`), which starts the tray.
    mod startup_task {
        use crate::install::MSIX_STARTUP_TASK;
        use std::io;
        use windows::ApplicationModel::{StartupTask, StartupTaskState};
        use windows::core::HSTRING;

        fn err(e: windows::core::Error) -> io::Error {
            io::Error::other(e.message())
        }

        fn task() -> io::Result<StartupTask> {
            StartupTask::GetAsync(&HSTRING::from(MSIX_STARTUP_TASK))
                .and_then(|op| op.join())
                .map_err(err)
        }

        fn on(state: StartupTaskState) -> bool {
            state == StartupTaskState::Enabled || state == StartupTaskState::EnabledByPolicy
        }

        pub(super) fn enabled() -> io::Result<bool> {
            task()?.State().map(on).map_err(err)
        }

        pub(super) fn enable() -> io::Result<()> {
            let state = task()?
                .RequestEnableAsync()
                .and_then(|op| op.join())
                .map_err(err)?;
            match state {
                s if on(s) => Ok(()),
                StartupTaskState::DisabledByUser => Err(io::Error::other(
                    "it was turned off in Task Manager (Startup apps): turn it on there",
                )),
                StartupTaskState::DisabledByPolicy => Err(io::Error::other(
                    "your organization's policy doesn't allow it",
                )),
                _ => Err(io::Error::other("Windows didn't turn it on")),
            }
        }

        pub(super) fn disable() -> io::Result<()> {
            task()?.Disable().map_err(err)
        }
    }
}

/// Whether Filesync starts at logon.
pub fn installed() -> io::Result<bool> {
    os::installed()
}

/// Starts `program` ([`logon_program`]) at every logon from now on, replacing an earlier
/// entry.
pub fn install(program: &Program) -> io::Result<()> {
    os::install(program)
}

/// No more starting at logon. Doesn't stop what runs.
pub fn uninstall() -> io::Result<()> {
    os::uninstall()
}

/// Starts the daemon through the service manager, so that it supervises it; false where
/// there is none to ask (Windows, or the tray starts at logon), and the caller starts it.
pub fn start() -> io::Result<bool> {
    os::start()
}

/// Lists the tray among the desktop's applications (Linux; elsewhere the installer will): its
/// desktop entry in `$XDG_DATA_HOME/applications` and its icon in the icon theme. Started from
/// there or at logon, the session sees it as its own app (in System Monitor, say), not as part
/// of the terminal that started it. Installed from a package, which lists it itself, a launcher
/// written earlier goes. An autostart entry is brought up to date with it either way (a moved
/// or updated tray). Written only when it changed.
#[cfg(target_os = "linux")]
pub fn add_launcher(icon_png: &[u8]) -> io::Result<()> {
    os::add_launcher(icon_png)
}

#[cfg(target_os = "linux")]
fn write_launcher(
    data: &Path,
    autostart: &Path,
    tray: &Path,
    icon_png: &[u8],
    packaged: bool,
) -> io::Result<()> {
    let write = |path: &Path, bytes: &[u8]| hbsync_fileio::write_if_changed(path, bytes).map(drop);
    let entry = desktop_entry(tray, &passed_env());
    let icon = data
        .join("icons/hicolor/256x256/apps")
        .join(format!("{ICON}.png"));
    let launcher = data.join("applications").join(DESKTOP_ENTRY);
    if packaged {
        // The package lists it (and has the icon): a copy of ours would show it twice.
        crate::paths::remove_if_exists(&launcher)?;
        crate::paths::remove_if_exists(&icon)?;
    } else {
        write(&icon, icon_png)?;
        write(&launcher, entry.as_bytes())?;
    }
    if autostart.exists() {
        write(autostart, entry.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The packages (packaging/conveyor.conf) name the app, the tray and its icon as the code
    /// does: the bundle ID and launchd labels, the program the packages start, the icon our
    /// desktop entries name.
    #[test]
    fn the_packages_say_what_the_code_says() {
        let conf = include_str!("../../../packaging/conveyor.conf");
        for line in [
            format!("rdns-name = \"{LABEL}\""),
            format!("fsname = \"{TRAY}\""),
            format!("long-fsname = \"{ICON}\""),
            format!("main-binary = bin/{TRAY}"),
        ] {
            assert!(conf.contains(&line), "conveyor.conf lacks {line}");
        }
    }

    fn env() -> Vec<(String, String)> {
        vec![("HBSYNC_HOME".into(), "/tmp/a b & <c> 100% $x".into())]
    }

    #[test]
    fn the_launchd_agent_restarts_after_a_crash_only() {
        let plist = launchd_plist(
            &Program::Daemon("/Applications/Filesync/hbsyncd".into()),
            &[],
        );
        assert!(plist.contains("<string>id.homebase.filesync</string>"));
        assert!(plist.contains("<string>/Applications/Filesync/hbsyncd</string>"));
        assert!(plist.contains("<key>SuccessfulExit</key>\n    <false/>"));
        assert!(!plist.contains("EnvironmentVariables"));
        let plist = launchd_plist(&Program::Daemon("/x/hbsyncd".into()), &env());
        assert!(plist.contains(
            "<key>HBSYNC_HOME</key>\n    <string>/tmp/a b &amp; &lt;c&gt; 100% $x</string>"
        ));
    }

    #[test]
    fn the_systemd_unit_quotes_what_it_runs() {
        let unit = systemd_unit(Path::new("/home/a b/$bin/hbsyncd"), &env());
        assert!(
            unit.contains("ExecStart=\"/home/a b/$$bin/hbsyncd\"\n"),
            "{unit}"
        );
        assert!(
            unit.contains("Environment=\"HBSYNC_HOME=/tmp/a b & <c> 100%% $x\"\n"),
            "{unit}"
        );
        assert!(unit.contains("Restart=on-failure\n"));
        assert!(unit.contains("WantedBy=default.target\n"));
    }

    #[test]
    fn the_tray_starts_with_the_session() {
        let tray = Path::new("/opt/Filesync/homebase-filesync");
        let entry = desktop_entry(tray, &env());
        assert!(
            entry.contains(
                "Exec=env \"HBSYNC_HOME=/tmp/a b & <c> 100%% \\$x\" \"/opt/Filesync/homebase-filesync\"\n"
            ),
            "{entry}"
        );
        assert!(desktop_entry(tray, &[]).contains("Exec=\"/opt/Filesync/homebase-filesync\"\n"));
        assert!(entry.contains("Icon=homebase-filesync\n"));
        let plist = launchd_plist(&Program::Tray(tray.into()), &[]);
        assert!(plist.contains("<string>id.homebase.filesync.tray</string>"));
        assert!(plist.contains("<string>Interactive</string>"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_launcher_is_written_once() -> io::Result<()> {
        let data = tempfile::tempdir()?;
        let autostart = data.path().join("autostart.desktop");
        let tray = Path::new("/opt/Filesync/homebase-filesync");
        write_launcher(data.path(), &autostart, tray, b"png", false)?;
        assert!(
            !autostart.exists(),
            "starting at logon stays the user's choice"
        );
        let entry = data
            .path()
            .join("applications/id.homebase.filesync.desktop");
        let icon = data
            .path()
            .join("icons/hicolor/256x256/apps/homebase-filesync.png");
        assert!(std::fs::read_to_string(&entry)?.contains("Name=Homebase Filesync\n"));
        assert_eq!(std::fs::read(&icon)?, b"png");
        let written = std::fs::metadata(&entry)?.modified()?;
        std::thread::sleep(std::time::Duration::from_millis(20));
        write_launcher(data.path(), &autostart, tray, b"png", false)?;
        assert_eq!(
            std::fs::metadata(&entry)?.modified()?,
            written,
            "unchanged, not rewritten"
        );

        // An autostart entry from an older tray is brought up to date.
        std::fs::write(&autostart, "[Desktop Entry]\nExec=/old/homebase-filesync\n")?;
        write_launcher(data.path(), &autostart, tray, b"png", false)?;
        assert_eq!(std::fs::read(&autostart)?, std::fs::read(&entry)?);

        // Installed from a package: our launcher goes, the autostart entry follows the tray.
        let packaged = Path::new("/usr/lib/homebase/homebase-filesync/bin/homebase-filesync");
        write_launcher(data.path(), &autostart, packaged, b"png", true)?;
        assert!(!entry.exists() && !icon.exists());
        assert!(std::fs::read_to_string(&autostart)?.contains(&packaged.display().to_string()));
        Ok(())
    }

    #[test]
    fn the_run_value_is_the_quoted_path() {
        assert_eq!(
            run_value(Path::new(r"C:\Program Files\Filesync\hbsyncd.exe")),
            r#""C:\Program Files\Filesync\hbsyncd.exe""#
        );
    }

    /// Writes the user's real Run key, so only on CI runners.
    #[cfg(windows)]
    #[test]
    fn the_run_key_round_trips_on_ci() -> io::Result<()> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        install(&Program::Daemon(r"C:\hbsync-test\hbsyncd.exe".into()))?;
        assert!(installed()?);
        uninstall()?;
        assert!(!installed()?);
        uninstall()
    }
}
