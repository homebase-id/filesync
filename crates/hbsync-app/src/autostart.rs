//! Starting the daemon at logon, as this user (never as a service: it needs the user's
//! keychain and writes the user's files):
//! - macOS: a launchd agent, `~/Library/LaunchAgents/cloud.homebase.filesync.plist`;
//! - Linux: a systemd user unit, `~/.config/systemd/user/hbsync.service`;
//! - Windows: the per-user `Run` key (`HKCU\Software\Microsoft\Windows\CurrentVersion\Run`),
//!   which needs no administrator, unlike a logon task.
//!
//! launchd and systemd restart the daemon if it crashes, not when it was stopped. The daemon's
//! environment overrides (`HBSYNC_HOME`, `HBSYNC_CREDENTIALS`) are passed on where the manager
//! can (not on Windows).

use std::io;
use std::path::{Path, PathBuf};

/// The launchd label.
pub const LABEL: &str = "cloud.homebase.filesync";
/// The systemd unit.
pub const UNIT: &str = "hbsync.service";
/// The `Run` value's name.
pub const RUN_VALUE: &str = "Homebase Filesync";

/// The environment variables passed on to the daemon, as they are set now.
pub fn passed_env() -> Vec<(String, String)> {
    ["HBSYNC_HOME", "HBSYNC_CREDENTIALS"]
        .into_iter()
        .filter_map(|k| Some((k.to_owned(), std::env::var(k).ok()?)))
        .collect()
}

/// The launchd agent for `daemon`.
pub fn launchd_plist(daemon: &Path, env: &[(String, String)]) -> String {
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
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
  <string>{LABEL}</string>
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
  <string>Background</string>
{env_xml}</dict>
</plist>
"#,
        esc(&daemon.display().to_string())
    )
}

/// The systemd user unit for `daemon`.
pub fn systemd_unit(daemon: &Path, env: &[(String, String)]) -> String {
    // systemd's quoting: double quotes, with backslashes and quotes escaped, and its
    // specifiers (`%`) and variables (`$`) doubled.
    let quote = |s: &str| {
        let s = s
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$");
        format!("\"{s}\"")
    };
    let mut env_lines = String::new();
    for (k, v) in env {
        env_lines.push_str(&format!("Environment={}\n", quote(&format!("{k}={v}"))));
    }
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
        quote(&daemon.display().to_string())
    )
}

/// The `Run` value for `daemon`: its path, quoted.
pub fn run_value(daemon: &Path) -> String {
    format!("\"{}\"", daemon.display())
}

/// Where the daemon is: beside the running program.
pub fn daemon_exe() -> io::Result<PathBuf> {
    Ok(std::env::current_exe()?.with_file_name(format!("hbsyncd{}", std::env::consts::EXE_SUFFIX)))
}

#[cfg(unix)]
fn home() -> io::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("HOME isn't set"))
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

    fn plist() -> io::Result<PathBuf> {
        Ok(home()?
            .join("Library/LaunchAgents")
            .join(format!("{LABEL}.plist")))
    }

    fn domain() -> String {
        // SAFETY: getuid has no preconditions and can't fail.
        format!("gui/{}", unsafe { libc::getuid() })
    }

    pub fn installed() -> io::Result<bool> {
        Ok(plist()?.exists())
    }

    pub fn install(daemon: &Path) -> io::Result<()> {
        let path = plist()?;
        if path.exists() {
            uninstall()?;
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&path, launchd_plist(daemon, &passed_env()))?;
        run(
            "launchctl",
            &["bootstrap", &domain(), &path.display().to_string()],
        )
    }

    pub fn uninstall() -> io::Result<()> {
        // Not loaded is fine.
        let _ = run("launchctl", &["bootout", &format!("{}/{LABEL}", domain())]);
        match std::fs::remove_file(plist()?) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    pub fn start() -> io::Result<bool> {
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

    fn unit() -> io::Result<PathBuf> {
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .map_or_else(|| home().map(|h| h.join(".config")), Ok)?;
        Ok(config.join("systemd/user").join(UNIT))
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

    pub fn installed() -> io::Result<bool> {
        Ok(unit()?.exists())
    }

    pub fn install(daemon: &Path) -> io::Result<()> {
        let path = unit()?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&path, systemd_unit(daemon, &passed_env()))?;
        systemctl(&["daemon-reload"])?;
        systemctl(&["enable", UNIT])
    }

    pub fn uninstall() -> io::Result<()> {
        let path = unit()?;
        if !path.exists() {
            return Ok(());
        }
        let _ = systemctl(&["disable", UNIT]);
        std::fs::remove_file(&path)?;
        systemctl(&["daemon-reload"])
    }

    pub fn start() -> io::Result<bool> {
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

    pub fn installed() -> io::Result<bool> {
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

    pub fn install(daemon: &Path) -> io::Result<()> {
        let (key, value, data) = (wide(RUN_KEY), wide(RUN_VALUE), wide(&run_value(daemon)));
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

    pub fn uninstall() -> io::Result<()> {
        let (key, value) = (wide(RUN_KEY), wide(RUN_VALUE));
        // SAFETY: valid NUL-terminated strings.
        match unsafe { RegDeleteKeyValueW(HKEY_CURRENT_USER, key.as_ptr(), value.as_ptr()) } {
            ERROR_FILE_NOT_FOUND => Ok(()),
            code => check(code),
        }
    }

    /// The Run key only starts programs at logon: the caller starts the daemon itself.
    pub fn start() -> io::Result<bool> {
        Ok(false)
    }
}

/// Whether the daemon starts at logon.
pub fn installed() -> io::Result<bool> {
    os::installed()
}

/// Starts `daemon` at every logon from now on (replacing an earlier entry).
pub fn install(daemon: &Path) -> io::Result<()> {
    os::install(daemon)
}

/// No more starting at logon. Doesn't stop a running daemon.
pub fn uninstall() -> io::Result<()> {
    os::uninstall()
}

/// Starts the daemon through the service manager, so that it supervises it; false where
/// there is none to ask (Windows), and the caller starts it.
pub fn start() -> io::Result<bool> {
    os::start()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> Vec<(String, String)> {
        vec![("HBSYNC_HOME".into(), "/tmp/a b & <c> 100%".into())]
    }

    #[test]
    fn the_launchd_agent_restarts_after_a_crash_only() {
        let plist = launchd_plist(Path::new("/Applications/Filesync/hbsyncd"), &[]);
        assert!(plist.contains("<string>cloud.homebase.filesync</string>"));
        assert!(plist.contains("<string>/Applications/Filesync/hbsyncd</string>"));
        assert!(plist.contains("<key>SuccessfulExit</key>\n    <false/>"));
        assert!(!plist.contains("EnvironmentVariables"));
        let plist = launchd_plist(Path::new("/x/hbsyncd"), &env());
        assert!(plist.contains(
            "<key>HBSYNC_HOME</key>\n    <string>/tmp/a b &amp; &lt;c&gt; 100%</string>"
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
            unit.contains("Environment=\"HBSYNC_HOME=/tmp/a b & <c> 100%%\"\n"),
            "{unit}"
        );
        assert!(unit.contains("Restart=on-failure\n"));
        assert!(unit.contains("WantedBy=default.target\n"));
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
        install(Path::new(r"C:\hbsync-test\hbsyncd.exe"))?;
        assert!(installed()?);
        uninstall()?;
        assert!(!installed()?);
        uninstall()
    }
}
