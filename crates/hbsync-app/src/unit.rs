//! Linux: the tray and the daemon run in a systemd unit of the app's own, named as systemd
//! names desktop apps (`app-id.homebase.filesync@<n>.service`), as when the desktop starts them
//! from its menu or at login. The desktop then shows them as Homebase Filesync (KDE's System
//! Monitor lists applications by unit; one started from a terminal counts as the terminal's),
//! and closing the terminal they were started from takes nothing down with it.

use std::ffi::OsString;
use std::path::Path;
use std::process::{Command, Stdio};

use crate::autostart::LABEL;

/// Set in the units started here: what runs in one doesn't start another (whatever its cgroup
/// says).
const MARK: &str = "HBSYNC_UNIT";

/// What the desktop session needs, passed on as it is set here (the user's service manager may
/// have another session's, or none), with what decides where Filesync keeps its files.
const SESSION_ENV: &[&str] = &[
    "DISPLAY",
    "WAYLAND_DISPLAY",
    "XAUTHORITY",
    "DBUS_SESSION_BUS_ADDRESS",
    "XDG_CURRENT_DESKTOP",
    "XDG_SESSION_DESKTOP",
    "XDG_SESSION_TYPE",
    "XDG_DATA_DIRS",
    "XDG_DATA_HOME",
    "PATH",
    "LANG",
    "LANGUAGE",
    "RUST_LOG",
];

/// Whether this process runs in a unit of the app's: started by the desktop (from its menu, at
/// login) or by [`start`].
pub fn in_own_unit() -> bool {
    std::env::var_os(MARK).is_some()
        || std::fs::read_to_string("/proc/self/cgroup").is_ok_and(|c| ours(&c))
}

/// Whether the unit of a `/proc/<pid>/cgroup` (the last part of its path) is the app's:
/// `app-id.homebase.filesync@autostart.service` (at login), `app-gnome-id.homebase.filesync-
/// 1234.scope` (GNOME's menu), and the like.
fn ours(cgroup: &str) -> bool {
    cgroup
        .lines()
        .filter_map(|line| line.rsplit('/').next())
        .any(|unit| unit.starts_with("app-") && unit.contains(LABEL))
}

/// Starts `program` with `args` in a new unit of the app's, through `systemd-run`, and returns
/// once it runs. False when it can't (no systemd user session): the caller starts it itself.
pub fn start(program: &Path, args: &[OsString]) -> bool {
    let mut run = Command::new("systemd-run");
    run.args(["--user", "--quiet", "--collect"])
        // A service's random part follows an `@` (a scope's, a `-`): the app's ID is what
        // comes before it.
        .arg(format!("--unit=app-{LABEL}@{}.service", std::process::id()))
        .arg("--description=Homebase Filesync")
        // Started once the program runs; up while anything it started does (the daemon).
        .args(["--property=Type=exec", "--property=ExitType=cgroup"])
        .arg(format!("--setenv={MARK}=1"));
    let passed = crate::autostart::passed_env();
    let session = SESSION_ENV
        .iter()
        .filter_map(|k| Some((k.to_string(), std::env::var(k).ok()?)));
    for (key, value) in passed.into_iter().chain(session) {
        run.arg(format!("--setenv={key}={value}"));
    }
    run.arg("--").arg(program).args(args);
    match run.stdin(Stdio::null()).stdout(Stdio::null()).output() {
        Ok(out) if out.status.success() => true,
        Ok(out) => {
            tracing::info!(
                "systemd-run: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            false
        }
        Err(e) => {
            tracing::info!("systemd-run: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_apps_units_are_told_from_others() {
        let user = "0::/user.slice/user-1000.slice/user@1000.service/app.slice";
        for unit in [
            "app-id.homebase.filesync@autostart.service",
            "app-gnome-id.homebase.filesync-4242.scope",
            "app-app\\x2did.homebase.filesync.slice/app-id.homebase.filesync@17.service",
        ] {
            assert!(ours(&format!("{user}/{unit}\n")), "{unit}");
        }
        for unit in [
            "app-org.kde.konsole-7677.scope/tab(10211).scope",
            "app-org.kde.konsole-7677.scope",
            "session-3.scope",
        ] {
            assert!(!ours(&format!("{user}/{unit}\n")), "{unit}");
        }
        // cgroup v1: a line per hierarchy.
        assert!(ours(
            "12:pids:/user.slice\n1:name=systemd:/user.slice/user@1000.service/app.slice/\
             app-id.homebase.filesync@autostart.service\n"
        ));
    }
}
