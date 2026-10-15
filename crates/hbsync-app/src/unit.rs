//! Linux: the tray and the daemon run in a systemd unit of the app's own, named as systemd
//! names desktop apps (`app-id.homebase.filesync@<n>.service`), as when the desktop starts them
//! from its menu or at login. The desktop then shows them as Homebase Filesync (KDE's System
//! Monitor lists applications by unit; one started from a terminal counts as the terminal's),
//! and closing the terminal they were started from takes nothing down with it.

use std::ffi::OsString;
use std::path::Path;

use crate::autostart::LABEL;

/// Set in the units started here: what runs in one never starts another, should its cgroup
/// not show it.
const MARK: &str = "HBSYNC_UNIT";

/// Variables of the service manager's, not passed on to a unit.
const MANAGERS: &[&str] = &[
    MARK,
    "INVOCATION_ID",
    "JOURNAL_STREAM",
    "NOTIFY_SOCKET",
    "LISTEN_PID",
    "LISTEN_FDS",
    "LISTEN_FDNAMES",
    "MANAGERPID",
    "SYSTEMD_EXEC_PID",
];

/// Starts `program` with `args` again in a unit of the app's, unless this process runs in one
/// already (started from the menu, at login, or by this), is a private instance
/// (`HBSYNC_HOME`), or there is no systemd user session. True when it did: this process is
/// done.
pub fn relaunch(program: &Path, args: &[OsString]) -> bool {
    if crate::control::dev_home() || in_own_unit() {
        return false;
    }
    let mut run: Vec<OsString> = vec![
        "--user".into(),
        "--quiet".into(),
        "--collect".into(),
        // A service's random part follows an `@` (a scope's, a `-`): the app's ID is what
        // comes before it.
        format!("--unit=app-{LABEL}@{}.service", std::process::id()).into(),
        format!("--description={}", crate::APP_NAME).into(),
        // Started once the program runs; up while anything it started does (the daemon).
        "--property=Type=exec".into(),
        "--property=ExitType=cgroup".into(),
        format!("--setenv={MARK}=1").into(),
    ];
    // This environment (the user's service manager may have another session's, or none): by
    // name, systemd-run reads the values, which a command line would show to everyone.
    for (key, _) in std::env::vars_os() {
        if let Some(key) = key.to_str()
            && !MANAGERS.contains(&key)
        {
            run.push(format!("--setenv={key}").into());
        }
    }
    run.push("--".into());
    run.push(program.into());
    run.extend(args.iter().cloned());
    match crate::autostart::run("systemd-run", &run) {
        Ok(()) => true,
        Err(e) => {
            tracing::info!("not in a unit of its own: {e}");
            false
        }
    }
}

/// Whether this process runs in a unit of the app's.
fn in_own_unit() -> bool {
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
