//! Linux desktops: the tray and the daemon run in a systemd scope of the app's own, named as
//! systemd names desktop apps (`app-id.homebase.filesync-<random>.scope`), as when the desktop
//! starts them from its menu or at login. The desktop then shows them as Homebase Filesync
//! (KDE's System Monitor lists applications by unit; one started from a terminal counts as the
//! terminal's).

use std::path::Path;
use std::process::Command;

use crate::autostart::LABEL;

/// The command that starts `program` in a new scope of the app's: `systemd-run --user --scope`
/// runs it itself (the same process, with this environment). `None` where it should start as it
/// is: away from a desktop (a server, over SSH: a scope ends with the user's service manager,
/// at logout), in a unit of the app's already (from the menu, at login, or by this), for a
/// private instance (`HBSYNC_HOME`), or without a systemd user session.
pub fn command(program: &Path) -> Option<Command> {
    if crate::control::dev_home() || !desktop() || in_own_unit() || !user_manager() {
        return None;
    }
    let mut run = Command::new("systemd-run");
    run.args(["--user", "--scope", "--quiet", "--collect"])
        // A scope's random part follows the last `-` (a service's, an `@`): the app's ID is
        // what comes before it.
        .arg(format!(
            "--unit=app-{LABEL}-{}.scope",
            uuid::Uuid::new_v4().simple()
        ))
        .arg(format!("--description={}", crate::APP_NAME))
        .arg("--")
        .arg(program);
    Some(run)
}

/// Whether this session has a display.
fn desktop() -> bool {
    ["WAYLAND_DISPLAY", "DISPLAY"]
        .iter()
        .any(|k| std::env::var_os(k).is_some_and(|v| !v.is_empty()))
}

/// Whether the user's systemd instance runs (its private socket).
fn user_manager() -> bool {
    std::env::var_os("XDG_RUNTIME_DIR")
        .is_some_and(|dir| Path::new(&dir).join("systemd/private").exists())
}

/// Whether this process runs in a unit of the app's. Yes when that can't be read: a process
/// that can't tell never starts itself again (and again).
fn in_own_unit() -> bool {
    std::fs::read_to_string("/proc/self/cgroup").map_or(true, |c| ours(&c))
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
            "app-id.homebase.filesync-0b1c9e2d6f2a4a7e9c0d1e2f3a4b5c6d.scope",
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
