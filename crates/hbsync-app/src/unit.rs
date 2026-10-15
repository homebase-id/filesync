//! Linux desktops: the tray and the daemon run in a systemd scope of the app's own, named as
//! systemd names desktop apps (`app-id.homebase.filesync-<random>.scope`), as when the desktop
//! starts them from its menu or at login. The desktop then shows them as Homebase Filesync
//! (KDE's System Monitor lists applications by unit; one started from a terminal counts as the
//! terminal's).
//!
//! The process is moved where it runs, as the desktop's launchers do: the user's service manager
//! makes a scope for it (`StartTransientUnit` with its pid). Nothing starts again; a move that
//! fails leaves it where it was.

use std::time::Duration;

use zbus::blocking::Proxy;
use zbus::zvariant::{OwnedObjectPath, Value};

use crate::autostart::LABEL;

/// How long the service manager may take to answer.
const ANSWER: Duration = Duration::from_secs(2);

/// Moves process `pid` into a new scope of the app's, unless it shouldn't: away from a desktop
/// (over SSH a scope would end with the user's service manager, at logout), when this process
/// runs in a unit of the app's already (from the menu, at login: what it starts is in it too),
/// or for a private instance (`HBSYNC_HOME`). A failure is logged, and the process stays.
pub fn move_to_scope(pid: u32) {
    if crate::control::dev_home() || !desktop() || in_own_unit() {
        return;
    }
    if let Err(e) = start_scope(pid) {
        tracing::info!("not in a scope of its own: {e}");
    }
}

fn start_scope(pid: u32) -> zbus::Result<()> {
    let bus = zbus::blocking::connection::Builder::session()?
        .method_timeout(ANSWER)
        .build()?;
    let manager = Proxy::new(
        &bus,
        "org.freedesktop.systemd1",
        "/org/freedesktop/systemd1",
        "org.freedesktop.systemd1.Manager",
    )?;
    // A scope's random part follows the last `-` (a service's, an `@`): the app's ID is what
    // comes before it.
    let name = format!("app-{LABEL}-{}.scope", uuid::Uuid::new_v4().simple());
    let properties: Vec<(&str, Value<'_>)> = vec![
        ("PIDs", Value::from(vec![pid])),
        ("Description", Value::from(crate::APP_NAME)),
        ("CollectMode", Value::from("inactive-or-failed")),
    ];
    let aux: Vec<(&str, Vec<(&str, Value<'_>)>)> = Vec::new();
    manager
        .call::<_, _, OwnedObjectPath>(
            "StartTransientUnit",
            &(name.as_str(), "fail", properties, aux),
        )
        .map(drop)
}

/// Whether this is a graphical session (not SSH, even with X forwarding).
fn desktop() -> bool {
    std::env::var("XDG_SESSION_TYPE").is_ok_and(|t| t == "wayland" || t == "x11")
}

/// Whether this process runs in a unit of the app's. Yes when that can't be read.
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
