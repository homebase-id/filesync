//! Children let go: the daemon a program started beside itself, the openers. On Unix an exited
//! child stays a zombie until its parent waits for it, and the tray runs for days.

/// Lets `child` go on its own: on Unix a thread waits for it. The thread sleeps in `wait` (no
/// wakeups) on a small stack.
pub fn reap(child: std::process::Child) {
    #[cfg(unix)]
    {
        let mut child = child;
        wait_aside(move || drop(child.wait()));
    }
    #[cfg(not(unix))]
    drop(child);
}

/// Waits for the children this process has already: on Linux the tray restarts into its update
/// with an exec, which ends the threads waiting for the children it started (the daemon, an
/// opener) but keeps them its children. Before anything is started, so nothing else waits for
/// them.
#[cfg(target_os = "linux")]
pub fn adopt_children() {
    adopt(children());
}

/// This process's children, from `/proc/self/task/*/children`.
#[cfg(target_os = "linux")]
fn children() -> Vec<libc::pid_t> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| std::fs::read_to_string(task.path().join("children")).ok())
        .flat_map(|list| {
            list.split_whitespace()
                .filter_map(|pid| pid.parse().ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn adopt(pids: Vec<libc::pid_t>) {
    for pid in pids {
        wait_aside(move || {
            let mut status = 0;
            // SAFETY: waitpid only writes the status; a pid that isn't our child is ECHILD.
            while unsafe { libc::waitpid(pid, &mut status, 0) } < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
            }
        });
    }
}

#[cfg(unix)]
fn wait_aside(wait: impl FnOnce() + Send + 'static) {
    let waiter = std::thread::Builder::new()
        .name("reap".into())
        .stack_size(64 * 1024)
        .spawn(wait);
    if let Err(e) = waiter {
        tracing::warn!("can't wait for a child process: it stays a zombie when it ends: {e}");
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// The state of process `pid` (`Z` a zombie), or None when there is none.
    fn state(pid: u32) -> Option<char> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        stat.rsplit_once(')')?.1.trim_start().chars().next()
    }

    fn until(what: &str, done: impl Fn() -> bool) {
        let end = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < end, "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn a_child_let_go_leaves_no_zombie() -> std::io::Result<()> {
        let child = std::process::Command::new("true").spawn()?;
        let pid = child.id();
        until("the child ends", || state(pid) == Some('Z'));
        reap(child);
        // Once waited for, the pid may be someone else's, but never our zombie.
        until("the zombie goes", || state(pid) != Some('Z'));
        Ok(())
    }

    #[test]
    fn an_adopted_child_leaves_no_zombie() -> std::io::Result<()> {
        let child = std::process::Command::new("true").spawn()?;
        let pid = child.id();
        // As after an exec: a child of ours that nothing holds a `Child` for.
        std::mem::forget(child);
        until("the child ends", || state(pid) == Some('Z'));
        let raw = libc::pid_t::try_from(pid).map_err(std::io::Error::other)?;
        assert!(children().contains(&raw), "listed among the children");
        // Only this one: the other tests' children are theirs to wait for.
        adopt(vec![raw]);
        until("the zombie goes", || state(pid) != Some('Z'));
        Ok(())
    }
}
