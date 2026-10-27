//! Children let go: the daemon a program started beside itself, the openers. On Unix an exited
//! child stays a zombie until its parent waits for it, and the tray runs for days.

#[cfg(unix)]
use std::sync::Mutex;

/// The children being waited for, by pid: handed on when the tray restarts into its update
/// (an exec keeps them its children but ends the threads waiting for them).
#[cfg(unix)]
static WAITING: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// Lets `child` go on its own: on Unix a thread waits for it. The thread sleeps in `wait` (no
/// wakeups) on a small stack.
pub fn reap(child: std::process::Child) {
    #[cfg(unix)]
    {
        let pid = child.id();
        let mut child = child;
        wait_aside(pid, move || drop(child.wait()));
    }
    #[cfg(not(unix))]
    drop(child);
}

/// The children [`reap`] still waits for.
#[cfg(unix)]
pub fn waiting() -> Vec<u32> {
    WAITING.lock().map(|w| w.clone()).unwrap_or_default()
}

/// Waits for children this process has from before an exec ([`waiting`] then).
#[cfg(unix)]
pub fn adopt(pids: impl IntoIterator<Item = u32>) {
    for pid in pids {
        wait_aside(pid, move || {
            let Ok(raw) = libc::pid_t::try_from(pid) else {
                return;
            };
            let mut status = 0;
            // SAFETY: waitpid only writes the status; a pid that isn't our child is ECHILD.
            while unsafe { libc::waitpid(raw, &mut status, 0) } < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
            }
        });
    }
}

#[cfg(unix)]
fn wait_aside(pid: u32, wait: impl FnOnce() + Send + 'static) {
    let mark = move |add: bool| {
        if let Ok(mut w) = WAITING.lock() {
            if add {
                w.push(pid);
            } else {
                w.retain(|&p| p != pid);
            }
        }
    };
    mark(true);
    let waiter = std::thread::Builder::new()
        .name("reap".into())
        .stack_size(64 * 1024)
        .spawn(move || {
            wait();
            mark(false);
        });
    if let Err(e) = waiter {
        mark(false);
        tracing::warn!("can't wait for child process {pid}: it stays a zombie when it ends: {e}");
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
        until("no longer listed", || !waiting().contains(&pid));
        Ok(())
    }

    #[test]
    fn an_adopted_child_leaves_no_zombie() -> std::io::Result<()> {
        let child = std::process::Command::new("true").spawn()?;
        let pid = child.id();
        // As after an exec: a child of ours that nothing holds a `Child` for.
        std::mem::forget(child);
        until("the child ends", || state(pid) == Some('Z'));
        adopt([pid]);
        until("the zombie goes", || state(pid) != Some('Z'));
        Ok(())
    }
}
