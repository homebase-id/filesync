//! Noticing that the machine slept, on a thread of its own, for the whole daemon: the accounts
//! then have their notifications reconnect and their folders sync at once.
//!
//! - Linux: told by the kernel. A timer on the wall clock that is cancelled when the clock is
//!   set, which a resume counts as (`TFD_TIMER_CANCEL_ON_SET`): no wake-up until then. It was
//!   a sleep when the time spent suspended (`CLOCK_BOOTTIME` ahead of `CLOCK_MONOTONIC`) grew,
//!   not a mere clock set.
//! - Elsewhere (and if that fails): a slow poll. A wait of [`LOOK`] that took far longer by the
//!   wall clock means the machine slept (the waits leave out the sleep, or end after it); a
//!   clock set forward only costs a sync. Push notifications of the OS (Windows'
//!   `PowerRegisterSuspendResumeNotification`, macOS's `IORegisterForSystemPower`) are a
//!   follow-up.

use std::time::{Duration, SystemTime};

/// How often the poll looks.
const LOOK: Duration = Duration::from_secs(30);
/// A wait this much longer by the wall clock than asked means the machine slept.
const SLEPT: Duration = Duration::from_secs(30);

/// Calls `woke` each time the machine woke up, until it returns false. Blocks.
pub(super) fn watch(woke: impl Fn() -> bool) {
    #[cfg(target_os = "linux")]
    match linux::watch(&woke) {
        Ok(()) => return,
        Err(e) => tracing::warn!("can't be told of sleep ({e}): looking every {LOOK:?}"),
    }
    loop {
        let before = SystemTime::now();
        std::thread::sleep(LOOK);
        if slept(LOOK, SystemTime::now().duration_since(before).ok()) && !woke() {
            return;
        }
    }
}

/// Whether a wait of `asked` that took `wall` by the wall clock (`None`: the clock was set
/// back) spanned a sleep.
fn slept(asked: Duration, wall: Option<Duration>) -> bool {
    wall.is_some_and(|wall| wall > asked + SLEPT)
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::SLEPT;

    /// `clock`'s time.
    fn time(clock: libc::clockid_t) -> Duration {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: writes into `t`, which outlives the call.
        unsafe { libc::clock_gettime(clock, &mut t) };
        Duration::new(t.tv_sec as u64, t.tv_nsec as u32)
    }

    /// How long the machine has been suspended since it booted.
    fn suspended() -> Duration {
        time(libc::CLOCK_BOOTTIME).saturating_sub(time(libc::CLOCK_MONOTONIC))
    }

    /// Calls `woke` when the machine resumed from a sleep (the wall clock was set, and the time
    /// suspended grew), until it returns false.
    pub(super) fn watch(woke: &impl Fn() -> bool) -> io::Result<()> {
        // SAFETY: timerfd_create has no preconditions; the descriptor is checked, then owned.
        let fd = unsafe { libc::timerfd_create(libc::CLOCK_REALTIME, libc::TFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a valid descriptor nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let mut suspended_before = suspended();
        loop {
            // A year ahead: it never goes off, it is only cancelled.
            let at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                + Duration::from_secs(365 * 24 * 3600);
            let spec = libc::itimerspec {
                it_interval: libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: libc::timespec {
                    tv_sec: at.as_secs() as libc::time_t,
                    tv_nsec: 0,
                },
            };
            let flags = libc::TFD_TIMER_ABSTIME | libc::TFD_TIMER_CANCEL_ON_SET;
            // SAFETY: the descriptor is open; `spec` outlives the call; no old value asked.
            if unsafe { libc::timerfd_settime(fd.as_raw_fd(), flags, &spec, std::ptr::null_mut()) }
                < 0
            {
                return Err(io::Error::last_os_error());
            }
            // Armed: a resume from now on cancels it. Looked at after arming, so a sleep just
            // before (since the last look) counts too.
            let now = suspended();
            if now > suspended_before + SLEPT && !woke() {
                return Ok(());
            }
            suspended_before = now;
            let mut expirations = [0u8; 8];
            // SAFETY: reads at most the buffer's length into it.
            let read = unsafe {
                libc::read(
                    fd.as_raw_fd(),
                    expirations.as_mut_ptr().cast(),
                    expirations.len(),
                )
            };
            if read >= 0 {
                // A year went by: arm it again.
                continue;
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                // The clock was set (a resume, or a mere clock set): looked at as it is armed
                // again.
                Some(libc::ECANCELED | libc::EINTR) => {}
                _ => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_far_longer_by_the_wall_clock_spanned_a_sleep() {
        let s = Duration::from_secs;
        assert!(!slept(LOOK, Some(LOOK)));
        assert!(!slept(LOOK, Some(LOOK + s(5))), "a busy machine");
        assert!(slept(LOOK, Some(s(3600))));
        assert!(!slept(LOOK, None), "the clock set back");
    }

    /// The kernel's timer is armed, and waits without waking (it is left waiting).
    #[cfg(target_os = "linux")]
    #[test]
    fn the_kernel_timer_waits() {
        let waiting = std::thread::spawn(|| linux::watch(&|| false));
        std::thread::sleep(Duration::from_millis(300));
        assert!(!waiting.is_finished(), "{:?}", waiting.join());
    }
}
