//! Small tools the crate's threads share: a bell to wait on ([`Wakeup`]), how long a loop
//! waits ([`wait_for`], [`WAIT_AT_MOST`]), a lock that outlives a panic ([`lock`]), and a
//! function run on drop ([`OnDrop`]).

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// The longest any of the daemon's loops waits for an event: a bound on what a lost wake-up
/// could hold back.
pub(crate) const WAIT_AT_MOST: Duration = Duration::from_secs(600);

/// How long to wait from `now` for the earliest of `deadlines`, [`WAIT_AT_MOST`] at most.
pub(crate) fn wait_for(now: Instant, deadlines: impl IntoIterator<Item = Instant>) -> Duration {
    deadlines
        .into_iter()
        .fold(now + WAIT_AT_MOST, Instant::min)
        .saturating_duration_since(now)
}

/// Calls its function when dropped, also in a panic's unwinding: a thread's end told for sure.
pub(crate) struct OnDrop<F: FnOnce()>(pub Option<F>);

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// Raised when there is something to look at: whoever waits on it wakes.
#[derive(Debug, Default)]
pub(crate) struct Wakeup {
    rung: Mutex<u64>,
    rings: Condvar,
}

impl Wakeup {
    pub(crate) fn raise(&self) {
        *lock(&self.rung) += 1;
        self.rings.notify_all();
    }

    /// Waits until it was raised after `seen` (what this returned last; 0 at first), or
    /// `timeout` passed. Returns how often it has been raised.
    pub(crate) fn wait(&self, seen: u64, timeout: Duration) -> u64 {
        let rung = lock(&self.rung);
        let (rung, _) = self
            .rings
            .wait_timeout_while(rung, timeout, |rung| *rung == seen)
            .unwrap_or_else(|e| e.into_inner());
        *rung
    }
}

/// The lock, also after a panic elsewhere (the data is plain values).
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
