//! When a folder syncs: what asks for a sync (a change here or in its part of the drive, a
//! request, work a sync left unfinished; never time alone, daemon.md "No poll") and what holds
//! it back (a pause, the backoff after a failure); and when changes the drive announced are
//! pulled ([`Announced`]). Pure: the caller tells the time.

use std::time::{Duration, Instant};

use crate::protocol::{State, Until};

/// After a failure: wait this long, doubling up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// A sync announced by the drive waits until its changes have stopped coming this long...
const REMOTE_QUIET: Duration = Duration::from_millis(1000);
/// ...but no longer than this after the first.
const REMOTE_MAX_WAIT: Duration = Duration::from_secs(10);

/// How long to wait after each failure in a row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backoff(Duration);

impl Backoff {
    pub(crate) fn new() -> Backoff {
        Backoff(FIRST_BACKOFF)
    }

    /// The wait after a failure that left `state`. A refusal won't change until this device is
    /// authorized again: ask rarely.
    pub(crate) fn after(&mut self, state: State) -> Duration {
        let wait = if state == State::Denied {
            MAX_BACKOFF
        } else {
            self.0
        };
        self.0 = (self.0 * 2).min(MAX_BACKOFF);
        wait
    }
}

/// Changes the drive announced: pulled once they stop coming (a burst of deletes reaches the
/// mass-delete guard whole), or [`REMOTE_MAX_WAIT`] after the first.
#[derive(Debug, Default)]
pub(crate) struct Announced(Option<(Instant, Instant)>);

impl Announced {
    /// The drive announced a change.
    pub fn note(&mut self, now: Instant) {
        let first = self.0.map_or(now, |(first, _)| first);
        self.0 = Some((first, now + REMOTE_QUIET));
    }

    /// Whether announced changes are due to be pulled now: they are taken then.
    pub fn due(&mut self, now: Instant) -> bool {
        match self.0 {
            Some((first, quiet)) if now >= quiet || now >= first + REMOTE_MAX_WAIT => {
                self.0 = None;
                true
            }
            _ => false,
        }
    }
}

/// How a sync ended, for [`Schedule::synced`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Synced {
    /// Nothing left that another try could do.
    Done,
    /// Stopped by a pause or a stop: the rest waits for resume.
    Stopped,
    /// This many nodes left that another try may do (`SyncReport::unfinished`).
    Unfinished(u64),
}

#[derive(Debug)]
pub(crate) struct Schedule {
    /// How often what is local is looked at (`RunnerConfig::look`).
    look: Duration,
    next_look: Instant,
    /// Something asked for a sync since the last one.
    wanted: bool,
    /// No sync before then (a failure's backoff, a folder that is gone).
    not_before: Instant,
    /// Paused: until then (Unix seconds), or until resumed (`None`).
    pause: Option<Until>,
    backoff: Backoff,
    /// Work a sync left unfinished: tried again then, with how much was left. A timer of its
    /// own, with its own backoff, so a change meanwhile still syncs at once.
    retry: Option<(Instant, u64)>,
    retry_backoff: Backoff,
}

impl Schedule {
    /// A folder starting (paused, with `pause`): it syncs at once unless paused.
    pub fn new(now: Instant, look: Duration, pause: Option<Until>) -> Schedule {
        Schedule {
            look,
            next_look: now + look,
            wanted: true,
            not_before: now,
            pause,
            backoff: Backoff::new(),
            retry: None,
            retry_backoff: Backoff::new(),
        }
    }

    /// Something changed (here, or in this folder's part of the drive): a sync when allowed.
    pub fn want(&mut self) {
        self.wanted = true;
    }

    /// A sync asked for now (sync now, a confirmation): without waiting out a backoff.
    pub fn asked(&mut self, now: Instant) {
        self.wanted = true;
        self.not_before = now;
    }

    /// The machine woke up: sync now, the backoff forgotten.
    pub fn woke(&mut self, now: Instant) {
        self.asked(now);
        self.backoff = Backoff::new();
    }

    pub fn pause(&mut self, until: Until) {
        self.pause = Some(until);
    }

    pub fn paused(&self) -> Option<Until> {
        self.pause
    }

    /// Syncing goes on, at once. Whether it was paused.
    pub fn resume(&mut self, now: Instant) -> bool {
        self.asked(now);
        self.pause.take().is_some()
    }

    /// A pause until a time that has come (`unix`, Unix seconds) ends. Whether it did.
    pub fn pause_over(&mut self, now: Instant, unix: u64) -> bool {
        self.pause.flatten().is_some_and(|t| unix >= t) && self.resume(now)
    }

    /// Whether it is time to look at what is local again; the next look is a look later. It
    /// asks for no sync by itself.
    pub fn look_due(&mut self, now: Instant) -> bool {
        if now < self.next_look {
            return false;
        }
        self.next_look = now + self.look;
        true
    }

    /// A rescan of a folder without a watcher ended: a sync if it found a change.
    pub fn rescanned(&mut self, changed: bool) {
        if changed {
            self.want();
        }
    }

    /// Whether to sync now: wanted (or unfinished work's retry has come), not paused, not
    /// backing off.
    pub fn due(&self, now: Instant) -> bool {
        let retry = self.retry.is_some_and(|(at, _)| now >= at);
        self.pause.is_none() && (self.wanted || retry) && now >= self.not_before
    }

    /// Not now: tried again at the next look (the folder gone, rules that don't parse: both
    /// are local). Returns the wait.
    pub fn later(&mut self, now: Instant) -> Duration {
        self.not_before = self.next_look;
        self.next_look.saturating_duration_since(now)
    }

    /// A sync ended. Work it left unfinished is tried again after a backoff (5 s, doubling to
    /// 5 min, from the start again when less is left), so stuck work isn't a full sync every
    /// minute.
    pub fn synced(&mut self, now: Instant, outcome: Synced) {
        self.backoff = Backoff::new();
        match outcome {
            Synced::Stopped => {}
            Synced::Done => {
                self.wanted = false;
                self.retry = None;
                self.retry_backoff = Backoff::new();
            }
            Synced::Unfinished(left) => {
                self.wanted = false;
                if self.retry.is_none_or(|(_, before)| left < before) {
                    self.retry_backoff = Backoff::new();
                }
                let wait = self.retry_backoff.after(State::Failing);
                self.retry = Some((now + wait, left));
            }
        }
    }

    /// A sync failed, leaving `state`: tried again after a backoff. Returns the wait.
    pub fn failed(&mut self, now: Instant, state: State) -> Duration {
        let wait = self.backoff.after(state);
        self.not_before = now + wait;
        wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOOK: Duration = Duration::from_secs(60);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn it_syncs_at_start_then_when_something_changes() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        assert!(s.due(t0));
        s.synced(t0, Synced::Done);
        assert!(!s.due(t0 + ms(10)));
        s.want();
        assert!(s.due(t0 + ms(20)));
    }

    /// No poll: time alone asks for no sync, however long nothing happens.
    #[test]
    fn nothing_syncs_without_a_reason() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Done);
        assert!(!s.look_due(t0 + LOOK - ms(1)));
        assert!(s.look_due(t0 + LOOK));
        assert!(
            !s.look_due(t0 + LOOK + ms(1)),
            "the next look is a look later"
        );
        assert!(!s.due(t0 + LOOK * 100));
    }

    /// Work a sync left unfinished is tried again after a backoff that doubles while nothing
    /// gets done, starts over when less is left, and holds back nothing else.
    #[test]
    fn unfinished_work_backs_off_without_holding_back_changes() {
        let t0 = Instant::now();
        let s5 = Duration::from_secs(5);
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Unfinished(3));
        assert!(!s.due(t0 + s5 - ms(1)));
        assert!(s.due(t0 + s5));
        // Stuck: twice as long each time.
        s.synced(t0, Synced::Unfinished(3));
        assert!(!s.due(t0 + s5 * 2 - ms(1)));
        assert!(s.due(t0 + s5 * 2));
        // A change meanwhile syncs at once.
        s.want();
        assert!(s.due(t0 + ms(1)));
        // Less left: from the start again.
        s.synced(t0, Synced::Unfinished(2));
        assert!(s.due(t0 + s5));
        // Finished: nothing more.
        s.synced(t0, Synced::Done);
        assert!(!s.due(t0 + LOOK * 100));
    }

    /// Without a watcher, a rescan asks for a sync only when it found a change.
    #[test]
    fn a_rescan_syncs_only_what_it_found() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Done);
        s.rescanned(false);
        assert!(!s.due(t0 + ms(1)));
        s.rescanned(true);
        assert!(s.due(t0 + ms(1)));
    }

    /// A folder that is gone is looked for again at the next look.
    #[test]
    fn later_is_the_next_look() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        assert_eq!(s.later(t0 + ms(10)), LOOK - ms(10));
        assert!(!s.due(t0 + LOOK - ms(1)));
        assert!(s.due(t0 + LOOK));
    }

    /// Announcements are pulled once they stop for a second, or ten seconds after the first.
    #[test]
    fn announcements_wait_until_they_stop() {
        let t0 = Instant::now();
        let mut a = Announced::default();
        assert!(!a.due(t0), "nothing announced");
        a.note(t0);
        assert!(!a.due(t0 + ms(500)));
        a.note(t0 + ms(800));
        assert!(!a.due(t0 + ms(1500)), "still coming");
        assert!(a.due(t0 + ms(1800)), "quiet for a second");
        assert!(!a.due(t0 + ms(1900)), "taken");
        // A steady stream: pulled after ten seconds all the same.
        let t1 = t0 + Duration::from_secs(100);
        for i in 0..20 {
            a.note(t1 + ms(i * 500));
        }
        assert!(!a.due(t1 + ms(9_999)));
        assert!(a.due(t1 + ms(10_000)));
    }

    #[test]
    fn failures_back_off_doubling_and_waking_forgets_it() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        assert_eq!(s.failed(t0, State::Offline), Duration::from_secs(5));
        assert!(!s.due(t0 + Duration::from_secs(4)));
        assert!(s.due(t0 + Duration::from_secs(5)));
        assert_eq!(s.failed(t0, State::Offline), Duration::from_secs(10));
        assert_eq!(
            s.failed(t0, State::Denied),
            MAX_BACKOFF,
            "a refusal is asked rarely"
        );
        s.woke(t0);
        assert!(s.due(t0));
        assert_eq!(s.failed(t0, State::Failing), FIRST_BACKOFF);
        // A sync now doesn't wait out the backoff, nor forget it.
        s.asked(t0);
        assert!(s.due(t0));
        assert_eq!(s.failed(t0, State::Failing), Duration::from_secs(10));
    }

    #[test]
    fn a_pause_holds_syncs_until_resumed_or_over() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, Some(None));
        assert!(!s.due(t0), "started paused");
        s.want();
        assert!(!s.due(t0));
        assert!(s.resume(t0));
        assert!(s.due(t0));
        s.pause(Some(1_000));
        assert!(!s.pause_over(t0, 999));
        assert!(!s.due(t0));
        assert!(s.pause_over(t0, 1_000));
        assert_eq!(s.paused(), None);
        assert!(s.due(t0));
    }

    #[test]
    fn a_sync_stopped_by_a_pause_is_still_wanted() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Stopped);
        assert!(s.due(t0));
    }
}
