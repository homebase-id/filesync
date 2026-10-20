//! When a folder syncs: what asks for a sync (a change here or in its part of the drive, a
//! request, work a sync left unfinished; never time alone, daemon.md "No poll") and what holds
//! it back (a pause, the backoff after a failure). Pure: the caller tells the time.

use std::time::{Duration, Instant};

use hbsync_engine::policy::{Shown, policy};
use hbsync_remote::ErrorClass;

use crate::protocol::{Pause, State};

/// After a failure: wait this long, doubling up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// The longest wait a server's `Retry-After` gets.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

/// How long to wait after each failure in a row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backoff(Duration);

impl Backoff {
    pub(crate) fn new() -> Backoff {
        Backoff(FIRST_BACKOFF)
    }

    /// The wait after a failure of `class`: the longest at once where only the user changes
    /// what it found ([`Policy::rarely`](hbsync_engine::policy::Policy::rarely)), and not less
    /// than the server said (`retry_after`, up to [`MAX_RETRY_AFTER`]).
    pub(crate) fn after(&mut self, class: ErrorClass, retry_after: Option<Duration>) -> Duration {
        let wait = self.next();
        let wait = if policy(class).rarely {
            MAX_BACKOFF
        } else {
            wait
        };
        retry_after.map_or(wait, |asked| wait.max(asked.min(MAX_RETRY_AFTER)))
    }

    /// The next wait of the ladder.
    pub(crate) fn next(&mut self) -> Duration {
        let wait = self.0;
        self.0 = (self.0 * 2).min(MAX_BACKOFF);
        wait
    }
}

/// What a folder or an account shows while it fails with an error of `class`.
pub(crate) fn shown(class: ErrorClass) -> State {
    match policy(class).shown {
        Shown::Offline => State::Offline,
        Shown::Denied => State::Denied,
        Shown::Failing => State::Failing,
    }
}

/// How a sync ended, for [`Schedule::synced`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Synced {
    /// Nothing left that another try could do.
    Done,
    /// Stopped by a pause or a stop: the rest waits for resume.
    Stopped,
    /// Work left that another try may do (`SyncReport::unfinished`); `progressed`: this sync
    /// did some.
    Unfinished { progressed: bool },
}

#[derive(Debug)]
pub(crate) struct Schedule {
    /// How often what is local is looked at (`RunnerConfig::look`).
    look: Duration,
    next_look: Instant,
    /// Something asked for a sync since the last one.
    wanted: bool,
    /// The next sync reads the feed first, whatever the notifications say: it was asked for
    /// (sync now, a resume, a wake-up).
    read: bool,
    /// No sync before then (a failure's backoff, a folder that is gone).
    not_before: Instant,
    /// Paused, while it is.
    pause: Option<Pause>,
    backoff: Backoff,
    /// Work a sync left unfinished: tried again then. A timer of its own, with its own
    /// backoff, so a change meanwhile still syncs at once.
    retry: Option<Instant>,
    retry_backoff: Backoff,
}

impl Schedule {
    /// A folder starting (paused, with `pause`): it syncs at once unless paused.
    pub(crate) fn new(now: Instant, look: Duration, pause: Option<Pause>) -> Schedule {
        Schedule {
            look,
            next_look: now + look,
            wanted: true,
            read: false,
            not_before: now,
            pause,
            backoff: Backoff::new(),
            retry: None,
            retry_backoff: Backoff::new(),
        }
    }

    /// Something changed (here, or in this folder's part of the drive): a sync when allowed.
    pub(crate) fn want(&mut self) {
        self.wanted = true;
    }

    /// A sync asked for now (sync now, a confirmation, a resume, a wake-up): without waiting
    /// out a backoff, reading the feed first.
    pub(crate) fn asked(&mut self, now: Instant) {
        self.wanted = true;
        self.read = true;
        self.not_before = now;
    }

    /// Whether the sync starting now reads the feed first whatever the notifications say.
    /// Asked until a sync ended ([`Schedule::synced`]: it read first): a failed one asks again.
    pub(crate) fn read(&self) -> bool {
        self.read
    }

    /// The machine woke up: sync now, the backoff forgotten.
    pub(crate) fn woke(&mut self, now: Instant) {
        self.asked(now);
        self.backoff = Backoff::new();
    }

    /// The drive can be reached again (the notifications connected): a sync that waits, for a
    /// failure's backoff or to try unfinished work again, goes now, its backoff forgotten.
    /// One that waits for nothing doesn't.
    pub(crate) fn online(&mut self, now: Instant) {
        if self.retry.is_some() || now < self.not_before {
            self.woke(now);
            self.retry_backoff = Backoff::new();
        }
    }

    pub(crate) fn pause(&mut self, pause: Pause) {
        self.pause = Some(pause);
    }

    pub(crate) fn paused(&self) -> Option<Pause> {
        self.pause
    }

    /// Syncing goes on, at once. Whether it was paused.
    pub(crate) fn resume(&mut self, now: Instant) -> bool {
        self.asked(now);
        self.pause.take().is_some()
    }

    /// A pause that ran out at `unix` (Unix seconds) ends. Whether it did.
    pub(crate) fn pause_over(&mut self, now: Instant, unix: u64) -> bool {
        self.pause.is_some_and(|p| p.ran_out(unix)) && self.resume(now)
    }

    /// Whether it is time to look at what is local again; the next look is a look later. It
    /// asks for no sync by itself.
    pub(crate) fn look_due(&mut self, now: Instant) -> bool {
        if now < self.next_look {
            return false;
        }
        self.next_look = now + self.look;
        true
    }

    /// Whether to sync now: wanted (or unfinished work's retry has come), not paused, not
    /// backing off.
    pub(crate) fn due(&self, now: Instant) -> bool {
        let retry = self.retry.is_some_and(|at| now >= at);
        self.pause.is_none() && (self.wanted || retry) && now >= self.not_before
    }

    /// Not now: tried again at the next look (the folder gone, rules that don't parse: both
    /// are local). Returns the wait.
    pub(crate) fn later(&mut self, now: Instant) -> Duration {
        self.not_before = self.next_look;
        self.next_look.saturating_duration_since(now)
    }

    /// A sync ended. Work it left unfinished is tried again after a backoff (5 s, doubling to
    /// 5 min while the tries do nothing, from the start again after one that did some), so
    /// stuck work isn't a full sync every few seconds.
    pub(crate) fn synced(&mut self, now: Instant, outcome: Synced) {
        self.backoff = Backoff::new();
        self.read = false;
        match outcome {
            Synced::Stopped => {}
            Synced::Done => {
                self.wanted = false;
                self.retry = None;
                self.retry_backoff = Backoff::new();
            }
            Synced::Unfinished { progressed } => {
                self.wanted = false;
                if progressed {
                    self.retry_backoff = Backoff::new();
                }
                self.retry = Some(now + self.retry_backoff.next());
            }
        }
    }

    /// When time alone next changes what [`Schedule::due`], [`Schedule::look_due`] (when
    /// `looking`) or [`Schedule::pause_over`] say, `unix` being the Unix time at `now`: the
    /// runner waits for an event until then. `None`: only an event can.
    pub(crate) fn next_wake(&self, now: Instant, unix: u64, looking: bool) -> Option<Instant> {
        let look = looking.then_some(self.next_look);
        let (pause_end, sync) = match self.pause {
            Some(pause) => (pause.end_at(now, unix), None),
            None => {
                let wanted = self.wanted.then_some(now);
                let earliest = wanted.into_iter().chain(self.retry).min();
                (None, earliest.map(|at| at.max(self.not_before)))
            }
        };
        [look, pause_end, sync].into_iter().flatten().min()
    }

    /// A sync failed with an error of `class`: tried again after a backoff, and not before the
    /// server said (`retry_after`, up to an hour). Returns the wait.
    pub(crate) fn failed(
        &mut self,
        now: Instant,
        class: ErrorClass,
        retry_after: Option<Duration>,
    ) -> Duration {
        let wait = self.backoff.after(class, retry_after);
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

    /// Work a sync left unfinished is tried again after a backoff that doubles while the tries
    /// do nothing, starts over after one that did some, and holds back nothing else.
    #[test]
    fn unfinished_work_backs_off_without_holding_back_changes() {
        let t0 = Instant::now();
        let s5 = Duration::from_secs(5);
        let stuck = Synced::Unfinished { progressed: false };
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, stuck);
        assert!(!s.due(t0 + s5 - ms(1)));
        assert!(s.due(t0 + s5));
        // Stuck: twice as long each time.
        s.synced(t0, stuck);
        assert!(!s.due(t0 + s5 * 2 - ms(1)));
        assert!(s.due(t0 + s5 * 2));
        // A change meanwhile syncs at once.
        s.want();
        assert!(s.due(t0 + ms(1)));
        // A try that did some: from the start again.
        s.synced(t0, Synced::Unfinished { progressed: true });
        assert!(s.due(t0 + s5));
        // Finished: nothing more.
        s.synced(t0, Synced::Done);
        assert!(!s.due(t0 + LOOK * 100));
    }

    /// The drive reachable again: waiting work goes at once; a folder waiting for nothing
    /// doesn't sync.
    #[test]
    fn online_lets_waiting_work_go() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Done);
        s.online(t0);
        assert!(!s.due(t0), "nothing waits");
        s.synced(t0, Synced::Unfinished { progressed: false });
        s.online(t0 + ms(1));
        assert!(s.due(t0 + ms(1)), "unfinished work");
        s.synced(t0, Synced::Done);
        s.failed(t0, ErrorClass::Transient, None);
        s.online(t0 + ms(2));
        assert!(s.due(t0 + ms(2)), "a failure's backoff");
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

    #[test]
    fn failures_back_off_doubling_and_waking_forgets_it() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        assert_eq!(
            s.failed(t0, ErrorClass::Transient, None),
            Duration::from_secs(5)
        );
        assert!(!s.due(t0 + Duration::from_secs(4)));
        assert!(s.due(t0 + Duration::from_secs(5)));
        assert_eq!(
            s.failed(t0, ErrorClass::Transient, None),
            Duration::from_secs(10)
        );
        assert_eq!(
            s.failed(t0, ErrorClass::Denied, None),
            MAX_BACKOFF,
            "a refusal is asked rarely"
        );
        s.woke(t0);
        assert!(s.due(t0));
        assert_eq!(s.failed(t0, ErrorClass::Bug, None), FIRST_BACKOFF);
        // A sync now doesn't wait out the backoff, nor forget it.
        s.asked(t0);
        assert!(s.due(t0));
        assert_eq!(s.failed(t0, ErrorClass::Bug, None), Duration::from_secs(10));
    }

    #[test]
    fn a_pause_holds_syncs_until_resumed_or_over() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, Some(Pause::UNTIL_RESUMED));
        assert!(!s.due(t0), "started paused");
        s.want();
        assert!(!s.due(t0));
        assert!(s.resume(t0));
        assert!(s.due(t0));
        s.pause(Pause::ending_at(1_000));
        assert!(!s.pause_over(t0, 999));
        assert!(!s.due(t0));
        assert!(s.pause_over(t0, 1_000));
        assert_eq!(s.paused(), None);
        assert!(s.due(t0));
    }

    /// Idle, nothing wakes the runner but the look, and that only while it looks; whatever
    /// time alone would change is woken for, and nothing earlier.
    #[test]
    fn the_next_wake_is_the_first_thing_time_changes() {
        let t0 = Instant::now();
        let s5 = Duration::from_secs(5);
        let mut s = Schedule::new(t0, LOOK, None);
        assert_eq!(s.next_wake(t0, 0, true), Some(t0), "starting: at once");
        s.synced(t0, Synced::Done);
        assert_eq!(s.next_wake(t0, 0, true), Some(t0 + LOOK));
        assert_eq!(
            s.next_wake(t0, 0, false),
            None,
            "idle with a watcher: never"
        );
        for at in [t0 + ms(1), t0 + LOOK - ms(1)] {
            assert!(!s.due(at));
        }

        // A failure's backoff: a change waits for its end.
        let wait = s.failed(t0, ErrorClass::Transient, None);
        assert_eq!(s.next_wake(t0, 0, false), None, "nothing wanted");
        s.want();
        assert_eq!(s.next_wake(t0, 0, false), Some(t0 + wait));
        assert!(!s.due(t0 + wait - ms(1)));
        assert!(s.due(t0 + wait));

        // Unfinished work: its retry.
        s.synced(t0, Synced::Unfinished { progressed: false });
        assert_eq!(s.next_wake(t0, 0, false), Some(t0 + s5));
        assert!(s.due(t0 + s5));

        // A timed pause: its end, and no sync before.
        s.pause(Pause::ending_at(1_000));
        assert_eq!(
            s.next_wake(t0, 990, false),
            Some(t0 + Duration::from_secs(10))
        );
        assert!(!s.pause_over(t0 + Duration::from_secs(9), 999));
        assert!(s.pause_over(t0 + Duration::from_secs(10), 1_000));
        // So far ahead it has no instant: as if until resumed.
        s.pause(Pause::ending_at(u64::MAX));
        assert_eq!(s.next_wake(t0, 0, false), None);
        // Paused until resumed: only the look.
        s.pause(Pause::UNTIL_RESUMED);
        s.want();
        assert_eq!(s.next_wake(t0, 0, false), None);
        assert_eq!(s.next_wake(t0, 0, true), Some(t0 + LOOK));
    }

    #[test]
    fn a_sync_stopped_by_a_pause_is_still_wanted() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        s.synced(t0, Synced::Stopped);
        assert!(s.due(t0));
    }

    /// A server that says when to ask again isn't asked before then (up to an hour), however
    /// short the backoff.
    #[test]
    fn a_failure_waits_for_the_servers_retry_after() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        let asked = Duration::from_secs(600);
        assert_eq!(s.failed(t0, ErrorClass::RateLimited, Some(asked)), asked);
        assert!(!s.due(t0 + asked - ms(1)));
        assert!(s.due(t0 + asked));
        let day = Duration::from_secs(24 * 3600);
        assert_eq!(
            s.failed(t0, ErrorClass::RateLimited, Some(day)),
            MAX_RETRY_AFTER
        );
    }

    /// A sync that fails shows offline when the drive can't be reached, whatever reported it,
    /// and never for a fault of this computer's own.
    #[test]
    fn offline_is_the_drive_out_of_reach() {
        use hbsync_engine::EngineError;
        use hbsync_engine::exec::Stopped;
        use hbsync_engine::feed::FeedError;
        use hbsync_remote::RemoteError;
        let transient = || RemoteError::Transient("cut".into());
        let offline = [
            EngineError::Stopped(Stopped::Unreachable("cut".into())),
            EngineError::Feed(FeedError::Remote(transient())),
            EngineError::Remote(transient()),
        ];
        for e in offline {
            assert_eq!(shown(e.class()), State::Offline, "{e}");
        }
        let disk = EngineError::Feed(FeedError::Remote(RemoteError::Io(std::io::Error::other(
            "disk",
        ))));
        assert_eq!(shown(disk.class()), State::Failing);
        let denied = EngineError::Remote(RemoteError::Denied("401".into()));
        assert_eq!(shown(denied.class()), State::Denied);
    }

    /// A sync asked for reads the feed first; one that failed leaves that for the next try,
    /// one that ended doesn't.
    #[test]
    fn an_asked_read_outlives_a_failed_sync() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, None);
        assert!(!s.read(), "a start reads anyway");
        s.asked(t0);
        assert!(s.read());
        s.failed(t0, ErrorClass::Bug, None);
        assert!(s.read(), "the failed sync may not have read");
        s.synced(t0, Synced::Done);
        assert!(!s.read());
    }
}
