//! When a folder syncs: what asks for a sync (a change here or in its part of the drive, a
//! request, work a sync left unfinished; never time alone, daemon.md "No poll") and what holds
//! it back (a pause, the backoff after a failure). Pure: the caller tells the time.

use std::time::{Duration, Instant};

use hbsync_engine::policy::{Shown, policy};
use hbsync_remote::ErrorClass;

use crate::protocol::State;

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

/// What asked for a sync, told in its log line ([`Schedule::take_why`]), in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Why {
    /// The folder started.
    Start,
    /// A change here (the watcher's).
    Local,
    /// A change in this folder's part of the drive.
    Drive,
    /// A walk of the folder (no watcher, or one back) found changes.
    Scan,
    /// The ignore rules changed.
    Rules,
    /// Asked for: sync now, an answer to held deletes.
    Asked,
    Woke,
    /// The drive could be reached again.
    Online,
    Resumed,
    /// Work left unfinished, tried again (a sync's, a put back's).
    Retry,
}

impl Why {
    fn word(self) -> &'static str {
        match self {
            Why::Start => "start",
            Why::Local => "a change here",
            Why::Drive => "a change on the drive",
            Why::Scan => "a walk of the folder",
            Why::Rules => "new ignore rules",
            Why::Asked => "asked",
            Why::Woke => "a wake-up",
            Why::Online => "back online",
            Why::Resumed => "resumed",
            Why::Retry => "unfinished work",
        }
    }
}

/// What asked for a sync since the last one began: "a change here, a change on the drive".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Whys(std::collections::BTreeSet<Why>);

impl Whys {
    fn add(&mut self, why: Why) {
        self.0.insert(why);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Display for Whys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut words = self.0.iter().map(|w| w.word());
        match words.next() {
            None => f.write_str("nothing"),
            Some(first) => {
                f.write_str(first)?;
                words.try_for_each(|w| write!(f, ", {w}"))
            }
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
    /// Work left that another try may do (`SyncReport::unfinished`); `progressed`: this sync
    /// did some.
    Unfinished { progressed: bool },
}

#[derive(Debug)]
pub(crate) struct Schedule {
    /// How often what is local is looked at (`RunnerConfig::look`).
    look: Duration,
    next_look: Instant,
    /// What asked for a sync since the last one ended (none: no sync wanted).
    why: Whys,
    /// The next sync reads the feed first, whatever the notifications say: it was asked for
    /// (sync now, a resume, a wake-up).
    read: bool,
    /// No sync before then (a failure's backoff, a folder that is gone).
    not_before: Instant,
    /// Paused, until a resume (a timed pause too: the daemon's timer sends it).
    paused: bool,
    backoff: Backoff,
    /// Work a sync left unfinished: tried again then. A timer of its own, with its own
    /// backoff, so a change meanwhile still syncs at once.
    retry: Option<Instant>,
    retry_backoff: Backoff,
}

impl Schedule {
    /// A folder starting (paused or not): it syncs at once unless paused.
    pub(crate) fn new(now: Instant, look: Duration, paused: bool) -> Schedule {
        Schedule {
            look,
            next_look: now + look,
            why: Whys([Why::Start].into()),
            read: false,
            not_before: now,
            paused,
            backoff: Backoff::new(),
            retry: None,
            retry_backoff: Backoff::new(),
        }
    }

    /// Something changed (here, or in this folder's part of the drive): a sync when allowed.
    pub(crate) fn want(&mut self, why: Why) {
        self.why.add(why);
    }

    /// A sync asked for now (sync now, a confirmation, a resume, a wake-up): without waiting
    /// out a backoff, reading the feed first.
    pub(crate) fn asked(&mut self, now: Instant, why: Why) {
        self.want(why);
        self.read = true;
        self.not_before = now;
    }

    /// What asked for the sync beginning now (`now`): unfinished work's retry when nothing else
    /// did. Kept until a sync ends ([`Schedule::synced`]), so a failed one is asked for again.
    pub(crate) fn why(&self, now: Instant) -> Whys {
        let mut why = self.why.clone();
        if why.is_empty() && self.retry_due(now) {
            why.add(Why::Retry);
        }
        why
    }

    /// Whether unfinished work's retry has come.
    fn retry_due(&self, now: Instant) -> bool {
        self.retry.is_some_and(|at| now >= at)
    }

    /// Whether the sync starting now reads the feed first whatever the notifications say.
    /// Asked until a sync ended ([`Schedule::synced`]: it read first): a failed one asks again.
    pub(crate) fn read(&self) -> bool {
        self.read
    }

    /// The machine woke up: sync now, the backoff forgotten.
    pub(crate) fn woke(&mut self, now: Instant) {
        self.again(now, Why::Woke);
    }

    /// Sync now for `why`, the backoff forgotten.
    fn again(&mut self, now: Instant, why: Why) {
        self.asked(now, why);
        self.backoff = Backoff::new();
    }

    /// The drive can be reached again (the notifications connected): a sync that waits, for a
    /// failure's backoff or to try unfinished work again, goes now, its backoff forgotten.
    /// One that waits for nothing doesn't.
    pub(crate) fn online(&mut self, now: Instant) {
        if self.retry.is_some() || now < self.not_before {
            self.again(now, Why::Online);
            self.retry_backoff = Backoff::new();
        }
    }

    pub(crate) fn pause(&mut self) {
        self.paused = true;
    }

    pub(crate) fn paused(&self) -> bool {
        self.paused
    }

    /// Syncing goes on, at once. Whether it was paused.
    pub(crate) fn resume(&mut self, now: Instant) -> bool {
        self.asked(now, Why::Resumed);
        std::mem::take(&mut self.paused)
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
        !self.paused && (!self.why.is_empty() || self.retry_due(now)) && now >= self.not_before
    }

    /// Not now: tried again at the next look (the folder gone, rules that can't be read: both
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
                self.why = Whys::default();
                self.retry = None;
                self.retry_backoff = Backoff::new();
            }
            Synced::Unfinished { progressed } => {
                self.why = Whys::default();
                if progressed {
                    self.retry_backoff = Backoff::new();
                }
                self.retry = Some(now + self.retry_backoff.next());
            }
        }
    }

    /// When time alone next changes what [`Schedule::due`] or [`Schedule::look_due`] (when
    /// `looking`) say: the runner waits for an event until then. `None`: only an event can.
    pub(crate) fn next_wake(&self, now: Instant, looking: bool) -> Option<Instant> {
        let look = looking.then_some(self.next_look);
        let sync = match self.paused {
            true => None,
            false => {
                let wanted = (!self.why.is_empty()).then_some(now);
                let earliest = wanted.into_iter().chain(self.retry).min();
                earliest.map(|at| at.max(self.not_before))
            }
        };
        [look, sync].into_iter().flatten().min()
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

    /// A sync says what asked for it: the start, then what came since the sync before ended;
    /// unfinished work's retry when nothing else did. A sync that fails keeps them.
    #[test]
    fn a_sync_says_what_asked_for_it() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
        assert_eq!(s.why(t0).to_string(), "start");
        s.synced(t0, Synced::Unfinished { progressed: false });
        assert_eq!(s.why(t0).to_string(), "nothing", "told once");
        for why in [Why::Local, Why::Drive, Why::Local] {
            s.want(why);
        }
        assert_eq!(
            s.why(t0).to_string(),
            "a change here, a change on the drive"
        );
        s.failed(t0, ErrorClass::Transient, None);
        assert_eq!(
            s.why(t0).to_string(),
            "a change here, a change on the drive",
            "a failed sync's, asked for again"
        );
        s.synced(t0, Synced::Unfinished { progressed: false });
        assert_eq!(s.why(t0 + ms(10_000)).to_string(), "unfinished work");
        s.woke(t0);
        assert_eq!(s.why(t0).to_string(), "a wake-up");
    }

    #[test]
    fn it_syncs_at_start_then_when_something_changes() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
        assert!(s.due(t0));
        s.synced(t0, Synced::Done);
        assert!(!s.due(t0 + ms(10)));
        s.want(Why::Local);
        assert!(s.due(t0 + ms(20)));
    }

    /// No poll: time alone asks for no sync, however long nothing happens.
    #[test]
    fn nothing_syncs_without_a_reason() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
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
        let mut s = Schedule::new(t0, LOOK, false);
        s.synced(t0, stuck);
        assert!(!s.due(t0 + s5 - ms(1)));
        assert!(s.due(t0 + s5));
        // Stuck: twice as long each time.
        s.synced(t0, stuck);
        assert!(!s.due(t0 + s5 * 2 - ms(1)));
        assert!(s.due(t0 + s5 * 2));
        // A change meanwhile syncs at once.
        s.want(Why::Local);
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
        let mut s = Schedule::new(t0, LOOK, false);
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
        let mut s = Schedule::new(t0, LOOK, false);
        assert_eq!(s.later(t0 + ms(10)), LOOK - ms(10));
        assert!(!s.due(t0 + LOOK - ms(1)));
        assert!(s.due(t0 + LOOK));
    }

    #[test]
    fn failures_back_off_doubling_and_waking_forgets_it() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
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
        s.asked(t0, Why::Asked);
        assert!(s.due(t0));
        assert_eq!(s.failed(t0, ErrorClass::Bug, None), Duration::from_secs(10));
    }

    /// A pause holds syncs until resumed (a timed one's end is the daemon's to tell).
    #[test]
    fn a_pause_holds_syncs_until_resumed() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, true);
        assert!(!s.due(t0), "started paused");
        s.want(Why::Local);
        assert!(!s.due(t0 + Duration::from_secs(3600)));
        assert!(s.resume(t0));
        assert!(!s.paused());
        assert!(s.due(t0));
        s.pause();
        assert!(!s.due(t0));
        assert!(s.resume(t0));
        assert!(!s.resume(t0), "resumed once");
    }

    /// Idle, nothing wakes the runner but the look, and that only while it looks; whatever
    /// time alone would change is woken for, and nothing earlier.
    #[test]
    fn the_next_wake_is_the_first_thing_time_changes() {
        let t0 = Instant::now();
        let s5 = Duration::from_secs(5);
        let mut s = Schedule::new(t0, LOOK, false);
        assert_eq!(s.next_wake(t0, true), Some(t0), "starting: at once");
        s.synced(t0, Synced::Done);
        assert_eq!(s.next_wake(t0, true), Some(t0 + LOOK));
        assert_eq!(s.next_wake(t0, false), None, "idle with a watcher: never");
        for at in [t0 + ms(1), t0 + LOOK - ms(1)] {
            assert!(!s.due(at));
        }

        // A failure's backoff: a change waits for its end.
        let wait = s.failed(t0, ErrorClass::Transient, None);
        assert_eq!(s.next_wake(t0, false), None, "nothing wanted");
        s.want(Why::Local);
        assert_eq!(s.next_wake(t0, false), Some(t0 + wait));
        assert!(!s.due(t0 + wait - ms(1)));
        assert!(s.due(t0 + wait));

        // Unfinished work: its retry.
        s.synced(t0, Synced::Unfinished { progressed: false });
        assert_eq!(s.next_wake(t0, false), Some(t0 + s5));
        assert!(s.due(t0 + s5));

        // Paused (a timed pause too: the daemon ends it): no sync, only the look.
        s.pause();
        s.want(Why::Local);
        assert_eq!(s.next_wake(t0, false), None);
        assert_eq!(s.next_wake(t0, true), Some(t0 + LOOK));
    }

    #[test]
    fn a_sync_stopped_by_a_pause_is_still_wanted() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
        s.synced(t0, Synced::Stopped);
        assert!(s.due(t0));
    }

    /// A server that says when to ask again isn't asked before then (up to an hour), however
    /// short the backoff.
    #[test]
    fn a_failure_waits_for_the_servers_retry_after() {
        let t0 = Instant::now();
        let mut s = Schedule::new(t0, LOOK, false);
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
        let mut s = Schedule::new(t0, LOOK, false);
        assert!(!s.read(), "a start reads anyway");
        s.asked(t0, Why::Asked);
        assert!(s.read());
        s.failed(t0, ErrorClass::Bug, None);
        assert!(s.read(), "the failed sync may not have read");
        s.synced(t0, Synced::Done);
        assert!(!s.read());
    }
}
