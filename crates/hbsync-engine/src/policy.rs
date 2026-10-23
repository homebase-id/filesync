//! What the client does about an error, by its class ([`ErrorClass`]): the one table. The
//! executor, the feed, the runner's state and backoff, and the account's connect all decide
//! from it.

use hbsync_remote::ErrorClass;

/// What a run does about an operation that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InRun {
    /// The whole run's remote work pauses, longer for each failure in a row; after a few it
    /// stops, unreachable.
    Backoff,
    /// Tried again in this run, without a pause.
    Retry,
    /// The run stops: nothing else can succeed until the user acts.
    Stop,
    /// This and every later upload of the run are paused: the drive is full (not the guard's
    /// held deletes).
    HoldUploads,
    /// The run ends, as at a crash: the state DB failed.
    End,
    /// Left for the next plan: the world moved on since this one.
    Stale,
    /// Left for the next plan: this device can't do it as planned.
    Unsyncable,
    /// Left, and not sent again while it is the same (the refused record): the drive won't
    /// take it as it is.
    Refuse,
}

/// What a folder or an account shows while its syncs (or its connect) fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shown {
    /// The drive can't be reached for now.
    Offline,
    /// The identity refused this device: it waits for a login.
    Denied,
    /// Anything else.
    Failing,
}

/// What every part of the client does about an error of one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub in_run: InRun,
    pub shown: Shown,
    /// The next try waits the longest at once: only the user changes what it found.
    pub rarely: bool,
}

/// The table.
pub const fn policy(class: ErrorClass) -> Policy {
    use ErrorClass as C;
    let (in_run, shown) = match class {
        C::Transient => (InRun::Backoff, Shown::Offline),
        // As transient, but the run stops at once: the server said when to ask again.
        C::RateLimited => (InRun::Backoff, Shown::Offline),
        C::Denied => (InRun::Stop, Shown::Denied),
        C::Full => (InRun::HoldUploads, Shown::Failing),
        // A lock another program holds, or a full disk, doesn't go away within microseconds:
        // left for a later sync, which the runner paces, as one that went stale.
        C::Stale | C::Gone | C::LocalBusy | C::LocalFull => (InRun::Stale, Shown::Failing),
        C::Refused => (InRun::Refuse, Shown::Failing),
        C::LocalDenied => (InRun::Unsyncable, Shown::Failing),
        C::LocalRaced | C::LocalFault => (InRun::Retry, Shown::Failing),
        C::StateBusy | C::NeedsUser | C::Bug => (InRun::End, Shown::Failing),
    };
    Policy {
        in_run,
        shown,
        rarely: matches!(class, C::Denied | C::NeedsUser),
    }
}
