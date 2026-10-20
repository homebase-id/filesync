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
    /// This and every later upload of the run are held.
    HoldUploads,
    /// Left for the next plan: the world moved on since this one.
    Stale,
    /// Left for the next plan: this device can't do it as planned.
    Unsyncable,
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
        C::Denied => (InRun::Stop, Shown::Denied),
        C::Full => (InRun::HoldUploads, Shown::Failing),
        C::Stale | C::Gone => (InRun::Stale, Shown::Failing),
        C::Refused | C::LocalDenied => (InRun::Unsyncable, Shown::Failing),
        C::LocalBusy | C::LocalFull | C::LocalFault => (InRun::Retry, Shown::Failing),
        C::StateBusy | C::NeedsUser | C::Bug => (InRun::Retry, Shown::Failing),
    };
    Policy {
        in_run,
        shown,
        rarely: matches!(class, C::Denied),
    }
}
