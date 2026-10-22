//! What an operation and a run come to: an operation's [`Outcome`] (or why it was left,
//! [`Skip`]; why the run stopped, [`Stopped`]), how an error maps to one ([`Outcome::of`], by
//! the policy table), and the run's [`ExecReport`], with what it learned of the drive for the
//! account ([`crate::news::DriveNews`]).

use std::io;
use std::time::Duration;

use hbsync_platform::RelPath;
use hbsync_remote::{ErrorClass, RemoteError};
use hbsync_state::StateError;
use hbsync_types::activity::Synced;

use crate::news::DriveNews;
use crate::policy::{InRun, policy};
use crate::reconcile::Op;

/// What a run did.
#[derive(Debug, Default)]
pub(crate) struct ExecReport {
    pub done: usize,
    /// Left for the next plan, and why.
    pub skipped: Vec<(Op, Skip)>,
    /// Gave up after retries (transient errors, locked files, ordering).
    pub failed: Vec<(Op, String)>,
    /// Local folders whose contents differ from the DB: rescan them before planning again.
    pub rescan: Vec<RelPath>,
    /// Why the run stopped early, leaving the rest dirty for a later sync.
    pub stopped: Option<Stopped>,
    /// A refusal (or a download's strike) was recorded ([`crate::refused`]).
    pub refused: bool,
    /// What was done, for people (one entry per file or item).
    pub synced: Vec<Synced>,
    /// What it learned of the drive, for the account: behind (a remote write refused for the
    /// drive's state, an earlier try of it landed unanswered, an answer that didn't come), or
    /// full (the run's uploads from then on were held, left dirty).
    pub news: DriveNews,
}

/// Why a run stopped before its plan was done.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Stopped {
    /// The remote refused access: nothing else can succeed until the user authorizes again.
    #[error("access denied: {0}")]
    Denied(String),
    /// Transient remote errors kept coming: the network or the server is down.
    #[error("remote unreachable: {0}")]
    Unreachable(String),
    /// The server is busy or paused, and said when to ask again.
    #[error("the server asks to come back in {secs}s: {0}", secs = .1.as_secs())]
    Later(String, Duration),
    /// The caller asked the run to stop (pause, shutdown).
    #[error("stopped on request")]
    Requested,
}

#[derive(Debug)]
pub(super) enum Outcome {
    /// Done; the node still needs settling.
    Done,
    /// Done, and settled in the operation's own transaction.
    Settled,
    Retry(String),
    /// A transient remote error: retry after a pause.
    Backoff(String),
    /// Left for the next plan.
    Skip(Skip),
    /// Stop the run: the remote refused access, or is busy and said when to ask again.
    Stop(Stopped),
    /// The drive is out of storage: hold this and every later upload of the run (and on the
    /// account: for the time the drive said, if it did).
    Full(Option<Duration>),
    /// A download's bytes didn't match what the drive said they are: the first time at a
    /// version, the drive is read again; the second, it is refused.
    Mismatch(String),
    /// The state DB failed (a placement's commit, say): the run stops, as at a crash.
    Fatal(StateError),
}

impl Outcome {
    /// What the run does about an error of `class` ([`policy`]); `why`: the error; `after`:
    /// when the server said to ask again.
    pub(super) fn of(class: ErrorClass, why: String, after: Option<Duration>) -> Outcome {
        match policy(class).in_run {
            // A server that said when to ask again isn't asked before: the run stops.
            InRun::Backoff => match after {
                Some(after) => Outcome::Stop(Stopped::Later(why, after)),
                None => Outcome::Backoff(why),
            },
            InRun::Retry => Outcome::Retry(why),
            InRun::Stop => Outcome::Stop(Stopped::Denied(why)),
            InRun::HoldUploads => Outcome::Full(after),
            // Only the state DB's errors end a run, and they come as themselves
            // (`From<StateError>`).
            InRun::End => {
                debug_assert!(false, "{class:?} ends a run: {why}");
                Outcome::Retry(why)
            }
            // Told only once it keeps coming back (the runner's stalled skips).
            InRun::Stale => Outcome::Skip(Skip::Stale(why)),
            InRun::Unsyncable => Outcome::Skip(Skip::Unsyncable(why)),
            InRun::Refuse => Outcome::Skip(Skip::Refused(why)),
        }
    }

    pub(super) fn stale(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Stale(why.into()))
    }

    pub(super) fn waiting(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Waiting(why.into()))
    }

    pub(super) fn unsyncable(why: impl Into<String>) -> Outcome {
        Outcome::Skip(Skip::Unsyncable(why.into()))
    }
}

/// Why an operation was left for the next plan.
#[derive(Debug)]
pub enum Skip {
    /// The world moved on since the plan (an item changed or went, on either side, or what the
    /// operation needed isn't so any more): the next plan decides again.
    Stale(String),
    /// Another operation of the plan must go first and didn't (held or failed: that one says
    /// why).
    Waiting(String),
    /// This device can't do it as planned: a name or path it can't hold, access denied, a
    /// fault of its own.
    Unsyncable(String),
    /// The drive won't take it as it is (or gave other bytes than it said, twice): recorded, it
    /// isn't sent again while it is the same ([`crate::refused`]).
    Refused(String),
}

impl Stopped {
    /// What it means ([`ErrorClass`]).
    pub fn class(&self) -> ErrorClass {
        match self {
            Stopped::Denied(_) => ErrorClass::Denied,
            Stopped::Unreachable(_) => ErrorClass::Transient,
            Stopped::Later(..) => ErrorClass::RateLimited,
            // Not an error: a run stopped on request ends well.
            Stopped::Requested => ErrorClass::Bug,
        }
    }
}

impl Skip {
    pub fn as_str(&self) -> &str {
        match self {
            Skip::Stale(why) | Skip::Waiting(why) | Skip::Unsyncable(why) | Skip::Refused(why) => {
                why
            }
        }
    }
}

/// A DB error inside an operation ends the run, as one in a placement's commit does: tried
/// again, it would most likely fail the same way (a busy DB after its timeout, a broken one).
impl From<StateError> for Outcome {
    fn from(e: StateError) -> Self {
        Outcome::Fatal(e)
    }
}

/// An operation's result. `Err` ends it early: a guard that stopped it, or a DB error.
pub(super) type Step<T = Outcome> = Result<T, Outcome>;

pub(super) fn rescan(report: &mut ExecReport, path: &RelPath) -> Outcome {
    report.rescan.push(path.parent().unwrap_or_default());
    Outcome::stale(format!("{path} changed on disk"))
}

pub(super) fn io_outcome(e: io::Error) -> Outcome {
    let why = match hbsync_platform::classify(&e) {
        hbsync_platform::IoClass::Raced => format!("raced: {e}"),
        _ => e.to_string(),
    };
    Outcome::of(ErrorClass::of_io(&e), why, None)
}

pub(super) fn remote_outcome(e: RemoteError) -> Outcome {
    Outcome::of(e.class(), e.to_string(), e.retry_after())
}
