//! What an error means, whatever raised it: the one vocabulary the client decides by. Each
//! error type says its class (`RemoteError::class`, `StateError::class`, ...), and one table
//! says what every class does (`hbsync_engine::policy`).

use std::io;

use hbsync_platform::{IoClass, classify};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// No answer, a timeout, a server or a page in front of it in trouble: the network or the
    /// server is down for now.
    Transient,
    /// The identity refused the credentials or the drive (401, 403): nothing gets better until
    /// the user logs in again.
    Denied,
    /// The identity is out of storage (507).
    Full,
    /// The drive moved on since the plan: a version written since, a node id taken, bytes that
    /// changed on the way.
    Stale,
    /// The file isn't there: the identity's own "not found", or a tombstone.
    Gone,
    /// The drive won't take it as it is, or answered what can't be read: the same request
    /// gets the same answer.
    Refused,
    /// A local item is locked, or changed meanwhile (gone, or another in its place).
    LocalBusy,
    /// The local volume is full or over quota.
    LocalFull,
    /// Local access denied, or a name or path this OS can't hold.
    LocalDenied,
    /// Any other local fault: an I/O error, a source or sink that failed.
    LocalFault,
    /// The state DB is busy: another connection's write outlasted the busy timeout.
    StateBusy,
    /// Only the user can fix it: a state DB that is corrupt, or from another version.
    NeedsUser,
    /// A fault of this client: a panic, a broken invariant, an unexpected DB error.
    Bug,
}

impl ErrorClass {
    /// The class of a failed local file operation.
    pub fn of_io(e: &io::Error) -> ErrorClass {
        classify(e).into()
    }
}

impl From<IoClass> for ErrorClass {
    fn from(class: IoClass) -> Self {
        match class {
            IoClass::Locked | IoClass::Raced => ErrorClass::LocalBusy,
            IoClass::DiskFull => ErrorClass::LocalFull,
            IoClass::Denied | IoClass::TooLong => ErrorClass::LocalDenied,
            IoClass::Other => ErrorClass::LocalFault,
        }
    }
}
