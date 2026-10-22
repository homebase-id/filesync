//! What a folder's run or an operation of Recently deleted learned of the drive
//! ([`DriveNews`]): a leaf, for what records answers (the executor, the mirror, Recently
//! deleted) to tell the account ([`crate::Drive::absorb`]).

use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use = "what the drive said must be taken in"]
pub struct DriveNews {
    /// The account's picture may lack something: a write was refused for the drive's state,
    /// an earlier try of it had landed unanswered, or an answer didn't come. The feed is read.
    pub behind: bool,
    /// The drive refused an upload, out of storage: uploads are held on the account this long.
    pub full: Option<Duration>,
}
