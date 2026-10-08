//! OS change journals: Windows USN, macOS FSEvents.
//!
//! A journal reports *where* changes happened since a stored position, never what changed;
//! the caller rescans those places. Whenever a journal can't vouch for completeness it returns
//! [`JournalError::Unusable`] or sets [`JournalDelta::rescan_all`].

use std::path::{Path, PathBuf};

use crate::{FileId, FsFamily};

#[cfg(target_os = "macos")]
mod fsevents;
#[cfg(windows)]
mod usn;

/// Where a journal says a change happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Touched {
    /// An absolute folder path whose direct contents changed.
    Dir(PathBuf),
    /// A folder identified by its file id (USN records carry ids, not paths).
    DirId(FileId),
}

/// The changes since a stored position.
#[derive(Debug, Clone, Default)]
pub struct JournalDelta {
    pub touched: Vec<Touched>,
    /// The journal lost detail (dropped events, "must scan subdirs"): walk everything.
    pub rescan_all: bool,
    /// The position to store once the delta is applied.
    pub cursor: String,
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The journal can't answer for the stored position; fall back to a full walk.
    #[error("journal unusable: {0}")]
    Unusable(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// An OS change journal for the volume holding the sync root.
pub trait ChangeJournal: std::fmt::Debug + Send {
    /// The current position.
    fn now(&mut self) -> Result<String, JournalError>;
    /// The folders touched since `cursor` (a value returned by [`ChangeJournal::now`] or a
    /// previous delta).
    fn since(&mut self, cursor: &str) -> Result<JournalDelta, JournalError>;
}

/// Opens the journal for `root`, if the OS and file system have a trustworthy one.
pub fn open_journal(root: &Path, family: FsFamily) -> Option<Box<dyn ChangeJournal>> {
    if !family.supports_journal() {
        return None;
    }
    #[cfg(windows)]
    {
        usn::UsnJournal::open(root)
            .ok()
            .map(|j| Box::new(j) as Box<dyn ChangeJournal>)
    }
    #[cfg(target_os = "macos")]
    {
        fsevents::FsEventsJournal::open(root)
            .ok()
            .map(|j| Box::new(j) as Box<dyn ChangeJournal>)
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = root;
        None
    }
}
