//! Test hooks: steps a test makes fail, and places where it makes a run stop as if the process
//! died there ([`super::ExecConfig`]). The points are marked in every build; making them fail
//! or crash takes the `testkit` feature, so a release build never does.

/// The test hooks of a run: none without `testkit`.
#[derive(Debug, Clone, Default)]
pub struct Hooks {
    /// Stop the run at this point, as if the process died there.
    #[cfg(feature = "testkit")]
    pub crash_at: Option<CrashPoint>,
    /// The steps that fail, and how often.
    #[cfg(feature = "testkit")]
    pub faults: std::sync::Arc<Faults>,
}

impl Hooks {
    /// Whether `point` fails now (one of its failures taken).
    #[cfg(feature = "testkit")]
    pub(crate) fn fails(&self, point: FailPoint) -> bool {
        self.faults.fails(point)
    }

    #[cfg(not(feature = "testkit"))]
    pub(crate) fn fails(&self, _: FailPoint) -> bool {
        false
    }

    /// Whether a run stops at `point`.
    #[cfg(feature = "testkit")]
    pub(crate) fn crashes_at(&self, point: CrashPoint) -> bool {
        self.crash_at == Some(point)
    }

    #[cfg(not(feature = "testkit"))]
    pub(crate) fn crashes_at(&self, _: CrashPoint) -> bool {
        false
    }
}

/// Steps a test can make fail (`Faults`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailPoint {
    /// A placement's commit write, once its item is in place: the run stops, the record kept
    /// for the next sync's recovery.
    Commit,
    /// Reading a placed item back (a folder's stat, a file's metadata): recovery settles it in
    /// the run.
    PlacedRead,
    /// A tag write: the item keeps what it held.
    TagWrite,
    /// Reading what a failed tag write left: the item is taken to hold what it held before.
    TagRead,
    /// Putting a download's new version in place, once the old one it replaces from another
    /// folder is in the trash.
    AfterTrash,
    /// Bringing that old version back from the trash.
    Restore,
    /// Making a group of downloads' bytes durable: none of them is put in place.
    Barrier,
}

/// Tests only: how many more times each [`FailPoint`] fails.
#[cfg(feature = "testkit")]
#[derive(Debug, Default)]
pub struct Faults(std::sync::Mutex<std::collections::HashMap<FailPoint, usize>>);

#[cfg(feature = "testkit")]
impl Faults {
    /// Makes `point` fail its next `n` times.
    pub fn fail(&self, point: FailPoint, n: usize) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(point, n);
    }

    /// Whether `point` fails now (one of its failures taken).
    pub(crate) fn fails(&self, point: FailPoint) -> bool {
        let mut left = self.0.lock().unwrap_or_else(|p| p.into_inner());
        match left.get_mut(&point) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }
}

/// Places where a test can make the executor stop as if it crashed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// After parking an occupant on disk, before its row follows.
    AfterParkRename,
    /// After a conflict-copy rename on disk, before the rows follow.
    AfterForkRename,
    /// After a download is moved into place, or a folder made in place (before its tags are
    /// written), before its commit.
    AfterPlaced,
}
