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
    /// What lands between a folder's checks and its commit (a pull on another thread): run
    /// with the run's state DB before each folder's commit.
    #[cfg(feature = "testkit")]
    pub before_commit: Option<Meanwhile>,
    /// What an app does to an item the run just moved aside to delete, before it is checked
    /// (a write through a handle it had open): run with the item's place aside.
    #[cfg(feature = "testkit")]
    pub aside: Option<Aside>,
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

    /// Does what a test lands between a check and its commit ([`Hooks::before_commit`]).
    #[cfg(feature = "testkit")]
    pub(crate) fn before_commit(&self, db: &mut hbsync_state::StateDb) {
        if let Some(Meanwhile(f)) = &self.before_commit {
            f(db);
        }
    }

    #[cfg(not(feature = "testkit"))]
    pub(crate) fn before_commit(&self, _: &mut hbsync_state::StateDb) {}

    /// Does what a test does to an item moved aside ([`Hooks::aside`]).
    #[cfg(feature = "testkit")]
    pub(crate) fn aside(&self, at: &std::path::Path) {
        if let Some(Aside(f)) = &self.aside {
            f(at);
        }
    }

    #[cfg(not(feature = "testkit"))]
    pub(crate) fn aside(&self, _: &std::path::Path) {}
}

/// Tests only: what an app does to an item moved aside, with its place there.
#[cfg(feature = "testkit")]
#[derive(Clone)]
pub struct Aside(pub std::sync::Arc<dyn Fn(&std::path::Path) + Send + Sync>);

#[cfg(feature = "testkit")]
impl std::fmt::Debug for Aside {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Aside")
    }
}

/// Tests only: a write another thread makes meanwhile, with the run's state DB.
#[cfg(feature = "testkit")]
#[derive(Clone)]
pub struct Meanwhile(pub std::sync::Arc<dyn Fn(&mut hbsync_state::StateDb) + Send + Sync>);

#[cfg(feature = "testkit")]
impl std::fmt::Debug for Meanwhile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Meanwhile")
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
        crate::lock(&self.0).insert(point, n);
    }

    /// Whether `point` fails now (one of its failures taken).
    pub(crate) fn fails(&self, point: FailPoint) -> bool {
        let mut left = crate::lock(&self.0);
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
    /// After an item being deleted is moved aside, before it is checked.
    AfterAside,
    /// After an item being deleted is unlinked, before its rows and bases follow (the commit
    /// lost).
    AfterUnlink,
}
