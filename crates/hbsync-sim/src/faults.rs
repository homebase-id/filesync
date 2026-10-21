//! The faults a [`crate::Sim`] plays besides the users' actions, and what each keeps between
//! steps: the engine's world going wrong. Some run in every simulation (a tag write or a
//! placement's commit failing now and then, by `writes`; a crash's schedule, by `crashes`),
//! the others when a `Sim::with_*` switches them on, or a scripted test. The oracle reads two
//! of them: whether the drive refuses requests, and what it judged each (`refusing`,
//! `judged`); what it judges is the [`crate::oracle::Ledger`]'s.

use std::collections::{BTreeMap, BTreeSet};

use hbsync_types::NodeId;

#[derive(Debug, Default)]
pub(crate) struct Faults {
    /// Crashes so far ([`crate::Event::Crash`]): every other one leaves a download not hashed.
    pub(crate) crashes: u64,
    /// Local placements and tag writes so far: now and then a tag write fails (a volume
    /// refusing it), or a placement's commit (a commit write or a folder's stat failing).
    pub(crate) writes: u64,
    /// For scripted tests: the next placement with a record fails its commit.
    pub(crate) fail_commit_next: bool,
    /// For scripted tests: the next tag write fails.
    pub(crate) fail_tags_next: bool,
    /// A download takes two steps: fetching, then putting in place and committing; anything
    /// may happen between them ([`crate::Sim::with_split_downloads`]).
    pub(crate) split_downloads: bool,
    /// The drive refuses some requests as they are ([`crate::Sim::with_refusals`]): what it
    /// judged each request it was sent, by everything the request sends.
    pub(crate) refusing: bool,
    pub(crate) judged: BTreeMap<String, bool>,
    /// Every fourth plan a main folder makes is checked against the engine's own planning
    /// path ([`crate::Sim::with_narrow_check`]), in rounds counted here.
    pub(crate) narrow_check: bool,
    pub(crate) narrow_rounds: usize,
    /// The drive answers a wrong "not there" now and then
    /// ([`crate::Sim::with_wrong_not_theres`]): the plans heard so far, and the files it said
    /// it of.
    pub(crate) lying: bool,
    pub(crate) plans_heard: u64,
    pub(crate) lied: BTreeSet<NodeId>,
}
