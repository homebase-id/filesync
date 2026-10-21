//! The settle rules, pure: which version of the drive an operation's base settles against
//! ([`DriveVersion`], [`Against`], [`Acts`]), which attributes both sides hold once it is done
//! ([`Agreement`]), and what a tag write did ([`TagWrite`]). The executor's settle (the DB
//! half, [`crate::settle`]) and the simulator's both decide by them. The engine takes them
//! from here; [`crate::plan`] re-exports them, the crate's way out for the reconciler's users
//! and the simulator (the plan's operations carry a [`DriveVersion`]).

use std::collections::BTreeSet;

use hbsync_platform::Os;
use hbsync_types::{ContentHash, NodeState, RemoteRef, Tags, VersionTag};

/// A version of a node on the drive: what it holds, and which version it is; for a local
/// write, also the version of the base the plan saw (`None`: none), which its commit checks
/// the base is still at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriveVersion {
    pub state: NodeState,
    pub at: RemoteRef,
    pub base_at: Option<VersionTag>,
}

impl DriveVersion {
    /// The version mirror row `m` holds, for a write over it, the base having been at `base_at`.
    pub fn of(m: &hbsync_state::MirrorRow, base_at: Option<VersionTag>) -> Option<DriveVersion> {
        Some(DriveVersion {
            state: m.state()?,
            at: m.remote_ref(),
            base_at,
        })
    }

    /// What a download acts on, with the mirror at `mirror_at` holding bytes `expected`: the
    /// drive's version its plan saw (`plan`), only while the mirror still holds it; else, where
    /// this run's own write made the mirror's version since (`ours`, the base there: `base_at`),
    /// the bytes the same (a tag write, a move), or the plan saw none, the mirror's. Where the
    /// drive moved on otherwise (another folder's engine pulled the next version), it is stale:
    /// the next plan decides on that. The executor and the simulator both decide by it.
    pub fn acted_by_download(
        plan: Option<&DriveVersion>,
        mirror_at: RemoteRef,
        expected: Option<ContentHash>,
        ours: Option<RemoteRef>,
        base_at: Option<VersionTag>,
    ) -> Acts<'_> {
        let own = |d: &DriveVersion| {
            ours == Some(mirror_at)
                && base_at == ours.map(|o| o.version)
                && d.state.content.map(|c| c.hash) == expected
        };
        match plan {
            Some(d) if d.at == mirror_at => Acts::Plan(d),
            Some(d) if !own(d) => Acts::Stale,
            _ => Acts::Mirror,
        }
    }

    /// What a local write that acted on this version settles its content, place and version
    /// against (sync semantics §8: compare and set), with this version's tags: this version,
    /// where the base is still at the version the write's plan saw (`base_at`, the base's now),
    /// or where this run's own operations settled it (`ours`) at this version; the mirror's
    /// where they settled it at the version the mirror holds (this run's own remote write moved
    /// it past this one: `mirror` of that version, read only then, `None` where it holds
    /// another). `None`: the base moved otherwise, and only the tags settle. The executor's
    /// settle and the simulator's decide by it.
    pub fn settles_against(
        &self,
        base_at: Option<VersionTag>,
        ours: Option<RemoteRef>,
        mirror: impl FnOnce(RemoteRef) -> Option<(NodeState, RemoteRef)>,
    ) -> Option<(NodeState, RemoteRef)> {
        match ours {
            _ if base_at == self.base_at => Some((self.state.clone(), self.at)),
            Some(o) if o == self.at => Some((self.state.clone(), self.at)),
            Some(o) => mirror(o).map(|(r, at)| {
                let tags = self.state.tags.clone();
                (NodeState { tags, ..r }, at)
            }),
            None => None,
        }
    }
}

/// What a download acts on ([`DriveVersion::acted_by_download`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acts<'a> {
    /// The version its plan saw.
    Plan(&'a DriveVersion),
    /// The mirror's version.
    Mirror,
    /// Neither: the drive moved on since the plan.
    Stale,
}

/// What a tag write left (`written`) and what the item held before it (`before`), read as the
/// executor's settle reads them against the drive's tags (`reference`): what `os` can't hold is
/// the drive's, and of what was there, a tag `os` can't hold at all is left out (the item never
/// held it: the write leaves the drive's value of it). The executor's settle and the
/// simulator's read them alike.
pub fn tags_written(before: &Tags, written: &Tags, reference: &Tags, os: Os) -> (Tags, Tags) {
    let holdable = Tags::new(
        reference
            .iter()
            .filter(|t| hbsync_platform::tags::express(t, os).is_some())
            .cloned(),
    );
    (
        Tags::fill(Some(before), &holdable, os),
        Tags::fill(Some(written), reference, os),
    )
}

/// What an operation did to its item's tags, for the base's (sync semantics §11). The base
/// records exactly what a local write wrote, tag by tag, never a recomputation against an
/// older version: the next plan compares the drive's newer value with the written one, and
/// the scan reports what the user changed after the write. The commit, recovery and the
/// simulator settle tags through [`TagWrite::settle`] alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagWrite<'a> {
    /// Not a local write (a remote write, settling): agreed tag by tag against the drive as
    /// settled against.
    Agreed,
    /// Wrote none (a move, a folder made without tags): the base's stay.
    Nothing,
    /// The item held `before` and the write left `written` on it.
    Wrote { before: &'a Tags, written: &'a Tags },
    /// Wrote a whole set over an item whose tags (before, or as written) aren't known: the
    /// base's start afresh, each one the item holds and the version holds too.
    Unknown,
}

impl TagWrite<'_> {
    /// The base's tags after the write, from the base's `base` (none without one), the item
    /// read as `local`, against the drive's version it acted on (`acted`). A tag the write
    /// changed takes the written value where the version holds it (both sides do then); every
    /// other tag keeps the base's.
    pub fn settle(self, base: &Tags, local: &Tags, acted: &Tags) -> Tags {
        match self {
            TagWrite::Agreed => Tags::agreed(base, local, acted),
            TagWrite::Nothing => base.clone(),
            TagWrite::Wrote { before, written } => {
                let names: BTreeSet<&str> = base
                    .iter()
                    .chain(before.iter())
                    .chain(written.iter())
                    .map(|t| t.name.as_str())
                    .collect();
                Tags::new(names.into_iter().filter_map(|name| {
                    let w = written.get(name);
                    let recorded = before.get(name) != w && w == acted.get(name);
                    if recorded { w } else { base.get(name) }.cloned()
                }))
            }
            TagWrite::Unknown => Tags::agreed(&Tags::default(), local, acted),
        }
    }
}

/// The base's tags once a folder made back here (no version of the drive live: its restore to
/// follow) holds `held` after a write of its tags that failed: a tag the base and the drive's
/// last version (`drive`: the base's own where none is known) hold and the folder lacks never
/// reached it, and goes. Kept, its absence would read as the user's removal, and remove it on
/// the drive. One that version lacks stays: its absence is the drive's removal (made before the
/// delete), which the restore merges in.
pub fn not_reached(base: &Tags, held: &Tags, drive: &Tags) -> Tags {
    Tags::new(
        base.iter()
            .filter(|t| held.get(&t.name).is_some() || drive.get(&t.name).is_none())
            .cloned(),
    )
}

/// What an operation's base settles against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Against<'a> {
    /// The drive as the mirror has it now.
    Mirror,
    /// The drive's version a local write acted on without putting the item in place (its
    /// tags): the mirror may have moved on, or lost the node, since.
    Acted(&'a DriveVersion),
    /// The drive's version a local write put the item in place from (made, fetched or moved
    /// it; a download's with the bytes it fetched).
    Placed(&'a DriveVersion),
}

impl<'a> Against<'a> {
    /// A write that acted on `drive`, if it knows it; the mirror otherwise.
    pub fn acted(drive: Option<&'a DriveVersion>) -> Self {
        drive.map_or(Against::Mirror, Against::Acted)
    }

    /// A write that put the item in place from `drive`, if it knows it; the mirror otherwise
    /// (which it didn't place from: no base on content alone).
    pub fn placed(drive: Option<&'a DriveVersion>) -> Self {
        drive.map_or(Against::Mirror, Against::Placed)
    }

    /// The version settled against, if not the mirror.
    pub fn drive(self) -> Option<&'a DriveVersion> {
        match self {
            Against::Mirror => None,
            Against::Acted(d) | Against::Placed(d) => Some(d),
        }
    }

    /// Whether the item was put in place from the version settled against: a node without a
    /// base gets one on content alone then ([`Agreement::settle`]).
    pub fn placed_from(self) -> bool {
        matches!(self, Against::Placed(_))
    }
}

/// Which attributes of a node both sides hold once an operation is done. Settling records
/// exactly those in the base (sync-semantics: the base only takes a value when both sides
/// hold it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Agreement {
    pub place: bool,
    pub content: bool,
}

impl Agreement {
    /// Where `local` and `remote` agree. `same_bytes`: the operation just made the bytes equal
    /// (a transfer), even if their hashes use different algorithms.
    pub fn of(local: &NodeState, remote: &NodeState, same_bytes: bool) -> Self {
        let content = match (&local.content, &remote.content) {
            (None, None) => true,
            (Some(l), Some(r)) => same_bytes || (l.hash == r.hash && l.executable == r.executable),
            _ => false,
        };
        Agreement {
            place: local.same_place(remote),
            content,
        }
    }

    /// Whether a node without a base gets one ([`Agreement::settle`]): both sides agree on
    /// place and content (bytes proven equal across hash algorithms can't be proven again), or
    /// on content where a local write put the item in place from the drive's version
    /// (`placed`): that version is what both sides came from.
    pub fn founds_base(self, placed: bool) -> bool {
        self.content && (self.place || placed)
    }

    /// The base after settling: the agreed attributes are taken from `remote`, the tags one
    /// by one ([`Tags::agreed`]). A node without a base gets one where [`Agreement::founds_base`].
    pub fn settle(
        self,
        base: Option<NodeState>,
        local: &NodeState,
        remote: &NodeState,
        placed: bool,
    ) -> Option<NodeState> {
        let Some(mut b) = base else {
            return self.founds_base(placed).then(|| NodeState {
                tags: Tags::agreed(&Tags::default(), &local.tags, &remote.tags),
                ..remote.clone()
            });
        };
        if self.place {
            b.parent = remote.parent;
            b.name = remote.name.clone();
        }
        if self.content {
            b.content = remote.content;
        }
        b.tags = Tags::agreed(&b.tags, &local.tags, &remote.tags);
        Some(b)
    }
}
