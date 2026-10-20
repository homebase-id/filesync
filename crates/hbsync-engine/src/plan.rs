//! A plan of operations and what goes into making one: the reconciler's input and options,
//! the operations, conflicts, and what an operation needs before it may run.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use hbsync_local::Filter;
use hbsync_types::{
    ContentHash, ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Tags,
    VersionTag,
};

use crate::scope::Scope;
use crate::tree::Tree;

/// Per-node facts about the base that aren't part of [`NodeState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseExtra {
    /// Hash of the local bytes when the base was agreed (the base's `content.hash` is the
    /// remote's representation).
    pub local_hash: Option<ContentHash>,
    pub content_state: ContentState,
    /// The remote version the base was agreed at. Distinguishes conflict instances: a
    /// conflict copy id derived from it is stable across replans of one conflict and new for
    /// the next one.
    pub version: Option<VersionTag>,
}

/// What a folder leaves out of the drive, which the reconciler masks: this device's filter
/// (§10) and the part of the drive outside the folder's scope (§12). The folder's own
/// ([`crate::FolderCtx::masking`]).
#[derive(Debug, Clone, Copy)]
pub struct Masking<'a> {
    pub filter: &'a Filter,
    pub scope: &'a Scope,
}

/// Settings that affect decisions.
#[derive(Debug, Clone)]
pub struct Options {
    /// Device name used in conflict-copy names.
    pub device: String,
    /// This computer's id for the account (its state DB's own id): a conflict copy's id
    /// includes it, so two computers never make the same copy.
    pub computer: String,
    /// Today's date (`YYYY-MM-DD`) for conflict-copy names. Passed in to keep this pure.
    pub date: String,
    /// Deletes above `max(mass_delete_min, mass_delete_fraction × synced files)` are held.
    pub mass_delete_min: usize,
    pub mass_delete_fraction: f64,
    /// Number of synced files (the base's file count), the guard's denominator.
    pub synced_files: usize,
    /// Let every delete through (the user turned the guard off).
    pub allow_mass_delete: bool,
    /// Nodes whose deletes the user confirmed: neither counted by the guard nor held.
    pub confirmed: Arc<HashSet<NodeId>>,
    /// Files pending deletion across the whole dirty set, not just this plan's batch:
    /// (gone locally → remote deletes, gone remotely → local trash). The guard trips on these
    /// totals too, so a large delete split over several batches is still held.
    pub pending_deletes: (usize, usize),
    /// Ids a detach must not give: already known here (an earlier detach of the same node).
    pub taken: Arc<HashSet<NodeId>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            device: "device".into(),
            computer: String::new(),
            date: "1970-01-01".into(),
            mass_delete_min: 50,
            mass_delete_fraction: 0.25,
            synced_files: 0,
            allow_mass_delete: false,
            confirmed: Arc::default(),
            pending_deletes: (0, 0),
            taken: Arc::default(),
        }
    }
}

/// Everything the reconciler looks at. Trees contain the dirty nodes (where they exist on
/// that side) plus the context the shell loaded: their ancestors and the siblings in affected
/// folders. Nodes not in `dirty` are taken as unchanged context.
#[derive(Debug, Default)]
pub struct Input {
    pub base: Tree,
    pub local: Tree,
    pub remote: Tree,
    pub base_extra: HashMap<NodeId, BaseExtra>,
    pub remote_refs: HashMap<NodeId, RemoteRef>,
    /// Local files whose hash isn't known yet (being written, locked): decided later.
    pub local_unhashed: HashSet<NodeId>,
    pub dirty: BTreeSet<NodeId>,
    /// The remote tree holds every live node the remote has that matters here (the feed is
    /// drained, which reads past the rows it holds back, and parents were looked up). Then a
    /// remote node whose parent is unknown everywhere is a true orphan and moves into
    /// Recovered files instead of waiting forever (§6.3).
    pub remote_complete: bool,
    /// The folders in Recently deleted that nodes of `remote` are in, and the removed folders
    /// above them: a folder's delete removes the folder first, then what is in it (§6.3).
    pub removed: HashMap<NodeId, Removed>,
    /// The server's time of each remote node's version, where this device knows it (node
    /// schema, Recently deleted): a node in a removed folder written no later than the
    /// folder's removal went with it.
    pub changed_at: HashMap<NodeId, i64>,
    /// Which children the trees hold: `None`, all of every folder's that matters; else as
    /// [`Coverage`] says.
    pub coverage: Option<Coverage>,
}

/// The children of each folder that an [`Input`] holds, on every side, when it doesn't hold
/// them all: those of the folders in `full`, and in the others those whose names have one of
/// the folder's sibling hashes in `named` ([`hbsync_namemap::sibling_hash`]): everything that
/// may clash with those names. A plan that needs more says which folders in
/// [`Plan::uncovered`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    pub full: HashSet<NodeId>,
    pub named: HashMap<NodeId, HashSet<i32>>,
}

impl Coverage {
    /// Whether every child of `folder` whose name may clash with `name` is held.
    pub(crate) fn holds(&self, folder: NodeId, name: &str) -> bool {
        self.full.contains(&folder)
            || self
                .named
                .get(&folder)
                .is_some_and(|keys| keys.contains(&hbsync_namemap::sibling_hash(name)))
    }
}

/// A folder in Recently deleted, as [`Input::removed`] has it.
#[derive(Debug, Clone, PartialEq)]
pub struct Removed {
    /// When it was removed: the server's time (Unix ms).
    pub at: i64,
    /// Where it was, to bring it back.
    pub state: NodeState,
}

/// One step for the executor. Locations are (parent node, remote name); the executor turns
/// them into local paths when it runs, so earlier operations can't invalidate them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Rename the local file of `node` to `name` in its folder; from then on it is `new_node`
    /// (a conflict copy, uploaded by a later `RemoteCreate`).
    LocalFork {
        node: NodeId,
        new_node: NodeId,
        name: String,
    },
    /// `drive` (here and in the other local writes): the drive's version the plan saw, which
    /// the write acts on; the base settles against it, not a later one.
    LocalMkdir {
        node: NodeId,
        parent: NodeId,
        name: String,
        tags: Tags,
        drive: Option<Box<DriveVersion>>,
    },
    LocalMove {
        node: NodeId,
        parent: NodeId,
        name: String,
        drive: Option<Box<DriveVersion>>,
    },
    /// Fetch the remote content of `node`. `replace`: a local file exists and is overwritten
    /// (only if it still has the fingerprint the plan saw).
    LocalDownload {
        node: NodeId,
        parent: NodeId,
        name: String,
        replace: bool,
        tags: Tags,
        drive: Option<Box<DriveVersion>>,
    },
    /// Give the local item of `node` these tags (only the tags changed).
    LocalTags {
        node: NodeId,
        tags: Tags,
        drive: Option<Box<DriveVersion>>,
    },
    /// Move the local item and everything below it to the sync root's trash, with their bases.
    /// `subtree` is every node the plan expects there (sorted, `node` included): if the folder
    /// holds anything else by the time this runs, it is not trashed.
    LocalTrash { node: NodeId, subtree: Vec<NodeId> },
    /// Create the node remotely; files upload the local bytes.
    RemoteCreate {
        node: NodeId,
        parent: NodeId,
        name: String,
        kind: NodeKind,
        tags: Tags,
    },
    /// Rewrite location, tags and, with `upload`, the content, if the remote is still at
    /// `expected`.
    RemoteUpdate {
        node: NodeId,
        file_id: RemoteFileId,
        expected: VersionTag,
        parent: NodeId,
        name: String,
        upload: bool,
        tags: Tags,
    },
    /// Rewrite only the tags, if the remote is still at `expected`.
    RemoteTags {
        node: NodeId,
        file_id: RemoteFileId,
        expected: VersionTag,
        tags: Tags,
    },
    /// Remove to Recently deleted, if the remote is still at `expected`; `parent`: its folder
    /// on the drive.
    RemoteDelete {
        node: NodeId,
        parent: NodeId,
        file_id: RemoteFileId,
        expected: VersionTag,
    },
    /// Both sides agree; record that as the base.
    Settle { node: NodeId },
    /// The local item of `node` becomes the new node `new_node`, where it is (with what is
    /// below it): `node` moved remotely to where this device doesn't sync, and the item holds
    /// what must stay here (§10). A later op creates it remotely.
    LocalDetach { node: NodeId, new_node: NodeId },
    /// This device no longer syncs `node` (§10): its local item goes to the trash, as with
    /// [`Op::LocalTrash`] with `subtree` (empty if nothing of it is here), with its bases; where
    /// nothing of it is here, `node`'s base goes all the same. A base left behind would read as
    /// a local delete once the node is back in view.
    Forget { node: NodeId, subtree: Vec<NodeId> },
}

impl Op {
    /// The operation's kind, as logs and notes name it (`RemoteCreate`).
    pub fn kind(&self) -> &'static str {
        match self {
            Op::LocalFork { .. } => "LocalFork",
            Op::LocalMkdir { .. } => "LocalMkdir",
            Op::LocalMove { .. } => "LocalMove",
            Op::LocalDownload { .. } => "LocalDownload",
            Op::LocalTags { .. } => "LocalTags",
            Op::LocalTrash { .. } => "LocalTrash",
            Op::RemoteCreate { .. } => "RemoteCreate",
            Op::RemoteUpdate { .. } => "RemoteUpdate",
            Op::RemoteTags { .. } => "RemoteTags",
            Op::RemoteDelete { .. } => "RemoteDelete",
            Op::Settle { .. } => "Settle",
            Op::LocalDetach { .. } => "LocalDetach",
            Op::Forget { .. } => "Forget",
        }
    }

    pub fn node(&self) -> NodeId {
        match self {
            Op::LocalFork { node, .. }
            | Op::LocalMkdir { node, .. }
            | Op::LocalMove { node, .. }
            | Op::LocalDownload { node, .. }
            | Op::LocalTags { node, .. }
            | Op::LocalTrash { node, .. }
            | Op::RemoteCreate { node, .. }
            | Op::RemoteUpdate { node, .. }
            | Op::RemoteTags { node, .. }
            | Op::RemoteDelete { node, .. }
            | Op::Settle { node }
            | Op::LocalDetach { node, .. }
            | Op::Forget { node, .. } => *node,
        }
    }

    /// The drive's version a local write acts on (the plan saw it).
    pub fn drive(&self) -> Option<&DriveVersion> {
        match self {
            Op::LocalMkdir { drive, .. }
            | Op::LocalMove { drive, .. }
            | Op::LocalDownload { drive, .. }
            | Op::LocalTags { drive, .. } => drive.as_deref(),
            _ => None,
        }
    }

    /// Whether it moves file bytes: the executor runs these together on its worker pool.
    pub fn is_transfer(&self) -> bool {
        matches!(self, Op::LocalDownload { .. }) || self.uploads()
    }

    /// Whether it sends the local item's bytes.
    pub fn uploads(&self) -> bool {
        matches!(
            self,
            Op::RemoteCreate {
                kind: NodeKind::File,
                ..
            } | Op::RemoteUpdate { upload: true, .. }
        )
    }

    pub fn is_delete(&self) -> bool {
        self.deletes().is_some()
    }

    /// The node this op deletes on the drive or trashes here: what the mass-delete guard
    /// judges, and what a user confirms.
    pub fn deletes(&self) -> Option<NodeId> {
        match self {
            Op::RemoteDelete { node, .. } => Some(*node),
            _ => self.trashes(),
        }
    }

    /// The node whose local item (with everything below it) this op moves to the trash.
    pub fn trashes(&self) -> Option<NodeId> {
        match self {
            Op::LocalTrash { node, .. } => Some(*node),
            Op::Forget { node, subtree } if !subtree.is_empty() => Some(*node),
            _ => None,
        }
    }

    /// Whether the executor runs it on its worker pool: the transfers, and every write to
    /// the remote.
    pub(crate) fn pooled(&self) -> bool {
        matches!(self, Op::LocalDownload { .. }) || self.writes_remote()
    }

    /// Whether it writes to the remote.
    pub(crate) fn writes_remote(&self) -> bool {
        matches!(
            self,
            Op::RemoteCreate { .. }
                | Op::RemoteUpdate { .. }
                | Op::RemoteTags { .. }
                | Op::RemoteDelete { .. }
        )
    }

    /// The node whose local name this op gives up (it moves away, is trashed, forked, or
    /// replaced by a download that may land elsewhere). A target held by that node can be
    /// freed early by parking the node under a temporary name.
    pub fn vacates_local_name(&self) -> Option<NodeId> {
        if let Some(node) = self.trashes() {
            return Some(node);
        }
        match self {
            Op::LocalMove { node, .. }
            | Op::LocalFork { node, .. }
            | Op::LocalDownload {
                node,
                replace: true,
                ..
            } => Some(*node),
            _ => None,
        }
    }

    /// What must have happened on the server (in the client's mirror, or earlier in the plan)
    /// for this op to run; until then it waits (sync-semantics §8a).
    pub fn requires(&self) -> Option<Requirement> {
        match self {
            Op::RemoteCreate { parent, .. } | Op::RemoteUpdate { parent, .. }
                if !parent.is_root() =>
            {
                Some(Requirement::RemoteFolder(*parent))
            }
            Op::RemoteDelete { parent, .. } => Some(Requirement::FolderFirst(*parent)),
            _ => None,
        }
    }
}

/// An ordering condition on the server side (see [`Op::requires`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// The folder exists remotely: nothing is created or moved into a folder whose own create
    /// failed, or other clients would see children pointing at nothing.
    RemoteFolder(NodeId),
    /// The folder's removal, if the plan has one, ran first: what a folder's delete takes along
    /// is removed no earlier than the folder (node schema, Recently deleted).
    FolderFirst(NodeId),
}

/// Whether an operation's [`Requirement`] is met, as the executor's order guards judge it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Met {
    /// It may run.
    Yes,
    /// Not yet: later in this run (what it waits for may still happen).
    Later,
    /// Not in this run: its folder's removal ended without removing it, so what is in it waits
    /// for the next plan (nothing is removed before its folder: node schema, Recently deleted).
    NextPlan,
}

impl Requirement {
    /// Whether the requirement is met: `on_drive` says whether the mirror has a folder,
    /// `removing` whether a folder's removal is still to run in this run (or to retry), and
    /// `unremoved` whether one ended in this run without removing it. The executor and the
    /// simulator both judge by it.
    pub fn met<E>(
        self,
        on_drive: impl FnOnce(NodeId) -> Result<bool, E>,
        removing: impl FnOnce(NodeId) -> bool,
        unremoved: impl FnOnce(NodeId) -> bool,
    ) -> Result<Met, E> {
        Ok(match self {
            Requirement::RemoteFolder(folder) if on_drive(folder)? => Met::Yes,
            Requirement::RemoteFolder(_) => Met::Later,
            Requirement::FolderFirst(folder) if unremoved(folder) => Met::NextPlan,
            Requirement::FolderFirst(folder) if removing(folder) => Met::Later,
            Requirement::FolderFirst(_) => Met::Yes,
        })
    }
}

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

    /// What a download acts on, with the mirror at `mirror_at`: the drive's version its plan
    /// saw (`plan`), only while the mirror still holds it; else, where `own` says this run's own
    /// write made the mirror's version of it since (the bytes the same: a tag write, a move),
    /// or the plan saw none, the mirror's. Where the drive moved on otherwise (another folder's
    /// engine pulled the next version), it is stale: the next plan decides on that. The
    /// executor and the simulator both decide by it.
    pub fn acted_by_download(
        plan: Option<&DriveVersion>,
        mirror_at: RemoteRef,
        own: impl FnOnce(&DriveVersion) -> bool,
    ) -> Acts<'_> {
        match plan {
            Some(d) if d.at == mirror_at => Acts::Plan(d),
            Some(d) if !own(d) => Acts::Stale,
            _ => Acts::Mirror,
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

/// A conflict the user should hear about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conflict {
    /// Both sides edited the file; the local version was kept as `copy`.
    EditEdit { node: NodeId, copy: NodeId },
    /// Both sides moved the node to different places; the remote move won.
    MoveMove { node: NodeId },
    /// A delete lost against an edit on the other side.
    EditDelete { node: NodeId },
    /// A name clash; `node` was renamed.
    NameClash { node: NodeId },
    /// A move would have created a cycle and was undone.
    Cycle { node: NodeId },
    /// The node's folder is gone from the drive for good; it was moved into Recovered files
    /// ([`NodeId::RECOVERED`]; §6.3).
    Orphan { node: NodeId },
    /// Both sides edited the file, and the local version's conflict copy can't be made yet
    /// (its folder waits for its own): both versions stay as they are until it can.
    CopyWaits { node: NodeId },
}

impl Conflict {
    /// The node the conflict is about.
    pub fn node(&self) -> NodeId {
        match self {
            Conflict::EditEdit { node, .. }
            | Conflict::MoveMove { node }
            | Conflict::EditDelete { node }
            | Conflict::NameClash { node }
            | Conflict::Cycle { node }
            | Conflict::Orphan { node }
            | Conflict::CopyWaits { node } => *node,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// In execution order.
    pub ops: Vec<Op>,
    /// Deletes held by the mass-delete guard.
    pub held: Vec<Op>,
    pub conflicts: Vec<Conflict>,
    /// Dirty nodes that need nothing (both sides and the base agree): the dirty flag can go.
    pub clean: Vec<NodeId>,
    /// Dirty nodes that can't be decided yet (unhashed, unknown parent): keep them dirty.
    pub deferred: Vec<NodeId>,
    /// Folders whose names the plan judged without all their children in the input
    /// ([`Input::coverage`]): plan again with them covered in full.
    pub uncovered: BTreeSet<NodeId>,
}
