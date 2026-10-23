//! A plan of operations and what goes into making one: the reconciler's input and options,
//! the operations, conflicts, and what an operation needs before it may run.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use hbsync_local::Filter;
use hbsync_types::{
    ContentHash, ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Tags,
    VersionTag,
};

use crate::scope::Scope;
use crate::tree::Tree;

pub use crate::settle::rules::{
    Acts, Against, Agreement, DriveVersion, TagWrite, not_reached, tags_written,
};

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
    /// What the mass-delete guard judges with (§7).
    pub guarding: Guarding,
    /// Ids a detach must not give: already known here (an earlier detach of the same node).
    pub taken: Arc<HashSet<NodeId>>,
    /// The page is a piece of a folder's subtree too big for a page
    /// ([`crate::pages::OwnPages`]), so what is in it doesn't wait for a folder above it that
    /// another page deletes ([`Input::going_above`]): that one goes after it. What that
    /// folder's removal on the drive took along, unchanged here, waits for it all the same.
    pub piece: bool,
}

/// What the mass-delete guard judges a plan's deletes with (sync semantics §7): its settings,
/// the counts it weighs them against, and the user's answers so far.
#[derive(Debug, Clone, Default)]
pub struct Guarding {
    pub settings: DeleteGuard,
    /// Number of synced files (the base's file count), the guard's denominator.
    pub synced: usize,
    /// Nodes whose deletes the user confirmed: neither counted by the guard nor held.
    pub confirmed: Arc<HashSet<NodeId>>,
    /// Nodes whose removal elsewhere the guard held as every synced file: held, with what is
    /// under them, until the user answers, whatever the count since.
    pub wiped: Arc<HashSet<NodeId>>,
    /// Files pending deletion across the whole dirty set and the held deletes out of it, not
    /// just this plan's batch: (gone locally → remote deletes, gone remotely → local delete).
    /// The guard trips on these totals too, so a large delete split over several batches is
    /// still held.
    pub pending: (usize, usize),
    /// The files the held deletes out of the dirty set take ([`crate::held`]): (removed from
    /// the drive, deleted here). The guard counts them with each plan's own, as it would if
    /// they were planned with it.
    pub held: (usize, usize),
}

/// The mass-delete guard's settings: deletes above `max(min, fraction × synced files)` are
/// held for the user to confirm, unless the user turned the guard off (`allow`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeleteGuard {
    pub min: usize,
    pub fraction: f64,
    pub allow: bool,
}

impl DeleteGuard {
    /// The limit with `synced` files synced: `max(min, fraction × synced)`.
    pub fn limit(&self, synced: usize) -> usize {
        self.min.max((synced as f64 * self.fraction) as usize)
    }

    /// Whether removing `n` files from the drive (deleted here, or newly ignored, §10 rule 1)
    /// is held: over the limit, or every synced file.
    pub fn trips(&self, n: usize, synced: usize) -> bool {
        n > self.limit(synced) || (synced > 0 && n >= synced && n > 1)
    }

    /// Whether removals elsewhere of `n` files are held: what another device removed is
    /// followed here (deleted here, and Recently deleted on the drive keeps it), but
    /// every synced file at once over the limit: a broken device or a wiped drive.
    pub fn every(&self, n: usize, synced: usize) -> bool {
        synced > 0 && n >= synced && n > self.limit(synced)
    }
}

impl Default for DeleteGuard {
    fn default() -> Self {
        DeleteGuard {
            min: 50,
            fraction: 0.25,
            allow: false,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            device: "device".into(),
            computer: String::new(),
            date: "1970-01-01".into(),
            guarding: Guarding::default(),
            taken: Arc::default(),
            piece: false,
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
    /// The nodes the drive has in a version this device can't read (a newer writer's, one that
    /// doesn't decrypt, a change of kind; the store's `unreadable`): there all the same, so
    /// what is in one isn't an orphan, and a file that is one isn't a file to recover from.
    pub unreadable: HashSet<NodeId>,
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
    /// What the view holds that is dirty but planned in another page (the engine plans its
    /// dirty set in pages), or a held delete out of the dirty set (§7): its change isn't merged
    /// in this plan, so it is left as it is (§6.5): context as it is, a repair moved it with
    /// what it was there (a tag added here lost).
    pub dirty_elsewhere: HashSet<NodeId>,
    /// The page's topmost dirty folders, synced before, that may go, with something still to
    /// plan below them, on any side, in another page: they wait for it, with everything of
    /// the page below them, left as they are, and are planned with their whole dirty subtree
    /// as a page of their own next (§6.5). Planned apart, what is below read alive as context
    /// and kept a deleted folder alive (review 14 R1), or the folder's delete took along what
    /// changed there (an edit lost).
    pub going: HashSet<NodeId>,
    /// The folders above the page's nodes, on any side, synced before, that may go and that
    /// another page plans: what of the page is below them waits for them the same way, so a
    /// folder's delete goes as one, never item by item (but in a piece of a page of its own:
    /// [`Options::piece`], where only what its removal took along waits).
    pub going_above: HashSet<NodeId>,
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
    /// Whether every child of `folder` whose name may clash with `name` is in the input.
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
    /// Delete the local item and everything below it (sync semantics §7: aside, checked,
    /// unlinked), with their bases. `subtree` is every node the plan expects there (sorted,
    /// `node` included): if the folder holds anything else by the time this runs, it stays.
    LocalDelete { node: NodeId, subtree: Vec<NodeId> },
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
    /// This device no longer syncs `node` (§10): its local item is deleted, as with
    /// [`Op::LocalDelete`] with `subtree` (empty if nothing of it is here), with its bases; where
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
            Op::LocalDelete { .. } => "LocalDelete",
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
            | Op::LocalDelete { node, .. }
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

    /// The node this op deletes, on the drive or here: what the mass-delete guard
    /// judges, and what a user confirms.
    pub fn deletes(&self) -> Option<NodeId> {
        match self {
            Op::RemoteDelete { node, .. } => Some(*node),
            _ => self.deletes_here(),
        }
    }

    /// The node whose local item (with everything below it) this op deletes here.
    pub fn deletes_here(&self) -> Option<NodeId> {
        match self {
            Op::LocalDelete { node, .. } => Some(*node),
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

    /// The node whose local name this op gives up (it moves away, is deleted, forked, or
    /// replaced by a download that may land elsewhere). A target held by that node can be
    /// freed early by parking the node under a temporary name.
    pub fn vacates_local_name(&self) -> Option<NodeId> {
        if let Some(node) = self.deletes_here() {
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
    /// The folder's removal, if the plan has one, ran first: what is below a folder that is
    /// deleted is removed no earlier than the folder (node schema, Recently deleted).
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
    /// The files each of `held` takes, by its node, as the guard counted them: (removed from
    /// the drive, deleted here).
    pub held_takes: BTreeMap<NodeId, (usize, usize)>,
    pub conflicts: Vec<Conflict>,
    /// Dirty nodes that need nothing (both sides and the base agree): the dirty flag can go.
    pub clean: Vec<NodeId>,
    /// Dirty nodes that can't be decided yet (unhashed, unknown parent): keep them dirty.
    pub deferred: Vec<NodeId>,
    /// Files here not hashed yet that something else waits for (a change the drive has of
    /// them, or a folder they are in, or their name; sync semantics §6.3): read them, and plan
    /// again. By the ids the store knows them by (a detached item's old one, unlike
    /// `deferred`).
    pub waiting: Vec<NodeId>,
    /// Nodes whose delete waits for a file in them not hashed yet (§6.3): a
    /// confirmation of it lasts ([`Engine::confirm_deletes`]).
    ///
    /// [`Engine::confirm_deletes`]: crate::Engine::confirm_deletes
    pub delete_waits: Vec<NodeId>,
    /// Folders the plan judged without all their children in the input ([`Input::coverage`]):
    /// names that may clash with one not in it, or a rename; a masked child; a node in a
    /// file. Plan again with them covered in full.
    pub uncovered: BTreeSet<NodeId>,
    /// The folders that may go that this page waits for ([`Input::going`],
    /// [`Input::going_above`]): the engine plans each one's dirty subtree as a page of its
    /// own next (§6.5). In id order.
    pub waits_for_pages: Vec<NodeId>,
    /// Nodes of this page whose names clash with what another page plans, each group with
    /// those: planned together as a page of their own next, the clash decided there as in one
    /// page (§6.5).
    pub waits_together: Vec<Vec<NodeId>>,
}
