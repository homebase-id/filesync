//! The reconciler: a pure function from (base, local, remote) to a plan of operations.
//!
//! Spec: `docs/spec/sync-semantics.md`. Steps:
//! 1. classify each node's local and remote change against the base (§2);
//! 2. merge per node into a target state (§4), forking a conflict copy for edit/edit (§5),
//!    and the tags three-way on their own (§11);
//! 3. repair the merged tree: orphans, cycles, duplicate names (§6);
//! 4. diff the merged tree against each side into operations, ordered so parents exist
//!    before children and deletes run bottom-up (§8);
//! 5. hold deletes that trip the mass-delete guard (§7).
//!
//! Content comparisons stay within one side's representation: local hashes (BLAKE3) are
//! compared with the base's local hash, remote hashes with the base's remote hash. A node
//! written by another client with SHA-256 therefore never needs a cross-algorithm comparison,
//! except to tell whether a concurrent edit on both sides was the same edit (if it can't tell,
//! it keeps both versions).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_local::Filter;

use hbsync_platform::name::name_key;
use hbsync_types::{
    ContentHash, ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Tags,
    VersionTag,
};

use crate::mask::Masks;
use crate::scope::Scope;
use crate::tree::{Tree, conflict_copy_name, first_free, numbered_conflict_name};

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

/// Settings that affect decisions.
#[derive(Debug, Clone)]
pub struct Options {
    /// Device name used in conflict-copy names.
    pub device: String,
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
    /// What this device never syncs: remote nodes it matches are masked (§10).
    pub filter: Arc<Filter>,
    /// The part of the drive this engine syncs: what is outside it is masked (§12).
    pub scope: Arc<Scope>,
    /// Ids a detach must not give: already known here (an earlier detach of the same node).
    pub taken: Arc<HashSet<NodeId>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            device: "device".into(),
            date: "1970-01-01".into(),
            mass_delete_min: 50,
            mass_delete_fraction: 0.25,
            synced_files: 0,
            allow_mass_delete: false,
            confirmed: Arc::default(),
            pending_deletes: (0, 0),
            filter: Arc::default(),
            scope: Arc::new(Scope::main()),
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
    /// drained and parents were looked up). Then a remote node whose parent is unknown
    /// everywhere is a true orphan and is moved to the root instead of waiting forever.
    pub remote_complete: bool,
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
    LocalMkdir {
        node: NodeId,
        parent: NodeId,
        name: String,
        tags: Tags,
    },
    LocalMove {
        node: NodeId,
        parent: NodeId,
        name: String,
    },
    /// Fetch the remote content of `node`. `replace`: a local file exists and is overwritten
    /// (only if it still has the fingerprint the plan saw).
    LocalDownload {
        node: NodeId,
        parent: NodeId,
        name: String,
        replace: bool,
        tags: Tags,
    },
    /// Give the local item of `node` these tags (only the tags changed).
    LocalTags { node: NodeId, tags: Tags },
    /// Move the local item and everything below it to the sync root's trash. `subtree` is
    /// every node the plan expects there (sorted, `node` included): if the folder holds
    /// anything else by the time this runs, it is not trashed.
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
    RemoteDelete {
        node: NodeId,
        file_id: RemoteFileId,
        expected: VersionTag,
    },
    /// Both sides agree; record that as the base.
    Settle { node: NodeId },
    /// The local item of `node` becomes the new node `new_node`, where it is (with what is
    /// below it): `node` moved remotely to where this device doesn't sync, and the item holds
    /// what must stay here (§10). A later op creates it remotely.
    LocalDetach { node: NodeId, new_node: NodeId },
    /// This device no longer syncs `node` (§10): its local item goes to the trash, like
    /// [`Op::LocalTrash`] with `subtree` (empty if nothing of it is here), and the bases of
    /// `node` and `subtree` go with it, in one step. A base left behind would read as a local
    /// delete once the node is back in view.
    Forget { node: NodeId, subtree: Vec<NodeId> },
}

impl Op {
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

    /// Whether this op calls the remote (transfers aside, which run on the worker pool).
    pub fn touches_remote(&self) -> bool {
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

    /// What the server must already show (in the client's mirror) for this op to run; until
    /// then it waits (sync-semantics §8a).
    pub fn requires(&self) -> Option<Requirement> {
        match self {
            Op::RemoteCreate { parent, .. } | Op::RemoteUpdate { parent, .. }
                if !parent.is_root() =>
            {
                Some(Requirement::RemoteFolder(*parent))
            }
            Op::RemoteDelete { node, .. } => Some(Requirement::NoRemoteChildren(*node)),
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
    /// The node has no remote children left: a folder goes only after its contents, so a child
    /// whose delete failed is never left live under a tombstone.
    NoRemoteChildren(NodeId),
}

/// Which attributes of a node both sides hold once an operation is done. Settling records
/// exactly those in the base (sync-semantics: the base only takes a value when both sides
/// hold it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Agreement {
    pub place: bool,
    pub content: bool,
    pub tags: bool,
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
            tags: local.tags == remote.tags,
        }
    }

    /// The base after settling: the agreed attributes are taken from `remote`. A node without a
    /// base gets one when both sides agree on place and content (bytes proven equal across
    /// hash algorithms can't be proven again later), with the tags both sides have: the merge
    /// then brings each side the other's (§11).
    pub fn settle(
        self,
        base: Option<NodeState>,
        local: &NodeState,
        remote: &NodeState,
    ) -> Option<NodeState> {
        let Some(mut b) = base else {
            return (self.place && self.content).then(|| NodeState {
                tags: local.tags.intersection(&remote.tags),
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
        if self.tags {
            b.tags = remote.tags.clone();
        }
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
    /// The node's folder no longer exists anywhere; it was moved to the root.
    Orphan { node: NodeId },
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// Never existed on this side (and not in the base).
    Absent,
    Unchanged,
    Created,
    Deleted,
    Changed {
        moved: bool,
        edited: bool,
    },
}

impl Change {
    fn is_change(self) -> bool {
        !matches!(self, Change::Absent | Change::Unchanged)
    }
}

/// Where the merged content of a node comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Local,
    Remote,
    Base,
}

pub fn reconcile(input: &Input, opts: &Options) -> Plan {
    let masks = Masks::of(input, &opts.filter, &opts.scope, &opts.device, &opts.taken);
    let input = masks.input.as_ref().unwrap_or(input);
    Reconciler::new(input, opts, &masks).run()
}

struct Reconciler<'a> {
    /// The input with the masks applied.
    input: &'a Input,
    opts: &'a Options,
    masks: &'a Masks,
    /// The merged tree: context plus targets.
    merged: Tree,
    /// Nodes ops are generated for.
    scope: BTreeSet<NodeId>,
    /// Content origin per scoped node.
    origin: HashMap<NodeId, Origin>,
    /// Local file of `node` becomes the conflict copy `copy`.
    forks: BTreeMap<NodeId, NodeId>,
    /// Becomes `plan.deferred`.
    deferred: BTreeSet<NodeId>,
    plan: Plan,
}

impl<'a> Reconciler<'a> {
    fn new(input: &'a Input, opts: &'a Options, masks: &'a Masks) -> Self {
        Reconciler {
            input,
            opts,
            masks,
            merged: Tree::new(),
            scope: BTreeSet::new(),
            origin: HashMap::new(),
            forks: BTreeMap::new(),
            deferred: BTreeSet::new(),
            plan: Plan::default(),
        }
    }

    fn run(mut self) -> Plan {
        self.build_context();
        let dirty: Vec<NodeId> = self.input.dirty.iter().copied().collect();
        for node in dirty {
            self.merge_node(node);
        }
        self.fix_orphans();
        self.fix_cycles();
        self.fix_names();
        self.emit();
        self.guard_mass_delete();
        self.plan.deferred = self.deferred.into_iter().collect();
        self.plan
    }

    // ---- 1–2: classify and merge ----

    fn build_context(&mut self) {
        let i = self.input;
        let ids: BTreeSet<NodeId> = i
            .base
            .ids()
            .chain(i.local.ids())
            .chain(i.remote.ids())
            .filter(|n| !i.dirty.contains(n))
            .collect();
        for node in ids {
            if let Some(state) = i
                .remote
                .get(node)
                .or(i.local.get(node))
                .or(i.base.get(node))
            {
                self.merged.insert(node, state.clone());
            }
        }
    }

    fn content_state(&self, node: NodeId) -> ContentState {
        self.input
            .base_extra
            .get(&node)
            .map_or(ContentState::Local, |e| e.content_state)
    }

    /// The local state, with what the local side can't express filled in from the base: an
    /// online-only file has no local bytes but is still there.
    fn local_state(&self, node: NodeId) -> Option<NodeState> {
        let i = self.input;
        match i.local.get(node) {
            Some(l) => Some(l.clone()),
            None if self.content_state(node) == ContentState::OnlineOnly => {
                i.base.get(node).cloned()
            }
            None => None,
        }
    }

    fn local_change(&self, node: NodeId) -> Change {
        let i = self.input;
        let (b, l) = (i.base.get(node), self.local_state(node));
        match (b, &l) {
            (None, None) => Change::Absent,
            (None, Some(_)) => Change::Created,
            (Some(_), None) => Change::Deleted,
            (Some(b), Some(l)) => {
                let moved = !b.same_place(l);
                let edited = match (&b.content, &l.content) {
                    (Some(bc), Some(lc)) => {
                        let base_local = i.base_extra.get(&node).and_then(|e| e.local_hash);
                        let local_now = lc.hash;
                        let bytes_changed = match base_local {
                            Some(h) => h != local_now,
                            // Never had local bytes (online-only): unchanged unless replaced.
                            None => {
                                self.content_state(node) != ContentState::OnlineOnly
                                    && bc.hash != local_now
                            }
                        };
                        bytes_changed || bc.executable != lc.executable
                    }
                    _ => false,
                };
                if moved || edited {
                    Change::Changed { moved, edited }
                } else {
                    Change::Unchanged
                }
            }
        }
    }

    fn remote_change(&self, node: NodeId) -> Change {
        let i = self.input;
        match (i.base.get(node), i.remote.get(node)) {
            (None, None) => Change::Absent,
            (None, Some(_)) => Change::Created,
            (Some(_), None) => Change::Deleted,
            (Some(b), Some(r)) => {
                let moved = !b.same_place(r);
                let edited = match (&b.content, &r.content) {
                    (Some(bc), Some(rc)) => bc.hash != rc.hash || bc.executable != rc.executable,
                    _ => false,
                };
                if moved || edited {
                    Change::Changed { moved, edited }
                } else {
                    Change::Unchanged
                }
            }
        }
    }

    /// Leaves `node` for a later plan, keeping it in the merged tree where it is now, so that
    /// a new local file still being written keeps its folder alive (§6.3) and its name.
    fn defer(&mut self, node: NodeId) {
        self.deferred.insert(node);
        let i = self.input;
        if let Some(state) = i
            .remote
            .get(node)
            .or(i.base.get(node))
            .or(i.local.get(node))
        {
            self.merged.insert(node, state.clone());
        }
    }

    fn merge_node(&mut self, node: NodeId) {
        let i = self.input;
        if i.local_unhashed.contains(&node) {
            self.defer(node);
            return;
        }
        let (lc, rc) = (self.local_change(node), self.remote_change(node));
        let (b, l, r) = (i.base.get(node), self.local_state(node), i.remote.get(node));
        let (target, origin): (Option<NodeState>, Origin) = match (lc, rc) {
            // Gone from what this device syncs; what must stay here was detached (§10).
            _ if self.masks.masked.contains(&node) => (None, Origin::Base),
            (Change::Absent | Change::Unchanged, Change::Absent | Change::Unchanged) => {
                (r.or(l.as_ref()).or(b).cloned(), Origin::Base)
            }
            (Change::Deleted, Change::Deleted | Change::Unchanged)
            | (Change::Unchanged, Change::Deleted) => (None, Origin::Base),
            (Change::Deleted, Change::Created | Change::Changed { .. }) => {
                self.plan.conflicts.push(Conflict::EditDelete { node });
                (r.cloned(), Origin::Remote)
            }
            (Change::Created | Change::Changed { .. }, Change::Deleted) => {
                self.plan.conflicts.push(Conflict::EditDelete { node });
                (l.clone(), Origin::Local)
            }
            (lc, Change::Absent | Change::Unchanged) if lc.is_change() => (
                l.clone(),
                if content_changed(lc) {
                    Origin::Local
                } else {
                    Origin::Base
                },
            ),
            (Change::Absent | Change::Unchanged, rc) if rc.is_change() => (
                r.cloned(),
                if content_changed(rc) {
                    Origin::Remote
                } else {
                    Origin::Base
                },
            ),
            // Both sides changed (or both have a node the base doesn't know).
            (lc, rc) => {
                let (Some(l), Some(r)) = (l.as_ref(), r) else {
                    self.defer(node);
                    return;
                };
                self.merge_both(node, l, r, lc, rc)
            }
        };
        // A masked live child keeps its folder (§10): never delete what this device can't see.
        let (mut target, origin) = match (target, r) {
            (None, Some(r)) if self.masks.keep_alive.contains(&node) => {
                (Some(r.clone()), Origin::Base)
            }
            (target, _) => (target, origin),
        };
        // Tags merge on their own, whatever else changed (§11); a merge too large for the
        // drive leaves the drive's.
        if let (Some(t), Some(l), Some(r)) = (&mut target, &l, r)
            && !self.masks.masked.contains(&node)
        {
            let none = Tags::default();
            let merged = Tags::merge(b.map_or(&none, |b| &b.tags), &l.tags, &r.tags);
            t.tags = if merged.fits() {
                merged
            } else {
                r.tags.clone()
            };
        }
        self.scope.insert(node);
        self.origin.insert(node, origin);
        match target {
            Some(t) => {
                self.merged.insert(node, t);
            }
            None => {
                self.merged.remove(node);
            }
        }
    }

    /// Both sides changed: combine location and content.
    fn merge_both(
        &mut self,
        node: NodeId,
        l: &NodeState,
        r: &NodeState,
        lc: Change,
        rc: Change,
    ) -> (Option<NodeState>, Origin) {
        let (l_moved, l_edited) = match lc {
            Change::Changed { moved, edited } => (moved, edited),
            _ => (true, true), // created on both sides: every attribute may differ
        };
        let (r_moved, r_edited) = match rc {
            Change::Changed { moved, edited } => (moved, edited),
            _ => (true, true),
        };
        let mut target = r.clone();
        // A node brought back into view from where this device filters (§10) keeps its local
        // place: the remote one is filtered too, and would take it out of view again.
        if l_moved && (!r_moved || self.masks.brought_back.contains(&node)) {
            target.parent = l.parent;
            target.name = l.name.clone();
        } else if l_moved && r_moved && !l.same_place(r) {
            self.plan.conflicts.push(Conflict::MoveMove { node });
        }
        let mut origin = Origin::Remote;
        if l.kind == NodeKind::File {
            let same_edit = match (&l.content, &r.content) {
                (Some(lc), Some(rc)) => lc.hash == rc.hash && lc.executable == rc.executable,
                _ => false,
            };
            if l_edited && r_edited && !same_edit {
                // Keep both: the remote version keeps the node, the local bytes become a copy.
                let base_version = self.input.base_extra.get(&node).and_then(|e| e.version);
                let copy = fork_id(node, l.content.map(|c| c.hash), base_version);
                self.forks.insert(node, copy);
                self.plan.conflicts.push(Conflict::EditEdit { node, copy });
                let copy_state = NodeState {
                    parent: l.parent,
                    name: conflict_copy_name(&l.name, &self.opts.device, &self.opts.date),
                    kind: NodeKind::File,
                    content: l.content,
                    tags: l.tags.clone(),
                };
                self.merged.insert(copy, copy_state);
                self.scope.insert(copy);
                self.origin.insert(copy, Origin::Local);
            } else if l_edited && !r_edited {
                target.content = l.content;
                origin = Origin::Local;
            } else if same_edit || !r_edited {
                origin = Origin::Base;
            }
        }
        (Some(target), origin)
    }

    // ---- 3: invariants on the merged tree ----

    /// A repair puts `node` at `state` in the merged tree; ops follow for it.
    fn retarget(&mut self, node: NodeId, state: NodeState) {
        self.merged.insert(node, state);
        self.scope.insert(node);
        self.origin.entry(node).or_insert(Origin::Base);
    }

    /// The best known state of a node that the merge removed or never saw. A masked node has
    /// none: it can't come back here under its id.
    fn known_state(&self, node: NodeId) -> Option<NodeState> {
        if self.masks.masked.contains(&node) {
            return None;
        }
        let i = self.input;
        i.remote
            .get(node)
            .or(i.local.get(node))
            .or(i.base.get(node))
            .cloned()
    }

    fn fix_orphans(&mut self) {
        let mut queue: Vec<NodeId> = self.merged.ids().collect();
        queue.sort();
        // Nodes taken out below; their children follow them.
        let mut withdrawn: HashSet<NodeId> = HashSet::new();
        while let Some(node) = queue.pop() {
            let Some(parent) = self.merged.get(node).map(|s| s.parent) else {
                continue;
            };
            if self.merged.exists(parent) {
                continue;
            }
            let parent_state = if withdrawn.contains(&parent) {
                None
            } else {
                self.known_state(parent)
            };
            match parent_state {
                Some(state) if state.kind == NodeKind::Folder => {
                    // A live child keeps its folder alive (§6.3).
                    self.retarget(parent, state);
                    queue.push(parent);
                }
                None if self.input.remote_complete
                    && self.input.remote.contains(node)
                    && !withdrawn.contains(&parent) =>
                {
                    // Nobody knows the parent: the server kept a child of a folder deleted
                    // concurrently. Keep the data visible by moving it to the root.
                    if let Some(mut state) = self.merged.get(node).cloned() {
                        state.parent = NodeId::ROOT;
                        self.retarget(node, state);
                        self.plan.conflicts.push(Conflict::Orphan { node });
                    }
                }
                _ => {
                    // The parent is unknown (or not a folder, or itself waiting): wait for more
                    // information. Children already checked against this node are re-checked.
                    queue.extend(self.merged.children(node).collect::<Vec<_>>());
                    self.merged.remove(node);
                    self.scope.remove(&node);
                    withdrawn.insert(node);
                    if self.input.dirty.contains(&node) {
                        self.deferred.insert(node);
                    }
                }
            }
        }
    }

    fn fix_cycles(&mut self) {
        let candidates: Vec<NodeId> = self.scope.iter().copied().collect();
        for node in candidates {
            loop {
                let members = self.merged.cycle_members(node);
                if members.is_empty() {
                    break;
                }
                // Undo a local move first: the remote side has already published its moves.
                let local_moved = members.iter().copied().find(|m| {
                    self.input
                        .local
                        .get(*m)
                        .zip(self.merged.get(*m))
                        .is_some_and(|(l, t)| l.same_place(t))
                        && self
                            .input
                            .remote
                            .get(*m)
                            .zip(self.merged.get(*m))
                            .is_some_and(|(r, t)| !r.same_place(t))
                });
                let victim = local_moved.unwrap_or_else(|| *members.iter().min().unwrap_or(&node));
                let fallback = self
                    .input
                    .remote
                    .get(victim)
                    .or(self.input.base.get(victim))
                    .map(|s| (s.parent, s.name.clone()));
                let Some(mut state) = self.merged.get(victim).cloned() else {
                    break;
                };
                match fallback {
                    Some((parent, name)) if !self.merged.would_cycle(victim, parent) => {
                        state.parent = parent;
                        state.name = name;
                    }
                    _ => state.parent = NodeId::ROOT,
                }
                self.retarget(victim, state);
                self.plan.conflicts.push(Conflict::Cycle { node: victim });
            }
        }
    }

    fn fix_names(&mut self) {
        let parents: BTreeSet<NodeId> = self
            .scope
            .iter()
            .filter_map(|n| self.merged.get(*n).map(|s| s.parent))
            .collect();
        for parent in parents {
            let mut by_key: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
            for child in self.merged.children(parent) {
                if let Some(s) = self.merged.get(child) {
                    by_key.entry(name_key(&s.name)).or_default().push(child);
                }
            }
            // Names held remotely by nodes this device doesn't sync (§10) are taken: their
            // holders keep them, and every node here loses.
            let masked = self.masks.names.get(&parent);
            let taken = |key: &str| masked.is_some_and(|m| m.contains(key));
            for (key, mut group) in by_key {
                let keep = usize::from(!taken(&key));
                if group.len() <= keep {
                    continue;
                }
                // The lowest id keeps the name, except that a deferred node (a file still
                // being written) is never renamed: it wins, and is left alone if it loses.
                group.sort_by_key(|n| (!self.deferred.contains(n), *n));
                for loser in group.into_iter().skip(keep) {
                    if self.deferred.contains(&loser) {
                        continue;
                    }
                    let Some(mut state) = self.merged.get(loser).cloned() else {
                        continue;
                    };
                    // Unbounded: some numbered name is always free.
                    let candidates = (1..).map(|n| numbered_conflict_name(&state.name, n));
                    let free = |c: &str| {
                        Ok::<_, Infallible>(
                            self.merged.child_named(parent, c, Some(loser)).is_none()
                                && !taken(&name_key(c)),
                        )
                    };
                    let Ok(Some(new_name)) = first_free(candidates, free, |_| true) else {
                        continue;
                    };
                    state.name = new_name;
                    self.retarget(loser, state);
                    self.plan
                        .conflicts
                        .push(Conflict::NameClash { node: loser });
                }
            }
        }
    }

    // ---- 4: operations ----

    fn emit(&mut self) {
        let i = self.input;
        let mut forks = Vec::new();
        let mut creates = Vec::new(); // (depth, op)
        let mut moves = Vec::new();
        let mut content = Vec::new();
        let mut deletes = Vec::new(); // (depth, op)
        let mut settles = Vec::new();

        let scope: Vec<NodeId> = self.scope.iter().copied().collect();
        let fork_copies: HashSet<NodeId> = self.forks.values().copied().collect();
        for node in scope {
            let target = self.merged.get(node).cloned();
            let forked = self.forks.get(&node).copied();
            let local = if forked.is_some() {
                None
            } else {
                self.local_state(node)
            };
            let local_real = i.local.get(node);
            let remote = i.remote.get(node);
            let depth = |tree: &Tree| tree.ancestors(node).len();

            if let Some(copy) = forked {
                let name = self
                    .merged
                    .get(copy)
                    .map(|s| s.name.clone())
                    .unwrap_or_default();
                forks.push(Op::LocalFork {
                    node,
                    new_node: copy,
                    name,
                });
            }

            // Remote side.
            let mut remote_ops = false;
            match (&target, remote) {
                (None, Some(_)) => {
                    if let Some(r) = i.remote_refs.get(&node) {
                        deletes.push((
                            depth(&i.remote),
                            Op::RemoteDelete {
                                node,
                                file_id: r.file_id,
                                expected: r.version,
                            },
                        ));
                        remote_ops = true;
                    }
                }
                (Some(t), None) => {
                    let has_bytes = t.kind == NodeKind::Folder
                        || fork_copies.contains(&node)
                        || local.as_ref().is_some_and(|l| l.same_content(t));
                    if has_bytes {
                        creates.push((
                            self.merged.ancestors(node).len(),
                            Op::RemoteCreate {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                kind: t.kind,
                                tags: t.tags.clone(),
                            },
                        ));
                        remote_ops = true;
                    } else {
                        self.deferred.insert(node);
                    }
                }
                (Some(t), Some(r)) => {
                    let upload = self.origin.get(&node) == Some(&Origin::Local)
                        && t.kind == NodeKind::File
                        && !self.remote_has(r, t);
                    let retag = t.tags != r.tags;
                    if let Some(rr) = i.remote_refs.get(&node) {
                        if !t.same_place(r) || upload {
                            let op = Op::RemoteUpdate {
                                node,
                                file_id: rr.file_id,
                                expected: rr.version,
                                parent: t.parent,
                                name: t.name.clone(),
                                upload,
                                tags: t.tags.clone(),
                            };
                            if upload {
                                content.push(op)
                            } else {
                                moves.push(op)
                            }
                            remote_ops = true;
                        } else if retag {
                            content.push(Op::RemoteTags {
                                node,
                                file_id: rr.file_id,
                                expected: rr.version,
                                tags: t.tags.clone(),
                            });
                            remote_ops = true;
                        }
                    }
                }
                (None, None) => {}
            }

            // Local side (a fork copy already exists locally).
            let mut local_ops = false;
            if !fork_copies.contains(&node) {
                match (&target, local_real.filter(|_| forked.is_none())) {
                    (None, Some(_)) => {
                        let mut subtree = i.local.subtree(node);
                        subtree.sort();
                        let op = if self.masks.masked.contains(&node) {
                            Op::Forget { node, subtree }
                        } else {
                            Op::LocalTrash { node, subtree }
                        };
                        deletes.push((depth(&i.local), op));
                        local_ops = true;
                    }
                    (Some(t), None) => {
                        if t.kind == NodeKind::Folder {
                            creates.push((
                                self.merged.ancestors(node).len(),
                                Op::LocalMkdir {
                                    node,
                                    parent: t.parent,
                                    name: t.name.clone(),
                                    tags: t.tags.clone(),
                                },
                            ));
                            local_ops = true;
                        } else if self.content_state(node) != ContentState::OnlineOnly {
                            content.push(Op::LocalDownload {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                replace: false,
                                tags: t.tags.clone(),
                            });
                            local_ops = true;
                        }
                    }
                    (Some(t), Some(l)) => {
                        let download = t.kind == NodeKind::File
                            && self.origin.get(&node) == Some(&Origin::Remote)
                            && !self.local_matches_remote(node, l, t);
                        if download {
                            // Downloading to the new place moves and updates in one step.
                            content.push(Op::LocalDownload {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                replace: true,
                                tags: t.tags.clone(),
                            });
                            local_ops = true;
                        } else {
                            if !t.same_place(l) {
                                moves.push(Op::LocalMove {
                                    node,
                                    parent: t.parent,
                                    name: t.name.clone(),
                                });
                                local_ops = true;
                            }
                            if t.tags != l.tags {
                                content.push(Op::LocalTags {
                                    node,
                                    tags: t.tags.clone(),
                                });
                                local_ops = true;
                            }
                        }
                    }
                    (None, None) => {}
                }
            }

            if !remote_ops && !local_ops && !self.deferred.contains(&node) {
                let needs_settle = match (&target, i.base.get(node)) {
                    (Some(t), Some(b)) => t != b || self.base_local_hash_stale(node, t),
                    (Some(_), None) | (None, Some(_)) => true,
                    (None, None) => false,
                };
                if needs_settle && target.is_none() && self.masks.masked.contains(&node) {
                    // A detached node's base goes with the detach.
                    if !self.masks.detached.contains_key(&node) {
                        let op = Op::Forget {
                            node,
                            subtree: Vec::new(),
                        };
                        deletes.push((depth(&i.base), op));
                    }
                } else if needs_settle {
                    settles.push(Op::Settle { node });
                } else if i.dirty.contains(&node) {
                    self.plan.clean.push(node);
                }
            }
        }

        // Parents before children for creates, all folders before the files (which the executor
        // uploads together on its pool, until a folder create stops it); children before parents
        // for deletes; only the topmost local trash of a subtree is needed.
        let uploads = |op: &Op| {
            matches!(
                op,
                Op::RemoteCreate {
                    kind: NodeKind::File,
                    ..
                }
            )
        };
        creates.sort_by_key(|(d, op)| (uploads(op), *d, op.node()));
        deletes.sort_by_key(|(d, op)| (std::cmp::Reverse(*d), op.node()));
        let trashed: HashSet<NodeId> = deletes.iter().filter_map(|(_, op)| op.trashes()).collect();
        let deletes: Vec<Op> = deletes
            .into_iter()
            .map(|(_, op)| op)
            .filter(|op| match op.trashes() {
                Some(node) => !i.local.ancestors(node).iter().any(|a| trashed.contains(a)),
                None => true,
            })
            .collect();

        self.plan.ops.extend(
            self.masks
                .detached
                .iter()
                .map(|(&node, &new_node)| Op::LocalDetach { node, new_node }),
        );
        self.plan.ops.extend(forks);
        self.plan.ops.extend(creates.into_iter().map(|(_, op)| op));
        self.plan.ops.extend(moves);
        self.plan.ops.extend(content);
        self.plan.ops.extend(deletes);
        self.plan.ops.extend(settles);
    }

    /// Whether the remote already holds the target content (same algorithm, same hash).
    fn remote_has(&self, r: &NodeState, t: &NodeState) -> bool {
        match (&r.content, &t.content) {
            (Some(rc), Some(tc)) => rc.hash == tc.hash && rc.executable == tc.executable,
            _ => true,
        }
    }

    /// Whether the local file already holds the target content (no download needed).
    fn local_matches_remote(&self, node: NodeId, l: &NodeState, t: &NodeState) -> bool {
        let (Some(lc), Some(tc)) = (&l.content, &t.content) else {
            return true;
        };
        if lc.executable != tc.executable {
            return false;
        }
        if lc.hash.algorithm == tc.hash.algorithm {
            return lc.hash == tc.hash;
        }
        // Different algorithms: the local bytes are the target if they are the base's bytes
        // and the base already had the target's remote hash.
        let base = self.input.base.get(node).and_then(|b| b.content);
        let base_local = self.input.base_extra.get(&node).and_then(|e| e.local_hash);
        base.is_some_and(|b| b.hash == tc.hash) && base_local == Some(lc.hash)
    }

    fn base_local_hash_stale(&self, node: NodeId, t: &NodeState) -> bool {
        let (Some(l), Some(_)) = (
            self.input.local.get(node).and_then(|l| l.content),
            t.content,
        ) else {
            return false;
        };
        self.input.base_extra.get(&node).and_then(|e| e.local_hash) != Some(l.hash)
    }

    // ---- 5: mass-delete guard ----

    fn guard_mass_delete(&mut self) {
        if self.opts.allow_mass_delete {
            return;
        }
        let i = self.input;
        let files_under = |tree: &Tree, node: NodeId| {
            tree.subtree(node)
                .into_iter()
                .filter(|n| tree.get(*n).is_some_and(|s| s.kind == NodeKind::File))
                .count()
        };
        // Deletes the user confirmed are theirs: only the others are judged.
        let confirmed = |op: &Op| self.opts.confirmed.contains(&op.node());
        let (mut local_files, mut remote_files) = (0usize, 0usize);
        for op in self.plan.ops.iter().filter(|op| !confirmed(op)) {
            match op {
                Op::RemoteDelete { node, .. }
                    if i.remote
                        .get(*node)
                        .is_some_and(|s| s.kind == NodeKind::File) =>
                {
                    remote_files += 1
                }
                _ => {
                    if let Some(node) = op.trashes() {
                        local_files += files_under(&i.local, node);
                    }
                }
            }
        }
        let synced = self.opts.synced_files;
        let limit = self
            .opts
            .mass_delete_min
            .max((synced as f64 * self.opts.mass_delete_fraction) as usize);
        let trips = |n: usize| n > limit || (synced > 0 && n >= synced && n > 1);
        let (gone_locally, gone_remotely) = self.opts.pending_deletes;
        // Trashing locally follows remote deletes; remote deletes follow local ones.
        let hold_local = trips(local_files) || trips(gone_remotely);
        let hold_remote = trips(remote_files) || trips(gone_locally);
        if !hold_local && !hold_remote {
            return;
        }
        let ops = std::mem::take(&mut self.plan.ops);
        for op in ops {
            let held = !confirmed(&op)
                && match op {
                    Op::RemoteDelete { .. } => hold_remote,
                    _ => op.trashes().is_some() && hold_local,
                };
            if held {
                self.plan.held.push(op);
            } else {
                self.plan.ops.push(op);
            }
        }
    }
}

/// A deterministic id for the conflict copy of `node`'s local bytes. The same conflict
/// (same bytes, same base version) gets the same id when it is replanned after a crash; a later
/// conflict, even with identical bytes, has another base version and gets another id.
fn fork_id(
    node: NodeId,
    local_hash: Option<ContentHash>,
    base_version: Option<VersionTag>,
) -> NodeId {
    let mut bytes = *node.0.as_bytes();
    if let Some(h) = local_hash {
        for (b, x) in bytes.iter_mut().zip(h.bytes) {
            *b ^= x;
        }
    }
    if let Some(v) = base_version {
        for (b, x) in bytes.iter_mut().zip(v.0.as_bytes().iter().rev()) {
            *b = b.rotate_left(3) ^ x;
        }
    }
    bytes[0] ^= 0xA5;
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // keep it a well-formed v4 UUID
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    NodeId(hbsync_types::Uuid::from_bytes(bytes))
}

fn content_changed(c: Change) -> bool {
    matches!(c, Change::Created | Change::Changed { edited: true, .. })
}

/// [`reconcile`], planned again while a detach gives an id `known` says is in use (the same
/// node detached here before, its first copy still around): each time with that id taken.
pub fn reconcile_with_known(
    input: &Input,
    opts: &Options,
    mut known: impl FnMut(NodeId) -> bool,
) -> Plan {
    let mut plan = reconcile(input, opts);
    let mut opts = opts.clone();
    // Bounded: each round takes at least one more id.
    for _ in 0..8 {
        let taken: Vec<NodeId> = plan
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::LocalDetach { new_node, .. } => Some(*new_node),
                _ => None,
            })
            .filter(|n| !opts.taken.contains(n) && known(*n))
            .collect();
        if taken.is_empty() {
            break;
        }
        let mut all = (*opts.taken).clone();
        all.extend(taken);
        opts.taken = Arc::new(all);
        plan = reconcile(input, &opts);
    }
    plan
}
