//! The reconciler: a pure function from (base, local, remote) to a plan of operations.
//!
//! Spec: `docs/spec/sync-semantics.md`. Steps:
//! 1. classify each node's local and remote change against the base (§2), with what a folder's
//!    removal takes along counted as deleted (§6.3);
//! 2. merge per node into a target state (§4), forking a conflict copy for edit/edit (§5),
//!    and the tags three-way on their own (§11);
//! 3. repair the merged tree: orphans, cycles, duplicate names (§6);
//! 4. diff the merged tree against each side into operations, ordered so parents exist
//!    before children, removals on the drive run top-down and deletes here bottom-up (§8);
//! 5. hold deletes that trip the mass-delete guard (§7).
//!
//! Content comparisons stay within one side's representation: local hashes (BLAKE3) are
//! compared with the base's local hash, remote hashes with the base's remote hash. A node
//! written by another client with SHA-256 therefore never needs a cross-algorithm comparison,
//! except to tell whether a concurrent edit on both sides was the same edit (if it can't tell,
//! it keeps both versions).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use hbsync_platform::name::name_key;
use hbsync_types::{ContentState, FileContent, NodeId, NodeKind, NodeState, Tags, VersionTag};

use crate::mask::Masks;
use crate::tree::{Tree, conflict_copy_name, numbered_conflict_name};

mod emit;
mod guard;
pub(crate) mod merge;
mod repair;

pub use crate::plan::{
    Agreement, BaseExtra, Conflict, Coverage, DeleteGuard, DriveVersion, Input, Masking, Op,
    Options, Plan, Removed, Requirement,
};

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

/// The drive's names `plan`'s held removals keep ([`Holds::kept`]), by folder, where
/// something else here has one of them: none, most of the time.
fn kept_names(view: &Input, plan: &Plan) -> HashMap<NodeId, HashSet<String>> {
    let held: HashSet<NodeId> = plan
        .held
        .iter()
        .filter_map(|op| match op {
            Op::RemoteDelete { node, .. } => Some(*node),
            _ => None,
        })
        .collect();
    let mut kept: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for node in &held {
        if let Some(r) = view.remote.get(*node) {
            kept.entry(r.parent).or_default().insert(name_key(&r.name));
        }
    }
    kept.retain(|parent, keys| {
        view.local.children(*parent).any(|c| {
            !held.contains(&c)
                && view
                    .local
                    .get(c)
                    .is_some_and(|l| keys.contains(&name_key(&l.name)))
        })
    });
    kept
}

pub fn reconcile(input: &Input, opts: &Options, masking: Masking<'_>) -> Plan {
    let masks = Masks::of(input, masking, &opts.computer, &opts.taken);
    let view = masks.input.as_ref().unwrap_or(input);
    // What would take a name held by what waits for a hash waits too (§6.3), where it is,
    // with its folders: planned again holding it, till no more come.
    // Ends: each plan again holds at least one node more.
    let taken = merge::taken_along(view);
    let mut holds = Holds::of(view, opts.own_page);
    // And this device's removals from the drive held for the user (§7): their names there
    // stay taken meanwhile, so what would take one here gets a numbered name: planned again
    // once, only when something here has such a name (hardly ever).
    let mut kept_judged = false;
    // What waits to be planned with another page's nodes, judged before it waited.
    let mut together: Vec<Vec<NodeId>> = Vec::new();
    let mut plan = loop {
        let r = Reconciler::new(view, opts, masking, &masks, &holds, taken.clone());
        let (mut plan, waiters, named, placed) = r.run();
        together.append(&mut plan.waits_together);
        holds.named = named;
        let new: Vec<NodeId> = waiters
            .into_iter()
            .filter(|n| !holds.waits.contains(n))
            .collect();
        // Placements that clash across pages: held as they are, with their folders.
        let placed: Vec<NodeId> = placed
            .into_iter()
            .filter(|n| !holds.held.contains(n))
            .collect();
        for &node in &placed {
            holds.held.extend(held_from(view, node));
            holds.held.insert(node);
            holds.paged.insert(node);
        }
        if new.is_empty() && !placed.is_empty() {
            continue;
        }
        if new.is_empty() {
            if kept_judged {
                break plan;
            }
            kept_judged = true;
            holds.kept = kept_names(view, &plan);
            if holds.kept.is_empty() {
                break plan;
            }
            continue;
        }
        for node in new {
            holds.held.extend(held_from(view, node));
            holds.held.insert(node);
            holds.waits.insert(node);
        }
    };
    together.sort();
    together.dedup();
    plan.waits_together = together;
    // What a masked node does to its folder (keeps it, takes its name, leaves with it) is
    // judged with all of the folder's children.
    if let Some(coverage) = &input.coverage {
        plan.uncovered.extend(
            masks
                .masked
                .iter()
                .filter_map(|m| input.remote.get(*m).or(input.base.get(*m)))
                .map(|s| s.parent)
                .filter(|p| !coverage.full.contains(p)),
        );
        // And so is what another writer put in a file on the drive (§6.3: a node in a file;
        // a file this device holds for a version it can't read, #175): any of it may keep it
        // (a masked one), so all of it is read. Hardly ever any.
        plan.uncovered
            .extend(input.remote.iter().map(|(_, s)| s.parent).filter(|p| {
                input
                    .remote
                    .get(*p)
                    .is_some_and(|f| f.kind != NodeKind::Folder)
                    && !coverage.full.contains(p)
            }));
    }
    plan
}

/// Whether `node` is a file here not hashed yet (§6.3), which every view loads
/// (`ReadTx::unhashed_files`). A row the scan has no state for yet (its folder unrecorded, a
/// name the drive can't hold) is only deferred: it holds nothing.
fn is_unhashed_file(input: &Input, node: NodeId) -> bool {
    input.local_unhashed.contains(&node) && input.local.contains(node)
}

/// Rounds of orphan and cycle repair at most (each round ends at least one cycle; a real tree
/// needs one or two).
const MAX_REPAIRS: usize = 64;

struct Reconciler<'a> {
    /// The input with the masks applied.
    input: &'a Input,
    opts: &'a Options,
    /// What the folder leaves out ([`Masking`]): the masks come from it; Recovered files reads it.
    masking: Masking<'a>,
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
    /// Orphans the repairs move into Recovered files (§6.3): they lose a name clash there.
    adopted: HashSet<NodeId>,
    /// Remote nodes a folder's removal takes along ([`merge::taken_along`]).
    taken: HashSet<NodeId>,
    /// Nodes held as files not hashed yet are ([`Holds::waits`]).
    waits: &'a BTreeSet<NodeId>,
    /// What waits for files here not hashed yet, or for `waits` ([`Holds::held`]).
    held: &'a HashSet<NodeId>,
    /// The folders this page waits for ([`Holds::going`]).
    going: &'a [NodeId],
    /// Of `held`, what waits for them ([`Holds::paged`]).
    paged: &'a HashSet<NodeId>,
    /// What of this plan waits to be placed with another page's nodes
    /// ([`Reconciler::places_held_elsewhere`]): planned again held as it is.
    placed: BTreeSet<NodeId>,
    /// What is dirty in another page (`Input::dirty_elsewhere`): context as it is, its change
    /// unmerged here, so no repair of this plan moves it, renames it or adopts it (it keeps
    /// its name against this page's nodes); its own page decides it.
    elsewhere: &'a HashSet<NodeId>,
    /// Each file not hashed yet, with the folders it holds.
    held_by: &'a [(NodeId, Vec<NodeId>)],
    /// Names the held removals keep on the drive ([`Holds::kept`]).
    kept: &'a HashMap<NodeId, HashSet<String>>,
    /// What waited for a name this plan, with what is below it ([`Reconciler::fix_names`]).
    waiters: BTreeSet<NodeId>,
    /// Held nodes whose names something else waits for ([`Reconciler::fix_names`]).
    name_waits: HashSet<NodeId>,
    plan: Plan,
}

/// What waits for files here not hashed yet (being written, locked, unreadable, a placement a
/// crash left unread; sync semantics §6.3), from `seed`: every folder it is in here, on the
/// drive or in the base, and every folder those are in, on any side. Nothing is done to such a
/// file till it is hashed, nor to such a folder that its plan would delete, restore or move on
/// either side (deferred, not undone); a node that would take a name one of them holds waits
/// too, and the file is never an orphan to adopt.
pub(crate) fn held_from(input: &Input, seed: NodeId) -> Vec<NodeId> {
    let mut held = Vec::new();
    let mut seen = HashSet::from([seed]);
    let mut queue = vec![seed];
    while let Some(node) = queue.pop() {
        for tree in [&input.local, &input.remote, &input.base] {
            if let Some(s) = tree.get(node)
                && !s.parent.is_root()
                && seen.insert(s.parent)
            {
                held.push(s.parent);
                queue.push(s.parent);
            }
        }
    }
    held
}

/// What waits for a hash as a plan starts (§6.3, [`held_from`]): the same for every plan of an
/// input but for what waits for a name, which grows.
struct Holds {
    /// Each file not hashed yet, with the folders it holds.
    by_file: Vec<(NodeId, Vec<NodeId>)>,
    /// What would take a name what waits holds (an earlier plan's waiters, [`reconcile`]):
    /// held as such a file is.
    waits: BTreeSet<NodeId>,
    /// All of those, and what they hold.
    held: HashSet<NodeId>,
    /// What holds the names waited for.
    named: HashSet<NodeId>,
    /// The drive's names of what this device's held removals keep there (§7), by folder.
    kept: HashMap<NodeId, HashSet<String>>,
    /// The folders that wait for other pages, planned with their subtrees as pages of their
    /// own (`Plan::waits_for_pages`).
    going: Vec<NodeId>,
    /// Of `held`, what waits for `going`: those, the folders above them and what of the page
    /// is below them.
    paged: HashSet<NodeId>,
}

impl Holds {
    fn of(input: &Input, own_page: bool) -> Self {
        let mut by_file: Vec<(NodeId, Vec<NodeId>)> = input
            .local_unhashed
            .iter()
            .filter(|n| is_unhashed_file(input, **n))
            .map(|&node| (node, held_from(input, node)))
            .collect();
        by_file.sort();
        let mut held: HashSet<NodeId> = by_file
            .iter()
            .flat_map(|(node, on_paths)| on_paths.iter().copied().chain([*node]))
            .collect();
        // A folder that may go with something to plan below it in another page, or above the
        // page's nodes and planned in another page, waits for it, held as it is with the
        // folders above it and everything of the page below it, as a file not hashed yet
        // holds its folders: alive as context, what is below unmerged kept a deleted folder
        // alive (review 14 R1), the folder's trash took along what changed there, or its
        // items went one by one before it. The engine plans it with its whole dirty subtree
        // as a page of its own next (`pages::OwnPages`). (What is dirty in another page is
        // never moved by this plan's repairs: `Reconciler::elsewhere`.)
        // A folder of the page below one another page plans waits for that one alone: its
        // page covers both (a piece of a split doesn't wait for the folder above it).
        let above: HashSet<NodeId> = match own_page {
            true => HashSet::new(),
            false => input.going_above.clone(),
        };
        let mut going: Vec<NodeId> = input
            .going
            .iter()
            .copied()
            .filter(|g| !held_from(input, *g).iter().any(|a| above.contains(a)))
            .chain(above.iter().copied())
            .collect();
        going.sort();
        going.dedup();
        let mut paged = HashSet::new();
        for &folder in &going {
            paged.insert(folder);
            paged.extend(held_from(input, folder));
            for tree in [&input.base, &input.local, &input.remote] {
                for node in tree.subtree(folder) {
                    // With the folders it is in now, on every side (moved out of it, say).
                    if input.dirty.contains(&node) && paged.insert(node) {
                        paged.extend(held_from(input, node));
                    }
                }
            }
        }
        held.extend(&paged);
        Holds {
            by_file,
            waits: BTreeSet::new(),
            held,
            named: HashSet::new(),
            kept: HashMap::new(),
            going,
            paged,
        }
    }
}

impl<'a> Reconciler<'a> {
    fn new(
        input: &'a Input,
        opts: &'a Options,
        masking: Masking<'a>,
        masks: &'a Masks,
        holds: &'a Holds,
        taken: HashSet<NodeId>,
    ) -> Self {
        Reconciler {
            input,
            opts,
            masking,
            masks,
            merged: Tree::new(),
            scope: BTreeSet::new(),
            origin: HashMap::new(),
            forks: BTreeMap::new(),
            deferred: BTreeSet::new(),
            adopted: HashSet::new(),
            taken,
            waits: &holds.waits,
            held: &holds.held,
            going: &holds.going,
            paged: &holds.paged,
            elsewhere: &input.dirty_elsewhere,
            placed: BTreeSet::new(),
            held_by: &holds.by_file,
            kept: &holds.kept,
            waiters: BTreeSet::new(),
            name_waits: holds.named.clone(),
            plan: Plan::default(),
        }
    }

    /// The plan, what waited for a name in it, and what held the names (with an earlier
    /// plan's).
    fn run(mut self) -> (Plan, BTreeSet<NodeId>, HashSet<NodeId>, BTreeSet<NodeId>) {
        self.build_context();
        let dirty: Vec<NodeId> = self.input.dirty.iter().copied().collect();
        for node in dirty {
            self.merge_node(node);
        }
        self.fix_orphans();
        // Undoing a move can put a node back into a folder deleted meanwhile (a new orphan,
        // whose folder its live child keeps alive), and keeping a folder can close a cycle:
        // both again until neither changes anything.
        for _ in 0..MAX_REPAIRS {
            if !self.fix_cycles() {
                break;
            }
            self.fix_orphans();
        }
        debug_assert!(
            self.scope.iter().all(|&n| {
                self.merged.cycle_members(n).is_empty()
                    && self
                        .merged
                        .get(n)
                        .is_none_or(|s| self.merged.exists(s.parent))
            }),
            "the repairs left a cycle or an orphan"
        );
        // A node held where it is for a name may hold the name another would take: again,
        // till none more waits (each pass holds at least one node more).
        loop {
            let waited = self.waiters.len();
            self.fix_names();
            if self.waiters.len() == waited {
                break;
            }
        }
        self.emit();
        self.places_held_elsewhere();
        self.guard_mass_delete();
        self.plan.waiting = self.waiting();
        // The folders this page waits for, with everything of it below them.
        self.plan.waits_for_pages = self.going.iter().map(|n| self.masks.store_id(*n)).collect();
        self.plan.deferred = self.deferred.into_iter().collect();
        (self.plan, self.waiters, self.name_waits, self.placed)
    }

    /// Where this plan puts a node into a folder another page plans that isn't on that side
    /// yet, or onto a name such a node's item holds there now (it moves away in its own
    /// page): the two wait to be planned together, as a page of their own next
    /// (`Plan::waits_together`), each alone failing for the other (a swap of places). So does
    /// what goes into a folder that isn't on that side and that this plan doesn't make (held
    /// for another page, or for such a wait). The node is planned again held as it is (`placed`): nothing sent to
    /// fail meanwhile.
    fn places_held_elsewhere(&mut self) {
        if self.elsewhere.is_empty() {
            return;
        }
        let i = self.input;
        let made = |local: bool| -> HashSet<NodeId> {
            self.plan
                .ops
                .iter()
                .filter_map(|op| match op {
                    Op::LocalMkdir { node, .. } if local => Some(*node),
                    Op::RemoteCreate { node, .. } if !local => Some(*node),
                    _ => None,
                })
                .collect()
        };
        let (made_here, made_there) = (made(true), made(false));
        let mut together = Vec::new();
        for op in &self.plan.ops {
            let (node, parent, name, side) = match op {
                Op::LocalMkdir {
                    node, parent, name, ..
                }
                | Op::LocalMove {
                    node, parent, name, ..
                }
                | Op::LocalDownload {
                    node, parent, name, ..
                } => (*node, *parent, name, (&i.local, &made_here)),
                Op::RemoteCreate {
                    node, parent, name, ..
                }
                | Op::RemoteUpdate {
                    node, parent, name, ..
                } => (*node, *parent, name, (&i.remote, &made_there)),
                _ => continue,
            };
            let (side, made) = side;
            let mut with: Vec<NodeId> = Vec::new();
            if (self.elsewhere(parent) || self.paged.contains(&parent))
                && !side.contains(parent)
                && !made.contains(&parent)
            {
                with.push(parent);
            }
            if let Some(occupant) = side.child_named(parent, name, Some(node))
                && self.elsewhere(occupant)
            {
                with.push(occupant);
            }
            if !with.is_empty() {
                with.push(node);
                together.push(with);
            }
        }
        for group in together {
            self.placed
                .extend(group.iter().filter(|n| self.scope.contains(n)));
            self.wait_together(group);
        }
    }

    /// Whether `node` is planned in another page, not in this one (`Input::dirty_elsewhere`):
    /// no fix here moves, renames or adopts it, and what it blocks waits to be planned with it
    /// ([`Reconciler::wait_together`]).
    fn elsewhere(&self, node: NodeId) -> bool {
        self.elsewhere.contains(&node) && !self.scope.contains(&node)
    }

    /// `nodes` wait to be planned together, as a page of their own next
    /// (`Plan::waits_together`): one of them is planned in another page.
    fn wait_together(&mut self, nodes: Vec<NodeId>) {
        let mut nodes: Vec<NodeId> = nodes.into_iter().map(|n| self.masks.store_id(n)).collect();
        nodes.sort();
        nodes.dedup();
        self.plan.waits_together.push(nodes);
    }

    /// Whether `node` waits for a hash as it is, whatever its plan: a file not hashed yet, or
    /// what would take a name one holds.
    fn held_as_is(&self, node: NodeId) -> bool {
        is_unhashed_file(self.input, node) || self.waits.contains(&node)
    }

    /// The files here not hashed yet that something else waits for ([`Plan::waiting`]), by
    /// their ids in the store.
    fn waiting(&self) -> Vec<NodeId> {
        let waits = |n: &NodeId| self.deferred.contains(n) || self.name_waits.contains(n);
        self.held_by
            .iter()
            .filter(|(node, on_paths)| {
                self.remote_change(*node).is_change()
                    || self.name_waits.contains(node)
                    || on_paths.iter().any(waits)
            })
            // As the store knows it: a detached item's old id (§10).
            .map(|(node, _)| self.masks.store_id(*node))
            .collect()
    }
}

/// A deterministic id for the conflict copy of `node`'s local bytes made on `computer`
/// ([`Options::computer`]). The same conflict (same bytes and executable bit, same base
/// version) gets the same id when it is replanned on this computer after a crash; a later
/// conflict, even with identical bytes, has another base version and gets another id. Another
/// computer's copy of the same conflict has another id: two computers never share a copy (with
/// the same bytes, the drive gets two).
fn fork_id(
    node: NodeId,
    local: Option<FileContent>,
    base_version: Option<VersionTag>,
    computer: &str,
) -> NodeId {
    let hash = local.map(|c| c.hash.bytes);
    let executable = [u8::from(local.is_some_and(|c| c.executable))];
    let version = base_version.map(|v| *v.0.as_bytes());
    crate::id::derived_id(
        &[
            node.0.as_bytes(),
            hash.as_ref().map_or(&[], |h| &h[..]),
            &executable,
            version.as_ref().map_or(&[], |v| &v[..]),
            computer.as_bytes(),
        ],
        b"fork",
    )
}

fn content_changed(c: Change) -> bool {
    matches!(c, Change::Created | Change::Changed { edited: true, .. })
}

/// [`reconcile`], planned again while a detach gives an id `known` says is in use (the same
/// node detached here before, its first copy still around): each time with that id taken.
pub fn reconcile_with_known(
    input: &Input,
    opts: &Options,
    masking: Masking<'_>,
    mut known: impl FnMut(NodeId) -> bool,
) -> Plan {
    let mut plan = reconcile(input, opts, masking);
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
        plan = reconcile(input, &opts, masking);
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Conflict copy ids are stored and on the drive: the same conflict on the same computer
    /// keeps its id, release after release.
    #[test]
    fn fork_ids_are_pinned() {
        let uuid = |s: &str| s.parse::<hbsync_types::Uuid>().expect("uuid");
        let content = FileContent {
            hash: hbsync_fileio::hash_bytes(b"mine", hbsync_fileio::HashAlgorithm::Blake3),
            size: 4,
            mtime_ms: 0,
            executable: false,
        };
        let id = fork_id(
            NodeId(uuid("0274575b-c2cf-4ac7-b39b-d56b2c121b75")),
            Some(content),
            Some(VersionTag(uuid("9b2f4c1e-0d7a-4e3b-8c55-2a6f1e9d0b47"))),
            "0123456789abcdef0123456789abcdef",
        );
        assert_eq!(id.0.to_string(), "703c54e5-af51-4afd-a3cf-21549310332c");
    }
}
