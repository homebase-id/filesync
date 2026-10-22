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

mod coverage;
mod emit;
mod guard;
pub(crate) mod merge;
mod repair;
mod waits;

use waits::Waits;

pub use crate::plan::{
    Agreement, BaseExtra, Conflict, Coverage, DeleteGuard, DriveVersion, Guarding, Input, Masking,
    Op, Options, Plan, Removed, Requirement,
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

pub fn reconcile(input: &Input, opts: &Options, masking: Masking<'_>) -> Plan {
    let masks = Masks::of(input, masking, &opts.computer, &opts.taken);
    let view = masks.input.as_ref().unwrap_or(input);
    let taken = merge::taken_along(view);
    // What would take a name one that waits for a hash holds waits too (§6.3), where it is,
    // with its folders: planned again with it waiting, till no more come.
    // Ends: in each plan again at least one node more waits.
    let mut waits = Waits::of(view, opts.own_page);
    // And this device's removals from the drive held for the user (§7): their names there
    // stay taken meanwhile, so what would take one here gets a numbered name: planned again
    // once, only when something here has such a name (hardly ever).
    let mut kept = HashMap::new();
    // What waits to be planned with another page's nodes, judged before it waited.
    let mut together: Vec<Vec<NodeId>> = Vec::new();
    let mut plan = loop {
        let r = Reconciler::new(view, opts, masking, &masks, &waits, &kept, taken.clone());
        let (mut plan, waiters, name_waits, placed) = r.run();
        together.append(&mut plan.waits_together);
        waits.name_waits = name_waits;
        // Placements that clash across pages, and what would take a name what waits holds:
        // they wait as they are, with their folders. Planned again while more come.
        let mut more = false;
        for node in placed {
            more |= waits.add_placed(view, node);
        }
        for node in waiters {
            more |= waits.add_name_taker(view, node);
        }
        if more {
            continue;
        }
        // Judged once: planned again only for names kept.
        if !kept.is_empty() {
            break plan;
        }
        kept = guard::kept_names(view, &plan);
        if kept.is_empty() {
            break plan;
        }
    };
    together.sort();
    together.dedup();
    plan.waits_together = together;
    // What the plan needs more of, from a narrow view (`Plan::uncovered`'s producers are
    // listed in `coverage`; `fix_names` adds its own).
    coverage::masked_and_in_files(input, &masks, &mut plan);
    plan
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
    /// What would take a name what waits holds: waits as a file not hashed yet does
    /// ([`Waits::name_takers`]).
    name_takers: &'a BTreeSet<NodeId>,
    /// What waits for files here not hashed yet, for `name_takers`, or for other pages
    /// ([`Waits::waits`]).
    waits: &'a HashSet<NodeId>,
    /// The folders this page waits for ([`Waits::going`]).
    going: &'a [NodeId],
    /// Of `waits`, what waits for them ([`Waits::paged`]).
    paged: &'a HashSet<NodeId>,
    /// What of this plan waits to be placed with another page's nodes
    /// ([`Reconciler::places_across_pages`]): planned again waiting as it is.
    placed: BTreeSet<NodeId>,
    /// What is dirty in another page (`Input::dirty_elsewhere`): context as it is, its change
    /// unmerged here, so no repair of this plan moves it, renames it or adopts it (it keeps
    /// its name against this page's nodes); its own page decides it.
    elsewhere: &'a HashSet<NodeId>,
    /// Each file not hashed yet, with the folders it is in (which wait for it).
    by_file: &'a [(NodeId, Vec<NodeId>)],
    /// Names the held removals keep on the drive ([`guard::kept_names`]).
    kept: &'a HashMap<NodeId, HashSet<String>>,
    /// What waited for a name this plan, with what is below it ([`Reconciler::fix_names`]).
    waiters: BTreeSet<NodeId>,
    /// Nodes that wait whose names something else waits for ([`Reconciler::fix_names`]).
    name_waits: HashSet<NodeId>,
    plan: Plan,
}

impl<'a> Reconciler<'a> {
    fn new(
        input: &'a Input,
        opts: &'a Options,
        masking: Masking<'a>,
        masks: &'a Masks,
        waits: &'a Waits,
        kept: &'a HashMap<NodeId, HashSet<String>>,
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
            name_takers: &waits.name_takers,
            waits: &waits.waits,
            going: &waits.going,
            paged: &waits.paged,
            elsewhere: &input.dirty_elsewhere,
            placed: BTreeSet::new(),
            by_file: &waits.by_file,
            kept,
            waiters: BTreeSet::new(),
            name_waits: waits.name_waits.clone(),
            plan: Plan::default(),
        }
    }

    /// The plan, what waited for a name in it, what holds the names (with an earlier plan's),
    /// and what waits to be placed with another page's nodes.
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
        // A removed folder kept keeps what its removal took along (node schema, Recently
        // deleted): planned again as live, then its place checked as anything else's.
        if self.untake_kept() {
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
        // A node left where it is for a name may hold the name another would take: again,
        // till none more waits (in each pass at least one node more waits).
        loop {
            let waited = self.waiters.len();
            self.fix_names();
            if self.waiters.len() == waited {
                break;
            }
        }
        self.emit();
        self.places_across_pages();
        self.guard_mass_delete();
        self.plan.waiting = self.waited_for();
        // The folders this page waits for, with everything of it below them.
        self.plan.waits_for_pages = self.going.iter().map(|n| self.masks.store_id(*n)).collect();
        self.plan.deferred = self.deferred.into_iter().collect();
        (self.plan, self.waiters, self.name_waits, self.placed)
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
