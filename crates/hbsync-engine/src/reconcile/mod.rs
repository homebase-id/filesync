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
mod merge;
mod repair;

pub use crate::plan::{
    Agreement, BaseExtra, Conflict, Coverage, DriveVersion, Input, Op, Options, Plan, Removed,
    Requirement,
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

pub fn reconcile(input: &Input, opts: &Options<'_>) -> Plan {
    let masks = Masks::of(input, opts.filter, opts.scope, &opts.computer, &opts.taken);
    let view = masks.input.as_ref().unwrap_or(input);
    let mut plan = Reconciler::new(view, opts, &masks).run();
    // What a masked node does to its folder (keeps it, takes its name, leaves with it) is
    // judged with all of the folder's children.
    if let Some(coverage) = &input.coverage {
        plan.unlisted.extend(
            masks
                .masked
                .iter()
                .filter_map(|m| input.remote.get(*m).or(input.base.get(*m)))
                .map(|s| s.parent)
                .filter(|p| !coverage.full.contains(p)),
        );
    }
    plan
}

/// Rounds of orphan and cycle repair at most (each round ends at least one cycle; a real tree
/// needs one or two).
const MAX_REPAIRS: usize = 64;

struct Reconciler<'a> {
    /// The input with the masks applied.
    input: &'a Input,
    opts: &'a Options<'a>,
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
    plan: Plan,
}

impl<'a> Reconciler<'a> {
    fn new(input: &'a Input, opts: &'a Options<'a>, masks: &'a Masks) -> Self {
        Reconciler {
            input,
            opts,
            masks,
            merged: Tree::new(),
            scope: BTreeSet::new(),
            origin: HashMap::new(),
            forks: BTreeMap::new(),
            deferred: BTreeSet::new(),
            adopted: HashSet::new(),
            taken: merge::taken_along(input),
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
        self.fix_names();
        self.emit();
        self.guard_mass_delete();
        self.plan.deferred = self.deferred.into_iter().collect();
        self.plan
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
    opts: &Options<'_>,
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
