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
use std::sync::Arc;

use hbsync_platform::name::name_key;
use hbsync_types::{ContentHash, ContentState, NodeId, NodeKind, NodeState, Tags, VersionTag};

use crate::mask::Masks;
use crate::tree::{Tree, conflict_copy_name, numbered_conflict_name};

mod emit;
mod guard;
mod merge;
mod repair;

pub use crate::plan::{Agreement, BaseExtra, Conflict, Input, Op, Options, Plan, Requirement};

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

/// Rounds of orphan and cycle repair at most (each round ends at least one cycle; a real tree
/// needs one or two).
const MAX_REPAIRS: usize = 64;

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
