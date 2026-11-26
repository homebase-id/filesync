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
pub use merge::taken_along;
pub use waits::taken_by;
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

/// Plans again at most this often for what moves away from a masked folder (§10): a
/// [`reconcile`] runs [`plan_with`] (each with its own rounds for names that wait) 1 +
/// `AWAY_ROUNDS` times at most, and [`reconcile_with_known`] runs `reconcile` up to 9 times:
/// 36 at worst; in practice once, twice where something moves away.
const AWAY_ROUNDS: usize = 3;

pub fn reconcile(input: &Input, opts: &Options, masking: Masking<'_>) -> Plan {
    let masks = Masks::of(input, masking, &opts.computer, &opts.taken);
    let first = plan_with(input, opts, masking, &masks);
    // What this plan moves away from a masked folder, detached to hold it (§10, `mask.rs`):
    // planned again without it holding the folder. Hardly ever.
    let away = masks.moved_away(&first);
    let detached = masks.detached.len();
    spare_away(first, masks, away, |masks, away| {
        let spared = masks.sparing(input, &opts.computer, &opts.taken, away, detached)?;
        let plan = plan_with(input, opts, masking, &spared);
        let stayed = spared.not_moved_away(input, &plan, away);
        Some((plan, stayed, spared))
    })
}

/// `first`, or the plan `replan` makes (from `state`) with the items `away` holding no folder,
/// once it moves each of them away: the ones it returns hold theirs again, for the next round.
/// `None`, or [`AWAY_ROUNDS`] spent: `first`.
fn spare_away<P, S>(
    first: P,
    mut state: S,
    mut away: HashMap<NodeId, NodeId>,
    mut replan: impl FnMut(S, &HashMap<NodeId, NodeId>) -> Option<(P, Vec<NodeId>, S)>,
) -> P {
    for _ in 0..AWAY_ROUNDS {
        if away.is_empty() {
            break;
        }
        let Some((plan, stayed, next)) = replan(state, &away) else {
            break;
        };
        if stayed.is_empty() {
            return plan;
        }
        for node in stayed {
            away.remove(&node);
        }
        state = next;
    }
    first
}

/// The plan of `input` with the masks `masks`.
fn plan_with(input: &Input, opts: &Options, masking: Masking<'_>, masks: &Masks) -> Plan {
    let view = masks.input.as_ref().unwrap_or(input);
    // By the drive as the filter leaves it: a node outside this folder's scope (its computer's
    // own Desktop in Desktops) written after a delete keeps the folders above it as any does.
    let taken = merge::taken_along(input, &masks.filtered);
    // What would take a name one that waits for a hash holds waits too (§6.3), where it is,
    // with its folders: planned again with it waiting, till no more come.
    // Ends: in each plan again at least one node more waits.
    let mut waits = Waits::of(view, opts.piece);
    // And this device's removals from the drive held for the user (§7): their names there
    // stay taken meanwhile, so what would take one here gets a numbered name: planned again
    // once, only when something here has such a name (hardly ever).
    let mut kept = HashMap::new();
    let mut plan = loop {
        let r = Reconciler::new(view, opts, masking, masks, &waits, &kept, taken.clone());
        let (mut plan, waiters, name_waits, placed) = r.run();
        waits.together.append(&mut plan.waits_together);
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
    let mut together = std::mem::take(&mut waits.together);
    together.sort();
    together.dedup();
    // A group a larger one holds goes (one grown by what waits with it: `waits_for_its_folder`):
    // planned in it, a page fewer. Groups are sorted.
    let held_by = |small: &[NodeId], big: &[NodeId]| {
        small.len() < big.len() && small.iter().all(|n| big.binary_search(n).is_ok())
    };
    let held: Vec<bool> = together
        .iter()
        .map(|g| together.iter().any(|big| held_by(g, big)))
        .collect();
    let mut held = held.into_iter();
    together.retain(|_| !held.next().unwrap_or(false));
    plan.waits_together = together;
    // What the plan needs more of, from a narrow view (`Plan::uncovered`'s producers are
    // listed in `coverage`; `fix_names` adds its own).
    coverage::masked_and_in_files(input, masks, &mut plan);
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
    /// What moves out of a read-only folder before the drive's state comes back (§13).
    evicted: BTreeSet<NodeId>,
    /// What a read-only name clash left out (§13), each once.
    left: BTreeSet<NodeId>,
    /// The names held here by what waits for its folder there ([`Reconciler::fix_orphans`]:
    /// out of the plan, where it is here), and by the folders kept for it, by folder: taken
    /// here till the wait ends. What would take one is left out here meanwhile, never
    /// renamed there for it.
    held_waiting: HashMap<NodeId, HashSet<String>>,
    /// What is here and a repair took out of the plan, on an identity's own drive: what waits
    /// for its folder there ([`Reconciler::fix_orphans`]), and what was left out for a name
    /// and put back where it is here ([`Reconciler::leave_out`]). Changed here, each keeps the
    /// folders it is in ([`Reconciler::keep_for_what_is_out`]).
    out_here: BTreeSet<NodeId>,
    /// The folders gone there that [`Reconciler::fix_orphans`] brought back for a live node in
    /// them: one brought back for a conflict copy alone goes again where the copy isn't made
    /// after all ([`Reconciler::unfork`]).
    brought_back: BTreeSet<NodeId>,
    /// The nodes this identity may change on another identity's drive that follow it in this
    /// plan all the same (§13, `one_way`): one gone there or here, or edited on both sides
    /// where no copy of it may be added (the merge's); one a repair gave its place (an orphan let go of, a cycle's move
    /// undone: the drive is its owner's to mend, no repair here writes to it). (A name
    /// clash's loser is left out of the plan; what this device leaves out, §10, isn't in the
    /// view's drive: neither is written.)
    follows: HashSet<NodeId>,
    /// The nodes this plan adds to another identity's drive (§13): new here, in a folder this
    /// identity may add to there (`adds_to`), and the conflict copies made in one. They are
    /// planned as on an identity's own drive (`one_way`).
    adds: HashSet<NodeId>,
    /// Whether the delete here of each topmost node deleted here is sent to another
    /// identity's drive (§13, `removes`): asked once for a folder and all that was in it.
    removals: HashMap<NodeId, bool>,
    /// The folders here that hold what a name pass left out, or what is below it (§13): judged
    /// again.
    rejudge: BTreeSet<NodeId>,
    /// Becomes `plan.deferred`.
    deferred: BTreeSet<NodeId>,
    /// Orphans the repairs move into Recovered files (§6.3): they lose a name clash there.
    adopted: HashSet<NodeId>,
    /// Orphans left to the devices that sync where they would go, of which this device has a
    /// base and no item: the base goes with this plan (nothing here is the node's).
    released: BTreeSet<NodeId>,
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
    /// What earlier plans of this input left waiting with another page's nodes
    /// ([`Waits::together`]).
    together: &'a [Vec<NodeId>],
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
            evicted: BTreeSet::new(),
            left: BTreeSet::new(),
            held_waiting: HashMap::new(),
            out_here: BTreeSet::new(),
            brought_back: BTreeSet::new(),
            follows: HashSet::new(),
            adds: HashSet::new(),
            removals: HashMap::new(),
            rejudge: BTreeSet::new(),
            deferred: BTreeSet::new(),
            adopted: HashSet::new(),
            released: BTreeSet::new(),
            taken,
            name_takers: &waits.name_takers,
            waits: &waits.waits,
            going: &waits.going,
            paged: &waits.paged,
            together: &waits.together,
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
        // A node left where it is for a name, or left out read-only (§13: where it is here,
        // maybe in a folder judged already), may hold the name another would take: again, till
        // none more waits or is left out (in each pass at least one node more does).
        self.keep_off_taken_names();
        let mut only = None;
        loop {
            let waited = self.waiters.len();
            self.fix_names(only.as_ref());
            // What a name left out is put back where it is here, maybe in a folder the plan
            // deletes here: changed here, it keeps that folder, which holds its name, in a
            // folder judged again for it.
            self.keep_for_what_is_out();
            only = if self.waiters.len() != waited {
                // Every folder again: none just for what was left out.
                self.rejudge.clear();
                None
            } else if self.rejudge.is_empty() {
                break;
            } else {
                Some(std::mem::take(&mut self.rejudge))
            };
        }
        self.emit();
        self.places_across_pages();
        self.waits_for_its_folder();
        self.deletes_wait_with_their_folder();
        self.with_what_it_took_along();
        self.guard_mass_delete();
        // Read-only (§13): the drive is never written, but of a node this identity may change
        // there or adds there (emit plans no other write): changed where it is, or moved
        // between folders it may add to; made in one; removed (to its owner's Recently
        // deleted) where it was deleted here.
        debug_assert!(
            !self.masking.read_only
                || (self.plan.ops.iter().chain(&self.plan.held)).all(|op| match op {
                    Op::RemoteUpdate { node, parent, .. } => {
                        !self.one_way(*node)
                            && self.remote_state(*node).is_some_and(|r| {
                                r.parent == *parent
                                    || (self.adds_to(*parent) && self.adds_to(r.parent))
                            })
                    }
                    Op::RemoteTags { node, .. } => !self.one_way(*node),
                    Op::RemoteCreate { node, parent, .. } => {
                        self.adds.contains(node) && self.adds_to(*parent)
                    }
                    Op::RemoteDelete { node, .. } => {
                        !self.one_way(*node) && !self.input.local.contains(*node)
                    }
                    op => !op.writes_remote(),
                }),
            "a read-only plan writes the drive: {:?}",
            self.plan.ops
        );
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

    /// Planned again with fewer items away each time one didn't move away: the one that didn't
    /// goes, the rest stay; once the rounds are spent, the first plan stands.
    #[test]
    fn what_did_not_move_away_is_planned_again_without_it() {
        let [a, b, c] = [NodeId::new(), NodeId::new(), NodeId::new()];
        let away: HashMap<NodeId, NodeId> = [a, b, c].map(|n| (n, NodeId::ROOT)).into();
        let mut asked: Vec<BTreeSet<NodeId>> = Vec::new();
        let plan = spare_away("first", (), away.clone(), |(), away| {
            asked.push(away.keys().copied().collect());
            Some(match asked.len() {
                1 => ("second", vec![b], ()),
                _ => ("third", vec![], ()),
            })
        });
        assert_eq!(plan, "third");
        assert_eq!(asked, [BTreeSet::from([a, b, c]), BTreeSet::from([a, c])]);

        // Never all moved away: the first plan, after the rounds.
        let mut rounds = 0;
        let mut left = vec![a, b, c, NodeId::new()];
        let plan = spare_away("first", (), away.clone(), |(), _| {
            rounds += 1;
            Some(("again", vec![left.pop().expect("one more")], ()))
        });
        assert_eq!((plan, rounds), ("first", AWAY_ROUNDS));

        // No folder spared, or nothing away: the first plan, asked once at most.
        assert_eq!(spare_away("first", (), away, |(), _| None), "first");
        let plan = spare_away(
            "first",
            (),
            HashMap::new(),
            |(), _| -> Option<(&str, _, ())> { panic!("nothing away") },
        );
        assert_eq!(plan, "first");
    }

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
