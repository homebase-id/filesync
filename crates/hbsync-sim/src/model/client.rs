//! A client: one folder of a computer, with its local tree, its base and its executor's
//! state.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use hbsync_engine::reconcile::{BaseExtra, DriveVersion, Op};
use hbsync_engine::scope::Scope;
use hbsync_engine::tree::Tree;
use hbsync_local::{Filter, Verdict};
use hbsync_platform::Os;
use hbsync_state::Coords;
use hbsync_types::{NodeId, NodeKind, RemoteRef, VersionTag};

use crate::crash::Placement;

/// Where a client's sweep over its nodes in pages is ([`crate::Sim::with_small_pages`]), as the
/// engine's over its dirty set.
#[derive(Debug, Default)]
pub(crate) struct Pages {
    /// The last node of the last page (`None`: a sweep starts).
    pub(crate) after: Option<NodeId>,
    /// What its pages wait for, planned as pages of their own.
    pub(crate) own: hbsync_engine::pages::OwnPages,
    /// The sweep ended with the last page: a new one starts at the next.
    pub(crate) ended: bool,
    /// What pages of their own planned this sweep: not planned again in its pages.
    pub(crate) planned_own: BTreeSet<NodeId>,
}

#[derive(Debug)]
pub(crate) struct Client {
    /// Its sweep over its nodes in pages, where it plans so.
    pub(crate) pages: std::cell::RefCell<Pages>,
    pub(crate) name: String,
    /// Its computer's id for the account (`Options::computer`): the folders of one computer
    /// share it, as they share one state DB.
    pub(crate) computer: String,
    pub(crate) local: Tree,
    pub(crate) base: Tree,
    pub(crate) extra: HashMap<NodeId, BaseExtra>,
    /// Pending operations with their retry count.
    pub(crate) plan: VecDeque<(Op, u8)>,
    /// What the pending plan removed to Recently deleted so far.
    pub(crate) removed_by_plan: BTreeSet<NodeId>,
    /// The folders whose removal in the pending plan ended without removing them: what is in
    /// them waits for the next plan.
    pub(crate) unremoved: HashSet<NodeId>,
    /// Items being deleted, moved aside (`.hbsync/aside`) and not yet checked and unlinked: each
    /// with what was below it then, its top at its place in the folder (sync semantics §7).
    pub(crate) aside: BTreeMap<NodeId, Tree>,
    /// Nodes parked under a temporary name, with their original (parent, name).
    pub(crate) parked: HashMap<NodeId, (NodeId, String)>,
    /// The local tree when the pending plan was made (for the fingerprint guard).
    pub(crate) planned_local: Tree,
    /// The server snapshot the pending plan was made from (contents to download), updated
    /// by the client's own remote writes: what the executor's mirror holds.
    pub(crate) planned_remote: Tree,
    /// Where the pending plan saw each remote node when it was made, whatever its own writes
    /// did since, the folders it saw in Recently deleted included (§12: what is in a folder's
    /// part in Recently deleted is its own).
    pub(crate) plan_saw: Tree,
    pub(crate) filter: Arc<Filter>,
    /// The filter its pending plan runs with: a rule change counts from the next plan, as the
    /// executor holds one for its run.
    pub(crate) run_filter: Arc<Filter>,
    /// The nodes of the deletes the guard held in the last plan.
    pub(crate) held: Vec<NodeId>,
    /// Operations the drive refused, by node and kind, with their key
    /// (`hbsync_engine::refused::key`) and whether refused (else one strike: a download whose
    /// bytes didn't match once): not sent again while their key is the same.
    pub(crate) refused: HashMap<(NodeId, &'static str), (String, bool)>,
    /// The operations the last plan held back for a refusal: refused themselves (`true`), or
    /// waiting for one.
    pub(crate) blocked: Vec<(Op, bool)>,
    /// The operations the last plan sent to run (with refusals on: once nothing more happens,
    /// those left are blocked, behind a refusal).
    pub(crate) last_plan: Vec<Op>,
    /// The nodes whose deletes the user confirmed, until no delete is pending for them.
    pub(crate) confirmed: HashSet<NodeId>,
    /// The nodes whose removal elsewhere the guard held as every synced file: held until
    /// answered, as the engine's `guard.wiped` (sync semantics §7).
    pub(crate) wiped: HashSet<NodeId>,
    /// How the client holds tags.
    pub(crate) os: Os,
    /// How the client's folder sees the drive: the main folder sees it as it is, a role
    /// folder its top as the root and the rest as one folder it leaves out (§12).
    pub(crate) coords: Coords,
    /// Downloads fetched, not yet put in place ([`Sim::with_split_downloads`]): the version
    /// each acts on, with the bytes fetched (the executor's write-ahead record).
    ///
    /// [`Sim::with_split_downloads`]: crate::Sim::with_split_downloads
    pub(crate) fetched: BTreeMap<NodeId, DriveVersion>,
    /// Downloads and folders put in place whose commit a crash lost.
    pub(crate) placing: BTreeMap<NodeId, Placement>,
    /// The version of each node `planned_remote` holds (the executor's mirror).
    pub(crate) planned_refs: BTreeMap<NodeId, RemoteRef>,
    /// The folders in Recently deleted above live nodes, with when each went there, and the
    /// server's time of each live node's version, as the pending plan's input had them (the
    /// mirror's view of what a folder's removal took along).
    pub(crate) planned_removed: BTreeMap<NodeId, i64>,
    pub(crate) planned_changed: HashMap<NodeId, i64>,
    /// Where the pending plan's operations settled each node's base (the executor's): a later
    /// one's compare and set takes the base there as the one its plan saw.
    pub(crate) run_settled: HashMap<NodeId, VersionTag>,
    /// Files the last recovery found unhashed (their record kept): the plan defers them.
    pub(crate) unhashed: HashSet<NodeId>,
    /// Files the user is still writing ([`Action::EditWriting`]): the next plan finds them
    /// not hashed yet, as it does the `unhashed`.
    ///
    /// [`Action::EditWriting`]: crate::Action::EditWriting
    pub(crate) writing: HashSet<NodeId>,
    /// Files another app holds locked ([`crate::Action::Lock`]): not hashed at any plan till
    /// let go.
    pub(crate) locked: HashSet<NodeId>,
    /// The last plan found files still being written: the scan hashes them once written, and
    /// that asks for a sync (the engine keeps their nodes dirty).
    pub(crate) hashing: bool,
    /// The identity's "not there" heard once, per file ([`crate::gone`]), the files taken as
    /// gone for good (`gone_files`: never seen again), and the client's syncs, its clock.
    pub(crate) suspects: BTreeMap<crate::gone::File, hbsync_state::GoneSuspect>,
    pub(crate) gone: BTreeSet<hbsync_types::RemoteFileId>,
    pub(crate) syncs: i64,
    /// Another device changes the node of the next remote write once it landed
    /// ([`crate::Action::Meanwhile`], its `op`).
    pub(crate) meanwhile: Option<usize>,
    /// Another device changes the node of the next local operation of a kind between its
    /// check and its commit ([`crate::Action::PullMidLocal`]: `on`, `change`).
    pub(crate) pull_mid_local: Option<(usize, usize)>,
}

impl Client {
    /// The files no plan now finds hashed: a record recovery kept, one still being written,
    /// one held locked.
    pub(crate) fn unhashed_now(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.unhashed
            .iter()
            .chain(&self.writing)
            .chain(&self.locked)
            .copied()
    }

    pub(crate) fn new(name: String, os: Os, filter: Filter, coords: Coords) -> Self {
        Client {
            computer: name.clone(),
            name,
            local: Tree::new(),
            base: Tree::new(),
            extra: HashMap::new(),
            plan: VecDeque::new(),
            removed_by_plan: BTreeSet::new(),
            unremoved: HashSet::new(),
            aside: BTreeMap::new(),
            parked: HashMap::new(),
            planned_local: Tree::new(),
            planned_remote: Tree::new(),
            pages: Default::default(),
            plan_saw: Tree::new(),
            run_filter: Arc::new(filter.clone()),
            filter: Arc::new(filter),
            held: Vec::new(),
            refused: HashMap::new(),
            blocked: Vec::new(),
            last_plan: Vec::new(),
            confirmed: HashSet::new(),
            wiped: HashSet::new(),
            os,
            coords,
            fetched: BTreeMap::new(),
            placing: BTreeMap::new(),
            planned_refs: BTreeMap::new(),
            planned_removed: BTreeMap::new(),
            planned_changed: HashMap::new(),
            run_settled: HashMap::new(),
            unhashed: HashSet::new(),
            writing: HashSet::new(),
            locked: HashSet::new(),
            hashing: false,
            suspects: BTreeMap::new(),
            gone: BTreeSet::new(),
            syncs: 0,
            meanwhile: None,
            pull_mid_local: None,
        }
    }

    /// `node` isn't synced here any more: its base goes.
    pub(crate) fn drop_base(&mut self, node: NodeId) {
        self.base.remove(node);
        self.extra.remove(&node);
    }

    /// The part of the drive the client's folder syncs.
    pub(crate) fn scope(&self) -> Scope {
        Scope::of(self.coords.top)
    }
}

pub(crate) fn ignoring_d() -> Filter {
    Filter::from_rules("d\n", false).unwrap_or_default()
}

/// What the filter of the client's run says about its local item `node`.
pub(crate) fn verdict(client: &Client, node: NodeId) -> Verdict {
    verdict_in(&client.local, &client.run_filter, node)
}

/// What `filter` says about item `node` of `tree`, at its place there.
pub(crate) fn verdict_in(tree: &Tree, filter: &Filter, node: NodeId) -> Verdict {
    let Some(path) = tree.path(node) else {
        return Verdict::Sync;
    };
    let is_dir = tree.get(node).is_some_and(|s| s.kind == NodeKind::Folder);
    filter.names(path, is_dir)
}

/// Gives the client's local item `node` the id `new`: in its tree, in the tree its plan was
/// made from, and in the parking records of its children.
pub(crate) fn rekey_client(client: &mut Client, node: NodeId, new: NodeId) {
    rekey(&mut client.local, node, new);
    rekey(&mut client.planned_local, node, new);
    for (parent, _) in client.parked.values_mut() {
        if *parent == node {
            *parent = new;
        }
    }
}

/// Gives the item `node` in `tree` the id `new`, children included.
fn rekey(tree: &mut Tree, node: NodeId, new: NodeId) {
    let kids: Vec<NodeId> = tree.children(node).collect();
    if let Some(state) = tree.remove(node) {
        tree.insert(new, state);
    }
    for kid in kids {
        if let Some(mut state) = tree.get(kid).cloned() {
            state.parent = new;
            tree.insert(kid, state);
        }
    }
}
