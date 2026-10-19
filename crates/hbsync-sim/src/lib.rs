//! A deterministic multi-client simulator for the reconciler.
//!
//! Clients hold an in-memory local tree and a base; the server holds live nodes with file ids
//! and version tags. A client's sync is split into **plan** (snapshot the server, reconcile)
//! and **execute** (one operation at a time), and other clients may act in between, so plans
//! go stale exactly like they do for real. Operations are applied with the same guards the
//! executor uses:
//! - remote writes need the planned version (VersionTag); a stale one is skipped;
//! - local writes need the local item unchanged since planning (the fingerprint guard), a
//!   free target name and an existing parent;
//! - after every successful operation the node is settled attribute by attribute: the base
//!   takes a value only when both sides hold it.
//!
//! [`Sim::check`] then verifies convergence, invariants, and that no file version was lost
//! unless some client overwrote or deleted it after seeing it.
//!
//! Clients tag like their OS (sync semantics §11): client 0 like macOS (names and colours),
//! 1 like Linux (names only), 2 like Windows (no tags); each client sees the tags it can't hold
//! as the drive has them, as the engine's view does.
//!
//! Clients filter (sync semantics §10): every client has the built-in junk list, and odd
//! clients start out also ignoring the name `d` (any client can toggle that). A client's filtered items are invisible to its sync, as
//! the scanner leaves them out; convergence is judged on what each client syncs, and an
//! ignored item, kept on its client only, counts as present for the no-loss check.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_engine::Against;
use hbsync_engine::deleted;
use hbsync_engine::exec::parked_name;
use hbsync_engine::mask::masked;
use hbsync_engine::plan::TagWrite;
use hbsync_engine::reconcile::{
    Agreement, BaseExtra, DriveVersion, Input, Op, Options, Removed, Requirement,
    reconcile_with_known,
};
use hbsync_engine::scope::Scope;
use hbsync_engine::tree::{Tree, first_free, numbered_names, restored_names};
use hbsync_engine::walk::End;
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_local::{Filter, Verdict};
use hbsync_platform::Os;
use hbsync_platform::name::check_remote_name;
use hbsync_platform::tags::express;
use hbsync_state::Coords;
use hbsync_types::{
    ContentHash, ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef,
    Role, Tag, Tags, Uuid, VersionTag,
};

/// A user action on one client's local tree. Indices pick among existing items modulo their
/// count, so every generated value is meaningful (and shrinks well).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    CreateFile {
        parent: usize,
        name: usize,
    },
    CreateFolder {
        parent: usize,
        name: usize,
    },
    Edit {
        file: usize,
    },
    Move {
        node: usize,
        parent: usize,
        name: usize,
    },
    Delete {
        node: usize,
    },
    /// A file on the built-in junk list (an OS thumbnail cache).
    CreateJunk {
        parent: usize,
    },
    /// The client starts or stops ignoring `d` (a rule change, picked up at the next plan).
    ToggleIgnore,
    /// The user restores one item of Recently deleted, on the server at once, with what was
    /// deleted with it (node schema: in it, removed no earlier than it), folders first: back in
    /// its folder, or at the top if that is gone, under a free name. (The engine's restore also
    /// brings back the folders above: each such step is one of these.)
    Restore {
        node: usize,
    },
    /// The daily purge ([`Server::purge`]): the server's clock moves on by [`PURGE_EVERY`];
    /// what the deletes of the items of Recently deleted removed [`TRACE`] before took along
    /// and left live is removed (`along::taken_levels`), and those items go for good, as the
    /// engine selects them ([`hbsync_engine::deleted::select`]): never a live node, so a folder
    /// holding one stays.
    Purge,
    /// Another device (the phone, or a computer never seen again) deletes a folder of the
    /// drive and stops half way: the folder, then the first `done` of what is in it, top-down
    /// (node schema, Recently deleted); the rest stays live, taken along. Any folder, the role
    /// folder and what is in it too ([`Sim::stopped_folders`]); nothing when the drive holds
    /// none.
    DeleteStopped {
        pick: usize,
        done: usize,
    },
    /// A user lists Recently deleted ([`hbsync_engine::deleted::deletes`]): what it holds now,
    /// each node at its version, is what emptying it as listed may take.
    ListDeleted,
    /// The user empties Recently deleted as last listed (nothing without a listing): what the
    /// engine selects of the rows still as listed ([`hbsync_engine::deleted::select`]), in its
    /// order.
    EmptyListed,
    /// The user empties one item of Recently deleted as last listed (a pick of its rows; nothing
    /// without a listing): it and all of Recently deleted in it ([`deleted::inside`]: what was
    /// deleted from it before too), at their listed versions, as the engine selects them. An
    /// item holding nothing live and nothing written since the listing goes.
    EmptyItem {
        pick: usize,
    },
    /// Another device (the phone, which opened it before) saves an edit of a file a folder's
    /// delete took along, its own removal still on its way: on the server at once, the folder
    /// still deleted. Nothing when no file is taken along.
    EditTaken {
        pick: usize,
    },
    /// The user adds a tag of [`TAGS`] to an item, or removes it, as far as the client's OS
    /// lets them.
    Tag {
        node: usize,
        tag: usize,
    },
    /// The user gives a tag of an item another colour (macOS).
    Colour {
        node: usize,
        tag: usize,
    },
    /// Another device (the phone) changes the role folder ([`Sim::with_role_folder`]), on the
    /// server at once; nothing without one.
    Phone {
        op: usize,
        pick: usize,
    },
    /// The phone deletes the role folder itself, with what is in it (to Recently deleted); a
    /// device syncing it brings it back (as `Drive::ensure_role`).
    DeleteRoleFolder,
    /// The user moves a file from one folder of the client's computer to the other, at its
    /// top ([`Sim::with_role_device`]): the scan of the other folder finds a new item. Nothing
    /// on a client with one folder.
    MoveAcross {
        file: usize,
    },
}

/// Tags the users pick from: one no Linux client can hold, one with a newline no macOS client
/// can hold.
const TAGS: &[(&str, u8)] = &[("Red", 6), ("Work", 0), ("a,b", 4), ("x\ny", 2)];

/// The junk name [`Action::CreateJunk`] uses.
const JUNK: &str = "Thumbs.db";

/// One step of a simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Act {
        client: usize,
        action: Action,
    },
    /// Snapshot the server and plan (replacing any unfinished plan).
    Plan {
        client: usize,
    },
    /// Execute the next planned operation.
    Step {
        client: usize,
    },
    /// The user confirms the deletes the mass-delete guard held in the client's last plan
    /// (with [`Sim::guard`] set).
    Confirm {
        client: usize,
    },
    /// The client's process dies during its next step: a download's commit is lost after its
    /// file is in place ([`Sim::with_split_downloads`]), and the rest of its plan with it. Its
    /// next plan recovers first, as the engine's start of a sync does.
    Crash {
        client: usize,
    },
}

/// Names drawn from a small pool so clashes (including case-only ones) are common. The last
/// two are as long as a name may be, so conflict names have to shorten them.
const NAMES: &[&str] = &[
    "a",
    "b",
    "A",
    "c.txt",
    "C.TXT",
    "d",
    "LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL.txt",
    "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll.TXT",
];

#[derive(Debug, Clone, PartialEq)]
struct ServerNode {
    state: NodeState,
    remote: RemoteRef,
}

/// [`Server::snapshot`].
type Snapshot = (BTreeMap<NodeId, ServerNode>, BTreeMap<NodeId, ServerNode>);

#[derive(Debug, Default)]
struct Server {
    live: BTreeMap<NodeId, ServerNode>,
    /// Recently deleted: out of the tree, with their bytes, until restored or purged.
    removed: BTreeMap<NodeId, ServerNode>,
    /// When each node went to Recently deleted (the server's time of that write: a header's
    /// `updated`, [`time`]).
    removed_at: BTreeMap<NodeId, i64>,
    /// The tags each node had when it was last removed: a restore keeps them (§11).
    tags_at_removal: BTreeMap<NodeId, BTreeSet<String>>,
    next: u128,
    /// Every node that was ever live (the server doesn't check parents, like Homebase).
    ever: BTreeSet<NodeId>,
    /// The first write that pointed a node at a folder the server never had.
    dangling: Option<String>,
    /// The last listing of Recently deleted ([`Action::ListDeleted`]): each node at its version.
    listed: Option<BTreeSet<(NodeId, VersionTag)>>,
    /// The first node emptying or the purge deleted for good that wasn't a removed row chosen
    /// at that version (a live node, say), or a folder holding anything not going after it.
    emptied_unlisted: Option<String>,
    /// The first item emptied on its own that stayed though nothing kept it.
    emptied_kept: Option<String>,
}

/// How long an item stays in Recently deleted, and a delete for good leaves a trace in the feed
/// (30 days: `hbsync_engine::feed::TRACE_MS`), on the server's clock ([`time`]).
pub const TRACE: i64 = 24;
/// How far the server's clock moves on from one purge to the next.
pub const PURGE_EVERY: i64 = TRACE / 2;

/// The server's time of the write that made `version`: every write takes the next id, so ids
/// are the server's clock (Homebase's `updated`).
fn time(version: VersionTag) -> i64 {
    version.0.as_u128() as i64
}

impl Server {
    /// A row of Recently deleted as a listing names it: its node at its version.
    fn at((node, n): (&NodeId, &ServerNode)) -> (NodeId, VersionTag) {
        (*node, n.remote.version)
    }

    fn uuid(&mut self) -> Uuid {
        self.next += 1;
        Uuid::from_u128(self.next)
    }

    /// Puts `node` in Recently deleted: a write, at its time.
    fn remove(&mut self, node: NodeId, mut n: ServerNode) {
        let tags = n.state.tags.iter().map(|t| t.name.clone()).collect();
        self.tags_at_removal.insert(node, tags);
        n.remote.version = VersionTag(self.uuid());
        self.removed_at.insert(node, time(n.remote.version));
        self.removed.insert(node, n);
    }

    /// The nodes in Recently deleted that were deleted with folder `node` (node schema): in it,
    /// removed no earlier than it.
    fn deleted_with(&self, node: NodeId) -> Vec<NodeId> {
        let Some(at) = self.removed_at.get(&node) else {
            return Vec::new();
        };
        self.removed
            .iter()
            .filter(|(n, r)| {
                r.state.parent == node && self.removed_at.get(n).is_some_and(|t| t >= at)
            })
            .map(|(n, _)| *n)
            .collect()
    }

    /// The live nodes a folder's removal took along, by the engine's one rule
    /// ([`hbsync_engine::along::taken_along`]). They count as removed, whether or not a device
    /// marked them so yet.
    fn taken_along(&self) -> BTreeSet<NodeId> {
        hbsync_engine::along::taken_along(
            self.live.iter().map(|(n, l)| (*n, l.state.parent)),
            |f| {
                self.removed
                    .contains_key(&f)
                    .then(|| self.removed_at.get(&f).copied())
                    .flatten()
            },
            |n| self.live.get(&n).map(|l| time(l.remote.version)),
        )
        .into_iter()
        .collect()
    }

    /// The engine's purge (`deleted::purge`), its clock moved on [`PURGE_EVERY`], first part:
    /// what the deletes of the items removed [`TRACE`] before now (each deleted on its own)
    /// took along and left live, top-down, as the engine picks it
    /// ([`hbsync_engine::along::taken_levels`]; the purging device knows every version here),
    /// for [`Sim::remove_live`]. Then [`Server::due`].
    fn purge(&mut self) -> Vec<NodeId> {
        self.next += PURGE_EVERY as u128;
        let folders: HashMap<NodeId, i64> = self
            .items(self.purge_before())
            .into_iter()
            .flatten()
            .filter(|n| self.removed[n].state.kind == NodeKind::Folder)
            .filter_map(|n| Some((n, *self.removed_at.get(&n)?)))
            .collect();
        hbsync_engine::along::taken_levels(
            self.live.iter().map(|(n, l)| (*n, l.state.parent)),
            |f| folders.get(&f).copied(),
            |n| self.live.get(&n).map(|l| time(l.remote.version)),
            |n| n == Role::Documents.node(),
        )
        .into_iter()
        .flatten()
        .collect()
    }

    /// The purge's cut-off: removed before it is kept long enough.
    fn purge_before(&self) -> i64 {
        self.next as i64 - TRACE
    }

    /// The purge's second part: the items all removed [`TRACE`] before now, with what was
    /// deleted with each, at their versions: what [`Sim::delete_for_good`] deletes as the
    /// engine selects it (never a live node, nor a folder holding one). Without a trace or
    /// leaving a tombstone is the same here (no feed: a client finds it missing).
    fn due(&self) -> BTreeSet<(NodeId, VersionTag)> {
        let before = self.purge_before();
        let mut due = BTreeSet::new();
        for item in self.items(before) {
            // Due once all of it is: one removed later has its own time.
            if item
                .iter()
                .all(|n| self.removed_at.get(n).is_some_and(|at| *at < before))
            {
                due.extend(item.iter().map(|n| (*n, self.removed[n].remote.version)));
            }
        }
        due
    }

    /// The items of Recently deleted removed before `before` (each deleted on its own), each
    /// with what was deleted with it: removed nodes deleted with their folder, level by level.
    fn items(&self, before: i64) -> Vec<Vec<NodeId>> {
        let time = |n: &NodeId| self.removed_at.get(n).copied().unwrap_or(i64::MAX);
        let tops = self.removed.iter().filter(|(n, r)| {
            time(n) < before
                && !(self.removed.contains_key(&r.state.parent) && time(&r.state.parent) <= time(n))
        });
        tops.map(|(top, _)| {
            let mut item = vec![*top];
            let mut next = 0;
            while next < item.len() {
                let folder = item[next];
                let at = time(&folder);
                let with: Vec<NodeId> = self
                    .removed
                    .iter()
                    .filter(|(n, r)| r.state.parent == folder && time(n) >= at && !item.contains(n))
                    .map(|(n, _)| *n)
                    .collect();
                item.extend(with);
                next += 1;
            }
            item
        })
        .collect()
    }

    fn tree(&self) -> Tree {
        let mut t = Tree::new();
        for (id, n) in &self.live {
            t.insert(*id, n.state.clone());
        }
        t
    }

    /// Puts `state` on the server as another device would, as a new file.
    fn put(&mut self, node: NodeId, state: NodeState) -> RemoteRef {
        let remote = RemoteRef {
            file_id: RemoteFileId(self.uuid()),
            version: VersionTag(self.uuid()),
        };
        self.ever.insert(node);
        self.live.insert(node, ServerNode { state, remote });
        remote
    }

    /// Where a walk up from `node` ends: at the role folder (`End::At` it) for the role folder
    /// and what lies in it, at the drive's top for the rest of what reaches it.
    fn place(&self, node: NodeId) -> End {
        let docs = Role::Documents.node();
        let Ok(up) = hbsync_engine::walk::up::<_, Infallible>(node, |at| {
            Ok((at != docs)
                .then(|| self.live.get(&at).map(|n| ((), n.state.parent)))
                .flatten())
        });
        up.end
    }

    /// Whether `node` is the role folder or lies in it.
    fn in_role_folder(&self, node: NodeId) -> bool {
        self.place(node) == End::At(Role::Documents.node())
    }

    /// What the drive holds, live and in Recently deleted: to tell whether it changed since.
    fn snapshot(&self) -> Snapshot {
        (self.live.clone(), self.removed.clone())
    }

    fn changed_since(&self, snapshot: &Snapshot) -> bool {
        (&self.live, &self.removed) != (&snapshot.0, &snapshot.1)
    }
}

#[derive(Debug)]
struct Client {
    name: String,
    /// Its computer's id for the account (`Options::computer`): the folders of one computer
    /// share it, as they share one state DB.
    computer: String,
    local: Tree,
    base: Tree,
    extra: HashMap<NodeId, BaseExtra>,
    /// Pending operations with their retry count.
    plan: VecDeque<(Op, u8)>,
    /// What the pending plan removed to Recently deleted so far.
    removed_by_plan: BTreeSet<NodeId>,
    /// Nodes parked under a temporary name, with their original (parent, name).
    parked: HashMap<NodeId, (NodeId, String)>,
    /// The local tree when the pending plan was made (for the fingerprint guard).
    planned_local: Tree,
    /// The server snapshot the pending plan was made from (contents to download), updated
    /// by the client's own remote writes: what the executor's mirror holds.
    planned_remote: Tree,
    /// Where the pending plan saw each remote node when it was made, whatever its own writes
    /// did since, the folders it saw in Recently deleted included (§12: what is in a folder's
    /// part in Recently deleted is its own).
    plan_saw: Tree,
    filter: Arc<Filter>,
    /// The filter its pending plan runs with: a rule change counts from the next plan, as the
    /// executor holds one for its run.
    run_filter: Arc<Filter>,
    /// The nodes of the deletes the guard held in the last plan.
    held: Vec<NodeId>,
    /// The nodes whose deletes the user confirmed, until no delete is pending for them.
    confirmed: HashSet<NodeId>,
    /// How the client holds tags.
    os: Os,
    /// How the client's folder sees the drive: the main folder sees it as it is, a role
    /// folder its top as the root and the rest as one folder it leaves out (§12).
    coords: Coords,
    /// Downloads fetched, not yet put in place ([`Sim::with_split_downloads`]): the version
    /// each acts on, with the bytes fetched (the executor's write-ahead record).
    fetched: BTreeMap<NodeId, DriveVersion>,
    /// Downloads and folders put in place whose commit a crash lost.
    placing: BTreeMap<NodeId, Placement>,
    /// The version of each node `planned_remote` holds (the executor's mirror).
    planned_refs: BTreeMap<NodeId, RemoteRef>,
    /// The folders in Recently deleted above live nodes, with when each went there, and the
    /// server's time of each live node's version, as the pending plan's input had them (the
    /// mirror's view of what a folder's removal took along).
    planned_removed: BTreeMap<NodeId, i64>,
    planned_changed: HashMap<NodeId, i64>,
    /// Where the pending plan's operations settled each node's base (the executor's): a later
    /// one's compare and set takes the base there as the one its plan saw.
    run_settled: HashMap<NodeId, VersionTag>,
    /// Files the last recovery found unhashed (their record kept): the plan defers them.
    unhashed: HashSet<NodeId>,
}

impl Client {
    fn new(name: String, os: Os, filter: Filter, coords: Coords) -> Self {
        Client {
            computer: name.clone(),
            name,
            local: Tree::new(),
            base: Tree::new(),
            extra: HashMap::new(),
            plan: VecDeque::new(),
            removed_by_plan: BTreeSet::new(),
            parked: HashMap::new(),
            planned_local: Tree::new(),
            planned_remote: Tree::new(),
            plan_saw: Tree::new(),
            run_filter: Arc::new(filter.clone()),
            filter: Arc::new(filter),
            held: Vec::new(),
            confirmed: HashSet::new(),
            os,
            coords,
            fetched: BTreeMap::new(),
            placing: BTreeMap::new(),
            planned_refs: BTreeMap::new(),
            planned_removed: BTreeMap::new(),
            planned_changed: HashMap::new(),
            run_settled: HashMap::new(),
            unhashed: HashSet::new(),
        }
    }

    /// The part of the drive the client's folder syncs.
    fn scope(&self) -> Scope {
        Scope::of(self.coords.top)
    }
}

/// The simulation.
#[derive(Debug)]
pub struct Sim {
    server: Server,
    clients: Vec<Client>,
    /// Content blob → its hash. Every edit or create writes a fresh blob.
    next_blob: u64,
    blobs: HashMap<ContentHash, u64>,
    /// Blobs some client replaced or deleted after holding them locally.
    superseded: BTreeSet<u64>,
    /// Blobs ever written.
    written: BTreeSet<u64>,
    pub ops_executed: usize,
    pub conflicts: usize,
    /// Items that stopped being tracked so far (each gets a new id).
    released: u64,
    /// Crashes so far ([`Event::Crash`]).
    crashes: u64,
    /// Local placements and tag writes so far: now and then a tag write fails (a volume
    /// refusing it), or a placement's commit (a commit write or a folder's stat failing).
    writes: u64,
    /// For scripted tests: the next placement with a record fails its commit.
    fail_commit_next: bool,
    /// For scripted tests: the next tag write fails.
    fail_tags_next: bool,
    /// Record every plan and skipped operation in `trace` (debugging).
    pub verbose: bool,
    pub trace: Vec<String>,
    /// The mass-delete guard, on with this minimum (the fraction 0), else off. With it on,
    /// [`Sim::quiesce`] confirms what is held, as a user eventually does.
    pub guard: Option<usize>,
    /// The first plan that deleted more than the guard allows without a confirmation.
    unguarded: Option<String>,
    /// Tags users added (on which client, to which node), and removed, per node; and nodes
    /// some user deleted. A tag added and never removed must be on its node in the end, unless
    /// the node was deleted. An item that gets a new id on its client takes its tags along.
    tags_added: BTreeSet<(usize, NodeId, String)>,
    tags_removed: BTreeSet<(NodeId, String)>,
    /// Tags a client removed from a node its base had them on, and not added again since:
    /// they stay removed, unless another client added them (a merge keeps either side's add).
    tags_cleared: BTreeSet<(usize, NodeId, String)>,
    /// Tags a client had added to an item it forked off as a conflict copy: the merge may give
    /// them to the original too, as that client's add.
    tags_forked: BTreeSet<(usize, NodeId, String)>,
    /// Tag adds a client's remote write sent up (until the client adds the tag again): they
    /// stand once the item goes from it; a restore keeps an add only if it holds it, or the add
    /// is still to go up ([`Sim::restored`]).
    tags_sent: BTreeSet<(usize, NodeId, String)>,
    /// Tags a download wrote on a client's item whose record recovery dropped (the item
    /// changed since: it can't be told from the file there before), that the item still
    /// holds: read against the old base, they are that client's adds, and a removal made
    /// elsewhere meanwhile may come back (sync semantics §8, §11: the known limit).
    tags_unproven: BTreeSet<(usize, NodeId, String)>,
    deleted: BTreeSet<NodeId>,
    /// The first operation a client ran outside its folder's part of the drive.
    scope_breach: Option<String>,
    /// The first folder a plan removed after something in it (node schema, Recently deleted:
    /// what a folder's delete takes along is removed no earlier than the folder).
    removed_after_contents: Option<String>,
    /// Clients that are two folders of one computer (the main folder, a role folder).
    computers: Vec<(usize, usize)>,
    /// A download takes two steps: fetching, then putting in place and committing; anything
    /// may happen between them ([`Sim::with_split_downloads`]).
    split_downloads: bool,
    /// The version the download just put in place acted on, for its commit.
    placed_from: Option<DriveVersion>,
}

impl Sim {
    pub fn new(clients: usize) -> Self {
        Sim {
            server: Server::default(),
            clients: (0..clients)
                .map(|i| {
                    let filter = match i % 2 {
                        1 => ignoring_d(),
                        _ => Filter::default(),
                    };
                    let os = [Os::MacOs, Os::Linux, Os::Windows][i % 3];
                    Client::new(format!("c{i}"), os, filter, Coords::MAIN)
                })
                .collect(),
            next_blob: 0,
            blobs: HashMap::new(),
            superseded: BTreeSet::new(),
            written: BTreeSet::new(),
            ops_executed: 0,
            conflicts: 0,
            released: 0,
            crashes: 0,
            writes: 0,
            fail_commit_next: false,
            fail_tags_next: false,
            verbose: false,
            trace: Vec::new(),
            guard: None,
            unguarded: None,
            tags_added: BTreeSet::new(),
            tags_forked: BTreeSet::new(),
            tags_sent: BTreeSet::new(),
            tags_removed: BTreeSet::new(),
            tags_unproven: BTreeSet::new(),
            tags_cleared: BTreeSet::new(),
            deleted: BTreeSet::new(),
            scope_breach: None,
            removed_after_contents: None,
            computers: Vec::new(),
            split_downloads: false,
            placed_from: None,
        }
    }

    /// A download's fetch and its commit are two steps, as on the transfer pool: the drive may
    /// move on, or remove the node, between them, and a crash ([`Event::Crash`]) can lose the
    /// commit of a file already in place, for recovery to settle.
    pub fn with_split_downloads(mut self) -> Self {
        self.split_downloads = true;
        self
    }

    /// [`Sim::with_role_folder`], and client 0's computer syncs it too, as a folder of its own
    /// (one more client, last, in the role folder's coordinates). Both folders converge to
    /// their parts of the drive; the user moves files between them ([`Action::MoveAcross`]).
    pub fn with_role_device(self) -> Self {
        self.with_role_folder().with_documents_on(0)
    }

    /// No role folder on the drive; client 0's and client 1's computers each add Documents (one
    /// more client each, last): the first to sync makes it on the drive, the other joins it,
    /// merging what both hold (plan E1).
    pub fn with_new_role_devices(self) -> Self {
        self.with_documents_on(0).with_documents_on(1)
    }

    /// Client `main`'s computer syncs Documents too: one more client, last, in its coordinates.
    fn with_documents_on(mut self, main: usize) -> Self {
        let first = &self.clients[main];
        let mut docs = Client::new(
            format!("{}-docs", first.name),
            first.os,
            Filter::default(),
            Coords {
                top: Role::Documents.node(),
            },
        );
        docs.computer = first.computer.clone();
        self.clients.push(docs);
        self.computers.push((main, self.clients.len() - 1));
        self
    }

    /// The server as client `c`'s folder sees it ([`Coords`]).
    fn server_view(&self, c: usize) -> Tree {
        let mut t = Tree::new();
        for id in self.server.live.keys() {
            if let Some(state) = self.server_state(c, *id) {
                t.insert(*id, state);
            }
        }
        if self.clients[c].coords != Coords::MAIN {
            t.insert(
                NodeId::OUTSIDE,
                NodeState::folder(NodeId::ROOT, Coords::OUTSIDE_NAME),
            );
        }
        t
    }

    /// The server's state of `node` as client `c` sees it.
    fn server_state(&self, c: usize, node: NodeId) -> Option<NodeState> {
        let coords = self.clients[c].coords;
        let n = self
            .server
            .live
            .get(&node)
            .filter(|_| !coords.hides(node))?;
        Some(NodeState {
            parent: coords.from_drive(n.state.parent),
            ..n.state.clone()
        })
    }

    /// What client `c`'s executor records after writing `node`: the server's state as its
    /// folder sees it.
    fn record_remote(&mut self, c: usize, node: NodeId) {
        if let Some(state) = self.server_state(c, node) {
            self.clients[c].planned_remote.insert(node, state);
        }
        if let Some(n) = self.server.live.get(&node) {
            self.clients[c].planned_refs.insert(node, n.remote);
        }
    }

    /// Whether drive node `node` is known to lie outside client `c`'s folder's part of the
    /// drive: in the role folder for the main folder, under the drive's top elsewhere for the
    /// role folder. A node whose place isn't known (a gap, or a cycle a sync's moves make for a
    /// moment) isn't.
    fn known_outside(&self, c: usize, node: NodeId) -> bool {
        match self.clients[c].coords.top {
            top if top.is_root() => self.server.in_role_folder(node),
            _ => self.server.place(node) == End::Top,
        }
    }

    /// Puts the Documents role folder on the server, named like the users' items (a role
    /// folder may have any name), with a file another device made in it. Every client syncs the
    /// main folder, which leaves role folders out (§12): no client may touch it or what is in
    /// it, whatever the phone does there ([`Action::Phone`]).
    pub fn with_role_folder(mut self) -> Self {
        let docs = Role::Documents.node();
        self.server
            .put(docs, NodeState::folder(NodeId::ROOT, NAMES[0]));
        let cv = NodeState::file(docs, NAMES[3], self.new_blob());
        self.server.put(NodeId(Uuid::from_u128(u128::MAX)), cv);
        self
    }

    /// [`Sim::with_role_folder`], with client 0 holding it and its file in its main folder as
    /// synced: what a bug could leave (no version ever synced a role folder there). It lets go
    /// of it as of anything masked (§10): a test of the masks, not a case to support.
    pub fn with_role_folder_held(self) -> Self {
        let mut sim = self.with_role_folder();
        for (node, n) in sim.server.live.clone() {
            let holder = &mut sim.clients[0];
            if n.state.kind == NodeKind::File {
                holder
                    .extra
                    .insert(node, extra(&n.state, Some(n.remote.version)));
            }
            holder.local.insert(node, n.state.clone());
            holder.base.insert(node, n.state);
        }
        sim
    }

    /// The phone deletes the role folder with everything in it, to Recently deleted.
    fn delete_role_folder(&mut self) {
        let docs = Role::Documents.node();
        if !self.server.live.contains_key(&docs) {
            return;
        }
        let mut below: Vec<NodeId> = self
            .server
            .live
            .keys()
            .copied()
            .filter(|n| self.server.in_role_folder(*n))
            .collect();
        // The folder first, then what is in it, top-down (node schema, Recently deleted).
        let tree = self.server.tree();
        below.sort_by_key(|n| tree.ancestors(*n).len());
        self.remove_live([docs].into_iter().chain(below));
    }

    /// Moves `nodes`, in order, from the tree to Recently deleted, on the server at once. A tag
    /// change loses to a delete (§11).
    fn remove_live(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        for node in nodes {
            if let Some(n) = self.server.live.remove(&node) {
                self.server.remove(node, n);
                self.deleted_now(node);
            }
        }
    }

    /// The folders [`Action::DeleteStopped`] picks from: the drive's, the role folder too.
    fn stopped_folders(&self) -> Vec<NodeId> {
        self.server
            .live
            .iter()
            .filter(|(_, s)| s.state.kind == NodeKind::Folder)
            .map(|(n, _)| *n)
            .collect()
    }

    /// [`Action::DeleteStopped`].
    fn delete_stopped(&mut self, pick: usize, done: usize) {
        let folders = self.stopped_folders();
        let Some(&folder) = folders.get(pick % folders.len().max(1)) else {
            return;
        };
        let tree = self.server.tree();
        let mut below: Vec<NodeId> = tree
            .subtree(folder)
            .into_iter()
            .filter(|n| *n != folder)
            .collect();
        below.sort_by_cached_key(|n| (tree.ancestors(*n).len(), *n));
        below.truncate(done % (below.len() + 1));
        self.remove_live([folder].into_iter().chain(below));
    }

    /// Another device (the phone) changes a file in the role folder: `op` picks adding, editing,
    /// removing (to Recently deleted) or moving one to the top, under a free name.
    fn phone(&mut self, op: usize, pick: usize) {
        let docs = Role::Documents.node();
        // Only in a live role folder: the file browser doesn't add to a deleted one.
        if !self.server.live.contains_key(&docs) {
            return;
        }
        let files: Vec<NodeId> = self
            .server
            .live
            .iter()
            .filter(|(n, s)| s.state.kind == NodeKind::File && self.server.in_role_folder(**n))
            .map(|(n, _)| *n)
            .collect();
        let tree = self.server.tree();
        let free = |parent: NodeId, name: &str| tree.child_named(parent, name, None).is_none();
        let version = VersionTag(self.server.uuid());
        match (op % 4, files.get(pick % files.len().max(1)).copied()) {
            (0, _) | (_, None) => {
                let name = NAMES[pick % NAMES.len()];
                if free(docs, name) {
                    let state = NodeState::file(docs, name, self.new_blob());
                    self.server.put(NodeId::new(), state);
                }
            }
            (1, Some(file)) => {
                let content = self.new_blob();
                let old = self.server.live.get(&file).and_then(|n| n.state.content);
                self.supersede(old);
                if let Some(n) = self.server.live.get_mut(&file) {
                    n.state.content = Some(content);
                    n.remote.version = version;
                }
            }
            (2, Some(file)) => {
                if let Some(n) = self.server.live.remove(&file) {
                    self.server.remove(file, n);
                    // A tag change loses to a delete (§11), the phone's too.
                    self.deleted_now(file);
                }
            }
            (_, Some(file)) => {
                if free(NodeId::ROOT, &self.server.live[&file].state.name)
                    && let Some(n) = self.server.live.get_mut(&file)
                {
                    n.state.parent = NodeId::ROOT;
                    n.remote.version = version;
                }
            }
        }
    }

    fn new_blob(&mut self) -> FileContent {
        self.next_blob += 1;
        let blob = self.next_blob;
        let hash = hash_bytes(&blob.to_le_bytes(), HashAlgorithm::Blake3);
        self.blobs.insert(hash, blob);
        self.written.insert(blob);
        FileContent {
            hash,
            size: 8,
            mtime_ms: blob as i64,
            executable: false,
        }
    }

    pub fn run(&mut self, events: &[Event]) {
        for e in events {
            match *e {
                Event::Act { client, action } => {
                    let c = client % self.clients.len();
                    self.act(c, action);
                    if self.verbose {
                        self.trace.push(format!(
                            "act c{c} {action:?} -> {:?}",
                            view_with(&self.clients[c].local, &self.blobs)
                        ));
                    }
                }
                Event::Plan { client } => self.plan(client % self.clients.len()),
                Event::Step { client } => {
                    let c = client % self.clients.len();
                    self.step(c);
                }
                Event::Confirm { client } => {
                    let c = client % self.clients.len();
                    let client = &mut self.clients[c];
                    let held = std::mem::take(&mut client.held);
                    client.confirmed.extend(held);
                }
                Event::Crash { client } => self.crash(client % self.clients.len()),
            }
        }
    }

    /// Client `c` dies during its next step ([`Event::Crash`]): a download it fetched, or a
    /// folder synced before (the engine's write-ahead record), is put in place and its commit
    /// lost; anything else isn't started.
    fn crash(&mut self, c: usize) {
        self.crashes += 1;
        let front = self.clients[c].plan.front().map(|(op, _)| op.clone());
        match front {
            Some(op @ Op::LocalDownload { node, .. })
                if self.clients[c].fetched.contains_key(&node) =>
            {
                // What the file it replaces held (nothing where there is none).
                let client = &self.clients[c];
                let prior = (client.os != Os::Windows)
                    .then(|| client.local.get(node).map(|l| l.tags.clone()))
                    .map(Option::unwrap_or_default);
                let fresh = !client.local.contains(node);
                if self.execute(c, &op) == Outcome::Done
                    && let Some(record) = self.placed_from.take()
                {
                    self.lose_commit(c, &op, record, prior, fresh);
                }
            }
            // A folder is made in place, then the crash, before its tags are written. Only one
            // synced before has the engine's write-ahead record, for the version it acts on: the
            // plan's, else the mirror's as the executor reads it then.
            Some(op @ Op::LocalMkdir { node, .. }) => {
                let client = &self.clients[c];
                let base_at = client.extra.get(&node).and_then(|e| e.version);
                let record = op.drive().cloned().or_else(|| {
                    Some(DriveVersion {
                        state: client.planned_remote.get(node)?.clone(),
                        at: *client.planned_refs.get(&node)?,
                        base_at,
                    })
                });
                let synced_before = client.base.contains(node);
                let fresh = !client.local.contains(node);
                if self.execute(c, &op) == Outcome::Done {
                    let client = &mut self.clients[c];
                    if let Some(mut state) = client.local.get(node).cloned() {
                        state.tags = Tags::default();
                        client.local.insert(node, state);
                    }
                    if synced_before && let Some(record) = record {
                        // Nothing was there.
                        self.lose_commit(c, &op, record, Some(Tags::default()), fresh);
                    } else {
                        self.placed_back(node);
                    }
                }
            }
            _ => {}
        }
        let client = &mut self.clients[c];
        client.plan.clear();
        client.fetched.clear();
        client.run_settled.clear();
    }

    /// `op` of client `c` put its item in place from `record`, and its commit is lost: the
    /// write-ahead record stays, claiming tags as the engine's does: a download those it wrote
    /// as the OS holds them, the empty set too (not known where the OS keeps none); a folder,
    /// made without tags and lost before they are written, holds none only if it is to hold
    /// none (not known otherwise). `prior`: what the item there held before; `fresh`: the node
    /// had no item here before (the scan finds the one put in place as new).
    fn lose_commit(
        &mut self,
        c: usize,
        op: &Op,
        record: DriveVersion,
        prior: Option<Tags>,
        fresh: bool,
    ) {
        let (Op::LocalDownload {
            node,
            parent,
            name,
            tags,
            ..
        }
        | Op::LocalMkdir {
            node,
            parent,
            name,
            tags,
            ..
        }) = op
        else {
            return;
        };
        self.placed_back(*node);
        let client = &mut self.clients[c];
        // The parking record goes with the commit, void now anyway.
        client.parked.remove(node);
        let held = tags.held_by(client.os);
        let tags = match op {
            Op::LocalDownload { .. } => (client.os != Os::Windows).then_some(held),
            _ => held.is_empty().then_some(held),
        };
        // Every other crash (the first too) leaves its file unhashed for a sync.
        let unhashed = matches!(op, Op::LocalDownload { .. }) && self.crashes % 2 == 1;
        let placement = Placement {
            record,
            parent: *parent,
            name: name.clone(),
            tags,
            prior,
            fresh,
            unhashed,
            held: false,
        };
        client.placing.insert(*node, placement);
        if self.verbose {
            self.trace
                .push(format!("crash c{c}: {node:?} put in place, commit lost"));
        }
    }

    /// `op` of client `c` put its item in place from `record`, and its commit failed (the run
    /// goes on, the operation ends): the write-ahead record stays, for the next plan's recovery,
    /// with the tags its write left (`None`: not known) and what was there before.
    fn keep_record(
        &mut self,
        c: usize,
        op: &Op,
        record: DriveVersion,
        tags: Option<Tags>,
        prior: Option<Tags>,
        fresh: bool,
    ) {
        let (Op::LocalDownload {
            node, parent, name, ..
        }
        | Op::LocalMkdir {
            node, parent, name, ..
        }) = op
        else {
            return;
        };
        self.placed_back(*node);
        let client = &mut self.clients[c];
        client.parked.remove(node);
        let placement = Placement {
            record,
            parent: *parent,
            name: name.clone(),
            tags,
            prior,
            fresh,
            unhashed: false,
            held: false,
        };
        client.placing.insert(*node, placement);
        if self.verbose {
            self.trace.push(format!(
                "  commit failed c{c}: {node:?} put in place, record kept"
            ));
        }
    }

    /// What the engine's recovery does at the start of a sync for the downloads and folders put
    /// in place whose commit was lost: the item still at its place with the fetched bytes is the
    /// node,
    /// settled as the commit would have (the same rule, against the same version, the item
    /// read with the tags it was put in place with).
    fn recover(&mut self, c: usize) {
        self.clients[c].unhashed.clear();
        for (node, p) in std::mem::take(&mut self.clients[c].placing) {
            let there = self.record_there(c, node, &p);
            if there && p.unhashed {
                // The item can't be read as the node yet: the record stays, for the next sync.
                let client = &mut self.clients[c];
                client.unhashed.insert(node);
                let p = Placement {
                    unhashed: false,
                    ..p
                };
                client.placing.insert(node, p);
                continue;
            }
            if self.verbose {
                let local = self.clients[c].local.get(node);
                self.trace
                    .push(format!("recover c{c} {node:?} there={there} {local:?}"));
            }
            if there {
                if !self.settle_record(c, node, &p) {
                    // Its node gone, its item not one the drive takes: held once (as a file not
                    // hashed yet), then dropped.
                    if !p.held {
                        let p = Placement { held: true, ..p };
                        self.clients[c].placing.insert(node, p);
                    }
                }
            } else if !p.fresh {
                self.unproven_tags(c, node, &p);
            } else if self.clients[c].local.contains(node) {
                // An item new to this client (a first download, a folder made again after a
                // local delete) moved or changed since it was put in place: the scan found it
                // as a new item, and recovery doesn't take it for the node.
                let new = self.release_id();
                rekey_client(&mut self.clients[c], node, new);
                self.retrack(c, node, new);
            }
        }
    }

    /// The record `p` of the node's own item on client `c` dropped (the item changed since): the
    /// tags its download wrote, changing what the item held before, that the item still holds
    /// are [`Sim::tags_unproven`].
    fn unproven_tags(&mut self, c: usize, node: NodeId, p: &Placement) {
        let (Some(written), Some(before)) = (&p.tags, &p.prior) else {
            return;
        };
        let Some(item) = self.clients[c].local.get(node) else {
            return;
        };
        for tag in written.iter() {
            if before.get(&tag.name) != Some(tag) && item.tags.get(&tag.name) == Some(tag) {
                self.tags_unproven.insert((c, node, tag.name.clone()));
            }
        }
    }

    /// Whether the item record `p` places is at its place with the record's bytes.
    fn record_there(&self, c: usize, node: NodeId, p: &Placement) -> bool {
        self.clients[c].local.get(node).is_some_and(|l| {
            l.parent == p.parent && l.name == p.name && l.content == p.record.state.content
        })
    }

    /// Settles record `p`, its item there, as the commit would (the engine's
    /// `settle_placement`, recovery's and a retry's alike): where this client's mirror no
    /// longer has its node ([`Sim::gone_for`]), at the record's version whatever the plan saw,
    /// so the removal reaches the item. Returns whether it did: not where the node is gone and
    /// the item isn't one the drive takes (the record stays).
    fn settle_record(&mut self, c: usize, node: NodeId, p: &Placement) -> bool {
        let mut record = p.record.clone();
        if self.gone_for(c, node) {
            let client = &self.clients[c];
            if !client.local.contains(node) || !verdict(client, node).is_sync() {
                return false;
            }
            record.base_at = client
                .extra
                .get(&node)
                .and_then(|e| e.version)
                .filter(|_| client.base.contains(node));
        }
        let how = Against::Placed(&record);
        let tags = match (&p.prior, &p.tags) {
            (Some(before), Some(written)) => TagWrite::Wrote { before, written },
            _ => TagWrite::Unknown,
        };
        self.settle_against(c, node, how, true, tags, None);
        true
    }

    /// Whether client `c`'s mirror no longer has `node` as its plan would read it (the engine's
    /// `gone_on_drive`): not there, or in a folder in Recently deleted, written no later than
    /// that folder's removal (taken along; a version of its own writing has no time yet).
    fn gone_for(&self, c: usize, node: NodeId) -> bool {
        let client = &self.clients[c];
        let Some(state) = client.planned_remote.get(node) else {
            return true;
        };
        let version = client.planned_refs.get(&node).map(|r| time(r.version));
        let changed = client
            .planned_changed
            .get(&node)
            .copied()
            .filter(|t| Some(*t) == version);
        let mut seen = HashSet::new();
        let mut at = state.parent;
        while !at.is_root() && seen.insert(at) {
            if let Some(removed) = client.planned_removed.get(&at) {
                return changed.is_none_or(|t| t <= *removed);
            }
            match client.planned_remote.get(at) {
                Some(up) => at = up.parent,
                None => break,
            }
        }
        false
    }

    // ---- user actions ----

    /// The folders a create's `parent` picks from on client `c` (its root included).
    fn folders_of(&self, c: usize) -> Vec<NodeId> {
        let mut folders: Vec<NodeId> = vec![NodeId::ROOT];
        folders.extend(
            self.clients[c]
                .local
                .iter()
                .filter(|(_, s)| s.kind == NodeKind::Folder)
                .map(|(id, _)| id),
        );
        folders.sort();
        folders
    }

    /// The items an action's `node` picks from on client `c`.
    fn items_of(&self, c: usize) -> Vec<NodeId> {
        let mut all: Vec<NodeId> = self.clients[c].local.ids().collect();
        all.sort();
        all
    }

    /// For scripted tests: an item on client `c` whose name ends with `suffix`.
    pub fn item_named(&self, c: usize, suffix: &str) -> Option<NodeId> {
        self.clients[c]
            .local
            .iter()
            .find(|(_, s)| s.name.ends_with(suffix))
            .map(|(id, _)| id)
    }

    /// For scripted tests: the tags (`name:colour`) of `node` in Recently deleted.
    pub fn removed_tags(&self, node: NodeId) -> Vec<String> {
        self.server
            .removed
            .get(&node)
            .map(|n| tag_names(&n.state.tags))
            .unwrap_or_default()
    }

    /// For scripted tests: the next placement with a record fails its commit once its item is
    /// in place.
    pub fn fail_next_commit(&mut self) {
        self.fail_commit_next = true;
    }

    /// For scripted tests: the next tag write fails (where the OS keeps tags).
    pub fn fail_next_tag_write(&mut self) {
        self.fail_tags_next = true;
    }

    /// For scripted tests: the item at `path` on client `c`.
    pub fn item(&self, c: usize, path: &str) -> Option<NodeId> {
        at_path(&self.clients[c].local, path)
    }

    /// For scripted tests: what an action's `node` (a create's `parent` for a folder) is to pick
    /// `node` on client `c`.
    pub fn pick(&self, c: usize, node: NodeId, as_parent: bool) -> Option<usize> {
        let list = if as_parent {
            self.folders_of(c)
        } else {
            self.items_of(c)
        };
        list.iter().position(|n| *n == node)
    }

    /// For scripted tests: what `Edit`'s `file` is to pick file `node` on client `c`.
    pub fn file_pick(&self, c: usize, node: NodeId) -> Option<usize> {
        self.files_of(c).iter().position(|n| *n == node)
    }

    /// The files an action's `file` picks from on client `c`.
    fn files_of(&self, c: usize) -> Vec<NodeId> {
        let mut files: Vec<NodeId> = self.clients[c]
            .local
            .iter()
            .filter(|(_, s)| s.kind == NodeKind::File)
            .map(|(id, _)| id)
            .collect();
        files.sort();
        files
    }

    /// For scripted tests: what `Restore`'s `node` is to pick `node` in Recently deleted.
    pub fn removed_pick(&self, node: NodeId) -> Option<usize> {
        self.server.removed.keys().position(|n| *n == node)
    }

    /// The paths of the server's live nodes, sorted.
    pub fn server_paths(&self) -> Vec<String> {
        let tree = self.server.tree();
        let mut paths: Vec<String> = tree
            .ids()
            .filter_map(|n| tree.path(n).map(|p| p.join("/")))
            .collect();
        paths.sort();
        paths
    }

    /// For scripted tests: what [`Action::DeleteStopped`]'s `pick` is to pick folder `node`.
    pub fn stopped_pick(&self, node: NodeId) -> Option<usize> {
        self.stopped_folders().iter().position(|n| *n == node)
    }

    /// Whether `node` is in Recently deleted.
    pub fn is_removed(&self, node: NodeId) -> bool {
        self.server.removed.contains_key(&node)
    }

    fn act(&mut self, c: usize, action: Action) {
        let folders = self.folders_of(c);
        let all = self.items_of(c);
        let files = self.files_of(c);
        let local = &self.clients[c].local;
        let name_free = |t: &Tree, parent: NodeId, name: &str, except: Option<NodeId>| {
            t.child_named(parent, name, except).is_none()
        };
        match action {
            Action::Tag { node, tag } | Action::Colour { node, tag } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                let (name, colour) = TAGS[tag % TAGS.len()];
                let os = self.clients[c].os;
                let Some(mut state) = local.get(node).cloned() else {
                    return;
                };
                let mut tags: Vec<Tag> = state.tags.iter().cloned().collect();
                let had = tags.iter().position(|t| t.name == name);
                match (action, had) {
                    (Action::Colour { .. }, Some(i)) if os == Os::MacOs => {
                        tags[i].colour = (tags[i].colour + 1) % 8;
                    }
                    (Action::Tag { .. }, Some(i)) => {
                        tags.remove(i);
                        self.tags_removed.insert((node, name.to_owned()));
                        let synced = self.clients[c]
                            .base
                            .get(node)
                            .is_some_and(|b| b.tags.get(name).is_some());
                        if synced {
                            self.tags_cleared.insert((c, node, name.to_owned()));
                        }
                    }
                    (Action::Tag { .. }, None) => {
                        let Some(tag) = express(&Tag::new(name, colour), os) else {
                            return; // this OS can't hold it
                        };
                        tags.push(tag);
                        self.tags_added.insert((c, node, name.to_owned()));
                        self.tags_cleared.remove(&(c, node, name.to_owned()));
                        // Added again: not sent up yet.
                        self.tags_sent.remove(&(c, node, name.to_owned()));
                    }
                    _ => return,
                }
                state.tags = Tags::new(tags);
                self.clients[c].local.insert(node, state);
            }
            Action::Restore { node } => self.restore(node),
            Action::Phone { op, pick } => self.phone(op, pick),
            Action::DeleteRoleFolder => self.delete_role_folder(),
            Action::DeleteStopped { pick, done } => self.delete_stopped(pick, done),
            Action::MoveAcross { file } => {
                let Some(&(a, b)) = self.computers.iter().find(|(a, b)| *a == c || *b == c) else {
                    return;
                };
                let to = if a == c { b } else { a };
                if files.is_empty() {
                    return;
                }
                let node = files[file % files.len()];
                let Some(state) = self.clients[c].local.get(node).cloned() else {
                    return;
                };
                // Not one the executor holds under a temporary name; to the other folder's top,
                // if its name is free there.
                if self.clients[c].parked.contains_key(&node)
                    || !name_free(&self.clients[to].local, NodeId::ROOT, &state.name, None)
                {
                    return;
                }
                self.clients[c].local.remove(node);
                // The other folder's scan finds an item it never had: a new id. Its tags went
                // along.
                let new = self.release_id();
                self.clients[to].local.insert(
                    new,
                    NodeState {
                        parent: NodeId::ROOT,
                        ..state
                    },
                );
                self.retrack(c, node, new);
                let moved: Vec<_> = self
                    .tags_added
                    .iter()
                    .filter(|(at, n, _)| *at == c && *n == new)
                    .cloned()
                    .collect();
                for (_, n, name) in moved {
                    self.tags_added.remove(&(c, n, name.clone()));
                    self.tags_added.insert((to, n, name));
                }
            }
            Action::ListDeleted => {
                self.server.listed = Some(self.server.removed.iter().map(Server::at).collect());
            }
            Action::EmptyListed => self.empty_listed(),
            Action::EmptyItem { pick } => self.empty_item(pick),
            Action::EditTaken { pick } => {
                let taken: Vec<NodeId> = self
                    .server
                    .taken_along()
                    .into_iter()
                    .filter(|n| self.server.live[n].state.kind == NodeKind::File)
                    .collect();
                let Some(&file) = taken.get(pick % taken.len().max(1)) else {
                    return;
                };
                let content = self.new_blob();
                let version = VersionTag(self.server.uuid());
                let old = self.server.live.get(&file).and_then(|n| n.state.content);
                self.supersede(old);
                if let Some(n) = self.server.live.get_mut(&file) {
                    n.state.content = Some(content);
                    n.remote.version = version;
                }
            }
            Action::Purge => {
                let taken = self.server.purge();
                self.remove_live(taken);
                let due = self.server.due();
                self.delete_for_good(&due);
            }
            Action::ToggleIgnore => {
                let client = &mut self.clients[c];
                client.filter = Arc::new(if client.filter.rule_count() == 0 {
                    ignoring_d()
                } else {
                    Filter::default()
                });
            }
            Action::CreateJunk { parent } => {
                let parent = folders[parent % folders.len()];
                if name_free(local, parent, JUNK, None) {
                    let state = NodeState::file(parent, JUNK, self.new_blob());
                    self.clients[c].local.insert(NodeId::new(), state);
                }
            }
            Action::CreateFile { parent, name } | Action::CreateFolder { parent, name } => {
                let parent = folders[parent % folders.len()];
                let name = NAMES[name % NAMES.len()];
                if !name_free(local, parent, name, None) {
                    return;
                }
                let state = if matches!(action, Action::CreateFile { .. }) {
                    NodeState::file(parent, name, self.new_blob())
                } else {
                    NodeState::folder(parent, name)
                };
                self.clients[c].local.insert(NodeId::new(), state);
            }
            Action::Edit { file } => {
                if files.is_empty() {
                    return;
                }
                let node = files[file % files.len()];
                let mut state = local.get(node).cloned().unwrap_or_else(|| unreachable!());
                self.supersede(state.content);
                state.content = Some(self.new_blob());
                self.clients[c].local.insert(node, state);
            }
            Action::Move { node, parent, name } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                let parent = folders[parent % folders.len()];
                let name = NAMES[name % NAMES.len()];
                if local.would_cycle(node, parent) || !name_free(local, parent, name, Some(node)) {
                    return;
                }
                let mut state = local.get(node).cloned().unwrap_or_else(|| unreachable!());
                state.parent = parent;
                state.name = name.to_owned();
                self.clients[c].local.insert(node, state);
            }
            Action::Delete { node } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                for n in local.subtree(node) {
                    // What the server has goes to Recently deleted, and must be found there.
                    let content = self.clients[c].local.get(n).and_then(|s| s.content);
                    let on_server = self
                        .server
                        .live
                        .get(&n)
                        .is_some_and(|s| s.state.content == content);
                    if !on_server {
                        self.supersede(content);
                    }
                }
                for n in self.clients[c].local.subtree(node) {
                    self.clients[c].local.remove(n);
                    self.deleted_here(c, n);
                    self.tags_added
                        .retain(|(at, node, _)| (*at, *node) != (c, n));
                }
            }
        }
    }

    fn restore(&mut self, pick: usize) {
        let Some(node) = self
            .server
            .removed
            .keys()
            .nth(pick % self.server.removed.len().max(1))
            .copied()
        else {
            return;
        };
        self.restore_with(node);
    }

    /// Brings `node` back from Recently deleted with what was deleted with it, folders before
    /// what is in them; what doesn't come back keeps what is in it there.
    fn restore_with(&mut self, node: NodeId) {
        let mut queue = VecDeque::from([node]);
        while let Some(n) = queue.pop_front() {
            let with = self.server.deleted_with(n);
            self.restore_node(n);
            if self.server.live.contains_key(&n) {
                queue.extend(with);
            }
        }
    }

    /// Brings `node` back from Recently deleted: into its folder if that is live, else at the
    /// top, under a free name (it stays deleted if none is).
    fn restore_node(&mut self, node: NodeId) {
        let Some(mut n) = self.server.removed.remove(&node) else {
            return;
        };
        if !n.state.parent.is_root() && !self.server.live.contains_key(&n.state.parent) {
            n.state.parent = NodeId::ROOT;
        }
        let tree = self.server.tree();
        let Some(name) = restored_names(&n.state.name)
            .find(|name| tree.child_named(n.state.parent, name, None).is_none())
        else {
            self.server.removed.insert(node, n); // no free name: it stays deleted
            return;
        };
        n.state.name = name;
        n.remote.version = VersionTag(self.server.uuid());
        self.server.live.insert(node, n);
        self.restored_on_drive(node);
    }

    /// `node` left the drive (a delete won): it settles the tag removals made before it.
    fn deleted_now(&mut self, node: NodeId) {
        self.deleted.insert(node);
        self.tags_cleared.retain(|(_, n, _)| *n != node);
    }

    /// The user of client `c` deleted `node` there: what they did to its tags there went with
    /// it, won or lost; other clients' tag changes stand until the delete wins.
    fn deleted_here(&mut self, c: usize, node: NodeId) {
        self.deleted.insert(node);
        self.tags_cleared
            .retain(|(by, n, _)| (*by, *n) != (c, node));
        self.tags_added.retain(|(at, n, _)| (*at, *n) != (c, node));
    }

    /// `node` is back on the drive (from Recently deleted, or made again after the purge).
    fn restored_on_drive(&mut self, node: NodeId) {
        let held = self
            .server
            .tags_at_removal
            .get(&node)
            .cloned()
            .unwrap_or_default();
        self.restored(node, &held);
    }

    /// `node` is back where a user deleted it (on the drive, or here where the delete lost)
    /// with the tags named `held`: no longer deleted, so what users do to its tags from now on
    /// counts. What they did before is settled by the delete: tag removals (the version back
    /// has what it had), and tags added are there only if it holds them; but for an add a
    /// client hasn't sent up and its item, synced there, still holds: that client never saw
    /// the delete (one that did lost the item, and the add with it), and sends the add up
    /// against the version back, as any add.
    fn restored(&mut self, node: NodeId, held: &BTreeSet<String>) {
        if !self.deleted.remove(&node) {
            return;
        }
        self.tags_cleared.retain(|(_, n, _)| *n != node);
        let (sent, clients) = (&self.tags_sent, &self.clients);
        self.tags_added.retain(|add| {
            let (c, n, name) = add;
            let pending = || {
                *c != DETACHED
                    && !sent.contains(add)
                    && clients.get(*c).is_some_and(|client| {
                        client
                            .local
                            .get(node)
                            .is_some_and(|item| item.tags.get(name).is_some())
                            && !masked(&client.local, &client.filter).contains(&node)
                    })
            };
            *n != node || held.contains(name) || pending()
        });
    }

    /// A client put `node` in place: where a user's delete of it lost (the drive has it), it is
    /// no longer deleted. A lost delete settles nothing: what other clients did to its tags
    /// still counts. One the drive doesn't have (a folder made here to restore it, its
    /// RemoteCreate to follow) is back once the drive has it again ([`Sim::restored_on_drive`]):
    /// a delete that won settles the tags then.
    fn placed_back(&mut self, node: NodeId) {
        if self.server.live.contains_key(&node) {
            self.deleted.remove(&node);
        }
    }

    /// [`Action::EmptyListed`].
    fn empty_listed(&mut self) {
        if let Some(listed) = self.server.listed.clone() {
            self.delete_for_good(&listed);
        }
    }

    /// [`Action::EmptyItem`].
    fn empty_item(&mut self, pick: usize) {
        let Some(listed) = self.server.listed.clone() else {
            return;
        };
        let rows: Vec<(NodeId, VersionTag)> = listed.iter().copied().collect();
        let Some(&(item, _)) = rows.get(pick % rows.len().max(1)) else {
            return;
        };
        let server = &self.server;
        if !server.removed.contains_key(&item) {
            return;
        }
        let folder = |n: NodeId| {
            server
                .removed
                .get(&n)
                .is_some_and(|r| r.state.kind == NodeKind::Folder)
        };
        let Ok(inside) = deleted::inside(
            item,
            |n| (*n, folder(*n)),
            |f| {
                Ok::<_, std::convert::Infallible>(
                    server
                        .removed
                        .iter()
                        .filter(|(_, r)| r.state.parent == f)
                        .map(|(n, _)| *n)
                        .collect(),
                )
            },
        );
        let chosen: BTreeSet<(NodeId, VersionTag)> = inside
            .iter()
            .filter_map(|n| server.removed.get_key_value(n).map(Server::at))
            .filter(|at| listed.contains(at))
            .collect();
        // Kept only by a live node in it, or a row in it written since the listing.
        let kept = chosen.len() < inside.len()
            || server
                .live
                .values()
                .any(|n| inside.contains(&n.state.parent));
        self.delete_for_good(&chosen);
        if !kept && self.server.removed.contains_key(&item) && self.server.emptied_kept.is_none() {
            self.server.emptied_kept = Some(format!("{item:?}"));
        }
    }

    /// Deletes for good the rows of Recently deleted still at a version in `chosen`, as the
    /// engine selects them ([`deleted::select`], with what the folders hold on the server), in
    /// its order. Records the first it deletes that it mustn't: one not chosen at that version
    /// (a live node, say), or a folder holding anything not going after it.
    fn delete_for_good(&mut self, chosen: &BTreeSet<(NodeId, VersionTag)>) {
        let server = &self.server;
        let rows: Vec<deleted::Chosen> = server
            .removed
            .iter()
            .filter(|(n, r)| chosen.contains(&Server::at((n, r))))
            .map(|(n, r)| deleted::Chosen {
                node: *n,
                parent: r.state.parent,
                folder: r.state.kind == NodeKind::Folder,
            })
            .collect();
        let mut holdings = deleted::Holdings::default();
        for r in server.removed.values() {
            *holdings.removed.entry(r.state.parent).or_default() += 1;
        }
        holdings.live = server
            .live
            .values()
            .map(|n| n.state.parent)
            .filter(|p| server.removed.contains_key(p))
            .collect();
        let order = deleted::select(&rows, &holdings);
        let mut pending: BTreeSet<NodeId> = order.iter().copied().collect();
        for n in order {
            pending.remove(&n);
            let was_chosen = self
                .server
                .removed
                .get_key_value(&n)
                .is_some_and(|row| chosen.contains(&Server::at(row)));
            let held = self
                .server
                .live
                .iter()
                .chain(&self.server.removed)
                .find(|(c, s)| s.state.parent == n && !pending.contains(*c))
                .map(|(c, _)| *c);
            if (!was_chosen || held.is_some()) && self.server.emptied_unlisted.is_none() {
                self.server.emptied_unlisted = Some(format!("{n:?} (holding {held:?})"));
            }
            self.server.removed_at.remove(&n);
            self.server.tags_at_removal.remove(&n);
            if let Some(gone) = self.server.removed.remove(&n) {
                self.deleted_now(n);
                self.supersede(gone.state.content);
            }
        }
    }

    fn supersede(&mut self, content: Option<FileContent>) {
        if let Some(blob) = content.and_then(|c| self.blobs.get(&c.hash)) {
            self.superseded.insert(*blob);
        }
    }

    // ---- sync ----

    fn plan(&mut self, c: usize) {
        self.clients[c].run_filter = Arc::clone(&self.clients[c].filter);
        // A new run: what the last one fetched and didn't put in place is gone (its temporary
        // file is fetched again, for what the drive holds then); a lost commit is recovered.
        self.clients[c].fetched.clear();
        self.recover(c);
        // A role folder that isn't on the drive comes back from Recently deleted with what was
        // deleted with it (node schema: in it, removed no earlier than it; not what was restored
        // from it since and deleted again elsewhere), or is made, by the first of its devices to
        // sync (as `Drive::ensure_role`), named like the users' items.
        let top = self.clients[c].coords.top;
        if !top.is_root() && !self.server.live.contains_key(&top) {
            if self.server.removed.contains_key(&top) {
                self.restore_with(top);
            } else {
                self.server
                    .put(top, NodeState::folder(NodeId::ROOT, NAMES[0]));
            }
        }
        self.unpark(c);
        self.release_filtered(c);
        let remote = self.server_view(c);
        let client = &self.clients[c];
        // A parked node is reported at its original place: the temporary name is the
        // executor's business and must never reach the remote.
        // A record only counts while the node still sits at its parked name: if the user moved
        // it since, the user's move is what the reconciler must see.
        let mut local_view = client.local.clone();
        for (node, (parent, name)) in &client.parked {
            if let Some(state) = local_view.get(*node).cloned()
                && state.name == parked_name(*node)
            {
                local_view.insert(
                    *node,
                    NodeState {
                        parent: *parent,
                        name: name.clone(),
                        ..state
                    },
                );
            }
        }
        // The scanner never reports filtered items (judged by their names on disk).
        for n in masked(&client.local, &client.filter) {
            local_view.remove(n);
        }
        // A file not hashed yet is reported without content: the plan defers it.
        for n in &client.unhashed {
            if let Some(state) = local_view.get(*n).cloned() {
                local_view.insert(
                    *n,
                    NodeState {
                        content: None,
                        ..state
                    },
                );
            }
        }
        // What the client can't hold of the tags is the drive's, as the engine's view has it.
        see_held_tags(client, &mut local_view, &remote);
        let mut input = Input {
            base: client.base.clone(),
            local: local_view,
            remote: remote.clone(),
            base_extra: client.extra.clone(),
            remote_complete: true,
            local_unhashed: client.unhashed.iter().copied().collect(),
            ..Default::default()
        };
        input.remote_refs = self
            .server
            .live
            .iter()
            .filter(|(id, _)| !client.coords.hides(**id))
            .map(|(id, n)| (*id, n.remote))
            .collect();
        // The folders in Recently deleted that live nodes are in, and the removed ones above
        // them, and the server's time of each live node's version, as the engine's view has them.
        let outside = client.scope().outside;
        for mut folder in remote.missing_parents() {
            while !input.removed.contains_key(&folder)
                && !outside.contains(&folder)
                && let Some(n) = self.server.removed.get(&client.coords.to_drive(folder))
            {
                let state = NodeState {
                    parent: client.coords.from_drive(n.state.parent),
                    ..n.state.clone()
                };
                let drive = client.coords.to_drive(folder);
                // Known at once, the engine's own removal's too: the write's answer has it.
                let at = self.server.removed_at[&drive];
                let parent = state.parent;
                input.removed.insert(folder, Removed { at, state });
                folder = parent;
            }
        }
        input.changed_at = remote
            .ids()
            .filter_map(|id| {
                let n = self.server.live.get(&client.coords.to_drive(id))?;
                Some((id, time(n.remote.version)))
            })
            .collect();
        input.dirty = client
            .base
            .ids()
            .chain(client.local.ids())
            .chain(remote.ids())
            .collect();
        let confirmed = Arc::new(client.confirmed.clone());
        let synced_files = client
            .base
            .iter()
            .filter(|(_, s)| s.kind == NodeKind::File)
            .count();
        let opts = Options {
            device: client.name.clone(),
            computer: client.computer.clone(),
            date: "2026-10-08".into(),
            allow_mass_delete: self.guard.is_none(),
            mass_delete_min: self.guard.unwrap_or_default(),
            mass_delete_fraction: 0.0,
            synced_files,
            confirmed: Arc::clone(&confirmed),
            filter: Arc::clone(&client.filter),
            scope: Arc::new(client.scope()),
            ..Options::default()
        };
        // Ids the client or the server knows are taken, as in the executor's state DB.
        let plan = reconcile_with_known(&input, &opts, |id| {
            client.local.contains(id) || client.base.contains(id) || self.server.ever.contains(&id)
        });
        // The guard's rule, checked independently: no plan deletes more files on a side than
        // the limit (or every file) without the user's confirmation.
        if let Some(min) = self.guard
            && self.unguarded.is_none()
        {
            let trips = |n: usize| n > min || (synced_files > 0 && n >= synced_files && n > 1);
            let files_under = |tree: &Tree, node: NodeId| {
                tree.subtree(node)
                    .into_iter()
                    .filter(|n| tree.get(*n).is_some_and(|s| s.kind == NodeKind::File))
                    .count()
            };
            let (mut deleted, mut trashed) = (0, 0);
            for op in plan.ops.iter().filter(|op| !confirmed.contains(&op.node())) {
                match op {
                    Op::RemoteDelete { node, .. }
                        if remote.get(*node).is_some_and(|s| s.kind == NodeKind::File) =>
                    {
                        deleted += 1;
                    }
                    _ => trashed += op.trashes().map_or(0, |n| files_under(&input.local, n)),
                }
            }
            if trips(deleted) || trips(trashed) {
                self.unguarded = Some(format!(
                    "{} deletes {deleted} remote and trashes {trashed} local files unconfirmed \
                     (limit {min}, {synced_files} synced): {:?}",
                    client.name, plan.ops
                ));
            }
        }
        if self.verbose && !plan.ops.is_empty() {
            self.trace
                .push(format!("plan {}: {:?}", client.name, plan.ops));
            self.trace.push(format!(
                "  local:  {:?}\n  base:   {:?}\n  remote: {:?}",
                view_with(&client.local, &self.blobs),
                view_with(&client.base, &self.blobs),
                view_with(&remote, &self.blobs)
            ));
        }
        self.conflicts += plan.conflicts.len();
        let client = &mut self.clients[c];
        // A confirmation lasts while the plan deletes its node.
        let deleting: HashSet<NodeId> = plan.ops.iter().filter_map(Op::deletes).collect();
        client.confirmed = confirmed.intersection(&deleting).copied().collect();
        client.held = plan.held.iter().map(Op::node).collect();
        client.plan = plan.ops.into_iter().map(|op| (op, 0)).collect();
        client.removed_by_plan.clear();
        client.planned_local = client.local.clone();
        client.planned_remote = remote;
        client.planned_refs = std::mem::take(&mut input.remote_refs).into_iter().collect();
        client.planned_removed = input.removed.iter().map(|(f, r)| (*f, r.at)).collect();
        client.planned_changed = std::mem::take(&mut input.changed_at).into_iter().collect();
        client.run_settled.clear();
        client.plan_saw = std::mem::take(&mut input.remote);
        for (folder, r) in input.removed {
            if !client.plan_saw.contains(folder) {
                client.plan_saw.insert(folder, r.state);
            }
        }
    }

    /// The id the scanner gives an item it stopped tracking: any new one (counted, to keep
    /// runs repeatable).
    fn release_id(&mut self) -> NodeId {
        self.released += 1;
        let hash = hash_bytes(&self.released.to_le_bytes(), HashAlgorithm::Sha256);
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.bytes[..16]);
        NodeId(Uuid::from_bytes(bytes))
    }

    /// The scanner keeps no rows for filtered items: one that was tracked loses its node id,
    /// and gets a new one if it ever becomes visible again.
    fn release_filtered(&mut self, c: usize) {
        let hidden = masked(&self.clients[c].local, &self.clients[c].filter);
        let mut tracked: Vec<NodeId> = hidden
            .into_iter()
            .filter(|n| {
                self.clients[c].base.contains(*n)
                    || self.server.ever.contains(n)
                    || self.clients[c].planned_remote.contains(*n)
            })
            .collect();
        tracked.sort();
        for n in tracked {
            let new = self.release_id();
            let client = &mut self.clients[c];
            client.parked.remove(&n);
            rekey_client(client, n, new);
            self.retrack(c, n, new);
        }
    }

    /// Item `node` of client `c` is `new` from now on: the tags added to it there go along.
    fn retrack(&mut self, c: usize, node: NodeId, new: NodeId) {
        let moved: Vec<String> = self
            .tags_added
            .iter()
            .filter(|(at, n, _)| *at == c && *n == node)
            .map(|(_, _, name)| name.clone())
            .collect();
        for name in moved {
            self.tags_added.remove(&(c, node, name.clone()));
            self.tags_added.insert((c, new, name));
        }
        // A removal on the item before counts for it after.
        let removed: Vec<String> = self
            .tags_removed
            .iter()
            .filter(|(n, _)| *n == node)
            .map(|(_, name)| name.clone())
            .collect();
        for name in removed {
            self.tags_removed.insert((new, name));
        }
        let cleared: Vec<String> = self
            .tags_cleared
            .iter()
            .filter(|(at, n, _)| *at == c && *n == node)
            .map(|(_, _, name)| name.clone())
            .collect();
        // The item's removal is the new id's now: the old node no longer has it.
        for name in cleared {
            self.tags_cleared.remove(&(c, node, name.clone()));
            self.tags_cleared.insert((c, new, name));
        }
    }

    /// Executes the next planned operation. Returns `false` if the plan is empty.
    fn step(&mut self, c: usize) -> bool {
        let Some((op, attempts)) = self.clients[c].plan.pop_front() else {
            return false;
        };
        let node = op.node();
        // Every remote write that lands stays in the folder's part of the drive (§12): what it
        // writes, and where to (judged before it runs; one refused on its version wrote nothing).
        let breach = self.breaches(c, &op);
        // A move to where the item already is puts nothing in place.
        let moves = match &op {
            Op::LocalMove { parent, name, .. } => self.clients[c]
                .local
                .get(node)
                .is_none_or(|l| (&l.parent, &l.name) != (parent, name)),
            _ => true,
        };
        let placement = matches!(op, Op::LocalDownload { .. } | Op::LocalMkdir { .. });
        let synced_before = self.clients[c].base.contains(node);
        // What the item held of tags before the operation (nothing where there was none).
        let held_before = self.clients[c].local.get(node).cloned();
        let before_tags = held_before.as_ref().map(|l| l.tags.clone());
        let fresh = held_before.is_none();
        let outcome = self.execute(c, &op);
        let landed = outcome == Outcome::Landed;
        let outcome = if landed { Outcome::Done } else { outcome };
        if outcome == Outcome::Done && breach && self.scope_breach.is_none() {
            self.scope_breach = Some(format!("{} {op:?}", self.clients[c].name));
        }
        if outcome == Outcome::Done && self.server.dangling.is_none() {
            let parent_unknown = self.server.live.get(&node).is_some_and(|n| {
                !n.state.parent.is_root() && !self.server.ever.contains(&n.state.parent)
            });
            if parent_unknown {
                self.server.dangling = Some(format!("c{c} {op:?}"));
            }
        }
        if self.verbose && outcome != Outcome::Done {
            self.trace.push(format!("  {outcome:?} {op:?}"));
        }
        if outcome == Outcome::Retry && attempts < 3 {
            self.clients[c].plan.push_back((op.clone(), attempts + 1));
        }
        if outcome == Outcome::Fetched {
            // Put in place and committed by the next step, whatever happens meanwhile.
            self.clients[c].plan.push_front((op, attempts));
            return true;
        }
        // Now and then a tag write fails (the item keeps what it held, a placed one holds none,
        // read so), or a placement's commit fails once its item is in place.
        let (mut tag_failed, mut commit_failed) = (false, false);
        let writes_tags = placement || matches!(op, Op::LocalTags { .. });
        if outcome == Outcome::Done && writes_tags && !landed {
            self.writes += 1;
            tag_failed = self.clients[c].os != Os::Windows
                && (std::mem::take(&mut self.fail_tags_next) || self.writes % 13 == 7);
            let recorded = matches!(op, Op::LocalDownload { .. })
                || matches!(op, Op::LocalMkdir { drive: Some(_), .. }) && synced_before;
            commit_failed =
                recorded && (std::mem::take(&mut self.fail_commit_next) || self.writes % 11 == 5);
        }
        // A tag write that fails leaves the item as it was (a tag write: nothing recorded), or a
        // placed item with none (what it holds, as read, is what settles: the executor's; where
        // that read fails too, it takes what the item held before, which is the same).
        if tag_failed {
            // A download whose failed write would lose a tag the item there holds and the drive
            // doesn't (the user's, not sent up yet) isn't put in place: the item stays as it
            // was, for a retry, the plan sending the tag up meanwhile (the executor's).
            if let Op::LocalDownload { tags, .. } = &op
                && let Some(record) = &self.placed_from
                && before_tags.as_ref().is_some_and(|held| {
                    hbsync_engine::exec::tags_lost(held, tags, &record.state.tags)
                })
            {
                self.placed_from = None;
                let client = &mut self.clients[c];
                match held_before {
                    Some(state) => client.local.insert(node, state),
                    None => client.local.remove(node),
                };
                if self.verbose {
                    self.trace
                        .push(format!("  tags not written, not placed: {op:?}"));
                }
                if attempts < 3 {
                    client.plan.push_back((op, attempts + 1));
                }
                return true;
            }
            let client = &mut self.clients[c];
            if let Some(mut state) = client.local.get(node).cloned() {
                state.tags = match &op {
                    Op::LocalTags { .. } => before_tags.clone().unwrap_or_default(),
                    _ => Tags::default(),
                };
                client.local.insert(node, state);
            }
            if let Op::LocalDownload { tags, .. } | Op::LocalMkdir { tags, .. } = &op {
                let refused = before_tags.iter().flat_map(Tags::iter).chain(tags.iter());
                for tag in refused {
                    let name = tag.name.clone();
                    // The item no longer holds what this client's user added to it: the add
                    // stays with the node (on the drive, or nowhere), not with the item (a
                    // conflict copy made of it later doesn't take it along).
                    if self.tags_added.remove(&(c, node, name.clone())) {
                        self.tags_added.insert((DETACHED, node, name));
                    }
                }
            }
        }
        if commit_failed {
            let record = match &op {
                Op::LocalDownload { .. } => self.placed_from.take(),
                _ => op.drive().cloned(),
            };
            if let Some(record) = record {
                let client = &self.clients[c];
                let windows = client.os == Os::Windows;
                let written = client.local.get(node).map(|l| l.tags.held_by(client.os));
                let tags = match &op {
                    _ if tag_failed => None,
                    Op::LocalDownload { .. } if windows => None,
                    _ => written,
                };
                let prior = match &op {
                    Op::LocalDownload { .. } if windows => None,
                    Op::LocalDownload { .. } => Some(before_tags.clone().unwrap_or_default()),
                    _ => Some(Tags::default()),
                };
                // The operation ends, and recovery settles what it put in place right away, by
                // that item alone (no other is read again, as the executor's).
                self.keep_record(c, &op, record, tags, prior, fresh);
                if let Some(p) = self.clients[c].placing.get(&node).cloned()
                    && self.record_there(c, node, &p)
                {
                    self.clients[c].placing.remove(&node);
                    if !self.settle_record(c, node, &p) {
                        self.clients[c].placing.insert(node, p);
                    }
                    self.ops_executed += 1;
                    let client = &mut self.clients[c];
                    if let Some(state) = client.local.get(node).cloned() {
                        client.planned_local.insert(node, state);
                    }
                }
                return true;
            }
        }
        if outcome == Outcome::Done {
            // A remote write sends up what this client's user added to the node's tags; a create
            // that found the node there (another device brought it back) wrote nothing.
            if let Op::RemoteTags { tags, .. }
            | Op::RemoteUpdate { tags, .. }
            | Op::RemoteCreate { tags, .. } = &op
                && !landed
            {
                for tag in tags.iter() {
                    let add = (c, node, tag.name.clone());
                    if self.tags_added.contains(&add) {
                        self.tags_sent.insert(add);
                    }
                }
            }
            if matches!(
                op,
                Op::LocalMove { .. }
                    | Op::LocalTrash { .. }
                    | Op::LocalFork { .. }
                    | Op::LocalDownload { .. }
            ) {
                self.clients[c].parked.remove(&node);
            }
            self.ops_executed += 1;
            if let Op::LocalFork { new_node, .. } = op
                && let Some(at) = self.settle(c, new_node)
            {
                self.clients[c].run_settled.insert(new_node, at);
            }
            // The executor's row follows its own local write: a later write in the plan checks
            // the item against that, not against the plan's view of it.
            if matches!(
                op,
                Op::LocalMkdir { .. }
                    | Op::LocalMove { .. }
                    | Op::LocalTags { .. }
                    | Op::LocalDownload { .. }
            ) {
                let client = &mut self.clients[c];
                if let Some(state) = client.local.get(node).cloned() {
                    client.planned_local.insert(node, state);
                }
            }
            // A local write settles against the drive's version it acted on; a download against
            // its record, with the bytes it fetched.
            if matches!(
                op,
                Op::LocalMkdir { .. } | Op::LocalDownload { replace: false, .. }
            ) {
                self.placed_back(node);
            }
            let record = self.placed_from.take();
            let against = match &op {
                Op::LocalDownload { .. } => Against::placed(record.as_ref()),
                Op::LocalMkdir { .. } => Against::placed(op.drive()),
                Op::LocalMove { .. } if moves => Against::placed(op.drive()),
                _ => Against::acted(op.drive()),
            };
            // A transfer just made the bytes equal; one that landed earlier holds what the item
            // held then: compared (as the executor does).
            // A placement settles as its recovery would: not by what the plan settled.
            let ours = match &op {
                Op::LocalDownload { .. } | Op::LocalMkdir { .. } => None,
                _ => self.clients[c].run_settled.get(&node).copied(),
            };
            // What it did to the item's tags, as the executor records it.
            let client = &self.clients[c];
            let windows = client.os == Os::Windows;
            let written = client.local.get(node).map(|l| l.tags.clone());
            let (before, written) = (before_tags.unwrap_or_default(), written.unwrap_or_default());
            let wrote = TagWrite::Wrote {
                before: &before,
                written: &written,
            };
            let tags = match &op {
                // A failed tag write: what the item holds read back, unchanged (the executor's).
                Op::LocalTags { .. } if tag_failed => wrote,
                _ if tag_failed => TagWrite::Unknown,
                Op::LocalTags { .. } if windows => TagWrite::Nothing,
                Op::LocalDownload { .. } if windows => TagWrite::Unknown,
                Op::LocalMkdir { drive: None, .. } => TagWrite::Agreed,
                Op::LocalMkdir { .. } if windows => TagWrite::Nothing,
                Op::LocalTags { .. } | Op::LocalDownload { .. } | Op::LocalMkdir { .. } => wrote,
                Op::LocalMove { .. } => TagWrite::Nothing,
                _ => TagWrite::Agreed,
            };
            // A folder made from Recently deleted (restored, its RemoteCreate to follow) settles
            // against the version there, as any placement does: the base until the restore
            // settles, as the executor's.
            let coords = self.clients[c].coords;
            let restored = match &op {
                Op::LocalMkdir { drive: None, .. } if !self.clients[c].base.contains(node) => {
                    self.server.removed.get(&node).map(|r| DriveVersion {
                        state: NodeState {
                            parent: coords.from_drive(r.state.parent),
                            ..r.state.clone()
                        },
                        at: r.remote,
                        base_at: None,
                    })
                }
                _ => None,
            };
            let against = match &restored {
                Some(d) => Against::placed(Some(d)),
                None => against,
            };
            let at = self.settle_against(c, node, against, op.is_transfer() && !landed, tags, ours);
            if let Some(at) = at {
                self.clients[c].run_settled.insert(node, at);
            }
        }
        true
    }

    /// Whether `op` of client `c` would write outside its folder's part of the drive.
    fn breaches(&self, c: usize, op: &Op) -> bool {
        let coords = self.clients[c].coords;
        let touched = match op {
            Op::RemoteCreate { parent, .. } => vec![*parent],
            Op::RemoteUpdate { node, parent, .. } => vec![*node, *parent],
            Op::RemoteTags { node, .. } | Op::RemoteDelete { node, .. } => vec![*node],
            _ => Vec::new(),
        };
        // Outside now, unless its plan saw it where it may write: another device moved it
        // since (a race no client can rule out; a decision on what it saw is what counts).
        touched.iter().any(|n| {
            let on_drive = coords.to_drive(*n);
            coords.is_outside(*n)
                || self.known_outside(c, on_drive) && !self.planned_writable(c, *n)
        })
    }

    /// Whether client `c`'s plan saw `node` (in its coordinates) where it may write: in its
    /// part of the drive (a chain of parents reaching its root past no folder it leaves out),
    /// or, for the main folder, at no known place (it repairs such nodes, §12).
    fn planned_writable(&self, c: usize, node: NodeId) -> bool {
        let client = &self.clients[c];
        let main = client.coords.top.is_root();
        let left_out = |n: NodeId| match main {
            true => n == Role::Documents.node(),
            false => n == NodeId::OUTSIDE,
        };
        // Where the plan saw it, whatever the plan's own writes did since; a node the plan made
        // itself is where it made it.
        let parent = |n: NodeId| {
            client
                .plan_saw
                .get(n)
                .or_else(|| client.planned_remote.get(n))
                .map(|s| s.parent)
        };
        let mut at = node;
        let steps = client.plan_saw.len() + client.planned_remote.len();
        for _ in 0..=steps {
            if at.is_root() {
                return true;
            }
            if left_out(at) {
                return false;
            }
            match parent(at) {
                Some(p) => at = p,
                None => return main && at != node,
            }
        }
        main
    }

    /// Applies one op if its guards pass.
    fn execute(&mut self, c: usize, op: &Op) -> Outcome {
        use Outcome::{Done, Retry, Skip};
        let unchanged = |client: &Client, node: NodeId| {
            client.local.get(node) == client.planned_local.get(node)
        };
        // The executor's server-side order guards, against the client's mirror.
        if !ready(&self.clients[c], op) {
            return Retry;
        }
        // No placement over a record an earlier sync left (recovery kept it): the next plan,
        // once recovery settled it.
        if matches!(op, Op::LocalDownload { .. } | Op::LocalMkdir { .. })
            && self.clients[c].placing.contains_key(&op.node())
        {
            return Skip;
        }
        let coords = self.clients[c].coords;
        match op.clone() {
            Op::LocalFork {
                node,
                new_node,
                name,
            } => {
                let client = &mut self.clients[c];
                let Some(state) = client.local.get(node).cloned() else {
                    return Skip;
                };
                if !unchanged(client, node) || client.local.contains(new_node) {
                    return Skip;
                }
                // As the executor: a leaving holder is parked; one that stays leaves the copy a
                // numbered name.
                let mut name = name;
                if !park_occupant(client, state.parent, &name, None) {
                    match numbered_name(client, None, state.parent, &name, false) {
                        Some(n) => name = n,
                        None => return Retry,
                    }
                }
                client.local.remove(node);
                client.local.insert(new_node, NodeState { name, ..state });
                let added = self
                    .tags_added
                    .iter()
                    .filter(|(at, n, _)| (*at, *n) == (c, node));
                let forked: Vec<_> = added.cloned().collect();
                self.tags_forked.extend(forked);
                self.retrack(c, node, new_node);
                Done
            }
            Op::LocalMkdir {
                node,
                parent,
                name,
                tags,
                ..
            } => {
                let client = &mut self.clients[c];
                if client.local.contains(node) {
                    return Skip;
                }
                if !client.local.exists(parent) || !park_occupant(client, parent, &name, None) {
                    return Retry;
                }
                let tags = tags.held_by(client.os);
                client.local.insert(
                    node,
                    NodeState {
                        tags,
                        ..NodeState::folder(parent, name)
                    },
                );
                Done
            }
            Op::LocalTags { node, tags, .. } => {
                let client = &mut self.clients[c];
                let Some(state) = client.local.get(node).cloned() else {
                    return Skip;
                };
                if !unchanged(client, node) {
                    return Skip;
                }
                let tags = tags.held_by(client.os);
                client.local.insert(node, NodeState { tags, ..state });
                Done
            }
            Op::LocalDownload {
                node,
                parent,
                name,
                replace,
                tags,
                ..
            } => {
                let record = match self.clients[c].fetched.remove(&node) {
                    Some(record) => record,
                    None => {
                        // The bytes fetched are the server's current ones; they must be what
                        // was planned. The record: the version the plan saw, with them.
                        let (Some(server), Some(now)) =
                            (self.server.live.get(&node), self.server_state(c, node))
                        else {
                            return Skip;
                        };
                        let content = server.state.content;
                        let planned = self.clients[c].planned_remote.get(node);
                        if planned.map(|s| s.content) != Some(content) {
                            return Skip;
                        }
                        // Where this run's own write made the version (the base there), it acts
                        // on that one, as the executor does.
                        let client = &self.clients[c];
                        let own = client.run_settled.get(&node) == Some(&server.remote.version)
                            && client.extra.get(&node).and_then(|e| e.version)
                                == Some(server.remote.version);
                        let record = match op.drive() {
                            Some(_) if own => DriveVersion {
                                state: now,
                                at: server.remote,
                                base_at: Some(server.remote.version),
                            },
                            _ => DriveVersion {
                                state: NodeState {
                                    content,
                                    ..op.drive().map_or(now, |d| d.state.clone())
                                },
                                at: server.remote,
                                base_at: op.drive().and_then(|d| d.base_at),
                            },
                        };
                        if self.split_downloads {
                            self.clients[c].fetched.insert(node, record);
                            return Outcome::Fetched;
                        }
                        record
                    }
                };
                let content = record.state.content;
                let client = &mut self.clients[c];
                if replace != client.local.contains(node) || !unchanged(client, node) {
                    return Skip;
                }
                if !client.local.exists(parent)
                    || client.local.contains(node) && client.local.would_cycle(node, parent)
                    || !park_occupant(client, parent, &name, Some(node))
                {
                    return Retry;
                }
                let tags = tags.held_by(client.os);
                client.local.insert(
                    node,
                    NodeState {
                        parent,
                        name,
                        kind: NodeKind::File,
                        content,
                        tags,
                    },
                );
                self.placed_from = Some(record);
                Done
            }
            Op::LocalMove {
                node, parent, name, ..
            } => {
                let client = &mut self.clients[c];
                let Some(state) = client.local.get(node).cloned() else {
                    return Skip;
                };
                if !unchanged(client, node) {
                    return Skip;
                }
                if !client.local.exists(parent)
                    || client.local.would_cycle(node, parent)
                    || !park_occupant(client, parent, &name, Some(node))
                {
                    return Retry;
                }
                client.local.insert(
                    node,
                    NodeState {
                        parent,
                        name,
                        ..state
                    },
                );
                Done
            }
            Op::LocalTrash { node, subtree } => self.trash(c, node, &subtree),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind,
                tags,
            } => {
                if self.server.live.contains_key(&node) {
                    return Outcome::Landed; // 4105: another device brought it back
                }
                // In Recently deleted: restored at its file id (an edit wins over the delete),
                // unless it holds a version this client never had: that one stays, and this
                // version becomes a new item (as the executor detaches it).
                let file_id = self.server.removed.get(&node).map(|n| n.remote.file_id);
                if let Some(r) = self.server.removed.get(&node)
                    && kind == NodeKind::File
                    && r.state.content.map(|c| c.hash)
                        != self.clients[c]
                            .base
                            .get(node)
                            .and_then(|b| b.content)
                            .map(|c| c.hash)
                {
                    let client = &self.clients[c];
                    if !client.local.contains(node) || !unchanged(client, node) {
                        return Skip;
                    }
                    let new_node = NodeId::new();
                    return self.execute(c, &Op::LocalDetach { node, new_node });
                }
                let client = &self.clients[c];
                // The local node must still be there, as planned (the executor reads it).
                let Some(local) = client.local.get(node) else {
                    return Skip;
                };
                if !unchanged(client, node) {
                    return Skip;
                }
                let content = match kind {
                    NodeKind::Folder => None,
                    NodeKind::File => local.content,
                };
                // A restore keeps the tags the removed version has (the executor merges them).
                let tags = match self.server.removed.get(&node) {
                    Some(r) => self.merged_with_drive(c, node, &tags, &r.state.tags),
                    None => tags,
                };
                let state = NodeState {
                    parent,
                    name,
                    kind,
                    content,
                    tags,
                };
                let remote = RemoteRef {
                    file_id: file_id.unwrap_or_else(|| RemoteFileId(self.server.uuid())),
                    version: VersionTag(self.server.uuid()),
                };
                // Back on the drive: from Recently deleted, or made again after the purge.
                self.server.removed.remove(&node);
                self.restored_on_drive(node);
                self.server.ever.insert(node);
                let state = NodeState {
                    parent: coords.to_drive(state.parent),
                    ..state
                };
                self.server.live.insert(node, ServerNode { state, remote });
                self.record_remote(c, node);
                Done
            }
            Op::RemoteUpdate {
                node,
                expected,
                parent,
                name,
                upload,
                tags,
                ..
            } => {
                let local_content = self.clients[c].local.get(node).and_then(|s| s.content);
                // Over a version this client never had (taken along by a folder's delete):
                // that one stays, and this version becomes a new item (as the executor
                // detaches it).
                let base_content = self.clients[c].base.get(node).and_then(|b| b.content);
                if upload
                    && let Some(server) = self.server.live.get(&node)
                    && server.remote.version == expected
                    && server.state.content.map(|c| c.hash) != base_content.map(|c| c.hash)
                {
                    let client = &self.clients[c];
                    if !client.local.contains(node) || !unchanged(client, node) {
                        return Skip;
                    }
                    let new_node = NodeId::new();
                    return self.execute(c, &Op::LocalDetach { node, new_node });
                }
                let version = VersionTag(self.server.uuid());
                let drive = self.server.live.get(&node).map(|n| n.state.tags.clone());
                let tags = self.merged_with_drive(c, node, &tags, &drive.unwrap_or_default());
                let Some(server) = self.server.live.get_mut(&node) else {
                    return Skip;
                };
                if server.remote.version != expected {
                    return Skip; // 4160
                }
                if upload {
                    let Some(content) = local_content else {
                        return Skip;
                    };
                    server.state.content = Some(content);
                }
                server.state.parent = coords.to_drive(parent);
                server.state.name = name;
                server.state.tags = tags;
                server.remote.version = version;
                self.record_remote(c, node);
                Done
            }
            Op::RemoteTags {
                node,
                expected,
                tags,
                ..
            } => {
                let version = VersionTag(self.server.uuid());
                let drive = self.server.live.get(&node).map(|n| n.state.tags.clone());
                let tags = self.merged_with_drive(c, node, &tags, &drive.unwrap_or_default());
                let Some(server) = self.server.live.get_mut(&node) else {
                    return Skip;
                };
                if server.remote.version != expected {
                    return Skip; // 4160
                }
                server.state.tags = tags;
                server.remote.version = version;
                self.record_remote(c, node);
                Done
            }
            Op::RemoteDelete { node, expected, .. } => match self.server.live.get(&node) {
                Some(server) if server.remote.version != expected => Skip, // changed meanwhile
                // Removed to Recently deleted now, or already gone: done either way.
                _ => {
                    if let Some(n) = self.server.live.remove(&node) {
                        self.server.remove(node, n);
                        let client = &mut self.clients[c];
                        let after = client.removed_by_plan.iter().find(|r| {
                            self.server
                                .removed
                                .get(r)
                                .is_some_and(|n| n.state.parent == node)
                        });
                        if let Some(r) = after
                            && self.removed_after_contents.is_none()
                        {
                            self.removed_after_contents =
                                Some(format!("{} removed {node} after {r} in it", client.name));
                        }
                        client.removed_by_plan.insert(node);
                    }
                    self.clients[c].planned_remote.remove(node);
                    // Its tags go with it: a tag change loses to a delete (§11).
                    self.deleted_now(node);
                    Done
                }
            },
            Op::Settle { .. } => Done,
            Op::LocalDetach { node, new_node } => {
                let client = &mut self.clients[c];
                if !client.local.contains(node) {
                    return if client.local.contains(new_node) {
                        Done
                    } else {
                        Skip
                    };
                }
                // A parked item's temporary name carries its old id: it gets a real one first, as
                // in the executor.
                if let Some((parent, original)) = client.parked.remove(&node)
                    && let Some(state) = client.local.get(node).cloned()
                    && state.name == parked_name(node)
                {
                    if client.planned_local.get(node) != Some(&state) {
                        client.parked.insert(node, (parent, original));
                        return Skip;
                    }
                    let is_dir = state.kind == NodeKind::Folder;
                    let Some(name) = release_name(client, node, state.parent, &original, is_dir)
                    else {
                        client.parked.insert(node, (parent, original));
                        return Skip;
                    };
                    let state = NodeState { name, ..state };
                    client.local.insert(node, state.clone());
                    client.planned_local.insert(node, state);
                }
                rekey_client(client, node, new_node);
                // Not synced here any more; see the executor.
                client.base.remove(node);
                client.extra.remove(&node);
                self.retrack(c, node, new_node);
                Done
            }
            Op::Forget { node, subtree } => {
                if !subtree.is_empty() {
                    let outcome = self.trash(c, node, &subtree);
                    if outcome == Done {
                        let client = &mut self.clients[c];
                        for n in &subtree {
                            client.base.remove(*n);
                            client.extra.remove(n);
                        }
                    }
                    return outcome;
                }
                let client = &mut self.clients[c];
                // Still here, or its children's bases first, unless they are forgotten in this
                // run too (as in the executor).
                let forgetting = |n: NodeId| {
                    client
                        .plan
                        .iter()
                        .any(|(op, _)| matches!(op, Op::Forget { .. }) && op.node() == n)
                };
                if client.local.contains(node) || client.base.children(node).any(|n| !forgetting(n))
                {
                    return Skip;
                }
                client.base.remove(node);
                client.extra.remove(&node);
                Done
            }
        }
    }

    /// Moves `node` and everything below it to the trash, as the executor does: only if the
    /// visible part is `expected` and unchanged since planning. Junk goes along; ignored items
    /// stay, and so do the folders they sit in, released (§10).
    fn trash(&mut self, c: usize, node: NodeId, expected: &[NodeId]) -> Outcome {
        use Outcome::{Done, Skip};
        let unchanged = |client: &Client, node: NodeId| {
            client.local.get(node) == client.planned_local.get(node)
        };
        let client = &self.clients[c];
        let all: BTreeSet<NodeId> = client.local.subtree(node).into_iter().collect();
        let hidden = masked(&client.local, &client.run_filter);
        let mut shown: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|n| !hidden.contains(n))
            .collect();
        shown.sort();
        if all.is_empty() || shown.iter().any(|n| !unchanged(client, *n)) || shown != expected {
            return Skip;
        }
        // Junk goes along. Ignored items stay, and so do the folders they sit in, but
        // released: the scanner finds them as new folders.
        let kept: HashSet<NodeId> = all
            .iter()
            .copied()
            .filter(|n| hidden.contains(n) && verdict(client, *n) == Verdict::Ignored)
            .collect();
        let mut released: BTreeSet<NodeId> = BTreeSet::new();
        for k in &kept {
            released.extend(
                client
                    .local
                    .ancestors(*k)
                    .into_iter()
                    .filter(|a| all.contains(a) && !kept.contains(a)),
            );
        }
        // Junk sitting directly in a released folder, or anywhere in a kept one, isn't touched.
        let gone: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|n| !kept.contains(n) && !released.contains(n))
            .filter(|n| {
                let parent = client.local.get(*n).map(|s| s.parent);
                !(hidden.contains(n) && parent.is_some_and(|p| released.contains(&p)))
                    && !client.local.ancestors(*n).iter().any(|a| kept.contains(a))
            })
            .collect();
        // A released folder can't keep a temporary name (it carries the old id): it gets a real
        // one, deepest first, before the rest goes (the executor's DB still holds it then).
        let mut parked: Vec<NodeId> = released
            .iter()
            .copied()
            .filter(|r| {
                client.parked.contains_key(r)
                    && client
                        .local
                        .get(*r)
                        .is_some_and(|s| s.name == parked_name(*r))
            })
            .collect();
        parked.sort_by_key(|r| std::cmp::Reverse(client.local.ancestors(*r).len()));
        for r in parked {
            let client = &mut self.clients[c];
            let (Some(state), Some((_, original))) =
                (client.local.get(r).cloned(), client.parked.get(&r).cloned())
            else {
                continue;
            };
            if let Some(name) = release_name(client, r, state.parent, &original, true) {
                client.local.insert(r, NodeState { name, ..state });
            }
        }
        for n in gone {
            if hidden.contains(&n) {
                let content = self.clients[c].local.get(n).and_then(|s| s.content);
                self.supersede(content); // junk, disposed of by design
            }
            let client = &mut self.clients[c];
            client.local.remove(n);
            client.parked.remove(&n);
            // Tag changes made here and not synced yet went to the trash with the item; an add
            // sent up stands.
            let sent = &self.tags_sent;
            self.tags_added
                .retain(|add| (add.0, add.1) != (c, n) || sent.contains(add));
            self.tags_cleared
                .retain(|(by, node, _)| (*by, *node) != (c, n));
        }
        for r in released {
            let new = self.release_id();
            let client = &mut self.clients[c];
            client.parked.remove(&r);
            rekey_client(client, r, new);
            self.retrack(c, r, new);
        }
        Done
    }

    /// The tags client `c`'s write of `node` sends over the drive's version with `drive`'s, as
    /// the executor's `merged_with_drive`: its own merged with that version's.
    fn merged_with_drive(&self, c: usize, node: NodeId, ours: &Tags, drive: &Tags) -> Tags {
        let client = &self.clients[c];
        let base = client.base.get(node).map(|b| b.tags.clone());
        Tags::merge_onto(&base.unwrap_or_default(), ours, drive, client.os)
    }

    /// Moves parked nodes back where they came from, when that name is free again.
    fn unpark(&mut self, c: usize) {
        let client = &mut self.clients[c];
        let mut parked: Vec<(NodeId, (NodeId, String))> = client.parked.drain().collect();
        parked.sort();
        for (node, (parent, name)) in parked {
            let Some(state) = client.local.get(node).cloned() else {
                continue;
            };
            if state.name != parked_name(node) {
                continue; // the user moved it: the record is void
            }
            let is_dir = state.kind == NodeKind::Folder;
            let free = |client: &Client, name: &str| {
                client.local.child_named(parent, name, Some(node)).is_none()
            };
            // A filtered original name may be held by an item the scanner never tracks: the
            // first free numbered name instead (as the executor does).
            let mut name = name;
            if !syncs_at(client, parent, &name, is_dir)
                && let Some(n) = release_name(client, node, parent, &name, is_dir)
            {
                name = n;
            }
            if client.local.exists(parent)
                && free(client, &name)
                && !client.local.would_cycle(node, parent)
            {
                client.local.insert(
                    node,
                    NodeState {
                        parent,
                        name,
                        ..state
                    },
                );
            } else {
                client.parked.insert(node, (parent, name));
            }
        }
    }

    /// The executor's settle rule ([`Agreement`]) on the simulated sides.
    fn settle(&mut self, c: usize, node: NodeId) -> Option<VersionTag> {
        self.settle_against(c, node, Against::Mirror, false, TagWrite::Agreed, None)
    }

    /// [`Sim::settle`] as the executor's `settle`: against the drive's version a local write
    /// acted on, whatever the server holds by then, or against the server; `same_bytes`: a
    /// transfer just made the bytes equal (the executor doesn't compare them again);
    /// `tags`: what the operation did to the item's tags, which the base records
    /// ([`TagWrite::settle`], as the executor's). A
    /// local write's tags settle one by one against the version it acted on, always; the rest
    /// (content, place, version) as the executor's compare and set has it: where the base is no
    /// longer at the version its plan saw, nor at `ours` (where the plan's own operations
    /// settled it: none for a placement, which settles as its recovery would), the rest is left
    /// alone (the next plan decides); where the plan's own remote write moved the node past the
    /// version it acted on, the rest settles against that write's version while the server
    /// holds it. Returns the version it put the base at, if it did.
    fn settle_against(
        &mut self,
        c: usize,
        node: NodeId,
        against: Against<'_>,
        same_bytes: bool,
        tags: TagWrite<'_>,
        ours: Option<VersionTag>,
    ) -> Option<VersionTag> {
        let server = self.server_state(c, node);
        let live = self.server.live.get(&node).map(|n| n.remote.version);
        let client = &self.clients[c];
        let base_at = client
            .extra
            .get(&node)
            .and_then(|e| e.version)
            .filter(|_| client.base.contains(node));
        let ours = ours.filter(|o| base_at == Some(*o));
        let drive = against.drive();
        let acted = drive.map(|d| {
            let state = match ours {
                _ if base_at == d.base_at => Some((d.state.clone(), d.at.version)),
                Some(o) if o == d.at.version => Some((d.state.clone(), d.at.version)),
                Some(o) => server.clone().filter(|_| live == Some(o)).map(|r| (r, o)),
                None => None,
            };
            state.map(|(r, v)| {
                let tags = d.state.tags.clone();
                (NodeState { tags, ..r }, v)
            })
        });
        let base_moved = matches!(acted, Some(None));
        // What the local tags are read against: the drive's version a local write acted on.
        let reference = match drive {
            Some(d) => Some(d.state.clone()),
            None => server.clone(),
        };
        let remote = match acted {
            Some(acted) => acted,
            None => server.clone().zip(live),
        };
        let client = &mut self.clients[c];
        let local = client
            .local
            .get(node)
            .filter(|_| verdict(client, node).is_sync())
            .map(|l| NodeState {
                tags: held_tags(client, reference.as_ref(), node, &l.tags),
                ..l.clone()
            });
        // What the write left, as the item reads, and what was there, read alike but for a tag
        // the OS can't hold at all (the item never held that: the write leaves the drive's), as
        // the executor's.
        let filled = match tags {
            TagWrite::Wrote { before, written } => {
                let written = filled_tags(client, reference.as_ref(), node, Some(written));
                let reference = reference
                    .as_ref()
                    .map(|s| s.tags.clone())
                    .unwrap_or_default();
                let holdable: Vec<Tag> = reference
                    .iter()
                    .filter(|t| express(t, client.os).is_some())
                    .cloned()
                    .collect();
                let before = Tags::fill(Some(before), &Tags::new(holdable), client.os);
                Some((before, written))
            }
            _ => None,
        };
        let tags = match &filled {
            Some((before, written)) => TagWrite::Wrote { before, written },
            None => tags,
        };
        match (local, remote) {
            (None, _) if server.is_none() => {
                client.base.remove(node);
                client.extra.remove(&node);
                None
            }
            // The base moved otherwise: only the tags settle.
            (Some(l), None) if base_moved => {
                if let (Some(b), Some(d)) = (client.base.get(node).cloned(), drive) {
                    let tags = tags.settle(&b.tags, &l.tags, &d.state.tags);
                    client.base.insert(node, NodeState { tags, ..b });
                }
                None
            }
            (Some(l), Some((r, version))) => {
                let agreed = Agreement::of(&l, &r, same_bytes);
                let base = client.base.get(node).cloned();
                let prior = base.as_ref().map(|b| b.tags.clone()).unwrap_or_default();
                let settled = agreed.settle(base, &l, &r, against.placed_from());
                if let Some(mut b) = settled {
                    b.tags = tags.settle(&prior, &l.tags, &r.tags);
                    // The base is at this version now, whatever was agreed.
                    if agreed.content {
                        client.extra.insert(node, extra(&l, Some(version)));
                    } else if let Some(e) = client.extra.get_mut(&node) {
                        e.version = Some(version);
                    }
                    client.base.insert(node, b);
                    return Some(version);
                }
                None
            }
            _ => None,
        }
    }

    /// Plans and executes every client until nothing changes. Returns `false` if it doesn't
    /// settle within `max_rounds`.
    pub fn quiesce(&mut self, max_rounds: usize) -> bool {
        for _ in 0..max_rounds {
            let before = self.ops_executed;
            // A sync may change the drive without an operation (a role folder brought back, as
            // `Drive::ensure_role`): the others have to see it.
            let server = self.server.snapshot();
            let mut any_planned = false;
            for c in 0..self.clients.len() {
                self.plan(c);
                if self.guard.is_some() && !self.clients[c].held.is_empty() {
                    // The user looks at what is held and confirms it.
                    self.run(&[Event::Confirm { client: c }]);
                    self.plan(c);
                }
                // Still held after a confirmation would be a bug: keep going, and fail. A record
                // recovery kept is settled by a later sync (the scan hashes its file meanwhile).
                let client = &self.clients[c];
                any_planned |= !client.plan.is_empty()
                    || !client.held.is_empty()
                    || !client.placing.is_empty();
                while self.step(c) {}
            }
            let unchanged = !self.server.changed_since(&server);
            if unchanged && (!any_planned || (self.ops_executed == before && self.all_settled())) {
                return true;
            }
        }
        false
    }

    /// Like [`Sim::quiesce`], but as the daemon syncs, without a poll: a client syncs only when
    /// something asks it to: at start, when the server changed by another's hand (the change
    /// announced, or caught up on at a reconnect), and when the user confirms what the guard
    /// held. Never by its own writes: the notifications leave them out (`excludeOwnWrites`,
    /// odin-core #1971), and what a client needs of them (an own removal's time) comes with the
    /// write's answer. Another folder of the same computer is asked, by the account's
    /// drive, which its writes mark. A sync plans and runs until a plan does nothing. Returns whether every client is
    /// then settled (planning one unasked finds nothing: a timer would have nothing to do)
    /// within `max_syncs` syncs.
    pub fn quiesce_announced(&mut self, max_syncs: usize) -> bool {
        let mut asked = vec![true; self.clients.len()];
        for _ in 0..max_syncs {
            let Some(c) = asked.iter().position(|a| *a) else {
                return self.all_settled();
            };
            asked[c] = false;
            let server = self.server.snapshot();
            for _ in 0..30 {
                let before = self.ops_executed;
                self.plan(c);
                if self.guard.is_some() && !self.clients[c].held.is_empty() {
                    // The user confirms what is held: a request.
                    self.run(&[Event::Confirm { client: c }]);
                    self.plan(c);
                }
                while self.step(c) {}
                if self.ops_executed == before {
                    break;
                }
            }
            if self.server.changed_since(&server) {
                for (other, a) in asked.iter_mut().enumerate() {
                    *a |= other != c;
                }
            }
            // The scan, once it hashed the file of a record recovery kept, asks for a sync.
            asked[c] |= !self.clients[c].placing.is_empty();
        }
        false
    }

    fn all_settled(&mut self) -> bool {
        (0..self.clients.len()).all(|c| {
            self.plan(c);
            let client = &self.clients[c];
            client.plan.is_empty() && client.held.is_empty() && client.placing.is_empty()
        })
    }

    /// The tags (`name:colour`) of the server's item at `path` (`/`-separated), if it has one.
    pub fn server_tags(&self, path: &str) -> Option<Vec<String>> {
        tags_at(&self.server.tree(), path)
    }

    /// For scripted tests: the tags (`name:colour`) of client `c`'s item at `path`.
    pub fn local_tags(&self, c: usize, path: &str) -> Option<Vec<String>> {
        tags_at(&self.clients[c].local, path)
    }

    /// Checks convergence, invariants and no data loss. Returns a description of the first
    /// failure.
    pub fn check(&self) -> Result<(), String> {
        if let Some(op) = &self.server.dangling {
            return Err(format!("created under a folder the server never had: {op}"));
        }
        if let Some(plan) = &self.unguarded {
            return Err(format!("a mass delete went through unconfirmed: {plan}"));
        }
        // A record recovery keeps holds its node off placements until settled: never for good.
        for (c, client) in self.clients.iter().enumerate() {
            if let Some(node) = client.placing.keys().next() {
                return Err(format!("c{c}: the record of {node:?} is never settled"));
            }
        }
        // What a folder's removal took along is removed, marked so or not yet.
        let taken = self.server.taken_along();
        let mut server = self.server.tree();
        for n in &taken {
            server.remove(*n);
        }
        let mut kept_here: BTreeSet<u64> = BTreeSet::new();
        for (c, client) in self.clients.iter().enumerate() {
            let mut server = self.server_view(c);
            for n in &taken {
                server.remove(*n);
            }
            let hidden = masked(&client.local, &client.filter);
            let mut visible = client.local.clone();
            for n in &hidden {
                if let Some(blob) = visible
                    .remove(*n)
                    .and_then(|s| s.content)
                    .and_then(|c| self.blobs.get(&c.hash))
                {
                    kept_here.insert(*blob);
                }
            }
            let mut synced = server.clone();
            for n in masked(&server, &client.filter) {
                synced.remove(n);
            }
            for n in client.scope().out_of_scope(&server) {
                synced.remove(n);
            }
            // The tags as the client sees them.
            see_held_tags(client, &mut visible, &server);
            let server_view = view(&synced);
            let local_view = view(&visible);
            if local_view != server_view {
                return Err(format!(
                    "{} diverged:\n  local:  {local_view:?}\n  server: {server_view:?}",
                    client.name
                ));
            }
            if !client.local.violations().is_empty() {
                return Err(format!("{}: {:?}", client.name, client.local.violations()));
            }
            if view(&client.base) != server_view {
                return Err(format!("{}: base not settled", client.name));
            }
            if !synced.violations().is_empty() {
                return Err(format!(
                    "server as {} syncs it: {:?}",
                    client.name,
                    synced.violations()
                ));
            }
        }
        // Names are judged per client (above): a clash among nodes every client filters stays,
        // as no client syncs them, and any client that does would rename one.
        let broken: Vec<_> = server
            .violations()
            .into_iter()
            .filter(|v| !matches!(v, hbsync_engine::tree::Violation::DuplicateName { .. }))
            .collect();
        if !broken.is_empty() {
            return Err(format!("server: {broken:?}"));
        }
        if let Some(what) = &self.removed_after_contents {
            return Err(format!("a folder was removed after what is in it: {what}"));
        }
        if let Some(what) = &self.server.emptied_unlisted {
            return Err(format!("deleting for good took what it mustn't: {what}"));
        }
        if let Some(what) = &self.server.emptied_kept {
            return Err(format!(
                "an item emptied stayed, though nothing kept it: {what}"
            ));
        }
        // No folder wrote outside its part of the drive.
        if let Some(op) = &self.scope_breach {
            return Err(format!(
                "a folder wrote outside its part of the drive: {op}"
            ));
        }
        // A computer holds a node in one of its folders at most.
        for &(a, b) in &self.computers {
            let (a, b) = (&self.clients[a].local, &self.clients[b].local);
            if let Some(both) = a.ids().find(|n| b.contains(*n)) {
                return Err(format!("{both:?} is in both folders of one computer"));
            }
        }
        // Deleted it may be (the phone), moved never; a device syncing it brings it back.
        let docs = server.get(Role::Documents.node());
        if docs.is_some_and(|s| s.parent != NodeId::ROOT) {
            return Err("the role folder left the top".into());
        }
        if docs.is_none() && self.clients.iter().any(|c| !c.coords.top.is_root()) {
            return Err("the role folder is gone though a device syncs it".into());
        }
        if let Some((_, bad)) = server
            .iter()
            .find(|(_, s)| check_remote_name(&s.name).is_err() || s.name.starts_with(".hbsync-"))
        {
            return Err(format!("server: invalid name {:?}", bad.name));
        }
        let mut present: BTreeSet<u64> = server
            .iter()
            .filter_map(|(_, s)| s.content.and_then(|c| self.blobs.get(&c.hash).copied()))
            .collect();
        // Recently deleted keeps what was deleted, until the purge.
        let taken = taken.iter().filter_map(|n| self.server.live.get(n));
        present.extend(self.server.removed.values().chain(taken).filter_map(|n| {
            n.state
                .content
                .and_then(|c| self.blobs.get(&c.hash).copied())
        }));
        // Filtered items are kept on their client only.
        present.extend(kept_here);
        let lost: Vec<u64> = self
            .written
            .difference(&self.superseded)
            .filter(|b| !present.contains(b))
            .copied()
            .collect();
        if !lost.is_empty() {
            return Err(format!("lost blobs {lost:?}; present {present:?}"));
        }
        // A tag change loses to a delete (§11), unless the removed version held the tag: then
        // what brings it back keeps it.
        let held_at_removal = |node: &NodeId, name: &String| {
            self.server
                .tags_at_removal
                .get(node)
                .is_some_and(|t| t.contains(name))
        };
        for (_, node, name) in &self.tags_added {
            if let Some(s) = server.get(*node)
                && !self.tags_removed.contains(&(*node, name.clone()))
                && (!self.deleted.contains(node) || held_at_removal(node, name))
                && s.tags.get(name).is_none()
            {
                return Err(format!("lost tag {name:?} of {node:?}: {:?}", s.tags));
            }
        }
        // A synced tag a client removed stays removed (§11), unless another client added it,
        // or holds it from a download whose record recovery dropped (`tags_unproven`, the known
        // limit). One removed before a delete was settled by it (`deleted_now`,
        // `restored_on_drive`).
        for (by, node, name) in &self.tags_cleared {
            let added_elsewhere = self
                .tags_added
                .iter()
                .chain(&self.tags_forked)
                .chain(&self.tags_unproven)
                .any(|(c, n, t)| c != by && n == node && t == name);
            if let Some(s) = server.get(*node)
                && !added_elsewhere
                && s.tags.get(name).is_some()
            {
                return Err(format!(
                    "removed tag {name:?} of {node:?} came back: {:?}",
                    s.tags
                ));
            }
        }
        Ok(())
    }
}

/// The client a tag add belongs to once no item holds it ([`Sim::tags_added`]): it stays with
/// its node.
const DETACHED: usize = usize::MAX;

/// Frees `name` in `parent` if a node that is about to move away or be trashed holds it, by
/// parking that node under a temporary name (what the executor does for swaps and for a
/// folder that must appear where a file is leaving). Returns `false` if the name stays taken.
fn park_occupant(client: &mut Client, parent: NodeId, name: &str, except: Option<NodeId>) -> bool {
    let Some(occupant) = client.local.child_named(parent, name, except) else {
        return true;
    };
    let leaving = client
        .plan
        .iter()
        .any(|(p, _)| p.vacates_local_name() == Some(occupant));
    let Some(state) = client.local.get(occupant).cloned() else {
        return false;
    };
    if !leaving || client.local.get(occupant) != client.planned_local.get(occupant) {
        return false;
    }
    client
        .parked
        .entry(occupant)
        .or_insert((state.parent, state.name.clone()));
    let parked = NodeState {
        name: parked_name(occupant),
        ..state
    };
    client.local.insert(occupant, parked.clone());
    client.planned_local.insert(occupant, parked);
    true
}

/// A real name in `parent` for `node`, leaving its temporary one, as the executor picks it:
/// `name` if free, else the first free numbered one, one the client syncs if any is.
fn release_name(
    client: &Client,
    node: NodeId,
    parent: NodeId,
    name: &str,
    is_dir: bool,
) -> Option<String> {
    if client.local.child_named(parent, name, Some(node)).is_none() {
        return Some(name.to_owned());
    }
    numbered_name(client, Some(node), parent, name, is_dir)
}

/// The first numbered variant of `name` in `parent` that nothing but `except` holds, one the
/// client syncs if any is, as the executor picks it.
fn numbered_name(
    client: &Client,
    except: Option<NodeId>,
    parent: NodeId,
    name: &str,
    is_dir: bool,
) -> Option<String> {
    let free =
        |c: &String| Ok::<_, Infallible>(client.local.child_named(parent, c, except).is_none());
    let Ok(name) = first_free(numbered_names(name), free, |c| {
        syncs_at(client, parent, c, is_dir)
    });
    name
}

/// Whether the client syncs an item `name` in its folder `parent`.
fn syncs_at(client: &Client, parent: NodeId, name: &str, is_dir: bool) -> bool {
    client.local.path(parent).is_some_and(|mut path| {
        path.push(name);
        client.filter.names(path, is_dir).is_sync()
    })
}

fn ignoring_d() -> Filter {
    Filter::from_rules("d\n", false).unwrap_or_default()
}

/// What the filter of the client's run says about its local item `node`.
fn verdict(client: &Client, node: NodeId) -> Verdict {
    let Some(path) = client.local.path(node) else {
        return Verdict::Sync;
    };
    let is_dir = client
        .local
        .get(node)
        .is_some_and(|s| s.kind == NodeKind::Folder);
    client.run_filter.names(path, is_dir)
}

/// Gives the client's local item `node` the id `new`: in its tree, in the tree its plan was
/// made from, and in the parking records of its children.
fn rekey_client(client: &mut Client, node: NodeId, new: NodeId) {
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

/// The tags a client sees on its item `node` that holds `read` on disk: what its OS can't hold
/// comes from the drive (`remote`), else the base (the engine's view does the same).
fn held_tags(client: &Client, remote: Option<&NodeState>, node: NodeId, read: &Tags) -> Tags {
    let read = (client.os != Os::Windows).then_some(read);
    filled_tags(client, remote, node, read)
}

/// The tags of the client's item `node` from what was `read` of them (`None`: not known), the
/// drive's (else the agreed) standing for what it can't hold.
fn filled_tags(
    client: &Client,
    remote: Option<&NodeState>,
    node: NodeId,
    read: Option<&Tags>,
) -> Tags {
    let reference = remote
        .or(client.base.get(node))
        .map(|s| s.tags.clone())
        .unwrap_or_default();
    Tags::fill(read, &reference, client.os)
}

/// Gives each item of `view` (a client's local tree) the tags the client sees on it.
fn see_held_tags(client: &Client, view: &mut Tree, remote: &Tree) {
    let changed: Vec<(NodeId, NodeState)> = view
        .iter()
        .filter_map(|(n, state)| {
            let tags = held_tags(client, remote.get(n), n, &state.tags);
            (tags != state.tags).then(|| {
                (
                    n,
                    NodeState {
                        tags,
                        ..state.clone()
                    },
                )
            })
        })
        .collect();
    for (n, state) in changed {
        view.insert(n, state);
    }
}

/// The tags (`name:colour`) of the item at `path` (`/`-separated) in `tree`.
fn tags_at(tree: &Tree, path: &str) -> Option<Vec<String>> {
    Some(tag_names(&tree.get(at_path(tree, path)?)?.tags))
}

/// The node at `path` (`/`-separated) in `tree`.
fn at_path(tree: &Tree, path: &str) -> Option<NodeId> {
    tree.ids()
        .find(|n| tree.path(*n).is_some_and(|p| p.join("/") == path))
}

/// Tags as `name:colour`.
fn tag_names(tags: &Tags) -> Vec<String> {
    tags.iter()
        .map(|t| format!("{}:{}", t.name, t.colour))
        .collect()
}

/// Whether `op`'s server-side requirements hold in the client's mirror (the snapshot it planned
/// from, plus its own writes since) and its plan, as the executor checks them.
fn ready(client: &Client, op: &Op) -> bool {
    match op.requires() {
        None => true,
        Some(Requirement::RemoteFolder(folder)) => client.planned_remote.contains(folder),
        Some(Requirement::FolderFirst(folder)) => !client
            .plan
            .iter()
            .any(|(o, _)| matches!(o, Op::RemoteDelete { node, .. } if *node == folder)),
    }
}

/// A download or folder put in place whose commit a crash lost: its record, where it went, and
/// the tags its write left on the item there (`None`: not known), and `prior`. `unhashed`: the next sync's
/// scan doesn't hash the file yet (the engine's scan may leave that for later): recovery keeps
/// the record for the sync after, and the plan defers the node meanwhile.
#[derive(Debug, Clone)]
struct Placement {
    record: DriveVersion,
    parent: NodeId,
    name: String,
    tags: Option<Tags>,
    /// What the item there held before (`None`: not known; none where there was none).
    prior: Option<Tags>,
    /// The node had no item on this client before it was put in place: with the commit lost,
    /// the scan finds the item as a new one, the node's only where recovery takes it over (at
    /// its place with the record's bytes).
    fresh: bool,
    unhashed: bool,
    /// Held once already (its node gone, its item not the drive's to take): it goes next.
    held: bool,
}

/// What happened to one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Done,
    /// Blocked by the order of operations (target taken by a leaving node, parent not there
    /// yet, a move that would nest a folder in itself for now): try again later in the plan.
    Retry,
    /// The world changed since planning: drop it, the next plan decides again.
    Skip,
    /// A download fetched its bytes: the next step puts them in place.
    Fetched,
    /// A create found its node on the drive (another device brought it back): it wrote
    /// nothing, and settles against what is there.
    Landed,
}

fn extra(local: &NodeState, version: Option<VersionTag>) -> BaseExtra {
    BaseExtra {
        local_hash: local.content.map(|c| c.hash),
        content_state: ContentState::Local,
        version,
    }
}

/// A tree as a sorted list of `path kind content id`: node ids are part of identity across
/// clients, so two trees only match if the same nodes sit at the same paths.
fn view(tree: &Tree) -> Vec<String> {
    view_with(tree, &HashMap::new())
}

fn view_with(tree: &Tree, blobs: &HashMap<ContentHash, u64>) -> Vec<String> {
    let mut out: Vec<String> = tree
        .iter()
        .map(|(id, s)| {
            let path = tree
                .path(id)
                .map(|p| p.join("/"))
                .unwrap_or_else(|| "<detached>".into());
            let content = s
                .content
                .map(|c| match blobs.get(&c.hash) {
                    Some(b) => format!("blob{b}"),
                    None => c.hash.to_hex()[..8].to_owned(),
                })
                .unwrap_or_default();
            let tags = tag_names(&s.tags);
            format!(
                "{path} {:?} {content} {} {tags:?}",
                s.kind,
                &id.0.simple().to_string()[24..]
            )
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn act(client: usize, action: Action) -> Event {
        Event::Act { client, action }
    }

    /// The scope check catches a write to a node the plan saw outside its folder's part, judged
    /// on where the plan saw it before what the plan's own writes recorded since: Documents'
    /// plan saw the file at the top of the drive.
    #[test]
    fn a_write_to_what_the_plan_saw_outside_is_a_breach() {
        let mut sim = Sim::new(3).with_new_role_devices();
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            Event::Plan { client: 0 },
            Event::Step { client: 0 },
            Event::Plan { client: 3 },
        ]);
        let file = sim.item(0, "a").expect("the file");
        // What a write of the plan's own would have recorded: inside Documents.
        let state = sim.clients[3].plan_saw.get(file).expect("seen").clone();
        sim.clients[3].planned_remote.insert(
            file,
            NodeState {
                parent: NodeId::ROOT,
                ..state
            },
        );
        let remote = sim.server.live[&file].remote;
        let op = Op::RemoteDelete {
            node: file,
            parent: NodeId::ROOT,
            file_id: remote.file_id,
            expected: remote.version,
        };
        assert!(sim.breaches(3, &op));
    }

    /// A delete here that loses (another device edited the file) settles nothing of what other
    /// clients did to its tags: a removal and an addition client 1 hasn't synced yet still
    /// count. Only the deleting client's own went with its item.
    #[test]
    fn a_lost_delete_settles_nothing_of_other_clients() {
        let mut sim = Sim::new(2);
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            act(0, Action::Tag { node: 0, tag: 0 }), // Red
        ]);
        assert!(sim.quiesce(10));
        let file = sim.item(0, "a").expect("the file");
        sim.run(&[
            act(1, Action::Edit { file: 0 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
            act(1, Action::Tag { node: 0, tag: 0 }), // Red removed
            act(1, Action::Tag { node: 0, tag: 1 }), // Work added
            act(0, Action::Delete { node: 0 }),
            Event::Plan { client: 0 },
        ]);
        assert!(sim.quiesce(10));
        assert!(sim.item(0, "a").is_some(), "the delete lost");
        assert!(sim.tags_cleared.contains(&(1, file, "Red".into())));
        assert!(sim.tags_added.contains(&(1, file, "Work".into())));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
    }

    /// A folder never synced, made again where a user's delete of it lost (the drive has it),
    /// and the client dies before its commit: it is back all the same (no longer deleted), as a
    /// step that commits.
    #[test]
    fn a_folder_made_back_without_a_record_is_no_longer_deleted() {
        let mut sim = Sim::new(1);
        let node = NodeId::new();
        sim.server.put(node, NodeState::folder(NodeId::ROOT, "d"));
        sim.deleted.insert(node);
        sim.clients[0].plan.push_back((
            Op::LocalMkdir {
                node,
                parent: NodeId::ROOT,
                name: "d".into(),
                tags: Tags::default(),
                drive: None,
            },
            0,
        ));
        sim.crash(0);
        assert!(sim.clients[0].local.contains(node));
        assert!(!sim.deleted.contains(&node));
    }

    /// A folder synced before, made again: the engine records it for the version it acts on,
    /// the plan's, else its mirror's, and none where it has neither. A crash then leaves no
    /// record of a version the client never saw.
    #[test]
    fn a_folder_crash_records_only_what_the_engine_would() {
        let mut sim = Sim::new(1);
        let node = NodeId::new();
        let folder = NodeState::folder(NodeId::ROOT, "d");
        sim.clients[0].base.insert(node, folder.clone());
        // On the drive, but not in the client's mirror.
        sim.server.put(node, folder);
        sim.clients[0].plan.push_back((
            Op::LocalMkdir {
                node,
                parent: NodeId::ROOT,
                name: "d".into(),
                tags: Tags::default(),
                drive: None,
            },
            0,
        ));
        sim.crash(0);
        assert!(sim.clients[0].placing.is_empty());
        assert!(
            sim.clients[0].local.contains(node),
            "made in place all the same"
        );
        // Never synced: made in place, without a record.
        let mut sim = Sim::new(1);
        let node = NodeId::new();
        sim.clients[0].plan.push_back((
            Op::LocalMkdir {
                node,
                parent: NodeId::ROOT,
                name: "e".into(),
                tags: Tags::default(),
                drive: None,
            },
            0,
        ));
        sim.crash(0);
        assert!(sim.clients[0].local.contains(node));
        assert!(sim.clients[0].placing.is_empty());
    }

    /// A restore ends a delete: a tag removed after it counts, and coming back is reported. A
    /// node once deleted stayed exempt from the check for good.
    #[test]
    fn a_tag_removed_after_a_restore_counts() {
        let mut sim = Sim::new(2);
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            act(0, Action::Tag { node: 0, tag: 1 }),
        ]);
        assert!(sim.quiesce(10));
        let file = sim.item(0, "a").expect("the file");
        sim.run(&[act(0, Action::Delete { node: 0 })]);
        assert!(sim.quiesce(10));
        assert!(sim.is_removed(file), "the file is in Recently deleted");
        sim.run(&[act(0, Action::Restore { node: 0 })]);
        assert!(sim.quiesce(10));
        sim.run(&[act(0, Action::Tag { node: 0, tag: 1 })]);
        assert!(sim.quiesce(10));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
        // Back everywhere, as an engine that resurrected it would leave it.
        let work = Tags::new(vec![Tag::new("Work", 0)]);
        sim.server.live.get_mut(&file).expect("live").state.tags = work.clone();
        for client in &mut sim.clients {
            for tree in [&mut client.local, &mut client.base] {
                if let Some(mut s) = tree.get(file).cloned() {
                    s.tags = work.clone();
                    tree.insert(file, s);
                }
            }
        }
        let back = sim
            .check()
            .expect_err("a tag removed after the restore coming back is reported");
        assert!(back.contains("came back"), "{back}");
    }

    /// The tag check catches a synced tag a client removed coming back.
    #[test]
    fn a_removed_tag_that_comes_back_is_reported() {
        let mut sim = Sim::new(2);
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            act(0, Action::Tag { node: 0, tag: 1 }),
        ]);
        assert!(sim.quiesce(10));
        sim.run(&[act(0, Action::Tag { node: 0, tag: 1 })]);
        assert!(sim.quiesce(10));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
        let file = sim.item(0, "a").expect("the file");
        let work = Tags::new(vec![Tag::new("Work", 0)]);
        // Back everywhere, as an engine that resurrected it would leave it: converged, but wrong.
        sim.server.live.get_mut(&file).expect("live").state.tags = work.clone();
        for client in &mut sim.clients {
            for tree in [&mut client.local, &mut client.base] {
                if let Some(mut s) = tree.get(file).cloned() {
                    s.tags = work.clone();
                    tree.insert(file, s);
                }
            }
        }
        let back = sim
            .check()
            .expect_err("a removed tag coming back is reported");
        assert!(back.contains("came back"), "{back}");
    }

    /// A file edited elsewhere and tagged here: the plan writes the tag to the drive, then
    /// downloads the edit, which acts on the version the tag write left. The run leaves nothing
    /// for the next plan (behind the run's own write, the download used to leave the base
    /// behind, and a later edit elsewhere made a conflict copy of the file).
    #[test]
    fn a_download_after_the_runs_own_tag_write_settles_in_the_run() {
        let mut sim = Sim::new(2);
        sim.run(&[act(0, Action::CreateFile { parent: 0, name: 0 })]);
        assert!(sim.quiesce(10));
        sim.run(&[
            act(1, Action::Edit { file: 0 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
            act(0, Action::Tag { node: 0, tag: 0 }),
        ]);
        sim.plan(0);
        let ops: Vec<&'static str> = sim.clients[0]
            .plan
            .iter()
            .map(|(op, _)| op.kind())
            .collect();
        assert_eq!(ops, ["RemoteTags", "LocalDownload"]);
        while sim.step(0) {}
        sim.plan(0);
        assert!(sim.clients[0].plan.is_empty(), "{:?}", sim.clients[0].plan);
        assert!(sim.quiesce(10));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
    }

    /// A file tagged red here, edited elsewhere: the plan here sends red up and downloads the
    /// edit. Another device tags the file work first, so sending red up goes stale, and the
    /// download's tag write fails. The download isn't put in place (it would leave red
    /// nowhere): the file keeps red, and the next plan sends it up.
    #[test]
    fn a_tag_not_sent_up_outlives_a_failed_download_tag_write() {
        let mut sim = Sim::new(2);
        sim.run(&[act(0, Action::CreateFile { parent: 0, name: 0 })]);
        assert!(sim.quiesce(10));
        sim.run(&[
            act(1, Action::Edit { file: 0 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
            act(0, Action::Tag { node: 0, tag: 0 }),
            Event::Plan { client: 0 },
            act(1, Action::Tag { node: 0, tag: 1 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
        ]);
        let ops: Vec<&'static str> = sim.clients[0]
            .plan
            .iter()
            .map(|(op, _)| op.kind())
            .collect();
        assert_eq!(ops, ["RemoteTags", "LocalDownload"]);
        sim.fail_next_tag_write();
        while sim.step(0) {}
        assert!(sim.quiesce(10));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
    }

    /// Recovery reads a node as gone from what this client's mirror holds, as the engine does:
    /// not from the server, which another device changed since; and a node in a folder in
    /// Recently deleted, written before the removal, as gone (taken along).
    #[test]
    fn recovery_reads_gone_from_the_clients_mirror() {
        // A folder with a file in it, synced on every client.
        let synced = |clients| {
            let mut sim = Sim::new(clients);
            sim.run(&[act(0, Action::CreateFolder { parent: 0, name: 0 })]);
            let folders = sim.folders_of(0);
            let parent = folders
                .iter()
                .position(|f| !f.is_root())
                .expect("the folder");
            sim.run(&[act(0, Action::CreateFile { parent, name: 1 })]);
            assert!(sim.quiesce(10));
            let of = |kind| {
                sim.clients[0]
                    .local
                    .iter()
                    .find(|(_, s)| s.kind == kind)
                    .map(|(id, _)| id)
                    .expect("synced")
            };
            let (folder, file) = (of(NodeKind::Folder), of(NodeKind::File));
            (sim, folder, file)
        };
        let (mut sim, _, file) = synced(2);
        let picked = sim.items_of(1).iter().position(|n| *n == file);
        sim.run(&[
            act(
                1,
                Action::Delete {
                    node: picked.expect("there"),
                },
            ),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
        ]);
        assert!(!sim.server.live.contains_key(&file));
        assert!(!sim.gone_for(0, file), "the server's, not the mirror's");
        sim.plan(0);
        assert!(sim.gone_for(0, file));
        // Taken along: its folder in Recently deleted after it was written.
        let (mut sim, folder, file) = synced(1);
        assert!(!sim.gone_for(0, file));
        sim.clients[0].planned_removed.insert(folder, i64::MAX);
        assert!(sim.gone_for(0, file));
    }

    /// The tag check still catches a tag dropped from a node nobody deleted.
    #[test]
    fn a_tag_dropped_from_a_live_node_is_lost() {
        let mut sim = Sim::new(2);
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            act(0, Action::Tag { node: 0, tag: 0 }),
        ]);
        assert!(sim.quiesce(10));
        sim.check().unwrap_or_else(|e| panic!("{e}"));
        let file = sim.item(0, "a").expect("the file");
        // Dropped on the drive and every computer, as an engine that lost it would leave it:
        // converged, but lost.
        sim.server.live.get_mut(&file).expect("live").state.tags = Tags::default();
        for client in &mut sim.clients {
            for tree in [&mut client.local, &mut client.base] {
                if let Some(mut s) = tree.get(file).cloned() {
                    s.tags = Tags::default();
                    tree.insert(file, s);
                }
            }
        }
        let lost = sim.check().expect_err("a dropped tag is reported");
        assert!(lost.contains("lost tag"), "{lost}");
    }
}
