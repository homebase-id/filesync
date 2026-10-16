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

use hbsync_engine::deleted;
use hbsync_engine::exec::parked_name;
use hbsync_engine::mask::masked;
use hbsync_engine::reconcile::{
    Agreement, BaseExtra, Input, Op, Options, Removed, Requirement, reconcile_with_known,
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
    /// The purge: everything in Recently deleted is deleted for good.
    Purge,
    /// A user lists Recently deleted ([`hbsync_engine::deleted::deletes`]): what it holds now,
    /// each node at its version, is what emptying it as listed may take.
    ListDeleted,
    /// The user empties Recently deleted as last listed (nothing without a listing): what the
    /// engine selects of the rows still as listed ([`hbsync_engine::deleted::select`]), in its
    /// order.
    EmptyListed,
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

#[derive(Debug, Default)]
struct Server {
    live: BTreeMap<NodeId, ServerNode>,
    /// Recently deleted: out of the tree, with their bytes, until restored or purged.
    removed: BTreeMap<NodeId, ServerNode>,
    /// When each node went to Recently deleted (the server's time of that write: a header's
    /// `updated`, [`time`]).
    removed_at: BTreeMap<NodeId, i64>,
    next: u128,
    /// Which client wrote each version ([`time`]); one not in it was the users' elsewhere.
    writer: BTreeMap<i64, usize>,
    /// Every node that was ever live (the server doesn't check parents, like Homebase).
    ever: BTreeSet<NodeId>,
    /// The first write that pointed a node at a folder the server never had.
    dangling: Option<String>,
    /// The last listing of Recently deleted ([`Action::ListDeleted`]): each node at its version.
    listed: Option<BTreeSet<(NodeId, VersionTag)>>,
    /// The first node emptying as listed deleted for good that wasn't a removed row listed at
    /// that version (a live node, say), or a folder holding anything not going after it.
    emptied_unlisted: Option<String>,
}

/// The server's time of the write that made `version`: every write takes the next id, so ids
/// are the server's clock (Homebase's `updated`).
fn time(version: VersionTag) -> i64 {
    version.0.as_u128() as i64
}

impl Server {
    fn uuid(&mut self) -> Uuid {
        self.next += 1;
        Uuid::from_u128(self.next)
    }

    /// Whether every write after `at` was client `c`'s: then its feed may not have brought its
    /// own write of `at` back yet, as it brought nothing later (the feed is in order); after
    /// a write from elsewhere it has, so its time is known.
    fn only_own_writes_since(&self, at: i64, c: usize) -> bool {
        let now = self.next as i64;
        (at + 1..=now).all(|t| self.writer.get(&t) == Some(&c))
    }

    /// Puts `node` in Recently deleted: a write, at its time.
    fn remove(&mut self, node: NodeId, mut n: ServerNode) {
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

    /// The live nodes a folder's removal took along (node schema, Recently deleted): in a
    /// removed folder, directly or through live folders in it, written no later than its
    /// removal. They count as removed, whether or not a device marked them so yet.
    fn taken_along(&self) -> BTreeSet<NodeId> {
        let taken = |n: &ServerNode| {
            let mut seen = BTreeSet::new();
            let mut at = n.state.parent;
            loop {
                if self.removed.contains_key(&at) {
                    return self
                        .removed_at
                        .get(&at)
                        .is_some_and(|removed| time(n.remote.version) <= *removed);
                }
                match self.live.get(&at) {
                    Some(folder) if seen.insert(at) => at = folder.state.parent,
                    _ => return false,
                }
            }
        };
        self.live
            .iter()
            .filter(|(_, n)| taken(n))
            .map(|(id, _)| *id)
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
}

#[derive(Debug)]
struct Client {
    name: String,
    local: Tree,
    base: Tree,
    extra: HashMap<NodeId, BaseExtra>,
    /// Pending operations with their retry count.
    plan: VecDeque<(Op, u8)>,
    /// What the pending plan removed to Recently deleted so far.
    removed_by_plan: BTreeSet<NodeId>,
    /// This client's removals whose time it doesn't know yet (drive ids): the engine's own
    /// removal is open until the feed brings it back. Its next plan sees them so, where that
    /// is how an engine could see them ([`Server::only_own_writes_since`]); later plans don't.
    unconfirmed: BTreeSet<NodeId>,
    /// Nodes parked under a temporary name, with their original (parent, name).
    parked: HashMap<NodeId, (NodeId, String)>,
    /// The local tree when the pending plan was made (for the fingerprint guard).
    planned_local: Tree,
    /// The server snapshot the pending plan was made from (contents to download), updated
    /// by the client's own remote writes: what the executor's mirror holds.
    planned_remote: Tree,
    filter: Arc<Filter>,
    /// The nodes of the deletes the guard held in the last plan.
    held: Vec<NodeId>,
    /// The nodes whose deletes the user confirmed, until no delete is pending for them.
    confirmed: HashSet<NodeId>,
    /// How the client holds tags.
    os: Os,
    /// How the client's folder sees the drive: the main folder sees it as it is, a role
    /// folder its top as the root and the rest as one folder it leaves out (§12).
    coords: Coords,
}

impl Client {
    fn new(name: String, os: Os, filter: Filter, coords: Coords) -> Self {
        Client {
            name,
            local: Tree::new(),
            base: Tree::new(),
            extra: HashMap::new(),
            plan: VecDeque::new(),
            removed_by_plan: BTreeSet::new(),
            unconfirmed: BTreeSet::new(),
            parked: HashMap::new(),
            planned_local: Tree::new(),
            planned_remote: Tree::new(),
            filter: Arc::new(filter),
            held: Vec::new(),
            confirmed: HashSet::new(),
            os,
            coords,
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
    /// Plans that saw a removal's time open (the client's own, not back yet).
    pub open_times: usize,
    /// Items that stopped being tracked so far (each gets a new id).
    released: u64,
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
    deleted: BTreeSet<NodeId>,
    /// What the phone deleted with the role folder, brought back with it.
    role_folder_deleted_with: Vec<NodeId>,
    /// The first operation a client ran outside its folder's part of the drive.
    scope_breach: Option<String>,
    /// The first folder a plan removed after something in it (node schema, Recently deleted:
    /// what a folder's delete takes along is removed no earlier than the folder).
    removed_after_contents: Option<String>,
    /// Clients that are two folders of one computer (the main folder, a role folder).
    computers: Vec<(usize, usize)>,
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
            open_times: 0,
            released: 0,
            verbose: false,
            trace: Vec::new(),
            guard: None,
            unguarded: None,
            tags_added: BTreeSet::new(),
            tags_removed: BTreeSet::new(),
            deleted: BTreeSet::new(),
            role_folder_deleted_with: Vec::new(),
            scope_breach: None,
            removed_after_contents: None,
            computers: Vec::new(),
        }
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
        let docs = Client::new(
            format!("{}-docs", first.name),
            first.os,
            Filter::default(),
            Coords {
                top: Role::Documents.node(),
            },
        );
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
        for node in [docs].into_iter().chain(below.iter().copied()) {
            if let Some(n) = self.server.live.remove(&node) {
                self.server.remove(node, n);
                // A tag change loses to a delete (§11).
                self.deleted.insert(node);
            }
        }
        self.role_folder_deleted_with = below;
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
                    self.deleted.insert(file);
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
            }
        }
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
                    }
                    (Action::Tag { .. }, None) => {
                        let Some(tag) = express(&Tag::new(name, colour), os) else {
                            return; // this OS can't hold it
                        };
                        tags.push(tag);
                        self.tags_added.insert((c, node, name.to_owned()));
                    }
                    _ => return,
                }
                state.tags = Tags::new(tags);
                self.clients[c].local.insert(node, state);
            }
            Action::Restore { node } => self.restore(node),
            Action::Phone { op, pick } => self.phone(op, pick),
            Action::DeleteRoleFolder => self.delete_role_folder(),
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
                let listed = self.server.removed.iter();
                let listed = listed.map(|(n, r)| (*n, r.remote.version)).collect();
                self.server.listed = Some(listed);
            }
            Action::EmptyListed => self.empty_listed(),
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
                self.server.removed_at.clear();
                for (_, n) in std::mem::take(&mut self.server.removed) {
                    self.supersede(n.state.content);
                }
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
                    self.deleted.insert(n);
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
    }

    /// [`Action::EmptyListed`]: the engine's own selection ([`deleted::select`]) over the rows
    /// still as listed, with what the folders hold on the server, deleted in its order.
    fn empty_listed(&mut self) {
        let Some(listed) = self.server.listed.clone() else {
            return;
        };
        let server = &self.server;
        let chosen: Vec<deleted::Chosen> = server
            .removed
            .iter()
            .filter(|(n, r)| listed.contains(&(**n, r.remote.version)))
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
        let mut pending: BTreeSet<NodeId> = BTreeSet::new();
        let order = deleted::select(&chosen, &holdings);
        pending.extend(&order);
        for n in order {
            pending.remove(&n);
            // Only a removed row listed at that version, never a live node; and no folder while
            // it holds anything not going after it.
            let listed_row = self
                .server
                .removed
                .get(&n)
                .is_some_and(|r| listed.contains(&(n, r.remote.version)));
            let held = self
                .server
                .live
                .iter()
                .chain(&self.server.removed)
                .find(|(c, s)| s.state.parent == n && !pending.contains(*c))
                .map(|(c, _)| *c);
            if (!listed_row || held.is_some()) && self.server.emptied_unlisted.is_none() {
                self.server.emptied_unlisted = Some(format!("{n:?} (holding {held:?})"));
            }
            self.server.removed_at.remove(&n);
            if let Some(gone) = self.server.removed.remove(&n) {
                self.deleted.insert(n);
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
        // A role folder that isn't on the drive comes back from Recently deleted with what was
        // deleted with it, or is made, by the first of its devices to sync (as
        // `Drive::ensure_role`), named like the users' items.
        let top = self.clients[c].coords.top;
        if !top.is_root() && !self.server.live.contains_key(&top) {
            if self.server.removed.contains_key(&top) {
                self.restore_node(top);
                // Folders before what is in them; one whose folder didn't come back, at the top.
                let mut with = std::mem::take(&mut self.role_folder_deleted_with);
                while !with.is_empty() {
                    let before = with.len();
                    for n in with.clone() {
                        let parent = self.server.removed.get(&n).map(|r| r.state.parent);
                        if parent.is_none_or(|p| self.server.live.contains_key(&p)) {
                            self.restore_node(n);
                            with.retain(|w| *w != n);
                        }
                    }
                    if with.len() == before {
                        with.iter().for_each(|n| self.restore_node(*n));
                        break;
                    }
                }
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
        // What the client can't hold of the tags is the drive's, as the engine's view has it.
        see_held_tags(client, &mut local_view, &remote);
        let mut input = Input {
            base: client.base.clone(),
            local: local_view,
            remote: remote.clone(),
            base_extra: client.extra.clone(),
            remote_complete: true,
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
                let at = self.server.removed_at[&drive];
                // The engine's own removal: open, the newest of all, while its feed hasn't
                // brought it back.
                let at = if client.unconfirmed.contains(&drive)
                    && self.server.only_own_writes_since(at, c)
                {
                    self.open_times += 1;
                    i64::MAX
                } else {
                    at
                };
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
        // The feed brings this client's removals back by its next plan.
        client.unconfirmed.clear();
        client.planned_local = client.local.clone();
        client.planned_remote = remote;
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
        let before = self.server.next;
        let outcome = self.execute(c, &op);
        for t in before + 1..=self.server.next {
            self.server.writer.insert(t as i64, c);
        }
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
        if outcome == Outcome::Done {
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
            if let Op::LocalFork { new_node, .. } = op {
                self.settle(c, new_node);
            }
            self.settle(c, node);
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
        let mut at = node;
        for _ in 0..=client.planned_remote.len() {
            if at.is_root() {
                return true;
            }
            if left_out(at) {
                return false;
            }
            match client.planned_remote.get(at) {
                Some(state) => at = state.parent,
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
                self.retrack(c, node, new_node);
                Done
            }
            Op::LocalMkdir {
                node,
                parent,
                name,
                tags,
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
            Op::LocalTags { node, tags } => {
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
            } => {
                // The bytes fetched are the server's current ones; they must be what was planned.
                let Some(server) = self.server.live.get(&node) else {
                    return Skip;
                };
                let content = server.state.content;
                let client = &mut self.clients[c];
                if client.planned_remote.get(node).map(|s| s.content) != Some(content)
                    || replace != client.local.contains(node)
                    || !unchanged(client, node)
                {
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
                Done
            }
            Op::LocalMove { node, parent, name } => {
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
                    return Done; // 4105: an earlier attempt landed
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
                    Some(r) => {
                        let base = client.base.get(node).map(|b| b.tags.clone());
                        Tags::merge(&base.unwrap_or_default(), &tags, &r.state.tags)
                    }
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
                self.server.removed.remove(&node);
                self.server.ever.insert(node);
                self.clients[c].planned_remote.insert(node, state.clone());
                let state = NodeState {
                    parent: coords.to_drive(state.parent),
                    ..state
                };
                self.server.live.insert(node, ServerNode { state, remote });
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
                let state = NodeState {
                    parent,
                    ..server.state.clone()
                };
                self.clients[c].planned_remote.insert(node, state);
                Done
            }
            Op::RemoteTags {
                node,
                expected,
                tags,
                ..
            } => {
                let version = VersionTag(self.server.uuid());
                let Some(server) = self.server.live.get_mut(&node) else {
                    return Skip;
                };
                if server.remote.version != expected {
                    return Skip; // 4160
                }
                server.state.tags = tags;
                server.remote.version = version;
                let state = NodeState {
                    parent: coords.from_drive(server.state.parent),
                    ..server.state.clone()
                };
                self.clients[c].planned_remote.insert(node, state);
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
                        client.unconfirmed.insert(node);
                    }
                    self.clients[c].planned_remote.remove(node);
                    // Its tags go with it: a tag change loses to a delete (§11), and one brought
                    // back later is a new start.
                    self.deleted.insert(node);
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
                        for n in subtree {
                            client.base.remove(n);
                            client.extra.remove(&n);
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
        let hidden = masked(&client.local, &client.filter);
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
            // Tags added here and not synced yet went to the trash with the item.
            self.tags_added
                .retain(|(at, node, _)| (*at, *node) != (c, n));
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
    fn settle(&mut self, c: usize, node: NodeId) {
        let remote = self.server_state(c, node);
        let version = self.server.live.get(&node).map(|n| n.remote.version);
        let client = &mut self.clients[c];
        let local = client
            .local
            .get(node)
            .filter(|_| verdict(client, node).is_sync())
            .map(|l| NodeState {
                tags: held_tags(
                    client,
                    self.server.live.get(&node).map(|n| &n.state),
                    node,
                    &l.tags,
                ),
                ..l.clone()
            });
        match (local, remote) {
            (None, None) => {
                client.base.remove(node);
                client.extra.remove(&node);
            }
            (Some(l), Some(r)) => {
                let agreed = Agreement::of(&l, &r, false);
                if let Some(b) = agreed.settle(client.base.get(node).cloned(), &l, &r) {
                    if agreed.content {
                        client.extra.insert(node, extra(&l, version));
                    }
                    client.base.insert(node, b);
                }
            }
            _ => {}
        }
    }

    /// Plans and executes every client until nothing changes. Returns `false` if it doesn't
    /// settle within `max_rounds`.
    pub fn quiesce(&mut self, max_rounds: usize) -> bool {
        for _ in 0..max_rounds {
            let before = self.ops_executed;
            let mut any_planned = false;
            for c in 0..self.clients.len() {
                self.plan(c);
                if self.guard.is_some() && !self.clients[c].held.is_empty() {
                    // The user looks at what is held and confirms it.
                    self.run(&[Event::Confirm { client: c }]);
                    self.plan(c);
                }
                // Still held after a confirmation would be a bug: keep going, and fail.
                any_planned |= !self.clients[c].plan.is_empty() || !self.clients[c].held.is_empty();
                while self.step(c) {}
            }
            if !any_planned || (self.ops_executed == before && self.all_settled()) {
                return true;
            }
        }
        false
    }

    /// Like [`Sim::quiesce`], but as the daemon syncs, without a poll: a client syncs only when
    /// something asks it to: at start, when the server changed by another's hand (the change
    /// announced, or caught up on at a reconnect), and when the user confirms what the guard
    /// held. A sync plans and runs until a plan does nothing. Returns whether every client is
    /// then settled (planning one unasked finds nothing: a timer would have nothing to do)
    /// within `max_syncs` syncs.
    pub fn quiesce_announced(&mut self, max_syncs: usize) -> bool {
        let mut asked = vec![true; self.clients.len()];
        for _ in 0..max_syncs {
            let Some(c) = asked.iter().position(|a| *a) else {
                return self.all_settled();
            };
            asked[c] = false;
            let server = (self.server.live.clone(), self.server.removed.clone());
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
            if (&self.server.live, &self.server.removed) != (&server.0, &server.1) {
                for (other, a) in asked.iter_mut().enumerate() {
                    *a |= other != c;
                }
            }
        }
        false
    }

    fn all_settled(&mut self) -> bool {
        (0..self.clients.len()).all(|c| {
            self.plan(c);
            self.clients[c].plan.is_empty() && self.clients[c].held.is_empty()
        })
    }

    /// The tags (`name:colour`) of the server's item at `path` (`/`-separated), if it has one.
    pub fn server_tags(&self, path: &str) -> Option<Vec<String>> {
        let tree = self.server.tree();
        let state = tree.get(at_path(&tree, path)?)?;
        Some(tag_names(&state.tags))
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
            return Err(format!(
                "emptying as listed deleted what it mustn't: {what}"
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
        for (_, node, name) in &self.tags_added {
            if let Some(s) = server.get(*node)
                && !self.tags_removed.contains(&(*node, name.clone()))
                && !self.deleted.contains(node)
                && s.tags.get(name).is_none()
            {
                return Err(format!("lost tag {name:?} of {node:?}: {:?}", s.tags));
            }
        }
        Ok(())
    }
}

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

/// What the client's filter says about its local item `node`.
fn verdict(client: &Client, node: NodeId) -> Verdict {
    let Some(path) = client.local.path(node) else {
        return Verdict::Sync;
    };
    let is_dir = client
        .local
        .get(node)
        .is_some_and(|s| s.kind == NodeKind::Folder);
    client.filter.names(path, is_dir)
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
    let reference = remote
        .or(client.base.get(node))
        .map(|s| s.tags.clone())
        .unwrap_or_default();
    let read = (client.os != Os::Windows).then_some(read);
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

/// What happened to one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Done,
    /// Blocked by the order of operations (target taken by a leaving node, parent not there
    /// yet, a move that would nest a folder in itself for now): try again later in the plan.
    Retry,
    /// The world changed since planning: drop it, the next plan decides again.
    Skip,
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
