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
//! Clients filter (sync semantics §10): every client has the built-in junk list, and odd
//! clients start out also ignoring the name `d` (any client can toggle that). A client's filtered items are invisible to its sync, as
//! the scanner leaves them out; convergence is judged on what each client syncs, and an
//! ignored item, kept on its client only, counts as present for the no-loss check.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use hbsync_engine::exec::parked_name;
use hbsync_engine::mask::masked;
use hbsync_engine::reconcile::{Agreement, BaseExtra, Input, Op, Options, Requirement, reconcile};
use hbsync_engine::tree::{Tree, numbered_names};
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_local::{Filter, Verdict};
use hbsync_platform::name::check_remote_name;
use hbsync_types::{
    ContentHash, ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef,
    Uuid, VersionTag,
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
}

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

#[derive(Debug, Clone)]
struct ServerNode {
    state: NodeState,
    remote: RemoteRef,
}

#[derive(Debug, Default)]
struct Server {
    live: BTreeMap<NodeId, ServerNode>,
    next: u128,
    /// Every node that was ever live (the server doesn't check parents, like Homebase).
    ever: BTreeSet<NodeId>,
    /// The first write that pointed a node at a folder the server never had.
    dangling: Option<String>,
}

impl Server {
    fn uuid(&mut self) -> Uuid {
        self.next += 1;
        Uuid::from_u128(self.next)
    }

    fn tree(&self) -> Tree {
        let mut t = Tree::new();
        for (id, n) in &self.live {
            t.insert(*id, n.state.clone());
        }
        t
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
    /// Record every plan and skipped operation in `trace` (debugging).
    pub verbose: bool,
    pub trace: Vec<String>,
    /// The mass-delete guard, on with this minimum (the fraction 0), else off. With it on,
    /// [`Sim::quiesce`] confirms what is held, as a user eventually does.
    pub guard: Option<usize>,
    /// The first plan that deleted more than the guard allows without a confirmation.
    unguarded: Option<String>,
}

impl Sim {
    pub fn new(clients: usize) -> Self {
        Sim {
            server: Server::default(),
            clients: (0..clients)
                .map(|i| Client {
                    name: format!("c{i}"),
                    local: Tree::new(),
                    base: Tree::new(),
                    extra: HashMap::new(),
                    plan: VecDeque::new(),
                    parked: HashMap::new(),
                    planned_local: Tree::new(),
                    planned_remote: Tree::new(),
                    filter: Arc::new(if i % 2 == 1 {
                        ignoring_d()
                    } else {
                        Filter::default()
                    }),
                    held: Vec::new(),
                    confirmed: HashSet::new(),
                })
                .collect(),
            next_blob: 0,
            blobs: HashMap::new(),
            superseded: BTreeSet::new(),
            written: BTreeSet::new(),
            ops_executed: 0,
            conflicts: 0,
            released: 0,
            verbose: false,
            trace: Vec::new(),
            guard: None,
            unguarded: None,
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

    fn act(&mut self, c: usize, action: Action) {
        let local = &self.clients[c].local;
        let mut folders: Vec<NodeId> = vec![NodeId::ROOT];
        folders.extend(
            local
                .iter()
                .filter(|(_, s)| s.kind == NodeKind::Folder)
                .map(|(id, _)| id),
        );
        folders.sort();
        let mut files: Vec<NodeId> = local
            .iter()
            .filter(|(_, s)| s.kind == NodeKind::File)
            .map(|(id, _)| id)
            .collect();
        files.sort();
        let mut all: Vec<NodeId> = local.ids().collect();
        all.sort();
        let name_free = |t: &Tree, parent: NodeId, name: &str, except: Option<NodeId>| {
            t.child_named(parent, name, except).is_none()
        };
        match action {
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
                    let content = self.clients[c].local.get(n).and_then(|s| s.content);
                    self.supersede(content);
                }
                for n in self.clients[c].local.subtree(node) {
                    self.clients[c].local.remove(n);
                }
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
        self.unpark(c);
        self.release_filtered(c);
        let remote = self.server.tree();
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
            .map(|(id, n)| (*id, n.remote))
            .collect();
        input.dirty = client
            .base
            .ids()
            .chain(client.local.ids())
            .chain(remote.ids())
            .collect();
        let confirmed = client.confirmed.clone();
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
            confirmed: Arc::new(confirmed.clone()),
            filter: Arc::clone(&client.filter),
            ..Options::default()
        };
        let plan = reconcile(&input, &opts);
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
        }
    }

    /// Executes the next planned operation. Returns `false` if the plan is empty.
    fn step(&mut self, c: usize) -> bool {
        let Some((op, attempts)) = self.clients[c].plan.pop_front() else {
            return false;
        };
        let node = op.node();
        let outcome = self.execute(c, &op);
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
                Done
            }
            Op::LocalMkdir { node, parent, name } => {
                let client = &mut self.clients[c];
                if client.local.contains(node) {
                    return Skip;
                }
                if !client.local.exists(parent) || !park_occupant(client, parent, &name, None) {
                    return Retry;
                }
                client.local.insert(node, NodeState::folder(parent, name));
                Done
            }
            Op::LocalDownload {
                node,
                parent,
                name,
                replace,
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
                client.local.insert(
                    node,
                    NodeState {
                        parent,
                        name,
                        kind: NodeKind::File,
                        content,
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
            } => {
                if self.server.live.contains_key(&node) {
                    return Done; // 4105: an earlier attempt landed
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
                let state = NodeState {
                    parent,
                    name,
                    kind,
                    content,
                };
                let remote = RemoteRef {
                    file_id: RemoteFileId(self.server.uuid()),
                    version: VersionTag(self.server.uuid()),
                };
                self.server.ever.insert(node);
                self.clients[c].planned_remote.insert(node, state.clone());
                self.server.live.insert(node, ServerNode { state, remote });
                Done
            }
            Op::RemoteUpdate {
                node,
                expected,
                parent,
                name,
                upload,
                ..
            } => {
                let local_content = self.clients[c].local.get(node).and_then(|s| s.content);
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
                server.state.parent = parent;
                server.state.name = name;
                server.remote.version = version;
                let state = server.state.clone();
                self.clients[c].planned_remote.insert(node, state);
                Done
            }
            Op::RemoteDelete { node, expected, .. } => match self.server.live.get(&node) {
                Some(server) if server.remote.version != expected => Skip, // changed meanwhile
                // Deleted now, or already: a delete of a tombstone succeeds, as on the fake and
                // Homebase.
                _ => {
                    self.server.live.remove(&node);
                    self.clients[c].planned_remote.remove(node);
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
        }
        for r in released {
            let new = self.release_id();
            let client = &mut self.clients[c];
            client.parked.remove(&r);
            rekey_client(client, r, new);
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
        let remote = self.server.live.get(&node).map(|n| n.state.clone());
        let version = self.server.live.get(&node).map(|n| n.remote.version);
        let client = &mut self.clients[c];
        let local = client
            .local
            .get(node)
            .filter(|_| verdict(client, node).is_sync())
            .cloned();
        match (local, remote) {
            (None, None) => {
                client.base.remove(node);
                client.extra.remove(&node);
            }
            (Some(l), Some(r)) => {
                let agreed = Agreement::of(&l, &r, false);
                if let Some(b) = agreed.settle(client.base.get(node).cloned(), &r) {
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

    fn all_settled(&mut self) -> bool {
        (0..self.clients.len()).all(|c| {
            self.plan(c);
            self.clients[c].plan.is_empty() && self.clients[c].held.is_empty()
        })
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
        let server = self.server.tree();
        let mut kept_here: BTreeSet<u64> = BTreeSet::new();
        for client in &self.clients {
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
    let mut filtered = None;
    for candidate in numbered_names(name) {
        if client
            .local
            .child_named(parent, &candidate, except)
            .is_none()
        {
            if syncs_at(client, parent, &candidate, is_dir) {
                return Some(candidate);
            }
            filtered.get_or_insert(candidate);
        }
    }
    filtered
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

/// Whether `op`'s server-side requirements hold in the client's mirror (the snapshot it planned
/// from, plus its own writes since), as the executor checks them.
fn ready(client: &Client, op: &Op) -> bool {
    match op.requires() {
        None => true,
        Some(Requirement::RemoteFolder(folder)) => client.planned_remote.contains(folder),
        Some(Requirement::NoRemoteChildren(node)) => {
            client.planned_remote.children(node).next().is_none()
        }
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
            format!(
                "{path} {:?} {content} {}",
                s.kind,
                &id.0.simple().to_string()[24..]
            )
        })
        .collect();
    out.sort();
    out
}
