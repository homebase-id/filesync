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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use hbsync_engine::reconcile::{BaseExtra, Input, Op, Options, RemoteRef, reconcile};
use hbsync_engine::tree::Tree;
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_platform::name::name_key;
use hbsync_types::{
    ContentHash, ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, Uuid,
    VersionTag,
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
}

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
}

/// Names drawn from a small pool so clashes (including case-only ones) are common.
const NAMES: &[&str] = &["a", "b", "A", "c.txt", "C.TXT", "d"];

#[derive(Debug, Clone)]
struct ServerNode {
    state: NodeState,
    file_id: RemoteFileId,
    version: VersionTag,
}

#[derive(Debug, Default)]
struct Server {
    live: BTreeMap<NodeId, ServerNode>,
    next: u128,
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
    /// The server snapshot the pending plan was made from (contents to download).
    planned_remote: Tree,
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
    /// Record every plan and skipped operation in `trace` (debugging).
    pub verbose: bool,
    pub trace: Vec<String>,
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
                })
                .collect(),
            next_blob: 0,
            blobs: HashMap::new(),
            superseded: BTreeSet::new(),
            written: BTreeSet::new(),
            ops_executed: 0,
            conflicts: 0,
            verbose: false,
            trace: Vec::new(),
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
        let mut input = Input {
            base: client.base.clone(),
            local: local_view,
            remote: remote.clone(),
            base_extra: client.extra.clone(),
            remote_complete: true,
            ..Default::default()
        };
        for (id, n) in &self.server.live {
            input.remote_refs.insert(
                *id,
                RemoteRef {
                    file_id: n.file_id,
                    version: n.version,
                },
            );
        }
        input.dirty = client
            .base
            .ids()
            .chain(client.local.ids())
            .chain(remote.ids())
            .collect();
        let opts = Options {
            device: client.name.clone(),
            date: "2026-10-08".into(),
            allow_mass_delete: true,
            synced_files: 0,
            ..Options::default()
        };
        let plan = reconcile(&input, &opts);
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
        client.plan = plan.ops.into_iter().map(|op| (op, 0)).collect();
        client.planned_local = client.local.clone();
        client.planned_remote = remote;
    }

    /// Executes the next planned operation. Returns `false` if the plan is empty.
    fn step(&mut self, c: usize) -> bool {
        let Some((op, attempts)) = self.clients[c].plan.pop_front() else {
            return false;
        };
        let node = op.node();
        let outcome = self.execute(c, &op);
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
                if !unchanged(client, node) {
                    return Skip;
                }
                if client
                    .local
                    .child_named(state.parent, &name, None)
                    .is_some()
                {
                    return Retry;
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
            Op::LocalTrash {
                node,
                subtree: expected,
            } => {
                let client = &mut self.clients[c];
                let mut subtree = client.local.subtree(node);
                subtree.sort();
                if subtree.is_empty()
                    || subtree.iter().any(|n| !unchanged(client, *n))
                    || subtree != expected
                {
                    return Skip;
                }
                for n in subtree {
                    client.local.remove(n);
                    client.parked.remove(&n);
                }
                Done
            }
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
                let (file_id, version) = (
                    RemoteFileId(self.server.uuid()),
                    VersionTag(self.server.uuid()),
                );
                self.server.live.insert(
                    node,
                    ServerNode {
                        state,
                        file_id,
                        version,
                    },
                );
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
                if server.version != expected {
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
                server.version = version;
                Done
            }
            Op::RemoteDelete { node, expected, .. } => match self.server.live.get(&node) {
                Some(server) if server.version == expected => {
                    self.server.live.remove(&node);
                    Done
                }
                _ => Skip,
            },
            Op::Settle { .. } => Done,
        }
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
            if client.local.exists(parent)
                && client
                    .local
                    .child_named(parent, &name, Some(node))
                    .is_none()
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

    /// The executor's settle rule: the base takes each attribute both sides agree on.
    fn settle(&mut self, c: usize, node: NodeId) {
        let remote = self.server.live.get(&node).map(|n| n.state.clone());
        let client = &mut self.clients[c];
        let local = client.local.get(node).cloned();
        match (local, remote) {
            (None, None) => {
                client.base.remove(node);
                client.extra.remove(&node);
            }
            (Some(l), Some(r)) => {
                let place = l.same_place(&r);
                let content = l.same_content(&r);
                match client.base.get(node).cloned() {
                    None if place && content => {
                        client.extra.insert(node, extra(&l));
                        client.base.insert(node, r);
                    }
                    None => {}
                    Some(mut b) => {
                        if place {
                            b.parent = r.parent;
                            b.name = r.name.clone();
                        }
                        if content {
                            b.content = r.content;
                            client.extra.insert(node, extra(&l));
                        }
                        client.base.insert(node, b);
                    }
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
                any_planned |= !self.clients[c].plan.is_empty();
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
            self.clients[c].plan.is_empty()
        })
    }

    /// Checks convergence, invariants and no data loss. Returns a description of the first
    /// failure.
    pub fn check(&self) -> Result<(), String> {
        let server = self.server.tree();
        let server_view = view(&server);
        for client in &self.clients {
            let local_view = view(&client.local);
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
        }
        if !server.violations().is_empty() {
            return Err(format!("server: {:?}", server.violations()));
        }
        let present: BTreeSet<u64> = server
            .iter()
            .filter_map(|(_, s)| s.content.and_then(|c| self.blobs.get(&c.hash).copied()))
            .collect();
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
    let leaving = client.plan.iter().any(|(p, _)| match p {
        Op::LocalMove { node, .. }
        | Op::LocalTrash { node, .. }
        | Op::LocalFork { node, .. }
        | Op::LocalDownload {
            node,
            replace: true,
            ..
        } => *node == occupant,
        _ => false,
    });
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

fn parked_name(node: NodeId) -> String {
    format!(".hbsync-move-{}", node.0.simple())
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

fn extra(local: &NodeState) -> BaseExtra {
    BaseExtra {
        local_hash: local.content.map(|c| c.hash),
        content_state: ContentState::Local,
    }
}

/// A tree as a sorted list of `path kind content`, independent of node ids... except that
/// node ids are part of identity across clients, so they are included too.
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

/// Name keys must be unique per folder; exposed for tests.
pub fn has_duplicate_names(tree: &Tree) -> bool {
    let mut seen = BTreeSet::new();
    tree.iter()
        .any(|(_, s)| !seen.insert((s.parent, name_key(&s.name))))
}
