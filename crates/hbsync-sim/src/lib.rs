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
//!
//! The modules: `action` (what happens), `model` (the server and the clients), `world` (what
//! users, other devices and the server do), `exec` (what mirrors the engine), `crash` (a lost
//! commit and its recovery), `gone` (the identity's "not there"), `oracle` (the ledger and the
//! check), `tagbook` (the tags the oracle owes), `narrow` (the engine's own planning path, for
//! the narrow check), `faults` (the faults the simulation plays, and what they keep), `ids`
//! (counted node ids: a run replays the same from its events) and `trace`.

mod action;
mod crash;
mod exec;
mod faults;
mod gone;
mod ids;
mod model;
mod narrow;
mod oracle;
mod tagbook;
mod trace;
mod world;

pub use action::{Action, Event};
pub use model::server::{PURGE_EVERY, TRACE};

use hbsync_engine::tree::Tree;
use hbsync_local::Filter;
use hbsync_platform::Os;
use hbsync_state::Coords;
use hbsync_types::{NodeId, NodeKind, NodeState, Role, Uuid};

use crate::action::NAMES;
use crate::exec::extra;
use crate::faults::Faults;
use crate::ids::Ids;
use crate::model::client::{Client, ignoring_d};
use crate::model::server::Server;
use crate::oracle::{Ledger, tag_names, view_with};
use hbsync_engine::reconcile::DriveVersion;

/// The simulation.
#[derive(Debug)]
pub struct Sim {
    server: Server,
    clients: Vec<Client>,
    /// The last blob written (blobs count up).
    next_blob: u64,
    ledger: Ledger,
    pub ops_executed: usize,
    pub conflicts: usize,
    /// Where new items' ids come from.
    ids: Ids,
    /// A store [`Action::Meanwhile`] records answers in, opened at its first, left empty.
    answers_db: Option<hbsync_state::StateDb>,
    /// Record every plan and skipped operation in `trace` (debugging).
    pub verbose: bool,
    pub trace: Vec<String>,
    /// The mass-delete guard, on with this minimum (the fraction 0), else off. With it on,
    /// [`Sim::quiesce`] confirms what is held, as a user eventually does.
    pub guard: Option<usize>,
    /// Clients that are two folders of one computer (the main folder, a role folder).
    computers: Vec<(usize, usize)>,
    /// The version the download just put in place acted on, for its commit.
    placed_from: Option<DriveVersion>,
    /// The faults it plays, and what they keep ([`Faults`]).
    faults: Faults,
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
            ledger: Ledger::default(),
            ops_executed: 0,
            conflicts: 0,
            ids: Ids::default(),
            answers_db: None,
            verbose: false,
            trace: Vec::new(),
            guard: None,
            computers: Vec::new(),
            faults: Faults::default(),
            placed_from: None,
        }
    }

    /// A download's fetch and its commit are two steps, as on the transfer pool: the drive may
    /// move on, or remove the node, between them, and a crash ([`Event::Crash`]) can lose the
    /// commit of a file already in place, for recovery to settle.
    pub fn with_split_downloads(mut self) -> Self {
        self.faults.split_downloads = true;
        self
    }

    /// Each client plans its nodes in pages of `size`, in node id order, as the engine plans
    /// its dirty set (`EngineConfig::batch`): a node in another page is held as it is, one in
    /// a page's folder that may go holds that folder too (`Input::dirty_elsewhere`), and what
    /// a page held so, or whose names or places clash across pages, is planned as a page of
    /// its own next (`pages::OwnPages`). A plan's operations run before the next page is
    /// planned (a step that finds none plans it).
    pub fn with_small_pages(mut self, size: usize) -> Self {
        self.faults.page_size = Some(size.max(1));
        self
    }

    /// No client ignores anything by rule (the odd ones ignore `d` otherwise).
    pub fn without_ignore_rules(mut self) -> Self {
        for client in &mut self.clients {
            client.filter = std::sync::Arc::new(Filter::default());
        }
        self
    }

    /// Every fourth plan a main folder makes is checked against the engine's own planning path: its
    /// planning input filled into a state DB as the engine's store holds it, planned from
    /// narrow views as the engine does (`view::plan_narrow`), against whole folders for the
    /// same dirty nodes, and the whole one against the sim's plan.
    pub fn with_narrow_check(mut self) -> Self {
        self.faults.narrow_check = true;
        self
    }

    /// Every plan a main folder makes that holds deletes (the guard's, §7) is planned again
    /// through the engine's views: once with its dirty nodes as the engine keeps them, once
    /// with what the engine takes out of the dirty set till the user answers
    /// ([`hbsync_engine::held`]) out of it. Unless that view reaches them, the two plan alike
    /// but those deletes.
    pub fn with_held_check(mut self) -> Self {
        self.faults.held_check = true;
        self
    }

    /// For scripted tests: the tag adds [`Sim::check`] found lost to a known limit of the
    /// engine, as `"Limit node tag"`.
    pub fn tags_lost_to_known_limits(&self) -> Vec<String> {
        self.ledger
            .tags
            .excused()
            .into_iter()
            .map(|(limit, (_, node, name))| format!("{limit:?} {node:?} {name}"))
            .collect()
    }

    /// The drive refuses some requests as they are, for good, as Homebase refuses one it won't
    /// take (a write), or gives other bytes than it says (a download, at a version): every
    /// fourth request it hasn't seen before ([`Sim::drive_refuses`]), and the same request
    /// again as it did the first time; another (an edit, a rename, a delete, a new version on
    /// the drive) is judged anew. The clients record each refusal and don't send the same operation again
    /// (`hbsync_engine::refused`; a download once it didn't match twice).
    pub fn with_refusals(mut self) -> Self {
        self.faults.refusing = true;
        self
    }

    /// The identity says now and then that a live file isn't there (its own 404, a hiccup),
    /// once of each such file: every third plan, of a file the planning client holds. The
    /// clients judge it as the engine does (sync semantics §8b), and none may take the file as
    /// gone for good ([`crate::gone`]).
    pub fn with_wrong_not_theres(mut self) -> Self {
        self.faults.lying = true;
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

    pub fn run(&mut self, events: &[Event]) {
        for e in events {
            match *e {
                Event::Act { client, action } => {
                    let c = client % self.clients.len();
                    self.act(c, action);
                    self.note(|sim| {
                        format!(
                            "act c{c} {action:?} -> {:?}",
                            view_with(&sim.clients[c].local, sim.ledger.blobs())
                        )
                    });
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
                Event::PutBack { client } => self.put_back(client % self.clients.len()),
                Event::Crash { client } => self.crash(client % self.clients.len()),
            }
        }
    }

    /// For scripted tests: an item on client `c` whose name ends with `suffix`.
    /// `None` if several are.
    pub fn item_named(&self, c: usize, suffix: &str) -> Option<NodeId> {
        let mut named = self.clients[c]
            .local
            .iter()
            .filter(|(_, s)| s.name.ends_with(suffix))
            .map(|(id, _)| id);
        named.next().filter(|_| named.next().is_none())
    }

    /// For scripted tests: the kinds of the operations client `c` has still to run.
    pub fn planned(&self, c: usize) -> Vec<&'static str> {
        self.clients[c]
            .plan
            .iter()
            .map(|(op, _)| op.kind())
            .collect()
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
        self.faults.fail_commit_next = true;
    }

    /// For scripted tests: the next crash in a delete's unlinks ([`Event::Crash`]) comes after
    /// `n` of them.
    pub fn crash_after_unlinks(&mut self, n: usize) {
        self.faults.unlinks_before_crash = Some(n);
    }

    /// For scripted tests: the next tag write fails (where the OS keeps tags).
    pub fn fail_next_tag_write(&mut self) {
        self.faults.fail_tags_next = true;
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

    /// For scripted tests: what `Restore`'s `node` is to pick `node` in Recently deleted.
    pub fn removed_pick(&self, node: NodeId) -> Option<usize> {
        self.server.removed.keys().position(|n| *n == node)
    }

    /// How many folders and files the drive holds live.
    pub fn server_counts(&self) -> (usize, usize) {
        let live = &self.server.live;
        let folders = live
            .values()
            .filter(|s| s.state.kind == NodeKind::Folder)
            .count();
        (folders, live.len() - folders)
    }

    /// How many files were removed before their folder (`deleted_with`, node schema
    /// "Recently deleted": listed as items of their own, one by one), both still gone, whether
    /// Recently deleted lists them still or the purge took them since: which of them it takes
    /// depends on how many writes came after each, not on what was removed with what.
    pub fn removed_files_apart(&self) -> usize {
        let s = &self.server;
        let gone = |n: &NodeId| !s.live.contains_key(n);
        s.removals
            .iter()
            .filter(|(n, (folder, at, kind))| {
                *kind == NodeKind::File
                    && gone(n)
                    && gone(folder)
                    && s.removals
                        .get(folder)
                        .is_some_and(|(_, f, fk)| *fk == NodeKind::Folder && at < f)
            })
            .count()
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

    /// Plans and executes every client until nothing changes. Returns `false` if it doesn't
    /// settle within `max_rounds`.
    pub fn quiesce(&mut self, max_rounds: usize) -> bool {
        self.let_go();
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
                    || !client.placing.is_empty()
                    || client.hashing;
                // In pages, the sync goes on with the next pages (`next_page_planned`), as the
                // engine's rounds, a sync's at most.
                let mut rounds = 0;
                while self.step(c) || (rounds < 64 && self.next_page_planned(c)) {
                    rounds += 1;
                }
            }
            let unchanged = !self.server.changed_since(&server);
            // In pages, a step plans the next page (its sweep's): what ran counts too.
            if unchanged && self.ops_executed == before && (!any_planned || self.all_settled()) {
                return true;
            }
        }
        false
    }

    /// Like [`Sim::quiesce`], until nothing more happens: a round in which no operation runs
    /// and the drive doesn't change, whatever plans are left (an operation that waits for one
    /// the drive refused waits for good). Returns whether that came within `max_rounds`.
    pub fn quiesce_still(&mut self, max_rounds: usize) -> bool {
        self.let_go();
        for _ in 0..max_rounds {
            let before = self.ops_executed;
            let server = self.server.snapshot();
            for c in 0..self.clients.len() {
                self.plan(c);
                while self.step(c) {}
            }
            if self.ops_executed == before && !self.server.changed_since(&server) {
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
        self.let_go();
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
            asked[c] |= !self.clients[c].placing.is_empty() || self.clients[c].hashing;
        }
        false
    }

    /// The apps holding files locked let go ([`Action::Lock`]): the next plans find them
    /// hashed (each settling starts with every client asked to sync).
    fn let_go(&mut self) {
        for client in &mut self.clients {
            client.locked.clear();
        }
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
