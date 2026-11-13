//! A client: one folder of a computer, with its local tree, its base and its executor's
//! state.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use hbsync_engine::reconcile::{BaseExtra, DriveVersion, Op};
use hbsync_engine::scope::Scope;
use hbsync_engine::tree::Tree;
use hbsync_local::{Filter, Verdict};
use hbsync_namemap::sibling_hash;
use hbsync_platform::Os;
use hbsync_state::Coords;
use hbsync_types::{NodeId, NodeKind, RemoteRef, VersionTag};

use crate::crash::Placement;
use crate::model::server::time;

/// A node in Recently deleted as a client's mirror holds it (its state is `Client::plan_saw`'s).
#[derive(Debug, Clone, Copy)]
pub(crate) struct SeenRemoved {
    pub(crate) kind: NodeKind,
    pub(crate) version: RemoteRef,
}

impl SeenRemoved {
    /// When it went there: the server's time, its version's (a removal is a write).
    pub(crate) fn at(&self) -> i64 {
        time(self.version.version)
    }
}

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
    /// The marks the engine keeps beyond what its scan and feed mark (in pages only): a page's
    /// nodes its round didn't read clean, and a clash's loser outside the page with an
    /// operation left once refusals are taken out (`pages::losers_sent`: not one refused, nor
    /// one waiting for a refused one), till a round reads them clean. Not modelled: the
    /// executor's own marks (a fork's or a detach's ids, a skipped or stopped operation), which
    /// the simulator reads as what differs.
    pub(crate) marks: BTreeSet<NodeId>,
    /// What this sweep's held deletes take out of the dirty set (`held::out_of_dirty`), as the
    /// engine's `HoldOut`: out of `marks` (into `out`) at the sweep's end if it did nothing and
    /// wasn't stopped (the sync's end); else dropped. Not modelled: the engine's limit on what
    /// else stays dirty (`LEFTOVERS_MAX`), its plan of the rest alone reaching them, and a sync
    /// that starts in the middle of a sweep (the simulator's sweeps run on across them).
    pub(crate) held_out: BTreeSet<NodeId>,
    /// What held deletes took out of the dirty set: dirty again once a round's view reaches it
    /// (`HeldDeletes::plan_again`).
    pub(crate) out: BTreeSet<NodeId>,
    /// This sweep planned work, or its run stopped with work left (a plan made again before its
    /// operations ran): its held deletes stay dirty.
    pub(crate) busy: bool,
}

impl Pages {
    /// A sweep ended (its last page planned, and what its pages waited for): what its held
    /// deletes take out of the dirty set leaves it if the sweep did nothing and wasn't stopped,
    /// as the engine's sync ends so; a new sweep starts with nothing.
    pub(crate) fn sweep_ended(&mut self) {
        let held_out = std::mem::take(&mut self.held_out);
        if !std::mem::take(&mut self.busy) {
            self.marks.retain(|n| !held_out.contains(n));
            self.out.extend(held_out);
        }
    }
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
    /// The folders the pending plan removed something from to Recently deleted so far, each
    /// with the first it removed there: the folder it was in then (what the drive did with it
    /// since, restored and removed again elsewhere by another device, isn't the plan's).
    pub(crate) removed_by_plan: BTreeMap<NodeId, NodeId>,
    /// The folders whose removal in the pending plan ended without removing them: what is in
    /// them waits for the next plan.
    pub(crate) unremoved: HashSet<NodeId>,
    /// Items being deleted, moved aside (`.hbsync/aside`) and not yet checked and unlinked: each
    /// with what was below it then, its top at its place in the folder (sync semantics §7).
    pub(crate) aside: BTreeMap<NodeId, Tree>,
    /// Of those, the old versions a download replaced from another folder, with the bytes each
    /// held when it went aside (its name there): unlinked once the new one is committed, if
    /// still those (sync semantics §7, Old versions).
    pub(crate) replaced: BTreeMap<NodeId, Option<hbsync_types::FileContent>>,
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
    /// Pulls land between the pending plan and its steps (`Faults::plans`): the executor reads
    /// its mirror as the drive is at each step; else as the plan had it, with this client's
    /// own writes since ([`crate::Sim::mirror_read`]).
    pub(crate) pulls: bool,
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
    /// The operations the last plan blocked for a refusal: refused themselves (`true`), or
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
    /// The nodes in Recently deleted above live nodes, and the server's time of each live
    /// node's version, as the pending plan's input had them, with what its own writes removed
    /// since: the mirror's Recently deleted (and its view of what a folder's removal took
    /// along).
    pub(crate) planned_removed: BTreeMap<NodeId, SeenRemoved>,
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
    /// New files the scan leaves to be hashed as they are sent (sync semantics §3, Hash on
    /// send): until their first upload's commit (a base), or a plan finds them not free
    /// ([`Client::free_to_send`]: then they are read first).
    pub(crate) send_hash: BTreeSet<NodeId>,
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
    /// Files executable as the user made them here, or moved or named them so
    /// ([`crate::Action::Chmod`], [`crate::Action::Launcher`], a move), till the executor writes
    /// their bit: the only launchers a disk that doesn't hold their bit may hold executable
    /// (sync semantics §1).
    pub(crate) trusted: HashSet<NodeId>,
    /// Files whose bit, or name, a user changed here ([`crate::Action::Chmod`], a move): only
    /// an upload of one of them may change the drive's bit.
    pub(crate) user_x: HashSet<NodeId>,
    /// A main folder's: the drive's nodes its computer's other folders hold (a Desktop's), left
    /// out of it as the role folders are (§12), as its last plan read them.
    pub(crate) own_tops: BTreeSet<NodeId>,
    /// What the folder is: the main folder, a role folder, a Desktop.
    pub(crate) kind: hbsync_state::FolderKind,
    /// Whether the user added the folder: one not yet ([`crate::Action::AddDesktop`]) holds
    /// what its user made there, and syncs nothing.
    pub(crate) added: bool,
    /// A Desktop's: the name its folder on the drive is to have (its computer's), and the one
    /// last given it ([`crate::Action::RenameComputer`]; the engine's `DESKTOP_NAME(D)`).
    pub(crate) desktop_name: String,
    pub(crate) desktop_named: Option<String>,
    /// The folder is read-only (sync semantics §13): it never writes the drive, and what
    /// changed here goes back to the drive's.
    pub(crate) read_only: bool,
    /// What moved out of it, read-only, to keep what changed here: each item with what was
    /// below it (`Op::LocalEvict`).
    pub(crate) moved_out: Vec<Tree>,
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

    /// Whether `node`, a file left to be hashed on send, is free to be sent so against `remote`
    /// (the drive as the plan sees it), as the engine's view tells (`view::free_to_send`): no
    /// base, not on the drive, nothing there in its folder of its name, not parked.
    pub(crate) fn free_to_send(&self, node: NodeId, remote: &Tree) -> bool {
        let Some(l) = self.local.get(node) else {
            return false;
        };
        let key = sibling_hash(&l.name);
        l.kind == NodeKind::File
            && !self.base.contains(node)
            && !remote.contains(node)
            && !self.parked.contains_key(&node)
            && remote
                .children(l.parent)
                .filter_map(|k| remote.get(k))
                .all(|r| sibling_hash(&r.name) != key)
    }

    pub(crate) fn new(name: String, os: Os, filter: Filter, coords: Coords) -> Self {
        Client {
            computer: name.clone(),
            name,
            local: Tree::new(),
            base: Tree::new(),
            extra: HashMap::new(),
            plan: VecDeque::new(),
            removed_by_plan: BTreeMap::new(),
            unremoved: HashSet::new(),
            aside: BTreeMap::new(),
            replaced: BTreeMap::new(),
            parked: HashMap::new(),
            planned_local: Tree::new(),
            planned_remote: Tree::new(),
            pages: Default::default(),
            plan_saw: Tree::new(),
            pulls: false,
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
            send_hash: BTreeSet::new(),
            hashing: false,
            suspects: BTreeMap::new(),
            gone: BTreeSet::new(),
            syncs: 0,
            meanwhile: None,
            pull_mid_local: None,
            trusted: HashSet::new(),
            user_x: HashSet::new(),
            own_tops: BTreeSet::new(),
            kind: kind_for(coords.top),
            added: true,
            desktop_name: String::new(),
            desktop_named: None,
            read_only: false,
            moved_out: Vec::new(),
        }
    }

    /// `node` isn't synced here any more: its base goes.
    pub(crate) fn drop_base(&mut self, node: NodeId) {
        self.base.remove(node);
        self.extra.remove(&node);
    }

    /// The client's base's tag `name` of `node` (synced here), if it has one.
    pub(crate) fn base_tag(&self, node: NodeId, name: &str) -> Option<&hbsync_types::Tag> {
        self.base.get(node).and_then(|b| b.tags.get(name))
    }

    /// The part of the drive the client's folder syncs.
    pub(crate) fn scope(&self) -> Scope {
        Scope::on_device(self.coords.top, self.own_tops.iter().copied())
    }

    /// Whether the client's folder is a computer's Desktop (its top in Desktops when made).
    pub(crate) fn is_desktop(&self) -> bool {
        self.kind == hbsync_state::FolderKind::Desktop
    }
}

pub(crate) fn ignoring_d() -> Filter {
    Filter::from_rules("d\n", false).unwrap_or_default()
}

/// What the filter of the client's run says about its local item `node`.
pub(crate) fn verdict(client: &Client, node: NodeId) -> Verdict {
    verdict_in(&client.local, &client.run_filter, node)
}

/// Whether the client's item `node` in `tree`, one its filter hides, keeps its folder from a
/// delete as the engine's does ([`hbsync_engine::exec::keeps_its_folder`]): an ignored item and
/// a junk folder do, a junk file goes along.
pub(crate) fn keeps_in(tree: &Tree, filter: &Filter, node: NodeId) -> bool {
    let is_dir = tree.get(node).is_some_and(|s| s.kind == NodeKind::Folder);
    let verdict = verdict_in(tree, filter, node);
    !verdict.is_sync() && hbsync_engine::exec::keeps_its_folder(verdict == Verdict::Junk, is_dir)
}

/// What `filter` says about item `node` of `tree`, at its place there.
pub(crate) fn verdict_in(tree: &Tree, filter: &Filter, node: NodeId) -> Verdict {
    let Some(path) = tree.path(node) else {
        return Verdict::Sync;
    };
    let is_dir = tree.get(node).is_some_and(|s| s.kind == NodeKind::Folder);
    filter.names(path, is_dir)
}

/// Moves the user's marks on the client's item `node` ([`Client::trusted`], [`Client::user_x`])
/// to `new`.
pub(crate) fn rekey_marks(client: &mut Client, node: NodeId, new: NodeId) {
    for set in [&mut client.trusted, &mut client.user_x] {
        if set.remove(&node) {
            set.insert(new);
        }
    }
}

/// Gives the client's local item `node` the id `new`: in its tree, in the tree its plan was
/// made from, and in the parking records of its children.
pub(crate) fn rekey_client(client: &mut Client, node: NodeId, new: NodeId) {
    rekey(&mut client.local, node, new);
    rekey(&mut client.planned_local, node, new);
    if client.send_hash.remove(&node) {
        client.send_hash.insert(new);
    }
    rekey_marks(client, node, new);
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

/// The kind of a simulated folder holding the drive's node `top`: the main folder the root's, a
/// role folder its fixed folder's, a Desktop any other (its computer's folder in Desktops).
fn kind_for(top: NodeId) -> hbsync_state::FolderKind {
    use hbsync_state::FolderKind;
    match top {
        t if t.is_root() => FolderKind::Main,
        t if hbsync_types::Role::of(t).is_some() => FolderKind::Role,
        _ => FolderKind::Desktop,
    }
}
