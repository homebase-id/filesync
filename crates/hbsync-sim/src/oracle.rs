//! The oracle: the [`Ledger`] and [`Sim::check`] (convergence, invariants, no loss).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;

use hbsync_engine::tree::{Tree, Violation};
use hbsync_local::Filter;
use hbsync_platform::Os;
use hbsync_platform::name::check_remote_name;
use hbsync_types::{ContentHash, FileContent, NodeId, NodeKind, NodeState, Role, Tags, VersionTag};

use crate::Sim;
use crate::tagbook::TagBook;

/// The oracle's bookkeeping: every blob written and the ones no longer owed, the last listing
/// of Recently deleted, and the first breach of each rule the model saw happen (each kept
/// once: the check behind it runs only while none is, its message built only then). The model feeds it through methods
/// named for the fact they record; [`Sim::check`] reads it.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// Content hash → its blob. Every edit or create writes a fresh blob.
    blobs: HashMap<ContentHash, u64>,
    /// Blobs ever written.
    written: BTreeSet<u64>,
    /// Blobs no longer owed ([`Ledger::blob_superseded`]).
    superseded: BTreeSet<u64>,
    /// The last listing of Recently deleted ([`Action::ListDeleted`]): each node at its version.
    ///
    /// [`Action::ListDeleted`]: crate::Action::ListDeleted
    listed: Option<BTreeSet<(NodeId, VersionTag)>>,
    /// The first write that pointed a node at a folder the server never had.
    dangling: Option<String>,
    /// The first plan that deleted more than the guard allows without a confirmation.
    unguarded: Option<String>,
    /// The first operation a client ran outside its folder's part of the drive.
    scope_breach: Option<String>,
    /// The first operation that moved, renamed or deleted a folder a file not hashed yet is
    /// in (sync semantics §6.3).
    moved_waiting: Option<String>,
    /// The first folder a plan removed after something in it (node schema, Recently deleted:
    /// what a folder's delete takes along is removed no earlier than the folder).
    removed_after_contents: Option<String>,
    /// The first node emptying or the purge deleted for good that wasn't a removed row chosen
    /// at that version (a live node, say), or a folder holding anything not going after it.
    emptied_unlisted: Option<String>,
    /// The first item emptied on its own that stayed though nothing kept it.
    emptied_kept: Option<String>,
    /// The first client that took a file the drive holds as gone for good (a wrong "not
    /// there" believed: [`crate::Sim::with_wrong_not_theres`]).
    gone_while_there: Option<String>,
    /// For each folder in Recently deleted, what its last rightful removal took along as it
    /// happened: the live nodes in it then, through live folders, each at its version then.
    took: BTreeMap<NodeId, BTreeMap<NodeId, VersionTag>>,
    /// The rows of Recently deleted (each node at its version there) a user's delete or a true
    /// take-along put there: only their blobs may go for good.
    removed_rightly: BTreeSet<(NodeId, VersionTag)>,
    /// The items each client's user deleted there (or moved out of its folder, or a filter
    /// rule took out of it), not back there since, as the user saw them then.
    user_deleted: BTreeMap<(usize, NodeId), Seen>,
    /// For each client, what a delete took along when it made its pending plan, each node at
    /// its version then.
    plan_taken: BTreeMap<usize, BTreeSet<(NodeId, VersionTag)>>,
    /// The first node that went to Recently deleted though no one deleted it.
    wrongly_removed: Option<String>,
    /// What users did to tags, and what became of it.
    pub(crate) tags: TagBook,
    /// The first answer of a client's write the store recorded wrongly: left out alone, or
    /// over a newer state of its node a pull recorded meanwhile ([`crate::Action::Meanwhile`]).
    answer_undid: Option<String>,
    /// The first plan the engine's own planning path made otherwise ([`Sim::with_narrow_check`]).
    narrow_differs: Option<String>,
    /// The first item a client unlinked that wasn't the version its base records (sync
    /// semantics §7: a delete is checked, then final), junk aside.
    deleted_unverified: Option<String>,
    /// Items a client unlinked whose delete's commit a crash lost, each as its base had it: the
    /// known limit of a delete without a write-ahead record (sync semantics §8a). Its next plan
    /// reads the item as deleted there; where the drive has it so (a restore since, a folder's
    /// removal that took it along), it removes it, as a user's delete of it would.
    commit_lost: BTreeMap<(usize, NodeId), Seen>,
}

/// An item as a user saw it on the drive, but for its tags (a tag change loses to a delete):
/// its folder there, its name, and its bytes.
pub(crate) type Seen = (NodeId, String, Option<ContentHash>);

/// Keeps the first breach of a rule: `what` is asked only while none is kept.
fn first(kept: &mut Option<String>, what: impl FnOnce() -> Option<String>) {
    if kept.is_none() {
        *kept = what();
    }
}

impl Ledger {
    /// Blob `blob`, with `hash`, was written.
    pub(crate) fn blob_written(&mut self, blob: u64, hash: ContentHash) {
        self.blobs.insert(hash, blob);
        self.written.insert(blob);
    }

    /// The blob of `content`, if it is one, is no longer owed: a user replaced or deleted it
    /// after holding it, another device wrote over it, it was junk disposed of, or it went
    /// for good.
    pub(crate) fn blob_superseded(&mut self, content: Option<FileContent>) {
        if let Some(blob) = content.and_then(|c| self.blobs.get(&c.hash)) {
            self.superseded.insert(*blob);
        }
    }

    /// The blob each content hash holds, for traces.
    pub(crate) fn blobs(&self) -> &HashMap<ContentHash, u64> {
        &self.blobs
    }

    /// A client unlinked an item that may not be the version its base records: `what`, if so.
    pub(crate) fn deleted_unverified(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.deleted_unverified, what);
    }

    /// Client `c` unlinked `node`, as its base had it (`seen`), and a crash lost the commit:
    /// [`Ledger::commit_lost`].
    pub(crate) fn delete_commit_lost(&mut self, c: usize, node: NodeId, seen: Seen) {
        self.commit_lost.insert((c, node), seen);
    }

    /// Whether client `c`'s removal of `node`, as the drive has it now, is the one a delete's
    /// lost commit makes ([`Ledger::commit_lost`]): once, as it removes it.
    pub(crate) fn removed_by_lost_commit(&mut self, c: usize, node: NodeId, now: &Seen) -> bool {
        let lost = self.commit_lost.get(&(c, node)) == Some(now);
        if lost {
            self.commit_lost.remove(&(c, node));
        }
        lost
    }

    /// A user listed Recently deleted: `rows`, each node at its version.
    pub(crate) fn deleted_listed(&mut self, rows: BTreeSet<(NodeId, VersionTag)>) {
        self.listed = Some(rows);
    }

    /// The last listing of Recently deleted, if a user made one: what emptying it as listed
    /// acts on (the model reads it here), and what the emptying is judged against.
    pub(crate) fn listing(&self) -> Option<&BTreeSet<(NodeId, VersionTag)>> {
        self.listed.as_ref()
    }

    /// A client took a file the drive holds as gone for good, if `what` says which.
    pub(crate) fn took_as_gone(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.gone_while_there, what);
    }

    /// Whether a client took a file the drive holds as gone for good.
    pub(crate) fn took_a_live_file_as_gone(&self) -> bool {
        self.gone_while_there.is_some()
    }

    /// A write pointed a node at a folder the server never had, if `what` names one.
    pub(crate) fn dangling_write(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.dangling, what);
    }

    /// An operation wrote outside its folder's part of the drive, if `what` names one.
    pub(crate) fn wrote_outside_scope(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.scope_breach, what);
    }

    /// A plan removed a folder after something in it, if `what` names one.
    pub(crate) fn folder_removed_after_contents(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.removed_after_contents, what);
    }

    /// Emptying or the purge deleted for good what it mustn't, if `what` names one.
    pub(crate) fn emptied_unlisted(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.emptied_unlisted, what);
    }

    /// An item emptied on its own stayed though nothing kept it, if `what` names one.
    pub(crate) fn emptied_kept(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.emptied_kept, what);
    }

    /// Folder `folder` went to Recently deleted rightly, with the live nodes in it then
    /// (through live folders) each at its version: what its removal took along, as it happened.
    pub(crate) fn folder_removed(&mut self, folder: NodeId, below: BTreeMap<NodeId, VersionTag>) {
        self.took.insert(folder, below);
    }

    /// A client's write's answer was recorded wrongly, if `what` says how
    /// ([`crate::Action::Meanwhile`]).
    pub(crate) fn answer_undid(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.answer_undid, what);
    }

    /// Whether the engine's planning path was found planning otherwise than the sim.
    pub(crate) fn found_narrow_differs(&self) -> bool {
        self.narrow_differs.is_some()
    }

    /// The engine's planning path planned otherwise than the sim, if `what` says how.
    pub(crate) fn narrow_differs(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.narrow_differs, what);
    }

    /// Client `c` made a plan while a delete had taken `taken` along (each at its version).
    pub(crate) fn planned_with(&mut self, c: usize, taken: BTreeSet<(NodeId, VersionTag)>) {
        self.plan_taken.insert(c, taken);
    }

    /// Whether client `c`'s pending plan was made while a delete had taken `node` along at
    /// `version` (the plan may bring the folder back for an edit and mark what its removal took
    /// along, in one go). The mark may also come after a user restored the folder, which no
    /// device can rule out: a removal can't be made conditional on another node's state. Such a
    /// node is in Recently deleted, as its own item, for the user to restore.
    pub(crate) fn plan_saw_taken(&self, c: usize, node: NodeId, version: VersionTag) -> bool {
        self.plan_taken
            .get(&c)
            .is_some_and(|t| t.contains(&(node, version)))
    }

    /// The live nodes a folder's removal took along, by what happened ([`Ledger::folder_removed`]),
    /// not by the engine's rule: each node below a folder in Recently deleted (`removed`; the
    /// nearest one on its way up through live folders) that was in it when it went, at the same
    /// version since, and so was every folder between them; but a node not taken keeps the
    /// folders above it, up to the removed one (node schema, Recently deleted). `live` gives
    /// each live node's folder and version.
    pub(crate) fn taken_along(
        &self,
        live: &BTreeMap<NodeId, (NodeId, VersionTag)>,
        removed: impl Fn(NodeId) -> bool,
    ) -> BTreeSet<NodeId> {
        let mut taken = BTreeSet::new();
        let mut kept = BTreeSet::new();
        for &node in live.keys() {
            // Up through live folders to a removed one (a cycle or a gap ends the walk).
            let mut path = vec![node];
            let folder = loop {
                let at = path[path.len() - 1];
                let Some(&(parent, _)) = live.get(&at) else {
                    break None;
                };
                if removed(parent) {
                    break Some(parent);
                }
                if !live.contains_key(&parent) || path.contains(&parent) {
                    break None;
                }
                path.push(parent);
            };
            let Some(folder) = folder else {
                continue;
            };
            let took = self.took.get(&folder);
            let unchanged =
                |n: &NodeId| took.and_then(|t| t.get(n)) == live.get(n).map(|(_, version)| version);
            if path.iter().all(unchanged) {
                taken.insert(node);
            } else {
                kept.extend(path);
            }
        }
        &taken - &kept
    }

    /// Row `row` (a node at its version) went to Recently deleted; `rightly`: a user deleted it
    /// (on a device, or another app), or a folder's delete took it along. One that went there
    /// wrongly is a breach, whatever becomes of it.
    pub(crate) fn row_removed(&mut self, row: (NodeId, VersionTag), rightly: bool) {
        if rightly {
            self.removed_rightly.insert(row);
        }
        first(&mut self.wrongly_removed, || {
            (!rightly).then(|| format!("{:?} at {:?}", row.0, row.1))
        });
    }

    /// Whether row `row` of Recently deleted went there rightly ([`Ledger::row_removed`]): only
    /// then may its blob go for good.
    pub(crate) fn removed_rightly(&self, row: &(NodeId, VersionTag)) -> bool {
        self.removed_rightly.contains(row)
    }

    /// The user of client `c` deleted its item `node` there (or moved it out of the folder, or
    /// a filter rule took it out), having seen it as `seen`.
    pub(crate) fn user_deleted(&mut self, c: usize, node: NodeId, seen: Seen) {
        self.user_deleted.insert((c, node), seen);
    }

    /// Whether some client's user deleted `node` there, as the drive has it now (`now`: an
    /// edit or a move made elsewhere since wins over the delete), and it isn't back there
    /// since: whichever device then removes it from the drive does what that user asked.
    pub(crate) fn deleted_by_user(&self, node: NodeId, now: &Seen) -> bool {
        self.user_deleted
            .iter()
            .any(|((_, n), seen)| *n == node && seen == now)
    }

    /// `node` is back on client `c`: its user's delete there lost, or the node came back to the
    /// drive since and from there to `c` (a restore alone doesn't undo the delete: the client
    /// sends it up again over the version back).
    pub(crate) fn back(&mut self, c: usize, node: NodeId) {
        self.user_deleted.remove(&(c, node));
        self.commit_lost.remove(&(c, node));
    }
}

impl Sim {
    /// Checks convergence, invariants and no data loss. Returns a description of the first
    /// failure.
    pub fn check(&self) -> Result<(), String> {
        let ledger = &self.ledger;
        if let Some(op) = &ledger.dangling {
            return Err(format!("created under a folder the server never had: {op}"));
        }
        if let Some(what) = &ledger.narrow_differs {
            return Err(what.clone());
        }
        if let Some(what) = &ledger.answer_undid {
            return Err(format!("a write's answer was recorded wrongly: {what}"));
        }
        if let Some(plan) = &ledger.unguarded {
            return Err(format!("a mass delete went through unconfirmed: {plan}"));
        }
        if let Some(gone) = &ledger.gone_while_there {
            return Err(gone.clone());
        }
        if let Some(what) = &ledger.deleted_unverified {
            return Err(format!("deleted what wasn't checked: {what}"));
        }
        // An item being deleted is aside for a step, or till the next sync's recovery: never
        // once the clients settled.
        for (c, client) in self.clients.iter().enumerate() {
            if let Some(node) = client.aside.keys().next() {
                return Err(format!("c{c}: {node:?} is left aside"));
            }
        }
        // A record recovery keeps holds its node off placements until settled: never for good.
        for (c, client) in self.clients.iter().enumerate() {
            if let Some(node) = client.placing.keys().next() {
                return Err(format!("c{c}: the record of {node:?} is never settled"));
            }
        }
        // Recently deleted holds only what a user deleted, or a folder's removal took along.
        if let Some(what) = &ledger.wrongly_removed {
            return Err(format!(
                "{what} went to Recently deleted, though no one deleted it"
            ));
        }
        // What a folder's removal took along is removed, marked so or not yet.
        let taken = self.taken_along();
        let drive = self.server.tree();
        let mut server = drive.clone();
        for n in &taken {
            server.remove(*n);
        }
        let mut kept_here: BTreeSet<u64> = BTreeSet::new();
        // Items the drive refuses as they are (with the name it refuses), with what is in them.
        let mut refused_here: BTreeSet<NodeId> = BTreeSet::new();
        // What some client syncs: a node in a file no client syncs is left there by all.
        let mut synced_somewhere: BTreeSet<NodeId> = BTreeSet::new();
        for (c, client) in self.clients.iter().enumerate() {
            let mut visible = client.local.clone();
            for n in filtered(&client.local, &client.filter) {
                if let Some(blob) = visible
                    .remove(n)
                    .and_then(|s| s.content)
                    .and_then(|c| ledger.blobs.get(&c.hash))
                {
                    kept_here.insert(*blob);
                }
            }
            let mut synced = self.expected(c, &drive, &taken);
            synced_somewhere.extend(synced.ids());
            // What the client holds back for a refusal: each operation refused itself is one
            // the drive refuses as it is now (by its own judgement: a record that held back one
            // it takes hid a later change); it stays on its client with what is in it, and
            // anything else converges.
            // Once nothing more happens, what the last plan still sent was tried and is blocked
            // behind one (an operation that waits for a refused one stays: a local cost, told
            // when it keeps coming back): it, too, stays as it is, unhidden.
            let mut base = client.base.clone();
            for (op, refused) in &client.held_back {
                if *refused && !self.drive_refused(c, op) {
                    return Err(format!(
                        "{}: the refusal record hid {op:?}, which the drive takes as it is",
                        client.name
                    ));
                }
            }
            // A parked item waits for its name, which a folder held back may keep.
            let left = client.held_back.iter().map(|(op, _)| op.node());
            let parked = client
                .parked
                .keys()
                .copied()
                .filter(|_| self.faults.refusing);
            let nodes: Vec<NodeId> = left
                .chain(client.last_plan.iter().map(|op| op.node()))
                .chain(parked)
                .collect();
            for n in nodes {
                let mut inside = below(&visible, n);
                inside.extend(below(&synced, n));
                inside.extend(below(&base, n));
                for m in inside {
                    refused_here.insert(m);
                    synced.remove(m);
                    base.remove(m);
                    if let Some(blob) = visible
                        .remove(m)
                        .and_then(|s| s.content)
                        .and_then(|c| ledger.blobs.get(&c.hash))
                    {
                        kept_here.insert(*blob);
                    }
                }
            }
            // The tags as the client holds them, on both sides.
            let server_view = view(&held_by(&synced, client.os));
            let local_view = view(&held_by(&visible, client.os));
            if local_view != server_view {
                return Err(format!(
                    "{} diverged:\n  local:  {local_view:?}\n  server: {server_view:?}",
                    client.name
                ));
            }
            if !client.local.violations().is_empty() {
                return Err(format!("{}: {:?}", client.name, client.local.violations()));
            }
            if view(&base) != view(&synced) {
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
        // An orphan whose adoption the drive refused stays one (the record holds it back).
        let broken: Vec<_> = server
            .violations()
            .into_iter()
            .filter(|v| !matches!(v, Violation::DuplicateName { .. }))
            .filter(|v| !matches!(v, Violation::Orphan { node, .. } if refused_here.contains(node)))
            // What is in a folder no client reads waits for it, in Recently deleted too.
            .filter(|v| {
                !matches!(v, Violation::Orphan { parent, .. }
                    if self.server.unreadable_on_drive().any(|(n, _)| n == parent))
            })
            .filter(|v| {
                !matches!(v, Violation::ParentNotFolder { node, .. }
                    if !synced_somewhere.contains(node))
            })
            // A cycle whose repair (a move out of it) the drive refused stays one.
            .filter(|v| {
                !matches!(v, Violation::Cycle { node }
                    if up_from(&server, *node).iter().any(|n| refused_here.contains(n)))
            })
            .collect();
        if !broken.is_empty() {
            return Err(format!("server: {broken:?}"));
        }
        if let Some(what) = &ledger.removed_after_contents {
            return Err(format!("a folder was removed after what is in it: {what}"));
        }
        if let Some(what) = &ledger.emptied_unlisted {
            return Err(format!("deleting for good took what it mustn't: {what}"));
        }
        if let Some(what) = &ledger.emptied_kept {
            return Err(format!(
                "an item emptied stayed, though nothing kept it: {what}"
            ));
        }
        // Nothing moved what waits for a hash.
        if let Some(what) = &ledger.moved_waiting {
            return Err(what.clone());
        }
        // No folder wrote outside its part of the drive.
        if let Some(op) = &ledger.scope_breach {
            return Err(format!(
                "a folder wrote outside its part of the drive: {op}"
            ));
        }
        // A computer holds a node in one of its folders at most.
        for &(a, b) in &self.computers {
            let (a, b) = (&self.clients[a].local, &self.clients[b].local);
            if let Some(both) = a.ids().filter(|n| b.contains(*n)).min() {
                return Err(format!("{both:?} is in both folders of one computer"));
            }
        }
        // What is in a folder no client reads stays in it while it is there.
        for (folder, kids) in &self.server.unreadable {
            if !self.server.live.contains_key(folder) {
                continue;
            }
            if let Some(kid) = kids.iter().find(|k| {
                self.server
                    .live
                    .get(k)
                    .is_some_and(|n| n.state.parent != *folder)
            }) {
                return Err(format!("{kid:?} left the folder no client reads"));
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
            .filter(|(_, s)| check_remote_name(&s.name).is_err() || s.name.starts_with(".hbsync-"))
            .min_by_key(|(n, _)| *n)
        {
            return Err(format!("server: invalid name {:?}", bad.name));
        }
        let mut present: BTreeSet<u64> = server
            .iter()
            .filter_map(|(_, s)| s.content.and_then(|c| ledger.blobs.get(&c.hash).copied()))
            .collect();
        // Recently deleted keeps what was deleted, until the purge.
        let taken = taken.iter().filter_map(|n| self.server.live.get(n));
        present.extend(self.server.removed.values().chain(taken).filter_map(|n| {
            n.state
                .content
                .and_then(|c| ledger.blobs.get(&c.hash).copied())
        }));
        // Filtered items are kept on their client only.
        present.extend(kept_here);
        let lost: Vec<u64> = ledger
            .written
            .difference(&ledger.superseded)
            .filter(|b| !present.contains(b))
            .copied()
            .collect();
        if !lost.is_empty() {
            return Err(format!("lost blobs {lost:?}; present {present:?}"));
        }
        let held_at_removal = |node: NodeId, name: &str| {
            self.server
                .tags_at_removal
                .get(&node)
                .is_some_and(|t| t.contains(name))
        };
        self.ledger
            .tags
            .check(&server, &refused_here, held_at_removal)?;
        Ok(())
    }
}

impl Sim {
    /// The live nodes a folder's removal took along, as it happened ([`Ledger::taken_along`]),
    /// not as the engine's rule has it.
    pub(crate) fn taken_along(&self) -> BTreeSet<NodeId> {
        let live = self
            .server
            .live
            .iter()
            .map(|(n, l)| (*n, (l.state.parent, l.remote.version)))
            .collect();
        self.ledger
            .taken_along(&live, |f| self.server.removed.contains_key(&f))
    }

    /// What client `c` syncs of the drive, in its folder's coordinates, by the oracle's own
    /// walks: the live nodes in its folder's part (the main folder's: all but the role folders
    /// and what is in them; a role folder's: what is in it), but for what a delete took along
    /// (`taken`) and what its filter leaves out.
    fn expected(&self, c: usize, drive: &Tree, taken: &BTreeSet<NodeId>) -> Tree {
        let coords = self.clients[c].coords;
        // A folder no client reads, while it is on the drive, and what is in it: what is in
        // it waits for it (`Action::NewerFolder`).
        let unread: BTreeSet<NodeId> = self.server.unreadable_on_drive().map(|(n, _)| *n).collect();
        let mut tree = Tree::new();
        for (node, n) in &self.server.live {
            let up = up_from(drive, *node);
            let ours = match coords.top {
                top if top.is_root() => !Role::ALL.iter().any(|r| up.contains(&r.node())),
                top => *node != top && up.contains(&top),
            };
            let waits = up.iter().any(|a| unread.contains(a));
            if ours && !waits && !taken.contains(node) {
                let parent = coords.from_drive(n.state.parent);
                tree.insert(
                    *node,
                    NodeState {
                        parent,
                        ..n.state.clone()
                    },
                );
            }
        }
        for n in filtered(&tree, &self.clients[c].filter) {
            tree.remove(n);
        }
        tree
    }
}

/// A planned operation as the oracle reads it: it may not import the engine's `Op`. `op` is
/// the operation, for the oracle's messages.
pub(crate) struct Planned<'a> {
    pub(crate) node: NodeId,
    pub(crate) what: What,
    pub(crate) op: &'a dyn Debug,
}

/// What a [`Planned`] operation does, as far as the oracle's checks of a plan go.
pub(crate) enum What {
    RemoteDelete,
    LocalTrash {
        subtree: Vec<NodeId>,
    },
    /// Another operation that trashes (a forget with something here).
    Trash,
    LocalMove {
        parent: NodeId,
        name: String,
    },
    RemoteUpdate {
        parent: NodeId,
        name: String,
    },
    /// A new id for the item here (§10).
    Detach {
        new_node: NodeId,
    },
    Other,
}

/// A plan and what it was made from, for [`Ledger::plan_made`].
pub(crate) struct PlanSeen<'a> {
    pub(crate) client: &'a str,
    pub(crate) local: &'a Tree,
    pub(crate) base: &'a Tree,
    /// The drive as the client sees it.
    pub(crate) remote: &'a Tree,
    pub(crate) filter: &'a Filter,
    /// The files here not hashed yet.
    pub(crate) unhashed: &'a HashSet<NodeId>,
    pub(crate) ops: &'a [Planned<'a>],
    pub(crate) held: &'a [Planned<'a>],
    pub(crate) all_ops: &'a dyn Debug,
    pub(crate) all_held: &'a dyn Debug,
    pub(crate) synced_files: usize,
    pub(crate) confirmed: &'a HashSet<NodeId>,
    pub(crate) wiped: &'a HashSet<NodeId>,
}

impl Ledger {
    /// Checks a plan `seen` made: it moves nothing that waits for a hash (sync semantics
    /// §6.3), and, with the guard at `guard`, deletes no more than the guard lets through
    /// unconfirmed (§7).
    pub(crate) fn plan_made(&mut self, seen: &PlanSeen<'_>, guard: Option<usize>) {
        let PlanSeen {
            local,
            base,
            remote,
            unhashed,
            ..
        } = *seen;
        // The new ids of those the plan detaches (§10).
        let detached: HashMap<NodeId, NodeId> = seen
            .ops
            .iter()
            .filter_map(|p| match p.what {
                What::Detach { new_node } => Some((p.node, new_node)),
                _ => None,
            })
            .collect();
        // What waits for a hash, worked out on its own (sync semantics §6.3): every folder a
        // file here not hashed yet is in, on any side, and every folder those are in, on the
        // drive as this device sees it (what its filter leaves out is out of view); a detached
        // item is a new one here, held where it is here only, under its new id.
        let out_of_view: HashSet<NodeId> = filtered(remote, seen.filter).into_iter().collect();
        let mut held: HashSet<NodeId> = HashSet::new();
        let mut queue: Vec<(NodeId, bool)> = unhashed
            .iter()
            .map(|n| (*n, !detached.contains_key(n)))
            .collect();
        while let Some((n, synced)) = queue.pop() {
            let sides: &[(&Tree, bool)] = match synced {
                true => &[(local, false), (remote, true), (base, false)],
                false => &[(local, false)],
            };
            for (i, (tree, on_drive)) in sides.iter().enumerate() {
                if *on_drive && out_of_view.contains(&n) {
                    continue;
                }
                if let Some(s) = tree.get(n)
                    && !s.parent.is_root()
                    && !(*on_drive && out_of_view.contains(&s.parent))
                    && held.insert(s.parent)
                {
                    // Reached here, a detached folder is its new item, here only; reached
                    // through the drive or the base, its old node, on every side.
                    let here = i == 0;
                    queue.push((s.parent, !(here && detached.contains_key(&s.parent))));
                }
            }
        }
        // A detached folder held under both its ids.
        let held: HashSet<NodeId> = held
            .iter()
            .filter_map(|n| detached.get(n).copied())
            .chain(held.iter().copied())
            .collect();
        let old_of: HashMap<NodeId, NodeId> =
            detached.iter().map(|(old, new)| (*new, *old)).collect();
        // No plan touches a file here not hashed yet, or moves, renames or deletes what holds
        // it, on any side.
        first(&mut self.moved_waiting, || {
            let touched: HashSet<NodeId> = unhashed
                .iter()
                .map(|n| detached.get(n).copied().unwrap_or(*n))
                .collect();
            seen.ops
                .iter()
                .find(|p| {
                    let node = p.node;
                    // A detached item's place here is its old node's.
                    let at = old_of.get(&node).copied().unwrap_or(node);
                    let moved = |t: &Tree, parent: &NodeId, name: &str| {
                        t.get(at)
                            .is_some_and(|s| s.parent != *parent || s.name != name)
                    };
                    // A detach only gives the item here another id (§10): it stays as it is.
                    let detach = matches!(p.what, What::Detach { .. });
                    ((touched.contains(&node) || unhashed.contains(&node)) && !detach)
                        || (held.contains(&node)
                            && match &p.what {
                                What::RemoteDelete | What::LocalTrash { .. } | What::Trash => true,
                                What::LocalMove { parent, name } => moved(local, parent, name),
                                What::RemoteUpdate { parent, name } => moved(remote, parent, name),
                                _ => false,
                            })
                })
                .map(|p| format!("{}: {:?} moves what waits for a hash", seen.client, p.op))
        });
        // The guard's rule, checked independently: no plan deletes more files on a side than
        // the limit (or every file) without the user's confirmation.
        let Some(min) = guard else {
            return;
        };
        let (synced_files, confirmed, wiped) = (seen.synced_files, seen.confirmed, seen.wiped);
        // Counted only until the first breach is kept.
        first(&mut self.unguarded, || {
            let trips = |n: usize| n > min || (synced_files > 0 && n >= synced_files && n > 1);
            let files_under = |tree: &Tree, node: NodeId| {
                tree.subtree(node)
                    .into_iter()
                    .filter(|n| tree.get(*n).is_some_and(|s| s.kind == NodeKind::File))
                    .count()
            };
            let (mut deleted, mut trashed) = (0, 0);
            // What was in a confirmed folder whose delete waits for a hash is confirmed: one
            // synced, gone on a side (or taken along by a folder gone on the drive), holding a
            // file here not hashed yet on any side.
            let waits: HashSet<NodeId> = confirmed
                .iter()
                .copied()
                .filter(|f| {
                    let gone =
                        !local.contains(*f) || remote.path(*f).is_none() || out_of_view.contains(f);
                    base.contains(*f) && gone && held.contains(f)
                })
                .collect();
            let unconfirmed = |p: &&Planned<'_>| {
                !confirmed.contains(&p.node)
                    && !base.ancestors(p.node).iter().any(|a| waits.contains(a))
            };
            for p in seen.ops.iter().filter(unconfirmed) {
                match p.what {
                    What::RemoteDelete
                        if remote.get(p.node).is_some_and(|s| s.kind == NodeKind::File) =>
                    {
                        deleted += 1;
                    }
                    What::LocalTrash { .. } => trashed += files_under(local, p.node),
                    _ => {}
                }
            }
            // Removals from elsewhere are followed, but every synced file over the limit.
            let every = |n: usize| synced_files > 0 && n >= synced_files && n > min;
            // Held before (sticky till answered) isn't judged again.
            let was_wiped = |n: NodeId| {
                wiped.contains(&n) || base.ancestors(n).iter().any(|a| wiped.contains(a))
            };
            let held_trashed: usize = seen
                .held
                .iter()
                .map(|p| match p.what {
                    What::LocalTrash { .. } if !was_wiped(p.node) => files_under(local, p.node),
                    _ => 0,
                })
                .sum();
            // And what was held so stays held till answered.
            if let Some(p) = seen.ops.iter().filter(unconfirmed).find(|p| {
                matches!(&p.what, What::LocalTrash { subtree }
                        if was_wiped(p.node) || subtree.iter().any(|n| wiped.contains(n)))
            }) {
                return Some(format!(
                    "{} trashes what was held as every synced file, unanswered: {:?}",
                    seen.client, p.op
                ));
            }
            let held_elsewhere = held_trashed > 0 && !every(held_trashed + trashed);
            if held_elsewhere {
                return Some(format!(
                    "{} held {held_trashed} removals from elsewhere, short of every synced \
                     file ({synced_files}): {:?}",
                    seen.client, seen.all_held
                ));
            }
            (trips(deleted) || every(trashed)).then(|| {
                format!(
                    "{} deletes {deleted} remote and trashes {trashed} local files \
                     unconfirmed (limit {min}, {synced_files} synced): {:?}",
                    seen.client, seen.all_ops
                )
            })
        });
    }
}

/// The items of `tree` `filter` leaves out, by their paths (each folder on the way judged, so
/// what is in a folder left out is too); one not connected to the root isn't judged.
fn filtered(tree: &Tree, filter: &Filter) -> Vec<NodeId> {
    tree.iter()
        .filter(|(n, s)| {
            tree.path(*n)
                .is_some_and(|path| !filter.names(path, s.kind == NodeKind::Folder).is_sync())
        })
        .map(|(n, _)| n)
        .collect()
}

/// `tree` with each item's tags as `os` holds them: what the other side has of what it can't
/// hold doesn't count.
fn held_by(tree: &Tree, os: Os) -> Tree {
    let mut held = tree.clone();
    for (n, s) in tree.iter() {
        let tags = s.tags.held_by(os);
        held.insert(n, NodeState { tags, ..s.clone() });
    }
    held
}

/// Tags as `name:colour`.
pub(crate) fn tag_names(tags: &Tags) -> Vec<String> {
    tags.iter()
        .map(|t| format!("{}:{}", t.name, t.colour))
        .collect()
}

/// A tree as a sorted list of `path kind content id`: node ids are part of identity across
/// clients, so two trees only match if the same nodes sit at the same paths.
fn view(tree: &Tree) -> Vec<String> {
    view_with(tree, &HashMap::new())
}

pub(crate) fn view_with(tree: &Tree, blobs: &HashMap<ContentHash, u64>) -> Vec<String> {
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

/// `node` and the folders above it in `tree`, going up until a gap (the first missing one
/// included) or a cycle (each once).
fn up_from(tree: &Tree, node: NodeId) -> Vec<NodeId> {
    let mut seen = Vec::new();
    let mut at = node;
    while !seen.contains(&at) {
        seen.push(at);
        match tree.get(at) {
            Some(s) => at = s.parent,
            None => break,
        }
    }
    seen
}

/// `node` and everything below it in `tree`, each once: a tree a refusal left half moved may
/// hold a cycle (which the checks report), and a walk must still end.
fn below(tree: &Tree, node: NodeId) -> Vec<NodeId> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![node];
    while let Some(n) = stack.pop() {
        if seen.insert(n) {
            stack.extend(tree.children(n));
        }
    }
    seen.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Action, Event, act};
    use hbsync_types::Tag;

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

    /// What a stopped delete took along is what happened: the folder's contents as they were
    /// then. A file the phone edited since wasn't taken (it keeps its folders), its untouched
    /// sibling was.
    #[test]
    fn taken_along_is_what_happened() {
        let mut sim = Sim::new(1);
        sim.run(&[act(0, Action::CreateFolder { parent: 0, name: 1 })]);
        let parent = sim.pick(0, sim.item(0, "b").expect("b"), true).expect("b");
        sim.run(&[
            act(0, Action::CreateFile { parent, name: 0 }),
            act(0, Action::CreateFile { parent, name: 3 }),
        ]);
        assert!(sim.quiesce(10));
        let [b, a, c] = ["b", "b/a", "b/c.txt"].map(|p| sim.item(0, p).expect(p));
        let pick = sim.stopped_pick(b).expect("b");
        sim.run(&[act(0, Action::DeleteStopped { pick, done: 0 })]);
        assert_eq!(sim.taken_along(), BTreeSet::from([a, c]));
        // EditTaken picks among the files taken along, in id order: a.
        sim.run(&[act(
            0,
            Action::EditTaken {
                pick: usize::from(a > c),
            },
        )]);
        assert_eq!(sim.taken_along(), BTreeSet::from([c]));
    }

    /// A node a client removes from the drive though no one deleted it (nor took it along) is
    /// reported, the engine's word on it notwithstanding.
    #[test]
    fn a_removal_no_one_asked_for_is_reported() {
        let mut sim = Sim::new(2);
        sim.run(&[act(0, Action::CreateFile { parent: 0, name: 0 })]);
        assert!(sim.quiesce(10));
        let file = sim.item(1, "a").expect("a");
        let remote = sim.server.live[&file].remote;
        let op = hbsync_engine::reconcile::Op::RemoteDelete {
            node: file,
            parent: NodeId::ROOT,
            file_id: remote.file_id,
            expected: remote.version,
        };
        sim.execute(1, &op);
        assert!(sim.quiesce(10));
        let wrong = sim.check().expect_err("a removal no one asked for");
        assert!(wrong.contains("no one deleted it"), "{wrong}");
    }

    /// A user's delete covers the item as they saw it: an edit made elsewhere since wins, and a
    /// removal of the edited version is reported.
    #[test]
    fn a_users_delete_doesnt_cover_a_later_edit() {
        let mut sim = Sim::new(2);
        sim.run(&[act(0, Action::CreateFile { parent: 0, name: 0 })]);
        assert!(sim.quiesce(10));
        let file = sim.item(0, "a").expect("a");
        sim.run(&[
            act(0, Action::Delete { node: 0 }),
            act(1, Action::Edit { file: 0 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
        ]);
        let remote = sim.server.live[&file].remote;
        let op = hbsync_engine::reconcile::Op::RemoteDelete {
            node: file,
            parent: NodeId::ROOT,
            file_id: remote.file_id,
            expected: remote.version,
        };
        sim.execute(0, &op);
        let wrong = sim.check().expect_err("the edit was removed");
        assert!(wrong.contains("no one deleted it"), "{wrong}");
    }

    /// The oracle trusts the engine no more than it does now: no path into the engine, nor into
    /// the rest of the crate (the model and `exec` mirror the engine), but these `use` lines.
    #[test]
    fn the_oracle_reaches_no_further_into_the_engine() {
        // Of the engine, `Tree` and its `Violation`s only: what a delete took along, a folder's
        // part of the drive and what a filter leaves out are the oracle's own walks. (What it
        // reads of the model, through `Sim`, is the model's state, and the drive's own judgement:
        // whether it refuses requests and what it judged each (`Faults::refusing`, `judged`, by
        // `drive_refused`); it calls no engine rule through them.)
        let paths = |source: &str| -> Vec<String> {
            // Up to a test module (`lines` reads a Windows checkout's line ends too).
            source
                .lines()
                .take_while(|l| *l != "mod tests {")
                .map(str::trim)
                .filter(|l| !l.starts_with("//"))
                .filter(|l| {
                    ["hbsync_engine", "crate::", "super::"]
                        .iter()
                        .any(|p| l.contains(p))
                })
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(
            paths(include_str!("tagbook.rs")),
            ["use hbsync_engine::tree::Tree;"],
            "the tag bookkeeping's paths into the engine and the crate"
        );
        assert_eq!(
            paths(include_str!("oracle.rs")),
            [
                "use hbsync_engine::tree::{Tree, Violation};",
                "use crate::Sim;",
                "use crate::tagbook::TagBook;",
            ],
            "the oracle's paths into the engine and the crate"
        );
    }
}
