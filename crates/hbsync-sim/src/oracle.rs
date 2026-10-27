//! The oracle: the [`Ledger`], its checks of each plan ([`Ledger::plan_made`]), and
//! [`Sim::check`] (convergence, invariants, no loss). It reads the engine's [`Tree`] only.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;

use hbsync_engine::tree::{Tree, Violation};
use hbsync_local::Filter;
use hbsync_platform::Os;
use hbsync_platform::name::check_remote_name;
use hbsync_types::{
    ContentHash, FileContent, NodeId, NodeKind, NodeState, RECOVERED_NAME, Role, Tags, VersionTag,
};

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
    /// How many times files' bytes went up hashed on send (sync semantics §3).
    pub(crate) sent_on_send: usize,
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
    /// The first node a put back left held again though nothing of it waits in Recently
    /// deleted any more ([`Sim::with_failing_put_backs`]).
    held_after_put_back: Option<String>,
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

    /// A plan right after a put back held a node again that nothing waits for, if `what`
    /// says which.
    pub(crate) fn held_after_put_back(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.held_after_put_back, what);
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
    /// each live node's folder and version, `folder` whether a node is a folder: a file, live
    /// or removed, takes nothing along (what is in it is an orphan for Recovered files).
    pub(crate) fn taken_along(
        &self,
        live: &BTreeMap<NodeId, (NodeId, VersionTag)>,
        removed: impl Fn(NodeId) -> bool,
        folder: impl Fn(NodeId) -> bool,
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
                if !folder(parent) {
                    break None;
                }
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
        // The marker of a file hashed on send is the planner's alone: never stored, nor sent.
        let marked = |s: &NodeState| s.content.is_some_and(|c| c.hash.is_on_send());
        for (c, client) in self.clients.iter().enumerate() {
            let trees = [&client.local, &client.base, &client.planned_remote];
            if let Some((n, _)) = trees.iter().flat_map(|t| t.iter()).find(|(_, s)| marked(s)) {
                return Err(format!("c{c}: {n:?} holds the hash-on-send marker"));
            }
            if let Some(e) = client
                .extra
                .iter()
                .find(|(_, e)| e.local_hash.is_some_and(|h| h.is_on_send()))
            {
                return Err(format!(
                    "c{c}: {:?}'s base holds the hash-on-send marker",
                    e.0
                ));
            }
        }
        if let Some((n, _)) = self.server.tree().iter().find(|(_, s)| marked(s)) {
            return Err(format!("the drive holds the hash-on-send marker in {n:?}"));
        }
        if let Some(what) = &ledger.narrow_differs {
            return Err(what.clone());
        }
        if let Some(what) = &ledger.held_after_put_back {
            return Err(format!("after a put back: {what}"));
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
        // Orphans some client leaves, its place there one it doesn't sync (§6.3, §10).
        let mut left_somewhere: BTreeSet<NodeId> = BTreeSet::new();
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
            let (mut synced, left) = self.expected(c, &drive, &taken);
            synced_somewhere.extend(synced.ids());
            left_somewhere.extend(left);
            // What the client blocks for a refusal: each operation refused itself is one the
            // drive refuses as it is now (by its own judgement: a record that blocked one it
            // takes hid a later change); it stays on its client with what is in it, and
            // anything else converges.
            // Once nothing more happens, what the last plan still sent was tried and is blocked
            // behind one (an operation that waits for a refused one stays: a local cost, told
            // when it keeps coming back): it, too, stays as it is, unhidden.
            let mut base = client.base.clone();
            for (op, refused) in &client.blocked {
                if *refused && !self.drive_refused(c, op) {
                    return Err(format!(
                        "{}: the refusal record hid {op:?}, which the drive takes as it is",
                        client.name
                    ));
                }
            }
            // A parked item waits for its name, which a folder held back may keep.
            let left = client.blocked.iter().map(|(op, _)| op.node());
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
            // One left by a client whose place there it doesn't sync, and synced by none, stays
            // (§6.3, §10).
            .filter(|v| {
                !matches!(v, Violation::Orphan { node, .. }
                    if left_somewhere.contains(node) && !synced_somewhere.contains(node))
            })
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
        let folder = |n: NodeId| {
            self.server
                .node(n)
                .is_some_and(|(s, _)| s.state.kind == NodeKind::Folder)
        };
        self.ledger
            .taken_along(&live, |f| self.server.removed.contains_key(&f), folder)
    }

    /// What client `c` syncs of the drive, in its folder's coordinates, by the oracle's own
    /// walks: the live nodes in its folder's part (the main folder's: all but the role folders
    /// and what is in them; a role folder's: what is in it), but for what a delete took along
    /// (`taken`) and what its filter leaves out; with the orphans its engine leaves to other
    /// clients ([`Sim::left_orphans`]), out of it.
    fn expected(
        &self,
        c: usize,
        drive: &Tree,
        taken: &BTreeSet<NodeId>,
    ) -> (Tree, BTreeSet<NodeId>) {
        let coords = self.clients[c].coords;
        // A folder no client reads, while it is on the drive, and what is in it: what is in
        // it waits for it (`Action::NewerFolder`).
        let unread: BTreeSet<NodeId> = self.server.unreadable_on_drive().map(|(n, _)| *n).collect();
        // A node's place goes through the folders in Recently deleted above it too (sync
        // semantics §12): what is in one whose chain reaches a role folder is that folder's,
        // never the main folder's, even where no device brings it back (the role folder's
        // client then leaves it, `left_orphans`).
        let removed = |n: &NodeId| self.server.removed.get(n).map(|r| r.state.parent);
        let mut tree = Tree::new();
        for (node, n) in &self.server.live {
            let mut up = up_from(drive, *node);
            while let Some(&last) = up.last()
                && let Some(parent) = removed(&last)
                && !up.contains(&parent)
            {
                up.push(parent);
                up.extend(up_from(drive, parent).into_iter().skip(1));
            }
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
        let left = self.left_orphans(c, &tree, drive);
        for n in &left {
            tree.remove(*n);
        }
        (tree, left)
    }

    /// What of `tree` (what client `c` syncs of the drive) its engine leaves to other clients
    /// (sync semantics §6.3, §10): an orphan (its folder nowhere, or a file) it holds nothing
    /// of, whose place it doesn't sync, in Recovered files or at its top, with what is below
    /// it. (What is below a recovered one at an ignored name is connected once it is, and
    /// filtered as anything.)
    fn left_orphans(&self, c: usize, tree: &Tree, drive: &Tree) -> BTreeSet<NodeId> {
        let client = &self.clients[c];
        // What the client's scan reports: not what it ignores, kept on its device only.
        let ignored: BTreeSet<NodeId> = filtered(&client.local, &client.filter)
            .into_iter()
            .collect();
        let held = |n: &NodeId| {
            client.base.contains(*n) || (client.local.contains(*n) && !ignored.contains(n))
        };
        let names = |path: &[&str], is_dir| client.filter.names(path.iter().copied(), is_dir);
        // Where an orphan goes, as the engine's `recovered_path` finds it: Recovered files
        // where the tree has it; masked on the drive, the top; live but out of the tree
        // otherwise (taken along, waiting, in a role folder), or in Recently deleted with its
        // folder out of the tree, nowhere known: every orphan left; made here if the client
        // syncs its name, else the top; at a role folder's top, the top.
        let removed = self.server.removed.get(&NodeId::RECOVERED);
        let recovered: Option<Vec<&str>> = match tree.path(NodeId::RECOVERED) {
            _ if !client.coords.top.is_root() => Some(Vec::new()),
            Some(path) => Some(path),
            None if drive.contains(NodeId::RECOVERED) => drive
                .path(NodeId::RECOVERED)
                .filter(|path| !names(path, true).is_sync())
                .map(|_| Vec::new()),
            None => match removed {
                Some(r) => tree.path(r.state.parent).map(|mut path| {
                    path.push(&r.state.name);
                    path
                }),
                None if names(&[RECOVERED_NAME], true).is_sync() => Some(vec![RECOVERED_NAME]),
                None => Some(Vec::new()),
            },
        };
        let mut left = BTreeSet::new();
        for (node, state) in tree.iter() {
            let orphan = !state.parent.is_root()
                && tree
                    .get(state.parent)
                    .is_none_or(|p| p.kind == NodeKind::File);
            if !orphan {
                continue;
            }
            let subtree = below(tree, node);
            if subtree.iter().any(held) {
                continue;
            }
            // In a folder in Recently deleted at a place the client filters (masked): another
            // device's to bring back with it, no orphan (§6.3, §10).
            if let Some(parent) = self.server.removed.get(&state.parent)
                && let Some(path) = self.through_removed(c, state.parent)
                && !names(&path, parent.state.kind == NodeKind::Folder).is_sync()
            {
                left.extend(subtree);
                continue;
            }
            let synced = recovered.as_ref().is_some_and(|place| {
                let mut path = place.clone();
                path.push(&state.name);
                names(&path, state.kind == NodeKind::Folder).is_sync()
            });
            if !synced {
                left.extend(subtree);
            }
        }
        left
    }

    /// The names from client `c`'s top down to `folder`, through live folders and folders in
    /// Recently deleted: `None` where the chain doesn't reach the top.
    fn through_removed(&self, c: usize, folder: NodeId) -> Option<Vec<&str>> {
        let top = self.clients[c].coords.top;
        let mut names = Vec::new();
        let mut at = folder;
        while at != top {
            let state = match self.server.removed.get(&at) {
                Some(r) => &r.state,
                None => &self.server.live.get(&at)?.state,
            };
            names.push(state.name.as_str());
            at = state.parent;
            if names.len() > self.server.live.len() + self.server.removed.len() {
                return None; // a cycle
            }
        }
        names.reverse();
        Some(names)
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
    LocalDelete {
        subtree: Vec<NodeId>,
    },
    /// Another operation that deletes (a forget with something here).
    DeletesHere,
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
    /// The synced files gone here and gone there, unconfirmed, in the whole dirty set, and of
    /// them those in the rest of it (another page's, in pages): the guard weighs the whole
    /// delete (§7), a file gone on both sides too.
    pub(crate) pending: (usize, usize),
    pub(crate) outside: (usize, usize),
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
        // item is a new one here, waiting where it is here only, under its new id.
        let out_of_view: HashSet<NodeId> = filtered(remote, seen.filter).into_iter().collect();
        let mut waiting: HashSet<NodeId> = HashSet::new();
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
                    && waiting.insert(s.parent)
                {
                    // Reached here, a detached folder is its new item, here only; reached
                    // through the drive or the base, its old node, on every side.
                    let here = i == 0;
                    queue.push((s.parent, !(here && detached.contains_key(&s.parent))));
                }
            }
        }
        // A detached folder waits under both its ids.
        let waiting: HashSet<NodeId> = waiting
            .iter()
            .filter_map(|n| detached.get(n).copied())
            .chain(waiting.iter().copied())
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
                        || (waiting.contains(&node)
                            && match &p.what {
                                What::RemoteDelete
                                | What::LocalDelete { .. }
                                | What::DeletesHere => true,
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
            // Remote deletes of files, and of them those synced here (a synced file gone here: the
            // guard's pending counts those over the whole dirty set; one not synced here, as what
            // a folder removed elsewhere left on the drive, it weighs in its plan only).
            let (mut deleted, mut deleted_synced, mut deleted_here) = (0, 0, 0);
            // What was in a confirmed folder whose delete waits for a hash is confirmed: one
            // synced, gone on a side (or taken along by a folder gone on the drive), holding a
            // file here not hashed yet on any side.
            let waits: HashSet<NodeId> = confirmed
                .iter()
                .copied()
                .filter(|f| {
                    let gone =
                        !local.contains(*f) || remote.path(*f).is_none() || out_of_view.contains(f);
                    base.contains(*f) && gone && waiting.contains(f)
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
                        deleted_synced += usize::from(base.contains(p.node));
                    }
                    What::LocalDelete { .. } => deleted_here += files_under(local, p.node),
                    _ => {}
                }
            }
            // Removals from elsewhere are followed, but every synced file over the limit.
            let every = |n: usize| synced_files > 0 && n >= synced_files && n > min;
            // Held before (sticky till answered) isn't judged again.
            let was_wiped = |n: NodeId| {
                wiped.contains(&n) || base.ancestors(n).iter().any(|a| wiped.contains(a))
            };
            let held_deleted_here: usize = seen
                .held
                .iter()
                .map(|p| match &p.what {
                    // Nor a delete taking along what was held so (held with it, §7).
                    What::LocalDelete { subtree }
                        if !was_wiped(p.node) && !subtree.iter().any(|n| wiped.contains(n)) =>
                    {
                        files_under(local, p.node)
                    }
                    _ => 0,
                })
                .sum();
            // And what was held so stays held till answered.
            if let Some(p) = seen.ops.iter().filter(unconfirmed).find(|p| {
                matches!(&p.what, What::LocalDelete { subtree }
                        if was_wiped(p.node) || subtree.iter().any(|n| wiped.contains(n)))
            }) {
                return Some(format!(
                    "{} deletes what was held as every synced file, unanswered: {:?}",
                    seen.client, p.op
                ));
            }
            // Judged with what is held so already: the engine weighs all its removals elsewhere
            // together (a new one held with a held wipe as every synced file).
            let held_all: usize = seen
                .held
                .iter()
                .map(|p| match p.what {
                    What::LocalDelete { .. } => files_under(local, p.node),
                    _ => 0,
                })
                .sum();
            // In pages, with what other pages have to delete; all of it, as the dirty set has
            // it (what the drive no longer has: removed every synced file).
            let (here_elsewhere, there_elsewhere) = seen.outside;
            let there_all = seen
                .pending
                .1
                .max(held_all + deleted_here + there_elsewhere);
            let held_elsewhere = held_deleted_here > 0 && !every(there_all);
            // And what isn't held is judged with what is: one let through that makes every
            // synced file with a held wipe should have been held with it.
            if deleted_here > 0 && held_all > held_deleted_here && every(there_all) {
                return Some(format!(
                    "{} let {deleted_here} removals from elsewhere through that, with the {held_all}                      held, are every synced file ({synced_files})",
                    seen.client
                ));
            }
            if held_elsewhere {
                return Some(format!(
                    "{} held {held_deleted_here} removals from elsewhere ({held_all} with what was \
                     held before), short of every synced file ({synced_files}): {:?}",
                    seen.client, seen.all_held
                ));
            }
            // Gone here over the whole dirty set: the guard's pending count, or the page's synced
            // deletes with the other pages' (one a folder's removal took along isn't marked
            // dirty); and the page's own deletes, synced or not.
            let here_all = (seen.pending.0)
                .max(deleted_synced + here_elsewhere)
                .max(deleted);
            let tripped = deleted > 0 && trips(here_all);
            (tripped || (deleted_here > 0 && every(there_all))).then(|| {
                format!(
                    "{} deletes {deleted} remote and deletes {deleted_here} local files \
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

    /// A plan that removes a file from its folder and then the folder is reported: restored,
    /// the folder comes back without it. By the folder the file was in when the plan removed
    /// it, whatever the drive did with it since.
    #[test]
    fn a_folder_removed_after_what_is_in_it_is_reported() {
        let mut sim = Sim::new(1);
        sim.run(&[act(0, Action::CreateFolder { parent: 0, name: 1 })]);
        let folder = sim.item(0, "b").expect("b");
        let parent = sim.pick(0, folder, true).expect("b");
        sim.run(&[act(0, Action::CreateFile { parent, name: 0 })]);
        assert!(sim.quiesce(10));
        let file = sim.item(0, "b/a").expect("b/a");
        // The user deletes b; the plan's removals go wrong way round.
        let node = sim.pick(0, folder, false).expect("b");
        sim.run(&[act(0, Action::Delete { node })]);
        let delete = |sim: &Sim, node: NodeId, parent: NodeId| {
            let remote = sim.server.live[&node].remote;
            hbsync_engine::reconcile::Op::RemoteDelete {
                node,
                parent,
                file_id: remote.file_id,
                expected: remote.version,
            }
        };
        let op = delete(&sim, file, folder);
        sim.execute(0, &op);
        let op = delete(&sim, folder, NodeId::ROOT);
        sim.execute(0, &op);
        assert!(sim.quiesce(10));
        let wrong = sim.check().expect_err("the folder after what was in it");
        assert!(
            wrong.contains("a folder was removed after what is in it"),
            "{wrong}"
        );
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
