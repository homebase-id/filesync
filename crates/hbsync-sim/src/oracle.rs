//! The oracle: the [`Ledger`] and [`Sim::check`] (convergence, invariants, no loss).

use std::collections::{BTreeSet, HashMap};

use hbsync_engine::mask::masked;
use hbsync_engine::tree::{Tree, Violation};
use hbsync_platform::name::check_remote_name;
use hbsync_types::{ContentHash, FileContent, NodeId, Role, Tags, VersionTag};

use crate::Sim;
use crate::exec::see_held_tags;

/// The oracle's bookkeeping: every blob written and the ones no longer owed, the last listing
/// of Recently deleted, and the first breach of each rule the model saw happen (each kept
/// once: the check behind it runs only while none is). The model feeds it through methods
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
    /// The first folder a plan removed after something in it (node schema, Recently deleted:
    /// what a folder's delete takes along is removed no earlier than the folder).
    removed_after_contents: Option<String>,
    /// The first node emptying or the purge deleted for good that wasn't a removed row chosen
    /// at that version (a live node, say), or a folder holding anything not going after it.
    emptied_unlisted: Option<String>,
    /// The first item emptied on its own that stayed though nothing kept it.
    emptied_kept: Option<String>,
}

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

    /// A user listed Recently deleted: `rows`, each node at its version.
    pub(crate) fn deleted_listed(&mut self, rows: BTreeSet<(NodeId, VersionTag)>) {
        self.listed = Some(rows);
    }

    /// The last listing of Recently deleted, if a user made one: what emptying it as listed
    /// acts on (the model reads it here), and what the emptying is judged against.
    pub(crate) fn listing(&self) -> Option<&BTreeSet<(NodeId, VersionTag)>> {
        self.listed.as_ref()
    }

    /// A plan deleted more than the guard allows without a confirmation, if `what` names one.
    pub(crate) fn unconfirmed_mass_delete(&mut self, what: impl FnOnce() -> Option<String>) {
        first(&mut self.unguarded, what);
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
}

impl Sim {
    /// Checks convergence, invariants and no data loss. Returns a description of the first
    /// failure.
    pub fn check(&self) -> Result<(), String> {
        let ledger = &self.ledger;
        if let Some(op) = &ledger.dangling {
            return Err(format!("created under a folder the server never had: {op}"));
        }
        if let Some(plan) = &ledger.unguarded {
            return Err(format!("a mass delete went through unconfirmed: {plan}"));
        }
        if let Some(gone) = &self.gone_while_there {
            return Err(gone.clone());
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
        // Items the drive refuses as they are (with the name it refuses), with what is in them.
        let mut refused_here: BTreeSet<NodeId> = BTreeSet::new();
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
                    .and_then(|c| ledger.blobs.get(&c.hash))
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
            let parked = client.parked.keys().copied().filter(|_| self.refusing);
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
            if view(&base) != server_view {
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
            // A cycle whose repair (a move out of it) the drive refused stays one.
            .filter(|v| {
                !matches!(v, Violation::Cycle { node }
                    if cycle_of(&server, *node).iter().any(|n| refused_here.contains(n)))
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
        // No folder wrote outside its part of the drive.
        if let Some(op) = &ledger.scope_breach {
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
                && !refused_here.contains(node)
                && !self.tags_removed.contains(&(*node, name.clone()))
                && (!self.deleted.contains(node) || held_at_removal(node, name))
                && s.tags.get(name).is_none()
            {
                return Err(format!("lost tag {name:?} of {node:?}: {:?}", s.tags));
            }
        }
        // A synced tag a client removed stays removed (§11), unless another client added it
        // and the add wasn't on the drive yet when it was removed (not sent up then: the
        // removal, sent up after an add on the drive, wins), or holds it from a download whose
        // record recovery dropped (`tags_unproven`, the known limit). One removed before a
        // delete was settled by it (`deleted_now`, `restored_on_drive`).
        for ((by, node, name), removed) in &self.tags_cleared {
            let elsewhere = |(c, n, t): &(usize, NodeId, String)| c != by && n == node && t == name;
            let added_elsewhere = self.tags_added.iter().any(|add| {
                elsewhere(add) && self.tags_sent.get(add).is_none_or(|sent| sent > removed)
            }) || self
                .tags_forked
                .iter()
                .chain(&self.tags_unproven)
                .any(elsewhere);
            if let Some(s) = server.get(*node)
                && !added_elsewhere
                && !refused_here.contains(node)
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

/// The folders of the cycle `node` is in, going up from it in `tree`.
fn cycle_of(tree: &Tree, node: NodeId) -> Vec<NodeId> {
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
    use crate::action::{Action, act};
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

    /// The oracle trusts the engine no more than it does now: no path into the engine, nor into
    /// the rest of the crate (the model and `exec` mirror the engine), but these `use` lines.
    #[test]
    fn the_oracle_reaches_no_further_into_the_engine() {
        // TODO(B5.1): with the oracle's own take-along and filter walk, only `tree::Tree`. It
        // also leans on the engine through `see_held_tags` and through `Sim` methods it calls
        // (`server_view`, `Server::taken_along`, `Client::scope`): R6.
        const ALLOWED: &[&str] = &[
            "use hbsync_engine::mask::masked;",
            "use hbsync_engine::tree::{Tree, Violation};",
            "use crate::Sim;",
            "use crate::exec::see_held_tags;",
        ];
        // Up to this module (`lines` reads a Windows checkout's line ends too).
        let source = include_str!("oracle.rs").lines();
        let paths: Vec<&str> = source
            .take_while(|l| *l != "mod tests {")
            .map(str::trim)
            .filter(|l| !l.starts_with("//"))
            .filter(|l| {
                ["hbsync_engine", "crate::", "super::"]
                    .iter()
                    .any(|p| l.contains(p))
            })
            .collect();
        assert_eq!(
            paths, ALLOWED,
            "the oracle's paths into the engine and the crate"
        );
    }
}
