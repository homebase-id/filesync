//! A client dying between putting an item in place and its commit, and the recovery at the
//! start of its next sync.

use std::collections::HashSet;

use hbsync_engine::Against;
use hbsync_engine::plan::TagWrite;
use hbsync_engine::reconcile::{DriveVersion, Op};
use hbsync_platform::Os;
use hbsync_types::{NodeId, Tags};

use crate::Sim;
use crate::exec::Outcome;
use crate::model::client::{rekey_client, verdict};
use crate::model::server::time;

impl Sim {
    /// Client `c` dies during its next step ([`Event::Crash`]): a download it fetched, or a
    /// folder synced before (the engine's write-ahead record), is put in place and its commit
    /// lost; anything else isn't started.
    ///
    /// [`Event::Crash`]: crate::Event::Crash
    pub(crate) fn crash(&mut self, c: usize) {
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
                        self.placed_back(c, node);
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
        self.placed_back(c, *node);
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
        self.note(|_| format!("crash c{c}: {node:?} put in place, commit lost"));
    }

    /// `op` of client `c` put its item in place from `record`, and its commit failed (the run
    /// goes on, the operation ends): the write-ahead record stays, for the next plan's recovery,
    /// with the tags its write left (`None`: not known) and what was there before.
    pub(crate) fn keep_record(
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
        self.placed_back(c, *node);
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
        self.note(|_| format!("  commit failed c{c}: {node:?} put in place, record kept"));
    }

    /// What the engine's recovery does at the start of a sync for the downloads and folders put
    /// in place whose commit was lost: the item still at its place with the fetched bytes is the
    /// node,
    /// settled as the commit would have (the same rule, against the same version, the item
    /// read with the tags it was put in place with).
    pub(crate) fn recover(&mut self, c: usize) {
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
            self.note(|sim| {
                let local = sim.clients[c].local.get(node);
                format!("recover c{c} {node:?} there={there} {local:?}")
            });
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
                let new = self.ids.fresh();
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
    pub(crate) fn record_there(&self, c: usize, node: NodeId, p: &Placement) -> bool {
        self.clients[c].local.get(node).is_some_and(|l| {
            l.parent == p.parent && l.name == p.name && l.content == p.record.state.content
        })
    }

    /// Settles record `p`, its item there, as the commit would (the engine's
    /// `settle_placement`, recovery's and a retry's alike): where this client's mirror no
    /// longer has its node ([`Sim::gone_for`]), at the record's version whatever the plan saw,
    /// so the removal reaches the item. Returns whether it did: not where the node is gone and
    /// the item isn't one the drive takes (the record stays).
    pub(crate) fn settle_record(&mut self, c: usize, node: NodeId, p: &Placement) -> bool {
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
}

/// A download or folder put in place whose commit a crash lost: its record, where it went, and
/// the tags its write left on the item there (`None`: not known), and `prior`. `unhashed`: the next sync's
/// scan doesn't hash the file yet (the engine's scan may leave that for later): recovery keeps
/// the record for the sync after, and the plan defers the node meanwhile.
#[derive(Debug, Clone)]
pub(crate) struct Placement {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Action, Event, act};
    use hbsync_types::{NodeKind, NodeState};

    /// A folder never synced, made again where a user's delete of it lost (the drive has it),
    /// and the client dies before its commit: it is back all the same (no longer deleted), as a
    /// step that commits.
    #[test]
    fn a_folder_made_back_without_a_record_is_no_longer_deleted() {
        let mut sim = Sim::new(1);
        let node = sim.ids.fresh();
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
        let node = sim.ids.fresh();
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
        let node = sim.ids.fresh();
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
}
