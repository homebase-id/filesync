//! Everything that mirrors the engine: planning, the executor's steps and guards, the trash,
//! unparking and settling. Each function calls the engine or says what of it it copies, but
//! for the scope check (`breaches`, `planned_writable`), which the oracle records: it reads the
//! engine's `Op`, which the oracle may not import.

use std::collections::{BTreeSet, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_engine::Against;
use hbsync_engine::mask::masked;
use hbsync_engine::pages::subtree_any_side;
use hbsync_engine::plan::{Acts, Met, TagWrite, not_reached, tags_written};
use hbsync_engine::reconcile::{
    Agreement, BaseExtra, DeleteGuard, DriveVersion, Guarding, Input, Masking, Op, Options,
    Removed, reconcile_with_known,
};
use hbsync_engine::tree::{Tree, first_free, numbered_names};
use hbsync_local::Verdict;
use hbsync_local::layout::parked_name;
use hbsync_platform::Os;
use hbsync_state::{Answer, RemovedRow, StateDb, StateError};
use hbsync_types::{
    ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Role, Tags, VersionTag,
};

use crate::Sim;
use crate::action::NAMES;
use crate::model::client::{Client, rekey_client, verdict, verdict_in};
use crate::model::server::{ServerNode, time};
use crate::oracle::{PlanSeen, Planned, What, view_with};

impl Sim {
    /// The version a folder made here by `op` of client `c` acts on, which a folder synced
    /// before keeps a write-ahead record of (the executor's): the plan's, else the mirror's,
    /// else, nowhere live, the one in Recently deleted it is made again from (for recovery
    /// only: its commit keeps its base).
    pub(crate) fn mkdir_acts_on(&self, c: usize, node: NodeId, op: &Op) -> Option<DriveVersion> {
        if let Some(d) = op.drive() {
            return Some(d.clone());
        }
        let client = &self.clients[c];
        if !client.base.contains(node) {
            return None;
        }
        let base_at = client.extra.get(&node).and_then(|e| e.version);
        if let Some(state) = client.planned_remote.get(node) {
            return Some(DriveVersion {
                state: state.clone(),
                at: *client.planned_refs.get(&node)?,
                base_at,
            });
        }
        let coords = client.coords;
        self.server.removed.get(&node).map(|r| DriveVersion {
            state: NodeState {
                parent: coords.from_drive(r.state.parent),
                ..r.state.clone()
            },
            at: r.remote,
            base_at,
        })
    }

    /// In pages ([`Sim::with_small_pages`]): client `c`'s plan ran out: its next pages are
    /// planned, as the engine's next rounds (into the next sweep: a sync goes on till a sweep
    /// does nothing), till one has work or a whole sweep's worth had none. Whether one had.
    pub(crate) fn next_page_planned(&mut self, c: usize) -> bool {
        let Some(size) = self.faults.page_size else {
            return false;
        };
        let client = &self.clients[c];
        let nodes = client.base.len() + client.local.len() + self.server.live.len();
        // A sweep's worth of pages, and what they wait for, each a page of its own (a chain of
        // waits grows a page at a time; bounded: each such page is larger).
        let mut planned = 0;
        while planned < nodes / size + 2
            || (planned < 2 * nodes + 2 && !self.clients[c].pages.borrow().own.is_empty())
        {
            planned += 1;
            self.plan(c);
            if !self.clients[c].plan.is_empty() {
                return true;
            }
        }
        false
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

    /// Client `c` starts a sync: recovers, unparks, and plans from the server as its folder sees
    /// it, as the engine's sync does (its view's input, then `reconcile_with_known`).
    pub(crate) fn plan(&mut self, c: usize) {
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
        self.hear_not_theres(c);
        let remote = self.drive_seen(c);
        let client = &self.clients[c];
        // A parked node is reported at its original place ([`at_place`]): the temporary name
        // is the executor's business and must never reach the remote.
        let mut local_view = client.local.clone();
        for &node in client.parked.keys() {
            if let Some(state) = client.local.get(node) {
                local_view.insert(node, at_place(client, node, state));
            }
        }
        // The scanner never reports filtered items (judged by their names on disk).
        for n in masked(&client.local, &client.filter) {
            local_view.remove(n);
        }
        // A file not hashed yet is reported without content: the plan defers it.
        for n in client.unhashed_now() {
            if let Some(state) = local_view.get(n).cloned() {
                local_view.insert(
                    n,
                    NodeState {
                        content: None,
                        ..state
                    },
                );
            }
        }
        // What the client can't hold of the tags is the drive's, as the engine's view has it.
        see_held_tags(client, &mut local_view, &remote);
        // Of the items the scan reports (as the engine's view marks its rows).
        let local_unhashed = client
            .unhashed_now()
            .filter(|n| local_view.contains(*n))
            .collect();
        let mut input = Input {
            base: client.base.clone(),
            local: local_view,
            remote: remote.clone(),
            base_extra: client.extra.clone(),
            remote_complete: true,
            local_unhashed,
            ..Default::default()
        };
        // On the drive, in a version no client reads ([`Action::NewerFolder`]).
        input.unreadable = self
            .server
            .unreadable_on_drive()
            .map(|(n, _)| client.coords.from_drive(*n))
            .collect();
        input.remote_refs = self
            .server
            .live
            .iter()
            .filter(|(id, n)| {
                !client.coords.hides(**id) && !client.gone.contains(&n.remote.file_id)
            })
            .map(|(id, n)| (*id, n.remote))
            .collect();
        // The folders in Recently deleted that live nodes are in, and the removed ones above
        // them, and the server's time of each live node's version, as the engine's view has them.
        let outside = client.scope().outside;
        for mut folder in hbsync_engine::view::looked_up_removed(&remote, client.coords.top) {
            // Not one in a version no client reads: the feed records no row of it (only that
            // it is there, unread), in Recently deleted or not.
            while !input.removed.contains_key(&folder)
                && !outside.contains(&folder)
                && !self
                    .server
                    .unreadable
                    .contains_key(&client.coords.to_drive(folder))
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
        // In pages, as the engine: the next page of the sweep over what is dirty (what differs
        // between the base and a side, as the engine's scan and feed mark them), what else is
        // to plan held as it is.
        let mut own_page = false;
        if let Some(size) = self.faults.page_size {
            // What differs between the base and a side, the drive's as it is: what a folder's
            // removal took along isn't marked (the feed marks only the folder).
            let all: BTreeSet<NodeId> = std::mem::take(&mut input.dirty)
                .into_iter()
                .filter(|n| {
                    let base = input.base.get(*n);
                    base != input.local.get(*n) || base != input.remote.get(*n)
                })
                .collect();
            let page;
            (page, own_page) = next_page(client, &input, &all, size);
            input.dirty = page.iter().copied().collect();
            let scope = client.scope();
            let mut pending = SimPending {
                trees: [
                    input.base.clone(),
                    input.local.clone(),
                    input.remote.clone(),
                ],
                all: &all,
            };
            let Ok(()) = hbsync_engine::pages::dirty_elsewhere(
                &mut input,
                &client.filter,
                &scope,
                &mut pending,
            );
        }
        let confirmed = Arc::new(client.confirmed.clone());
        // Held as every synced file: until answered, by the engine's rule (it keeps
        // `guard.wiped` so), over what its store has: the mirror (the plan's view of the drive)
        // and what the scanner reports here.
        let sides = crate::world::ModelSides {
            base: &client.base,
            local: &input.local,
            live: &|n| remote.contains(n),
            server: &self.server,
            coords: client.coords,
        };
        let wiped: HashSet<NodeId> = client
            .wiped
            .iter()
            .copied()
            .filter(|n| {
                let Ok(keep) = hbsync_engine::held::keep_held(*n, confirmed.contains(n), &sides);
                keep
            })
            .collect();
        let synced_files = client
            .base
            .iter()
            .filter(|(_, s)| s.kind == NodeKind::File)
            .count();
        let scope = client.scope();
        let opts = Options {
            device: client.name.clone(),
            computer: client.computer.clone(),
            date: "2026-10-08".into(),
            guarding: Guarding {
                settings: DeleteGuard {
                    min: self.guard.unwrap_or_default(),
                    fraction: 0.0,
                    allow: self.guard.is_none(),
                },
                synced: synced_files,
                confirmed: Arc::clone(&confirmed),
                wiped: Arc::new(wiped.clone()),
                ..Guarding::default()
            },
            own_page,
            ..Options::default()
        };
        let masking = Masking {
            filter: &client.filter,
            scope: &scope,
        };
        // Ids the client or the server knows are taken, as in the executor's state DB.
        let known = |id: NodeId| {
            client.local.contains(id) || client.base.contains(id) || self.server.ever.contains(&id)
        };
        let plan = reconcile_with_known(&input, &opts, masking, known);
        if self.faults.page_size.is_some() {
            client
                .pages
                .borrow_mut()
                .own
                .add(&plan.waits_for_pages, &plan.waits_together);
        }
        // Every fourth plan (each costs a state DB and three more plans), until a difference
        // is found (it is reported).
        if self.faults.narrow_check && !self.ledger.found_narrow_differs() {
            self.faults.narrow_rounds += 1;
            if self.faults.narrow_rounds.is_multiple_of(4) {
                let round = self.faults.narrow_rounds / 4;
                let what = self.check_narrow(c, &input, &opts, &known, &plan, round);
                self.ledger.narrow_differs(|| what);
            }
        }
        if self.faults.held_check && !self.ledger.found_narrow_differs() {
            let what = self.check_held(c, &input, &opts, &known, &plan);
            self.ledger.narrow_differs(|| what);
        }
        // The files here not hashed yet (as the engine's view holds them: a row without a state
        // only waits), for the oracle's checks of the plan.
        let unhashed: HashSet<NodeId> = input
            .local_unhashed
            .iter()
            .copied()
            .filter(|n| input.local.contains(*n))
            .collect();
        let (ops, held_ops) = (planned(&plan.ops), planned(&plan.held));
        self.ledger.plan_made(
            &PlanSeen {
                client: &client.name,
                local: &input.local,
                base: &input.base,
                remote: &remote,
                filter: &client.filter,
                unhashed: &unhashed,
                ops: &ops,
                held: &held_ops,
                all_ops: &plan.ops,
                all_held: &plan.held,
                synced_files,
                confirmed: &confirmed,
                wiped: &wiped,
            },
            self.guard,
        );
        if !plan.ops.is_empty() {
            self.note(|sim| format!("plan {}: {:?}", sim.clients[c].name, plan.ops));
            self.note(|sim| {
                let (client, blobs) = (&sim.clients[c], sim.ledger.blobs());
                format!(
                    "  local:  {:?}\n  base:   {:?}\n  remote: {:?}",
                    view_with(&client.local, blobs),
                    view_with(&client.base, blobs),
                    view_with(&remote, blobs)
                )
            });
        }
        self.conflicts += plan.conflicts.len();
        // For the oracle: what a delete had taken along when this plan was made.
        let taken = self
            .taken_along()
            .into_iter()
            .map(|n| (n, self.server.live[&n].remote.version))
            .collect();
        self.ledger.planned_with(c, taken);
        // What the drive refused as it is isn't sent again (as `Engine::refused_out`).
        let (ops, refused) = if !self.faults.refusing {
            (plan.ops.clone(), Vec::new())
        } else {
            let client = &self.clients[c];
            let split = hbsync_engine::refused::take_refused(
                plan.ops.clone(),
                |op| {
                    Ok::<_, Infallible>(
                        client.refused.get(&(op.node(), op.kind())).is_some_and(
                            |(key, refused)| *refused && *key == refusal_key(client, op),
                        ),
                    )
                },
                |node| {
                    let local = client.local.get(node).map(|s| (s.parent, s.name.clone()));
                    let folders = [&client.local, &client.base, &remote]
                        .iter()
                        .filter_map(|t| t.get(node).map(|s| s.parent))
                        .collect();
                    Ok(hbsync_engine::refused::Place { local, folders })
                },
                client.os != Os::Linux,
            );
            let Ok(split) = split;
            split
        };
        let waiting: Vec<Op> = match self.faults.refusing {
            true => plan
                .ops
                .iter()
                .filter(|op| !ops.contains(op) && !refused.contains(op))
                .cloned()
                .collect(),
            false => Vec::new(),
        };
        let client = &mut self.clients[c];
        client.blocked = refused
            .into_iter()
            .map(|op| (op, true))
            .chain(waiting.into_iter().map(|op| (op, false)))
            .collect();
        // A confirmation lasts while the plan deletes its node, or its delete waits for a hash.
        let deleting: HashSet<NodeId> = plan
            .ops
            .iter()
            .filter_map(Op::deletes)
            .chain(plan.delete_waits.iter().copied())
            .collect();
        client.confirmed = confirmed.intersection(&deleting).copied().collect();
        client.held = plan.held.iter().map(Op::node).collect();
        client.wiped = wiped;
        client
            .wiped
            .extend(plan.held.iter().filter_map(|op| match op {
                Op::LocalTrash { node, .. } => Some(*node),
                _ => None,
            }));
        if self.faults.refusing {
            client.last_plan = ops.clone();
        }
        client.plan = ops.into_iter().map(|op| (op, 0)).collect();
        // Written by the next plan (hashed then): the scan asks for it.
        client.hashing = !client.writing.is_empty();
        client.writing.clear();
        client.removed_by_plan.clear();
        client.unremoved.clear();
        // A plan made since sends what the user does from now on.
        self.ledger.tags.planned(c);
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
            let new = self.ids.fresh();
            let client = &mut self.clients[c];
            client.parked.remove(&n);
            // Its row goes from the scan, and with it the mark of a hash still to read.
            client.unhashed.remove(&n);
            rekey_client(client, n, new);
            self.retrack(c, n, new);
            // The user's rule took it out of what this client syncs: its removal from the
            // drive (§10) is theirs.
            self.user_removed(c, n);
        }
    }

    /// Executes the next planned operation. Returns `false` if the plan is empty.
    pub(crate) fn step(&mut self, c: usize) -> bool {
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
        let local_before = self.clients[c].local.get(node).cloned();
        let before_tags = local_before.as_ref().map(|l| l.tags.clone());
        let fresh = local_before.is_none();
        // The version of its file a remote write replaces (none: a file it makes).
        let replaced = match self.clients[c].meanwhile {
            Some(_) => self.server.node(node).map(|(n, r)| (n.clone(), r)),
            None => None,
        };
        let outcome = self.execute(c, &op);
        // Checked and done here, not committed yet: another device's change of the node may
        // be pulled now ([`Action::PullMidLocal`]).
        if outcome == Outcome::Done
            && let Some((on, change)) = self.clients[c].pull_mid_local
            && pulled_on(&op, on)
        {
            self.clients[c].pull_mid_local = None;
            self.pull_mid_local(c, node, change);
        }
        let landed = outcome == Outcome::Landed;
        let remote_write = outcome == Outcome::Done && writes_drive(&op);
        let outcome = if landed { Outcome::Done } else { outcome };
        if outcome == Outcome::Done {
            self.clients[c].refused.remove(&(node, op.kind()));
            let name = &self.clients[c].name;
            self.ledger
                .wrote_outside_scope(|| breach.then(|| format!("{name} {op:?}")));
            let server = &self.server;
            self.ledger.dangling_write(|| {
                let parent_unknown = server.live.get(&node).is_some_and(|n| {
                    !n.state.parent.is_root() && !server.ever.contains(&n.state.parent)
                });
                parent_unknown.then(|| format!("c{c} {op:?}"))
            });
        }
        if outcome != Outcome::Done {
            self.note(|_| format!("  {outcome:?} {op:?}"));
        }
        let retried = outcome == Outcome::Retry && attempts < 3;
        if retried {
            self.clients[c].plan.push_back((op.clone(), attempts + 1));
        }
        // A folder's removal that ended without removing it holds back what is in it until
        // the next plan (the executor's `Schedule`).
        if matches!(op, Op::RemoteDelete { .. }) && outcome != Outcome::Done && !retried {
            self.clients[c].unremoved.insert(node);
        }
        if matches!(outcome, Outcome::Fetched | Outcome::Aside) {
            // Put in place and committed (a download), or checked and unlinked (a delete), by
            // the next step, whatever happens meanwhile.
            self.clients[c].plan.push_front((op, attempts));
            return true;
        }
        // Now and then a tag write fails (the item keeps what it held, a placed one holds none,
        // read so), or a placement's commit fails once its item is in place.
        let (mut tag_failed, mut commit_failed) = (false, false);
        let writes_tags = placement || matches!(op, Op::LocalTags { .. });
        if outcome == Outcome::Done && writes_tags && !landed {
            self.faults.writes += 1;
            let os = self.clients[c].os;
            // A folder made with none to write holds none, the write failing or not (the
            // executor's: it claims none).
            let nothing = matches!(&op, Op::LocalMkdir { tags, .. } if tags.held_by(os).is_empty());
            tag_failed = os != Os::Windows
                && !nothing
                && (std::mem::take(&mut self.faults.fail_tags_next)
                    || self.faults.writes % 13 == 7);
            let recorded = matches!(op, Op::LocalDownload { .. })
                || matches!(op, Op::LocalMkdir { drive: Some(_), .. }) && synced_before;
            commit_failed = recorded
                && (std::mem::take(&mut self.faults.fail_commit_next)
                    || self.faults.writes % 11 == 5);
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
                match local_before {
                    Some(state) => client.local.insert(node, state),
                    None => client.local.remove(node),
                };
                self.note(|_| format!("  tags not written, not placed: {op:?}"));
                if attempts < 3 {
                    self.clients[c].plan.push_back((op, attempts + 1));
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
                    // The item no longer holds what this client's user added to it: the add
                    // stays with the node (on the drive, or nowhere), not with the item (a
                    // conflict copy made of it later doesn't take it along).
                    self.ledger.tags.detach((c, node, tag.name.clone()));
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
                // Where the volume keeps no tags, the record claims none (the executor's
                // `placed_tags`), a folder's as a file's.
                let tags = match &op {
                    _ if tag_failed || windows => None,
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
                let now = self.server.now();
                for tag in tags.iter() {
                    self.ledger.tags.sent(c, node, &tag.name, now);
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
                self.placed_back(c, node);
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
                // Where the volume keeps no tags, the record claims none it wrote, and recovery
                // reads that so: not known (the executor's `placed_tags`, from its record).
                Op::LocalMkdir { .. } if windows => TagWrite::Unknown,
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
            // One synced here before, its tag write failed, keeps its base but for a tag that
            // didn't reach it and the drive's last version holds (the executor's).
            if let Op::LocalMkdir { drive: None, .. } = &op
                && tag_failed
                && self.server_state(c, node).is_none()
                && let Some(b) = self.clients[c].base.get(node).cloned()
                && let Some(held) = self.clients[c].local.get(node).map(|l| l.tags.clone())
            {
                let last = self.server.removed.get(&node).map(|r| &r.state.tags);
                let tags = not_reached(&b.tags, &held, last.unwrap_or(&b.tags));
                self.clients[c].base.insert(node, NodeState { tags, ..b });
            }
            let against = match &restored {
                Some(d) => Against::placed(Some(d)),
                None => against,
            };
            let at = self.settle_against(c, node, against, op.is_transfer() && !landed, tags, ours);
            if let Some(at) = at {
                self.clients[c].run_settled.insert(node, at);
            }
            if fresh && matches!(op, Op::LocalDownload { .. } | Op::LocalMkdir { .. }) {
                let from = restored.as_ref().or(op.drive());
                self.new_item_settled(c, node, from.map(|d| &d.state));
            }
            // Committed: the old version a download replaced goes, if still what it was.
            if matches!(op, Op::LocalDownload { .. }) && !self.faults.keep_replaced {
                self.unlink_replaced(c, node);
            }
        }
        if remote_write && let Some(pick) = self.clients[c].meanwhile.take() {
            self.meanwhile(c, node, replaced, pick);
            if let Op::RemoteTags { tags, .. }
            | Op::RemoteUpdate { tags, .. }
            | Op::RemoteCreate { tags, .. } = &op
            {
                self.ledger.tags.answer_superseded(c, node, tags);
            }
        }
        true
    }

    /// Client `c` settled `node`'s base once it put a new item there (none was there before),
    /// from `drive` (the version it came from, if known): a tag the base keeps that neither the
    /// item nor that version holds is the known limit (`Limit::NewItemKeepsBaseTag`).
    pub(crate) fn new_item_settled(&mut self, c: usize, node: NodeId, drive: Option<&NodeState>) {
        let client = &self.clients[c];
        let lacks =
            |s: Option<&NodeState>, name: &str| s.is_none_or(|s| s.tags.get(name).is_none());
        let item = client.local.get(node);
        let kept: Vec<String> = client
            .base
            .get(node)
            .iter()
            .flat_map(|b| b.tags.iter())
            .map(|t| t.name.clone())
            .filter(|name| lacks(item, name) && lacks(drive, name))
            .collect();
        for name in kept {
            self.ledger.tags.kept_on_new_item(c, node, &name);
        }
    }

    /// Another device changes `node` between client `c`'s local operation's check and its
    /// commit, and the account pulls it ([`Action::PullMidLocal`], `change`): edits it where
    /// it is a file (`change` not 2), else removes it, or restores it from Recently deleted.
    ///
    /// [`Action::PullMidLocal`]: crate::Action::PullMidLocal
    fn pull_mid_local(&mut self, c: usize, node: NodeId, change: usize) {
        if node == NodeId::RECOVERED || Role::of(node).is_some() {
            return;
        }
        if let Some(what) = self.drive_change(node, change % 3 != 2) {
            self.note(|_| format!("  pulled mid-commit of c{c} {node}: {what}"));
        }
    }

    /// Another device changes `node` on the drive: restores it where it is removed, else edits
    /// it where it is a file and `edit` says so, else removes it. What it did; `None` where the
    /// drive never had it.
    fn drive_change(&mut self, node: NodeId, edit: bool) -> Option<&'static str> {
        let (kind, removed) = self
            .server
            .node(node)
            .map(|(n, removed)| (n.state.kind, removed))?;
        Some(match (kind, removed) {
            (_, true) => {
                self.restore_with(node);
                "restored"
            }
            (NodeKind::File, false) if edit => {
                let version = VersionTag(self.server.uuid());
                self.edit_on_drive(node, version);
                "edited"
            }
            _ => {
                self.remove_live([node], true);
                "removed"
            }
        })
    }

    /// Another device changes `node`, which client `c`'s remote write over `replaced` (the
    /// node as the drive had it, removed or not; `None`: a node it made) just left on the
    /// drive, and the account pulls that change before the write's answer is recorded
    /// ([`Action::Meanwhile`], `pick`). The node has settled against the answer already (as the
    /// executor's settle does where a pull superseded it: as if the pull came after). The
    /// store's record of the answer ([`AccountTx::answered`]) is checked on a store holding the
    /// node as before the write: alone, the answer goes in; after the pull's change, that
    /// stays.
    ///
    /// [`Action::Meanwhile`]: crate::Action::Meanwhile
    /// [`AccountTx::answered`]: hbsync_state::AccountTx::answered
    fn meanwhile(
        &mut self,
        c: usize,
        node: NodeId,
        replaced: Option<(ServerNode, bool)>,
        pick: usize,
    ) {
        if node == NodeId::RECOVERED || Role::of(node).is_some() {
            return;
        }
        let Some(answer) = self.server.node(node).map(|(n, r)| (n.clone(), r)) else {
            return;
        };
        let alone = self.answer_lands(node, replaced.as_ref(), &answer, None);
        self.drive_change(node, pick % 2 == 1);
        self.note(|_| format!("  meanwhile {node}: removed {}, pick {pick}", answer.1));
        // What the pull recorded: the drive's node now.
        let Some(now) = self.server.node(node).map(|(n, r)| (n.clone(), r)) else {
            return;
        };
        let raced = self.answer_lands(node, replaced.as_ref(), &answer, Some(&now));
        let name = &self.clients[c].name;
        let held = |n: &(ServerNode, bool)| match n.1 {
            true => (None, Some(n.0.remote.version)),
            false => (Some(n.0.remote.version), None),
        };
        let (want_alone, want_raced) = (held(&answer), held(&now));
        self.ledger.answer_undid(|| match (alone, raced) {
            (Ok(alone), Ok(raced)) if alone == want_alone && raced == want_raced => None,
            got => Some(format!(
                "{name} {node}: the store holds {got:?} (alone, after the pull; live, removed), \
                 the drive {want_alone:?} then {want_raced:?}"
            )),
        });
    }

    /// Where a store holding `node` as `replaced` (live or removed; `None`: not at all), then as
    /// `pulled` (if a pull recorded that), has the node once the answer `answer` (the node as
    /// written, live or removed) is recorded over `replaced`: (its live version, its removed
    /// one). The store is left empty for the next.
    fn answer_lands(
        &mut self,
        node: NodeId,
        replaced: Option<&(ServerNode, bool)>,
        answer: &(ServerNode, bool),
        pulled: Option<&(ServerNode, bool)>,
    ) -> Result<(Option<VersionTag>, Option<VersionTag>), StateError> {
        let removed_at = |n: &ServerNode| Some(time(n.remote.version));
        let row = |n: &ServerNode| crate::narrow::mirror_row(node, &n.state, n.remote);
        let record =
            |s: hbsync_state::AccountTx<'_>, (n, removed): &(ServerNode, bool)| match removed {
                true => s.move_to_removed(&RemovedRow {
                    row: row(n),
                    removed_at: removed_at(n),
                }),
                false => s.put_live(&row(n)),
            };
        let expected = replaced.map(|(n, _)| n.remote.version);
        let answered = match answer.1 {
            true => Answer::removal(expected, row(&answer.0)),
            false => Answer::live(expected, row(&answer.0)),
        };
        let db = match &mut self.answers_db {
            Some(db) => db,
            empty => empty.insert(StateDb::open_in_memory()?),
        };
        db.write(|s| {
            let s = s.account();
            replaced
                .into_iter()
                .chain(pulled)
                .try_for_each(|n| record(s, n))?;
            s.answered(&answered).map(drop)
        })?;
        db.write(|s| {
            let picture = (
                s.get_mirror(node)?.map(|m| m.version),
                s.removed_by_node(node)?.map(|r| r.row.version),
            );
            // Gone for good long ago, then forgotten: nothing of it left.
            let s = s.account();
            s.forget_file(answer.0.remote.file_id, Some(0))?;
            s.prune_gone(1, 1)?;
            Ok(picture)
        })
    }

    /// Whether `op` of client `c` would write outside its folder's part of the drive: not a copy
    /// of the engine, the scope check [`Sim::step`] records for the oracle.
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
    pub(crate) fn execute(&mut self, c: usize, op: &Op) -> Outcome {
        use Outcome::{Done, Retry, Skip};
        let unchanged = |client: &Client, node: NodeId| {
            client.local.get(node) == client.planned_local.get(node)
        };
        // The executor's server-side order guards, against the client's mirror.
        match ready(&self.clients[c], op) {
            Met::Yes => {}
            Met::Later => return Retry,
            Met::NextPlan => return Skip,
        }
        // The drive refuses it as it is: recorded, the operation isn't sent again as it is (a
        // download once its bytes didn't match twice: the first time the drive is read again).
        if self.drive_refuses(c, op) {
            let key = refusal_key(&self.clients[c], op);
            let record = self.clients[c].refused.get(&(op.node(), op.kind()));
            let refused = !matches!(op, Op::LocalDownload { .. })
                || record.is_some_and(|(k, refused)| !refused && *k == key);
            self.clients[c]
                .refused
                .insert((op.node(), op.kind()), (key, refused));
            return Skip;
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
                self.ledger.tags.forked(c, node);
                self.retrack(c, node, new_node);
                Done
            }
            Op::LocalMkdir {
                node,
                parent,
                name,
                tags,
                drive,
            } => {
                let client = &mut self.clients[c];
                if client.local.contains(node) {
                    return Skip;
                }
                if !client.local.exists(parent) {
                    return Retry;
                }
                let tags = tags.held_by(client.os);
                // Made from Recently deleted, never synced here, with tags to write: restored,
                // changed or purged since the plan, the version there may no longer hold them
                // (the executor's).
                let holds =
                    |r: &ServerNode| tags.iter().all(|t| r.state.tags.get(&t.name) == Some(t));
                if drive.is_none()
                    && !client.base.contains(node)
                    && !tags.is_empty()
                    && !self.server.removed.get(&node).is_some_and(holds)
                {
                    return Skip;
                }
                if !park_occupant(client, parent, &name, None) {
                    return Retry;
                }
                client.local.insert(
                    node,
                    NodeState {
                        tags,
                        ..NodeState::folder(parent, name)
                    },
                );
                Done
            }
            Op::LocalTags { node, tags, drive } => {
                let remote = match drive {
                    Some(d) => d.base_at.is_none().then_some(d.state),
                    None => self.server_state(c, node),
                };
                let client = &mut self.clients[c];
                let Some(state) = client.local.get(node).cloned() else {
                    return Skip;
                };
                if !unchanged(client, node) {
                    return Skip;
                }
                // No base to record what it writes, and none it founds: it waits (the
                // executor's `tags_recorded`).
                let here = at_place(client, node, &state);
                let founds = |r: &NodeState| Agreement::of(&here, r, false).founds_base(false);
                if !client.base.contains(node)
                    && !(verdict(client, node).is_sync() && remote.as_ref().is_some_and(founds))
                {
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
                        // The bytes fetched are the server's current ones; they must be the
                        // mirror's (the executor checks the hash it expects).
                        let client = &self.clients[c];
                        let (Some(server), Some(mirror), Some(&mirror_at)) = (
                            self.server.live.get(&node),
                            client.planned_remote.get(node),
                            client.planned_refs.get(&node),
                        ) else {
                            return Skip;
                        };
                        let content = server.state.content;
                        if mirror.content != content {
                            return Skip;
                        }
                        // What it acts on, as the executor decides it: the version the plan
                        // saw while the mirror holds it, or the one this run's own write made
                        // of it since.
                        let ours = client.run_settled.get(&node).map(|v| RemoteRef {
                            version: *v,
                            ..mirror_at
                        });
                        let base_at = client
                            .extra
                            .get(&node)
                            .and_then(|e| e.version)
                            .filter(|_| client.base.contains(node));
                        let expected = content.map(|c| c.hash);
                        let acts = DriveVersion::acted_by_download(
                            op.drive(),
                            mirror_at,
                            expected,
                            ours,
                            base_at,
                        );
                        let record = match acts {
                            Acts::Plan(d) => d.clone(),
                            Acts::Mirror => DriveVersion {
                                state: mirror.clone(),
                                at: mirror_at,
                                base_at,
                            },
                            Acts::Stale => return Skip,
                        };
                        if self.faults.split_downloads {
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
                // An earlier version still aside waits for the next sync's recovery.
                if client.aside.contains_key(&node) {
                    return Skip;
                }
                // From another folder: the old version goes aside first (the executor's).
                self.replace_aside(c, node, parent, &name);
                let client = &mut self.clients[c];
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
                    let new_node = self.ids.fresh();
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
                    let new_node = self.ids.fresh();
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
                    let now = self.on_drive(node);
                    // Rightly so where a user deleted it as it is now, or a folder's removal had
                    // taken it along when the plan was made: anything else the engine removes no
                    // user wanted gone.
                    let version = self.server.live.get(&node).map(|n| n.remote.version);
                    let rightly = now.as_ref().is_some_and(|now| {
                        self.ledger.deleted_by_user(node, now)
                            || self.ledger.removed_by_lost_commit(c, node, now)
                    }) || version
                        .is_some_and(|v| self.ledger.plan_saw_taken(c, node, v));
                    if let Some(n) = self.server.live.remove(&node) {
                        self.remove_on_drive(node, n, rightly);
                        let client = &mut self.clients[c];
                        let after = client.removed_by_plan.iter().find(|r| {
                            self.server
                                .removed
                                .get(r)
                                .is_some_and(|n| n.state.parent == node)
                        });
                        self.ledger.folder_removed_after_contents(|| {
                            after.map(|r| format!("{} removed {node} after {r} in it", client.name))
                        });
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
                client.drop_base(node);
                self.retrack(c, node, new_node);
                Done
            }
            Op::Forget { node, subtree } => {
                if !subtree.is_empty() {
                    return self.trash(c, node, &subtree); // the bases go with it
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
                client.drop_base(node);
                Done
            }
        }
    }

    /// Deletes `node` and everything below it, as the executor does (sync semantics §7): only
    /// if the visible part is `expected` and unchanged since planning, with the bases of all of
    /// it. In two steps: the item is moved aside ([`Outcome::Aside`]), then, at the next step,
    /// checked again there and unlinked ([`Sim::finish_delete`]); anything else found there puts
    /// it back. Junk goes along; ignored items stay, and so do the folders they sit in, released
    /// (§10): those go item by item, at once.
    fn trash(&mut self, c: usize, node: NodeId, expected: &[NodeId]) -> Outcome {
        use Outcome::Skip;
        if self.clients[c].aside.contains_key(&node) {
            return self.finish_delete(c, node, expected);
        }
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
        // The rest goes aside: with nothing kept, the whole item, one rename; else each item
        // that goes, the released folders staying.
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
        let client = &mut self.clients[c];
        let mut aside = Tree::new();
        for n in gone {
            if let Some(state) = client.local.remove(n) {
                aside.insert(n, state);
            }
        }
        client.aside.insert(node, aside);
        Outcome::Aside
    }

    /// The second step of deleting `node` (moved aside by [`Sim::trash`]): what is aside is
    /// checked against what the plan saw ([`Sim::aside_check`]) and unlinked, with the bases of
    /// `expected`, and the folders that stayed (holding ignored items) are released; anything
    /// else aside puts it all back, for the next plan.
    fn finish_delete(&mut self, c: usize, node: NodeId, expected: &[NodeId]) -> Outcome {
        let Some(hidden) = self.aside_check(c, node, expected) else {
            self.note(|_| format!("  c{c} {node:?} isn't as planned aside: put back"));
            self.put_back_aside(c, node);
            return Outcome::Skip;
        };
        self.unlink_aside(c, node, &hidden, usize::MAX);
        // What stayed of what the plan saw: the folders holding ignored items. Released: a
        // temporary name (it carries the old id) gives way to a real one, deepest first, and
        // each takes a new id, for the scanner to find as a new folder.
        let client = &self.clients[c];
        let mut released: Vec<NodeId> = expected
            .iter()
            .copied()
            .filter(|n| client.local.contains(*n))
            .collect();
        released.sort_by_cached_key(|r| std::cmp::Reverse(client.local.ancestors(*r).len()));
        for &r in &released {
            let client = &mut self.clients[c];
            let (Some(state), Some((_, original))) =
                (client.local.get(r).cloned(), client.parked.get(&r).cloned())
            else {
                continue;
            };
            if state.name != parked_name(r) {
                continue;
            }
            if let Some(name) = release_name(client, r, state.parent, &original, true) {
                client.local.insert(r, NodeState { name, ..state });
            }
        }
        // What went isn't synced here any more: its bases go with it, whatever the drive holds
        // by now (the executor's; one left behind would read as a local delete once the node
        // is back in view). Of what it had rows of: what isn't filtered (an ignored item kept
        // here keeps its base).
        let client = &mut self.clients[c];
        for n in expected {
            client.drop_base(*n);
        }
        for r in released {
            let new = self.ids.fresh();
            let client = &mut self.clients[c];
            client.parked.remove(&r);
            rekey_client(client, r, new);
            self.retrack(c, r, new);
        }
        Outcome::Done
    }

    /// Whether what is aside of `node` is exactly what the plan saw (`expected`, unchanged), but
    /// for junk, which goes along: nothing new, nothing changed, nothing ignored (the filter
    /// judging each item at its place). What of it the filter hides there, if so.
    pub(crate) fn aside_check(
        &self,
        c: usize,
        node: NodeId,
        expected: &[NodeId],
    ) -> Option<HashSet<NodeId>> {
        let client = &self.clients[c];
        let aside = client.aside.get(&node)?;
        let (placed, hidden) = at_place_filtered(client, aside);
        let mut shown = Vec::new();
        for n in aside.ids() {
            if !hidden.contains(&n) {
                shown.push(n);
            } else if verdict_in(&placed, &client.run_filter, n) == Verdict::Ignored {
                return None;
            }
        }
        shown.sort();
        // What the plan saw, but for the folders that stayed (holding ignored items).
        let went: Vec<NodeId> = expected
            .iter()
            .copied()
            .filter(|n| !client.local.contains(*n))
            .collect();
        let unchanged = shown
            .iter()
            .all(|n| aside.get(*n) == client.planned_local.get(*n));
        (shown == went && unchanged).then_some(hidden)
    }

    /// Unlinks up to `count` items of what is aside of `node`, deepest first, and returns those
    /// the client syncs: junk (of `hidden`, [`Sim::aside_check`]'s) is disposed of; anything else
    /// must be the version its base records ([`Ledger::deleted_unverified`]). The entry goes once
    /// nothing is left of it.
    ///
    /// [`Ledger::deleted_unverified`]: crate::oracle::Ledger::deleted_unverified
    pub(crate) fn unlink_aside(
        &mut self,
        c: usize,
        node: NodeId,
        hidden: &HashSet<NodeId>,
        count: usize,
    ) -> Vec<NodeId> {
        let Some(mut aside) = self.clients[c].aside.remove(&node) else {
            return Vec::new();
        };
        let mut order: Vec<NodeId> = aside.ids().collect();
        order.sort_by_cached_key(|n| (std::cmp::Reverse(aside.ancestors(*n).len()), *n));
        let mut synced = Vec::new();
        for n in order.into_iter().take(count) {
            let Some(state) = aside.remove(n) else {
                continue;
            };
            if hidden.contains(&n) {
                self.ledger.blob_superseded(state.content); // junk, disposed of by design
            } else {
                let base = self.clients[c].base.get(n);
                let verified = base.is_some_and(|b| {
                    b.kind == state.kind
                        && (state.kind == NodeKind::Folder || b.content == state.content)
                });
                let name = &self.clients[c].name;
                self.ledger.deleted_unverified(|| {
                    (!verified).then(|| format!("{name} unlinked {n:?} {state:?}, base {base:?}"))
                });
                synced.push(n);
            }
            self.clients[c].parked.remove(&n);
            // Tag changes made here went with the item, but for an add sent up.
            self.tag_changes_left(c, n);
        }
        if !aside.is_empty() {
            self.clients[c].aside.insert(node, aside);
        }
        synced
    }

    /// What a run left aside of `node` (a crash, or a plan that replaced its own), decided at the
    /// next plan by what it is, as the executor's recovery does: each synced item the version
    /// its base records (the drive's delete stands), nothing new, nothing ignored: unlinked, its
    /// bases with it, whatever became of its folder meanwhile; anything else put back
    /// ([`Sim::put_back_aside`]).
    pub(crate) fn recover_aside(&mut self, c: usize, node: NodeId) {
        if let Some(name) = self.clients[c].replaced.remove(&node) {
            return self.recover_replaced(c, node, name);
        }
        let client = &self.clients[c];
        let Some(aside) = client.aside.get(&node) else {
            return;
        };
        let (placed, hidden) = at_place_filtered(client, aside);
        let unchanged = aside.iter().all(|(n, state)| {
            if hidden.contains(&n) {
                return verdict_in(&placed, &client.run_filter, n) != Verdict::Ignored;
            }
            client.base.get(n).is_some_and(|b| {
                b.kind == state.kind
                    && (state.kind == NodeKind::Folder || b.content == state.content)
            })
        });
        if !unchanged {
            self.note(|_| format!("  c{c} {node:?} aside isn't as synced: put back"));
            return self.put_back_aside(c, node);
        }
        self.note(|_| format!("  c{c} {node:?} aside as synced: unlinked"));
        for n in self.unlink_aside(c, node, &hidden, usize::MAX) {
            self.clients[c].drop_base(n);
        }
    }

    /// Moves the old version of `node` aside where a download puts the new one at another place
    /// (`parent`, `name`), with the bytes it held (the executor's `place_download`).
    pub(crate) fn replace_aside(&mut self, c: usize, node: NodeId, parent: NodeId, name: &str) {
        let client = &mut self.clients[c];
        let Some(old) = client
            .local
            .get(node)
            .filter(|l| (l.parent, l.name.as_str()) != (parent, name))
            .cloned()
        else {
            return;
        };
        client.local.remove(node);
        client.replaced.insert(node, old.content);
        // Parked, it is known by the name it had (the executor's `parked_place`; with its row
        // gone, recovery places it by its base): what comes back of it never takes the
        // temporary one.
        let mut aside = Tree::new();
        aside.insert(node, at_place(client, node, &old));
        client.aside.insert(node, aside);
    }

    /// The download of `node` committed: its old version aside goes, if it still holds the
    /// bytes it went aside with; else it waits for the next sync's recovery.
    fn unlink_replaced(&mut self, c: usize, node: NodeId) {
        let client = &mut self.clients[c];
        let Some(name) = client.replaced.get(&node).copied() else {
            return;
        };
        let held = client.aside.get(&node).and_then(|t| t.get(node));
        if held.is_some_and(|s| s.content == name) {
            client.aside.remove(&node);
            client.replaced.remove(&node);
        }
    }

    /// The old version of `node` a download replaced, left aside (a crash, or written to),
    /// decided at the next plan as the executor's recovery does. The download committed (the
    /// base holds the new bytes): the old version goes if it still holds the bytes it went
    /// aside with, else it comes back as a new item, never onto the new one. Not committed: it
    /// goes only as the synced version (those bytes, its base's too), its base with it; else
    /// it comes back, as its node where nothing of the new one is in place, else as a new item.
    fn recover_replaced(
        &mut self,
        c: usize,
        node: NodeId,
        name: Option<hbsync_types::FileContent>,
    ) {
        let client = &mut self.clients[c];
        let Some(old) = client.aside.get(&node).and_then(|t| t.get(node)).cloned() else {
            client.aside.remove(&node);
            return;
        };
        let base = client.base.get(node).map(|b| b.content);
        let committed = base.is_some_and(|b| b != name);
        let unchanged = old.content == name;
        let synced = base == Some(name);
        self.note(|_| {
            format!("  c{c} recovers {node:?}'s old version: committed {committed}, unchanged {unchanged}, synced {synced}")
        });
        let client = &mut self.clients[c];
        if unchanged && (committed || synced) {
            client.aside.remove(&node);
            if !committed {
                client.drop_base(node);
            }
            return;
        }
        if !committed && !client.local.contains(node) {
            return self.put_back_aside(c, node);
        }
        client.aside.remove(&node);
        let new = self.ids.fresh();
        let client = &mut self.clients[c];
        // Beside its node, where that is now (the executor's row), numbered: the node holds
        // the name, its own when parked too (`parked_place`): never the temporary one.
        let (parent, wanted, parked) = match client.local.get(node) {
            Some(now) => {
                let at = at_place(client, node, now);
                (now.parent, at.name.clone(), at.name != now.name)
            }
            None => (old.parent, old.name.clone(), false),
        };
        let parent = match client.local.get(parent) {
            Some(p) if p.kind == NodeKind::Folder => parent,
            _ => NodeId::ROOT,
        };
        let name = if parked {
            numbered_name(client, Some(new), parent, &wanted, false)
        } else {
            release_name(client, new, parent, &wanted, false)
        }
        .unwrap_or(wanted);
        client.local.insert(
            new,
            NodeState {
                parent,
                name,
                ..old
            },
        );
    }

    /// Puts what is aside of `node` back into the folder, each item at the top of what is aside
    /// (the item, or what went of it) at its place, else under the first free numbered name
    /// there, else at the folder's top (its folder gone), as the executor does. Never a delete:
    /// what was put back is planned again.
    pub(crate) fn put_back_aside(&mut self, c: usize, node: NodeId) {
        let client = &mut self.clients[c];
        let Some(aside) = client.aside.remove(&node) else {
            return;
        };
        let mut tops: Vec<NodeId> = aside
            .iter()
            .filter(|(_, s)| !aside.contains(s.parent))
            .map(|(n, _)| n)
            .collect();
        tops.sort();
        for (n, state) in aside.iter() {
            if !tops.contains(&n) {
                client.local.insert(n, state.clone());
            }
        }
        for top in tops {
            let Some(state) = aside.get(top).cloned() else {
                continue;
            };
            let is_dir = state.kind == NodeKind::Folder;
            // A parked item goes back under its own name, never the temporary one (it carries
            // the old id), and its record goes.
            let wanted = match client.parked.remove(&top) {
                Some((_, original)) if state.name == parked_name(top) => original,
                _ => state.name.clone(),
            };
            let parent = match client.local.get(state.parent) {
                Some(p) if p.kind == NodeKind::Folder => state.parent,
                _ => NodeId::ROOT,
            };
            // Of the names, a hundred numbered ones are tried: one is free.
            let name = release_name(client, top, parent, &wanted, is_dir).unwrap_or(wanted);
            client.local.insert(
                top,
                NodeState {
                    parent,
                    name,
                    ..state
                },
            );
        }
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
            let original = name.clone();
            let mut name = name;
            if !syncs_at(client, parent, &name, is_dir)
                && let Some(n) = release_name(client, node, parent, &name, is_dir)
            {
                name = n;
            }
            // Its folder gone (it went elsewhere since): never reported at a place that is no
            // more, it takes a real name where it is, and the record goes.
            if !client.local.exists(parent) {
                let at = match client.local.exists(state.parent) {
                    true => state.parent,
                    false => NodeId::ROOT,
                };
                let name = release_name(client, node, at, &original, is_dir).unwrap_or(original);
                client.local.insert(
                    node,
                    NodeState {
                        parent: at,
                        name,
                        ..state
                    },
                );
                continue;
            }
            if free(client, &name) && !client.local.would_cycle(node, parent) {
                client.local.insert(
                    node,
                    NodeState {
                        parent,
                        name,
                        ..state
                    },
                );
            } else {
                // Still parked: the record keeps the name it came from (the executor's
                // `restore_parked` keeps the record as it was), not the numbered one.
                client.parked.insert(node, (parent, original));
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
    ///
    /// Which version it settles against and how the written tags read come from the engine
    /// ([`DriveVersion::settles_against`], [`tags_written`]); the rest stays a copy of the
    /// executor's `settle`: there it is woven through its store's reads (the local row and its
    /// spelling, an online-only base), which the sim's trees stand for.
    pub(crate) fn settle_against(
        &mut self,
        c: usize,
        node: NodeId,
        against: Against<'_>,
        same_bytes: bool,
        tags: TagWrite<'_>,
        ours: Option<VersionTag>,
    ) -> Option<VersionTag> {
        let server = self.server_state(c, node);
        let remote_ref = self.server.live.get(&node).map(|n| n.remote);
        let live = remote_ref.map(|r| r.version);
        let client = &self.clients[c];
        let base_at = client
            .extra
            .get(&node)
            .and_then(|e| e.version)
            .filter(|_| client.base.contains(node));
        let ours = ours.filter(|o| base_at == Some(*o));
        let drive = against.drive();
        // The executor's compare and set. The mirror it reads here is the drive as it is now
        // (another folder's engine may have pulled it), where the run's downloads and order
        // guards read the plan's snapshot with its own writes (no pull mid-run): both happen in
        // the engine, though not mixed in one run as here.
        let acted = drive.map(|d| {
            // The node's file id is the version's: ours is at another version of it.
            let ours = ours.map(|version| RemoteRef { version, ..d.at });
            let mirror = |o| server.clone().zip(remote_ref).filter(|(_, at)| *at == o);
            d.settles_against(base_at, ours, mirror)
                .map(|(r, at)| (r, at.version))
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
        // A parked node is at its original place, as the executor's `local_node` reads it.
        let local = client
            .local
            .get(node)
            .filter(|_| verdict(client, node).is_sync())
            .map(|l| NodeState {
                tags: held_tags(client, reference.as_ref(), node, &l.tags),
                ..at_place(client, node, l)
            });
        // What the write left, as the item reads, and what was there, read alike but for a tag
        // the OS can't hold at all (the item never held that: the write leaves the drive's), as
        // the executor's.
        let filled = match tags {
            TagWrite::Wrote { before, written } => {
                let reference = reference.as_ref().map(|s| s.tags.clone());
                let reference = reference.unwrap_or_default();
                Some(tags_written(before, written, &reference, client.os))
            }
            _ => None,
        };
        let tags = match &filled {
            Some((before, written)) => TagWrite::Wrote { before, written },
            None => tags,
        };
        match (local, remote) {
            (None, _) if server.is_none() => {
                client.drop_base(node);
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
}

/// Item `node` of `client` (`state`) at its place: a parked one under its original name, in the
/// folder it is in (parking never leaves it), while it still sits at its parked name (the
/// temporary name is the executor's business; a user's move since is the user's), as the
/// executor's `parked_place` reads it.
fn at_place(client: &Client, node: NodeId, state: &NodeState) -> NodeState {
    match client.parked.get(&node) {
        Some((_, name)) if state.name == parked_name(node) => NodeState {
            name: name.clone(),
            ..state.clone()
        },
        _ => state.clone(),
    }
}

/// What `client` has aside of an item (`aside`) back at its place in the tree its plan saw,
/// and which of it the filter of its run hides there (junk, ignored): ignore rules go by path.
fn at_place_filtered(client: &Client, aside: &Tree) -> (Tree, HashSet<NodeId>) {
    let mut placed = client.planned_local.clone();
    for (n, state) in aside.iter() {
        placed.insert(n, state.clone());
    }
    let hidden = masked(&placed, &client.run_filter);
    (placed, hidden)
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
    // A record of an earlier parking is replaced, as the executor's (`put_parked`).
    client
        .parked
        .insert(occupant, (state.parent, state.name.clone()));
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

/// Whether `op`'s server-side requirements hold in the client's mirror (the snapshot it planned
/// from, plus its own writes since) and its run, by the executor's rule
/// ([`hbsync_engine::reconcile::Requirement::met`]).
fn ready(client: &Client, op: &Op) -> Met {
    let Some(requires) = op.requires() else {
        return Met::Yes;
    };
    let removing = |folder| {
        client
            .plan
            .iter()
            .any(|(o, _)| matches!(o, Op::RemoteDelete { node, .. } if *node == folder))
    };
    let Ok(met) = requires.met::<Infallible>(
        |folder| Ok(client.planned_remote.contains(folder)),
        removing,
        |folder| client.unremoved.contains(&folder),
    );
    met
}

/// What happened to one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Done,
    /// Blocked by the order of operations (target taken by a leaving node, parent not there
    /// yet, a move that would nest a folder in itself for now): try again later in the plan.
    Retry,
    /// The world changed since planning: drop it, the next plan decides again.
    Skip,
    /// A download fetched its bytes: the next step puts them in place.
    Fetched,
    /// An item being deleted was moved aside: the next step checks and unlinks it.
    Aside,
    /// A create found its node on the drive (another device brought it back): it wrote
    /// nothing, and settles against what is there.
    Landed,
}

/// The key of a refusal of `op` on `client` (`hbsync_engine::refused::key`): with the local
/// file it sends if it uploads.
pub(crate) fn refusal_key(client: &Client, op: &Op) -> String {
    hbsync_engine::refused::key(op, uploaded(client, op).as_ref())
}

/// The local file `op` sends from `client`'s item, if it uploads.
fn uploaded(client: &Client, op: &Op) -> Option<hbsync_types::FileContent> {
    op.uploads()
        .then(|| client.local.get(op.node()).and_then(|s| s.content))
        .flatten()
}

impl Sim {
    /// Whether the drive refuses `op` of client `c` as it is ([`Sim::with_refusals`]): a write
    /// or a download, by everything it sends or fetches (the operation as planned, the bytes it
    /// uploads). The simulator's own judgement: the client's record must follow it, not the
    /// other way round.
    pub(crate) fn drive_refuses(&mut self, c: usize, op: &Op) -> bool {
        let Some(what) = self.request(c, op) else {
            return false;
        };
        let seen = self.faults.judged.len();
        *self.faults.judged.entry(what).or_insert(seen % 4 == 3)
    }

    /// Whether the drive refused `op` of client `c` as it is now, when it was sent.
    pub(crate) fn drive_refused(&self, c: usize, op: &Op) -> bool {
        self.request(c, op)
            .is_some_and(|what| self.faults.judged.get(&what) == Some(&true))
    }

    /// What `op` of client `c` sends the drive or fetches from it, if it does
    /// ([`Sim::with_refusals`]): written out by the simulator itself, not with the client's key
    /// (`hbsync_engine::refused::key`), so a key that leaves out an input is caught.
    fn request(&self, c: usize, op: &Op) -> Option<String> {
        let sends = writes_drive(op) || matches!(op, Op::LocalDownload { .. });
        (self.faults.refusing && sends)
            .then(|| format!("{op:?} {:?}", uploaded(&self.clients[c], op)))
    }
}

/// Whether `op` writes to the drive.
fn writes_drive(op: &Op) -> bool {
    matches!(
        op,
        Op::RemoteCreate { .. }
            | Op::RemoteUpdate { .. }
            | Op::RemoteTags { .. }
            | Op::RemoteDelete { .. }
    )
}

/// The base's extra the executor records for `local` at `version`.
pub(crate) fn extra(local: &NodeState, version: Option<VersionTag>) -> BaseExtra {
    BaseExtra {
        local_hash: local.content.map(|c| c.hash),
        content_state: ContentState::Local,
        version,
    }
}

/// The next page of `client`'s sweep over `all` (its nodes) in pages of `size`, as the engine
/// pages its dirty set: what a page waits for first ([`hbsync_engine::pages::OwnPages`]), else
/// the next `size` nodes in id order. And whether it is a piece of a folder too big for a page
/// (`Options::own_page`).
fn next_page(
    client: &Client,
    input: &Input,
    all: &BTreeSet<NodeId>,
    size: usize,
) -> (Vec<NodeId>, bool) {
    let mut pages = client.pages.borrow_mut();
    // A new sweep once the last one's last page was planned and what its pages wait for too,
    // each a page of its own, as the engine's.
    if pages.ended && pages.own.is_empty() {
        pages.ended = false;
        pages.own.new_sweep();
        pages.planned_own.clear();
    }
    let mut store = SimPages { client, input, all };
    if let Ok(Some((page, piece))) = pages.own.next(size, &mut store) {
        pages.planned_own.extend(page.iter().copied());
        return (page, piece);
    }
    loop {
        let read: Vec<NodeId> = match pages.after {
            Some(after) => all
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .take(size)
                .copied()
                .collect(),
            None => all.iter().take(size).copied().collect(),
        };
        if read.len() < size {
            pages.after = None;
            pages.ended = true;
        } else {
            pages.after = read.last().copied();
        }
        // What a page of its own planned this sweep isn't planned again in it.
        let page: Vec<NodeId> = read
            .into_iter()
            .filter(|n| !pages.planned_own.contains(n))
            .collect();
        if !page.is_empty() || pages.ended {
            return (page, false);
        }
    }
}

/// A client's model as [`hbsync_engine::pages::dirty_elsewhere`] asks it: `all` its dirty
/// nodes, `trees` its whole view's sides.
struct SimPending<'a> {
    trees: [hbsync_engine::tree::Tree; 3],
    all: &'a BTreeSet<NodeId>,
}

impl hbsync_engine::pages::Pending for SimPending<'_> {
    type Error = Infallible;
    fn among(&mut self, nodes: &[NodeId]) -> Result<HashSet<NodeId>, Infallible> {
        Ok(nodes
            .iter()
            .copied()
            .filter(|n| self.all.contains(n))
            .collect())
    }
    fn below(&mut self, folder: NodeId, page: &BTreeSet<NodeId>) -> Result<bool, Infallible> {
        let [base, local, remote] = &self.trees;
        Ok(subtree_any_side([base, local, remote], folder)
            .into_iter()
            .any(|n| self.all.contains(&n) && !page.contains(&n)))
    }
}

/// A client's model as [`hbsync_engine::pages::OwnPages`] reads it: `all` its dirty nodes.
struct SimPages<'a> {
    client: &'a Client,
    input: &'a Input,
    all: &'a BTreeSet<NodeId>,
}

impl hbsync_engine::pages::PageStore for SimPages<'_> {
    type Mark = NodeId;
    type Error = Infallible;
    fn marks_of(&mut self, nodes: &[NodeId]) -> Result<Vec<NodeId>, Infallible> {
        Ok(nodes
            .iter()
            .copied()
            .filter(|n| self.all.contains(n))
            .collect())
    }
    fn marks_under(&mut self, folder: NodeId, limit: usize) -> Result<Vec<NodeId>, Infallible> {
        let i = self.input;
        let under = subtree_any_side([&i.base, &i.local, &i.remote], folder);
        Ok(under
            .into_iter()
            .filter(|n| self.all.contains(n))
            .take(limit)
            .collect())
    }
    fn folders_in(&mut self, folder: NodeId) -> Result<Vec<NodeId>, Infallible> {
        let base = &self.client.base;
        Ok(base
            .children(folder)
            .filter(|n| base.get(*n).is_some_and(|s| s.kind == NodeKind::Folder))
            .collect())
    }
}

/// `ops` as the oracle reads them ([`Planned`]): it may not import the engine's `Op`.
fn planned(ops: &[Op]) -> Vec<Planned<'_>> {
    ops.iter()
        .map(|op| {
            let what = match op {
                Op::RemoteDelete { .. } => What::RemoteDelete,
                Op::LocalTrash { subtree, .. } => What::LocalTrash {
                    subtree: subtree.clone(),
                },
                _ if op.trashes().is_some() => What::Trash,
                Op::LocalMove { parent, name, .. } => What::LocalMove {
                    parent: *parent,
                    name: name.clone(),
                },
                Op::RemoteUpdate { parent, name, .. } => What::RemoteUpdate {
                    parent: *parent,
                    name: name.clone(),
                },
                Op::LocalDetach { new_node, .. } => What::Detach {
                    new_node: *new_node,
                },
                _ => What::Other,
            };
            Planned {
                node: op.node(),
                what,
                op,
            }
        })
        .collect()
}

/// Whether a pull between a local operation's check and its commit ([`Action::PullMidLocal`],
/// `on`) lands on `op`: any local operation the executor checks, does, then commits; or a tag
/// write, a download, a move, a trash.
///
/// [`Action::PullMidLocal`]: crate::Action::PullMidLocal
fn pulled_on(op: &Op, on: usize) -> bool {
    match on % 5 {
        1 => matches!(op, Op::LocalTags { .. }),
        2 => matches!(op, Op::LocalDownload { .. }),
        3 => matches!(op, Op::LocalMove { .. }),
        4 => matches!(op, Op::LocalTrash { .. }),
        _ => matches!(
            op,
            Op::LocalMkdir { .. }
                | Op::LocalMove { .. }
                | Op::LocalDownload { .. }
                | Op::LocalTags { .. }
                | Op::LocalTrash { .. }
                | Op::LocalFork { .. }
                | Op::LocalDetach { .. }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Action, Event, act};

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
}
