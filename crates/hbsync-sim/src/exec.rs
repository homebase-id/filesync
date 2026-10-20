//! Everything that mirrors the engine: planning, the executor's steps and guards, the trash,
//! unparking and settling. Each function calls the engine or says what of it it copies, but
//! for the scope check (`breaches`, `planned_writable`), which the oracle records: it reads the
//! engine's `Op`, which the oracle may not import.

use std::collections::{BTreeSet, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_engine::Against;
use hbsync_engine::mask::masked;
use hbsync_engine::plan::{Acts, Met, TagWrite, not_reached, tags_written};
use hbsync_engine::reconcile::{
    Agreement, BaseExtra, DriveVersion, Input, Masking, Op, Options, Removed, reconcile_with_known,
};
use hbsync_engine::tree::{Tree, first_free, numbered_names};
use hbsync_local::Verdict;
use hbsync_local::layout::parked_name;
use hbsync_platform::Os;
use hbsync_types::{
    ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, Role, Tags, VersionTag,
};

use crate::Sim;
use crate::action::NAMES;
use crate::model::client::{Client, rekey_client, verdict};
use crate::model::server::{ServerNode, time};
use crate::oracle::view_with;

impl Sim {
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
        for n in &client.unhashed {
            if let Some(state) = local_view.get(*n).cloned() {
                local_view.insert(
                    *n,
                    NodeState {
                        content: None,
                        ..state
                    },
                );
            }
        }
        // What the client can't hold of the tags is the drive's, as the engine's view has it.
        see_held_tags(client, &mut local_view, &remote);
        let mut input = Input {
            base: client.base.clone(),
            local: local_view,
            remote: remote.clone(),
            base_extra: client.extra.clone(),
            remote_complete: true,
            local_unhashed: client.unhashed.iter().copied().collect(),
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
            while !input.removed.contains_key(&folder)
                && !outside.contains(&folder)
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
        let confirmed = Arc::new(client.confirmed.clone());
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
            allow_mass_delete: self.guard.is_none(),
            mass_delete_min: self.guard.unwrap_or_default(),
            mass_delete_fraction: 0.0,
            synced_files,
            confirmed: Arc::clone(&confirmed),
            ..Options::default()
        };
        let masking = Masking {
            filter: &client.filter,
            scope: &scope,
        };
        // Ids the client or the server knows are taken, as in the executor's state DB.
        let plan = reconcile_with_known(&input, &opts, masking, |id| {
            client.local.contains(id) || client.base.contains(id) || self.server.ever.contains(&id)
        });
        // The guard's rule, checked independently: no plan deletes more files on a side than
        // the limit (or every file) without the user's confirmation.
        if let Some(min) = self.guard {
            // Counted only until the first breach is kept.
            self.ledger.unconfirmed_mass_delete(|| {
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
                (trips(deleted) || trips(trashed)).then(|| {
                    format!(
                        "{} deletes {deleted} remote and trashes {trashed} local files \
                         unconfirmed (limit {min}, {synced_files} synced): {:?}",
                        client.name, plan.ops
                    )
                })
            });
        }
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
        let (ops, refused) = if !self.refusing {
            (plan.ops.clone(), Vec::new())
        } else {
            let client = &self.clients[c];
            let held = hbsync_engine::refused::hold_back(
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
            let Ok(held) = held;
            held
        };
        let waiting: Vec<Op> = match self.refusing {
            true => plan
                .ops
                .iter()
                .filter(|op| !ops.contains(op) && !refused.contains(op))
                .cloned()
                .collect(),
            false => Vec::new(),
        };
        let client = &mut self.clients[c];
        client.held_back = refused
            .into_iter()
            .map(|op| (op, true))
            .chain(waiting.into_iter().map(|op| (op, false)))
            .collect();
        // A confirmation lasts while the plan deletes its node.
        let deleting: HashSet<NodeId> = plan.ops.iter().filter_map(Op::deletes).collect();
        client.confirmed = confirmed.intersection(&deleting).copied().collect();
        client.held = plan.held.iter().map(Op::node).collect();
        if self.refusing {
            client.last_plan = ops.clone();
        }
        client.plan = ops.into_iter().map(|op| (op, 0)).collect();
        client.removed_by_plan.clear();
        client.unremoved.clear();
        // A plan made since sends what the user does from now on.
        self.tags_unsent.retain(|(at, _, _)| *at != c);
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
        let held_before = self.clients[c].local.get(node).cloned();
        let before_tags = held_before.as_ref().map(|l| l.tags.clone());
        let fresh = held_before.is_none();
        let outcome = self.execute(c, &op);
        let landed = outcome == Outcome::Landed;
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
        if outcome == Outcome::Fetched {
            // Put in place and committed by the next step, whatever happens meanwhile.
            self.clients[c].plan.push_front((op, attempts));
            return true;
        }
        // Now and then a tag write fails (the item keeps what it held, a placed one holds none,
        // read so), or a placement's commit fails once its item is in place.
        let (mut tag_failed, mut commit_failed) = (false, false);
        let writes_tags = placement || matches!(op, Op::LocalTags { .. });
        if outcome == Outcome::Done && writes_tags && !landed {
            self.writes += 1;
            let os = self.clients[c].os;
            // A folder made with none to write holds none, the write failing or not (the
            // executor's: it claims none).
            let nothing = matches!(&op, Op::LocalMkdir { tags, .. } if tags.held_by(os).is_empty());
            tag_failed = os != Os::Windows
                && !nothing
                && (std::mem::take(&mut self.fail_tags_next) || self.writes % 13 == 7);
            let recorded = matches!(op, Op::LocalDownload { .. })
                || matches!(op, Op::LocalMkdir { drive: Some(_), .. }) && synced_before;
            commit_failed =
                recorded && (std::mem::take(&mut self.fail_commit_next) || self.writes % 11 == 5);
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
                match held_before {
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
                    let name = tag.name.clone();
                    // The item no longer holds what this client's user added to it: the add
                    // stays with the node (on the drive, or nowhere), not with the item (a
                    // conflict copy made of it later doesn't take it along).
                    self.detach_add((c, node, name));
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
                for tag in tags.iter() {
                    let add = (c, node, tag.name.clone());
                    let now = self.server.now();
                    if self.tags_added.contains(&add) {
                        // The last write carrying it: one after a removal sends it again, as
                        // this client's add (a node with no base here yet merges both sides').
                        self.tags_sent.insert(add, now);
                    } else if self.tags_unsent.remove(&add) {
                        // Its item deleted here since, the plan sent it all the same: it stands
                        // with the node.
                        self.tags_added.insert(add.clone());
                        self.tags_sent.insert(add.clone(), now);
                        self.detach_add(add);
                    }
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
        }
        true
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
                let added = self
                    .tags_added
                    .iter()
                    .filter(|(at, n, _)| (*at, *n) == (c, node));
                let forked: Vec<_> = added.cloned().collect();
                self.tags_forked.extend(forked);
                self.retrack(c, node, new_node);
                Done
            }
            Op::LocalMkdir {
                node,
                parent,
                name,
                tags,
                ..
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
                        if self.split_downloads {
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
                    let rightly = now
                        .as_ref()
                        .is_some_and(|now| self.ledger.deleted_by_user(node, now))
                        || version.is_some_and(|v| self.ledger.plan_saw_taken(c, node, v));
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

    /// Moves `node` and everything below it to the trash, as the executor does: only if the
    /// visible part is `expected` and unchanged since planning, with the bases of all of it.
    /// Junk goes along; ignored items stay, and so do the folders they sit in, released (§10).
    fn trash(&mut self, c: usize, node: NodeId, expected: &[NodeId]) -> Outcome {
        use Outcome::{Done, Skip};
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
                self.ledger.blob_superseded(content); // junk, disposed of by design
            }
            let client = &mut self.clients[c];
            client.local.remove(n);
            client.parked.remove(&n);
            // Tag changes made here went to the trash with the item, but for an add sent up.
            self.tag_changes_left(c, n);
        }
        // What went isn't synced here any more: its bases go with it, whatever the drive holds
        // by now (the executor's; one left behind would read as a local delete once the node
        // is back in view). Of what it had rows of: what isn't filtered (an ignored item kept
        // here keeps its base).
        let client = &mut self.clients[c];
        for n in &shown {
            client.drop_base(*n);
        }
        for r in released {
            let new = self.ids.fresh();
            let client = &mut self.clients[c];
            client.parked.remove(&r);
            rekey_client(client, r, new);
            self.retrack(c, r, new);
        }
        Done
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
        let seen = self.judged.len();
        *self.judged.entry(what).or_insert(seen % 4 == 3)
    }

    /// Whether the drive refused `op` of client `c` as it is now, when it was sent.
    pub(crate) fn drive_refused(&self, c: usize, op: &Op) -> bool {
        self.request(c, op)
            .is_some_and(|what| self.judged.get(&what) == Some(&true))
    }

    /// What `op` of client `c` sends the drive or fetches from it, if it does
    /// ([`Sim::with_refusals`]): written out by the simulator itself, not with the client's key
    /// (`hbsync_engine::refused::key`), so a key that leaves out an input is caught.
    fn request(&self, c: usize, op: &Op) -> Option<String> {
        let sends = matches!(
            op,
            Op::RemoteCreate { .. }
                | Op::RemoteUpdate { .. }
                | Op::RemoteTags { .. }
                | Op::RemoteDelete { .. }
                | Op::LocalDownload { .. }
        );
        (self.refusing && sends).then(|| format!("{op:?} {:?}", uploaded(&self.clients[c], op)))
    }
}

/// The base's extra the executor records for `local` at `version`.
pub(crate) fn extra(local: &NodeState, version: Option<VersionTag>) -> BaseExtra {
    BaseExtra {
        local_hash: local.content.map(|c| c.hash),
        content_state: ContentState::Local,
        version,
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
