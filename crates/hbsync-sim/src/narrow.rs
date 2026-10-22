//! The engine's own planning path over a simulated client ([`Sim::with_narrow_check`]): what
//! the client plans from is filled into a state DB as its scan, feed and syncs leave it there,
//! and planned as the engine plans, from narrow views ([`view::plan_narrow`]). Checked against
//! a plan from whole folders ([`view::build_input`]) for the same dirty nodes, and the whole
//! one against the sim's own plan: the narrow view loads what the rules need, and the sim's
//! input is the engine's.
//!
//! [`Sim::with_narrow_check`]: crate::Sim::with_narrow_check

use std::collections::BTreeSet;
use std::sync::Arc;

use hbsync_engine::FolderCtx;
use hbsync_engine::held;
use hbsync_engine::reconcile::{Input, Op, Options, Plan, reconcile_with_known};
use hbsync_engine::view;
use hbsync_fileio::HashAlgorithm;
use hbsync_platform::{FileId, Fingerprint, Os, RelPath};
use hbsync_state::{BaseRow, LocalRow, MirrorRow, RemovedRow, StateDb, StateError};
use hbsync_types::schema::NodeContent;
use hbsync_types::{
    ContentState, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, VersionTag,
};

use crate::Sim;
use crate::model::client::Client;

impl Sim {
    /// Client `c`'s plan `plan`, made from `input` with `opts` and the ids it takes as known,
    /// checked against the engine's planning path over the same state (`round`: which of the
    /// nodes whose sides agree the narrow plan takes dirty): what differs, if anything.
    pub(crate) fn check_narrow(
        &self,
        c: usize,
        input: &Input,
        opts: &Options,
        known: &dyn Fn(NodeId) -> bool,
        plan: &Plan,
        round: usize,
    ) -> Option<String> {
        let client = &self.clients[c];
        if !client.coords.top.is_root() {
            return None; // the main folder only: a role folder's coordinates aren't filled in
        }
        let folder = self.folder_ctx(c);
        let checked = (|| -> Result<Option<String>, StateError> {
            let mut db = self.fill(c, input)?;
            let reconcile =
                |i: &Input, _: &mut StateDb| reconcile_with_known(i, opts, folder.masking(), known);
            let all: Vec<NodeId> = input.dirty.iter().copied().collect();
            let viewed = db.read(|s| view::build_input(s, &all, &folder))?;
            let whole = reconcile(&viewed, &mut db);
            if whole != *plan {
                let differ = input_differences(input, &viewed);
                return Ok(Some(format!(
                    "c{c}: the engine's view plans otherwise than the sim's input:\n  engine: {whole:?}\n  sim:    {plan:?}\n  inputs (sim, then engine): {differ}"
                )));
            }
            // The nodes whose sides differ dirty, as the engine keeps them, and every other
            // one of the rest, by the planning round.
            let differs = engine_dirty(input, plan);
            let some: Vec<NodeId> = all
                .iter()
                .copied()
                .enumerate()
                .filter(|(i, n)| differs.contains(n) || (i + round).is_multiple_of(2))
                .map(|(_, n)| n)
                .collect();
            if some.is_empty() {
                return Ok(None);
            }
            let narrow = view::plan_narrow(&mut db, &some, &folder, reconcile)?;
            let full = match some.len() == all.len() {
                true => whole,
                false => {
                    let full = db.read(|s| view::build_input(s, &some, &folder))?;
                    reconcile(&full, &mut db)
                }
            };
            Ok((narrow != full).then(|| {
                format!(
                    "c{c}: the narrow view plans otherwise than whole folders for {some:?}:\n  narrow: {narrow:?}\n  whole:  {full:?}"
                )
            }))
        })();
        checked.unwrap_or_else(|e| Some(format!("c{c}: the store failed: {e}")))
    }

    /// Client `c`'s plan `plan` holds deletes (the guard's, §7): planned through the engine's
    /// views, then again with what the engine takes out of the dirty set till the user answers
    /// ([`hbsync_engine::held`]) out of it, it plans the same but those, unless its view
    /// reaches them (the engine then plans them again too): what differs, if anything.
    pub(crate) fn check_held(
        &self,
        c: usize,
        input: &Input,
        opts: &Options,
        known: &dyn Fn(NodeId) -> bool,
        plan: &Plan,
    ) -> Option<String> {
        let client = &self.clients[c];
        if plan.held.is_empty() || !client.coords.top.is_root() {
            return None;
        }
        let folder = self.folder_ctx(c);
        let checked = (|| -> Result<Option<String>, StateError> {
            let mut db = self.fill(c, input)?;
            // The nodes whose sides differ dirty, and those the plan has work for, as the engine
            // keeps them.
            let dirty: Vec<NodeId> = engine_dirty(input, plan).into_iter().collect();
            // The store's marks are those: filling it marks what it writes (a pull's rows), and
            // a mark the page doesn't hold reads as another page's (§6.5).
            db.write(|s| {
                for n in s.dirty_nodes(usize::MAX)? {
                    s.folder().clear_dirty(n)?;
                }
                dirty.iter().try_for_each(|&n| s.folder().mark_dirty(n))
            })?;
            let folder = &folder;
            let with = |opts: &Options| {
                let opts = opts.clone();
                move |i: &Input, _: &mut StateDb| -> Plan {
                    reconcile_with_known(i, &opts, folder.masking(), known)
                }
            };
            let all = view::plan_narrow(&mut db, &dirty, folder, with(opts))?;
            let (out, nodes) = held::out_of_dirty(&all, &dirty);
            if out.is_empty() {
                return Ok(None);
            }
            let rest: Vec<NodeId> = dirty
                .iter()
                .copied()
                .filter(|n| !nodes.contains(n))
                .collect();
            // Out of the dirty set, as the engine takes them ([`held`]).
            db.write(|s| {
                for &n in &nodes {
                    if let Some(mark) = s.dirty_mark(n)? {
                        s.folder().hold_delete(mark)?;
                    }
                }
                Ok(())
            })?;
            let mut opts = opts.clone();
            opts.guarding.held = held::files(&all, out.iter().copied());
            let reached = std::cell::Cell::new(false);
            let reconcile = with(&opts);
            let without =
                view::plan_narrow(&mut db, &rest, folder, |i: &Input, db: &mut StateDb| {
                    reached.set(reached.get() || held::touches(i, &nodes));
                    reconcile(i, db)
                })?;
            if reached.get() {
                return Ok(None);
            }
            let mut held_all = all.held.clone();
            let mut held_without: Vec<Op> = without.held.clone();
            held_without.extend(out.iter().map(|op| (*op).clone()));
            held_all.sort_by_key(Op::node);
            held_without.sort_by_key(Op::node);
            let alike = all.ops == without.ops
                && all.conflicts == without.conflicts
                && all.deferred == without.deferred
                && all.waiting == without.waiting
                && all.delete_waits == without.delete_waits
                && held_all == held_without
                && all
                    .clean
                    .iter()
                    .filter(|n| !nodes.contains(n))
                    .eq(without.clean.iter());
            Ok((!alike).then(|| {
                format!(
                    "c{c}: with the held deletes {nodes:?} out of the dirty set, it plans otherwise:\n  all:     {all:?}\n  without: {without:?}"
                )
            }))
        })();
        checked.unwrap_or_else(|e| Some(format!("c{c}: the store failed: {e}")))
    }

    /// The folder client `c`'s main folder plans in, as the engine's.
    fn folder_ctx(&self, c: usize) -> FolderCtx {
        let client = &self.clients[c];
        FolderCtx {
            root: std::path::PathBuf::from("/sim"),
            os: client.os,
            algorithm: HashAlgorithm::Blake3,
            case_insensitive: client.os != Os::Linux,
            filter: Arc::clone(&client.filter),
            scope: Arc::new(client.scope()),
            tags_kept: Arc::default(),
            mtime_proves_bytes: Arc::default(),
            family: Arc::default(),
        }
    }

    /// A state DB holding what client `c` plans from (`input`), as the engine's store has it.
    fn fill(&self, c: usize, input: &Input) -> Result<StateDb, StateError> {
        let client = &self.clients[c];
        let mut db = StateDb::open_in_memory()?;
        let hidden: BTreeSet<NodeId> = hbsync_engine::mask::masked(&client.local, &client.filter)
            .into_iter()
            .collect();
        db.write(|s| {
            // The scan's rows, at the paths on disk (a parked item under its temporary name),
            // each folder before what is in it.
            let mut rows: Vec<(NodeId, &NodeState, RelPath)> = client
                .local
                .iter()
                .filter(|(node, _)| !hidden.contains(node))
                .filter_map(|(node, state)| {
                    let path = client.local.path(node).and_then(|p| path_of(&p))?;
                    Some((node, state, path))
                })
                .collect();
            rows.sort_by_key(|(node, _, path)| (path.depth(), *node));
            for (node, state, path) in rows {
                s.folder()
                    .put_local(&local_row(client, node, state, path))?;
            }
            for (node, (parent, name)) in &client.parked {
                let place = match parent {
                    p if p.is_root() => Some(name.clone()),
                    p => client.local.path(*p).map(|mut p| {
                        p.push(name);
                        p.join("/")
                    }),
                };
                if let Some(original) = place.and_then(|p| RelPath::new(&p).ok()) {
                    s.folder().put_parked(*node, &original)?;
                }
            }
            for (node, state) in input.base.iter() {
                let extra = input.base_extra.get(&node);
                let at = RemoteRef {
                    file_id: self.file_id(node),
                    version: extra
                        .and_then(|e| e.version)
                        .unwrap_or(VersionTag(hbsync_types::Uuid::nil())),
                };
                let mut row = BaseRow::new(node, state.clone(), at);
                if let Some(e) = extra {
                    row.content_state = e.content_state;
                    row.local_hash = e.local_hash;
                } else {
                    row.content_state = ContentState::Local;
                }
                s.folder().put_base(&row)?;
            }
            for (node, state) in input.remote.iter() {
                let Some(&at) = input.remote_refs.get(&node) else {
                    continue;
                };
                let mut row = mirror_row(node, state, at);
                row.changed_at = input.changed_at.get(&node).copied();
                s.account().put_live(&row)?;
            }
            // What the drive holds in a version no client reads.
            for node in &input.unreadable {
                let Some((n, _)) = self.server.node(*node) else {
                    continue;
                };
                s.account()
                    .put_unreadable(n.remote, Some(*node), Some(n.state.parent))?;
            }
            for (folder, removed) in &input.removed {
                let Some(r) = self.server.removed.get(folder) else {
                    continue;
                };
                let row = mirror_row(*folder, &removed.state, r.remote);
                s.account().move_to_removed(&RemovedRow {
                    row,
                    removed_at: Some(removed.at),
                })?;
            }
            Ok(())
        })?;
        Ok(db)
    }

    /// `node`'s file id on the drive (live or removed); one of its own where it has none there
    /// any more (gone for good: file ids are unique in the store).
    fn file_id(&self, node: NodeId) -> RemoteFileId {
        self.server
            .node(node)
            .map_or(RemoteFileId(node.0), |(n, _)| n.remote.file_id)
    }
}

/// Where the sim's input `sim` and the engine's view `engine` differ, field by field (sorted:
/// the trees iterate in no set order).
fn input_differences(sim: &Input, engine: &Input) -> String {
    fn sorted<T: std::fmt::Debug>(items: impl Iterator<Item = T>) -> Vec<String> {
        let mut v: Vec<String> = items.map(|i| format!("{i:?}")).collect();
        v.sort();
        v
    }
    let fields = |i: &Input| {
        [
            ("local", sorted(i.local.iter())),
            ("base", sorted(i.base.iter())),
            ("remote", sorted(i.remote.iter())),
            ("base_extra", sorted(i.base_extra.iter())),
            ("remote_refs", sorted(i.remote_refs.iter())),
            ("local_unhashed", sorted(i.local_unhashed.iter())),
            ("dirty", sorted(i.dirty.iter())),
            ("unreadable", sorted(i.unreadable.iter())),
            ("removed", sorted(i.removed.iter())),
            ("changed_at", sorted(i.changed_at.iter())),
        ]
    };
    fields(sim)
        .into_iter()
        .zip(fields(engine))
        .filter(|((_, a), (_, b))| a != b)
        .map(|((name, a), (_, b))| format!("\n    {name}: {a:?}\n    {name}: {b:?}"))
        .collect()
}

/// `path` (names, outermost first) as a relative path, if each name is one.
fn path_of(path: &[&str]) -> Option<RelPath> {
    RelPath::new(&path.join("/")).ok()
}

/// The scan's row of the client's item `node` (`state`) at `path`.
fn local_row(client: &Client, node: NodeId, state: &NodeState, path: RelPath) -> LocalRow {
    let content = state.content;
    LocalRow {
        path,
        node,
        kind: state.kind,
        fingerprint: Fingerprint {
            size: content.map_or(0, |c| c.size),
            mtime_ns: content.map_or(0, |c| c.mtime_ms * 1_000_000),
            ctime_ns: 0,
            file_id: FileId {
                volume: 1,
                file: node.0.as_u128(),
            },
        },
        executable: content.map(|c| c.executable),
        hash: content
            .filter(|_| !client.unhashed_now().any(|n| n == node))
            .map(|c| c.hash),
        tags: (client.os != Os::Windows).then(|| state.tags.clone()),
    }
}

/// The drive's row of `node`, `state` at `at`, as the feed records it.
pub(crate) fn mirror_row(node: NodeId, state: &NodeState, at: RemoteRef) -> MirrorRow {
    let mut content = match (state.kind, state.content) {
        (NodeKind::File, Some(c)) => {
            NodeContent::file(&state.name, c.mtime_ms, c.size, c.executable)
        }
        _ => NodeContent::folder(&state.name),
    };
    content.set_tags(&state.tags);
    MirrorRow::from_content(
        at,
        node,
        state.parent,
        state.kind,
        &content,
        state.content.map(|c| c.hash),
    )
}

/// The nodes of `input` whose sides differ, as the engine's scan and feed mark every change
/// dirty (what a delete took along reads as gone).
pub(crate) fn sides_differ(input: &Input) -> BTreeSet<NodeId> {
    let taken = hbsync_engine::along::taken_along(
        input.remote.iter().map(|(n, s)| (n, s.parent)),
        |f| input.removed.get(&f).map(|r| r.at),
        |n| input.changed_at.get(&n).copied(),
    );
    input
        .dirty
        .iter()
        .copied()
        .filter(|n| {
            let base = input.base.get(*n);
            let remote = input.remote.get(*n).filter(|_| !taken.contains(n));
            base != input.local.get(*n) || base != remote
        })
        .collect()
}

/// The nodes of `input` dirty as the engine keeps them, `plan` made from it: those whose sides
/// differ ([`sides_differ`]), and those the plan has work for (a node a plan didn't settle
/// stays dirty).
fn engine_dirty(input: &Input, plan: &Plan) -> BTreeSet<NodeId> {
    let work: BTreeSet<NodeId> = plan
        .ops
        .iter()
        .map(|op| op.node())
        .chain(plan.conflicts.iter().map(|c| c.node()))
        .chain(plan.deferred.iter().copied())
        .chain(plan.held.iter().map(|op| op.node()))
        .collect();
    let mut dirty = sides_differ(input);
    dirty.extend(input.dirty.iter().filter(|n| work.contains(n)));
    dirty
}
