//! Building the reconciler's input from the state DB.
//!
//! For each dirty node the three sides are loaded, plus the context the rules need:
//! - every ancestor on every side (paths, cycles, orphans);
//! - in each folder it is in on any side, the children whose names may clash with its names
//!   there (name clashes): [`build_narrow`] reads those by their sibling hash, [`build_input`]
//!   all children;
//! - the children of a dirty folder that may go (live children keep it);
//! - the full local subtree of a folder that is gone remotely (what a trash would take).
//!
//! The local side is translated from paths to the node model here: the parent is the node at
//! the parent path, the name is the remote spelling (recorded mapping or [`to_remote`]), and a
//! node parked under a temporary name is reported at its original place ([`parked_place`]).

use std::collections::{BTreeSet, HashSet, VecDeque};

use hbsync_local::layout::parked_name;
use hbsync_namemap::to_remote;
use hbsync_platform::{Os, RelPath};
use hbsync_state::{BaseRow, LocalRow, MirrorRow, ReadTx, StateDb, StateError};
use hbsync_types::{FileContent, NodeId, NodeKind, NodeState, RECOVERED_NAME, Tags};

use crate::folder::FolderCtx;
use crate::plan::{Coverage, Plan};
use crate::reconcile::{BaseExtra, Input, Removed};
use crate::scope::Scope;
use crate::walk::End;

/// The local state of one node, as the reconciler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalNode {
    pub row: LocalRow,
    /// `None` if the parent path isn't known in the DB (the scan hasn't caught up).
    pub state: Option<NodeState>,
}

/// Reads one node's local state. `remote_tags`: the drive's tags of the node, if it is there
/// (what this device can't hold of them stands).
pub(crate) fn local_node(
    s: &ReadTx<'_>,
    node: NodeId,
    os: Os,
    remote_tags: Option<&Tags>,
) -> Result<Option<LocalNode>, StateError> {
    let Some(row) = s.local_by_node(node)? else {
        return Ok(None);
    };
    let state = local_state(s, &row, os, remote_tags)?;
    Ok(Some(LocalNode { row, state }))
}

fn local_state(
    s: &ReadTx<'_>,
    row: &LocalRow,
    os: Os,
    remote_tags: Option<&Tags>,
) -> Result<Option<NodeState>, StateError> {
    let base = s.get_base(row.node)?;
    // A parked node counts as being at its original place.
    let path = parked_place(s, row)?.unwrap_or_else(|| row.path.clone());
    let Some(local_name) = path.name() else {
        return Ok(None);
    };
    // Parking never leaves the folder, so the row's folder is the place's folder too.
    let Some(parent) = s.local_parent(row.node)? else {
        return Ok(None);
    };
    let name = match &base {
        Some(b) if b.local_name.as_deref() == Some(local_name) => b.state.name.clone(),
        _ => match to_remote(local_name, os) {
            Ok(n) => n,
            Err(_) => return Ok(None),
        },
    };
    // What this device can't hold of the tags is the drive's (else the agreed) (§11).
    let reference = match remote_tags {
        Some(t) => t.clone(),
        None => base
            .as_ref()
            .map(|b| b.state.tags.clone())
            .unwrap_or_default(),
    };
    Ok(Some(NodeState {
        parent,
        name,
        kind: row.kind,
        // An unhashed file is reported without content; the caller defers it.
        content: local_content(row, base.as_ref()),
        tags: Tags::fill(row.tags.as_ref(), &reference, os),
    }))
}

/// The synced content of a local file: `None` for folders and files not hashed yet. Where the
/// OS has no x bit, the base's value is kept.
pub(crate) fn local_content(row: &LocalRow, base: Option<&BaseRow>) -> Option<FileContent> {
    if row.kind != NodeKind::File {
        return None;
    }
    Some(FileContent {
        hash: row.hash?,
        size: row.fingerprint.size,
        mtime_ms: row.fingerprint.mtime_ns.div_euclid(1_000_000),
        executable: row.executable.unwrap_or_else(|| {
            base.and_then(|b| b.state.content)
                .is_some_and(|c| c.executable)
        }),
    })
}

/// Where a parked node really belongs: its current folder, under the name it had before it
/// was parked. `None` if it isn't parked: no record, or it no longer carries its temporary
/// name (the user renamed it, and the record is void). Parking never leaves the folder, so the
/// place follows the folder (or the item) wherever it is moved meanwhile.
pub(crate) fn parked_place(s: &ReadTx<'_>, row: &LocalRow) -> Result<Option<RelPath>, StateError> {
    let Some(name) = s.get_parked(row.node)? else {
        return Ok(None);
    };
    if row.path.name() != Some(parked_name(row.node).as_str()) {
        return Ok(None);
    }
    Ok(row.path.parent().unwrap_or_default().join(&name).ok())
}

/// Loads everything the reconciler needs for `dirty`, with every child of each folder involved,
/// for the sync folder `ctx` (its filter, §10, and its scope, §12).
pub fn build_input(s: &ReadTx<'_>, dirty: &[NodeId], ctx: &FolderCtx) -> Result<Input, StateError> {
    build(s, dirty, ctx, None)
}

/// [`build_input`] with only the children the rules may need, except in the folders in `full`
/// ([`Input::coverage`]): planning a change in a large folder reads a few rows, not the folder.
/// A plan that needs more says so ([`crate::plan::Plan::uncovered`]).
pub(crate) fn build_narrow(
    s: &ReadTx<'_>,
    dirty: &[NodeId],
    ctx: &FolderCtx,
    full: &HashSet<NodeId>,
) -> Result<Input, StateError> {
    build(s, dirty, ctx, Some(full))
}

/// Plans `dirty` from narrow views ([`build_narrow`]) with `reconcile`: each plan that needs more
/// of a folder than its view held ([`crate::plan::Plan::uncovered`]) is made again with those
/// folders whole. The engine plans so; the simulator, over a store it fills, checks it against
/// whole folders.
pub fn plan_narrow(
    db: &mut StateDb,
    dirty: &[NodeId],
    folder: &FolderCtx,
    mut reconcile: impl FnMut(&Input, &mut StateDb) -> Plan,
) -> Result<Plan, StateError> {
    let mut full: HashSet<NodeId> = HashSet::new();
    loop {
        let input = db.read(|s| build_narrow(s, dirty, folder, &full))?;
        let plan = reconcile(&input, db);
        // Each round covers at least one more folder whole: it ends.
        let before = full.len();
        full.extend(&plan.uncovered);
        if full.len() == before {
            return Ok(plan);
        }
    }
}

/// Past this many names to look up in one folder, it is listed whole: one query instead.
const NAMED_MAX: usize = 32;

fn build(
    s: &ReadTx<'_>,
    dirty: &[NodeId],
    ctx: &FolderCtx,
    full: Option<&HashSet<NodeId>>,
) -> Result<Input, StateError> {
    let (filter, scope) = (&*ctx.filter, &*ctx.scope);
    let mut b = Builder {
        s,
        os: ctx.os,
        input: Input::default(),
        base_done: HashSet::new(),
        mirror_done: HashSet::new(),
        local_done: HashSet::new(),
    };
    b.input.dirty = dirty.iter().copied().collect();
    // The feed is drained before every plan, and every loaded node brings its ancestors.
    b.input.remote_complete = true;
    // Mostly none: one query. In this folder's coordinates, as the rest.
    let coords = s.coords()?;
    b.input.unreadable = s
        .unreadable_nodes()?
        .into_iter()
        .map(|n| coords.from_drive(n))
        .collect();

    for &node in dirty {
        b.load(node)?;
    }
    // The files not hashed yet, dirty or not, with what they are in on every side: what waits
    // for them waits in every view alike (sync semantics §6.3). Hardly ever any.
    for node in s.unhashed_files()? {
        b.load(node)?;
    }
    let mut coverage = Coverage::default();
    match full {
        None => {
            // Every child of every folder involved, and of the root.
            coverage.full.insert(NodeId::ROOT);
            for &node in dirty {
                for tree in [&b.input.base, &b.input.remote, &b.input.local] {
                    if let Some(state) = tree.get(node) {
                        coverage.full.insert(state.parent);
                        if state.kind == NodeKind::Folder {
                            coverage.full.insert(node);
                        }
                    }
                }
            }
        }
        Some(full) => {
            // In each folder a dirty node is in, what may clash with its names; all of a dirty
            // folder that may go (gone on a side, hidden, or in a folder gone remotely).
            coverage.full.extend(full);
            let hidden = crate::mask::hidden(&b.input.remote, filter, scope);
            for &node in dirty {
                let mut folder = false;
                for tree in [&b.input.base, &b.input.remote, &b.input.local] {
                    if let Some(state) = tree.get(node) {
                        coverage
                            .named
                            .entry(state.parent)
                            .or_default()
                            .insert(hbsync_namemap::sibling_hash(&state.name));
                        folder |= state.kind == NodeKind::Folder;
                    }
                }
                if folder && crate::pages::may_go(&b.input, node, &hidden) {
                    coverage.full.insert(node);
                }
            }
            let Coverage { full, named } = &mut coverage;
            named.retain(|folder, keys| {
                if keys.len() > NAMED_MAX {
                    full.insert(*folder);
                }
                !full.contains(folder)
            });
            // A parked item is at its original name (`parked_place`), which its row's hash
            // isn't: those in the folders read by name come along (there are hardly ever any).
            for node in s.all_parked()? {
                if s.local_parent(node)?
                    .is_some_and(|p| coverage.named.contains_key(&p))
                {
                    b.load(node)?;
                }
            }
        }
    }
    let mut folders: Vec<NodeId> = coverage.full.iter().copied().collect();
    folders.sort();
    for folder in folders {
        b.load_children(folder)?;
    }
    let mut named: Vec<(NodeId, Vec<i32>)> = coverage
        .named
        .iter()
        .map(|(folder, keys)| {
            let mut keys: Vec<i32> = keys.iter().copied().collect();
            keys.sort();
            (*folder, keys)
        })
        .collect();
    named.sort();
    for (folder, keys) in named {
        b.load_named(folder, &keys)?;
    }
    // A folder that is gone remotely (or masked) but present locally may be trashed or
    // detached: load all of it.
    // Each folder once: a dirty folder inside one already loaded is in it.
    let hidden = crate::mask::hidden(&b.input.remote, filter, scope);
    let mut loaded: HashSet<NodeId> = HashSet::new();
    for &node in dirty {
        if !loaded.contains(&node)
            && b.input.base.contains(node)
            && (!b.input.remote.contains(node) || hidden.contains(&node))
            && let Some(row) = s
                .local_by_node(node)?
                .filter(|r| r.kind == NodeKind::Folder)
        {
            for child in s.local_subtree(&row.path)? {
                loaded.insert(child.node);
                b.load(child.node)?;
            }
        }
    }
    // Whether a masked folder synced here leaves the drive depends on everything in it
    // remotely (§10): load all of it.
    let masked = crate::mask::masked(&b.input.remote, filter);
    let mut expanded: HashSet<NodeId> = HashSet::new();
    for &node in dirty {
        if !(masked.contains(&node) && b.input.base.contains(node)) {
            continue;
        }
        let mut folders = vec![node];
        while let Some(folder) = folders.pop() {
            if b.input.remote.get(folder).map(|s| s.kind) != Some(NodeKind::Folder)
                || !expanded.insert(folder)
            {
                continue;
            }
            if coverage.full.insert(folder) {
                coverage.named.remove(&folder);
            }
            b.load_children(folder)?;
            folders.extend(b.input.remote.children(folder));
        }
    }
    // A role folder at the top that this device holds nothing of (always, but for a bug) isn't
    // the plan's at all: nothing to mask. One moved into a folder stays, masked, and keeps that
    // folder alive (§6.3).
    let top: Vec<NodeId> = scope
        .outside
        .iter()
        .copied()
        .filter(|n| b.input.remote.get(*n).is_some_and(|s| s.parent.is_root()))
        .collect();
    let held = |n: &NodeId| b.input.base.contains(*n) || b.input.local.contains(*n);
    for folder in top {
        let subtree = b.input.remote.subtree(folder);
        if !subtree.iter().any(held) {
            for node in subtree {
                b.input.remote.remove(node);
            }
        }
    }
    // A remote node whose folder isn't in the mirror, or is a file, may be an orphan, which the main folder
    // puts into Recovered files: where that is (live, removed or synced here before; loaded
    // after the role folders held nothing of are dropped: one holding it masks it), and what
    // may clash with the names coming, in it and, for its own, at the top (the plan asks for
    // the rest: `Plan::uncovered`).
    let missing = looked_up_removed(&b.input.remote, scope.top);
    if missing.contains(&NodeId::RECOVERED) {
        b.load(NodeId::RECOVERED)?;
        match full {
            None => b.load_children(NodeId::RECOVERED)?,
            Some(_) => {
                let remote = &b.input.remote;
                let coming: BTreeSet<i32> = remote
                    .iter()
                    .filter(|(_, st)| missing.contains(&st.parent) || in_a_file(remote, st))
                    .map(|(_, st)| hbsync_namemap::sibling_hash(&st.name))
                    .collect();
                let own = BTreeSet::from([hbsync_namemap::sibling_hash(RECOVERED_NAME)]);
                for (folder, keys) in [(NodeId::RECOVERED, coming), (NodeId::ROOT, own)] {
                    if coverage.full.contains(&folder) {
                        continue;
                    }
                    let keys: Vec<i32> = keys
                        .into_iter()
                        .filter(|k| coverage.named.entry(folder).or_default().insert(*k))
                        .collect();
                    b.load_named(folder, &keys)?;
                }
            }
        }
    }
    // The folders in Recently deleted that remote nodes are in, and the removed ones above them
    // in this folder's part of the drive: what a folder's delete took along goes by their times
    // (node schema, Recently deleted). And the live folder each such chain reaches, where it
    // reaches this folder's top through live folders none of which it leaves out: without it a
    // narrow view read the removed folder as gone for good, and what was written into it since
    // as an orphan (for Recovered files); with the names in it the removed one may take back.
    // That folder's own folders may be in Recently deleted too: until none new is missing.
    let mut looked: HashSet<NodeId> = HashSet::new();
    let mut left_out: HashSet<NodeId> = HashSet::new();
    let mut missing = missing;
    while !missing.is_empty() {
        looked.extend(missing.iter().copied());
        for start in missing {
            let mut folder = start;
            let mut coming = None;
            while !b.input.removed.contains_key(&folder)
                && !scope.outside.contains(&folder)
                && let Some(r) = s.removed_by_node(folder)?
                && let Some(state) = r.row.state()
            {
                coming = Some(hbsync_namemap::sibling_hash(&state.name));
                b.input.removed.insert(
                    folder,
                    Removed {
                        at: r.time(),
                        state,
                    },
                );
                folder = r.row.parent;
            }
            let Some(coming) = coming else {
                continue;
            };
            match reach(s, folder, scope)? {
                Reach::Top if !folder.is_root() => {
                    b.load(folder)?;
                    if full.is_some()
                        && !coverage.full.contains(&folder)
                        && coverage.named.entry(folder).or_default().insert(coming)
                    {
                        b.load_named(folder, &[coming])?;
                    }
                }
                Reach::Top | Reach::Gone => {}
                Reach::Outside => {
                    left_out.insert(start);
                }
            }
        }
        missing = looked_up_removed(&b.input.remote, scope.top)
            .into_iter()
            .filter(|m| !looked.contains(m))
            .collect();
    }
    // What lies in a removed folder in a part of the drive this folder leaves out (a role
    // folder's) isn't the plan's, as a role folder this device holds nothing of isn't: unless
    // it holds some of it, which stays as it was.
    let held = |n: &NodeId| b.input.base.contains(*n) || b.input.local.contains(*n);
    let below: Vec<NodeId> = b
        .input
        .remote
        .iter()
        .filter(|(_, st)| left_out.contains(&st.parent))
        .map(|(n, _)| n)
        .collect();
    for node in below {
        let subtree = b.input.remote.subtree(node);
        if !subtree.iter().any(held) {
            for n in subtree {
                b.input.remote.remove(n);
            }
        }
    }
    // What the page leaves to other pages (§6.5): nothing where it is all there is to plan
    // (a count; the page's nodes are dirty marks).
    if s.pending_count()? > b.input.dirty.len() as u64 {
        crate::pages::dirty_elsewhere(&mut b.input, filter, scope, &mut StorePending(s))?;
    }
    if full.is_some() {
        b.input.coverage = Some(coverage);
    }
    Ok(b.input)
}

/// Where a walk up from `folder` through the drive's folders, live or in Recently deleted, ends.
enum Reach {
    /// At this folder's top.
    Top,
    /// At a folder this folder leaves out (a role folder's, for the main folder).
    Outside,
    /// At a folder gone for good, or nowhere (a cycle, too deep).
    Gone,
}

/// [`Reach`] of `folder`.
fn reach(s: &ReadTx<'_>, folder: NodeId, scope: &Scope) -> Result<Reach, StateError> {
    let mut outside = false;
    let up = crate::walk::up::<_, StateError>(folder, |at| {
        if scope.outside.contains(&at) {
            outside = true;
            return Ok(None);
        }
        if let Some(m) = s.get_mirror(at)? {
            return Ok(Some(((), m.parent)));
        }
        Ok(s.removed_by_node(at)?.map(|r| ((), r.row.parent)))
    })?;
    Ok(match up.end {
        End::Top => Reach::Top,
        _ if outside => Reach::Outside,
        _ => Reach::Gone,
    })
}

/// The folders whose rows in Recently deleted a plan of `remote` looks up, in the folder whose
/// top is `top`: those remote nodes are in that aren't in it, and Recovered files where one may
/// be an orphan the main folder adopts (sync semantics §6.3): one whose folder is missing, or
/// is a file (another writer's doing). One pass.
pub fn looked_up_removed(remote: &crate::tree::Tree, top: NodeId) -> HashSet<NodeId> {
    let mut missing = HashSet::new();
    let mut orphans = false;
    for (_, state) in remote.iter() {
        match remote.get(state.parent) {
            None if !state.parent.is_root() => {
                missing.insert(state.parent);
                orphans = true;
            }
            Some(p) if p.kind == NodeKind::File => orphans = true,
            _ => {}
        }
    }
    if top.is_root() && orphans {
        missing.insert(NodeId::RECOVERED);
    }
    missing
}

/// Whether `state`'s folder in `remote` is a file (another writer's doing): an orphan.
fn in_a_file(remote: &crate::tree::Tree, state: &NodeState) -> bool {
    remote
        .get(state.parent)
        .is_some_and(|p| p.kind == NodeKind::File)
}

/// Whether the mirror no longer has `node` as the plan reads it: not there, or taken along by
/// the folder in Recently deleted its folders reach first in this folder's part of the drive
/// (the rule of [`crate::along::taken_along`], over the live folders between and what is in
/// `node`; one outside, as the view leaves it out, isn't this folder's to bring back). A removed
/// file takes nothing along (§6.3).
pub(crate) fn gone_on_drive(
    s: &ReadTx<'_>,
    node: NodeId,
    scope: &Scope,
) -> Result<bool, StateError> {
    let Some(row) = s.get_mirror(node)? else {
        return Ok(true);
    };
    let coords = s.coords()?;
    let Some((folder, at)) = s.removed_above(coords.to_drive(row.parent), coords.top)? else {
        return Ok(false);
    };
    let removed = coords.from_drive(folder);
    if scope.outside.contains(&removed)
        || s.removed_by_node(folder)?
            .is_none_or(|r| r.row.kind != NodeKind::Folder)
    {
        return Ok(false);
    }
    // What the rule reads: `node`, the live folders up to the removed one, and what is in
    // `node` (a later write in it keeps it).
    let mut live: Vec<(NodeId, NodeId)> = vec![(node, row.parent)];
    let mut changed: std::collections::HashMap<NodeId, Option<i64>> =
        std::collections::HashMap::from([(node, row.changed_at)]);
    let mut up = row.parent;
    while up != removed && changed.len() < hbsync_state::MAX_DEPTH {
        let Some(r) = s.get_mirror(up)? else {
            break;
        };
        live.push((up, r.parent));
        changed.insert(up, r.changed_at);
        up = r.parent;
    }
    let mut below = vec![node];
    while let Some(folder) = below.pop() {
        for child in s.mirror_children(folder)? {
            if changed.insert(child.node, child.changed_at).is_none() {
                live.push((child.node, folder));
                below.push(child.node);
            }
        }
    }
    let removal = hbsync_types::removal_time(at);
    let taken = crate::along::taken_along(
        live,
        |f| (f == removed).then_some(removal),
        |n| changed.get(&n).copied().flatten(),
    );
    Ok(taken.contains(&node))
}

struct Builder<'s, 'c> {
    s: &'s ReadTx<'c>,
    os: Os,
    input: Input,
    base_done: HashSet<NodeId>,
    mirror_done: HashSet<NodeId>,
    local_done: HashSet<NodeId>,
}

impl Builder<'_, '_> {
    /// Loads a node on all three sides, and its ancestors.
    fn load(&mut self, node: NodeId) -> Result<(), StateError> {
        let mut queue = VecDeque::from([node]);
        while let Some(n) = queue.pop_front() {
            if n.is_root() {
                continue;
            }
            if self.base_done.insert(n)
                && let Some(row) = self.s.get_base(n)?
            {
                queue.push_back(row.state.parent);
                self.add_base(row);
            }
            if self.mirror_done.insert(n)
                && let Some(row) = self.s.get_mirror(n)?
            {
                queue.push_back(row.parent);
                self.add_mirror(row);
            }
            if self.local_done.insert(n)
                && let Some(row) = self.s.local_by_node(n)?
            {
                if let Some(parent) = self.s.local_parent(n)? {
                    queue.push_back(parent);
                }
                self.add_local(row)?;
            }
        }
        Ok(())
    }

    /// Loads the children of `folder` (which is loaded) on every side. The rows the listings
    /// return are used directly; `load` then only reads the sides they didn't cover.
    fn load_children(&mut self, folder: NodeId) -> Result<(), StateError> {
        self.load_kids(folder, None)
    }

    /// Loads the children of `folder` (which is loaded) whose names have one of the sibling
    /// hashes `keys`, on every side.
    fn load_named(&mut self, folder: NodeId, keys: &[i32]) -> Result<(), StateError> {
        self.load_kids(folder, Some(keys))
    }

    /// The children of `folder` on every side: all, or those with one of `keys`.
    fn load_kids(&mut self, folder: NodeId, keys: Option<&[i32]>) -> Result<(), StateError> {
        let s = self.s;
        let path = if folder.is_root() {
            Some(RelPath::root())
        } else {
            s.local_by_node(folder)?.map(|r| r.path)
        };
        let (mut base, mut mirror, mut local) = (Vec::new(), Vec::new(), Vec::new());
        match keys {
            None => {
                base = s.base_children(folder)?;
                mirror = s.mirror_children(folder)?;
                if let Some(path) = &path {
                    local = s.local_children(path)?;
                }
            }
            Some(keys) => {
                for &key in keys {
                    base.extend(s.base_named(folder, key)?);
                    mirror.extend(s.mirror_named(folder, key)?);
                    if let Some(path) = &path {
                        local.extend(s.local_named(path, key)?);
                    }
                }
            }
        }
        let mut kids: Vec<NodeId> = Vec::new();
        for row in base {
            kids.push(row.node);
            if self.base_done.insert(row.node) {
                self.add_base(row);
            }
        }
        for row in mirror {
            kids.push(row.node);
            if self.mirror_done.insert(row.node) {
                self.add_mirror(row);
            }
        }
        for row in local {
            kids.push(row.node);
            if self.local_done.insert(row.node) {
                self.add_local(row)?;
            }
        }
        for kid in kids {
            self.load(kid)?;
        }
        Ok(())
    }

    fn add_base(&mut self, row: BaseRow) {
        self.input.base_extra.insert(
            row.node,
            BaseExtra {
                local_hash: row.local_hash,
                content_state: row.content_state,
                version: Some(row.remote.version),
            },
        );
        self.input.base.insert(row.node, row.state);
    }

    fn add_mirror(&mut self, row: MirrorRow) {
        self.input.remote_refs.insert(row.node, row.remote_ref());
        if let Some(at) = row.changed_at {
            self.input.changed_at.insert(row.node, at);
        }
        if let Some(state) = row.state() {
            self.input.remote.insert(row.node, state);
        }
    }

    fn add_local(&mut self, row: LocalRow) -> Result<(), StateError> {
        let node = row.node;
        // The drive's tags, from the mirror row loaded already, or else read for them alone
        // (loading it here would skip its ancestors).
        let remote_tags = match self.input.remote.get(node) {
            Some(r) => Some(r.tags.clone()),
            None if self.mirror_done.contains(&node) => None,
            None => self.s.get_mirror(node)?.map(|m| m.tags()),
        };
        match local_state(self.s, &row, self.os, remote_tags.as_ref())? {
            Some(state) => {
                if state.kind == NodeKind::File && state.content.is_none() {
                    self.input.local_unhashed.insert(node);
                }
                self.input.local.insert(node, state);
            }
            // The scan hasn't recorded the parent yet: decide later.
            None => {
                self.input.local_unhashed.insert(node);
            }
        }
        Ok(())
    }
}

/// The state DB as [`crate::pages::dirty_elsewhere`] asks it.
struct StorePending<'a, 'b>(&'a ReadTx<'b>);

impl crate::pages::Pending for StorePending<'_, '_> {
    type Error = StateError;
    fn among(&mut self, nodes: &[NodeId]) -> Result<HashSet<NodeId>, StateError> {
        self.0.pending_among(nodes)
    }
    fn below(&mut self, folder: NodeId, page: &BTreeSet<NodeId>) -> Result<bool, StateError> {
        // One beyond the page's nodes is enough to tell.
        let under = self.0.dirty_under(folder, page.len() + 1)?;
        Ok(under.iter().any(|n| !page.contains(n)))
    }
}
