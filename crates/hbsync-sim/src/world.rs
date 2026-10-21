//! What the users, other devices and the server do, and how each folder sees the drive.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use hbsync_engine::deleted;
use hbsync_engine::mask::masked;
use hbsync_engine::tree::Tree;
use hbsync_engine::walk::End;
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_local::Filter;
use hbsync_platform::Os;
use hbsync_platform::name::name_key;
use hbsync_platform::tags::express;
use hbsync_state::Coords;
use hbsync_types::{
    FileContent, NodeId, NodeKind, NodeState, RemoteRef, Role, Tag, Tags, VersionTag,
};

use crate::Sim;
use crate::action::{Action, JUNK, NAMES, TAGS};
use crate::model::client::{Client, ignoring_d};
use crate::model::server::{Server, ServerNode};
use crate::oracle::Seen;
use crate::tagbook::Limit;

impl Sim {
    /// The server as client `c`'s folder sees it ([`Coords`]).
    pub(crate) fn server_view(&self, c: usize) -> Tree {
        let mut t = Tree::new();
        for id in self.server.live.keys() {
            if let Some(state) = self.server_state(c, *id) {
                t.insert(*id, state);
            }
        }
        if self.clients[c].coords != Coords::MAIN {
            t.insert(
                NodeId::OUTSIDE,
                NodeState::folder(NodeId::ROOT, Coords::OUTSIDE_NAME),
            );
        }
        t
    }

    /// The server's state of `node` as client `c` sees it.
    pub(crate) fn server_state(&self, c: usize, node: NodeId) -> Option<NodeState> {
        let coords = self.clients[c].coords;
        let n = self
            .server
            .live
            .get(&node)
            .filter(|_| !coords.hides(node) && !self.server.unreadable.contains_key(&node))?;
        Some(NodeState {
            parent: coords.from_drive(n.state.parent),
            ..n.state.clone()
        })
    }

    /// Whether drive node `node` is known to lie outside client `c`'s folder's part of the
    /// drive: in the role folder for the main folder, under the drive's top elsewhere for the
    /// role folder. A node whose place isn't known (a gap, or a cycle a sync's moves make for a
    /// moment) isn't.
    pub(crate) fn known_outside(&self, c: usize, node: NodeId) -> bool {
        match self.clients[c].coords.top {
            top if top.is_root() => self.server.in_role_folder(node),
            _ => self.server.place(node) == End::Top,
        }
    }

    /// The phone deletes the role folder with everything in it, to Recently deleted.
    fn delete_role_folder(&mut self) {
        let docs = Role::Documents.node();
        if !self.server.live.contains_key(&docs) {
            return;
        }
        let mut below: Vec<NodeId> = self
            .server
            .live
            .keys()
            .copied()
            .filter(|n| self.server.in_role_folder(*n))
            .collect();
        // The folder first, then what is in it, top-down (node schema, Recently deleted).
        let tree = self.server.tree();
        below.sort_by_key(|n| tree.ancestors(*n).len());
        self.remove_live([docs].into_iter().chain(below), true);
    }

    /// Moves `nodes`, in order, from the tree to Recently deleted, on the server at once;
    /// `rightly`: as a user deleted them ([`Sim::remove_on_drive`]). A tag change loses to a
    /// delete (§11).
    fn remove_live(&mut self, nodes: impl IntoIterator<Item = NodeId>, rightly: bool) {
        for node in nodes {
            if let Some(n) = self.server.live.remove(&node) {
                self.remove_on_drive(node, n, rightly);
                self.deleted_now(node);
            }
        }
    }

    /// Puts `node`, just taken out of the drive's tree as `n`, in Recently deleted: a write, at
    /// its time. For the oracle: what is in it then (its removal takes that
    /// along), and whether it goes `rightly`: a user deleted it (here, on a device, or in
    /// another app), or a delete took it along (only then may its blob go for good).
    pub(crate) fn remove_on_drive(&mut self, node: NodeId, n: ServerNode, rightly: bool) {
        // A file too may hold nodes (another app moved them into it: `Action::IntoFile`): its
        // removal takes them along as a folder's does, by the same rule.
        if rightly {
            // What is in it, through live folders (`node` is out of the tree already).
            let mut kids: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
            for (child, c) in &self.server.live {
                kids.entry(c.state.parent).or_default().push(*child);
            }
            let mut below = BTreeMap::new();
            let mut folders = vec![node];
            while let Some(f) = folders.pop() {
                for child in kids.get(&f).into_iter().flatten() {
                    below.insert(*child, self.server.live[child].remote.version);
                    folders.push(*child);
                }
            }
            self.ledger.folder_removed(node, below);
        }
        self.server.remove(node, n);
        let version = self.server.removed[&node].remote.version;
        self.ledger.row_removed((node, version), rightly);
    }

    /// The folders [`Action::DeleteStopped`] picks from: the drive's, the role folder too.
    pub(crate) fn stopped_folders(&self) -> Vec<NodeId> {
        self.server
            .live
            .iter()
            .filter(|(_, s)| s.state.kind == NodeKind::Folder)
            .map(|(n, _)| *n)
            .collect()
    }

    /// The folder `pick` picks of [`Sim::stopped_folders`], if the drive holds one.
    fn pick_folder(&self, pick: usize) -> Option<NodeId> {
        let folders = self.stopped_folders();
        folders.get(pick % folders.len().max(1)).copied()
    }

    /// [`Action::DeleteOutright`].
    fn delete_outright(&mut self, pick: usize) {
        if let Some(folder) = self.pick_folder(pick) {
            // Gone for good, as the purge's deletes: what an earlier removal kept goes too.
            self.server.live.remove(&folder);
            self.server.removed_at.remove(&folder);
            self.server.tags_at_removal.remove(&folder);
            self.deleted_now(folder);
        }
    }

    /// What [`Action::IntoFile`] picks from: the drive's live nodes and files outside the role
    /// folder, the fixed folders left out. The role folder's delete (the phone's) and restore
    /// would take a node in a file along and leave it: Recently deleted has no rule for what is
    /// in a file (a question for the purge's rules, not modelled).
    fn file_move_candidates(&self) -> (Vec<NodeId>, Vec<NodeId>) {
        let server = &self.server;
        let newer = |n: &NodeId| {
            server
                .unreadable
                .iter()
                .any(|(f, kids)| f == n || kids.contains(n))
        };
        let free = |n: &NodeId| {
            *n != NodeId::RECOVERED
                && Role::of(*n).is_none()
                && !server.in_role_folder(*n)
                && !newer(n)
        };
        let nodes = server.live.keys().filter(|n| free(n)).copied().collect();
        let files = server
            .live
            .iter()
            .filter(|(n, s)| s.state.kind == NodeKind::File && free(n))
            .map(|(n, _)| *n)
            .collect();
        (nodes, files)
    }

    /// For scripted tests: the picks of [`Action::IntoFile`] that move `node` into `file`.
    pub fn file_move_picks(&self, node: NodeId, file: NodeId) -> Option<(usize, usize)> {
        let (nodes, files) = self.file_move_candidates();
        let at = |list: &[NodeId], n: NodeId| list.iter().position(|m| *m == n);
        Some((at(&nodes, node)?, at(&files, file)?))
    }

    /// [`Action::NewerFolder`].
    fn newer_folder(&mut self, name: usize) {
        let name = NAMES[name % NAMES.len()];
        if self
            .server
            .tree()
            .child_named(NodeId::ROOT, name, None)
            .is_some()
        {
            return;
        }
        let folder = self.ids.fresh();
        self.server
            .put(folder, NodeState::folder(NodeId::ROOT, name));
        let file = self.ids.fresh();
        let state = NodeState::file(folder, NAMES[0], self.new_blob());
        self.server.put(file, state);
        self.server.unreadable.insert(folder, [file].into());
    }

    /// [`Action::IntoFile`].
    fn move_into_file(&mut self, pick: usize, file: usize) {
        let (nodes, files) = self.file_move_candidates();
        let (Some(&node), Some(&into)) = (
            nodes.get(pick % nodes.len().max(1)),
            files.get(file % files.len().max(1)),
        ) else {
            return;
        };
        if node == into {
            return;
        }
        let version = VersionTag(self.server.uuid());
        if let Some(n) = self.server.live.get_mut(&node) {
            n.state.parent = into;
            n.remote.version = version;
        }
    }

    /// [`Action::DeleteStopped`].
    fn delete_stopped(&mut self, pick: usize, done: usize) {
        let Some(folder) = self.pick_folder(pick) else {
            return;
        };
        let tree = self.server.tree();
        let mut below: Vec<NodeId> = tree
            .subtree(folder)
            .into_iter()
            .filter(|n| *n != folder)
            .collect();
        below.sort_by_cached_key(|n| (tree.ancestors(*n).len(), *n));
        below.truncate(done % (below.len() + 1));
        self.remove_live([folder].into_iter().chain(below), true);
    }

    /// Another device (the phone) changes a file in the role folder: `op` picks adding, editing,
    /// removing (to Recently deleted) or moving one to the top, under a free name.
    fn phone(&mut self, op: usize, pick: usize) {
        let docs = Role::Documents.node();
        // Only in a live role folder: the file browser doesn't add to a deleted one.
        if !self.server.live.contains_key(&docs) {
            return;
        }
        let files: Vec<NodeId> = self
            .server
            .live
            .iter()
            .filter(|(n, s)| s.state.kind == NodeKind::File && self.server.in_role_folder(**n))
            .map(|(n, _)| *n)
            .collect();
        let tree = self.server.tree();
        let free = |parent: NodeId, name: &str| tree.child_named(parent, name, None).is_none();
        let version = VersionTag(self.server.uuid());
        match (op % 4, files.get(pick % files.len().max(1)).copied()) {
            (0, _) | (_, None) => {
                let name = NAMES[pick % NAMES.len()];
                if free(docs, name) {
                    let state = NodeState::file(docs, name, self.new_blob());
                    self.server.put(self.ids.fresh(), state);
                }
            }
            (1, Some(file)) => {
                let content = self.new_blob();
                let old = self.server.live.get(&file).and_then(|n| n.state.content);
                self.ledger.blob_superseded(old);
                if let Some(n) = self.server.live.get_mut(&file) {
                    n.state.content = Some(content);
                    n.remote.version = version;
                }
            }
            (2, Some(file)) => {
                if let Some(n) = self.server.live.remove(&file) {
                    self.remove_on_drive(file, n, true);
                    // A tag change loses to a delete (§11), the phone's too.
                    self.deleted_now(file);
                }
            }
            (_, Some(file)) => {
                if free(NodeId::ROOT, &self.server.live[&file].state.name)
                    && let Some(n) = self.server.live.get_mut(&file)
                {
                    n.state.parent = NodeId::ROOT;
                    n.remote.version = version;
                }
            }
        }
    }

    pub(crate) fn new_blob(&mut self) -> FileContent {
        self.next_blob += 1;
        let blob = self.next_blob;
        let hash = hash_bytes(&blob.to_le_bytes(), HashAlgorithm::Blake3);
        self.ledger.blob_written(blob, hash);
        FileContent {
            hash,
            size: 8,
            mtime_ms: blob as i64,
            executable: false,
        }
    }

    /// The folders a create's `parent` picks from on client `c` (its root included).
    pub(crate) fn folders_of(&self, c: usize) -> Vec<NodeId> {
        let mut folders: Vec<NodeId> = vec![NodeId::ROOT];
        folders.extend(
            self.clients[c]
                .local
                .iter()
                .filter(|(_, s)| s.kind == NodeKind::Folder)
                .map(|(id, _)| id),
        );
        folders.sort();
        folders
    }

    /// The items an action's `node` picks from on client `c`.
    pub(crate) fn items_of(&self, c: usize) -> Vec<NodeId> {
        let mut all: Vec<NodeId> = self.clients[c].local.ids().collect();
        all.sort();
        all
    }

    /// The files an action's `file` picks from on client `c`.
    pub(crate) fn files_of(&self, c: usize) -> Vec<NodeId> {
        let mut files: Vec<NodeId> = self.clients[c]
            .local
            .iter()
            .filter(|(_, s)| s.kind == NodeKind::File)
            .map(|(id, _)| id)
            .collect();
        files.sort();
        files
    }

    pub(crate) fn act(&mut self, c: usize, action: Action) {
        let folders = self.folders_of(c);
        let all = self.items_of(c);
        let files = self.files_of(c);
        let local = &self.clients[c].local;
        let name_free = |t: &Tree, parent: NodeId, name: &str, except: Option<NodeId>| {
            t.child_named(parent, name, except).is_none()
        };
        match action {
            Action::Tag { node, tag } | Action::Colour { node, tag } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                let (name, colour) = TAGS[tag % TAGS.len()];
                let os = self.clients[c].os;
                let Some(mut state) = local.get(node).cloned() else {
                    return;
                };
                let mut tags: Vec<Tag> = state.tags.iter().cloned().collect();
                let had = tags.iter().position(|t| t.name == name);
                match (action, had) {
                    (Action::Colour { .. }, Some(i)) if os == Os::MacOs => {
                        tags[i].colour = (tags[i].colour + 1) % 8;
                    }
                    (Action::Tag { .. }, Some(i)) => {
                        tags.remove(i);
                        let synced = self.clients[c]
                            .base
                            .get(node)
                            .is_some_and(|b| b.tags.get(name).is_some());
                        let now = synced.then(|| self.server.now());
                        self.ledger.tags.remove(c, node, name, now);
                    }
                    (Action::Tag { .. }, None) => {
                        let Some(tag) = express(&Tag::new(name, colour), os) else {
                            return; // this OS can't hold it
                        };
                        tags.push(tag);
                        let (concurrent, limit) = self.removal_unseen(c, node, name);
                        self.ledger.tags.add(c, node, name, concurrent, limit);
                    }
                    _ => return,
                }
                state.tags = Tags::new(tags);
                self.clients[c].local.insert(node, state);
            }
            Action::Restore { node } => self.restore(node),
            Action::Phone { op, pick } => self.phone(op, pick),
            Action::DeleteRoleFolder => self.delete_role_folder(),
            Action::DeleteStopped { pick, done } => self.delete_stopped(pick, done),
            Action::DeleteOutright { pick } => self.delete_outright(pick),
            Action::IntoFile { pick, file } => self.move_into_file(pick, file),
            Action::NewerFolder { name } => self.newer_folder(name),
            Action::MoveAcross { file } => {
                let Some(&(a, b)) = self.computers.iter().find(|(a, b)| *a == c || *b == c) else {
                    return;
                };
                let to = if a == c { b } else { a };
                if files.is_empty() {
                    return;
                }
                let node = files[file % files.len()];
                let Some(state) = self.clients[c].local.get(node).cloned() else {
                    return;
                };
                // Not one the executor holds under a temporary name; to the other folder's top,
                // if its name is free there.
                if self.clients[c].parked.contains_key(&node)
                    || !name_free(&self.clients[to].local, NodeId::ROOT, &state.name, None)
                {
                    return;
                }
                self.clients[c].local.remove(node);
                self.user_removed(c, node);
                // The other folder's scan finds an item it never had: a new id. Its tags went
                // along.
                let new = self.ids.fresh();
                self.clients[to].local.insert(
                    new,
                    NodeState {
                        parent: NodeId::ROOT,
                        ..state
                    },
                );
                self.retrack(c, node, new);
                self.ledger.tags.moved(c, to, new);
            }
            Action::ListDeleted => {
                let rows = self.server.removed.iter().map(Server::at).collect();
                self.ledger.deleted_listed(rows);
            }
            Action::EmptyListed => self.empty_listed(),
            Action::EmptyItem { pick } => self.empty_item(pick),
            Action::EditTaken { pick } => {
                let taken: Vec<NodeId> = self
                    .taken_along()
                    .into_iter()
                    .filter(|n| self.server.live[n].state.kind == NodeKind::File)
                    .collect();
                let Some(&file) = taken.get(pick % taken.len().max(1)) else {
                    return;
                };
                let content = self.new_blob();
                let version = VersionTag(self.server.uuid());
                let old = self.server.live.get(&file).and_then(|n| n.state.content);
                self.ledger.blob_superseded(old);
                if let Some(n) = self.server.live.get_mut(&file) {
                    n.state.content = Some(content);
                    n.remote.version = version;
                }
            }
            Action::Purge => {
                // The engine's pick, judged by what happened now (one taken along whose folder
                // is back isn't any more): one no delete took along is removed wrongly, which
                // the check reports.
                let taken = self.server.purge();
                let truly = self.taken_along();
                for n in taken {
                    self.remove_live([n], truly.contains(&n));
                }
                let due = self.server.due();
                // What was removed since the cut-off may not have reached the purging device.
                let before = self.server.purge_before();
                let known: BTreeSet<(NodeId, VersionTag)> = self
                    .server
                    .removed
                    .iter()
                    .filter(|(n, _)| self.server.removed_at.get(n).is_some_and(|at| *at < before))
                    .map(Server::at)
                    .collect();
                self.delete_for_good(&due, &known);
            }
            Action::ToggleIgnore => {
                let client = &mut self.clients[c];
                client.filter = Arc::new(if client.filter.rule_count() == 0 {
                    ignoring_d()
                } else {
                    Filter::default()
                });
            }
            Action::CreateJunk { parent } => {
                let parent = folders[parent % folders.len()];
                if name_free(local, parent, JUNK, None) {
                    let state = NodeState::file(parent, JUNK, self.new_blob());
                    self.clients[c].local.insert(self.ids.fresh(), state);
                }
            }
            Action::CreateFile { parent, name } | Action::CreateFolder { parent, name } => {
                let parent = folders[parent % folders.len()];
                let name = NAMES[name % NAMES.len()];
                if !name_free(local, parent, name, None) {
                    return;
                }
                let state = if matches!(action, Action::CreateFile { .. }) {
                    NodeState::file(parent, name, self.new_blob())
                } else {
                    NodeState::folder(parent, name)
                };
                self.clients[c].local.insert(self.ids.fresh(), state);
            }
            Action::Edit { file } => {
                if files.is_empty() {
                    return;
                }
                let node = files[file % files.len()];
                let mut state = local.get(node).cloned().unwrap_or_else(|| unreachable!());
                self.ledger.blob_superseded(state.content);
                state.content = Some(self.new_blob());
                self.clients[c].local.insert(node, state);
            }
            Action::Move { node, parent, name } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                let parent = folders[parent % folders.len()];
                let name = NAMES[name % NAMES.len()];
                if local.would_cycle(node, parent) || !name_free(local, parent, name, Some(node)) {
                    return;
                }
                let mut state = local.get(node).cloned().unwrap_or_else(|| unreachable!());
                state.parent = parent;
                state.name = name.to_owned();
                self.clients[c].local.insert(node, state);
            }
            Action::Delete { node } => {
                if all.is_empty() {
                    return;
                }
                let node = all[node % all.len()];
                for n in local.subtree(node) {
                    // What the server has goes to Recently deleted, and must be found there.
                    let content = self.clients[c].local.get(n).and_then(|s| s.content);
                    let on_server = self
                        .server
                        .live
                        .get(&n)
                        .is_some_and(|s| s.state.content == content);
                    if !on_server {
                        self.ledger.blob_superseded(content);
                    }
                }
                for n in self.clients[c].local.subtree(node) {
                    self.clients[c].local.remove(n);
                    self.deleted_here(c, n);
                }
            }
        }
    }

    fn restore(&mut self, pick: usize) {
        let Some(node) = self
            .server
            .removed
            .keys()
            .nth(pick % self.server.removed.len().max(1))
            .copied()
        else {
            return;
        };
        self.restore_with(node);
    }

    /// Brings `node` back from Recently deleted with what was deleted with it, and the folders
    /// above it in Recently deleted first (just those, outermost first), as the engine's restore
    /// does: in waves, each after the folders it comes back into, by the engine's own rule
    /// ([`deleted::restore_wave`]): first what takes its own name, free in its folder, then the
    /// rest under the first free restored name (it stays deleted if none is). One whose folder
    /// is gone for good comes back as it was (an orphan, for Recovered files); one whose folder
    /// didn't come back stays, with what is in it. It plans from the drive as it is: a device
    /// plans from its store, which may lag (a stale device's restore isn't simulated).
    pub(crate) fn restore_with(&mut self, node: NodeId) {
        let removed = &self.server.removed;
        let Some(top) = removed.get(&node) else {
            return;
        };
        // The folders above it in Recently deleted, outermost first (past any depth a path may
        // have, nothing comes back: above is still a removed folder, as the engine's).
        let Ok(up) = hbsync_engine::walk::up::<_, Infallible>(top.state.parent, |at| {
            Ok(removed.get(&at).map(|f| (at, f.state.parent)))
        });
        if up.end == End::Deep {
            return;
        }
        let mut nodes = up.items;
        nodes.reverse();
        // Then it and what was deleted with it ([`Server::group`]), each once.
        let mut seen: HashSet<NodeId> = nodes.iter().copied().collect();
        let group = self.server.group(node);
        nodes.extend(group.into_iter().filter(|n| seen.insert(*n)));
        let rows: Vec<Row> = nodes
            .iter()
            .map(|n| Row {
                node: *n,
                parent: removed[n].state.parent,
                name: removed[n].state.name.clone(),
            })
            .collect();
        // The folders in the tree the rows may go into, and the names taken in those and in
        // the folders coming back (one that is neither went for good: no name is taken there).
        let tree = self.server.tree();
        let mut live = HashSet::from([NodeId::ROOT]);
        let mut taken: HashMap<NodeId, HashSet<String>> = HashMap::new();
        for parent in rows.iter().map(|r| r.parent).chain([NodeId::ROOT]) {
            if taken.contains_key(&parent) {
                continue;
            }
            if self.server.live.contains_key(&parent) {
                live.insert(parent);
            } else if !parent.is_root() && !seen.contains(&parent) {
                continue;
            }
            let names = tree.children(parent).filter_map(|c| tree.get(c));
            taken.insert(parent, names.map(|c| name_key(&c.name)).collect());
        }
        let mut left: Vec<&Row> = rows.iter().collect();
        while let Some(wave) =
            deleted::restore_wave(left, |r| (r.parent, &r.name), &live, &seen, &mut taken)
        {
            left = wave.later;
            for row in wave.asked {
                self.restore_as(row.node, row.name.clone());
                live.insert(row.node);
            }
            for row in wave.rest {
                let Some(name) = deleted::take_restored_name(&mut taken, row.parent, &row.name)
                else {
                    continue; // no free name: it stays deleted
                };
                self.restore_as(row.node, name);
                live.insert(row.node);
            }
        }
    }

    /// Brings `node` back from Recently deleted, named `name`: a write.
    fn restore_as(&mut self, node: NodeId, name: String) {
        let Some(mut n) = self.server.removed.remove(&node) else {
            return;
        };
        n.state.name = name;
        n.remote.version = VersionTag(self.server.uuid());
        self.server.live.insert(node, n);
        self.restored_on_drive(node);
    }

    /// The user of client `c` took its item `node` out of what it syncs (a delete, a move to
    /// another folder, a filter rule): as they saw it, as the client's base has it.
    pub(crate) fn user_removed(&mut self, c: usize, node: NodeId) {
        let client = &self.clients[c];
        if let Some(b) = client.base.get(node) {
            let seen = (
                client.coords.to_drive(b.parent),
                b.name.clone(),
                b.content.map(|c| c.hash),
            );
            self.ledger.user_deleted(c, node, seen);
        }
    }

    /// Live `node` as the drive has it, but for its tags ([`Seen`]).
    ///
    /// [`Seen`]: crate::oracle::Seen
    pub(crate) fn on_drive(&self, node: NodeId) -> Option<Seen> {
        let n = &self.server.live.get(&node)?.state;
        Some((n.parent, n.name.clone(), n.content.map(|c| c.hash)))
    }

    /// `node` left the drive (a delete won): it settles the tag removals made before it.
    pub(crate) fn deleted_now(&mut self, node: NodeId) {
        self.ledger.tags.deleted_now(node);
    }

    /// The user of client `c` deleted `node` there: what they did to its tags there went with
    /// it, won or lost, but for an add sent up ([`Sim::tag_changes_left`]); other clients' tag
    /// changes stand until the delete wins.
    fn deleted_here(&mut self, c: usize, node: NodeId) {
        self.ledger.tags.deleted(node);
        self.user_removed(c, node);
        self.tag_changes_left(c, node);
    }

    /// Client `c`'s item `node` left it (deleted or trashed there): [`TagBook::left`].
    ///
    /// [`TagBook::left`]: crate::tagbook::TagBook::left
    pub(crate) fn tag_changes_left(&mut self, c: usize, node: NodeId) {
        self.ledger.tags.left(c, node);
    }

    /// Whether a removal of tag `name` from `node` is under way that client `c`'s add may lose
    /// to: on another client, not sent up yet (its base has the tag, its item doesn't, or a
    /// plan of its still sends the removal), or on the drive while `c`'s base still has it;
    /// and the known limit it meets where the removal is `c`'s own, planned and still to
    /// write the node ([`Limit::OwnRemovalInFlight`]).
    fn removal_unseen(&self, c: usize, node: NodeId, name: &str) -> (bool, Option<Limit>) {
        let has = |s: Option<&NodeState>| s.is_some_and(|s| s.tags.get(name).is_some());
        let lacks = |s: Option<&NodeState>| s.is_some_and(|s| s.tags.get(name).is_none());
        // A plan of a client's made from the removal, still to write the node.
        let planned = |client: &Client| {
            lacks(client.planned_local.get(node))
                && client.plan.iter().any(|(op, _)| op.node() == node)
        };
        // Of another client whose items can hold the tag at all: an item that lacks it, or a
        // plan sending its removal (the item gone since).
        let pending = self.clients.iter().enumerate().any(|(d, client)| {
            d != c
                && express(&Tag::new(name, 0), client.os).is_some()
                && has(client.base.get(node))
                && (lacks(client.local.get(node)) || planned(client))
        });
        let client = &self.clients[c];
        let synced = has(client.base.get(node));
        // On the drive, live or in Recently deleted (a restore brings that version back).
        let server = &self.server;
        let stale = lacks(
            server
                .live
                .get(&node)
                .or(server.removed.get(&node))
                .map(|n| &n.state),
        );
        let own = (synced && planned(client)).then_some(Limit::OwnRemovalInFlight);
        (pending || (synced && stale), own)
    }

    /// `node` is back on the drive (from Recently deleted, or made again after the purge).
    pub(crate) fn restored_on_drive(&mut self, node: NodeId) {
        let held = self
            .server
            .tags_at_removal
            .get(&node)
            .cloned()
            .unwrap_or_default();
        self.restored(node, &held);
    }

    /// `node` is back where a user deleted it (on the drive, or here where the delete lost)
    /// with the tags named `held`: no longer deleted, so what users do to its tags from now on
    /// counts. What they did before is settled by the delete: tag removals (the version back
    /// has what it had), and tags added are there only if it holds them; but for an add a
    /// client hasn't sent up and its item, synced there, still holds: that client never saw
    /// the delete (one that did lost the item, and the add with it), and sends the add up
    /// against the version back, as any add.
    fn restored(&mut self, node: NodeId, held: &BTreeSet<String>) {
        let clients = &self.clients;
        self.ledger.tags.restored(node, held, |(c, _, name)| {
            clients.get(*c).is_some_and(|client| {
                client
                    .local
                    .get(node)
                    .is_some_and(|item| item.tags.get(name).is_some())
                    && !masked(&client.local, &client.filter).contains(&node)
            })
        });
    }

    /// A client put `node` in place: where a user's delete of it lost (the drive has it), it is
    /// no longer deleted. A lost delete settles nothing: what other clients did to its tags
    /// still counts. One the drive doesn't have (a folder made here to restore it, its
    /// RemoteCreate to follow) is back once the drive has it again ([`Sim::restored_on_drive`]):
    /// a delete that won settles the tags then.
    pub(crate) fn placed_back(&mut self, c: usize, node: NodeId) {
        self.ledger.back(c, node);
        if self.server.live.contains_key(&node) {
            self.ledger.tags.back(node);
        }
    }

    /// [`Action::EmptyListed`].
    fn empty_listed(&mut self) {
        if let Some(listed) = self.ledger.listing().cloned() {
            self.delete_for_good(&listed, &listed);
        }
    }

    /// [`Action::EmptyItem`].
    fn empty_item(&mut self, pick: usize) {
        let Some(listed) = self.ledger.listing().cloned() else {
            return;
        };
        let rows: Vec<(NodeId, VersionTag)> = listed.iter().copied().collect();
        let Some(&(item, _)) = rows.get(pick % rows.len().max(1)) else {
            return;
        };
        let server = &self.server;
        if !server.removed.contains_key(&item) {
            return;
        }
        let folder = |n: NodeId| {
            server
                .removed
                .get(&n)
                .is_some_and(|r| r.state.kind == NodeKind::Folder)
        };
        let Ok(inside) = deleted::inside(
            item,
            |n| (*n, folder(*n)),
            |f| {
                Ok::<_, std::convert::Infallible>(
                    server
                        .removed
                        .iter()
                        .filter(|(_, r)| r.state.parent == f)
                        .map(|(n, _)| *n)
                        .collect(),
                )
            },
        );
        let chosen: BTreeSet<(NodeId, VersionTag)> = inside
            .iter()
            .filter_map(|n| server.removed.get_key_value(n).map(Server::at))
            .filter(|at| listed.contains(at))
            .collect();
        // Kept only by a live node in it, or a row in it written since the listing.
        let kept = chosen.len() < inside.len()
            || server
                .live
                .values()
                .any(|n| inside.contains(&n.state.parent));
        self.delete_for_good(&chosen, &listed);
        let stayed = !kept && self.server.removed.contains_key(&item);
        self.ledger
            .emptied_kept(|| stayed.then(|| format!("{item:?}")));
    }

    /// Deletes for good the rows of Recently deleted still at a version in `chosen`, as the
    /// engine does ([`deleted::passes`], with what the folders hold as the emptying device
    /// knows it: of Recently deleted, the rows in `known` at their versions), in its order: before a pass's folders go, the drive is asked which of them hold a
    /// file as the engine asks ([`deleted::before_folders_go`]), and those stay. Records the first it deletes that it mustn't: one not chosen at that version (a
    /// live node, say), or a folder holding anything not going after it.
    fn delete_for_good(
        &mut self,
        chosen: &BTreeSet<(NodeId, VersionTag)>,
        known: &BTreeSet<(NodeId, VersionTag)>,
    ) {
        let server = &self.server;
        let rows: Vec<deleted::Chosen> = server
            .removed
            .iter()
            .filter(|(n, r)| chosen.contains(&Server::at((n, r))))
            .map(|(n, r)| deleted::Chosen {
                node: *n,
                parent: r.state.parent,
                folder: r.state.kind == NodeKind::Folder,
            })
            .collect();
        let mut holdings = deleted::Holdings::default();
        for row in &server.removed {
            if known.contains(&Server::at(row)) {
                *holdings.removed.entry(row.1.state.parent).or_default() += 1;
            }
        }
        holdings.live = server
            .live
            .values()
            .map(|n| n.state.parent)
            .filter(|p| server.removed.contains_key(p))
            .collect();
        let passes = deleted::passes(&rows, &holdings);
        let mut pending: BTreeSet<NodeId> = passes.iter().flatten().copied().collect();
        for pass in passes {
            for n in &pass {
                pending.remove(n);
            }
            let server = &self.server;
            let later: HashSet<RemoteRef> =
                pending.iter().map(|n| server.removed[n].remote).collect();
            let holds = deleted::before_folders_go(&later);
            let holding: HashSet<NodeId> = server
                .live
                .values()
                .map(|n| (n, false))
                .chain(server.removed.values().map(|n| (n, true)))
                .filter(|(n, removed)| holds.counts(n.remote, *removed))
                .map(|(n, _)| n.state.parent)
                .collect();
            for n in pass {
                // Only a pass's folders are asked about, as the engine asks (a file another
                // writer put something in holds nothing: what is in it is its own item).
                let folder = self.server.removed[&n].state.kind == NodeKind::Folder;
                if folder && holding.contains(&n) {
                    continue;
                }
                let was_chosen = self
                    .server
                    .removed
                    .get_key_value(&n)
                    .is_some_and(|row| chosen.contains(&Server::at(row)));
                // A folder never goes holding what isn't going; a file holds nothing.
                let held = self
                    .server
                    .live
                    .iter()
                    .chain(&self.server.removed)
                    .find(|(c, s)| folder && s.state.parent == n && !pending.contains(*c))
                    .map(|(c, _)| *c);
                let wrong = !was_chosen || held.is_some();
                self.ledger
                    .emptied_unlisted(|| wrong.then(|| format!("{n:?} (holding {held:?})")));
                self.server.removed_at.remove(&n);
                self.server.tags_at_removal.remove(&n);
                if let Some(gone) = self.server.removed.remove(&n) {
                    self.deleted_now(n);
                    // Not owed any more only if a user's delete or a true take-along put it
                    // there: one put there wrongly was lost.
                    if self.ledger.removed_rightly(&(n, gone.remote.version)) {
                        self.ledger.blob_superseded(gone.state.content);
                    }
                }
            }
        }
    }

    /// Item `node` of client `c` is `new` from now on: the tags added to it there go along.
    pub(crate) fn retrack(&mut self, c: usize, node: NodeId, new: NodeId) {
        self.ledger.tags.retrack(c, node, new);
    }
}

/// A row of Recently deleted a restore brings back: its node, folder and name.
struct Row {
    node: NodeId,
    parent: NodeId,
    name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Event, act};
    use hbsync_types::Tag;

    /// A restore names as the engine's does: what takes its own name first, then the rest. F
    /// comes back holding `x` and `x (restored)`, beside a live `x` its delete took along: `x
    /// (restored)` keeps its name, and the removed `x` takes the next free one.
    #[test]
    fn a_restore_names_its_own_first() {
        let mut sim = Sim::new(1);
        let f = sim.ids.fresh();
        sim.server.put(f, NodeState::folder(NodeId::ROOT, "F"));
        let put = |sim: &mut Sim, name: &str| {
            let node = sim.ids.fresh();
            let blob = sim.new_blob();
            sim.server.put(node, NodeState::file(f, name, blob));
            node
        };
        let (x, restored) = (put(&mut sim, "x"), put(&mut sim, "x (restored)"));
        // F and what is in it go to Recently deleted; then another x, live, in removed F.
        for node in [f, x, restored] {
            let n = sim.server.live.remove(&node).expect("live");
            sim.remove_on_drive(node, n, true);
        }
        let live = put(&mut sim, "x");
        sim.restore_with(f);
        let name = |node: NodeId| sim.server.live[&node].state.name.clone();
        assert_eq!(name(live), "x");
        assert_eq!(name(restored), "x (restored)");
        assert_eq!(name(x), "x (restored 2)");
    }

    /// A delete here that loses (another device edited the file) settles nothing of what other
    /// clients did to its tags: a removal and an addition client 1 hasn't synced yet still
    /// count. Of the deleting client's own, only an add sent up stays: the drive has it. One
    /// sent before client 1 removed the tag doesn't excuse the tag coming back.
    #[test]
    fn a_lost_delete_settles_nothing_of_other_clients() {
        let mut sim = Sim::new(2);
        sim.run(&[
            act(0, Action::CreateFile { parent: 0, name: 0 }),
            act(0, Action::Tag { node: 0, tag: 0 }), // Red
        ]);
        assert!(sim.quiesce(10));
        let file = sim.item(0, "a").expect("the file");
        sim.run(&[
            act(1, Action::Edit { file: 0 }),
            Event::Plan { client: 1 },
            Event::Step { client: 1 },
            act(1, Action::Tag { node: 0, tag: 0 }), // Red removed
            act(1, Action::Tag { node: 0, tag: 1 }), // Work added
            act(0, Action::Delete { node: 0 }),
            Event::Plan { client: 0 },
        ]);
        assert!(sim.quiesce(10));
        assert!(sim.item(0, "a").is_some(), "the delete lost");
        assert!(sim.ledger.tags.has_cleared(&(1, file, "Red".into())));
        assert!(sim.ledger.tags.has_added(&(1, file, "Work".into())));
        assert!(
            sim.ledger.tags.has_detached(&(0, file, "Red".into())),
            "sent up, it stays with the node"
        );
        sim.check().unwrap_or_else(|e| panic!("{e}"));
        // Red back everywhere, as an engine that kept it over the removal would leave it.
        assert_eq!(sim.server_tags("a"), Some(vec!["Work:0".to_owned()]));
        let back = Tags::new(vec![Tag::new("Red", 6), Tag::new("Work", 0)]);
        sim.server.live.get_mut(&file).expect("live").state.tags = back.clone();
        for client in &mut sim.clients {
            for tree in [&mut client.local, &mut client.base] {
                if let Some(mut s) = tree.get(file).cloned() {
                    s.tags = back.clone();
                    tree.insert(file, s);
                }
            }
        }
        let back = sim.check().expect_err("Red coming back is reported");
        assert!(back.contains("came back"), "{back}");
    }
}
