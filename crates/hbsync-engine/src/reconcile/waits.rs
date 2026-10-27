//! What waits: for a hash (sync semantics §6.3: files here not hashed yet, being written,
//! locked, unreadable, a placement a crash left unread; every folder they are in; what would
//! take a name one of them holds), or for another page (§6.5: a folder that may go with
//! something of another page below it, a clash across pages). What waits is left as it is:
//! nothing is done to it, nor to such a folder that its plan would delete, restore or move on
//! either side (deferred, not undone), and a file not hashed yet is never an orphan to adopt.

use super::*;
use crate::pages::above_any_side;

/// Whether `node` is a file here not hashed yet (§6.3), which every view loads
/// (`ReadTx::unhashed_files`). A row the scan has no state for yet (its folder unrecorded, a
/// name the drive can't hold) is only deferred: nothing waits for it.
fn is_unhashed_file(input: &Input, node: NodeId) -> bool {
    input.local_unhashed.contains(&node) && input.local.contains(node)
}

/// What waits as a plan starts: the same for every plan of an input but for what waits for a
/// name or for a place across pages, which grows ([`reconcile`]).
pub(super) struct Waits {
    /// Each file not hashed yet, with the folders it is in (which wait for it).
    pub(super) by_file: Vec<(NodeId, Vec<NodeId>)>,
    /// What would take a name what waits holds (an earlier plan's waiters): waits as such a
    /// file does.
    pub(super) name_takers: BTreeSet<NodeId>,
    /// Everything that waits: the files not hashed yet, `name_takers`, `paged` and the
    /// folders each is in.
    pub(super) waits: HashSet<NodeId>,
    /// The nodes whose names something waits for (they hold them).
    pub(super) name_waits: HashSet<NodeId>,
    /// The folders that wait for other pages, planned with their subtrees as pages of their
    /// own (`Plan::waits_for_pages`).
    pub(super) going: Vec<NodeId>,
    /// Of `waits`, what waits for `going`: those, the folders above them and what of the page
    /// is below them.
    pub(super) paged: HashSet<NodeId>,
    /// What waits to be planned with another page's nodes (`Plan::waits_together`), as the
    /// plans of the input so far judged it, before it waited; by store ids.
    pub(super) together: Vec<Vec<NodeId>>,
}

impl Waits {
    pub(super) fn of(input: &Input, piece: bool) -> Self {
        let mut by_file: Vec<(NodeId, Vec<NodeId>)> = input
            .local_unhashed
            .iter()
            .filter(|n| is_unhashed_file(input, **n))
            .map(|&node| (node, above_any_side(input, node)))
            .collect();
        by_file.sort();
        let mut waits: HashSet<NodeId> = by_file
            .iter()
            .flat_map(|(node, on_paths)| on_paths.iter().copied().chain([*node]))
            .collect();
        // A folder that may go with something to plan below it in another page, or above the
        // page's nodes and planned in another page, waits for it, as it is with the folders
        // above it and everything of the page below it, as the folders a file not hashed yet
        // is in wait for it: alive as context, what is below unmerged kept a deleted folder alive
        // (review 14 R1), the folder's delete took along what changed there, or its items
        // went one by one before it. The engine plans it with its whole dirty subtree as a
        // page of its own next (`pages::OwnPages`). (What is dirty in another page is never
        // moved by this plan's repairs: `Reconciler::elsewhere`.)
        // A folder of the page below one another page plans waits for that one alone: its
        // page covers both (a piece of a split doesn't wait for the folder above it, but what
        // that one's removal took along does: `Reconciler::waits_for_its_folder`).
        let above: HashSet<NodeId> = match piece {
            true => HashSet::new(),
            false => input.going_above.clone(),
        };
        let mut going: Vec<NodeId> = input
            .going
            .iter()
            .copied()
            .filter(|g| !above_any_side(input, *g).iter().any(|a| above.contains(a)))
            .chain(above.iter().copied())
            .collect();
        going.sort();
        going.dedup();
        // The folders and what of the page is below them, across sides: left as they are,
        // nothing done (`paged`). The folders above those, and the folders each is in now
        // (moved out, say): left as they are, as above a file not hashed yet.
        let mut paged = HashSet::new();
        let mut above_them = HashSet::new();
        let trees = [&input.base, &input.local, &input.remote];
        for &folder in &going {
            paged.insert(folder);
            above_them.extend(above_any_side(input, folder));
            for node in crate::pages::subtree_any_side(trees, folder) {
                if input.dirty.contains(&node) && paged.insert(node) {
                    above_them.extend(above_any_side(input, node));
                }
            }
        }
        waits.extend(&paged);
        waits.extend(above_them);
        Waits {
            by_file,
            name_takers: BTreeSet::new(),
            waits,
            name_waits: HashSet::new(),
            going,
            paged,
            together: Vec::new(),
        }
    }

    /// `node` would take a name what waits holds: it waits too, with its folders. False when it
    /// waited so already.
    pub(super) fn add_name_taker(&mut self, input: &Input, node: NodeId) -> bool {
        if !self.name_takers.insert(node) {
            return false;
        }
        self.wait_with_folders(input, node);
        true
    }

    /// `node`'s placement clashes across pages: it waits as it is, with its folders. False
    /// when it waited so already.
    pub(super) fn add_placed(&mut self, input: &Input, node: NodeId) -> bool {
        if !self.paged.insert(node) {
            return false;
        }
        self.wait_with_folders(input, node);
        true
    }

    /// `node` waits, with the folders it is in on any side.
    fn wait_with_folders(&mut self, input: &Input, node: NodeId) {
        self.waits.extend(above_any_side(input, node));
        self.waits.insert(node);
    }
}

impl Reconciler<'_> {
    /// Whether `node` waits for a hash as it is, whatever its plan: a file not hashed yet, or
    /// what would take a name one holds.
    pub(super) fn waits_as_is(&self, node: NodeId) -> bool {
        is_unhashed_file(self.input, node) || self.name_takers.contains(&node)
    }

    /// The files here not hashed yet that something else waits for ([`Plan::waiting`]), by
    /// their ids in the store.
    pub(super) fn waited_for(&self) -> Vec<NodeId> {
        let waited = |n: &NodeId| self.deferred.contains(n) || self.name_waits.contains(n);
        self.by_file
            .iter()
            .filter(|(node, on_paths)| {
                self.remote_change(*node).is_change()
                    || self.name_waits.contains(node)
                    || on_paths.iter().any(waited)
            })
            // As the store knows it: a detached item's old id (§10).
            .map(|(node, _)| self.masks.store_id(*node))
            .collect()
    }

    /// Where this plan puts a node into a folder another page plans that isn't on that side
    /// yet, or onto a name such a node's item holds there now (it moves away in its own
    /// page): the two wait to be planned together, as a page of their own next
    /// (`Plan::waits_together`), each alone failing for the other (a swap of places). So does
    /// what goes into a folder that isn't on that side and that this plan doesn't make
    /// (waiting for another page, or for such a wait). The node is planned again waiting as it
    /// is (`placed`): nothing sent to fail meanwhile.
    pub(super) fn places_across_pages(&mut self) {
        if self.elsewhere.is_empty() && self.paged.is_empty() {
            return;
        }
        let i = self.input;
        let mut together = Vec::new();
        for op in &self.plan.ops {
            let (node, parent, name, side) = match op {
                Op::LocalMkdir {
                    node, parent, name, ..
                }
                | Op::LocalMove {
                    node, parent, name, ..
                }
                | Op::LocalDownload {
                    node, parent, name, ..
                } => (*node, *parent, name, &i.local),
                Op::RemoteCreate {
                    node, parent, name, ..
                }
                | Op::RemoteUpdate {
                    node, parent, name, ..
                } => (*node, *parent, name, &i.remote),
                _ => continue,
            };
            // Neither is made by this plan: one is in another page, the other left as it is.
            let waits =
                |f: NodeId| (self.elsewhere(f) || self.paged.contains(&f)) && !side.contains(f);
            let mut with: Vec<NodeId> = Vec::new();
            if waits(parent) {
                // With the folders above it, on any side, that wait so too, all at once: else
                // a chain of new folders is found a page at a time, each page a folder larger.
                with.push(parent);
                with.extend(
                    crate::pages::above_any_side(i, parent)
                        .into_iter()
                        .filter(|&f| waits(f)),
                );
            }
            if let Some(occupant) = side.child_named(parent, name, Some(node))
                && self.elsewhere(occupant)
            {
                with.push(occupant);
            }
            if !with.is_empty() {
                with.push(node);
                // A removed folder kept: what its removal took along stays only if it does,
                // so it waits with it (planned now, it went with the removal: a tag added
                // here lost, the item apart in Recently deleted).
                with.extend(self.untaken.get(&node).into_iter().flatten());
                together.push(with);
            }
        }
        for group in together {
            self.placed
                .extend(group.iter().filter(|n| self.scope.contains(n)));
            self.wait_together(group);
        }
    }

    /// In a piece of a folder too big for a page (`Options::piece`), what the removal on the
    /// drive of a folder above it took along (§6.3), unchanged here, waits for that folder's
    /// plan, which another page makes ([`Input::going_above`]): planned again as it is
    /// (`placed`). The folder's plan decides whether it stays, and with it whether what its
    /// removal took along does ([`Reconciler::untake_kept`]); planned first, that went with the
    /// removal while the folder came back. A node changed here plans the same either way.
    pub(super) fn waits_for_its_folder(&mut self) {
        let i = self.input;
        if !self.opts.piece || i.going_above.is_empty() || self.taken.is_empty() {
            return;
        }
        let waiting: Vec<NodeId> = i
            .dirty
            .iter()
            .copied()
            .filter(|&n| {
                self.taken.contains(&n)
                    && self.scope.contains(&n)
                    && !self.local_change(n).is_change()
                    && taken_by(i, n).is_some_and(|f| i.going_above.contains(&f))
            })
            .collect();
        self.placed.extend(waiting);
    }

    /// A folder this plan would delete that waits for another page (something in it does: a
    /// placement or a name across pages, or what goes into a folder that waits for its own
    /// page; §6.5) is
    /// deleted with that page: what else this plan deletes in it waits with it, planned again
    /// as it is (`placed`), and with it as a page of its own next (`Plan::waits_together`), so
    /// the folder goes as one, with what was in it, as in one page. Deleted now, what was in it
    /// went to Recently deleted apart from it. Only while what waits does so with a node
    /// outside this page: each such page is larger, so they end; then what still keeps the
    /// folder isn't another page, and what is deleted in it goes as before. Nor for a folder
    /// whose delete waits for a file not hashed yet: it goes item by item (§6.3).
    pub(super) fn deletes_wait_with_their_folder(&mut self) {
        if self.plan.delete_waits.is_empty()
            || (self.together.is_empty()
                && self.plan.waits_together.is_empty()
                && self.going.is_empty())
        {
            return;
        }
        let i = self.input;
        // Only while a node waits with one outside this page, or for a folder's page of its
        // own (`going`: what is below it, or it, is planned in another page): then the page
        // planned with it comes, larger, and the folder's wait may end there. With all of them
        // in it, what still keeps the folder isn't another page's: what is deleted in it goes
        // as before.
        let page: HashSet<NodeId> = i.dirty.iter().map(|n| self.masks.store_id(*n)).collect();
        if self.going.is_empty()
            && !self
                .together
                .iter()
                .chain(&self.plan.waits_together)
                .flatten()
                .any(|n| !page.contains(n))
        {
            return;
        }
        let folders: HashSet<NodeId> = self
            .plan
            .delete_waits
            .iter()
            .copied()
            .filter(|f| !self.by_file.iter().any(|(_, above)| above.contains(f)))
            .collect();
        if folders.is_empty() {
            return;
        }
        let mut groups: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        for node in self.plan.ops.iter().filter_map(Op::deletes) {
            if let Some(f) = i
                .base
                .ancestors(node)
                .into_iter()
                .find(|f| folders.contains(f))
            {
                groups.entry(f).or_default().push(node);
            }
        }
        for (folder, mut nodes) in groups {
            self.placed.extend(nodes.iter().copied());
            nodes.push(folder);
            self.wait_together(nodes);
        }
    }

    /// Whether `node` is planned in another page, not in this one (`Input::dirty_elsewhere`):
    /// no fix here moves, renames or adopts it, and what it blocks waits to be planned with it
    /// ([`Reconciler::wait_together`]).
    pub(super) fn elsewhere(&self, node: NodeId) -> bool {
        self.elsewhere.contains(&node) && !self.scope.contains(&node)
    }

    /// `nodes` wait to be planned together, as a page of their own next
    /// (`Plan::waits_together`): one of them is planned in another page.
    pub(super) fn wait_together(&mut self, nodes: Vec<NodeId>) {
        let mut nodes: Vec<NodeId> = nodes.into_iter().map(|n| self.masks.store_id(n)).collect();
        nodes.sort();
        nodes.dedup();
        self.plan.waits_together.push(nodes);
    }
}

/// The removed folder whose removal on the drive took `node` along: the first folder above it
/// there that isn't live, if it is in Recently deleted.
fn taken_by(input: &Input, node: NodeId) -> Option<NodeId> {
    let mut at = input.remote.get(node)?.parent;
    // Bounded: a cycle on the drive ends at a node met again.
    let mut seen = HashSet::from([node]);
    while let Some(s) = input.remote.get(at) {
        if !seen.insert(at) {
            return None;
        }
        at = s.parent;
    }
    input
        .removed
        .get(&at)
        .filter(|r| r.state.kind == NodeKind::Folder)
        .map(|_| at)
}
