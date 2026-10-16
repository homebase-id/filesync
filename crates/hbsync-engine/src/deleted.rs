//! Recently deleted (node schema): what a delete took out of the tree, kept on the drive with
//! its bytes until the purge. A restore puts it back in the tree, where the next sync brings it
//! to every device; the purge deletes it for good.
//!
//! It is the whole drive's, kept with the account ([`crate::drive::Drive`]): each item comes
//! back to the folder of this device whose part of the drive it was in, if any.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use hbsync_platform::name::name_key;
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_state::{RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind, Role};

use crate::engine::EngineError;
use crate::exec::{remove_all, trashed_at, update_meta};
use crate::tree::restored_names;
use crate::walk::End;
use hbsync_local::layout;

/// Recently deleted as a whole: how much it holds, and what emptying all of it as it is now
/// takes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeletedSummary {
    /// The items deleted on their own.
    pub deletes: usize,
    /// What emptying all of it now deletes for good ([`select`]): every item, those its deletes
    /// took along too, but a folder holding a live node or a removal that stays.
    pub items: usize,
    /// What the listing saw: [`empty_listed`] with it empties exactly that.
    pub listed: Listed,
}

/// What a listing of Recently deleted saw: the rows of this DB (its [`Store::db_id`]) recorded
/// up to the counter it read ([`hbsync_state::Recorded::seq`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listed {
    pub db: String,
    pub seq: u64,
}

/// The drive's newest `limit` deletes of Recently deleted, newest first, each with how many
/// items it took along, and the summary of all of it.
pub fn deletes(
    s: &Store<'_>,
    limit: usize,
) -> Result<(Vec<(Deleted, usize)>, DeletedSummary), StateError> {
    let mut listing = Listing::read(s)?;
    let summary = DeletedSummary {
        deletes: listing.deletes().count(),
        items: listing.going,
        listed: Listed {
            db: s.db_id()?,
            seq: s.removed_seq()?,
        },
    };
    let picked = listing.deletes().take(limit).collect();
    Ok((listing.with_along(picked)?, summary))
}

/// The items of Recently deleted (deletes, or items a delete took along) that are `nodes`, each
/// with how many items it took along: only those rows are read, and walked up from.
pub fn find(s: &Store<'_>, nodes: &[NodeId]) -> Result<Vec<(Deleted, usize)>, StateError> {
    let mut listing = Listing::lazy(s)?;
    let mut found = Vec::new();
    for &node in nodes {
        if let Some(item) = listing.describe_node(node)? {
            found.push(item);
        }
    }
    Ok(found)
}

/// Every item of Recently deleted, newest first, each with how many items it took along: to be
/// picked by its place, or listed whole (tests).
pub fn all(s: &Store<'_>) -> Result<Vec<(Deleted, usize)>, StateError> {
    let mut listing = Listing::read(s)?;
    let picked = (0..listing.roots.len()).collect();
    listing.with_along(picked)
}

/// Recently deleted's rows, newest first, each with the item whose delete took it along.
struct Listing<'a, 's> {
    rows: RemovedRows<'a, 's>,
    /// For each of the first rows (the listed ones), its item: itself, deleted on its own.
    roots: Vec<NodeId>,
    /// Of the listed rows, how many emptying all of them deletes for good, and of those, how many
    /// each delete took along.
    going: usize,
    along: HashMap<NodeId, usize>,
    folders: HashMap<NodeId, String>,
    /// Where a walk up stops: a folder's top (here or not), the role folders whether synced
    /// here or not; above them all, the main folder's top (the drive's root).
    tops: HashSet<NodeId>,
    places: HashMap<NodeId, (String, NodeId)>,
}

impl<'a, 's> Listing<'a, 's> {
    /// Nothing read yet: rows come as [`Listing::describe_node`] asks for them.
    fn lazy(s: &'a Store<'s>) -> Result<Self, StateError> {
        let folders: HashMap<NodeId, String> =
            s.folders()?.into_iter().map(|f| (f.top, f.path)).collect();
        let tops = folders
            .keys()
            .copied()
            .chain(Role::ALL.iter().map(|r| r.node()))
            .collect();
        Ok(Listing {
            rows: RemovedRows::new(s, Vec::new()),
            roots: Vec::new(),
            going: 0,
            along: HashMap::new(),
            folders,
            tops,
            places: HashMap::new(),
        })
    }

    /// Every row, each with its item.
    fn read(s: &'a Store<'s>) -> Result<Self, StateError> {
        let mut listing = Listing::lazy(s)?;
        let listed = s.list_removed(usize::MAX)?;
        let count = listed.len();
        listing.rows = RemovedRows::new(s, listed);
        let mut deleted_by = HashMap::new();
        listing.roots = (0..count)
            .map(|i| {
                let node = listing.rows.rows[i].row.node;
                deleted_by_item(&mut listing.rows, node, &mut deleted_by)
            })
            .collect::<Result<Vec<_>, _>>()?;
        // What emptying all of it deletes: from the rows read, and one query.
        let rows = &listing.rows.rows[..count];
        let mut holdings = Holdings {
            live: s.removed_holding_live()?,
            removed: HashMap::new(),
        };
        for r in rows {
            *holdings.removed.entry(r.row.parent).or_default() += 1;
        }
        let chosen: Vec<Chosen> = rows.iter().map(Chosen::of).collect();
        let going: HashSet<NodeId> = select(&chosen, &holdings).into_iter().collect();
        listing.going = going.len();
        for (r, root) in rows.iter().zip(&listing.roots) {
            if *root != r.row.node && going.contains(&r.row.node) {
                *listing.along.entry(*root).or_default() += 1;
            }
        }
        Ok(listing)
    }

    /// `node`'s row, if it is in Recently deleted, with how many items its delete took along
    /// (0 for an item a delete took along itself).
    fn describe_node(&mut self, node: NodeId) -> Result<Option<(Deleted, usize)>, StateError> {
        let Some(top) = self.rows.get(node)?.cloned() else {
            return Ok(None);
        };
        let root = deleted_by_item(&mut self.rows, node, &mut HashMap::new())?;
        // What emptying it deletes, but itself.
        let along = match root == node {
            true => {
                let going = going(self.rows.s, group(self.rows.s, top, false)?)?;
                going.iter().filter(|r| r.row.node != node).count()
            }
            false => 0,
        };
        let i = self.rows.index[&node].unwrap_or_default();
        Ok(Some((self.describe_as(i, root)?, along)))
    }

    /// The rows of the items deleted on their own, newest first (the items' own rows give
    /// the order).
    fn deletes(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.roots.len()).filter(|&i| self.roots[i] == self.rows.rows[i].row.node)
    }

    /// Rows `picked`, described, each with how many items it took along that emptying deletes.
    fn with_along(&mut self, picked: Vec<usize>) -> Result<Vec<(Deleted, usize)>, StateError> {
        picked
            .into_iter()
            .map(|i| {
                let d = self.describe(i)?;
                let n = match d.root == d.node {
                    true => self.along.get(&d.node).copied().unwrap_or(0),
                    false => 0,
                };
                Ok((d, n))
            })
            .collect()
    }

    /// Row `i` as listed: where it comes back to and its path there.
    fn describe(&mut self, i: usize) -> Result<Deleted, StateError> {
        let root = self.roots[i];
        self.describe_as(i, root)
    }

    /// Row `i`, deleted with `root`'s delete.
    fn describe_as(&mut self, i: usize, root: NodeId) -> Result<Deleted, StateError> {
        let r = &self.rows.rows[i];
        let (node, parent, name) = (r.row.node, r.row.parent, r.row.name.clone());
        let (kind, size, removed_at_ms) = (r.row.kind, r.row.file.map(|f| f.size), r.removed_at);
        let (path, top) = place(&mut self.rows, &self.tops, parent, &mut self.places)?;
        Ok(Deleted {
            node,
            parent,
            path: join(&path, &name),
            kind,
            size,
            removed_at_ms,
            root,
            folder: self.folders.get(&top).cloned(),
        })
    }
}

/// Rows of Recently deleted by node: the ones read already, else from the store, each once.
struct RemovedRows<'a, 's> {
    s: &'a Store<'s>,
    rows: Vec<RemovedRow>,
    index: HashMap<NodeId, Option<usize>>,
}

impl<'a, 's> RemovedRows<'a, 's> {
    fn new(s: &'a Store<'s>, rows: Vec<RemovedRow>) -> Self {
        let index = rows
            .iter()
            .enumerate()
            .map(|(i, r)| (r.row.node, Some(i)))
            .collect();
        RemovedRows { s, rows, index }
    }

    fn get(&mut self, node: NodeId) -> Result<Option<&RemovedRow>, StateError> {
        let i = match self.index.get(&node) {
            Some(i) => *i,
            None => {
                let i = self.s.removed_by_node(node)?.map(|r| {
                    self.rows.push(r);
                    self.rows.len() - 1
                });
                self.index.insert(node, i);
                i
            }
        };
        Ok(i.map(|i| &self.rows[i]))
    }

    /// The folder `node` was deleted with ([`deleted_with`]), if it was.
    fn deleted_with(&mut self, node: NodeId) -> Result<Option<NodeId>, StateError> {
        let Some((parent, at)) = self.get(node)?.map(|r| (r.row.parent, r.time())) else {
            return Ok(None);
        };
        Ok(self
            .get(parent)?
            .filter(|folder| deleted_with(at, folder))
            .map(|folder| folder.row.node))
    }
}

/// Whether a node removed at `removed_at` was deleted with `folder`, the folder it is in, in
/// Recently deleted too: removed no earlier than the folder (node schema, Recently deleted).
fn deleted_with(removed_at: i64, folder: &RemovedRow) -> bool {
    folder.time() <= removed_at
}

/// The item of Recently deleted whose delete took `node` along: up through the folders it was
/// deleted with, as far as they go (`node` itself, deleted on its own). Memoized in `known`.
fn deleted_by_item(
    rows: &mut RemovedRows<'_, '_>,
    node: NodeId,
    known: &mut HashMap<NodeId, NodeId>,
) -> Result<NodeId, StateError> {
    let up = crate::walk::up::<_, StateError>(node, |at| {
        if known.contains_key(&at) {
            return Ok(None);
        }
        Ok(rows.deleted_with(at)?.map(|folder| (at, folder)))
    })?;
    let item = match up.end {
        End::At(at) => known.get(&at).copied().unwrap_or(at),
        // At the top, or past any depth a path may have: the last row walked. Cut off deep,
        // that row is a delete of its own (remembered below), so the listing lists it.
        End::Top | End::Deep => up.items.last().copied().unwrap_or(node),
        // Removed rows in a cycle (left by a remote one): one of them, the same whichever row
        // the walk started from.
        End::Cycle => cycle_root(rows, &up.items)?,
    };
    if let End::At(at) = up.end {
        known.insert(at, item);
    }
    known.extend(up.items.into_iter().map(|n| (n, item)));
    Ok(item)
}

/// The item of a cycle of removed rows, each deleted with the next, that `walked` (a walk up
/// that ended in it) reached: its smallest node, so a listing and a lookup of any of its rows
/// agree.
fn cycle_root(rows: &mut RemovedRows<'_, '_>, walked: &[NodeId]) -> Result<NodeId, StateError> {
    let Some(&last) = walked.last() else {
        return Ok(NodeId::ROOT);
    };
    // The walk ends where it would step again onto a row it walked: the cycle starts there.
    let start = match rows.deleted_with(last)? {
        Some(next) => walked.iter().position(|n| *n == next).unwrap_or(0),
        None => 0,
    };
    Ok(walked[start..].iter().copied().min().unwrap_or(last))
}

/// The path of folder `at` below the top it is under (one of `tops`, else the drive's root:
/// a folder deleted for good ends the path, which starts below it), and that top, from the
/// tree or from Recently deleted. Memoized in `known`.
fn place(
    rows: &mut RemovedRows<'_, '_>,
    tops: &HashSet<NodeId>,
    at: NodeId,
    known: &mut HashMap<NodeId, (String, NodeId)>,
) -> Result<(String, NodeId), StateError> {
    let up = crate::walk::up::<_, StateError>(at, |n| {
        if known.contains_key(&n) || tops.contains(&n) {
            return Ok(None);
        }
        Ok(match rows.s.get_mirror(n)? {
            Some(row) => Some(((n, row.name), row.parent)),
            None => rows
                .get(n)?
                .map(|r| ((n, r.row.name.clone()), r.row.parent)),
        })
    })?;
    let (mut path, top) = match up.end {
        End::At(n) if tops.contains(&n) => (String::new(), n),
        End::At(n) => known
            .get(&n)
            .cloned()
            .unwrap_or((String::new(), NodeId::ROOT)),
        End::Top | End::Cycle | End::Deep => (String::new(), NodeId::ROOT),
    };
    // Top-down, remembering each folder on the way.
    for (id, name) in up.items.into_iter().rev() {
        path = join(&path, &name);
        known.insert(id, (path.clone(), top));
    }
    Ok((path, top))
}

fn join(folder: &str, name: &str) -> String {
    if folder.is_empty() {
        name.to_owned()
    } else {
        format!("{folder}/{name}")
    }
}

/// Restores `node` with what was deleted with it, and the folders it was in that are in
/// Recently deleted too (just those, not their other contents). Returns how many items came
/// back; 0 when `node` isn't in Recently deleted (any more).
pub fn restore(db: &mut StateDb, remote: &dyn Remote, node: NodeId) -> Result<usize, EngineError> {
    let Some(plan) = db.read(|s| plan_restore(s, node))? else {
        return Ok(0);
    };
    let RestorePlan {
        rows,
        mut live,
        mut taken,
    } = plan;
    let in_plan: HashSet<NodeId> = rows.iter().map(|r| r.row.node).collect();
    let mut restored = 0;
    for r in &rows {
        let row = &r.row;
        // Its folder is back (or never left). One that was to come back and didn't keeps it in
        // Recently deleted; one deleted for good, the top takes it.
        let parent = if live.contains(&row.parent) {
            row.parent
        } else if in_plan.contains(&row.parent) {
            continue;
        } else {
            NodeId::ROOT
        };
        let names = taken.entry(parent).or_default();
        let Some(name) = restored_names(&row.name).find(|n| !names.contains(&name_key(n))) else {
            continue; // no free name: it stays in Recently deleted
        };
        let meta = NodeMeta {
            node: row.node,
            parent,
            kind: row.kind,
            content: row.content(&name),
            removed: false,
        };
        let back = match update_meta(remote, &row.target(), &meta, row.file.map(|f| f.hash)) {
            Ok(back) => back,
            // Changed or deleted for good meanwhile: the feed tells what it is now.
            Err(RemoteError::VersionMismatch | RemoteError::NotFound) => continue,
            Err(e) => return Err(EngineError::Remote(e)),
        };
        names.insert(name_key(&name));
        live.insert(row.node);
        // The mirror's write marks it dirty in the folders it comes back to.
        db.write(|s| {
            s.delete_removed(row.node)?;
            s.put_mirror(&back)
        })?;
        restored += 1;
    }
    Ok(restored)
}

/// What a restore of a node does, worked out in one read.
struct RestorePlan {
    /// In order: the folders above that are in Recently deleted (top-down), then the node and
    /// what was deleted with it (each after its folder).
    rows: Vec<RemovedRow>,
    /// Folders in the tree that the rows may go into (the top included).
    live: HashSet<NodeId>,
    /// The name keys taken in each of those folders.
    taken: HashMap<NodeId, HashSet<String>>,
}

/// `top` and what its delete took along below it, level by level: the removed items in it
/// deleted with their folder ([`deleted_with`]), and the live ones written no later than its
/// removal (node schema: their own removal was still on its way), which count as removed then.
/// What was deleted from it before stays out.
///
/// Without `live`, only the removed items: what Recently deleted lists with it.
fn group(s: &Store<'_>, top: RemovedRow, live: bool) -> Result<Vec<RemovedRow>, StateError> {
    // Each node once: removed rows may form a cycle (left by a remote one).
    let mut seen = HashSet::from([top.row.node]);
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        if rows[next].row.kind == NodeKind::Folder {
            let folder = &rows[next];
            let removed = s
                .removed_children(folder.row.node)?
                .into_iter()
                .filter(|r| deleted_with(r.time(), folder));
            let along = match live {
                true => s.mirror_children(folder.row.node)?,
                false => Vec::new(),
            };
            let along = along
                .into_iter()
                .filter(|m| m.changed_at.is_none_or(|at| at <= folder.time()))
                .map(|row| RemovedRow {
                    row,
                    removed_at: folder.removed_at,
                });
            let with: Vec<RemovedRow> = removed
                .chain(along)
                .filter(|r| seen.insert(r.row.node))
                .collect();
            rows.extend(with);
        }
        next += 1;
    }
    Ok(rows)
}

fn plan_restore(s: &Store<'_>, node: NodeId) -> Result<Option<RestorePlan>, StateError> {
    let Some(top) = s.removed_by_node(node)? else {
        return Ok(None);
    };
    // The folders above, as far as they are in Recently deleted (then in the tree, or deleted
    // for good: the top it is).
    let mut chain = crate::walk::up::<_, StateError>(top.row.parent, |at| {
        Ok(s.removed_by_node(at)?.map(|f| {
            let parent = f.row.parent;
            (f, parent)
        }))
    })?
    .items;
    chain.reverse();
    // Then the node and what was deleted with it, each after its folder; each node once. What
    // is live still comes back with its folder by itself.
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for r in chain.into_iter().chain(group(s, top, true)?) {
        if seen.insert(r.row.node) && s.get_mirror(r.row.node)?.is_none() {
            rows.push(r);
        }
    }
    // The folders in the tree the rows may go into, and their names.
    let mut live = HashSet::from([NodeId::ROOT]);
    let mut taken: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for parent in rows.iter().map(|r| r.row.parent).chain([NodeId::ROOT]) {
        if taken.contains_key(&parent) {
            continue;
        }
        if !parent.is_root() && s.get_mirror(parent)?.is_some() {
            live.insert(parent);
        }
        let names = s
            .mirror_children(parent)?
            .iter()
            .map(|m| name_key(&m.name))
            .collect();
        taken.insert(parent, names);
    }
    Ok(Some(RestorePlan { rows, live, taken }))
}

/// Deletes for good the items of Recently deleted removed before `before_ms` (Unix ms), each
/// with what was deleted with it ([`select`]: removed nodes only, never a live one), files
/// before their folders, and empties every folder's trash (`.hbsync/trash`) of the same age.
/// Returns how many items went.
pub fn purge(db: &mut StateDb, remote: &dyn Remote, before_ms: i64) -> Result<usize, EngineError> {
    let gone = expire(db, remote, before_ms)?;
    for folder in db.read(|s| s.folders())? {
        if folder.path.is_empty() {
            continue;
        }
        // Only what went there since the purge has been emptying it: older entries (from
        // before Recently deleted) may be the only copy left, and stay.
        let since = db.write(
            |s| match s.folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE)? {
                Some(v) => Ok(v.parse().unwrap_or(u64::MAX)),
                None => {
                    let now = hbsync_types::unix_now();
                    s.set_folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE, &now.to_string())?;
                    Ok(now)
                }
            },
        )?;
        empty_trash(
            &layout::trash_dir(Path::new(&folder.path)),
            since,
            before_ms,
        );
    }
    Ok(gone)
}

/// Deletes for good the items `nodes` name, each with what was deleted with it ([`select`]).
/// Returns how many items went.
pub fn empty(
    db: &mut StateDb,
    remote: &dyn Remote,
    nodes: &[NodeId],
) -> Result<usize, EngineError> {
    let tops = db.read(|s| {
        let mut tops = Vec::new();
        for &node in nodes {
            tops.extend(s.removed_by_node(node)?);
        }
        Ok(tops)
    })?;
    delete_groups(db, remote, tops)
}

/// Deletes for good what a listing saw ([`DeletedSummary::listed`]): the rows recorded up to
/// its counter, an open removal too; nothing recorded since (a removal that arrives later,
/// whatever its server time, or a node deleted again after a restore), and never a live node
/// ([`select`]). Refused for a listing of another DB (the account's, made again) or one this DB
/// never gave. Returns how many items went.
pub fn empty_listed(
    db: &mut StateDb,
    remote: &dyn Remote,
    listed: &Listed,
) -> Result<usize, EngineError> {
    let rows = db.read(|s| {
        if !listed_here(s, listed)? {
            return Err(StateError::Conflict(
                "Recently deleted was never listed so here: look at it again".to_owned(),
            ));
        }
        let chosen = s.recorded_upto(listed.seq)?;
        going(s, chosen.into_iter().map(|r| r.removed).collect())
    })?;
    delete_for_good(db, remote, &rows)
}

/// Whether this DB gave `listed`: its own, up to a counter it reached.
pub fn listed_here(s: &Store<'_>, listed: &Listed) -> Result<bool, StateError> {
    Ok(!listed.db.is_empty() && listed.db == s.db_id()? && listed.seq <= s.removed_seq()?)
}

/// What emptying `chosen` (rows of Recently deleted) deletes for good ([`select`]), in that
/// order: what the folders hold read in two queries.
fn going(s: &Store<'_>, chosen: Vec<RemovedRow>) -> Result<Vec<RemovedRow>, StateError> {
    let holdings = Holdings {
        live: s.removed_holding_live()?,
        removed: s.removed_per_folder()?,
    };
    let picked: Vec<Chosen> = chosen.iter().map(Chosen::of).collect();
    let mut rows: HashMap<NodeId, RemovedRow> =
        chosen.into_iter().map(|r| (r.row.node, r)).collect();
    Ok(select(&picked, &holdings)
        .into_iter()
        .filter_map(|n| rows.remove(&n))
        .collect())
}

/// A row of Recently deleted as emptying sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chosen {
    pub node: NodeId,
    pub parent: NodeId,
    pub folder: bool,
}

impl Chosen {
    fn of(r: &RemovedRow) -> Chosen {
        Chosen {
            node: r.row.node,
            parent: r.row.parent,
            folder: r.row.kind == NodeKind::Folder,
        }
    }
}

/// What the folders of Recently deleted hold on the drive.
#[derive(Debug, Clone, Default)]
pub struct Holdings {
    /// The removed folders holding a live node.
    pub live: HashSet<NodeId>,
    /// How many removed nodes each folder holds.
    pub removed: HashMap<NodeId, u64>,
}

/// Of `chosen` (rows of Recently deleted), the ones emptying them deletes for good, in the
/// order it deletes them. Only removed rows, never a live node: a folder holding a live node
/// (one its delete took along, or one written since) or a removed node not chosen stays, and
/// so do the folders it is in. Files first, then each folder after the folders in it (a cycle
/// of them in some order), by the links among the chosen alone, however deep.
pub fn select(chosen: &[Chosen], holdings: &Holdings) -> Vec<NodeId> {
    let mut links: HashMap<NodeId, (NodeId, bool)> = HashMap::new();
    let mut chosen_in: HashMap<NodeId, u64> = HashMap::new();
    for c in chosen {
        if let Entry::Vacant(link) = links.entry(c.node) {
            link.insert((c.parent, c.folder));
            *chosen_in.entry(c.parent).or_default() += 1;
        }
    }
    let mut stays = HashSet::new();
    for (&folder, &(_, is_folder)) in &links {
        let removed = holdings.removed.get(&folder).copied().unwrap_or(0);
        let holds = holdings.live.contains(&folder)
            || removed > chosen_in.get(&folder).copied().unwrap_or(0);
        if is_folder && holds {
            let mut at = folder;
            while links.contains_key(&at) && stays.insert(at) {
                at = links[&at].0;
            }
        }
    }
    // How many of them each is in, worked out once each.
    let mut depth: HashMap<NodeId, usize> = HashMap::new();
    for &start in links.keys() {
        let (mut path, mut on_path) = (Vec::new(), HashSet::new());
        let mut at = start;
        let mut d = loop {
            if let Some(&known) = depth.get(&at) {
                break known + 1;
            }
            match links.get(&at) {
                Some(&(parent, _)) if on_path.insert(at) => {
                    path.push(at);
                    at = parent;
                }
                // Above them all, or round a cycle.
                _ => break 0,
            }
        };
        for n in path.into_iter().rev() {
            depth.insert(n, d);
            d += 1;
        }
    }
    let mut going: Vec<NodeId> = links
        .keys()
        .filter(|n| !stays.contains(*n))
        .copied()
        .collect();
    going.sort_by_key(|n| (links[n].1, std::cmp::Reverse(depth[n]), *n));
    going
}

/// Deletes for good the items of Recently deleted removed before `before_ms`, each with what
/// was deleted with it.
fn expire(db: &mut StateDb, remote: &dyn Remote, before_ms: i64) -> Result<usize, EngineError> {
    let tops = db.read(|s| {
        let before = s.removed_before(before_ms)?;
        let count = before.len();
        let mut rows = RemovedRows::new(s, before);
        let (mut tops, mut known) = (Vec::new(), HashMap::new());
        for i in 0..count {
            // The items deleted on their own, as listed: in a cycle of rows, one of them.
            let node = rows.rows[i].row.node;
            if deleted_by_item(&mut rows, node, &mut known)? == node {
                tops.push(rows.rows[i].clone());
            }
        }
        Ok(tops)
    })?;
    delete_groups(db, remote, tops)
}

/// Deletes for good `tops`, each with what was deleted with it ([`select`]).
fn delete_groups(
    db: &mut StateDb,
    remote: &dyn Remote,
    tops: Vec<RemovedRow>,
) -> Result<usize, EngineError> {
    let rows = db.read(|s| {
        let mut chosen = Vec::new();
        for top in tops {
            chosen.extend(group(s, top, false)?);
        }
        going(s, chosen)
    })?;
    delete_for_good(db, remote, &rows)
}

fn delete_for_good(
    db: &mut StateDb,
    remote: &dyn Remote,
    rows: &[RemovedRow],
) -> Result<usize, EngineError> {
    // What each folder holds of the rows still to go: what is in a folder goes before it (but
    // round a cycle).
    let mut to_go: HashMap<NodeId, u64> = HashMap::new();
    for r in rows {
        *to_go.entry(r.row.parent).or_default() += 1;
    }
    let mut gone = 0;
    for r in rows {
        let held = to_go.get(&r.row.node).copied().unwrap_or(0);
        if let Some(n) = to_go.get_mut(&r.row.parent) {
            *n -= 1;
        }
        // A folder holding anything else now (what failed to go, or came meanwhile) stays.
        if r.row.kind == NodeKind::Folder && db.read(|s| s.child_count(r.row.node))? > held {
            continue;
        }
        match remote.delete(r.row.file_id, r.row.version) {
            Ok(()) | Err(RemoteError::NotFound) => {}
            // Restored or changed meanwhile: not ours to delete.
            Err(RemoteError::VersionMismatch) => continue,
            Err(e) => return Err(EngineError::Remote(e)),
        }
        db.write(|s| s.delete_removed(r.row.node).map(drop))?;
        gone += 1;
    }
    Ok(gone)
}

/// Deletes the trash entries (`<Unix s>-<node>`) that went there from `since` (Unix s) until
/// before `before_ms`. Best effort: one that can't go now goes at the next purge.
fn empty_trash(trash: &Path, since: u64, before_ms: i64) {
    let Ok(entries) = fs::read_dir(trash) else {
        return;
    };
    for entry in entries.flatten() {
        let at = entry.file_name().to_str().and_then(trashed_at);
        if at.is_some_and(|s| s >= since && (s as i64).saturating_mul(1000) < before_ms) {
            let _ = remove_all(&entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use hbsync_state::MirrorRow;
    use hbsync_types::{RemoteFileId, Uuid, VersionTag};

    use super::*;

    fn removed(node: NodeId, parent: NodeId, removed_at: i64) -> RemovedRow {
        RemovedRow {
            row: MirrorRow {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: VersionTag(Uuid::new_v4()),
                node,
                parent,
                name: node.0.to_string(),
                kind: NodeKind::Folder,
                file: None,
                content_extra: None,
                token: None,
                changed_at: None,
            },
            removed_at: Some(removed_at),
        }
    }

    /// Removed folders in a cycle (a remote one left them so) are restored once each, and the
    /// plan ends; so does the walk to the item that took them along.
    #[test]
    fn a_cycle_in_recently_deleted_ends() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (a, b) = (NodeId::new(), NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(a, b, 1))?;
            s.put_removed(&removed(b, a, 1))
        })?;
        let plan = db.read(|s| plan_restore(s, a))?.map(|p| p.rows.len());
        assert_eq!(plan, Some(2));
        // One of them is the item, the same one whichever row a walk starts from: the listing
        // and a lookup of either row agree on it and on its count.
        let item = a.min(b);
        let listed: Vec<(NodeId, NodeId, usize)> = db
            .read(all)?
            .into_iter()
            .map(|(d, n)| (d.node, d.root, n))
            .collect();
        assert_eq!(listed.len(), 2);
        assert!(
            listed.iter().all(|(_, root, _)| *root == item),
            "{listed:?}"
        );
        for node in [a, b] {
            let found = db.read(|s| find(s, &[node]))?;
            let listed = listed
                .iter()
                .find(|(n, _, _)| *n == node)
                .map(|l| (l.1, l.2));
            assert_eq!(Some((found[0].0.root, found[0].1)), listed, "{node:?}");
        }
        Ok(())
    }

    fn fake() -> hbsync_remote::fake::FakeRemote {
        use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
        // The drive has none of the files: each delete for good finds it gone, which is done.
        FakeRemote::new(FakeServer::new(ServerConfig::default()))
    }

    fn listed(db: &mut StateDb) -> Result<Listed, StateError> {
        Ok(db.read(|s| deletes(s, 0))?.1.listed)
    }

    fn left(db: &mut StateDb) -> Result<Vec<NodeId>, StateError> {
        let mut left: Vec<NodeId> = db.read(all)?.into_iter().map(|(d, _)| d.node).collect();
        left.sort();
        Ok(left)
    }

    fn sorted(mut nodes: Vec<NodeId>) -> Vec<NodeId> {
        nodes.sort();
        nodes
    }

    /// Emptying as listed deletes for good exactly what the listing saw: an open removal too,
    /// what a delete took along by then; not what was recorded after it (a removal that arrives
    /// later with an older server time, an item a listed folder's delete took along that came
    /// later, which keeps that folder), nor a node restored and deleted again since.
    #[test]
    fn emptying_takes_what_was_listed_and_nothing_since() -> Result<(), Box<dyn std::error::Error>>
    {
        let remote = fake();
        let mut db = StateDb::open_in_memory()?;
        let [f, with, own, lone, late, later_with] = [(); 6].map(|_| NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(f, NodeId::ROOT, 10))?;
            s.put_removed(&removed(with, f, 11))?;
            s.put_removed(&RemovedRow {
                removed_at: None, // this device's own, its server time not back yet
                ..removed(own, NodeId::ROOT, 0)
            })?;
            s.put_removed(&removed(lone, NodeId::ROOT, 20))
        })?;
        let (deletes, summary) = db.read(|s| deletes(s, 10))?;
        assert_eq!((deletes.len(), summary.items), (3, 4));

        db.write(|s| {
            // Recorded after the listing, though removed before it on the server.
            s.put_removed(&removed(late, NodeId::ROOT, 5))?;
            s.put_removed(&removed(later_with, f, 12))?;
            // Restored, then deleted again.
            s.delete_removed(lone)?;
            s.put_removed(&removed(lone, NodeId::ROOT, 30))?;
            // Its own removal confirmed: the same removal, still listed.
            match s.removed_by_node(own)? {
                Some(r) => s.confirm_removed(r.row.remote_ref(), 25),
                None => Err(StateError::Conflict("own isn't listed".into())),
            }
        })?;
        assert_eq!(empty_listed(&mut db, &remote, &summary.listed)?, 2);
        assert_eq!(left(&mut db)?, sorted(vec![f, late, later_with, lone]));
        Ok(())
    }

    /// Emptying deletes for good only removed rows, never a live node: a folder holding one
    /// (its delete took it along, it was written since, or its time isn't known) stays, and so
    /// do the folders it is in, emptied of the rest. Emptying as listed, emptying the deletes
    /// and the purge agree, and the listing counts what they delete.
    #[test]
    fn a_live_node_is_never_deleted_for_good() -> Result<(), Box<dyn std::error::Error>> {
        let [outer, f, gone, other, g, h] = [(); 6].map(|_| NodeId::new());
        let [taken, taken_deep, later, unknown, h_child] = [(); 5].map(|_| NodeId::new());
        let live = |node, parent, changed_at| MirrorRow {
            changed_at,
            ..removed(node, parent, 0).row
        };
        for how in ["listed", "deletes", "purge"] {
            let remote = fake();
            let mut db = StateDb::open_in_memory()?;
            db.write(|s| {
                for (node, parent) in [
                    (outer, NodeId::ROOT),
                    (f, outer),
                    (gone, f),
                    (other, NodeId::ROOT),
                    (g, NodeId::ROOT),
                    (h, NodeId::ROOT),
                ] {
                    s.put_removed(&removed(node, parent, 10))?;
                }
                s.put_mirror(&live(taken, f, Some(5)))?;
                s.put_mirror(&live(taken_deep, taken, Some(6)))?;
                s.put_mirror(&live(later, f, Some(20)))?;
                s.put_mirror(&live(unknown, g, None))?;
                s.put_mirror(&live(h_child, h, Some(10)))
            })?;
            let along: HashMap<NodeId, usize> = db
                .read(all)?
                .into_iter()
                .map(|(d, n)| (d.node, n))
                .collect();
            assert_eq!((along[&outer], along[&g], along[&h]), (1, 0, 0));
            let found = db.read(|s| find(s, &[outer]))?;
            assert_eq!(found[0].1, 1, "a pick counts the same");
            let went = match how {
                "listed" => {
                    // The count it shows is what emptying takes.
                    let (_, summary) = db.read(|s| deletes(s, 0))?;
                    assert_eq!((summary.deletes, summary.items), (4, 2));
                    empty_listed(&mut db, &remote, &summary.listed)?
                }
                "deletes" => empty(&mut db, &remote, &[outer, other, g, h])?,
                _ => expire(&mut db, &remote, 11)?,
            };
            assert_eq!(went, 2, "{how}");
            assert_eq!(left(&mut db)?, sorted(vec![outer, f, g, h]), "{how}");
            for n in [taken, taken_deep, later, unknown, h_child] {
                assert!(db.read(|s| s.get_mirror(n))?.is_some(), "{how}: {n:?}");
            }
        }
        Ok(())
    }

    /// A folder whose contents didn't all go (one changed meanwhile, its delete refused) stays
    /// with them: each folder is looked at again just before its delete.
    #[test]
    fn a_folder_stays_when_a_delete_in_it_fails() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        let mut db = StateDb::open_in_memory()?;
        let (f, child) = (NodeId::new(), NodeId::new());
        let mut row = removed(child, f, 10);
        // On the drive at a version other than the one recorded: its delete is refused.
        let name = row.row.name.clone();
        let target = remote.create(
            &NodeMeta {
                node: child,
                parent: f,
                kind: NodeKind::Folder,
                content: row.row.content(&name),
                removed: true,
            },
            None,
        )?;
        row.row.file_id = target.at.file_id;
        db.write(|s| {
            s.put_removed(&removed(f, NodeId::ROOT, 10))?;
            s.put_removed(&row)
        })?;
        let listing = listed(&mut db)?;
        assert_eq!(empty_listed(&mut db, &remote, &listing)?, 0);
        assert_eq!(left(&mut db)?, sorted(vec![f, child]));
        Ok(())
    }

    /// A listing of another DB of the same identity (made again since), or one this DB never
    /// gave, empties nothing; one that saw nothing empties nothing.
    #[test]
    fn emptying_an_unknown_listing_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        let mut db = StateDb::open_in_memory()?;
        let x = NodeId::new();
        db.write(|s| s.put_removed(&removed(x, NodeId::ROOT, 1)))?;
        let listing = listed(&mut db)?;
        let ahead = Listed {
            seq: listing.seq + 1,
            ..listing.clone()
        };
        assert!(empty_listed(&mut db, &remote, &ahead).is_err());
        // Another DB, its counter as far.
        let mut again = StateDb::open_in_memory()?;
        let elsewhere = listed(&mut again)?;
        assert_ne!(elsewhere.db, listing.db);
        let other = Listed {
            db: elsewhere.db,
            ..listing.clone()
        };
        assert!(empty_listed(&mut db, &remote, &other).is_err());
        let nothing = Listed {
            seq: 0,
            ..listing.clone()
        };
        assert_eq!(empty_listed(&mut db, &remote, &nothing)?, 0);
        assert_eq!(left(&mut db)?, [x], "nothing went");
        assert_eq!(empty_listed(&mut db, &remote, &listing)?, 1);
        Ok(())
    }

    /// Removed folders in a cycle are listed, so emptying as listed deletes them for good, and
    /// so does the purge once they expire.
    #[test]
    fn a_cycle_in_recently_deleted_is_emptied() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        for purge in [false, true] {
            let mut db = StateDb::open_in_memory()?;
            let (a, b) = (NodeId::new(), NodeId::new());
            db.write(|s| {
                s.put_removed(&removed(a, b, 1))?;
                s.put_removed(&removed(b, a, 1))
            })?;
            let gone = match purge {
                false => {
                    let listing = listed(&mut db)?;
                    empty_listed(&mut db, &remote, &listing)?
                }
                true => expire(&mut db, &remote, 2)?,
            };
            assert_eq!(gone, 2, "purge: {purge}");
            assert_eq!(left(&mut db)?, [], "purge: {purge}");
        }
        Ok(())
    }

    /// A chain of removed folders deeper than any path may be isn't a cycle: its item is the
    /// last row walked, as at the top, remembered for the chain; that row is its own item, so the
    /// listing lists it as a delete.
    #[test]
    fn a_chain_too_deep_ends_at_the_last_row_walked() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let chain: Vec<NodeId> = (0..hbsync_state::MAX_DEPTH + 2)
            .map(|_| NodeId::new())
            .collect();
        db.write(|s| {
            for w in chain.windows(2) {
                s.put_removed(&removed(w[0], w[1], 1))?;
            }
            Ok(())
        })?;
        let deep = hbsync_state::MAX_DEPTH;
        let items = db.read(|s| {
            let mut rows = RemovedRows::new(s, Vec::new());
            let mut known = HashMap::new();
            let mut items = Vec::new();
            for n in [chain[0], chain[1], chain[deep - 1]] {
                items.push(deleted_by_item(&mut rows, n, &mut known)?);
            }
            Ok(items)
        })?;
        assert_eq!(items, [chain[deep - 1]; 3]);
        Ok(())
    }

    /// Emptying orders and keeps by the links among what it empties alone, however deep: each
    /// folder after the folders in it, and a folder holding something that stays keeps every
    /// folder it is in.
    #[test]
    fn emptying_a_chain_deeper_than_any_path() {
        let chain: Vec<NodeId> = (0..hbsync_state::MAX_DEPTH + 10)
            .map(|_| NodeId::new())
            .collect();
        let folder = |w: &[NodeId]| Chosen {
            node: w[0],
            parent: w.get(1).copied().unwrap_or(NodeId::ROOT),
            folder: true,
        };
        let mut chosen: Vec<Chosen> = (0..chain.len()).map(|i| folder(&chain[i..])).collect();
        let file = NodeId::new();
        chosen.push(Chosen {
            node: file,
            parent: chain[0],
            folder: false,
        });
        let mut holdings = Holdings::default();
        for c in &chosen {
            *holdings.removed.entry(c.parent).or_default() += 1;
        }
        let mut expected = vec![file];
        expected.extend(&chain);
        assert_eq!(select(&chosen, &holdings), expected);
        holdings.live.insert(chain[0]);
        assert_eq!(select(&chosen, &holdings), [file]);
    }

    /// A folder's restore brings back what was removed with it or after it, not what was removed
    /// from it before; each such item is one of its own in Recently deleted.
    #[test]
    fn what_went_before_its_folder_stays() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (f, early, with, deeper) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
        db.write(|s| {
            s.put_removed(&removed(early, f, 5))?;
            s.put_removed(&removed(f, NodeId::ROOT, 10))?;
            s.put_removed(&removed(with, f, 11))?;
            s.put_removed(&removed(deeper, with, 12))
        })?;
        let plan: Option<Vec<NodeId>> = db
            .read(|s| plan_restore(s, f))?
            .map(|p| p.rows.iter().map(|r| r.row.node).collect());
        assert_eq!(plan, Some(vec![f, with, deeper]));
        let tops: HashMap<NodeId, NodeId> = db
            .read(all)?
            .into_iter()
            .map(|(d, _)| (d.node, d.root))
            .collect();
        assert_eq!(tops[&early], early, "deleted on its own");
        assert_eq!(
            (tops[&with], tops[&deeper]),
            (f, f),
            "deleted with the folder"
        );
        Ok(())
    }
}
