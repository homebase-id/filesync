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
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use hbsync_platform::name::name_key;
use hbsync_remote::{Answers, Holds, NodeMeta, Remote, RemoteError};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind, RemoteRef, Role, Target};

use crate::error::EngineError;
use crate::exec::{meta_at, record_removed, remove_all};
use crate::feed::{TRACE_MS, apply_tombstone, gone_after, gone_for_good};
use crate::tree::restored_names;
use crate::walk::End;
use hbsync_local::layout;

/// The fewest days Recently deleted keeps an item: the purge is the whole account's, and every
/// client keeps 30 (node schema, The purge). A device may keep longer, never shorter.
pub const MIN_KEEP_DAYS: u32 = 30;

/// Recently deleted as a whole: how much it holds, and what emptying all of it as it is now
/// takes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeletedSummary {
    /// The items deleted on their own.
    pub deletes: usize,
    /// What emptying all of it now deletes for good ([`select`]): every item, those its deletes
    /// took along too, but a folder holding a live node or a removal that stays.
    pub items: usize,
    /// The items deleted on their own that emptying leaves: they hold a live node.
    pub stays: usize,
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
/// items are in it ([`inside`]), and the summary of all of it.
pub fn deletes(
    s: &Store<'_>,
    limit: usize,
) -> Result<(Vec<(Deleted, usize)>, DeletedSummary), StateError> {
    let mut listing = Listing::read(s)?;
    let summary = DeletedSummary {
        deletes: listing.deletes().count(),
        items: listing.going,
        stays: listing
            .deletes()
            .filter(|&i| {
                listing
                    .held
                    .get(&listing.rows.rows[i].row.node)
                    .is_some_and(|h| h.stays)
            })
            .count(),
        listed: listed_now(s)?,
    };
    let picked = listing.deletes().take(limit).collect();
    Ok((listing.with_along(picked)?, summary))
}

/// What a listing of Recently deleted that reads it now sees ([`Listed`]): to read in the
/// transaction that reads the items shown.
pub fn listed_now(s: &Store<'_>) -> Result<Listed, StateError> {
    Ok(Listed {
        db: s.db_id()?,
        seq: s.removed_seq()?,
    })
}

/// The items of Recently deleted (deletes, or items a delete took along) that are `nodes`, each
/// with how many items are in it: only those rows are read, and walked up and down from.
pub(crate) fn find(s: &Store<'_>, nodes: &[NodeId]) -> Result<Vec<(Deleted, usize)>, StateError> {
    let mut listing = Listing::lazy(s)?;
    let mut found = Vec::new();
    for &node in nodes {
        if let Some(item) = listing.describe_node(node)? {
            found.push(item);
        }
    }
    Ok(found)
}

/// The items of Recently deleted named `names` (deletes, or items a delete took along), each
/// with how many items are in it: to pick by their places, which end in those names. Only
/// those rows are read whole, and walked up from.
pub(crate) fn named(s: &Store<'_>, names: &[String]) -> Result<Vec<(Deleted, usize)>, StateError> {
    let names: HashSet<&str> = names.iter().map(String::as_str).collect();
    let names: Vec<&str> = names.into_iter().collect();
    find(s, &s.removed_named(&names)?)
}

/// Every item of Recently deleted, newest first, each with how many items are in it: listed
/// whole (tests).
#[cfg(any(test, feature = "testkit"))]
pub(crate) fn all(s: &Store<'_>) -> Result<Vec<(Deleted, usize)>, StateError> {
    let mut listing = Listing::read(s)?;
    let picked = (0..listing.roots.len()).collect();
    listing.with_along(picked)
}

/// Recently deleted's rows, newest first, each with the item whose delete took it along.
struct Listing<'a, 's> {
    rows: RemovedRows<'a, 's>,
    /// For each of the first rows (the listed ones), its item: itself, deleted on its own.
    roots: Vec<NodeId>,
    /// Of the listed rows, how many emptying all of them deletes for good ([`select`]).
    going: usize,
    /// What each row holds ([`held_below`]).
    held: HashMap<NodeId, Held>,
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
            held: HashMap::new(),
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
        let rows = &listing.rows.rows[..count];
        let holdings = holdings(s)?;
        let chosen: Vec<Chosen> = rows.iter().map(Chosen::of).collect();
        listing.held = held_below(&chosen, &holdings.live);
        // What emptying all of it deletes.
        listing.going = select(&chosen, &holdings).len();
        Ok(listing)
    }

    /// `node`'s row, if it is in Recently deleted, with how many items are in it (as the listing
    /// counts them).
    fn describe_node(&mut self, node: NodeId) -> Result<Option<(Deleted, usize)>, StateError> {
        let Some(top) = self.rows.get(node)?.cloned() else {
            return Ok(None);
        };
        let root = deleted_by_item(&mut self.rows, node, &mut HashMap::new())?;
        let i = self.rows.index[&node].unwrap_or_default();
        // All of Recently deleted in it goes with it: it stays only for a live node in it.
        let s = self.rows.s;
        let below = inside_of(s, top)?;
        let stays = below.iter().any(|r| r.row.kind == NodeKind::Folder) && {
            let live = s.removed_holding_live()?;
            below.iter().any(|r| live.contains(&r.row.node))
        };
        Ok(Some((self.describe_as(i, root, stays)?, below.len() - 1)))
    }

    /// The rows of the items deleted on their own, newest first (the items' own rows give
    /// the order).
    fn deletes(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.roots.len()).filter(|&i| self.roots[i] == self.rows.rows[i].row.node)
    }

    /// Rows `picked`, described, each with how many items are in it.
    fn with_along(&mut self, picked: Vec<usize>) -> Result<Vec<(Deleted, usize)>, StateError> {
        picked
            .into_iter()
            .map(|i| {
                let d = self.describe(i)?;
                let held = self.held.get(&d.node).copied().unwrap_or_default();
                Ok((
                    Deleted {
                        stays: held.stays,
                        ..d
                    },
                    held.count,
                ))
            })
            .collect()
    }

    /// Row `i` as listed: where it comes back to and its path there.
    fn describe(&mut self, i: usize) -> Result<Deleted, StateError> {
        let root = self.roots[i];
        self.describe_as(i, root, false)
    }

    /// Row `i`, deleted with `root`'s delete; `stays` when emptying it leaves it.
    fn describe_as(&mut self, i: usize, root: NodeId, stays: bool) -> Result<Deleted, StateError> {
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
            stays,
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
        // that row is a delete of its own (remembered below), so the listing lists it. (A chain
        // that deep is no path: only a malicious drive makes one; it lists, in pieces.)
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
/// a folder deleted for good ends the path, which starts in Recovered files, where a restore
/// brings what was in it), and that top, from the
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
        End::At(n) => match known.get(&n) {
            Some(place) => place.clone(),
            // Deleted for good: a restore brings it back an orphan, for Recovered files.
            None => (recovered_name(rows.s)?, NodeId::ROOT),
        },
        End::Top | End::Cycle | End::Deep => (String::new(), NodeId::ROOT),
    };
    // Top-down, remembering each folder on the way.
    for (id, name) in up.items.into_iter().rev() {
        path = join(&path, &name);
        known.insert(id, (path.clone(), top));
    }
    Ok((path, top))
}

/// Recovered files' name on the drive, or the one it gets when made.
fn recovered_name(s: &Store<'_>) -> Result<String, StateError> {
    Ok(s.get_mirror(NodeId::RECOVERED)?
        .map_or_else(|| hbsync_types::RECOVERED_NAME.to_owned(), |m| m.name))
}

fn join(folder: &str, name: &str) -> String {
    if folder.is_empty() {
        name.to_owned()
    } else {
        format!("{folder}/{name}")
    }
}

/// The account's DB, held only while it is read or written: never while the server answers.
fn locked(account: &Mutex<StateDb>) -> MutexGuard<'_, StateDb> {
    account.lock().unwrap_or_else(|e| e.into_inner())
}

/// Restores `node` with what was deleted with it, and the folders it was in that are in
/// Recently deleted too (just those, not their other contents). Returns how many items came
/// back; 0 when `node` isn't in Recently deleted (any more).
/// `moved` is set when the drive refused a write for its state (changed or deleted for good
/// meanwhile), or a write may have landed unanswered: the account's picture lacks that.
pub fn restore(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    node: NodeId,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let Some(plan) = locked(account).read(|s| plan_restore(s, node))? else {
        return Ok(0);
    };
    let RestorePlan { rows, live, taken } = plan;
    let mut r = Restoring {
        remote,
        in_plan: rows.iter().map(|r| r.row.node).collect(),
        live,
        taken,
        moved,
        back: Vec::new(),
        gone: Vec::new(),
        failed: None,
    };
    let mut restored = 0;
    let mut left: Vec<&MirrorRow> = rows.iter().map(|r| &r.row).collect();
    // In waves, each after the folders it comes back into: the rows whose folder is back (or
    // never left, or went for good), then those in them. One that was to come back into a
    // folder that didn't stays in Recently deleted.
    while r.failed.is_none() {
        let (wave, later): (Vec<&MirrorRow>, _) = left
            .into_iter()
            .partition(|row| r.live.contains(&row.parent) || !r.in_plan.contains(&row.parent));
        if wave.is_empty() {
            break;
        }
        left = later;
        // Back in its own folder under its own name: only its removal undone, with the others
        // in one request. One whose folder isn't live (so not in the plan either: gone for
        // good) comes back so too, an orphan the main folders move into Recovered files (sync
        // semantics §6.3). The rest, once those answered (a name one didn't take is free
        // again), under a free name: a rewrite each.
        let mut asked = Vec::new();
        let mut rest = Vec::new();
        for row in wave {
            let free = !r.live.contains(&row.parent)
                || r.taken
                    .entry(row.parent)
                    .or_default()
                    .insert(name_key(&row.name));
            if free {
                asked.push(row)
            } else {
                rest.push(row)
            }
        }
        // As many a request as the drive takes.
        for part in asked.chunks(crate::transfer::REMOVALS_AT_ONCE) {
            let targets: Vec<Target> = part.iter().map(|row| row.target()).collect();
            let (answers, stopped) = remote
                .set_removed(&targets, false)
                .unwrap_or_else(Answers::stopped)
                .padded(targets.len());
            // One the call left unanswered fails as it did.
            for (row, answer) in part.iter().zip(answers) {
                let meta = meta_at(row, row.parent, &row.name, false);
                r.settle(row, &meta, answer);
            }
            if let Some(e) = stopped {
                *r.moved = true; // it may have landed
                r.failed.get_or_insert(e);
                break;
            }
        }
        for row in rest {
            if r.failed.is_some() {
                break;
            }
            let names = r.taken.entry(row.parent).or_default();
            let Some(name) = restored_names(&row.name).find(|n| !names.contains(&name_key(n)))
            else {
                continue; // no free name: it stays in Recently deleted
            };
            names.insert(name_key(&name));
            let meta = meta_at(row, row.parent, &name, false);
            let answer = remote.update(&row.target(), &meta, None);
            r.settle(row, &meta, answer);
        }
        // What came back, and what was deleted for good meanwhile, in one write. The mirror's
        // write marks each dirty in the folders it comes back to.
        let (back, gone) = (std::mem::take(&mut r.back), std::mem::take(&mut r.gone));
        locked(account).write(|s| {
            gone.iter().try_for_each(|f| apply_tombstone(s, *f))?;
            back.iter().try_for_each(|m| {
                s.delete_removed(m.node)?;
                s.put_mirror(m)
            })
        })?;
        restored += back.len();
    }
    match r.failed {
        Some(e) => Err(EngineError::Remote(e)),
        None => Ok(restored),
    }
}

/// A restore under way: what is back or going, and the names taken.
struct Restoring<'a> {
    remote: &'a dyn Remote,
    in_plan: HashSet<NodeId>,
    live: HashSet<NodeId>,
    taken: HashMap<NodeId, HashSet<String>>,
    moved: &'a mut bool,
    /// Written back, to record.
    back: Vec<MirrorRow>,
    /// Deleted for good meanwhile, to forget.
    gone: Vec<hbsync_types::RemoteFileId>,
    /// What stops the restore.
    failed: Option<RemoteError>,
}

impl Restoring<'_> {
    /// Takes the drive's answer to `row`'s restore as `meta`.
    fn settle(&mut self, row: &MirrorRow, meta: &NodeMeta, answer: Result<Target, RemoteError>) {
        let e = match answer {
            Ok(w) => {
                let hash = row.file.map(|f| f.hash);
                self.live.insert(row.node);
                self.back.push(MirrorRow::written(
                    w,
                    meta.node,
                    meta.parent,
                    meta.kind,
                    &meta.content,
                    hash,
                ));
                return;
            }
            Err(e) => e,
        };
        // The name it didn't take is free again.
        if let Some(names) = self.taken.get_mut(&meta.parent) {
            names.remove(&name_key(&meta.content.name));
        }
        *self.moved = true;
        match e {
            // Deleted for good meanwhile (its answer or its header says so), maybe without a
            // trace in the feed: gone here too, and what was to come back into it comes back as
            // into any folder gone for good.
            RemoteError::NotFound | RemoteError::Deleted => {
                match gone_after(self.remote, &e, row.file_id) {
                    Ok(true) => {
                        self.gone.push(row.file_id);
                        self.in_plan.remove(&row.node);
                    }
                    Ok(false) => {}
                    Err(e) => {
                        self.failed.get_or_insert(e);
                    }
                }
            }
            // Changed meanwhile: the feed tells what it is now.
            RemoteError::VersionMismatch => {}
            e => {
                self.failed.get_or_insert(e);
            }
        }
    }
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

/// `top` and what was deleted with it below it, level by level: the removed items in it
/// deleted with their folder ([`deleted_with`]). What was deleted from it before stays out, and
/// so do the live nodes its delete took along ([`crate::along`]): what restoring it restores.
fn group(s: &Store<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    // Each node once: removed rows may form a cycle (left by a remote one).
    let mut seen = HashSet::from([top.row.node]);
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        if rows[next].row.kind == NodeKind::Folder {
            let folder = &rows[next];
            let with: Vec<RemovedRow> = s
                .removed_children(folder.row.node)?
                .into_iter()
                .filter(|r| deleted_with(r.time(), folder) && seen.insert(r.row.node))
                .collect();
            rows.extend(with);
        }
        next += 1;
    }
    Ok(rows)
}

/// `top` and every removed node below it, through the removed folders, whenever each was
/// removed (one deleted from a folder before the folder is in it too): what emptying `top`
/// deletes for good with it, as far as it may (node schema, The purge). Each node once: removed
/// rows may form a cycle. `at` gives a row's node and whether it is a folder, `children` a
/// folder's removed children.
pub fn inside<T, E>(
    top: T,
    at: impl Fn(&T) -> (NodeId, bool),
    mut children: impl FnMut(NodeId) -> Result<Vec<T>, E>,
) -> Result<Vec<T>, E> {
    let mut seen = HashSet::from([at(&top).0]);
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        let (node, folder) = at(&rows[next]);
        if folder {
            let below: Vec<T> = children(node)?
                .into_iter()
                .filter(|r| seen.insert(at(r).0))
                .collect();
            rows.extend(below);
        }
        next += 1;
    }
    Ok(rows)
}

/// What a row of Recently deleted holds: how many rows are below it, through removed folders
/// ([`inside`]), and whether emptying it leaves it (a folder in it holds a live node).
#[derive(Debug, Clone, Copy, Default)]
struct Held {
    count: usize,
    stays: bool,
}

/// What each of `rows` (all of Recently deleted) holds, in one pass, given the removed folders
/// holding a live node (`live`). With every row chosen, only a live node keeps one ([`select`]).
/// Rows in a cycle (only a remote one leaves one) are each walked down on their own instead, as
/// a lookup of one does.
fn held_below(rows: &[Chosen], live: &HashSet<NodeId>) -> HashMap<NodeId, Held> {
    let mut children: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for r in rows.iter().filter(|r| r.node != r.parent) {
        children.entry(r.parent).or_default().push(r.node);
    }
    let none = Vec::new();
    let below = |n: &NodeId| children.get(n).unwrap_or(&none);
    let mut held: HashMap<NodeId, Held> = HashMap::new();
    let mut on_path = HashSet::new();
    let mut cycle = false;
    for r in rows {
        let mut stack = vec![(r.node, false)];
        while let Some((n, done)) = stack.pop() {
            if done {
                let mut h = Held {
                    count: 0,
                    stays: live.contains(&n),
                };
                for c in below(&n) {
                    if let Some(ch) = held.get(c) {
                        h.count += 1 + ch.count;
                        h.stays |= ch.stays;
                    }
                }
                held.insert(n, h);
                on_path.remove(&n);
            } else if !held.contains_key(&n) {
                if on_path.insert(n) {
                    stack.push((n, true));
                    stack.extend(below(&n).iter().map(|c| (*c, false)));
                } else {
                    cycle = true;
                }
            }
        }
    }
    if cycle {
        for r in rows {
            let Ok(reach) = inside(
                r.node,
                |n| (*n, true),
                |n| Ok::<_, std::convert::Infallible>(below(&n).clone()),
            );
            let stays = reach.iter().any(|n| live.contains(n));
            held.insert(
                r.node,
                Held {
                    count: reach.len() - 1,
                    stays,
                },
            );
        }
    }
    held
}

/// [`inside`] `top`, from the store.
fn inside_of(s: &Store<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    inside(
        top,
        |r| (r.row.node, r.row.kind == NodeKind::Folder),
        |n| s.removed_children(n),
    )
}

fn plan_restore(s: &Store<'_>, node: NodeId) -> Result<Option<RestorePlan>, StateError> {
    let Some(top) = s.removed_by_node(node)? else {
        return Ok(None);
    };
    // The folders above, as far as they are in Recently deleted (then in the tree, or deleted
    // for good: the item comes back as an orphan, for Recovered files). A chain past any depth
    // a path may have (only a malicious drive makes one) stays where it is: above its top is
    // still a removed folder, not one gone for good.
    let up = crate::walk::up::<_, StateError>(top.row.parent, |at| {
        Ok(s.removed_by_node(at)?.map(|f| {
            let parent = f.row.parent;
            (f, parent)
        }))
    })?;
    if matches!(up.end, End::Deep) {
        return Ok(None);
    }
    let mut chain = up.items;
    chain.reverse();
    // Then the node and what was deleted with it, each after its folder; each node once. What
    // is live still comes back with its folder by itself.
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for r in chain.into_iter().chain(group(s, top)?) {
        if seen.insert(r.row.node) && s.get_mirror(r.row.node)?.is_none() {
            rows.push(r);
        }
    }
    // The folders in the tree the rows may go into, and the folders coming back: their names.
    // (One that is neither went for good: what was in it takes no name there.)
    let mut live = HashSet::from([NodeId::ROOT]);
    let mut taken: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for parent in rows.iter().map(|r| r.row.parent).chain([NodeId::ROOT]) {
        if taken.contains_key(&parent) {
            continue;
        }
        if !parent.is_root() && s.get_mirror(parent)?.is_some() {
            live.insert(parent);
        } else if !parent.is_root() && !seen.contains(&parent) {
            continue;
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

/// The purge: deletes for good the items of Recently deleted kept `keep_ms` by `now_ms` (the
/// server's clock), each with what was deleted with it, as emptying them would ([`select`]:
/// removed nodes only, never a live one, and never a folder holding one), files before their
/// folders; the tombstones as old as [`TRACE_MS`]; and every folder's trash (`.hbsync/trash`)
/// of the same age (by this device's clock, which named its entries). Node schema, "The
/// purge":
/// - It never deletes a live node for good. What the delete of an item kept that long took
///   along and left live (a delete that stopped half way) it first removes, a folder before
///   what is in it ([`remove_taken_along`]): each goes 30 days after its own removal. A
///   removed folder still holding a live node it doesn't take (a file written into it since,
///   one whose time isn't known, one this device doesn't read) stays in Recently deleted,
///   restorable, until someone restores it or the node is removed: possibly indefinitely.
/// - An item holding a removal whose time isn't known yet waits for a purge that knows it.
/// - Each node goes without a trace (a hard delete) once its own removal is [`TRACE_MS`] old,
///   else leaving a tombstone, which goes hard [`TRACE_MS`] after its own time.
/// - What is known gone longer than that is forgotten ([`Store::prune_gone`]).
///
/// Each part runs whatever became of the others (the trash only once the items went); the
/// first error is returned at the end.
/// Returns how many items went. `moved` is set when the drive refused a delete for its state
/// (written since, restored, gone already): the account's picture lacks that.
pub fn purge(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    keep_ms: i64,
    now_ms: i64,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let traced = now_ms.saturating_sub(TRACE_MS);
    let before = now_ms.saturating_sub(keep_ms);
    // Each part on its own: one that fails doesn't keep the others from their work, but for the
    // trash: while Recently deleted's items didn't go, the local copies of deletes stay too.
    let taken = remove_taken_along(account, remote, before, moved);
    let expired = expire(account, remote, before, Some(traced), moved);
    let tombstones = delete_tombstones(account, remote, traced);
    let pruned = locked(account)
        .write(|s| s.prune_gone(traced, now_ms))
        .map(drop)
        .map_err(EngineError::from);
    let trash = match &expired {
        Ok(_) => empty_trashes(account, keep_ms),
        Err(_) => Ok(()),
    };
    taken?;
    let gone = expired?;
    tombstones?;
    pruned?;
    trash?;
    Ok(gone)
}

/// Empties every folder's trash (`.hbsync/trash`) of what went there `keep_ms` ago or more
/// (by this device's clock, which named its entries).
fn empty_trashes(account: &Mutex<StateDb>, keep_ms: i64) -> Result<(), EngineError> {
    let trashed_before = hbsync_types::unix_now_ms().saturating_sub(keep_ms);
    let folders = locked(account).read(|s| s.folders())?;
    for folder in folders {
        if folder.path.is_empty() {
            continue;
        }
        // Only what went there since the purge has been emptying it: older entries (from
        // before Recently deleted) may be the only copy left, and stay.
        let since = locked(account).write(|s| {
            match s.folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE)? {
                Some(v) => Ok(v.parse().unwrap_or(u64::MAX)),
                None => {
                    let now = hbsync_types::unix_now();
                    s.set_folder_kv_of(folder.id, keys::TRASH_PURGED_SINCE, &now.to_string())?;
                    Ok(now)
                }
            }
        })?;
        empty_trash(
            &layout::trash_dir(Path::new(&folder.path)),
            since,
            trashed_before,
        );
    }
    Ok(())
}

/// Deletes for good the items `nodes` name as `listed` showed them, each with all of Recently
/// deleted in it then ([`inside`]: what was deleted with it, and what was deleted from it
/// before), as [`select`]s it, leaving tombstones: nothing recorded since the listing (a removal
/// that arrived inside it later, or the item deleted again after a restore), and never a live
/// node. Refused like [`empty_listed`] for a listing this DB never gave. Returns how many items
/// went.
pub fn empty(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    nodes: &[NodeId],
    listed: &Listed,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let passes = locked(account).read(|s| {
        refuse_unlisted(s, listed)?;
        let mut chosen = Vec::new();
        for &node in nodes {
            if let Some(top) = s.recorded_by_node(node)? {
                // Each row with its `seq`: what the listing saw is up to its counter.
                let rows = inside(
                    top,
                    |r| (r.removed.row.node, r.removed.row.kind == NodeKind::Folder),
                    |n| s.recorded_children(n),
                )?;
                chosen.extend(
                    rows.into_iter()
                        .filter(|r| r.seq <= listed.seq)
                        .map(|r| r.removed),
                );
            }
        }
        going(s, chosen)
    })?;
    delete_for_good(account, remote, &passes, None, moved)
}

/// Deletes for good what a listing saw ([`DeletedSummary::listed`]): the rows recorded up to
/// its counter, an open removal too; nothing recorded since (a removal that arrives later,
/// whatever its server time, or a node deleted again after a restore), and never a live node
/// ([`select`]). Leaves tombstones. Refused for a listing of another DB (the account's, made
/// again) or one this DB never gave. Returns how many items went.
pub fn empty_listed(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    listed: &Listed,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let passes = locked(account).read(|s| {
        refuse_unlisted(s, listed)?;
        let chosen = s.recorded_upto(listed.seq)?;
        going(s, chosen.into_iter().map(|r| r.removed).collect())
    })?;
    delete_for_good(account, remote, &passes, None, moved)
}

/// Refuses `listed` unless this DB gave it ([`listed_here`]).
fn refuse_unlisted(s: &Store<'_>, listed: &Listed) -> Result<(), StateError> {
    match listed_here(s, listed)? {
        true => Ok(()),
        false => Err(StateError::Conflict(
            "Recently deleted was never listed so here: look at it again".to_owned(),
        )),
    }
}

/// Whether this DB gave `listed`: its own, up to a counter it reached.
pub fn listed_here(s: &Store<'_>, listed: &Listed) -> Result<bool, StateError> {
    Ok(!listed.db.is_empty() && listed.db == s.db_id()? && listed.seq <= s.removed_seq()?)
}

/// What emptying `chosen` (rows of Recently deleted) deletes for good, in the passes
/// [`passes`] gives.
fn going(s: &Store<'_>, chosen: Vec<RemovedRow>) -> Result<Vec<Vec<RemovedRow>>, StateError> {
    let picked: Vec<Chosen> = chosen.iter().map(Chosen::of).collect();
    let mut rows: HashMap<NodeId, RemovedRow> =
        chosen.into_iter().map(|r| (r.row.node, r)).collect();
    Ok(passes(&picked, &holdings(s)?)
        .into_iter()
        .map(|pass| pass.into_iter().filter_map(|n| rows.remove(&n)).collect())
        .collect())
}

/// What the folders of Recently deleted hold on the drive, read once (two queries).
fn holdings(s: &Store<'_>) -> Result<Holdings, StateError> {
    Ok(Holdings {
        live: s.removed_holding_live()?,
        removed: s.removed_per_folder()?,
    })
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
pub(crate) fn select(chosen: &[Chosen], holdings: &Holdings) -> Vec<NodeId> {
    passes(chosen, holdings).into_iter().flatten().collect()
}

/// [`select`]'s order in passes, each deleted at once (a hard delete takes a pass in batches):
/// the files, then the folders a level at a time, the deepest first. Nothing in a pass is in
/// another node of it.
pub fn passes(chosen: &[Chosen], holdings: &Holdings) -> Vec<Vec<NodeId>> {
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
    let level = |n: &NodeId| (links[n].1, std::cmp::Reverse(depth[n]));
    let mut going: Vec<NodeId> = links
        .keys()
        .filter(|n| !stays.contains(*n))
        .copied()
        .collect();
    going.sort_by_key(|n| (level(n), *n));
    // Files are one pass, whatever their depth.
    let pass = |n: &NodeId| match links[n].1 {
        false => (false, std::cmp::Reverse(0)),
        true => level(n),
    };
    going
        .chunk_by(|a, b| pass(a) == pass(b))
        .map(<[NodeId]>::to_vec)
        .collect()
}

/// The items of Recently deleted removed before `before_ms` (Unix ms, the server's): those
/// deleted on their own, as listed.
fn due(account: &Mutex<StateDb>, before_ms: i64) -> Result<Vec<RemovedRow>, StateError> {
    locked(account).read(|s| {
        let before = s.removed_before(before_ms)?;
        let count = before.len();
        let mut rows = RemovedRows::new(s, before);
        let (mut tops, mut known) = (Vec::new(), HashMap::new());
        for i in 0..count {
            // In a cycle of rows, one of them.
            let node = rows.rows[i].row.node;
            if deleted_by_item(&mut rows, node, &mut known)? == node {
                tops.push(rows.rows[i].clone());
            }
        }
        Ok(tops)
    })
}

/// The passes ([`passes`]) that delete the items `tops` for good, each with what was deleted
/// with it, if every member was removed before `before` (Unix ms, the server's), by its own
/// removal: one removed later in a folder removed long ago (that a live node kept, say) has its
/// own time in Recently deleted, and so does the item. One whose time isn't known yet waits for
/// a purge that knows it.
fn passes_of(
    s: &Store<'_>,
    tops: Vec<RemovedRow>,
    before: i64,
) -> Result<Vec<Vec<RemovedRow>>, StateError> {
    let mut chosen = Vec::new();
    for top in tops {
        let item = group(s, top)?;
        let due = |r: &RemovedRow| r.removed_at.is_some_and(|at| at < before);
        if item.iter().all(due) {
            chosen.extend(item);
        }
    }
    going(s, chosen)
}

/// Deletes for good the items removed before `before` (Unix ms, the server's), each with what
/// was deleted with it, as [`delete_for_good`] does with `hard_before`: an item goes once all
/// of it was removed before then ([`passes_of`]).
fn expire(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    before: i64,
    hard_before: Option<i64>,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let tops = due(account, before)?;
    let passes = locked(account).read(|s| passes_of(s, tops, before))?;
    delete_for_good(account, remote, &passes, hard_before, moved)
}

/// Removes what the deletes of the items removed before `before` (Unix ms, the server's) took
/// along and left live, a delete that stopped half way (node schema, The purge, 2.), so the
/// items can go: [`crate::along::taken_levels`] picks them, by the reconciler's rule, from the
/// removed folders of those items that hold a live node, and that the drive confirms still
/// removed at the version this device has (a header read each). Top-down, a level at a time,
/// each node at the version this device knows (`update-archival-status-batch`, many a
/// request): one written since stays, and so does what is in it. They go to Recently deleted
/// with their folder's item, at the server's time of this write: 30 days on, they go for good
/// with it.
///
/// A live folder it removes is the nearest removed folder of what is in it from then on: a
/// node in it this device didn't know of (written since its last read, or one it doesn't
/// read) would count as taken along by it. So once the drive's queries show what was written
/// up to the last removal ([`Remote::hold_back`]), the drive is asked whether any of them still
/// holds a live node, and those are put back, with the ones above them it removed
/// ([`put_back`]).
///
/// Returns how many it removed (and didn't put back). A refused or unanswered one sets `moved`;
/// a request that fails stops it with its error, after putting back what needs it; another
/// error is returned once the rest is done.
fn remove_taken_along(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    before: i64,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    let Stuck { folders, live } = locked(account).read(|s| stuck_below(s, before))?;
    let removed_at: HashMap<NodeId, i64> = folders
        .iter()
        .filter_map(|f| Some((f.row.node, f.removed_at?)))
        .collect();
    let levels = crate::along::taken_levels(
        live.values().map(|m| (m.node, m.parent)),
        |f| removed_at.get(&f).copied(),
        |n| live[&n].changed_at,
        |n| Role::of(n).is_some(),
    );
    // The folders it takes from, as the drive has them now: one restored or written since
    // takes nothing.
    let first: HashSet<NodeId> = levels
        .first()
        .into_iter()
        .flatten()
        .map(|n| live[n].parent)
        .collect();
    let mut removed: HashSet<NodeId> = HashSet::new();
    let mut failed = None;
    for f in folders.iter().filter(|f| first.contains(&f.row.node)) {
        match remote.header(f.row.file_id) {
            Ok(h) if h.version == f.row.version && h.removed_at.is_some() => {
                removed.insert(f.row.node);
            }
            Ok(_) | Err(RemoteError::NotFound | RemoteError::Deleted) => *moved = true,
            Err(e) if e.is_transient() => return Err(EngineError::Remote(e)),
            Err(e) => {
                failed.get_or_insert(e);
            }
        }
    }
    // The live folders it removed, as written, with their depth.
    let mut made: Vec<(MirrorRow, usize)> = Vec::new();
    let mut last = Instant::now();
    let mut count = 0;
    let mut stopped = None;
    'levels: for (depth, level) in levels.iter().enumerate() {
        // What is in a node that stays, stays.
        let going: Vec<&MirrorRow> = level
            .iter()
            .map(|n| &live[n])
            .filter(|m| removed.contains(&m.parent))
            .collect();
        for part in going.chunks(crate::transfer::REMOVALS_AT_ONCE) {
            let targets: Vec<Target> = part.iter().map(|m| m.target()).collect();
            let (answers, stop) = remote
                .set_removed(&targets, true)
                .unwrap_or_else(Answers::stopped)
                .padded(part.len());
            last = Instant::now();
            let mut done = Vec::new();
            for (m, answer) in part.iter().zip(answers) {
                match answer {
                    Ok(w) => done.push(MirrorRow {
                        version: w.at.version,
                        token: w.token,
                        changed_at: w.changed_at,
                        ..(*m).clone()
                    }),
                    // Written since, or gone; or unanswered, so it may have landed: the feed
                    // tells what it is now.
                    Err(_) if stop.is_some() => *moved = true,
                    Err(
                        RemoteError::VersionMismatch | RemoteError::NotFound | RemoteError::Deleted,
                    ) => *moved = true,
                    Err(e) => {
                        failed.get_or_insert(e);
                    }
                }
            }
            count += done.len();
            removed.extend(done.iter().map(|m| m.node));
            made.extend(
                done.iter()
                    .filter(|m| m.kind == NodeKind::Folder)
                    .map(|m| (m.clone(), depth)),
            );
            locked(account).write(|s| {
                done.into_iter()
                    .try_for_each(|m| record_removed(s, m.file_id, Some(m)))
            })?;
            if stop.is_some() {
                stopped = stop;
                break 'levels;
            }
        }
    }
    count -= put_back(account, remote, made, last, moved)?;
    if count > 0 {
        tracing::info!("the purge removed {count} items a delete took along and left");
    }
    match stopped.or(failed) {
        Some(e) => Err(EngineError::Remote(e)),
        None => Ok(count),
    }
}

/// Puts back the live folders [`remove_taken_along`] removed (`made`, each as written, with its
/// depth) that the drive says hold a live node, asked once its queries show what was written
/// up to `last` (the last removal), and the folders above them among `made`: top-down, each at
/// the version written, as a restore does. One that doesn't come back (written since) sets
/// `moved`. Returns how many came back.
fn put_back(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    made: Vec<(MirrorRow, usize)>,
    last: Instant,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    if made.is_empty() {
        return Ok(0);
    }
    std::thread::sleep(remote.hold_back().saturating_sub(last.elapsed()));
    let nodes: Vec<NodeId> = made.iter().map(|(m, _)| m.node).collect();
    let holding = remote.holding(&nodes, Holds::Live)?;
    if holding.is_empty() {
        return Ok(0);
    }
    let made: HashMap<NodeId, (MirrorRow, usize)> =
        made.into_iter().map(|(m, d)| (m.node, (m, d))).collect();
    let mut back: Vec<&(MirrorRow, usize)> = Vec::new();
    let mut seen = HashSet::new();
    for &folder in &holding {
        let mut at = folder;
        while let Some(row) = made.get(&at)
            && seen.insert(at)
        {
            back.push(row);
            at = row.0.parent;
        }
    }
    back.sort_by_key(|(m, depth)| (*depth, m.node));
    let mut came = 0;
    for level in back.chunk_by(|a, b| a.1 == b.1) {
        let targets: Vec<Target> = level.iter().map(|(m, _)| m.target()).collect();
        let (answers, stopped) = remote
            .set_removed(&targets, false)
            .unwrap_or_else(Answers::stopped)
            .padded(targets.len());
        let rows: Vec<MirrorRow> = level
            .iter()
            .zip(answers)
            .filter_map(|((m, _), answer)| match answer {
                Ok(w) => Some(MirrorRow {
                    version: w.at.version,
                    token: w.token,
                    changed_at: w.changed_at,
                    ..m.clone()
                }),
                Err(_) => {
                    *moved = true;
                    None
                }
            })
            .collect();
        came += rows.len();
        locked(account).write(|s| {
            rows.iter().try_for_each(|m| {
                s.delete_removed(m.node)?;
                s.put_mirror(m)
            })
        })?;
        if let Some(e) = stopped {
            return Err(EngineError::Remote(e));
        }
    }
    Ok(came)
}

/// The removed folders whose delete may have left live what it took along, and what is live
/// below them.
#[derive(Default)]
struct Stuck {
    folders: Vec<RemovedRow>,
    /// By node.
    live: HashMap<NodeId, MirrorRow>,
}

/// The removed folders of the items removed before `before` (Unix ms, the server's) that hold
/// a live node written no later than their removal ([`Store::removed_holding_taken`]: none,
/// mostly, at the cost of one query), and the live nodes below them, directly or through live
/// folders written by then too (one written after takes nothing in it along).
fn stuck_below(s: &Store<'_>, before: i64) -> Result<Stuck, StateError> {
    let mut stuck = Stuck::default();
    let mut rows = RemovedRows::new(s, Vec::new());
    let mut known = HashMap::new();
    for folder in s.removed_holding_taken()? {
        let item = deleted_by_item(&mut rows, folder, &mut known)?;
        let due = rows
            .get(item)?
            .is_some_and(|r| r.removed_at.is_some_and(|at| at < before));
        if due && let Some(row) = rows.get(folder)? {
            stuck.folders.push(row.clone());
        }
    }
    let mut next: Vec<(NodeId, i64)> = stuck
        .folders
        .iter()
        .filter_map(|f| Some((f.row.node, f.removed_at?)))
        .collect();
    while let Some((folder, removed_at)) = next.pop() {
        for m in s.mirror_children(folder)? {
            if m.kind == NodeKind::Folder
                && m.changed_at.is_some_and(|at| at <= removed_at)
                && !stuck.live.contains_key(&m.node)
            {
                next.push((m.node, removed_at));
            }
            stuck.live.insert(m.node, m);
        }
    }
    Ok(stuck)
}

/// Deletes `passes` for good ([`going`]), one pass at a time, each node at the version this
/// device knows: one written since stays, and so do the folders above it. A folder goes only
/// while nothing else is in it, on this device or on the drive: one question for the pass's
/// folders that may go ([`Holds::AnyBut`]), asked once the drive's queries show what was
/// written up to the pass's start ([`Remote::hold_back`]; a pass with none asks nothing, and
/// doesn't wait). Any file there that isn't deleted, live or removed, keeps its folder, one
/// this device doesn't know or doesn't read too: the earlier passes' files went, and nothing
/// in a pass is in another node of it, so it isn't going; but the later passes' rows at their
/// versions ([`before_folders_go`]): round a cycle of removed folders, one holds another
/// still to go, and goes first. A node removed before `hard_before` (Unix ms, the
/// server's) goes without a trace; any other leaves a tombstone (`None`: every one does),
/// recorded with its time; both in batches, many files a request. Returns how many went. One written since, or not found, sets `moved`; one not found
/// is recorded gone once its header says so too. A request that fails stops it with its error:
/// what it didn't answer stays, for the next purge.
fn delete_for_good(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    passes: &[Vec<RemovedRow>],
    hard_before: Option<i64>,
    moved: &mut bool,
) -> Result<usize, EngineError> {
    // What each folder holds of the rows still to go: what is in a folder goes before it (but
    // round a cycle).
    let mut to_go: HashMap<NodeId, u64> = HashMap::new();
    for r in passes.iter().flatten() {
        *to_go.entry(r.row.parent).or_default() += 1;
    }
    let hard = |r: &RemovedRow| {
        hard_before.is_some_and(|before| r.removed_at.is_some_and(|at| at > 0 && at < before))
    };
    // The rows of the passes not yet begun, at their versions.
    let mut later: HashSet<RemoteRef> = passes
        .iter()
        .flatten()
        .map(|r| r.row.remote_ref())
        .collect();
    let mut gone = 0;
    for pass in passes {
        let began = Instant::now();
        for r in pass {
            later.remove(&r.row.remote_ref());
        }
        let mut going: Vec<&RemovedRow> = Vec::new();
        for r in pass {
            let held = to_go.get(&r.row.node).copied().unwrap_or(0);
            if let Some(n) = to_go.get_mut(&r.row.parent) {
                *n -= 1;
            }
            // A folder holding anything else here (what failed to go, or came meanwhile) stays:
            // the drive's answer below covers it too, this keeps it out of the question.
            if r.row.kind == NodeKind::Folder
                && locked(account).read(|s| s.child_count(r.row.node))? > held
            {
                continue;
            }
            going.push(r);
        }
        let folders: Vec<NodeId> = going
            .iter()
            .filter(|r| r.row.kind == NodeKind::Folder)
            .map(|r| r.row.node)
            .collect();
        if !folders.is_empty() {
            // The drive's queries leave out what changed in their last moments: what was
            // written up to the pass's start shows once that has passed.
            std::thread::sleep(remote.hold_back().saturating_sub(began.elapsed()));
            let holding = remote.holding(&folders, before_folders_go(&later))?;
            going.retain(|r| !holding.contains(&r.row.node));
        }
        let (hard, soft): (Vec<&RemovedRow>, Vec<&RemovedRow>) =
            going.into_iter().partition(|r| hard(r));
        let refs = |rows: &[&RemovedRow]| -> Vec<RemoteRef> {
            rows.iter().map(|r| r.row.remote_ref()).collect()
        };
        let (answers, mut failed) = hard_delete(remote, &refs(&hard));
        let mut answered: Vec<(&RemovedRow, Answer)> = hard.into_iter().zip(answers).collect();
        if failed.is_none() {
            let (answers, stopped) = answers_to(&refs(&soft), |f| remote.delete(f), |at| at);
            failed = stopped;
            answered.extend(soft.into_iter().zip(answers));
        }
        // Each gone with its tombstone's time, if it left one.
        let mut gone_now: Vec<(&RemovedRow, Option<i64>)> = Vec::new();
        let mut not_found: Vec<&RemovedRow> = Vec::new();
        for (r, answer) in answered {
            match answer {
                Answer::Deleted(tombstone) => gone_now.push((r, tombstone)),
                Answer::Missing => not_found.push(r),
                // Restored or changed meanwhile: not ours to delete.
                Answer::Written => *moved = true,
                Answer::Kept => {}
            }
        }
        // Not found: deleted for good meanwhile (another device's purge), if its header says
        // so too; else it stays. Either way the picture lacked it. A request that fails stops
        // the asking: the rest stay, for the next purge.
        if !not_found.is_empty() {
            *moved = true;
        }
        if failed.is_none() {
            for r in not_found {
                match gone_for_good(remote, r.row.file_id) {
                    Ok(true) => gone_now.push((r, None)),
                    Ok(false) => {}
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
        }
        locked(account).write(|s| {
            for (r, tombstone) in &gone_now {
                s.delete_removed(r.row.node)?;
                // Its id never comes back, nor its row from a page read before: gone since its
                // tombstone's time, if the answer gave it (the tombstone itself, at its
                // version, comes with the feed's row).
                match tombstone {
                    Some(at) => s.mark_file_gone_at(r.row.file_id, *at)?,
                    None => s.mark_file_gone(r.row.file_id)?,
                }
            }
            Ok(())
        })?;
        gone += gone_now.len();
        if let Some(e) = failed {
            return Err(EngineError::Remote(e));
        }
    }
    Ok(gone)
}

/// What the drive is asked about a pass's folders just before they go for good, `later` the
/// rows of the passes after it, at their versions: whether a file that isn't deleted is in one,
/// live or removed, but those ([`delete_for_good`]). One that is keeps its folder.
pub fn before_folders_go(later: &HashSet<RemoteRef>) -> Holds<'_> {
    Holds::AnyBut(later)
}

/// What the drive answered for a file a delete for good asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// Gone, leaving a tombstone written at this time (Unix ms, the server's), if it said.
    Deleted(Option<i64>),
    /// Not there: gone already, by another device's purge, maybe (the picture lacked that).
    Missing,
    /// Written since: it stays, and the picture lacked that.
    Written,
    /// Refused for another reason (logged, and an error of the call): it stays.
    Kept,
}

/// Deletes `files` without a trace, each only at its version: [`answers_to`].
fn hard_delete(remote: &dyn Remote, files: &[RemoteRef]) -> (Vec<Answer>, Option<RemoteError>) {
    answers_to(files, |f| remote.delete_for_good(f), |()| None)
}

/// What the drive answered when `delete` asked about `files` (nothing for none), each deleted
/// with `tombstone` of its answer: an answer for each of the first files, and the error of the
/// request that stopped it there, if one did (the files not answered stay), or else the first
/// answer that isn't one of [`Answer`]'s.
fn answers_to<T>(
    files: &[RemoteRef],
    delete: impl FnOnce(&[RemoteRef]) -> Result<Answers<T>, RemoteError>,
    tombstone: impl Fn(T) -> Option<i64>,
) -> (Vec<Answer>, Option<RemoteError>) {
    if files.is_empty() {
        return (Vec::new(), None);
    }
    let Answers { answers, stopped } = delete(files).unwrap_or_else(Answers::stopped);
    // Any other answer for a file: it stays, and fails the purge (retried later).
    let mut refused: Option<(usize, RemoteError)> = None;
    let answers = answers
        .into_iter()
        .map(|answer| match answer {
            Ok(t) => Answer::Deleted(tombstone(t)),
            Err(RemoteError::NotFound) => Answer::Missing,
            Err(RemoteError::VersionMismatch) => Answer::Written,
            Err(e) => {
                match &mut refused {
                    Some((n, _)) => *n += 1,
                    None => refused = Some((1, e)),
                }
                Answer::Kept
            }
        })
        .collect();
    let refused = refused.map(|(n, e)| {
        tracing::warn!("{n} of {} files weren't deleted: {e}", files.len());
        e
    });
    (answers, stopped.or(refused))
}

/// Deletes hard the tombstones the feed brought that were written before `before_ms` (Unix ms,
/// the server's clock): a delete for good that left one leaves nothing in the feed once every
/// device has had time to see it. Each at its version: one written since stays, and so does
/// one a failed request didn't answer.
fn delete_tombstones(
    account: &Mutex<StateDb>,
    remote: &dyn Remote,
    before_ms: i64,
) -> Result<(), EngineError> {
    let tombstones = locked(account).read(|s| s.tombstones_before(before_ms))?;
    let (answers, failed) = hard_delete(remote, &tombstones);
    locked(account).write(|s| {
        tombstones
            .iter()
            .zip(answers)
            .filter(|(_, answer)| matches!(answer, Answer::Deleted(_) | Answer::Missing))
            .try_for_each(|(t, _)| s.delete_tombstone_at(*t).map(drop))
    })?;
    match failed {
        Some(e) => Err(EngineError::Remote(e)),
        None => Ok(()),
    }
}

/// Deletes the trash entries (`<Unix s>-<node>`) that went there from `since` (Unix s) until
/// before `before_ms`. Best effort: one that can't go now goes at the next purge.
fn empty_trash(trash: &Path, since: u64, before_ms: i64) {
    let Ok(entries) = fs::read_dir(trash) else {
        return;
    };
    for entry in entries.flatten() {
        let at = entry.file_name().to_str().and_then(layout::trashed_at);
        if at.is_some_and(|s| s >= since && (s as i64).saturating_mul(1000) < before_ms) {
            let _ = remove_all(&entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use hbsync_remote::NodeMeta;
    use hbsync_state::MirrorRow;
    use hbsync_types::{RemoteFileId, Uuid, VersionTag};

    use super::*;

    /// The account's DB as the drive holds it, read and written by a test as by the drive.
    struct Db(Mutex<StateDb>);

    impl Db {
        fn new() -> Result<Db, StateError> {
            Ok(Db(Mutex::new(StateDb::open_in_memory()?)))
        }

        fn read<T>(
            &self,
            f: impl FnOnce(&Store<'_>) -> Result<T, StateError>,
        ) -> Result<T, StateError> {
            locked(&self.0).read(f)
        }

        fn write<T>(
            &self,
            f: impl FnOnce(&Store<'_>) -> Result<T, StateError>,
        ) -> Result<T, StateError> {
            locked(&self.0).write(f)
        }
    }

    impl std::ops::Deref for Db {
        type Target = Mutex<StateDb>;

        fn deref(&self) -> &Mutex<StateDb> {
            &self.0
        }
    }

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
        let db = Db::new()?;
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

    fn listed(db: &Db) -> Result<Listed, StateError> {
        Ok(db.read(|s| deletes(s, 0))?.1.listed)
    }

    fn left(db: &Db) -> Result<Vec<NodeId>, StateError> {
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
        let db = Db::new()?;
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
        assert_eq!(empty_listed(&db, &remote, &summary.listed, &mut false)?, 2);
        assert_eq!(left(&db)?, sorted(vec![f, late, later_with, lone]));
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
            let db = Db::new()?;
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
            // Each delete counts its members (its restore restores them), whatever emptying
            // can take now; a pick counts the same.
            assert_eq!((along[&outer], along[&g], along[&h]), (2, 0, 0));
            let found = db.read(|s| find(s, &[outer]))?;
            assert_eq!(found[0].1, 2, "a pick counts the same");
            let went = match how {
                "listed" => {
                    // The count it shows is what emptying takes.
                    let (_, summary) = db.read(|s| deletes(s, 0))?;
                    assert_eq!((summary.deletes, summary.items), (4, 2));
                    empty_listed(&db, &remote, &summary.listed, &mut false)?
                }
                "deletes" => empty(
                    &db,
                    &remote,
                    &[outer, other, g, h],
                    &listed(&db)?,
                    &mut false,
                )?,
                _ => expire(&db, &remote, 11, None, &mut false)?,
            };
            assert_eq!(went, 2, "{how}");
            assert_eq!(left(&db)?, sorted(vec![outer, f, g, h]), "{how}");
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
        let db = Db::new()?;
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
        let listing = listed(&db)?;
        assert_eq!(empty_listed(&db, &remote, &listing, &mut false)?, 0);
        assert_eq!(left(&db)?, sorted(vec![f, child]));
        Ok(())
    }

    /// A listing of another DB of the same identity (made again since), or one this DB never
    /// gave, empties nothing; one that saw nothing empties nothing.
    #[test]
    fn emptying_an_unknown_listing_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        let db = Db::new()?;
        let x = NodeId::new();
        db.write(|s| s.put_removed(&removed(x, NodeId::ROOT, 1)))?;
        let listing = listed(&db)?;
        let ahead = Listed {
            seq: listing.seq + 1,
            ..listing.clone()
        };
        assert!(empty_listed(&db, &remote, &ahead, &mut false).is_err());
        // Another DB, its counter as far.
        let again = Db::new()?;
        let elsewhere = listed(&again)?;
        assert_ne!(elsewhere.db, listing.db);
        let other = Listed {
            db: elsewhere.db,
            ..listing.clone()
        };
        assert!(empty_listed(&db, &remote, &other, &mut false).is_err());
        let nothing = Listed {
            seq: 0,
            ..listing.clone()
        };
        assert_eq!(empty_listed(&db, &remote, &nothing, &mut false)?, 0);
        assert_eq!(left(&db)?, [x], "nothing went");
        assert_eq!(empty_listed(&db, &remote, &listing, &mut false)?, 1);
        Ok(())
    }

    /// Removed folders in a cycle are listed, so emptying as listed deletes them for good, and
    /// so does the purge once they expire.
    #[test]
    fn a_cycle_in_recently_deleted_is_emptied() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        for purge in [false, true] {
            let db = Db::new()?;
            let (a, b) = (NodeId::new(), NodeId::new());
            db.write(|s| {
                s.put_removed(&removed(a, b, 1))?;
                s.put_removed(&removed(b, a, 1))
            })?;
            let gone = match purge {
                false => {
                    let listing = listed(&db)?;
                    empty_listed(&db, &remote, &listing, &mut false)?
                }
                true => expire(&db, &remote, 2, None, &mut false)?,
            };
            assert_eq!(gone, 2, "purge: {purge}");
            assert_eq!(left(&db)?, [], "purge: {purge}");
        }
        Ok(())
    }

    /// A chain of removed folders deeper than any path may be isn't a cycle: its item is the
    /// last row walked, as at the top, remembered for the chain; that row is its own item, so the
    /// listing lists it as a delete.
    #[test]
    fn a_chain_too_deep_ends_at_the_last_row_walked() -> Result<(), StateError> {
        let db = Db::new()?;
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

    /// Items picked by name are those rows alone, described and counted as the listing does
    /// them: a delete with its members, an item taken along with none.
    #[test]
    fn items_are_found_by_name() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let [run, data, file, other, other_data] = [(); 5].map(|_| NodeId::new());
        let named = |node, parent, name: &str, at| RemovedRow {
            row: MirrorRow {
                name: name.into(),
                ..removed(node, parent, at).row
            },
            ..removed(node, parent, at)
        };
        db.write(|s| {
            s.put_removed(&named(run, NodeId::ROOT, "run", 10))?;
            s.put_removed(&named(data, run, "data", 10))?;
            s.put_removed(&named(file, data, "f.bin", 10))?;
            s.put_removed(&named(other, NodeId::ROOT, "other", 20))?;
            s.put_removed(&named(other_data, other, "data", 5))
        })?;
        let mut found: Vec<(NodeId, NodeId, usize)> = db
            .read(named_items)?
            .into_iter()
            .map(|(d, n)| (d.node, d.root, n))
            .collect();
        found.sort();
        let listed: HashMap<NodeId, (NodeId, usize)> = db
            .read(all)?
            .into_iter()
            .map(|(d, n)| (d.node, (d.root, n)))
            .collect();
        let mut expected = vec![
            (data, listed[&data].0, listed[&data].1),
            (other_data, listed[&other_data].0, listed[&other_data].1),
        ];
        expected.sort();
        assert_eq!(found, expected);
        assert_eq!(listed[&data], (run, 1), "taken along, holding f.bin");
        assert_eq!(
            listed[&other_data],
            (other_data, 0),
            "deleted on its own, before"
        );
        Ok(())
    }

    fn named_items(s: &Store<'_>) -> Result<Vec<(Deleted, usize)>, StateError> {
        named(s, &["data".to_owned()])
    }

    /// A folder's restore brings back what was removed with it or after it, not what was removed
    /// from it before; each such item is one of its own in Recently deleted.
    #[test]
    fn what_went_before_its_folder_stays() -> Result<(), StateError> {
        let db = Db::new()?;
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

    /// The purge removes what a delete that stopped half way left live, a folder before what is
    /// in it, at the versions this device knows: one written since (its version here an old
    /// one) stays, and so does what is in it; so does one written after the removal, one whose
    /// time isn't known, what a folder restored on the drive since holds, and what the delete
    /// of an item not kept long enough left.
    #[test]
    fn the_purge_removes_only_what_a_delete_took_along() -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        let db = Db::new()?;
        let meta = |node: NodeId, parent: NodeId| NodeMeta {
            node,
            parent,
            kind: NodeKind::Folder,
            content: hbsync_types::schema::NodeContent::folder(node.0.to_string()),
            removed: false,
        };
        let [f, a, b, stale, in_stale, late, unknown] = [(); 7].map(|_| NodeId::new());
        let [back, in_back, young, in_young] = [(); 4].map(|_| NodeId::new());
        let mut rows: HashMap<NodeId, MirrorRow> = HashMap::new();
        let row = |node, parent, w: Target| {
            let m = meta(node, parent);
            MirrorRow::written(w, node, parent, m.kind, &m.content, None)
        };
        for (node, parent) in [
            (f, NodeId::ROOT),
            (a, f),
            (b, a),
            (stale, f),
            (in_stale, stale),
            (late, f),
            (unknown, f),
            (back, NodeId::ROOT),
            (in_back, back),
            (young, NodeId::ROOT),
            (in_young, young),
        ] {
            rows.insert(
                node,
                row(node, parent, remote.create(&meta(node, parent), None)?),
            );
        }
        let set_removed =
            |row: &MirrorRow, removed: bool| -> Result<Target, Box<dyn std::error::Error>> {
                let answers = remote
                    .set_removed(&[row.target()], removed)?
                    .into_answers()?;
                Ok(answers.into_iter().next().ok_or("no answer")??)
            };
        // The deletes: the folders alone (each stopped there); young's not kept long enough.
        for node in [f, back] {
            rows.insert(
                node,
                row(node, NodeId::ROOT, set_removed(&rows[&node], true)?),
            );
        }
        remote.server().advance(1000);
        let before = rows[&f].changed_at.ok_or("no time")? + 500;
        rows.insert(
            young,
            row(young, NodeId::ROOT, set_removed(&rows[&young], true)?),
        );
        // Written since, on another device: this one has the old version.
        remote.update(&rows[&stale].target(), &meta(stale, f), None)?;
        // Written after the removal, as this device knows.
        let w = remote.update(&rows[&late].target(), &meta(late, f), None)?;
        rows.insert(late, row(late, f, w));
        // Restored on the drive since: this device still has it removed.
        set_removed(&rows[&back], false)?;
        db.write(|s| {
            for (node, r) in &rows {
                match *node {
                    n if [f, back, young].contains(&n) => s.put_removed(&RemovedRow {
                        removed_at: r.changed_at,
                        row: r.clone(),
                    })?,
                    n if n == unknown => s.put_mirror(&MirrorRow {
                        changed_at: None,
                        ..r.clone()
                    })?,
                    _ => s.put_mirror(r)?,
                }
            }
            Ok(())
        })?;
        let mut moved = false;
        assert_eq!(remove_taken_along(&db, &remote, before, &mut moved)?, 2);
        assert!(moved, "the drive had what this device didn't");
        for n in [a, b] {
            assert!(db.read(|s| s.removed_by_node(n))?.is_some(), "{n:?}");
            assert!(db.read(|s| s.get_mirror(n))?.is_none(), "{n:?}");
        }
        let removed_on_drive: HashSet<NodeId> =
            remote.server().removed().filter_map(|h| h.node()).collect();
        assert_eq!(removed_on_drive, HashSet::from([f, a, b, young]));
        for n in [stale, in_stale, late, unknown, in_back, in_young] {
            assert!(db.read(|s| s.get_mirror(n))?.is_some(), "{n:?}");
        }
        // Its folder kept that long, its own removal not: it waits for its own 30 days.
        assert_eq!(expire(&db, &remote, before, None, &mut false)?, 0);
        Ok(())
    }

    /// A folder goes for good only with everything removed below it: a removed node on the
    /// drive this device doesn't know (removed since its last read, or one it doesn't read)
    /// keeps its folder, which stays in Recently deleted. The rest goes, a cycle of removed
    /// folders too (each holds the other, still to go). Emptying as listed, emptying the
    /// deletes and the purge agree.
    #[test]
    fn a_folder_holding_a_removal_this_device_doesnt_know_stays()
    -> Result<(), Box<dyn std::error::Error>> {
        for how in ["listed", "deletes", "purge"] {
            let remote = fake();
            let db = Db::new()?;
            let [f, unknown, other, in_other, a, b] = [(); 6].map(|_| NodeId::new());
            for (node, parent) in [
                (f, NodeId::ROOT),
                (unknown, f),
                (other, NodeId::ROOT),
                (in_other, other),
                (a, b),
                (b, a),
            ] {
                let name = node.0.to_string();
                let meta = NodeMeta {
                    node,
                    parent,
                    kind: NodeKind::Folder,
                    content: hbsync_types::schema::NodeContent::folder(name),
                    removed: true,
                };
                let w = remote.create(&meta, None)?;
                if node != unknown {
                    let row = MirrorRow::written(w, node, parent, meta.kind, &meta.content, None);
                    db.write(|s| {
                        s.put_removed(&RemovedRow {
                            row,
                            removed_at: Some(10),
                        })
                    })?;
                }
            }
            let went = match how {
                "listed" => {
                    let listing = listed(&db)?;
                    empty_listed(&db, &remote, &listing, &mut false)?
                }
                "deletes" => empty(&db, &remote, &[f, other, a], &listed(&db)?, &mut false)?,
                _ => expire(&db, &remote, 11, None, &mut false)?,
            };
            assert_eq!(went, 4, "{how}");
            assert_eq!(left(&db)?, [f], "{how}");
            let on_drive: HashSet<NodeId> =
                remote.server().removed().filter_map(|h| h.node()).collect();
            assert_eq!(on_drive, HashSet::from([f, unknown]), "{how}");
        }
        Ok(())
    }

    /// A live folder the purge removes becomes the nearest removed folder of what is in it: a
    /// node in it this device didn't know of (written since its last read, or one it doesn't
    /// read) would count as taken along by it. The drive is asked after, and the folder put
    /// back, with the ones above it the purge removed; what went with them stays removed.
    #[test]
    fn a_folder_holding_what_this_device_didnt_know_is_put_back()
    -> Result<(), Box<dyn std::error::Error>> {
        let remote = fake();
        let db = Db::new()?;
        let folder = |node: NodeId, parent: NodeId| NodeMeta {
            node,
            parent,
            kind: NodeKind::Folder,
            content: hbsync_types::schema::NodeContent::folder(node.0.to_string()),
            removed: false,
        };
        let [f, outer, a, z, y, b] = [(); 6].map(|_| NodeId::new());
        let mut rows: HashMap<NodeId, MirrorRow> = HashMap::new();
        for (node, parent) in [
            (f, NodeId::ROOT),
            (outer, f),
            (a, outer),
            (z, a),
            (y, outer),
            (b, f),
        ] {
            let m = folder(node, parent);
            let w = remote.create(&m, None)?;
            rows.insert(
                node,
                MirrorRow::written(w, node, parent, m.kind, &m.content, None),
            );
        }
        let w = remote
            .set_removed(&[rows[&f].target()], true)?
            .into_answers()?;
        let w = w.into_iter().next().ok_or("no answer")??;
        let removal = w.changed_at.ok_or("no time")?;
        rows.insert(
            f,
            MirrorRow {
                version: w.at.version,
                changed_at: w.changed_at,
                ..rows[&f].clone()
            },
        );
        // Written since this device's last read: z in a, and in b a node it doesn't read.
        remote.update(&rows[&z].target(), &folder(z, a), None)?;
        let (_, unread_at) = remote.server().seed_file(
            b,
            "unread.bin",
            b"u",
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
        remote.server().unlist(unread_at.file_id);
        db.write(|s| {
            for (node, r) in &rows {
                if *node == f {
                    s.put_removed(&RemovedRow {
                        removed_at: Some(removal),
                        row: r.clone(),
                    })?;
                } else {
                    s.put_mirror(r)?;
                }
            }
            Ok(())
        })?;
        remote.server().advance(1000);
        let mut moved = false;
        // a, outer above it, and b came back; y stays removed.
        assert_eq!(
            remove_taken_along(&db, &remote, removal + 500, &mut moved)?,
            1
        );
        assert!(moved);
        let live: HashSet<NodeId> = remote.server().live().filter_map(|h| h.node()).collect();
        assert_eq!(live.len(), 5, "{live:?}");
        assert!(
            [outer, a, z, b].iter().all(|n| live.contains(n)),
            "{live:?}"
        );
        assert!(db.read(|s| s.removed_by_node(y))?.is_some());
        for n in [outer, a, b] {
            assert!(
                db.read(|s| s.get_mirror(n))?.is_some(),
                "{n:?} back here too"
            );
            assert!(db.read(|s| s.removed_by_node(n))?.is_none(), "{n:?}");
        }
        Ok(())
    }
}
