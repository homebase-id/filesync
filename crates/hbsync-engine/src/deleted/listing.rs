//! Recently deleted as listed: its items, newest first, each with where it comes back to and
//! how many items are in it, and the token a listing gives ([`Listed`]) that emptying it takes.

use std::collections::{HashMap, HashSet};

use hbsync_state::{ReadTx, RemovedRow, StateError};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind, Role};

use crate::root::AccountReader;
use crate::walk::End;

use super::rules::{Chosen, Held, Holdings, deleted_with, held_below, inside, select};

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
    /// What the listing saw: [`AccountOps::empty_listed`](super::AccountOps::empty_listed) with it
    /// empties exactly that.
    pub listed: Listed,
}

/// What a listing of Recently deleted saw: the rows of this DB (its [`ReadTx::db_id`]) recorded
/// up to the counter it read ([`hbsync_state::Recorded::seq`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listed {
    pub db: String,
    pub seq: u64,
}

/// The drive's newest `limit` deletes of Recently deleted, newest first, each with how many
/// items are in it ([`inside`]), and the summary of all of it.
pub fn deletes(
    s: &ReadTx<'_>,
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
pub fn listed_now(s: &ReadTx<'_>) -> Result<Listed, StateError> {
    Ok(Listed {
        db: s.db_id()?,
        seq: s.removed_seq()?,
    })
}

/// The items of Recently deleted (deletes, or items a delete took along) that are `nodes`, each
/// with how many items are in it: only those rows are read, and walked up and down from.
pub(super) fn find(s: &ReadTx<'_>, nodes: &[NodeId]) -> Result<Vec<(Deleted, usize)>, StateError> {
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
pub(super) fn named(s: &ReadTx<'_>, names: &[String]) -> Result<Vec<(Deleted, usize)>, StateError> {
    let names: HashSet<&str> = names.iter().map(String::as_str).collect();
    let names: Vec<&str> = names.into_iter().collect();
    find(s, &s.removed_named(&names)?)
}

/// The items of Recently deleted removed before `before_ms` (Unix ms, the server's): those
/// deleted on their own, as listed.
pub(super) fn deletes_before(
    s: &ReadTx<'_>,
    before_ms: i64,
) -> Result<Vec<RemovedRow>, StateError> {
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
}

/// Every item of Recently deleted, newest first, each with how many items are in it: listed
/// whole (tests).
#[cfg(any(test, feature = "testkit"))]
pub(super) fn all(s: &ReadTx<'_>) -> Result<Vec<(Deleted, usize)>, StateError> {
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
    fn lazy(s: &'a ReadTx<'s>) -> Result<Self, StateError> {
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
    fn read(s: &'a ReadTx<'s>) -> Result<Self, StateError> {
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
pub(super) struct RemovedRows<'a, 's> {
    s: &'a ReadTx<'s>,
    rows: Vec<RemovedRow>,
    index: HashMap<NodeId, Option<usize>>,
}

impl<'a, 's> RemovedRows<'a, 's> {
    pub(super) fn new(s: &'a ReadTx<'s>, rows: Vec<RemovedRow>) -> Self {
        let index = rows
            .iter()
            .enumerate()
            .map(|(i, r)| (r.row.node, Some(i)))
            .collect();
        RemovedRows { s, rows, index }
    }

    pub(super) fn get(&mut self, node: NodeId) -> Result<Option<&RemovedRow>, StateError> {
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

/// The items of Recently deleted whose deletes took `removed` (rows of it) along
/// ([`deleted_by_item`]).
pub(crate) fn items_of(
    s: &ReadTx<'_>,
    removed: Vec<RemovedRow>,
) -> Result<HashSet<NodeId>, StateError> {
    let nodes: Vec<NodeId> = removed.iter().map(|r| r.row.node).collect();
    let (mut rows, mut known) = (RemovedRows::new(s, removed), HashMap::new());
    nodes
        .into_iter()
        .map(|n| deleted_by_item(&mut rows, n, &mut known))
        .collect()
}

/// The item of Recently deleted whose delete took `node` along: up through the folders it was
/// deleted with, as far as they go (`node` itself, deleted on its own). Memoized in `known`.
pub(super) fn deleted_by_item(
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
fn recovered_name(s: &ReadTx<'_>) -> Result<String, StateError> {
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

/// `top` and what was deleted with it below it, level by level: the removed items in it
/// deleted with their folder ([`deleted_with`]). What was deleted from it before stays out, and
/// so do the live nodes its delete took along ([`crate::along`]): what restoring it restores.
pub(super) fn group(s: &ReadTx<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    inside(
        top,
        |r| (r.row.node, r.row.kind == NodeKind::Folder),
        |folder| {
            let with = s.removed_children(folder.row.node)?.into_iter();
            Ok(with.filter(|r| deleted_with(r.time(), folder)).collect())
        },
    )
}

/// [`inside`] `top`, from the store.
fn inside_of(s: &ReadTx<'_>, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
    inside(
        top,
        |r| (r.row.node, r.row.kind == NodeKind::Folder),
        |r| s.removed_children(r.row.node),
    )
}

/// Refuses `listed` unless this DB gave it ([`listed_here`]).
pub(super) fn refuse_unlisted(s: &ReadTx<'_>, listed: &Listed) -> Result<(), StateError> {
    match listed_here(s, listed)? {
        true => Ok(()),
        false => Err(StateError::Conflict(
            "Recently deleted was never listed so here: look at it again".to_owned(),
        )),
    }
}

/// Whether this DB gave `listed`: its own, up to a counter it reached.
pub fn listed_here(s: &ReadTx<'_>, listed: &Listed) -> Result<bool, StateError> {
    Ok(!listed.db.is_empty() && listed.db == s.db_id()? && listed.seq <= s.removed_seq()?)
}

/// What the folders of Recently deleted hold on the drive, read once (two queries).
pub(super) fn holdings(s: &ReadTx<'_>) -> Result<Holdings, StateError> {
    Ok(Holdings {
        live: s.removed_holding_live()?,
        removed: s.removed_per_folder()?,
    })
}

/// Recently deleted, as the account's DB answers for reading only ([`AccountReader`]).
impl AccountReader {
    /// Recently deleted's newest deletes with their counts, and its summary ([`deletes`]).
    pub fn deletes(
        &mut self,
        limit: usize,
    ) -> Result<(Vec<(Deleted, usize)>, DeletedSummary), StateError> {
        self.db.read(|s| deletes(s, limit))
    }

    /// Whether this DB gave `listed` ([`listed_here`]).
    pub fn listed_here(&mut self, listed: &Listed) -> Result<bool, StateError> {
        self.db.read(|s| listed_here(s, listed))
    }

    /// Items of Recently deleted by node (`find`), and the listing that saw them, which
    /// emptying them takes.
    pub fn find_deleted(
        &mut self,
        nodes: &[NodeId],
    ) -> Result<(Vec<(Deleted, usize)>, Listed), StateError> {
        self.db.read(|s| Ok((find(s, nodes)?, listed_now(s)?)))
    }

    /// Every item of Recently deleted (`all`).
    #[cfg(any(test, feature = "testkit"))]
    pub fn all_deleted(&mut self) -> Result<Vec<(Deleted, usize)>, StateError> {
        self.db.read(all)
    }

    /// The items of Recently deleted named `names` (`named`), and the listing that saw them.
    pub fn named_deleted(
        &mut self,
        names: &[String],
    ) -> Result<(Vec<(Deleted, usize)>, Listed), StateError> {
        self.db.read(|s| Ok((named(s, names)?, listed_now(s)?)))
    }
}
