//! Recently deleted's rules, pure: what a folder's delete took with it, what each item holds,
//! what emptying deletes for good and in which order, and a restore's waves. The simulator
//! shares the public ones.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use hbsync_platform::name::name_key;
use hbsync_remote::Holds;
use hbsync_state::RemovedRow;
use hbsync_types::{NodeId, NodeKind, RemoteRef, Role};

use crate::tree::restored_names;

/// Whether a node removed at `removed_at` was deleted with `folder`, the folder it is in, in
/// Recently deleted too: removed no earlier than the folder (node schema, Recently deleted).
pub(crate) fn deleted_with(removed_at: i64, folder: &RemovedRow) -> bool {
    folder.time() <= removed_at
}

/// The next wave of a restore, its rows split by the names they come back under.
#[derive(Debug, PartialEq)]
pub struct RestoreWave<'r, R> {
    /// Back in its own folder under its own name, free there (taken now). One whose folder isn't
    /// live (so not in the restore either: gone for good) comes back so too, an orphan the main
    /// folders move into Recovered files (sync semantics §6.3).
    pub asked: Vec<&'r R>,
    /// Its name taken in its folder: under a free name.
    pub rest: Vec<&'r R>,
    /// In a folder that isn't back yet: for a later wave.
    pub later: Vec<&'r R>,
}

/// Of `left`, the rows of a restore still to come back, the next wave: those whose folder is
/// back (`live`), or never left or went for good (not in the restore: `in_plan`), each after the
/// folders it comes back into; the rest wait for a later one. The rows asked back under their
/// own names take them in `taken` (the name keys taken in each live folder). `None` when no row
/// can come back now. `place` gives a row's folder and name. The engine's restore and the
/// simulator's share it.
pub fn restore_wave<'r, R>(
    left: Vec<&'r R>,
    place: impl Fn(&R) -> (NodeId, &str),
    live: &HashSet<NodeId>,
    in_plan: &HashSet<NodeId>,
    taken: &mut HashMap<NodeId, HashSet<String>>,
) -> Option<RestoreWave<'r, R>> {
    let (wave, later): (Vec<&R>, _) = left.into_iter().partition(|row| {
        let parent = place(row).0;
        live.contains(&parent) || !in_plan.contains(&parent)
    });
    if wave.is_empty() {
        return None;
    }
    let mut asked = Vec::new();
    let mut rest = Vec::new();
    for row in wave {
        let (parent, name) = place(row);
        // A sync root's top holds the state folder's name even where it went for good (a role
        // folder made again later takes its id).
        let free = (!live.contains(&parent) && !is_sync_top(parent))
            || taken_at(taken, parent).insert(name_key(name));
        if free {
            asked.push(row)
        } else {
            rest.push(row)
        }
    }
    Some(RestoreWave { asked, rest, later })
}

/// The name keys a restore finds taken in live folder `parent`, from the names of what is in it
/// on the drive (`names`); at the top of a sync root (the drive's, a role folder's) also the
/// state folder's, in any case or spelling (`.hbsync`): each computer's own folder there, which
/// no computer syncs, so a row removed under that name comes back under a numbered one. The
/// engine's restore and the simulator's share it.
pub fn taken_in<'a>(parent: NodeId, names: impl IntoIterator<Item = &'a str>) -> HashSet<String> {
    let reserved = is_sync_top(parent).then(|| hbsync_local::state_dir_key().to_owned());
    names.into_iter().map(name_key).chain(reserved).collect()
}

/// Whether `node` is the top of a sync root: the drive's, or a role folder's (§12).
fn is_sync_top(node: NodeId) -> bool {
    node.is_root() || Role::of(node).is_some()
}

/// The name keys taken in `parent`: none yet but the reserved ones ([`taken_in`]) where it holds
/// nothing known.
fn taken_at(taken: &mut HashMap<NodeId, HashSet<String>>, parent: NodeId) -> &mut HashSet<String> {
    taken
        .entry(parent)
        .or_insert_with(|| taken_in(parent, std::iter::empty()))
}

/// The name a restored row that can't take its own (`name`) in `parent` comes back under: the
/// first of its restored names free there, taken now (`None`: none is). The engine's restore and
/// the simulator's share it.
pub fn take_restored_name(
    taken: &mut HashMap<NodeId, HashSet<String>>,
    parent: NodeId,
    name: &str,
) -> Option<String> {
    let names = taken_at(taken, parent);
    let name = restored_names(name).find(|n| !names.contains(&name_key(n)))?;
    names.insert(name_key(&name));
    Some(name)
}

/// `top` and every removed node below it, through the removed folders, whenever each was
/// removed (one deleted from a folder before the folder is in it too): what emptying `top`
/// deletes for good with it, as far as it may (node schema, The purge). Each node once: removed
/// rows may form a cycle. `at` gives a row's node and whether it is a folder, `children` the
/// removed children a folder's row takes in (all of them, or with a filter of its own: those
/// deleted with it, say).
pub fn inside<T, E>(
    top: T,
    at: impl Fn(&T) -> (NodeId, bool),
    mut children: impl FnMut(&T) -> Result<Vec<T>, E>,
) -> Result<Vec<T>, E> {
    let mut seen = HashSet::from([at(&top).0]);
    let mut rows = vec![top];
    let mut next = 0;
    while next < rows.len() {
        let folder = at(&rows[next]).1;
        if folder {
            let below: Vec<T> = children(&rows[next])?
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
pub(super) struct Held {
    pub(super) count: usize,
    pub(super) stays: bool,
}

/// What each of `rows` (all of Recently deleted) holds, in one pass, given the removed folders
/// holding a live node (`live`). With every row chosen, only a live node keeps one ([`select`]).
/// Rows in a cycle (only a remote one leaves one) are each walked down on their own instead, as
/// a lookup of one does.
pub(super) fn held_below(rows: &[Chosen], live: &HashSet<NodeId>) -> HashMap<NodeId, Held> {
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
                |n| Ok::<_, std::convert::Infallible>(below(n).clone()),
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

/// A row of Recently deleted as emptying sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chosen {
    pub node: NodeId,
    pub parent: NodeId,
    pub folder: bool,
}

impl Chosen {
    pub(super) fn of(r: &RemovedRow) -> Chosen {
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
pub(super) fn select(chosen: &[Chosen], holdings: &Holdings) -> Vec<NodeId> {
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

/// What the drive is asked about a pass's folders just before they go for good, `later` the rows of
/// the passes after it, at their versions: whether a file that isn't deleted is in one, live or
/// removed, but those ([`AccountOps::delete_for_good`](super::AccountOps::delete_for_good)). One
/// that is keeps its folder.
pub fn before_folders_go(later: &HashSet<RemoteRef>) -> Holds<'_> {
    Holds::AnyBut(later)
}
