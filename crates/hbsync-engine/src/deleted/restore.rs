//! Restoring an item of Recently deleted: it, what was deleted with it, and the folders above
//! it in Recently deleted too, back in the tree under a free name.

use std::collections::{HashMap, HashSet};

use hbsync_platform::name::name_key;
use hbsync_remote::{Answers, NodeMeta, Remote, RemoteError};
use hbsync_state::{Answer, MirrorRow, ReadTx, RemovedRow, StateError};
use hbsync_types::{NodeId, RemoteFileId, Target, Written};

use crate::error::EngineError;
use crate::gone::{self, Said, gone_after};
use crate::mirror::{self, meta_at};
use crate::walk::End;

use super::listing::group;
use super::rules::{RestoreWave, restore_wave, take_restored_name, taken_in};
use super::{AccountOps, lock};

/// What a restore did, beyond how many items came back: for the user (sync semantics §8b).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Restored {
    /// How many items came back.
    pub back: usize,
    /// The names of rows it was to bring back that stayed in Recently deleted, with no error:
    /// no free name in their folder, changed meanwhile, or in a folder that didn't come back.
    pub left: Vec<String>,
    /// The names of rows it found deleted for good meanwhile: they can't come back.
    pub went: Vec<String>,
    /// Of the nodes asked for, those neither in Recently deleted nor on the drive: deleted for
    /// good before it ran (another path found them so: the purge's asking again, say).
    pub missing: Vec<NodeId>,
}

impl AccountOps<'_> {
    /// Restores each of `nodes` with what was deleted with it, and the folders it was in that
    /// are in Recently deleted too (just those, not their other contents), all together: one
    /// read, then each wave's rows (of all of them) in as few requests as the drive takes and
    /// one write. What two of them hold comes back once. Returns how many items came back; 0
    /// for a node not in Recently deleted (any more).
    /// Leaves the account behind ([`AccountOps::run`]) when the drive refused a write for its
    /// state (changed or deleted for good meanwhile), or a write may have landed unanswered.
    pub fn restore(&mut self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        self.restore_told(nodes).map(|r| r.back)
    }

    /// [`AccountOps::restore`], with what stayed in Recently deleted and what went for good
    /// ([`Restored`]). A first "not there" fails it ([`EngineError::NotThereOnce`]): asked
    /// again a minute on, it is gone or there. Costs, when something didn't come back, a read
    /// of its row each.
    pub fn restore_told(&mut self, nodes: &[NodeId]) -> Result<Restored, EngineError> {
        let (plan, missing) = lock(self.account).read(|s| {
            let mut missing = Vec::new();
            for &node in nodes {
                if s.removed_by_node(node)?.is_none() && s.get_mirror(node)?.is_none() {
                    missing.push(node);
                }
            }
            Ok((plan_restore(s, nodes)?, missing))
        })?;
        let Some(RestorePlan { rows, live, taken }) = plan else {
            return Ok(Restored {
                missing,
                ..Restored::default()
            });
        };
        let mut r = Restoring {
            remote: self.remote,
            in_plan: rows.iter().map(|r| r.row.node).collect(),
            live,
            taken,
            behind: &mut self.behind,
            back: Vec::new(),
            heard: Vec::new(),
            changed: HashSet::new(),
            failed: None,
        };
        let mut restored = 0;
        let mut went: HashSet<NodeId> = HashSet::new();
        let mut not_there = 0;
        let mut left: Vec<&MirrorRow> = rows.iter().map(|r| &r.row).collect();
        // In waves, each after the folders it comes back into ([`restore_wave`]). One that was to
        // come back into a folder that didn't stays in Recently deleted.
        while r.failed.is_none() {
            let Some(RestoreWave { asked, rest, later }) = restore_wave(
                left,
                |row| (row.parent, &row.name),
                &r.live,
                &r.in_plan,
                &mut r.taken,
            ) else {
                break;
            };
            left = later;
            // The rows under their own names first, only their removals undone, as many a request
            // as the drive takes; the rest once those answered (a name one didn't take is free
            // again), a rewrite each.
            for part in asked.chunks(hbsync_remote::BATCH_FILES) {
                if r.failed.is_some() {
                    break;
                }
                let targets: Vec<Target> = part.iter().map(|row| row.target()).collect();
                let (answers, stopped) = self
                    .remote
                    .set_removed(&targets, false)
                    .unwrap_or_else(Answers::stopped)
                    .padded(targets.len());
                // One the call left unanswered fails as it did.
                for (row, answer) in part.iter().zip(answers) {
                    let meta = meta_at(row, row.parent, &row.name, false);
                    r.settle(row, &meta, answer);
                }
                if let Some(e) = stopped {
                    *r.behind = true; // it may have landed
                    r.failed.get_or_insert(e);
                    break;
                }
            }
            for row in rest {
                if r.failed.is_some() {
                    break;
                }
                let Some(name) = take_restored_name(&mut r.taken, row.parent, &row.name) else {
                    continue; // no free name: it stays in Recently deleted
                };
                let meta = meta_at(row, row.parent, &name, false);
                let answer = self.remote.update(&row.target(), &meta, None);
                r.settle(row, &meta, answer);
            }
            // What came back, and what was deleted for good meanwhile (the identity's "not there"
            // only the second time: sync semantics §8b), in one write. The mirror's write marks
            // each dirty in the folders it comes back to.
            let (back, heard) = (std::mem::take(&mut r.back), std::mem::take(&mut r.heard));
            let (gone, superseded) = lock(self.account).write(|s| {
                let gone = heard
                    .iter()
                    .map(|&(f, _, said)| gone::heard(s.account(), f, said))
                    .collect::<Result<Vec<bool>, _>>()?;
                let superseded = mirror::answered(s.account(), &back)?;
                Ok((gone, superseded))
            })?;
            *r.behind |= superseded;
            restored += back.len();
            // Gone: what was to come back into it comes back as into any folder gone for good.
            // Said not there once only: it stays in Recently deleted, and the restore fails, to
            // try again (a minute on, it is gone).
            let mut doubted: HashSet<NodeId> = HashSet::new();
            for ((_, node, said), gone) in heard.into_iter().zip(gone) {
                if gone {
                    r.in_plan.remove(&node);
                    went.insert(node);
                } else if said == Said::NotThere {
                    doubted.insert(node);
                    not_there += 1;
                }
            }
            // What was to come back into a folder said not there once is asked about too (a
            // header read each): asked again a minute on, all of it is gone at once (or there),
            // not a level of folders a minute.
            doubt_below(self.remote, self.account, &left, doubted);
            // Stopped, to try again a minute on.
            if not_there > 0 {
                break;
            }
        }
        if let Some(e) = r.failed {
            return Err(EngineError::Remote(e));
        }
        let names = |nodes: &dyn Fn(NodeId) -> bool| -> Vec<String> {
            rows.iter()
                .filter(|row| nodes(row.row.node))
                .map(|row| row.row.name.clone())
                .collect()
        };
        // What it was to bring back and didn't, still in Recently deleted: a read each, only
        // where something didn't come back. One the drive had changed (another device's
        // restore, likely) isn't named: the feed tells what it is.
        let maybe: Vec<NodeId> = rows
            .iter()
            .map(|row| row.row.node)
            .filter(|n| !r.live.contains(n) && !went.contains(n) && !r.changed.contains(n))
            .collect();
        let stayed: HashSet<NodeId> = match maybe.is_empty() {
            true => HashSet::new(),
            false => lock(self.account).read(|s| {
                let mut stayed = HashSet::new();
                for node in maybe {
                    if s.removed_by_node(node)?.is_some() {
                        stayed.insert(node);
                    }
                }
                Ok(stayed)
            })?,
        };
        let went = names(&|n| went.contains(&n));
        if not_there > 0 {
            // To restore again: every row of it still in Recently deleted (a folder that came
            // back no longer stands for what in it didn't).
            let rest = rows
                .iter()
                .filter(|row| stayed.contains(&row.row.node))
                .map(|row| (row.row.node, row.row.name.clone()))
                .collect();
            return Err(EngineError::NotThereOnce {
                files: not_there,
                rest,
                went,
            });
        }
        Ok(Restored {
            back: restored,
            left: names(&|n| stayed.contains(&n)),
            went,
            missing,
        })
    }
}

/// Asks the drive about the rows of `left` (in order, each after its folder) in the folders
/// `doubted` (said not there once) or below them, and takes what it says ([`gone::heard`]): not
/// there, a first "not there" each. A request that fails stops the asking, and a write that
/// fails is only logged: the restore fails anyway, and its error is the one told. Costs a
/// header read per row, once (else a folder level a minute).
fn doubt_below(
    remote: &dyn Remote,
    account: &std::sync::Mutex<hbsync_state::StateDb>,
    left: &[&MirrorRow],
    mut doubted: HashSet<NodeId>,
) {
    let below: Vec<RemoteFileId> = left
        .iter()
        .filter(|row| doubted.contains(&row.parent) && doubted.insert(row.node))
        .map(|row| row.file_id)
        .collect();
    if below.is_empty() {
        return;
    }
    let (heard, _) = gone::look_all(remote, below);
    // Only more suspicions: the restore's own error is the one to tell.
    if let Err(e) = lock(account).write(|s| gone::heard_all(s.account(), &heard)) {
        tracing::warn!("can't record what isn't there below a folder that isn't: {e}");
    }
}

/// A restore under way: what is back or going, and the names taken.
struct Restoring<'a> {
    remote: &'a dyn Remote,
    in_plan: HashSet<NodeId>,
    live: HashSet<NodeId>,
    taken: HashMap<NodeId, HashSet<String>>,
    behind: &'a mut bool,
    /// Written back, to record.
    back: Vec<Answer>,
    /// What the drive said of files not found, to take ([`gone::heard`]).
    heard: Vec<(hbsync_types::RemoteFileId, NodeId, Said)>,
    /// Rows the drive had changed since (refused for their version).
    changed: HashSet<NodeId>,
    /// What stops the restore.
    failed: Option<RemoteError>,
}

impl Restoring<'_> {
    /// Takes the drive's answer to `row`'s restore as `meta`.
    fn settle(&mut self, row: &MirrorRow, meta: &NodeMeta, answer: Result<Written, RemoteError>) {
        let e = match answer {
            Ok(w) => {
                let hash = row.file.map(|f| f.hash);
                self.live.insert(row.node);
                self.back.push(Answer::live(
                    Some(row.version),
                    mirror::written(meta, hash, w),
                ));
                return;
            }
            Err(e) => e,
        };
        // The name it didn't take is free again.
        if let Some(names) = self.taken.get_mut(&meta.parent) {
            names.remove(&name_key(&meta.content.name));
        }
        *self.behind = true;
        match e {
            // Deleted for good meanwhile (its answer or its header says so), maybe without a
            // trace in the feed: gone here too, and what was to come back into it comes back as
            // into any folder gone for good.
            RemoteError::NotFound | RemoteError::Deleted => {
                // Taken with the wave's write: a "not there" is gone the second time only.
                match gone_after(self.remote, &e, row.file_id) {
                    Ok(said) => self.heard.push((row.file_id, row.node, said)),
                    Err(e) => {
                        self.failed.get_or_insert(e);
                    }
                }
            }
            // Changed meanwhile: the feed tells what it is now.
            RemoteError::VersionMismatch => {
                self.changed.insert(row.node);
            }
            e => {
                self.failed.get_or_insert(e);
            }
        }
    }
}

/// What a restore of some nodes does, worked out in one read.
#[derive(Debug)]
pub struct RestorePlan<R = RemovedRow> {
    /// In order, for each node: the folders above it that are in Recently deleted (top-down),
    /// then it and what was deleted with it (each after its folder); each row once.
    pub rows: Vec<R>,
    /// Folders in the tree that the rows may go into (the top included).
    pub live: HashSet<NodeId>,
    /// The name keys taken in each of those folders.
    pub taken: HashMap<NodeId, HashSet<String>>,
}

/// What [`plan_restore_from`] reads: the engine's store, or the simulator's drive.
pub trait RestoreSource {
    /// A row of Recently deleted.
    type Row;
    type Error;
    /// `node`'s row in Recently deleted, if it is there.
    fn removed(&mut self, node: NodeId) -> Result<Option<Self::Row>, Self::Error>;
    /// `top` and what was deleted with it, each after its folder (the listing's `group`).
    fn group(&mut self, top: Self::Row) -> Result<Vec<Self::Row>, Self::Error>;
    /// Whether `node` is live on the drive.
    fn is_live(&mut self, node: NodeId) -> Result<bool, Self::Error>;
    /// The names of what is live in `folder`.
    fn names_in(&mut self, folder: NodeId) -> Result<Vec<String>, Self::Error>;
    /// A row's node and its folder.
    fn place(row: &Self::Row) -> (NodeId, NodeId);
}

/// The plan of a restore of `nodes`, from `source` (the one rule: the engine's, which the
/// simulator calls too).
pub fn plan_restore_from<S: RestoreSource>(
    source: &mut S,
    nodes: &[NodeId],
) -> Result<Option<RestorePlan<S::Row>>, S::Error> {
    // Each row once, whichever of `nodes` takes it in; `grouped`: those whose group (what was
    // deleted with them) is planned already.
    let (mut seen, mut grouped) = (HashSet::new(), HashSet::new());
    let mut rows = Vec::new();
    for &node in nodes {
        if grouped.contains(&node) {
            continue; // in what another of them took along
        }
        let Some(top) = source.removed(node)? else {
            continue;
        };
        // The folders above, as far as they are in Recently deleted (then in the tree, or
        // deleted for good: the item comes back as an orphan, for Recovered files), up to one
        // planned already. A chain past any depth a path may have (only a malicious drive makes
        // one) stays where it is: above its top is still a removed folder, not one gone for
        // good.
        let up = crate::walk::up::<_, S::Error>(S::place(&top).1, |at| {
            if seen.contains(&at) {
                return Ok(None);
            }
            Ok(source.removed(at)?.map(|f| {
                let parent = S::place(&f).1;
                (f, parent)
            }))
        })?;
        if matches!(up.end, End::Deep) {
            continue;
        }
        let mut chain = up.items;
        chain.reverse();
        // Then the node and what was deleted with it, each after its folder. What is live
        // still comes back with its folder by itself.
        let group = source.group(top)?;
        grouped.extend(group.iter().map(|r| S::place(r).0));
        for r in chain.into_iter().chain(group) {
            let node = S::place(&r).0;
            if seen.insert(node) && !source.is_live(node)? {
                rows.push(r);
            }
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    // The folders in the tree the rows may go into, and the folders coming back: their names,
    // each folder read once. (One that is neither went for good: what was in it takes no name
    // there.)
    let mut live = HashSet::from([NodeId::ROOT]);
    let mut taken: HashMap<NodeId, HashSet<String>> = HashMap::new();
    let mut looked = HashSet::new();
    let parents: Vec<NodeId> = rows.iter().map(|r| S::place(r).1).collect();
    for parent in parents.into_iter().chain([NodeId::ROOT]) {
        if !looked.insert(parent) {
            continue;
        }
        if !parent.is_root() && source.is_live(parent)? {
            live.insert(parent);
        } else if !parent.is_root() && !seen.contains(&parent) {
            continue;
        }
        let names = source.names_in(parent)?;
        taken.insert(parent, taken_in(parent, names.iter().map(String::as_str)));
    }
    Ok(Some(RestorePlan { rows, live, taken }))
}

/// [`plan_restore_from`] the store.
pub(super) fn plan_restore(
    s: &ReadTx<'_>,
    nodes: &[NodeId],
) -> Result<Option<RestorePlan>, StateError> {
    plan_restore_from(&mut Store(s), nodes)
}

/// The store as a [`RestoreSource`].
struct Store<'s, 'c>(&'s ReadTx<'c>);

impl RestoreSource for Store<'_, '_> {
    type Row = RemovedRow;
    type Error = StateError;
    fn removed(&mut self, node: NodeId) -> Result<Option<RemovedRow>, StateError> {
        self.0.removed_by_node(node)
    }
    fn group(&mut self, top: RemovedRow) -> Result<Vec<RemovedRow>, StateError> {
        group(self.0, top)
    }
    fn is_live(&mut self, node: NodeId) -> Result<bool, StateError> {
        Ok(self.0.get_mirror(node)?.is_some())
    }
    fn names_in(&mut self, folder: NodeId) -> Result<Vec<String>, StateError> {
        Ok(self
            .0
            .mirror_children(folder)?
            .into_iter()
            .map(|m| m.name)
            .collect())
    }
    fn place(row: &RemovedRow) -> (NodeId, NodeId) {
        (row.row.node, row.row.parent)
    }
}
