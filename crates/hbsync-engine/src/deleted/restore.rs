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

impl AccountOps<'_> {
    /// Restores each of `nodes` with what was deleted with it, and the folders it was in that
    /// are in Recently deleted too (just those, not their other contents), all together: one
    /// read, then each wave's rows (of all of them) in as few requests as the drive takes and
    /// one write. What two of them hold comes back once. Returns how many items came back; 0
    /// for a node not in Recently deleted (any more).
    /// Leaves the account behind ([`AccountOps::run`]) when the drive refused a write for its
    /// state (changed or deleted for good meanwhile), or a write may have landed unanswered.
    pub fn restore(&mut self, nodes: &[NodeId]) -> Result<usize, EngineError> {
        let Some(RestorePlan { rows, live, taken }) =
            lock(self.account).read(|s| plan_restore(s, nodes))?
        else {
            return Ok(0);
        };
        let mut r = Restoring {
            remote: self.remote,
            in_plan: rows.iter().map(|r| r.row.node).collect(),
            live,
            taken,
            behind: &mut self.behind,
            back: Vec::new(),
            heard: Vec::new(),
            failed: None,
        };
        let mut restored = 0;
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
            for ((f, node, said), gone) in heard.into_iter().zip(gone) {
                if gone {
                    r.in_plan.remove(&node);
                } else if said == Said::NotThere {
                    doubted.insert(node);
                    r.failed.get_or_insert(RemoteError::Transient(format!(
                        "{f:?}: the identity says it isn't there; asked again later"
                    )));
                }
            }
            // What was to come back into a folder said not there once is asked about too (a
            // header read each): asked again a minute on, all of it is gone at once (or there),
            // not a level of folders a minute.
            doubt_below(self.remote, self.account, &left, doubted);
        }
        match r.failed {
            Some(e) => Err(EngineError::Remote(e)),
            None => Ok(restored),
        }
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
            RemoteError::VersionMismatch => {}
            e => {
                self.failed.get_or_insert(e);
            }
        }
    }
}

/// What a restore of a node does, worked out in one read.
pub(super) struct RestorePlan {
    /// In order: the folders above that are in Recently deleted (top-down), then the node and
    /// what was deleted with it (each after its folder).
    pub(super) rows: Vec<RemovedRow>,
    /// Folders in the tree that the rows may go into (the top included).
    live: HashSet<NodeId>,
    /// The name keys taken in each of those folders.
    taken: HashMap<NodeId, HashSet<String>>,
}

pub(super) fn plan_restore(
    s: &ReadTx<'_>,
    nodes: &[NodeId],
) -> Result<Option<RestorePlan>, StateError> {
    // Each node and what was deleted with it once, whichever of `nodes` takes it in.
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    let mut any = false;
    for &node in nodes {
        if seen.contains(&node) {
            continue; // in what another of them takes along
        }
        let Some(top) = s.removed_by_node(node)? else {
            continue;
        };
        // The folders above, as far as they are in Recently deleted (then in the tree, or
        // deleted for good: the item comes back as an orphan, for Recovered files). A chain
        // past any depth a path may have (only a malicious drive makes one) stays where it is:
        // above its top is still a removed folder, not one gone for good.
        let up = crate::walk::up::<_, StateError>(top.row.parent, |at| {
            if seen.contains(&at) {
                return Ok(None); // planned already, with what is above it
            }
            Ok(s.removed_by_node(at)?.map(|f| {
                let parent = f.row.parent;
                (f, parent)
            }))
        })?;
        if matches!(up.end, End::Deep) {
            continue;
        }
        any = true;
        let mut chain = up.items;
        chain.reverse();
        // Then the node and what was deleted with it, each after its folder. What is live
        // still comes back with its folder by itself.
        for r in chain.into_iter().chain(group(s, top)?) {
            if seen.insert(r.row.node) && s.get_mirror(r.row.node)?.is_none() {
                rows.push(r);
            }
        }
    }
    if !any {
        return Ok(None);
    }
    // The folders in the tree the rows may go into, and the folders coming back: their names,
    // each folder read once. (One that is neither went for good: what was in it takes no name
    // there.)
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
        let children = s.mirror_children(parent)?;
        let names = taken_in(parent, children.iter().map(|m| m.name.as_str()));
        taken.insert(parent, names);
    }
    Ok(Some(RestorePlan { rows, live, taken }))
}
