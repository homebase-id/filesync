//! Deleting Recently deleted for good: emptying it (as listed, or items of it), and the daily
//! purge of what it kept long enough, with the tombstones.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use hbsync_remote::{Answers, Holds, Remote, RemoteError};
use hbsync_state::{MirrorRow, ReadTx, RemovedRow, StateError};
use hbsync_types::{Doomed, NodeId, NodeKind, RemoteFileId, RemoteRef, Role, Target};

use crate::error::EngineError;
use crate::gone;
use crate::mirror;
use crate::time::TRACE_MS;

use super::AccountOps;
use super::listing::{
    Listed, RemovedRows, deleted_by_item, deletes_before, group, holdings, refuse_unlisted,
};
use super::rules::{Chosen, before_folders_go, inside, passes};

impl AccountOps<'_> {
    /// The purge: deletes for good the items of Recently deleted kept `keep_ms` by `now_ms` (the
    /// server's clock), each with what was deleted with it, as emptying them would
    /// ([`select`](super::rules::select): removed nodes only, never a live one, and never a folder
    /// holding one), files before their folders; and the tombstones as old as [`TRACE_MS`]. Node
    /// schema, "The purge":
    /// - It never deletes a live node for good. What the delete of an item kept that long took
    ///   along and left live (a delete that stopped half way) it first removes, a folder before
    ///   what is in it ([`Self::remove_taken_along`]): each goes 30 days after its own removal. A
    ///   removed folder still holding a live node it doesn't take (a file written into it since,
    ///   one whose time isn't known, one this device doesn't read) stays in Recently deleted,
    ///   restorable, until someone restores it or the node is removed: possibly indefinitely.
    /// - An item holding a removal whose time isn't known yet waits for a purge that knows it.
    /// - Each node goes without a trace (a hard delete) once its own removal is [`TRACE_MS`] old,
    ///   else leaving a tombstone, which goes hard [`TRACE_MS`] after its own time.
    /// - What is known gone longer than that is forgotten ([`hbsync_state::AccountTx::prune_gone`]).
    ///
    /// Each part runs whatever became of the others; the first error is returned at the end.
    /// Returns how many items went. Leaves the account behind ([`AccountOps::run`]) when the
    /// drive refused a delete for its state (written since, restored, gone already).
    pub fn purge(&mut self, keep_ms: i64, now_ms: i64) -> Result<usize, EngineError> {
        let traced = now_ms.saturating_sub(TRACE_MS);
        let before = now_ms.saturating_sub(keep_ms);
        // Each part on its own: one that fails doesn't keep the others from their work.
        let taken = self.remove_taken_along(before);
        let expired = self.expire(before, Some(traced));
        let tombstones = self.delete_tombstones(traced);
        let pruned = self
            .db()
            .write(|s| s.account().prune_gone(traced, now_ms))
            .map(drop)
            .map_err(EngineError::from);
        taken?;
        let gone = expired?;
        tombstones?;
        pruned?;
        Ok(gone)
    }

    /// Deletes for good the items `nodes` name as `listed` showed them, each with all of Recently
    /// deleted in it then ([`inside`]: what was deleted with it, and what was deleted from it
    /// before), as [`select`](super::rules::select)s it, leaving tombstones: nothing recorded since
    /// the listing (a removal that arrived inside it later, or the item deleted again after a
    /// restore), and never a live node. Refused like [`Self::empty_listed`] for a listing this DB
    /// never gave. Returns how many items went.
    pub fn empty(&mut self, nodes: &[NodeId], listed: &Listed) -> Result<usize, EngineError> {
        let passes = self.db().read(|s| {
            refuse_unlisted(s, listed)?;
            let mut chosen = Vec::new();
            for &node in nodes {
                if let Some(top) = s.recorded_by_node(node)? {
                    // Each row with its `seq`: what the listing saw is up to its counter.
                    let rows = inside(
                        top,
                        |r| (r.removed.row.node, r.removed.row.kind == NodeKind::Folder),
                        |r| s.recorded_children(r.removed.row.node),
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
        self.delete_for_good(&passes, None)
    }

    /// Deletes for good what a listing saw
    /// ([`DeletedSummary::listed`](super::DeletedSummary::listed)): the rows recorded up to its
    /// counter, an open removal too; nothing recorded since (a removal that arrives later, whatever
    /// its server time, or a node deleted again after a restore), and never a live node
    /// ([`select`](super::rules::select)). Leaves tombstones. Refused for a listing of another DB
    /// (the account's, made again) or one this DB never gave. Returns how many items went.
    pub fn empty_listed(&mut self, listed: &Listed) -> Result<usize, EngineError> {
        let passes = self.db().read(|s| {
            refuse_unlisted(s, listed)?;
            let chosen = s.recorded_upto(listed.seq)?;
            going(s, chosen.into_iter().map(|r| r.removed).collect())
        })?;
        self.delete_for_good(&passes, None)
    }

    /// The items of Recently deleted removed before `before_ms` (Unix ms, the server's): those
    /// deleted on their own, as listed.
    fn due(&self, before_ms: i64) -> Result<Vec<RemovedRow>, StateError> {
        self.db().read(|s| deletes_before(s, before_ms))
    }

    /// Deletes for good the items removed before `before` (Unix ms, the server's), each with what
    /// was deleted with it, as [`Self::delete_for_good`] does with `hard_before`: an item goes once
    /// all of it was removed before then ([`passes_of`]).
    pub(super) fn expire(
        &mut self,
        before: i64,
        hard_before: Option<i64>,
    ) -> Result<usize, EngineError> {
        let tops = self.due(before)?;
        let passes = self.db().read(|s| passes_of(s, tops, before))?;
        self.delete_for_good(&passes, hard_before)
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
    /// ([`Self::put_back`]).
    ///
    /// Returns how many it removed (and didn't put back). A refused or unanswered one leaves
    /// the account behind ([`AccountOps::run`]); a request that fails stops it with its error,
    /// after putting back what needs it; another error is returned once the rest is done.
    pub(super) fn remove_taken_along(&mut self, before: i64) -> Result<usize, EngineError> {
        let Stuck { folders, live } = self.db().read(|s| stuck_below(s, before))?;
        let removed: HashMap<NodeId, (NodeKind, i64)> = folders
            .iter()
            .filter_map(|f| Some((f.row.node, (f.row.kind, f.removed_at?))))
            .collect();
        let levels = crate::along::taken_levels(
            live.values().map(|m| (m.node, m.parent, m.kind)),
            |f| removed.get(&f).copied(),
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
            match self.remote.header(f.row.file_id) {
                Ok(h) if h.version == f.row.version && h.removed => {
                    removed.insert(f.row.node);
                }
                Ok(_) | Err(RemoteError::NotFound | RemoteError::Deleted) => self.behind = true,
                // Down for now, or busy and saying when to ask again: the purge stops.
                Err(e)
                    if crate::policy::policy(e.class()).in_run == crate::policy::InRun::Backoff =>
                {
                    return Err(EngineError::Remote(e));
                }
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
            for part in going.chunks(hbsync_remote::BATCH_FILES) {
                let targets: Vec<Target> = part.iter().map(|m| m.target()).collect();
                let (answers, stop) = self
                    .remote
                    .set_removed(&targets, true)
                    .unwrap_or_else(Answers::stopped)
                    .padded(part.len());
                last = Instant::now();
                let mut done = Vec::new();
                for (m, answer) in part.iter().zip(answers) {
                    match answer {
                        Ok(w) => done.push(hbsync_state::Answer::removal(
                            Some(m.version),
                            m.rewritten(w),
                        )),
                        // Written since, or gone; or unanswered, so it may have landed: the feed
                        // tells what it is now.
                        Err(_) if stop.is_some() => self.behind = true,
                        Err(
                            RemoteError::VersionMismatch
                            | RemoteError::NotFound
                            | RemoteError::Deleted,
                        ) => self.behind = true,
                        Err(e) => {
                            failed.get_or_insert(e);
                        }
                    }
                }
                count += done.len();
                removed.extend(done.iter().map(|a| a.row().node));
                made.extend(
                    done.iter()
                        .filter(|a| a.row().kind == NodeKind::Folder)
                        .map(|a| (a.row().clone(), depth)),
                );
                self.behind |= self.db().write(|s| mirror::answered(s.account(), &done))?;
                if stop.is_some() {
                    stopped = stop;
                    break 'levels;
                }
            }
        }
        count -= self.put_back(made, last)?;
        if count > 0 {
            tracing::info!("the purge removed {count} items a delete took along and left");
        }
        match stopped.or(failed) {
            Some(e) => Err(EngineError::Remote(e)),
            None => Ok(count),
        }
    }

    /// Puts back the live folders [`Self::remove_taken_along`] removed (`made`, each as written,
    /// with its depth) that the drive says hold a live node, asked once its queries show what was
    /// written up to `last` (the last removal), and the folders above them among `made`: top-down,
    /// each at the version written, as a restore does. One that doesn't come back (written since)
    /// leaves the account behind ([`AccountOps::run`]). Returns how many came back.
    fn put_back(
        &mut self,
        made: Vec<(MirrorRow, usize)>,
        last: Instant,
    ) -> Result<usize, EngineError> {
        if made.is_empty() {
            return Ok(0);
        }
        std::thread::sleep(self.remote.hold_back().saturating_sub(last.elapsed()));
        let nodes: Vec<NodeId> = made.iter().map(|(m, _)| m.node).collect();
        let holding = self.remote.holding(&nodes, Holds::Live)?;
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
            let (answers, stopped) = self
                .remote
                .set_removed(&targets, false)
                .unwrap_or_else(Answers::stopped)
                .padded(targets.len());
            let rows: Vec<hbsync_state::Answer> = level
                .iter()
                .zip(answers)
                .filter_map(|((m, _), answer)| match answer {
                    Ok(w) => Some(hbsync_state::Answer::live(Some(m.version), m.rewritten(w))),
                    Err(_) => {
                        self.behind = true;
                        None
                    }
                })
                .collect();
            came += rows.len();
            self.behind |= self.db().write(|s| mirror::answered(s.account(), &rows))?;
            if let Some(e) = stopped {
                return Err(EngineError::Remote(e));
            }
        }
        Ok(came)
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
    /// recorded with its time; both in batches, many files a request. Returns how many went. One
    /// written since, or not found, leaves the account behind ([`AccountOps::run`]); one not
    /// found is recorded gone once its header says so too. A request that fails stops it with its
    /// error: what it didn't answer stays, for the next purge.
    fn delete_for_good(
        &mut self,
        passes: &[Vec<RemovedRow>],
        hard_before: Option<i64>,
    ) -> Result<usize, EngineError> {
        // What each folder holds of the rows still to go: what is in a folder goes before it (but
        // round a cycle).
        let mut to_go: HashMap<NodeId, u64> = HashMap::new();
        for r in passes.iter().flatten() {
            *to_go.entry(r.row.parent).or_default() += 1;
        }
        let hard = |r: &RemovedRow| hard_before.is_some_and(|before| r.removed_before(before));
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
                    && self.db().read(|s| s.child_count(r.row.node))? > held
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
                std::thread::sleep(self.remote.hold_back().saturating_sub(began.elapsed()));
                let holding = self.remote.holding(&folders, before_folders_go(&later))?;
                going.retain(|r| !holding.contains(&r.row.node));
            }
            let (hard, soft): (Vec<&RemovedRow>, Vec<&RemovedRow>) =
                going.into_iter().partition(|r| hard(r));
            let refs =
                |rows: &[&RemovedRow]| -> Vec<Doomed> { rows.iter().map(|r| r.doomed()).collect() };
            let (answers, mut failed) = hard_delete(self.remote, &refs(&hard));
            let mut answered: Vec<(&RemovedRow, Answer)> = hard.into_iter().zip(answers).collect();
            if failed.is_none() {
                let (answers, stopped) =
                    answers_to(&refs(&soft), |f| self.remote.delete(f), |at| at);
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
                    Answer::Written => self.behind = true,
                    Answer::Kept => {}
                }
            }
            // Not found: deleted for good meanwhile (another device's purge), if its header says
            // so too; else it stays. Either way the picture lacked it. A request that fails stops
            // the asking: the rest stay, for the next purge.
            if !not_found.is_empty() {
                self.behind = true;
            }
            // What its header says, taken in the write ([`gone::heard`]: the identity's "not
            // there" only the second time, a minute on, sync semantics §8b).
            let mut heard = Vec::new();
            if failed.is_none() {
                (heard, failed) =
                    gone::look_all(self.remote, not_found.iter().map(|r| r.row.file_id));
            }
            // Its id never comes back, nor its row from a page read before.
            let confirmed = self.db().write(|s| {
                let confirmed = gone::heard_all(s.account(), &heard)?;
                gone_now
                    .iter()
                    .try_for_each(|(r, at)| s.account().forget_file(r.row.file_id, *at))?;
                Ok(confirmed)
            })?;
            gone += confirmed + gone_now.len();
            if let Some(e) = failed {
                return Err(EngineError::Remote(e));
            }
        }
        Ok(gone)
    }

    /// Asks again about the files of Recently deleted the identity said once weren't there,
    /// whose second asking is due by `now_ms` (this device's clock): said a minute to an hour
    /// before ([`gone::CONFIRM_AFTER_MS`]). A header read each, taken as the purge takes it
    /// ([`gone::heard`]): not there again, gone for good and forgotten; there, the doubt goes.
    /// Nothing else is asked or written. Returns how many went for good. A request that fails
    /// stops the asking: the rest wait for the next purge.
    pub fn ask_again(&mut self, now_ms: i64) -> Result<usize, EngineError> {
        let due: Vec<RemoteFileId> = self
            .db()
            .read(|s| s.removed_suspects())?
            .into_iter()
            .filter(|(_, first)| gone::confirms(first, now_ms))
            .map(|(f, _)| f)
            .collect();
        if due.is_empty() {
            return Ok(0);
        }
        let (heard, failed) = gone::look_all(self.remote, due);
        let gone = self.db().write(|s| gone::heard_all(s.account(), &heard))?;
        match failed {
            Some(e) => Err(EngineError::Remote(e)),
            None => Ok(gone),
        }
    }

    /// Deletes hard the tombstones the feed brought that were written before `before_ms` (Unix ms,
    /// the server's clock): a delete for good that left one leaves nothing in the feed once every
    /// device has had time to see it. Each at its version: one written since stays, and so does
    /// one a failed request didn't answer.
    fn delete_tombstones(&self, before_ms: i64) -> Result<(), EngineError> {
        let tombstones = self.db().read(|s| s.tombstones_before(before_ms))?;
        let (answers, failed) = hard_delete(self.remote, &tombstones);
        self.db().write(|s| {
            tombstones
                .iter()
                .zip(answers)
                .filter(|(_, answer)| matches!(answer, Answer::Deleted(_) | Answer::Missing))
                .try_for_each(|(t, _)| s.account().delete_tombstone_at(t.at()).map(drop))
        })?;
        match failed {
            Some(e) => Err(EngineError::Remote(e)),
            None => Ok(()),
        }
    }
}

/// What emptying `chosen` (rows of Recently deleted) deletes for good, in the passes
/// [`passes`] gives.
fn going(s: &ReadTx<'_>, chosen: Vec<RemovedRow>) -> Result<Vec<Vec<RemovedRow>>, StateError> {
    let picked: Vec<Chosen> = chosen.iter().map(Chosen::of).collect();
    let mut rows: HashMap<NodeId, RemovedRow> =
        chosen.into_iter().map(|r| (r.row.node, r)).collect();
    Ok(passes(&picked, &holdings(s)?)
        .into_iter()
        .map(|pass| pass.into_iter().filter_map(|n| rows.remove(&n)).collect())
        .collect())
}

/// The passes ([`passes`]) that delete the items `tops` for good, each with what was deleted
/// with it, if every member was removed before `before` (Unix ms, the server's), by its own
/// removal: one removed later in a folder removed long ago (that a live node kept, say) has its
/// own time in Recently deleted, and so does the item. One whose time isn't known yet waits for
/// a purge that knows it.
fn passes_of(
    s: &ReadTx<'_>,
    tops: Vec<RemovedRow>,
    before: i64,
) -> Result<Vec<Vec<RemovedRow>>, StateError> {
    let mut chosen = Vec::new();
    for top in tops {
        let item = group(s, top)?;
        if item.iter().all(|r| r.removed_before(before)) {
            chosen.extend(item);
        }
    }
    going(s, chosen)
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
/// a live node written no later than their removal ([`ReadTx::removed_holding_taken`]: none,
/// mostly, at the cost of one query), and the live nodes below them, directly or through live
/// folders written by then too (one written after takes nothing in it along).
fn stuck_below(s: &ReadTx<'_>, before: i64) -> Result<Stuck, StateError> {
    let mut stuck = Stuck::default();
    let mut rows = RemovedRows::new(s, Vec::new());
    let mut known = HashMap::new();
    for folder in s.removed_holding_taken()? {
        let item = deleted_by_item(&mut rows, folder, &mut known)?;
        let due = rows.get(item)?.is_some_and(|r| r.removed_before(before));
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
fn hard_delete(remote: &dyn Remote, files: &[Doomed]) -> (Vec<Answer>, Option<RemoteError>) {
    answers_to(files, |f| remote.delete_for_good(f), |()| None)
}

/// What the drive answered when `delete` asked about `files` (nothing for none), each deleted
/// with `tombstone` of its answer: an answer for each of the first files, and the error of the
/// request that stopped it there, if one did (the files not answered stay), or else the first
/// answer that isn't one of [`Answer`]'s.
fn answers_to<T>(
    files: &[Doomed],
    delete: impl FnOnce(&[Doomed]) -> Result<Answers<T>, RemoteError>,
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
