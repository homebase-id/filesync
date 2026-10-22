//! The guard's held deletes of one folder's engine (sync semantics §7), as it keeps them
//! between syncs: what the user confirmed, what is held as every synced file (kept in the
//! store as [`keys::WIPED`]), and the held deletes out of the dirty set. Their rules are
//! [`crate::held`]'s, which the simulator shares.

use std::collections::HashSet;

use hbsync_state::{Mark, ReadTx, StateDb, StateError, keys};
use hbsync_types::{NodeId, NodeKind};

use crate::drive::Drive;
use crate::error::EngineError;
use crate::held::{self, Sides};
use crate::reconcile::{DeleteGuard, Guarding, Op, Plan};

/// The held deletes of one folder's engine.
#[derive(Debug)]
pub(crate) struct HeldDeletes {
    /// Nodes whose held deletes the user confirmed, until they are done (or undone).
    confirmed: HashSet<NodeId>,
    /// Nodes whose removal elsewhere the guard held as every synced file, till answered
    /// ([`keys::WIPED`]).
    wiped: HashSet<NodeId>,
    /// Those out of the dirty set.
    out: HeldOut,
}

/// The held deletes out of the dirty set ([`crate::held`]), as this engine took them out (the
/// store keeps which nodes; a new engine plans them again).
#[derive(Debug, Default)]
struct HeldOut {
    /// The deletes, shown with every sync's report.
    ops: Vec<Op>,
    /// Their nodes out of the dirty set.
    nodes: HashSet<NodeId>,
    /// The files they take ([`Guarding::held`]).
    files: (usize, usize),
    /// Their part of the guard's totals ([`ReadTx::held_file_deletes`]), once read: what
    /// changes them marks a node of theirs dirty, and the plan that reaches it plans them all
    /// again.
    pending: Option<(u64, u64)>,
    /// A plan's view reached one of `nodes` ([`held::touches`]).
    reached: bool,
    /// To plan again at the next sync: an answer came, or this engine is new.
    plan_again: bool,
}

impl HeldOut {
    fn add(&mut self, ops: impl IntoIterator<Item = Op>, (here, there): (usize, usize)) {
        self.ops.extend(ops);
        self.files = (self.files.0 + here, self.files.1 + there);
    }
}

/// The held deletes of a sweep's pages that may leave the dirty set at its end
/// ([`held::out_of_dirty`]): the deletes, each with the files it takes, and the marks of the
/// nodes they cover as each page read them. A node planned again (a page of its own after its
/// sweep page) counts as its last plan says.
#[derive(Debug, Default)]
pub(crate) struct HoldOut {
    ops: Vec<(Op, (usize, usize))>,
    marks: Vec<Mark>,
}

impl HoldOut {
    /// What of `plan`, made for the page `marks` (of the nodes `page`), may leave the dirty
    /// set, replacing what earlier pages held of its nodes.
    pub(crate) fn add(&mut self, plan: &Plan, page: &[NodeId], marks: &[Mark]) {
        let (out, nodes) = held::out_of_dirty(plan, page);
        let planned: HashSet<NodeId> = page.iter().copied().collect();
        self.ops.retain(|(op, _)| !planned.contains(&op.node()));
        self.marks.retain(|m| !planned.contains(&m.node));
        self.ops.extend(
            out.into_iter()
                .map(|op| (op.clone(), held::files(plan, [op]))),
        );
        self.marks
            .extend(marks.iter().filter(|m| nodes.contains(&m.node)));
    }

    /// The files its deletes take.
    fn files(&self) -> (usize, usize) {
        self.ops
            .iter()
            .fold((0, 0), |(here, there), (_, (h, t))| (here + h, there + t))
    }

    /// The nodes it would take out.
    pub(crate) fn nodes(&self) -> HashSet<NodeId> {
        self.marks.iter().map(|m| m.node).collect()
    }
}

impl HeldDeletes {
    /// A new engine's: what an earlier run held out is planned again at its first sync.
    pub(crate) fn new() -> Self {
        HeldDeletes {
            confirmed: HashSet::new(),
            wiped: HashSet::new(),
            out: HeldOut {
                plan_again: true,
                ..HeldOut::default()
            },
        }
    }

    /// The user's Delete of `nodes` ([`crate::Engine::confirm_deletes`]).
    pub(crate) fn confirm(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.out.plan_again = true;
        self.confirmed.extend(nodes);
    }

    /// At the start of a sync: what an earlier run held out, or what the user answered since,
    /// is planned again.
    pub(crate) fn start(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        if std::mem::take(&mut self.out.plan_again) {
            self.plan_again(db)?;
        }
        Ok(())
    }

    /// Starts a sweep: what the guard judges its plans with, the [`Counts`] it was read from,
    /// and this computer's id for the account (every open gives the DB one, so none is a
    /// fault, never a reason to plan without; checked before anything is written): the
    /// confirmations still pending, the totals of what is dirty and held out, the synced files,
    /// and the holds of removals elsewhere, kept while a removal is still to be followed in
    /// them. Plans this device's own removals held out again once the guard would let them
    /// through (more files synced since, say), as it would if they had stayed dirty, and all of
    /// them with the guard off. No plan has reached what is held out yet.
    pub(crate) fn begin_sweep(
        &mut self,
        db: &mut StateDb,
        settings: DeleteGuard,
    ) -> Result<(Guarding, Counts, String), StateError> {
        self.out.reached = false;
        // The guard must judge the whole delete, even when it is planned in several batches;
        // confirmed deletes don't count. Confirmations of nodes with nothing left to do are
        // dropped (those whose plan no longer deletes them, in `after_plan`).
        let any_out = !self.out.ops.is_empty();
        let (mut counts, saved, computer) = db.read(|s| {
            let computer = s.db_id()?;
            if computer.is_empty() {
                return Err(StateError::Corrupt("it has no id (db.id)".into()));
            }
            // Held as every synced file: until answered, each while its removal is pending.
            let saved: HashSet<NodeId> = read_wiped(s.get_kv(keys::WIPED)?.as_deref())
                .into_iter()
                .collect();
            Ok((
                guard_counts(s, &self.confirmed, self.out.pending, None, any_out)?,
                saved,
                computer,
            ))
        })?;
        self.confirmed = std::mem::take(&mut counts.confirmed);
        // The held deletes' part, read once while they are out.
        self.out.pending = Some(counts.held);
        let (pending, synced) = (
            (counts.pending.0 as usize, counts.pending.1 as usize),
            counts.synced.unwrap_or(0) as usize,
        );
        // Before the hold of removals elsewhere is looked at: that of one out of the dirty set
        // is kept as it was.
        let own = self
            .out
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { .. }));
        if any_out && settings.allow
            || own
                && !settings.trips(pending.0, synced)
                && !settings.trips(self.out.files.0, synced)
        {
            self.plan_again(db)?;
        }
        // A row lookup or two each; a subtree walk only for one back on the drive. One held out
        // of the dirty set is as it was then: anything that changed it marked it dirty, and the
        // plan that reaches it plans all of them again, from here.
        let mut wiped = HashSet::new();
        db.read(|s| {
            if saved.is_empty() {
                return Ok(());
            }
            for &node in &saved {
                let keep = self.out.nodes.contains(&node)
                    || held::keep_held(node, self.confirmed.contains(&node), &StoreSides(s))?;
                if keep {
                    wiped.insert(node);
                }
            }
            Ok(())
        })?;
        if wiped != saved {
            save_wiped(db, &wiped)?;
        }
        self.wiped = wiped.clone();
        let guarding = Guarding {
            settings,
            synced,
            confirmed: std::sync::Arc::new(self.confirmed.clone()),
            wiped: std::sync::Arc::new(wiped),
            pending,
            held: self.out.files,
        };
        Ok((guarding, counts, computer))
    }

    /// In the read of a page with marks (`s`), the counts again if `counts` are stale: another
    /// connection committed since they were read (a pull, an announcement: what it marked
    /// counts too), or nothing dirty had them skip the synced files. Never less pending than
    /// the sweep judged its earlier pages by: what this sync did meanwhile (a base gone with a
    /// file gone on both sides) or a restore elsewhere doesn't let through the rest of what
    /// those pages held.
    pub(crate) fn recount(
        &self,
        s: &ReadTx<'_>,
        counts: &Counts,
    ) -> Result<Option<Counts>, StateError> {
        if counts.synced.is_some() && s.data_version()? == counts.version {
            return Ok(None);
        }
        let mut fresh = guard_counts(s, &self.confirmed, None, counts.synced, true)?;
        fresh.pending = (
            fresh.pending.0.max(counts.pending.0),
            fresh.pending.1.max(counts.pending.1),
        );
        Ok(Some(fresh))
    }

    /// `guarding` with `fresh` counts ([`HeldDeletes::recount`]), and what is held as every
    /// synced file so far this sync.
    pub(crate) fn recounted(&self, guarding: &mut Guarding, fresh: &Counts) {
        guarding.synced = fresh.synced.unwrap_or(0) as usize;
        guarding.pending = (fresh.pending.0 as usize, fresh.pending.1 as usize);
        guarding.wiped = std::sync::Arc::new(self.wiped.clone());
    }

    /// After a page's `plan` (for the dirty nodes `dirty`): what it holds of removals elsewhere
    /// stays held till answered (only the every-file rule holds those), kept before anything
    /// of the page runs or goes clean. A confirmation lasts while its node's plan deletes it,
    /// or its delete waits for a file in it not hashed yet (§6.3): one whose delete was undone
    /// (the file back) is forgotten, so a later delete is judged anew.
    pub(crate) fn after_plan(
        &mut self,
        db: &mut StateDb,
        plan: &Plan,
        dirty: &[NodeId],
    ) -> Result<(), StateError> {
        let before = self.wiped.len();
        self.wiped
            .extend(plan.held.iter().filter_map(|op| match op {
                Op::LocalTrash { node, .. } => Some(*node),
                _ => None,
            }));
        if self.wiped.len() != before {
            save_wiped(db, &self.wiped)?;
        }
        let deleting: HashSet<NodeId> = plan
            .ops
            .iter()
            .filter_map(Op::deletes)
            .chain(plan.delete_waits.iter().copied())
            .collect();
        for node in dirty {
            if !deleting.contains(node) {
                self.confirmed.remove(node);
            }
        }
        Ok(())
    }

    /// The nodes out of the dirty set.
    pub(crate) fn out(&self) -> &HashSet<NodeId> {
        &self.out.nodes
    }

    /// A plan's view reached `reached` of them ([`held::touches`]).
    pub(crate) fn reached(&mut self, reached: bool) {
        self.out.reached |= reached;
    }

    /// Whether a plan's view reached any of them since the last asked.
    pub(crate) fn take_reached(&mut self) -> bool {
        std::mem::take(&mut self.out.reached)
    }

    /// Takes the held deletes of `page` out of the dirty set, if it still has `dirty` nodes
    /// (checked in the same write: a mark that came meanwhile leaves them dirty).
    pub(crate) fn take_out(
        &mut self,
        db: &mut StateDb,
        page: HoldOut,
        dirty: u64,
    ) -> Result<(), StateError> {
        let taken = db.write(|s| {
            if s.dirty_count()? != dirty {
                return Ok(false);
            }
            page.marks
                .iter()
                .try_for_each(|m| s.folder().hold_delete(*m))?;
            Ok(true)
        })?;
        if taken {
            self.out.nodes.extend(page.marks.iter().map(|m| m.node));
            let files = page.files();
            self.out.add(page.ops.into_iter().map(|(op, _)| op), files);
            self.out.pending = None;
        }
        Ok(())
    }

    /// `held`, the deletes a sweep held, with those held out of the dirty set: what the user is
    /// shown.
    pub(crate) fn shown(&self, mut held: Vec<Op>) -> Vec<Op> {
        if self.out.ops.is_empty() {
            return held;
        }
        let mut nodes: HashSet<NodeId> = held.iter().map(Op::node).collect();
        held.extend(
            self.out
                .ops
                .iter()
                .filter(|op| nodes.insert(op.node()))
                .cloned(),
        );
        held
    }

    /// Puts the held deletes out of the dirty set back into it: planned again, all of them.
    pub(crate) fn plan_again(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        // Should the write fail: at the next sync.
        self.out.plan_again = true;
        db.write(|s| s.folder().unhold_deletes().map(drop))?;
        self.out = HeldOut::default();
        Ok(())
    }

    /// "Put them back" of `nodes` ([`held::put_back_plan`]), after a read of the feed: the
    /// bases to forget go, and the items to restore are restored together. Returns how many it
    /// put back. The held deletes are planned again at the next sync, whatever it did.
    pub(crate) fn put_back(
        &mut self,
        db: &mut StateDb,
        drive: &Drive,
        nodes: &[NodeId],
    ) -> Result<usize, EngineError> {
        self.out.plan_again = true;
        drive.pull(None)?;
        let forget = |db: &mut StateDb, nodes: &[NodeId]| {
            db.write(|s| {
                for &node in nodes {
                    s.folder().delete_base(node)?;
                    s.folder().mark_dirty(node)?;
                }
                Ok(())
            })
        };
        let plan = db.read(|s| held::put_back_plan(nodes, &StoreSides(s)))?;
        forget(db, &plan.forget)?;
        if plan.items.is_empty() {
            return Ok(plan.forget.len());
        }
        let restored = drive.restore_read(&plan.items);
        // As the restore left the drive (also when it failed: its waves' writes stand): what it
        // found deleted for good goes up again, and a folder kept, gone for good, once nothing
        // below it waits in Recently deleted any more. Its own error is the one told.
        let after = db
            .read(|s| held::after_restore(&plan, &StoreSides(s)))
            .and_then(|after| forget(db, &after).map(|()| after));
        let after = match (restored, after) {
            (Err(e), after) => {
                if let Err(a) = after {
                    tracing::warn!("can't look again at what a failed put back left: {a}");
                }
                return Err(e);
            }
            (Ok(_), after) => after?,
        };
        let kept: HashSet<NodeId> = plan.kept.iter().copied().collect();
        let kept_gone = after.iter().filter(|n| kept.contains(n)).count();
        Ok(plan.forget.len() + plan.restored.len() + kept_gone)
    }
}

/// What the mass-delete guard weighs a page's deletes against, as one read had it.
#[derive(Debug)]
pub(crate) struct Counts {
    /// Files pending deletion across the dirty set and the held deletes out of it, but the
    /// confirmed ones: (gone locally, gone remotely).
    pending: (u64, u64),
    /// The synced files; `None` when not counted (nothing was dirty).
    synced: Option<u64>,
    /// The confirmations whose deletes are still pending.
    confirmed: HashSet<NodeId>,
    /// The part of `pending` that the held deletes out of the dirty set take.
    held: (u64, u64),
    /// The connection's [`ReadTx::data_version`] before the read.
    version: u64,
}

/// The guard's [`Counts`] in `s`: `held`, the files of the held deletes out of the dirty set
/// (read here when `None`); `synced`, the synced files as counted already for the sweep (kept:
/// it judges all its pages by one), else counted here if `count` or anything is dirty (with
/// nothing dirty there is nothing to weigh, and a sync with nothing new reads no table).
fn guard_counts(
    s: &ReadTx<'_>,
    confirmed: &HashSet<NodeId>,
    held: Option<(u64, u64)>,
    synced: Option<u64>,
    count: bool,
) -> Result<Counts, StateError> {
    let version = s.data_version()?;
    let (here, there) = s.pending_file_deletes()?;
    let held = match held {
        Some(held) => held,
        None => s.held_file_deletes()?,
    };
    let (here, there) = (here + held.0, there + held.1);
    let mut still = HashSet::new();
    // Each file once, however many confirmed trashes it is under.
    let (mut confirmed_here, mut confirmed_there) = (0, HashSet::new());
    for &node in confirmed {
        if let Some((gone_here, gone_there)) = s.confirmed_file_deletes(node)? {
            confirmed_here += u64::from(gone_here);
            confirmed_there.extend(gone_there);
            still.insert(node);
        }
    }
    let synced = match synced {
        Some(synced) => Some(synced),
        None if count || s.dirty_count()? > 0 => Some(s.base_file_count()?),
        None => None,
    };
    Ok(Counts {
        pending: (
            here.saturating_sub(confirmed_here),
            there.saturating_sub(confirmed_there.len() as u64),
        ),
        synced,
        confirmed: still,
        held,
        version,
    })
}

/// What the rules of held deletes read ([`held::Sides`]), from the store.
struct StoreSides<'a, 's>(&'a ReadTx<'s>);

impl Sides for StoreSides<'_, '_> {
    type Error = StateError;

    fn synced(&self, node: NodeId) -> Result<Option<NodeId>, StateError> {
        Ok(self.0.get_base(node)?.map(|b| b.state.parent))
    }

    fn synced_children(&self, folder: NodeId) -> Result<Vec<NodeId>, StateError> {
        Ok(self
            .0
            .base_children(folder)?
            .into_iter()
            .map(|b| b.node)
            .collect())
    }

    fn here(&self, node: NodeId) -> Result<Option<NodeKind>, StateError> {
        Ok(self.0.local_by_node(node)?.map(|r| r.kind))
    }

    fn here_children(&self, node: NodeId) -> Result<Vec<(NodeId, NodeKind)>, StateError> {
        Ok(match self.0.local_by_node(node)? {
            Some(row) => self
                .0
                .local_children_of(&row)?
                .into_iter()
                .map(|r| (r.node, r.kind))
                .collect(),
            None => Vec::new(),
        })
    }

    fn live(&self, node: NodeId) -> Result<bool, StateError> {
        Ok(self.0.get_mirror(node)?.is_some())
    }

    fn removed(&self, node: NodeId) -> Result<Option<(NodeId, i64)>, StateError> {
        Ok(self
            .0
            .removed_by_node(node)?
            .map(|r| (r.row.parent, r.time())))
    }

    fn removal_pending_under(&self, node: NodeId) -> Result<bool, StateError> {
        self.0.removal_pending_under(node)
    }
}

/// The nodes [`keys::WIPED`] holds; an id that doesn't parse is left out.
fn read_wiped(value: Option<&str>) -> Vec<NodeId> {
    crate::scope::Scope::outside_of(value.unwrap_or_default())
        .into_iter()
        .collect()
}

/// Keeps `wiped` as [`keys::WIPED`] (in [`crate::scope::Scope`]'s id list).
fn save_wiped(db: &mut StateDb, wiped: &HashSet<NodeId>) -> Result<(), StateError> {
    let mut ids: Vec<String> = wiped.iter().map(|n| n.0.to_string()).collect();
    ids.sort();
    db.write(|s| match ids.is_empty() {
        true => s.folder().delete_kv(keys::WIPED),
        false => s.folder().set_kv(keys::WIPED, &ids.join(",")),
    })
}

#[cfg(test)]
mod tests {
    use hbsync_state::testkit::RawPicture;
    use hbsync_state::{MirrorRow, RemovedRow, StateDb};
    use hbsync_types::{RemoteFileId, Uuid, VersionTag};

    use super::*;

    fn removed(node: NodeId, parent: NodeId, removed_at: Option<i64>) -> RemovedRow {
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
            removed_at,
        }
    }

    /// The store's rows of Recently deleted as the rules read them ([`Sides::removed`]): the
    /// folder each was in, and its removal's time, an open one the newest.
    #[test]
    fn the_store_gives_a_removed_rows_folder_and_time() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let (f, x, y) = (NodeId::new(), NodeId::new(), NodeId::new());
        db.write(|s| {
            s.account()
                .put_removed(&removed(f, NodeId::ROOT, Some(5)))?;
            s.account().put_removed(&removed(x, f, Some(7)))?;
            s.account().put_removed(&removed(y, f, None))
        })?;
        db.read(|s| {
            let sides = StoreSides(s);
            assert_eq!(sides.removed(f)?, Some((NodeId::ROOT, 5)));
            assert_eq!(sides.removed(x)?, Some((f, 7)));
            assert_eq!(sides.removed(y)?, Some((f, i64::MAX)));
            assert_eq!(sides.removed(NodeId::new())?, None);
            Ok(())
        })
    }
}
