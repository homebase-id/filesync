//! Remote operations: folders, renames and moves, deletes. Each is checked here and written on
//! the worker pool ([`Job::Meta`]), next to the transfers.

use super::*;
use crate::mirror;

/// What a metadata write means for the DB once it landed.
pub(super) enum After {
    /// A folder created, or restored from Recently deleted.
    Created,
    /// Renamed, moved or tagged.
    Updated,
    /// Moved to Recently deleted: its mirror row (`file_id`) goes, its removed row comes.
    Removed { file_id: hbsync_types::RemoteFileId },
}

impl Executor<'_> {
    /// The tags a write over a drive version sends: `ours` merged with that version's, which may
    /// hold tags this device never saw (§11): a no-op where the plan merged them already, the
    /// drive's tags kept where it didn't (a node restored from Recently deleted, or taken along).
    /// What this device can't hold is that version's ([`Tags::merge_onto`]).
    pub(super) fn merged_with_drive(
        &self,
        base: Option<&BaseRow>,
        ours: &Tags,
        drive: &Tags,
    ) -> Tags {
        let base = base.map(|b| b.state.tags.clone()).unwrap_or_default();
        Tags::merge_onto(&base, ours, drive, self.folder.os)
    }

    // ---- remote operations: guards before the worker pool, the DB after ----

    pub(super) fn prepare_folder(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        tags: &Tags,
    ) -> Step<(Job, Ctx)> {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Err(Outcome::stale("gone locally"));
        };
        if !self.on_disk_unchanged(&row) {
            return Err(rescan(&mut self.report, &row.path));
        }
        let removed = self.db.read(|s| s.removed_by_node(node))?;
        let mut content = NodeContent::folder(name);
        match &removed {
            // In Recently deleted: restored, keeping its keys (`ct`).
            Some(r) => {
                content.unknown = r.row.carried_keys();
                let base = self.db.read(|s| s.get_base(node))?;
                let drive = r.row.tags();
                content.set_tags_over(&self.merged_with_drive(base.as_ref(), tags, &drive), &drive);
            }
            None => {
                content.set_created_ms(ops::created_ms(&self.disk.abs(&row.path)));
                content.set_tags(tags);
            }
        }
        let meta = NodeMeta {
            node,
            parent,
            kind: NodeKind::Folder,
            content,
            removed: false,
        };
        let target = removed.map(|r| r.row.target());
        Ok(meta_job(meta, target, None, None, After::Created))
    }

    /// A metadata-only update: move or rename (to `place`; `None` keeps it) and the tags,
    /// keeping the payload.
    pub(super) fn prepare_rename(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
        place: Option<(NodeId, &str)>,
        tags: &Tags,
    ) -> Step<(Job, Ctx)> {
        let Some(mirror) = self
            .db
            .read(|s| s.get_mirror(node))?
            .filter(|m| m.file_id == file_id)
        else {
            return Err(Outcome::stale("gone remotely"));
        };
        let (parent, name) = place.unwrap_or((mirror.parent, mirror.name.as_str()));
        let mut content = mirror.content(name);
        let base = self.db.read(|s| s.get_base(node))?;
        let drive = mirror.tags();
        content.set_tags_over(&self.merged_with_drive(base.as_ref(), tags, &drive), &drive);
        // A node from before creation times, or from a client that sends none.
        if content.created_ms().is_none()
            && let Some(row) = self.db.read(|s| s.local_by_node(node))?
        {
            content.set_created_ms(ops::created_ms(&self.disk.abs(&row.path)));
        }
        let meta = NodeMeta {
            node,
            parent,
            kind: mirror.kind,
            content,
            removed: false,
        };
        let hash = mirror.file.map(|f| f.hash);
        Ok(meta_job(
            meta,
            Some(mirror.target_at(expected)),
            hash,
            Some(mirror.parent),
            After::Updated,
        ))
    }

    /// A delete: the node goes to Recently deleted (a metadata-only update setting its
    /// removal), its bytes kept there until the purge. A folder goes first (node schema,
    /// Recently deleted; [`Batch::holds_up`]).
    pub(super) fn prepare_remove(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> Step<(Job, Ctx)> {
        let Some(mirror) = self
            .db
            .read(|s| s.mirror_by_file(file_id))?
            .filter(|m| m.node == node)
        else {
            return Err(Outcome::Done); // already out of the tree
        };
        let meta = mirror::meta(&mirror, true);
        let hash = mirror.file.map(|f| f.hash);
        Ok(meta_job(
            meta,
            Some(mirror.target_at(expected)),
            hash,
            Some(mirror.parent),
            After::Removed { file_id },
        ))
    }

    /// Records a metadata write that landed over `target`, as written (`w`).
    pub(super) fn commit_meta(
        &mut self,
        meta: NodeMeta,
        hash: Option<hbsync_types::ContentHash>,
        after: After,
        target: Option<&Target>,
        w: Written,
    ) -> Step {
        let removed = matches!(after, After::Removed { .. });
        let answer = answer(&meta, hash, target, w, removed);
        self.finish_answered(&[(Settle::node(meta.node), answer)])
    }

    /// Records removals that landed together (one request's answers) as
    /// [`Executor::commit_meta`] records each, in one transaction: a commit for the request,
    /// not one per file. They change nothing on disk: a crash before it loses only their
    /// answers, as one before the first commit did (the nodes stay dirty, planned again from
    /// the feed).
    pub(super) fn commit_removed(&mut self, answers: Vec<Answer>) -> Step {
        let answers: Vec<(Settle<'_>, Answer)> = answers
            .into_iter()
            .map(|a| (Settle::node(a.row().node), a))
            .collect();
        self.finish_answered(&answers)
    }
}

/// The drive's answer to a write of `meta` over `target`, as written (`w`): a removal where
/// `removed`.
pub(super) fn answer(
    meta: &NodeMeta,
    hash: Option<hbsync_types::ContentHash>,
    target: Option<&Target>,
    w: Written,
    removed: bool,
) -> Answer {
    let expected = target.map(|t| t.at.version);
    let row = mirror::written(meta, hash, w);
    match removed {
        true => Answer::removal(expected, row),
        false => Answer::live(expected, row),
    }
}

/// The job and the commit context of a metadata write; `from`: the folder the node leaves
/// (its parent on the drive now), if any.
fn meta_job(
    meta: NodeMeta,
    target: Option<Target>,
    hash: Option<hbsync_types::ContentHash>,
    from: Option<NodeId>,
    after: After,
) -> (Job, Ctx) {
    (Job::Meta { meta, target }, Ctx::Meta { hash, from, after })
}
