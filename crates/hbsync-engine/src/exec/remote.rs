//! Remote operations: folders, renames and moves, deletes. Each is checked here and written on
//! the worker pool ([`Job::Meta`]), next to the transfers.

use super::*;

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
                content.set_tags(&restored_tags(base.as_ref(), tags, r));
            }
            None => {
                content.set_created_ms(ops::created_ms(&self.abs(&row.path)));
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
        content.set_tags(tags);
        // A node from before creation times, or from a client that sends none.
        if content.created_ms().is_none()
            && let Some(row) = self.db.read(|s| s.local_by_node(node))?
        {
            content.set_created_ms(ops::created_ms(&self.abs(&row.path)));
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
        let meta = NodeMeta {
            node,
            parent: mirror.parent,
            kind: mirror.kind,
            content: mirror.content(&mirror.name),
            removed: true,
        };
        let hash = mirror.file.map(|f| f.hash);
        Ok(meta_job(
            meta,
            Some(mirror.target_at(expected)),
            hash,
            Some(mirror.parent),
            After::Removed { file_id },
        ))
    }

    /// Records a metadata write that landed (`written`), or a removal the drive no longer
    /// needed (`None`: deleted for good meanwhile).
    pub(super) fn commit_meta(
        &mut self,
        meta: NodeMeta,
        hash: Option<hbsync_types::ContentHash>,
        after: After,
        written: Option<Target>,
    ) -> Step {
        let node = meta.node;
        let row = written.map(|w| written_row(w, &meta, hash));
        self.finish(&[(node, false)], |s| match after {
            After::Created => {
                s.delete_removed(node)?;
                row.map_or(Ok(()), |row| s.put_mirror(&row))
            }
            After::Updated => row.map_or(Ok(()), |row| s.put_mirror(&row)),
            // Out of the tree, into Recently deleted with its time open: the write's answer has
            // none, and this device's clock isn't the server's. The feed brings the time back
            // (by the end of the sync at the latest); it knows that version meanwhile.
            After::Removed { file_id } => {
                s.delete_mirror(file_id)?;
                if let Some(row) = row {
                    s.put_removed(&RemovedRow {
                        row,
                        removed_at: None,
                        seq: 0,
                    })?;
                }
                s.mark_dirty(node)
            }
        })
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
