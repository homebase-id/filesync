//! Remote operations: folders, renames and moves, deletes.

use super::*;

impl Executor<'_> {
    // ---- remote operations ----

    pub(super) fn create_folder(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
        tags: &Tags,
    ) -> Step {
        debug_assert_eq!(kind, NodeKind::Folder, "file creates run through flush");
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::Skip("gone locally".into()));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
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
        let new = NodeMeta {
            node,
            parent,
            kind,
            content: content.clone(),
        };
        let result = match &removed {
            Some(r) => self.remote.update(r.row.file_id, r.row.version, &new, None),
            None => self.remote.create(&new, None),
        };
        let written = match result {
            Ok(w) => w,
            Err(RemoteError::AlreadyExists) => return Ok(Outcome::Done),
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.delete_removed(node)?;
            s.put_mirror(&written_row(written, &new, None))
        })
    }

    /// A metadata-only update: move or rename (to `place`; `None` keeps it) and the tags,
    /// keeping the payload.
    pub(super) fn rename_remote(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
        place: Option<(NodeId, &str)>,
        tags: &Tags,
    ) -> Step {
        let Some(mirror) = self.db.read(|s| s.get_mirror(node))? else {
            return Ok(Outcome::Skip("gone remotely".into()));
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
        };
        let hash = mirror.file.map(|f| f.hash);
        let row = match update_meta(self.remote, file_id, expected, &meta, hash) {
            Ok(row) => row,
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| s.put_mirror(&row))
    }

    /// A delete: the node goes to Recently deleted (a metadata-only update setting its
    /// removal), its bytes kept there until the purge.
    pub(super) fn remove(
        &mut self,
        node: NodeId,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> Step {
        let Some(mirror) = self
            .db
            .read(|s| s.mirror_by_file(file_id))?
            .filter(|m| m.node == node)
        else {
            return Ok(Outcome::Done); // already out of the tree
        };
        let removal = Removal {
            at_ms: hbsync_types::unix_now_ms(),
            root: self.db.read(|s| removal_root(s, node))?,
        };
        let mut content = mirror.content(&mirror.name);
        content.set_removal(Some(removal));
        let meta = NodeMeta {
            node,
            parent: mirror.parent,
            kind: mirror.kind,
            content,
        };
        let hash = mirror.file.map(|f| f.hash);
        let removed = match update_meta(self.remote, file_id, expected, &meta, hash) {
            Ok(row) => Some(RemovedRow { row, removal }),
            Err(RemoteError::NotFound) => None, // deleted for good meanwhile
            Err(e) => return Ok(remote_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.delete_mirror(file_id)?;
            if let Some(removed) = &removed {
                s.put_removed(removed)?;
            }
            s.mark_dirty(node)
        })
    }
}
