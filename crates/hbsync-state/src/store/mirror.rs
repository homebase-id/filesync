//! The remote mirror: the drive as the feed told it, and what a change of it concerns.

use super::*;

impl ReadTx<'_> {
    // ---- remote mirror ----

    pub fn get_mirror(&self, node: NodeId) -> Result<Option<MirrorRow>> {
        if node.is_root() {
            return Ok(None);
        }
        if self.coords()?.is_outside(node) {
            return Ok(Some(outside_row()));
        }
        match self.one(
            &format!("{MIRROR_SELECT} WHERE n.uuid = ?1"),
            node.0.as_bytes(),
            mirror_row,
        )? {
            Some(row) => self.seen(row),
            None => Ok(None),
        }
    }

    /// Whether the mirror has file `at` at that version (the node is the file's), or, with
    /// `removed`, Recently deleted has.
    pub fn has_version(&self, at: RemoteRef, removed: bool) -> Result<bool> {
        let table = if removed { "removed" } else { "remote" };
        Ok(self
            .prep(&format!(
                "SELECT 1 FROM {table} WHERE file_id = ?1 AND version_tag = ?2"
            ))?
            .exists([at.file_id.0.as_bytes(), at.version.0.as_bytes()])?)
    }

    pub fn mirror_by_file(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        match self.one(
            &format!("{MIRROR_SELECT} WHERE m.file_id = ?1"),
            file_id.0.as_bytes(),
            mirror_row,
        )? {
            Some(row) => self.seen(row),
            None => Ok(None),
        }
    }

    pub fn mirror_children(&self, parent: NodeId) -> Result<Vec<MirrorRow>> {
        self.mirror_in(parent, None)
    }

    /// The children of `parent` whose name has sibling hash `key`, as
    /// [`ReadTx::base_named`].
    pub fn mirror_named(&self, parent: NodeId, key: i32) -> Result<Vec<MirrorRow>> {
        self.mirror_in(parent, Some(key))
    }

    /// The children of `parent`: all, or those whose name has sibling hash `key`.
    fn mirror_in(&self, parent: NodeId, key: Option<i32>) -> Result<Vec<MirrorRow>> {
        let c = self.coords()?;
        let drive = c.to_drive(parent);
        let rows = match key {
            None => self.many(
                &format!("{MIRROR_SELECT} WHERE m.parent = {ID_OF}"),
                [drive.0.as_bytes()],
                mirror_row,
            )?,
            Some(key) => self.many(
                &format!("{MIRROR_SELECT} WHERE m.parent = {ID_OF} AND m.name_key = ?2"),
                params![drive.0.as_bytes(), key],
                mirror_row,
            )?,
        };
        let mut seen = Vec::with_capacity(rows.len() + 1);
        for row in rows {
            seen.extend(self.seen(row)?);
        }
        // The rest of the drive is a folder at the top.
        if parent.is_root()
            && c.is_outside(NodeId::OUTSIDE)
            && key.is_none_or(|k| k == sibling_hash(Coords::OUTSIDE_NAME))
        {
            seen.push(outside_row());
        }
        Ok(seen)
    }

    /// The folder in Recently deleted that the drive's folder `folder` is, or is in through the
    /// mirror's folders, below `top` (the drive's root, or a role folder's top): the first one
    /// missing from the mirror, if it is there, with when it went there (`None`: open, this
    /// device's own removal). A folder the mirror has is live, whatever row Recently deleted
    /// may still hold for it.
    pub fn removed_above(
        &self,
        folder: NodeId,
        top: NodeId,
    ) -> Result<Option<(NodeId, Option<i64>)>> {
        let mut parent = self.prep(MIRROR_PARENT)?;
        let mut removed = self.prep("SELECT removed_at FROM removed WHERE node = ?1")?;
        let mut at = folder;
        for _ in 0..MAX_DEPTH {
            if at.is_root() || at == top {
                return Ok(None);
            }
            match parent
                .query_row([at.0.as_bytes()], |r| r.get::<_, Vec<u8>>(0))
                .optional()?
            {
                Some(up) => at = codec::node(&up)?,
                None => {
                    return Ok(removed
                        .query_row([at.0.as_bytes()], |r| r.get::<_, Option<i64>>(0))
                        .optional()?
                        .map(|when| (at, when)));
                }
            }
        }
        Ok(None)
    }

    /// Whether file `file_id` was deleted for good ([`AccountTx::mark_file_gone`]).
    pub fn file_gone(&self, file_id: RemoteFileId) -> Result<bool> {
        Ok(self
            .prep("SELECT 1 FROM gone_files WHERE file_id = ?1")?
            .exists([file_id.0.as_bytes()])?)
    }

    /// Marks `node` dirty in the folders a change of it, now in the drive's folder `place`,
    /// concerns: those holding a base or local row of it, and the one whose part of the drive
    /// `place` is in (the folder with the nearest top above it). Every folder when `place`
    /// doesn't reach the drive's top through the mirror (a gap or a cycle): none is left out.
    pub(super) fn fan_out(&self, node: NodeId, place: NodeId) -> Result<()> {
        let folders = self.folder_list()?;
        let id = self.key_or_insert(node)?;
        let marked: std::collections::BTreeSet<i64> = match &*folders {
            [only] => {
                self.mark_dirty_in(only.id, id)?;
                self.touched.borrow_mut().insert(only.id);
                return Ok(());
            }
            _ => {
                let mut holders = self
                    .prep(
                        "SELECT folder FROM base WHERE id = ?1 \
                         UNION SELECT folder FROM local WHERE id = ?1",
                    )?
                    .query_map([id], |r| r.get(0))?
                    .collect::<rusqlite::Result<std::collections::BTreeSet<i64>>>()?;
                match self.place_of(place, &folders)? {
                    Some(Place::Folder(folder)) => {
                        holders.insert(folder);
                    }
                    Some(Place::Unheld) => {}
                    // A gap or a cycle: whose it is isn't known.
                    None => holders.extend(folders.iter().map(|f| f.id)),
                }
                holders
            }
        };
        for &folder in &marked {
            self.mark_dirty_in(folder, id)?;
        }
        self.touched.borrow_mut().extend(marked);
        Ok(())
    }

    /// The place of the drive's folder `place` (the folder with the nearest top above it);
    /// `None` if the mirror doesn't connect it to the top. Remembered for every folder on the
    /// way, until one of them changes ([`ReadTx::moved_folder`]).
    pub(super) fn place_of(&self, place: NodeId, folders: &[FolderRow]) -> Result<Option<Place>> {
        let mut parent = self.prep(MIRROR_PARENT)?;
        let mut chain = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut at = place;
        let found = loop {
            if let Some(&known) = self.places.borrow().get(&at) {
                break Some(known);
            }
            if let Some(folder) = folders.iter().find(|f| f.top == at) {
                break Some(Place::Folder(folder.id));
            }
            if at.is_root() {
                break Some(Place::Unheld);
            }
            chain.push(at);
            let up: Option<Vec<u8>> = parent
                .query_row([at.0.as_bytes()], |r| r.get(0))
                .optional()?;
            match up {
                Some(up) if seen.insert(at) && seen.len() <= MAX_DEPTH => at = codec::node(&up)?,
                _ => break None,
            }
        };
        // Not known: not remembered, so a folder arriving later fills the gap.
        if let Some(found) = found {
            let mut places = self.places.borrow_mut();
            for n in chain {
                places.insert(n, found);
            }
        }
        Ok(found)
    }

    /// Folder `node` of the mirror changes (moves, goes): the places remembered through it
    /// may be stale.
    pub(super) fn moved_folder(&self, node: NodeId, kind: NodeKind) {
        if kind == NodeKind::Folder && self.places.borrow().contains_key(&node) {
            self.places.borrow_mut().clear();
        }
    }
}

/// The rest of the drive as a folder holding part of it sees it: one folder at its top.
fn outside_row() -> MirrorRow {
    MirrorRow {
        file_id: RemoteFileId(hbsync_types::Uuid::nil()),
        version: VersionTag(hbsync_types::Uuid::nil()),
        node: NodeId::OUTSIDE,
        parent: NodeId::ROOT,
        name: Coords::OUTSIDE_NAME.into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: None,
    }
}

impl AccountTx<'_> {
    /// Records a live remote node, and marks it dirty in the folders the change concerns
    /// (`fan_out`). A row for the same node under an older file id (the node was
    /// deleted and re-created) is replaced. A version the mirror has already (this device's own
    /// write coming back through the feed, or a page applied again) changes nothing, and so does
    /// a file id deleted for good ([`ReadTx::file_gone`]: a page fetched before, or a write
    /// committed after, can't bring it back).
    pub fn put_mirror(&self, row: &MirrorRow) -> Result<()> {
        if self.has_version(row.remote_ref(), false)? || self.file_gone(row.file_id)? {
            return Ok(());
        }
        // A new row of it: it is there, whatever the identity said once.
        self.clear_gone_suspect(row.file_id)?;
        self.clear_unreadable(row.file_id)?;
        let Some(row) = &self.on_drive(row)? else {
            return Ok(());
        };
        self.moved_folder(row.node, row.kind);
        let (id, parent) = (
            self.key_or_insert(row.node)?,
            self.key_or_insert(row.parent)?,
        );
        let f = row.file.as_ref();
        self.prep(
            "INSERT INTO remote (id, file_id, version_tag, parent, kind, name, \
                 hash, size, mtime_ms, executable, content_extra, token, changed_at, name_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
                 ON CONFLICT (id) DO UPDATE SET file_id = ?2, version_tag = ?3, parent = ?4, \
                 kind = ?5, name = ?6, hash = ?7, size = ?8, mtime_ms = ?9, executable = ?10, \
                 content_extra = ?11, token = ?12, changed_at = ?13, name_key = ?14",
        )?
        .execute(params![
            id,
            row.file_id.0.as_bytes(),
            row.version.0.as_bytes(),
            parent,
            codec::kind_to(row.kind),
            row.name,
            f.map(|f| codec::hash_to(&f.hash)),
            f.map(|f| f.size as i64),
            f.and_then(|f| f.mtime_ms),
            f.is_some_and(|f| f.executable),
            row.content_extra,
            row.token.as_ref().map(|t| &t.0[..]),
            row.changed_at,
            sibling_hash(&row.name),
        ])?;
        self.fan_out(row.node, row.parent)
    }

    /// Removes the mirror row of a deleted file, marks its node dirty where that concerns
    /// (`fan_out`), and returns the row.
    pub fn delete_mirror(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
        let Some(row) = self.one(
            &format!("{MIRROR_SELECT} WHERE m.file_id = ?1"),
            file_id.0.as_bytes(),
            mirror_row,
        )?
        else {
            return Ok(None);
        };
        self.fan_out(row.node, row.parent)?;
        self.moved_folder(row.node, row.kind);
        self.change(
            "DELETE FROM remote WHERE file_id = ?1",
            file_id.0.as_bytes(),
        )?;
        self.seen(row)
    }

    /// Keeps `token` for file `file_id` (in Recently deleted if `removed`), if it differs.
    pub fn set_token(&self, file_id: RemoteFileId, removed: bool, token: &FileToken) -> Result<()> {
        let table = if removed { "removed" } else { "remote" };
        self.prep(&format!(
            "UPDATE {table} SET token = ?2 WHERE file_id = ?1 AND token IS NOT ?2"
        ))?
        .execute(params![file_id.0.as_bytes(), &token.0[..]])?;
        Ok(())
    }

    /// Records the server's time of the version the mirror has as `file_id` in `parent` (drive
    /// coordinates), where it matters: a node in a removed folder whose time isn't known yet.
    pub fn refresh_time(&self, file_id: RemoteFileId, parent: NodeId, at: i64) -> Result<()> {
        let unknown = self
            .prep("SELECT 1 FROM remote WHERE file_id = ?1 AND changed_at IS NULL")?
            .exists([file_id.0.as_bytes()])?;
        if unknown && self.removed_above(parent, NodeId::ROOT)?.is_some() {
            self.prep(
                "UPDATE remote SET changed_at = ?2 WHERE file_id = ?1 AND changed_at IS NULL",
            )?
            .execute(params![file_id.0.as_bytes(), at])?;
        }
        Ok(())
    }

    /// Records file `file_id` as deleted for good: Homebase never gives a file id back.
    pub fn mark_file_gone(&self, file_id: RemoteFileId) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO gone_files (file_id) VALUES (?1)")?
            .execute([file_id.0.as_bytes()])?;
        self.clear_unreadable(file_id)?;
        self.clear_gone_suspect(file_id)
    }
}

impl Store<'_> {
    /// The folders `fan_out` marked a node dirty in, so far.
    pub(crate) fn take_touched(&self) -> std::collections::BTreeSet<i64> {
        self.touched.take()
    }
}
