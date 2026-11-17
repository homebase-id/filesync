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

    /// The children of `parent`: all, or those whose name has sibling hash `key`. On another
    /// identity's drive, as [`ReadTx::seen`] places them: the top also holds the nodes whose
    /// folder the picture doesn't have ([`ReadTx::folderless`]), and such a folder holds none.
    fn mirror_in(&self, parent: NodeId, key: Option<i32>) -> Result<Vec<MirrorRow>> {
        let c = self.coords()?;
        let drive = c.to_drive(parent);
        let mut rows = if self.lacks(drive)? {
            Vec::new()
        } else {
            match key {
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
            }
        };
        if drive.is_root() && self.shared()? {
            rows.extend(self.folderless(key)?);
        }
        // Placed already: each row is where the drive has it, or at the top.
        let mut seen = Vec::with_capacity(rows.len() + 1);
        for row in rows {
            seen.extend(self.in_coords(row)?);
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

    /// Whether the picture has `node`, live or in Recently deleted: on another identity's
    /// drive, whether what the mirror holds in it is in it, or at the top for want of it
    /// ([`ReadTx::seen`]). What a folder in Recently deleted holds stays in it (taken along,
    /// node schema).
    pub(super) fn has_node(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .prep(
                "SELECT EXISTS (SELECT 1 FROM remote m JOIN nodes n ON n.id = m.id \
                 WHERE n.uuid = ?1) OR EXISTS (SELECT 1 FROM removed WHERE node = ?1)",
            )?
            .query_row([node.0.as_bytes()], |r| r.get(0))?)
    }

    /// Whether `folder` is one the picture of another identity's drive ([`ReadTx::shared`])
    /// doesn't have ([`ReadTx::has_node`]): what the mirror holds in it is at the top. Never
    /// the drive's root, and never on this identity's own drive (no probe there).
    pub(super) fn lacks(&self, folder: NodeId) -> Result<bool> {
        Ok(!folder.is_root() && self.shared()? && !self.has_node(folder)?)
    }

    /// Another identity's drive: the mirror's nodes whose folder the picture doesn't have
    /// (all, or those whose name has sibling hash `key`), each as at the top: what is in the
    /// folders the picture lacks ([`ReadTx::lacking`]), a read along the index for each (one
    /// statement, bound again for each folder). None where every folder that holds something
    /// shared is shared itself.
    fn folderless(&self, key: Option<i32>) -> Result<Vec<MirrorRow>> {
        let lacking = self.lacking()?;
        if lacking.is_empty() {
            return Ok(Vec::new());
        }
        let named = if key.is_some() {
            " AND m.name_key = ?2"
        } else {
            ""
        };
        let mut stmt = self.prep(&format!("{MIRROR_SELECT} WHERE m.parent = ?1{named}"))?;
        let decode = |r: &Row<'_>| Ok(mirror_row(r));
        let mut rows = Vec::new();
        for folder in lacking.iter() {
            let found = match key {
                None => stmt.query_map([folder], decode)?,
                Some(key) => stmt.query_map(params![folder, key], decode)?,
            };
            for row in found {
                let mut row = row??;
                row.parent = NodeId::ROOT;
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Another identity's drive: the folders (their ids in `nodes`) that the mirror's rows
    /// name and the picture doesn't have. Found by folder, not by node: the folders named (a
    /// step along the index for each), then the ones among them the picture lacks (two probes
    /// each), so the cost is the number of folders holding something shared, whatever the
    /// number of files. Worked out once per transaction: a plan asks for every name at the top.
    fn lacking(&self) -> Result<std::rc::Rc<[i64]>> {
        if let Some(known) = self.lacking.borrow().as_ref() {
            return Ok(known.clone());
        }
        let ids: std::rc::Rc<[i64]> = self
            .prep(
                "WITH RECURSIVE up(id) AS (SELECT min(parent) FROM remote UNION ALL \
                 SELECT (SELECT min(parent) FROM remote WHERE parent > up.id) FROM up \
                 WHERE up.id IS NOT NULL) \
                 SELECT up.id FROM up JOIN nodes u ON u.id = up.id \
                 WHERE u.uuid <> ?1 AND NOT EXISTS (SELECT 1 FROM remote f WHERE f.id = up.id) \
                 AND NOT EXISTS (SELECT 1 FROM removed r WHERE r.node = u.uuid)",
            )?
            .query_map([NodeId::ROOT.0.as_bytes()], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        *self.lacking.borrow_mut() = Some(ids.clone());
        Ok(ids)
    }

    /// The picture changes (a row of the mirror or of Recently deleted comes, moves or goes):
    /// which folders it lacks ([`ReadTx::lacking`]) is worked out again.
    pub(super) fn picture_changed(&self) {
        self.lacking.take();
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

    /// Whether file `file_id` was deleted for good (`mark_file_gone`).
    pub fn file_gone(&self, file_id: RemoteFileId) -> Result<bool> {
        Ok(self
            .prep("SELECT 1 FROM gone_files WHERE file_id = ?1")?
            .exists([file_id.0.as_bytes()])?)
    }

    /// Marks `node` dirty in the folders a change of it, now in the drive's folder `place`,
    /// concerns: those holding a base or local row of it, and the one whose part of the drive
    /// `place` is in (the folder with the nearest top above it, [`ReadTx::place_of`]). Every
    /// folder when `place` doesn't reach the drive's top (a gap or a cycle): none is left out.
    pub(super) fn fan_out(&self, node: NodeId, place: NodeId) -> Result<()> {
        let folders = self.folder_list()?;
        let id = self.key_or_insert(node)?;
        let marked: std::collections::BTreeSet<i64> = match &*folders {
            [only] => {
                self.mark_changed_in(only.id, id)?;
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
        self.mark_changed_in_all(&marked, id)
    }

    /// Marks `node` dirty in every folder: a node whose folder left the drive for good, or that
    /// waited in one this client couldn't read, has no place to go by (an orphan, for the main
    /// folder to adopt, §6.3), whatever its folder's place was.
    pub(super) fn fan_out_everywhere(&self, node: NodeId) -> Result<()> {
        let folders: std::collections::BTreeSet<i64> =
            self.folder_list()?.iter().map(|f| f.id).collect();
        self.mark_changed_in_all(&folders, self.key_or_insert(node)?)
    }

    /// [`ReadTx::mark_changed_in`] in each of `folders`, which `fan_out` touched.
    fn mark_changed_in_all(
        &self,
        folders: &std::collections::BTreeSet<i64>,
        id: i64,
    ) -> Result<()> {
        for &folder in folders {
            self.mark_changed_in(folder, id)?;
        }
        self.touched.borrow_mut().extend(folders);
        Ok(())
    }

    /// The place of the drive's folder `place` (the folder with the nearest top above it);
    /// `None` if it doesn't reach the top. A folder the mirror lacks has its place where its row
    /// in Recently deleted is (sync semantics §6.3: where its own folder is, live or in Recently
    /// deleted in turn): what a folder's delete takes along is that folder's only, not every
    /// folder's. Remembered for every live folder on the way, until one of them changes
    /// ([`ReadTx::moved_folder`]).
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
            let up: Option<Vec<u8>> = match parent
                .query_row([at.0.as_bytes()], |r| r.get(0))
                .optional()?
            {
                Some(up) => Some(up),
                None => {
                    // Only live folders are remembered: a row of Recently deleted may go
                    // (deleted for good) with nothing telling the places.
                    chain.clear();
                    self.prep("SELECT parent FROM removed WHERE node = ?1")?
                        .query_row([at.0.as_bytes()], |r| r.get(0))
                        .optional()?
                }
            };
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
/// Tests: puts Desktops and a folder in it for each of `desktops` (its node and name) in the
/// mirror of the account's DB at `db`, as the feed would.
#[cfg(feature = "testkit")]
pub fn put_desktops(db: &std::path::Path, desktops: &[(NodeId, &str)]) -> Result<()> {
    let folder = |node, parent, name: &str| MirrorRow {
        file_id: RemoteFileId(hbsync_types::Uuid::new_v4()),
        version: VersionTag(hbsync_types::Uuid::new_v4()),
        node,
        parent,
        name: name.into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: Some(5),
    };
    crate::StateDb::open_account(db)?.write(|s| {
        let a = s.account();
        a.put_live(&folder(NodeId::DESKTOPS, NodeId::ROOT, "Desktops"))?;
        for (node, name) in desktops {
            a.put_live(&folder(*node, NodeId::DESKTOPS, name))?;
        }
        Ok(())
    })
}

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
    pub(crate) fn put_mirror(&self, row: &MirrorRow) -> Result<()> {
        if self.has_version(row.remote_ref(), false)? || self.file_gone(row.file_id)? {
            return Ok(());
        }
        // A new row of it: it is there, whatever the identity said once.
        self.clear_file_marks(row.file_id)?;
        let Some(row) = &self.on_drive(row)? else {
            return Ok(());
        };
        self.moved_folder(row.node, row.kind);
        self.picture_changed();
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
    /// (`fan_out`), and returns the row, in the folder the drive had it in.
    pub(crate) fn delete_mirror(&self, file_id: RemoteFileId) -> Result<Option<MirrorRow>> {
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
        self.picture_changed();
        self.change(
            "DELETE FROM remote WHERE file_id = ?1",
            file_id.0.as_bytes(),
        )?;
        self.in_coords(row)
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
    /// coordinates), where it matters: a node in a removed folder whose time isn't known yet,
    /// or is another (as [`AccountTx::confirm_removed`] has it). One read where it has that
    /// time.
    pub fn refresh_time(&self, file_id: RemoteFileId, parent: NodeId, at: i64) -> Result<()> {
        let other = self
            .prep("SELECT 1 FROM remote WHERE file_id = ?1 AND changed_at IS NOT ?2")?
            .exists(params![file_id.0.as_bytes(), at])?;
        if other && self.removed_above(parent, NodeId::ROOT)?.is_some() {
            self.prep(
                "UPDATE remote SET changed_at = ?2 WHERE file_id = ?1 AND changed_at IS NOT ?2",
            )?
            .execute(params![file_id.0.as_bytes(), at])?;
        }
        Ok(())
    }

    /// Records file `file_id` as deleted for good: Homebase never gives a file id back.
    pub(crate) fn mark_file_gone(&self, file_id: RemoteFileId) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO gone_files (file_id) VALUES (?1)")?
            .execute([file_id.0.as_bytes()])?;
        self.clear_file_marks(file_id)
    }
}

impl Store<'_> {
    /// The folders `fan_out` marked a node dirty in, so far.
    pub(crate) fn take_touched(&self) -> std::collections::BTreeSet<i64> {
        self.touched.take()
    }
}
