//! The folders of the account, and the node ids they share.

use super::*;

impl ReadTx<'_> {
    // ---- folders ----

    /// How this store's folder sees the drive's rows (the mirror, Recently deleted): its top
    /// as the root, the rest of the drive as [`NodeId::OUTSIDE`]. The account (no folder) sees
    /// them as they are. A folder the DB doesn't have (removed meanwhile) is an error, never
    /// the main folder: a role folder seeing the whole drive would sync the wrong part of it.
    pub fn coords(&self) -> Result<Coords> {
        if let Some(c) = self.coords.get() {
            return Ok(c);
        }
        let c = if self.folder == crate::NO_FOLDER {
            Coords::MAIN
        } else {
            self.folder_list()?
                .iter()
                .find(|f| f.id == self.folder)
                .map(|f| Coords { top: f.top })
                .ok_or_else(|| {
                    StateError::Conflict(format!("no folder {} in this DB", self.folder))
                })?
        };
        self.coords.set(Some(c));
        Ok(c)
    }

    /// `row` of the drive as this folder sees it; `None` for its top (its root).
    pub(super) fn seen(&self, mut row: MirrorRow) -> Result<Option<MirrorRow>> {
        let c = self.coords()?;
        if c.hides(row.node) {
            return Ok(None);
        }
        row.parent = c.from_drive(row.parent);
        Ok(Some(row))
    }

    pub(super) fn seen_removed(&self, mut r: RemovedRow) -> Result<Option<RemovedRow>> {
        let Some(row) = self.seen(r.row)? else {
            return Ok(None);
        };
        r.row = row;
        Ok(Some(r))
    }

    /// `row` as the drive has it: the folder's root is its top. `None` for a row claiming the
    /// root's id (another client's mistake: never stored, as the feed must go on past it); the
    /// rest of the drive ([`NodeId::OUTSIDE`]) is never written.
    pub(super) fn on_drive(&self, row: &MirrorRow) -> Result<Option<MirrorRow>> {
        let c = self.coords()?;
        if row.node.is_root() {
            return Ok(None);
        }
        if c.is_outside(row.node) {
            return Err(StateError::Conflict(format!(
                "{:?} isn't a node of the drive",
                row.node
            )));
        }
        Ok(Some(MirrorRow {
            parent: c.to_drive(row.parent),
            ..row.clone()
        }))
    }

    pub fn folders(&self) -> Result<Vec<FolderRow>> {
        Ok(self.folder_list()?.to_vec())
    }

    /// [`ReadTx::folders`], shared.
    pub(super) fn folder_list(&self) -> Result<std::rc::Rc<[FolderRow]>> {
        if let Some(known) = self.folders.borrow().as_ref() {
            return Ok(known.clone());
        }
        let mut stmt = self.prep("SELECT id, top, path, marker FROM folders ORDER BY id")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get(2)?,
                r.get(3)?,
            ))
        })?;
        rows.map(|r| {
            let (id, top, path, marker) = r?;
            Ok(FolderRow {
                id,
                top: codec::node(&top)?,
                path,
                marker,
            })
        })
        .collect::<Result<std::rc::Rc<[FolderRow]>>>()
        .inspect(|all| *self.folders.borrow_mut() = Some(all.clone()))
    }

    // ---- node ids ----

    pub(super) fn key(&self, node: NodeId) -> Result<Option<i64>> {
        Ok(self
            .prep("SELECT id FROM nodes WHERE uuid = ?1")?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?)
    }

    /// Whether this DB knows `node`: anywhere, or until its id is collected.
    pub fn knows_node(&self, node: NodeId) -> Result<bool> {
        Ok(self.key(node)?.is_some())
    }

    /// The integer id of `node`, assigned on first use.
    pub(super) fn key_or_insert(&self, node: NodeId) -> Result<i64> {
        self.node_key(node).map(|(id, _)| id)
    }

    /// The id of `node`, inserted if it had none; whether it was.
    pub(super) fn node_key(&self, node: NodeId) -> Result<(i64, bool)> {
        if let Some(id) = self.key(node)? {
            return Ok((id, false));
        }
        self.prep("INSERT INTO nodes (uuid) VALUES (?1)")?
            .execute([node.0.as_bytes()])?;
        Ok((self.conn.last_insert_rowid(), true))
    }

    pub(super) fn uuid_of(&self, id: i64) -> Result<NodeId> {
        let bytes: Vec<u8> = self
            .prep("SELECT uuid FROM nodes WHERE id = ?1")?
            .query_row([id], |r| r.get(0))?;
        codec::node(&bytes)
    }

    pub fn node_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM nodes")
    }
}

impl AccountTx<'_> {
    /// Registers a folder holding the drive's node `top`; its id.
    pub fn add_folder(&self, top: NodeId, path: &str, marker: &str) -> Result<i64> {
        self.folders.take();
        self.places.borrow_mut().clear();
        self.prep(
            "INSERT INTO folders (top, path, marker, added_at) VALUES (?1, ?2, ?3, unixepoch())",
        )?
        .execute(params![top.0.as_bytes(), path, marker])?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Records that folder `id` is at `path` now (it was moved).
    pub fn set_folder_path(&self, id: i64, path: &str) -> Result<()> {
        self.folders.take();
        self.prep("UPDATE folders SET path = ?2 WHERE id = ?1")?
            .execute(params![id, path])?;
        Ok(())
    }

    /// Forgets folder `id` and every row it has (the account's stay).
    pub fn remove_folder(&self, id: i64) -> Result<bool> {
        self.folders.take();
        self.coords.take();
        self.places.borrow_mut().clear();
        self.forget_local_ids();
        self.change("DELETE FROM folders WHERE id = ?1", id)
    }

    /// Forgets the ids of nodes nothing refers to any more (gone everywhere, nothing pending).
    /// Only ids that lost a reference since the last call are checked (triggers note them in
    /// `gone`). Ids are never handed out again, so an id held anywhere can't come to mean
    /// another node.
    pub fn collect_nodes(&self) -> Result<usize> {
        let n = self
            .prep(
                "DELETE FROM nodes WHERE id IN (SELECT id FROM gone) \
             AND NOT EXISTS (SELECT 1 FROM local WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM local WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM base WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM base WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM remote WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM remote WHERE parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM dirty WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM parked WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM held_deletes WHERE id = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM forks WHERE id = nodes.id OR new_node = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM placing WHERE id = nodes.id OR parent = nodes.id) \
             AND NOT EXISTS (SELECT 1 FROM refused WHERE id = nodes.id)",
            )?
            .execute([])?;
        self.prep("DELETE FROM gone")?.execute([])?;
        Ok(n)
    }
}
