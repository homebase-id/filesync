//! The folders of the account, and the node ids they share.

use super::*;

impl Store<'_> {
    // ---- folders ----

    /// The folder this store sees.
    pub fn folder(&self) -> i64 {
        self.folder
    }

    /// How this store's folder sees the drive's rows (the mirror, Recently deleted): its top
    /// as the root, the rest of the drive as [`NodeId::OUTSIDE`]. The account (no folder) sees
    /// them as they are.
    pub fn coords(&self) -> Result<Coords> {
        if let Some(c) = self.coords.get() {
            return Ok(c);
        }
        let c = self
            .folders()?
            .into_iter()
            .find(|f| f.id == self.folder)
            .map_or(Coords::MAIN, |f| Coords { top: f.top });
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
        r.removal.root = self.coords()?.from_drive(r.removal.root);
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

    pub fn folders(&self) -> Result<Vec<FolderRow>> {
        Ok(self.folder_list()?.to_vec())
    }

    /// [`Store::folders`], shared.
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
        self.places.borrow_mut().clear();
        self.local_ids.borrow_mut().clear();
        self.local_paths.borrow_mut().clear();
        self.change("DELETE FROM folders WHERE id = ?1", id)
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
        if let Some(id) = self.key(node)? {
            return Ok(id);
        }
        self.prep("INSERT INTO nodes (uuid) VALUES (?1)")?
            .execute([node.0.as_bytes()])?;
        Ok(self.conn.last_insert_rowid())
    }

    pub(super) fn uuid_of(&self, id: i64) -> Result<NodeId> {
        let bytes: Vec<u8> = self
            .prep("SELECT uuid FROM nodes WHERE id = ?1")?
            .query_row([id], |r| r.get(0))?;
        codec::node(&bytes)
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
             AND NOT EXISTS (SELECT 1 FROM forks WHERE id = nodes.id OR new_node = nodes.id)",
            )?
            .execute([])?;
        self.prep("DELETE FROM gone")?.execute([])?;
        Ok(n)
    }

    pub fn node_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM nodes")
    }
}
