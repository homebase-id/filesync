//! Local observations: what each folder holds on disk.

use super::*;

impl Store<'_> {
    // ---- local observations ----

    /// The id of the local row at `path`; for the root, the root's id (it has no row). Walks
    /// down from the nearest folder found before in this transaction.
    pub(super) fn local_id_at(&self, path: &RelPath) -> Result<Option<i64>> {
        let mut missing = Vec::new();
        let mut at = path.clone();
        let mut id = loop {
            if at.is_root() {
                match self.key(NodeId::ROOT)? {
                    Some(root) => break root,
                    None => return Ok(None),
                }
            }
            if let Some(&known) = self.local_ids.borrow().get(&at) {
                break known;
            }
            let parent = at.parent().unwrap_or_default();
            missing.push(std::mem::replace(&mut at, parent));
        };
        let mut stmt =
            self.prep("SELECT id FROM local WHERE folder = $F AND parent = ?1 AND name = ?2")?;
        for below in missing.into_iter().rev() {
            let name = below.name().unwrap_or_default();
            match stmt.query_row(params![id, name], |r| r.get(0)).optional()? {
                Some(child) => id = child,
                None => return Ok(None),
            }
            self.local_ids.borrow_mut().insert(below, id);
        }
        Ok(Some(id))
    }

    /// Forgets the folder ids [`Store::local_id_at`] found: a local write may change them.
    fn forget_local_ids(&self) {
        self.local_ids.borrow_mut().clear();
        self.local_paths.borrow_mut().clear();
    }

    /// The id of the folder at `path`, to write a row into: the root's id is assigned on first
    /// use.
    pub(super) fn folder_id_for_write(&self, path: &RelPath) -> Result<Option<i64>> {
        if path.is_root() {
            return self.key_or_insert(NodeId::ROOT).map(Some);
        }
        self.local_id_at(path)
    }

    /// The path of the folder `id` (the root's id gives the root), walking up its parents
    /// until a folder found before in this transaction; every folder on the way is kept.
    pub(super) fn folder_path(&self, id: i64) -> Result<RelPath> {
        let mut cache = self.local_paths.borrow_mut();
        if cache.is_empty()
            && let Some(root) = self.key(NodeId::ROOT)?
        {
            cache.insert(root, RelPath::root());
        }
        let mut stmt = self.prep("SELECT parent, name FROM local WHERE folder = $F AND id = ?1")?;
        let mut chain: Vec<(i64, String)> = Vec::new();
        let mut at = id;
        let mut path = loop {
            if let Some(p) = cache.get(&at) {
                break p.clone();
            }
            if chain.len() >= MAX_DEPTH {
                return Err(StateError::Corrupt(format!(
                    "local row {id}: parent chain too deep, or a cycle"
                )));
            }
            let (parent, name): (i64, String) = stmt
                .query_row([at], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?
                .ok_or_else(|| {
                    StateError::Corrupt(format!("local row {id} is not connected to the root"))
                })?;
            chain.push((at, name));
            at = parent;
        };
        for (folder, name) in chain.into_iter().rev() {
            path = path
                .join(&name)
                .map_err(|e| StateError::Corrupt(format!("local name {name:?}: {e}")))?;
            cache.insert(folder, path.clone());
        }
        Ok(path)
    }

    pub(super) fn raw_local(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<RawLocal>> {
        let mut stmt = self.prep(sql)?;
        let rows = stmt.query_map(params, |r| RawLocal::read(r, 0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Rows found without their path: each path is its folder's (derived once per folder)
    /// plus its name.
    pub(super) fn with_paths(&self, raw: Vec<RawLocal>) -> Result<Vec<LocalRow>> {
        raw.into_iter()
            .map(|r| {
                let path = self
                    .folder_path(r.parent)?
                    .join(&r.name)
                    .map_err(|e| StateError::Corrupt(format!("local name {:?}: {e}", r.name)))?;
                r.into_row(path)
            })
            .collect()
    }

    /// The folder `node` is in locally: the root for a top-level item, `None` without a row.
    pub fn local_parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        let parent: Option<Vec<u8>> = self
            .prep(
                "SELECT p.uuid FROM local l JOIN nodes n ON n.id = l.id \
                 JOIN nodes p ON p.id = l.parent WHERE n.uuid = ?1 AND l.folder = $F",
            )?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?;
        parent.as_deref().map(codec::node).transpose()
    }

    pub fn get_local(&self, path: &RelPath) -> Result<Option<LocalRow>> {
        if path.is_root() {
            return Ok(None);
        }
        let Some(id) = self.local_id_at(path)? else {
            return Ok(None);
        };
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE l.id = ?1"),
            [id],
        )?;
        raw.into_iter()
            .next()
            .map(|r| r.into_row(path.clone()))
            .transpose()
    }

    pub fn local_by_node(&self, node: NodeId) -> Result<Option<LocalRow>> {
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE n.uuid = ?1"),
            [node.0.as_bytes()],
        )?;
        Ok(self.with_paths(raw)?.into_iter().next())
    }

    /// The node of the folder at `path`: the root for the empty path, `None` if no row is
    /// recorded there.
    pub fn local_node_at(&self, path: &RelPath) -> Result<Option<NodeId>> {
        if path.is_root() {
            return Ok(Some(NodeId::ROOT));
        }
        self.local_id_at(path)?
            .map(|id| self.uuid_of(id))
            .transpose()
    }

    pub fn local_by_file_id(&self, id: &FileId) -> Result<Vec<LocalRow>> {
        let (lo, hi) = codec::file_id_parts(id);
        let raw = self.raw_local(
            &format!(
                "SELECT {LOCAL_COLS} {LOCAL_FROM} \
                 WHERE l.file_id = ?1 AND l.file_id_hi IS ?2 AND l.volume = ?3"
            ),
            params![lo, hi, id.volume as i64],
        )?;
        self.with_paths(raw)
    }

    /// Inserts or updates the row of `row.node` at `row.path`. Fails with
    /// [`StateError::Conflict`] if the node is recorded at another path (move it, or delete
    /// that row, first), if another node holds the path, or if the parent folder has no row.
    pub fn put_local(&self, row: &LocalRow) -> Result<()> {
        let (Some(parent_path), Some(name)) = (row.path.parent(), row.path.name()) else {
            return Err(StateError::Conflict("the root has no row".into()));
        };
        let parent = self.folder_id_for_write(&parent_path)?.ok_or_else(|| {
            StateError::Conflict(format!("{} has no recorded parent folder", row.path))
        })?;
        let id = self.key_or_insert(row.node)?;
        let place: Option<(i64, String)> = self
            .prep("SELECT parent, name FROM local WHERE folder = $F AND id = ?1")?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        if let Some((p, n)) = place
            && (p != parent || n != name)
        {
            let at = self.local_by_node(row.node)?.map(|r| r.path);
            return Err(StateError::Conflict(format!(
                "{:?} is already at {at:?}",
                row.node
            )));
        }
        let holder: Option<i64> = self
            .prep("SELECT id FROM local WHERE folder = $F AND parent = ?1 AND name = ?2")?
            .query_row(params![parent, name], |r| r.get(0))
            .optional()?;
        if holder.is_some_and(|h| h != id) {
            return Err(StateError::Conflict(format!("{} is taken", row.path)));
        }
        let fp = &row.fingerprint;
        let (lo, hi) = codec::file_id_parts(&fp.file_id);
        self.prep(
                "INSERT INTO local (folder, id, parent, name, kind, size, mtime_ns, ctime_ns, \
                 volume, file_id, file_id_hi, executable, hash, tags) \
                 VALUES ($F, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
                 ON CONFLICT (id, folder) DO UPDATE SET parent = ?2, name = ?3, kind = ?4, size = ?5, \
                 mtime_ns = ?6, ctime_ns = ?7, volume = ?8, file_id = ?9, file_id_hi = ?10, \
                 executable = ?11, hash = ?12, tags = ?13",
            )?
            .execute(params![
                id,
                parent,
                name,
                codec::kind_to(row.kind),
                fp.size as i64,
                fp.mtime_ns,
                fp.ctime_ns,
                fp.file_id.volume as i64,
                lo,
                hi,
                row.executable,
                row.hash.as_ref().map(codec::hash_to),
                row.tags.as_ref().map(tags_to_string),
            ])?;
        Ok(())
    }

    /// Deletes the row of `node`. A folder's contents must go first (or use
    /// [`Store::delete_local_subtree`]): rows below a deleted folder would have no path.
    pub fn delete_local(&self, node: NodeId) -> Result<bool> {
        // Forgotten after the write: the lookups before it find the old places again.
        let done = self.delete_local_rows(node);
        self.forget_local_ids();
        done
    }

    fn delete_local_rows(&self, node: NodeId) -> Result<bool> {
        let Some(id) = self.key(node)? else {
            return Ok(false);
        };
        let has_children = self
            .prep("SELECT 1 FROM local WHERE folder = $F AND parent = ?1 LIMIT 1")?
            .query_row([id], |_| Ok(()))
            .optional()?
            .is_some();
        if has_children {
            return Err(StateError::Conflict(format!(
                "{node:?} still has rows below it"
            )));
        }
        Ok(self
            .prep("DELETE FROM local WHERE folder = $F AND id = ?1")?
            .execute([id])?
            > 0)
    }

    /// The item recorded for `old` is node `new` from now on (adoption): same place, same
    /// observation, and its contents stay inside it.
    pub fn rekey_local(&self, old: NodeId, new: NodeId) -> Result<()> {
        // Forgotten after the write: the lookups before it find the old places again.
        let done = self.rekey_local_rows(old, new);
        self.forget_local_ids();
        done
    }

    fn rekey_local_rows(&self, old: NodeId, new: NodeId) -> Result<()> {
        let old_id = self
            .key(old)?
            .ok_or_else(|| StateError::Conflict(format!("{old:?} has no row")))?;
        let new_id = self.key_or_insert(new)?;
        self.prep("UPDATE local SET id = ?2 WHERE folder = $F AND id = ?1")?
            .execute([old_id, new_id])?;
        self.prep("UPDATE local SET parent = ?2 WHERE folder = $F AND parent = ?1")?
            .execute([old_id, new_id])?;
        Ok(())
    }

    /// `path` and everything below it with their ids, unsorted.
    pub(super) fn subtree_raw(&self, path: &RelPath) -> Result<Vec<(RelPath, RawLocal)>> {
        let Some(start) = self.local_id_at(path)? else {
            return Ok(Vec::new());
        };
        let mut stmt = self.prep(&format!(
            "WITH RECURSIVE sub(id, path, depth) AS ( \
                 VALUES (?1, ?2, 0) \
                 UNION ALL \
                 SELECT l.id, CASE WHEN sub.path = '' THEN l.name ELSE sub.path || '/' || l.name END, \
                 sub.depth + 1 \
                 FROM local l JOIN sub ON l.parent = sub.id \
                 WHERE l.folder = $F AND sub.depth < ?3) \
             SELECT {LOCAL_COLS}, sub.path, sub.depth FROM sub \
             JOIN local l ON l.folder = $F AND l.id = sub.id JOIN nodes n ON n.id = l.id"
        ))?;
        let rows = stmt.query_map(params![start, path.as_str(), MAX_DEPTH as i64], |r| {
            Ok((
                r.get::<_, String>(LOCAL_COL_COUNT)?,
                r.get::<_, i64>(LOCAL_COL_COUNT + 1)?,
                RawLocal::read(r, 0)?,
            ))
        })?;
        rows.map(|r| {
            let (p, depth, raw) = r?;
            if depth as usize >= MAX_DEPTH {
                return Err(StateError::Corrupt(format!(
                    "below {path}: too deep, or a cycle"
                )));
            }
            Ok((codec::rel_path(&p)?, raw))
        })
        .collect()
    }

    /// `path` and everything below it, in walk order.
    pub fn local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        let mut rows = self
            .subtree_raw(path)?
            .into_iter()
            .map(|(p, raw)| raw.into_row(p))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    /// The direct children of the folder at `parent` (the root for an empty path), in walk
    /// order.
    pub fn local_children(&self, parent: &RelPath) -> Result<Vec<LocalRow>> {
        match self.local_id_at(parent)? {
            Some(id) => self.children_of(id, parent),
            None => Ok(Vec::new()),
        }
    }

    /// The direct children of `folder` (a row read in this transaction), in walk order.
    pub fn local_children_of(&self, folder: &LocalRow) -> Result<Vec<LocalRow>> {
        match self.key(folder.node)? {
            Some(id) => self.children_of(id, &folder.path),
            None => Ok(Vec::new()),
        }
    }

    pub(super) fn children_of(&self, id: i64, path: &RelPath) -> Result<Vec<LocalRow>> {
        let raw = self.raw_local(
            &format!("SELECT {LOCAL_COLS} {LOCAL_FROM} WHERE l.parent = ?1"),
            [id],
        )?;
        let mut rows = raw
            .into_iter()
            .map(|r| {
                let child = path
                    .join(&r.name)
                    .map_err(|e| StateError::Corrupt(format!("local name {:?}: {e}", r.name)))?;
                r.into_row(child)
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    /// Moves the row at `from`, and with it everything below, to `to` (a folder was moved or
    /// renamed): one row changes. Fails if `to` is taken or its folder has no row.
    pub fn move_local_subtree(&self, from: &RelPath, to: &RelPath) -> Result<()> {
        // Forgotten after the write: the lookups before it find the old places again.
        let done = self.move_local_subtree_rows(from, to);
        self.forget_local_ids();
        done
    }

    fn move_local_subtree_rows(&self, from: &RelPath, to: &RelPath) -> Result<()> {
        let conflict = || StateError::Conflict(format!("can't move {from} to {to}"));
        if from.is_root() || to.starts_with(from) {
            return Err(conflict());
        }
        let (Some(to_parent), Some(name)) = (to.parent(), to.name()) else {
            return Err(conflict());
        };
        let id = self.local_id_at(from)?.ok_or_else(conflict)?;
        let parent = self.folder_id_for_write(&to_parent)?.ok_or_else(conflict)?;
        if self.local_id_at(to)?.is_some() {
            return Err(conflict());
        }
        self.prep("UPDATE local SET parent = ?2, name = ?3 WHERE folder = $F AND id = ?1")?
            .execute(params![id, parent, name])?;
        Ok(())
    }

    /// Deletes the rows of `path` and everything below it and returns them.
    pub fn delete_local_subtree(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        // Forgotten after the write: the lookups before it find the old places again.
        let done = self.delete_local_subtree_rows(path);
        self.forget_local_ids();
        done
    }

    fn delete_local_subtree_rows(&self, path: &RelPath) -> Result<Vec<LocalRow>> {
        let raw = self.subtree_raw(path)?;
        let mut stmt = self.prep("DELETE FROM local WHERE folder = $F AND id = ?1")?;
        for (_, r) in &raw {
            stmt.execute([r.id])?;
        }
        let mut rows = raw
            .into_iter()
            .map(|(p, r)| r.into_row(p))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(rows)
    }

    pub fn local_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM local WHERE folder = $F")
    }
}

/// A local row as stored, before its path is known.
pub(super) struct RawLocal {
    id: i64,
    uuid: Vec<u8>,
    kind: i64,
    size: i64,
    mtime_ns: i64,
    ctime_ns: i64,
    volume: i64,
    file_id: i64,
    file_id_hi: Option<i64>,
    executable: Option<bool>,
    hash: Option<Vec<u8>>,
    parent: i64,
    name: String,
    tags: Option<String>,
}

impl RawLocal {
    /// Reads [`LOCAL_COLS`] starting at column `at`.
    pub(super) fn read(r: &Row<'_>, at: usize) -> rusqlite::Result<Self> {
        Ok(RawLocal {
            id: r.get(at)?,
            uuid: r.get(at + 1)?,
            kind: r.get(at + 2)?,
            size: r.get(at + 3)?,
            mtime_ns: r.get(at + 4)?,
            ctime_ns: r.get(at + 5)?,
            volume: r.get(at + 6)?,
            file_id: r.get(at + 7)?,
            file_id_hi: r.get(at + 8)?,
            executable: r.get(at + 9)?,
            hash: r.get(at + 10)?,
            parent: r.get(at + 11)?,
            name: r.get(at + 12)?,
            tags: r.get(at + 13)?,
        })
    }

    pub(super) fn into_row(self, path: RelPath) -> Result<LocalRow> {
        Ok(LocalRow {
            path,
            node: codec::node(&self.uuid)?,
            kind: codec::kind_from(self.kind)?,
            fingerprint: codec::fingerprint_from(
                self.size,
                self.mtime_ns,
                self.ctime_ns,
                self.volume,
                self.file_id,
                self.file_id_hi,
            ),
            executable: self.executable,
            hash: self.hash.as_deref().map(codec::hash_from).transpose()?,
            tags: self.tags.as_deref().map(tags_from_str),
        })
    }
}
