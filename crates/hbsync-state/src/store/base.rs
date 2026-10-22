//! The base: each folder's last synced state of its nodes.

use super::*;

impl ReadTx<'_> {
    // ---- base ----

    pub fn get_base(&self, node: NodeId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("{BASE_SELECT} WHERE n.uuid = ?1"),
            node.0.as_bytes(),
            base_row,
        )
    }

    pub fn base_by_remote_file(&self, file_id: RemoteFileId) -> Result<Option<BaseRow>> {
        self.one(
            &format!("{BASE_SELECT} WHERE b.remote_file_id = ?1"),
            file_id.0.as_bytes(),
            base_row,
        )
    }

    /// The node a base row of any folder records for remote file `file_id`.
    pub fn node_of_file(&self, file_id: RemoteFileId) -> Result<Option<NodeId>> {
        let node: Option<Vec<u8>> = self
            .prep(
                "SELECT n.uuid FROM base b JOIN nodes n ON n.id = b.id \
                 WHERE b.remote_file_id = ?1 LIMIT 1",
            )?
            .query_row([file_id.0.as_bytes()], |r| r.get(0))
            .optional()?;
        node.as_deref().map(codec::node).transpose()
    }

    pub fn base_children(&self, parent: NodeId) -> Result<Vec<BaseRow>> {
        self.many(
            &format!("{BASE_SELECT} WHERE b.parent = {ID_OF}"),
            [parent.0.as_bytes()],
            base_row,
        )
    }

    /// The children of `parent` whose name has sibling hash `key`
    /// ([`hbsync_namemap::sibling_hash`]): every one that may clash with a name of that hash.
    pub fn base_named(&self, parent: NodeId, key: i32) -> Result<Vec<BaseRow>> {
        self.many(
            &format!("{BASE_SELECT} WHERE b.parent = {ID_OF} AND b.name_key = ?2"),
            params![parent.0.as_bytes(), key],
            base_row,
        )
    }

    pub fn base_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM base WHERE folder = $F")
    }

    /// Number of file (not folder) nodes in the base; the mass-delete guard's denominator.
    /// One row: the triggers on `base` keep it (a file's base row in or out costs a row update).
    pub fn base_file_count(&self) -> Result<u64> {
        let n: i64 = self
            .prep("SELECT coalesce((SELECT n FROM base_files WHERE folder = $F), 0)")?
            .query_row([], |r| r.get(0))?;
        // Below zero would be a count gone wrong: never read as a huge one (no guard at all).
        let n = u64::try_from(n)
            .map_err(|_| StateError::Corrupt(format!("base_files counts {n} files")))?;
        // Debug builds (every test) check it against the rows it stands for.
        debug_assert_eq!(
            Some(n),
            self.count("SELECT COUNT(*) FROM base WHERE +folder = $F AND kind = 0")
                .ok(),
            "base_files drifted from the base"
        );
        Ok(n)
    }
}

impl FolderTx<'_> {
    /// Inserts or replaces the base row of `row.node`.
    pub fn put_base(&self, row: &BaseRow) -> Result<()> {
        let c = row.state.content.as_ref();
        let (id, parent) = (
            self.key_or_insert(row.node)?,
            self.key_or_insert(row.state.parent)?,
        );
        self.prep(
                "INSERT INTO base (folder, id, parent, name, kind, hash, size, mtime_ms, \
                 executable, remote_file_id, version_tag, content_state, local_name, local_hash, \
                 tags, name_key) \
                 VALUES ($F, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) \
                 ON CONFLICT (id, folder) DO UPDATE SET parent = ?2, name = ?3, kind = ?4, hash = ?5, \
                 size = ?6, mtime_ms = ?7, executable = ?8, remote_file_id = ?9, \
                 version_tag = ?10, content_state = ?11, local_name = ?12, local_hash = ?13, \
                 tags = ?14, name_key = ?15",
            )?
            .execute(params![
                id,
                parent,
                row.state.name,
                codec::kind_to(row.state.kind),
                c.map(|c| codec::hash_to(&c.hash)),
                c.map(|c| c.size as i64),
                c.map(|c| c.mtime_ms),
                c.is_some_and(|c| c.executable),
                row.remote.file_id.0.as_bytes(),
                row.remote.version.0.as_bytes(),
                codec::content_state_to(row.content_state),
                row.local_name,
                row.local_hash.as_ref().map(codec::hash_to),
                (!row.state.tags.is_empty()).then(|| tags_to_string(&row.state.tags)),
                sibling_hash(&row.state.name),
            ])?;
        Ok(())
    }

    pub fn delete_base(&self, node: NodeId) -> Result<bool> {
        self.change(
            &format!("DELETE FROM base WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )
    }
}
