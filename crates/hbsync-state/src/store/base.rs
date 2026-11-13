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
    /// A scan of the base (every folder's rows), which a read keeps for the connection
    /// (`FileCount`): used again while no other connection committed and this one wrote no
    /// base row. A write counts afresh.
    pub fn base_file_count(&self) -> Result<u64> {
        let count = || self.count("SELECT COUNT(*) FROM base WHERE folder = $F AND kind = 0");
        let Some(files) = self.files else {
            return count();
        };
        let version = self.data_version()?;
        if let Some(n) = files.at(version) {
            // Debug builds (every test) check it against the rows it stands for.
            debug_assert_eq!(count().ok(), Some(n), "the base file count drifted");
            return Ok(n);
        }
        let n = count()?;
        files.keep(version, n);
        Ok(n)
    }

    /// This write wrote a base row of the folder.
    pub(super) fn wrote_base(&self) {
        self.base_written.set(true);
    }

    /// Whether this write wrote a base row of the folder (`FileCount::forget`).
    pub(crate) fn base_written(&self) -> bool {
        self.base_written.get()
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
        self.wrote_base();
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
        self.wrote_base();
        self.change(
            &format!("DELETE FROM base WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )
    }

    /// Forgets `node`'s base (both sides agreed on it, no more): planned again, from what each
    /// side has now (Put them back: an item comes down again, or goes up; a detached node,
    /// synced here no more).
    pub fn forget_base(&self, node: NodeId) -> Result<()> {
        self.delete_base(node)?;
        self.mark_dirty(node)
    }
}
