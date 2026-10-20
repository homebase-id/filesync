//! Items parked under a temporary name, conflict copies being made, downloads being placed.

use super::*;

impl ReadTx<'_> {
    // ---- parked items ----

    /// The name a parked node had.
    pub fn get_parked(&self, node: NodeId) -> Result<Option<String>> {
        Ok(self
            .prep(&format!(
                "SELECT original_name FROM parked WHERE folder = $F AND id = {ID_OF}"
            ))?
            .query_row([node.0.as_bytes()], |r| r.get(0))
            .optional()?)
    }

    /// Every parked node.
    pub fn all_parked(&self) -> Result<Vec<NodeId>> {
        let mut stmt = self
            .prep("SELECT n.uuid FROM parked k JOIN nodes n ON n.id = k.id WHERE k.folder = $F")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    // ---- forks in progress ----

    /// `(node, new node, conflict-copy name)` of every fork in progress.
    pub fn all_forks(&self) -> Result<Vec<(NodeId, NodeId, String)>> {
        let mut stmt = self.prep(
            "SELECT n.uuid, m.uuid, f.to_name FROM forks f \
             JOIN nodes n ON n.id = f.id JOIN nodes m ON m.id = f.new_node WHERE f.folder = $F",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|r| {
            let (node, new_node, name) = r?;
            Ok((codec::node(&node)?, codec::node(&new_node)?, name))
        })
        .collect()
    }

    // ---- downloads being placed ----

    /// Every download being placed.
    pub fn all_placing(&self) -> Result<Vec<Placing>> {
        self.many(PLACING_SELECT, [], placing_row)
    }

    /// The record of `node`'s download or folder being placed, if any.
    pub fn get_placing(&self, node: NodeId) -> Result<Option<Placing>> {
        self.one(
            &format!("{PLACING_SELECT} AND k.id = {ID_OF}"),
            node.0.as_bytes(),
            placing_row,
        )
    }

    /// Whether `node` has a download or folder being placed.
    pub fn is_placing(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .prep(&format!(
                "SELECT 1 FROM placing WHERE folder = $F AND id = {ID_OF}"
            ))?
            .query_row([node.0.as_bytes()], |_| Ok(()))
            .optional()?
            .is_some())
    }
}

/// The columns of a base row, then the target and the tags put on it.
const PLACING_SELECT: &str = "SELECT n.uuid, p.uuid, k.name, k.kind, k.hash, k.size, k.mtime_ms, k.executable, \
             k.remote_file_id, k.version_tag, 0, NULL, NULL, k.tags, k.path, k.placed_tags, \
             k.base_at, k.prior_tags, k.held_ms \
             FROM placing k \
             JOIN nodes n ON n.id = k.id JOIN nodes p ON p.id = k.parent WHERE k.folder = $F";

fn placing_row(r: &Row<'_>) -> Result<Placing> {
    Ok(Placing {
        base: base_row(r)?,
        target: codec::rel_path(&r.get::<_, String>(14)?)?,
        placed_tags: r
            .get::<_, Option<String>>(15)?
            .as_deref()
            .map(tags_from_str),
        prior_tags: r
            .get::<_, Option<String>>(17)?
            .as_deref()
            .map(tags_from_str),
        base_at: match r.get::<_, Option<Vec<u8>>>(16)?.as_deref() {
            None => PlannedBase::Unchecked,
            Some([]) => PlannedBase::Absent,
            Some(v) => PlannedBase::At(codec::version(v)?),
        },
        held_ms: r.get::<_, Option<i64>>(18)?,
    })
}

impl FolderTx<'_> {
    /// Records that `node`, at `original`, is being parked under a temporary name in the same
    /// folder. Only the original name is kept: the place follows from the node's row (see the
    /// engine's `parked_place`).
    pub fn put_parked(&self, node: NodeId, original: &RelPath) -> Result<()> {
        let name = original
            .name()
            .ok_or_else(|| StateError::Conflict("the root can't be parked".into()))?;
        let id = self.key_or_insert(node)?;
        self.prep("INSERT OR REPLACE INTO parked (folder, id, original_name) VALUES ($F, ?1, ?2)")?
            .execute(params![id, name])?;
        Ok(())
    }

    pub fn delete_parked(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM parked WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }

    /// Records, before the rename, that `node`'s file becomes the conflict copy `new_node`
    /// under the name of `to`, in the same folder. Only the name is kept, so the record holds
    /// wherever the folder moves meanwhile.
    pub fn put_fork(&self, node: NodeId, new_node: NodeId, to: &RelPath) -> Result<()> {
        let name = to
            .name()
            .ok_or_else(|| StateError::Conflict("the root can't be forked".into()))?;
        let (id, new_id) = (self.key_or_insert(node)?, self.key_or_insert(new_node)?);
        self.prep(
            "INSERT OR REPLACE INTO forks (folder, id, new_node, to_name) VALUES ($F, ?1, ?2, ?3)",
        )?
        .execute(params![id, new_id, name])?;
        Ok(())
    }

    pub fn delete_fork(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM forks WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }

    /// Records, before a batch of downloads runs, the download of `p.base.node`.
    pub fn put_placing(&self, p: &Placing) -> Result<()> {
        let b = &p.base;
        let c = b.state.content.as_ref();
        let (id, parent) = (
            self.key_or_insert(b.node)?,
            self.key_or_insert(b.state.parent)?,
        );
        self.prep(
            "INSERT OR REPLACE INTO placing (folder, id, path, parent, name, kind, hash, size, \
             mtime_ms, executable, remote_file_id, version_tag, tags, placed_tags, prior_tags, \
             base_at, held_ms) \
             VALUES ($F, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        )?
        .execute(params![
            id,
            p.target.as_str(),
            parent,
            b.state.name,
            codec::kind_to(b.state.kind),
            c.map(|c| codec::hash_to(&c.hash)),
            c.map(|c| c.size as i64),
            c.map(|c| c.mtime_ms),
            c.is_some_and(|c| c.executable),
            b.remote.file_id.0.as_bytes(),
            b.remote.version.0.as_bytes(),
            (!b.state.tags.is_empty()).then(|| tags_to_string(&b.state.tags)),
            p.placed_tags.as_ref().map(tags_to_string),
            p.prior_tags.as_ref().map(tags_to_string),
            match p.base_at {
                PlannedBase::Unchecked => None,
                PlannedBase::Absent => Some(Vec::new()),
                PlannedBase::At(v) => Some(v.0.as_bytes().to_vec()),
            },
            p.held_ms,
        ])?;
        Ok(())
    }

    pub fn delete_placing(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM placing WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }
}
