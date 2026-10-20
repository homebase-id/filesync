//! Rows of the drive this client can't read as nodes (one that doesn't decrypt, a version that
//! changes a node's kind, a newer writer's), remembered as there all the same (review 7-F2,
//! F3): a folder that is one still holds what is in it, so nothing in it is an orphan; and a
//! node whose newer version is one is held: this device's writes to it would name a version
//! the drive no longer has.

use super::*;

impl ReadTx<'_> {
    /// The nodes of the rows this client can't read (their `uniqueId`, where it read).
    pub fn unreadable_nodes(&self) -> Result<std::collections::HashSet<NodeId>> {
        Ok(self
            .many(
                "SELECT node FROM unreadable WHERE node IS NOT NULL",
                [],
                |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
            )?
            .into_iter()
            .collect())
    }

    /// Whether the drive has a version of file `file_id` this client can't read: the one this
    /// device knows of it is behind.
    pub fn is_held(&self, file_id: RemoteFileId) -> Result<bool> {
        Ok(self
            .prep("SELECT 1 FROM unreadable WHERE file_id = ?1")?
            .exists([file_id.0.as_bytes()])?)
    }

    /// The kind this device knows `node` as: its mirror row's, else its row's in Recently
    /// deleted. One indexed lookup, no row decoded.
    pub fn known_kind(&self, node: NodeId) -> Result<Option<NodeKind>> {
        self.one(
            "SELECT m.kind FROM remote m JOIN nodes n ON n.id = m.id WHERE n.uuid = ?1 \
             UNION ALL SELECT kind FROM removed WHERE node = ?1 LIMIT 1",
            node.0.as_bytes(),
            |r| codec::kind_from(r.get(0)?),
        )
    }
}

impl AccountTx<'_> {
    /// Records file `at` as there at that version but not a node this client reads, with its
    /// node and folder where they read, in place of an earlier unreadable version of it; its
    /// node is planned again, where it is held now.
    pub fn put_unreadable(
        &self,
        at: RemoteRef,
        node: Option<NodeId>,
        parent: Option<NodeId>,
    ) -> Result<()> {
        self.prep(
            "INSERT OR REPLACE INTO unreadable (file_id, version_tag, node, parent) \
             VALUES (?1, ?2, ?3, ?4)",
        )?
        .execute(params![
            at.file_id.0.as_bytes(),
            at.version.0.as_bytes(),
            node.map(|n| n.0.as_bytes().to_vec()),
            parent.map(|p| p.0.as_bytes().to_vec()),
        ])?;
        if let Some(node) = node {
            self.fan_out(node, parent.unwrap_or(NodeId::ROOT))?;
        }
        Ok(())
    }

    /// File `file_id` read again, or deleted: no longer an unreadable row.
    pub(super) fn clear_unreadable(&self, file_id: RemoteFileId) -> Result<()> {
        self.prep("DELETE FROM unreadable WHERE file_id = ?1")?
            .execute([file_id.0.as_bytes()])?;
        Ok(())
    }
}
