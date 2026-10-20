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

    /// The files the drive has in a version this client can't read (held, where it knows an
    /// older one).
    pub fn held_files(&self) -> Result<std::collections::HashSet<RemoteFileId>> {
        Ok(self
            .many("SELECT file_id FROM unreadable", [], |r| {
                Ok(RemoteFileId(codec::uuid_from(&r.get::<_, Vec<u8>>(0)?)?))
            })?
            .into_iter()
            .collect())
    }

    /// The version this device knows of file `file_id`, its kind, and whether it is in Recently
    /// deleted: one lookup by file id.
    pub fn known_file(
        &self,
        file_id: RemoteFileId,
    ) -> Result<Option<(VersionTag, NodeKind, bool)>> {
        self.one(
            "SELECT version_tag, kind, 0 FROM remote WHERE file_id = ?1 \
             UNION ALL SELECT version_tag, kind, 1 FROM removed WHERE file_id = ?1 LIMIT 1",
            file_id.0.as_bytes(),
            |r| {
                Ok((
                    codec::version(&r.get::<_, Vec<u8>>(0)?)?,
                    codec::kind_from(r.get(1)?)?,
                    r.get(2)?,
                ))
            },
        )
    }

    /// Whether the store has no unreadable row, read once per transaction.
    fn none_unreadable(&self) -> Result<bool> {
        if let Some(none) = self.no_unreadable.get() {
            return Ok(none);
        }
        let none = !self.prep("SELECT 1 FROM unreadable")?.exists([])?;
        self.no_unreadable.set(Some(none));
        Ok(none)
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
        self.no_unreadable.set(Some(false));
        if let Some(node) = node {
            self.fan_out(node, parent.unwrap_or(NodeId::ROOT))?;
        }
        Ok(())
    }

    /// A new row of file `file_id`, or its delete: it is there (or gone), whatever the identity
    /// said once, and no longer a row this client can't read.
    pub(super) fn clear_file_marks(&self, file_id: RemoteFileId) -> Result<()> {
        self.clear_gone_suspect(file_id)?;
        if !self.none_unreadable()? {
            self.prep("DELETE FROM unreadable WHERE file_id = ?1")?
                .execute([file_id.0.as_bytes()])?;
        }
        Ok(())
    }
}
