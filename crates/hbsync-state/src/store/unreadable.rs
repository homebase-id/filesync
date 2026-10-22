//! Rows of the drive this client can't read as nodes (one that doesn't decrypt, a version that
//! changes a node's kind, a newer writer's), remembered as there all the same (review 7-F2,
//! F3): a folder that is one still holds what is in it, so nothing in it is an orphan; and a
//! node whose newer version is one is unreadable, kept as this device had it: its writes to
//! it would name a version the drive no longer has.

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

    /// Whether version `at` is one this client recorded it can't read (none, mostly: no query
    /// then).
    pub fn is_unreadable_version(&self, at: RemoteRef) -> Result<bool> {
        if self.none_unreadable()? {
            return Ok(false);
        }
        Ok(self
            .prep("SELECT 1 FROM unreadable WHERE file_id = ?1 AND version_tag = ?2")?
            .exists([at.file_id.0.as_bytes(), at.version.0.as_bytes()])?)
    }

    /// The version of file `file_id` this client recorded it can't read, if any (none, mostly:
    /// no query then).
    pub fn unreadable_version(&self, file_id: RemoteFileId) -> Result<Option<VersionTag>> {
        if self.none_unreadable()? {
            return Ok(None);
        }
        self.one(
            "SELECT version_tag FROM unreadable WHERE file_id = ?1",
            file_id.0.as_bytes(),
            |r| codec::version(&r.get::<_, Vec<u8>>(0)?),
        )
    }

    /// The files the drive has in a version this client can't read (unreadable: kept as it had
    /// them, where it knows an older version).
    pub fn unreadable_files(&self) -> Result<std::collections::HashSet<RemoteFileId>> {
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
    /// node and folder where they read (never the root's id, nor the rest of the drive's), in
    /// place of an earlier unreadable version of it; its node is planned again, where it is
    /// kept now. A file deleted for good (a page fetched before), a version recorded already,
    /// or the very version this device has read (a client with a stricter reader since: it
    /// isn't behind) changes nothing.
    pub fn put_unreadable(
        &self,
        at: RemoteRef,
        node: Option<NodeId>,
        parent: Option<NodeId>,
    ) -> Result<()> {
        if self.file_gone(at.file_id)?
            || self.is_unreadable_version(at)?
            || self
                .known_file(at.file_id)?
                .is_some_and(|(v, ..)| v == at.version)
        {
            return Ok(());
        }
        let fixed = |n: &NodeId| n.is_root() || *n == NodeId::OUTSIDE;
        let node = node.filter(|n| !fixed(n));
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
    /// said once, and no longer a row this client can't read. What waited in it is planned
    /// again (a delete for good leaves it an orphan, for Recovered files).
    pub(super) fn clear_file_marks(&self, file_id: RemoteFileId) -> Result<()> {
        self.clear_gone_suspect(file_id)?;
        if self.none_unreadable()? {
            return Ok(());
        }
        let node = self.one(
            "DELETE FROM unreadable WHERE file_id = ?1 RETURNING node",
            file_id.0.as_bytes(),
            |r| {
                r.get::<_, Option<Vec<u8>>>(0)?
                    .map(|n| codec::node(&n))
                    .transpose()
            },
        )?;
        if let Some(node) = node.flatten() {
            self.fan_out(node, NodeId::ROOT)?;
            for child in self.mirror_children(node)? {
                self.fan_out(child.node, node)?;
            }
        }
        Ok(())
    }
}
