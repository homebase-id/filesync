//! The dirty set: what each folder plans again.

use super::*;

/// A dirty node as a round read it with its page ([`ReadTx::dirty_marks`]): its mark's version.
/// A change of the drive's picture moves the version of a node dirty already
/// ([`ReadTx::mark_changed_in`]): another engine's pull or an announcement may record one while
/// a folder's round plans, and the round clears only the mark it read ([`FolderTx::clear_mark`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark {
    pub node: NodeId,
    /// `nodes.id`.
    id: i64,
    pub version: u64,
}

impl ReadTx<'_> {
    // ---- dirty set ----

    /// Marks the node with integer id `id` dirty in folder `folder`, as the folder's own engine
    /// does (its scan, its runs): never while its own round plans, so a node dirty already stays
    /// as it is (no write).
    pub(super) fn mark_dirty_in(&self, folder: i64, id: i64) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO dirty (folder, id) VALUES (?1, ?2)")?
            .execute([folder, id])?;
        Ok(())
    }

    /// [`ReadTx::mark_dirty_in`] for a change of the drive's picture (`fan_out`), which may
    /// come while the folder's round plans: a node dirty already has its mark's version moved
    /// on (a row written), so that round doesn't clear it.
    pub(super) fn mark_changed_in(&self, folder: i64, id: i64) -> Result<()> {
        self.prep(
            "INSERT INTO dirty (folder, id) VALUES (?1, ?2) \
             ON CONFLICT (id, folder) DO UPDATE SET seq = seq + 1",
        )?
        .execute([folder, id])?;
        Ok(())
    }

    /// Whether a removal elsewhere is still to be followed at `node` or below it in the base:
    /// a synced item still here (in the base and the local table) that the drive no longer
    /// has. Walks `node`'s base subtree: for the mass-delete guard's held removals, which the
    /// engine checks this way only while the drive has `node` again.
    pub fn removal_pending_under(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .prep(&format!(
                "WITH RECURSIVE sub(id) AS (SELECT id FROM base WHERE folder = $F AND id = {ID_OF} \
                   UNION SELECT b.id FROM base b JOIN sub ON b.parent = sub.id AND b.folder = $F) \
                 SELECT 1 FROM sub JOIN local l ON l.folder = $F AND l.id = sub.id \
                 WHERE NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = sub.id) LIMIT 1"
            ))?
            .query_row([node.0.as_bytes()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    pub fn is_dirty(&self, node: NodeId) -> Result<bool> {
        Ok(self
            .prep(&format!(
                "SELECT 1 FROM dirty WHERE folder = $F AND id = {ID_OF}"
            ))?
            .query_row([node.0.as_bytes()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Up to `limit` dirty nodes in id order, starting after `after`, each with its [`Mark`],
    /// and the cursor for the next page: successive pages visit the whole set, so nodes that
    /// stay dirty can't keep the others waiting. Ids are never reused, so a cursor stays valid
    /// whatever happens to its node.
    pub fn dirty_marks(
        &self,
        after: Option<DirtyCursor>,
        limit: usize,
    ) -> Result<(Vec<Mark>, Option<DirtyCursor>)> {
        let mut stmt = self.prep(
            "SELECT d.id, n.uuid, d.seq FROM dirty d JOIN nodes n ON n.id = d.id \
             WHERE d.folder = $F AND d.id > ?1 ORDER BY d.id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after.map_or(0, |c| c.0), limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        let mut marks = Vec::new();
        for row in rows {
            let (id, uuid, seq) = row?;
            marks.push(Mark {
                node: codec::node(&uuid)?,
                id,
                version: seq as u64,
            });
        }
        let last = marks.last().map(|m| DirtyCursor(m.id));
        Ok((marks, last))
    }

    /// Local rows of dirty nodes that have neither a base nor a mirror row: what adoption may
    /// match against remote nodes.
    pub fn unpaired_local(&self) -> Result<Vec<LocalRow>> {
        let raw = self.raw_local(
            &format!(
                "SELECT {LOCAL_COLS} {LOCAL_FROM} \
                 WHERE l.id IN (SELECT id FROM dirty WHERE folder = $F) \
                 AND NOT EXISTS (SELECT 1 FROM base b WHERE b.folder = $F AND b.id = l.id) \
                 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = l.id)"
            ),
            [],
        )?;
        self.with_paths(raw)
    }

    pub fn dirty_nodes(&self, limit: usize) -> Result<Vec<NodeId>> {
        let mut stmt = self.prep(
            "SELECT n.uuid FROM dirty d JOIN nodes n ON n.id = d.id WHERE d.folder = $F LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| codec::node(&r?)).collect()
    }

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be trashed locally). Online-only files have
    /// no local copy by design and don't count. Used by the mass-delete guard, which must see
    /// the whole delete, not one planning batch.
    pub fn pending_file_deletes(&self) -> Result<(u64, u64)> {
        let gone = |predicate: &str| {
            self.count(&format!(
                // From the dirty set (CROSS JOIN keeps it outer): not a scan of every base row.
                "SELECT COUNT(*) FROM dirty d CROSS JOIN base b ON b.folder = d.folder AND b.id = d.id \
                 WHERE d.folder = $F AND {predicate}"
            ))
        };
        Ok((gone(GONE_LOCALLY)?, gone(GONE_REMOTELY)?))
    }

    /// What a confirmed delete of dirty `node` covers of the
    /// [`pending_file_deletes`](Self::pending_file_deletes): whether `node` is a file gone
    /// locally (deleted remotely, one node at a time), and the files gone remotely in its local
    /// subtree (a trash takes everything under it along). `None` when `node` isn't dirty.
    pub fn confirmed_file_deletes(&self, node: NodeId) -> Result<Option<(bool, Vec<NodeId>)>> {
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "WITH RECURSIVE top(id) AS (SELECT {ID_OF}), \
                 sub(id) AS (SELECT id FROM top \
                   UNION SELECT l.id FROM local l JOIN sub ON l.parent = sub.id \
                   WHERE l.folder = $F) \
                 SELECT n.uuid, d.id = (SELECT id FROM top), \
                   coalesce({GONE_LOCALLY}, 0), coalesce({GONE_REMOTELY}, 0) \
                 FROM sub JOIN dirty d ON d.folder = $F AND d.id = sub.id \
                 JOIN nodes n ON n.id = d.id LEFT JOIN base b ON b.folder = $F AND b.id = d.id"
            )
        });
        let mut stmt = self.prep(&SQL)?;
        let rows = stmt.query_map([node.0.as_bytes()], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, bool>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, bool>(3)?,
            ))
        })?;
        let (mut dirty, mut gone_here, mut gone_there) = (false, false, Vec::new());
        for row in rows {
            let (uuid, top, here, there) = row?;
            if top {
                dirty = true;
                gone_here = here;
            }
            if there {
                gone_there.push(codec::node(&uuid)?);
            }
        }
        Ok(dirty.then_some((gone_here, gone_there)))
    }

    pub fn dirty_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM dirty WHERE folder = $F")
    }
}

impl FolderTx<'_> {
    pub fn mark_dirty(&self, node: NodeId) -> Result<()> {
        self.mark_dirty_in(self.folder, self.key_or_insert(node)?)
    }

    /// Marks every node of this folder dirty: its base rows, and the mirror's nodes in its part
    /// of the drive (below its top; the whole drive for the main folder).
    pub fn mark_synced_dirty(&self) -> Result<()> {
        let top = self.coords()?.top;
        self.prep(
            "INSERT OR IGNORE INTO dirty (folder, id) SELECT $F, id FROM base WHERE folder = $F",
        )?
        .execute([])?;
        if top.is_root() {
            self.prep("INSERT OR IGNORE INTO dirty (folder, id) SELECT $F, id FROM remote")?
                .execute([])?;
        } else {
            self.prep(&format!(
                "WITH RECURSIVE sub(id) AS (SELECT id FROM nodes WHERE uuid = ?1 \
                 UNION SELECT r.id FROM remote r JOIN sub ON r.parent = sub.id) \
                 INSERT OR IGNORE INTO dirty (folder, id) \
                 SELECT $F, id FROM sub WHERE id != {ID_OF}"
            ))?
            .execute([top.0.as_bytes()])?;
        }
        Ok(())
    }

    /// Clears `mark`, its node's dirty mark as a round read it, unless the mark moved on since
    /// (a change of the drive's picture recorded meanwhile): that one stays, for the next.
    pub fn clear_mark(&self, mark: Mark) -> Result<()> {
        self.prep("DELETE FROM dirty WHERE folder = $F AND id = ?1 AND seq = ?2")?
            .execute(params![mark.id, mark.version as i64])?;
        Ok(())
    }

    pub fn clear_dirty(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM dirty WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }
}
