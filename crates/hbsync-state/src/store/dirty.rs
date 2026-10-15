//! The dirty set: what each folder plans again.

use super::*;

impl Store<'_> {
    // ---- dirty set ----

    pub fn mark_dirty(&self, node: NodeId) -> Result<()> {
        self.mark_dirty_in(self.folder, self.key_or_insert(node)?)
    }

    /// Marks the node with integer id `id` dirty in folder `folder`.
    pub(super) fn mark_dirty_in(&self, folder: i64, id: i64) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO dirty (folder, id) VALUES (?1, ?2)")?
            .execute([folder, id])?;
        Ok(())
    }

    /// Marks every node with a base or a remote row dirty: all of them are planned again.
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

    pub fn clear_dirty(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM dirty WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
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

    /// Up to `limit` dirty nodes in id order, starting after `after`, and the cursor for the
    /// next page: successive pages visit the whole set, so nodes that stay dirty can't keep the
    /// others waiting. Ids are never reused, so a cursor stays valid whatever happens to its
    /// node.
    pub fn dirty_page(
        &self,
        after: Option<DirtyCursor>,
        limit: usize,
    ) -> Result<(Vec<NodeId>, Option<DirtyCursor>)> {
        let mut stmt = self.prep(
            "SELECT d.id, n.uuid FROM dirty d JOIN nodes n ON n.id = d.id \
             WHERE d.folder = $F AND d.id > ?1 ORDER BY d.id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after.map_or(0, |c| c.0), limit as i64], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let mut nodes = Vec::new();
        let mut last = None;
        for row in rows {
            let (id, uuid) = row?;
            nodes.push(codec::node(&uuid)?);
            last = Some(DirtyCursor(id));
        }
        Ok((nodes, last))
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
