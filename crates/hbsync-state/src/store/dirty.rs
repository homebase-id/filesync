//! The dirty set: what each folder plans again.

use super::*;
use std::collections::HashSet;

/// A dirty node as a round read it with its page ([`ReadTx::dirty_marks`]): its mark's version.
/// A change of the drive's picture moves the version of a node dirty already
/// ([`ReadTx::mark_changed_in`]): another engine's pull or an announcement may record one while
/// a folder's round plans, and the round clears only the mark it read ([`FolderTx::clear_mark`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mark {
    pub node: NodeId,
    /// `nodes.id`.
    id: i64,
    pub version: u64,
}

/// A recursive CTE `sub(id)`: the node `?1` and its subtree on any side (the base's, here,
/// the drive's), across sides (a child here of a folder in the base, say).
fn subtree_any_side() -> String {
    format!(
        "sub(id) AS (SELECT {ID_OF} \
           UNION SELECT b.id FROM base b JOIN sub ON b.parent = sub.id AND b.folder = $F \
           UNION SELECT l.id FROM local l JOIN sub ON l.parent = sub.id AND l.folder = $F \
           UNION SELECT m.id FROM remote m JOIN sub ON m.parent = sub.id)"
    )
}

impl Mark {
    /// The node's row id in the store (`nodes.id`): 8 bytes, for sets of many.
    pub fn row(&self) -> i64 {
        self.id
    }
}

/// Nodes a query of [`ReadTx::pending_among`] asks about (SQLite allows only so many
/// variables in one).
const PENDING_CHUNK: usize = 500;

/// A dirty mark from a row of `d.id, n.uuid, d.seq`.
fn mark_row(r: &Row<'_>) -> rusqlite::Result<Result<Mark>> {
    let (id, uuid, seq) = (r.get(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?);
    Ok(codec::node(&uuid).map(|node| Mark {
        node,
        id,
        version: seq as u64,
    }))
}

impl ReadTx<'_> {
    // ---- dirty set ----

    /// Marks the node with integer id `id` dirty in folder `folder`, as the folder's own engine
    /// does (its scan, its runs): a node dirty already stays as it is (no write). Between a
    /// round's read of its marks and their clearing only the engine's own rescans mark so:
    /// before it plans (what an earlier round left placed, recovered placements: the plan sees
    /// what they record), and its read of the files a plan waits for (any hash or change it
    /// records plans the page again before a mark is cleared; a read that fails records
    /// nothing new).
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

    /// Of `nodes` (a page's view outside it), those whose change is still to be planned: dirty,
    /// or with a delete held for the user out of the dirty set ([`FolderTx::hold_delete`]).
    /// A query per [`PENDING_CHUNK`] nodes, the last one padded, so its text is the same
    /// (cached).
    pub fn pending_among(&self, nodes: &[NodeId]) -> Result<HashSet<NodeId>> {
        let mut pending = HashSet::new();
        if nodes.is_empty() {
            return Ok(pending);
        }
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            let rows = vec!["(?)"; PENDING_CHUNK].join(", ");
            format!(
                "WITH q(u) AS (VALUES {rows}) SELECT DISTINCT q.u FROM q \
                 CROSS JOIN nodes n ON n.uuid = q.u \
                 WHERE EXISTS (SELECT 1 FROM dirty d WHERE d.folder = $F AND d.id = n.id) \
                 OR EXISTS (SELECT 1 FROM held_deletes h WHERE h.folder = $F AND h.id = n.id)"
            )
        });
        let mut stmt = self.prep(&SQL)?;
        for chunk in nodes.chunks(PENDING_CHUNK) {
            let pad = std::iter::repeat_n(&chunk[0], PENDING_CHUNK - chunk.len());
            let params = chunk.iter().chain(pad).map(|n| n.0.as_bytes().to_vec());
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
                r.get::<_, Vec<u8>>(0)
            })?;
            for row in rows {
                pending.insert(codec::node(&row?)?);
            }
        }
        Ok(pending)
    }

    /// `node`'s dirty mark, if it is dirty.
    pub fn dirty_mark(&self, node: NodeId) -> Result<Option<Mark>> {
        self.prep(&format!(
            "SELECT d.id, n.uuid, d.seq FROM dirty d JOIN nodes n ON n.id = d.id \
             WHERE d.folder = $F AND d.id = {ID_OF}"
        ))?
        .query_row([node.0.as_bytes()], mark_row)
        .optional()?
        .transpose()
    }

    pub fn is_dirty(&self, node: NodeId) -> Result<bool> {
        Ok(self.dirty_mark(node)?.is_some())
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
        let marks = self.many(
            "SELECT d.id, n.uuid, d.seq FROM dirty d JOIN nodes n ON n.id = d.id \
             WHERE d.folder = $F AND d.id > ?1 ORDER BY d.id LIMIT ?2",
            params![after.map_or(0, |c| c.0), limit as i64],
            |r| mark_row(r)?,
        )?;
        let last = marks.last().map(|m| DirtyCursor(m.id));
        Ok((marks, last))
    }

    /// The dirty nodes of `node`'s subtree on any side (it too: the base's, here, the drive's),
    /// each with its [`Mark`], up to `limit` (`usize::MAX`: all), in id order: what a deleted
    /// folder takes or holds, planned as a page of its own. Walks the subtree: one query.
    pub fn dirty_marks_under(&self, node: NodeId, limit: usize) -> Result<Vec<Mark>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.many(
            &format!(
                "WITH RECURSIVE {} \
                 SELECT d.id, n.uuid, d.seq FROM sub JOIN dirty d ON d.folder = $F AND d.id = sub.id \
                 JOIN nodes n ON n.id = d.id ORDER BY d.id LIMIT ?2",
                subtree_any_side()
            ),
            params![node.0.as_bytes(), limit],
            |r| mark_row(r)?,
        )
    }

    /// Up to `limit` dirty nodes in `node`'s subtree on any side, it too, in no order: what a
    /// page of its own of `node` would plan ([`ReadTx::dirty_marks_under`]; a held delete out
    /// of the dirty set is planned again only when a view reaches it, as in one page). Walks
    /// the subtree till it has them: one query.
    pub fn dirty_under(&self, node: NodeId, limit: usize) -> Result<Vec<NodeId>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.many(
            &format!(
                "WITH RECURSIVE {} \
                 SELECT n.uuid FROM sub JOIN dirty d ON d.folder = $F AND d.id = sub.id \
                 JOIN nodes n ON n.id = d.id LIMIT ?2",
                subtree_any_side()
            ),
            params![node.0.as_bytes(), limit],
            |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
        )
    }

    /// The folders directly in `folder` in the base: where a subtree too big for a page splits.
    pub fn base_child_folders(&self, folder: NodeId) -> Result<Vec<NodeId>> {
        self.many(
            &format!(
                "SELECT n.uuid FROM base b JOIN nodes n ON n.id = b.id \
                 WHERE b.folder = $F AND b.parent = {ID_OF} AND b.kind = 1"
            ),
            [folder.0.as_bytes()],
            |r| codec::node(&r.get::<_, Vec<u8>>(0)?),
        )
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

    pub fn dirty_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM dirty WHERE folder = $F")
    }

    /// The dirty nodes and the held deletes, each once: what is still to do or to answer.
    pub fn pending_count(&self) -> Result<u64> {
        Ok(self.dirty_count()?
            + self.count(
                "SELECT COUNT(*) FROM held_deletes h WHERE h.folder = $F AND NOT EXISTS \
                 (SELECT 1 FROM dirty d WHERE d.folder = $F AND d.id = h.id)",
            )?)
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
