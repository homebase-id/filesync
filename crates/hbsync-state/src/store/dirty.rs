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

/// Live node `m` was written by the time of removal `r` (`along::taken_along`): its time or the
/// removal's not known yet (this device's own write, an open removal) counts as written before.
const WRITTEN_BY_REMOVAL: &str =
    "(r.removed_at IS NULL OR m.changed_at IS NULL OR m.changed_at <= r.removed_at)";
/// [`WRITTEN_BY_REMOVAL`] below a node `t` taken along by the removal at `t.at`.
const WRITTEN_BY_TAKEN: &str = "(t.at IS NULL OR m.changed_at IS NULL OR m.changed_at <= t.at)";

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

/// A recursive CTE `taken(id, at)`: the live nodes a folder's removal took along
/// (`along::taken_along`), each with the time of the removal that took it.
fn taken_along() -> String {
    format!(
        "taken(id, at) AS ( \
           SELECT m.id, r.removed_at FROM removed r JOIN nodes n ON n.uuid = r.node \
           JOIN remote m ON m.parent = n.id \
           WHERE r.kind = 1 AND {WRITTEN_BY_REMOVAL} \
           UNION SELECT m.id, t.at FROM remote m JOIN taken t ON m.parent = t.id \
           WHERE {WRITTEN_BY_TAKEN})"
    )
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

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be trashed locally). Online-only files have
    /// no local copy by design and don't count. Used by the mass-delete guard, which must see
    /// the whole delete, not one planning batch: with [`ReadTx::held_file_deletes`].
    pub fn pending_file_deletes(&self) -> Result<(u64, u64)> {
        // From the dirty set (CROSS JOIN keeps it outer): not a scan of every base row.
        let (here, there) = self.file_deletes("dirty d WHERE d.folder = $F")?;
        Ok((here, there + self.taken_files()?))
    }

    /// Synced files still live below a folder in Recently deleted that its removal took along
    /// (a delete stopped half way, §6.3; the rule of `along::taken_along`: written no later
    /// than the removal of the nearest removed folder above, or at a time not known yet),
    /// dirty or not (a folder's removal marks only itself), gone remotely as the guard counts,
    /// also where planned in pages apart from their folder. One query, nothing read while
    /// nothing is dirty (no plan to judge): the removed folders, and what is live below them
    /// (hardly ever any).
    fn taken_files(&self) -> Result<u64> {
        if self.dirty_count()? == 0 {
            return Ok(0);
        }
        self.count(&format!(
            "WITH RECURSIVE {} \
             SELECT COUNT(DISTINCT t.id) FROM taken t JOIN base b ON b.folder = $F AND b.id = t.id \
             WHERE b.kind = 0",
            taken_along()
        ))
    }

    /// [`ReadTx::pending_file_deletes`] of the nodes of held deletes out of the dirty set
    /// ([`FolderTx::hold_delete`]), but those dirty again.
    pub fn held_file_deletes(&self) -> Result<(u64, u64)> {
        self.file_deletes(
            "held_deletes d WHERE d.folder = $F AND NOT EXISTS \
             (SELECT 1 FROM dirty k WHERE k.folder = $F AND k.id = d.id)",
        )
    }

    /// Synced files among the nodes `from` (`<table> d WHERE ...`) that are gone locally, and
    /// those gone remotely, in one pass.
    fn file_deletes(&self, from: &str) -> Result<(u64, u64)> {
        let sql = format!(
            "SELECT coalesce(sum({GONE_LOCALLY}), 0), coalesce(sum({GONE_REMOTELY}), 0) \
             FROM (SELECT d.id FROM {from}) d \
             CROSS JOIN base b ON b.folder = $F AND b.id = d.id"
        );
        Ok(self.prep(&sql)?.query_row([], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64))
        })?)
    }

    /// What a confirmed delete of dirty `node` covers of the
    /// [`pending_file_deletes`](Self::pending_file_deletes): whether `node` is a file gone
    /// locally (deleted remotely, one node at a time), and the files gone remotely in its local
    /// subtree (a trash takes everything under it along): the dirty ones gone from the drive,
    /// and the synced ones a folder's removal took along, still live there (as `taken_files`
    /// counts them). `None` when `node` isn't dirty.
    pub fn confirmed_file_deletes(&self, node: NodeId) -> Result<Option<(bool, Vec<NodeId>)>> {
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "WITH RECURSIVE top(id) AS (SELECT {ID_OF}), \
                 sub(id) AS (SELECT id FROM top \
                   UNION SELECT l.id FROM local l JOIN sub ON l.parent = sub.id \
                   WHERE l.folder = $F), {} \
                 SELECT n.uuid, k.id IS NOT NULL AND d.id = (SELECT id FROM top), \
                   k.id IS NOT NULL AND coalesce({GONE_LOCALLY}, 0), \
                   coalesce(k.id IS NOT NULL AND {GONE_REMOTELY}, 0) \
                     OR coalesce(b.kind = 0 AND EXISTS (SELECT 1 FROM taken t WHERE t.id = d.id), 0) \
                 FROM (SELECT id FROM sub) d LEFT JOIN dirty k ON k.folder = $F AND k.id = d.id \
                 JOIN nodes n ON n.id = d.id LEFT JOIN base b ON b.folder = $F AND b.id = d.id",
                taken_along()
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

    /// SQLite's `data_version` of this connection: it changes when another connection (another
    /// folder's engine, the account's pull of the feed or an announcement) commits, so what was
    /// counted before may be stale.
    pub fn data_version(&self) -> Result<u64> {
        self.count("PRAGMA data_version")
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

    /// How many nodes of held deletes are out of the dirty set ([`FolderTx::hold_delete`]).
    pub fn held_delete_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM held_deletes WHERE folder = $F")
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

    /// Takes `mark`'s node, whose delete the mass-delete guard holds for the user (sync
    /// semantics §7), out of the dirty set until [`FolderTx::unhold_deletes`]: not planned
    /// again meanwhile. Unless the mark moved on since ([`FolderTx::clear_mark`]): then it is
    /// both, and planned.
    pub fn hold_delete(&self, mark: Mark) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO held_deletes (folder, id) VALUES ($F, ?1)")?
            .execute([mark.id])?;
        self.clear_mark(mark)
    }

    /// Puts every held delete's node back into the dirty set, to be planned again. Returns how
    /// many there were.
    pub fn unhold_deletes(&self) -> Result<usize> {
        self.prep(
            "INSERT OR IGNORE INTO dirty (folder, id) SELECT folder, id FROM held_deletes \
             WHERE folder = $F",
        )?
        .execute([])?;
        Ok(self
            .prep("DELETE FROM held_deletes WHERE folder = $F")?
            .execute([])?)
    }

    pub fn clear_dirty(&self, node: NodeId) -> Result<()> {
        self.change(
            &format!("DELETE FROM dirty WHERE folder = $F AND id = {ID_OF}"),
            node.0.as_bytes(),
        )?;
        Ok(())
    }
}
