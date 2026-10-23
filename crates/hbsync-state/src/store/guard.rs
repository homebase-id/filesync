//! What the mass-delete guard reads and keeps (sync semantics §7): the deletes a sync would
//! send or make, counted whole whatever page plans them (with what a folder's removal took
//! along), what a confirmed delete covers, and the held deletes out of the dirty set.

use super::*;

/// A dirty node `d` with base `b` is a synced file gone locally (online-only files have no
/// local copy by design), or gone remotely: what the mass-delete guard counts.
const GONE_LOCALLY: &str = "b.kind = 0 AND b.content_state != 2 \
    AND NOT EXISTS (SELECT 1 FROM local l WHERE l.folder = $F AND l.id = d.id)";
const GONE_REMOTELY: &str = "b.kind = 0 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = d.id)";

/// Live node `m` was written by the time of removal `r` (`along::taken_along`): its time or the
/// removal's not known yet (this device's own write, an open removal) counts as written before.
const WRITTEN_BY_REMOVAL: &str =
    "(r.removed_at IS NULL OR m.changed_at IS NULL OR m.changed_at <= r.removed_at)";
/// [`WRITTEN_BY_REMOVAL`] below a node `t` taken along by the removal at `t.at`.
const WRITTEN_BY_TAKEN: &str = "(t.at IS NULL OR m.changed_at IS NULL OR m.changed_at <= t.at)";

/// A recursive CTE `taken(id, at, kind)`: the live nodes a folder's removal took along
/// (`along::taken_along`), each with the time of the removal that took it: below the removed
/// folders `n` that `seeds` lets in, through live folders (a file takes nothing along).
fn taken_along(seeds: &str) -> String {
    format!(
        "taken(id, at, kind) AS ( \
           SELECT m.id, r.removed_at, m.kind FROM removed r JOIN nodes n ON n.uuid = r.node \
           JOIN remote m ON m.parent = n.id \
           WHERE r.kind = 1 AND {WRITTEN_BY_REMOVAL} AND {seeds} \
           UNION SELECT m.id, t.at, m.kind FROM remote m JOIN taken t ON m.parent = t.id \
           WHERE t.kind = 1 AND {WRITTEN_BY_TAKEN})"
    )
}

impl ReadTx<'_> {
    /// SQLite's `data_version` of this connection: it changes when another connection (another
    /// folder's engine, the account's pull of the feed or an announcement) commits, so what was
    /// counted before may be stale (the guard counts again).
    pub fn data_version(&self) -> Result<u64> {
        self.count("PRAGMA data_version")
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

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be deleted locally). Online-only files have
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
        self.count(&format!(
            "WITH RECURSIVE {} \
             SELECT COUNT(DISTINCT t.id) FROM taken t JOIN base b ON b.folder = $F AND b.id = t.id \
             WHERE b.kind = 0",
            taken_along("EXISTS (SELECT 1 FROM dirty WHERE folder = $F)")
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
    /// subtree (a delete takes everything under it along): the dirty ones gone from the drive,
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
                taken_along("n.id IN (SELECT id FROM sub)")
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

    /// How many nodes of held deletes are out of the dirty set ([`FolderTx::hold_delete`]).
    pub fn held_delete_count(&self) -> Result<u64> {
        self.count("SELECT COUNT(*) FROM held_deletes WHERE folder = $F")
    }
}

impl FolderTx<'_> {
    /// Takes `mark`'s node, whose delete the mass-delete guard holds for the user (sync
    /// semantics §7), out of the dirty set until [`FolderTx::unhold_deletes`]: not planned
    /// again meanwhile. Unless the mark moved on since ([`FolderTx::clear_mark`]): then it is
    /// both, and planned.
    pub fn hold_delete(&self, mark: Mark) -> Result<()> {
        self.prep("INSERT OR IGNORE INTO held_deletes (folder, id) VALUES ($F, ?1)")?
            .execute([mark.row()])?;
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
}
