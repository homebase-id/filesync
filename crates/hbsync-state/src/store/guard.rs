//! What the mass-delete guard reads and keeps (sync semantics §7): the deletes a sync would
//! send or make, counted whole whatever page plans them (with what a folder's removal took
//! along), what a confirmed delete covers, and the held deletes out of the dirty set.

use super::*;

/// A dirty node `d` with base `b` is a synced file gone locally (but where the base keeps its
/// absence, [`ContentState::keeps_absence`]: online-only, 2, no local copy by design; wanted, 3,
/// its item gone into a conflict copy), or gone remotely: what the mass-delete guard counts.
const GONE_LOCALLY: &str = "b.kind = 0 AND b.content_state NOT IN (2, 3) \
    AND NOT EXISTS (SELECT 1 FROM local l WHERE l.folder = $F AND l.id = d.id)";
const GONE_REMOTELY: &str = "b.kind = 0 AND NOT EXISTS (SELECT 1 FROM remote m WHERE m.id = d.id)";

/// Node `d` is marked in this folder: dirty, or a held delete out of the dirty set.
const MARKED: &str = "(EXISTS (SELECT 1 FROM dirty md WHERE md.folder = $F AND md.id = d.id) \
    OR EXISTS (SELECT 1 FROM held_deletes mh WHERE mh.folder = $F AND mh.id = d.id))";

/// A file `d` taken along (with base `b`, if synced) that this device removes from the drive,
/// finishing the removal ([`ReadTx::taken_files`]): one synced (but one marked and gone
/// locally, counted already), or one not synced but marked here.
fn removes_taken() -> String {
    format!(
        "CASE WHEN b.id IS NULL THEN {MARKED} \
         ELSE b.kind = 0 AND NOT ({MARKED} AND coalesce({GONE_LOCALLY}, 0)) END"
    )
}

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

    /// Synced files among the dirty nodes that are gone locally (and would be deleted
    /// remotely), and those gone remotely (would be deleted locally), with what a folder's
    /// removal took along ([`ReadTx::taken_files`]) on both sides. Online-only files have no
    /// local copy by design and don't count. Used by the mass-delete guard, which must see the
    /// whole delete, not one planning batch: with [`ReadTx::held_file_deletes`].
    pub fn pending_file_deletes(&self) -> Result<(u64, u64)> {
        // From the dirty set (CROSS JOIN keeps it outer): not a scan of every base row.
        let (here, there) = self.file_deletes("dirty d WHERE d.folder = $F")?;
        let (taken_here, taken_there) = self.taken_files()?;
        Ok((here + taken_here, there + taken_there))
    }

    /// Files still live below a folder in Recently deleted that its removal took along (a
    /// delete stopped half way, §6.3; the rule of `along::taken_along`: written no later than
    /// the removal of the nearest removed folder above, or at a time not known yet), dirty or
    /// not (a folder's removal marks only itself), also where planned in pages apart from their
    /// folder, as the guard counts them: (gone here, gone there).
    /// - Gone here: those this device removes from the drive (finishing the removal), so pages
    ///   weigh them with the files gone here as one plan would. That is a synced one, but one
    ///   marked here ([`MARKED`]) and gone locally (counted already, here or in
    ///   [`ReadTx::held_file_deletes`]); or one not synced but marked here (the feed marked it,
    ///   or its base went with this device's delete of that folder while its removal from the
    ///   drive was held). One never synced nor marked here, as what another folder's part of
    ///   the drive holds, isn't.
    /// - Gone there: the synced ones.
    ///
    /// One query, nothing read while nothing is dirty (no plan to judge): the removed folders,
    /// and what is live below them (hardly ever any). Each node is taken once (one parent).
    fn taken_files(&self) -> Result<(u64, u64)> {
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "WITH RECURSIVE {} \
                 SELECT coalesce(sum(d.kind = 0 AND {}), 0), coalesce(sum(b.kind = 0), 0) \
                 FROM taken d LEFT JOIN base b ON b.folder = $F AND b.id = d.id",
                taken_along("EXISTS (SELECT 1 FROM dirty WHERE folder = $F)"),
                removes_taken()
            )
        });
        self.count2(&SQL)
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
        self.count2(&format!(
            "SELECT coalesce(sum({GONE_LOCALLY}), 0), coalesce(sum({GONE_REMOTELY}), 0) \
             FROM (SELECT d.id FROM {from}) d \
             CROSS JOIN base b ON b.folder = $F AND b.id = d.id"
        ))
    }

    /// What a confirmed delete of dirty `node` covers of the
    /// [`pending_file_deletes`](Self::pending_file_deletes): gone here, `node` if it is a file
    /// gone locally, or one a folder's removal took along that this device removes from the
    /// drive (either deleted remotely, one node at a time; dirty, so marked:
    /// [`ReadTx::taken_files`]), and the files in its local subtree its removal took along that
    /// this device removes from the drive with its delete here (as `taken_files` counts them,
    /// sync semantics §7);
    /// and the files gone remotely in its local subtree (a delete takes everything below
    /// it with it): the dirty ones gone from the drive, and the synced ones a folder's removal
    /// took along, still live there, whether that folder is in `node` or above it (as
    /// `taken_files` counts them). `None` when `node` isn't dirty.
    pub fn confirmed_file_deletes(
        &self,
        node: NodeId,
    ) -> Result<Option<(Vec<NodeId>, Vec<NodeId>)>> {
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "WITH RECURSIVE top(id) AS (SELECT {ID_OF}), \
                 sub(id) AS (SELECT id FROM top \
                   UNION SELECT l.id FROM local l JOIN sub ON l.parent = sub.id \
                   WHERE l.folder = $F), \
                 up(id) AS (SELECT m.parent FROM remote m WHERE m.id = (SELECT id FROM top) \
                   UNION SELECT m.parent FROM remote m JOIN up ON m.id = up.id), {} \
                 SELECT n.uuid, k.id IS NOT NULL AND d.id = (SELECT id FROM top), \
                   CASE WHEN d.id = (SELECT id FROM top) THEN \
                     k.id IS NOT NULL AND (coalesce({GONE_LOCALLY}, 0) \
                       OR coalesce(t.kind = 0 AND (b.id IS NULL OR b.kind = 0), 0)) \
                   ELSE coalesce(t.id IS NOT NULL AND t.kind = 0 AND {}, 0) END, \
                   coalesce(k.id IS NOT NULL AND {GONE_REMOTELY}, 0) \
                     OR coalesce(b.kind = 0 AND t.id IS NOT NULL, 0) \
                 FROM (SELECT id FROM sub) d LEFT JOIN dirty k ON k.folder = $F AND k.id = d.id \
                 JOIN nodes n ON n.id = d.id LEFT JOIN base b ON b.folder = $F AND b.id = d.id \
                 LEFT JOIN taken t ON t.id = d.id",
                // The removed folders in `node`, and the one above it on the drive (`up`: its
                // live folders' parents, up to the first not live): its removal takes what is
                // in `node` along too (only what is in `node` is kept, below).
                taken_along("(n.id IN (SELECT id FROM sub) OR n.id IN (SELECT id FROM up))"),
                removes_taken()
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
        let (mut dirty, mut gone_here, mut gone_there) = (false, Vec::new(), Vec::new());
        for row in rows {
            let (uuid, top, here, there) = row?;
            dirty |= top;
            if here {
                gone_here.push(codec::node(&uuid)?);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The content states [`GONE_LOCALLY`] leaves out are those that keep a file's absence.
    #[test]
    fn gone_locally_leaves_out_the_states_that_keep_absence() {
        let kept: Vec<String> = (0..)
            .map_while(|v| codec::content_state_from(v).ok())
            .filter(|s| s.keeps_absence())
            .map(|s| codec::content_state_to(s).to_string())
            .collect();
        assert!(
            GONE_LOCALLY.contains(&format!("NOT IN ({})", kept.join(", "))),
            "{GONE_LOCALLY}"
        );
    }
}
