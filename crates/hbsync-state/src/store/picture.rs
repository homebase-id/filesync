//! The drive's picture as named operations: a node is live (the mirror), in Recently deleted,
//! or its file deleted for good, never two of them. The facts a feed page, a restore or a
//! delete for good brings are recorded through these (the engine's `mirror` module).

use super::*;

impl Store<'_> {
    // ---- the drive's picture ----

    /// A live version of a node: out of Recently deleted (whatever the mirror then takes), into
    /// the mirror ([`Store::put_mirror`]: a version it has, or a file deleted for good, changes
    /// nothing there).
    pub fn put_live(&self, row: &MirrorRow) -> Result<()> {
        self.delete_removed(row.node)?;
        self.put_mirror(row)
    }

    /// A node's file moved to Recently deleted, as the feed says: out of the mirror (its node
    /// planned again where it was held, also where the mirror had no row of it), into Recently
    /// deleted ([`Store::put_removed`]).
    pub fn move_to_removed(&self, removed: &RemovedRow) -> Result<()> {
        self.leave_tree(removed.row.file_id)?;
        self.put_removed(removed)
    }

    /// File `file_id` deleted for good, since `at` if known (Unix ms, the server's: the time of
    /// the tombstone it left; [`Store::prune_gone`] counts from it): never back in the mirror or
    /// Recently deleted ([`Store::file_gone`]), and out of both now. A file already known gone
    /// keeps the time it had.
    pub fn forget_file(&self, file_id: RemoteFileId, at: Option<i64>) -> Result<()> {
        match at {
            Some(at) => self.mark_file_gone_at(file_id, at)?,
            None => self.mark_file_gone(file_id)?,
        }
        self.delete_removed_file(file_id)?;
        self.leave_tree(file_id)
    }

    /// A tombstone the feed brought ([`Store::put_tombstone`], if it says when it was written):
    /// its file is deleted for good ([`Store::forget_file`]).
    pub fn tombstone(&self, tombstone: RemoteRef, at: Option<i64>) -> Result<()> {
        if let Some(at) = at {
            self.put_tombstone(tombstone, at)?;
        }
        self.forget_file(tombstone.file_id, None)
    }

    /// The mirror without file `file_id`, its node marked to plan again where it was held: by
    /// its row ([`Store::delete_mirror`]), or, where the mirror has none, where a base holds the
    /// file (a tombstone has no node id: the node is found by file id).
    fn leave_tree(&self, file_id: RemoteFileId) -> Result<()> {
        if self.delete_mirror(file_id)?.is_none()
            && let Some(node) = self.node_of_file(file_id)?
        {
            // Its place went with its row: the folders holding it are the ones concerned.
            self.fan_out(node, NodeId::ROOT)?;
        }
        Ok(())
    }
}
