//! The drive's picture as named operations: a node is live (the mirror), in Recently deleted,
//! or its file deleted for good, never two of them. The facts a feed page, a restore or a
//! delete for good brings are recorded through these (the engine's `mirror` module), and the
//! answers to this device's own writes ([`AccountTx::answered`]). They are the only writes of
//! the picture outside this crate: the raw ones they are made of are the crate's (and a
//! test's, through `testkit::RawPicture`), so the rule holds by type.

use super::*;

/// The drive's answer to a write of this device: what it left (`left`), written over the
/// version of its file the picture held when the write was made (`expected`; `None`: a file the
/// write made).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub expected: Option<VersionTag>,
    pub left: Left,
}

/// What a write left of its node on the drive ([`Answer`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Left {
    Live(MirrorRow),
    Removed(RemovedRow),
}

impl Answer {
    /// A write over `expected` that left `row` live.
    pub fn live(expected: Option<VersionTag>, row: MirrorRow) -> Answer {
        Answer {
            expected,
            left: Left::Live(row),
        }
    }

    /// A removal over `expected` that left `row` in Recently deleted, at the answer's time (open
    /// if it gave none, until the feed brings it).
    pub fn removal(expected: Option<VersionTag>, row: MirrorRow) -> Answer {
        let removed_at = row.changed_at;
        Answer {
            expected,
            left: Left::Removed(RemovedRow { row, removed_at }),
        }
    }

    /// The row the write left, live or removed.
    pub fn row(&self) -> &MirrorRow {
        match &self.left {
            Left::Live(row) => row,
            Left::Removed(r) => &r.row,
        }
    }

    /// The row the write left live (`None`: a removal).
    pub fn live_row(&self) -> Option<&MirrorRow> {
        match &self.left {
            Left::Live(row) => Some(row),
            Left::Removed(_) => None,
        }
    }
}

/// What [`AccountTx::answered`] made of an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answered {
    /// In the picture.
    Recorded,
    /// Left out: a pull recorded a newer state of the file after the write landed, which the
    /// picture keeps.
    Superseded,
}

impl Answered {
    /// What an answer at `answer` to a write over `expected` makes of the picture holding the
    /// file at `held` (`None`: not at all): recorded while it still holds the version the write
    /// replaced, or the answer itself (its echo came first). Any other version came after the
    /// write: a file's versions follow each other, and the write was checked against
    /// `expected` (verified in odin-core: a header's write is one statement conditional on its
    /// version tag, `TableDriveMainIndex`). Or before it, from a page fetched before the write
    /// landed: the account reads the feed again, which brings the answer or what followed it.
    pub fn of(held: Option<VersionTag>, expected: Option<VersionTag>, answer: VersionTag) -> Self {
        if held == expected || held == Some(answer) {
            Answered::Recorded
        } else {
            Answered::Superseded
        }
    }
}

impl AccountTx<'_> {
    // ---- the drive's picture ----

    /// A live version of a node: out of Recently deleted (whatever the mirror then takes), into
    /// the mirror (`put_mirror`: a version it has, or a file deleted for good, changes
    /// nothing there).
    pub fn put_live(&self, row: &MirrorRow) -> Result<()> {
        self.delete_removed(row.node)?;
        self.put_mirror(row)
    }

    /// A node's file moved to Recently deleted, as the feed says: out of the mirror (its node
    /// planned again where it was held, also where the mirror had no row of it), into Recently
    /// deleted (`put_removed`).
    pub fn move_to_removed(&self, removed: &RemovedRow) -> Result<()> {
        self.leave_tree(removed.row.file_id)?;
        self.put_removed(removed)
    }

    /// File `file_id` deleted for good, since `at` if known (Unix ms, the server's: the time of
    /// the tombstone it left; [`AccountTx::prune_gone`] counts from it): never back in the mirror or
    /// Recently deleted ([`ReadTx::file_gone`]), and out of both now. A file already known gone
    /// keeps the time it had.
    pub fn forget_file(&self, file_id: RemoteFileId, at: Option<i64>) -> Result<()> {
        match at {
            Some(at) => self.mark_file_gone_at(file_id, at)?,
            None => self.mark_file_gone(file_id)?,
        }
        self.delete_removed_file(file_id)?;
        self.leave_tree(file_id)
    }

    /// This device's write's answer (`answer`), into the picture as a live version
    /// ([`AccountTx::put_live`]) or a removal ([`AccountTx::move_to_removed`]), unless a pull
    /// recorded a newer state of the file since the write landed ([`Answered::of`]; a delete
    /// for good, or a version this client can't read, are newer too): that stays.
    /// Its echo first changes nothing. Checked and written in the caller's transaction, which no
    /// pull's commit comes into (writes to a state DB take turns: its process's gate, and
    /// SQLite's write lock across processes).
    pub fn answered(&self, answer: &Answer) -> Result<Answered> {
        let row = answer.row();
        let held = self.known_file(row.file_id)?;
        let echo = held.is_some_and(|(v, _, removed)| {
            v == row.version && removed == matches!(answer.left, Left::Removed(_))
        });
        let answered = match held {
            // Deleted for good since.
            None if self.file_gone(row.file_id)? => Answered::Superseded,
            // A version since that this client can't read (held: the picture keeps the one
            // before it, and the mark).
            _ if self
                .unreadable_version(row.file_id)?
                .is_some_and(|v| v != row.version) =>
            {
                Answered::Superseded
            }
            _ => Answered::of(held.map(|(v, ..)| v), answer.expected, row.version),
        };
        if answered == Answered::Superseded || echo {
            return Ok(answered);
        }
        match &answer.left {
            Left::Live(row) => self.put_live(row)?,
            Left::Removed(removed) => self.move_to_removed(removed)?,
        }
        Ok(answered)
    }

    /// A tombstone the feed brought (`put_tombstone`, if it says when it was written):
    /// its file is deleted for good ([`AccountTx::forget_file`]).
    pub fn tombstone(&self, tombstone: RemoteRef, at: Option<i64>) -> Result<()> {
        if let Some(at) = at {
            self.put_tombstone(tombstone, at)?;
        }
        self.forget_file(tombstone.file_id, None)
    }

    /// A file deleted for good with nothing left (a hard delete, announced): a tombstone this
    /// device holds at that version has nothing left to delete, and the file is forgotten
    /// ([`AccountTx::forget_file`]).
    pub fn hard_deleted(&self, at: RemoteRef) -> Result<()> {
        self.delete_tombstone_at(at)?;
        self.forget_file(at.file_id, None)
    }

    /// The mirror without file `file_id`, its node marked to plan again where it was held: by
    /// its row (`delete_mirror`), or, where the mirror has none, where a base holds the
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
