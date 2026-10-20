//! Operations the drive refused for good: not sent again while what they would send is the
//! same (sync semantics §8a, refused).

use super::*;

/// A refusal of a folder's operation on a node ([`FolderTx::put_refused`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub node: NodeId,
    /// The operation's kind (`Op::kind`).
    pub op: String,
    /// What the operation would send or fetch, as the engine keys it: another key is another
    /// operation, not refused.
    pub key: String,
    /// Refused for good; `false`: one strike (a download whose bytes didn't match once).
    pub refused: bool,
    pub why: String,
    /// When (Unix ms).
    pub at: i64,
}

impl ReadTx<'_> {
    /// The folder's refusals.
    pub fn all_refused(&self) -> Result<Vec<Refused>> {
        self.many(
            "SELECT n.uuid, r.op, r.key, r.refused, r.why, r.at FROM refused r \
             JOIN nodes n ON n.id = r.id WHERE r.folder = $F",
            [],
            |r| {
                Ok(Refused {
                    node: codec::node(&r.get::<_, Vec<u8>>(0)?)?,
                    op: r.get(1)?,
                    key: r.get(2)?,
                    refused: r.get(3)?,
                    why: r.get(4)?,
                    at: r.get(5)?,
                })
            },
        )
    }
}

impl FolderTx<'_> {
    /// Records `refused`, in place of an earlier one of its node and kind.
    pub fn put_refused(&self, refused: &Refused) -> Result<()> {
        let id = self.key_or_insert(refused.node)?;
        self.prep(
            "INSERT OR REPLACE INTO refused (folder, id, op, key, refused, why, at) \
             VALUES ($F, ?1, ?2, ?3, ?4, ?5, ?6)",
        )?
        .execute(params![
            id,
            refused.op,
            refused.key,
            refused.refused,
            refused.why,
            refused.at
        ])?;
        Ok(())
    }

    /// Forgets the refusal of `node`'s operation of kind `op`.
    pub fn delete_refused(&self, node: NodeId, op: &str) -> Result<()> {
        self.prep(&format!(
            "DELETE FROM refused WHERE folder = $F AND id = {ID_OF} AND op = ?2"
        ))?
        .execute(params![node.0.as_bytes(), op])?;
        Ok(())
    }

    /// Forgets the refusals recorded before `before` (Unix ms; every one: `i64::MAX`).
    pub fn prune_refused(&self, before: i64) -> Result<usize> {
        Ok(self
            .prep("DELETE FROM refused WHERE folder = $F AND at < ?1")?
            .execute([before])?)
    }
}
