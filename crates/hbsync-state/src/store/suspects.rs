//! Files the identity said once aren't there (its own 404): gone for good only once it says
//! so again, a minute on at least, at the version this device knew (sync semantics §8b).

use super::*;

/// The first "not there" of a file ([`AccountTx::put_gone_suspect`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoneSuspect {
    /// The version this device knew of it then; `None`: none.
    pub version: Option<VersionTag>,
    /// When (Unix ms, this device's clock).
    pub at: i64,
}

impl ReadTx<'_> {
    /// The first "not there" of file `file_id` not cleared since, if any.
    pub fn gone_suspect(&self, file_id: RemoteFileId) -> Result<Option<GoneSuspect>> {
        self.one(
            "SELECT version_tag, at FROM gone_suspects WHERE file_id = ?1",
            file_id.0.as_bytes(),
            |r| {
                Ok(GoneSuspect {
                    version: r
                        .get::<_, Option<Vec<u8>>>(0)?
                        .map(|v| codec::version(&v))
                        .transpose()?,
                    at: r.get(1)?,
                })
            },
        )
    }

    /// The version of file `file_id` this device knows: in the mirror or in Recently deleted.
    pub fn known_version(&self, file_id: RemoteFileId) -> Result<Option<VersionTag>> {
        self.one(
            "SELECT version_tag FROM remote WHERE file_id = ?1 \
             UNION ALL SELECT version_tag FROM removed WHERE file_id = ?1 LIMIT 1",
            file_id.0.as_bytes(),
            |r| codec::version(&r.get::<_, Vec<u8>>(0)?),
        )
    }
}

impl AccountTx<'_> {
    /// Records the first "not there" of file `file_id`, in place of an earlier one.
    pub fn put_gone_suspect(&self, file_id: RemoteFileId, suspect: &GoneSuspect) -> Result<()> {
        self.prep(
            "INSERT OR REPLACE INTO gone_suspects (file_id, version_tag, at) VALUES (?1, ?2, ?3)",
        )?
        .execute(params![
            file_id.0.as_bytes(),
            suspect.version.map(|v| v.0.as_bytes().to_vec()),
            suspect.at
        ])?;
        Ok(())
    }

    /// Tests: makes every "not there" `ms` older, as if that long had passed since.
    #[cfg(feature = "testkit")]
    pub fn age_gone_suspects(&self, ms: i64) -> Result<usize> {
        Ok(self
            .prep("UPDATE gone_suspects SET at = at - ?1")?
            .execute([ms])?)
    }

    /// Forgets the "not there" of file `file_id`: it is there after all (a row of it, an
    /// answer), or gone for good.
    pub fn clear_gone_suspect(&self, file_id: RemoteFileId) -> Result<()> {
        self.prep("DELETE FROM gone_suspects WHERE file_id = ?1")?
            .execute([file_id.0.as_bytes()])?;
        Ok(())
    }
}
