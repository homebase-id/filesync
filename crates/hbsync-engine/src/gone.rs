//! When a file counts as deleted for good (sync semantics §8b). Proof is the drive's own word
//! that it is a tombstone (a 4002 answer, a header that says deleted, a tombstone in the
//! feed) or a delete the feed announced. The identity's "not there" (its own 404: never a proxy's
//! or a page's in front of it, `homebase-api-notes.md`) is a suspicion only: the file counts as
//! gone once the identity says so again, [`CONFIRM_AFTER_MS`] on at least, at the version this
//! device knew the first time. A row of the file, or an answer that it is there, clears the
//! suspicion. A wrong 404 for a moment (a server hiccup) then never hides a live file for the
//! 30 days a gone file is remembered, nor deletes it here.
//!
//! The whole decision lives here: what the drive says of a file ([`look`], [`gone_after`]),
//! and what that makes of it in the store ([`judged`], [`heard`]), the rule itself pure
//! ([`judge`]).

use hbsync_remote::{Remote, RemoteError, RemoteHeader};
use hbsync_state::{AccountTx, GoneSuspect, StateError};
use hbsync_types::{RemoteFileId, VersionTag};

/// How long after the first "not there" a second one confirms it (Unix ms): the next sync.
pub const CONFIRM_AFTER_MS: i64 = 60_000;

/// How long a first "not there" waits for its second (Unix ms): one older (the file never
/// asked about since) is a first again, so two wrong 404s far apart never make a file gone.
pub const CONFIRM_WITHIN_MS: i64 = 3_600_000;

/// What an identity's "not there" means ([`judge`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotThere {
    /// Gone for good: said twice, far enough apart, at one version.
    Gone,
    /// Suspected only: this is the suspicion to keep (the first, or a new one).
    Suspect(GoneSuspect),
}

/// What the identity's "not there" for a file means, at `now_ms` (this device's clock): the
/// file's earlier suspicion `prior`, if any, and the version this device knows of it now.
pub fn judge(prior: Option<GoneSuspect>, version: Option<VersionTag>, now_ms: i64) -> NotThere {
    let since = prior
        .filter(|p| p.version == version)
        .map(|p| (p, now_ms - p.at));
    match since {
        Some((_, since)) if (CONFIRM_AFTER_MS..=CONFIRM_WITHIN_MS).contains(&since) => {
            NotThere::Gone
        }
        // Said again too soon: the first stands, its time with it.
        Some((p, since)) if (0..CONFIRM_AFTER_MS).contains(&since) => NotThere::Suspect(p),
        // The first, at another version, long after the first, or the clock went back: it
        // starts now.
        _ => NotThere::Suspect(GoneSuspect {
            version,
            at: now_ms,
        }),
    }
}

// ---- asking the drive ----

/// What the drive says of a file a request found not there ([`gone_after`], [`look`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Said {
    /// A tombstone: deleted for good, proof on its own.
    Gone,
    /// The identity's own "not there" (a 404): a suspicion until it says so again
    /// ([`not_there`]).
    NotThere,
    /// There after all.
    There,
}

/// What the drive says of a file a write was refused for with `e` (not found, or a tombstone):
/// a tombstone's answer says it, else its header is read ([`look`]).
pub(crate) fn gone_after(
    remote: &dyn Remote,
    e: &RemoteError,
    file_id: RemoteFileId,
) -> Result<Said, RemoteError> {
    match e {
        RemoteError::Deleted => Ok(Said::Gone),
        _ => look(remote, file_id),
    }
}

/// What the drive says of file `file_id`, by its header: a write or read answered not found
/// may have other reasons.
pub(crate) fn look(remote: &dyn Remote, file_id: RemoteFileId) -> Result<Said, RemoteError> {
    read(remote, file_id).map(|(said, _)| said)
}

/// [`look`], with the header read, where there was one (a tombstone's says when it was
/// written).
pub(crate) fn read(
    remote: &dyn Remote,
    file_id: RemoteFileId,
) -> Result<(Said, Option<RemoteHeader>), RemoteError> {
    match remote.header(file_id) {
        Ok(header) if header.is_deleted() => Ok((Said::Gone, Some(header))),
        Ok(header) => Ok((Said::There, Some(header))),
        Err(RemoteError::Deleted) => Ok((Said::Gone, None)),
        Err(RemoteError::NotFound) => Ok((Said::NotThere, None)),
        Err(e) => Err(e),
    }
}

// ---- taking what it said ----

/// The identity said file `file_id` isn't there (its own 404), now: whether that makes it
/// deleted for good ([`judge`]: said twice, a minute apart, at the version known). Else the
/// suspicion is kept, for the next time.
pub(crate) fn not_there(s: AccountTx<'_>, file_id: RemoteFileId) -> Result<bool, StateError> {
    let prior = s.gone_suspect(file_id)?;
    let version = s.known_version(file_id)?;
    match judge(prior, version, hbsync_types::unix_now_ms()) {
        NotThere::Gone => {
            s.clear_gone_suspect(file_id)?;
            Ok(true)
        }
        NotThere::Suspect(suspect) => {
            if prior != Some(suspect) {
                tracing::warn!("{file_id:?}: the identity says it isn't there; asked again later");
                s.put_gone_suspect(file_id, &suspect)?;
            }
            Ok(false)
        }
    }
}

/// Whether what the drive `said` of file `file_id` ([`look`]) makes it deleted for good: a
/// tombstone does, the identity's "not there" the second time a minute on ([`not_there`]);
/// there after all clears the doubt (a "not there" of it is forgotten).
pub(crate) fn judged(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    said: Said,
) -> Result<bool, StateError> {
    match said {
        Said::Gone => Ok(true),
        Said::NotThere => not_there(s, file_id),
        Said::There => s.clear_gone_suspect(file_id).map(|()| false),
    }
}

/// [`judged`], and a file deleted for good is forgotten. Whether it was.
pub(crate) fn heard(
    s: AccountTx<'_>,
    file_id: RemoteFileId,
    said: Said,
) -> Result<bool, StateError> {
    let gone = judged(s, file_id, said)?;
    if gone {
        s.forget_file(file_id, None)?;
    }
    Ok(gone)
}

#[cfg(test)]
mod tests {
    use hbsync_types::Uuid;

    use super::*;

    /// Once is a suspicion; again too soon, the first stands; again a minute to an hour on at
    /// the same version, gone; at another version, later, or earlier, a suspicion anew.
    #[test]
    fn not_there_twice_a_minute_apart_at_one_version_is_gone() {
        let v = Some(VersionTag(Uuid::from_u128(1)));
        let NotThere::Suspect(first) = judge(None, v, 1_000) else {
            panic!("once is a suspicion");
        };
        assert_eq!(
            judge(Some(first), v, 1_000 + CONFIRM_AFTER_MS - 1),
            NotThere::Suspect(first)
        );
        assert_eq!(
            judge(Some(first), v, 1_000 + CONFIRM_AFTER_MS),
            NotThere::Gone
        );
        assert_eq!(
            judge(Some(first), v, 1_000 + CONFIRM_WITHIN_MS),
            NotThere::Gone
        );
        // Long after, or before it by the clock: a first again.
        for now in [1_001 + CONFIRM_WITHIN_MS, 999] {
            let again = GoneSuspect {
                version: v,
                at: now,
            };
            assert_eq!(judge(Some(first), v, now), NotThere::Suspect(again));
        }
        let other = Some(VersionTag(Uuid::from_u128(2)));
        assert_eq!(
            judge(Some(first), other, 1_000 + CONFIRM_AFTER_MS),
            NotThere::Suspect(GoneSuspect {
                version: other,
                at: 1_000 + CONFIRM_AFTER_MS
            })
        );
    }
}
