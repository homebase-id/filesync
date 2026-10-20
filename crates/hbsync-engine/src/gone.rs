//! When a file counts as deleted for good (sync semantics §8b). Proof is the drive's own word
//! that it is a tombstone (a 4002 answer, a header that says deleted, a tombstone in the
//! feed) or a delete the feed announced. The identity's "not there" (its own 404: never a proxy's
//! or a page's in front of it, `homebase-api-notes.md`) is a suspicion only: the file counts as
//! gone once the identity says so again, [`CONFIRM_AFTER_MS`] on at least, at the version this
//! device knew the first time. A row of the file, or an answer that it is there, clears the
//! suspicion. A wrong 404 for a moment (a server hiccup) then never hides a live file for the
//! 30 days a gone file is remembered, nor deletes it here.

use hbsync_state::GoneSuspect;
use hbsync_types::VersionTag;

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
