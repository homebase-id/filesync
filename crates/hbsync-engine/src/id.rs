//! Node ids the engine derives instead of drawing at random: a replan picks the same one.

use hbsync_fileio::{FNV1A64_OFFSET, fnv1a64_from};
use hbsync_types::{NodeId, Uuid};

/// The id derived from `parts` for the use `tag` (`b"fork"`, `b"detach"`): the same parts give
/// the same id. A hash, never a mix that undoes itself, and each part's length goes in before
/// it: no two different lists of parts read as the same bytes. Stored and on the drive once
/// used: the encoding must never change.
pub(crate) fn derived_id(parts: &[&[u8]], tag: &[u8]) -> NodeId {
    let mut bytes = [0u8; 16];
    for (half, out) in bytes.chunks_mut(8).enumerate() {
        let mut h = FNV1A64_OFFSET ^ half as u64;
        for part in parts.iter().copied().chain([tag]) {
            h = fnv1a64_from(h, &(part.len() as u64).to_le_bytes());
            h = fnv1a64_from(h, part);
        }
        out.copy_from_slice(&h.to_le_bytes());
    }
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // a well-formed v4 UUID
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    NodeId(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Derived ids are stored and on the drive: the hash must never change.
    #[test]
    fn derived_ids_are_pinned() {
        let id = |parts: &[&[u8]], tag: &[u8]| derived_id(parts, tag).0.to_string();
        assert_eq!(id(&[], b"fork"), "5dfd596b-7f2f-4a55-ac4f-c881f3f9c868");
        assert_eq!(
            id(&[b"node", b"computer"], b"detach"),
            "8da9091a-57cd-48f9-8425-9e1cc9d2cf45"
        );
    }

    #[test]
    fn parts_never_run_into_each_other() {
        let tag = b"fork";
        assert_ne!(
            derived_id(&[b"ab", b"c"], tag),
            derived_id(&[b"a", b"bc"], tag)
        );
        assert_ne!(derived_id(&[b"ab"], tag), derived_id(&[b"ab", b""], tag));
        assert_ne!(derived_id(&[b"ab"], tag), derived_id(&[b"ab"], b"detach"));
    }
}
