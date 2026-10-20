//! A hash that never changes between builds (FNV-1a, 64 bits), for names and ids that are
//! stored or shared.

/// The FNV-1a 64-bit offset basis: the state [`fnv1a64`] starts from.
pub const FNV1A64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, 64 bits. Unlike `std`'s hasher it's the same in every build, for names and ids
/// that are stored or shared and must not change with the compiler.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_from(FNV1A64_OFFSET, bytes)
}

/// [`fnv1a64`] continued from `state` (a hash so far, or another seed).
pub fn fnv1a64_from(state: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(state, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published FNV-1a 64 test vectors: stored names and ids depend on these.
    #[test]
    fn fnv1a64_golden_values() {
        let h = |s: &str| fnv1a64(s.as_bytes());
        assert_eq!(h(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(h("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(h("foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(fnv1a64_from(h("foo"), b"bar"), h("foobar"), "it continues");
    }
}
