//! Content hashes: BLAKE3 (what this client writes) and SHA-256 (what other clients may write).
//!
//! Wire format and golden vectors: `../odin-core/docs/payload-hashes.md`.

use std::io::{self, Read};

use hbsync_types::{ContentHash, HashAlgorithm};
use sha2::Digest as _;

/// Bytes hashed per read. Large enough for BLAKE3's SIMD paths, small enough to keep memory flat.
pub(crate) const BUF_SIZE: usize = 256 * 1024;

/// An incremental hasher for either algorithm.
#[derive(Debug, Clone)]
pub enum Hasher {
    Blake3(Box<blake3::Hasher>),
    Sha256(sha2::Sha256),
}

impl Hasher {
    pub fn new(algorithm: HashAlgorithm) -> Self {
        match algorithm {
            HashAlgorithm::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
            HashAlgorithm::Sha256 => Hasher::Sha256(sha2::Sha256::new()),
        }
    }

    pub(crate) fn algorithm(&self) -> HashAlgorithm {
        match self {
            Hasher::Blake3(_) => HashAlgorithm::Blake3,
            Hasher::Sha256(_) => HashAlgorithm::Sha256,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Blake3(h) => {
                h.update(data);
            }
            Hasher::Sha256(h) => h.update(data),
        }
    }

    pub fn finalize(self) -> ContentHash {
        let algorithm = self.algorithm();
        let bytes: [u8; 32] = match self {
            Hasher::Blake3(h) => *h.finalize().as_bytes(),
            Hasher::Sha256(h) => h.finalize().into(),
        };
        ContentHash { algorithm, bytes }
    }
}

/// Hashes everything `reader` yields, with constant memory. Returns the hash and byte count.
pub fn hash_reader(reader: impl Read, algorithm: HashAlgorithm) -> io::Result<(ContentHash, u64)> {
    let mut hasher = Hasher::new(algorithm);
    let total = feed(reader, &mut hasher)?;
    Ok((hasher.finalize(), total))
}

/// Feeds everything `reader` yields into `hasher`, in large reads. Returns the byte count.
pub(crate) fn feed(mut reader: impl Read, hasher: &mut Hasher) -> io::Result<u64> {
    let mut buf = vec![0u8; BUF_SIZE];
    let mut total = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => return Ok(total),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(&buf[..n]);
        total += n as u64;
    }
}

/// Hashes a byte slice in one call.
pub fn hash_bytes(data: &[u8], algorithm: HashAlgorithm) -> ContentHash {
    let mut hasher = Hasher::new(algorithm);
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_types::HashParseError;

    /// The golden-vector input from odin-core: 1025 bytes, byte i = i % 251.
    pub(crate) fn golden_input() -> Vec<u8> {
        (0..1025u32).map(|i| (i % 251) as u8).collect()
    }

    const SHA256_GOLDEN: &str = "bc0b6b10b89b9487a12fda2a8cc13194e7091c217aabf8b92846274026f4bcd0";
    const BLAKE3_GOLDEN: &str = "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444";

    #[test]
    fn golden_vectors() {
        let input = golden_input();
        assert_eq!(
            hash_bytes(&input, HashAlgorithm::Sha256).to_hex(),
            SHA256_GOLDEN
        );
        assert_eq!(
            hash_bytes(&input, HashAlgorithm::Blake3).to_hex(),
            BLAKE3_GOLDEN
        );
    }

    #[test]
    fn streaming_equals_one_shot() -> io::Result<()> {
        // Larger than BUF_SIZE so the loop runs more than once.
        let data: Vec<u8> = (0..(BUF_SIZE * 2 + 17)).map(|i| (i % 253) as u8).collect();
        for alg in [HashAlgorithm::Blake3, HashAlgorithm::Sha256] {
            let (h, n) = hash_reader(data.as_slice(), alg)?;
            assert_eq!(h, hash_bytes(&data, alg));
            assert_eq!(n, data.len() as u64);
        }
        Ok(())
    }

    #[test]
    fn encodings_round_trip() -> Result<(), HashParseError> {
        let h = hash_bytes(&golden_input(), HashAlgorithm::Blake3);
        assert_eq!(
            h.to_base64(),
            "0AJ4rkfrJ7NPrs9ntP4mP4LVQSkWwf/ZfIy3+4FLhEQ="
        );
        assert_eq!(
            ContentHash::from_base64(HashAlgorithm::Blake3, &h.to_base64())?,
            h
        );
        assert_eq!(ContentHash::parse_tagged(&h.to_string())?, h);
        assert_eq!(
            ContentHash::from_base64(HashAlgorithm::Sha256, "AAAA"),
            Err(HashParseError::Length(3))
        );
        assert!("md5".parse::<HashAlgorithm>().is_err());
        Ok(())
    }
}
