//! Content hashes: BLAKE3 (what this client writes) and SHA-256 (what other clients may write).
//!
//! Wire format and golden vectors: `../odin-core/docs/payload-hashes.md`.

use std::fmt;
use std::io::{self, Read};
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use sha2::Digest as _;

/// Bytes hashed per read. Large enough for BLAKE3's SIMD paths, small enough to keep memory flat.
pub const BUF_SIZE: usize = 256 * 1024;

/// The two algorithms node schema v1 allows. The set is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HashAlgorithm {
    Blake3,
    Sha256,
}

impl HashAlgorithm {
    /// The wire name (`"blake3"` / `"sha256"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            HashAlgorithm::Blake3 => "blake3",
            HashAlgorithm::Sha256 => "sha256",
        }
    }
}

impl FromStr for HashAlgorithm {
    type Err = HashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blake3" => Ok(HashAlgorithm::Blake3),
            "sha256" => Ok(HashAlgorithm::Sha256),
            _ => Err(HashParseError::UnknownAlgorithm(s.to_owned())),
        }
    }
}

impl fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HashParseError {
    #[error("unknown hash algorithm {0:?}")]
    UnknownAlgorithm(String),
    #[error("hash is not valid base64")]
    Base64,
    #[error("hash has {0} bytes, expected 32")]
    Length(usize),
}

/// A 32-byte digest together with the algorithm that produced it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentHash {
    pub algorithm: HashAlgorithm,
    pub bytes: [u8; 32],
}

impl ContentHash {
    pub fn to_base64(&self) -> String {
        STANDARD.encode(self.bytes)
    }

    pub fn from_base64(algorithm: HashAlgorithm, b64: &str) -> Result<Self, HashParseError> {
        let raw = STANDARD.decode(b64).map_err(|_| HashParseError::Base64)?;
        let bytes: [u8; 32] = raw
            .as_slice()
            .try_into()
            .map_err(|_| HashParseError::Length(raw.len()))?;
        Ok(ContentHash { algorithm, bytes })
    }

    pub fn to_hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parses `algorithm:hex`, the form used in the state DB and logs.
    pub fn parse_tagged(s: &str) -> Result<Self, HashParseError> {
        let (alg, hex) = s.split_once(':').ok_or(HashParseError::Base64)?;
        let algorithm: HashAlgorithm = alg.parse()?;
        if hex.len() != 64 {
            return Err(HashParseError::Length(hex.len() / 2));
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
                .map_err(|_| HashParseError::Base64)?;
        }
        Ok(ContentHash { algorithm, bytes })
    }
}

impl fmt::Display for ContentHash {
    /// `blake3:d00278ae…`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.to_hex())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}…", self.algorithm, &self.to_hex()[..12])
    }
}

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

    pub fn algorithm(&self) -> HashAlgorithm {
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
pub fn hash_reader(
    mut reader: impl Read,
    algorithm: HashAlgorithm,
) -> io::Result<(ContentHash, u64)> {
    let mut hasher = Hasher::new(algorithm);
    let mut buf = vec![0u8; BUF_SIZE];
    let mut total = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((hasher.finalize(), total))
}

/// Hashes a byte slice in one call.
pub fn hash_bytes(data: &[u8], algorithm: HashAlgorithm) -> ContentHash {
    let mut hasher = Hasher::new(algorithm);
    hasher.update(data);
    hasher.finalize()
}

/// A reader that hashes the bytes passing through it (used when streaming an upload).
#[derive(Debug)]
pub struct HashingReader<R> {
    inner: R,
    hasher: Hasher,
    count: u64,
}

impl<R: Read> HashingReader<R> {
    pub fn new(inner: R, algorithm: HashAlgorithm) -> Self {
        HashingReader {
            inner,
            hasher: Hasher::new(algorithm),
            count: 0,
        }
    }

    /// The hash and length of everything read so far.
    pub fn finish(self) -> (ContentHash, u64) {
        (self.hasher.finalize(), self.count)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        self.count += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let mut reader = HashingReader::new(data.as_slice(), alg);
            io::copy(&mut reader, &mut io::sink())?;
            assert_eq!(reader.finish(), (h, n));
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
