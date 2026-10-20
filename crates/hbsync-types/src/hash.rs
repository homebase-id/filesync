//! Content hash values: a 32-byte digest and the algorithm that made it (node schema v1).
//! Computing them is `hbsync-fileio`'s.

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

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
