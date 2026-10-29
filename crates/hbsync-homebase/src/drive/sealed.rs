//! Homebase file encryption (`docs/spec/encryption.md`): one random AES-128 key per file,
//! AES-CBC with PKCS#7, a random IV for the header and for every payload upload, as odin-js
//! and chat-kmp do it. Verified in odin-core `KeyHeader`, `EncryptedKeyHeader`, `PayloadHash`,
//! `FileSystemUpdateWriterBase`, and odin-js `UploadHelpers`, `SecurityHelpers`.

use std::io::{self, Read, Write};
use std::ops::Range;

use aes::Aes128;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cbc::cipher::block_padding::{Padding, Pkcs7};
use cbc::cipher::{Array, BlockModeDecrypt, BlockModeEncrypt, KeyIvInit};
use hbsync_types::{ContentHash, FileToken, HashAlgorithm};
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroize;

use crate::HomebaseError;
use crate::crypto::{Envelope, Key, aes_cbc_decrypt, aes_cbc_encrypt, random};

const BLOCK: usize = 16;

/// A sealed key header as bytes: the wrap IV (16), then the encrypted IV and key (48).
const SEALED_LEN: usize = 64;

/// Streams are encrypted and decrypted this many bytes at a time.
const CHUNK: usize = 64 * 1024;

/// The IV of an encrypted content hash is the payload IV XOR this, in every byte
/// (odin-core `PayloadHash.ContentHashIv`).
const HASH_IV_MASK: u8 = 0xAA;

/// A file's key and its header IV. The header IV encrypts `appData.content`; each payload has
/// an IV of its own.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyHeader {
    pub iv: [u8; 16],
    pub key: Key,
}

impl std::fmt::Debug for KeyHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeyHeader(..)")
    }
}

impl Drop for KeyHeader {
    fn drop(&mut self) {
        self.iv.zeroize();
        self.key.zeroize();
    }
}

/// A key header encrypted with a shared secret, as the JSON of a file header carries it
/// (`sharedSecretEncryptedKeyHeader`) and an upload sends it (`encryptedKeyHeader`); on a
/// private (zero-knowledge) drive, encrypted the same way with the drive's key instead (its
/// type `zkDrive`: odin-core `docs/zero-knowledge-drive-api-plan.md`, decision 3).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SealedKeyHeader {
    #[serde(default)]
    pub iv: String,
    #[serde(default)]
    pub encrypted_aes_key: String,
    /// `aes` (11), `sealed` (12) or `zkDrive` (13), by name or number.
    #[serde(default, rename = "type")]
    pub kind: Value,
}

impl SealedKeyHeader {
    /// The binary form of the `SharedSecretEncryptedHeader64` response header: IV (16),
    /// encrypted key (48), version (4), base64 (odin-core `EncryptedKeyHeader.ToBase64`).
    pub(crate) fn from_base64(b64: &str) -> Result<Self, HomebaseError> {
        let bytes = STANDARD
            .decode(b64.trim())
            .map_err(|_| crypto("the key header isn't base64"))?;
        if bytes.len() != SEALED_LEN + 4 {
            return Err(crypto(format!("a key header of {} bytes", bytes.len())));
        }
        Self::from_token(&FileToken(bytes[..SEALED_LEN].into()))
            .ok_or_else(|| crypto("a key header that isn't one"))
    }

    /// Locked with the drive's key, not the shared secret.
    pub(crate) fn locked_with_drive_key(&self) -> bool {
        self.kind == "zkDrive" || self.kind == 13
    }

    /// As sync keeps it, a file's token: the wrap IV, then the encrypted IV and key. `None` if
    /// it isn't that.
    pub(crate) fn to_token(&self) -> Option<FileToken> {
        let mut bytes = STANDARD
            .decode(&self.iv)
            .ok()
            .filter(|iv| iv.len() == BLOCK)?;
        bytes.extend(STANDARD.decode(&self.encrypted_aes_key).ok()?);
        (bytes.len() == SEALED_LEN).then(|| FileToken(bytes.into()))
    }

    /// A token back as a sealed key header; `None` if it isn't one.
    pub(crate) fn from_token(token: &FileToken) -> Option<Self> {
        let bytes = &token.0;
        (bytes.len() == SEALED_LEN).then(|| SealedKeyHeader {
            iv: STANDARD.encode(&bytes[..BLOCK]),
            encrypted_aes_key: STANDARD.encode(&bytes[BLOCK..]),
            kind: Value::Null,
        })
    }
}

impl KeyHeader {
    /// A new key and header IV, for a new file.
    pub(crate) fn random() -> Result<Self, HomebaseError> {
        Ok(KeyHeader {
            iv: strong()?,
            key: strong()?,
        })
    }

    /// The same key with a new header IV: what an update sends. The server keeps the file's
    /// key and takes the IV (odin-core `OverwriteMetadataInternal`).
    pub(crate) fn with_new_iv(&self) -> Result<Self, HomebaseError> {
        Ok(KeyHeader {
            iv: strong()?,
            key: self.key,
        })
    }

    /// `iv || key`, AES-CBC under `shared_secret` with `wrap_iv`, as an upload sends it.
    pub(crate) fn seal(&self, shared_secret: &Key, wrap_iv: &[u8; 16]) -> Value {
        let mut plain = [0u8; 32];
        plain[..16].copy_from_slice(&self.iv);
        plain[16..].copy_from_slice(&self.key);
        let sealed = aes_cbc_encrypt(shared_secret, wrap_iv, &plain);
        plain.zeroize();
        json!({
            "encryptionVersion": 1,
            "type": "aes",
            "iv": STANDARD.encode(wrap_iv),
            "encryptedAesKey": STANDARD.encode(sealed),
        })
    }

    /// `iv || key`, AES-CBC under a private drive's key with the header IV, as an upload to it
    /// sends it (`zkDrive`): the server stores it as it is.
    pub(crate) fn lock(&self, drive_key: &Key) -> Value {
        let mut locked = self.seal(drive_key, &self.iv);
        locked["type"] = json!("zkDrive");
        locked
    }

    /// The token the server will give for this key header once written: it seals it under
    /// `shared_secret` with the header's own IV as the wrap IV (**[verified]** odin-core
    /// `DriveFileUtility.CreateClientFileHeader`, the stored `EncryptedKeyHeader.Iv` being the
    /// key header's, `DriveStorageServiceBase.EncryptKeyHeader`).
    pub(crate) fn token(&self, shared_secret: &Key) -> FileToken {
        let mut plain = [0u8; 32];
        plain[..16].copy_from_slice(&self.iv);
        plain[16..].copy_from_slice(&self.key);
        let mut bytes = self.iv.to_vec();
        bytes.extend(aes_cbc_encrypt(shared_secret, &self.iv, &plain));
        plain.zeroize();
        FileToken(bytes.into())
    }

    /// Opens a key header sealed with `shared_secret`.
    pub(crate) fn open(
        sealed: &SealedKeyHeader,
        shared_secret: &Key,
    ) -> Result<Self, HomebaseError> {
        let mut plain = Envelope {
            iv: sealed.iv.clone(),
            data: sealed.encrypted_aes_key.clone(),
        }
        .open(shared_secret)?;
        if plain.len() != 32 {
            plain.zeroize();
            return Err(crypto(format!("a key header of {} bytes", plain.len())));
        }
        let mut header = KeyHeader {
            iv: [0; 16],
            key: [0; 16],
        };
        header.iv.copy_from_slice(&plain[..16]);
        header.key.copy_from_slice(&plain[16..]);
        plain.zeroize();
        Ok(header)
    }

    /// `appData.content` of an encrypted file: AES-CBC under the file key with the header IV,
    /// base64.
    pub(crate) fn seal_text(&self, text: &str) -> String {
        STANDARD.encode(aes_cbc_encrypt(&self.key, &self.iv, text.as_bytes()))
    }

    pub(crate) fn open_text(&self, b64: &str) -> Result<String, HomebaseError> {
        let sealed = STANDARD
            .decode(b64)
            .map_err(|_| crypto("the content isn't base64"))?;
        String::from_utf8(aes_cbc_decrypt(&self.key, &self.iv, &sealed)?)
            .map_err(|_| crypto("the content isn't UTF-8"))
    }

    /// The content hash of a payload with `payload_iv`, encrypted under the file key with the
    /// payload IV XOR 0xAA…: 48 bytes, base64.
    pub(crate) fn seal_hash(&self, payload_iv: &[u8; 16], hash: &ContentHash) -> String {
        STANDARD.encode(aes_cbc_encrypt(
            &self.key,
            &hash_iv(payload_iv),
            &hash.bytes,
        ))
    }

    pub(crate) fn open_hash(
        &self,
        payload_iv: &[u8; 16],
        algorithm: HashAlgorithm,
        b64: &str,
    ) -> Result<ContentHash, HomebaseError> {
        let sealed = STANDARD
            .decode(b64)
            .map_err(|_| crypto("the content hash isn't base64"))?;
        let bytes: [u8; 32] = aes_cbc_decrypt(&self.key, &hash_iv(payload_iv), &sealed)?
            .try_into()
            .map_err(|_| crypto("the content hash isn't 32 bytes"))?;
        Ok(ContentHash { algorithm, bytes })
    }
}

fn hash_iv(payload_iv: &[u8; 16]) -> [u8; 16] {
    payload_iv.map(|b| b ^ HASH_IV_MASK)
}

/// 16 random bytes the server takes as a key or IV: not every group of four bytes the same
/// (odin-core `ByteArrayUtil.IsStrongKey`).
pub(crate) fn strong() -> Result<[u8; 16], HomebaseError> {
    loop {
        let bytes = random::<16>()?;
        if bytes.chunks(4).any(|g| g.iter().any(|b| *b != g[0])) {
            return Ok(bytes);
        }
    }
}

fn crypto(why: impl Into<String>) -> HomebaseError {
    HomebaseError::Crypto(why.into())
}

/// The size of `plain` bytes encrypted: PKCS#7 always adds 1 to 16 bytes. Panics past what a
/// payload can hold ([`checked_sealed_len`]): no file is that large.
pub const fn sealed_len(plain: u64) -> u64 {
    match checked_sealed_len(plain) {
        Some(n) => n,
        None => panic!("larger than a payload can be"),
    }
}

/// [`sealed_len`] of a size a stored payload can have: `None` past odin-core's limit, a
/// signed 64-bit `BytesWritten` (**[verified]** `PayloadDescriptor.cs`), where the sum
/// would also overflow.
pub const fn checked_sealed_len(plain: u64) -> Option<u64> {
    match (plain / BLOCK as u64 + 1).checked_mul(BLOCK as u64) {
        Some(n) if n <= i64::MAX as u64 => Some(n),
        _ => None,
    }
}

/// Encrypts what `inner` yields with AES-CBC, PKCS#7 at the end.
pub(crate) struct Encrypting<R> {
    inner: R,
    cipher: cbc::Encryptor<Aes128>,
    /// `buf[at..ready]` is ciphertext to hand out; `buf[ready..ready + carry]` plaintext short
    /// of a whole block, waiting for more.
    buf: Box<[u8]>,
    at: usize,
    ready: usize,
    carry: usize,
    done: bool,
}

impl<R> std::fmt::Debug for Encrypting<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Encrypting(..)")
    }
}

impl<R: Read> Encrypting<R> {
    pub(crate) fn new(inner: R, key: &Key, iv: &[u8; 16]) -> Self {
        Encrypting {
            inner,
            cipher: cbc::Encryptor::new(key.into(), iv.into()),
            // A carried partial block, a chunk read, and room for a padding block.
            buf: vec![0; CHUNK + 2 * BLOCK].into_boxed_slice(),
            at: 0,
            ready: 0,
            carry: 0,
            done: false,
        }
    }

    /// Reads the next chunk after the carried bytes and encrypts its whole blocks in place.
    fn refill(&mut self) -> io::Result<()> {
        self.buf.copy_within(self.ready..self.ready + self.carry, 0);
        let mut len = self.carry;
        let n = loop {
            match self.inner.read(&mut self.buf[len..len + CHUNK]) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => break other?,
            }
        };
        len += n;
        if n == 0 {
            // The end: pad to a whole block (a whole block of padding if already whole).
            let pad = BLOCK - len % BLOCK;
            self.buf[len..len + pad].fill(pad as u8);
            len += pad;
            self.done = true;
        }
        let whole = len / BLOCK * BLOCK;
        let (blocks, _) = Array::slice_as_chunks_mut(&mut self.buf[..whole]);
        self.cipher.encrypt_blocks(blocks);
        (self.at, self.ready, self.carry) = (0, whole, len - whole);
        Ok(())
    }
}

impl<R: Read> Read for Encrypting<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.at == self.ready {
            if self.done {
                return Ok(0);
            }
            self.refill()?;
        }
        let n = buf.len().min(self.ready - self.at);
        buf[..n].copy_from_slice(&self.buf[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Where plaintext `range` of an encrypted payload is in its ciphertext. CBC decrypts any block
/// given the one before it, so the read starts a block early unless it starts at the
/// beginning (then the payload IV is the one before).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CipherRange {
    /// The ciphertext bytes to fetch.
    pub fetch: Range<u64>,
    /// Whether the first fetched block is the IV, not data.
    pub iv_first: bool,
    /// Plaintext bytes to drop before `range.start`.
    pub skip: u64,
}

impl CipherRange {
    /// A range past the plaintext reaches into the padding block, which the reader removes
    /// ([`Decrypting::finish`]); past the stored payload, the server cuts it short.
    pub(crate) fn of(range: &Range<u64>) -> Self {
        let block = BLOCK as u64;
        let first = range.start / block * block;
        let end = range.end.div_ceil(block) * block;
        if first == 0 {
            CipherRange {
                fetch: 0..end,
                iv_first: false,
                skip: range.start,
            }
        } else {
            CipherRange {
                fetch: first - block..end,
                iv_first: true,
                skip: range.start - first,
            }
        }
    }
}

/// Decrypts the ciphertext written to it into `sink`, block by block: `len` plaintext bytes
/// after the first `skip`. With no IV given, the first block written is the IV. The last block
/// waits for [`Decrypting::finish`], which knows whether it is the payload's last, holding the
/// padding; fewer than `len` bytes come out when the plaintext ends first. The content hash,
/// not the padding, tells good bytes from changed ones (CBC can't).
pub(crate) struct Decrypting<'a> {
    key: Key,
    cipher: Option<cbc::Decryptor<Aes128>>,
    sink: &'a mut dyn Write,
    /// Ciphertext short of a whole block (or of the IV), waiting for more.
    carry: Vec<u8>,
    skip: u64,
    left: u64,
}

impl std::fmt::Debug for Decrypting<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decrypting")
            .field("left", &self.left)
            .finish_non_exhaustive()
    }
}

impl Drop for Decrypting<'_> {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl<'a> Decrypting<'a> {
    pub(crate) fn new(
        key: &Key,
        iv: Option<&[u8; 16]>,
        skip: u64,
        len: u64,
        sink: &'a mut dyn Write,
    ) -> Self {
        Decrypting {
            key: *key,
            cipher: iv.map(|iv| cbc::Decryptor::new(key.into(), iv.into())),
            sink,
            carry: Vec::with_capacity(CHUNK + BLOCK),
            skip,
            left: len,
        }
    }

    /// Plaintext bytes asked for that didn't come (past the plaintext's end).
    pub(crate) fn left(&self) -> u64 {
        self.left
    }

    /// After the last ciphertext: decrypts the last block and passes it on, without the
    /// padding when it is the payload's last (`padded`).
    pub(crate) fn finish(&mut self, padded: bool) -> io::Result<()> {
        let bad = |why: &str| io::Error::new(io::ErrorKind::InvalidData, why.to_owned());
        if self.carry.is_empty() && !padded {
            return Ok(());
        }
        let cipher = match (&mut self.cipher, self.carry.len()) {
            (Some(cipher), BLOCK) => cipher,
            _ => return Err(bad("the ciphertext doesn't end on a block")),
        };
        let (blocks, _) = Array::slice_as_chunks_mut(&mut self.carry[..BLOCK]);
        cipher.decrypt_blocks(blocks);
        let kept = match padded {
            true => Pkcs7::raw_unpad(&self.carry)
                .map_err(|_| bad("bad padding (another key, or changed bytes)"))?
                .len(),
            false => BLOCK,
        };
        self.pass_on(kept)?;
        self.carry.clear();
        Ok(())
    }

    /// Writes what `skip` and `left` keep of the first `n` decrypted bytes in `carry`.
    fn pass_on(&mut self, n: usize) -> io::Result<()> {
        let skip = self.skip.min(n as u64) as usize;
        self.skip -= skip as u64;
        let keep = self.left.min((n - skip) as u64) as usize;
        self.sink.write_all(&self.carry[skip..skip + keep])?;
        self.left -= keep as u64;
        Ok(())
    }
}

impl Write for Decrypting<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.carry.extend_from_slice(buf);
        let cipher = match &mut self.cipher {
            Some(cipher) => cipher,
            None if self.carry.len() < BLOCK => return Ok(buf.len()),
            None => {
                let mut iv = [0u8; BLOCK];
                iv.copy_from_slice(&self.carry[..BLOCK]);
                self.carry.drain(..BLOCK);
                self.cipher
                    .insert(cbc::Decryptor::new((&self.key).into(), (&iv).into()))
            }
        };
        let mut whole = self.carry.len() / BLOCK * BLOCK;
        // The last block waits for `finish`: only then is it known to be the last.
        if whole == self.carry.len() {
            whole = whole.saturating_sub(BLOCK);
        }
        let (blocks, _) = Array::slice_as_chunks_mut(&mut self.carry[..whole]);
        cipher.decrypt_blocks(blocks);
        self.pass_on(whole)?;
        self.carry.copy_within(whole.., 0);
        self.carry.truncate(self.carry.len() - whole);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// odin-core's vector of a key header locked with a drive's key
    /// (`ClientLockedKeyHeaderVectorTests`, js-lib `SecurityHelpers.test.ts`).
    #[test]
    fn a_key_header_locks_with_the_drive_key_as_odin_core_does() -> Result<(), HomebaseError> {
        let drive_key: Key = std::array::from_fn(|i| i as u8);
        let kh = KeyHeader {
            iv: std::array::from_fn(|i| 0x10 + i as u8),
            key: std::array::from_fn(|i| 0x20 + i as u8),
        };
        let locked = kh.lock(&drive_key);
        assert_eq!(locked["type"], "zkDrive");
        assert_eq!(locked["iv"], "EBESExQVFhcYGRobHB0eHw==");
        assert_eq!(
            locked["encryptedAesKey"],
            "xqE7N4ePW4JvT4FiocjYeQeFPxlO+hclYcV072RPhmMJbAd9FxFTFUuXv4ql/85L"
        );
        let sealed: SealedKeyHeader = serde_json::from_value(locked)?;
        assert!(sealed.locked_with_drive_key());
        assert_eq!(KeyHeader::open(&sealed, &drive_key)?, kh);
        let numbered: SealedKeyHeader = serde_json::from_value(json!({ "type": 13 }))?;
        assert!(numbered.locked_with_drive_key());
        let aes: SealedKeyHeader = serde_json::from_value(json!({ "type": "aes" }))?;
        assert!(!aes.locked_with_drive_key());
        Ok(())
    }

    fn sample(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + i / 251) as u8).collect()
    }

    fn encrypt(kh: &KeyHeader, iv: &[u8; 16], plain: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // Small reads, so blocks straddle reads.
        let mut reader = Encrypting::new(plain, &kh.key, iv);
        let mut buf = [0u8; 7];
        loop {
            let n = reader.read(&mut buf).unwrap_or(0);
            if n == 0 {
                break out;
            }
            out.extend_from_slice(&buf[..n]);
        }
    }

    #[test]
    fn streaming_encryption_is_plain_cbc() -> Result<(), HomebaseError> {
        let kh = KeyHeader::random()?;
        let iv = strong()?;
        for n in [
            0,
            1,
            15,
            16,
            17,
            31,
            32,
            1000,
            CHUNK - 1,
            CHUNK,
            CHUNK + 5,
            3 * CHUNK,
        ] {
            let plain = sample(n);
            let sealed = encrypt(&kh, &iv, &plain);
            assert_eq!(sealed, aes_cbc_encrypt(&kh.key, &iv, &plain), "{n} bytes");
            assert_eq!(sealed.len() as u64, sealed_len(n as u64));
            assert_eq!(checked_sealed_len(n as u64), Some(sealed_len(n as u64)));
            assert_eq!(aes_cbc_decrypt(&kh.key, &iv, &sealed)?, plain);
        }
        Ok(())
    }

    #[test]
    fn sizes_past_what_a_payload_can_hold_have_no_sealed_size() {
        let largest = i64::MAX as u64 - 16;
        assert_eq!(checked_sealed_len(largest), Some(i64::MAX as u64 - 15));
        for size in [largest + 1, i64::MAX as u64, u64::MAX - 15, u64::MAX] {
            assert_eq!(checked_sealed_len(size), None, "{size}");
        }
    }

    #[test]
    fn every_range_decrypts_like_the_whole() -> Result<(), HomebaseError> {
        let kh = KeyHeader::random()?;
        let iv = strong()?;
        for n in [0usize, 1, 16, 33, 100] {
            let plain = sample(n);
            let sealed = aes_cbc_encrypt(&kh.key, &iv, &plain);
            // Ranges that run past the plaintext, up to the padded end, too.
            for start in 0..=n {
                for end in start..=sealed.len() {
                    let range = start as u64..end as u64;
                    let at = CipherRange::of(&range);
                    let fetched = &sealed[at.fetch.start as usize..at.fetch.end as usize];
                    let mut out = Vec::new();
                    let first_iv = (!at.iv_first).then_some(&iv);
                    let mut d = Decrypting::new(
                        &kh.key,
                        first_iv,
                        at.skip,
                        end as u64 - start as u64,
                        &mut out,
                    );
                    // Written in odd pieces, as a network delivers them.
                    for piece in fetched.chunks(5) {
                        d.write_all(piece).map_err(HomebaseError::Io)?;
                    }
                    d.finish(at.fetch.end as usize == sealed.len())
                        .map_err(HomebaseError::Io)?;
                    let there = end.min(n);
                    assert_eq!(d.left(), (end - there) as u64, "{n}: {range:?}");
                    drop(d);
                    assert_eq!(out, plain[start..there], "{n}: {range:?}");
                }
            }
            // A changed byte before the last block changes the padding: refused. (Flipping the
            // top bit of the byte that feeds the pad length always makes it over 16.)
            let (mut changed, mut changed_iv) = (sealed.clone(), iv);
            match sealed.len().checked_sub(BLOCK + 1) {
                Some(i) => changed[i] ^= 0x80,
                None => changed_iv[BLOCK - 1] ^= 0x80,
            }
            let mut out = Vec::new();
            let mut d = Decrypting::new(&kh.key, Some(&changed_iv), 0, n as u64, &mut out);
            d.write_all(&changed).map_err(HomebaseError::Io)?;
            assert!(d.finish(true).is_err(), "{n}");
        }
        Ok(())
    }

    #[test]
    fn key_headers_text_and_hashes_round_trip() -> Result<(), HomebaseError> {
        let shared = random::<16>()?;
        let kh = KeyHeader::random()?;
        let wrap = strong()?;
        let sealed: SealedKeyHeader = serde_json::from_value(kh.seal(&shared, &wrap))?;
        assert_eq!(KeyHeader::open(&sealed, &shared)?, kh);
        // A wrong shared secret never gives the key back.
        assert!(KeyHeader::open(&sealed, &random::<16>()?).map_or(true, |k| k != kh));
        // As sync keeps it, a token: the same key back.
        let token = sealed.to_token().ok_or_else(|| crypto("no token"))?;
        let back = SealedKeyHeader::from_token(&token).ok_or_else(|| crypto("not one"))?;
        assert_eq!(KeyHeader::open(&back, &shared)?, kh);
        assert!(SealedKeyHeader::default().to_token().is_none());
        // The token a write computes is the server's, sealed with the header's own IV.
        let server: SealedKeyHeader = serde_json::from_value(kh.seal(&shared, &kh.iv))?;
        assert_eq!(server.to_token(), Some(kh.token(&shared)));

        // The response header's binary form: IV, encrypted key, version.
        let mut binary = STANDARD.decode(&sealed.iv).map_err(|_| crypto("iv"))?;
        binary.extend(
            STANDARD
                .decode(&sealed.encrypted_aes_key)
                .map_err(|_| crypto("key"))?,
        );
        binary.extend(1i32.to_le_bytes());
        let from_header = SealedKeyHeader::from_base64(&STANDARD.encode(binary))?;
        assert_eq!(KeyHeader::open(&from_header, &shared)?, kh);

        let text = "{\"v\":1,\"name\":\"Résumé.txt\"}";
        assert_eq!(kh.open_text(&kh.seal_text(text))?, text);

        let hash = hbsync_fileio::hash_bytes(b"x", HashAlgorithm::Blake3);
        let payload_iv = strong()?;
        let sealed_hash = kh.seal_hash(&payload_iv, &hash);
        assert_eq!(
            STANDARD
                .decode(&sealed_hash)
                .map_err(|_| crypto("b64"))?
                .len(),
            48
        );
        assert_eq!(
            kh.open_hash(&payload_iv, HashAlgorithm::Blake3, &sealed_hash)?,
            hash
        );
        // Bound to the payload IV: under the header IV it doesn't open to the hash.
        assert!(
            kh.open_hash(&strong()?, HashAlgorithm::Blake3, &sealed_hash)
                .map_or(true, |h| h != hash)
        );
        Ok(())
    }

    /// The same bytes as odin-js: `docs/spec/fixtures/encryption-odin-js.json`, made with
    /// js-lib's own functions.
    #[test]
    fn matches_odin_js() -> Result<(), Box<dyn std::error::Error>> {
        let path = format!(
            "{}/../../docs/spec/fixtures/encryption-odin-js.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let v: Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
        let bytes = |name: &str| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            let hex = v[name].as_str().ok_or(name.to_owned())?;
            (0..hex.len())
                .step_by(2)
                .map(|i| Ok(u8::from_str_radix(&hex[i..i + 2], 16)?))
                .collect()
        };
        let block = |name: &str| -> Result<[u8; 16], Box<dyn std::error::Error>> {
            Ok(bytes(name)?.try_into().map_err(|_| name.to_owned())?)
        };
        let shared = block("sharedSecret")?;
        let kh = KeyHeader {
            iv: block("headerIv")?,
            key: block("key")?,
        };
        let sealed = kh.seal(&shared, &block("transferIv")?);
        assert_eq!(
            sealed["encryptedAesKey"],
            STANDARD.encode(bytes("encryptedAesKey")?)
        );
        assert_eq!(
            kh.seal_text(v["content"].as_str().ok_or("content")?),
            v["sealedContentBase64"]
        );
        let payload_iv = block("payloadIv")?;
        assert_eq!(
            encrypt(&kh, &payload_iv, &bytes("payload")?),
            bytes("sealedPayload")?
        );

        // A payload response's key header, and odin-js's ranged read of it.
        let header = SealedKeyHeader::from_base64(v["payloadKeyHeader64"].as_str().ok_or("h")?)?;
        let opened = KeyHeader::open(&header, &shared)?;
        assert_eq!((opened.iv, opened.key), (payload_iv, kh.key));
        let r = &v["range"];
        let field = |name: &str| r[name].as_u64().ok_or(name.to_owned());
        let range = field("start")?..field("end")?;
        let at = CipherRange::of(&range);
        assert_eq!(
            at.fetch,
            field("fetchStart")?..field("fetchEndInclusive")? + 1
        );
        let sealed_payload = bytes("sealedPayload")?;
        let mut out = Vec::new();
        let mut d = Decrypting::new(&kh.key, None, at.skip, range.end - range.start, &mut out);
        d.write_all(&sealed_payload[at.fetch.start as usize..at.fetch.end as usize])?;
        d.finish(at.fetch.end as usize == sealed_payload.len())?;
        drop(d);
        let want = r["plain"].as_str().ok_or("plain")?;
        assert_eq!(
            out.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            want
        );
        Ok(())
    }

    /// Throughput of the stream cipher, next to hashing (what an upload also does):
    /// `cargo test --release -p hbsync-homebase throughput -- --ignored --nocapture`.
    #[test]
    #[ignore = "a measurement"]
    fn throughput() -> Result<(), Box<dyn std::error::Error>> {
        const SIZE: u64 = 1 << 30;
        let kh = KeyHeader::random()?;
        let iv = strong()?;
        let rate = |what: &str, started: std::time::Instant| {
            let secs = started.elapsed().as_secs_f64();
            eprintln!("{what:<34} {:>7.0} MB/s", SIZE as f64 / secs / 1e6);
        };
        let source = || io::repeat(0x5a).take(SIZE);

        let started = std::time::Instant::now();
        io::copy(
            &mut Encrypting::new(source(), &kh.key, &iv),
            &mut io::sink(),
        )?;
        rate("encrypt (stream)", started);

        let started = std::time::Instant::now();
        let sealing = Encrypting::new(source(), &kh.key, &iv);
        hbsync_fileio::hash_reader(sealing, HashAlgorithm::Blake3)?;
        rate("encrypt + BLAKE3 (upload pass 1)", started);

        let started = std::time::Instant::now();
        hbsync_fileio::hash_reader(source(), HashAlgorithm::Blake3)?;
        rate("BLAKE3 alone", started);
        let started = std::time::Instant::now();
        hbsync_fileio::hash_reader(source(), HashAlgorithm::Sha256)?;
        rate("SHA-256 alone", started);

        // The crates alone, in place on one buffer: the ceiling for the streams above.
        let mut buf = vec![0x33u8; CHUNK];
        let mut cipher = cbc::Encryptor::<Aes128>::new((&kh.key).into(), (&iv).into());
        let started = std::time::Instant::now();
        for _ in 0..SIZE / CHUNK as u64 {
            cipher.encrypt_blocks(Array::slice_as_chunks_mut(&mut buf).0);
        }
        rate("encrypt (cbc crate, in place)", started);
        let mut cipher = cbc::Decryptor::<Aes128>::new((&kh.key).into(), (&iv).into());
        let started = std::time::Instant::now();
        for _ in 0..SIZE / CHUNK as u64 {
            cipher.decrypt_blocks(Array::slice_as_chunks_mut(&mut buf).0);
        }
        rate("decrypt (cbc crate, in place)", started);

        // Decrypting what arrives in 64 KiB pieces, as a download does.
        let sealed = vec![0x33u8; CHUNK];
        let mut sink = io::sink();
        let mut d = Decrypting::new(&kh.key, Some(&iv), 0, SIZE, &mut sink);
        let started = std::time::Instant::now();
        for _ in 0..SIZE / CHUNK as u64 {
            d.write_all(&sealed)?;
        }
        rate("decrypt (stream)", started);
        Ok(())
    }

    #[test]
    fn new_keys_and_ivs_are_strong() -> Result<(), HomebaseError> {
        let kh = KeyHeader::random()?;
        let next = kh.with_new_iv()?;
        assert_eq!(next.key, kh.key);
        assert_ne!(next.iv, kh.iv);
        let weak = |b: &[u8; 16]| b.chunks(4).all(|g| g.iter().all(|x| *x == g[0]));
        assert!(!weak(&kh.iv) && !weak(&kh.key) && !weak(&next.iv));
        Ok(())
    }
}
