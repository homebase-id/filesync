//! Homebase file encryption (`docs/spec/encryption.md`): one random AES-128 key per file,
//! AES-CBC with PKCS#7, a random IV for the header and for every payload upload, as odin-js
//! and chat-kmp do it. Verified in odin-core `KeyHeader`, `EncryptedKeyHeader`, `PayloadHash`,
//! `FileSystemUpdateWriterBase`, and odin-js `UploadHelpers`, `SecurityHelpers`.

use std::io::{self, Read, Write};
use std::ops::Range;

use aes::Aes128;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cbc::cipher::{Array, BlockModeDecrypt, BlockModeEncrypt, KeyIvInit};
use hbsync_types::{ContentHash, HashAlgorithm};
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroize;

use crate::HomebaseError;
use crate::crypto::{Key, aes_cbc_decrypt, aes_cbc_encrypt, random};

const BLOCK: usize = 16;

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
/// (`sharedSecretEncryptedKeyHeader`) and an upload sends it (`encryptedKeyHeader`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SealedKeyHeader {
    #[serde(default)]
    pub iv: String,
    #[serde(default)]
    pub encrypted_aes_key: String,
}

impl SealedKeyHeader {
    /// The binary form of the `SharedSecretEncryptedHeader64` response header: IV (16),
    /// encrypted key (48), version (4), base64 (odin-core `EncryptedKeyHeader.ToBase64`).
    pub fn from_base64(b64: &str) -> Result<Self, HomebaseError> {
        let bytes = STANDARD
            .decode(b64.trim())
            .map_err(|_| crypto("the key header isn't base64"))?;
        if bytes.len() != 16 + 48 + 4 {
            return Err(crypto(format!("a key header of {} bytes", bytes.len())));
        }
        Ok(SealedKeyHeader {
            iv: STANDARD.encode(&bytes[..16]),
            encrypted_aes_key: STANDARD.encode(&bytes[16..64]),
        })
    }
}

impl KeyHeader {
    /// A new key and header IV, for a new file.
    pub fn random() -> Result<Self, HomebaseError> {
        Ok(KeyHeader {
            iv: strong()?,
            key: strong()?,
        })
    }

    /// The same key with a new header IV: what an update sends. The server keeps the file's
    /// key and takes the IV (odin-core `OverwriteMetadataInternal`).
    pub fn with_new_iv(&self) -> Result<Self, HomebaseError> {
        Ok(KeyHeader {
            iv: strong()?,
            key: self.key,
        })
    }

    /// `iv || key`, AES-CBC under `shared_secret` with `wrap_iv`, as an upload sends it.
    pub fn seal(&self, shared_secret: &Key, wrap_iv: &[u8; 16]) -> Value {
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

    /// The key header an unencrypted file sends, since the server wants one anyway: all
    /// zeros, sealed the same way (as chat-kmp does).
    pub fn seal_none(shared_secret: &Key, wrap_iv: &[u8; 16]) -> Value {
        KeyHeader {
            iv: [0; 16],
            key: [0; 16],
        }
        .seal(shared_secret, wrap_iv)
    }

    /// Opens a key header sealed with `shared_secret`.
    pub fn open(sealed: &SealedKeyHeader, shared_secret: &Key) -> Result<Self, HomebaseError> {
        let decode = |b64: &str| {
            STANDARD
                .decode(b64)
                .map_err(|_| crypto("the key header isn't base64"))
        };
        let mut plain = aes_cbc_decrypt(
            shared_secret,
            &decode(&sealed.iv)?,
            &decode(&sealed.encrypted_aes_key)?,
        )?;
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
    pub fn seal_text(&self, text: &str) -> String {
        STANDARD.encode(aes_cbc_encrypt(&self.key, &self.iv, text.as_bytes()))
    }

    pub fn open_text(&self, b64: &str) -> Result<String, HomebaseError> {
        let sealed = STANDARD
            .decode(b64)
            .map_err(|_| crypto("the content isn't base64"))?;
        String::from_utf8(aes_cbc_decrypt(&self.key, &self.iv, &sealed)?)
            .map_err(|_| crypto("the content isn't UTF-8"))
    }

    /// The content hash of a payload with `payload_iv`, encrypted under the file key with the
    /// payload IV XOR 0xAA…: 48 bytes, base64.
    pub fn seal_hash(&self, payload_iv: &[u8; 16], hash: &ContentHash) -> String {
        STANDARD.encode(aes_cbc_encrypt(
            &self.key,
            &hash_iv(payload_iv),
            &hash.bytes,
        ))
    }

    pub fn open_hash(
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
pub fn strong() -> Result<[u8; 16], HomebaseError> {
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

/// The size of `plain` bytes encrypted: PKCS#7 always adds 1 to 16 bytes.
pub fn sealed_len(plain: u64) -> u64 {
    (plain / BLOCK as u64 + 1) * BLOCK as u64
}

/// Encrypts what `inner` yields with AES-CBC, PKCS#7 at the end.
pub struct Encrypting<R> {
    inner: R,
    cipher: cbc::Encryptor<Aes128>,
    /// Ciphertext ready to hand out, from `at`.
    out: Vec<u8>,
    at: usize,
    /// Plaintext short of a whole block, waiting for more.
    carry: Vec<u8>,
    done: bool,
}

impl<R> std::fmt::Debug for Encrypting<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Encrypting(..)")
    }
}

impl<R: Read> Encrypting<R> {
    pub fn new(inner: R, key: &Key, iv: &[u8; 16]) -> Self {
        Encrypting {
            inner,
            cipher: cbc::Encryptor::new(key.into(), iv.into()),
            out: Vec::with_capacity(CHUNK + BLOCK),
            at: 0,
            carry: Vec::with_capacity(BLOCK),
            done: false,
        }
    }

    /// Encrypts the next chunk into `out`.
    fn refill(&mut self) -> io::Result<()> {
        let mut plain = std::mem::take(&mut self.carry);
        plain.resize(plain.len() + CHUNK, 0);
        let start = plain.len() - CHUNK;
        let n = loop {
            match self.inner.read(&mut plain[start..]) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => break other?,
            }
        };
        plain.truncate(start + n);
        if n == 0 {
            // The end: pad to a whole block (a whole block of padding if already whole).
            let pad = BLOCK - plain.len() % BLOCK;
            plain.resize(plain.len() + pad, pad as u8);
            self.done = true;
        }
        let whole = plain.len() / BLOCK * BLOCK;
        self.carry.extend_from_slice(&plain[whole..]);
        plain.truncate(whole);
        let (blocks, _) = Array::slice_as_chunks_mut(&mut plain);
        self.cipher.encrypt_blocks(blocks);
        self.out = plain;
        self.at = 0;
        Ok(())
    }
}

impl<R: Read> Read for Encrypting<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.at == self.out.len() {
            if self.done {
                return Ok(0);
            }
            self.refill()?;
        }
        let n = buf.len().min(self.out.len() - self.at);
        buf[..n].copy_from_slice(&self.out[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Where plaintext `range` of an encrypted payload is in its ciphertext. CBC decrypts any block
/// given the one before it, so the read starts a block early unless it starts at the
/// beginning (then the payload IV is the one before).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CipherRange {
    /// The ciphertext bytes to fetch.
    pub fetch: Range<u64>,
    /// Whether the first fetched block is the IV, not data.
    pub iv_first: bool,
    /// Plaintext bytes to drop before `range.start`.
    pub skip: u64,
}

impl CipherRange {
    /// Requires `range.end` ≤ the plaintext size: the padding block is never needed then, as
    /// the caller knows where the plaintext ends.
    pub fn of(range: &Range<u64>) -> Self {
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
/// after the first `skip`. With no IV given, the first block written is the IV. The padding is
/// never checked: the content hash is (CBC can't tell changed bytes from good ones).
pub struct Decrypting<'a> {
    key: Key,
    cipher: Option<cbc::Decryptor<Aes128>>,
    sink: &'a mut dyn Write,
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
    pub fn new(
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

    /// Plaintext bytes still to come.
    pub fn left(&self) -> u64 {
        self.left
    }
}

impl Write for Decrypting<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.carry.extend_from_slice(buf);
        if self.cipher.is_none() {
            if self.carry.len() < BLOCK {
                return Ok(buf.len());
            }
            let iv: [u8; 16] = self.carry[..BLOCK].try_into().unwrap_or_default();
            self.cipher = Some(cbc::Decryptor::new((&self.key).into(), (&iv).into()));
            self.carry.drain(..BLOCK);
        }
        let whole = self.carry.len() / BLOCK * BLOCK;
        if whole == 0 {
            return Ok(buf.len());
        }
        let mut plain: Vec<u8> = self.carry.drain(..whole).collect();
        if let Some(cipher) = &mut self.cipher {
            let (blocks, _) = Array::slice_as_chunks_mut(&mut plain);
            cipher.decrypt_blocks(blocks);
        }
        let mut out = &plain[..];
        let skip = self.skip.min(out.len() as u64) as usize;
        out = &out[skip..];
        self.skip -= skip as u64;
        let keep = self.left.min(out.len() as u64) as usize;
        self.sink.write_all(&out[..keep])?;
        self.left -= keep as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(aes_cbc_decrypt(&kh.key, &iv, &sealed)?, plain);
        }
        Ok(())
    }

    #[test]
    fn every_range_decrypts_like_the_whole() -> Result<(), HomebaseError> {
        let kh = KeyHeader::random()?;
        let iv = strong()?;
        for n in [0usize, 1, 16, 33, 100] {
            let plain = sample(n);
            let sealed = aes_cbc_encrypt(&kh.key, &iv, &plain);
            for start in 0..=n {
                for end in start..=n {
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
                    assert_eq!(d.left(), 0, "{n}: {range:?}");
                    drop(d);
                    assert_eq!(out, plain[start..end], "{n}: {range:?}");
                }
            }
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
        drop(d);
        let want = r["plain"].as_str().ok_or("plain")?;
        assert_eq!(
            out.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            want
        );
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
