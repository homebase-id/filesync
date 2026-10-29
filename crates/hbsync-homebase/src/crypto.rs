//! The cryptography of talking to Homebase: the YouAuth key exchange (P-384 ECDH, then
//! HKDF-SHA256 to a 16-byte key) and the shared-secret envelope (AES-128-CBC, PKCS#7), both as
//! odin-core does them (verified in `EccKeyData.GetEcdhSharedSecret`, `HashUtil.Hkdf`,
//! `AesCbc`, `SharedSecretEncryptedPayload`); and the zero-knowledge keys' two forms, a value
//! locked with a key and one sealed to a public key (odin-core `docs/owner-keys.md`, `ZkSeal`).

use aes::Aes128;
use aes_gcm::aead::Aead;
use aes_gcm::{Aes128Gcm, KeyInit};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockModeDecrypt, BlockModeEncrypt, KeyIvInit};
use p384::elliptic_curve::sec1::ToSec1Point;
use p384::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};

use crate::HomebaseError;

/// A 16-byte AES key: the YouAuth exchange key, or the shared secret of a client.
pub(crate) type Key = [u8; 16];

pub(crate) fn random<const N: usize>() -> Result<[u8; N], HomebaseError> {
    let mut out = [0u8; N];
    getrandom::fill(&mut out).map_err(|e| HomebaseError::Crypto(format!("no randomness: {e}")))?;
    Ok(out)
}

/// One side of the YouAuth key exchange.
pub(crate) struct Exchange {
    secret: SecretKey,
}

impl std::fmt::Debug for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Exchange(..)")
    }
}

#[derive(Serialize, Deserialize)]
struct Jwk {
    kty: String,
    crv: String,
    x: String,
    y: String,
}

impl Exchange {
    pub(crate) fn new() -> Result<Self, HomebaseError> {
        loop {
            // Almost every 48-byte value is a valid scalar; retry the rare one that isn't.
            if let Ok(secret) = SecretKey::from_slice(&random::<48>()?) {
                return Ok(Exchange { secret });
            }
        }
    }

    /// The public key as YouAuth sends it: a JWK `{kty, crv, x, y}`, base64url (no padding)
    /// as a whole.
    pub(crate) fn public_jwk_base64url(&self) -> String {
        let point = self.secret.public_key().to_sec1_point(false);
        let (x, y) = (point.x(), point.y());
        let jwk = Jwk {
            kty: "EC".into(),
            crv: "P-384".into(),
            x: URL_SAFE_NO_PAD.encode(x.map(|b| b.as_slice()).unwrap_or_default()),
            y: URL_SAFE_NO_PAD.encode(y.map(|b| b.as_slice()).unwrap_or_default()),
        };
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&jwk).unwrap_or_default())
    }

    /// The exchange key both sides derive: ECDH with the peer's JWK, the x coordinate at its
    /// full 48 bytes, then HKDF-SHA256 with `salt` and no info, 16 bytes.
    pub(crate) fn derive(
        &self,
        peer_jwk_base64url: &str,
        salt: &[u8],
    ) -> Result<Key, HomebaseError> {
        let bad = |what: &str| HomebaseError::Crypto(format!("peer public key: {what}"));
        let json = URL_SAFE_NO_PAD
            .decode(peer_jwk_base64url.trim_end_matches('='))
            .map_err(|_| bad("not base64url"))?;
        let jwk: Jwk = serde_json::from_slice(&json).map_err(|_| bad("not a JWK"))?;
        if jwk.kty != "EC" || jwk.crv != "P-384" {
            return Err(bad("not a P-384 key"));
        }
        let coord = |c: &str| -> Result<[u8; 48], HomebaseError> {
            let bytes = URL_SAFE_NO_PAD
                .decode(c.trim_end_matches('='))
                .map_err(|_| bad("bad coordinate"))?;
            // Left-pad a coordinate sent without its leading zero bytes.
            let mut out = [0u8; 48];
            let at = 48usize
                .checked_sub(bytes.len())
                .ok_or_else(|| bad("coordinate too long"))?;
            out[at..].copy_from_slice(&bytes);
            Ok(out)
        };
        // Uncompressed SEC1: 0x04 | x | y.
        let mut sec1 = vec![0x04];
        sec1.extend_from_slice(&coord(&jwk.x)?);
        sec1.extend_from_slice(&coord(&jwk.y)?);
        let peer = PublicKey::from_sec1_bytes(&sec1).map_err(|_| bad("not on the curve"))?;
        self.agree(&peer, Some(salt), b"")
    }

    /// A value sealed to this key (`ZkSeal` v1): base64 of `0x01`, the key id (4), a throwaway
    /// P-384 public key (97, uncompressed), a 12-byte IV, then AES-GCM's ciphertext and tag,
    /// under HKDF-SHA256 of the ECDH secret with no salt and the info `homebase-zk-seal`.
    pub(crate) fn unseal(&self, sealed_b64: &str) -> Result<Vec<u8>, HomebaseError> {
        let bad = |what: &str| HomebaseError::Crypto(format!("sealed value: {what}"));
        let bytes = STANDARD.decode(sealed_b64).map_err(|_| bad("not base64"))?;
        if bytes.len() < 1 + 4 + 97 + 12 + 16 || bytes[0] != 1 {
            return Err(bad("not a seal of version 1"));
        }
        let throwaway =
            PublicKey::from_sec1_bytes(&bytes[5..102]).map_err(|_| bad("not a P-384 key"))?;
        let key = self.agree(&throwaway, None, b"homebase-zk-seal")?;
        gcm_open(&key, &bytes[102..114], &bytes[114..])
    }

    /// ECDH with `peer`, the x coordinate at its full 48 bytes, then HKDF-SHA256 to 16 bytes.
    fn agree(
        &self,
        peer: &PublicKey,
        salt: Option<&[u8]>,
        info: &[u8],
    ) -> Result<Key, HomebaseError> {
        let shared = p384::ecdh::diffie_hellman(self.secret.to_nonzero_scalar(), peer.as_affine());
        let mut key = [0u8; 16];
        hkdf::Hkdf::<sha2::Sha256>::new(salt, shared.raw_secret_bytes())
            .expand(info, &mut key)
            .map_err(|_| HomebaseError::Crypto("HKDF output length".into()))?;
        Ok(key)
    }
}

/// A value locked with `key` (`zkLock`): base64 of a 12-byte IV, then AES-GCM's ciphertext
/// and tag.
pub(crate) fn zk_unlock(key: &Key, locked_b64: &str) -> Result<Vec<u8>, HomebaseError> {
    let bytes = STANDARD
        .decode(locked_b64)
        .map_err(|_| HomebaseError::Crypto("locked value: not base64".into()))?;
    if bytes.len() < 12 + 16 {
        return Err(HomebaseError::Crypto("locked value: too short".into()));
    }
    gcm_open(key, &bytes[..12], &bytes[12..])
}

fn gcm_open(key: &Key, iv: &[u8], sealed: &[u8]) -> Result<Vec<u8>, HomebaseError> {
    let nonce = iv
        .try_into()
        .map_err(|_| HomebaseError::Crypto("AES-GCM: the IV isn't 12 bytes".into()))?;
    Aes128Gcm::new(key.into())
        .decrypt(nonce, sealed)
        .map_err(|_| HomebaseError::Crypto("AES-GCM: wrong key or damaged data".into()))
}

pub(crate) fn aes_cbc_encrypt(key: &Key, iv: &[u8; 16], data: &[u8]) -> Vec<u8> {
    cbc::Encryptor::<Aes128>::new(key.into(), iv.into()).encrypt_padded_vec::<Pkcs7>(data)
}

pub(crate) fn aes_cbc_decrypt(key: &Key, iv: &[u8], data: &[u8]) -> Result<Vec<u8>, HomebaseError> {
    let iv: [u8; 16] = iv
        .try_into()
        .map_err(|_| HomebaseError::Crypto("IV must be 16 bytes".into()))?;
    cbc::Decryptor::<Aes128>::new(key.into(), (&iv).into())
        .decrypt_padded_vec::<Pkcs7>(data)
        .map_err(|_| HomebaseError::Crypto("AES-CBC: wrong key or damaged data".into()))
}

/// The shared-secret envelope every API request and encrypted response uses: `{"iv", "data"}`,
/// both standard base64.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Envelope {
    pub iv: String,
    pub data: String,
}

impl Envelope {
    pub(crate) fn seal(key: &Key, plaintext: &[u8]) -> Result<Self, HomebaseError> {
        let iv = random::<16>()?;
        Ok(Envelope {
            iv: STANDARD.encode(iv),
            data: STANDARD.encode(aes_cbc_encrypt(key, &iv, plaintext)),
        })
    }

    pub(crate) fn open(&self, key: &Key) -> Result<Vec<u8>, HomebaseError> {
        let bad = |_| HomebaseError::Crypto("envelope: not base64".into());
        aes_cbc_decrypt(
            key,
            &STANDARD.decode(&self.iv).map_err(bad)?,
            &STANDARD.decode(&self.data).map_err(bad)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_sides_derive_the_same_key() -> Result<(), HomebaseError> {
        let (a, b) = (Exchange::new()?, Exchange::new()?);
        let salt = random::<16>()?;
        let ka = a.derive(&b.public_jwk_base64url(), &salt)?;
        let kb = b.derive(&a.public_jwk_base64url(), &salt)?;
        assert_eq!(ka, kb);
        assert_ne!(ka, a.derive(&b.public_jwk_base64url(), &random::<16>()?)?);
        Ok(())
    }

    /// odin-core's vector, sealed by the browser (`RecoveryPhraseTests.TheSharedSealVectorOpens`,
    /// js-lib `ZeroKnowledgeKeys.test.ts`).
    #[test]
    fn the_shared_seal_vector_opens() -> Result<(), HomebaseError> {
        let pkcs8 = STANDARD.decode(
            "MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDAdIBNZdX4cc2HkbmyjAsYLgBCJOD4PKl+19MNegdIFHBNvBHXjKKPd1U43jVFC4W2hZANiAAQh7s8LrEkJbWspBmjbFT45ZclVZc9go3WGP86OB1nf16QB+2o3yNTYCg0YJ4Atpknyf0xxJGe5a/LTuILGNwlUaPDmFSyzZ4ALW18WutsGr8ItoW2/V6/fK5tpF8iT70k=",
        ).map_err(|_| HomebaseError::Crypto("vector".into()))?;
        // The private scalar: the 48 bytes after the SEC1 key's `04 30`.
        let at = pkcs8
            .windows(2)
            .position(|w| w == [0x04, 0x30])
            .ok_or_else(|| HomebaseError::Crypto("vector".into()))?
            + 2;
        let secret = SecretKey::from_slice(&pkcs8[at..at + 48])
            .map_err(|_| HomebaseError::Crypto("vector".into()))?;
        let sealed = "AYGsbtYEJzx9/APLDFRU02ky+GZwV8b3H7z+kwGsq61keI1Spr1TwEe4xemX1gXqt4ebsODaFTM9CTX1ZzYz8AEOKmwRfyCxFiAYyUY9XK31wB7oShI/GiQX6RjUGj2KekD+/f9GXM+hXaDl9UqGteiAsqESPMoIVY4iVeGO3GC/P2Dy4Igq2MsVPmZeNSxogKR0ZeC+rPTMjyeOkbM=";
        assert_eq!(
            Exchange { secret }.unseal(sealed)?,
            b"correct horse battery staple"
        );
        assert!(Exchange::new()?.unseal(sealed).is_err());
        Ok(())
    }

    /// odin-core's vector of a locked value (`ZeroKnowledgeKeysTests.TheSharedVectorOpens`: the
    /// zero-knowledge master key under the second password key).
    #[test]
    fn the_shared_lock_vector_opens() -> Result<(), HomebaseError> {
        let key: Key = STANDARD
            .decode("+pL3zwkIcDcDpYKkLbVc4A==")
            .ok()
            .and_then(|k| k.try_into().ok())
            .ok_or_else(|| HomebaseError::Crypto("vector".into()))?;
        let locked = "ICEiIyQlJicoKSorjWGICGzIuJPS3SPnEBWvlaM3EZ0jd9msGp8vJrhDYXA=";
        assert_eq!(
            STANDARD.encode(zk_unlock(&key, locked)?),
            "EBESExQVFhcYGRobHB0eHw=="
        );
        assert!(zk_unlock(&[0; 16], locked).is_err());
        Ok(())
    }

    #[test]
    fn envelope_round_trip_and_wrong_key() -> Result<(), HomebaseError> {
        let key = random::<16>()?;
        let sealed = Envelope::seal(&key, b"{\"a\":1}")?;
        assert_eq!(sealed.open(&key)?, b"{\"a\":1}");
        // A wrong key fails, or (when the garbage happens to end in valid padding, about 1 in
        // 256) gives other bytes.
        let wrong = sealed.open(&random::<16>()?);
        assert!(wrong.map_or(true, |bytes| bytes != b"{\"a\":1}"));
        Ok(())
    }

    #[test]
    fn aes_cbc_matches_a_known_vector() -> Result<(), HomebaseError> {
        // NIST SP 800-38A F.2.1 (CBC-AES128), first block, plus the PKCS#7 padding block.
        let key: Key = [
            0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf,
            0x4f, 0x3c,
        ];
        let iv: [u8; 16] = std::array::from_fn(|i| i as u8);
        let plain = [
            0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93,
            0x17, 0x2a,
        ];
        let sealed = aes_cbc_encrypt(&key, &iv, &plain);
        assert_eq!(
            sealed[..16],
            [
                0x76, 0x49, 0xab, 0xac, 0x81, 0x19, 0xb2, 0x46, 0xce, 0xe9, 0x8e, 0x9b, 0x12, 0xe9,
                0x19, 0x7d
            ]
        );
        assert_eq!(sealed.len(), 32);
        assert_eq!(aes_cbc_decrypt(&key, &iv, &sealed)?, plain);
        Ok(())
    }
}
