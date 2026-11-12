//! The keys of the files others share with this identity: Selective Share's blobs
//! (`docs/spec/encryption.md`, "Files shared with you"; **[verified]** against odin-core's
//! golden vector `SelectiveShareKeyVectorTests`, which chat-kmp's `ShareKeys` also checks).

use uuid::Uuid;
use zeroize::Zeroizing;

use crate::HomebaseError;
use crate::crypto::{Key, WriteOnlyKey, gcm_open, key_of};

/// The `ZkSeal` version of a sealed circle key.
const SEALED_VERSION: u8 = 2;
/// The format of a locked file key bound to its file id.
const BY_FILE_ID: u8 = 2;
/// The format of a locked file key bound to its global transit id (one a member added, whose
/// file id the server picks).
const BY_GLOBAL_TRANSIT_ID: u8 = 3;

/// A circle's key, with the circle and the drive it shares, which every file key under it is
/// bound to.
pub(crate) struct CircleKey {
    key: Zeroizing<Key>,
    circle: Uuid,
    drive: Uuid,
}

impl std::fmt::Debug for CircleKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CircleKey({}, ..)", self.circle)
    }
}

impl CircleKey {
    /// The key of `circle` a sharer sealed to `member`'s drive (`sealedShareKey`), opened with
    /// that drive's write-only key: AAD `circleId ‖` the UTF-8 of the member's lowercased
    /// identity. `drive` is the sharer's drive (its alias) the circle shares: the seal doesn't
    /// bind it, so a wrong one shows only as file keys that don't open.
    pub(crate) fn unseal(
        sealed: &[u8],
        write_only: &WriteOnlyKey,
        circle: Uuid,
        drive: Uuid,
        member: &str,
    ) -> Result<CircleKey, HomebaseError> {
        // A domain is ASCII (an international one in punycode), which .NET's
        // `ToLowerInvariant` lowercases as this does; another name would never open.
        if !member.is_ascii() {
            return Err(HomebaseError::Protocol(format!(
                "{member:?} isn't an identity in ASCII (punycode)"
            )));
        }
        let mut aad = circle.to_bytes_le().to_vec();
        aad.extend_from_slice(member.to_ascii_lowercase().as_bytes());
        let opened = write_only.unseal(sealed, SEALED_VERSION, &aad)?;
        Ok(CircleKey {
            key: Zeroizing::new(key_of(&opened, "a circle's key")?),
            circle,
            drive,
        })
    }

    /// A file's key from its share row (`lockedFileKey`): `format ‖ nonce (12) ‖ ciphertext (16)
    /// ‖ tag (16)` under this key, bound to `drive ‖ file_id ‖ circle` (format 2) or
    /// `drive ‖ global_transit_id ‖ circle` (format 3).
    pub(crate) fn file_key(
        &self,
        locked: &[u8],
        file_id: Uuid,
        global_transit_id: Option<Uuid>,
    ) -> Result<Key, HomebaseError> {
        let bad = |why: &str| HomebaseError::Protocol(format!("a locked file key: {why}"));
        if locked.len() != 1 + 12 + 16 + 16 {
            return Err(bad("not 45 bytes"));
        }
        let id = match locked[0] {
            BY_FILE_ID => file_id,
            BY_GLOBAL_TRANSIT_ID => {
                global_transit_id.ok_or_else(|| bad("bound to a global transit id it lacks"))?
            }
            _ => return Err(bad("of an unknown format")),
        };
        let mut aad = [0u8; 48];
        for (at, id) in [self.drive, id, self.circle].iter().enumerate() {
            aad[at * 16..][..16].copy_from_slice(&id.to_bytes_le());
        }
        let opened = gcm_open(&self.key, &locked[1..13], &locked[13..], &aad)?;
        key_of(&opened, "a file's key")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    use super::*;

    // odin-core `SelectiveShareKeyVectorTests` and `ClientLockedKeyHeaderVectorTests`.
    const CIRCLE_KEY: &str = "MDEyMzQ1Njc4OTo7PD0+Pw==";
    pub(crate) const FILE_KEY: &str = "ICEiIyQlJicoKSorLC0uLw==";
    const DRIVE: &str = "11111111-2222-3333-4444-555555555555";
    pub(crate) const FILE: &str = "66666666-7777-8888-9999-aaaaaaaaaaaa";
    pub(crate) const GTID: &str = "12345678-9abc-def0-1234-56789abcdef0";
    pub(crate) const CIRCLE: &str = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    pub(crate) const MEMBER: &str = "Sam.DotYou.Cloud";
    pub(crate) const LOCKED: &str = "AlBRUlNUVVZXWFlaWyNALFXaUVoKLLDbDk0g5bIydyRsCSpKQjTVXgru8mq5";
    pub(crate) const LOCKED_BY_GTID: &str =
        "A1BRUlNUVVZXWFlaWyNALFXaUVoKLLDbDk0g5bID6/ZsXceCKma5R4GJLiTG";
    const SEALED: &str = "AhRSmXQE7NVFE2N+zrbujRavkL1z/E4GHalzavkps48544kxb1C5vlxaQZbfnbT2N+RRSjZob4zGxLf4jftVPwAgZq3c692FaLeGasq2pZGKZ1iIyxpJ7Fbilv77slwQlOIeKC7Rag3IKHq3cJUyfbDKapD3G1IWdxUh0JkipMakO1Wy1Um0cvyaU9IBa5FBFpM=";
    /// The write-only key of the vectors' member drive (PKCS#8).
    pub(crate) const WRITE_ONLY_PKCS8: &str = "MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDCZAel29rv0BgBwa7LGp4+tmSi5g4LlRzkW9/ZLQNgeVdrB44imUOGeaKmn1eiPZsShZANiAATKPHfd0ojszgjchR06XVN2p/iaosbJedgQGD86jcH39CM7EjWiXa6wVTW3cWGdDXwWQYx4NHDg8lIWULARJhvn02W1/eoTtDkhdpQrY6KmVbxpY/ehNZlDlhlh3MntF40=";

    pub(crate) fn bytes(b64: &str) -> Vec<u8> {
        STANDARD.decode(b64).expect("base64")
    }

    pub(crate) fn id(s: &str) -> Uuid {
        s.parse().expect("uuid")
    }

    /// The vectors' sealed circle key, opened with `key` for `member`.
    pub(crate) fn unseal_with(
        key: &WriteOnlyKey,
        member: &str,
    ) -> Result<CircleKey, HomebaseError> {
        CircleKey::unseal(&bytes(SEALED), key, id(CIRCLE), id(DRIVE), member)
    }

    /// A file key is bound to its circle: the key of another circle (another id, the same
    /// bytes even) doesn't open it.
    #[test]
    fn a_file_key_opens_only_with_its_circles_key() {
        let other = |circle: Uuid| CircleKey {
            key: Zeroizing::new(key_of(&bytes(CIRCLE_KEY), "a circle's key").expect("key")),
            circle,
            drive: id(DRIVE),
        };
        let locked = bytes(LOCKED);
        assert!(other(id(CIRCLE)).file_key(&locked, id(FILE), None).is_ok());
        assert!(
            other(Uuid::new_v4())
                .file_key(&locked, id(FILE), None)
                .is_err()
        );
    }

    #[test]
    fn a_sealed_circle_key_opens_for_that_circle_and_member() {
        let key = WriteOnlyKey::from_pkcs8(&bytes(WRITE_ONLY_PKCS8)).expect("pkcs8");
        let opened = unseal_with(&key, MEMBER).expect("opens");
        assert_eq!(STANDARD.encode(*opened.key), CIRCLE_KEY);
        assert!(unseal_with(&key, "pippin.dotyou.cloud").is_err());
        assert!(unseal_with(&key, "säm.dotyou.cloud").is_err());
        let other_circle = CircleKey::unseal(&bytes(SEALED), &key, id(FILE), id(DRIVE), MEMBER);
        assert!(other_circle.is_err());
    }

    #[test]
    fn a_locked_file_key_opens_on_its_own_row() {
        let circle = CircleKey {
            key: Zeroizing::new(key_of(&bytes(CIRCLE_KEY), "vector").expect("16 bytes")),
            circle: id(CIRCLE),
            drive: id(DRIVE),
        };
        let open = |locked: &[u8], file: &str, gtid: Option<&str>| {
            circle
                .file_key(locked, id(file), gtid.map(id))
                .map(|k| STANDARD.encode(k))
        };
        let (locked, by_gtid) = (bytes(LOCKED), bytes(LOCKED_BY_GTID));
        assert_eq!(open(&locked, FILE, None).expect("opens"), FILE_KEY);
        assert!(open(&locked, GTID, None).is_err(), "another file's row");
        assert_eq!(open(&by_gtid, FILE, Some(GTID)).expect("opens"), FILE_KEY);
        assert!(
            open(&by_gtid, FILE, Some(FILE)).is_err(),
            "bound to its gtid"
        );
        assert!(open(&by_gtid, FILE, None).is_err(), "no gtid to bind");
        assert!(open(&locked[..44], FILE, None).is_err(), "cut short");
    }
}
