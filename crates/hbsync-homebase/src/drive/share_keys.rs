//! The keys of the files others share with this identity: Selective Share's blobs
//! (`docs/spec/encryption.md`, "Files shared with you"; **[verified]** against odin-core's
//! golden vector `SelectiveShareKeyVectorTests`, which chat-kmp's `ShareKeys` also checks).

use uuid::Uuid;
use zeroize::Zeroizing;

use crate::HomebaseError;
use crate::crypto::{Key, WriteOnlyKey, gcm_open, gcm_seal, key_of, random};

/// The `ZkSeal` version of a sealed circle key.
const SEALED_VERSION: u8 = 2;
/// The format of a circle key locked with its app's key.
const LOCKED_VERSION: u8 = 2;
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

    /// The key of `circle`, one of this identity's own, as its server keeps it for the app
    /// that owns the circle (`appKeyLockedShareKey`): `format ‖ nonce (12) ‖ ciphertext (16) ‖
    /// tag (16)` under the app's zero-knowledge key, bound to the circle (**[verified]**
    /// odin-core's golden vector `SelectiveShareKeyVectorTests`, chat-kmp
    /// `ShareKeys.openCircleKey`). `drive` is this identity's drive the circle shares.
    pub(crate) fn own(
        locked: &[u8],
        app_key: &Key,
        circle: Uuid,
        drive: Uuid,
    ) -> Result<CircleKey, HomebaseError> {
        if locked.len() != 1 + 12 + 16 + 16 || locked[0] != LOCKED_VERSION {
            return Err(HomebaseError::Protocol(
                "a circle's locked key: not 45 bytes of format 2".into(),
            ));
        }
        let opened = gcm_open(
            app_key,
            &locked[1..13],
            &locked[13..],
            &circle.to_bytes_le(),
        )?;
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
        let opened = gcm_open(&self.key, &locked[1..13], &locked[13..], &self.aad(id))?;
        key_of(&opened, "a file's key")
    }

    /// What a file's locked key is bound to: `drive ‖ id ‖ circle`, each a Guid's .NET bytes.
    fn aad(&self, id: Uuid) -> [u8; 48] {
        let mut aad = [0u8; 48];
        for (at, id) in [self.drive, id, self.circle].iter().enumerate() {
            aad[at * 16..][..16].copy_from_slice(&id.to_bytes_le());
        }
        aad
    }

    /// The key of a file this identity adds to what the circle shares, locked under the
    /// circle's key for the file's share row (`lockedFileKey`): bound to the global transit
    /// id picked for it (format 3), as the file has no id yet ([`CircleKey::file_key`] opens
    /// it; **[verified]** odin-core `SelectiveShareKeyVectorTests`, chat-kmp
    /// `ShareKeys.lockFileKeyByGlobalTransitId`).
    pub(crate) fn lock_file_key(
        &self,
        file_key: &Key,
        global_transit_id: Uuid,
    ) -> Result<Vec<u8>, HomebaseError> {
        self.lock_with(file_key, global_transit_id, &random::<12>()?)
    }

    fn lock_with(
        &self,
        file_key: &Key,
        global_transit_id: Uuid,
        nonce: &[u8; 12],
    ) -> Result<Vec<u8>, HomebaseError> {
        let mut locked = vec![BY_GLOBAL_TRANSIT_ID];
        locked.extend(nonce);
        locked.extend(gcm_seal(
            &self.key,
            nonce,
            file_key,
            &self.aad(global_transit_id),
        )?);
        Ok(locked)
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
    /// odin-core `ClientLockedKeyHeaderVectorTests`: the key header of [`HEADER_IV`] and
    /// [`FILE_KEY`], sealed to the write-only key below.
    pub(crate) const SEALED_KEY_HEADER: &str = "ARRSmXQEgEhVDqJf8V4wR/vfRnjuBiMDCgpqQ0EvwP8Sbhg/htjK6DPN0tPu9kf4VKIvY0JpI7ekZehCr+RJr6buacGObBkW+4KXwPFn4aNzze/w+3mTcGuj+8oKsvICcbFc5rPogLwn6Oz/OQuWmSn3xtY70ljeN7b6eV3wkBqmS0sKbr1o/iBvA+hgSlg/4KiDGsTq2pxlrCRLPvr2Vxjk";
    pub(crate) const HEADER_IV: &str = "EBESExQVFhcYGRobHB0eHw==";
    /// The public half of the write-only key below, as a drive answers it.
    pub(crate) const WRITE_ONLY_JWK: &str = r#"{"kty":"EC","crv":"P-384","x":"yjx33dKI7M4I3IUdOl1Tdqf4mqLGyXnYEBg_Oo3B9_QjOxI1ol2usFU1t3FhnQ18","y":"FkGMeDRw4PJSFlCwESYb59Nltf3qE7Q5IXaUK2OiplW8aWP3oTWZQ5YZYdzJ7ReN"}"#;
    /// The write-only key of the vectors' member drive (PKCS#8).
    pub(crate) const WRITE_ONLY_PKCS8: &str = "MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDCZAel29rv0BgBwa7LGp4+tmSi5g4LlRzkW9/ZLQNgeVdrB44imUOGeaKmn1eiPZsShZANiAATKPHfd0ojszgjchR06XVN2p/iaosbJedgQGD86jcH39CM7EjWiXa6wVTW3cWGdDXwWQYx4NHDg8lIWULARJhvn02W1/eoTtDkhdpQrY6KmVbxpY/ehNZlDlhlh3MntF40=";

    pub(crate) fn bytes(b64: &str) -> Vec<u8> {
        STANDARD.decode(b64).expect("base64")
    }

    pub(crate) fn id(s: &str) -> Uuid {
        s.parse().expect("uuid")
    }

    /// odin-core's key header sealed to the vectors' write-only key
    /// ([`SEALED_KEY_HEADER`]): what it opens to, and how a server sends it.
    pub(crate) fn sealed_key_header() -> (crate::drive::sealed::KeyHeader, serde_json::Value) {
        let opened = crate::drive::sealed::KeyHeader {
            iv: bytes(HEADER_IV).try_into().expect("an IV"),
            key: bytes(FILE_KEY).try_into().expect("a key"),
        };
        let sent = serde_json::json!({
            "encryptionVersion": 1, "type": "sealed",
            "iv": HEADER_IV, "encryptedAesKey": SEALED_KEY_HEADER,
        });
        (opened, sent)
    }

    /// The vectors' sealed circle key, opened with `key` for `member`.
    pub(crate) fn unseal_with(
        key: &WriteOnlyKey,
        member: &str,
    ) -> Result<CircleKey, HomebaseError> {
        CircleKey::unseal(&bytes(SEALED), key, id(CIRCLE), id(DRIVE), member)
    }

    /// A new file's key locks as odin-core's vector does (its nonce given), and opens again
    /// only for the global transit id, circle and drive it was locked for.
    #[test]
    fn a_new_files_key_locks_as_odin_core_does() -> Result<(), HomebaseError> {
        let write_only = WriteOnlyKey::from_pkcs8(&bytes(WRITE_ONLY_PKCS8))?;
        let circle = unseal_with(&write_only, MEMBER)?;
        let file_key: Key = bytes(FILE_KEY).try_into().expect("a key");
        let vector = bytes(LOCKED_BY_GTID);
        let nonce: [u8; 12] = vector[1..13].try_into().expect("a nonce");
        assert_eq!(circle.lock_with(&file_key, id(GTID), &nonce)?, vector);

        let locked = circle.lock_file_key(&file_key, id(GTID))?;
        assert_ne!(locked, vector, "a new nonce each time");
        assert_eq!(
            circle.file_key(&locked, Uuid::nil(), Some(id(GTID)))?,
            file_key
        );
        assert!(circle.file_key(&locked, id(GTID), Some(id(FILE))).is_err());
        assert!(circle.file_key(&locked, id(GTID), None).is_err());
        Ok(())
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

    /// odin-core `SelectiveShareKeyVectorTests.AnAppKeyLockedShareKeyIsTheCircleKeyUnderTheAppsKey`.
    #[test]
    fn a_circles_key_opens_with_the_apps_key() {
        const APP_KEY: &str = "QEFCQ0RFRkdISUpLTE1OTw==";
        const LOCKED_CIRCLE_KEY: &str =
            "AmBhYmNkZWZnaGlqa8AlHXzCApsgaEfKX1KFpSBA7DLsgycVGxPm/K9OaD6A";
        let app_key = key_of(&bytes(APP_KEY), "vector").expect("16 bytes");
        let locked = bytes(LOCKED_CIRCLE_KEY);
        let own =
            |locked: &[u8], circle: &str| CircleKey::own(locked, &app_key, id(circle), id(DRIVE));
        let opened = own(&locked, CIRCLE).expect("opens");
        assert_eq!(STANDARD.encode(*opened.key), CIRCLE_KEY);
        // What it locks, the member's sealed copy of the key opens.
        let file_key = key_of(&bytes(FILE_KEY), "vector").expect("16 bytes");
        let row = opened.lock_file_key(&file_key, id(GTID)).expect("locks");
        let member = WriteOnlyKey::from_pkcs8(&bytes(WRITE_ONLY_PKCS8)).expect("pkcs8");
        let theirs = unseal_with(&member, MEMBER).expect("opens");
        assert_eq!(
            theirs
                .file_key(&row, id(FILE), Some(id(GTID)))
                .expect("opens"),
            file_key
        );
        assert!(own(&locked, FILE).is_err(), "another circle's");
        assert!(own(&locked[..44], CIRCLE).is_err(), "cut short");
        let mut other_format = locked.clone();
        other_format[0] = 3;
        assert!(own(&other_format, CIRCLE).is_err(), "another format");
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
        // Bound to its circle: the same key under another circle's id doesn't open it.
        let elsewhere = CircleKey {
            circle: Uuid::new_v4(),
            ..circle
        };
        assert!(
            elsewhere.file_key(&locked, id(FILE), None).is_err(),
            "another circle"
        );
    }
}
