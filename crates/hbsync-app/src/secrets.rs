//! Where this device's credentials for an identity are kept: the OS keychain (Keychain on macOS,
//! Credential Manager on Windows, the Secret Service on Linux) or, when none is reachable, a
//! file only this user can read in the config folder.
//!
//! Credentials found in the file while the keychain works are moved into the keychain.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use hbsync_homebase::{Credentials, HomebaseError};
use keyring_core::{CredentialStore as Keychain, Error as KeychainError};

use crate::{Identity, Paths};

/// The keychain entries' service name; the account is the identity.
const SERVICE: &str = "Homebase Filesync";

/// How entries are written: on Windows kept on this computer (`Local` persistence), not
/// `Enterprise`, the store's default, which travels with a roaming profile.
const MODIFIERS: &[(&str, &str)] = if cfg!(windows) {
    &[("persistence", "Local")]
} else {
    &[]
};

/// How many times the keychain is opened, and a call on it tried, while it fails as a broken
/// session ([`broken`]).
const TRIES: usize = 4;

/// Opens the keychain: [`os_keychain`], but in tests.
type Open = fn() -> keyring_core::Result<Arc<Keychain>>;

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("credentials file: {0}")]
    File(#[from] HomebaseError),
    #[error("keychain: {0}")]
    Keychain(String),
    #[error("the keychain entry isn't valid credentials: {0}")]
    Corrupt(serde_json::Error),
}

/// Where a device's credentials are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Location {
    Keychain,
    /// In a file, because the keychain couldn't be used (`why`).
    File {
        path: PathBuf,
        why: String,
    },
}

#[derive(Debug)]
pub(crate) struct CredentialStore {
    /// The OS keychain, or why there is none. Opened again after a call failed on it
    /// ([`CredentialStore::call`]).
    keychain: Result<Mutex<Arc<Keychain>>, String>,
    open: Open,
    /// Files only, on purpose (tests, development): no keychain to miss.
    files_only: bool,
    /// The folder of the credential files.
    dir: PathBuf,
}

impl CredentialStore {
    /// The OS keychain, with files in the config folder as the fallback.
    /// `HBSYNC_CREDENTIALS=file` uses the files only (tests, development).
    pub(crate) fn open(paths: &Paths) -> CredentialStore {
        let files_only = std::env::var_os("HBSYNC_CREDENTIALS").is_some_and(|v| v == "file");
        let keychain = if files_only {
            Err("HBSYNC_CREDENTIALS=file".to_owned())
        } else {
            opened(os_keychain).map(Mutex::new)
        };
        CredentialStore {
            keychain,
            open: os_keychain,
            files_only,
            dir: paths.config.join("credentials"),
        }
    }

    /// Files in `dir` only.
    #[cfg(test)]
    pub(crate) fn files_in(dir: &Path) -> CredentialStore {
        CredentialStore {
            keychain: Err("files only".to_owned()),
            open: os_keychain,
            files_only: true,
            dir: dir.to_path_buf(),
        }
    }

    /// Whether the keychain is used, or why not.
    #[cfg(test)]
    pub(crate) fn keychain(&self) -> Result<(), &str> {
        self.keychain.as_ref().map(|_| ()).map_err(String::as_str)
    }

    /// The file the credentials for `identity` are kept in without a keychain:
    /// `<its file key>.json` ([`Identity::file_key`]).
    pub(crate) fn file(&self, identity: &Identity) -> PathBuf {
        self.dir.join(format!("{}.json", identity.file_key()))
    }

    /// Where versions before 0.8 kept that file, if elsewhere: named after the identity as is,
    /// a port's `:` too (a name Windows refuses). Moved to its name now when found there.
    fn earlier_file(&self, identity: &Identity) -> Option<PathBuf> {
        identity
            .contains(':')
            .then(|| self.dir.join(format!("{identity}.json")))
    }

    /// Saves `creds`: in the keychain if it takes them, else in the file. A copy left in the
    /// file is removed once the keychain has them.
    pub(crate) fn save(&self, creds: &Credentials) -> Result<Location, CredentialError> {
        let identity = Identity::new(&creds.identity);
        let file = self.file(&identity);
        let json = serde_json::to_string(creds).map_err(HomebaseError::from)?;
        let saved = self.call(&identity, |entry| {
            entry.set_password(&json)?;
            // Read back where a session can be broken: it may have stored garbage, and its read
            // fails. One more call per save (at a login).
            if cfg!(all(unix, not(target_os = "macos"))) {
                entry.get_password()?;
            }
            Ok(())
        });
        let why = match saved {
            Ok(Ok(())) => {
                remove(&file)?;
                return Ok(Location::Keychain);
            }
            Ok(Err(e)) => e.to_string(),
            Err(why) => why,
        };
        save_private(creds, &file)?;
        Ok(Location::File { path: file, why })
    }

    /// The saved credentials for `identity`, and where they are. `None` only when they are
    /// known to be missing: a keychain that is there but can't be read now (locked at logon)
    /// is an error, not "not set up".
    pub(crate) fn load(
        &self,
        identity: &Identity,
    ) -> Result<Option<(Credentials, Location)>, CredentialError> {
        // Why the file is read: no keychain on this system (`Ok`), or one that failed (`Err`);
        // `None` when the keychain works and has no entry.
        let why: Option<Result<String, String>> =
            match self.call(identity, |entry| entry.get_password()) {
                Err(why) => Some(Ok(why)),
                Ok(Ok(json)) => {
                    let creds = serde_json::from_str(&json).map_err(CredentialError::Corrupt)?;
                    return Ok(Some((creds, Location::Keychain)));
                }
                Ok(Err(KeychainError::NoEntry)) => None,
                Ok(Err(e)) => Some(Err(e.to_string())),
            };
        let path = self.file(identity);
        let Some(creds) = self.load_file(identity, &path)? else {
            return match why {
                Some(Err(failed)) => Err(CredentialError::Keychain(failed)),
                _ => Ok(None),
            };
        };
        let location = match why {
            Some(Ok(why) | Err(why)) => Location::File { path, why },
            // The keychain works and is empty: move them there.
            None => self.save(&creds)?,
        };
        Ok(Some((creds, location)))
    }

    /// The credentials in the file at `path` ([`CredentialStore::file`]), moved there first
    /// from the file an earlier version kept them in, if that is where they are. Only the
    /// file at `path` counts when that one can't be read or moved (on Windows the earlier name
    /// is a stream of another file, or no name at all).
    fn load_file(
        &self,
        identity: &Identity,
        path: &Path,
    ) -> Result<Option<Credentials>, CredentialError> {
        if let Some(earlier) = self.earlier_file(identity)
            && !path.exists()
            && let Ok(Some(_)) = Credentials::load(&earlier)
        {
            match std::fs::rename(&earlier, path) {
                Ok(()) => tracing::info!("credentials moved to {}", path.display()),
                Err(e) => tracing::warn!("can't move {} ({e})", earlier.display()),
            }
        }
        Ok(Credentials::load(path)?)
    }

    /// Forgets the credentials for `identity`, wherever they are. Returns why the keychain
    /// couldn't be looked at, if it couldn't (it may still hold them: say so).
    pub(crate) fn delete(&self, identity: &Identity) -> Result<Option<String>, CredentialError> {
        remove(&self.file(identity))?;
        if let Some(earlier) = self.earlier_file(identity) {
            // Never there on Windows (no such name): looked for only to be removed.
            if let Err(e) = remove(&earlier) {
                tracing::warn!("can't remove {}: {e}", earlier.display());
            }
        }
        match self.call(identity, |entry| entry.delete_credential()) {
            Err(_) if self.files_only => Ok(None),
            Err(why) => Ok(Some(why)),
            Ok(Ok(()) | Err(KeychainError::NoEntry)) => Ok(None),
            Ok(Err(e)) => Err(CredentialError::Keychain(e.to_string())),
        }
    }

    /// `call` on `identity`'s keychain entry; `Err` when there is no keychain. A broken session
    /// ([`broken`]) is tried again on the keychain opened again, [`TRIES`] times in all.
    fn call<T>(
        &self,
        identity: &Identity,
        call: impl Fn(&keyring_core::Entry) -> keyring_core::Result<T>,
    ) -> Result<keyring_core::Result<T>, String> {
        let keychain = self.keychain.as_ref().map_err(Clone::clone)?;
        let mut keychain = keychain.lock().unwrap_or_else(PoisonError::into_inner);
        let modifiers: HashMap<&str, &str> = MODIFIERS.iter().copied().collect();
        let mut tries = 1;
        loop {
            match keychain
                .build(SERVICE, identity, Some(&modifiers))
                .and_then(|entry| call(&entry))
            {
                Err(e) if broken(&e) && tries < TRIES => {
                    tracing::info!("keychain: {e}; opening it again");
                    match opened(self.open) {
                        Ok(again) => *keychain = again,
                        Err(e) => tracing::warn!("keychain: {e}"),
                    }
                    tries += 1;
                }
                done => return Ok(done),
            }
        }
    }
}

/// The keychain `open` opens, [`TRIES`] times at most while it opens broken, or why it can't
/// be.
fn opened(open: Open) -> Result<Arc<Keychain>, String> {
    let mut tries = 1;
    loop {
        match open() {
            Err(e) if broken(&e) && tries < TRIES => tries += 1,
            done => return done.map_err(|e| e.to_string()),
        }
    }
}

/// Whether `e` may be a broken session, one opened again may not be. On Linux each opening
/// agrees a session key with the Secret Service, and KDE's (ksecretd) refuses about one in 200
/// ("client public key size is invalid": secret-service sends a key with a leading zero byte
/// short) and gets about one in 125 wrong (measured: 16 and 24 of 3,000): every call on it
/// fails to decrypt, or reads garbage, until it is opened again. Elsewhere none is retried (a
/// macOS failure may be the user's no to a prompt).
fn broken(e: &KeychainError) -> bool {
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        use secret_service::Error as Session;
        match e {
            KeychainError::BadEncoding(_) => true,
            KeychainError::PlatformFailure(e) => {
                matches!(
                    e.downcast_ref(),
                    Some(Session::Crypto(_) | Session::Zbus(_))
                )
            }
            _ => false,
        }
    }
    #[cfg(not(all(unix, not(target_os = "macos"))))]
    {
        let _ = e;
        false
    }
}

fn os_keychain() -> keyring_core::Result<Arc<Keychain>> {
    #[cfg(target_os = "macos")]
    let store: Arc<Keychain> = apple_native_keyring_store::keychain::Store::new()?;
    #[cfg(windows)]
    let store: Arc<Keychain> = windows_native_keyring_store::Store::new()?;
    #[cfg(all(unix, not(target_os = "macos")))]
    let store: Arc<Keychain> = zbus_secret_service_keyring_store::Store::new()?;
    Ok(store)
}

/// Writes `creds` to `path` (0600, in one step) in a folder only this user can open (on
/// Windows, the user's profile folders already are).
fn save_private(creds: &Credentials, path: &Path) -> Result<(), HomebaseError> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    creds.save(path)
}

fn remove(path: &Path) -> Result<(), CredentialError> {
    crate::paths::remove_if_exists(path).map_err(|e| HomebaseError::from(e).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(identity: &str) -> Credentials {
        Credentials {
            identity: identity.to_owned(),
            client_auth_token: "Y2F0".to_owned(),
            shared_secret: "AAECAwQFBgcICQoLDA0ODw==".to_owned(),
            zk_app_key: None,
        }
    }

    fn same(a: &Credentials, b: &Credentials) -> bool {
        (&a.identity, &a.client_auth_token, &a.shared_secret)
            == (&b.identity, &b.client_auth_token, &b.shared_secret)
    }

    /// Credentials saved before the circle's id was dropped from them still load.
    #[test]
    fn credentials_with_a_circle_id_still_load() -> Result<(), serde_json::Error> {
        let old = r#"{"identity":"a.example","client_auth_token":"Y2F0",
            "shared_secret":"AAECAwQFBgcICQoLDA0ODw==",
            "circle_id":"ae76d900-4394-4541-8a72-273b755211d1"}"#;
        let creds: Credentials = serde_json::from_str(old)?;
        assert!(same(&creds, &sample("a.example")));
        Ok(())
    }

    #[test]
    fn files_round_trip() -> Result<(), CredentialError> {
        let dir = tempfile::tempdir().map_err(HomebaseError::from)?;
        let store = CredentialStore::files_in(&dir.path().join("credentials"));
        assert!(store.keychain().is_err());
        assert!(store.load(&"a.example".into())?.is_none());

        let creds = sample("a.example");
        let location = store.save(&creds)?;
        let path = store.file(&"a.example".into());
        assert_eq!(
            location,
            Location::File {
                path: path.clone(),
                why: "files only".into()
            }
        );
        let (back, at) = store.load(&"a.example".into())?.expect("saved");
        assert!(same(&back, &creds));
        assert_eq!(at, location);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).map(|m| m.permissions().mode() & 0o777);
            assert_eq!(mode(&path).map_err(HomebaseError::from)?, 0o600);
            assert_eq!(
                mode(path.parent().expect("dir")).map_err(HomebaseError::from)?,
                0o700
            );
        }

        assert_eq!(store.delete(&"a.example".into())?, None);
        assert_eq!(store.delete(&"a.example".into())?, None);
        assert!(store.load(&"a.example".into())?.is_none());
        Ok(())
    }

    /// The file of an identity with a port is named without its `:` (Windows refuses it); one
    /// an earlier version named with it is read and moved. Unix only: Windows can't name it so.
    #[cfg(unix)]
    #[test]
    fn a_file_named_with_a_colon_moves() -> Result<(), CredentialError> {
        let dir = tempfile::tempdir().map_err(HomebaseError::from)?;
        let store = CredentialStore::files_in(&dir.path().join("credentials"));
        let identity = Identity::new("a.example:8443");
        let (earlier, now) = (
            dir.path().join("credentials/a.example:8443.json"),
            dir.path().join("credentials/a.example_8443.json"),
        );
        assert_eq!(store.file(&identity), now);
        let creds = sample("a.example:8443");
        save_private(&creds, &earlier)?;
        let (back, _) = store
            .load(&identity)?
            .expect("found under its earlier name");
        assert!(same(&back, &creds));
        assert!(now.exists() && !earlier.exists(), "moved");
        // Forgotten under either name.
        save_private(&creds, &earlier)?;
        store.delete(&identity)?;
        assert!(!now.exists() && !earlier.exists());
        assert!(store.load(&identity)?.is_none());
        Ok(())
    }

    /// A keychain as KDE's Secret Service gives it: its first opening is refused, and every
    /// other one after is a broken session (a write stores garbage; a read fails to decrypt,
    /// or reads garbage). Per test thread.
    #[cfg(all(unix, not(target_os = "macos")))]
    mod flaky {
        use std::any::Any;
        use std::cell::{Cell, RefCell};
        use std::collections::HashMap;
        use std::sync::Arc;

        use keyring_core::api::{CredentialApi, CredentialStoreApi};
        use keyring_core::{Credential, CredentialStore, Entry, Error, Result};
        use secret_service::Error as Session;

        thread_local! {
            static OPENED: Cell<usize> = const { Cell::new(0) };
            static SECRETS: RefCell<HashMap<String, Vec<u8>>> = RefCell::new(HashMap::new());
        }

        /// Opens the keychain: refused the first time, broken every other time.
        pub(super) fn open() -> Result<Arc<CredentialStore>> {
            match OPENED.replace(OPENED.get() + 1) {
                0 => Err(Error::PlatformFailure(Box::new(Session::Zbus(
                    zbus::Error::Failure("client public key size is invalid".into()),
                )))),
                n => Ok(Arc::new(Store { broken: n % 2 == 1 })),
            }
        }

        #[derive(Debug)]
        struct Store {
            broken: bool,
        }

        #[derive(Debug)]
        struct Cred {
            user: String,
            broken: bool,
        }

        impl CredentialStoreApi for Store {
            fn vendor(&self) -> String {
                "flaky".into()
            }
            fn id(&self) -> String {
                "flaky".into()
            }
            fn build(&self, _: &str, user: &str, _: Option<&HashMap<&str, &str>>) -> Result<Entry> {
                Ok(Entry::new_with_credential(Arc::new(Cred {
                    user: user.into(),
                    broken: self.broken,
                })))
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        impl CredentialApi for Cred {
            fn set_secret(&self, secret: &[u8]) -> Result<()> {
                let stored = match self.broken {
                    true => vec![0xff; secret.len()],
                    false => secret.to_vec(),
                };
                SECRETS.with_borrow_mut(|s| s.insert(self.user.clone(), stored));
                Ok(())
            }
            fn get_secret(&self) -> Result<Vec<u8>> {
                let stored = SECRETS.with_borrow(|s| s.get(&self.user).cloned());
                match (stored, self.broken) {
                    (None, _) => Err(Error::NoEntry),
                    // Decrypted with the wrong key: its padding fails, or it is garbage.
                    (Some(stored), true) if stored.len() % 2 == 0 => Ok(vec![0xff; 3]),
                    (Some(_), true) => Err(Error::PlatformFailure(Box::new(Session::Crypto(
                        "message decryption failed",
                    )))),
                    (Some(stored), false) => Ok(stored),
                }
            }
            fn delete_credential(&self) -> Result<()> {
                SECRETS.with_borrow_mut(|s| s.remove(&self.user));
                Ok(())
            }
            fn get_credential(&self) -> Result<Option<Arc<Credential>>> {
                Ok(None)
            }
            fn get_specifiers(&self) -> Option<(String, String)> {
                Some(("flaky".into(), self.user.clone()))
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    /// A keychain refused at first, and opened as a broken session, is opened again: the
    /// credentials are kept there (not in the file), and read back by another process meeting
    /// the same.
    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn a_broken_keychain_session_is_opened_again() -> Result<(), CredentialError> {
        let dir = tempfile::tempdir().map_err(HomebaseError::from)?;
        let store = || CredentialStore {
            keychain: opened(flaky::open).map(Mutex::new),
            open: flaky::open,
            files_only: false,
            dir: dir.path().join("credentials"),
        };
        let identity = Identity::new("a.example");
        for creds in [sized(&identity, 0), sized(&identity, 1)] {
            // Refused, then broken: what it stored is garbage, read back failing.
            let first = store();
            assert_eq!(first.save(&creds)?, Location::Keychain);
            assert!(!first.file(&identity).exists());
            // Another process, on a broken session first.
            let (back, at) = store().load(&identity)?.expect("saved");
            assert!(same(&back, &creds));
            assert_eq!(at, Location::Keychain);
        }
        assert_eq!(store().delete(&identity)?, None);
        assert!(store().load(&identity)?.is_none());
        Ok(())
    }

    /// [`sample`] with an even (`parity` 0) or odd (1) length as JSON: a broken session reads
    /// the one as garbage, and fails to decrypt the other ([`flaky`]).
    #[cfg(all(unix, not(target_os = "macos")))]
    fn sized(identity: &str, parity: usize) -> Credentials {
        let mut creds = sample(identity);
        if serde_json::to_string(&creds).map_or(0, |j| j.len()) % 2 != parity {
            creds.client_auth_token.push('A');
        }
        creds
    }

    /// Writes to the real keychain, so only on CI runners (`CI` is set). macOS and Windows
    /// runners have a keychain; a Linux runner has no Secret Service, and the file is used.
    #[test]
    fn keychain_round_trip_on_ci() -> Result<(), CredentialError> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        let dir = tempfile::tempdir().map_err(HomebaseError::from)?;
        let store = CredentialStore::open(&Paths::under(dir.path()));
        let identity = Identity::new(&format!("hbsync-test-{}.example", std::process::id()));
        let creds = sample(&identity);
        let location = store.save(&creds)?;
        if cfg!(any(windows, target_os = "macos")) {
            assert_eq!(location, Location::Keychain, "{:?}", store.keychain());
            assert!(!store.file(&identity).exists());
        }
        // On Windows kept on this computer.
        #[cfg(windows)]
        {
            let persistence = || -> Result<String, CredentialError> {
                let attributes = store
                    .call(&identity, |entry| entry.get_attributes())
                    .map_err(CredentialError::Keychain)?
                    .map_err(|e| CredentialError::Keychain(e.to_string()))?;
                Ok(attributes.get("persistence").cloned().unwrap_or_default())
            };
            assert_eq!(persistence()?, "Local");
        }
        let (back, at) = store.load(&identity)?.expect("saved");
        assert!(same(&back, &creds));
        assert_eq!(at, location);
        store.delete(&identity)?;
        assert!(store.load(&identity)?.is_none());
        Ok(())
    }
}
