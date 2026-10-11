//! Where this device's credentials for an identity are kept: the OS keychain (Keychain on macOS,
//! Credential Manager on Windows, the Secret Service on Linux) or, when none is reachable, a
//! file only this user can read in the config folder.
//!
//! Credentials found in the file while the keychain works are moved into the keychain.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hbsync_homebase::{Credentials, HomebaseError};
use keyring_core::{CredentialStore as Keychain, Error as KeychainError};

use crate::Paths;

/// The keychain entries' service name; the account is the identity.
const SERVICE: &str = "Homebase Filesync";

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
pub enum Location {
    Keychain,
    /// In a file, because the keychain couldn't be used (`why`).
    File {
        path: PathBuf,
        why: String,
    },
}

#[derive(Debug)]
pub struct CredentialStore {
    /// The OS keychain, or why there is none.
    keychain: Result<Arc<Keychain>, String>,
    /// Files only, on purpose (tests, development): no keychain to miss.
    files_only: bool,
    /// The folder of the credential files.
    dir: PathBuf,
}

impl CredentialStore {
    /// The OS keychain, with files in the config folder as the fallback.
    /// `HBSYNC_CREDENTIALS=file` uses the files only (tests, development).
    pub fn open(paths: &Paths) -> CredentialStore {
        let files_only = std::env::var_os("HBSYNC_CREDENTIALS").is_some_and(|v| v == "file");
        let keychain = if files_only {
            Err("HBSYNC_CREDENTIALS=file".to_owned())
        } else {
            os_keychain().map_err(|e| e.to_string())
        };
        CredentialStore {
            keychain,
            files_only,
            dir: paths.config.join("credentials"),
        }
    }

    /// Files in `dir` only.
    pub fn files_in(dir: &Path) -> CredentialStore {
        CredentialStore {
            keychain: Err("files only".to_owned()),
            files_only: true,
            dir: dir.to_path_buf(),
        }
    }

    /// Whether the keychain is used, or why not.
    pub fn keychain(&self) -> Result<(), &str> {
        self.keychain.as_ref().map(|_| ()).map_err(String::as_str)
    }

    /// The file the credentials for `identity` are kept in without a keychain.
    pub fn file(&self, identity: &str) -> PathBuf {
        self.dir.join(format!("{identity}.json"))
    }

    /// Saves `creds`: in the keychain if it takes them, else in the file. A copy left in the
    /// file is removed once the keychain has them.
    pub fn save(&self, creds: &Credentials) -> Result<Location, CredentialError> {
        let file = self.file(&creds.identity);
        let why = match self.keychain_entry(&creds.identity) {
            Ok(entry) => {
                let json = serde_json::to_string(creds).map_err(HomebaseError::from)?;
                match entry.set_password(&json) {
                    Ok(()) => {
                        remove(&file)?;
                        return Ok(Location::Keychain);
                    }
                    Err(e) => e.to_string(),
                }
            }
            Err(why) => why,
        };
        save_private(creds, &file)?;
        Ok(Location::File { path: file, why })
    }

    /// The saved credentials for `identity`, and where they are. `None` only when they are
    /// known to be missing: a keychain that is there but can't be read now (locked at logon)
    /// is an error, not "not set up".
    pub fn load(&self, identity: &str) -> Result<Option<(Credentials, Location)>, CredentialError> {
        // Why the file is read: no keychain on this system (`Ok`), or one that failed (`Err`);
        // `None` when the keychain works and has no entry.
        let why: Option<Result<String, String>> = match &self.keychain {
            Err(why) => Some(Ok(why.clone())),
            Ok(_) => match self
                .keychain_entry(identity)
                .map_err(Some)
                .and_then(|entry| {
                    entry.get_password().map_err(|e| match e {
                        KeychainError::NoEntry => None,
                        e => Some(e.to_string()),
                    })
                }) {
                Ok(json) => {
                    let creds = serde_json::from_str(&json).map_err(CredentialError::Corrupt)?;
                    return Ok(Some((creds, Location::Keychain)));
                }
                Err(failed) => failed.map(Err),
            },
        };
        let path = self.file(identity);
        let Some(creds) = Credentials::load(&path)? else {
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

    /// Forgets the credentials for `identity`, wherever they are. Returns why the keychain
    /// couldn't be looked at, if it couldn't (it may still hold them: say so).
    pub fn delete(&self, identity: &str) -> Result<Option<String>, CredentialError> {
        remove(&self.file(identity))?;
        match &self.keychain {
            Err(_) if self.files_only => Ok(None),
            Err(why) => Ok(Some(why.clone())),
            Ok(_) => {
                let entry = self
                    .keychain_entry(identity)
                    .map_err(CredentialError::Keychain)?;
                match entry.delete_credential() {
                    Ok(()) | Err(KeychainError::NoEntry) => Ok(None),
                    Err(e) => Err(CredentialError::Keychain(e.to_string())),
                }
            }
        }
    }

    fn keychain_entry(&self, identity: &str) -> Result<keyring_core::Entry, String> {
        let keychain = self.keychain.as_ref().map_err(Clone::clone)?;
        keychain
            .build(SERVICE, identity, None)
            .map_err(|e| e.to_string())
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
            circle_id: Some(uuid::Uuid::from_u128(7)),
        }
    }

    fn same(a: &Credentials, b: &Credentials) -> bool {
        (
            &a.identity,
            &a.client_auth_token,
            &a.shared_secret,
            a.circle_id,
        ) == (
            &b.identity,
            &b.client_auth_token,
            &b.shared_secret,
            b.circle_id,
        )
    }

    #[test]
    fn files_round_trip() -> Result<(), CredentialError> {
        let dir = tempfile::tempdir().map_err(HomebaseError::from)?;
        let store = CredentialStore::files_in(&dir.path().join("credentials"));
        assert!(store.keychain().is_err());
        assert!(store.load("a.example")?.is_none());

        let creds = sample("a.example");
        let location = store.save(&creds)?;
        let path = store.file("a.example");
        assert_eq!(
            location,
            Location::File {
                path: path.clone(),
                why: "files only".into()
            }
        );
        let (back, at) = store.load("a.example")?.expect("saved");
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

        assert_eq!(store.delete("a.example")?, None);
        assert_eq!(store.delete("a.example")?, None);
        assert!(store.load("a.example")?.is_none());
        Ok(())
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
        let identity = format!("hbsync-test-{}.example", std::process::id());
        let creds = sample(&identity);
        let location = store.save(&creds)?;
        if cfg!(any(windows, target_os = "macos")) {
            assert_eq!(location, Location::Keychain, "{:?}", store.keychain());
            assert!(!store.file(&identity).exists());
        }
        let (back, at) = store.load(&identity)?.expect("saved");
        assert!(same(&back, &creds));
        assert_eq!(at, location);
        store.delete(&identity)?;
        assert!(store.load(&identity)?.is_none());
        Ok(())
    }
}
