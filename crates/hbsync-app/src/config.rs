//! The config file, `config.toml`: what to sync with which identity, and the few settings a
//! user can change. Ignore rules are not here: they are per sync folder, in `.hbsync/ignore`.
//!
//! ```toml
//! device_name = "Anna's laptop"   # optional, defaults to the computer's name
//! paused = false
//!
//! [mass_delete]
//! min_files = 50
//! fraction = 0.25
//!
//! [[roots]]
//! identity = "anna.example.com"
//! folder = "/home/anna/Homebase"
//! ```

use std::path::{Path, PathBuf};

use hbsync_types::Role;
use serde::{Deserialize, Serialize};

use crate::Identity;
use crate::protocol::Pause;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{0}: {1}")]
    Parse(PathBuf, toml::de::Error),
    #[error(transparent)]
    Write(#[from] toml::ser::Error),
    #[error("{0} main folders are configured; this version syncs one")]
    TooManyRoots(usize),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Shown in conflict-copy names; the computer's name when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    /// Syncing stopped by the user (survives a restart).
    #[serde(default)]
    pub paused: bool,
    /// A pause ends then (Unix seconds); `None`: when resumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_until: Option<u64>,
    #[serde(default)]
    pub mass_delete: MassDelete,
    /// Recently deleted keeps an item this long, then deletes it for good: at least
    /// [`KeepDays::MIN`].
    #[serde(default)]
    pub keep_deleted_days: KeepDays,
    #[serde(default)]
    pub roots: Vec<Root>,
}

/// Days Recently deleted keeps an item, and the trash its local copy (sync semantics §7): the
/// purge is the account's, so a value below [`KeepDays::MIN`] reads as it ([`Config::load`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeepDays(u32);

impl KeepDays {
    /// What every client keeps (node schema, The purge): a device may keep longer only.
    pub const MIN: u32 = hbsync_engine::time::MIN_KEEP_DAYS;

    /// The days kept, at least [`KeepDays::MIN`].
    pub fn days(self) -> u32 {
        self.0.max(Self::MIN)
    }
}

impl Default for KeepDays {
    fn default() -> Self {
        KeepDays(Self::MIN)
    }
}

/// A local folder synced with an identity's Filesync drive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
    /// The identity's domain, e.g. `anna.example.com`.
    pub identity: Identity,
    pub folder: PathBuf,
    /// The role folder it is (sync semantics §12); none for the main folder, which holds the
    /// drive's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
}

impl Root {
    /// The main folder at `folder`, synced with `identity`.
    pub fn main(identity: &Identity, folder: &Path) -> Root {
        Root {
            identity: identity.clone(),
            folder: folder.to_path_buf(),
            role: None,
        }
    }
}

/// When deletes are held for confirmation (sync semantics, the mass-delete guard): at least
/// `min_files` files, and at least `fraction` of the synced files.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MassDelete {
    pub min_files: usize,
    pub fraction: f64,
}

impl Default for MassDelete {
    fn default() -> Self {
        MassDelete {
            min_files: 50,
            fraction: 0.25,
        }
    }
}

impl Config {
    /// The config at `path`; `None` when there is no file yet.
    pub fn load(path: &Path) -> Result<Option<Config>, ConfigError> {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(ConfigError::Io(path.to_owned(), e)),
        };
        let mut config: Config =
            toml::from_str(&text).map_err(|e| ConfigError::Parse(path.to_owned(), e))?;
        if config.keep_deleted_days.0 < KeepDays::MIN {
            tracing::warn!(
                "{}: keep_deleted_days = {} is below the {min} days every device keeps: {min} it is",
                path.display(),
                config.keep_deleted_days.0,
                min = KeepDays::MIN,
            );
            config.keep_deleted_days = KeepDays::default();
        }
        // Earlier versions kept Windows folders with `\\?\` (setup's `canonicalize`).
        for root in &mut config.roots {
            root.folder = hbsync_platform::winpath::plain(&root.folder);
        }
        Ok(Some(config))
    }

    /// Writes the config to `path` in one step: a crash leaves the old file or the new one.
    fn save(&self, path: &Path) -> Result<(), ConfigError> {
        let text = toml::to_string_pretty(self)?;
        hbsync_fileio::write_atomic(path, text.as_bytes())
            .map_err(|e| ConfigError::Io(path.to_owned(), e))
    }

    /// Changes the config at `path` (an empty one when there is none) with `f`, and saves it
    /// when `f` changed it. One change at a time, in any process or thread
    /// ([`ConfigLock`]): none undoes another's.
    pub fn update<T>(path: &Path, f: impl FnOnce(&mut Config) -> T) -> Result<T, ConfigError> {
        ConfigLock::take(path)?.update(f)
    }

    /// The folder `identity` syncs as `role` (`None`: its main folder), if it does.
    pub fn folder(&self, identity: &Identity, role: Option<Role>) -> Option<&Root> {
        self.roots
            .iter()
            .find(|r| r.identity == *identity && r.role == role)
    }

    /// The identity a command is for: `given` if it is set up here, else the only one set up.
    pub fn identity(&self, given: Option<&str>) -> Result<Identity, String> {
        let mut set_up: Vec<&Identity> = self.roots.iter().map(|r| &r.identity).collect();
        set_up.sort_unstable();
        set_up.dedup();
        if let Some(given) = given {
            let identity = Identity::new(given);
            return match set_up.contains(&&identity) {
                true => Ok(identity),
                false => Err(format!(
                    "{identity} isn't set up here: `hbsync setup {identity}` first"
                )),
            };
        }
        match set_up[..] {
            [one] => Ok(one.clone()),
            [] => Err("not set up: `hbsync setup <identity>` first".into()),
            _ => Err(format!(
                "set up with {}: pick one with --identity",
                set_up
                    .iter()
                    .map(|i| i.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// The main folder, if any (one at most, for now).
    pub fn root(&self) -> Result<Option<&Root>, ConfigError> {
        let mains: Vec<&Root> = self.roots.iter().filter(|r| r.role.is_none()).collect();
        match mains.as_slice() {
            [] => Ok(None),
            [root] => Ok(Some(root)),
            more => Err(ConfigError::TooManyRoots(more.len())),
        }
    }

    /// The pause in effect at `now` (Unix seconds); `None` when syncing, also after a pause
    /// that has run out.
    pub fn pause(&self, now: u64) -> Option<Pause> {
        let pause = Pause {
            until: self.paused_until,
        };
        (self.paused && !pause.ran_out(now)).then_some(pause)
    }

    pub(crate) fn set_pause(&mut self, pause: Option<Pause>) {
        self.paused = pause.is_some();
        self.paused_until = pause.and_then(|p| p.until);
    }

    /// The name shown in conflict copies: the configured one, or the computer's.
    pub fn device_name(&self) -> String {
        self.device_name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(hbsync_platform::host::device_name)
    }
}

/// The lock on changing the config at a path: on `config.lock` beside it, held by one
/// process and one thread at a time. Held while the config is read, changed and written
/// ([`Config::update`]); by a program other than the daemon from looking whether the daemon
/// runs until it changed the config itself, when none runs; and by the daemon as it starts,
/// until it has read the config and bound its socket. So while the daemon runs, it alone changes the config (the
/// others ask it), and a daemon starting reads what was changed before it. Held for one
/// change (adding a folder makes its DB entry under it), or while the daemon stops for a log
/// out or unlink: a daemon starting waits for it.
#[derive(Debug)]
pub(crate) struct ConfigLock {
    /// Locked, unless the file system can't lock it; unlocked as it closes.
    _file: Option<std::fs::File>,
    /// This process's threads, one at a time: a file lock emulated per process (NFS) doesn't
    /// keep them apart. Taken first.
    _turn: std::sync::MutexGuard<'static, ()>,
    /// The config's.
    path: PathBuf,
}

impl ConfigLock {
    /// Waits for the lock of the config at `path`.
    pub(crate) fn take(path: &Path) -> Result<ConfigLock, ConfigError> {
        static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let turn = crate::sync::lock(&TURN);
        let lock = path.with_extension("lock");
        let file = crate::lock::lock_file(&lock).map_err(|e| ConfigError::Io(lock.clone(), e))?;
        let file = match file.lock() {
            Ok(()) => Some(file),
            // A file system without locks (some network ones): this process's threads are
            // still kept apart.
            Err(e) => {
                tracing::warn!("can't lock {}: {e}", lock.display());
                None
            }
        };
        Ok(ConfigLock {
            _file: file,
            _turn: turn,
            path: path.to_path_buf(),
        })
    }

    /// The config as it is now; `None` when there is no file yet.
    pub(crate) fn load(&self) -> Result<Option<Config>, ConfigError> {
        Config::load(&self.path)
    }

    /// Changes the config (an empty one when there is none) with `f`, and saves it when `f`
    /// changed it.
    pub(crate) fn update<T>(&self, f: impl FnOnce(&mut Config) -> T) -> Result<T, ConfigError> {
        let before = self.load()?.unwrap_or_default();
        let mut config = before.clone();
        let out = f(&mut config);
        if config != before {
            config.save(&self.path)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().expect("ok");
        let path = dir.path().join("sub").join("config.toml");
        assert_eq!(Config::load(&path).expect("ok"), None);

        let config = Config {
            device_name: Some("Anna's laptop".into()),
            paused: true,
            paused_until: Some(1_800_000_000),
            mass_delete: MassDelete {
                min_files: 10,
                fraction: 0.5,
            },
            keep_deleted_days: KeepDays(45),
            roots: vec![
                Root::main(&"anna.example.com".into(), Path::new("/home/anna/Homebase")),
                Root {
                    role: Some(Role::Documents),
                    ..Root::main(
                        &"anna.example.com".into(),
                        Path::new("/home/anna/Documents"),
                    )
                },
            ],
        };
        config.save(&path).expect("ok");
        assert_eq!(Config::load(&path).expect("ok"), Some(config.clone()));
        assert_eq!(config.root().expect("ok"), Some(&config.roots[0]));
        assert_eq!(config.device_name(), "Anna's laptop");

        // Saving again replaces the file.
        let empty = Config::default();
        empty.save(&path).expect("ok");
        assert_eq!(Config::load(&path).expect("ok"), Some(empty));
    }

    #[test]
    fn a_windows_folder_loads_without_the_extended_prefix() -> Result<(), ConfigError> {
        let dir = tempfile::tempdir().expect("ok");
        let path = dir.path().join("config.toml");
        let old = "[[roots]]\nidentity = \"anna.example.com\"\nfolder = '\\\\?\\C:\\Users\\anna\\Homebase'\n";
        std::fs::write(&path, old).expect("ok");
        let config = Config::load(&path)?.expect("a config");
        assert_eq!(
            config.roots[0].folder,
            PathBuf::from(r"C:\Users\anna\Homebase")
        );
        Ok(())
    }

    #[test]
    fn defaults_fill_what_is_left_out() {
        let config: Config = toml::from_str("").expect("ok");
        assert_eq!(config, Config::default());
        assert_eq!(config.mass_delete, MassDelete::default());
        assert_eq!(config.keep_deleted_days.days(), 30);
        assert_eq!(config.root().expect("ok"), None);
        assert!(!config.device_name().is_empty());
    }

    /// The purge is the whole account's: one device can't make Recently deleted keep less than
    /// every client does, only more.
    #[test]
    fn keep_deleted_days_has_a_floor() -> Result<(), ConfigError> {
        let dir = tempfile::tempdir().expect("ok");
        let path = dir.path().join("config.toml");
        for (written, kept) in [(0, 30), (1, 30), (29, 30), (30, 30), (90, 90)] {
            std::fs::write(&path, format!("keep_deleted_days = {written}\n")).expect("ok");
            let config = Config::load(&path)?.expect("a config");
            assert_eq!(config.keep_deleted_days, KeepDays(kept), "{written}");
        }
        assert_eq!(KeepDays(3).days(), 30);
        Ok(())
    }

    #[test]
    fn a_partial_table_takes_the_rest_from_the_defaults() -> Result<(), toml::de::Error> {
        let config: Config = toml::from_str("[mass_delete]\nmin_files = 200\n")?;
        assert_eq!(config.mass_delete.min_files, 200);
        assert_eq!(config.mass_delete.fraction, MassDelete::default().fraction);
        Ok(())
    }

    #[test]
    fn unknown_keys_and_extra_roots_are_errors() {
        assert!(toml::from_str::<Config>("pasued = true").is_err());
        assert!(
            toml::from_str::<Config>("[[roots]]\nidentity = \"a\"\nfolder = \"/a\"\nfoo = 1")
                .is_err()
        );
        let two: Config = toml::from_str(
            "[[roots]]\nidentity = \"a\"\nfolder = \"/a\"\n[[roots]]\nidentity = \"b\"\nfolder = \"/b\"",
        )
        .expect("ok");
        assert!(matches!(two.root(), Err(ConfigError::TooManyRoots(2))));

        let dir = tempfile::tempdir().expect("ok");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "paused = maybe").expect("ok");
        let err = Config::load(&path).expect_err("an error").to_string();
        assert!(err.contains("config.toml"), "{err}");
    }

    /// Changes made at once, from threads here (or other processes), each take the config
    /// as the one before left it: none is lost.
    #[test]
    fn changes_at_once_are_each_kept() -> Result<(), ConfigError> {
        let dir = tempfile::tempdir().map_err(|e| ConfigError::Io(PathBuf::new(), e))?;
        let path = dir.path().join("config.toml");
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..5 {
                        Config::update(&path, |c| c.mass_delete.min_files += 1).expect("saved");
                    }
                });
            }
        });
        let config = Config::load(&path)?.expect("a config");
        assert_eq!(config.mass_delete.min_files, 50 + 20);
        Ok(())
    }

    /// The lock is the file's: one taken through another handle (as another process takes
    /// it) holds a change back until it goes.
    #[test]
    fn a_change_waits_for_the_lock_of_another_process() -> Result<(), ConfigError> {
        let io = |e| ConfigError::Io(PathBuf::new(), e);
        let dir = tempfile::tempdir().map_err(io)?;
        let path = dir.path().join("config.toml");
        let other = crate::lock::lock_file(&path.with_extension("lock")).map_err(io)?;
        other.lock().map_err(io)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let changing = {
            let path = path.clone();
            std::thread::spawn(move || {
                let changed = Config::update(&path, |c| c.paused = true);
                let _ = tx.send(());
                changed
            })
        };
        let waited = rx.recv_timeout(std::time::Duration::from_millis(300));
        assert!(waited.is_err(), "changed while another held the lock");
        drop(other);
        changing.join().expect("no panic")?;
        assert!(Config::load(&path)?.expect("a config").paused);
        Ok(())
    }
}
