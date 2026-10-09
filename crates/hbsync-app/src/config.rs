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

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{0}: {1}")]
    Parse(PathBuf, toml::de::Error),
    #[error(transparent)]
    Write(#[from] toml::ser::Error),
    #[error("{0} folders are configured; this version syncs one")]
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
    #[serde(default)]
    pub mass_delete: MassDelete,
    #[serde(default)]
    pub roots: Vec<Root>,
}

/// A local folder synced with an identity's Filesync drive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
    /// The identity's domain, e.g. `anna.example.com`.
    pub identity: String,
    pub folder: PathBuf,
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
        toml::from_str(&text)
            .map(Some)
            .map_err(|e| ConfigError::Parse(path.to_owned(), e))
    }

    /// Writes the config to `path` in one step: a crash leaves the old file or the new one.
    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        let text = toml::to_string_pretty(self)?;
        let io = |e| ConfigError::Io(path.to_owned(), e);
        let dir = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir).map_err(io)?;
        let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(io)?;
        tmp.write_all(text.as_bytes()).map_err(io)?;
        tmp.as_file().sync_all().map_err(io)?;
        tmp.persist(path).map_err(|e| io(e.error))?;
        Ok(())
    }

    /// The one folder to sync, if any.
    pub fn root(&self) -> Result<Option<&Root>, ConfigError> {
        match self.roots.as_slice() {
            [] => Ok(None),
            [root] => Ok(Some(root)),
            more => Err(ConfigError::TooManyRoots(more.len())),
        }
    }

    /// The name shown in conflict copies: the configured one, or the computer's.
    pub fn device_name(&self) -> String {
        self.device_name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(hbsync_platform::host::device_name)
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
            mass_delete: MassDelete {
                min_files: 10,
                fraction: 0.5,
            },
            roots: vec![Root {
                identity: "anna.example.com".into(),
                folder: "/home/anna/Homebase".into(),
            }],
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
    fn defaults_fill_what_is_left_out() {
        let config: Config = toml::from_str("").expect("ok");
        assert_eq!(config, Config::default());
        assert_eq!(config.mass_delete, MassDelete::default());
        assert_eq!(config.root().expect("ok"), None);
        assert!(!config.device_name().is_empty());
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
}
