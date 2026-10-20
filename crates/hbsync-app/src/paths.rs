//! Where the client keeps its files outside the sync folder: the config, the credentials file
//! (when there is no keychain), the daemon's lock, logs and socket.
//!
//! `HBSYNC_HOME` puts all of them in one folder (tests, a second instance while developing).
//! Otherwise each OS's conventions apply. The state DB is not here: it lives in the sync
//! folder's `.hbsync`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use hbsync_platform::Os;

/// The folder name on macOS and Windows.
const APP_FOLDER: &str = "Homebase Filesync";
/// The folder name on Linux.
const APP_FOLDER_XDG: &str = "hbsync";

#[derive(Debug, thiserror::Error)]
#[error("no home folder: set {0}")]
pub struct NoHome(&'static str);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    /// The config file and the credentials file.
    pub config: PathBuf,
    /// The daemon's lock and anything else it keeps between runs.
    pub state: PathBuf,
    pub logs: PathBuf,
    /// The control socket.
    pub runtime: PathBuf,
}

/// Removes the file at `path`; already gone is fine.
pub(crate) fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// The user's home folder: `HOME`, or on Windows `USERPROFILE` (absolute only).
pub fn home() -> Option<PathBuf> {
    ["HOME", "USERPROFILE"].iter().find_map(|key| {
        std::env::var_os(key)
            .map(PathBuf::from)
            .filter(|p| p.has_root())
    })
}

/// An XDG base folder: `$var` when it is absolute (the spec ignores relative ones), else
/// `default` in the home folder (`XDG_CONFIG_HOME`, `.config`).
pub fn xdg_dir(var: &str, default: &str) -> Option<PathBuf> {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| p.has_root())
        .or_else(|| home().map(|h| h.join(default)))
}

impl Paths {
    /// This user's paths on this OS.
    pub fn resolve() -> Result<Paths, NoHome> {
        Paths::from_env(Os::current(), |key| std::env::var_os(key))
    }

    /// The paths on `os`, reading the environment through `var`.
    pub fn from_env(os: Os, var: impl Fn(&str) -> Option<OsString>) -> Result<Paths, NoHome> {
        if let Some(home) = var("HBSYNC_HOME").filter(|h| !h.is_empty()) {
            return Ok(Paths::under(Path::new(&home)));
        }
        // Absolute paths only: the XDG spec says to ignore relative ones. (`has_root`, so the
        // rules of each OS can be tested on all of them.)
        let dir = |key: &str| var(key).map(PathBuf::from).filter(|p| p.has_root());
        match os {
            Os::Linux => {
                let home = || dir("HOME").ok_or(NoHome("HOME"));
                let config = match dir("XDG_CONFIG_HOME") {
                    Some(d) => d,
                    None => home()?.join(".config"),
                }
                .join(APP_FOLDER_XDG);
                let state = match dir("XDG_STATE_HOME") {
                    Some(d) => d,
                    None => home()?.join(".local").join("state"),
                }
                .join(APP_FOLDER_XDG);
                let runtime = dir("XDG_RUNTIME_DIR")
                    .map_or_else(|| state.clone(), |d| d.join(APP_FOLDER_XDG));
                Ok(Paths {
                    config,
                    logs: state.join("logs"),
                    state,
                    runtime,
                })
            }
            Os::MacOs => {
                let library = dir("HOME").ok_or(NoHome("HOME"))?.join("Library");
                let support = library.join("Application Support").join(APP_FOLDER);
                Ok(Paths {
                    config: support.clone(),
                    state: support.clone(),
                    logs: library.join("Logs").join(APP_FOLDER),
                    runtime: support,
                })
            }
            Os::Windows => {
                let config = dir("APPDATA").ok_or(NoHome("APPDATA"))?.join(APP_FOLDER);
                let state = dir("LOCALAPPDATA")
                    .ok_or(NoHome("LOCALAPPDATA"))?
                    .join(APP_FOLDER);
                Ok(Paths {
                    config,
                    logs: state.join("logs"),
                    runtime: state.clone(),
                    state,
                })
            }
        }
    }

    /// Everything in `home`.
    pub fn under(home: &Path) -> Paths {
        Paths {
            config: home.to_path_buf(),
            state: home.to_path_buf(),
            logs: home.join("logs"),
            runtime: home.to_path_buf(),
        }
    }

    pub fn config_file(&self) -> PathBuf {
        self.config.join("config.toml")
    }

    /// The state DB of `identity`'s account: `<state>/accounts/<identity in lower case, : as
    /// _>/state.db` ([`hbsync_engine::root::SyncRoot`]).
    pub fn account_db(&self, identity: &str) -> PathBuf {
        let key = identity.to_lowercase().replace(':', "_");
        self.state.join("accounts").join(key).join("state.db")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_account_db_is_named_after_its_identity() {
        let paths = Paths::under(Path::new("/h"));
        assert!(
            paths
                .account_db("Anna.Example.com:8443")
                .ends_with("accounts/anna.example.com_8443/state.db")
        );
    }

    /// The MSIX keeps these folders out of its virtualization (packaging/conveyor.conf), so the
    /// command line and the zip build see the same files.
    #[test]
    fn the_msix_leaves_our_folders_alone() {
        let conf = include_str!("../../../packaging/conveyor.conf");
        for known in ["RoamingAppData", "LocalAppData"] {
            let dir = format!("\"{known}/{APP_FOLDER}\"");
            assert!(conf.contains(&dir), "conveyor.conf lacks {dir}");
        }
    }

    fn env<'a>(vars: &'a [(&str, &str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |key| {
            vars.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| OsString::from(v))
        }
    }

    #[test]
    fn hbsync_home_holds_everything() {
        for os in [Os::Linux, Os::MacOs, Os::Windows] {
            let p = Paths::from_env(os, env(&[("HBSYNC_HOME", "/t/h"), ("HOME", "/home/a")]))
                .expect("ok");
            assert_eq!(p, Paths::under(Path::new("/t/h")));
            assert_eq!(p.config_file(), Path::new("/t/h/config.toml"));
        }
    }

    #[test]
    fn linux_follows_xdg() {
        let p = Paths::from_env(Os::Linux, env(&[("HOME", "/home/a")])).expect("ok");
        assert_eq!(p.config, Path::new("/home/a/.config/hbsync"));
        assert_eq!(p.state, Path::new("/home/a/.local/state/hbsync"));
        assert_eq!(p.logs, Path::new("/home/a/.local/state/hbsync/logs"));
        assert_eq!(p.runtime, p.state);

        let p = Paths::from_env(
            Os::Linux,
            env(&[
                ("HOME", "/home/a"),
                ("XDG_CONFIG_HOME", "/c"),
                ("XDG_STATE_HOME", "relative/is/ignored"),
                ("XDG_RUNTIME_DIR", "/run/user/1000"),
            ]),
        )
        .expect("ok");
        assert_eq!(p.config, Path::new("/c/hbsync"));
        assert_eq!(p.state, Path::new("/home/a/.local/state/hbsync"));
        assert_eq!(p.runtime, Path::new("/run/user/1000/hbsync"));

        // Everything given: HOME isn't needed.
        let p = Paths::from_env(
            Os::Linux,
            env(&[("XDG_CONFIG_HOME", "/c"), ("XDG_STATE_HOME", "/s")]),
        )
        .expect("ok");
        assert_eq!(p.state, Path::new("/s/hbsync"));
        assert!(Paths::from_env(Os::Linux, env(&[])).is_err());
    }

    #[test]
    fn macos_and_windows_use_their_app_folders() {
        let p = Paths::from_env(Os::MacOs, env(&[("HOME", "/Users/a")])).expect("ok");
        assert_eq!(
            p.config,
            Path::new("/Users/a/Library/Application Support/Homebase Filesync")
        );
        assert_eq!(p.logs, Path::new("/Users/a/Library/Logs/Homebase Filesync"));

        let p = Paths::from_env(
            Os::Windows,
            env(&[("APPDATA", "/u/Roaming"), ("LOCALAPPDATA", "/u/Local")]),
        )
        .expect("ok");
        assert_eq!(p.config, Path::new("/u/Roaming/Homebase Filesync"));
        assert_eq!(p.state, Path::new("/u/Local/Homebase Filesync"));
        assert_eq!(p.logs, Path::new("/u/Local/Homebase Filesync/logs"));
        assert!(Paths::from_env(Os::Windows, env(&[("APPDATA", "/u/Roaming")])).is_err());
    }
}
