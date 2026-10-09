//! The client harness shared by the engine's integration tests: a sync root in a temp folder
//! with its own state DB and engine, talking to a shared fake server.

// Each test file uses its own subset.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::{Engine, EngineConfig, SyncReport};
use hbsync_local::STATE_DIR;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::StateDb;

pub type Res<T> = Result<T, Box<dyn std::error::Error>>;

pub fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

/// The engine config tests use: no real pause between transient retries.
pub fn config(root: &Path, name: &str) -> EngineConfig {
    let mut cfg = EngineConfig::new(root, name);
    cfg.exec.retry_pause = std::time::Duration::from_millis(1);
    cfg
}

pub struct Client {
    _dir: tempfile::TempDir,
    pub root: PathBuf,
    pub engine: Engine,
    pub remote: FakeRemote,
}

impl Client {
    pub fn new(server: &Arc<Mutex<FakeServer>>, name: &str) -> Res<Self> {
        Self::with_config(server, name, |_| {})
    }

    pub fn with_config(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
        tweak: impl FnOnce(&mut EngineConfig),
    ) -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(STATE_DIR))?;
        let db = StateDb::open(&Self::db_path_in(&root))?;
        let remote = FakeRemote::new(server.clone());
        let mut cfg = config(&root, name);
        tweak(&mut cfg);
        let engine = Engine::new(db, Box::new(remote.clone()), cfg);
        Ok(Client {
            _dir: dir,
            root,
            engine,
            remote,
        })
    }

    fn db_path_in(root: &Path) -> PathBuf {
        hbsync_local::state_db_path(root)
    }

    /// The state DB file (for a second connection or a restart).
    pub fn db_path(&self) -> PathBuf {
        Self::db_path_in(&self.root)
    }

    /// Scan, then sync until idle. The report accumulates every pass.
    pub fn sync(&mut self) -> Res<SyncReport> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            self.engine.scan_local()?;
            let pass = self.engine.sync_once("2026-10-08")?;
            let idle = pass.ops_done == 0;
            total.absorb(pass);
            if idle {
                break;
            }
        }
        Ok(total)
    }

    pub fn write(&self, rel: &str, data: impl AsRef<[u8]>) -> std::io::Result<()> {
        let p = self.root.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, data)
    }

    pub fn read(&self, rel: &str) -> std::io::Result<String> {
        fs::read_to_string(self.root.join(rel))
    }

    /// `(path, content)` for files, `(path/, "")` for folders, sorted; the state folder
    /// excluded.
    pub fn tree(&self) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, &mut out);
        out.sort();
        out
    }

    /// [`Client::tree`] as text: `path = content` for files, `path/` for folders.
    pub fn tree_text(&self) -> Vec<String> {
        self.tree()
            .into_iter()
            .map(|(path, data)| {
                if path.ends_with('/') {
                    path
                } else {
                    format!("{path} = {}", String::from_utf8_lossy(&data))
                }
            })
            .collect()
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if rel == STATE_DIR {
            continue;
        }
        if path.is_dir() {
            out.push((format!("{rel}/"), Vec::new()));
            walk(root, &path, out);
        } else {
            out.push((rel, fs::read(&path).unwrap_or_default()));
        }
    }
}
