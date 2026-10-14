//! A test client: a sync root in a temp folder with its own state DB and engine, on any
//! [`Remote`] (the fake in the engine's tests, Homebase in the live ones). Feature `testkit`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hbsync_local::STATE_DIR;
use hbsync_remote::Remote;
use hbsync_state::StateDb;

use crate::{Engine, EngineConfig, SyncReport};

pub type Res<T> = Result<T, Box<dyn std::error::Error>>;

/// Sets up a client's engine config; run again on every [`TestClient::restart`].
type Configure = Box<dyn Fn(&mut EngineConfig) -> Res<()>>;

pub struct TestClient<R> {
    pub root: PathBuf,
    pub name: String,
    pub engine: Engine,
    pub remote: R,
    configure: Configure,
    /// Last: removed after the engine has closed the state DB in it (Windows can't remove an
    /// open file).
    _dir: tempfile::TempDir,
}

impl<R> std::fmt::Debug for TestClient<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestClient")
            .field("name", &self.name)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<R: Remote + Clone + 'static> TestClient<R> {
    /// A client called `name` (its conflict copies say so) on `remote`, its engine config set
    /// up by `configure`.
    pub fn new(
        remote: R,
        name: &str,
        configure: impl Fn(&mut EngineConfig) -> Res<()> + 'static,
    ) -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(STATE_DIR))?;
        let configure: Configure = Box::new(configure);
        let engine = Self::engine(&root, name, Box::new(remote.clone()), &configure, |_| {})?;
        Ok(TestClient {
            root,
            name: name.to_owned(),
            engine,
            remote,
            configure,
            _dir: dir,
        })
    }

    fn engine(
        root: &Path,
        name: &str,
        remote: Box<dyn Remote>,
        configure: &Configure,
        tweak: impl FnOnce(&mut EngineConfig),
    ) -> Res<Engine> {
        let mut cfg = EngineConfig::new(root, name);
        configure(&mut cfg)?;
        tweak(&mut cfg);
        let db = StateDb::open(&hbsync_local::state_db_path(root))?;
        Ok(Engine::new(db, remote, cfg))
    }

    /// A new engine on the same state DB, as after a restart.
    pub fn restart(&mut self) -> Res<()> {
        self.restart_with(Box::new(self.remote.clone()), |_| {})
    }

    /// [`TestClient::restart`] on `remote` (one wrapping [`TestClient::remote`], say), its
    /// config changed by `tweak` too.
    pub fn restart_with(
        &mut self,
        remote: Box<dyn Remote>,
        tweak: impl FnOnce(&mut EngineConfig),
    ) -> Res<()> {
        self.engine = Self::engine(&self.root, &self.name, remote, &self.configure, tweak)?;
        Ok(())
    }

    /// The state DB file (for a second connection).
    pub fn db_path(&self) -> PathBuf {
        hbsync_local::state_db_path(&self.root)
    }

    /// Scan, then sync until idle (six passes at most), `before` each pass; every pass's
    /// report together.
    pub fn sync_until_idle(&mut self, today: &str, mut before: impl FnMut()) -> Res<SyncReport> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            before();
            self.engine.scan_local()?;
            let pass = self.engine.sync_once(today)?;
            let idle = pass.ops_done == 0;
            total.absorb(pass);
            if idle {
                break;
            }
        }
        Ok(total)
    }

    pub fn write(&self, rel: &str, data: impl AsRef<[u8]>) -> io::Result<()> {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, data)
    }

    pub fn read(&self, rel: &str) -> io::Result<Vec<u8>> {
        fs::read(self.root.join(rel))
    }

    pub fn read_text(&self, rel: &str) -> io::Result<String> {
        Ok(String::from_utf8_lossy(&self.read(rel)?).into_owned())
    }

    /// `(path, content)` for files, `(path/, [])` for folders, under `folder` ("" for the
    /// whole root, the state folder left out), sorted.
    pub fn entries(&self, folder: &str) -> Vec<(String, Vec<u8>)> {
        let base = self.root.join(folder);
        let mut out = Vec::new();
        walk(&base, &base, &mut out);
        out.sort();
        out
    }

    /// [`TestClient::entries`] as text: `path = content` for files (`path = <n bytes>` past
    /// 100 bytes), `path/` for folders.
    pub fn tree_text(&self, folder: &str) -> Vec<String> {
        self.entries(folder)
            .into_iter()
            .map(|(path, data)| match data.len() {
                _ if path.ends_with('/') => path,
                n if n > 100 => format!("{path} = <{n} bytes>"),
                _ => format!("{path} = {}", String::from_utf8_lossy(&data)),
            })
            .collect()
    }
}

fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let rel = path
            .strip_prefix(base)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if rel == STATE_DIR {
            continue;
        }
        if path.is_dir() {
            out.push((format!("{rel}/"), Vec::new()));
            walk(base, &path, out);
        } else {
            out.push((rel, fs::read(&path).unwrap_or_default()));
        }
    }
}
