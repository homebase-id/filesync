//! A test client: a sync root in a temp folder with its own state DB and engine, on any
//! [`Remote`] (the fake in the engine's tests, Homebase in the live ones). Feature `testkit`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hbsync_local::STATE_DIR;
use hbsync_remote::Remote;

use crate::root::{AccountReader, SyncRoot};
use crate::{Drive, Engine, EngineConfig, SyncReport};

/// The identity test clients sync with.
const IDENTITY: &str = "test.example";

pub type Res<T> = Result<T, Box<dyn std::error::Error>>;

/// Called by the engine at a fixed point of a sync, for what another engine or an announcement
/// may do then: between a round's plan and its clearing of the dirty marks the plan found
/// clean ([`EngineConfig::after_plan`]), or between a sweep's reading of the guard's counts
/// and its first page ([`EngineConfig::after_counts`]).
#[derive(Clone)]
pub struct AfterPlan(pub Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for AfterPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AfterPlan")
    }
}

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
        let root = dir.path().canonicalize()?.join("root");
        fs::create_dir_all(&root)?;
        SyncRoot::create(&db_path(&root), IDENTITY, &root)?;
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
        cfg.check_narrow = true;
        configure(&mut cfg)?;
        tweak(&mut cfg);
        let db = db_path(root);
        let root = SyncRoot::open(&db, IDENTITY, root)?;
        let drive = Drive::open(&db, IDENTITY, Arc::from(remote))?;
        Ok(Engine::open(&root, drive, cfg)?)
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
        db_path(&self.root)
    }

    /// Recently deleted, as the daemon answers it (a connection that only reads).
    pub fn recently_deleted(&self, limit: usize) -> Res<Vec<hbsync_types::activity::Deleted>> {
        let mut reader = AccountReader::open(&self.db_path(), IDENTITY)?.ok_or("no state DB")?;
        // The newest `limit` deletes, each with what it took along.
        let (deletes, _) = reader.deletes(limit)?;
        let shown: std::collections::HashSet<_> = deletes.iter().map(|(d, _)| d.node).collect();
        Ok(reader
            .all_deleted()?
            .into_iter()
            .map(|(d, _)| d)
            .filter(|d| shown.contains(&d.root))
            .collect())
    }

    /// As if a minute had passed since the identity first said a file isn't there: said
    /// again, it is gone (sync semantics §8b). How many such files there are.
    pub fn a_minute_on(&self) -> Res<usize> {
        Ok(a_minute_on(&mut hbsync_state::StateDb::open(
            &self.db_path(),
        )?)?)
    }

    /// What a listing of Recently deleted now saw: what emptying it as listed takes.
    pub fn listed(&self) -> Res<crate::deleted::Listed> {
        let mut reader = AccountReader::open(&self.db_path(), IDENTITY)?.ok_or("no state DB")?;
        Ok(reader.deletes(0)?.1.listed)
    }

    /// Scan, then sync until idle (six passes at most), `before` each pass; every pass's
    /// report together. Each pass reads the feed first if `read`, else only when the drive
    /// isn't current ([`crate::Engine::sync`]), as the daemon syncs a local change.
    pub fn sync_until_idle(
        &mut self,
        today: &str,
        read: bool,
        mut before: impl FnMut(),
    ) -> Res<SyncReport> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            before();
            self.engine.scan_local()?;
            let pass = self.engine.sync(today, read)?;
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

/// The account's DB, beside the folder (as the product keeps it with the account).
fn db_path(root: &Path) -> PathBuf {
    root.with_file_name("state.db")
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

/// As if a minute had passed in `db` since the identity first said each file isn't there
/// (sync semantics §8b). How many such files there are.
pub fn a_minute_on(db: &mut hbsync_state::StateDb) -> Result<usize, hbsync_state::StateError> {
    db.write(|s| s.account().age_gone_suspects(crate::gone::CONFIRM_AFTER_MS))
}
