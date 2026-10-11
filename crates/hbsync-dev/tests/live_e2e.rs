//! Two clients syncing two folders through a live identity:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live_e2e -- --ignored`.
//! Needs the credentials `hbsync-register` saves. Both clients sync the whole drive; the test
//! works and checks inside one top-level folder of its own and deletes it at the end.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use hbsync_engine::{Engine, EngineConfig, SyncReport};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_local::{Filter, IGNORE_FILE, STATE_DIR, state_db_path};
use hbsync_remote::Remote;
use hbsync_state::StateDb;

type Res<T> = Result<T, Box<dyn std::error::Error>>;

struct Client {
    _dir: tempfile::TempDir,
    root: PathBuf,
    name: String,
    remote: Arc<HomebaseRemote>,
    engine: Engine,
}

impl Client {
    fn new(identity: &str, name: &str) -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(STATE_DIR))?;
        let remote = Arc::new(HomebaseRemote::connect(hbsync_dev::saved_client(
            identity,
        )?)?);
        let engine = Self::engine(&root, name, &remote)?;
        Ok(Client {
            _dir: dir,
            root,
            name: name.to_owned(),
            remote,
            engine,
        })
    }

    fn engine(root: &Path, name: &str, remote: &Arc<HomebaseRemote>) -> Res<Engine> {
        let mut cfg = EngineConfig::new(root, name);
        cfg.allow_mass_delete = true; // the test deletes its whole folder at the end
        cfg.exec.filter = Arc::new(Filter::load(root, cfg.exec.case_insensitive)?);
        let db = StateDb::open(&state_db_path(root))?;
        Ok(Engine::new(db, Box::new(remote.clone()), cfg))
    }

    /// A new engine on the same state DB, as after a restart.
    fn restart(&mut self) -> Res<()> {
        self.engine = Self::engine(&self.root, &self.name, &self.remote)?;
        Ok(())
    }

    /// [`Client::restart`] with the ignore rules `rules`.
    fn restart_ignoring(&mut self, rules: &str) -> Res<()> {
        fs::write(self.root.join(STATE_DIR).join(IGNORE_FILE), rules)?;
        self.restart()?;
        assert!(self.engine.apply_filter_change()?);
        Ok(())
    }

    /// Scan, then sync until idle.
    fn sync(&mut self) -> Res<SyncReport> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            // Homebase holds back feed rows of the current millisecond.
            std::thread::sleep(Duration::from_millis(20));
            self.engine.scan_local()?;
            let pass = self.engine.sync_once(&hbsync_app::today())?;
            let idle = pass.ops_done == 0;
            total.absorb(pass);
            if idle {
                break;
            }
        }
        assert!(
            total.failed.is_empty(),
            "{}: failed {:?}",
            self.name,
            total.failed
        );
        let skipped = self.remote.take_skipped();
        assert!(
            skipped.is_empty(),
            "{}: feed rows skipped {skipped:?}",
            self.name
        );
        Ok(total)
    }

    fn write(&self, rel: &str, data: impl AsRef<[u8]>) -> Res<()> {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(fs::write(path, data)?)
    }

    fn read(&self, rel: &str) -> Res<Vec<u8>> {
        Ok(fs::read(self.root.join(rel))?)
    }

    /// The tree under `folder`: `path = content` for small files, `path = <n bytes>` for
    /// large ones, `path/` for folders, sorted.
    fn tree(&self, folder: &str) -> Vec<String> {
        let mut out = Vec::new();
        walk(&self.root.join(folder), &self.root.join(folder), &mut out);
        out.sort();
        out
    }
}

fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let rel = path
            .strip_prefix(base)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if path.is_dir() {
            out.push(format!("{rel}/"));
            walk(base, &path, out);
        } else {
            let data = fs::read(&path).unwrap_or_default();
            if data.len() > 100 {
                out.push(format!("{rel} = <{} bytes>", data.len()));
            } else {
                out.push(format!("{rel} = {}", String::from_utf8_lossy(&data)));
            }
        }
    }
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn two_clients_converge_through_homebase() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("e2e");
    let mut a = Client::new(&identity, "a")?;
    let mut b = Client::new(&identity, "b")?;
    let result = scenarios(&run, &mut a, &mut b);

    // Clean up whatever happened: delete the run folder on both sides.
    let _ = fs::remove_dir_all(a.root.join(&run));
    let cleanup = a.sync().and_then(|_| b.sync());
    result?;
    cleanup?;
    assert!(
        !b.root.join(&run).exists(),
        "the run folder is gone on b too"
    );
    Ok(())
}

fn scenarios(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let at = |rel: &str| format!("{run}/{rel}");
    // A large file goes up streamed and comes down in ranged chunks.
    let big: Vec<u8> = (0..50_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();

    // Create.
    a.write(&at("doc.txt"), "v1")?;
    a.write(&at("sub/x.txt"), "x")?;
    a.write(&at("Résumé 履歴書.txt"), "unicode")?;
    a.write(&at("big.bin"), &big)?;
    fs::create_dir_all(a.root.join(at("empty")))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree(run), a.tree(run));
    assert!(
        b.read(&at("big.bin"))? == big,
        "the large file arrived intact"
    );

    // Edit on one side, rename and move on the other.
    a.write(&at("doc.txt"), "v2")?;
    fs::rename(b.root.join(at("sub")), b.root.join(at("sub2")))?;
    fs::rename(
        b.root.join(at("sub2/x.txt")),
        b.root.join(at("x-moved.txt")),
    )?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree(run), b.tree(run));
    assert_eq!(b.read(&at("doc.txt"))?, b"v2");
    assert!(a.root.join(at("x-moved.txt")).is_file());

    // Delete.
    fs::remove_file(a.root.join(at("x-moved.txt")))?;
    let ra = a.sync()?;
    let rb = b.sync()?;
    assert!(
        !b.root.join(at("x-moved.txt")).exists(),
        "a: {ra:?}\nb: {rb:?}"
    );

    // Edit/edit: both versions survive, one as a conflict copy.
    a.write(&at("doc.txt"), "from a")?;
    b.write(&at("doc.txt"), "from b")?;
    a.sync()?;
    let report = b.sync()?;
    assert_eq!(report.conflicts.len(), 1, "{:?}", report.conflicts);
    a.sync()?;
    assert_eq!(a.tree(run), b.tree(run));
    let versions: Vec<String> = a
        .tree(run)
        .into_iter()
        .filter(|l| l.starts_with("doc"))
        .collect();
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert!(versions.iter().any(|l| l.ends_with("= from a")));
    assert!(versions.iter().any(|l| l.ends_with("= from b")));

    // Edit/delete: the edit wins.
    fs::remove_file(a.root.join(at("Résumé 履歴書.txt")))?;
    b.write(&at("Résumé 履歴書.txt"), "edited while deleted")?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read(&at("Résumé 履歴書.txt"))?, b"edited while deleted");
    assert_eq!(a.tree(run), b.tree(run));

    // A restart picks up what changed while it was down.
    b.restart()?;
    a.write(&at("while-b-was-down.txt"), "hi")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(a.tree(run), b.tree(run));

    filters(run, a, b)
}

/// Sync semantics §10: junk never syncs, and what b starts ignoring stays on b only.
fn filters(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let at = |rel: &str| format!("{run}/proj/{rel}");
    let proj = format!("{run}/proj");
    a.write(&at(".DS_Store"), "finder")?;
    a.write(&at("main.c"), "int main")?;
    a.write(&at("build/out.o"), "obj")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree(&proj),
        ["build/", "build/out.o = obj", "main.c = int main"]
    );

    // b starts ignoring build/: it leaves the drive (a's copy goes to a's trash), stays on b.
    b.restart_ignoring("build/\n")?;
    b.write(&at("build/local.o"), "b only")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree(&proj), [".DS_Store = finder", "main.c = int main"]);
    assert_eq!(
        b.tree(&proj),
        [
            "build/",
            "build/local.o = b only",
            "build/out.o = obj",
            "main.c = int main"
        ]
    );

    // a deletes the folder: b keeps what it ignores and the folder holding it, which goes
    // back up, empty.
    fs::remove_dir_all(a.root.join(&proj))?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        b.tree(&proj),
        ["build/", "build/local.o = b only", "build/out.o = obj"]
    );
    assert!(a.root.join(&proj).is_dir() && a.tree(&proj).is_empty());

    // Clean up b's own files; the empty folder goes everywhere.
    fs::remove_dir_all(b.root.join(&proj))?;
    b.sync()?;
    a.sync()?;
    assert!(!a.root.join(&proj).exists());
    Ok(())
}
