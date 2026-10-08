//! Layer 4 milestone: two clients on real folders, syncing through the fake remote.
//!
//! The randomized test drives real file-system operations on both clients with interleaved
//! syncs, then checks that both folders and the server converge and that no file version was
//! lost unless a client overwrote or deleted it (the simulator's properties, on a real disk).

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::{Engine, EngineConfig, SyncReport};
use hbsync_platform::name::name_key;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::StateDb;

type R = Result<(), Box<dyn std::error::Error>>;
type Res<T> = Result<T, Box<dyn std::error::Error>>;

struct Client {
    _dir: tempfile::TempDir,
    root: PathBuf,
    engine: Engine,
    remote: FakeRemote,
}

impl Client {
    fn new(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
        tweak: impl FnOnce(&mut EngineConfig),
    ) -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join(".hbsync"))?;
        let db = StateDb::open(&root.join(".hbsync").join("state.db"))?;
        let remote = FakeRemote::new(server.clone());
        let mut cfg = EngineConfig::new(&root, name);
        tweak(&mut cfg);
        let engine = Engine::new(db, Box::new(remote.clone()), cfg);
        Ok(Client {
            _dir: dir,
            root,
            engine,
            remote,
        })
    }

    fn sync(&mut self) -> Res<SyncReport> {
        let mut total = SyncReport::default();
        for _ in 0..6 {
            self.engine.scan_local()?;
            let pass = self.engine.sync_once("2026-10-08")?;
            total.ops_done += pass.ops_done;
            total.conflicts.extend(pass.conflicts);
            total.failed.extend(pass.failed);
            total.held = pass.held;
            total.pending = pass.pending;
            if pass.ops_done == 0 {
                break;
            }
        }
        Ok(total)
    }

    fn write(&self, rel: &str, data: &[u8]) -> std::io::Result<()> {
        let p = self.root.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, data)
    }

    /// `(path, content)` for files, `(path/, "")` for folders, sorted; `.hbsync` excluded.
    fn tree(&self) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, &mut out);
        out.sort();
        out
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if rel == ".hbsync" {
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

fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

/// Syncs both clients until a full round changes nothing.
fn settle_all(a: &mut Client, b: &mut Client) -> Res<()> {
    for _ in 0..6 {
        let (ra, rb) = (a.sync()?, b.sync()?);
        if ra.ops_done == 0 && rb.ops_done == 0 {
            let ra2 = a.sync()?;
            if ra2.ops_done == 0 {
                return Ok(());
            }
        }
    }
    Err("no convergence".into())
}

// ---- deterministic randomized test ----

/// SplitMix64: small, deterministic, good enough for test choices.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

const NAMES: &[&str] = &["a", "b", "A", "c.txt", "C.TXT", "d"];

/// Tracks every content ever written and which were consciously replaced or deleted.
#[derive(Default)]
struct Ledger {
    next: u64,
    written: BTreeSet<Vec<u8>>,
    superseded: HashSet<Vec<u8>>,
}

impl Ledger {
    fn fresh(&mut self) -> Vec<u8> {
        self.next += 1;
        let data = format!("content #{}", self.next).into_bytes();
        self.written.insert(data.clone());
        data
    }
}

fn entries(root: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let (mut dirs, mut files) = (vec![PathBuf::new()], Vec::new());
    let mut stack = vec![PathBuf::new()];
    while let Some(rel) = stack.pop() {
        for e in fs::read_dir(root.join(&rel))
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = e.file_name();
            if rel.as_os_str().is_empty() && name == ".hbsync" {
                continue;
            }
            let child = rel.join(&name);
            if e.path().is_dir() {
                dirs.push(child.clone());
                stack.push(child);
            } else {
                files.push(child);
            }
        }
    }
    dirs.sort();
    files.sort();
    (dirs, files)
}

fn name_free(root: &Path, dir: &Path, name: &str) -> bool {
    let key = name_key(name);
    fs::read_dir(root.join(dir))
        .into_iter()
        .flatten()
        .flatten()
        .all(|e| name_key(&e.file_name().to_string_lossy()) != key)
}

/// One random user action on `root`.
fn act(root: &Path, rng: &mut Rng, ledger: &mut Ledger) -> std::io::Result<()> {
    let (dirs, files) = entries(root);
    let dir = &dirs[rng.below(dirs.len())];
    let name = NAMES[rng.below(NAMES.len())];
    match rng.below(10) {
        0..=2 if name_free(root, dir, name) => fs::write(root.join(dir).join(name), ledger.fresh()),
        3 if name_free(root, dir, name) && dirs.len() < 8 => {
            fs::create_dir(root.join(dir).join(name))
        }
        4..=6 if !files.is_empty() => {
            let f = &files[rng.below(files.len())];
            ledger.superseded.insert(fs::read(root.join(f))?);
            fs::write(root.join(f), ledger.fresh())
        }
        7 | 8 => {
            let all: Vec<&PathBuf> = dirs.iter().skip(1).chain(files.iter()).collect();
            if all.is_empty() {
                return Ok(());
            }
            let item = all[rng.below(all.len())];
            if dir.starts_with(item) || !name_free(root, dir, name) {
                return Ok(());
            }
            fs::rename(root.join(item), root.join(dir).join(name))
        }
        9 => {
            let all: Vec<&PathBuf> = dirs.iter().skip(1).chain(files.iter()).collect();
            if all.is_empty() {
                return Ok(());
            }
            let item = root.join(all[rng.below(all.len())]);
            if item.is_dir() {
                for f in entries(&item).1 {
                    ledger.superseded.insert(fs::read(item.join(f))?);
                }
                fs::remove_dir_all(item)
            } else {
                ledger.superseded.insert(fs::read(&item)?);
                fs::remove_file(item)
            }
        }
        _ => Ok(()),
    }
}

#[test]
fn randomized_two_client_convergence() -> R {
    let seeds: u64 = std::env::var("MILESTONE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    for seed in 0..seeds {
        let server = server();
        let allow = |c: &mut EngineConfig| c.allow_mass_delete = true;
        let mut a = Client::new(&server, "a", allow)?;
        let mut b = Client::new(&server, "b", allow)?;
        let mut rng = Rng(seed);
        let mut ledger = Ledger::default();
        for _ in 0..30 {
            match rng.below(4) {
                0 => act(&a.root, &mut rng, &mut ledger)?,
                1 => act(&b.root, &mut rng, &mut ledger)?,
                2 => {
                    a.sync()?;
                }
                _ => {
                    b.sync()?;
                }
            }
        }
        settle_all(&mut a, &mut b).map_err(|e| format!("seed {seed}: {e}"))?;
        let (ta, tb) = (a.tree(), b.tree());
        assert_eq!(ta, tb, "seed {seed}: clients diverged");
        let present: HashSet<&Vec<u8>> = ta.iter().map(|(_, c)| c).collect();
        let lost: Vec<String> = ledger
            .written
            .iter()
            .filter(|c| !ledger.superseded.contains(*c) && !present.contains(c))
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect();
        assert!(lost.is_empty(), "seed {seed}: lost {lost:?}\ntree: {ta:?}");
        // The server holds the same tree.
        let live = server.lock().map_err(|_| "poisoned")?.live().count();
        assert_eq!(live, ta.len(), "seed {seed}: server differs");
    }
    Ok(())
}

// ---- cross-OS cases ----

#[test]
fn unicode_and_case_clashes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a", |_| {})?;
    let mut b = Client::new(&server, "b", |_| {})?;
    // Decomposed on one side, composed on the other: the same name.
    a.write("Re\u{301}sume\u{301}.txt", b"from a")?;
    a.sync()?;
    b.sync()?;
    let names: Vec<String> = b.tree().into_iter().map(|(p, _)| p).collect();
    assert_eq!(names.len(), 1);
    assert_eq!(hbsync_platform::nfc(&names[0]), "Résumé.txt");

    // Case-only clash created concurrently: both files survive under distinct names.
    a.write("Report.txt", b"upper")?;
    b.write("report.TXT", b"lower")?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    // The user's own (decomposed) spelling stays on a; b has the canonical NFC name.
    let nfc_tree = |c: &Client| -> Vec<(String, Vec<u8>)> {
        let mut t: Vec<_> = c
            .tree()
            .into_iter()
            .map(|(p, d)| (hbsync_platform::nfc(&p), d))
            .collect();
        t.sort();
        t
    };
    assert_eq!(nfc_tree(&a), nfc_tree(&b));
    let contents: Vec<Vec<u8>> = b.tree().into_iter().map(|(_, c)| c).collect();
    assert!(contents.contains(&b"upper".to_vec()) && contents.contains(&b"lower".to_vec()));
    Ok(())
}

#[test]
fn windows_illegal_names_are_mapped_locally_only() -> R {
    use hbsync_remote::{NewNode, Remote, Upload};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, NodeKind};

    let server = server();
    // Another (Linux) client created names Windows can't hold; put them on the server directly.
    let other = FakeRemote::new(server.clone());
    for (name, data) in [("what?.txt", &b"q"[..]), ("CON", &b"device name"[..])] {
        let node = NodeId::new();
        let new = NewNode {
            node,
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file(name, 1_700_000_000_000, data.len() as u64, false),
        };
        let hash = hbsync_fileio::hash_bytes(data, hbsync_fileio::HashAlgorithm::Blake3);
        other.create(
            &new,
            Some(Upload {
                hash,
                size: data.len() as u64,
                reader: &mut &data[..],
            }),
        )?;
    }
    // A client applying the Windows rules (works on any disk: the escaped names are legal).
    let mut win = Client::new(&server, "win", |c| c.exec.os = hbsync_platform::Os::Windows)?;
    win.sync()?;
    let names: BTreeSet<String> = win.tree().into_iter().map(|(p, _)| p).collect();
    assert!(names.contains("what\u{F025}.txt"), "{names:?}");
    assert!(names.contains("CON\u{F02A}"), "{names:?}");

    // An edit on the Windows side goes back under the original remote name.
    win.write("what\u{F025}.txt", b"edited on windows")?;
    win.sync()?;
    let srv = server.lock().map_err(|_| "poisoned")?;
    let remote_names: BTreeSet<String> = srv
        .live()
        .filter_map(|h| h.state().map(|s| s.name))
        .collect();
    assert_eq!(
        remote_names,
        BTreeSet::from(["CON".to_owned(), "what?.txt".to_owned()])
    );
    let edited = srv
        .live()
        .find(|h| h.state().is_some_and(|s| s.name == "what?.txt"))
        .map(|h| h.file_id)
        .ok_or("what?.txt")?;
    assert_eq!(srv.payload(edited)?.as_slice(), b"edited on windows");
    Ok(())
}

#[test]
fn large_file_streams_and_resumes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a", |_| {})?;
    let mut b = Client::new(&server, "b", |_| {})?;
    let data: Vec<u8> = (0..6_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    a.write("video.bin", &data)?;
    a.sync()?;
    b.remote.faults().cut_download_after = Some(2_000_000);
    b.sync()?;
    assert_eq!(fs::read(b.root.join("video.bin"))?, data);
    assert_eq!(
        b.remote.faults().served,
        data.len() as u64,
        "the resumed download fetched each byte once"
    );
    Ok(())
}

#[test]
fn deleting_everything_is_held() -> R {
    let server = server();
    let mut a = Client::new(&server, "a", |_| {})?;
    for i in 0..60 {
        a.write(&format!("f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..60 {
        fs::remove_file(a.root.join(format!("f{i:02}")))?;
    }
    let report = a.sync()?;
    assert_eq!(report.held.len(), 60);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        60,
        "nothing was deleted remotely"
    );
    Ok(())
}

#[test]
fn a_mass_delete_split_over_batches_is_still_held() -> R {
    let server = server();
    let small_batches = |c: &mut EngineConfig| {
        c.batch = 10;
        c.mass_delete_min = 20;
    };
    let mut a = Client::new(&server, "a", small_batches)?;
    for i in 0..30 {
        a.write(&format!("dir/f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    // 30 deletes, planned 10 at a time: each batch alone is under the limit of 20.
    fs::remove_dir_all(a.root.join("dir"))?;
    let report = a.sync()?;
    assert!(!report.held.is_empty());
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        31,
        "nothing was deleted remotely"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn executable_bit_syncs() -> R {
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::new(&server, "a", |_| {})?;
    let mut b = Client::new(&server, "b", |_| {})?;
    a.write("run.sh", b"#!/bin/sh\necho hi\n")?;
    fs::set_permissions(a.root.join("run.sh"), fs::Permissions::from_mode(0o755))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        fs::metadata(b.root.join("run.sh"))?.permissions().mode() & 0o111,
        0o111
    );
    Ok(())
}
