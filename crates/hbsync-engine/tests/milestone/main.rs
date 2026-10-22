//! Layer 4 milestone: two clients on real folders, syncing through the fake remote.
//!
//! The randomized test drives real file-system operations on both clients with interleaved
//! syncs, then checks that both folders and the server converge and that no file version was
//! lost unless a client overwrote or deleted it (the simulator's properties, on a real disk).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::exec::Skip;
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_platform::name::name_key;
use hbsync_remote::fake::FakeServer;
use hbsync_state::testkit::Commit;
use hbsync_types::NodeId;

#[path = "../common/mod.rs"]
mod common;

use common::{Client, Kept, Res, server};

type R = Result<(), Box<dyn std::error::Error>>;

/// Asserts a sync went cleanly: nothing failed, skipped, held or in conflict.
fn assert_clean(report: &SyncReport) {
    assert!(
        report.failed.is_empty() && report.held.is_empty() && report.conflicts.is_empty(),
        "{report:?}"
    );
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

// ---- the randomized runs' harness ----

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

/// Tracks every content ever written, which were consciously replaced or deleted, and which
/// a client's scan may take for a copy.
#[derive(Default)]
struct Ledger {
    next: u64,
    written: BTreeSet<Vec<u8>>,
    superseded: HashSet<Vec<u8>>,
    /// The places files were moved away from since their client's last scan (folder and name
    /// key), with their bytes.
    vacated: HashMap<(PathBuf, String), Vec<u8>>,
    /// Bytes that may be there once more for each time they were copied: a file moved away
    /// from a place another file took before the scan reads as the file there edited and a
    /// new one with its bytes (sync semantics §3 rule 1: an editor keeping a backup by
    /// renaming), a copy.
    copied: HashMap<Vec<u8>, usize>,
}

impl Ledger {
    fn fresh(&mut self) -> Vec<u8> {
        self.next += 1;
        let data = format!("content #{}", self.next).into_bytes();
        self.written.insert(data.clone());
        data
    }

    /// Where `rel` is under `root`: its folder and its name's key.
    fn place(root: &Path, rel: &Path) -> (PathBuf, String) {
        let at = root.join(rel);
        let name = name_key(&at.file_name().unwrap_or_default().to_string_lossy());
        (at.parent().unwrap_or(root).to_owned(), name)
    }

    /// `item` (under `root`) is about to move to `to`: every file in it leaves its place.
    fn moving(&mut self, root: &Path, item: &Path, to: &Path) -> std::io::Result<()> {
        let files: Vec<(PathBuf, PathBuf)> = if root.join(item).is_dir() {
            let files = entries(&root.join(item)).1.into_iter();
            files.map(|f| (item.join(&f), to.join(f))).collect()
        } else {
            vec![(item.to_owned(), to.to_owned())]
        };
        // No file goes where another one of the move was.
        for (from, to) in files {
            let bytes = fs::read(root.join(&from))?;
            self.filled(root, &to, &bytes);
            self.vacated.insert(Self::place(root, &from), bytes);
        }
        Ok(())
    }

    /// A file with `bytes` is at `rel` now: another one moved away from there before is a copy,
    /// if the scan hasn't seen it go.
    fn filled(&mut self, root: &Path, rel: &Path, bytes: &[u8]) {
        if let Some(left) = self.vacated.remove(&Self::place(root, rel))
            && left != bytes
        {
            *self.copied.entry(left).or_default() += 1;
        }
    }

    /// The client at `root` scanned.
    fn scanned(&mut self, root: &Path) {
        self.vacated.retain(|(dir, _), _| !dir.starts_with(root));
    }
}

/// Syncs `c`, whose scan then saw what its user did.
fn sync(c: &mut Client, ledger: &mut Ledger) -> Res<()> {
    c.sync()?;
    ledger.scanned(&c.root);
    Ok(())
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

/// What holds `name` in `dir` (under any case), if anything.
fn occupant(root: &Path, dir: &Path, name: &str) -> Option<PathBuf> {
    let key = name_key(name);
    fs::read_dir(root.join(dir))
        .into_iter()
        .flatten()
        .flatten()
        .find(|e| name_key(&e.file_name().to_string_lossy()) == key)
        .map(|e| dir.join(e.file_name()))
}

fn name_free(root: &Path, dir: &Path, name: &str) -> bool {
    occupant(root, dir, name).is_none()
}

/// The seeds the randomized tests run: `MILESTONE_SEEDS` of them (12 by default). The first half
/// are fixed (`0..`), the same every run; the rest are new every run, from a random base printed
/// once (`MILESTONE_SEED_BASE` replays a run: a failure names its seed).
fn seeds() -> Vec<u64> {
    seeds_with(true)
}

/// [`seeds`], or without `random`, the second half fixed too (`0..MILESTONE_SEEDS`, the same
/// every run) unless `MILESTONE_SEED_BASE` is set: a replay runs the same seeds as [`seeds`].
fn seeds_with(random: bool) -> Vec<u64> {
    let n: u64 = std::env::var("MILESTONE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let fixed = n / 2;
    let base: Option<u64> = std::env::var("MILESTONE_SEED_BASE")
        .ok()
        .and_then(|s| s.parse().ok());
    let base = match base {
        Some(base) => base,
        None if random => random_base(),
        None => fixed,
    };
    eprintln!(
        "milestone: {fixed} fixed seeds, {} from base {base}",
        n - fixed
    );
    (0..fixed).chain(base..base + (n - fixed)).collect()
}

/// A base for this run's new seeds, above the fixed ones (`RandomState` is keyed afresh per
/// process).
fn random_base() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(0);
    (h.finish() >> 1) | (1 << 32)
}

/// Runs `one` on each of `seeds`, every failure naming its seed: an error, a failed assertion or
/// another panic. What each seed returned.
fn each_seed<T>(
    seeds: impl IntoIterator<Item = u64>,
    mut one: impl FnMut(u64) -> Res<T>,
) -> Res<Vec<T>> {
    fn failed(seed: u64) -> String {
        format!("seed {seed} failed (MILESTONE_SEEDS=1 MILESTONE_SEED_BASE={seed} replays it)")
    }
    /// Names its seed if dropped by a panic.
    struct Naming(u64);
    impl Drop for Naming {
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("milestone: {}", failed(self.0));
            }
        }
    }
    seeds
        .into_iter()
        .map(|seed| {
            let _naming = Naming(seed);
            one(seed).map_err(|e| format!("{}: {e}", failed(seed)).into())
        })
        .collect()
}

/// Two clients of `server` that may delete everything.
fn pair(server: &Arc<Mutex<FakeServer>>) -> Res<[Client; 2]> {
    let allow = |c: &mut EngineConfig| c.guard.allow = true;
    Ok([
        Client::with_config(server, "a", allow)?,
        Client::with_config(server, "b", allow)?,
    ])
}

/// Deletes one item of `root` (one of `dirs`, but the top, or `files`), as a user does, the
/// ledger told what goes.
fn delete_some(
    root: &Path,
    dirs: &[PathBuf],
    files: &[PathBuf],
    rng: &mut Rng,
    ledger: &mut Ledger,
) -> std::io::Result<()> {
    let all: Vec<&PathBuf> = dirs.iter().skip(1).chain(files.iter()).collect();
    if all.is_empty() {
        return Ok(());
    }
    let item = root.join(all[rng.below(all.len())]);
    if item.is_dir() {
        // Every file in it: a `.hbsync` in it is the user's (only the top's is the
        // client's own).
        let mut dirs = vec![item.clone()];
        while let Some(dir) = dirs.pop() {
            for e in fs::read_dir(&dir)?.flatten() {
                match e.path().is_dir() {
                    true => dirs.push(e.path()),
                    false => {
                        ledger.superseded.insert(fs::read(e.path())?);
                    }
                }
            }
        }
        fs::remove_dir_all(item)
    } else {
        ledger.superseded.insert(fs::read(&item)?);
        fs::remove_file(item)
    }
}

/// One random user action on `root`.
fn act(root: &Path, rng: &mut Rng, ledger: &mut Ledger) -> std::io::Result<()> {
    let (dirs, files) = entries(root);
    let dir = &dirs[rng.below(dirs.len())];
    let name = NAMES[rng.below(NAMES.len())];
    match rng.below(10) {
        0..=2 if name_free(root, dir, name) => {
            let bytes = ledger.fresh();
            fs::write(root.join(dir).join(name), &bytes)?;
            ledger.filled(root, &dir.join(name), &bytes);
            Ok(())
        }
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
            if dir.starts_with(item) {
                return Ok(());
            }
            let to = match occupant(root, dir, name) {
                None => dir.join(name),
                // A file moved over another file (`mv c d` over `d`): d's bytes go.
                Some(over) if files.contains(item) && files.contains(&over) && *item != over => {
                    ledger.superseded.insert(fs::read(root.join(&over))?);
                    over
                }
                Some(_) => return Ok(()),
            };
            ledger.moving(root, item, &to)?;
            fs::rename(root.join(item), root.join(to))
        }
        9 => delete_some(root, &dirs, &files, rng, ledger),
        _ => Ok(()),
    }
}

mod files;
mod guard;
mod hostile;
mod names;
mod randomized;
mod unreadable;
