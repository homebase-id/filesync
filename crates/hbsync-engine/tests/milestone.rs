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

mod common;

use common::{Client, Kept, Res, server};

type R = Result<(), Box<dyn std::error::Error>>;

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
    let n: u64 = std::env::var("MILESTONE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let fixed = n / 2;
    let base = std::env::var("MILESTONE_SEED_BASE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(random_base);
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
    each_seed(seeds(), |seed| {
        let server = server();
        let [mut a, mut b] = pair(&server)?;
        let mut rng = Rng(seed);
        let mut ledger = Ledger::default();
        for _ in 0..30 {
            match rng.below(4) {
                0 => act(&a.root, &mut rng, &mut ledger)?,
                1 => act(&b.root, &mut rng, &mut ledger)?,
                2 => sync(&mut a, &mut ledger)?,
                _ => sync(&mut b, &mut ledger)?,
            }
        }
        converged(&mut a, &mut b, &ledger, &server)
    })?;
    Ok(())
}

/// The identity says now and then, for one sync of a client, that the files it asks about
/// aren't there (its own 404 to writes and header reads, a hiccup; sync semantics §8b), once per
/// client, while the users work and time goes on between syncs: a file the identity said gone
/// once but is live is never forgotten nor deleted here, and the clients converge without loss.
#[test]
fn randomized_wrong_not_theres() -> R {
    let heard = each_seed(seeds(), wrong_not_theres_of)?;
    assert!(
        heard.iter().sum::<usize>() > 0 || heard.len() < 4,
        "no wrong \"not there\" was heard"
    );
    Ok(())
}

/// Seeds of [`randomized_wrong_not_theres`] that failed:
/// - 8474444713664359667: a download from another folder kept the old version in the trash,
///   and the drive's delete of the node came within the same second: its trash entry was
///   taken, the file stayed (the clients diverged).
/// - 6526174988904104872: a file moved over another, a folder made where it was: the scan read
///   the file as the one it replaced, edited, and its bytes went up twice.
/// - 2476141012248678664: both clients moved a file away and made a new one in its place
///   before a scan, each read as an edit there and a copy (§3 rule 1): its bytes twice, which
///   the ledger didn't allow.
#[test]
fn regression_wrong_not_theres_seeds() -> R {
    let seeds = [
        8_474_444_713_664_359_667,
        6_526_174_988_904_104_872,
        2_476_141_012_248_678_664,
    ];
    each_seed(seeds, wrong_not_theres_of).map(drop)
}

/// One seed of [`randomized_wrong_not_theres`]: how many headers the identity said weren't
/// there.
fn wrong_not_theres_of(seed: u64) -> Res<usize> {
    let server = server();
    let mut clients = pair(&server)?;
    let mut rng = Rng(seed);
    let mut ledger = Ledger::default();
    let mut lied = [false; 2];
    let mut heard = 0;
    for _ in 0..30 {
        let i = rng.below(2);
        match rng.below(4) {
            0 | 1 => act(&clients[i].root, &mut rng, &mut ledger)?,
            2 if !lied[i] => {
                lied[i] = true;
                set_not_there(&clients[i], true);
                let before = clients[i].remote.faults().headers;
                sync(&mut clients[i], &mut ledger)?;
                heard += clients[i].remote.faults().headers - before;
                set_not_there(&clients[i], false);
            }
            _ => {
                // A minute or more since the last sync.
                clients[i].a_minute_on()?;
                sync(&mut clients[i], &mut ledger)?;
            }
        }
    }
    let [mut a, mut b] = clients;
    converged(&mut a, &mut b, &ledger, &server)?;
    Ok(heard)
}

/// The identity answers `client`'s writes and header reads not found (`on`), or not.
fn set_not_there(client: &Client, on: bool) {
    let mut faults = client.remote.faults();
    faults.not_found_writes = on;
    faults.not_found_headers = on;
}

/// Settles both clients, then checks that they and the server hold the same tree, that no
/// content was lost that a user didn't replace or delete, and none is there twice.
fn converged(
    a: &mut Client,
    b: &mut Client,
    ledger: &Ledger,
    server: &Arc<Mutex<FakeServer>>,
) -> Res<()> {
    settle_all(a, b)?;
    let (ta, tb) = (a.entries(""), b.entries(""));
    assert_eq!(ta, tb, "clients diverged");
    let present: HashSet<&Vec<u8>> = ta.iter().map(|(_, c)| c).collect();
    let lost: Vec<String> = ledger
        .written
        .iter()
        .filter(|c| !ledger.superseded.contains(*c) && !present.contains(c))
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect();
    assert!(lost.is_empty(), "lost {lost:?}\ntree: {ta:?}");
    // Every write has bytes of its own, and nothing copies a file but a scan reading a move
    // as one: bytes twice are an item taken for another one.
    let mut seen: HashMap<&Vec<u8>, usize> = HashMap::new();
    let twice: Vec<&String> = ta
        .iter()
        .filter(|(path, _)| !path.ends_with('/'))
        .filter(|(_, c)| {
            let n = seen.entry(c).or_default();
            *n += 1;
            *n > 1 + ledger.copied.get(c).copied().unwrap_or(0)
        })
        .map(|(path, _)| path)
        .collect();
    assert!(twice.is_empty(), "copies {twice:?}\ntree: {ta:?}");
    // The server holds the same tree.
    let live = server.lock().map_err(|_| "poisoned")?.live().count();
    assert_eq!(live, ta.len(), "server differs");
    Ok(())
}

/// Crashes `c` at its state DB's `at`-th commit of a sync (if it gets that far), just before
/// or after it, the DB keeping what `kept` says. A crash between operations,
/// or inside one; one between an operation's change on disk and a commit of its write-ahead
/// record is that record's case (the `CrashPoint` tests).
fn crash_at(c: &mut Client, at: usize, before: bool, kept: Kept) -> Res<bool> {
    common::crash(c, kept, move |now, n| {
        n == at && (now == Commit::Before) == before
    })
}

/// Crashes and power cuts at any of the first 64 commits of a sync, just before or after one:
/// the clients still converge, with nothing lost and nothing twice.
#[test]
fn randomized_crashes() -> R {
    let crashes = each_seed(seeds(), crashes_of)?;
    let total: usize = crashes.iter().sum();
    assert!(total >= crashes.len(), "only {total} crashes");
    Ok(())
}

/// Seed 291 of [`randomized_crashes`]: a download's commit lost, and recovered with the local
/// spelling of a conflict copy recorded for the drive's name although the two places differ.
/// The item was then moved, again and again, to where it already was: no convergence.
#[test]
fn regression_crash_seed_291() -> R {
    each_seed([291], crashes_of).map(drop)
}

/// One seed of [`randomized_crashes`]: how many crashes it had.
fn crashes_of(seed: u64) -> Res<usize> {
    let server = server();
    let mut clients = pair(&server)?;
    let mut rng = Rng(seed);
    let mut ledger = Ledger::default();
    let mut crashes = 0;
    // A client that crashed is down until it starts again: its first sync (the daemon's
    // start-up) comes before its user can do anything there; the other one goes on.
    let mut down = [false; 2];
    for _ in 0..30 {
        let i = rng.below(2);
        match rng.below(3) {
            0 => {
                if std::mem::take(&mut down[i]) {
                    sync(&mut clients[i], &mut ledger)?;
                }
                act(&clients[i].root, &mut rng, &mut ledger)?;
            }
            1 => {
                down[i] = false;
                sync(&mut clients[i], &mut ledger)?;
            }
            _ => {
                // Few syncs have many commits: the cut lands early more often.
                let at = if rng.below(2) == 0 {
                    rng.below(8)
                } else {
                    rng.below(64)
                };
                let before = rng.below(2) == 0;
                let kept = match rng.below(3) {
                    0 => Kept::All,
                    1 => Kept::Durable(None),
                    _ => Kept::Durable(Some(rng.below(at + 1))),
                };
                down[i] = crash_at(&mut clients[i], at, before, kept)?;
                crashes += usize::from(down[i]);
            }
        }
    }
    let [mut a, mut b] = clients;
    converged(&mut a, &mut b, &ledger, &server)?;
    Ok(crashes)
}

// ---- cross-OS cases ----

#[test]
fn unicode_and_case_clashes() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    let mut b = Client::with_config(&server, "b", |_| {})?;
    // Decomposed on one side, composed on the other: the same name.
    a.write("Re\u{301}sume\u{301}.txt", b"from a")?;
    a.sync()?;
    b.sync()?;
    let names: Vec<String> = b.entries("").into_iter().map(|(p, _)| p).collect();
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
            .entries("")
            .into_iter()
            .map(|(p, d)| (hbsync_platform::nfc(&p), d))
            .collect();
        t.sort();
        t
    };
    assert_eq!(nfc_tree(&a), nfc_tree(&b));
    let contents: Vec<Vec<u8>> = b.entries("").into_iter().map(|(_, c)| c).collect();
    assert!(contents.contains(&b"upper".to_vec()) && contents.contains(&b"lower".to_vec()));
    Ok(())
}

/// Asserts a sync went cleanly: nothing failed, skipped, held or in conflict.
fn assert_clean(report: &SyncReport) {
    assert!(
        report.failed.is_empty() && report.held.is_empty() && report.conflicts.is_empty(),
        "{report:?}"
    );
}

#[test]
fn windows_illegal_names_are_mapped_locally_only() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_types::NodeId;

    let server = server();
    // Names Windows can't hold, created by a Linux client elsewhere.
    let (question, _) = server.lock().map_err(|_| "poisoned")?.seed_file(
        NodeId::ROOT,
        "what?.txt",
        b"q",
        Blake3,
    )?;
    server.lock().map_err(|_| "poisoned")?.seed_file(
        NodeId::ROOT,
        "CON",
        b"device name",
        Blake3,
    )?;

    // A client applying the Windows rules (works on any disk: the escaped names are legal).
    let mut win = Client::with_config(&server, "win", |c| {
        c.folder.os = hbsync_platform::Os::Windows
    })?;
    assert_clean(&win.sync()?);
    let names: BTreeSet<String> = win.entries("").into_iter().map(|(p, _)| p).collect();
    assert_eq!(
        names,
        BTreeSet::from(["CON\u{F02A}".to_owned(), "what\u{F025}.txt".to_owned()])
    );

    // An edit on the Windows side updates the same node under its original remote name.
    win.write("what\u{F025}.txt", b"edited on windows")?;
    assert_clean(&win.sync()?);
    {
        let srv = server.lock().map_err(|_| "poisoned")?;
        assert_eq!(
            srv.live_names(),
            ["CON", "what?.txt"],
            "no escaped name reached the server"
        );
        let file = srv
            .file_id_of(question)
            .ok_or("the original node is still live")?;
        assert_eq!(srv.payload(file)?.as_slice(), b"edited on windows");
    }

    // A client with Unix rules round-trips the original names (NTFS can't hold them).
    #[cfg(not(windows))]
    {
        let mut unix = Client::with_config(&server, "unix", |c| {
            c.folder.os = hbsync_platform::Os::Linux
        })?;
        assert_clean(&unix.sync()?);
        assert_eq!(fs::read(unix.root.join("what?.txt"))?, b"edited on windows");
        unix.write("new:name.txt", b"from unix")?;
        assert_clean(&unix.sync()?);
        assert_clean(&win.sync()?);
        assert_eq!(
            fs::read(win.root.join("new\u{F022}name.txt"))?,
            b"from unix"
        );
        let srv = server.lock().map_err(|_| "poisoned")?;
        assert_eq!(srv.live_names(), ["CON", "new:name.txt", "what?.txt"]);
        assert_eq!(
            srv.file_id_of(question)
                .map(|f| srv.payload(f).map(|p| p.len()))
                .transpose()?,
            Some(17)
        );
    }
    Ok(())
}

#[test]
fn a_name_windows_cannot_hold_is_skipped_not_retried() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_types::NodeId;

    let server = server();
    // A device name at the length limit: its escape marker would be the 256th UTF-16 unit.
    let device = format!("CON.{}", "x".repeat(251));
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        srv.seed_file(NodeId::ROOT, &device, b"d", Blake3)?;
        srv.seed_file(NodeId::ROOT, "fine.txt", b"f", Blake3)?;
    }
    let mut win = Client::with_config(&server, "win", |c| {
        c.folder.os = hbsync_platform::Os::Windows
    })?;
    let report = win.sync()?;
    assert_clean(&report);
    assert!(
        report
            .skipped
            .iter()
            .any(|(_, why)| matches!(why, Skip::Unsyncable(w) if w.starts_with("name can't be used here"))),
        "{report:?}"
    );
    let names: Vec<String> = win.entries("").into_iter().map(|(p, _)| p).collect();
    assert_eq!(names, ["fine.txt"]);
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live().count(),
        2,
        "the remote file is untouched"
    );
    Ok(())
}

/// Drive names the name map wouldn't give back from their local spelling (decomposed, which
/// every OS's map composes; on Windows, an escape's own code point), each with how this OS
/// spells it.
const UNMAPPED: [(hbsync_platform::Os, &str); 3] = [
    (hbsync_platform::Os::Linux, "Re\u{301}sume\u{301}.txt"),
    (hbsync_platform::Os::MacOs, "e\u{301}"),
    (hbsync_platform::Os::Windows, "a\u{F022}b"),
];

/// Names the name map wouldn't give back ([`UNMAPPED`]): from the first download on, the item
/// reads as the drive's name, not as renamed. Otherwise an edit renamed the node on the drive.
#[test]
fn a_downloaded_name_the_map_wont_give_back_reads_as_the_drives() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_types::NodeId;

    for (os, name) in UNMAPPED {
        let server = server();
        let (edited, gone) = (format!("1{name}"), format!("2{name}"));
        let (edited_node, gone_node) = {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            (
                srv.seed_file(NodeId::ROOT, &edited, b"v1", Blake3)?.0,
                srv.seed_file(NodeId::ROOT, &gone, b"v1", Blake3)?.0,
            )
        };
        let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
        assert_clean(&c.sync()?);
        // An edit here goes up to the same node, under the drive's name.
        c.write(&edited, b"v2")?;
        assert_clean(&c.sync()?);
        // The drive deletes the other: it goes here too, and doesn't come back.
        {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            let file = srv.file_id_of(gone_node).ok_or("live")?;
            let version = srv.header(file)?.version;
            srv.delete(file, version)?;
        }
        assert_clean(&c.sync()?);
        let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
        assert_eq!(names, std::slice::from_ref(&edited), "{os:?}");
        let srv = server.lock().map_err(|_| "poisoned")?;
        assert_eq!(srv.live_names(), [edited], "{os:?}");
        let file = srv.file_id_of(edited_node).ok_or("the same node")?;
        assert_eq!(srv.payload(file)?.as_slice(), b"v2", "{os:?}");
    }
    Ok(())
}

/// A rename on the drive to a name the name map wouldn't give back ([`UNMAPPED`]; on Windows,
/// from the name this device spells the same): it settles. Otherwise the item never read as at
/// the new name, and every sync moved it again, up to the round limit.
#[test]
fn a_rename_on_the_drive_to_a_name_the_map_wont_give_back_settles() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_remote::{NodeMeta, RemoteBody};
    use hbsync_types::NodeId;

    for (os, to) in UNMAPPED {
        let server = server();
        let from = hbsync_platform::nfc(&to.replace('\u{F022}', ":"));
        let node = (server.lock().map_err(|_| "poisoned")?)
            .seed_file(NodeId::ROOT, &from, b"v1", Blake3)?
            .0;
        let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
        assert_clean(&c.sync()?);
        {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            let file = srv.file_id_of(node).ok_or("live")?;
            let h = srv.header(file)?;
            let RemoteBody::Live { mut content, .. } = h.body else {
                return Err("live".into());
            };
            content.name = to.to_owned();
            let meta = NodeMeta {
                node,
                parent: h.parent,
                kind: h.kind,
                content,
                removed: false,
            };
            srv.update(file, h.version, &meta, None)?;
        }
        let report = c.sync()?;
        assert_clean(&report);
        assert_eq!(report.pending, 0, "{os:?}: {report:?}");
        assert!(c.sync()?.is_idle(), "{os:?}");
        let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
        assert_eq!(names, [to], "{os:?}");
        assert_eq!(
            server.lock().map_err(|_| "poisoned")?.live_names(),
            [to],
            "{os:?}"
        );
    }
    Ok(())
}

/// A first run over a copy of a node whose drive name is decomposed ([`UNMAPPED`]), hashed
/// with this device's algorithm or another: the copy is adopted, no conflict and nothing
/// uploaded. Otherwise it read as both sides moved, or, across hash algorithms, wasn't adopted
/// at all and went up as a conflict copy. (Not a drive name holding an escape's own code point
/// on Windows: adoption looks for the drive's node under the name the copy maps to, which
/// differs.)
#[test]
fn a_copy_at_a_decomposed_drive_name_is_adopted() -> R {
    use hbsync_fileio::HashAlgorithm::{Blake3, Sha256};
    use hbsync_platform::Os;
    use hbsync_types::NodeId;

    for (os, name) in UNMAPPED.into_iter().filter(|(os, _)| *os != Os::Windows) {
        for algorithm in [Blake3, Sha256] {
            let server = server();
            server.lock().map_err(|_| "poisoned")?.seed_file(
                NodeId::ROOT,
                name,
                b"same",
                algorithm,
            )?;
            let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
            c.write(name, b"same")?;
            let report = c.sync()?;
            assert_clean(&report);
            assert_eq!(report.adopted, 1, "{os:?}, {algorithm:?}: {report:?}");
            assert!(c.sync()?.is_idle(), "{os:?}, {algorithm:?}");
            let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
            assert_eq!(names, [name], "{os:?}, {algorithm:?}");
            assert_eq!(
                server.lock().map_err(|_| "poisoned")?.live_names(),
                [name],
                "{os:?}, {algorithm:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn large_file_streams_and_resumes() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    let mut b = Client::with_config(&server, "b", |_| {})?;
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
    let mut a = Client::with_config(&server, "a", |_| {})?;
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
fn a_confirmation_lets_through_exactly_the_deletes_the_user_saw() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    for i in 0..120 {
        a.write(&format!("f{i:03}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    let live = |server: &std::sync::Arc<std::sync::Mutex<hbsync_remote::fake::FakeServer>>| {
        server
            .lock()
            .map(|s| s.live().count())
            .map_err(|_| "poisoned")
    };
    let gone = |a: &Client, range: std::ops::Range<usize>| -> std::io::Result<()> {
        range
            .into_iter()
            .try_for_each(|i| fs::remove_file(a.root.join(format!("f{i:03}"))))
    };
    gone(&a, 0..60)?;
    let report = a.sync()?;
    assert_eq!(report.held.len(), 60);
    assert_eq!(a.engine.db().read(|s| s.pending_file_deletes())?, (60, 0));
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();

    // 55 more deletes after the user looked: not part of what they confirm. Alone they are
    // over the limit (max(50, 25 % of 120)), so they are held; the confirmed 60 go through.
    gone(&a, 60..115)?;
    a.engine.confirm_deletes(seen.iter().copied());
    let report = a.sync()?;
    assert_eq!(report.held.len(), 55, "{report:?}");
    assert!(report.held.iter().all(|op| !seen.contains(&op.node())));
    assert_eq!(live(&server)?, 60);

    // Confirming those too lets them through.
    a.engine
        .confirm_deletes(report.held.iter().map(|op| op.node()));
    assert!(a.sync()?.held.is_empty());
    assert_eq!(live(&server)?, 5);

    // A confirmation doesn't outlive its deletes: deleting the last five, now every file, is
    // held again.
    gone(&a, 115..120)?;
    assert_eq!(a.sync()?.held.len(), 5);
    assert_eq!(live(&server)?, 5);
    Ok(())
}

#[test]
fn a_confirmed_delete_that_was_undone_is_forgotten() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.min = 2)?;
    for i in 0..4 {
        a.write(&format!("f{i}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..4 {
        fs::remove_file(a.root.join(format!("f{i}")))?;
    }
    let report = a.sync()?;
    assert_eq!(report.held.len(), 4);
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();
    // The user brings the files back (the deletes undone) before the next sync, then
    // confirms what they were shown.
    for i in 0..4 {
        a.write(&format!("f{i}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    a.engine.confirm_deletes(seen);
    a.sync()?;
    // The same files deleted again later: a new delete, held again.
    for i in 0..4 {
        fs::remove_file(a.root.join(format!("f{i}")))?;
    }
    assert_eq!(a.sync()?.held.len(), 4);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 4);
    Ok(())
}

/// A confirmed trash of a folder takes the files under it along: they are the user's, and
/// don't count against other deletes. A small delete that comes in with it isn't held.
#[test]
fn a_confirmed_folder_trash_does_not_hold_other_deletes() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    for i in 0..60 {
        a.write(&format!("dir/f{i:02}"), format!("{i}").as_bytes())?;
        a.write(&format!("g{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(b.entries("").len(), 121);

    // Deleted elsewhere: trashing it here is held (60 of 120 files, over the limit of 50).
    fs::remove_dir_all(a.root.join("dir"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(!report.held.is_empty());
    assert!(b.root.join("dir").exists());
    let seen: Vec<_> = report.held.iter().map(|op| op.node()).collect();

    // The user confirms; meanwhile one more file is deleted elsewhere, well under the limit.
    // One sync (as the daemon runs it) does both.
    fs::remove_file(a.root.join("g00"))?;
    a.sync()?;
    b.engine.confirm_deletes(seen);
    b.engine.scan_local()?;
    let report = b.engine.sync_once("2026-10-08")?;
    assert!(report.held.is_empty(), "{:?}", report.held);
    assert!(!b.root.join("dir").exists());
    assert!(!b.root.join("g00").exists());
    Ok(())
}

#[test]
fn held_deletes_do_not_hold_up_other_changes() -> R {
    let server = server();
    // 5 nodes per round, 2 rounds per sync: one sync sees 10 of the 61 dirty nodes.
    let mut a = Client::with_config(&server, "a", |c| {
        c.batch = 5;
        c.max_rounds = 2;
    })?;
    for i in 0..60 {
        a.write(&format!("f{i:02}"), format!("{i}").as_bytes())?;
    }
    a.sync()?;
    for i in 0..60 {
        fs::remove_file(a.root.join(format!("f{i:02}")))?;
    }
    a.write("new.txt", b"new")?;
    a.engine.scan_local()?;
    let mut held = std::collections::BTreeSet::new();
    for _ in 0..8 {
        let report = a.engine.sync_once("2026-10-08")?;
        held.extend(report.held.iter().map(|op| format!("{op:?}")));
    }
    assert_eq!(held.len(), 60, "every delete was seen and held");
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(
        srv.live().count(),
        61,
        "the deletes are held, the new file is up"
    );
    assert!(srv.live_names().iter().any(|n| n == "new.txt"));
    Ok(())
}

#[test]
fn a_mass_delete_split_over_batches_is_still_held() -> R {
    let server = server();
    let small_batches = |c: &mut EngineConfig| {
        c.batch = 10;
        c.guard.min = 20;
    };
    let mut a = Client::with_config(&server, "a", small_batches)?;
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

/// On Windows, where files have no x bit, an edit keeps the bit the file has elsewhere (on Unix
/// the bit is read from disk, so this holds trivially there).
#[test]
fn an_edit_keeps_the_executable_bit() -> R {
    use hbsync_remote::{NodeMeta, RemoteBody, Upload};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, NodeKind};

    let server = server();
    // A Unix client uploaded an executable script.
    let script = b"#!/bin/sh\necho hi\n";
    server.lock().map_err(|_| "poisoned")?.create(
        &NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file("run.sh", 1_700_000_000_000, script.len() as u64, true),
            removed: false,
        },
        Some(Upload {
            hash: hbsync_fileio::hash_bytes(script, hbsync_fileio::HashAlgorithm::Blake3),
            size: script.len() as u64,
            reader: &mut std::io::Cursor::new(&script[..]),
            pace: None,
        }),
    )?;
    let mut a = Client::with_config(&server, "a", |_| {})?;
    assert_clean(&a.sync()?);
    a.write("run.sh", b"#!/bin/sh\necho edited\n")?;
    assert_clean(&a.sync()?);
    let srv = server.lock().map_err(|_| "poisoned")?;
    let x: Vec<bool> = srv
        .live()
        .filter_map(|h| match &h.body {
            RemoteBody::Live { content, .. } => Some(content.x),
            _ => None,
        })
        .collect();
    assert_eq!(x, [true]);
    Ok(())
}

/// A file's and a folder's creation time travels with them, set where the system allows it
/// (Windows, macOS; Linux can't set one, only send it), and stays through an editor's save (a
/// new file renamed over the old one) and a rename elsewhere. A download that replaces a file
/// gives it the node's time, not the one the replaced file had.
#[test]
fn creation_times_travel_and_stay() -> R {
    use hbsync_platform::ops;
    use hbsync_remote::RemoteBody;

    let (y2016, y2018, y2020) = (1_451_606_400_123, 1_514_764_800_789, 1_577_836_800_456);
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Folder/doc.txt", b"first")?;
    for rel in ["Folder/doc.txt", "Folder"] {
        let path = a.root.join(rel);
        ops::set_mtime(&path, y2020 * 1_000_000)?;
        ops::set_created(&path, y2016)?;
    }
    let sent = ops::created_ms(&a.root.join("Folder/doc.txt"));
    assert!(sent.is_some(), "every test file system records one");
    if ops::CAN_SET_CREATED {
        assert_eq!(sent, Some(y2016));
    }
    let on_server = |name: &str| -> Result<Option<i64>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let found = srv.live().find_map(|h| match &h.body {
            RemoteBody::Live { content, .. } if content.name == name => Some(content.created_ms()),
            _ => None,
        });
        Ok(found.ok_or("not on the server")?)
    };
    settle_all(&mut a, &mut b)?;
    assert_eq!(on_server("doc.txt")?, sent);
    if ops::CAN_SET_CREATED {
        assert_eq!(on_server("Folder")?, Some(y2016));
        assert_eq!(ops::created_ms(&b.root.join("Folder/doc.txt")), Some(y2016));
        assert_eq!(ops::created_ms(&b.root.join("Folder")), Some(y2016));
    }

    // a's copy gets another creation time; b saves an edit as editors do.
    ops::set_created(&a.root.join("Folder/doc.txt"), y2018)?;
    fs::write(b.root.join("Folder/doc.txt.tmp"), b"second")?;
    fs::rename(
        b.root.join("Folder/doc.txt.tmp"),
        b.root.join("Folder/doc.txt"),
    )?;
    settle_all(&mut a, &mut b)?;
    assert_eq!(on_server("doc.txt")?, sent, "the node keeps its time");
    assert_eq!(a.read_text("Folder/doc.txt")?, "second");
    if ops::CAN_SET_CREATED {
        assert_eq!(ops::created_ms(&a.root.join("Folder/doc.txt")), Some(y2016));
    }

    fs::rename(
        b.root.join("Folder/doc.txt"),
        b.root.join("Folder/renamed.txt"),
    )?;
    settle_all(&mut a, &mut b)?;
    assert_eq!(on_server("renamed.txt")?, sent);
    if ops::CAN_SET_CREATED {
        assert_eq!(
            ops::created_ms(&a.root.join("Folder/renamed.txt")),
            Some(y2016)
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn executable_bit_syncs() -> R {
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::with_config(&server, "a", |_| {})?;
    let mut b = Client::with_config(&server, "b", |_| {})?;
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

/// Another writer's folder named as this client's state folder in another case (`.HBSYNC`), at
/// the top, with an `ignore` file in it: on a volume that ignores case, that is the state
/// folder itself. The client never takes it for the drive's folder, nor puts what is in it
/// there (an ignore file would set this device's rules). It did on macOS and Windows (CI).
#[test]
fn a_folder_named_as_the_state_folder_in_another_case_stays_out_of_it() -> R {
    use hbsync_remote::NodeMeta;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, NodeKind};

    let server = server();
    let mut a = Client::new(&server, "a")?;
    let state = a.root.join(hbsync_local::STATE_DIR);
    let listing = |dir: &Path| -> BTreeSet<String> {
        fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect()
    };
    let before = listing(&state);
    let folder = NodeId::new();
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        srv.create(
            &NodeMeta {
                node: folder,
                parent: NodeId::ROOT,
                kind: NodeKind::Folder,
                content: NodeContent::folder(".HBSYNC"),
                removed: false,
            },
            None,
        )?;
        srv.seed_file(
            folder,
            "ignore",
            b"*\n",
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
    }
    a.sync()?;
    a.sync()?;
    let after = listing(&state);
    assert!(
        !after.contains("ignore") || before.contains("ignore"),
        "an ignore file came into the state folder: {after:?}"
    );
    // Nothing of the drive's in it (its own `tmp` may come).
    let new: Vec<&String> = after.difference(&before).filter(|n| *n != "tmp").collect();
    assert!(
        new.is_empty(),
        "the drive's {new:?} came into the state folder"
    );
    Ok(())
}
