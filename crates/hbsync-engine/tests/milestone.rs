//! Layer 4 milestone: two clients on real folders, syncing through the fake remote.
//!
//! The randomized test drives real file-system operations on both clients with interleaved
//! syncs, then checks that both folders and the server converge and that no file version was
//! lost unless a client overwrote or deleted it (the simulator's properties, on a real disk).

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hbsync_engine::exec::Skip;
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_platform::name::name_key;
use hbsync_remote::fake::FakeServer;
use hbsync_state::testkit::{Hook, on_commit};

mod common;

use common::{Client, Res, server};

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
            let to = root.join(dir).join(name);
            if dir.starts_with(item) {
                return Ok(());
            }
            if !name_free(root, dir, name) {
                // A file moved over another file (`mv c d` over `d`): d's bytes go.
                let over = files.iter().find(|f| {
                    f.parent() == Some(dir.as_path())
                        && f.file_name()
                            .is_some_and(|n| name_key(&n.to_string_lossy()) == name_key(name))
                });
                return match over {
                    Some(over) if files.contains(item) && item != over => {
                        ledger.superseded.insert(fs::read(root.join(over))?);
                        fs::rename(root.join(item), root.join(over))
                    }
                    _ => Ok(()),
                };
            }
            fs::rename(root.join(item), to)
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
        let mut a = Client::with_config(&server, "a", allow)?;
        let mut b = Client::with_config(&server, "b", allow)?;
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
        converged(seed, &mut a, &mut b, &ledger, &server)?;
    }
    Ok(())
}

/// Settles both clients, then checks that they and the server hold the same tree, that no
/// content was lost that a user didn't replace or delete, and none is there twice.
fn converged(
    seed: u64,
    a: &mut Client,
    b: &mut Client,
    ledger: &Ledger,
    server: &Arc<Mutex<FakeServer>>,
) -> R {
    settle_all(a, b).map_err(|e| format!("seed {seed}: {e}"))?;
    let (ta, tb) = (a.entries(""), b.entries(""));
    assert_eq!(ta, tb, "seed {seed}: clients diverged");
    let present: HashSet<&Vec<u8>> = ta.iter().map(|(_, c)| c).collect();
    let lost: Vec<String> = ledger
        .written
        .iter()
        .filter(|c| !ledger.superseded.contains(*c) && !present.contains(c))
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect();
    assert!(lost.is_empty(), "seed {seed}: lost {lost:?}\ntree: {ta:?}");
    // Every write has bytes of its own, and nothing copies a file: bytes twice are an item
    // taken for another one.
    let mut seen = HashSet::new();
    let twice: Vec<&String> = ta
        .iter()
        .filter(|(path, c)| !path.ends_with('/') && !seen.insert(c))
        .map(|(path, _)| path)
        .collect();
    assert!(
        twice.is_empty(),
        "seed {seed}: copies {twice:?}\ntree: {ta:?}"
    );
    // The server holds the same tree.
    let live = server.lock().map_err(|_| "poisoned")?.live().count();
    assert_eq!(live, ta.len(), "seed {seed}: server differs");
    Ok(())
}

/// Syncs `c` until it crashes at its state DB's `at`-th commit (if it gets that far): just
/// before it with `before` (that operation's DB changes are lost, what it did on disk or on the
/// drive stays), else just after. Then starts it again. Returns whether it crashed. A crash
/// between operations, or inside one; one between an operation's change on disk and a commit
/// of its write-ahead record is that record's case (the `CrashPoint` tests).
fn crash(c: &mut Client, at: usize, before: bool) -> Res<bool> {
    let db = c.db_path();
    let commits = AtomicUsize::new(0);
    let hook = Hook(Arc::new(move |committed| {
        if committed != before && commits.fetch_add(1, Ordering::SeqCst) == at {
            std::panic::panic_any(Crash);
        }
    }));
    on_commit(&db, Some(hook));
    c.restart()?;
    let synced = std::panic::catch_unwind(AssertUnwindSafe(|| c.sync()));
    on_commit(&db, None);
    let crashed = match synced {
        Ok(synced) => {
            synced?;
            false
        }
        Err(e) if e.is::<Crash>() => true,
        Err(e) => std::panic::resume_unwind(e),
    };
    c.restart()?;
    Ok(crashed)
}

/// The panic that crashes a client.
struct Crash;

/// Crashes at any of the first 64 commits of a sync, just before or after one: the clients
/// still converge, with nothing lost and nothing twice.
#[test]
fn randomized_crashes() -> R {
    let seeds: u64 = std::env::var("MILESTONE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let mut crashes = 0;
    for seed in 0..seeds {
        let server = server();
        let allow = |c: &mut EngineConfig| c.allow_mass_delete = true;
        let mut clients = [
            Client::with_config(&server, "a", allow)?,
            Client::with_config(&server, "b", allow)?,
        ];
        let mut rng = Rng(seed);
        let mut ledger = Ledger::default();
        // A client that crashed is down until it starts again: its first sync (the daemon's
        // start-up) comes before its user can do anything there; the other one goes on.
        let mut down = [false; 2];
        for _ in 0..30 {
            let i = rng.below(2);
            match rng.below(3) {
                0 => {
                    if std::mem::take(&mut down[i]) {
                        clients[i].sync()?;
                    }
                    act(&clients[i].root, &mut rng, &mut ledger)?;
                }
                1 => {
                    down[i] = false;
                    clients[i].sync()?;
                }
                _ => {
                    // Few syncs have many commits: the cut lands early more often.
                    let at = if rng.below(2) == 0 {
                        rng.below(8)
                    } else {
                        rng.below(64)
                    };
                    down[i] = crash(&mut clients[i], at, rng.below(2) == 0)
                        .map_err(|e| format!("seed {seed}: {e}"))?;
                    crashes += usize::from(down[i]);
                }
            }
        }
        let [mut a, mut b] = clients;
        converged(seed, &mut a, &mut b, &ledger, &server)?;
    }
    assert!(crashes as u64 >= seeds, "only {crashes} crashes");
    Ok(())
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
    let mut win =
        Client::with_config(&server, "win", |c| c.exec.os = hbsync_platform::Os::Windows)?;
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
        let mut unix =
            Client::with_config(&server, "unix", |c| c.exec.os = hbsync_platform::Os::Linux)?;
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
    let mut win =
        Client::with_config(&server, "win", |c| c.exec.os = hbsync_platform::Os::Windows)?;
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
    let mut a = Client::with_config(&server, "a", |c| c.mass_delete_min = 2)?;
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
    let mut a = Client::with_config(&server, "a", |c| c.allow_mass_delete = true)?;
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
        c.mass_delete_min = 20;
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
