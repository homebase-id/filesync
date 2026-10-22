//! Randomized runs of two clients: convergence, wrong not-theres, crashes, and crashes
//! around the guard's answers.

use super::*;

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

/// As [`randomized_crashes`], with the mass-delete guard on at its strictest (no minimum, no
/// share): every delete here is held, and so is a removal elsewhere of every synced file. The
/// user answers what is shown, confirming or putting back, now and then right after the sync,
/// and crashes and power cuts come at any commit around those answers: a confirmation lives
/// in the engine's memory, so the user gives it again after a start (as the dialog shows the
/// deletes again), and the confirmed deletes run in the sync that crashes. At the end what is
/// still held is confirmed till nothing is: the clients converge, nothing lost, nothing twice;
/// and the seeds confirmed deletes and put items back.
#[test]
fn randomized_guarded_crashes() -> R {
    let runs = each_seed(seeds(), guarded_crashes_of)?;
    let sum = |f: fn(&Guarded) -> usize| runs.iter().map(f).sum::<usize>();
    let (crashes, confirmed, put_back) = (
        sum(|g| g.crashes),
        sum(|g| g.confirmed),
        sum(|g| g.put_back),
    );
    assert!(crashes >= runs.len(), "only {crashes} crashes");
    assert!(
        runs.len() < 4 || (confirmed > 0 && put_back > 0),
        "answers: {confirmed} confirmed, {put_back} items put back"
    );
    Ok(())
}

/// What one seed of [`randomized_guarded_crashes`] had: crashes, and the answers given.
#[derive(Default)]
struct Guarded {
    crashes: usize,
    confirmed: usize,
    /// What put backs brought back.
    put_back: usize,
}

/// Two clients of `server` with the guard on at its strictest: every delete here is held.
fn guarded_pair(server: &Arc<Mutex<FakeServer>>) -> Res<[Client; 2]> {
    let guard = |c: &mut EngineConfig| {
        c.guard.min = 0;
        c.guard.fraction = 0.0;
    };
    Ok([
        Client::with_config(server, "a", guard)?,
        Client::with_config(server, "b", guard)?,
    ])
}

/// What a sync of `report` holds for the user, and whether it did anything.
fn shown(report: &SyncReport) -> (Vec<NodeId>, bool) {
    let held = report.held.iter().map(|op| op.node()).collect();
    (held, report.ops_done > 0)
}

/// Syncs `c`, whose scan then saw what its user did: [`shown`].
fn guarded_sync(c: &mut Client, ledger: &mut Ledger) -> Res<(Vec<NodeId>, bool)> {
    let report = c.sync()?;
    ledger.scanned(&c.root);
    Ok(shown(&report))
}

/// The user answers `held` on `c`: confirms them (kept in `confirmed`, to give again after a
/// start), or puts them back, all of them.
fn answer(
    c: &mut Client,
    held: Vec<NodeId>,
    confirmed: &mut Vec<NodeId>,
    rng: &mut Rng,
    run: &mut Guarded,
) -> Res<()> {
    if rng.below(2) == 0 {
        c.engine.confirm_deletes(held.iter().copied());
        confirmed.extend(held);
        run.confirmed += 1;
    } else {
        // Each that the drive still has as it was (removed meanwhile there too, it stays so),
        // counted with what in them it put back too: more than were held, or none.
        run.put_back += c.engine.put_back(&held)?;
    }
    Ok(())
}

/// One seed of [`randomized_guarded_crashes`].
fn guarded_crashes_of(seed: u64) -> Res<Guarded> {
    let server = server();
    let mut clients = guarded_pair(&server)?;
    let mut rng = Rng(seed);
    let mut ledger = Ledger::default();
    let mut run = Guarded::default();
    let mut down = [false; 2];
    // What each client last held for its user, and what the user confirmed of it since.
    let mut held: [Vec<NodeId>; 2] = Default::default();
    let mut confirmed: [Vec<NodeId>; 2] = Default::default();
    for _ in 0..30 {
        let i = rng.below(2);
        match rng.below(4) {
            0 => {
                if std::mem::take(&mut down[i]) {
                    held[i] = guarded_sync(&mut clients[i], &mut ledger)?.0;
                }
                let root = &clients[i].root;
                act(root, &mut rng, &mut ledger)?;
                // And deletes more often than a user's other acts: what the guard holds.
                if rng.below(3) == 0 {
                    let (dirs, files) = entries(root);
                    delete_some(root, &dirs, &files, &mut rng, &mut ledger)?;
                }
            }
            1 => {
                down[i] = false;
                held[i] = guarded_sync(&mut clients[i], &mut ledger)?.0;
                confirmed[i].clear();
                // As often as not, the user answers right away.
                if !held[i].is_empty() && rng.below(2) == 0 {
                    let shown = std::mem::take(&mut held[i]);
                    answer(
                        &mut clients[i],
                        shown,
                        &mut confirmed[i],
                        &mut rng,
                        &mut run,
                    )?;
                }
            }
            2 => {
                if !held[i].is_empty() && !down[i] {
                    let shown = std::mem::take(&mut held[i]);
                    answer(
                        &mut clients[i],
                        shown,
                        &mut confirmed[i],
                        &mut rng,
                        &mut run,
                    )?;
                } else {
                    held[i] = guarded_sync(&mut clients[i], &mut ledger)?.0;
                    confirmed[i].clear();
                }
            }
            _ => {
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
                let again = std::mem::take(&mut confirmed[i]);
                let (crashed, report) = common::crash_with(
                    &mut clients[i],
                    kept,
                    move |now, n| n == at && (now == Commit::Before) == before,
                    move |c| c.engine.confirm_deletes(again),
                )?;
                down[i] = crashed;
                run.crashes += usize::from(crashed);
                held[i] = report.map(|r| shown(&r).0).unwrap_or_default();
            }
        }
    }
    // The user confirms what is still held, till a round holds nothing and does nothing (a
    // delete confirmed on one may be held on the other: every synced file there).
    for _ in 0..12 {
        let mut any = false;
        for c in clients.iter_mut() {
            let (shown, did) = guarded_sync(c, &mut ledger)?;
            any |= did || !shown.is_empty();
            if !shown.is_empty() {
                c.engine.confirm_deletes(shown);
            }
        }
        if !any {
            break;
        }
    }
    let [mut a, mut b] = clients;
    converged(&mut a, &mut b, &ledger, &server)?;
    Ok(run)
}
