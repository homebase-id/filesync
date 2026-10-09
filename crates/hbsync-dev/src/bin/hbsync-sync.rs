//! Development tool: syncs a folder with an identity's Filesync drive until stopped.
//!
//! ```text
//! hbsync-sync <identity> <folder> [--once] [--allow-mass-delete]
//! ```
//!
//! Uses the credentials `hbsync-register` saved. The state DB lives in `<folder>/.hbsync`.
//! A sync runs when the folder changes (live watcher), when the identity announces a change
//! on the drive (WebSocket), and every minute regardless. After a failed sync it waits,
//! doubling up to five minutes. `--once` catches up, syncs and exits. Deletes of a large
//! share of the synced files are held unless `--allow-mass-delete` is given.

use std::sync::Arc;
use std::sync::mpsc::{self, TryRecvError};
use std::time::{Duration, Instant};

use hbsync_engine::{Engine, EngineConfig, EngineError, SyncReport};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_local::watch::Debounce;
use hbsync_local::{LocalObserver, STATE_DIR, ScanOptions, state_db_path};
use hbsync_state::StateDb;

const POLL: Duration = Duration::from_secs(60);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(identity), Some(folder)) = (args.first(), args.get(1)) else {
        return Err("usage: hbsync-sync <identity> <folder> [--once] [--allow-mass-delete]".into());
    };
    let once = args.iter().any(|a| a == "--once");

    let client = hbsync_dev::saved_client(identity)?;
    let remote = Arc::new(HomebaseRemote::connect(client.clone())?);
    std::fs::create_dir_all(folder)?;
    let root = std::fs::canonicalize(folder)?;
    std::fs::create_dir_all(root.join(STATE_DIR))?;
    let db_path = state_db_path(&root);
    let mut cfg = EngineConfig::new(&root, &hbsync_dev::device_name());
    cfg.allow_mass_delete = args.iter().any(|a| a == "--allow-mass-delete");
    let mut observer =
        LocalObserver::start(&root, ScanOptions::for_root(&root, cfg.exec.algorithm));
    let mut engine = Engine::new(StateDb::open(&db_path)?, Box::new(remote.clone()), cfg);
    println!(
        "Syncing {} with {identity} (drive {})",
        root.display(),
        remote.drive().alias.simple()
    );
    if let Some(why) = &observer.watcher_error {
        println!("No live watcher ({why}); the folder is rescanned at every poll.");
    }

    let started = Instant::now();
    let startup = engine.start_up(&mut observer, &db_path, &hbsync_dev::today());
    match startup {
        Ok(r) => {
            println!(
                "Start-up in {:.1?}: local {:?}, feed {} live / {} deleted",
                started.elapsed(),
                r.local,
                r.feed.live,
                r.feed.deleted
            );
            print_report(&r.sync, &remote);
        }
        Err(e) => return Err(stopped(e)),
    }
    if once {
        return Ok(());
    }

    let (tx, rx) = mpsc::channel();
    let _notifier = Notifier::spawn(client, remote.drive(), tx);
    let mut wanted = false;
    let mut next_poll = Instant::now() + POLL;
    let mut not_before = Instant::now();
    let mut backoff = Duration::from_secs(5);
    loop {
        // Local changes: the watcher's batches are recorded in the DB as they come.
        if observer
            .poll(engine.db(), Duration::from_millis(250), Debounce::default())?
            .is_some()
        {
            wanted = true;
        }
        loop {
            match rx.try_recv() {
                Ok(Event::Connected) => {
                    println!("Notifications connected.");
                    wanted = true;
                }
                Ok(Event::Changed) => wanted = true,
                Ok(Event::Lost(why)) => println!("Notifications lost ({why}); reconnecting."),
                Ok(Event::Refused(why)) => {
                    println!("Notifications refused ({why}); syncing on the minute poll only.");
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err("the notifier stopped".into()),
            }
        }
        let now = Instant::now();
        if now >= next_poll {
            // Once a minute, whether the syncs succeed or back off.
            next_poll = now + POLL;
            wanted = true;
            if !observer.has_watcher() {
                engine.scan_local()?;
            }
        }
        if !wanted || now < not_before {
            continue;
        }
        let started = Instant::now();
        match engine.sync_once(&hbsync_dev::today()) {
            Ok(report) => {
                if !report.is_idle() || report.feed.live + report.feed.deleted > 0 {
                    print!("Sync in {:.1?}: ", started.elapsed());
                    print_report(&report, &remote);
                }
                // Work that is left (deferred, held) waits for the next change or poll.
                wanted = false;
                backoff = Duration::from_secs(5);
            }
            Err(e) if e.is_denied() => return Err(stopped(e)),
            Err(e) => {
                println!("Sync failed ({e}); trying again in {backoff:?}.");
                not_before = Instant::now() + backoff;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

fn stopped(e: EngineError) -> Box<dyn std::error::Error> {
    if e.is_denied() {
        format!("{e}\nThe identity refused this device: authorize it again with hbsync-register.")
            .into()
    } else {
        e.into()
    }
}

fn print_report(r: &SyncReport, remote: &HomebaseRemote) {
    println!(
        "{} done, feed +{} live / {} deleted, {} adopted, {} conflicts, {} skipped, {} failed, {} pending",
        r.ops_done,
        r.feed.live,
        r.feed.deleted,
        r.adopted,
        r.conflicts.len(),
        r.skipped.len(),
        r.failed.len(),
        r.pending
    );
    for c in &r.conflicts {
        println!("  conflict: {c:?}");
    }
    for (op, why) in r.failed.iter().chain(&r.skipped) {
        println!("  {op:?}: {why}");
    }
    if !r.held.is_empty() {
        println!("  {} deletes held by the mass-delete guard", r.held.len());
    }
    for why in remote.take_skipped() {
        println!("  feed row skipped: {why}");
    }
}
