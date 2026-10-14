//! Development tool: syncs a folder with an identity's Filesync drive until stopped.
//!
//! ```text
//! hbsync-sync <identity> <folder> [--once] [--allow-mass-delete]
//! ```
//!
//! Uses the credentials `hbsync-register` saved, and the daemon's sync loop
//! ([`hbsync_app::Runner`]) without the daemon: the account's state DB lives beside the
//! folder (`<folder>.state.db`), and what happens is logged to stderr. `--once` catches up, syncs and exits. Deletes of a large
//! share of the synced files are held unless `--allow-mass-delete` is given.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use hbsync_app::{HintSource, Runner, RunnerConfig, State};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::Notifier;
use hbsync_local::{IGNORE_FILE, STATE_DIR};
use hbsync_remote::Remote;

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(identity), Some(folder)) = (args.first(), args.get(1)) else {
        return Err("usage: hbsync-sync <identity> <folder> [--once] [--allow-mass-delete]".into());
    };
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();

    let client = hbsync_dev::saved_client(identity)?;
    let remote = Arc::new(HomebaseRemote::connect(client.clone())?);
    std::fs::create_dir_all(folder)?;
    let root = std::fs::canonicalize(folder)?;
    let db = root.with_extension("state.db");
    if hbsync_app::accounts::open_folder(&db, identity, &root).is_err() {
        hbsync_app::accounts::add_folder(&db, identity, &root)?;
    }
    let device = hbsync_platform::host::device_name();
    let mut cfg = RunnerConfig::new(&root, &db, identity, &device);
    cfg.once = args.iter().any(|a| a == "--once");
    if args.iter().any(|a| a == "--allow-mass-delete") {
        cfg.tweak = Some(|c| c.allow_mass_delete = true);
    }
    println!(
        "Syncing {} with {identity} (drive {}); built-in junk filter, rules in {STATE_DIR}/{IGNORE_FILE}",
        root.display(),
        remote.drive().alias.simple(),
    );
    let hints = if cfg.once {
        HintSource::none()
    } else {
        let (tx, rx) = mpsc::channel();
        HintSource {
            notifier: Some(Notifier::spawn(client, remote.drive(), tx)),
            events: rx,
        }
    };
    let (handle, thread) = Runner::spawn(cfg, remote.clone(), hints)?;
    while !thread.is_finished() {
        std::thread::sleep(Duration::from_millis(500));
        for why in remote.take_skipped() {
            println!("feed row skipped: {why}");
        }
        if handle.status().state == State::Denied {
            break;
        }
    }
    let status = handle.status();
    if status.state == State::Denied {
        return Err(format!(
            "{}\nThe identity refused this device: authorize it again with hbsync-register.",
            status.error.unwrap_or_default()
        )
        .into());
    }
    thread.join().map_err(|_| "the sync thread panicked")??;
    if let Some(e) = status.error {
        return Err(e.into());
    }
    Ok(())
}
