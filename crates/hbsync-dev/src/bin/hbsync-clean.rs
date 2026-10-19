//! Development tool: clears a test identity's Filesync drive of what tests and benchmarks leave.
//!
//! ```text
//! hbsync-clean <identity> --tombstones [--dry]
//! ```
//!
//! `--tombstones` deletes for good (`hard-delete-batch`, 1,000 a request) every tombstone on
//! the drive, each at the version the feed shows: the traces of deletes, which clients keep 30
//! days so every device learns of them. Never a live row, removed (Recently deleted) or not.
//! For a test identity, between runs: a device that hasn't read the feed since loses the
//! trace of those deletes. `--dry` only counts them.

use hbsync_homebase::drive::HomebaseRemote;

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(identity), true) = (args.first(), args.iter().any(|a| a == "--tombstones")) else {
        return Err("usage: hbsync-clean <identity> --tombstones [--dry]".into());
    };
    let dry = args.iter().any(|a| a == "--dry");
    let remote = HomebaseRemote::connect(hbsync_dev::saved_client(identity)?)?;
    let tombstones = hbsync_dev::tombstones(&remote, None)?;
    println!("{} tombstones on the drive", tombstones.len());
    if dry || tombstones.is_empty() {
        return Ok(());
    }
    let swept = hbsync_dev::delete_tombstones(&remote, &tombstones)?;
    println!(
        "{} deleted for good, {} gone already, {} written since (left)",
        swept.deleted, swept.gone, swept.written
    );
    Ok(())
}
