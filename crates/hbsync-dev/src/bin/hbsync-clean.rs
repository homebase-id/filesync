//! Development tool: clears a test identity's Filesync drive of what tests and benchmarks leave.
//!
//! ```text
//! hbsync-clean <identity> --tombstones [--yes]
//! ```
//!
//! `--tombstones` counts the tombstones on the drive: the traces of deletes, which clients keep
//! 30 days so every device learns of them. With `--yes`, deletes every one for good
//! (`hard-delete-batch`, each at the version the feed shows); never a live row, removed
//! (Recently deleted) or not. For a test identity only, between runs: a device that hasn't read
//! the feed since loses the trace of those deletes.

use hbsync_homebase::drive::HomebaseRemote;

const USAGE: &str = "usage: hbsync-clean <identity> --tombstones [--yes]";

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((identity, flags)) = args.split_first() else {
        return Err(USAGE.into());
    };
    if identity.starts_with('-')
        || !flags.iter().any(|f| f == "--tombstones")
        || flags.iter().any(|f| f != "--tombstones" && f != "--yes")
    {
        return Err(USAGE.into());
    }
    let remote = HomebaseRemote::connect(hbsync_dev::saved_client(identity)?)?;
    let tombstones = hbsync_dev::tombstones(&remote, None)?;
    println!("{} tombstones on the drive", tombstones.len());
    if tombstones.is_empty() {
        return Ok(());
    }
    if !flags.iter().any(|f| f == "--yes") {
        println!("run again with --yes to delete them for good");
        return Ok(());
    }
    let swept = hbsync_dev::delete_tombstones(&remote, &tombstones)?;
    println!(
        "{} deleted for good, {} gone already, {} written since (left)",
        swept.deleted, swept.gone, swept.written
    );
    Ok(())
}
