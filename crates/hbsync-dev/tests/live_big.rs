//! A file just over 4 GiB through a live identity, up from one client and down to another:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test --release -p hbsync-dev --test live_big -- --ignored`.
//! Apart from the other live tests, as it sends 4 GiB each way. Its source is sparse where the
//! file system makes it so (not on NTFS); the copy downloaded takes 4 GiB in the temp folder
//! (RAM where it is a tmpfs), removed at once, and the identity holds it until the run folder
//! is deleted for good at the end.

mod common;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::time::Instant;

use common::Client;
use hbsync_engine::testkit::Res;
use hbsync_fileio::hash_file;
use hbsync_platform::RelPath;
use hbsync_remote::Remote;

/// Where a byte count or offset cut to 32 bits would wrap.
const AT_4_GIB: u64 = 4 << 30;
const SIZE: u64 = AT_4_GIB + (1 << 20);
/// Bytes that aren't zero, at 4 GiB in the sparse file.
const MARK: &[u8] = b"past 4 GiB";

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE); sends 4 GiB each way"]
fn a_file_over_4_gib_goes_up_and_comes_down() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("big");
    // Deleted for good at the end, whatever happens, after the clients' temp folders went.
    let _guard = hbsync_dev::RunFolder::new(&run)?;
    let mut a = Client::new(&identity, "a", &run)?;
    let mut b = Client::new(&identity, "b", &run)?;
    round_trip(&run, &mut a, &mut b)
}

fn round_trip(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let rel = RelPath::new(&format!("{run}/big.bin"))?;
    let source = a.root.join(rel.as_str());
    fs::create_dir_all(a.root.join(run))?;
    let mut file = fs::File::create(&source)?;
    file.set_len(SIZE)?;
    file.seek(SeekFrom::Start(AT_4_GIB))?;
    file.write_all(MARK)?;
    drop(file);

    let began = Instant::now();
    a.sync()?;
    eprintln!("up: {:.1?}", began.elapsed());
    // The source's hash as `a` hashed it, and the file it went up as.
    let (want, file_id) = a.engine.db().read(|s| {
        let node = s.local_node_at(&rel)?;
        let hash = node.map(|n| s.local_by_node(n)).transpose()?.flatten();
        let mirror = node.map(|n| s.get_mirror(n)).transpose()?.flatten();
        Ok((hash.and_then(|r| r.hash), mirror.map(|m| m.file_id)))
    })?;
    let (want, file_id) = want.zip(file_id).ok_or("big.bin didn't go up")?;

    let began = Instant::now();
    b.sync()?;
    eprintln!("down: {:.1?}", began.elapsed());
    let copy = b.root.join(rel.as_str());
    assert_eq!(fs::metadata(&copy)?.len(), SIZE);
    assert_eq!(hash_file(&copy, want.algorithm)?.hash, want);
    fs::remove_file(&copy)?;

    // A range that starts past 4 GiB, as a download resumed there asks.
    let mut tail = Vec::new();
    let range = AT_4_GIB..AT_4_GIB + MARK.len() as u64;
    let read = a.remote.read(file_id, Some(range), &mut tail)?;
    assert_eq!((read, tail.as_slice()), (MARK.len() as u64, MARK));
    Ok(())
}
