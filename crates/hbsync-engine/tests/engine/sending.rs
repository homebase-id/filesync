//! Hash on send (sync semantics §3): a new file nothing on the drive can be compared with is
//! sent unread, and the hash of the bytes sent settles it; any other is read first.

use super::*;
use hbsync_fileio::{HashAlgorithm, hash_file};
use hbsync_platform::RelPath;
use hbsync_types::ContentHash;

/// The local row's hash, whether it is still left to be hashed on send, and its base's hash.
type Hashes = (Option<ContentHash>, bool, Option<ContentHash>);

/// [`Hashes`] of the item at `rel`.
fn hashes(c: &mut Client, rel: &str) -> Result<Hashes, Box<dyn std::error::Error>> {
    let path = RelPath::new(rel)?;
    let row = c
        .engine
        .db()
        .read(|s| s.get_local(&path))?
        .ok_or("no row")?;
    let base = c.engine.db().read(|s| s.get_base(row.node))?;
    Ok((
        row.hash,
        row.send_hash,
        base.and_then(|b| b.state.content).map(|c| c.hash),
    ))
}

/// A first sync of new files reads each once: the scan hashes none, the upload sends each, and
/// its hash settles the base and the local row alike, as `hash_file` reads the file.
#[test]
fn a_first_sync_reads_each_new_file_once() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut total = 0;
    for i in 0..12usize {
        let data = vec![i as u8; 1000 * (i + 1)];
        total += data.len() as u64;
        a.write(&format!("d{}/f{i}.bin", i % 3), data)?;
    }
    assert_eq!(a.engine.scan_local()?.hashed, 0);
    a.sync()?;
    assert_eq!(a.remote.faults().upload_bytes, total, "each file read once");
    for i in 0..12usize {
        let rel = format!("d{}/f{i}.bin", i % 3);
        let read = hash_file(&a.root.join(&rel), HashAlgorithm::Blake3)?.hash;
        assert_eq!(
            hashes(&mut a, &rel)?,
            (Some(read), false, Some(read)),
            "{rel}"
        );
    }
    assert_eq!(a.engine.scan_local()?.hashed, 0, "nothing to read again");
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A new file where the drive has one of its name (another device's, unknown at the scan) is
/// read first: on a first run it is that node, adopted (nothing sent); later it is planned as
/// any file of that name, its bytes sent once.
#[test]
fn a_new_file_with_a_name_the_drive_has_is_read_first() -> R {
    let server = server();
    let mut b = Client::new(&server, "b")?;
    b.write("same.txt", "the bytes")?;
    b.sync()?;
    let mut a = Client::new(&server, "a")?;
    a.write("same.txt", "the bytes")?;
    assert_eq!(a.engine.scan_local()?.hashed, 0);
    let report = a.sync()?;
    assert_eq!(report.adopted, 1, "{report:?}");
    assert_eq!(a.remote.faults().uploads, 0, "nothing sent");
    assert_eq!(a.tree_text(""), ["same.txt = the bytes"]);

    b.write("later.txt", "b's")?;
    b.sync()?;
    a.write("later.txt", "a's")?;
    a.engine.scan_local()?;
    assert!(hashes(&mut a, "later.txt")?.1, "left to be hashed on send");
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.remote.faults().upload_bytes, 3, "a's bytes sent once");
    assert_eq!(a.tree_text(""), b.tree_text(""));
    let texts = a.tree_text("");
    assert!(texts.iter().any(|t| t.ends_with("= a's")), "{texts:?}");
    assert!(texts.iter().any(|t| t.ends_with("= b's")), "{texts:?}");
    Ok(())
}
