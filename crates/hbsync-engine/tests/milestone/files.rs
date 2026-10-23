//! Files: large ones streamed and resumed, the executable bit, creation times, the state
//! folder's name.

use super::*;

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
            hash: hbsync_remote::Planned::Known(hbsync_fileio::hash_bytes(
                script,
                hbsync_fileio::HashAlgorithm::Blake3,
            )),
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
