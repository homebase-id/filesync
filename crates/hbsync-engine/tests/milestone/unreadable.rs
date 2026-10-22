//! Files that can't be read yet, and masked folders they hold.

use super::*;

/// A file this device can't read (no permission: never hashed, its merge deferred every round)
/// whose node another writer puts into a file: it waits in place, told by name, and the rest
/// of the folder plans and converges around it (a new file comes down, another orphan goes
/// into Recovered files). Readable again, it is adopted into Recovered files with what it
/// holds.
#[cfg(unix)]
#[test]
fn an_unreadable_file_moved_into_a_file_waits_and_the_rest_converges() -> R {
    use hbsync_remote::{NodeMeta, RemoteBody};
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let [mut a, mut b] = pair(&server)?;
    fs::write(a.root.join("x"), b"x1")?;
    fs::write(a.root.join("o"), b"o1")?;
    fs::write(a.root.join("keep"), b"keep")?;
    // Tagged Red, where this volume keeps tags.
    let red = hbsync_types::Tags::new([hbsync_types::Tag::new("Red", 6)]);
    let tagged = hbsync_platform::tags::write(&a.root.join("x"), &red).is_ok_and(|w| w.is_some());
    settle_all(&mut a, &mut b)?;
    // a's x changes, and can't be read: a has nothing to hash it with.
    fs::write(a.root.join("x"), b"x2")?;
    fs::set_permissions(a.root.join("x"), fs::Permissions::from_mode(0o000))?;
    if fs::read(a.root.join("x")).is_ok() {
        fs::set_permissions(a.root.join("x"), fs::Permissions::from_mode(0o644))?;
        return Ok(()); // root reads anything: nothing to test
    }
    // Another writer moves x and o into the file `keep`.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let headers: Vec<_> = srv.live().cloned().collect();
        let node_of = |name: &str| {
            headers.iter().find_map(|h| match &h.body {
                RemoteBody::Live { node, content, .. } if content.name == name => {
                    Some((*node, h.clone()))
                }
                _ => None,
            })
        };
        let (keep, _) = node_of("keep").ok_or("no keep")?;
        for name in ["x", "o"] {
            let (node, h) = node_of(name).ok_or("no file")?;
            let RemoteBody::Live { content, .. } = &h.body else {
                continue;
            };
            let meta = NodeMeta {
                node,
                parent: keep,
                kind: h.kind,
                content: content.clone(),
                removed: false,
            };
            srv.update(h.file_id, h.version, &meta, None)?;
        }
    }
    fs::write(b.root.join("n"), b"n1")?;
    let mut waiting = Vec::new();
    for _ in 0..3 {
        waiting.extend(a.sync()?.waiting);
        b.sync()?;
    }
    // Told, by name: what waits for it is the drive's move.
    assert!(waiting.iter().any(|(p, _)| p == "x"), "{waiting:?}");
    let names = |c: &Client| -> Vec<String> { c.entries("").into_iter().map(|(p, _)| p).collect() };
    let (ta, tb) = (names(&a), names(&b));
    assert!(
        ta.contains(&"x".to_owned()),
        "x waits in place on a: {ta:?}"
    );
    assert!(
        ta.contains(&"n".to_owned()),
        "the new file came down: {ta:?}"
    );
    for t in [&ta, &tb] {
        assert!(
            t.contains(&"Recovered files/o".to_owned()),
            "o is recovered: {t:?}"
        );
    }
    // Readable again: adopted, with what it holds, everywhere.
    fs::set_permissions(a.root.join("x"), fs::Permissions::from_mode(0o644))?;
    settle_all(&mut a, &mut b)?;
    let (ea, eb) = (a.entries(""), b.entries(""));
    assert_eq!(ea, eb, "converged");
    assert!(
        ea.iter()
            .any(|(p, c)| p == "Recovered files/x" && c == b"x2"),
        "{ea:?}"
    );
    if tagged {
        // Its tags went along, never the drive's written over them.
        // By name: a volume may keep no colour.
        let names = |t: &hbsync_types::Tags| t.iter().map(|t| t.name.clone()).collect::<Vec<_>>();
        let held = hbsync_platform::tags::read(&b.root.join("Recovered files/x"));
        assert_eq!(
            held.as_ref().map(names),
            Some(vec!["Red".to_owned()]),
            "b's x holds Red"
        );
        let srv = server.lock().map_err(|_| "poisoned")?;
        let on_drive = srv.live().find_map(|h| match &h.body {
            RemoteBody::Live { content, .. } if content.name == "x" => Some(content.tags()),
            _ => None,
        });
        assert_eq!(
            on_drive.as_ref().map(names),
            Some(vec!["Red".to_owned()]),
            "Red on the drive"
        );
    }
    Ok(())
}

/// A file the scan couldn't read (locked then), in a folder the drive deleted: the round that
/// finds the delete waiting for it reads it first, and plans again in the same sync: readable
/// now, the new file keeps the folder, which comes back on the drive with it. Without that read
/// the delete waited for the next scan of that folder, maybe long.
#[cfg(unix)]
#[test]
fn a_round_reads_what_its_plan_waits_for_first() -> R {
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let [mut a, mut b] = pair(&server)?;
    fs::create_dir(a.root.join("p"))?;
    fs::write(a.root.join("p/keep"), b"k")?;
    settle_all(&mut a, &mut b)?;
    // A new file a can't read when it scans.
    fs::write(a.root.join("p/new"), b"new")?;
    fs::set_permissions(a.root.join("p/new"), fs::Permissions::from_mode(0o000))?;
    if fs::read(a.root.join("p/new")).is_ok() {
        return Ok(()); // root reads anything: nothing to test
    }
    a.engine.scan_local()?;
    fs::set_permissions(a.root.join("p/new"), fs::Permissions::from_mode(0o644))?;
    // b deletes p.
    fs::remove_dir_all(b.root.join("p"))?;
    b.sync()?;
    // One sync of a, no scan before it.
    let report = a.engine.sync("2026-10-08", true)?;
    assert!(report.waiting.is_empty(), "{report:?}");
    b.sync()?;
    assert_eq!(b.read("p/new")?, b"new", "the folder came back with it");
    Ok(())
}

/// A folder the drive renamed, holding a file here that can't be read, planned in a page of
/// its own (more dirty than a page holds): every view loads the files not hashed yet, so the
/// narrow view holds the folder as the whole one does (the testkit compares them on every
/// plan), and the rename waits; readable again, it comes down with the file's edit.
#[cfg(unix)]
#[test]
fn a_page_without_the_unread_file_holds_its_folder_too() -> R {
    use std::os::unix::fs::PermissionsExt;

    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.batch = 1)?;
    let mut b = Client::new(&server, "b")?;
    fs::create_dir(a.root.join("p"))?;
    fs::write(a.root.join("p/w"), b"w1")?;
    settle_all(&mut a, &mut b)?;
    fs::rename(b.root.join("p"), b.root.join("q"))?;
    b.sync()?;
    fs::write(a.root.join("p/w"), b"w2")?;
    fs::set_permissions(a.root.join("p/w"), fs::Permissions::from_mode(0o000))?;
    if fs::read(a.root.join("p/w")).is_ok() {
        return Ok(()); // root reads anything: nothing to test
    }
    let waiting = a.sync()?.waiting;
    assert!(a.root.join("p").is_dir(), "the rename waits");
    assert!(waiting.iter().any(|(p, _)| p == "p/w"), "{waiting:?}");
    fs::set_permissions(a.root.join("p/w"), fs::Permissions::from_mode(0o644))?;
    settle_all(&mut a, &mut b)?;
    assert_eq!(a.read("q/w")?, b"w2");
    assert_eq!(b.read("q/w")?, b"w2");
    Ok(())
}

/// A writer makes a synced file a folder (held here as the file it was, #175) and puts a role
/// folder in it (a role's fixed id, as a file, then made a folder: masked for the main
/// folder); another device edits its file in it, and this device's user deletes the file. The
/// narrow view plans as the whole one (the testkit checks every plan): the masked folder in it
/// keeps it, so the file is planned back down (and waits, the drive's version unreadable
/// here). The narrow view read only part of what is in a node kept so, and planned the
/// drive's node deleted, with the role folder and the other device's file. Hostile writer
/// seed 22000000438.
#[test]
fn an_unreadable_file_holding_a_masked_folder_plans_alike_narrow_and_whole() -> R {
    use hbsync_remote::{NodeMeta, PayloadInfo, RemoteBody};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeKind, Role};

    let server = server();
    let [mut a, mut b] = pair(&server)?;
    fs::write(b.root.join("C.TXT"), b"content #2")?;
    b.sync()?;
    // a sees only the folder.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let h = srv
            .live()
            .find(
                |h| matches!(&h.body, RemoteBody::Live { content, .. } if content.name == "C.TXT"),
            )
            .cloned()
            .ok_or("C.TXT")?;
        let node = h.node().ok_or("node")?;
        let folder = |node, parent, name: &str| NodeMeta {
            node,
            parent,
            kind: NodeKind::Folder,
            content: NodeContent::folder(name),
            removed: false,
        };
        srv.write_unchecked(Some(h.file_id), &folder(node, h.parent, "C.TXT"), None)?;
        // A role's id on a file in it, then made a folder.
        let role = Role::ALL[1].node();
        let bytes = b"hostile".to_vec();
        let info = PayloadInfo {
            hash: hbsync_fileio::hash_bytes(&bytes, hbsync_fileio::HashAlgorithm::Blake3),
            bytes: bytes.len() as u64,
        };
        let file = NodeMeta {
            kind: NodeKind::File,
            content: NodeContent::file("fixed", 1, bytes.len() as u64, false),
            ..folder(role, node, "fixed")
        };
        let at = srv.write_unchecked(None, &file, Some((bytes, info)))?;
        srv.write_unchecked(Some(at.file_id), &folder(role, node, "fixed"), None)?;
    }
    a.sync()?;
    b.sync()?;
    assert!(b.root.join("C.TXT").is_file(), "b holds the file it had");
    // a puts a file in the folder, and edits it in the sync b's user deletes the file: b's
    // plan then reads part of what is in the drive's node (that file's name).
    fs::write(a.root.join("C.TXT/a"), b"content #3")?;
    a.sync()?;
    b.sync()?;
    fs::write(a.root.join("C.TXT/a"), b"content #4")?;
    a.sync()?;
    fs::remove_file(b.root.join("C.TXT"))?;
    // Both views plan the file back down; the drive's version can't be read here, so that
    // waits (refused, #175), and nothing goes from the drive.
    b.sync()?;
    let report = b.sync()?;
    assert!(
        matches!(&report.refused[..], [(hbsync_engine::Op::LocalDownload { name, .. }, _)] if name == "C.TXT"),
        "the file planned back down: {report:?}"
    );
    assert_eq!(report.ops_done, 0, "{report:?}");
    let srv = server.lock().map_err(|_| "poisoned")?;
    let names: Vec<_> = srv
        .live()
        .filter_map(|h| match &h.body {
            RemoteBody::Live { content, .. } => Some(content.name.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"C.TXT".to_string()), "{names:?}");
    assert!(names.contains(&"fixed".to_string()), "{names:?}");
    assert!(
        names.contains(&"a".to_string()),
        "a keeps its file: {names:?}"
    );
    Ok(())
}

/// The same with a file this device can read: a writer puts a role folder in a synced file
/// (masked for the main folder), then a file in it as this device's user deletes the file. The
/// narrow view read only the new file's name in it and planned the drive's file deleted (it
/// ran, leaving the role folder under a deleted node); the masked folder keeps it: it comes
/// back down.
#[test]
fn a_file_holding_a_masked_folder_comes_back_when_deleted() -> R {
    use hbsync_remote::{NodeMeta, PayloadInfo, RemoteBody};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, NodeKind, Role};

    let server = server();
    let [mut a, mut b] = pair(&server)?;
    fs::write(b.root.join("C.TXT"), b"content #2")?;
    b.sync()?;
    a.sync()?;
    let file_meta = |node, parent, name: &str, bytes: &[u8]| NodeMeta {
        node,
        parent,
        kind: NodeKind::File,
        content: NodeContent::file(name, 1, bytes.len() as u64, false),
        removed: false,
    };
    let payload = |bytes: &[u8]| {
        let info = PayloadInfo {
            hash: hbsync_fileio::hash_bytes(bytes, hbsync_fileio::HashAlgorithm::Blake3),
            bytes: bytes.len() as u64,
        };
        Some((bytes.to_vec(), info))
    };
    let c = {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let c = srv
            .live()
            .find(
                |h| matches!(&h.body, RemoteBody::Live { content, .. } if content.name == "C.TXT"),
            )
            .and_then(|h| h.node())
            .ok_or("C.TXT")?;
        let role = Role::ALL[1].node();
        let at = srv.write_unchecked(None, &file_meta(role, c, "fixed", b"h"), payload(b"h"))?;
        let folder = NodeMeta {
            kind: NodeKind::Folder,
            content: NodeContent::folder("fixed"),
            ..file_meta(role, c, "fixed", b"")
        };
        srv.write_unchecked(Some(at.file_id), &folder, None)?;
        c
    };
    b.sync()?;
    assert_eq!(b.read("C.TXT")?, b"content #2");
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        srv.write_unchecked(None, &file_meta(NodeId::new(), c, "x", b"x"), payload(b"x"))?;
    }
    fs::remove_file(b.root.join("C.TXT"))?;
    b.sync()?;
    assert_eq!(
        b.read("C.TXT")?,
        b"content #2",
        "kept by the folder in it: back"
    );
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert!(
        srv.live().any(|h| h.node() == Some(c)),
        "the drive keeps it"
    );
    Ok(())
}
