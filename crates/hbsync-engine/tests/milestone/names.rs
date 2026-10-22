//! Names: Unicode and case, names Windows can't hold, the name map.

use super::*;

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
    let mut win = Client::with_config(&server, "win", |c| {
        c.folder.os = hbsync_platform::Os::Windows
    })?;
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
        let mut unix = Client::with_config(&server, "unix", |c| {
            c.folder.os = hbsync_platform::Os::Linux
        })?;
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
    let mut win = Client::with_config(&server, "win", |c| {
        c.folder.os = hbsync_platform::Os::Windows
    })?;
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

/// Drive names the name map wouldn't give back from their local spelling (decomposed, which
/// every OS's map composes; on Windows, an escape's own code point), each with how this OS
/// spells it.
const UNMAPPED: [(hbsync_platform::Os, &str); 3] = [
    (hbsync_platform::Os::Linux, "Re\u{301}sume\u{301}.txt"),
    (hbsync_platform::Os::MacOs, "e\u{301}"),
    (hbsync_platform::Os::Windows, "a\u{F022}b"),
];

/// Names the name map wouldn't give back ([`UNMAPPED`]): from the first download on, the item
/// reads as the drive's name, not as renamed. Otherwise an edit renamed the node on the drive.
#[test]
fn a_downloaded_name_the_map_wont_give_back_reads_as_the_drives() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_types::NodeId;

    for (os, name) in UNMAPPED {
        let server = server();
        let (edited, gone) = (format!("1{name}"), format!("2{name}"));
        let (edited_node, gone_node) = {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            (
                srv.seed_file(NodeId::ROOT, &edited, b"v1", Blake3)?.0,
                srv.seed_file(NodeId::ROOT, &gone, b"v1", Blake3)?.0,
            )
        };
        let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
        assert_clean(&c.sync()?);
        // An edit here goes up to the same node, under the drive's name.
        c.write(&edited, b"v2")?;
        assert_clean(&c.sync()?);
        // The drive deletes the other: it goes here too, and doesn't come back.
        {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            let file = srv.file_id_of(gone_node).ok_or("live")?;
            let version = srv.header(file)?.version;
            srv.delete(file, version)?;
        }
        assert_clean(&c.sync()?);
        let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
        assert_eq!(names, std::slice::from_ref(&edited), "{os:?}");
        let srv = server.lock().map_err(|_| "poisoned")?;
        assert_eq!(srv.live_names(), [edited], "{os:?}");
        let file = srv.file_id_of(edited_node).ok_or("the same node")?;
        assert_eq!(srv.payload(file)?.as_slice(), b"v2", "{os:?}");
    }
    Ok(())
}

/// A rename on the drive to a name the name map wouldn't give back ([`UNMAPPED`]; on Windows,
/// from the name this device spells the same): it settles. Otherwise the item never read as at
/// the new name, and every sync moved it again, up to the round limit.
#[test]
fn a_rename_on_the_drive_to_a_name_the_map_wont_give_back_settles() -> R {
    use hbsync_fileio::HashAlgorithm::Blake3;
    use hbsync_remote::{NodeMeta, RemoteBody};
    use hbsync_types::NodeId;

    for (os, to) in UNMAPPED {
        let server = server();
        let from = hbsync_platform::nfc(&to.replace('\u{F022}', ":"));
        let node = (server.lock().map_err(|_| "poisoned")?)
            .seed_file(NodeId::ROOT, &from, b"v1", Blake3)?
            .0;
        let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
        assert_clean(&c.sync()?);
        {
            let mut srv = server.lock().map_err(|_| "poisoned")?;
            let file = srv.file_id_of(node).ok_or("live")?;
            let h = srv.header(file)?;
            let RemoteBody::Live { mut content, .. } = h.body else {
                return Err("live".into());
            };
            content.name = to.to_owned();
            let meta = NodeMeta {
                node,
                parent: h.parent,
                kind: h.kind,
                content,
                removed: false,
            };
            srv.update(file, h.version, &meta, None)?;
        }
        let report = c.sync()?;
        assert_clean(&report);
        assert_eq!(report.pending, 0, "{os:?}: {report:?}");
        assert!(c.sync()?.is_idle(), "{os:?}");
        let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
        assert_eq!(names, [to], "{os:?}");
        assert_eq!(
            server.lock().map_err(|_| "poisoned")?.live_names(),
            [to],
            "{os:?}"
        );
    }
    Ok(())
}

/// A first run over a copy of a node whose drive name is decomposed ([`UNMAPPED`]), hashed
/// with this device's algorithm or another: the copy is adopted, no conflict and nothing
/// uploaded. Otherwise it read as both sides moved, or, across hash algorithms, wasn't adopted
/// at all and went up as a conflict copy. (Not a drive name holding an escape's own code point
/// on Windows: adoption looks for the drive's node under the name the copy maps to, which
/// differs.)
#[test]
fn a_copy_at_a_decomposed_drive_name_is_adopted() -> R {
    use hbsync_fileio::HashAlgorithm::{Blake3, Sha256};
    use hbsync_platform::Os;
    use hbsync_types::NodeId;

    for (os, name) in UNMAPPED.into_iter().filter(|(os, _)| *os != Os::Windows) {
        for algorithm in [Blake3, Sha256] {
            let server = server();
            server.lock().map_err(|_| "poisoned")?.seed_file(
                NodeId::ROOT,
                name,
                b"same",
                algorithm,
            )?;
            let mut c = Client::with_config(&server, "c", move |cfg| cfg.folder.os = os)?;
            c.write(name, b"same")?;
            let report = c.sync()?;
            assert_clean(&report);
            assert_eq!(report.adopted, 1, "{os:?}, {algorithm:?}: {report:?}");
            assert!(c.sync()?.is_idle(), "{os:?}, {algorithm:?}");
            let names: Vec<String> = c.entries("").into_iter().map(|(p, _)| p).collect();
            assert_eq!(names, [name], "{os:?}, {algorithm:?}");
            assert_eq!(
                server.lock().map_err(|_| "poisoned")?.live_names(),
                [name],
                "{os:?}, {algorithm:?}"
            );
        }
    }
    Ok(())
}
