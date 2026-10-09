use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{NewNode, Remote, RemoteBody, RemoteError, Upload};
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind};

type R = Result<(), Box<dyn std::error::Error>>;

fn remote() -> FakeRemote {
    FakeRemote::new(FakeServer::new(ServerConfig::default()))
}

fn folder(name: &str, parent: NodeId) -> NewNode {
    NewNode {
        node: NodeId::new(),
        parent,
        kind: NodeKind::Folder,
        content: NodeContent::folder(name),
    }
}

fn file(name: &str, parent: NodeId, data: &[u8]) -> NewNode {
    NewNode {
        node: NodeId::new(),
        parent,
        kind: NodeKind::File,
        content: NodeContent::file(name, 1_700_000_000_000, data.len() as u64, false),
    }
}

fn upload<'a>(data: &[u8], reader: &'a mut &[u8]) -> Upload<'a> {
    Upload {
        hash: hash_bytes(data, HashAlgorithm::Blake3),
        size: data.len() as u64,
        reader,
    }
}

fn create_file(
    r: &FakeRemote,
    node: &NewNode,
    data: &[u8],
) -> Result<hbsync_types::RemoteRef, RemoteError> {
    let mut reader = data;
    r.create(node, Some(upload(data, &mut reader)))
}

#[test]
fn create_read_and_feed() -> R {
    let r = remote();
    let dir = folder("Docs", NodeId::ROOT);
    let written_dir = r.create(&dir, None)?;
    let f = file("a.txt", dir.node, b"hello");
    let written = create_file(&r, &f, b"hello")?;

    let header = r.header(written.file_id)?;
    assert_eq!(header.node(), Some(f.node));
    assert_eq!(header.parent, dir.node);
    let state = header.state().ok_or("state")?;
    assert_eq!(state.name, "a.txt");
    assert_eq!(
        state.content.map(|c| c.hash),
        Some(hash_bytes(b"hello", HashAlgorithm::Blake3))
    );

    let mut out = Vec::new();
    assert_eq!(r.read(written.file_id, None, &mut out)?, 5);
    assert_eq!(out, b"hello");
    out.clear();
    r.read(written.file_id, Some(1..3), &mut out)?;
    assert_eq!(out, b"el");

    let page = r.changes(None, 100)?;
    let ids: Vec<_> = page.entries.iter().map(|h| h.file_id).collect();
    assert_eq!(ids, [written_dir.file_id, written.file_id]);
    assert!(!page.has_more);
    // Nothing new after the cursor.
    assert!(r.changes(Some(&page.cursor), 100)?.entries.is_empty());
    Ok(())
}

#[test]
fn duplicate_node_id_and_recreate_after_delete() -> R {
    let r = remote();
    let f = file("x", NodeId::ROOT, b"1");
    let first = create_file(&r, &f, b"1")?;
    assert!(matches!(
        create_file(&r, &f, b"1"),
        Err(RemoteError::AlreadyExists)
    ));
    r.delete(first.file_id, first.version)?;
    // A tombstone frees the node id; the new file gets a new file id.
    let again = create_file(&r, &f, b"1")?;
    assert_ne!(again.file_id, first.file_id);
    Ok(())
}

#[test]
fn version_guarded_update_and_metadata_only_update() -> R {
    let r = remote();
    let f = file("v.txt", NodeId::ROOT, b"v1");
    let w1 = create_file(&r, &f, b"v1")?;
    let moved = NodeContent::file("renamed.txt", 1, 2, false);

    let w2 = r.update(w1.file_id, w1.version, NodeId::ROOT, &moved, None)?;
    assert_ne!(w2.version, w1.version);
    // Stale version.
    assert!(matches!(
        r.update(w1.file_id, w1.version, NodeId::ROOT, &moved, None),
        Err(RemoteError::VersionMismatch)
    ));
    // Metadata-only update kept the payload.
    let mut out = Vec::new();
    r.read(w2.file_id, None, &mut out)?;
    assert_eq!(out, b"v1");
    // Content update.
    let mut reader: &[u8] = b"v2!";
    let w3 = r.update(
        w2.file_id,
        w2.version,
        NodeId::ROOT,
        &moved,
        Some(upload(b"v2!", &mut reader)),
    )?;
    out.clear();
    r.read(w3.file_id, None, &mut out)?;
    assert_eq!(out, b"v2!");
    Ok(())
}

#[test]
fn tombstones_keep_parent_and_kind_only() -> R {
    let r = remote();
    let dir = folder("d", NodeId::ROOT);
    r.create(&dir, None)?;
    let f = file("f", dir.node, b"data");
    let w = create_file(&r, &f, b"data")?;
    let cursor = r.changes(None, 100)?.cursor;
    r.delete(w.file_id, w.version)?;

    let page = r.changes(Some(&cursor), 100)?;
    assert_eq!(page.entries.len(), 1);
    let tomb = &page.entries[0];
    assert_eq!(
        (tomb.file_id, tomb.parent, tomb.kind),
        (w.file_id, dir.node, NodeKind::File)
    );
    assert_eq!(tomb.body, RemoteBody::Deleted);
    assert_eq!(tomb.node(), None);
    assert!(matches!(
        r.read(w.file_id, None, &mut Vec::new()),
        Err(RemoteError::NotFound)
    ));
    Ok(())
}

#[test]
fn stale_delete_is_refused_by_the_client_check() -> R {
    let r = remote();
    let f = file("f", NodeId::ROOT, b"1");
    let w = create_file(&r, &f, b"1")?;
    let content = NodeContent::file("f", 2, 1, false);
    r.update(w.file_id, w.version, NodeId::ROOT, &content, None)?;
    assert!(matches!(
        r.delete(w.file_id, w.version),
        Err(RemoteError::VersionMismatch)
    ));
    assert!(!r.header(w.file_id)?.is_deleted());
    Ok(())
}

#[test]
fn hash_rules() -> R {
    let r = remote();
    let f = file("f", NodeId::ROOT, b"real");
    let mut reader: &[u8] = b"corrupted";
    let lying = Upload {
        hash: hash_bytes(b"real", HashAlgorithm::Blake3),
        size: 4,
        reader: &mut reader,
    };
    assert!(matches!(
        r.create(&f, Some(lying)),
        Err(RemoteError::HashMismatch)
    ));
    assert!(r.server().file_id_of(f.node).is_none(), "nothing stored");
    assert!(matches!(r.create(&f, None), Err(RemoteError::HashRequired)));
    Ok(())
}

#[test]
fn feed_pages_resume_and_updated_rows_move_forward() -> R {
    let r = remote();
    let nodes: Vec<NewNode> = (0..5)
        .map(|i| folder(&format!("f{i}"), NodeId::ROOT))
        .collect();
    let written: Vec<_> = nodes
        .iter()
        .map(|n| r.create(n, None))
        .collect::<Result<_, _>>()?;

    let p1 = r.changes(None, 2)?;
    assert_eq!(p1.entries.len(), 2);
    assert!(p1.has_more);
    let p2 = r.changes(Some(&p1.cursor), 2)?;
    let p3 = r.changes(Some(&p2.cursor), 2)?;
    assert_eq!(
        (p2.entries.len(), p3.entries.len(), p3.has_more),
        (2, 1, false)
    );

    // Updating the first row moves it to the end of the feed: it shows up again, once.
    let renamed = NodeContent::folder("f0-renamed");
    r.update(
        written[0].file_id,
        written[0].version,
        NodeId::ROOT,
        &renamed,
        None,
    )?;
    let p4 = r.changes(Some(&p3.cursor), 10)?;
    assert_eq!(p4.entries.len(), 1);
    assert_eq!(p4.entries[0].file_id, written[0].file_id);
    let all = r.changes(None, 10)?;
    assert_eq!(
        all.entries.len(),
        5,
        "each file appears once, at its latest position"
    );
    Ok(())
}

#[test]
fn rows_of_the_current_millisecond_are_not_visible_yet() -> R {
    let server = FakeServer::new(ServerConfig {
        require_hashes: true,
        auto_tick: false,
    });
    let r = FakeRemote::new(server.clone());
    r.create(&folder("now", NodeId::ROOT), None)?;
    assert!(r.changes(None, 10)?.entries.is_empty());
    server.lock().map_err(|_| "poisoned")?.tick();
    assert_eq!(r.changes(None, 10)?.entries.len(), 1);
    Ok(())
}

#[test]
fn injected_faults() -> R {
    let r = remote();
    r.faults().fail_next = 1;
    assert!(r.changes(None, 10).is_err_and(|e| e.is_transient()));
    assert!(r.changes(None, 10).is_ok());

    // An upload cut mid-way stores nothing and is transient.
    let f = file("big", NodeId::ROOT, &[7u8; 1000]);
    r.faults().cut_upload_after = Some(100);
    assert!(create_file(&r, &f, &[7u8; 1000]).is_err_and(|e| e.is_transient()));
    assert!(r.server().file_id_of(f.node).is_none());
    let w = create_file(&r, &f, &[7u8; 1000])?;

    // A download cut leaves the bytes received so far in the sink; a ranged read resumes.
    r.faults().cut_download_after = Some(300);
    let mut out = Vec::new();
    assert!(
        r.read(w.file_id, None, &mut out)
            .is_err_and(|e| e.is_transient())
    );
    assert_eq!(out.len(), 300);
    r.read(w.file_id, Some(300..1000), &mut out)?;
    assert_eq!(out, vec![7u8; 1000]);
    Ok(())
}
