use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use std::time::Duration;

use hbsync_remote::contract::{Bench, file, folder, upload};
use hbsync_remote::{NodeMeta, Remote, RemoteError, Upload};
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, Target};

type R = Result<(), Box<dyn std::error::Error>>;

fn remote() -> FakeRemote {
    FakeRemote::new(FakeServer::new(ServerConfig::default()))
}

fn create_file(r: &FakeRemote, node: &NodeMeta, data: &[u8]) -> Result<Target, RemoteError> {
    r.create(node, Some(upload(&mut std::io::Cursor::new(data))))
}

#[test]
fn the_contract_holds() -> R {
    let r = remote();
    let bench = Bench::new(&r, "contract", Duration::ZERO)?;
    let failed = bench.run_all();
    assert!(failed.is_empty(), "{failed:#?}");
    bench.cleanup()?;
    let live: Vec<_> = r.server().live().map(|h| h.file_id).collect();
    assert!(live.is_empty(), "cleanup left {live:?}");
    Ok(())
}

#[test]
fn an_update_keeps_node_id_and_kind() -> R {
    let r = remote();
    let f = file("f", NodeId::ROOT, b"1");
    let w = create_file(&r, &f, b"1")?;
    let other_node = NodeMeta {
        node: NodeId::new(),
        ..f.clone()
    };
    let other_kind = NodeMeta {
        kind: NodeKind::Folder,
        content: NodeContent::folder("f"),
        ..f.clone()
    };
    for wrong in [other_node, other_kind] {
        assert!(matches!(
            r.update(&w, &wrong, None),
            Err(RemoteError::Fatal(_))
        ));
    }
    assert_eq!(r.header(w.at.file_id)?.node(), Some(f.node));
    Ok(())
}

/// The token a write returns is the one its header gives and an update takes; another file's
/// is refused (on Homebase it would write content under the wrong key).
#[test]
fn an_update_takes_only_the_files_own_token() -> R {
    let r = remote();
    let (f, g) = (file("f", NodeId::ROOT, b"1"), file("g", NodeId::ROOT, b"2"));
    let (wf, wg) = (create_file(&r, &f, b"1")?, create_file(&r, &g, b"2")?);
    assert!(wf.token.is_some() && wf.token == r.header(wf.at.file_id)?.token);
    let other = Target {
        token: wg.token,
        ..wf.clone()
    };
    assert!(matches!(
        r.update(&other, &f, None),
        Err(RemoteError::Fatal(_))
    ));
    let w2 = r.update(&wf, &f, None)?;
    assert_eq!(
        r.header(w2.at.file_id)?.token,
        w2.token,
        "a write's token is its header's"
    );
    Ok(())
}

#[test]
fn hash_rules() -> R {
    let r = remote();
    let f = file("f", NodeId::ROOT, b"real");
    let mut reader = std::io::Cursor::new(&b"corrupted"[..]);
    let lying = Upload {
        hash: hash_bytes(b"real", HashAlgorithm::Blake3),
        size: 4,
        reader: &mut reader,
        pace: None,
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
fn rows_of_the_current_millisecond_are_held_back() -> R {
    let server = FakeServer::new(ServerConfig {
        auto_tick: false,
        ..ServerConfig::default()
    });
    let r = FakeRemote::new(server.clone());
    r.create(&folder("now", NodeId::ROOT), None)?;
    let held = server.lock().map_err(|_| "poisoned")?.changes(None, 10)?;
    assert!(held.entries.is_empty());
    assert!(
        held.has_more,
        "held back, with more rows to come (odin-core #1906)"
    );
    server.lock().map_err(|_| "poisoned")?.tick();
    let page = r.changes(None, 10)?;
    assert_eq!((page.entries.len(), page.has_more), (1, false));
    // A client's read that held a row back: by its next, the clock has moved on.
    r.create(&folder("later", NodeId::ROOT), None)?;
    let held = r.changes(Some(&page.cursor), 10)?;
    assert!(held.entries.is_empty() && held.has_more);
    assert_eq!(r.changes(Some(&held.cursor), 10)?.entries.len(), 1);
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
        r.read(w.at.file_id, None, &mut out)
            .is_err_and(|e| e.is_transient())
    );
    assert_eq!(out.len(), 300);
    r.read(w.at.file_id, Some(300..1000), &mut out)?;
    assert_eq!(out, vec![7u8; 1000]);
    Ok(())
}
