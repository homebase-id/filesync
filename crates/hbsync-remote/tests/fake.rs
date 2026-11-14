use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use std::time::Duration;

use hbsync_remote::contract::{Bench, Shares, file, folder, upload};
use hbsync_remote::{ChangeRef, NodeMeta, Remote, RemoteError, RemoteHeader, Upload, feed_end};
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind, Target, Written};

type R = Result<(), Box<dyn std::error::Error>>;

fn remote() -> FakeRemote {
    FakeRemote::new(FakeServer::new(ServerConfig::default()))
}

fn create_file(r: &FakeRemote, node: &NodeMeta, data: &[u8]) -> Result<Written, RemoteError> {
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

/// A member reading what the sharer shares passes the read-only subset.
#[test]
fn the_shared_contract_holds() -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let bench = Bench::shared(&sharer, &member, &sharer, "contract", Duration::ZERO)?;
    let failed = bench.run_all();
    assert!(failed.is_empty(), "{failed:#?}");
    bench.cleanup()?;
    // Nothing left, as `Bench::cleanup` promises: no file, removed or not, no share row, and
    // nothing (tombstones neither) in either view's feed since the bench began.
    let server = sharer.server_handle();
    {
        let server = server.lock().unwrap_or_else(|p| p.into_inner());
        let live: Vec<_> = server.live().map(|h| h.file_id).collect();
        assert!(live.is_empty(), "cleanup left {live:?}");
        let removed: Vec<_> = server.removed().map(|h| h.file_id).collect();
        assert!(removed.is_empty(), "cleanup left removed {removed:?}");
        let shared: Vec<_> = server.shared().collect();
        assert!(shared.is_empty(), "cleanup left share rows of {shared:?}");
    }
    for (who, r) in [("sharer", &sharer), ("member", &member)] {
        let rows = rows_after(r, Some(bench.start()))?;
        assert!(
            rows.is_empty(),
            "cleanup left in the {who}'s feed: {rows:?}"
        );
    }
    Ok(())
}

/// Every row of `r`'s feed after `cursor` (from the start without one).
fn rows_after(r: &FakeRemote, cursor: Option<&str>) -> Result<Vec<RemoteHeader>, RemoteError> {
    let mut cursor = cursor.map(str::to_owned);
    let mut rows = Vec::new();
    loop {
        let page = r.changes(cursor.as_deref(), 1000)?;
        rows.extend(page.entries);
        if !page.has_more {
            return Ok(rows);
        }
        cursor = Some(page.cursor);
    }
}

/// A member's subscription hears what its feed shows: nothing of a file not shared with it, a
/// shared one as it is, and a share withdrawn as the tombstone without a version.
#[test]
fn a_members_subscription_hears_its_view() -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let (members, owners) = (member.subscribe(false), sharer.subscribe(false));
    let private = create_file(&sharer, &file("private", NodeId::ROOT, b"mine"), b"mine")?;
    let shared = create_file(&sharer, &file("shared", NodeId::ROOT, b"ours"), b"ours")?;
    sharer.share(shared.at().file_id)?;
    sharer.withdraw(shared.at().file_id)?;
    let heard: Vec<_> = members.try_iter().collect();
    assert_eq!(heard.len(), 2, "{heard:?}");
    assert!(heard.iter().all(|c| c.file_id == shared.at().file_id));
    assert_eq!(heard[0].version, shared.at().version);
    assert!(!heard[0].deleted, "{heard:?}");
    assert!(
        heard[1].is_unshared() && heard[1].parent.is_none(),
        "{heard:?}"
    );
    // The withdrawal is announced as the member's feed has it.
    let feed = rows_after(&member, None)?;
    assert_eq!(feed.len(), 1, "{feed:?}");
    assert_eq!(heard[1], ChangeRef::of(&feed[0]));
    // The owner hears every write as it is: two creates, a share and its withdrawal.
    let (private, shared) = (private.at().file_id, shared.at().file_id);
    assert_eq!(
        owners
            .try_iter()
            .map(|c| (c.file_id, c.deleted))
            .collect::<Vec<_>>(),
        [
            (private, false),
            (shared, false),
            (shared, false),
            (shared, false)
        ]
    );
    // A share withdrawn already changes nothing, nor is it announced; one never given is
    // refused.
    let before = feed_end(&sharer, None)?;
    sharer.withdraw(shared)?;
    assert!(
        sharer.withdraw(private).is_err(),
        "withdrew a share never given"
    );
    assert_eq!(members.try_iter().count() + owners.try_iter().count(), 0);
    assert_eq!(rows_after(&sharer, Some(&before))?, [], "the feed moved");
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
            r.update(&w.target, &wrong, None),
            Err(RemoteError::Fatal(_))
        ));
    }
    assert_eq!(r.header(w.at().file_id)?.node(), Some(f.node));
    Ok(())
}

/// The token a write returns is the one its header gives and an update takes; another file's
/// is refused (on Homebase it would write content under the wrong key).
#[test]
fn an_update_takes_only_the_files_own_token() -> R {
    let r = remote();
    let (f, g) = (file("f", NodeId::ROOT, b"1"), file("g", NodeId::ROOT, b"2"));
    let (wf, wg) = (create_file(&r, &f, b"1")?, create_file(&r, &g, b"2")?);
    assert!(wf.target.token.is_some() && wf.target.token == r.header(wf.at().file_id)?.token);
    let other = Target {
        token: wg.target.token,
        ..wf.target.clone()
    };
    assert!(matches!(
        r.update(&other, &f, None),
        Err(RemoteError::Fatal(_))
    ));
    let w2 = r.update(&wf.target, &f, None)?;
    assert_eq!(
        r.header(w2.at().file_id)?.token,
        w2.target.token,
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
        hash: hbsync_remote::Planned::Known(hash_bytes(b"real", HashAlgorithm::Blake3)),
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
        r.read(w.at().file_id, None, &mut out)
            .is_err_and(|e| e.is_transient())
    );
    assert_eq!(out.len(), 300);
    r.read(w.at().file_id, Some(300..1000), &mut out)?;
    assert_eq!(out, vec![7u8; 1000]);
    Ok(())
}
