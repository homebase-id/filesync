use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use std::time::Duration;

use hbsync_remote::contract::{self, Bench, Shares, file, folder, upload};
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
    // The sharer's own feed end: the bench's start is the member's cursor.
    let sharers_start = feed_end(&sharer, None)?;
    let bench = Bench::shared(&sharer, &member, "contract", Duration::ZERO)?;
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
    for (who, r, start) in [
        ("sharer", &sharer, sharers_start.as_str()),
        ("member", &member, bench.start()),
    ] {
        let rows = rows_after(r, Some(start))?;
        assert!(
            rows.is_empty(),
            "cleanup left in the {who}'s feed: {rows:?}"
        );
    }
    Ok(())
}

/// Every row of `r`'s feed after `cursor` (from the start without one).
fn rows_after(r: &FakeRemote, cursor: Option<&str>) -> Result<Vec<RemoteHeader>, RemoteError> {
    contract::rows_after(Duration::ZERO, cursor, |at, limit| r.changes(at, limit))
        .map(|(rows, _)| rows)
}

/// A share given until a time: the member's header says until when, and as the server's clock
/// reaches it nothing is written or announced. The file's place in the feed stays, a read of
/// it by id is refused, and the feed read from its beginning shows it as a share withdrawn
/// (homebase-api-notes, "Shared with you"). The owner's view doesn't change.
#[test]
fn a_share_that_ends_is_seen_as_withdrawn_and_nothing_is_written() -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let shared = create_file(&sharer, &file("shared", NodeId::ROOT, b"ours"), b"ours")?;
    let file_id = shared.at().file_id;
    let until = sharer.server().now() + 1_000;
    sharer.share_until(file_id, until)?;
    sharer.server().tick();
    let (members, owners) = (member.subscribe(false), sharer.subscribe(false));

    let live = member.header(file_id)?;
    assert_eq!((live.expires, live.is_deleted()), (Some(until), false));
    assert_eq!(
        sharer.header(file_id)?.expires,
        None,
        "the owner's own file"
    );
    let (seen, end) = (rows_after(&member, None)?, feed_end(&member, None)?);
    assert_eq!(seen, [live]);

    // A millisecond before: still read. At the time: no more.
    let now = sharer.server().now();
    sharer.server().advance(until - 1 - now);
    assert!(member.header(file_id).is_ok());
    sharer.server().advance(1);
    assert!(matches!(member.header(file_id), Err(RemoteError::Fatal(_))));
    assert!(!sharer.server().shared_live(file_id));
    // Nothing was written: no one hears of it, and the feed ends where it did.
    assert_eq!(members.try_iter().count() + owners.try_iter().count(), 0);
    assert_eq!(feed_end(&member, None)?, end);
    assert!(rows_after(&member, Some(&end))?.is_empty());
    // From its beginning, the member's feed shows it no longer shared.
    let gone = rows_after(&member, None)?;
    assert_eq!(gone.len(), 1, "{gone:?}");
    assert!(
        gone[0].is_unshared() && gone[0].expires.is_none(),
        "{gone:?}"
    );
    assert!(!sharer.header(file_id)?.is_deleted());

    // Given again without an end, it is back at its version, for good.
    sharer.share(file_id)?;
    sharer.server().advance(10_000);
    let back = member.header(file_id)?;
    assert_eq!((back.at(), back.expires), (shared.at(), None));
    Ok(())
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

/// After a share is withdrawn, every later write of the file announces the "no longer shared"
/// tombstone to the member again, at the write's time, and lists it again in the member's feed
/// (odin-core sends it after every change while a row names the member: **[verified]**
/// `DriveStorageServiceBase.NoLongerSharedTombstoneOf`, `NoLongerSharedViewOf`).
#[test]
fn a_write_after_a_withdrawal_announces_the_tombstone_again() -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let f = file("f", NodeId::ROOT, b"1");
    let w = create_file(&sharer, &f, b"1")?;
    sharer.share(w.at().file_id)?;
    sharer.withdraw(w.at().file_id)?;
    let withdrawn = feed_end(&member, None)?;
    let members = member.subscribe(false);
    let written = sharer.update(&w.target, &f, None)?;
    let heard: Vec<_> = members.try_iter().collect();
    let feed = rows_after(&member, Some(&withdrawn))?;
    assert_eq!(feed.len(), 1, "{feed:?}");
    let tomb = &feed[0];
    assert!(
        tomb.is_unshared() && tomb.file_id == written.at().file_id,
        "{tomb:?}"
    );
    assert_eq!(tomb.changed_at, sharer.header(w.at().file_id)?.changed_at);
    assert_eq!(heard, [ChangeRef::of(tomb)]);
    Ok(())
}

/// Sharing what isn't a live file is refused as Homebase refuses it (a 400: fatal), as is
/// withdrawing a share gone with a file deleted for good; a tombstone's share is withdrawn
/// without moving it (odin-core PR #2141). The member reads a folder's payload as none now, a
/// tombstone's as deleted, as `PeerRemote` does.
#[test]
fn shares_of_what_isnt_live_and_payloads_not_there() -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let dir = sharer.create(&folder("dir", NodeId::ROOT), None)?.at();
    let tomb = create_file(&sharer, &file("tomb", NodeId::ROOT, b"1"), b"1")?.at();
    let gone = create_file(&sharer, &file("gone", NodeId::ROOT, b"2"), b"2")?.at();
    for at in [dir, tomb, gone] {
        sharer.share(at.file_id)?;
    }
    let read = |at: hbsync_types::RemoteRef| member.read(at.file_id, None, &mut Vec::new());
    assert!(
        matches!(read(dir), Err(RemoteError::StaleRange(_))),
        "{:?}",
        read(dir)
    );
    sharer.server().delete(tomb.file_id, tomb.version)?;
    sharer.server().hard_delete(gone.file_id, gone.version)?;
    assert!(
        matches!(read(tomb), Err(RemoteError::Deleted)),
        "{:?}",
        read(tomb)
    );
    for refused in [
        sharer.share(tomb.file_id),
        sharer.share(gone.file_id),
        sharer.withdraw(gone.file_id),
    ] {
        assert!(matches!(refused, Err(RemoteError::Fatal(_))), "{refused:?}");
    }
    // The share goes, but the tombstone keeps its time and place in the feed, where the member
    // finds the "no longer shared" tombstone; the change is announced at that time.
    let (members, owners) = (member.subscribe(false), sharer.subscribe(false));
    let (before, feed) = (sharer.header(tomb.file_id)?, feed_end(&sharer, None)?);
    let shared = member.header(tomb.file_id)?;
    assert!(shared.is_deleted() && !shared.is_unshared(), "{shared:?}");
    sharer.withdraw(tomb.file_id)?;
    assert!(
        member.header(tomb.file_id).is_err(),
        "a share withdrawn read"
    );
    assert_eq!(sharer.header(tomb.file_id)?, before);
    assert_eq!(rows_after(&sharer, Some(&feed))?, [], "the feed moved");
    let unshared: Vec<_> = rows_after(&member, None)?
        .into_iter()
        .filter(|r| r.file_id == tomb.file_id)
        .collect();
    assert!(
        unshared.len() == 1 && unshared[0].is_unshared(),
        "{unshared:?}"
    );
    assert_eq!(unshared[0].changed_at, before.changed_at);
    assert_eq!(
        members.try_iter().collect::<Vec<_>>(),
        [ChangeRef::of(&unshared[0])]
    );
    assert_eq!(
        owners.try_iter().collect::<Vec<_>>(),
        [ChangeRef::of(&before)]
    );
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
