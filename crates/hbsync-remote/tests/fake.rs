use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use std::time::Duration;

use hbsync_remote::contract::{self, Bench, Shares, file, folder, upload, with};
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

/// A member writing to what the sharer shares with it for editing passes the contract's
/// scenarios of that, and leaves nothing behind.
#[test]
fn the_editing_contract_holds() -> R {
    shared_contract_holds(true)
}

/// A sharer with a folder named "shared" at the top of its drive, shared for editing.
fn sharing_a_folder_to_edit() -> Result<(FakeRemote, NodeMeta), RemoteError> {
    let sharer = remote();
    let scratch = folder("shared", NodeId::ROOT);
    let dir = sharer.create(&scratch, None)?.at();
    sharer.share_to_edit(dir.file_id)?;
    Ok((sharer, scratch))
}

/// A member that doesn't write is refused every write, of what is shared for editing too.
#[test]
fn a_member_writes_only_where_it_is_one_that_writes() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let reading = FakeRemote::member(sharer.server_handle());
    let new = file("mine", scratch.node, b"1");
    let refused = create_file(&reading, &new, b"1");
    assert!(
        matches!(refused, Err(RemoteError::ReadOnly(_))),
        "{refused:?}"
    );
    let writing = reading.clone().writing();
    let made = create_file(&writing, &new, b"1")?;
    assert!(made.target.token.is_none());
    // The member hears of what it added, as the owner does.
    assert!(sharer.server().shared_live(made.at().file_id));
    Ok(())
}

/// A member is refused before its upload is read, as the Homebase adapter refuses before it
/// sends: a new file where nothing may be added, a change to a file shared for reading alone,
/// and a change that comes with a token (a member has none).
#[test]
fn a_member_is_refused_before_its_upload_is_read() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let theirs = file("to read", scratch.node, b"1");
    let at = create_file(&sharer, &theirs, b"1")?;
    sharer.share(at.at().file_id)?;
    let member = FakeRemote::member(sharer.server_handle()).writing();
    let refused = |answer: Result<Written, RemoteError>| {
        let refused = matches!(
            answer,
            Err(RemoteError::Fatal(_) | RemoteError::ReadOnly(_))
        );
        assert!(refused, "{answer:?}");
    };
    refused(create_file(
        &member,
        &file("at the top", NodeId::ROOT, b"2"),
        b"2",
    ));
    let target = Target {
        at: at.at(),
        token: None,
    };
    let edited = with(&theirs, NodeContent::file("to read", 2, 2, false));
    let mut bytes = std::io::Cursor::new(&b"22"[..]);
    refused(member.update(&target, &edited, Some(upload(&mut bytes))));
    assert_eq!(member.faults().uploads, 0, "an upload was read");
    // With the owner's token: refused, though the file is shared for editing.
    sharer.share_to_edit(at.at().file_id)?;
    refused(member.update(&at.target, &edited, None));
    assert_eq!(sharer.header(at.at().file_id)?.version, at.at().version);
    Ok(())
}

/// A member's writes on their way are held on its server, not by one of its computers:
/// another client of the member waits for a node's earlier write too, and can have it
/// delivered.
#[test]
fn a_members_writes_on_their_way_are_its_servers() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let one = FakeRemote::member(sharer.server_handle()).writing();
    let other = one.sharing_token();
    let new = file("mine", scratch.node, b"1");
    one.faults().on_their_way = true;
    assert!(matches!(
        create_file(&one, &new, b"1"),
        Err(RemoteError::OnItsWay { .. })
    ));
    assert!(matches!(
        create_file(&other, &new, b"1"),
        Err(RemoteError::OnItsWay { .. })
    ));
    let delivered = other.deliver();
    assert!(matches!(delivered.as_slice(), [Ok(_)]), "{delivered:?}");
    assert!(one.deliver().is_empty());
    assert_eq!(sharer.server().live_names(), ["mine", "shared"]);
    Ok(())
}

/// The answer of a write still on its way.
fn waits<T: std::fmt::Debug>(answer: Result<T, RemoteError>) {
    assert!(
        matches!(answer, Err(RemoteError::OnItsWay { .. })),
        "{answer:?}"
    );
}

/// A sharer with a folder and a file "theirs" in it, both shared for editing, and a member
/// that writes.
fn a_file_to_edit() -> Result<(FakeRemote, FakeRemote, NodeMeta, NodeMeta, Target), RemoteError> {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let theirs = file("theirs", scratch.node, b"1");
    let at = create_file(&sharer, &theirs, b"1")?.at();
    sharer.share_to_edit(at.file_id)?;
    let member = FakeRemote::member(sharer.server_handle()).writing();
    Ok((sharer, member, scratch, theirs, Target { at, token: None }))
}

/// A member's writes held on their way answer that they wait and write nothing. While one
/// of a node is on its way, that write asked again, or another of the node, waits too and is
/// not sent a second time. Delivered, each is written; the write asked again is then
/// answered as written (once: the client that sent it had no answer to it yet), without a
/// token or a time, as the Homebase adapter answers a member's write.
#[test]
fn a_members_write_on_its_way_lands_when_it_is_delivered() -> R {
    let (sharer, member, scratch, theirs, target) = a_file_to_edit()?;
    let at = target.at;
    member.faults().on_their_way = true;
    let renamed = renamed(&theirs);
    waits(member.update(&target, &renamed, None));
    let new = file("mine", scratch.node, b"2");
    waits(create_file(&member, &new, b"2"));
    // The same again, and another write of the node: they wait, and nothing more is sent.
    waits(member.update(&target, &renamed, None));
    waits(create_file(&member, &new, b"2"));
    let stopped = member.set_removed(std::slice::from_ref(&target), true)?;
    assert!(stopped.answers.is_empty());
    assert!(matches!(
        stopped.stopped,
        Some(RemoteError::OnItsWay { .. })
    ));
    // Nothing is written yet.
    assert_eq!(sharer.header(at.file_id)?.version, at.version);
    assert_eq!(sharer.server().live_names(), ["shared", "theirs"]);

    // Their server reachable again: a node with a write on its way still waits.
    member.faults().on_their_way = false;
    waits(member.update(&target, &renamed, None));
    waits(member.create(&new, None));

    // Delivered: the rename and the new file, each once.
    let delivered = member.deliver();
    assert!(
        matches!(delivered.as_slice(), [Ok(_), Ok(_)]),
        "{delivered:?}"
    );
    assert_eq!(sharer.server().live_names(), ["mine", "renamed", "shared"]);
    assert!(!sharer.header(at.file_id)?.removed);

    // Asked again, as the engine asks: written, at the version each landed as.
    let again = member.update(&target, &renamed, None)?;
    assert_eq!(again.at(), sharer.header(at.file_id)?.at());
    assert_eq!((again.target.token, again.changed_at), (None, None));
    let added = create_file(&member, &new, b"2")?;
    let theirs_now: Vec<_> = sharer.server().live().map(|h| h.at()).collect();
    assert!(theirs_now.contains(&added.at()), "{added:?}");
    assert_eq!((added.target.token, added.changed_at), (None, None));
    // And once more, the answer had: the file is at another version, the new one is there.
    assert!(matches!(
        member.update(&target, &renamed, None),
        Err(RemoteError::VersionMismatch)
    ));
    assert!(matches!(
        create_file(&member, &new, b"2"),
        Err(RemoteError::AlreadyExists)
    ));
    assert!(member.deliver().is_empty());
    Ok(())
}

/// A write that landed is answered as written only to the client that sent it. Started
/// again (its memory of the send gone), the client meets what landed as someone else's.
#[test]
fn a_write_that_landed_is_anothers_to_a_client_started_again() -> R {
    let (_sharer, member, scratch, theirs, target) = a_file_to_edit()?;
    member.faults().on_their_way = true;
    waits(member.update(&target, &renamed(&theirs), None));
    let new = file("mine", scratch.node, b"2");
    waits(create_file(&member, &new, b"2"));
    let restarted = member.sharing_token();
    assert_eq!(member.deliver().len(), 2);
    assert!(matches!(
        restarted.update(&target, &renamed(&theirs), None),
        Err(RemoteError::VersionMismatch)
    ));
    assert!(matches!(
        create_file(&restarted, &new, b"2"),
        Err(RemoteError::AlreadyExists)
    ));
    Ok(())
}

/// A write that landed answers only that very write: another write of the node, by the
/// client that sent the first, is answered as the file is now.
#[test]
fn a_write_that_landed_answers_only_that_write() -> R {
    let (sharer, member, _, theirs, target) = a_file_to_edit()?;
    member.faults().on_their_way = true;
    waits(member.update(&target, &renamed(&theirs), None));
    member.faults().on_their_way = false;
    assert_eq!(member.deliver().len(), 1);
    let other = with(&theirs, NodeContent::file("other", 1, 1, false));
    assert!(matches!(
        member.update(&target, &other, None),
        Err(RemoteError::VersionMismatch)
    ));
    assert_eq!(sharer.server().live_names(), ["renamed", "shared"]);
    // The other write took nothing from the first: asked again, that one is answered.
    let again = member.update(&target, &renamed(&theirs), None)?;
    assert_eq!(again.at(), sharer.header(target.at.file_id)?.at());
    Ok(())
}

/// A send the owner's server didn't take (their file changed while it was on its way) has
/// no answer to give: asked again, the write is answered as the file is now.
#[test]
fn a_write_not_taken_is_answered_as_the_file_is_now() -> R {
    let (sharer, member, _, theirs, target) = a_file_to_edit()?;
    member.faults().on_their_way = true;
    waits(member.update(&target, &renamed(&theirs), None));
    let owners = Target {
        at: target.at,
        token: sharer.header(target.at.file_id)?.token,
    };
    let theirs_now = with(&theirs, NodeContent::file("the owner's", 1, 1, false));
    let now = sharer.update(&owners, &theirs_now, None)?.at();
    member.faults().on_their_way = false;
    // Their file changed: that is told before "still on its way", as the adapter reads their
    // file first. Another change, to the file as it is now, waits for the one staged still.
    assert!(matches!(
        member.update(&target, &renamed(&theirs), None),
        Err(RemoteError::VersionMismatch)
    ));
    let at_theirs = Target {
        at: now,
        token: None,
    };
    waits(member.update(&at_theirs, &renamed(&theirs), None));
    let delivered = member.deliver();
    assert!(
        matches!(delivered.as_slice(), [Err(RemoteError::VersionMismatch)]),
        "{delivered:?}"
    );
    assert!(matches!(
        member.update(&target, &renamed(&theirs), None),
        Err(RemoteError::VersionMismatch)
    ));
    assert_eq!(sharer.server().live_names(), ["shared", "the owner's"]);
    // Nothing on its way now: the change to the file as it is goes.
    member.update(&at_theirs, &renamed(&theirs), None)?;
    assert_eq!(sharer.server().live_names(), ["renamed", "shared"]);
    Ok(())
}

/// The word of a write that landed answers that write only: a later write of the node that
/// wasn't taken is not answered with the earlier one's.
#[test]
fn an_earlier_writes_word_doesnt_answer_a_later_one() -> R {
    let (sharer, member, _, theirs, target) = a_file_to_edit()?;
    member.faults().on_their_way = true;
    waits(member.update(&target, &renamed(&theirs), None));
    assert_eq!(member.deliver().len(), 1);
    // The first landed, and its answer was never asked for. A second one, to the file as it
    // is now, is on its way when the owner changes the file: it isn't taken.
    let now = sharer.header(target.at.file_id)?;
    let at_theirs = Target {
        at: now.at(),
        token: None,
    };
    let second = with(&theirs, NodeContent::file("second", 1, 1, false));
    waits(member.update(&at_theirs, &second, None));
    let owners = Target {
        at: now.at(),
        token: now.token,
    };
    let theirs_now = with(&theirs, NodeContent::file("the owner's", 1, 1, false));
    sharer.update(&owners, &theirs_now, None)?;
    let delivered = member.deliver();
    assert!(
        matches!(delivered.as_slice(), [Err(RemoteError::VersionMismatch)]),
        "{delivered:?}"
    );
    let answer = member.update(&at_theirs, &second, None);
    assert!(
        matches!(answer, Err(RemoteError::VersionMismatch)),
        "{answer:?}"
    );
    Ok(())
}

/// A new file that landed and that its owner deleted since is not answered as written: the
/// member finds no node of theirs, and sends it again.
#[test]
fn a_new_file_deleted_since_it_landed_is_sent_again() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let member = FakeRemote::member(sharer.server_handle()).writing();
    let new = file("mine", scratch.node, b"1");
    member.faults().on_their_way = true;
    waits(create_file(&member, &new, b"1"));
    member.faults().on_their_way = false;
    let landed = member.deliver().remove(0)?;
    sharer
        .delete_for_good(&contract::doomed(&[landed.at()]))?
        .single()?;
    let again = create_file(&member, &new, b"1")?;
    assert_ne!(again.at().file_id, landed.at().file_id);
    assert_eq!(sharer.header(again.at().file_id)?.at(), again.at());
    Ok(())
}

/// A word is its send's alone: a new file another client of the member sent again, which
/// the owner's server didn't take (the first one's is there), is not answered with the
/// first one's word.
#[test]
fn a_new_files_word_is_its_sends_alone() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let first = FakeRemote::member(sharer.server_handle()).writing();
    let new = file("mine", scratch.node, b"1");
    first.faults().on_their_way = true;
    waits(create_file(&first, &new, b"1"));
    assert_eq!(first.deliver().len(), 1);
    // Started again before it asked: the same file is sent once more.
    let second = first.sharing_token();
    second.faults().on_their_way = true;
    waits(create_file(&second, &new, b"1"));
    second.faults().on_their_way = false;
    let delivered = second.deliver();
    assert!(
        matches!(delivered.as_slice(), [Err(RemoteError::AlreadyExists)]),
        "{delivered:?}"
    );
    let answer = create_file(&second, &new, b"1");
    assert!(
        matches!(answer, Err(RemoteError::AlreadyExists)),
        "{answer:?}"
    );
    Ok(())
}

/// A file deleted by its owner is told as deleted, whatever became of a write to it: one
/// still on its way, or one that landed before the delete.
#[test]
fn a_deleted_file_is_told_before_what_became_of_a_write() -> R {
    for landed in [false, true] {
        let (sharer, member, _, theirs, target) = a_file_to_edit()?;
        member.faults().on_their_way = true;
        waits(member.update(&target, &renamed(&theirs), None));
        if landed {
            assert_eq!(member.deliver().len(), 1);
        }
        let at = sharer.header(target.at.file_id)?.at();
        sharer.delete(&contract::doomed(&[at]))?.single()?;
        let answer = member.update(&target, &renamed(&theirs), None);
        assert!(matches!(answer, Err(RemoteError::Deleted)), "{answer:?}");
    }
    Ok(())
}

/// Of several removals by a member, one whose file changed while an earlier write to it is
/// on its way has its own answer, and the rest go on; a failure that isn't one file's own
/// stops them.
#[test]
fn a_members_removals_go_on_past_a_files_own_answer_only() -> R {
    let (sharer, member, scratch, theirs, target) = a_file_to_edit()?;
    let other = file("other", scratch.node, b"1");
    let other_at = create_file(&sharer, &other, b"1")?.at();
    sharer.share_to_edit(other_at.file_id)?;
    let others = Target {
        at: other_at,
        token: None,
    };
    member.faults().on_their_way = true;
    waits(member.update(&target, &renamed(&theirs), None));
    member.faults().on_their_way = false;
    let owners = Target {
        at: target.at,
        token: sharer.header(target.at.file_id)?.token,
    };
    let theirs_now = with(&theirs, NodeContent::file("the owner's", 1, 1, false));
    sharer.update(&owners, &theirs_now, None)?;
    let both = member.set_removed(&[target.clone(), others], true)?;
    assert!(both.stopped.is_none(), "{both:?}");
    let [Err(RemoteError::VersionMismatch), Ok(removed)] = both.answers.as_slice() else {
        panic!("{both:?}");
    };

    // A failure injected for the first file is not that file's own: the second isn't sent.
    assert_eq!(member.deliver().len(), 1);
    let now = Target {
        at: sharer.header(target.at.file_id)?.at(),
        token: None,
    };
    member.faults().fail_writes_named = Some("the owner's".into());
    let stopped = member.set_removed(&[now, removed.target.clone()], false)?;
    assert!(stopped.answers.is_empty(), "{stopped:?}");
    assert!(matches!(stopped.stopped, Some(RemoteError::Transient(_))));
    assert!(sharer.header(other_at.file_id)?.removed);
    Ok(())
}

/// `meta`'s file, renamed.
fn renamed(meta: &NodeMeta) -> NodeMeta {
    with(meta, NodeContent::file("renamed", 1, 1, false))
}

/// Of several removals by a member, each file has its own answer, and the first still on
/// its way stops the rest, which are not sent: delivered, only that one is removed.
#[test]
fn a_members_removals_stop_at_the_one_on_its_way() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let member = FakeRemote::member(sharer.server_handle()).writing();
    let mut made = Vec::new();
    for name in ["a", "b", "c", "to read"] {
        let meta = file(name, scratch.node, b"1");
        let at = create_file(&sharer, &meta, b"1")?.at();
        match name {
            "to read" => sharer.share(at.file_id)?,
            _ => sharer.share_to_edit(at.file_id)?,
        }
        made.push((meta, Target { at, token: None }));
    }
    let targets =
        |names: &[usize]| -> Vec<Target> { names.iter().map(|i| made[*i].1.clone()).collect() };
    let removed = |i: usize| sharer.header(made[i].1.at.file_id).map(|h| h.removed);

    // Their file is looked at in the Homebase adapter's order. One as it is already is left
    // so, shared for editing or not; one at another version by now is told so before it is
    // told read-only.
    let as_it_is = member.set_removed(&targets(&[3]), false)?.single()?;
    assert_eq!(as_it_is.at(), made[3].1.at);
    let stale = Target {
        at: hbsync_types::RemoteRef {
            version: made[0].1.at.version,
            ..made[3].1.at
        },
        token: None,
    };
    assert!(matches!(
        member.update(&stale, &made[3].0, None),
        Err(RemoteError::VersionMismatch)
    ));

    // Held on their way: the first is sent, the rest are not.
    member.faults().on_their_way = true;
    let stopped = member.set_removed(&targets(&[0, 1, 2]), true)?;
    assert!(stopped.answers.is_empty(), "{stopped:?}");
    assert!(matches!(
        stopped.stopped,
        Some(RemoteError::OnItsWay { .. })
    ));
    member.faults().on_their_way = false;
    assert_eq!(member.deliver().len(), 1);
    assert_eq!(
        (removed(0)?, removed(1)?, removed(2)?),
        (true, false, false)
    );

    // One file's own answer doesn't stop the rest: one shared for reading alone is refused,
    // the next is removed. One on its way stops them: the file before it is written, the
    // one after isn't.
    member.faults().on_their_way = true;
    waits(member.update(&made[2].1, &renamed(&made[2].0), None));
    member.faults().on_their_way = false;
    let mixed = member.set_removed(&targets(&[3, 1, 2, 0]), true)?;
    assert!(matches!(mixed.stopped, Some(RemoteError::OnItsWay { .. })));
    assert!(
        matches!(
            mixed.answers.as_slice(),
            [Err(RemoteError::ReadOnly(_)), Ok(_)]
        ),
        "{mixed:?}"
    );
    assert_eq!(
        (removed(3)?, removed(1)?, removed(2)?),
        (false, true, false)
    );
    Ok(())
}

/// A member hears its own writes, whatever it asks of its subscription (a socket to another
/// identity's server leaves none out), and a file it adds is one write to those who listen:
/// it comes with its share.
#[test]
fn a_member_hears_its_own_writes_and_a_new_file_is_one() -> R {
    let (sharer, scratch) = sharing_a_folder_to_edit()?;
    let member = FakeRemote::member(sharer.server_handle()).writing();
    let (members, owners) = (member.subscribe(true), sharer.subscribe(false));
    let made = create_file(&member, &file("mine", scratch.node, b"1"), b"1")?;
    for (who, heard) in [("the member", members), ("the owner", owners)] {
        let heard: Vec<_> = heard.try_iter().collect();
        assert_eq!(heard.len(), 1, "{who} heard {heard:?}");
        assert_eq!(
            (heard[0].file_id, heard[0].version),
            (made.at().file_id, made.at().version),
            "{who}"
        );
    }
    Ok(())
}

/// A member reading what the sharer shares passes the read-only subset.
#[test]
fn the_shared_contract_holds() -> R {
    shared_contract_holds(false)
}

/// The contract of a member, one that writes (`writing`) or only reads, against the fake,
/// and nothing of it left afterwards.
fn shared_contract_holds(writing: bool) -> R {
    let sharer = remote();
    let member = FakeRemote::member(sharer.server_handle());
    let member = if writing { member.writing() } else { member };
    // The sharer's own feed end: the bench's start is the member's cursor.
    let sharers_start = feed_end(&sharer, None)?;
    let bench = match writing {
        true => Bench::editing(&sharer, &member, "contract", Duration::ZERO)?,
        false => Bench::shared(&sharer, &member, "contract", Duration::ZERO)?,
    };
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
    // (An end that isn't to come is refused, as Homebase refuses it.)
    let now = sharer.server().now();
    assert!(matches!(
        sharer.share_until(file_id, now),
        Err(RemoteError::Fatal(_))
    ));
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
