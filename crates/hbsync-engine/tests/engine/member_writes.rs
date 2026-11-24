//! A member that writes to what another identity shares with it for editing (sync semantics
//! §13, "What may be changed is told per item"): the engine and the executor end to end,
//! against the fake sharer.

use hbsync_remote::contract::Shares;
use hbsync_remote::fake::{FakeRemote, FakeServer};

use super::*;

type Server = Arc<Mutex<FakeServer>>;

/// The owner `a` shares what is named `name` on its drive with the member, for editing.
fn share_to_edit(a: &Client, server: &Server, name: &str) -> R {
    a.remote
        .share_to_edit(common::header_named(server, name).file_id)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok(())
}

/// A member of what `server`'s owner shares, whose folder writes to what is shared with it
/// for editing; with the folder that what it may not send moves out to.
fn writing_member(
    server: &Server,
    name: &str,
) -> Result<(Client, tempfile::TempDir), Box<dyn std::error::Error>> {
    let out = tempfile::tempdir()?;
    let to = out.path().to_path_buf();
    let remote = FakeRemote::member(server.clone()).writing();
    let client = Client::on(remote, name, move |cfg| {
        cfg.folder.read_only = Some(to.clone());
        cfg.folder.writes = true;
    })?;
    Ok((client, out))
}

/// Nothing moved out of the member's folder.
fn nothing_moved_out(out: &tempfile::TempDir) -> R {
    let moved = hbsync_engine::testkit::tree_text_of(out.path());
    assert!(moved.is_empty(), "moved out: {moved:?}");
    Ok(())
}

/// An edit and a rename of a file shared for editing reach the sharer's drive and its
/// devices; the file shared for reading beside it stays as it is, the member's edit of it
/// moved out.
#[test]
fn a_members_edit_and_rename_reach_the_sharer() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("to-edit.txt", "one")?;
    a.write("to-read.txt", "two")?;
    a.sync()?;
    share_to_edit(&a, &server, "to-edit.txt")?;
    a.remote
        .share(common::header_named(&server, "to-read.txt").file_id)?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["to-edit.txt = one", "to-read.txt = two"]);

    b.write("to-edit.txt", "one, edited by the member")?;
    b.sync()?;
    nothing_moved_out(&out)?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "to-edit.txt = one, edited by the member",
            "to-read.txt = two"
        ]
    );

    fs::rename(b.root.join("to-edit.txt"), b.root.join("renamed.txt"))?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "renamed.txt = one, edited by the member",
            "to-read.txt = two"
        ]
    );
    nothing_moved_out(&out)?;

    b.write("to-read.txt", "two, edited by the member")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read("to-read.txt")?, b"two");
    assert_eq!(b.read("to-read.txt")?, b"two");
    assert_eq!(
        hbsync_engine::testkit::tree_text_of(out.path()),
        ["to-read.txt = two, edited by the member"]
    );
    Ok(())
}

/// A file shared without its folder is at the member's top: edited and renamed there, it
/// stays in its folder on the sharer's drive. The write sends the folder the drive has it
/// in, never the member's top.
#[test]
fn a_file_at_the_members_top_stays_in_its_folder_on_the_sharers_drive() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/in.txt", "one")?;
    a.sync()?;
    share_to_edit(&a, &server, "in.txt")?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["in.txt = one"]);

    b.write("in.txt", "edited")?;
    let report = b.sync()?;
    // The mirror keeps the drive's folder (the row is shown at the top all the same), and
    // the base is settled where the file is here: nothing is planned again for it.
    assert_eq!(report.ops_done, 1, "{report:?}");
    let node = common::header_named(&server, "in.txt")
        .node()
        .ok_or("node")?;
    let base = b.engine.db().read(|s| s.get_base(node))?.ok_or("base")?;
    assert_eq!(base.state.parent, NodeId::ROOT);
    let row = b.engine.db().read(|s| s.get_mirror(node))?.ok_or("row")?;
    assert_eq!(row.parent, NodeId::ROOT, "shown at the top");
    assert!(!b.engine.db().read(|s| s.is_dirty(node))?);
    a.sync()?;
    assert_eq!(a.tree_text(""), ["d/", "d/in.txt = edited"]);
    assert_eq!(b.tree_text(""), ["in.txt = edited"]);

    // (As the daemon syncs a local change: the picture current, no feed read first.)
    b.announced();
    fs::rename(b.root.join("in.txt"), b.root.join("renamed.txt"))?;
    let report = b.sync_unasked()?;
    // One operation: the base takes the new name with the write's answer (settled against
    // the row as it is shown here, at the top), not by a plan of its own after it.
    assert_eq!(report.ops_done, 1, "{report:?}");
    a.sync()?;
    assert_eq!(a.tree_text(""), ["d/", "d/renamed.txt = edited"]);
    assert_eq!(b.tree_text(""), ["renamed.txt = edited"]);
    nothing_moved_out(&out)?;
    // Settled: another sync finds nothing to do, and the base has the new name, so the
    // sharer's next rename of it comes down (it isn't taken for one to undo).
    let report = b.sync()?;
    assert_eq!(report.ops_done, 0, "{report:?}");
    let base = b.engine.db().read(|s| s.get_base(node))?.ok_or("base")?;
    assert_eq!(
        (base.state.parent, base.state.name.as_str()),
        (NodeId::ROOT, "renamed.txt")
    );
    fs::rename(a.root.join("d/renamed.txt"), a.root.join("d/theirs.txt"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["theirs.txt = edited"]);
    a.sync()?;
    assert_eq!(a.tree_text(""), ["d/", "d/theirs.txt = edited"]);
    nothing_moved_out(&out)?;
    Ok(())
}

/// The sharer turned the share back to reading, and the member hasn't read that yet: its
/// store still says the file may be changed, so its edit is sent, and their server refuses
/// it. The file counts as read-only from there on: the store says so, the sharer's file is
/// as it was, and the edit moves out at the next sync, kept.
#[test]
fn a_write_the_sharer_refuses_makes_the_item_read_only_here() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("to-edit.txt", "one")?;
    a.sync()?;
    share_to_edit(&a, &server, "to-edit.txt")?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    let file = common::header_named(&server, "to-edit.txt").file_id;
    let may_change = |b: &mut Client| -> Result<bool, Box<dyn std::error::Error>> {
        let row = b.engine.db().read(|s| s.mirror_by_file(file))?;
        Ok(row.ok_or("no row")?.writable)
    };
    assert!(may_change(&mut b)?);

    // Back to reading; the member isn't told: its picture of the drive counts as current
    // (the notifications run), so its next sync reads no feed first.
    b.announced();
    a.remote.share(file)?;
    b.write("to-edit.txt", "the member's edit")?;
    let report = b.sync_unasked()?;
    let refused = |(_, why): &(_, hbsync_engine::exec::Skip)| matches!(why, hbsync_engine::exec::Skip::Stale(why) if why.contains("read-only"));
    assert!(
        report.skipped.iter().any(refused),
        "no write was refused: {report:?}"
    );
    assert!(
        !may_change(&mut b)?,
        "the store still says it may be changed"
    );
    // Planned again at the next sync, as any write the drive refused for its state: the
    // edit moves out, and the sharer's version comes back.
    b.sync_unasked()?;
    assert_eq!(
        hbsync_engine::testkit::tree_text_of(out.path()),
        ["to-edit.txt = the member's edit"]
    );
    b.sync()?;
    assert_eq!(b.tree_text(""), ["to-edit.txt = one"]);
    a.sync()?;
    assert_eq!(a.tree_text(""), ["to-edit.txt = one"]);
    Ok(())
}

/// The share still says "edit", and the sharer's server refuses the write all the same (its
/// circle lost the right to write: the share's rows don't tell). No feed row says otherwise,
/// so it is the refusal alone that makes the file read-only here: the store says so, and
/// the edit moves out at the next sync, not sent again.
#[test]
fn a_refusal_the_share_doesnt_tell_of_makes_the_item_read_only_here() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("to-edit.txt", "one")?;
    a.sync()?;
    share_to_edit(&a, &server, "to-edit.txt")?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    let file = common::header_named(&server, "to-edit.txt").file_id;
    let may_change = |b: &mut Client| -> Result<bool, Box<dyn std::error::Error>> {
        let row = b.engine.db().read(|s| s.mirror_by_file(file))?;
        Ok(row.ok_or("no row")?.writable)
    };
    assert!(may_change(&mut b)?);

    b.remote.faults().their_server_refuses = true;
    b.write("to-edit.txt", "the member's edit")?;
    b.sync()?;
    assert!(
        !may_change(&mut b)?,
        "the store still says it may be changed"
    );
    b.sync()?;
    assert_eq!(
        hbsync_engine::testkit::tree_text_of(out.path()),
        ["to-edit.txt = the member's edit"]
    );
    assert_eq!(b.tree_text(""), ["to-edit.txt = one"]);
    assert!(!may_change(&mut b)?);
    // Not sent again, also once their server would take it: the edit is out of the folder.
    b.remote.faults().their_server_refuses = false;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["to-edit.txt = one"]);
    Ok(())
}

/// What the member makes in a folder shared with it for editing is added to the sharer's
/// drive, a new folder with what is in it, and the member may change it from then on (it is
/// shared as the folder is). What it makes at the top of its folder moves out: the top is
/// no folder of the sharer's drive.
#[test]
fn what_a_member_makes_in_a_folder_shared_for_editing_reaches_the_sharer() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/theirs.txt", "one")?;
    a.sync()?;
    share_to_edit(&a, &server, "d")?;
    share_to_edit(&a, &server, "theirs.txt")?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["d/", "d/theirs.txt = one"]);

    b.write("d/mine.txt", "made by the member")?;
    b.write("d/New/below.txt", "below")?;
    b.write("at-the-top.txt", "not theirs")?;
    let report = b.sync()?;
    // A file in a folder made in the same run goes up in it: none is refused for want of
    // the folder's row.
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    a.sync()?;
    let both = [
        "d/",
        "d/New/",
        "d/New/below.txt = below",
        "d/mine.txt = made by the member",
        "d/theirs.txt = one",
    ];
    assert_eq!(a.tree_text(""), both);
    assert_eq!(b.tree_text(""), both);
    assert_eq!(
        hbsync_engine::testkit::tree_text_of(out.path()),
        ["at-the-top.txt = not theirs"]
    );

    // Its own now, to change: an edit of what it made goes up.
    b.write("d/mine.txt", "edited")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read("d/mine.txt")?, b"edited");
    // And the sharer's change of it comes down.
    a.write("d/New/below.txt", "theirs now")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("d/New/below.txt")?, b"theirs now");
    Ok(())
}

/// A file edited by both in a folder shared for editing: the sharer's version keeps the
/// file, and the member's becomes a conflict copy beside it, on both sides. A move between
/// two folders shared for editing reaches the sharer.
#[test]
fn a_members_conflict_copy_and_move_reach_the_sharer() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/both.txt", "base")?;
    a.write("d/moved.txt", "m")?;
    a.write("e/keep.txt", "k")?;
    a.sync()?;
    for name in ["d", "e", "both.txt", "moved.txt", "keep.txt"] {
        share_to_edit(&a, &server, name)?;
    }
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;

    a.write("d/both.txt", "theirs")?;
    a.sync()?;
    b.write("d/both.txt", "mine")?;
    fs::rename(b.root.join("d/moved.txt"), b.root.join("e/moved.txt"))?;
    b.sync()?;
    a.sync()?;
    nothing_moved_out(&out)?;
    let theirs = a.tree_text("");
    assert_eq!(theirs, b.tree_text(""));
    assert!(
        theirs.contains(&"d/both.txt = theirs".to_owned()),
        "{theirs:?}"
    );
    assert!(theirs.contains(&"e/moved.txt = m".to_owned()), "{theirs:?}");
    assert!(
        !theirs.iter().any(|l| l.starts_with("d/moved.txt")),
        "{theirs:?}"
    );
    let copies: Vec<&String> = (theirs.iter())
        .filter(|l| l.starts_with("d/both") && l.ends_with("= mine"))
        .collect();
    assert_eq!(copies.len(), 1, "{theirs:?}");
    Ok(())
}

/// The names of what the sharer's drive has in Recently deleted.
fn removed_names(server: &Server) -> Vec<String> {
    let server = server.lock().expect("server");
    let mut names: Vec<String> = (server.removed())
        .filter_map(|h| match &h.body {
            hbsync_remote::RemoteBody::Live { content, .. } => Some(content.name.clone()),
            hbsync_remote::RemoteBody::Deleted => None,
        })
        .collect();
    names.sort();
    names
}

/// A member's delete of what it may change is a removal on the sharer's drive: the item
/// goes to the sharer's Recently deleted (nothing is deleted for good), and off the
/// sharer's devices. A folder goes with all that is in it.
#[test]
fn a_members_delete_goes_to_the_sharers_recently_deleted() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/one.txt", "1")?;
    a.write("d/sub/two.txt", "2")?;
    a.write("d/keep.txt", "k")?;
    a.write("e/three.txt", "3")?;
    a.sync()?;
    for name in [
        "d",
        "sub",
        "one.txt",
        "two.txt",
        "keep.txt",
        "e",
        "three.txt",
    ] {
        share_to_edit(&a, &server, name)?;
    }
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;

    fs::remove_file(b.root.join("d/one.txt"))?;
    fs::remove_dir_all(b.root.join("d/sub"))?;
    b.sync()?;
    a.sync()?;
    let left = ["d/", "d/keep.txt = k", "e/", "e/three.txt = 3"];
    assert_eq!(a.tree_text(""), left);
    assert_eq!(b.tree_text(""), left);
    assert_eq!(removed_names(&server), ["one.txt", "sub", "two.txt"]);
    nothing_moved_out(&out)?;

    // The sharer restores one: it comes back to the member too.
    let one = common::header_named(&server, "one.txt");
    a.engine.drive().restore(&[one.node().ok_or("node")?])?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read("d/one.txt")?, b"1");
    Ok(())
}

/// A file shared without its folder, at the member's top: deleted there, it goes to the
/// sharer's Recently deleted from the folder the sharer has it in, and comes back into that
/// folder when the sharer restores it. (A removal names no folder: it is sent by the file's
/// id and version alone.)
#[test]
fn a_members_delete_at_its_top_removes_the_file_where_the_sharer_has_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/in.txt", "one")?;
    a.write("d/other.txt", "two")?;
    a.sync()?;
    share_to_edit(&a, &server, "in.txt")?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["in.txt = one"]);

    fs::remove_file(b.root.join("in.txt"))?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["d/", "d/other.txt = two"]);
    assert_eq!(removed_names(&server), ["in.txt"]);
    nothing_moved_out(&out)?;
    let node = common::header_named(&server, "in.txt")
        .node()
        .ok_or("node")?;
    a.engine.drive().restore(&[node])?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        ["d/", "d/in.txt = one", "d/other.txt = two"]
    );
    b.sync()?;
    assert_eq!(b.tree_text(""), ["in.txt = one"]);
    Ok(())
}

/// A folder with an item the member may only read: its delete here is undone, whole. The
/// folder comes back with all that was in it, and nothing is removed on the sharer's drive.
#[test]
fn a_members_delete_of_a_folder_it_may_not_empty_is_undone() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/mine-to-change.txt", "1")?;
    a.write("d/to-read.txt", "2")?;
    a.write("other.txt", "o")?;
    a.sync()?;
    for name in ["d", "mine-to-change.txt", "other.txt"] {
        share_to_edit(&a, &server, name)?;
    }
    a.remote
        .share(common::header_named(&server, "to-read.txt").file_id)?;
    let (mut b, out) = writing_member(&server, "b")?;
    b.sync()?;
    let all = [
        "d/",
        "d/mine-to-change.txt = 1",
        "d/to-read.txt = 2",
        "other.txt = o",
    ];
    assert_eq!(b.tree_text(""), all);

    fs::remove_dir_all(b.root.join("d"))?;
    b.sync()?;
    assert_eq!(b.tree_text(""), all);
    a.sync()?;
    assert_eq!(a.tree_text(""), all);
    assert!(
        removed_names(&server).is_empty(),
        "{:?}",
        removed_names(&server)
    );
    nothing_moved_out(&out)?;
    Ok(())
}
