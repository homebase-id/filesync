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

    fs::rename(b.root.join("in.txt"), b.root.join("renamed.txt"))?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["d/", "d/renamed.txt = edited"]);
    assert_eq!(b.tree_text(""), ["renamed.txt = edited"]);
    nothing_moved_out(&out)?;
    // Settled: another sync finds nothing to do.
    let report = b.sync()?;
    assert_eq!(report.ops_done, 0, "{report:?}");
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
