//! Read-only folders (sync semantics §13), end to end: what changed there moves out, the
//! drive's state comes back, and nothing is written to the drive.

use super::*;

/// The drive's live nodes, each at its version: what a read-only folder never changes.
fn versions(server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>) -> Vec<RemoteRef> {
    let server = server.lock().expect("server");
    let mut live: Vec<RemoteRef> = server.live().map(|h| h.at()).collect();
    live.sort_by_key(|r| r.file_id);
    live
}

/// `b` follows `a` read-only: its user's edit, new folder, rename and delete there are kept
/// (moved out where they would be lost) or undone, in one sync, and the drive stays as `a`
/// left it.
#[test]
fn a_read_only_folder_keeps_what_changed_and_follows_the_drive() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("top.txt", "top")?;
    a.write("gone.txt", "gone")?;
    a.sync()?;
    let (mut b, out) = Client::read_only(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));
    let drive = versions(&server);

    b.write("Docs/report.txt", "mine")?;
    b.write("New/x.txt", "x")?;
    fs::rename(b.root.join("top.txt"), b.root.join("renamed.txt"))?;
    fs::remove_file(b.root.join("gone.txt"))?;
    let report = b.sync()?;
    assert!(
        report.failed.is_empty() && report.skipped.is_empty(),
        "{report:?}"
    );
    assert_eq!(versions(&server), drive, "the drive is written to");
    assert_eq!(b.tree_text(""), a.tree_text(""));
    assert_eq!(b.read_text("Docs/report.txt")?, "quarterly");
    assert_eq!(fs::read_to_string(out.path().join("report.txt"))?, "mine");
    assert_eq!(
        fs::read_to_string(out.path().join("New").join("x.txt"))?,
        "x"
    );
    let mut moved: Vec<String> = report.moved_out.iter().map(|(p, _)| p.clone()).collect();
    moved.sort();
    assert_eq!(moved, ["Docs/report.txt", "New"]);
    // Settled: nothing more to do.
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// A crash or a power cut at each commit of a sync that moves things out: the next sync ends
/// as one without it, every change here kept once where it went, the drive as it was.
#[test]
fn a_crash_at_any_commit_while_moving_out_loses_nothing() -> R {
    // Commit after commit, till the sync has no more.
    for at in 0.. {
        let mut any = false;
        for before in [true, false] {
            for kept in [common::Kept::All, common::Kept::Durable(None)] {
                let case = format!("at {at}, before {before}, {kept:?}");
                let server = server();
                let mut a = Client::new(&server, "a")?;
                a.write("Docs/report.txt", "quarterly")?;
                a.write("top.txt", "top")?;
                a.sync()?;
                let (mut b, out) = Client::read_only(&server, "b")?;
                b.sync()?;
                let drive = versions(&server);
                b.write("Docs/report.txt", "mine")?;
                b.write("New/x.txt", "x")?;
                b.write("top.txt", "edited")?;
                any |= common::crash(&mut b, kept, move |now, n| {
                    n == at && (now == Commit::Before) == before
                })?;
                b.sync()?;
                assert_eq!(versions(&server), drive, "{case}");
                assert_eq!(b.tree_text(""), a.tree_text(""), "{case}");
                assert_eq!(
                    hbsync_engine::testkit::tree_text_of(out.path()),
                    [
                        "New/",
                        "New/x.txt = x",
                        "report.txt = mine",
                        "top.txt = edited"
                    ],
                    "{case}"
                );
            }
        }
        if !any {
            assert!(at > 10, "the sync crashed at {at} commits only");
            break;
        }
    }
    Ok(())
}

/// Putting held deletes back writes to the drive: refused read-only, whatever is held.
#[test]
fn nothing_is_put_back_from_a_read_only_folder() -> R {
    let server = server();
    let (mut b, _out) = Client::read_only(&server, "b")?;
    match b.engine.put_back(&[NodeId::new()]) {
        Err(hbsync_engine::EngineError::ReadOnly(_)) => Ok(()),
        other => Err(format!("{other:?}").into()),
    }
}

/// A folder removed on the drive, one item in it still live there (its removal failed): the
/// read-only folder lets both go, and the folder's restore brings back all of it, the item
/// that never changed on the drive included.
#[test]
fn a_restored_folder_comes_back_whole_in_a_read_only_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("d/keep.txt", "k")?;
    a.write("d/other.txt", "o")?;
    a.write("stay.txt", "s")?; // not everything is deleted: no mass-delete hold
    a.sync()?;
    let (mut b, _out) = Client::read_only(&server, "b")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("d"))?;
    a.remote.faults().fail_writes_named = Some("keep.txt".into());
    a.sync()?;
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["keep.txt", "stay.txt"]
    );
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = s"]);
    // Nothing of it is dirty here meanwhile: the folder coming back brings what is in it.
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    let d = a
        .recently_deleted(10)?
        .into_iter()
        .find(|i| i.path == "d")
        .ok_or("d")?;
    a.engine.drive().restore(&[d.node])?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["d/", "d/keep.txt = k", "d/other.txt = o", "stay.txt = s"]
    );
    Ok(())
}

/// A file its user made read-only in their own folder (a copy of a shared file keeps its
/// bits, #459) follows the drive as any file: another device's tag lands on it, read-only
/// as before; a new version replaces it; a delete removes it.
#[test]
fn a_read_only_file_of_ones_own_follows_the_drive() -> R {
    use hbsync_platform::ops::{is_read_only, set_read_only};
    use hbsync_platform::tags::{self, Tag, Tags};

    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("doc.txt", "one")?;
    a.write("bye.txt", "bye")?;
    a.sync()?;
    b.sync()?;
    for file in ["doc.txt", "bye.txt"] {
        set_read_only(&b.root.join(file), true)?;
    }
    // The bit is this device's: nothing goes up for it.
    b.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));

    // A tag from the other device (where the volume keeps them), then a delete.
    let work = Tags::new([Tag::new("Work", 0)]);
    let tagged = tags::CAN_TAG && tags::write(&a.root.join("doc.txt"), &work)?.is_some();
    fs::remove_file(a.root.join("bye.txt"))?;
    a.sync()?;
    b.sync()?;
    if tagged {
        assert_eq!(tags::read(&b.root.join("doc.txt")), Some(work));
    }
    assert!(is_read_only(&b.root.join("doc.txt")), "as the user had it");
    assert!(!b.root.join("bye.txt").exists());

    // A new version: the bit was the old file's.
    a.write("doc.txt", "two")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("doc.txt")?, "two");
    assert!(!is_read_only(&b.root.join("doc.txt")));
    assert_eq!(b.tree_text(""), a.tree_text(""));
    a.sync()?;
    assert_eq!(b.tree_text(""), a.tree_text(""));
    Ok(())
}
