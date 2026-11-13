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
    let out = tempfile::tempdir()?;
    let to = out.path().to_path_buf();
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.folder.read_only = Some(to.clone());
    })?;
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
    for at in 0..22 {
        for before in [true, false] {
            for kept in [common::Kept::All, common::Kept::Durable(None)] {
                let case = format!("at {at}, before {before}, {kept:?}");
                let server = server();
                let mut a = Client::new(&server, "a")?;
                a.write("Docs/report.txt", "quarterly")?;
                a.write("top.txt", "top")?;
                a.sync()?;
                let out = tempfile::tempdir()?;
                let to = out.path().to_path_buf();
                let mut b = Client::with_config(&server, "b", move |cfg| {
                    cfg.folder.read_only = Some(to.clone());
                })?;
                b.sync()?;
                let drive = versions(&server);
                b.write("Docs/report.txt", "mine")?;
                b.write("New/x.txt", "x")?;
                b.write("top.txt", "edited")?;
                // Every commit of the sync (21 of them) crashes once.
                let crashed = common::crash(&mut b, kept, move |now, n| {
                    n == at && (now == Commit::Before) == before
                })?;
                assert!(crashed || at > 20, "{case}: the sync has fewer commits now");
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
    }
    Ok(())
}
