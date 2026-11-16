//! Another identity's drive (sync semantics §13), end to end: a member's read-only folder of
//! what the drive's owner shares with it. An item whose folder isn't shared is at the top, a
//! share withdrawn takes its item off this device without its file being gone, and nothing is
//! written to the drive.

use hbsync_remote::contract::Shares;

use super::*;

type Server = Arc<Mutex<hbsync_remote::fake::FakeServer>>;

/// The owner `a` shares what is named `name` on its drive.
fn share(a: &Client, server: &Server, name: &str) -> R {
    a.remote.share(common::header_named(server, name).file_id)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok(())
}

/// The owner `a` withdraws its share of what is named `name` on its drive.
fn withdraw(a: &Client, server: &Server, name: &str) -> R {
    a.remote
        .withdraw(common::header_named(server, name).file_id)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok(())
}

/// The owner `a` shares all its drive has.
fn share_all(a: &Client, server: &Server) -> R {
    let all: Vec<_> = {
        let s = server.lock().map_err(|_| "poisoned")?;
        s.live().map(|h| h.file_id).collect()
    };
    for file_id in all {
        a.remote.share(file_id)?;
    }
    Ok(())
}

/// Nothing was moved out of the member's folder.
fn nothing_moved_out(out: &tempfile::TempDir) -> R {
    assert!(out.path().read_dir()?.next().is_none(), "nothing moved out");
    Ok(())
}

/// A file shared without its folder is at the top; its folder shared later takes it in, with
/// what else is shared in it, and the folder's share withdrawn leaves them at the top again.
#[test]
fn an_item_is_at_the_top_while_its_folder_is_not_shared() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("Docs/sub/deep.txt", "deep")?;
    a.write("Docs/private.txt", "mine")?;
    a.sync()?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));

    share(&a, &server, "report.txt")?;
    share(&a, &server, "deep.txt")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["deep.txt = deep", "report.txt = quarterly"]
    );

    share(&a, &server, "Docs")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["Docs/", "Docs/report.txt = quarterly", "deep.txt = deep"]
    );

    share(&a, &server, "sub")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "Docs/",
            "Docs/report.txt = quarterly",
            "Docs/sub/",
            "Docs/sub/deep.txt = deep"
        ]
    );

    withdraw(&a, &server, "Docs")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["report.txt = quarterly", "sub/", "sub/deep.txt = deep"]
    );
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    nothing_moved_out(&out)?;
    Ok(())
}

/// A share withdrawn takes the file off the member's device; given again, the file is back:
/// it was never gone.
#[test]
fn a_share_given_again_brings_the_file_back() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("top.txt", "top")?;
    a.write("stay.txt", "stay")?;
    a.sync()?;
    share(&a, &server, "top.txt")?;
    share(&a, &server, "stay.txt")?;
    let (mut b, _out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay", "top.txt = top"]);

    withdraw(&a, &server, "top.txt")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay"]);

    // Changed while not shared: the member hears of it (a tombstone again), and has nothing.
    a.write("top.txt", "newer")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay"]);

    share(&a, &server, "top.txt")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay", "top.txt = newer"]);
    Ok(())
}

/// Two items of one name, each shared without its folder: one is at the top, the other left
/// out until its folder is shared.
#[test]
fn a_name_taken_at_the_top_leaves_the_other_item_out() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("A/x.txt", "a's")?;
    a.write("B/x.txt", "b's")?;
    a.sync()?;
    share(&a, &server, "A")?;
    let (mut b, _out) = Client::member(&server, "b")?;
    share_all(&a, &server)?;
    withdraw(&a, &server, "A")?;
    withdraw(&a, &server, "B")?;
    b.sync()?;
    let tree = b.tree_text("");
    assert!(
        tree == ["x.txt = a's"] || tree == ["x.txt = b's"],
        "{tree:?}"
    );

    share(&a, &server, "A")?;
    share(&a, &server, "B")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["A/", "A/x.txt = a's", "B/", "B/x.txt = b's"]
    );
    Ok(())
}

/// A file synced in a shared folder is at the top once the folder's share is withdrawn, where
/// its name is taken by an item synced there: the folder leaves this device with the file (no
/// copy of it stays behind in a folder that is no longer here), the item at the top keeps its
/// name, and the file is told as left out at every sync while the clash lasts (never idle
/// then: sync semantics §13). The share given again brings both back.
#[test]
fn a_folder_no_longer_shared_leaves_with_a_file_whose_name_is_taken_at_the_top() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("D/a.txt", "in D")?;
    a.write("a.txt", "top")?;
    a.sync()?;
    share_all(&a, &server)?;
    let (mut b, _out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["D/", "D/a.txt = in D", "a.txt = top"]);

    withdraw(&a, &server, "D")?;
    for _ in 0..3 {
        let report = b.engine.sync_once("2026-10-08")?;
        assert_eq!(b.tree_text(""), ["a.txt = top"]);
        assert!(report.failed.is_empty(), "{report:?}");
        assert!(
            matches!(
                report.conflicts[..],
                [hbsync_engine::Conflict::NameTaken { .. }, ..]
            ),
            "{report:?}"
        );
        assert!(report.moved_out.is_empty(), "{report:?}");
    }

    share(&a, &server, "D")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["D/", "D/a.txt = in D", "a.txt = top"]);
    Ok(())
}

/// A member set up again over a folder that holds its files adopts them where they are: a
/// file whose folder isn't shared at the top, one in a shared folder there. Nothing is
/// downloaded.
#[test]
fn a_member_set_up_again_adopts_a_file_at_the_top() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("Pics/cat.jpg", "meow")?;
    a.sync()?;
    share(&a, &server, "report.txt")?;
    share(&a, &server, "Pics")?;
    share(&a, &server, "cat.jpg")?;

    let (mut b, out) = Client::member(&server, "b")?;
    b.write("report.txt", "quarterly")?;
    b.write("Pics/cat.jpg", "meow")?;
    let report = b.sync()?;
    assert_eq!(b.remote.faults().served, 0, "nothing was downloaded");
    assert_eq!(report.adopted, 3, "the folder and both files: {report:?}");
    assert_eq!(
        b.tree_text(""),
        ["Pics/", "Pics/cat.jpg = meow", "report.txt = quarterly"]
    );
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    nothing_moved_out(&out)?;
    Ok(())
}

/// The sharer's delete for good is final, as on any drive (its tombstone keeps its version):
/// the file is known gone here. A share withdrawn takes the file off this device too, and
/// records nothing: the file isn't gone.
#[test]
fn a_delete_by_the_sharer_is_final_and_a_withdrawal_is_not() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("gone.txt", "gone")?;
    a.write("held.txt", "held")?;
    a.sync()?;
    share(&a, &server, "gone.txt")?;
    share(&a, &server, "held.txt")?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["gone.txt = gone", "held.txt = held"]);

    let held = common::header_named(&server, "held.txt").file_id;
    let gone = common::header_named(&server, "gone.txt").at();
    withdraw(&a, &server, "held.txt")?;
    {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        s.delete(gone.file_id, gone.version)?;
        s.tick();
    }
    b.sync()?;
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    assert!(b.engine.db().read(|s| s.file_gone(gone.file_id))?);
    assert!(!b.engine.db().read(|s| s.file_gone(held))?);
    nothing_moved_out(&out)?;
    Ok(())
}

/// A shared folder the sharer deletes (in their Recently deleted) leaves this device with what
/// is in it, as on any device; put back, it is here again. While it is deleted, its share
/// withdrawn leaves a file of it that is still shared at the top: a known limit (§13), as
/// the picture no longer has the folder to hold it.
#[test]
fn a_folder_the_sharer_deletes_takes_along_what_is_shared_in_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("keep.txt", "keep")?;
    a.sync()?;
    share_all(&a, &server)?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["Docs/", "Docs/report.txt = quarterly", "keep.txt = keep"]
    );

    remove_on_drive(&server, &|name| name == "Docs")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = keep"]);
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());

    withdraw(&a, &server, "Docs")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["keep.txt = keep", "report.txt = quarterly"]
    );

    share(&a, &server, "Docs")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = keep"]);
    nothing_moved_out(&out)?;
    Ok(())
}
