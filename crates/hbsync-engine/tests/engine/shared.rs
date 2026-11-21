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

/// The owner `a` shares what is named `name` on its drive for `ms` from now, by the server's
/// clock.
fn share_for(a: &Client, server: &Server, name: &str, ms: i64) -> R {
    let until = server.lock().map_err(|_| "poisoned")?.now() + ms;
    a.remote
        .share_until(common::header_named(server, name).file_id, until)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok(())
}

/// A minute of the server's clock, which a share's end is asked by.
const MINUTE: i64 = 60_000;
const DAY: i64 = 24 * 60 * MINUTE;

/// The server's clock moves on by `ms`.
fn later(server: &Server, ms: i64) -> R {
    server.lock().map_err(|_| "poisoned")?.advance(ms);
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

/// By when `c` reads its drive's feed from its beginning next, by the server's clock.
fn reread_due(c: &mut Client) -> Result<i64, Box<dyn std::error::Error>> {
    Ok((c.engine.db().read(|s| s.reread_due())?).ok_or("no time kept")?)
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

/// A share given another end keeps the file's version, and a notice of the file says no end.
/// So on another identity's drive a notice of a live file is read from the feed, though the
/// store knows the version: the header there gives the new end
/// (`an_end_made_earlier_is_learned_by_the_read_another_end_starts`). The owner's own device
/// takes the same notice without a read (its own writes coming back).
#[test]
fn a_notice_of_a_shared_file_reads_the_feed_for_its_end() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("top.txt", "top")?;
    a.sync()?;
    share_for(&a, &server, "top.txt", 60 * MINUTE)?;
    let (mut b, _out) = Client::member(&server, "b")?;
    b.sync()?;

    // The share ends sooner now: the file is as it was.
    let before = common::header_named(&server, "top.txt");
    share_for(&a, &server, "top.txt", 10 * MINUTE)?;
    let now = common::header_named(&server, "top.txt");
    assert_eq!(now.version, before.version, "a share keeps the version");
    let notice = hbsync_remote::ChangeRef::of(&now);
    assert!(
        !b.engine.drive().announced(&[notice])?,
        "a version known, but the end isn't: read the feed"
    );
    assert!(a.engine.drive().announced(&[notice])?, "the owner's: known");
    Ok(())
}

/// When another identity's drive is read from its beginning next (a share's end, the week)
/// is by their server's clock, and nothing announces it: the drive says when that is by this
/// computer's clock, as each pull leaves it. A read from the beginning left open (a file
/// said "not there" once) is read on about a minute later, when the second saying can take
/// the file off: without that read, nothing would. An identity's own drive has no such time.
#[test]
fn a_shares_drive_says_when_it_is_next_read_from_its_beginning() -> R {
    use std::time::{Duration, Instant};

    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("top.txt", "top")?;
    a.write("gone.txt", "gone")?;
    a.sync()?;
    share(&a, &server, "top.txt")?;
    share(&a, &server, "gone.txt")?;
    let (mut b, _out) = Client::member(&server, "b")?;
    assert_eq!(b.engine.drive().reread_at(), None, "not read yet");
    // `ms` from now, give or take the minute a time is kept to and the test's own time.
    let in_about = |b: &Client, ms: i64| {
        let left = b
            .engine
            .drive()
            .reread_at()
            .map(|at| at.saturating_duration_since(Instant::now()));
        let (least, most) = ((ms - 10_000) as u64, (ms + MINUTE + 1_000) as u64);
        let fits = left.is_some_and(|left| {
            (Duration::from_millis(least)..=Duration::from_millis(most)).contains(&left)
        });
        assert!(fits, "{left:?}, not about {ms} ms");
    };
    b.sync()?;
    in_about(&b, 7 * DAY);

    share_for(&a, &server, "top.txt", 60 * MINUTE)?;
    b.sync()?;
    in_about(&b, 60 * MINUTE);
    // Their clock moved on: the next pull says what is left.
    later(&server, 50 * MINUTE)?;
    b.sync()?;
    in_about(&b, 10 * MINUTE);
    // Over: read from its beginning, and the week is what is kept.
    later(&server, 11 * MINUTE)?;
    b.sync()?;
    in_about(&b, 7 * DAY);

    // The week's read finds a file deleted for good "not there", once: read on in a
    // minute, not in a week. Said a second time, the file goes, and the week is kept.
    let h = common::header_named(&server, "gone.txt");
    (server.lock().map_err(|_| "poisoned")?).hard_delete(h.file_id, h.version)?;
    later(&server, 7 * DAY + 2 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["gone.txt = gone"], "not there once: kept");
    in_about(&b, MINUTE);
    b.a_minute_on()?;
    b.sync()?;
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    in_about(&b, 7 * DAY);

    assert_eq!(a.engine.drive().reread_at(), None, "an own drive");
    Ok(())
}

/// A share given until a time ends as the time passes. The drive writes nothing then (its
/// feed doesn't move), so the member asks: it kept the time its header gave, and reads the
/// feed from its beginning once the server's clock is past it, which shows the file as one no
/// longer shared. It leaves this device as a share withdrawn does; what is shared without an
/// end stays; nothing is asked before the time; and the share given again brings it back.
#[test]
fn a_share_that_reached_its_end_takes_the_file_off_this_device() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("stay.txt", "stay")?;
    a.write("until.txt", "until")?;
    a.write("dir/in.txt", "in")?;
    a.sync()?;
    share(&a, &server, "stay.txt")?;
    for name in ["until.txt", "dir", "in.txt"] {
        share_for(&a, &server, name, 10 * MINUTE)?;
    }
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    let all = [
        "dir/",
        "dir/in.txt = in",
        "stay.txt = stay",
        "until.txt = until",
    ];
    assert_eq!(b.tree_text(""), all);

    // Before the time all is as it was.
    later(&server, 5 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), all);

    // Past it (nothing was written to the drive): the member's next sync finds them gone.
    later(&server, 6 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay"]);
    nothing_moved_out(&out)?;

    // Given again, without an end: back, and it stays.
    share(&a, &server, "until.txt")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay", "until.txt = until"]);
    later(&server, 60 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay", "until.txt = until"]);
    Ok(())
}

/// An end a share is given later is learned, though the file's version is the one the member
/// has: the file is in the feed again, and its header is read for until when. So is one given
/// anew after the last one passed, before the member looked.
#[test]
fn an_end_given_to_a_share_later_is_learned() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("doc.txt", "doc")?;
    a.sync()?;
    share(&a, &server, "doc.txt")?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["doc.txt = doc"]);

    // Shared without an end so far: now until a time.
    share_for(&a, &server, "doc.txt", 10 * MINUTE)?;
    b.sync()?;
    later(&server, 11 * MINUTE)?;
    // It ended, and is given again until a later time before the member looks.
    share_for(&a, &server, "doc.txt", 10 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["doc.txt = doc"]);
    later(&server, 11 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), Vec::<String>::new());
    nothing_moved_out(&out)
}

/// An end changed to an earlier one isn't lost where another share's end is what the member
/// finds first: reading the feed from its beginning reads every header, and with it every
/// end as it is now.
#[test]
fn an_end_made_earlier_is_learned_by_the_read_another_end_starts() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("long.txt", "long")?;
    a.write("short.txt", "short")?;
    a.sync()?;
    share_for(&a, &server, "long.txt", 30 * MINUTE)?;
    share_for(&a, &server, "short.txt", 5 * MINUTE)?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["long.txt = long", "short.txt = short"]);

    // The long one is to end sooner; before the member looks, the short one ends.
    share_for(&a, &server, "long.txt", 10 * MINUTE)?;
    later(&server, 6 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["long.txt = long"]);
    later(&server, 5 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), Vec::<String>::new());
    nothing_moved_out(&out)
}

/// A file whose newest version this client can't read still ends with its share: the row
/// that doesn't read says until when, so the end isn't lost where another share's end has
/// the feed read from its beginning (which reads every end anew).
#[test]
fn a_file_that_doesnt_read_any_more_still_ends_with_its_share() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("odd.txt", "odd")?;
    a.write("short.txt", "short")?;
    a.sync()?;
    share_for(&a, &server, "odd.txt", 30 * MINUTE)?;
    share_for(&a, &server, "short.txt", 5 * MINUTE)?;
    let (mut b, _out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["odd.txt = odd", "short.txt = short"]);

    // Its rows don't read from now on; the member keeps what it has of it.
    let odd = common::header_named(&server, "odd.txt").file_id;
    server
        .lock()
        .map_err(|_| "poisoned")?
        .set_unreadable(odd, true);
    later(&server, 6 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["odd.txt = odd"]);
    // Its own end, learned again from the row that doesn't read.
    later(&server, 25 * MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), Vec::<String>::new());
    Ok(())
}

/// What the sharer deleted for good leaves the member's folder within a week. The drive
/// tells a member nothing of such a file (no row in its feed, no notice), so the member reads
/// the feed from its beginning a week after it last did and asks about each file it has that
/// the feed no longer shows: not there, said twice a minute apart (§8b), and it goes. Before
/// the week nothing is asked, and after the re-read the feed is read as before.
#[test]
fn what_the_sharer_deleted_for_good_leaves_a_week_on() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("gone.txt", "gone")?;
    a.write("stay.txt", "stay")?;
    a.write("dir/in.txt", "in")?;
    a.sync()?;
    share_all(&a, &server)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    let all = [
        "dir/",
        "dir/in.txt = in",
        "gone.txt = gone",
        "stay.txt = stay",
    ];
    assert_eq!(b.tree_text(""), all);
    let first = reread_due(&mut b)?;

    // Another app deletes a file for good, and a folder with what is in it (as the sharer's
    // purge does): nothing in the feed says so.
    for name in ["gone.txt", "in.txt", "dir"] {
        let h = common::header_named(&server, name);
        (server.lock().map_err(|_| "poisoned")?).hard_delete(h.file_id, h.version)?;
    }
    later(&server, 6 * DAY)?;
    let asked = b.remote.faults().requests;
    b.sync()?;
    assert_eq!(b.tree_text(""), all, "not a week yet");
    assert_eq!(reread_due(&mut b)?, first);
    assert_eq!(b.remote.faults().requests, asked + 1, "the feed's end only");

    later(&server, DAY + MINUTE)?;
    b.sync()?;
    assert_eq!(b.tree_text(""), all, "not there once: kept");
    // (A time is kept as the whole minute it is over by: a tick of the server's clock
    // between the two reads would make it the next.)
    let a_week_on = 7 * DAY + MINUTE..=7 * DAY + 2 * MINUTE;
    let second = reread_due(&mut b)?;
    assert!(
        a_week_on.contains(&(second - first)),
        "read whole: again a week on"
    );
    assert_eq!(b.a_minute_on()?, 3);
    b.sync()?;
    assert_eq!(b.tree_text(""), ["stay.txt = stay"]);
    nothing_moved_out(&out)?;

    // Read as before from then on, until the next week is over.
    let asked = b.remote.faults().requests;
    b.sync()?;
    assert_eq!(b.remote.faults().requests, asked + 1);
    later(&server, 7 * DAY - MINUTE)?;
    b.sync()?;
    assert_eq!(reread_due(&mut b)?, second);
    later(&server, 2 * MINUTE)?;
    b.sync()?;
    let third = reread_due(&mut b)?;
    assert!(a_week_on.contains(&(third - second)));
    assert_eq!(b.tree_text(""), ["stay.txt = stay"]);

    // A store that keeps no such time (one from before) reads it whole once.
    (b.engine.db()).write(|s| {
        s.account()
            .delete_account_kv(hbsync_state::keys::REREAD_DUE)
    })?;
    let asked = b.remote.faults().requests;
    b.sync()?;
    assert_eq!(reread_due(&mut b)?, third);
    assert_eq!(b.remote.faults().requests, asked + 2, "from the beginning");
    b.sync()?;
    assert_eq!(b.remote.faults().requests, asked + 3, "once");
    Ok(())
}

/// An identity's own drive is never read whole again for a week gone by: it hears of every
/// delete for good (its notices; the feed's trace of the removal before it).
#[test]
fn an_own_drive_is_not_read_whole_again_a_week_on() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    let due = |a: &mut Client| a.engine.db().read(|s| s.reread_due());
    assert_eq!(due(&mut a)?, None, "no time is kept for it");
    later(&server, 8 * DAY)?;
    let asked = a.remote.faults().requests;
    a.sync()?;
    assert_eq!(a.remote.faults().requests, asked + 1, "the feed's end only");
    assert_eq!(due(&mut a)?, None);
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

/// A file renamed on the drive to a name taken at the member's top keeps its old name here,
/// and a new file of that old name waits for it in turn (the name is held on this device):
/// each has its name once the first one's is free.
#[test]
fn a_new_file_waits_for_the_name_a_waiting_one_holds() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("D/x.txt", "in D")?;
    a.write("y.txt", "old")?;
    a.sync()?;
    share(&a, &server, "x.txt")?;
    share(&a, &server, "y.txt")?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["x.txt = in D", "y.txt = old"]);

    fs::rename(a.root.join("y.txt"), a.root.join("x.txt"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["x.txt = in D", "y.txt = old"]);
    a.write("y.txt", "new")?;
    a.sync()?;
    share(&a, &server, "y.txt")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["x.txt = in D", "y.txt = old"]);

    share(&a, &server, "D")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["D/", "D/x.txt = in D", "x.txt = old", "y.txt = new"]
    );
    nothing_moved_out(&out)
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

/// A member's daily purge deletes nothing on the sharer's drive: what is in their Recently
/// deleted and the tombstones of their deletes are their own devices' to delete for good.
/// It only forgets, a keeping time later, the files it knew gone, and it succeeds (a purge
/// that tried would be refused every day).
#[test]
fn a_members_purge_deletes_nothing_on_the_sharers_drive() -> R {
    const LATER: i64 = hbsync_engine::time::TRACE_MS + 24 * 60 * 60 * 1000;
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("removed.txt", "removed")?;
    a.write("deleted.txt", "deleted")?;
    a.write("keep.txt", "keep")?;
    a.sync()?;
    share_all(&a, &server)?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;

    let removed = common::header_named(&server, "removed.txt").file_id;
    let deleted = common::header_named(&server, "deleted.txt").at();
    remove_on_drive(&server, &|name| name == "removed.txt")?;
    {
        let mut s = server.lock().map_err(|_| "poisoned")?;
        s.delete(deleted.file_id, deleted.version)?;
        s.tick();
    }
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = keep"]);
    b.engine.db().read(|s| {
        assert!(s.file_gone(deleted.file_id)?);
        assert!(s.tombstones_before(i64::MAX)?.is_empty(), "the sharer's");
        Ok(())
    })?;

    server.lock().map_err(|_| "poisoned")?.advance(LATER);
    assert_eq!(b.engine.drive().purge(0, Going::UNWATCHED)?, 0);
    assert_eq!(b.engine.drive().purge_when_due(Going::UNWATCHED)?, Some(0));
    assert_eq!(b.engine.drive().purge_when_due(Going::UNWATCHED)?, None);
    {
        let s = server.lock().map_err(|_| "poisoned")?;
        assert!(s.removed().any(|h| h.file_id == removed), "still theirs");
        assert!(s.header(deleted.file_id)?.is_deleted(), "its tombstone too");
    }
    assert!(!b.engine.db().read(|s| s.file_gone(deleted.file_id))?);
    assert_eq!(b.tree_text(""), ["keep.txt = keep"]);
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    nothing_moved_out(&out)?;
    Ok(())
}

/// What the sharer deleted is theirs to restore or to delete for good: a member's restore and
/// its emptying of Recently deleted are refused before anything is asked of the drive.
#[test]
fn a_member_neither_restores_nor_empties_the_sharers_recently_deleted() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("removed.txt", "removed")?;
    a.write("keep.txt", "keep")?;
    a.sync()?;
    share_all(&a, &server)?;
    let (mut b, out) = Client::member(&server, "b")?;
    b.sync()?;
    let (removed, file) = {
        let header = common::header_named(&server, "removed.txt");
        (header.node().ok_or("a node")?, header.file_id)
    };
    remove_on_drive(&server, &|name| name == "removed.txt")?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["keep.txt = keep"]);

    let listed = b.listed()?;
    let asked = b.remote.faults().requests;
    let drive = b.engine.drive();
    let refused = [
        drive.restore(&[removed]),
        drive.empty(&[removed], &listed, Going::UNWATCHED),
        drive.empty_listed(&listed, Going::UNWATCHED),
    ];
    for done in refused {
        assert!(
            matches!(done, Err(hbsync_engine::EngineError::ReadOnly(_))),
            "{done:?}"
        );
    }
    assert_eq!(b.remote.faults().requests, asked, "nothing asked");
    assert!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .removed()
            .any(|h| h.file_id == file),
        "still in the sharer's Recently deleted"
    );
    nothing_moved_out(&out)?;
    Ok(())
}

/// What another identity shares only syncs read-only: a folder of it that would sync both ways
/// isn't opened (each change here would be a write the drive refuses).
#[test]
fn a_shared_drive_is_not_synced_both_ways() -> R {
    let server = server();
    match Client::on(FakeRemote::member(server.clone()), "b", |_| {}) {
        Err(e) if e.to_string().contains("only syncs read-only") => Ok(()),
        Err(e) => Err(e),
        Ok(_) => Err("opened".into()),
    }
}

/// A share that isn't synced here any more (it ended, or its marker turned it off) has its
/// copy taken off this device as a share withdrawn does it, all at once, with nothing read
/// from the drive or said to it: what is here as it was synced is deleted (however much:
/// the mass-delete guard doesn't hold it, as nothing is deleted for anyone), what was
/// changed or put here moves out, what a link leads to is left alone, and no file is
/// recorded gone: synced again later, all of it comes back.
#[test]
fn a_share_that_ended_takes_its_copy_off_this_device() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for i in 0..30 {
        a.write(&format!("Docs/n{i:02}.txt"), "as shared")?;
    }
    a.write("Docs/edited.txt", "theirs")?;
    a.write("top.txt", "at the top")?;
    a.sync()?;
    share_all(&a, &server)?;
    server.lock().map_err(|_| "poisoned")?.tick();
    // (The guard would hold these: few files allowed.)
    let out = tempfile::tempdir()?;
    let to = out.path().to_path_buf();
    let mut b = Client::on(
        hbsync_remote::fake::FakeRemote::member(server.clone()),
        "b",
        move |cfg| {
            cfg.folder.read_only = Some(to.clone());
            (cfg.guard.min, cfg.guard.fraction) = (2, 0.0);
        },
    )?;
    b.sync()?;
    // (The files, and their folder.)
    assert_eq!(b.tree_text("").len(), 33);
    let on_drive = |server: &Server| server.lock().map(|s| s.live().count()).unwrap_or(0);
    let before = on_drive(&server);

    // Changed here, put here, and a link out of the folder: none of them seen by a scan yet.
    b.write("Docs/edited.txt", "mine now")?;
    b.write("Docs/new.txt", "put here")?;
    let elsewhere = tempfile::tempdir()?;
    std::fs::write(elsewhere.path().join("kept.txt"), "not ours")?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(elsewhere.path(), b.root.join("Docs/link"))?;
    // A folder on the junk list, with something in it: the user's, to move out (a junk
    // file goes with its folder).
    std::fs::create_dir_all(b.root.join("Docs/.TemporaryItems"))?;
    std::fs::write(b.root.join("Docs/.TemporaryItems/draft"), "never synced")?;
    std::fs::write(b.root.join("Docs/.DS_Store"), "junk")?;
    // And a folder made here, with a link of its own: it is to leave whole.
    b.write("made here/notes.txt", "the user's own")?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(elsewhere.path(), b.root.join("made here/inner link"))?;

    let report = b.engine.end_share("2026-11-20")?;
    assert!(report.held.is_empty(), "nothing held: {:?}", report.held);
    assert!(!report.unfinished && !report.stopped);
    // Nothing is left; what was the user's moved out: the two files, the link as a link,
    // and the folder made here whole, with the link in it where it was.
    let left = b.tree_text("");
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(report.moved_out.len(), 4 + usize::from(cfg!(unix)));
    let draft = out.path().join(".TemporaryItems/draft");
    assert_eq!(std::fs::read_to_string(draft)?, "never synced");
    #[cfg(unix)]
    {
        assert!(out.path().join("link").symlink_metadata()?.is_symlink());
        let inner = out.path().join("made here/inner link");
        assert!(inner.symlink_metadata()?.is_symlink());
    }
    let kept = std::fs::read_to_string(out.path().join("made here/notes.txt"))?;
    assert_eq!(kept, "the user's own");
    let moved: Vec<String> = out
        .path()
        .read_dir()?
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|kind| kind.is_file()))
        .filter_map(|e| std::fs::read_to_string(e.path()).ok())
        .collect();
    assert!(moved.iter().any(|text| text == "mine now"), "{moved:?}");
    assert!(moved.iter().any(|text| text == "put here"), "{moved:?}");
    assert_eq!(moved.len(), 2, "{moved:?}");
    // A stop asked before it began: nothing is touched.
    b.sync()?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(true));
    b.engine.set_stop(Arc::clone(&stop));
    let files = b.tree_text("").len();
    let cut = b.engine.end_share("2026-11-20")?;
    assert!(cut.stopped && cut.ops_done == 0);
    assert_eq!(b.tree_text("").len(), files);
    // (Nor its picture: the files are still known as on the drive.)
    let known = common::header_named(&server, "top.txt").file_id;
    assert!(b.engine.db().read(|s| s.known_file(known))?.is_some());
    stop.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        std::fs::read_to_string(elsewhere.path().join("kept.txt"))?,
        "not ours"
    );
    // The drive is as it was, and none of its files is taken for gone.
    assert_eq!(on_drive(&server), before);
    let top = common::header_named(&server, "top.txt").file_id;
    assert!(!b.engine.db().read(|s| s.file_gone(top))?);

    // Synced again (its marker turned on again): all of it comes back.
    b.sync()?;
    let back = b.tree_text("");
    assert_eq!(
        back.iter().filter(|l| l.contains(".txt")).count(),
        32,
        "{back:?}"
    );
    Ok(())
}

/// Only what another identity shares is ended so: an identity's own folder is refused, and
/// nothing in it is touched.
#[test]
fn ending_a_share_is_refused_for_an_identitys_own_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("mine.txt", "mine")?;
    a.sync()?;
    assert!(matches!(
        a.engine.end_share("2026-11-20"),
        Err(hbsync_engine::EngineError::NotAShare)
    ));
    // (Read-only, but its own drive: refused too.)
    let (mut own, _out) = Client::read_only(&server, "b")?;
    own.sync()?;
    assert!(matches!(
        own.engine.end_share("2026-11-20"),
        Err(hbsync_engine::EngineError::NotAShare)
    ));
    assert_eq!(a.tree_text(""), ["mine.txt = mine"]);
    assert_eq!(own.tree_text(""), ["mine.txt = mine"]);
    Ok(())
}
