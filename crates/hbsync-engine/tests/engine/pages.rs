//! Planning in pages (§6.5): a folder's delete across pages goes as one.

use super::*;

/// What each device and the drive hold once a delete is followed, as text: b's tree, a's
/// tree, the drive's live names, and a fresh device's tree.
type Outcome = (Vec<String>, Vec<String>, Vec<String>, Vec<String>);

/// A delete made on a, on b or on the drive (the server).
type Delete = dyn Fn(&mut Client, &mut Client, &Arc<Mutex<hbsync_remote::fake::FakeServer>>) -> R;

/// A folder delete that spans planning pages ends as one planned in a page: `delete` deletes
/// on the drive as a (or another app) does, then b syncs in pages of `batch` dirty nodes
/// (review 14 R1: a node dirty in a later page read alive as context, and kept its deleted
/// folder alive, an empty skeleton coming back on every device).
fn delete_in_pages(
    batch: usize,
    write: &dyn Fn(&mut Client) -> R,
    delete: &Delete,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.allow = true;
        cfg.batch = batch;
    })?;
    write(&mut a)?;
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    delete(&mut a, &mut b, &server)?;
    // In pages, a folder waits for what is in it: a sweep a level (and a sync takes so many
    // rounds), so b syncs until nothing is left.
    let settled = |c: &mut Client| -> R {
        for _ in 0..8 {
            if c.sync()?.pending == 0 {
                return Ok(());
            }
        }
        Err("still pending after 8 syncs".into())
    };
    settled(&mut b)?;
    settled(&mut a)?;
    settled(&mut b)?;
    let mut c = Client::with_config(&server, "c", |cfg| cfg.guard.allow = true)?;
    c.sync()?;
    let live = server.lock().map_err(|_| "poisoned")?.live_names();
    Ok((b.tree_text(""), a.tree_text(""), live, c.tree_text("")))
}

/// [`delete_in_pages`] in pages of 1, 2 and 4, against one page.
fn deletes_in_pages_end_as_in_one(write: &dyn Fn(&mut Client) -> R, delete: &Delete) -> R {
    let whole = delete_in_pages(20_000, write, delete)?;
    for batch in [1, 2, 4] {
        assert_eq!(
            delete_in_pages(batch, write, delete)?,
            whole,
            "in pages of {batch}"
        );
    }
    Ok(())
}

fn nested(a: &mut Client) -> R {
    for i in 0..5 {
        a.write(&format!("d/e/f{i}.txt"), "x")?;
        a.write(&format!("d/g{i}.txt"), "y")?;
    }
    Ok(())
}

/// `d1`, `d2` and `d3`, each with `a.txt` and `x.txt`.
fn three_folders(a: &mut Client) -> R {
    for d in ["d1", "d2", "d3"] {
        a.write(&format!("{d}/a.txt"), "a")?;
        a.write(&format!("{d}/x.txt"), "x")?;
    }
    Ok(())
}

/// Another app removes everything on the drive but what `keep` names, folders first, as a
/// deleting client does: what is in a removed folder of those names is taken along, live.
fn remove_on_drive_except(
    server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>,
    keep: &[&str],
) -> R {
    remove_on_drive(server, &|name| !keep.contains(&name))
}

/// Deleted on another device, all of it: b follows it in pages, and nothing comes back.
#[test]
fn a_delete_elsewhere_planned_in_pages_ends_as_in_one() -> R {
    deletes_in_pages_end_as_in_one(&nested, &|a, _, _| {
        fs::remove_dir_all(a.root.join("d"))?;
        a.sync()?;
        Ok(())
    })
}

/// Deleted here, all of it: b sends it in pages, and nothing comes back.
#[test]
fn a_delete_here_planned_in_pages_ends_as_in_one() -> R {
    deletes_in_pages_end_as_in_one(&nested, &|_, b, _| {
        fs::remove_dir_all(b.root.join("d"))?;
        Ok(())
    })
}

/// Another app removes the top folders and what is in them, and stops before `x.txt` in each
/// (taken along, §6.3): b finishes the delete in pages as in one, and no `x.txt` comes back.
#[test]
fn a_stopped_delete_planned_in_pages_ends_as_in_one() -> R {
    deletes_in_pages_end_as_in_one(&three_folders, &|_, _, server| {
        remove_on_drive_except(server, &["x.txt", "keep.txt"])
    })
}

/// b deletes `d` (12 items) here while other dirty items come first in id order, so its delete
/// spans pages. The page holding `d` holds it for what in it is in another page, and the
/// engine plans its subtree as a page of its own: it goes from the drive top-down, as in one
/// page, and Recently deleted shows one item, `d`, restoring all of it (sync semantics §6.5).
/// Too big for a page (4), its folder `e` goes first, as a page of its own, then `d` with what
/// is directly in it, more than a page: planned whole, one larger page. Two items, never one
/// per file. Bottom-up, page by page, every file was "removed before" its folder: listed and
/// restored one by one.
#[test]
fn a_delete_here_in_pages_goes_as_one_to_recently_deleted() -> R {
    let roots = |batch: usize| -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        for i in 0..10 {
            a.write(&format!("a{i}.txt"), "a")?;
        }
        a.sync()?;
        nested(&mut a)?;
        a.sync()?;
        b.sync()?;
        for i in 0..10 {
            b.write(&format!("a{i}.txt"), "edited")?;
        }
        fs::remove_dir_all(b.root.join("d"))?;
        b.sync()?;
        let deleted = b.recently_deleted(100)?;
        let mut roots: Vec<String> = deleted
            .iter()
            .filter(|d| d.root == d.node)
            .map(|d| d.path.clone())
            .collect();
        roots.sort();
        assert_eq!(deleted.len(), 12, "{deleted:?}");
        Ok(roots)
    };
    assert_eq!(roots(20_000)?, ["d"]);
    assert_eq!(roots(12)?, ["d"], "in pages of 12");
    assert_eq!(roots(4)?, ["d", "d/e"], "in pages of 4");
    Ok(())
}

/// b deletes `d` here while a moved `d/m.txt` out of it into a new folder `n`: in pages of
/// one node, `m.txt` goes into `n`, which another page makes, so it waits, and keeps `d`
/// meanwhile. What else b deleted in `d` waits with it, so `d` goes as one, as in one page:
/// Recently deleted lists `d` alone. Before, `d/x.txt` went first, listed apart (§6.5).
#[test]
fn a_folder_kept_for_another_page_goes_as_one_to_recently_deleted() -> R {
    for batch in [1, 2, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        a.write("d/x.txt", "x")?;
        a.write("d/m.txt", "m")?;
        a.sync()?;
        b.sync()?;
        fs::create_dir(a.root.join("n"))?;
        fs::rename(a.root.join("d/m.txt"), a.root.join("n/m.txt"))?;
        a.sync()?;
        fs::remove_dir_all(b.root.join("d"))?;
        let mut pending = u64::MAX;
        for _ in 0..8 {
            pending = b.sync()?.pending;
            if pending == 0 {
                break;
            }
        }
        // The wait settles: nothing left to plan.
        assert_eq!(pending, 0, "pages of {batch}");
        assert_eq!(b.read_text("n/m.txt")?, "m", "pages of {batch}");
        let deleted = b.recently_deleted(100)?;
        let mut roots: Vec<String> = deleted
            .iter()
            .filter(|d| d.root == d.node)
            .map(|d| d.path.clone())
            .collect();
        roots.sort();
        assert_eq!(roots, ["d"], "pages of {batch}: {deleted:?}");
    }
    Ok(())
}

/// b swaps folder `p` and folder `q` in it here: `q` out to the top, then `p` into it. In pages
/// of one node `p` is planned with `q` where the drive has it (inside `p`): a cycle, which
/// another page's node is in, so `p` waits to be planned with it, and both moves go, as in
/// one page. Before, the cycle's repair undid the user's move of `p` (§6.5). `wipe`: b also
/// deletes the other folders, more than the guard lets through, so it holds those deletes for
/// the user: the wait settles all the same, a sync planning nothing once the hold does.
fn a_swap_planned_in_pages(wipe: bool) -> R {
    for batch in [1, 2, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            if wipe {
                cfg.guard.min = 2;
            } else {
                cfg.guard.allow = true;
            }
            cfg.batch = batch;
        })?;
        a.write("p/q/x.txt", "x")?;
        if wipe {
            three_folders(&mut a)?;
        }
        a.sync()?;
        b.sync()?;
        if wipe {
            for d in ["d1", "d2", "d3"] {
                fs::remove_dir_all(b.root.join(d))?;
            }
        }
        fs::rename(b.root.join("p/q"), b.root.join("q"))?;
        fs::rename(b.root.join("p"), b.root.join("q/p"))?;
        let mut syncs = Vec::new();
        for _ in 0..8 {
            let report = b.sync()?;
            syncs.push((report.rounds, report.ops_done, report.held.len()));
        }
        assert_eq!(b.read_text("q/x.txt")?, "x", "pages of {batch}");
        assert!(
            b.root.join("q/p").is_dir(),
            "pages of {batch}: {:?}",
            b.tree_text("")
        );
        let live = server.lock().map_err(|_| "poisoned")?.live_names();
        assert!(
            live.contains(&"q".to_owned()) && live.contains(&"p".to_owned()),
            "pages of {batch}: {live:?}"
        );
        let mut c = Client::with_config(&server, "c", |cfg| cfg.guard.allow = true)?;
        c.sync()?;
        assert!(
            c.root.join("q/p").is_dir(),
            "pages of {batch}: {:?}",
            c.tree_text("")
        );
        // Settled: the last syncs plan nothing (with a hold: it stays, shown).
        assert!(
            syncs[4..]
                .iter()
                .all(|&(rounds, ops, held)| rounds == 0 && ops == 0 && (held > 0) == wipe),
            "pages of {batch}: (rounds, ops, held) a sync: {syncs:?}"
        );
    }
    Ok(())
}

#[test]
fn a_swap_of_a_folder_and_the_one_in_it_planned_in_pages() -> R {
    a_swap_planned_in_pages(false)
}

#[test]
fn a_swap_planned_in_pages_beside_held_deletes_settles() -> R {
    a_swap_planned_in_pages(true)
}

/// Another app removes `r` on the drive and stops there (`r/k.txt` taken along, live). b moves
/// `d/m.txt` into `r` (which brings `r` back) and deletes `d`. In pages of one node, `m.txt`
/// waits for `r`'s page of its own and keeps `d` meanwhile: what else b deleted in `d` waits
/// with it, so `d` goes as one, as in one page, and Recently deleted lists no file of it
/// apart. Before, `d/x.txt` went first, listed apart (§6.5).
#[test]
fn a_folder_kept_for_a_folders_own_page_goes_as_one_to_recently_deleted() -> R {
    for batch in [1, 2, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        a.write("d/x.txt", "x")?;
        a.write("d/m.txt", "m")?;
        a.write("r/k.txt", "k")?;
        a.sync()?;
        b.sync()?;
        remove_on_drive(&server, &|name| name == "r")?;
        fs::rename(b.root.join("d/m.txt"), b.root.join("r/m.txt"))?;
        fs::remove_dir_all(b.root.join("d"))?;
        let mut pending = u64::MAX;
        for _ in 0..8 {
            pending = b.sync()?.pending;
            if pending == 0 {
                break;
            }
        }
        assert_eq!(pending, 0, "pages of {batch}");
        assert_eq!(b.read_text("r/m.txt")?, "m", "pages of {batch}");
        let deleted = b.recently_deleted(100)?;
        let apart: Vec<&str> = deleted
            .iter()
            .filter(|d| d.root == d.node && d.path.starts_with("d/"))
            .map(|d| d.path.as_str())
            .collect();
        assert!(apart.is_empty(), "pages of {batch}: {deleted:?}");
    }
    Ok(())
}

/// Another app removes every top folder and what is in them, and stops before `x.txt` in each
/// (taken along, §6.3): every synced file goes, so b holds the removal for the user, in pages
/// as in one. Counted from the dirty set, the taken-along files were still in the mirror, so a
/// page that deleted its folder alone didn't trip the every-file hold (review 14 R1).
#[test]
fn a_stopped_wipe_planned_in_pages_is_held() -> R {
    for batch in [1, 2, 3, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.min = 2;
            cfg.batch = batch;
        })?;
        three_folders(&mut a)?;
        a.sync()?;
        b.sync()?;
        remove_on_drive_except(&server, &["x.txt"])?;
        let report = b.sync()?;
        assert!(!report.held.is_empty(), "pages of {batch}: {report:?}");
        for d in ["d1", "d2", "d3"] {
            assert_eq!(b.read_text(&format!("{d}/x.txt"))?, "x", "pages of {batch}");
        }
    }
    Ok(())
}

/// As [`a_stopped_wipe_planned_in_pages_is_held`], with `d1/s`, taken along with `s/x.txt`,
/// tagged here: planned in a page without `d1` (a piece of it), it waits for `d1`'s plan
/// (§6.5), which the guard holds.
/// The wait settles with the hold: later syncs plan nothing (no spin), and `x.txt` stays.
#[test]
fn what_a_held_removal_took_along_waits_without_a_spin() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    for batch in [1, 2, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.min = 2;
            cfg.batch = batch;
        })?;
        for d in ["d1", "d2", "d3"] {
            a.write(&format!("{d}/a.txt"), "a")?;
            a.write(&format!("{d}/s/x.txt"), "x")?;
        }
        if !tags::CAN_TAG || tags::write(&a.root.join("d1/s"), &Tags::default())?.is_none() {
            eprintln!("no tags here; skipped");
            return Ok(());
        }
        a.sync()?;
        b.sync()?;
        remove_on_drive_except(&server, &["s", "x.txt"])?;
        tags::write(&b.root.join("d1/s"), &Tags::new([Tag::new("Red", 6)]))?;
        let mut syncs = Vec::new();
        for _ in 0..8 {
            let report = b.sync()?;
            syncs.push((report.rounds, report.ops_done, report.held.len()));
        }
        // Once the hold settles, a sync plans nothing: no wait planned again every sync.
        assert!(
            syncs[4..]
                .iter()
                .all(|&(rounds, ops, held)| rounds == 0 && ops == 0 && held > 0),
            "pages of {batch}: (rounds, ops, held) a sync: {syncs:?}"
        );
        for d in ["d1", "d2", "d3"] {
            assert_eq!(
                b.read_text(&format!("{d}/s/x.txt"))?,
                "x",
                "pages of {batch}"
            );
        }
    }
    Ok(())
}

/// Another app removes `d` on the drive and stops there: `e` and `f1.txt` in it are taken
/// along, live, and not marked (the feed marks the folder). b edits `d/e/f1.txt` and makes
/// `d/e/new.txt` meanwhile, their marks in later pages than `d`'s. The page holding `d` waits
/// for what is below it in other pages, so `d` is planned with its whole subtree: the edits
/// keep it, as in one page. Planned apart, its delete took them along (review 15).
#[test]
fn a_change_deep_in_a_folder_removed_elsewhere_keeps_it_in_pages() -> R {
    for batch in [1, 2, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        a.write("d/e/f1.txt", "f")?;
        a.write("keep.txt", "k")?;
        a.sync()?;
        b.sync()?;
        remove_on_drive(&server, &|name| name == "d")?;
        b.write("d/e/f1.txt", "edited here")?;
        b.write("d/e/new.txt", "new")?;
        for _ in 0..8 {
            if b.sync()?.pending == 0 {
                break;
            }
        }
        assert_eq!(
            b.read_text("d/e/f1.txt")?,
            "edited here",
            "pages of {batch}"
        );
        assert_eq!(b.read_text("d/e/new.txt")?, "new", "pages of {batch}");
        let mut c = Client::with_config(&server, "c", |cfg| cfg.guard.allow = true)?;
        c.sync()?;
        assert_eq!(
            c.read_text("d/e/f1.txt")?,
            "edited here",
            "pages of {batch}"
        );
        assert_eq!(c.read_text("d/e/new.txt")?, "new", "pages of {batch}");
    }
    Ok(())
}

/// Recently deleted's items after b deletes `d` here, in pages of `batch`, its files made
/// at the top before `d` and moved into it (lower ids, so in earlier pages than `d`).
fn roots_of_moved_in(batch: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    let mut b = Client::with_config(&server, "b", move |cfg| {
        cfg.guard.allow = true;
        cfg.batch = batch;
    })?;
    for i in 0..8 {
        a.write(&format!("m{i}.txt"), "m")?;
    }
    a.sync()?;
    b.sync()?;
    a.write("d/z.txt", "z")?;
    for i in 0..8 {
        fs::rename(
            a.root.join(format!("m{i}.txt")),
            a.root.join(format!("d/m{i}.txt")),
        )?;
    }
    a.sync()?;
    b.sync()?;
    fs::remove_dir_all(b.root.join("d"))?;
    for _ in 0..8 {
        if b.sync()?.pending == 0 {
            break;
        }
    }
    let deleted = b.recently_deleted(100)?;
    let mut roots: Vec<String> = deleted
        .iter()
        .filter(|d| d.root == d.node)
        .map(|d| d.path.clone())
        .collect();
    roots.sort();
    Ok(roots)
}

/// What is in a folder deleted here but in earlier pages than it (moved in, older) waits for
/// it: the folder goes as one, one item in Recently deleted, as in one page. Planned apart,
/// each file went alone first, an item of its own, and a restore of `d` brought back an empty
/// folder (review 15).
#[test]
fn what_was_moved_into_a_deleted_folder_goes_with_it_in_pages() -> R {
    for batch in [2, 4, 20_000] {
        assert_eq!(roots_of_moved_in(batch)?, ["d"], "pages of {batch}");
    }
    Ok(())
}

/// [`a_delete_here_in_pages_goes_as_one_to_recently_deleted`] in pages of every size from 3
/// to 13: what of a page is below the folder it waits for waits too, so no file of `d` is
/// ever an item of its own (`d/e` may be, a piece of `d` too big for a page).
#[test]
fn a_delete_in_pages_of_any_size_lists_no_file_apart() -> R {
    for batch in 3..14 {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        for i in 0..10 {
            a.write(&format!("a{i}.txt"), "a")?;
        }
        a.sync()?;
        nested(&mut a)?;
        a.sync()?;
        b.sync()?;
        for i in 0..10 {
            b.write(&format!("a{i}.txt"), "edited")?;
        }
        fs::remove_dir_all(b.root.join("d"))?;
        for _ in 0..8 {
            if b.sync()?.pending == 0 {
                break;
            }
        }
        let deleted = b.recently_deleted(100)?;
        assert_eq!(deleted.len(), 12, "pages of {batch}: {deleted:?}");
        for d in deleted.iter().filter(|d| d.root == d.node) {
            assert!(
                ["d", "d/e"].contains(&d.path.as_str()),
                "pages of {batch}: {deleted:?}"
            );
        }
    }
    Ok(())
}

/// Syncs `c` until nothing is left to do, or 8 times; whether any sync failed an operation.
fn settle(c: &mut Client) -> Result<bool, Box<dyn std::error::Error>> {
    let mut failed = false;
    for _ in 0..8 {
        let report = c.sync()?;
        failed |= !report.failed.is_empty();
        if report.pending == 0 {
            break;
        }
    }
    Ok(failed)
}

/// Another device removes `d` and `q`; b moves `d/n.txt` into `q` and makes `d/new.txt`. The
/// page that waits for `d` holds `n` with the folders it is in now: `q` stays with it, as in
/// one page, never deleted with `n` in it (review 16).
#[test]
fn a_node_moved_out_of_a_waiting_folder_keeps_its_new_one() -> R {
    for batch in [1, 2, 3, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        a.write("d/n.txt", "n")?;
        a.write("q/k.txt", "k")?;
        a.write("keep.txt", "k")?;
        a.sync()?;
        b.sync()?;
        remove_on_drive(&server, &|name| name == "d" || name == "q")?;
        fs::rename(b.root.join("d/n.txt"), b.root.join("q/n.txt"))?;
        b.write("d/new.txt", "new")?;
        settle(&mut b)?;
        assert_eq!(b.read_text("q/n.txt")?, "n", "pages of {batch}");
        assert_eq!(b.read_text("d/new.txt")?, "new", "pages of {batch}");
    }
    Ok(())
}

/// Folder `e`, older than `d`, is moved into it, and b deletes `d`: the page holding `e` waits
/// for `d` alone, whose page covers both, so Recently deleted lists one item, `d`, in pages
/// that hold all of `d` as in one (review 16: `e` went first, an item of its own).
#[test]
fn an_older_folder_moved_into_a_deleted_one_goes_with_it() -> R {
    for batch in [2, 4, 8, 9, 12, 20_000] {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        for i in 0..3 {
            a.write(&format!("e/x{i}.txt"), "x")?;
        }
        a.sync()?;
        b.sync()?;
        for i in 0..3 {
            a.write(&format!("d/y{i}.txt"), "y")?;
        }
        fs::rename(a.root.join("e"), a.root.join("d/e"))?;
        a.sync()?;
        b.sync()?;
        fs::remove_dir_all(b.root.join("d"))?;
        settle(&mut b)?;
        let deleted = b.recently_deleted(100)?;
        let roots: Vec<&str> = deleted
            .iter()
            .filter(|d| d.root == d.node)
            .map(|d| d.path.as_str())
            .collect();
        // `d` has 8 nodes: in smaller pages, `e` may go first, a piece of it.
        let piece = batch < 8 && roots == ["d", "d/e"];
        assert!(roots == ["d"] || piece, "pages of {batch}: {deleted:?}");
    }
    Ok(())
}

/// a makes `B`, then `A`, and moves `B` into `A` and `x.txt` into `B`; b, new, syncs in pages
/// of 1: what goes into a folder held for another page waits with it, so nothing is sent to
/// fail meanwhile (review 16), and b ends with all of it.
#[test]
fn what_goes_into_a_folder_held_for_another_page_waits_too() -> R {
    let server = server();
    let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
    a.write("B/b.txt", "b")?;
    a.write("x.txt", "x")?;
    a.sync()?;
    a.write("A/a.txt", "a")?;
    fs::rename(a.root.join("B"), a.root.join("A/B"))?;
    fs::rename(a.root.join("x.txt"), a.root.join("A/B/x.txt"))?;
    a.sync()?;
    let mut b = Client::with_config(&server, "b", |cfg| {
        cfg.guard.allow = true;
        cfg.batch = 1;
    })?;
    let failed = settle(&mut b)?;
    assert!(!failed, "an operation was sent to fail");
    assert_eq!(b.read_text("A/B/x.txt")?, "x");
    assert_eq!(b.read_text("A/B/b.txt")?, "b");
    Ok(())
}

/// b's delete of what is in `d/s` is held for the user (out of the dirty set), then another
/// device removes `d`: a held delete below `d` doesn't make it wait for a page that never
/// comes (review 16: planned again every sync, forever). In pages as in one.
#[test]
fn a_held_delete_below_a_removed_folder_doesnt_hold_it_for_good() -> R {
    let outcome = |batch: usize| -> Result<(Vec<String>, u64), Box<dyn std::error::Error>> {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.min = 2;
            cfg.batch = batch;
        })?;
        for i in 0..6 {
            a.write(&format!("d/s/f{i}.txt"), "f")?;
        }
        a.write("d/t.txt", "t")?;
        a.write("keep.txt", "k")?;
        a.sync()?;
        b.sync()?;
        for i in 0..6 {
            fs::remove_file(b.root.join(format!("d/s/f{i}.txt")))?;
        }
        let held = b.sync()?;
        assert!(!held.held.is_empty(), "pages of {batch}: {held:?}");
        b.sync()?;
        remove_on_drive(&server, &|name| name == "d")?;
        let mut pending = 0;
        for _ in 0..8 {
            pending = b.sync()?.pending;
        }
        Ok((b.tree_text(""), pending))
    };
    let whole = outcome(20_000)?;
    for batch in [1, 2, 3] {
        assert_eq!(outcome(batch)?, whole, "pages of {batch}");
    }
    Ok(())
}

/// On the drive `X/B/g/z.txt` and `A`; b moves `B` into `A` and deletes `A/B/g` here, while
/// another device edits `z.txt` and deletes `A` on the drive. `A` waits for the page that
/// plans the edit, with what of the page is below it across sides (`g` is in `A` only here):
/// the edit keeps `g` and `z.txt`, in pages as in one (review 17: `g` went, and the edit
/// with it into Recently deleted).
#[test]
fn a_folder_below_a_waiting_one_across_sides_waits_too() -> R {
    let outcome = |batch: usize| -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let server = server();
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
        })?;
        a.write("X/B/g/z.txt", "z")?;
        a.write("A/k.txt", "k")?;
        a.write("keep.txt", "k")?;
        a.sync()?;
        b.sync()?;
        fs::rename(b.root.join("X/B"), b.root.join("A/B"))?;
        fs::remove_dir_all(b.root.join("A/B/g"))?;
        a.write("X/B/g/z.txt", "edited")?;
        a.sync()?;
        fs::remove_dir_all(a.root.join("A"))?;
        a.sync()?;
        settle(&mut b)?;
        let mut c = Client::with_config(&server, "c", |cfg| cfg.guard.allow = true)?;
        c.sync()?;
        Ok((b.tree_text(""), c.tree_text("")))
    };
    let whole = outcome(20_000)?;
    assert!(
        whole.0.iter().any(|l| l.contains("z.txt = edited")),
        "{whole:?}"
    );
    for batch in [1, 2, 3] {
        assert_eq!(outcome(batch)?, whole, "pages of {batch}");
    }
    Ok(())
}

/// b has `a/a/a/f.txt`, all new, and a puts a file `a` at the top: b's folder `a` and it
/// clash, and in pages of a few nodes what waits for what is found a page at a time, each
/// such page larger (`f.txt` for its folder, that for its own, ... the top one for the
/// clash). b's first page is stopped before its operations (a pause), so the next sync starts
/// with what is left of the sweep, which waits: the sync goes on with what its pages wait for
/// before it ends, and b's tree is up. Before, the sweep ended there, with that queued, and
/// what b had was left unsynced until something else asked for a sync (§6.5).
#[test]
fn a_sync_plans_what_its_pages_wait_for_before_it_ends() -> R {
    use hbsync_engine::testkit::AfterPlan;
    use std::sync::atomic::{AtomicBool, Ordering};

    for batch in [2, 3, 4] {
        let server = server();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_once = Arc::new(AtomicBool::new(false));
        let hook = {
            let (stop, once) = (Arc::clone(&stop), Arc::clone(&stop_once));
            AfterPlan(Arc::new(move || {
                if once.swap(false, Ordering::SeqCst) {
                    stop.store(true, Ordering::SeqCst);
                }
            }))
        };
        let mut a = Client::with_config(&server, "a", |cfg| cfg.guard.allow = true)?;
        let mut b = Client::with_config(&server, "b", move |cfg| {
            cfg.guard.allow = true;
            cfg.batch = batch;
            cfg.after_plan = Some(hook.clone());
        })?;
        b.engine.set_stop(Arc::clone(&stop));
        b.write("a/a/a/f.txt", "f")?;
        stop_once.store(true, Ordering::SeqCst);
        let first = b.sync()?;
        assert!(first.stopped, "pages of {batch}");
        stop.store(false, Ordering::SeqCst);
        a.write("a", "top")?;
        a.sync()?;
        let report = b.sync()?;
        assert_eq!(report.pending, 0, "pages of {batch}: {:?}", b.tree_text(""));
        let live = server.lock().map_err(|_| "poisoned")?.live_names();
        assert!(
            live.contains(&"f.txt".to_owned()),
            "pages of {batch}: {live:?}"
        );
    }
    Ok(())
}
