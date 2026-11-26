//! Tags, and what other writers put on the drive (newer versions, unreadable ones).

use super::*;
use hbsync_platform::tags::{self, Tag, Tags};

/// The names of the tags the item at `rel` holds on `c`.
fn names(c: &Client, rel: &str) -> Vec<String> {
    tags::read(&c.root.join(rel))
        .unwrap_or_default()
        .iter()
        .map(|t| t.name.clone())
        .collect()
}

/// A pause, so the next change of an item reads as one (its change time moves).
fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

/// The drive's tags of the file named `name`, as `name:colour`.
fn drive_tags(server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>, name: &str) -> Vec<String> {
    match common::header_named(server, name).body {
        hbsync_remote::RemoteBody::Live { content, .. } => content
            .tags()
            .iter()
            .map(|t| format!("{}:{}", t.name, t.colour))
            .collect(),
        hbsync_remote::RemoteBody::Deleted => Vec::new(),
    }
}

/// Finder tags go both ways between two folders, on files and folders, and a download that
/// replaces a file keeps them. (Linux holds no colours; how they are kept for the Macs is the
/// simulator's.)
#[test]
fn tags_sync_between_folders() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Docs/report.txt", "quarterly")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("Docs"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let red = if cfg!(target_os = "macos") {
        "Red:6"
    } else {
        "Red:0"
    };
    a.sync()?;
    b.sync()?;

    // Tagged on a: a file (with a colour) and a folder.
    tick();
    tags::write(
        &a.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Red", 6)]),
    )?;
    tags::write(&a.root.join("Docs"), &Tags::new([Tag::new("Work", 0)]))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(names(&b, "Docs/report.txt"), ["Red"]);
    assert_eq!(names(&b, "Docs"), ["Work"]);
    assert_eq!(drive_tags(&server, "report.txt"), [red]);

    // b adds one.
    tick();
    tags::write(
        &b.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Red", 6), Tag::new("Todo", 0)]),
    )?;
    b.sync()?;
    a.sync()?;
    assert_eq!(drive_tags(&server, "report.txt"), [red, "Todo:0"]);
    assert_eq!(names(&a, "Docs/report.txt"), ["Red", "Todo"]);

    // A new version from a replaces b's file: the tags stay.
    a.write("Docs/report.txt", "quarterly, revised")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("Docs/report.txt")?, "quarterly, revised");
    assert_eq!(names(&b, "Docs/report.txt"), ["Red", "Todo"]);

    // A folder gets a new child and a tag in one go: the tag lands though the folder changed.
    tick();
    a.write("Docs/new.txt", "new")?;
    tags::write(
        &a.root.join("Docs"),
        &Tags::new([Tag::new("Work", 0), Tag::new("Q3", 0)]),
    )?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(names(&b, "Docs"), ["Q3", "Work"]);
    assert_eq!(b.read_text("Docs/new.txt")?, "new");

    // Removed on b, removed on a.
    tick();
    tags::write(
        &b.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Todo", 0)]),
    )?;
    b.sync()?;
    a.sync()?;
    assert_eq!(names(&a, "Docs/report.txt"), ["Todo"]);
    assert_eq!(drive_tags(&server, "report.txt"), ["Todo:0"]);
    // Nothing left to do on either side.
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// A tagged folder is deleted for good on the drive (another app's hard delete: no trace in the
/// feed) and made again there, as another file of the same node, by a device that never saw
/// its tag (§11, #439). What the new file lacks is no removal: the tag stays on the folder here
/// and goes up. A second folder made again with one of its two tags keeps both. (Read against
/// the base agreed with the old file, the new file's lack of the tag was the drive's removal,
/// and took the tag off the folder.)
#[test]
fn a_tag_stays_where_its_folder_is_made_again_without_it() -> R {
    use hbsync_types::NodeKind;
    use hbsync_types::schema::NodeContent;
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("Docs/report.txt", "quarterly")?;
    a.write("Mail/in.txt", "hello")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("Docs"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let both = Tags::new([Tag::new("Todo", 0), Tag::new("Work", 0)]);
    tags::write(&a.root.join("Docs"), &Tags::new([Tag::new("Work", 0)]))?;
    tags::write(&a.root.join("Mail"), &both)?;
    a.sync()?;
    assert_eq!(drive_tags(&server, "Docs"), ["Work:0"]);
    assert_eq!(drive_tags(&server, "Mail"), ["Todo:0", "Work:0"]);

    // Made again: Docs with no tag, Mail with one of its two.
    for (name, tags) in [
        ("Docs", Tags::default()),
        ("Mail", Tags::new([Tag::new("Todo", 0)])),
    ] {
        let was = common::header_named(&server, name);
        let mut content = NodeContent::folder(name);
        content.set_tags(&tags);
        let meta = NodeMeta {
            node: was.node().ok_or("a node")?,
            parent: was.parent,
            kind: NodeKind::Folder,
            content,
            removed: false,
        };
        let mut s = server.lock().map_err(|_| "poisoned")?;
        s.hard_delete(was.file_id, was.version)?;
        s.create(&meta, None)?;
    }
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(names(&a, "Docs"), ["Work"]);
    assert_eq!(names(&a, "Mail"), ["Todo", "Work"]);
    assert_eq!(drive_tags(&server, "Docs"), ["Work:0"]);
    assert_eq!(drive_tags(&server, "Mail"), ["Todo:0", "Work:0"]);
    assert_eq!(a.read_text("Docs/report.txt")?, "quarterly");
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// An executable file is deleted for good on the drive and made again there with other bytes,
/// as another file of the same node, by a device that never saw its bit (§1, #450). What the
/// new file lacks is no clearing: the new bytes come down, the file keeps its bit, and the
/// bit goes up; a second device that held it does the same, and finds it on the drive. No
/// conflict copy. (Read against the base agreed with the old file, the new file's lack of the
/// bit was the drive's clearing, and took it off every device.)
#[cfg(unix)]
#[test]
fn the_executable_bit_stays_where_its_file_is_made_again_without_it() -> R {
    use hbsync_remote::RemoteBody;
    use std::os::unix::fs::PermissionsExt;
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("run.sh", "echo old")?;
    fs::set_permissions(a.root.join("run.sh"), fs::Permissions::from_mode(0o755))?;
    a.sync()?;
    b.sync()?;
    let executable = |c: &Client| -> common::Res<bool> {
        Ok(fs::metadata(c.root.join("run.sh"))?.permissions().mode() & 0o111 != 0)
    };
    let on_drive = || match common::header_named(&server, "run.sh").body {
        RemoteBody::Live { content, .. } => content.x,
        RemoteBody::Deleted => false,
    };
    assert!(executable(&b)? && on_drive());

    // Made again, without the bit.
    {
        let was = common::header_named(&server, "run.sh");
        let node = was.node().ok_or("a node")?;
        let mut s = server.lock().map_err(|_| "poisoned")?;
        s.hard_delete(was.file_id, was.version)?;
        let blake3 = hbsync_fileio::HashAlgorithm::Blake3;
        s.seed_file_of(node, was.parent, ("run.sh", false), b"echo new", blake3)?;
    }
    assert!(!on_drive());
    for client in [&mut a, &mut b] {
        let report = client.sync()?;
        assert!(report.failed.is_empty(), "{report:?}");
        assert!(report.conflicts.is_empty(), "{report:?}");
        assert_eq!(client.read_text("run.sh")?, "echo new");
        assert!(executable(client)?, "kept where it was held");
        assert!(on_drive(), "and sent up");
        // No conflict copy beside it.
        let files = fs::read_dir(&client.root)?
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .count();
        assert_eq!(files, 1);
        assert!(client.engine.sync_once("2026-10-08")?.is_idle());
    }
    Ok(())
}

/// A conflict copy keeps the node's base for the run's remote writes (§5): this device's removal
/// of a tag, made with its edit, reaches the drive's version that won the edits (the remote
/// write merges its tags against the base). The conflict's download comes in the same run.
#[test]
fn a_tag_removed_with_a_conflicting_edit_is_removed_on_the_drive() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("f.txt", "base")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("f.txt"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    tags::write(&a.root.join("f.txt"), &Tags::new([Tag::new("Red", 0)]))?;
    a.sync()?;
    b.sync()?;
    b.write("f.txt", "b's edit")?;
    b.sync()?;
    tick();
    a.write("f.txt", "a's edit")?;
    tags::write(&a.root.join("f.txt"), &Tags::default())?;
    let report = a.sync()?;
    assert!(report.failed.is_empty(), "{report:?}");
    // The fork, the tag write, the download and the copy's upload: the download acts on the
    // version the run's own tag write made (the bytes the same), never stale.
    assert!(report.skipped.is_empty(), "{report:?}");
    assert_eq!(report.ops_done, 4, "{report:?}");
    assert!(drive_tags(&server, "f.txt").is_empty());
    assert_eq!(a.read_text("f.txt")?, "b's edit");
    Ok(())
}

/// The drive's version a conflict brings down settles its tags from the drive's, not from the
/// item that went into the copy (§5): a tag that item held and the drive dropped isn't kept in
/// the base, so another device adding it again later is not met as this device's removal.
#[test]
fn a_tag_the_conflict_copy_took_doesn_t_undo_a_later_add() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("f.txt", "base")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("f.txt"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let red = Tags::new([Tag::new("Red", 0)]);
    tags::write(&a.root.join("f.txt"), &red)?;
    a.sync()?;
    b.sync()?;
    // b drops Red with its edit; a's edit keeps it: a forks and takes b's version.
    tick();
    b.write("f.txt", "b's edit")?;
    tags::write(&b.root.join("f.txt"), &Tags::default())?;
    b.sync()?;
    a.write("f.txt", "a's edit")?;
    a.sync()?;
    assert_eq!(a.read_text("f.txt")?, "b's edit");
    // b adds Red again.
    tick();
    tags::write(&b.root.join("f.txt"), &red)?;
    b.sync()?;
    a.sync()?;
    b.sync()?;
    assert_eq!(names(&b, "f.txt"), ["Red"]);
    assert_eq!(names(&a, "f.txt"), ["Red"]);
    Ok(())
}

/// A copy proven equal only across hash algorithms gets its base though the tags differ (the
/// drive's are tagged, the local copy isn't): no conflict copy, and the tags come down.
#[test]
fn adopting_a_sha256_copy_whose_tags_differ_keeps_one_file() -> R {
    let server = server();
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let (node, written) = srv.seed_file(
            hbsync_types::NodeId::ROOT,
            "photo.jpg",
            b"photo bytes",
            hbsync_fileio::HashAlgorithm::Sha256,
        )?;
        let mut content =
            hbsync_types::schema::NodeContent::file("photo.jpg", 1_700_000_000_000, 11, false);
        content.set_tags(&Tags::new([Tag::new("Blue", 4)]));
        let meta = NodeMeta {
            node,
            parent: hbsync_types::NodeId::ROOT,
            kind: hbsync_types::NodeKind::File,
            content,
            removed: false,
        };
        srv.update(written.file_id, written.version, &meta, None)?;
    }
    let mut b = Client::new(&server, "b")?;
    b.write("photo.jpg", "photo bytes")?;
    let report = b.sync()?;
    assert!(report.conflicts.is_empty(), "{:?}", report.conflicts);
    assert_eq!(b.tree_text(""), ["photo.jpg = photo bytes"]);
    if tags::CAN_TAG && tags::read(&b.root.join("photo.jpg")).is_some() {
        assert_eq!(names(&b, "photo.jpg"), ["Blue"]);
    }
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// A rename writes the node's content again with what another writer put there as it was: no
/// `mtime` stays none (never 1970, on the drive or here), and the tags keep their form while
/// the set is the same.
#[test]
fn a_rename_keeps_what_another_writer_wrote() -> R {
    let server = server();
    // With a creation time: one without gets the file's on a rename (node schema, `ct`).
    let raw =
        r#"{"v":1,"name":"note.txt","size":4,"ct":7,"tags":[{"by":"phone","c":6,"n":"Red"}]}"#;
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let (node, written) = srv.seed_file(
            hbsync_types::NodeId::ROOT,
            "note.txt",
            b"note",
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
        let meta = NodeMeta {
            node,
            parent: hbsync_types::NodeId::ROOT,
            kind: hbsync_types::NodeKind::File,
            content: hbsync_types::schema::NodeContent::from_json(raw)?,
            removed: false,
        };
        srv.update(written.file_id, written.version, &meta, None)?;
    }
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    // No date on the drive: the download keeps its own, never 1970.
    let modified = fs::metadata(b.root.join("note.txt"))?.modified()?;
    assert!(modified > std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400));
    fs::rename(b.root.join("note.txt"), b.root.join("renamed.txt"))?;
    b.sync()?;
    let srv = server.lock().map_err(|_| "poisoned")?;
    let content = srv
        .live()
        .find_map(|h| match &h.body {
            hbsync_remote::RemoteBody::Live { content, .. } => Some(content.to_json()),
            _ => None,
        })
        .ok_or("not on the drive")?;
    assert_eq!(content, raw.replace("note.txt", "renamed.txt"));
    Ok(())
}

// ---- rows this device can't read ----

/// A folder this device can't read (a newer writer's) still holds what is in it: its files,
/// which read, wait for it, on every computer; none is moved into Recovered files.
#[test]
fn what_is_in_a_folder_that_doesnt_read_stays_in_it() -> R {
    let server = server();
    let (folder, file) = {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let meta = hbsync_remote::contract::folder("newer", hbsync_types::NodeId::ROOT);
        let folder = srv.create(&meta, None)?;
        let (_, file) = srv.seed_file(
            meta.node,
            "inside.txt",
            b"inside",
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
        srv.set_unreadable(folder.file_id, true);
        (folder, file)
    };
    let folder_at = folder;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    // It waits: nothing done, now or later.
    let again = b.engine.sync_once("2026-10-08")?;
    assert_eq!((again.ops_done, again.unfinished), (0, false), "{again:?}");
    let srv = server.lock().map_err(|_| "poisoned")?;
    let folder = srv
        .live()
        .find(|h| h.file_id == folder.file_id)
        .and_then(|h| h.node())
        .ok_or("live")?;
    assert_eq!(srv.header(file.file_id)?.parent, folder, "still in it");
    drop(srv);
    // Deleted for good: what waited in it is an orphan now, for Recovered files.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        srv.delete(folder_at.file_id, folder_at.version)?;
    }
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["Recovered files/", "Recovered files/inside.txt = inside"]
    );
    Ok(())
}

/// A file whose newer version this device can't read is held: an edit here goes up as a new
/// node, once; the drive's version stays as it is, and nothing is sent again.
#[test]
fn an_edit_of_a_file_whose_newer_version_doesnt_read_forks() -> R {
    let server = server();
    let mut b = Client::new(&server, "b")?;
    b.write("f.txt", "one")?;
    b.sync()?;
    let mut c = Client::new(&server, "c")?;
    c.sync()?;
    c.write("f.txt", "two, by a newer client")?;
    c.sync()?;
    let file = {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let file = srv
            .live()
            .find(|h| h.node().is_some() && h.kind == hbsync_types::NodeKind::File)
            .ok_or("on the drive")?
            .file_id;
        srv.set_unreadable(file, true);
        file
    };
    b.sync()?;
    assert_eq!(b.read_text("f.txt")?, "one", "held as it was");
    b.write("f.txt", "one, edited here")?;
    b.sync()?;
    let again = b.engine.sync_once("2026-10-08")?;
    assert_eq!((again.ops_done, again.unfinished), (0, false), "{again:?}");
    let srv = server.lock().map_err(|_| "poisoned")?;
    let files: Vec<_> = srv
        .live()
        .filter(|h| h.kind == hbsync_types::NodeKind::File)
        .collect();
    assert_eq!(files.len(), 2, "the drive's version and the fork");
    // The fork takes a free name: the drive's version holds the old one.
    assert_eq!(b.tree_text(""), ["f (conflict 1).txt = one, edited here"]);
    assert!(files.iter().any(|h| h.file_id == file));
    drop(srv);
    // Its next version reads: the file is held no more, and comes down.
    server
        .lock()
        .map_err(|_| "poisoned")?
        .set_unreadable(file, false);
    c.write("f.txt", "three")?;
    c.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["f (conflict 1).txt = one, edited here", "f.txt = three"]
    );
    Ok(())
}

/// The drive moves f (tagged Work) to another folder and edits it: b's download puts the new
/// version in place there, moves the old one aside, and b crashes before the commit. a then
/// removes the tag. b's recovery takes the placed file for its node, and the removal reaches
/// it: the tag never comes back.
#[test]
fn a_tag_removed_while_a_moved_download_waits_for_recovery_stays_removed() -> R {
    tag_removed_while_a_download_waits_for_recovery(true)
}

/// [`a_tag_removed_while_a_moved_download_waits_for_recovery_stays_removed`], the drive's edit
/// in place: the new version swapped in where it was (nothing aside), b's row of it not read
/// since.
#[test]
fn a_tag_removed_while_a_download_in_place_waits_for_recovery_stays_removed() -> R {
    tag_removed_while_a_download_waits_for_recovery(false)
}

/// f, tagged Work, edited on the drive (`moved`: moved to p2 first); b crashes between placing
/// the new version and the commit, then a removes the tag. The old version aside is decided in
/// b's first sync after it, and the tag stays removed.
fn tag_removed_while_a_download_waits_for_recovery(moved: bool) -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("p1/f", "one")?;
    a.write("p2/k", "k")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("p1/f"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    tags::write(&a.root.join("p1/f"), &Tags::new([Tag::new("Work", 0)]))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(names(&b, "p1/f"), ["Work"]);
    let at = if moved { "p2/f" } else { "p1/f" };
    if moved {
        fs::rename(a.root.join("p1/f"), a.root.join(at))?;
        a.sync()?;
    }
    tick();
    fs::OpenOptions::new()
        .append(true)
        .open(a.root.join(at))
        .and_then(|mut f| std::io::Write::write_all(&mut f, b" two"))?;
    a.sync()?;
    assert_eq!(drive_tags(&server, "f"), ["Work:0"]);
    let placed = b.root.join(at);
    let crashed = common::crash(&mut b, common::Kept::All, move |at, _| {
        at == Commit::Before && fs::read(&placed).is_ok_and(|b| b == b"one two")
    })?;
    assert!(crashed);
    assert_eq!(names(&b, at), ["Work"], "put in place, not committed");
    tick();
    tags::write(&a.root.join(at), &Tags::default())?;
    a.sync()?;
    assert!(drive_tags(&server, "f").is_empty());
    b.restart()?;
    b.sync()?;
    let aside = fs::read_dir(b.root.join(".hbsync/aside"))
        .map(|d| d.count())
        .unwrap_or(0);
    assert_eq!(aside, 0, "the old version decided in the same sync");
    a.sync()?;
    b.sync()?;
    assert!(
        drive_tags(&server, "f").is_empty(),
        "the removed tag came back"
    );
    assert!(names(&b, at).is_empty());
    assert_eq!(a.tree_text(""), b.tree_text(""));
    Ok(())
}

/// A known limit (sync semantics §11, own removal in flight): a tag's removal lands on the
/// drive but its answer is lost, and the user puts the tag back before the next sync. The base
/// still has the tag, so the plan reads the drive's lack of it as another device's removal,
/// and the tag goes from the file again.
#[test]
fn a_tag_put_back_after_its_removal_lost_its_answer_goes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("f.txt", "x")?;
    let file = a.root.join("f.txt");
    if !tags::CAN_TAG || tags::write(&file, &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let red = Tags::new([Tag::new("Red", 6)]);
    tags::write(&file, &red)?;
    a.sync()?;
    assert_eq!(drive_tags(&server, "f.txt").len(), 1);

    // The removal lands; its answer doesn't.
    tick();
    tags::write(&file, &Tags::default())?;
    a.remote.faults().lose_answer_named = Some("f.txt".into());
    let report = a.sync()?;
    a.remote.faults().lose_answer_named = None;
    assert_eq!(
        (report.ops_done, report.skipped.len()),
        (0, 1),
        "{report:?}"
    );
    assert!(drive_tags(&server, "f.txt").is_empty());

    // Put back before the next sync: lost to the removal.
    tick();
    tags::write(&file, &red)?;
    a.sync()?;
    assert!(names(&a, "f.txt").is_empty());
    assert!(drive_tags(&server, "f.txt").is_empty());
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}
