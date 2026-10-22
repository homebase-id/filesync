//! Tags, and what other writers put on the drive (newer versions, unreadable ones).

use super::*;

/// Finder tags go both ways between two folders, on files and folders, and a download that
/// replaces a file keeps them. (Linux holds no colours; how they are kept for the Macs is the
/// simulator's.)
#[test]
fn tags_sync_between_folders() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Docs/report.txt", "quarterly")?;
    if !tags::CAN_TAG || tags::write(&a.root.join("Docs"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    let names = |c: &Client, rel: &str| -> Vec<String> {
        tags::read(&c.root.join(rel))
            .unwrap_or_default()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    };
    let tick = || std::thread::sleep(std::time::Duration::from_millis(20));
    let drive_tags = |name: &str| -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let content = srv
            .live()
            .find_map(|h| match &h.body {
                hbsync_remote::RemoteBody::Live { content, .. } if content.name == name => {
                    Some(content.clone())
                }
                _ => None,
            })
            .ok_or("not on the drive")?;
        Ok(content
            .tags()
            .iter()
            .map(|t| format!("{}:{}", t.name, t.colour))
            .collect())
    };
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
    assert_eq!(drive_tags("report.txt")?, [red]);

    // b adds one.
    tick();
    tags::write(
        &b.root.join("Docs/report.txt"),
        &Tags::new([Tag::new("Red", 6), Tag::new("Todo", 0)]),
    )?;
    b.sync()?;
    a.sync()?;
    assert_eq!(drive_tags("report.txt")?, [red, "Todo:0"]);
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
    assert_eq!(drive_tags("report.txt")?, ["Todo:0"]);
    // Nothing left to do on either side.
    assert!(a.engine.sync_once("2026-10-08")?.is_idle());
    assert!(b.engine.sync_once("2026-10-08")?.is_idle());
    Ok(())
}

/// A copy proven equal only across hash algorithms gets its base though the tags differ (the
/// drive's are tagged, the local copy isn't): no conflict copy, and the tags come down.
#[test]
fn adopting_a_sha256_copy_whose_tags_differ_keeps_one_file() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
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
        let names: Vec<String> = tags::read(&b.root.join("photo.jpg"))
            .unwrap_or_default()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(names, ["Blue"]);
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
