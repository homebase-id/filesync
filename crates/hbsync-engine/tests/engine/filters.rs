//! What a device leaves out: junk, ignore rules, scopes and role folders.

use super::*;

#[test]
fn junk_never_syncs_and_goes_with_its_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("proj/doc.txt", "v1")?;
    a.write("proj/.DS_Store", "finder")?;
    a.write("~$doc.docx", "lock")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["proj/", "proj/doc.txt = v1"]);

    // b's own junk in the folder doesn't hold up a's delete of it.
    b.write("proj/Thumbs.db", "thumbs")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    assert!(b.tree_text("").is_empty(), "{:?}", b.tree_text(""));
    assert_eq!(a.tree_text(""), ["~$doc.docx = lock"]);
    Ok(())
}

/// A folder on the junk list (`.dropbox.cache`, `$RECYCLE.BIN`, a folder named `*.part`) may
/// hold what was never synced: it stays, with the folder it is in, when that folder is deleted
/// elsewhere, as ignored items do (§10); a junk file directly in that folder stays with it.
#[test]
fn a_junk_folder_stays_with_its_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("proj/doc.txt", "v1")?;
    a.write("readme.txt", "r")?; // so deleting proj isn't deleting everything
    a.sync()?;
    b.sync()?;
    b.write("proj/Chapter.part/only copy.txt", "never synced")?;
    b.write("proj/Thumbs.db", "thumbs")?;
    b.sync()?;
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.read_text("proj/Chapter.part/only copy.txt")?,
        "never synced"
    );
    assert!(b.read_text("proj/doc.txt").is_err(), "the synced file went");
    assert_eq!(b.read_text("proj/Thumbs.db")?, "thumbs");
    Ok(())
}

#[test]
fn ignored_items_stay_on_their_device_and_keep_their_folder() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("node_modules/\n*.log\n"))?;
    a.write("proj/main.js", "m")?;
    a.write("proj/run.log", "from a")?;
    a.write("readme.txt", "r")?; // so deleting proj isn't deleting everything
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        ["proj/", "proj/main.js = m", "readme.txt = r"]
    );

    b.write("proj/node_modules/x/y.js", "dep")?;
    b.write("proj/debug.log", "b's log")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "proj/",
            "proj/main.js = m",
            "proj/run.log = from a",
            "readme.txt = r"
        ]
    );

    // a deletes the project: b keeps what it ignores, and the folder holding it, which goes
    // back up as a new, empty folder.
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "proj/",
            "proj/debug.log = b's log",
            "proj/node_modules/",
            "proj/node_modules/x/",
            "proj/node_modules/x/y.js = dep",
            "readme.txt = r"
        ]
    );
    a.sync()?;
    assert_eq!(a.tree_text(""), ["proj/", "readme.txt = r"]);
    Ok(())
}

/// A device whose rules leave it one top-level folder of the drive (`/*`, `!/mine/`) takes
/// nothing else down and writes nothing else: not another folder's files, nor the name clash
/// in it (two files of one name, which a device that sees them renames on the drive). How the
/// live end-to-end tests keep to their own folder on a drive other tests share.
#[test]
fn a_device_kept_to_one_folder_touches_nothing_else_on_the_drive() -> R {
    use hbsync_remote::Remote;
    let server = server();
    let other = FakeRemote::new(server.clone());
    let elsewhere = hbsync_remote::contract::folder("elsewhere", NodeId::ROOT);
    other.create(&elsewhere, None)?;
    for data in [b"one", b"two"] {
        server.lock().map_err(|_| "poisoned")?.seed_file(
            elsewhere.node,
            "f",
            data,
            hbsync_fileio::HashAlgorithm::Blake3,
        )?;
    }
    let versions = || -> Result<Vec<_>, Box<dyn std::error::Error>> {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let mut v: Vec<_> = srv.live().map(|h| (h.file_id, h.version)).collect();
        v.sort_by_key(|(f, _)| f.0);
        Ok(v)
    };
    let before = versions()?;

    let mut a = Client::with_config(&server, "a", ignoring("/*\n!/mine/\n"))?;
    let mut b = Client::with_config(&server, "b", ignoring("/*\n!/mine/\n"))?;
    a.write("mine/x.txt", "x")?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    for c in [&a, &b] {
        assert_eq!(c.tree_text(""), ["mine/", "mine/x.txt = x"], "{}", c.name);
    }
    let after: Vec<_> = versions()?
        .into_iter()
        .filter(|(f, _)| before.iter().any(|(g, _)| g == f))
        .collect();
    assert_eq!(after, before, "nothing else on the drive was written");

    // A device that sees the folder resolves the clash on the drive.
    let mut c = Client::new(&server, "c")?;
    c.sync()?;
    assert!(
        server
            .lock()
            .map_err(|_| "poisoned")?
            .live_names()
            .iter()
            .any(|n| n.starts_with("f (")),
        "renamed"
    );
    Ok(())
}

#[test]
fn a_file_moved_into_a_folder_one_device_ignores_leaves_that_device_only() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("notes.txt", "n")?;
    a.write("draft.txt", "d1")?;
    a.sync()?;
    b.sync()?;
    // a moves both into build/; b edited the draft meanwhile.
    fs::create_dir(a.root.join("build"))?;
    fs::rename(a.root.join("notes.txt"), a.root.join("build/notes.txt"))?;
    fs::rename(a.root.join("draft.txt"), a.root.join("build/draft.txt"))?;
    b.write("draft.txt", "d2 from b")?;
    a.sync()?;
    b.sync()?;
    // The unchanged file went with the move; b's edit stays, as a new file everyone gets.
    assert_eq!(b.tree_text(""), ["draft.txt = d2 from b"]);
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "build/",
            "build/draft.txt = d1",
            "build/notes.txt = n",
            "draft.txt = d2 from b"
        ]
    );
    Ok(())
}

/// `c` restarted with its config changed by `tweak`, as at a start-up: returns whether the
/// filter changed.
fn restart_with(
    c: &mut Client,
    tweak: impl FnOnce(&mut hbsync_engine::EngineConfig),
) -> Result<bool, Box<dyn std::error::Error>> {
    let remote = Box::new(c.remote.clone());
    c.restart_with(remote, tweak)?;
    Ok(c.engine.apply_filter_change()?)
}

/// `c` restarted with the ignore rules `rules`.
fn restart_ignoring(c: &mut Client, rules: &'static str) -> R {
    assert!(restart_with(c, ignoring(rules))?, "the filter changed");
    assert!(!c.engine.apply_filter_change()?, "and is recorded");
    Ok(())
}

#[test]
fn a_folder_no_longer_ignored_comes_with_everything_in_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("readme.txt", "r")?;
    a.write("build/out.txt", "o")?;
    a.write("build/deep/more.txt", "m")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["readme.txt = r"]);

    restart_ignoring(&mut b, "")?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "build/",
            "build/deep/",
            "build/deep/more.txt = m",
            "build/out.txt = o",
            "readme.txt = r"
        ]
    );
    Ok(())
}

#[test]
fn a_folder_moved_out_of_an_ignored_one_comes_with_everything_in_it() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("build/\n"))?;
    a.write("readme.txt", "r")?;
    a.write("build/sub/out.txt", "o")?;
    a.write("build/sub/deep/more.txt", "m")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(""), ["readme.txt = r"]);

    fs::rename(a.root.join("build/sub"), a.root.join("sub"))?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "readme.txt = r",
            "sub/",
            "sub/deep/",
            "sub/deep/more.txt = m",
            "sub/out.txt = o"
        ]
    );
    Ok(())
}

#[test]
fn a_new_ignore_rule_takes_files_off_the_drive_but_not_off_the_disk() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("src/main.rs", "fn main")?;
    a.write("build/out.o", "obj")?;
    a.write("readme", "r")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("build/out.o")?, "obj");

    // a starts ignoring build/: it leaves the drive and b (for b's trash), and stays on a.
    restart_ignoring(&mut a, "build/\n")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(a.read_text("build/out.o")?, "obj");
    assert_eq!(
        b.tree_text(""),
        ["readme = r", "src/", "src/main.rs = fn main"]
    );

    // The rule goes: build/ is new on a again, and goes back up.
    restart_ignoring(&mut a, "")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read_text("build/out.o")?, "obj");
    Ok(())
}

#[test]
fn a_parked_folder_kept_for_what_it_ignores_gets_a_real_name() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", ignoring("node_modules/\n"))?;
    a.write("proj/main.js", "m")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    b.sync()?;
    b.write("proj/node_modules/x.js", "dep")?;
    b.sync()?;

    // a starts the project over: b's old folder makes way for the new one, and keeps what b
    // ignores under a real name, never the temporary one.
    fs::remove_dir_all(a.root.join("proj"))?;
    a.sync()?;
    a.write("proj/new.js", "n")?;
    a.sync()?;
    b.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        b.tree_text(""),
        [
            "proj (conflict 1)/",
            "proj (conflict 1)/node_modules/",
            "proj (conflict 1)/node_modules/x.js = dep",
            "proj/",
            "proj/new.js = n",
            "readme.txt = r"
        ]
    );
    assert_eq!(
        a.tree_text(""),
        [
            "proj (conflict 1)/",
            "proj/",
            "proj/new.js = n",
            "readme.txt = r"
        ]
    );
    Ok(())
}

#[test]
fn a_parked_file_whose_name_became_ignored_and_taken_moves_to_a_numbered_one() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("app.log", "keep me")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    // A crash right after parking app.log; then a new app.log appears, and a rule ignores
    // every numbered name too.
    let from = hbsync_platform::RelPath::new("app.log")?;
    let node = a
        .engine
        .db()
        .read(|s| s.get_local(&from))?
        .ok_or("row")?
        .node;
    let to = hbsync_platform::RelPath::new(&hbsync_local::layout::parked_name(node))?;
    fs::rename(from.to_native(&a.root), to.to_native(&a.root))?;
    a.engine.db().write(|s| {
        s.folder().move_local_subtree(&from, &to)?;
        s.folder().put_parked(node, &from)
    })?;
    a.write("app.log", "new log")?;
    restart_ignoring(&mut a, "*.log\n")?;
    a.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        [
            "app (conflict 1).log = keep me",
            "app.log = new log",
            "readme.txt = r"
        ]
    );
    // Ignored now: it left the drive.
    assert_eq!(
        server.lock().map_err(|_| "poisoned")?.live_names(),
        ["readme.txt"]
    );
    Ok(())
}

/// Another device moves a folder into a name this one ignores: it leaves this device (its
/// trash), followed without asking (§7: another device's removals aren't held, short of every
/// synced file), and the drive keeps the folder.
#[test]
fn a_folder_moved_elsewhere_into_an_ignored_name_leaves_unasked() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::with_config(&server, "b", |cfg| {
        ignoring("build/\n")(cfg);
        cfg.guard.min = 2;
    })?;
    for i in 0..4 {
        a.write(&format!("src/f{i}.txt"), format!("{i}"))?;
    }
    a.write("keep.txt", "k")?;
    a.sync()?;
    b.sync()?;
    fs::rename(a.root.join("src"), a.root.join("build"))?;
    a.sync()?;
    let report = b.sync()?;
    assert!(report.held.is_empty(), "{report:?}");
    assert_eq!(b.tree_text(""), ["keep.txt = k"]);
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 6);
    Ok(())
}

// ---- §12 scopes ----

/// The role folder `role` on the drive, made by another device, with one file in it.
fn seed_role(
    server: &Arc<std::sync::Mutex<hbsync_remote::fake::FakeServer>>,
    role: hbsync_types::Role,
    file: &str,
    data: &str,
) -> R {
    let mut srv = server.lock().map_err(|_| "poisoned")?;
    let folder = NodeMeta {
        node: role.node(),
        ..hbsync_remote::contract::folder(role.name(), hbsync_types::NodeId::ROOT)
    };
    srv.create(&folder, None)?;
    srv.seed_file(
        role.node(),
        file,
        data.as_bytes(),
        hbsync_fileio::HashAlgorithm::Blake3,
    )?;
    Ok(())
}

fn unscoped(cfg: &mut hbsync_engine::EngineConfig) {
    cfg.folder.scope = Arc::new(hbsync_engine::scope::Scope {
        top: hbsync_types::NodeId::ROOT,
        outside: Default::default(),
    });
}

#[test]
fn role_folders_stay_out_of_the_main_folder() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "mine")?;
    let mut a = Client::new(&server, "a")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["readme.txt = r"]);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Documents", "cv.txt", "readme.txt"]);
    Ok(())
}

#[test]
fn a_plain_documents_folder_beside_the_role_folder_keeps_its_name() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("Documents/mine.txt", "plain")?;
    a.sync()?;
    let mut b = Client::new(&server, "b")?;
    b.sync()?;
    // A role folder holds no name for the main folder: two at the top, told apart by id.
    let plain = ["Documents/", "Documents/mine.txt = plain"];
    assert_eq!(a.tree_text(""), plain);
    assert_eq!(b.tree_text(""), plain);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Documents", "Documents", "cv.txt", "mine.txt"]);
    Ok(())
}

#[test]
fn a_plain_documents_folder_keeps_its_name_when_the_role_folder_comes() -> R {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    let mut b = Client::new(&server, "b")?;
    a.write("Documents/mine.txt", "plain")?;
    a.sync()?;
    b.sync()?;
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    a.write("other.txt", "o")?;
    a.sync()?;
    b.sync()?;
    let mine = ["Documents/", "Documents/mine.txt = plain", "other.txt = o"];
    assert_eq!(a.tree_text(""), mine);
    assert_eq!(b.tree_text(""), mine);
    Ok(())
}

#[test]
fn a_role_folder_no_longer_left_out_comes_with_everything_in_it() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    assert_eq!(a.tree_text(""), ["readme.txt = r"]);

    restart_with(&mut a, unscoped)?;
    a.sync()?;
    assert_eq!(
        a.tree_text(""),
        ["Documents/", "Documents/cv.txt = role", "readme.txt = r"]
    );
    Ok(())
}

/// Recently deleted is the account's: the main folder lists the role folder's removals too,
/// each with the folder it comes back to (none here for a role folder this device doesn't
/// sync), and emptying what was shown takes both.
#[test]
fn recently_deleted_names_the_folder_each_item_comes_back_to() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("mine.txt", "m")?;
    a.sync()?;
    // Both removed elsewhere: one in the role folder, one at the top.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let files: Vec<_> = srv
            .live()
            .filter(|h| h.kind == hbsync_types::NodeKind::File)
            .cloned()
            .collect();
        assert_eq!(files.len(), 2);
        for h in files {
            if let hbsync_remote::RemoteBody::Live { node, content, .. } = h.body {
                let meta = hbsync_remote::contract::removed(&NodeMeta {
                    node,
                    parent: h.parent,
                    kind: h.kind,
                    content,
                    removed: false,
                });
                srv.update(h.file_id, h.version, &meta, None)?;
            }
        }
    }
    a.sync()?;
    let mut listed: Vec<(String, Option<String>)> = a
        .recently_deleted(10)?
        .into_iter()
        .map(|d| (d.path, d.folder))
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        [
            ("cv.txt".into(), None),
            (
                "mine.txt".into(),
                Some(a.root.to_string_lossy().into_owned())
            )
        ]
    );
    let listed = a.listed()?;
    a.engine.drive().empty_listed(&listed)?;
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert_eq!(srv.removed().count(), 0);
    Ok(())
}

#[test]
fn a_role_folder_moved_into_a_folder_keeps_it_alive() -> R {
    let server = server();
    seed_role(&server, hbsync_types::Role::Documents, "cv.txt", "role")?;
    let mut a = Client::new(&server, "a")?;
    a.write("Archive/old.txt", "o")?;
    a.write("readme.txt", "r")?;
    a.sync()?;
    // A client that breaks the schema moves the role folder into Archive.
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let archive = srv
            .live()
            .find_map(|h| match &h.body {
                hbsync_remote::RemoteBody::Live { node, content, .. }
                    if content.name == "Archive" =>
                {
                    Some(*node)
                }
                _ => None,
            })
            .ok_or("Archive")?;
        let docs = srv
            .live()
            .find(|h| {
                matches!(&h.body, hbsync_remote::RemoteBody::Live { node, .. }
                if *node == hbsync_types::Role::Documents.node())
            })
            .cloned()
            .ok_or("the role folder")?;
        let meta = NodeMeta {
            parent: archive,
            ..hbsync_remote::contract::folder("Documents", archive)
        };
        let meta = NodeMeta {
            node: hbsync_types::Role::Documents.node(),
            ..meta
        };
        srv.update(docs.file_id, docs.version, &meta, None)?;
    }
    a.sync()?;
    fs::remove_dir_all(a.root.join("Archive"))?;
    a.sync()?;
    // Archive holds what this device doesn't see: it stays on the drive, and comes back here.
    assert_eq!(a.tree_text(""), ["Archive/", "readme.txt = r"]);
    let names = server.lock().map_err(|_| "poisoned")?.live_names();
    assert_eq!(names, ["Archive", "Documents", "cv.txt", "readme.txt"]);
    Ok(())
}
