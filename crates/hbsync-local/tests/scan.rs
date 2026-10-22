use std::fs;
use std::path::Path;
use std::sync::Arc;

use hbsync_fileio::HashAlgorithm;
use hbsync_local::{Filter, ScanOptions, ScanReport, Settle, rescan_dirs, scan};
use hbsync_platform::RelPath;
use hbsync_state::{BaseRow, StateDb};
use hbsync_types::{
    ContentState, FileContent, NodeId, NodeKind, NodeState, RemoteFileId, RemoteRef, VersionTag,
};

type R = Result<(), Box<dyn std::error::Error>>;

fn p(s: &str) -> RelPath {
    RelPath::new(s).expect("valid path")
}

struct Fixture {
    dir: tempfile::TempDir,
    db: StateDb,
    opts: ScanOptions,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let opts = ScanOptions::for_root(dir.path(), HashAlgorithm::Blake3);
        Ok(Fixture {
            dir,
            db: StateDb::open_in_memory()?,
            opts,
        })
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn write(&self, rel: &str, data: &[u8]) -> std::io::Result<()> {
        let p = self.root().join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, data)
    }

    /// Scans and clears the dirty set, returning the report and the nodes that were dirty.
    fn scan(&mut self) -> Result<(ScanReport, Vec<NodeId>), Box<dyn std::error::Error>> {
        let root = self.dir.path().to_path_buf();
        let report = scan(&mut self.db, &root, &self.opts)?;
        Ok((report, self.take_dirty()?))
    }

    /// The nodes marked dirty, which are then clean.
    fn take_dirty(&mut self) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        Ok(self.db.write(|s| {
            let nodes = s.dirty_nodes(usize::MAX >> 1)?;
            for n in &nodes {
                s.folder().clear_dirty(*n)?;
            }
            Ok(nodes)
        })?)
    }

    /// Records every local file as synced (a base row), as a sync would.
    fn synced(&mut self) -> R {
        self.db.write(|s| {
            for row in s.local_subtree(&RelPath::root())? {
                let Some(hash) = row.hash.filter(|_| row.kind == NodeKind::File) else {
                    continue;
                };
                let id = || hbsync_types::Uuid::new_v4();
                s.folder().put_base(&BaseRow {
                    node: row.node,
                    state: NodeState::file(
                        NodeId::ROOT,
                        row.path.name().unwrap_or_default(),
                        FileContent {
                            hash,
                            size: row.fingerprint.size,
                            mtime_ms: 0,
                            executable: false,
                        },
                    ),
                    remote: RemoteRef {
                        file_id: RemoteFileId(id()),
                        version: VersionTag(id()),
                    },
                    content_state: ContentState::Local,
                    local_name: None,
                    local_hash: Some(hash),
                })?;
            }
            Ok(())
        })?;
        Ok(())
    }

    fn node(&mut self, rel: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        let path = RelPath::new(rel)?;
        Ok(self
            .db
            .read(|s| s.get_local(&path))?
            .ok_or(format!("no row for {rel}"))?
            .node)
    }
}

#[test]
fn first_scan_adds_and_hashes_everything() -> R {
    let mut f = Fixture::new()?;
    f.write("a/b/c.txt", b"c")?;
    f.write("a/d.txt", b"d")?;
    f.write("top", b"t")?;
    f.write(".hbsync/state.db", b"ignored")?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.added, report.hashed), (5, 3), "{report:?}");
    assert_eq!(dirty.len(), 5);
    assert_eq!(f.db.read(|s| s.local_count())?, 5);
    let path = RelPath::new("a/d.txt")?;
    let row = f.db.read(|s| s.get_local(&path))?;
    assert!(row.and_then(|r| r.hash).is_some());
    Ok(())
}

#[test]
fn unchanged_rescan_does_nothing_and_hashes_nothing() -> R {
    let mut f = Fixture::new()?;
    f.write("a/b.txt", b"b")?;
    f.scan()?;
    let (report, dirty) = f.scan()?;
    assert_eq!(
        (report.added, report.modified, report.removed, report.hashed),
        (0, 0, 0, 0)
    );
    assert!(dirty.is_empty());
    Ok(())
}

#[test]
fn edit_and_touch() -> R {
    let mut f = Fixture::new()?;
    f.write("f.txt", b"v1")?;
    f.scan()?;
    let node = f.node("f.txt")?;

    f.write("f.txt", b"v2 longer")?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.modified, report.hashed), (1, 1));
    assert_eq!(dirty, [node]);

    // Same bytes, new mtime: re-hashed, but not a change.
    let path = f.root().join("f.txt");
    hbsync_platform::ops::set_mtime(&path, 1_600_000_000_000_000_000)?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.modified, report.hashed), (0, 1));
    assert!(dirty.is_empty());
    Ok(())
}

/// An item a delete moved aside (`.hbsync/aside`, sync semantics §7) isn't gone to the scan:
/// its rows stay, with what is below it, for it to come back to (put back by the delete's check
/// or the next sync's recovery), and nothing is dirtied. Back in place, it is the same node.
#[test]
fn an_item_aside_keeps_its_rows() -> R {
    let mut f = Fixture::new()?;
    f.write("d/a.txt", b"a")?;
    f.write("d/sub/b.txt", b"b")?;
    f.scan()?;
    let (d, a) = (f.node("d")?, f.node("d/a.txt")?);
    let aside = hbsync_local::layout::aside_dir(f.root());
    fs::create_dir_all(&aside)?;
    let entry = aside.join(hbsync_local::layout::AsideEntry::Folder(d).name());
    fs::rename(f.root().join("d"), &entry)?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.removed, report.added), (0, 0), "{report:?}");
    assert!(dirty.is_empty(), "{dirty:?}");
    assert_eq!(f.node("d/a.txt")?, a);
    fs::rename(&entry, f.root().join("d"))?;
    let (report, _) = f.scan()?;
    assert_eq!((report.removed, report.added), (0, 0), "{report:?}");
    assert_eq!((f.node("d")?, f.node("d/a.txt")?), (d, a));
    Ok(())
}

/// A rescan of a folder below one that is aside (a journal's events from before a crash) keeps
/// its rows too: the aside folder's row, in the DB, says they are its.
#[test]
fn a_rescan_below_an_item_aside_keeps_its_rows() -> R {
    let mut f = Fixture::new()?;
    f.write("d/sub/b.txt", b"b")?;
    f.scan()?;
    let (d, b) = (f.node("d")?, f.node("d/sub/b.txt")?);
    let aside = hbsync_local::layout::aside_dir(f.root());
    fs::create_dir_all(&aside)?;
    let entry = aside.join(hbsync_local::layout::AsideEntry::Folder(d).name());
    fs::rename(f.root().join("d"), &entry)?;
    let root = f.root().to_path_buf();
    let report = hbsync_local::rescan_dirs(&mut f.db, &root, &[p("d/sub")], &f.opts)?;
    assert_eq!(report.removed, 0, "{report:?}");
    assert_eq!(f.node("d/sub/b.txt")?, b);
    Ok(())
}

#[test]
fn rename_keeps_node_without_rehash() -> R {
    let mut f = Fixture::new()?;
    f.write("old.txt", b"content")?;
    f.scan()?;
    let node = f.node("old.txt")?;
    fs::rename(f.root().join("old.txt"), f.root().join("new.txt"))?;
    let (report, dirty) = f.scan()?;
    assert_eq!(
        (report.moved, report.added, report.removed),
        (1, 0, 0),
        "{report:?}"
    );
    // Only file systems that never recycle ids can skip the hash on a lone rename.
    let expected_hashes = usize::from(f.opts.fs_family.reuses_file_ids());
    if f.opts.fs_family.has_stable_file_ids() {
        assert_eq!(report.hashed, expected_hashes);
    }
    assert_eq!(f.node("new.txt")?, node);
    assert_eq!(dirty, [node]);
    Ok(())
}

/// A new file id at a known path is the same node, edited (an editor's save through a temp
/// file, even one a scan saw; one keeping a backup by renaming), unless another synced file was
/// moved onto the name: then that file keeps its node there (from a path that is gone, along a
/// chain of such moves, or swapped), and the one that was there is gone, or moved on.
#[test]
fn a_new_file_id_at_a_path_is_an_edit_unless_a_synced_file_was_moved_onto_it() -> R {
    let mut f = Fixture::new()?;
    if !f.opts.fs_family.has_stable_file_ids() {
        return Ok(());
    }
    let names = [
        "doc.txt",
        "doc2.txt",
        "notes.txt",
        "notes.txt~",
        "a",
        "b",
        "c",
        "d",
    ];
    for name in names.iter().chain(&["e", "x", "f", "g", "h"]) {
        f.write(name, name.as_bytes())?;
    }
    f.scan()?;
    f.synced()?;
    let mut nodes = std::collections::HashMap::new();
    for name in names.iter().chain(&["e", "x", "f", "g", "h"]) {
        nodes.insert(*name, f.node(name)?);
    }
    // An editor's temp file, seen by a scan before it replaces doc2.txt.
    f.write(".doc2.txt.tmp", b"doc2 v2")?;
    f.scan()?;
    let root = f.root().to_path_buf();
    // Every file replaced below stays open until the scan, so no file made after it gets the id
    // it frees (ext4 hands a freed inode out again at once: a new notes.txt took the old
    // notes.txt~'s, which reads as the two swapping). An open handle doesn't hold a rename over
    // a file on Unix.
    #[cfg(unix)]
    let held = ["doc.txt", "doc2.txt", "notes.txt~", "x", "h"]
        .iter()
        .map(|name| fs::File::open(root.join(name)))
        .collect::<Result<Vec<_>, _>>()?;
    let mv = |from: &str, to: &str| fs::rename(root.join(from), root.join(to));
    mv(".doc2.txt.tmp", "doc2.txt")?;
    mv("d", "y")?; // d steps aside, and c takes its name
    mv("c", "d")?;
    mv("e", "x")?; // e is moved over x: x is gone
    mv("a", "t")?; // a and b swap
    mv("b", "a")?;
    mv("t", "b")?;
    mv("g", "h")?; // g over h, then f over g
    mv("f", "g")?;
    // Last, so no file made after them gets an id they freed: a save through a temp file, and
    // one keeping a backup by renaming over the old backup.
    f.write(".doc.txt.tmp", b"doc v2")?;
    mv(".doc.txt.tmp", "doc.txt")?;
    mv("notes.txt", "notes.txt~")?;
    f.write("notes.txt", b"notes v2")?;
    let (report, _) = f.scan()?;
    #[cfg(unix)]
    drop(held);

    let mut expect = |path: &str, was: &str| -> R {
        assert_eq!(f.node(path)?, nodes[was], "{path} holds {was}");
        Ok(())
    };
    for (path, was) in [
        ("doc.txt", "doc.txt"),
        ("doc2.txt", "doc2.txt"),
        ("notes.txt", "notes.txt"),
        ("notes.txt~", "notes.txt~"),
        ("y", "d"),
        ("d", "c"),
        ("x", "e"),
        ("a", "b"),
        ("b", "a"),
        ("g", "f"),
        ("h", "g"),
    ] {
        expect(path, was)?;
    }
    for gone in ["x", "h"] {
        let node = nodes[gone];
        assert!(
            f.db.read(|s| s.local_by_node(node))?.is_none(),
            "{gone} is gone"
        );
    }
    // Moved: d, c, e, a, b, f, g. Removed: x, h and the temp file.
    assert_eq!(
        (report.moved, report.added, report.removed),
        (7, 0, 3),
        "{report:?}"
    );
    Ok(())
}

/// A synced file moved over another, a folder then made where it was: it left its path (a
/// folder is no file there), so it keeps its node at the new name, and the file that was there
/// is gone. Read as an edit there, its bytes went up twice: as the node the other device moved
/// elsewhere meanwhile, and as this device's edit of the file it replaced (a milestone seed's
/// copies, `randomized_wrong_not_theres`, seed 6526174988904104872).
#[test]
fn a_file_moved_over_another_keeps_its_node_when_a_folder_takes_its_place() -> R {
    let mut f = Fixture::new()?;
    if !f.opts.fs_family.has_stable_file_ids() {
        return Ok(());
    }
    f.write("b", b"b")?;
    f.write("c", b"c")?;
    f.scan()?;
    f.synced()?;
    let (b, c) = (f.node("b")?, f.node("c")?);
    let root = f.root().to_path_buf();
    // Held open: no file made after it gets the id it frees.
    #[cfg(unix)]
    let held = fs::File::open(root.join("c"))?;
    fs::rename(root.join("b"), root.join("c"))?;
    fs::create_dir(root.join("b"))?;
    let (report, _) = f.scan()?;
    #[cfg(unix)]
    drop(held);
    assert_eq!(f.node("c")?, b, "c holds b");
    assert!(f.db.read(|s| s.local_by_node(c))?.is_none(), "c is gone");
    assert_ne!(f.node("b")?, b, "the folder is new");
    assert_eq!((report.moved, report.removed), (1, 1), "{report:?}");
    Ok(())
}

#[test]
fn folder_rename_dirties_only_the_folder() -> R {
    let mut f = Fixture::new()?;
    f.write("dir/x", b"x")?;
    f.write("dir/sub/y", b"y")?;
    f.scan()?;
    let (folder, x, y) = (f.node("dir")?, f.node("dir/x")?, f.node("dir/sub/y")?);
    fs::rename(f.root().join("dir"), f.root().join("renamed"))?;
    let (report, dirty) = f.scan()?;
    assert_eq!(report.moved, 1, "{report:?}");
    assert_eq!(
        report.hashed, 0,
        "files that moved with their folder aren't re-hashed"
    );
    assert_eq!(dirty, [folder]);
    assert_eq!((f.node("renamed/x")?, f.node("renamed/sub/y")?), (x, y));
    Ok(())
}

#[test]
fn move_by_hash_when_file_id_changes() -> R {
    let mut f = Fixture::new()?;
    f.write("a/report.pdf", b"unique report bytes")?;
    f.scan()?;
    let node = f.node("a/report.pdf")?;
    // Copy + delete (as across volumes): new file id, same content.
    f.write("b/report.pdf", b"unique report bytes")?;
    fs::remove_file(f.root().join("a/report.pdf"))?;
    let (report, dirty) = f.scan()?;
    assert_eq!(f.node("b/report.pdf")?, node);
    assert_eq!((report.moved, report.removed), (1, 0), "{report:?}");
    // The new folder "b" is added and the file moved into it.
    assert!(dirty.contains(&node));
    Ok(())
}

#[test]
fn ambiguous_hash_is_not_a_move() -> R {
    let mut f = Fixture::new()?;
    f.write("one", b"same")?;
    f.write("two", b"same")?;
    f.scan()?;
    // Hard links outside the root keep both inodes alive, so the new file can't reuse one of
    // them (which would be a legitimate id match).
    let keep = tempfile::tempdir()?;
    fs::hard_link(f.root().join("one"), keep.path().join("one"))?;
    fs::hard_link(f.root().join("two"), keep.path().join("two"))?;
    fs::remove_file(f.root().join("one"))?;
    fs::remove_file(f.root().join("two"))?;
    f.write("three", b"same")?;
    let (report, _) = f.scan()?;
    assert_eq!((report.added, report.removed, report.moved), (1, 2, 0));
    Ok(())
}

#[test]
fn recycled_inode_with_new_content_is_not_a_move() -> R {
    let mut f = Fixture::new()?;
    f.write("old", b"old content")?;
    f.scan()?;
    let old = f.node("old")?;
    let old_id = hbsync_platform::stat(&f.root().join("old"))?.file_id;
    fs::remove_file(f.root().join("old"))?;
    f.write("new", b"different")?;
    let new_id = hbsync_platform::stat(&f.root().join("new"))?.file_id;
    let (report, _) = f.scan()?;
    if f.opts.fs_family.reuses_file_ids() || old_id != new_id {
        assert_eq!(
            (report.added, report.removed, report.moved),
            (1, 1, 0),
            "{report:?}"
        );
        assert_ne!(f.node("new")?, old);
    }
    Ok(())
}

#[test]
fn recycled_inode_with_same_size_and_mtime_is_rehashed() -> R {
    let mut f = Fixture::new()?;
    f.write("a.bin", b"AAAA")?;
    f.scan()?;
    let old = f.node("a.bin")?;
    let meta = hbsync_platform::stat(&f.root().join("a.bin"))?;
    // Replace with different bytes of the same size and the same mtime (like `rsync -a`),
    // through a rename so the inode is certainly the old one.
    fs::write(f.root().join("tmp"), b"BBBB")?;
    hbsync_platform::ops::set_mtime(&f.root().join("tmp"), meta.mtime_ns)?;
    fs::remove_file(f.root().join("a.bin"))?;
    let tmp_meta = hbsync_platform::stat(&f.root().join("tmp"))?;
    fs::rename(f.root().join("tmp"), f.root().join("b.bin"))?;
    let (report, _) = f.scan()?;
    let b =
        f.db.read(|s| {
            s.get_local(
                &RelPath::new("b.bin")
                    .map_err(|e| hbsync_state::StateError::Corrupt(e.to_string()))?,
            )
        })?
        .ok_or("row")?;
    // Whatever identity it gets, the recorded hash must be the real one.
    assert_eq!(
        b.hash,
        Some(hbsync_fileio::hash_bytes(
            b"BBBB",
            hbsync_fileio::HashAlgorithm::Blake3
        ))
    );
    if f.opts.fs_family.reuses_file_ids() && tmp_meta.file_id == meta.file_id {
        assert_ne!(b.node, old, "{report:?}");
    }
    Ok(())
}

#[test]
fn file_id_spoof_cannot_skip_hashing() -> R {
    // A file that keeps the old id, size and mtime but has new bytes, appearing under a new
    // name, is re-hashed on file systems that recycle ids.
    let mut f = Fixture::new()?;
    f.write("x", b"1111")?;
    f.scan()?;
    let meta = hbsync_platform::stat(&f.root().join("x"))?;
    fs::rename(f.root().join("x"), f.root().join("y"))?;
    fs::write(f.root().join("y"), b"2222")?;
    hbsync_platform::ops::set_mtime(&f.root().join("y"), meta.mtime_ns)?;
    let (report, _) = f.scan()?;
    let y =
        f.db.read(|s| {
            s.get_local(
                &RelPath::new("y").map_err(|e| hbsync_state::StateError::Corrupt(e.to_string()))?,
            )
        })?
        .ok_or("row")?;
    if f.opts.fs_family.reuses_file_ids() {
        assert_eq!(
            y.hash,
            Some(hbsync_fileio::hash_bytes(
                b"2222",
                hbsync_fileio::HashAlgorithm::Blake3
            )),
            "{report:?}"
        );
    }
    Ok(())
}

#[test]
fn delete_and_kind_change() -> R {
    let mut f = Fixture::new()?;
    f.write("gone", b"x")?;
    f.write("thing", b"file")?;
    f.scan()?;
    let (gone, thing) = (f.node("gone")?, f.node("thing")?);
    fs::remove_file(f.root().join("gone"))?;
    fs::remove_file(f.root().join("thing"))?;
    fs::create_dir(f.root().join("thing"))?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.removed, report.added), (2, 1), "{report:?}");
    let new_thing = f.node("thing")?;
    assert_ne!(new_thing, thing);
    for n in [gone, thing, new_thing] {
        assert!(dirty.contains(&n));
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn unreadable_folder_is_not_a_delete() -> R {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new()?;
    f.write("locked/keep.txt", b"x")?;
    f.scan()?;
    let locked = f.root().join("locked");
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
    if fs::read_dir(&locked).is_ok() {
        return Ok(()); // root ignores permissions
    }
    let result = f.scan();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
    let (report, dirty) = result?;
    assert_eq!(report.removed, 0, "{report:?}");
    assert_eq!(report.unreadable.len(), 1);
    assert!(dirty.is_empty());
    assert!(f.node("locked/keep.txt").is_ok());
    Ok(())
}

#[cfg(unix)]
#[test]
fn contents_follow_a_folder_moved_somewhere_unreadable() -> R {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new()?;
    f.write("a/x.txt", b"x")?;
    f.scan()?;
    let x = f.node("a/x.txt")?;
    fs::rename(f.root().join("a"), f.root().join("b"))?;
    let b = f.root().join("b");
    fs::set_permissions(&b, fs::Permissions::from_mode(0o000))?;
    if fs::read_dir(&b).is_ok() {
        fs::set_permissions(&b, fs::Permissions::from_mode(0o755))?;
        return Ok(()); // root ignores permissions
    }
    let result = f.scan();
    fs::set_permissions(&b, fs::Permissions::from_mode(0o755))?;
    let (report, dirty) = result?;
    assert_eq!(report.removed, 0, "{report:?}");
    assert!(
        !dirty.contains(&x),
        "x moved with its folder; it wasn't deleted"
    );
    assert_eq!(f.node("b/x.txt")?, x);
    Ok(())
}

#[cfg(unix)]
#[test]
fn executable_bit_change_is_a_change() -> R {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new()?;
    f.write("run.sh", b"#!/bin/sh")?;
    f.scan()?;
    let node = f.node("run.sh")?;
    fs::set_permissions(f.root().join("run.sh"), fs::Permissions::from_mode(0o755))?;
    let (_, dirty) = f.scan()?;
    assert_eq!(dirty, [node]);
    Ok(())
}

mod targeted {
    use super::*;
    use hbsync_local::rescan_dirs;

    impl Fixture {
        fn rescan(
            &mut self,
            dirs: &[&str],
        ) -> Result<(ScanReport, Vec<NodeId>), Box<dyn std::error::Error>> {
            let root = self.dir.path().to_path_buf();
            let dirs: Vec<RelPath> = dirs.iter().map(|d| p(d)).collect();
            let report = rescan_dirs(&mut self.db, &root, &dirs, &self.opts)?;
            Ok((report, self.take_dirty()?))
        }

        /// (path, kind, hash) of every row: what a full scan would record, minus node ids.
        fn snapshot(&mut self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            Ok(self
                .db
                .read(|s| s.local_subtree(&RelPath::root()))?
                .into_iter()
                .map(|r| format!("{} {:?} {:?}", r.path, r.kind, r.hash))
                .collect())
        }
    }

    #[test]
    fn added_file_in_subfolder() -> R {
        let mut f = Fixture::new()?;
        f.write("a/b/old", b"1")?;
        f.scan()?;
        f.write("a/b/new", b"2")?;
        let (report, dirty) = f.rescan(&["a/b"])?;
        assert_eq!((report.added, report.removed), (1, 0));
        assert_eq!(dirty, [f.node("a/b/new")?]);
        Ok(())
    }

    #[test]
    fn an_event_inside_a_new_folder_records_the_folder_too() -> R {
        let mut f = Fixture::new()?;
        f.write("old", b"o")?;
        f.scan()?;
        f.write("new/sub/file", b"n")?;
        // The watcher delivered only the event for the file (the folder's came separately).
        let (report, _) = f.rescan(&["new/sub"])?;
        assert_eq!(report.added, 3, "{report:?}");
        assert!(f.node("new").is_ok() && f.node("new/sub").is_ok());
        Ok(())
    }

    #[test]
    fn untouched_sibling_subtrees_are_left_alone() -> R {
        let mut f = Fixture::new()?;
        f.write("deep/er/file", b"x")?;
        f.write("top", b"t")?;
        f.scan()?;
        f.write("top2", b"t2")?;
        let (report, _) = f.rescan(&[""])?;
        assert_eq!((report.added, report.removed), (1, 0), "{report:?}");
        assert!(f.node("deep/er/file").is_ok());
        Ok(())
    }

    #[test]
    fn move_between_folders_keeps_node() -> R {
        let mut f = Fixture::new()?;
        f.write("a/doc.txt", b"doc")?;
        fs::create_dir(f.root().join("b"))?;
        f.scan()?;
        let node = f.node("a/doc.txt")?;
        fs::rename(f.root().join("a/doc.txt"), f.root().join("b/doc.txt"))?;
        let (report, dirty) = f.rescan(&["a", "b"])?;
        assert_eq!(
            (report.moved, report.added, report.removed),
            (1, 0, 0),
            "{report:?}"
        );
        assert_eq!(f.node("b/doc.txt")?, node);
        assert_eq!(dirty, [node]);
        Ok(())
    }

    #[test]
    fn moved_folder_brings_its_subtree() -> R {
        let mut f = Fixture::new()?;
        f.write("x/inner/file", b"f")?;
        fs::create_dir(f.root().join("y"))?;
        f.scan()?;
        let (folder, file) = (f.node("x")?, f.node("x/inner/file")?);
        fs::rename(f.root().join("x"), f.root().join("y/x"))?;
        let (report, dirty) = f.rescan(&["", "y"])?;
        assert_eq!(report.moved, 1, "{report:?}");
        assert_eq!(dirty, [folder]);
        assert_eq!((f.node("y/x")?, f.node("y/x/inner/file")?), (folder, file));
        Ok(())
    }

    #[test]
    fn the_state_folder_is_never_rescanned() -> R {
        let mut f = Fixture::new()?;
        f.write("real.txt", b"r")?;
        f.write(".hbsync/state.db", b"db")?;
        f.write(".hbsync/aside/x/old.txt", b"old")?;
        f.scan()?;
        // A journal reports changes inside .hbsync (it changes on every run).
        let (report, dirty) = f.rescan(&[".hbsync", ".hbsync/aside/x"])?;
        assert_eq!((report.added, dirty.len()), (0, 0), "{report:?}");
        assert_eq!(f.db.read(|s| s.local_count())?, 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_folder_replaced_by_a_symlink_is_not_followed() -> R {
        let mut f = Fixture::new()?;
        f.write("a/inner/mine", b"m")?;
        f.scan()?;
        let outside = tempfile::tempdir()?;
        fs::create_dir_all(outside.path().join("inner"))?;
        fs::write(outside.path().join("inner/secret"), b"s")?;
        fs::write(outside.path().join("other"), b"o")?;
        fs::remove_dir_all(f.root().join("a"))?;
        std::os::unix::fs::symlink(outside.path(), f.root().join("a"))?;
        // The watcher reports both the link and the folder that was inside it.
        f.rescan(&["a/inner", "a"])?;
        let rows = f.snapshot()?;
        assert!(rows.iter().all(|r| !r.starts_with("a")), "{rows:?}");
        Ok(())
    }

    #[test]
    fn deleted_folder_removes_its_subtree() -> R {
        let mut f = Fixture::new()?;
        f.write("gone/a/b", b"b")?;
        f.write("stay", b"s")?;
        f.scan()?;
        fs::remove_dir_all(f.root().join("gone"))?;
        // The journal reported the vanished folder itself; the rescan climbs to its parent.
        let (report, _) = f.rescan(&["gone/a"])?;
        assert_eq!(report.removed, 3, "{report:?}");
        assert!(f.node("stay").is_ok());
        Ok(())
    }

    #[test]
    fn targeted_rescan_equals_full_scan() -> R {
        let mut f = Fixture::new()?;
        for path in ["a/1", "a/2", "a/s/3", "b/4", "c/d/e/5", "6"] {
            f.write(path, path.as_bytes())?;
        }
        f.scan()?;

        // A batch of edits, with the folders a journal would report.
        f.write("a/1", b"edited")?;
        fs::remove_file(f.root().join("a/2"))?;
        fs::rename(f.root().join("a/s"), f.root().join("b/s"))?;
        fs::rename(f.root().join("c/d"), f.root().join("dd"))?;
        f.write("c/new/deep/7", b"7")?;
        f.write("8", b"8")?;
        f.rescan(&["a", "b", "c", "c/d", "c/new", ""])?;
        let targeted = f.snapshot()?;

        let mut fresh = Fixture {
            dir: tempfile::tempdir()?,
            db: StateDb::open_in_memory()?,
            opts: f.opts.clone(),
        };
        std::mem::swap(&mut fresh.dir, &mut f.dir);
        fresh.scan()?;
        assert_eq!(targeted, fresh.snapshot()?);
        Ok(())
    }
}

#[test]
fn filtered_items_never_reach_the_db_and_a_newly_ignored_one_looks_deleted() -> R {
    let mut f = Fixture::new()?;
    f.write("a/.DS_Store", b"junk")?;
    f.write("a/notes.txt", b"n")?;
    f.write("a/build/out.o", b"o")?;
    f.write("a/run.log", b"l")?;
    let (report, _) = f.scan()?;
    assert_eq!(report.added, 5, "{report:?}"); // a, notes, build, out.o, run.log
    assert_eq!(report.skipped.len(), 1);

    // A rule appears: what it matches leaves the DB (a delete to sync), never the disk.
    f.opts.filter = Arc::new(Filter::from_rules("build/\n*.log\n", false)?);
    let (report, dirty) = f.scan()?;
    assert_eq!(report.removed, 3, "{report:?}");
    assert_eq!(dirty.len(), 3);
    for rel in ["a/build", "a/build/out.o", "a/run.log"] {
        let path = RelPath::new(rel)?;
        assert!(f.db.read(|s| s.get_local(&path))?.is_none(), "{rel}");
        assert!(f.root().join(rel).exists(), "{rel}");
    }
    // Changes inside an ignored folder are never looked at, whoever reports them.
    f.write("a/build/more.o", b"m")?;
    let root = f.root().to_path_buf();
    let report = rescan_dirs(&mut f.db, &root, &[RelPath::new("a/build")?], &f.opts)?;
    assert_eq!((report.added, report.removed), (0, 0), "{report:?}");

    // The rule goes: they are new again.
    f.opts.filter = Arc::default();
    let (report, _) = f.scan()?;
    assert_eq!(report.added, 4, "{report:?}");
    Ok(())
}

/// A tag change is a change of its own: the scan reads the tags and marks the node dirty,
/// without the bytes having changed. Tags that couldn't be read are read again.
#[test]
fn tags_are_read_and_a_tag_change_is_a_change() -> R {
    use hbsync_platform::tags::{self, Tag, Tags};
    let mut f = Fixture::new()?;
    f.write("doc.txt", b"text")?;
    if !tags::CAN_TAG || tags::write(&f.root().join("doc.txt"), &Tags::default())?.is_none() {
        eprintln!("no tags here; skipped");
        return Ok(());
    }
    f.scan()?;
    let node = f.node("doc.txt")?;
    let path = RelPath::new("doc.txt")?;
    let row = f.db.read(|s| s.get_local(&path))?.ok_or("no row")?;
    assert_eq!(row.tags, Some(Tags::default()));

    std::thread::sleep(std::time::Duration::from_millis(20)); // past a coarse clock tick
    let red = Tags::new([Tag::new("Red", 0)]);
    tags::write(&f.root().join("doc.txt"), &red)?;
    let (report, dirty) = f.scan()?;
    assert_eq!(dirty, [node]);
    assert_eq!(report.modified, 1);
    let row = f.db.read(|s| s.get_local(&path))?.ok_or("no row")?;
    assert_eq!(
        (row.tags.clone(), row.hash.is_some()),
        (Some(red.clone()), true)
    );
    assert!(f.scan()?.1.is_empty(), "nothing changed since");

    // Tags that couldn't be read are tried again at the next scan, nothing else changing.
    f.db.write(|s| {
        s.folder()
            .put_local(&hbsync_state::LocalRow { tags: None, ..row })
    })?;
    assert_eq!(f.scan()?.1, [node]);
    let read = f.db.read(|s| s.get_local(&path))?.and_then(|r| r.tags);
    assert_eq!(read, Some(red));
    Ok(())
}

/// Entries a watcher's events name, looked at by themselves where that is enough
/// (`rescan_entries`), on a volume with byte-exact names and on one without.
mod entries {
    use super::*;
    use hbsync_local::rescan_entries;

    impl Fixture {
        fn look(
            &mut self,
            exact: bool,
            entries: &[&str],
        ) -> Result<(ScanReport, Vec<NodeId>), Box<dyn std::error::Error>> {
            self.opts.exact_names = exact;
            let root = self.dir.path().to_path_buf();
            let entries: Vec<RelPath> = entries.iter().map(|e| p(e)).collect();
            let report = rescan_entries(&mut self.db, &root, &[], &entries, &self.opts)?;
            Ok((report, self.take_dirty()?))
        }

        fn big() -> Result<Self, Box<dyn std::error::Error>> {
            let mut f = Fixture::new()?;
            for i in 0..20 {
                f.write(&format!("big/f{i}"), format!("{i}").as_bytes())?;
            }
            f.scan()?;
            Ok(f)
        }
    }

    fn both(test: impl Fn(bool) -> R) -> R {
        for exact in [true, false] {
            test(exact).map_err(|e| format!("exact names {exact}: {e}"))?;
        }
        Ok(())
    }

    /// An edit in place is seen by itself, on any volume: the folder isn't listed (an edit
    /// no event named isn't seen).
    #[test]
    fn an_edit_is_seen_alone() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            f.write("big/f7", b"edited")?;
            f.write("big/f8", b"edited too")?;
            let (report, dirty) = f.look(exact, &["big/f7"])?;
            assert_eq!(report.modified, 1, "{report:?}");
            assert_eq!(dirty, [f.node("big/f7")?]);
            Ok(())
        })
    }

    /// The client's own write, named again by the watcher, changes nothing.
    #[test]
    fn an_unchanged_entry_changes_nothing() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            let (report, dirty) = f.look(exact, &["big/f1", "big/f2"])?;
            assert_eq!((report.modified, report.hashed), (0, 0), "{report:?}");
            assert!(dirty.is_empty());
            Ok(())
        })
    }

    /// A new file is added by itself. Where names aren't byte-exact, one whose name another
    /// row shares under case and normalization (maybe another spelling of it) rescans its
    /// folder, which finds the rest too.
    #[test]
    fn a_new_file() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            f.write("big/new", b"n")?;
            f.write("big/unnamed", b"u")?;
            let (report, _) = f.look(exact, &["big/new"])?;
            assert_eq!(report.added, 1, "{report:?}");
            assert!(f.node("big/new").is_ok());
            // On a volume that folds case this writes f1 itself: its file id is a row's, and the
            // folder is rescanned either way.
            let folds = !hbsync_platform::fskind::probe_case_sensitive(f.root())?;
            f.write("big/F1", b"another spelling?")?;
            let (report, _) = f.look(exact, &["big/F1"])?;
            assert_eq!(f.node("big/unnamed").is_ok(), !exact || folds, "{report:?}");
            assert_eq!(f.node("big/F1").is_ok(), !folds, "{report:?}");
            Ok(())
        })
    }

    #[test]
    fn a_deleted_file() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            let node = f.node("big/f3")?;
            fs::remove_file(f.root().join("big/f3"))?;
            let (report, dirty) = f.look(exact, &["big/f3"])?;
            assert_eq!(report.removed, 1, "{report:?}");
            assert_eq!(dirty, [node]);
            Ok(())
        })
    }

    /// A rename, also one of case only, keeps its node: both names are looked at together.
    #[test]
    fn a_rename_keeps_its_node() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            for (from, to) in [("big/f4", "big/g4"), ("big/f5", "big/F5")] {
                let node = f.node(from)?;
                fs::rename(f.root().join(from), f.root().join(to))?;
                let (report, dirty) = f.look(exact, &[from, to])?;
                assert_eq!(
                    (report.moved, report.added, report.removed),
                    (1, 0, 0),
                    "{report:?}"
                );
                assert_eq!(f.node(to)?, node);
                assert_eq!(dirty, [node]);
            }
            Ok(())
        })
    }

    /// A change of metadata alone (a tag, a mode) is looked at alone on any volume: the same
    /// file at its row's path. The folder isn't listed (a new file no event named isn't seen).
    #[test]
    fn a_metadata_change_is_seen_alone() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            let path = f.root().join("big/f6");
            let mode = fs::metadata(&path)?.permissions();
            fs::set_permissions(&path, mode)?;
            f.write("big/unnamed", b"u")?;
            let (report, _) = f.look(exact, &["big/f6"])?;
            assert_eq!(report.added, 0, "{report:?}");
            Ok(())
        })
    }

    /// The root gone (a volume unmounted, the folder renamed away): nothing at the top is
    /// taken for deleted.
    #[test]
    fn a_missing_root_deletes_nothing() -> R {
        both(|exact| {
            let mut f = Fixture::new()?;
            f.write("top", b"t")?;
            f.scan()?;
            let away = tempfile::tempdir()?;
            let gone = away.path().join("moved");
            fs::rename(f.root(), &gone)?;
            let result = f.look(exact, &["top"]);
            fs::rename(&gone, f.root())?;
            let (report, _) = result?;
            assert_eq!(report.removed, 0, "{report:?}");
            assert!(f.node("top").is_ok());
            Ok(())
        })
    }

    /// Folders are their folder's to judge: a new one is walked whole, a gone one takes its
    /// contents (named too) along once.
    #[test]
    fn folders_rescan_their_folder() -> R {
        both(|exact| {
            let mut f = Fixture::big()?;
            f.write("big/sub/x", b"x")?;
            let (report, _) = f.look(exact, &["big/sub", "big/sub/x"])?;
            assert_eq!(report.added, 2, "{report:?}");
            fs::remove_dir_all(f.root().join("big/sub"))?;
            let (report, _) = f.look(exact, &["big/sub", "big/sub/x"])?;
            assert_eq!(report.removed, 2, "{report:?}");
            // A file in a folder not known yet: the folders above are recorded with it.
            f.write("new/deep/file", b"n")?;
            let (report, _) = f.look(exact, &["new/deep/file"])?;
            assert_eq!(report.added, 3, "{report:?}");
            assert!(f.node("new/deep").is_ok());
            Ok(())
        })
    }

    /// Files on disk, by path, in walk order.
    fn files(root: &Path, dir: &str, out: &mut Vec<String>) {
        let mut names: Vec<String> = fs::read_dir(root.join(dir))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        names.sort();
        for name in names {
            let rel = if dir.is_empty() {
                name
            } else {
                format!("{dir}/{name}")
            };
            if root.join(&rel).is_dir() {
                files(root, &rel, out);
            } else {
                out.push(rel);
            }
        }
    }

    /// Changes as a watcher reports them (every path an event names) leave the DB as a full
    /// scan would, and a renamed file keeps its node.
    fn events_equal_a_full_scan(exact: bool, ops: &[(u8, u8, u8)]) -> R {
        let mut f = Fixture::new()?;
        for path in ["a/1", "a/2", "a/s/3", "b/4", "5"] {
            f.write(path, path.as_bytes())?;
        }
        f.scan()?;
        let folders = ["", "a", "b", "a/s", "c"];
        let mut entries: Vec<String> = Vec::new();
        let mut renamed: Vec<(String, NodeId)> = Vec::new();
        // Edited since the scan: renamed too, a new node where inodes are recycled.
        let mut edited: Vec<String> = Vec::new();
        for (i, &(op, x, y)) in ops.iter().enumerate() {
            let mut on_disk = Vec::new();
            files(f.root(), "", &mut on_disk);
            on_disk.retain(|p| !p.starts_with(".hbsync"));
            let pick = |n: u8| on_disk.get(n as usize % on_disk.len().max(1)).cloned();
            let folder = |n: u8| folders[n as usize % folders.len()];
            let join = |d: &str, name: String| {
                if d.is_empty() {
                    name
                } else {
                    format!("{d}/{name}")
                }
            };
            match op % 5 {
                0 => {
                    // A new file (its folder made if needed).
                    let path = join(folder(x), format!("n{i}"));
                    if let Some(d) = Path::new(&path)
                        .parent()
                        .filter(|d| !f.root().join(d).exists())
                    {
                        entries.push(d.to_string_lossy().into_owned());
                    }
                    f.write(&path, format!("new {i} {y}").as_bytes())?;
                    entries.push(path);
                }
                1 => {
                    if let Some(p) = pick(x) {
                        f.write(&p, format!("edit {i} {y}").as_bytes())?;
                        renamed.retain(|(r, _)| *r != p);
                        edited.push(p.clone());
                        entries.push(p);
                    }
                }
                2 => {
                    if let Some(p) = pick(x) {
                        fs::remove_file(f.root().join(&p))?;
                        entries.push(p);
                    }
                }
                3 => {
                    // A rename, maybe of case only, maybe into another folder.
                    if let Some(p) = pick(x) {
                        let name = Path::new(&p)
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        let to = if y % 2 == 0 {
                            join(
                                Path::new(&p)
                                    .parent()
                                    .map(|d| d.to_string_lossy().into_owned())
                                    .unwrap_or_default()
                                    .as_str(),
                                name.to_uppercase() + "x",
                            )
                        } else {
                            join(folder(y), format!("{name}-m{i}"))
                        };
                        let to_dir = f.root().join(&to);
                        if to_dir.exists() || !to_dir.parent().is_some_and(|d| d.is_dir()) {
                            continue;
                        }
                        let node = f.node(&p).ok().filter(|_| !edited.contains(&p));
                        if edited.contains(&p) {
                            edited.push(to.clone());
                        }
                        fs::rename(f.root().join(&p), &to_dir)?;
                        entries.push(p);
                        entries.push(to.clone());
                        if let Some(node) = node {
                            renamed.retain(|(_, n)| *n != node);
                            renamed.push((to, node));
                        }
                    }
                }
                _ => {
                    // A folder renamed.
                    let from = folder(x);
                    if from.is_empty() || !f.root().join(from).is_dir() {
                        continue;
                    }
                    let to = format!("r{i}");
                    fs::rename(f.root().join(from), f.root().join(&to))?;
                    entries.push(from.to_owned());
                    entries.push(to);
                }
            }
        }
        let renamed: Vec<(String, NodeId)> = renamed
            .into_iter()
            .filter(|(p, _)| f.root().join(p).is_file())
            .collect();
        let root = f.root().to_path_buf();
        f.opts.exact_names = exact;
        let entries: Vec<RelPath> = entries.iter().map(|e| p(e)).collect();
        rescan_entries(&mut f.db, &root, &[], &entries, &f.opts)?;
        for (path, node) in &renamed {
            if let Ok(now) = f.node(path) {
                assert_eq!(now, *node, "{path} kept its node");
            }
        }
        let watched: Vec<String> =
            f.db.read(|s| s.local_subtree(&RelPath::root()))?
                .into_iter()
                .map(|r| format!("{} {:?} {:?}", r.path, r.kind, r.hash))
                .collect();
        let mut fresh = Fixture {
            dir: tempfile::tempdir()?,
            db: StateDb::open_in_memory()?,
            opts: f.opts.clone(),
        };
        std::mem::swap(&mut fresh.dir, &mut f.dir);
        fresh.scan()?;
        let full: Vec<String> = fresh
            .db
            .read(|s| s.local_subtree(&RelPath::root()))?
            .into_iter()
            .map(|r| format!("{} {:?} {:?}", r.path, r.kind, r.hash))
            .collect();
        assert_eq!(watched, full);
        Ok(())
    }

    proptest::proptest! {
        #[test]
        fn watched_changes_equal_a_full_scan(
            exact in proptest::bool::ANY,
            ops in proptest::collection::vec((0u8..5, 0u8..16, 0u8..16), 1..8),
        ) {
            events_equal_a_full_scan(exact, &ops).map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
        }
    }
}

/// Now, in nanoseconds since the Unix epoch, `secs` on.
fn now_ns(secs: i64) -> i64 {
    hbsync_platform::meta::system_time_ns(std::time::SystemTime::now()) + secs * 1_000_000_000
}

/// The row of `rel`'s hash.
fn hash_of(
    f: &mut Fixture,
    rel: &str,
) -> Result<Option<hbsync_types::ContentHash>, Box<dyn std::error::Error>> {
    let path = RelPath::new(rel)?;
    Ok(f.db.read(|s| s.get_local(&path))?.ok_or("no row")?.hash)
}

/// A large file being written isn't read at every look, but once it has been quiet: till then
/// it is unhashed and dirty, as one that couldn't be read is, and said so with how long it has
/// left. A small one is read at once; so is one whose time is ahead of the clock (it would
/// never get old), and any without [`ScanOptions::settle`].
#[test]
fn a_large_file_being_written_is_read_once_it_is_quiet() -> R {
    let mut f = Fixture::new()?;
    let quiet = std::time::Duration::from_secs(60);
    f.opts.settle = Some(Settle {
        from_size: 4,
        quiet,
    });
    f.write("big", b"growing")?;
    f.write("small", b"abc")?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.added, report.hashed), (2, 1), "{report:?}");
    let young: Vec<String> = report.young.iter().map(|y| y.path.to_string()).collect();
    assert_eq!(young, ["big"]);
    assert!(report.young[0].left <= quiet && report.young[0].left > quiet / 2);
    assert!(report.unhashed.is_empty(), "{report:?}");
    let big = f.node("big")?;
    assert!(dirty.contains(&big));
    assert_eq!(hash_of(&mut f, "big")?, None);
    assert!(hash_of(&mut f, "small")?.is_some());

    // Written to again: still left.
    f.write("big", b"growing more")?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.hashed, report.young.len()), (0, 1), "{report:?}");
    assert_eq!(dirty, [big]);
    assert_eq!(hash_of(&mut f, "big")?, None);

    // Quiet for long enough: read.
    hbsync_platform::ops::set_mtime(&f.root().join("big"), now_ns(-120))?;
    let (report, dirty) = f.scan()?;
    assert_eq!((report.hashed, report.young.len()), (1, 0), "{report:?}");
    assert_eq!(dirty, [big]);
    assert!(hash_of(&mut f, "big")?.is_some());

    // A time ahead of the clock isn't recent.
    f.write("big", b"from the future")?;
    hbsync_platform::ops::set_mtime(&f.root().join("big"), now_ns(86_400))?;
    let (report, _) = f.scan()?;
    assert_eq!((report.hashed, report.young.len()), (1, 0), "{report:?}");

    // Without it, a changed file is read at once.
    f.opts.settle = None;
    f.write("big", b"written just now")?;
    let (report, _) = f.scan()?;
    assert_eq!((report.hashed, report.young.len()), (1, 0), "{report:?}");
    Ok(())
}

/// A new file that may be a vanished one moved (the same size: copied, then the original
/// deleted) is read at once though it was just written: its hash tells whether it keeps the
/// node.
#[test]
fn a_new_file_that_may_be_a_move_is_read_while_it_is_written() -> R {
    let mut f = Fixture::new()?;
    f.write("a/report.pdf", b"unique report bytes")?;
    f.write("a/other.pdf", b"other bytes")?;
    f.scan()?;
    let node = f.node("a/report.pdf")?;
    f.opts.settle = Some(Settle {
        from_size: 1,
        quiet: std::time::Duration::from_secs(60),
    });
    f.write("b/report.pdf", b"unique report bytes")?;
    f.write("b/new.pdf", b"a new file of its own size")?;
    fs::remove_file(f.root().join("a/report.pdf"))?;
    let (report, _) = f.scan()?;
    assert_eq!(f.node("b/report.pdf")?, node);
    assert_eq!((report.moved, report.removed), (1, 0), "{report:?}");
    let young: Vec<String> = report.young.iter().map(|y| y.path.to_string()).collect();
    assert_eq!(young, ["b/new.pdf"], "{report:?}");
    Ok(())
}
