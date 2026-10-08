use std::fs;
use std::path::Path;

use hbsync_fileio::HashAlgorithm;
use hbsync_local::{STATE_DIR, ScanOptions, ScanReport, scan};
use hbsync_platform::RelPath;
use hbsync_state::StateDb;
use hbsync_types::NodeId;

type R = Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    dir: tempfile::TempDir,
    db: StateDb,
    opts: ScanOptions,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let opts = ScanOptions {
            algorithm: HashAlgorithm::Blake3,
            fs_family: hbsync_platform::fskind::detect(dir.path())?,
            exclude_at_root: vec![STATE_DIR.into()],
        };
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
        let dirty = self.db.write(|s| {
            let nodes = s.dirty_nodes(usize::MAX >> 1)?;
            for n in &nodes {
                s.clear_dirty(*n)?;
            }
            Ok(nodes)
        })?;
        Ok((report, dirty))
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
    if f.opts.fs_family.has_stable_file_ids() {
        assert_eq!(report.hashed, 0);
    }
    assert_eq!(f.node("new.txt")?, node);
    assert_eq!(dirty, [node]);
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
    fs::remove_file(f.root().join("one"))?;
    fs::remove_file(f.root().join("two"))?;
    f.write("three", b"same")?;
    let (report, _) = f.scan()?;
    assert_eq!((report.added, report.removed, report.moved), (1, 2, 0));
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
