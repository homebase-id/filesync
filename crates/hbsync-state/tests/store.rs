use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_state::{BaseRow, LocalRow, RemoteRef, StateDb, StateError};
use hbsync_types::{
    ContentState, FileContent, HashAlgorithm, NodeId, NodeKind, NodeState, RemoteFileId, Uuid,
    VersionTag,
};

type R = Result<(), StateError>;

fn content(seed: &[u8]) -> FileContent {
    FileContent {
        hash: hbsync_fileio::hash_bytes(seed, HashAlgorithm::Blake3),
        size: seed.len() as u64,
        mtime_ms: 1_700_000_000_000,
        executable: false,
    }
}

fn remote() -> RemoteRef {
    RemoteRef {
        file_id: RemoteFileId(Uuid::new_v4()),
        version: VersionTag(Uuid::new_v4()),
    }
}

fn base(state: NodeState) -> BaseRow {
    BaseRow {
        node: NodeId::new(),
        state,
        remote: remote(),
        content_state: ContentState::Local,
        local_name: None,
        local_hash: None,
    }
}

fn path(s: &str) -> RelPath {
    RelPath::new(s).expect("valid path")
}

fn local(p: &str, kind: NodeKind, inode: u128) -> LocalRow {
    LocalRow {
        path: path(p),
        node: NodeId::new(),
        kind,
        fingerprint: Fingerprint {
            size: 3,
            mtime_ns: 1,
            ctime_ns: 2,
            file_id: FileId {
                volume: u64::MAX,
                file: inode,
            },
        },
        executable: Some(false),
        hash: None,
    }
}

#[test]
fn base_round_trip() -> R {
    let mut db = StateDb::open_in_memory()?;
    let folder = base(NodeState::folder(NodeId::ROOT, "Docs"));
    let mut file = base(NodeState::file(folder.node, "a.txt", content(b"a")));
    file.content_state = ContentState::Pinned;
    file.local_name = Some("a\u{F022}.txt".into());
    file.local_hash = Some(content(b"local").hash);
    db.write(|s| {
        s.put_base(&folder)?;
        s.put_base(&file)
    })?;
    db.read(|s| {
        assert_eq!(s.get_base(folder.node)?, Some(folder.clone()));
        assert_eq!(s.get_base(file.node)?, Some(file.clone()));
        assert_eq!(
            s.base_by_remote_file(file.remote.file_id)?,
            Some(file.clone())
        );
        assert_eq!(s.base_children(folder.node)?, vec![file.clone()]);
        assert_eq!(s.base_children(NodeId::ROOT)?, vec![folder.clone()]);
        assert_eq!((s.base_count()?, s.base_file_count()?), (2, 1));
        Ok(())
    })?;
    db.write(|s| s.delete_base(file.node))?;
    db.read(|s| s.get_base(file.node))?
        .map_or(Ok(()), |_| panic!("deleted"))
}

#[test]
fn local_rows_and_walk_order() -> R {
    let mut db = StateDb::open_in_memory()?;
    let rows = [
        local("b", NodeKind::File, 1),
        local("a-b", NodeKind::File, 2),
        local("a", NodeKind::Folder, 3),
        local("a/z", NodeKind::File, 4),
        local("a/b", NodeKind::Folder, 5),
        local("a/b/c", NodeKind::File, 6),
        local("a\u{1}x", NodeKind::File, 7),
    ];
    db.write(|s| rows.iter().try_for_each(|r| s.put_local(r)))?;

    db.read(|s| {
        let all: Vec<String> = s
            .local_page(None, 100)?
            .into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect();
        assert_eq!(all, ["a", "a/b", "a/b/c", "a/z", "a\u{1}x", "a-b", "b"]);

        // Paging resumes after the last path seen.
        let first = s.local_page(None, 3)?;
        let next = s.local_page(Some(&first[2].path), 3)?;
        assert_eq!(next[0].path, path("a/z"));

        let subtree: Vec<String> = s
            .local_subtree(&path("a"))?
            .into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect();
        assert_eq!(subtree, ["a", "a/b", "a/b/c", "a/z"]);

        assert_eq!(s.local_by_node(rows[3].node)?, Some(rows[3].clone()));
        let by_id = s.local_by_file_id(&rows[5].fingerprint.file_id)?;
        assert_eq!(by_id, vec![rows[5].clone()]);
        assert_eq!(s.local_count()?, 7);
        Ok(())
    })
}

#[test]
fn local_node_can_only_be_in_one_place() -> R {
    let mut db = StateDb::open_in_memory()?;
    let row = local("x", NodeKind::File, 1);
    db.write(|s| s.put_local(&row))?;
    let moved = LocalRow {
        path: path("y"),
        ..row.clone()
    };
    let err = db
        .write(|s| s.put_local(&moved))
        .expect_err("node already at x");
    assert!(matches!(err, StateError::Conflict(_)));
    // Moving is delete + put in one transaction.
    db.write(|s| {
        s.delete_local(&row.path)?;
        s.put_local(&moved)
    })?;
    assert_eq!(
        db.read(|s| s.local_by_node(row.node))?.map(|r| r.path),
        Some(path("y"))
    );
    Ok(())
}

#[test]
fn failed_write_rolls_back() -> R {
    let mut db = StateDb::open_in_memory()?;
    let node = NodeId::new();
    let result: Result<(), StateError> = db.write(|s| {
        s.mark_dirty(node)?;
        s.set_kv("cursor", "abc")?;
        Err(StateError::Conflict("abort".into()))
    });
    assert!(result.is_err());
    db.read(|s| {
        assert_eq!(s.dirty_count()?, 0);
        assert_eq!(s.get_kv("cursor")?, None);
        Ok(())
    })
}

#[test]
fn dirty_and_kv() -> R {
    let mut db = StateDb::open_in_memory()?;
    let (a, b) = (NodeId::new(), NodeId::new());
    db.write(|s| {
        s.mark_dirty(a)?;
        s.mark_dirty(a)?;
        s.mark_dirty(b)?;
        s.set_kv("k", "v1")?;
        s.set_kv("k", "v2")
    })?;
    db.read(|s| {
        assert_eq!(s.dirty_count()?, 2);
        assert_eq!(s.get_kv("k")?.as_deref(), Some("v2"));
        Ok(())
    })?;
    db.write(|s| s.clear_dirty(a))?;
    assert_eq!(db.read(|s| s.dirty_nodes(10))?, vec![b]);
    Ok(())
}

#[test]
fn file_db_survives_reopen_and_rejects_newer_schema() -> R {
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let folder = base(NodeState::folder(NodeId::ROOT, "Kept"));
    {
        let mut db = StateDb::open(&db_path)?;
        db.write(|s| s.put_base(&folder))?;
    }
    let mut db = StateDb::open(&db_path)?;
    assert_eq!(db.read(|s| s.get_base(folder.node))?, Some(folder));
    assert_eq!(db.schema_version()?, 4);
    drop(db);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.pragma_update(None, "user_version", 99)?;
    drop(conn);
    assert!(matches!(
        StateDb::open(&db_path),
        Err(StateError::TooNew { found: 99, .. })
    ));
    Ok(())
}

mod mirror {
    use super::*;
    use hbsync_state::{MirrorFile, MirrorRow};

    fn row(node: NodeId, parent: NodeId, name: &str, file: Option<MirrorFile>) -> MirrorRow {
        MirrorRow {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            node,
            parent,
            name: name.into(),
            kind: if file.is_some() {
                NodeKind::File
            } else {
                NodeKind::Folder
            },
            file,
            content_extra: Some(r#"{"future":1}"#.into()),
        }
    }

    #[test]
    fn round_trip_and_lookups() -> R {
        let mut db = StateDb::open_in_memory()?;
        let folder = row(NodeId::new(), NodeId::ROOT, "d", None);
        let c = content(b"x");
        let file = row(
            NodeId::new(),
            folder.node,
            "f",
            Some(MirrorFile {
                hash: Some(c.hash),
                size: c.size,
                mtime_ms: c.mtime_ms,
                executable: true,
            }),
        );
        let unhashed = row(
            NodeId::new(),
            folder.node,
            "legacy",
            Some(MirrorFile {
                hash: None,
                size: 3,
                mtime_ms: 0,
                executable: false,
            }),
        );
        db.write(|s| {
            s.put_mirror(&folder)?;
            s.put_mirror(&file)?;
            s.put_mirror(&unhashed)
        })?;
        db.read(|s| {
            assert_eq!(s.get_mirror(file.node)?, Some(file.clone()));
            assert_eq!(s.mirror_by_file(folder.file_id)?, Some(folder.clone()));
            assert_eq!(s.mirror_children(folder.node)?.len(), 2);
            assert_eq!(s.mirror_count()?, 3);
            Ok(())
        })?;
        assert_eq!(file.state().map(|st| st.name), Some("f".into()));
        assert_eq!(
            unhashed.state(),
            None,
            "a file without hash has no complete state"
        );
        let removed = db.write(|s| s.delete_mirror(file.file_id))?;
        assert_eq!(removed, Some(file.clone()));
        assert_eq!(db.read(|s| s.get_mirror(file.node))?, None);
        Ok(())
    }

    #[test]
    fn recreated_node_replaces_old_file_id() -> R {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let old = row(node, NodeId::ROOT, "n", None);
        let new = row(node, NodeId::ROOT, "n", None);
        db.write(|s| {
            s.put_mirror(&old)?;
            s.put_mirror(&new)
        })?;
        db.read(|s| {
            assert_eq!(s.mirror_by_file(old.file_id)?, None);
            assert_eq!(s.get_mirror(node)?.map(|r| r.file_id), Some(new.file_id));
            Ok(())
        })
    }
}

#[test]
fn children_subtree_moves_and_parking() -> R {
    let mut db = StateDb::open_in_memory()?;
    let rows = [
        local("a", NodeKind::Folder, 1),
        local("a/x", NodeKind::File, 2),
        local("a/sub", NodeKind::Folder, 3),
        local("a/sub/y", NodeKind::File, 4),
        local("a-b", NodeKind::File, 5),
        local("top", NodeKind::File, 6),
    ];
    db.write(|s| rows.iter().try_for_each(|r| s.put_local(r)))?;
    let names = |rows: Vec<LocalRow>| {
        rows.into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(db.read(|s| s.local_children(&RelPath::root()))?),
        ["a", "a-b", "top"]
    );
    assert_eq!(
        names(db.read(|s| s.local_children(&path("a")))?),
        ["a/sub", "a/x"]
    );

    // Move folder "a" to "moved".
    assert_eq!(
        db.write(|s| s.move_local_subtree(&path("a"), &path("moved")))?,
        4
    );
    assert_eq!(
        names(db.read(|s| s.local_children(&RelPath::root()))?),
        ["a-b", "moved", "top"]
    );
    assert_eq!(
        names(db.read(|s| s.local_children(&path("moved/sub")))?),
        ["moved/sub/y"]
    );
    assert_eq!(
        db.read(|s| s.local_by_node(rows[3].node))?.map(|r| r.path),
        Some(path("moved/sub/y"))
    );
    // Moving onto an occupied path fails and changes nothing.
    assert!(
        db.write(|s| s.move_local_subtree(&path("moved"), &path("top")))
            .is_err()
    );
    assert_eq!(db.read(|s| s.local_count())?, 6);

    let gone = db.write(|s| s.delete_local_subtree(&path("moved")))?;
    assert_eq!(gone.len(), 4);

    db.write(|s| s.put_parked(rows[5].node, &path(".hbsync-move-x"), &path("top")))?;
    assert_eq!(
        db.read(|s| s.all_parked())?,
        vec![(rows[5].node, path(".hbsync-move-x"), path("top"))]
    );
    db.write(|s| s.delete_parked(rows[5].node))?;
    assert!(db.read(|s| s.all_parked())?.is_empty());
    Ok(())
}
