use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_state::{BaseRow, LocalRow, StateDb, StateError, keys};
use hbsync_types::RemoteRef;
use hbsync_types::{
    ContentState, FileContent, HashAlgorithm, NodeId, NodeKind, NodeState, RemoteFileId, Tag, Tags,
    Uuid, VersionTag,
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
        tags: None,
    }
}

#[test]
fn base_round_trip() -> R {
    let mut db = StateDb::open_in_memory()?;
    let mut folder = base(NodeState::folder(NodeId::ROOT, "Docs"));
    folder.state.tags = Tags::new([Tag::new("Work", 0), Tag::new("Red", 6)]);
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
    let tagged = LocalRow {
        tags: Some(Tags::new([Tag::new("Blue", 4)])),
        ..local("a-b", NodeKind::File, 2)
    };
    let untagged = LocalRow {
        tags: Some(Tags::default()),
        ..local("b", NodeKind::File, 1)
    };
    let rows = [
        untagged,
        tagged,
        local("a", NodeKind::Folder, 3),
        local("a/z", NodeKind::File, 4),
        local("a/b", NodeKind::Folder, 5),
        local("a/b/c", NodeKind::File, 6),
        local("a\u{1}x", NodeKind::File, 7),
    ];
    db.write(|s| rows.iter().try_for_each(|r| s.put_local(r)))?;

    db.read(|s| {
        let all: Vec<String> = s
            .local_subtree(&RelPath::root())?
            .into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect();
        assert_eq!(all, ["a", "a/b", "a/b/c", "a/z", "a\u{1}x", "a-b", "b"]);
        let top: Vec<String> = s
            .local_children(&RelPath::root())?
            .into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect();
        assert_eq!(top, ["a", "a\u{1}x", "a-b", "b"]);

        let subtree: Vec<String> = s
            .local_subtree(&path("a"))?
            .into_iter()
            .map(|r| r.path.as_str().to_owned())
            .collect();
        assert_eq!(subtree, ["a", "a/b", "a/b/c", "a/z"]);

        for row in &rows {
            assert_eq!(s.local_by_node(row.node)?.as_ref(), Some(row));
        }
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
    // Moving is delete + put in one transaction (or move_local_subtree).
    db.write(|s| {
        s.delete_local(row.node)?;
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
        s.set_kv(keys::JOURNAL_CURSOR, "abc")?;
        Err(StateError::Conflict("abort".into()))
    });
    assert!(result.is_err());
    db.read(|s| {
        assert_eq!(s.dirty_count()?, 0);
        assert_eq!(s.get_kv(keys::JOURNAL_CURSOR)?, None);
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
        s.set_kv(keys::FILTER, "v1")?;
        s.set_kv(keys::FILTER, "v2")
    })?;
    db.read(|s| {
        assert_eq!(s.dirty_count()?, 2);
        assert_eq!(s.get_kv(keys::FILTER)?.as_deref(), Some("v2"));
        Ok(())
    })?;
    db.write(|s| s.clear_dirty(a))?;
    assert_eq!(db.read(|s| s.dirty_nodes(10))?, vec![b]);
    Ok(())
}

#[test]
fn dirty_pages_visit_every_node_once() -> R {
    let mut db = StateDb::open_in_memory()?;
    let nodes: Vec<NodeId> = (0..23).map(|_| NodeId::new()).collect();
    db.write(|s| nodes.iter().try_for_each(|n| s.mark_dirty(*n)))?;
    let mut seen: Vec<NodeId> = Vec::new();
    let mut after = None;
    loop {
        let (page, next) = db.read(|s| s.dirty_page(after, 5))?;
        seen.extend(&page);
        after = next;
        if page.len() < 5 {
            break;
        }
    }
    // Id order: the order the nodes were first seen.
    assert_eq!(seen, nodes);
    // A cursor whose node went clean, and was forgotten entirely, still works.
    let (_, at_10) = db.read(|s| s.dirty_page(None, 11))?;
    db.write(|s| {
        s.clear_dirty(nodes[10])?;
        s.collect_nodes()
    })?;
    assert_eq!(db.read(|s| s.dirty_page(at_10, 2))?.0, nodes[11..13]);
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
    assert_eq!(db.schema_version()?, 5);
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
    use hbsync_types::schema::{NodeContent, Removal};

    /// A node's content survives its mirror row, unknown keys, creation time and tags
    /// included; the removal stays out (the `removed` table has it in columns).
    #[test]
    fn content_round_trips_through_the_row() {
        let c = content(b"x");
        let mut file = NodeContent::file("f", c.mtime_ms, c.size, true);
        file.unknown.insert("future".into(), 1.into());
        file.set_created_ms(Some(1_451_606_400_123));
        file.set_tags(&Tags::new([Tag::new("Red", 6)]));
        let mut removed = file.clone();
        removed.set_removal(Some(Removal {
            at_ms: 1,
            root: NodeId::new(),
        }));
        let (node, at) = (NodeId::new(), remote());
        let row = MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &removed,
            Some(c.hash),
        );
        assert_eq!(row.remote_ref(), at);
        assert_eq!(row.content("f"), file);
        assert_eq!(row.file.map(|f| (f.hash, f.size)), Some((c.hash, c.size)));
        assert_eq!(row.created_ms(), Some(1_451_606_400_123));
        assert_eq!(row.tags(), file.tags());

        // A folder has no payload, whatever it's given.
        let folder = NodeContent::folder("d");
        let row = MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &folder,
            Some(c.hash),
        );
        assert_eq!((row.file, row.content_extra.clone()), (None, None));
        assert_eq!(row.content("d"), folder);
    }

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
                hash: c.hash,
                size: c.size,
                mtime_ms: c.mtime_ms,
                executable: true,
            }),
        );
        db.write(|s| {
            s.put_mirror(&folder)?;
            s.put_mirror(&file)
        })?;
        db.read(|s| {
            assert_eq!(s.get_mirror(file.node)?, Some(file.clone()));
            assert_eq!(s.mirror_by_file(folder.file_id)?, Some(folder.clone()));
            assert_eq!(s.mirror_children(folder.node)?.len(), 1);
            assert_eq!(s.mirror_count()?, 2);
            Ok(())
        })?;
        assert_eq!(file.state().map(|st| st.name), Some("f".into()));
        let tagged = MirrorRow {
            content_extra: Some(r#"{"ct":1,"tags":[{"n":"Red","c":6}]}"#.into()),
            ..folder.clone()
        };
        let red = Tags::new([Tag::new("Red", 6)]);
        assert_eq!(tagged.state().map(|st| st.tags), Some(red));
        assert_eq!(folder.state().map(|st| st.tags), Some(Tags::default()));
        let removed = db.write(|s| s.delete_mirror(file.file_id))?;
        assert_eq!(removed, Some(file.clone()));
        assert_eq!(db.read(|s| s.get_mirror(file.node))?, None);
        Ok(())
    }

    #[test]
    fn recently_deleted_lists_finds_and_forgets() -> R {
        use hbsync_state::RemovedRow;
        use hbsync_types::schema::Removal;
        let mut db = StateDb::open_in_memory()?;
        let folder = row(NodeId::new(), NodeId::ROOT, "d", None);
        let c = content(b"x");
        let file = row(
            NodeId::new(),
            folder.node,
            "f",
            Some(MirrorFile {
                hash: c.hash,
                size: c.size,
                mtime_ms: c.mtime_ms,
                executable: false,
            }),
        );
        let alone = row(NodeId::new(), NodeId::ROOT, "g", None);
        let with = |row: &MirrorRow, at_ms, root| RemovedRow {
            row: row.clone(),
            removal: Removal { at_ms, root },
        };
        let (rf, rd, ra) = (
            with(&file, 8, folder.node),
            with(&folder, 10, folder.node),
            with(&alone, 20, alone.node),
        );
        db.write(|s| {
            s.put_removed(&rf)?;
            s.put_removed(&rd)?;
            s.put_removed(&ra)
        })?;
        db.read(|s| {
            assert_eq!(s.removed_by_node(file.node)?, Some(rf.clone()));
            assert_eq!(s.removed_children(folder.node)?, std::slice::from_ref(&rf));
            let newest: Vec<_> = s
                .list_removed(10)?
                .into_iter()
                .map(|r| r.row.name)
                .collect();
            assert_eq!(newest, ["g", "d", "f"]);
            // Expired ones, files before folders.
            let expired: Vec<_> = s
                .removed_before(15)?
                .into_iter()
                .map(|r| r.row.name)
                .collect();
            assert_eq!(expired, ["f", "d"]);
            // The file went before its folder, but goes when the folder's delete does.
            assert!(s.removed_before(9)?.is_empty());
            Ok(())
        })?;
        db.write(|s| {
            assert!(s.delete_removed(alone.node)?);
            assert!(s.delete_removed_file(file.file_id)?);
            assert!(!s.delete_removed(file.node)?);
            Ok(())
        })?;
        assert_eq!(db.read(|s| s.list_removed(10))?, [rd]);
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
    db.write(|s| s.move_local_subtree(&path("a"), &path("moved")))?;
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
    assert_eq!(db.read(|s| s.local_count())?, 2);

    db.write(|s| s.put_parked(rows[5].node, &path("top")))?;
    assert_eq!(db.read(|s| s.all_parked())?, vec![rows[5].node]);
    assert_eq!(
        db.read(|s| s.get_parked(rows[5].node))?.as_deref(),
        Some("top")
    );
    db.write(|s| s.delete_parked(rows[5].node))?;
    assert!(db.read(|s| s.all_parked())?.is_empty());
    Ok(())
}

#[test]
fn local_tree_keeps_rows_connected() -> R {
    let mut db = StateDb::open_in_memory()?;
    let (dir, file) = (
        local("d", NodeKind::Folder, 1),
        local("d/f", NodeKind::File, 2),
    );
    // A row needs its folder's row.
    let orphan = db.write(|s| s.put_local(&file)).expect_err("no folder yet");
    assert!(matches!(orphan, StateError::Conflict(_)));
    db.write(|s| {
        s.put_local(&dir)?;
        s.put_local(&file)
    })?;
    // Another node can't take a recorded path.
    let taken = LocalRow {
        node: NodeId::new(),
        ..file.clone()
    };
    assert!(db.write(|s| s.put_local(&taken)).is_err());
    // A folder goes after its contents.
    assert!(db.write(|s| s.delete_local(dir.node)).is_err());

    // Adoption: the folder becomes another node, its contents stay inside it.
    let adopted = NodeId::new();
    db.write(|s| s.rekey_local(dir.node, adopted))?;
    db.read(|s| {
        assert_eq!(s.local_by_node(dir.node)?, None);
        assert_eq!(s.local_node_at(&path("d"))?, Some(adopted));
        assert_eq!(
            s.local_by_node(file.node)?.map(|r| r.path),
            Some(path("d/f"))
        );
        Ok(())
    })?;

    // Ids still referenced stay; ids of nodes gone everywhere are forgotten.
    db.write(|s| s.delete_local(file.node))?;
    db.write(|s| s.collect_nodes())?;
    assert_eq!(
        db.read(|s| s.local_node_at(&path("d")))?,
        Some(adopted),
        "the folder and the root are still referenced"
    );
    db.write(|s| s.delete_local(adopted))?;
    db.write(|s| s.collect_nodes())?;
    assert_eq!(db.read(|s| s.node_count())?, 0);
    Ok(())
}

#[test]
fn synced_keeps_the_newest() -> R {
    use hbsync_types::activity::{Synced, SyncedKind};

    let mut db = StateDb::open_in_memory()?;
    let entry = |i: u64| Synced {
        at: 1_700_000_000 + i,
        kind: if i.is_multiple_of(2) {
            SyncedKind::Uploaded
        } else {
            SyncedKind::TrashedLocally
        },
        path: format!("f{i}"),
        files: 1,
        bytes: i,
    };
    let first: Vec<_> = (0..4).map(entry).collect();
    db.write(|s| s.add_synced(&first, 5))?;
    let second: Vec<_> = (4..7).map(entry).collect();
    db.write(|s| s.add_synced(&second, 5))?;
    let recent = db.read(|s| s.recent_synced(10))?;
    // Newest first, the oldest two gone.
    assert_eq!(recent, (2..7).rev().map(entry).collect::<Vec<_>>());
    assert_eq!(db.read(|s| s.recent_synced(2))?, vec![entry(6), entry(5)]);
    Ok(())
}

/// Connections opening a new DB at the same time: one creates the schema, the rest find it.
#[test]
fn a_new_db_opened_at_once_is_created_once() -> R {
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let path = dir.path().join("state.db");
    let start = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let opens: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    StateDb::open(&path).map(drop)
                })
            })
            .collect();
        opens
            .into_iter()
            .try_for_each(|o| o.join().expect("no panic"))
    })?;
    assert_eq!(StateDb::open(&path)?.schema_version()?, 5);
    Ok(())
}

/// Two folders of one account in one DB: each has its own rows of the same node, its own
/// names and settings; the drive's mirror is shared. Forgetting a folder takes its rows along,
/// and the ids nothing else holds go too.
#[test]
fn folders_keep_their_own_rows() -> R {
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let mut main = StateDb::open(&db_path)?;
    let docs_top = NodeId::new();
    let docs = main.write(|s| s.add_folder(docs_top, "/home/a/Documents", "m2"))?;
    let mut docs_db = StateDb::open_folder(&db_path, docs)?;
    assert_eq!(
        (main.folder(), docs_db.folder()),
        (hbsync_state::FIRST_FOLDER, docs)
    );

    // The same node in both folders, at the same path and with the same remote file.
    let row = base(NodeState::folder(NodeId::ROOT, "Shared"));
    let mut seen = local("Shared", NodeKind::Folder, 7);
    seen.node = row.node;
    for db in [&mut main, &mut docs_db] {
        db.write(|s| {
            s.put_base(&row)?;
            s.put_local(&seen)?;
            s.mark_dirty(row.node)?;
            s.set_kv(keys::FILTER, "f")
        })?;
    }
    // A name is taken per folder only.
    let mut other = local("Shared", NodeKind::Folder, 8);
    assert!(matches!(
        main.write(|s| s.put_local(&other)),
        Err(StateError::Conflict(_))
    ));
    other.path = path("Only in docs");
    docs_db.write(|s| s.put_local(&other))?;
    assert_eq!(main.read(|s| s.local_count())?, 1);
    assert_eq!(docs_db.read(|s| s.local_count())?, 2);
    // The account's settings are shared; a folder's are its own.
    main.write(|s| s.set_account_kv(keys::REMOTE_CURSOR, "c1"))?;
    assert_eq!(
        docs_db
            .read(|s| s.account_kv(keys::REMOTE_CURSOR))?
            .as_deref(),
        Some("c1")
    );
    docs_db.write(|s| s.delete_kv(keys::FILTER))?;
    assert_eq!(main.read(|s| s.get_kv(keys::FILTER))?.as_deref(), Some("f"));

    // Forgetting docs takes its rows; main's keep the node's id.
    assert!(main.write(|s| s.remove_folder(docs))?);
    drop(docs_db);
    assert!(StateDb::open_folder(&db_path, docs).is_err());
    let (ids_before, collected) = main.write(|s| Ok((s.node_count()?, s.collect_nodes()?)))?;
    assert_eq!(collected, 1, "only docs' own node goes, of {ids_before}");
    main.read(|s| {
        assert_eq!(s.get_base(row.node)?, Some(row.clone()));
        assert!(s.is_dirty(row.node)?);
        assert_eq!(s.folders()?.len(), 1);
        Ok(())
    })
}

/// A change of the drive is marked dirty in the folders it concerns: those holding the node,
/// and the one whose part of the drive it is in; every folder when its place isn't known.
#[test]
fn drive_changes_fan_out_to_the_folders_they_concern() -> R {
    use hbsync_state::MirrorRow;
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let mut main = StateDb::open(&db_path)?;
    let (docs_top, pics_top) = (NodeId::new(), NodeId::new());
    let (docs, pics) = main.write(|s| {
        Ok((
            s.add_folder(docs_top, "/d", "m2")?,
            s.add_folder(pics_top, "/p", "m3")?,
        ))
    })?;
    let mut docs_db = StateDb::open_folder(&db_path, docs)?;
    let mut pics_db = StateDb::open_folder(&db_path, pics)?;
    let folder = |node, parent, name: &str| MirrorRow {
        file_id: RemoteFileId(Uuid::new_v4()),
        version: VersionTag(Uuid::new_v4()),
        node,
        parent,
        name: name.into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
    };
    let dirty = |db: &mut StateDb, node| db.read(|s| s.is_dirty(node));
    // The role folders' tops, then a folder inside Documents: only docs is concerned.
    let inside = NodeId::new();
    main.write(|s| {
        s.put_mirror(&folder(docs_top, NodeId::ROOT, "Documents"))?;
        s.put_mirror(&folder(pics_top, NodeId::ROOT, "Pictures"))?;
        s.put_mirror(&folder(inside, docs_top, "Work"))
    })?;
    assert!(dirty(&mut docs_db, inside)?);
    assert!(!dirty(&mut main, inside)? && !dirty(&mut pics_db, inside)?);
    // Pictures holds it (moved from there): pics is concerned too, the main folder isn't.
    let held = base(NodeState::folder(pics_top, "Work"));
    pics_db.write(|s| {
        s.put_base(&BaseRow {
            node: inside,
            ..held
        })?;
        s.clear_dirty(inside)
    })?;
    docs_db.write(|s| s.clear_dirty(inside))?;
    main.write(|s| s.put_mirror(&folder(inside, docs_top, "Work (2)")))?;
    assert!(dirty(&mut docs_db, inside)? && dirty(&mut pics_db, inside)?);
    assert!(!dirty(&mut main, inside)?);
    // The same version again changes nothing.
    pics_db.write(|s| s.clear_dirty(inside))?;
    let again = main
        .read(|s| s.get_mirror(inside))?
        .ok_or(StateError::Corrupt("gone".into()))?;
    main.write(|s| s.put_mirror(&again))?;
    assert!(!dirty(&mut pics_db, inside)?);
    // A place the mirror doesn't connect to the top: every folder looks.
    let lost = NodeId::new();
    main.write(|s| s.put_mirror(&folder(lost, NodeId::new(), "Lost")))?;
    for db in [&mut main, &mut docs_db, &mut pics_db] {
        assert!(dirty(db, lost)?);
    }
    Ok(())
}

/// A folder holding part of the drive sees the drive's rows in its own coordinates: its top
/// is its root (and no node), the rest of the drive one folder, OUTSIDE, at its top. What it
/// writes at its root goes to its top.
#[test]
fn a_role_folder_sees_its_top_as_the_root() -> R {
    use hbsync_state::{MirrorRow, RemovedRow};
    use hbsync_types::schema::Removal;
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let mut main = StateDb::open(&db_path)?;
    let top = NodeId::new();
    let docs = main.write(|s| s.add_folder(top, "/d", "m2"))?;
    let mut docs_db = StateDb::open_folder(&db_path, docs)?;
    let row = |node, parent, name: &str| MirrorRow {
        file_id: RemoteFileId(Uuid::new_v4()),
        version: VersionTag(Uuid::new_v4()),
        node,
        parent,
        name: name.into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
    };
    let (inside, elsewhere) = (NodeId::new(), NodeId::new());
    main.write(|s| {
        s.put_mirror(&row(top, NodeId::ROOT, "Documents"))?;
        s.put_mirror(&row(inside, top, "Work"))?;
        s.put_mirror(&row(elsewhere, NodeId::ROOT, "Music stuff"))
    })?;
    docs_db.read(|s| {
        assert_eq!(s.get_mirror(top)?, None, "its top is its root");
        assert_eq!(s.get_mirror(inside)?.map(|r| r.parent), Some(NodeId::ROOT));
        assert_eq!(
            s.get_mirror(elsewhere)?.map(|r| r.parent),
            Some(NodeId::OUTSIDE)
        );
        let outside = s
            .get_mirror(NodeId::OUTSIDE)?
            .ok_or(StateError::Corrupt("no".into()))?;
        assert_eq!(
            (outside.parent, outside.kind),
            (NodeId::ROOT, NodeKind::Folder)
        );
        let mut at_top: Vec<NodeId> = s
            .mirror_children(NodeId::ROOT)?
            .iter()
            .map(|r| r.node)
            .collect();
        at_top.sort();
        let mut expected = vec![inside, NodeId::OUTSIDE];
        expected.sort();
        assert_eq!(at_top, expected);
        let rest: Vec<NodeId> = s
            .mirror_children(NodeId::OUTSIDE)?
            .iter()
            .map(|r| r.node)
            .collect();
        assert_eq!(
            rest,
            [elsewhere],
            "the drive's top without the folder's own top"
        );
        Ok(())
    })?;
    // Written at its root, it is at its top on the drive; OUTSIDE is never written.
    let new = NodeId::new();
    docs_db.write(|s| s.put_mirror(&row(new, NodeId::ROOT, "New")))?;
    assert_eq!(
        main.read(|s| s.get_mirror(new))?.map(|r| r.parent),
        Some(top)
    );
    assert!(
        docs_db
            .write(|s| s.put_mirror(&row(NodeId::OUTSIDE, NodeId::ROOT, "x")))
            .is_err()
    );
    // Recently deleted too.
    let removal = Removal {
        at_ms: 1,
        root: new,
    };
    let gone = main
        .read(|s| s.get_mirror(new))?
        .ok_or(StateError::Corrupt("no".into()))?;
    docs_db.write(|s| {
        s.delete_mirror(gone.file_id)?;
        s.put_removed(&RemovedRow {
            row: MirrorRow {
                parent: NodeId::ROOT,
                ..gone.clone()
            },
            removal,
        })
    })?;
    assert_eq!(
        main.read(|s| s.removed_by_node(new))?.map(|r| r.row.parent),
        Some(top)
    );
    assert_eq!(docs_db.read(|s| s.removed_children(NodeId::ROOT))?.len(), 1);
    Ok(())
}
