use hbsync_platform::{FileId, Fingerprint, RelPath};
use hbsync_state::testkit::RawPicture;
use hbsync_state::{BaseRow, LocalRow, StateDb, StateError, keys};
use hbsync_types::{
    ContentState, FileContent, FileToken, HashAlgorithm, NodeId, NodeKind, NodeState, RemoteFileId,
    Tag, Tags, Uuid, VersionTag,
};
use hbsync_types::{Doomed, RemoteRef};

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
        s.folder().put_base(&folder)?;
        s.folder().put_base(&file)
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
    db.write(|s| s.folder().delete_base(file.node))?;
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
    db.write(|s| rows.iter().try_for_each(|r| s.folder().put_local(r)))?;

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
    db.write(|s| s.folder().put_local(&row))?;
    let moved = LocalRow {
        path: path("y"),
        ..row.clone()
    };
    let err = db
        .write(|s| s.folder().put_local(&moved))
        .expect_err("node already at x");
    assert!(matches!(err, StateError::Conflict(_)));
    // Moving is delete + put in one transaction (or move_local_subtree).
    db.write(|s| {
        s.folder().delete_local(row.node)?;
        s.folder().put_local(&moved)
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
        s.folder().mark_dirty(node)?;
        s.folder().set_kv(keys::JOURNAL_CURSOR, "abc")?;
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
        s.folder().mark_dirty(a)?;
        s.folder().mark_dirty(a)?;
        s.folder().mark_dirty(b)?;
        s.folder().set_kv(keys::FILTER, "v1")?;
        s.folder().set_kv(keys::FILTER, "v2")
    })?;
    db.read(|s| {
        assert_eq!(s.dirty_count()?, 2);
        assert_eq!(s.get_kv(keys::FILTER)?.as_deref(), Some("v2"));
        Ok(())
    })?;
    db.write(|s| s.folder().clear_dirty(a))?;
    assert_eq!(db.read(|s| s.dirty_nodes(10))?, vec![b]);
    Ok(())
}

/// A held delete's node leaves the dirty set (still pending) until all come back at once; one
/// whose mark moved on since its round read it (a change of the drive's picture) stays dirty
/// too, to be planned.
#[test]
fn held_deletes_leave_the_dirty_set_till_they_come_back() -> R {
    use hbsync_state::MirrorRow;
    let mut db = StateDb::open_in_memory()?;
    let (a, b) = (NodeId::new(), NodeId::new());
    let at = |v: u128| MirrorRow {
        file_id: RemoteFileId(Uuid::from_u128(7)),
        version: VersionTag(Uuid::from_u128(v)),
        node: a,
        parent: NodeId::ROOT,
        name: "a".into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: None,
    };
    db.write(|s| {
        s.account().put_live(&at(1))?;
        s.folder().mark_dirty(b)
    })?;
    let (read, _) = db.read(|s| s.dirty_marks(None, 10))?;
    db.write(|s| s.account().put_live(&at(2)))?;
    db.write(|s| read.iter().try_for_each(|m| s.folder().hold_delete(*m)))?;
    let counts = |db: &mut StateDb| {
        db.read(|s| {
            Ok((
                s.dirty_nodes(10)?,
                s.held_delete_count()?,
                s.pending_count()?,
            ))
        })
    };
    assert_eq!(counts(&mut db)?, (vec![a], 2, 2), "a changed since");
    assert_eq!(db.write(|s| s.folder().unhold_deletes())?, 2);
    let (mut dirty, held, pending) = counts(&mut db)?;
    dirty.sort();
    let mut both = vec![a, b];
    both.sort();
    assert_eq!((dirty, held, pending), (both, 0, 2));
    Ok(())
}

/// A round clears only the dirty marks it read: a node whose drive state changes after the
/// round read its mark (another engine's pull) stays dirty, though it was dirty already; one
/// the folder marked itself, or not marked since, is cleared.
#[test]
fn a_clear_keeps_a_mark_a_change_moved_on() -> R {
    use hbsync_state::MirrorRow;
    let mut db = StateDb::open_in_memory()?;
    let (a, b) = (NodeId::new(), NodeId::new());
    let file_id = RemoteFileId(Uuid::new_v4());
    let at = |v: u128| MirrorRow {
        file_id,
        version: VersionTag(Uuid::from_u128(v)),
        node: a,
        parent: NodeId::ROOT,
        name: "a".into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: None,
    };
    db.write(|s| {
        s.account().put_live(&at(1))?;
        s.folder().mark_dirty(b)
    })?;
    let (read, _) = db.read(|s| s.dirty_marks(None, 10))?;
    db.write(|s| {
        s.account().put_live(&at(2))?;
        s.folder().mark_dirty(b)
    })?;
    db.write(|s| read.iter().try_for_each(|m| s.folder().clear_mark(*m)))?;
    assert_eq!(db.read(|s| s.dirty_nodes(10))?, vec![a], "a changed since");
    let (read, _) = db.read(|s| s.dirty_marks(None, 10))?;
    db.write(|s| read.iter().try_for_each(|m| s.folder().clear_mark(*m)))?;
    assert_eq!(db.read(|s| s.dirty_count())?, 0);
    Ok(())
}

#[test]
fn dirty_pages_visit_every_node_once() -> R {
    let mut db = StateDb::open_in_memory()?;
    let nodes: Vec<NodeId> = (0..23).map(|_| NodeId::new()).collect();
    db.write(|s| nodes.iter().try_for_each(|n| s.folder().mark_dirty(*n)))?;
    let mut seen: Vec<NodeId> = Vec::new();
    let mut after = None;
    loop {
        let (page, next) = db.read(|s| s.dirty_marks(after, 5))?;
        seen.extend(page.iter().map(|m| m.node));
        after = next;
        if page.len() < 5 {
            break;
        }
    }
    // Id order: the order the nodes were first seen.
    assert_eq!(seen, nodes);
    // A cursor whose node went clean, and was forgotten entirely, still works.
    let (_, at_10) = db.read(|s| s.dirty_marks(None, 11))?;
    db.write(|s| {
        s.folder().clear_dirty(nodes[10])?;
        s.account().collect_nodes()
    })?;
    let (page, _) = db.read(|s| s.dirty_marks(at_10, 2))?;
    let page: Vec<NodeId> = page.iter().map(|m| m.node).collect();
    assert_eq!(page, nodes[11..13]);
    Ok(())
}

#[test]
fn file_db_survives_reopen_and_rejects_newer_schema() -> R {
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let folder = base(NodeState::folder(NodeId::ROOT, "Kept"));
    {
        let mut db = StateDb::open(&db_path)?;
        db.write(|s| s.folder().put_base(&folder))?;
    }
    let mut db = StateDb::open(&db_path)?;
    assert_eq!(db.read(|s| s.get_base(folder.node))?, Some(folder));
    assert_eq!(db.schema_version()?, 20);
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

/// A refusal is kept per node and kind, in place of an earlier one; it goes when done, when
/// old, or all at once; a node with one isn't collected.
#[test]
fn refusals_are_kept_until_forgotten() -> R {
    use hbsync_state::Refused;
    let mut db = StateDb::open_in_memory()?;
    let node = NodeId::new();
    let refused = |key: &str, at| Refused {
        node,
        op: "RemoteCreate".into(),
        key: key.into(),
        refused: true,
        why: "fatal: 400".into(),
        at,
    };
    db.write(|s| {
        s.folder().put_refused(&refused("one", 10))?;
        s.folder().put_refused(&refused("two", 20))
    })?;
    assert_eq!(db.read(|s| s.all_refused())?, [refused("two", 20)]);
    db.write(|s| s.account().collect_nodes().map(drop))?;
    assert_eq!(db.read(|s| s.all_refused())?.len(), 1, "its node is kept");
    assert_eq!(db.write(|s| s.folder().prune_refused(20))?, 0);
    assert_eq!(db.write(|s| s.folder().prune_refused(21))?, 1);
    db.write(|s| s.folder().put_refused(&refused("three", 30)))?;
    db.write(|s| s.folder().delete_refused(node, "RemoteCreate"))?;
    assert!(db.read(|s| s.all_refused())?.is_empty());
    Ok(())
}

/// A file the identity said once isn't there is a suspicion, kept across a reopen (a restart
/// doesn't make the next "not there" a first one), at the version known; a new row of the file,
/// or its going for good, clears it.
#[test]
fn a_gone_suspicion_is_kept_until_a_row_or_gone() -> Result<(), Box<dyn std::error::Error>> {
    use hbsync_state::{GoneSuspect, MirrorRow};
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.db");
    let row = MirrorRow {
        file_id: RemoteFileId(Uuid::new_v4()),
        version: VersionTag(Uuid::new_v4()),
        node: NodeId::new(),
        parent: NodeId::ROOT,
        name: "x".into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: None,
    };
    let f = row.file_id;
    let mut db = StateDb::open(&path)?;
    db.write(|s| s.account().put_mirror(&row))?;
    assert_eq!(db.read(|s| s.known_version(f))?, Some(row.version));
    let suspect = GoneSuspect {
        version: Some(row.version),
        at: 10,
    };
    db.write(|s| s.account().put_gone_suspect(f, &suspect))?;
    drop(db);
    let mut db = StateDb::open(&path)?;
    assert_eq!(db.read(|s| s.gone_suspect(f))?, Some(suspect), "kept");
    // A row it had (a page read again) changes nothing; a new version clears it.
    db.write(|s| s.account().put_mirror(&row))?;
    assert_eq!(db.read(|s| s.gone_suspect(f))?, Some(suspect));
    let newer = MirrorRow {
        version: VersionTag(Uuid::new_v4()),
        ..row.clone()
    };
    db.write(|s| s.account().put_mirror(&newer))?;
    assert_eq!(db.read(|s| s.gone_suspect(f))?, None, "a new row clears it");
    db.write(|s| s.account().put_gone_suspect(f, &suspect))?;
    db.write(|s| s.account().mark_file_gone(f))?;
    assert_eq!(db.read(|s| s.gone_suspect(f))?, None, "gone clears it");
    Ok(())
}

/// A placement record says which base its plan saw, none included; one written before that was
/// kept (its column empty) reads as not checked, never as "no base".
#[test]
fn a_placement_record_keeps_the_base_its_plan_saw() -> R {
    use hbsync_state::{Placing, PlannedBase};
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let record = |base_at| Placing {
        base: base(NodeState::folder(NodeId::ROOT, "d")),
        target: path("d"),
        placed_tags: None,
        prior_tags: Some(Tags::new([Tag::new("Red", 0)])),
        base_at,
        held_ms: None,
    };
    let (absent, at) = (
        record(PlannedBase::Absent),
        record(PlannedBase::At(remote().version)),
    );
    let mut db = StateDb::open(&db_path)?;
    db.write(|s| {
        s.folder().put_placing(&absent)?;
        s.folder().put_placing(&at)
    })?;
    let mut read = db.read(|s| s.all_placing())?;
    read.sort_by_key(|p| p.base.node);
    let mut want = vec![absent.clone(), at.clone()];
    want.sort_by_key(|p| p.base.node);
    assert_eq!(read, want);
    assert!(db.read(|s| s.is_placing(at.base.node))?);
    drop(db);

    // As a row from before the column held anything.
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("UPDATE placing SET base_at = NULL", [])?;
    drop(conn);
    let mut db = StateDb::open(&db_path)?;
    let read = db.read(|s| s.all_placing())?;
    assert!(read.iter().all(|p| p.base_at == PlannedBase::Unchecked));
    Ok(())
}

mod mirror {
    use super::*;
    use hbsync_state::{MirrorFile, MirrorRow, RemovedRow};
    use hbsync_types::schema::NodeContent;

    /// A node's content survives its mirror row, unknown keys, creation time and tags
    /// included.
    #[test]
    fn content_round_trips_through_the_row() {
        let c = content(b"x");
        let mut file = NodeContent::file("f", c.mtime_ms, c.size, true);
        file.unknown.insert("future".into(), 1.into());
        file.set_created_ms(Some(1_451_606_400_123));
        file.set_tags(&Tags::new([Tag::new("Red", 6)]));
        let (node, at) = (NodeId::new(), remote());
        let row =
            MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::File, &file, Some(c.hash));
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

    /// A folder's children are found by their sibling hash on every side: every spelling of a
    /// name, the local escapes included, and after a rename on every side.
    #[test]
    fn children_are_found_by_sibling_hash() -> R {
        let mut db = StateDb::open_in_memory()?;
        let docs = base(NodeState::folder(NodeId::ROOT, "Docs"));
        let named = |n: &str| hbsync_namemap::sibling_hash(n);
        let (upper, lower, other) = (NodeId::new(), NodeId::new(), NodeId::new());
        let mut escaped = local("Docs/a\u{F022}b", NodeKind::File, 7);
        db.write(|s| {
            s.folder().put_base(&docs)?;
            s.folder().put_local(&local("Docs", NodeKind::Folder, 1))?;
            s.folder().put_base(&BaseRow {
                node: upper,
                ..base(NodeState::file(docs.node, "Report.TXT", content(b"r")))
            })?;
            s.account()
                .put_mirror(&row(lower, docs.node, "report.txt", None))?;
            s.account()
                .put_mirror(&row(other, docs.node, "other.txt", None))?;
            s.folder()
                .put_local(&local("Docs/REPORT.txt", NodeKind::File, 5))?;
            s.folder().put_local(&escaped)
        })?;
        db.read(|s| {
            let key = named("report.txt");
            let base: Vec<NodeId> = s
                .base_named(docs.node, key)?
                .iter()
                .map(|r| r.node)
                .collect();
            assert_eq!(base, [upper]);
            let remote: Vec<NodeId> = s
                .mirror_named(docs.node, key)?
                .iter()
                .map(|r| r.node)
                .collect();
            assert_eq!(remote, [lower]);
            let local: Vec<String> = s
                .local_named(&path("Docs"), key)?
                .iter()
                .map(|r| r.path.to_string())
                .collect();
            assert_eq!(local, ["Docs/REPORT.txt"]);
            let escapes = s.local_named(&path("Docs"), named("a:b"))?;
            assert_eq!(escapes.len(), 1);
            Ok(())
        })?;
        // Renamed: found by the new name only.
        db.write(|s| {
            s.folder()
                .move_local_subtree(&escaped.path, &path("Docs/c.txt"))
        })?;
        escaped.path = path("Docs/c.txt");
        db.read(|s| {
            assert!(s.local_named(&path("Docs"), named("a:b"))?.is_empty());
            assert_eq!(
                s.local_named(&path("Docs"), named("C.TXT"))?,
                [escaped.clone()]
            );
            Ok(())
        })
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
            token: Some(FileToken(node.0.as_bytes().as_slice().into())),
            changed_at: None,
        }
    }

    /// Reading the whole feed again starts from its beginning, the files the store has listed;
    /// tombstones are kept with their time until deleted hard.
    #[test]
    fn a_reread_starts_over_and_tombstones_wait() -> R {
        let mut db = StateDb::open_in_memory()?;
        let (shown, unshown) = (
            row(NodeId::new(), NodeId::ROOT, "a", None),
            row(NodeId::new(), NodeId::ROOT, "b", None),
        );
        db.write(|s| {
            s.account().put_mirror(&shown)?;
            s.account().put_mirror(&unshown)?;
            s.account().set_account_kv(keys::REMOTE_CURSOR, "c")?;
            s.account().start_reread()
        })?;
        assert!(db.read(|s| s.rereading())?);
        assert_eq!(db.read(|s| s.account_kv(keys::REMOTE_CURSOR))?, None);
        let mut files = db.read(|s| s.drive_file_ids())?;
        files.sort_by_key(|f| f.0);
        let mut both = vec![shown.file_id, unshown.file_id];
        both.sort_by_key(|f| f.0);
        assert_eq!(files, both);
        db.write(|s| s.account().end_reread())?;
        assert!(!db.read(|s| s.rereading())?);

        let (old, young) = (shown.remote_ref(), unshown.remote_ref());
        db.write(|s| {
            s.account().put_tombstone(old, 10)?;
            s.account().put_tombstone(young, 20)
        })?;
        assert_eq!(
            db.read(|s| s.tombstones_before(15))?,
            [Doomed::unchecked(old)]
        );
        db.write(|s| s.account().delete_tombstone_at(old).map(drop))?;
        assert!(db.read(|s| s.tombstones_before(15))?.is_empty());
        assert!(db.read(|s| s.file_gone(old.file_id))?, "still gone");
        // Announced deleted hard elsewhere: only at the version held.
        let other = RemoteRef {
            file_id: RemoteFileId(Uuid::now_v7()),
            version: VersionTag(Uuid::now_v7()),
        };
        db.write(|s| s.account().put_tombstone(other, 12))?;
        let newer = RemoteRef {
            version: VersionTag(Uuid::now_v7()),
            ..other
        };
        assert!(!db.write(|s| s.account().delete_tombstone_at(newer))?);
        assert!(db.write(|s| s.account().delete_tombstone_at(other))?);
        assert!(db.read(|s| s.tombstones_before(15))?.is_empty());

        // Known gone from the next prune on, forgotten once that is older than the cut-off; a
        // tombstone still on the drive stays, however old.
        let announced = RemoteFileId(Uuid::now_v7());
        db.write(|s| s.account().mark_file_gone(announced))?;
        assert_eq!(db.write(|s| s.account().prune_gone(0, 100))?, 0);
        assert_eq!(
            db.write(|s| s.account().prune_gone(100, 200))?,
            0,
            "not before 100"
        );
        assert_eq!(
            db.write(|s| s.account().prune_gone(101, 200))?,
            3,
            "old, other and announced"
        );
        assert!(!db.read(|s| s.file_gone(announced))?);
        assert!(
            db.read(|s| s.file_gone(young.file_id))?,
            "its tombstone is on the drive"
        );

        // Gone at a tombstone's time (a delete's answer): forgotten once that is older than the
        // cut-off; one known gone already stays as it was.
        let (own, known) = (RemoteFileId(Uuid::now_v7()), young.file_id);
        db.write(|s| {
            s.account().mark_file_gone_at(own, 300)?;
            s.account().mark_file_gone_at(known, 300)
        })?;
        assert!(db.read(|s| s.file_gone(own))?);
        assert_eq!(
            db.write(|s| s.account().prune_gone(300, 1000))?,
            0,
            "not before 300"
        );
        assert_eq!(db.write(|s| s.account().prune_gone(301, 1000))?, 1, "own");
        assert!(!db.read(|s| s.file_gone(own))?);
        assert_eq!(db.read(|s| s.tombstones_before(15))?.len(), 0);
        assert!(
            db.read(|s| s.file_gone(known))? && db.read(|s| s.tombstones_before(21))?.len() == 1,
            "the feed's tombstone stays"
        );
        // Its tombstone deleted hard: the id kept from then on, whatever its time before.
        db.write(|s| s.account().delete_tombstone_at(young).map(drop))?;
        assert_eq!(
            db.write(|s| s.account().prune_gone(301, 1000))?,
            0,
            "kept from 1000"
        );
        assert_eq!(db.write(|s| s.account().prune_gone(1001, 2000))?, 1);
        Ok(())
    }

    /// A version the mirror has already isn't written again, nor marked to reconcile (the
    /// feed's echo of it takes only its token and time: `feed`'s tests).
    #[test]
    fn a_known_version_is_not_written_again() -> R {
        let mut db = StateDb::open_in_memory()?;
        let known = row(NodeId::new(), NodeId::ROOT, "d", None);
        let node = known.node;
        db.write(|s| {
            s.account().put_mirror(&known)?;
            s.folder().clear_dirty(node)
        })?;
        let renamed = MirrorRow {
            name: "other".into(),
            ..known.clone()
        };
        db.write(|s| s.account().put_mirror(&renamed))?;
        assert_eq!(db.read(|s| s.get_mirror(node))?, Some(known));
        assert!(!db.read(|s| s.is_dirty(node))?);
        Ok(())
    }

    /// A file id deleted for good never comes back: neither a page fetched before the delete
    /// nor a write committed after it puts its row in the mirror or Recently deleted.
    #[test]
    fn a_file_deleted_for_good_stays_out() -> R {
        use hbsync_state::RemovedRow;
        let mut db = StateDb::open_in_memory()?;
        let live = row(NodeId::new(), NodeId::ROOT, "a", None);
        let removed = RemovedRow {
            row: row(NodeId::new(), NodeId::ROOT, "b", None),
            removed_at: Some(1),
        };
        db.write(|s| {
            s.account().mark_file_gone(live.file_id)?;
            s.account().mark_file_gone(removed.row.file_id)?;
            s.account().put_mirror(&live)?;
            s.account().put_removed(&removed)
        })?;
        db.read(|s| {
            assert!(
                s.mirror_by_file(live.file_id)?.is_none(),
                "back in the mirror"
            );
            assert!(
                s.removed_by_node(removed.row.node)?.is_none(),
                "back in Recently deleted"
            );
            Ok(())
        })
    }

    /// The drive's picture keeps a node live or in Recently deleted, never both, and a file
    /// deleted for good in neither: whichever of them records it, the one before goes.
    #[test]
    fn a_node_is_live_or_removed_or_gone_never_two() -> R {
        let mut db = StateDb::open_in_memory()?;
        let live = row(NodeId::new(), NodeId::ROOT, "a", None);
        let node = live.node;
        let removed = RemovedRow {
            row: MirrorRow {
                version: VersionTag(Uuid::now_v7()),
                ..live.clone()
            },
            removed_at: Some(5),
        };
        let both = |db: &mut StateDb| {
            db.read(|s| {
                Ok((
                    s.get_mirror(node)?.is_some(),
                    s.removed_by_node(node)?.is_some(),
                ))
            })
        };
        db.write(|s| s.account().put_live(&live))?;
        assert_eq!(both(&mut db)?, (true, false));
        db.write(|s| s.account().move_to_removed(&removed))?;
        assert_eq!(both(&mut db)?, (false, true), "removed");
        let back = MirrorRow {
            version: VersionTag(Uuid::now_v7()),
            ..live.clone()
        };
        db.write(|s| s.account().put_live(&back))?;
        assert_eq!(both(&mut db)?, (true, false), "restored");
        // Deleted for good, live or removed: out of both, and gone since the time given.
        db.write(|s| s.account().forget_file(live.file_id, Some(7)))?;
        assert_eq!(both(&mut db)?, (false, false));
        assert!(db.read(|s| s.file_gone(live.file_id))?);
        assert_eq!(
            db.write(|s| s.account().prune_gone(8, 100))?,
            1,
            "gone since 7"
        );
        Ok(())
    }

    /// A file deleted for good that the mirror doesn't have (in Recently deleted, say) is
    /// planned again where a base still holds it: its node is found by file id.
    #[test]
    fn a_file_forgotten_is_planned_where_a_base_holds_it() -> R {
        let mut db = StateDb::open_in_memory()?;
        let held = base(NodeState::file(NodeId::ROOT, "f", content(b"f")));
        let removed = RemovedRow {
            row: MirrorRow {
                file_id: held.remote.file_id,
                version: held.remote.version,
                ..row(held.node, NodeId::ROOT, "f", None)
            },
            removed_at: Some(5),
        };
        db.write(|s| {
            s.folder().put_base(&held)?;
            s.account().put_removed(&removed)?;
            s.folder().clear_dirty(held.node)
        })?;
        db.write(|s| s.account().forget_file(held.remote.file_id, Some(7)))?;
        db.read(|s| {
            assert!(s.removed_by_node(held.node)?.is_none());
            assert!(s.is_dirty(held.node)?, "planned again");
            Ok(())
        })
    }

    /// This device's own removal is in Recently deleted at once, its time open: listed first,
    /// never purged, known as that version; the feed's echo confirms the server's time once.
    #[test]
    fn an_own_removal_is_open_until_the_feed_confirms_it() -> R {
        use hbsync_state::RemovedRow;
        let mut db = StateDb::open_in_memory()?;
        let earlier = row(NodeId::new(), NodeId::ROOT, "earlier", None);
        let own = row(NodeId::new(), NodeId::ROOT, "own", None);
        let at = own.remote_ref();
        db.write(|s| {
            s.account().put_removed(&RemovedRow {
                row: earlier.clone(),
                removed_at: Some(5),
            })?;
            s.account().put_removed(&RemovedRow {
                row: own.clone(),
                removed_at: None,
            })
        })?;
        let listed = db.read(|s| s.list_removed(10))?;
        assert_eq!(listed[0].row.node, own.node, "open: the newest");
        assert_eq!(listed[0].time(), i64::MAX);
        let purged: Vec<_> = db.read(|s| s.removed_before(i64::MAX))?;
        assert_eq!(purged.len(), 1, "an open time isn't purged: {purged:?}");
        let other = RemoteRef {
            version: VersionTag(Uuid::new_v4()),
            ..at
        };
        assert!(db.read(|s| s.known(&[(at, true), (earlier.remote_ref(), true)]))?);
        assert!(!db.read(|s| s.known(&[(at, true), (other, true)]))?);
        assert!(!db.read(|s| s.known(&[(at, false)]))?, "not in the mirror");
        db.write(|s| s.account().confirm_removed(at, 9))?;
        db.write(|s| s.account().confirm_removed(at, 10))?;
        let back = db.read(|s| s.removed_by_node(own.node))?.expect("removed");
        assert_eq!(back.removed_at, Some(9), "confirmed once");
        Ok(())
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
                mtime_ms: Some(c.mtime_ms),
                executable: true,
            }),
        );
        db.write(|s| {
            s.account().put_mirror(&folder)?;
            s.account().put_mirror(&file)
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
        let removed = db.write(|s| s.account().delete_mirror(file.file_id))?;
        assert_eq!(removed, Some(file.clone()));
        assert_eq!(db.read(|s| s.get_mirror(file.node))?, None);
        Ok(())
    }

    #[test]
    fn recently_deleted_lists_finds_and_forgets() -> R {
        use hbsync_state::RemovedRow;
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
                mtime_ms: Some(c.mtime_ms),
                executable: false,
            }),
        );
        let alone = row(NodeId::new(), NodeId::ROOT, "g", None);
        // Each with the place it is recorded in.
        let with = |row: &MirrorRow, removed_at| RemovedRow {
            row: row.clone(),
            removed_at,
        };
        let (rf, rd, ra) = (
            with(&file, Some(8)),
            with(&folder, Some(10)),
            with(&alone, Some(20)),
        );
        db.write(|s| {
            s.account().put_removed(&rf)?;
            s.account().put_removed(&rd)?;
            s.account().put_removed(&ra)
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
            // Expired ones, oldest first (what goes with what is the engine's).
            let expired = |before| -> hbsync_state::Result<Vec<String>> {
                Ok(s.removed_before(before)?
                    .into_iter()
                    .map(|r| r.row.name)
                    .collect())
            };
            assert_eq!(expired(15)?, ["f", "d"]);
            assert_eq!(expired(9)?, ["f"]);
            // Each recorded in turn, the counter at the last.
            let upto = |seq| -> hbsync_state::Result<Vec<(String, u64)>> {
                Ok(s.recorded_upto(seq)?
                    .into_iter()
                    .map(|r| (r.removed.row.name, r.seq))
                    .collect())
            };
            assert_eq!(upto(2)?, [("f".to_owned(), 1), ("d".to_owned(), 2)]);
            assert_eq!(s.removed_seq()?, 3);
            Ok(())
        })?;
        // The version recorded again keeps its place (and changes nothing); another version is
        // another removal, the next place; so is a restore then a delete again.
        db.write(|s| {
            s.account().put_removed(&rd)?;
            s.account().delete_removed(alone.node)?;
            s.account().put_removed(&with(&alone, Some(30)))
        })?;
        let mut again = with(&folder, Some(10));
        again.row.version = VersionTag(Uuid::new_v4());
        db.write(|s| s.account().put_removed(&again))?;
        let seq = |db: &mut StateDb, node| -> hbsync_state::Result<Option<u64>> {
            db.read(|s| {
                Ok(s.recorded_upto(u64::MAX)?
                    .into_iter()
                    .find(|r| r.removed.row.node == node)
                    .map(|r| r.seq))
            })
        };
        assert_eq!(seq(&mut db, folder.node)?, Some(5));
        assert_eq!(seq(&mut db, alone.node)?, Some(4));
        assert_eq!(db.read(|s| s.removed_seq())?, 5, "written once per commit");
        db.write(|s| {
            s.account().delete_removed(alone.node)?;
            s.account().put_removed(&ra).map(drop)
        })?;
        db.write(|s| {
            assert!(s.account().delete_removed(alone.node)?);
            assert!(s.account().delete_removed_file(file.file_id)?);
            assert!(!s.account().delete_removed(file.node)?);
            Ok(())
        })?;
        assert_eq!(db.read(|s| s.list_removed(10))?, [again]);
        Ok(())
    }

    /// This device's own removal, its time still open, keeps its place when confirmed; the
    /// feed's row of another version of that node (restored elsewhere, edited, deleted again)
    /// is another removal, recorded at the next place.
    #[test]
    fn another_version_after_an_open_removal_is_another_removal() -> R {
        let mut db = StateDb::open_in_memory()?;
        let x = row(NodeId::new(), NodeId::ROOT, "x", None);
        let open = RemovedRow {
            row: x.clone(),
            removed_at: None,
        };
        let mut again = RemovedRow {
            row: x.clone(),
            removed_at: Some(40),
        };
        again.row.version = VersionTag(Uuid::new_v4());
        let seqs = |db: &mut StateDb| -> hbsync_state::Result<Vec<(Option<i64>, u64)>> {
            db.read(|s| {
                Ok(s.recorded_upto(u64::MAX)?
                    .into_iter()
                    .map(|r| (r.removed.removed_at, r.seq))
                    .collect())
            })
        };
        db.write(|s| s.account().put_removed(&open))?;
        // Its own removal confirmed, and the feed's row of it: the same removal.
        db.write(|s| {
            s.account().confirm_removed(x.remote_ref(), 30)?;
            s.account().put_removed(&RemovedRow {
                removed_at: Some(30),
                ..open.clone()
            })
        })?;
        assert_eq!(seqs(&mut db)?, [(Some(30), 1)]);
        db.write(|s| s.account().put_removed(&again))?;
        assert_eq!(seqs(&mut db)?, [(Some(40), 2)]);
        Ok(())
    }

    #[test]
    fn recreated_node_replaces_old_file_id() -> R {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let old = row(node, NodeId::ROOT, "n", None);
        let new = row(node, NodeId::ROOT, "n", None);
        db.write(|s| {
            s.account().put_mirror(&old)?;
            s.account().put_mirror(&new)
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
    db.write(|s| rows.iter().try_for_each(|r| s.folder().put_local(r)))?;
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
    db.write(|s| s.folder().move_local_subtree(&path("a"), &path("moved")))?;
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
        db.write(|s| s.folder().move_local_subtree(&path("moved"), &path("top")))
            .is_err()
    );
    assert_eq!(db.read(|s| s.local_count())?, 6);

    let gone = db.write(|s| s.folder().delete_local_subtree(&path("moved")))?;
    assert_eq!(gone.len(), 4);
    assert_eq!(db.read(|s| s.local_count())?, 2);

    db.write(|s| s.folder().put_parked(rows[5].node, &path("top")))?;
    assert_eq!(db.read(|s| s.all_parked())?, vec![rows[5].node]);
    assert_eq!(
        db.read(|s| s.get_parked(rows[5].node))?.as_deref(),
        Some("top")
    );
    db.write(|s| s.folder().delete_parked(rows[5].node))?;
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
    let orphan = db
        .write(|s| s.folder().put_local(&file))
        .expect_err("no folder yet");
    assert!(matches!(orphan, StateError::Conflict(_)));
    db.write(|s| {
        s.folder().put_local(&dir)?;
        s.folder().put_local(&file)
    })?;
    // Another node can't take a recorded path.
    let taken = LocalRow {
        node: NodeId::new(),
        ..file.clone()
    };
    assert!(db.write(|s| s.folder().put_local(&taken)).is_err());
    // A folder goes after its contents.
    assert!(db.write(|s| s.folder().delete_local(dir.node)).is_err());

    // Adoption: the folder becomes another node, its contents stay inside it.
    let adopted = NodeId::new();
    db.write(|s| s.folder().rekey_local(dir.node, adopted))?;
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
    db.write(|s| s.folder().delete_local(file.node))?;
    db.write(|s| s.account().collect_nodes())?;
    assert_eq!(
        db.read(|s| s.local_node_at(&path("d")))?,
        Some(adopted),
        "the folder and the root are still referenced"
    );
    db.write(|s| s.folder().delete_local(adopted))?;
    db.write(|s| s.account().collect_nodes())?;
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
    db.write(|s| s.folder().add_synced(&first, 5))?;
    let second: Vec<_> = (4..7).map(entry).collect();
    db.write(|s| s.folder().add_synced(&second, 5))?;
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
    assert_eq!(StateDb::open(&path)?.schema_version()?, 20);
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
    let docs = main.write(|s| s.account().add_folder(docs_top, "/home/a/Documents", "m2"))?;
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
            s.folder().put_base(&row)?;
            s.folder().put_local(&seen)?;
            s.folder().mark_dirty(row.node)?;
            s.folder().set_kv(keys::FILTER, "f")
        })?;
    }
    // A name is taken per folder only.
    let mut other = local("Shared", NodeKind::Folder, 8);
    assert!(matches!(
        main.write(|s| s.folder().put_local(&other)),
        Err(StateError::Conflict(_))
    ));
    other.path = path("Only in docs");
    docs_db.write(|s| s.folder().put_local(&other))?;
    assert_eq!(main.read(|s| s.local_count())?, 1);
    assert_eq!(docs_db.read(|s| s.local_count())?, 2);
    // The account's settings are shared; a folder's are its own.
    main.write(|s| s.account().set_account_kv(keys::REMOTE_CURSOR, "c1"))?;
    assert_eq!(
        docs_db
            .read(|s| s.account_kv(keys::REMOTE_CURSOR))?
            .as_deref(),
        Some("c1")
    );
    docs_db.write(|s| s.folder().delete_kv(keys::FILTER))?;
    assert_eq!(main.read(|s| s.get_kv(keys::FILTER))?.as_deref(), Some("f"));

    // Forgetting docs takes its rows; main's keep the node's id.
    // A handle seeing docs doesn't take the drive's top for its own once docs is gone: not
    // in the transaction that removes it, nor later.
    assert!(docs_db.write(|s| {
        assert_eq!(s.coords()?.top, docs_top);
        let removed = s.account().remove_folder(docs)?;
        assert!(matches!(s.coords(), Err(StateError::Conflict(_))));
        Ok(removed)
    })?);
    assert!(matches!(
        docs_db.read(|s| s.coords()),
        Err(StateError::Conflict(_))
    ));
    drop(docs_db);
    assert!(StateDb::open_folder(&db_path, docs).is_err());
    let (ids_before, collected) =
        main.write(|s| Ok((s.node_count()?, s.account().collect_nodes()?)))?;
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
            s.account().add_folder(docs_top, "/d", "m2")?,
            s.account().add_folder(pics_top, "/p", "m3")?,
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
        token: None,
        changed_at: None,
    };
    let dirty = |db: &mut StateDb, node| db.read(|s| s.is_dirty(node));
    // The role folders' tops, then a folder inside Documents: only docs is concerned.
    let inside = NodeId::new();
    main.write(|s| {
        s.account()
            .put_mirror(&folder(docs_top, NodeId::ROOT, "Documents"))?;
        s.account()
            .put_mirror(&folder(pics_top, NodeId::ROOT, "Pictures"))?;
        s.account().put_mirror(&folder(inside, docs_top, "Work"))
    })?;
    assert!(dirty(&mut docs_db, inside)?);
    assert!(!dirty(&mut main, inside)? && !dirty(&mut pics_db, inside)?);
    // Pictures holds it (moved from there): pics is concerned too, the main folder isn't.
    let held = base(NodeState::folder(pics_top, "Work"));
    pics_db.write(|s| {
        s.folder().put_base(&BaseRow {
            node: inside,
            ..held
        })?;
        s.folder().clear_dirty(inside)
    })?;
    docs_db.write(|s| s.folder().clear_dirty(inside))?;
    main.write(|s| {
        s.account()
            .put_mirror(&folder(inside, docs_top, "Work (2)"))
    })?;
    assert!(dirty(&mut docs_db, inside)? && dirty(&mut pics_db, inside)?);
    assert!(!dirty(&mut main, inside)?);
    // The same version again changes nothing.
    pics_db.write(|s| s.folder().clear_dirty(inside))?;
    let again = main
        .read(|s| s.get_mirror(inside))?
        .ok_or(StateError::Corrupt("gone".into()))?;
    main.write(|s| s.account().put_mirror(&again))?;
    assert!(!dirty(&mut pics_db, inside)?);
    // A place the mirror doesn't connect to the top: every folder looks.
    let lost = NodeId::new();
    main.write(|s| s.account().put_mirror(&folder(lost, NodeId::new(), "Lost")))?;
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
    let dir = tempfile::tempdir().map_err(|e| StateError::Corrupt(e.to_string()))?;
    let db_path = dir.path().join("state.db");
    let mut main = StateDb::open(&db_path)?;
    let top = NodeId::new();
    let docs = main.write(|s| s.account().add_folder(top, "/d", "m2"))?;
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
        token: None,
        changed_at: None,
    };
    let (inside, elsewhere) = (NodeId::new(), NodeId::new());
    main.write(|s| {
        s.account()
            .put_mirror(&row(top, NodeId::ROOT, "Documents"))?;
        s.account().put_mirror(&row(inside, top, "Work"))?;
        s.account()
            .put_mirror(&row(elsewhere, NodeId::ROOT, "Music stuff"))
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
    docs_db.write(|s| s.account().put_mirror(&row(new, NodeId::ROOT, "New")))?;
    assert_eq!(
        main.read(|s| s.get_mirror(new))?.map(|r| r.parent),
        Some(top)
    );
    assert!(
        docs_db
            .write(|s| s
                .account()
                .put_mirror(&row(NodeId::OUTSIDE, NodeId::ROOT, "x")))
            .is_err()
    );
    // Recently deleted too.
    let gone = main
        .read(|s| s.get_mirror(new))?
        .ok_or(StateError::Corrupt("no".into()))?;
    docs_db.write(|s| {
        s.account().delete_mirror(gone.file_id)?;
        s.account().put_removed(&RemovedRow {
            row: MirrorRow {
                parent: NodeId::ROOT,
                ..gone.clone()
            },
            removed_at: Some(1),
        })
    })?;
    assert_eq!(
        main.read(|s| s.removed_by_node(new))?.map(|r| r.row.parent),
        Some(top)
    );
    assert_eq!(docs_db.read(|s| s.removed_children(NodeId::ROOT))?.len(), 1);
    Ok(())
}

/// Folder paths and ids found in a transaction are kept for it, and forgotten by every local
/// write that changes them: a move, a rekey, a delete.
#[test]
fn local_paths_found_in_a_transaction_follow_its_writes() -> R {
    let mut db = StateDb::open_in_memory()?;
    db.write(|s| {
        let a = local("a", NodeKind::Folder, 1);
        s.folder().put_local(&a)?;
        let x = local("a/x", NodeKind::File, 2);
        s.folder().put_local(&x)?;
        // Both ways in: a's id by its path, and its path by its id.
        assert_eq!(s.local_by_node(x.node)?.map(|r| r.path), Some(path("a/x")));
        s.folder().move_local_subtree(&path("a"), &path("b"))?;
        assert_eq!(s.local_by_node(x.node)?.map(|r| r.path), Some(path("b/x")));
        s.folder().put_local(&local("b/y", NodeKind::File, 3))?;
        assert!(
            s.folder()
                .put_local(&local("a/z", NodeKind::File, 4))
                .is_err(),
            "a went"
        );

        let renewed = NodeId::new();
        s.folder().rekey_local(a.node, renewed)?;
        let y = s
            .get_local(&path("b/y"))?
            .ok_or_else(|| StateError::Corrupt("b/y missing".into()))?;
        assert_eq!(s.local_parent(y.node)?, Some(renewed));
        s.folder().delete_local_subtree(&path("b"))?;
        assert!(
            s.folder()
                .put_local(&local("b/w", NodeKind::File, 5))
                .is_err(),
            "b went"
        );

        let c = local("c", NodeKind::Folder, 6);
        s.folder().put_local(&c)?;
        assert!(s.local_children(&path("c"))?.is_empty());
        assert!(s.folder().delete_local(c.node)?);
        assert!(
            s.folder()
                .put_local(&local("c/v", NodeKind::File, 7))
                .is_err(),
            "c went"
        );

        // A folder that became a file, then went.
        let d = local("d", NodeKind::Folder, 8);
        s.folder().put_local(&d)?;
        let d = LocalRow {
            kind: NodeKind::File,
            ..d
        };
        s.folder().put_local(&d)?;
        assert!(s.folder().delete_local(d.node)?);
        assert!(
            s.folder()
                .put_local(&local("d/u", NodeKind::File, 9))
                .is_err(),
            "d went"
        );
        Ok(())
    })?;
    Ok(())
}

/// What another writer put in a node and this client doesn't act on is written back as read:
/// a file without `mtime` stays without one (never 1970), a folder keeps its `mtime`, `size` and
/// `x`, and tags keep their form while the set is the same.
#[test]
fn a_rewrite_keeps_what_another_writer_wrote() -> R {
    use hbsync_state::MirrorRow;
    use hbsync_types::schema::NodeContent;
    let mut db = StateDb::open_in_memory()?;
    // Another writer's entry with a key of its own (written back with its keys sorted).
    let raw_tags = r#""tags":[{"by":"phone","c":6,"n":"Red"}]"#;
    let file = NodeContent::from_json(&format!(r#"{{"v":1,"name":"f","size":1,{raw_tags}}}"#))
        .expect("content");
    let folder = NodeContent::from_json(r#"{"v":1,"name":"d","mtime":5,"size":0,"x":true}"#)
        .expect("content");
    let hash = content(b"f").hash;
    let rows = [
        MirrorRow::from_content(
            remote(),
            NodeId::new(),
            NodeId::ROOT,
            NodeKind::File,
            &file,
            Some(hash),
        ),
        MirrorRow::from_content(
            remote(),
            NodeId::new(),
            NodeId::ROOT,
            NodeKind::Folder,
            &folder,
            None,
        ),
    ];
    db.write(|s| rows.iter().try_for_each(|r| s.account().put_live(r)))?;
    let back = db.read(|s| {
        rows.iter()
            .map(|r| s.get_mirror(r.node).map(|m| m.expect("stored")))
            .collect::<Result<Vec<_>, _>>()
    })?;
    // A rename writes the content again, with the tags it holds.
    let renamed = |row: &MirrorRow| {
        let mut c = row.content("new");
        c.set_tags_over(&row.tags(), &row.tags());
        c.to_json()
    };
    assert_eq!(
        renamed(&back[0]),
        format!(r#"{{"v":1,"name":"new","size":1,{raw_tags}}}"#)
    );
    assert_eq!(
        back[0].state().and_then(|s| s.content).map(|c| c.mtime_ms),
        Some(0)
    );
    assert_eq!(
        renamed(&back[1]),
        r#"{"v":1,"name":"new","mtime":5,"size":0,"x":true}"#
    );
    Ok(())
}

/// An answer to this device's write goes into the picture while it holds the version the write
/// replaced, or the answer's own (its echo first); over a newer state a pull recorded since,
/// it stays out.
#[test]
fn an_answer_records_only_over_what_its_write_replaced() -> R {
    use hbsync_state::{Answer, Answered, MirrorRow, RemovedRow};
    let mut db = StateDb::open_in_memory()?;
    let node = NodeId::new();
    let file_id = RemoteFileId(Uuid::new_v4());
    let at = |v: u128, name: &str| MirrorRow {
        file_id,
        version: VersionTag(Uuid::from_u128(v)),
        node,
        parent: NodeId::ROOT,
        name: name.into(),
        kind: NodeKind::Folder,
        file: None,
        content_extra: None,
        token: None,
        changed_at: Some(v as i64),
    };
    let answer = |expected: Option<u128>, row: MirrorRow, removed| {
        let expected = expected.map(|v| VersionTag(Uuid::from_u128(v)));
        match removed {
            true => Answer::removal(expected, row),
            false => Answer::live(expected, row),
        }
    };
    let picture = |db: &mut StateDb| -> Result<_, StateError> {
        db.read(|s| {
            let live = s.get_mirror(node)?.map(|m| m.version.0.as_u128());
            let removed = s.removed_by_node(node)?.map(|r| r.row.version.0.as_u128());
            Ok((live, removed))
        })
    };
    // Made: over nothing.
    let made = db.write(|s| s.account().answered(&answer(None, at(1, "d"), false)))?;
    assert_eq!(
        (made, picture(&mut db)?),
        (Answered::Recorded, (Some(1), None))
    );
    // Renamed over 1; removed by another device meanwhile (3): that stays.
    db.write(|s| {
        s.account().move_to_removed(&RemovedRow {
            row: at(3, "d"),
            removed_at: Some(3),
        })
    })?;
    let renamed = db.write(|s| s.account().answered(&answer(Some(1), at(2, "e"), false)))?;
    assert_eq!(
        (renamed, picture(&mut db)?),
        (Answered::Superseded, (None, Some(3)))
    );
    // Restored over 3, its echo first: as it is.
    db.write(|s| s.account().put_live(&at(4, "d")))?;
    let back = db.write(|s| s.account().answered(&answer(Some(3), at(4, "d"), false)))?;
    assert_eq!(
        (back, picture(&mut db)?),
        (Answered::Recorded, (Some(4), None))
    );
    // Removed over 4: out of the tree.
    let gone = db.write(|s| s.account().answered(&answer(Some(4), at(5, "d"), true)))?;
    assert_eq!(
        (gone, picture(&mut db)?),
        (Answered::Recorded, (None, Some(5)))
    );
    // Restored over 5; a version since this client can't read (6): held as it was.
    db.write(|s| {
        let at = RemoteRef {
            file_id,
            version: VersionTag(Uuid::from_u128(6)),
        };
        s.account()
            .put_unreadable(at, Some(node), Some(NodeId::ROOT))
    })?;
    let back = db.write(|s| s.account().answered(&answer(Some(5), at(7, "d"), false)))?;
    assert_eq!(
        (back, picture(&mut db)?),
        (Answered::Superseded, (None, Some(5)))
    );
    assert!(db.read(|s| s.held_files())?.contains(&file_id));
    // Deleted for good since: never back.
    db.write(|s| s.account().forget_file(file_id, None))?;
    let back = db.write(|s| s.account().answered(&answer(Some(5), at(8, "d"), false)))?;
    assert_eq!(
        (back, picture(&mut db)?),
        (Answered::Superseded, (None, None))
    );
    // A file the write made, deleted for good before its answer: gone.
    let made = MirrorRow {
        file_id: RemoteFileId(Uuid::new_v4()),
        ..at(9, "n")
    };
    db.write(|s| s.account().forget_file(made.file_id, None))?;
    let lost = db.write(|s| s.account().answered(&answer(None, made, false)))?;
    assert_eq!(lost, Answered::Superseded);
    Ok(())
}
