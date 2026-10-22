//! The executor on a hand-made state: operations and recovery where the plan, the mirror and
//! the disk disagree in ways a sync of two clients rarely reaches.

use super::recover::recover_placements;
use super::*;
use crate::scope::Scope;
use hbsync_fileio::hash_bytes;
use hbsync_platform::tags;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::testkit::RawPicture;
use hbsync_state::{MirrorFile, PlannedBase, RemovedRow};
use hbsync_types::schema::NodeContent;
use hbsync_types::{FileContent, NodeState, RemoteRef, Tag};
use std::path::PathBuf;

type R = Result<(), Box<dyn std::error::Error>>;

struct Harness {
    _dir: tempfile::TempDir,
    db: StateDb,
    remote: FakeRemote,
    folder: FolderCtx,
    cfg: ExecConfig,
}

impl Harness {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        Ok(Harness {
            folder: FolderCtx::new(dir.path()),
            _dir: dir,
            db: StateDb::open_in_memory()?,
            remote: FakeRemote::new(FakeServer::new(ServerConfig::default())),
            cfg: ExecConfig::default(),
        })
    }

    /// The native path of `rel` under the root.
    fn at(&self, rel: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        Ok(RelPath::new(rel)?.to_native(&self.folder.root))
    }

    /// Writes `data` at `rel` and records it as the scan would: `node`, hashed.
    fn local_file(&mut self, rel: &str, node: NodeId, data: &[u8]) -> R {
        let path = self.at(rel)?;
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        fs::write(&path, data)?;
        let row = LocalRow {
            path: RelPath::new(rel)?,
            node,
            kind: NodeKind::File,
            fingerprint: hbsync_platform::stat(&path)?.fingerprint(),
            executable: Some(false),
            hash: Some(hash_bytes(data, self.folder.algorithm)),
            tags: None,
        };
        self.db.write(|s| s.folder().put_local(&row))?;
        Ok(())
    }

    /// Records the folder at `rel` as the scan would.
    fn local_folder(&mut self, rel: &str, node: NodeId) -> R {
        let path = self.at(rel)?;
        fs::create_dir_all(&path)?;
        let row = LocalRow {
            path: RelPath::new(rel)?,
            node,
            kind: NodeKind::Folder,
            fingerprint: hbsync_platform::stat(&path)?.fingerprint(),
            executable: None,
            hash: None,
            tags: None,
        };
        self.db.write(|s| s.folder().put_local(&row))?;
        Ok(())
    }

    fn content(&self, data: &[u8]) -> FileContent {
        FileContent {
            hash: hash_bytes(data, self.folder.algorithm),
            size: data.len() as u64,
            mtime_ms: 0,
            executable: false,
        }
    }

    fn run(&mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        Executor::new(&mut self.db, &self.remote, &self.folder, &self.cfg).run(ops)
    }

    fn base(&mut self, node: NodeId) -> Result<Option<BaseRow>, StateError> {
        self.db.read(|s| s.get_base(node))
    }

    /// A file `name` at the top of the drive holding `data`, and the mirror's row of it.
    fn seed(
        &mut self,
        name: &str,
        data: &[u8],
    ) -> Result<(NodeId, RemoteRef), Box<dyn std::error::Error>> {
        let (node, at) =
            self.remote
                .server()
                .seed_file(NodeId::ROOT, name, data, self.folder.algorithm)?;
        self.mirror(node, name, at, data)?;
        Ok((node, at))
    }

    /// The mirror's row of `node`: a file `name` at the top holding `data`, at `at`.
    fn mirror(&mut self, node: NodeId, name: &str, at: RemoteRef, data: &[u8]) -> R {
        let c = self.content(data);
        self.db.write(|s| {
            s.account().put_mirror(&MirrorRow {
                file_id: at.file_id,
                version: at.version,
                node,
                parent: NodeId::ROOT,
                name: name.into(),
                kind: NodeKind::File,
                file: Some(MirrorFile {
                    hash: c.hash,
                    size: c.size,
                    mtime_ms: Some(0),
                    executable: false,
                }),
                content_extra: Default::default(),
                token: None,
                changed_at: None,
            })
        })?;
        Ok(())
    }

    fn records(&mut self) -> Result<Vec<Placing>, StateError> {
        self.db.read(|s| s.all_placing())
    }

    /// The scan read these tags on the item at `rel`.
    fn scanned_with(&mut self, rel: &str, tags: Tags) -> R {
        let path = RelPath::new(rel)?;
        self.db.write(|s| {
            let Some(row) = s.get_local(&path)? else {
                return Ok(());
            };
            s.folder().put_local(&LocalRow {
                tags: Some(tags),
                ..row
            })
        })?;
        Ok(())
    }
}

/// Whether tags can be written here (this OS, and the file system of the test's folder).
fn tags_kept(dir: &Path) -> bool {
    let probe = dir.join(".probe");
    tags::CAN_TAG
        && fs::write(&probe, b"").is_ok()
        && tags::write(&probe, &Tags::default())
            .ok()
            .flatten()
            .is_some()
}

fn some_version() -> RemoteRef {
    RemoteRef {
        file_id: hbsync_types::RemoteFileId(hbsync_types::Uuid::new_v4()),
        version: hbsync_types::VersionTag(hbsync_types::Uuid::new_v4()),
    }
}

/// An upload whose earlier try landed (its answer lost) finds the node there on the retry. The
/// drive holds the bytes the file had then; the user has edited it since. The base mustn't
/// take the drive's bytes for the local ones: the edit would never go up.
#[test]
fn an_upload_that_landed_earlier_agrees_only_on_the_same_bytes() -> R {
    let mut h = Harness::new()?;
    let (node, _) = h.seed("f", b"A")?;
    h.local_file("f", node, b"B")?;
    h.run(vec![Op::RemoteCreate {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        kind: NodeKind::File,
        tags: Tags::default(),
    }])?;
    let base = h.base(node)?;
    assert!(
        base.is_none(),
        "the base claims the drive's bytes for the edited file: {base:?}"
    );
    Ok(())
}

/// A move to where the item already is puts nothing in place: a node without a base doesn't
/// get one from the drive's version on content alone (the drive has it elsewhere).
#[test]
fn a_move_to_where_it_is_places_nothing() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    h.local_file("f", node, b"A")?;
    let drive = DriveVersion {
        state: NodeState::file(NodeId::ROOT, "g", h.content(b"A")),
        at: some_version(),
        base_at: None,
    };
    h.run(vec![Op::LocalMove {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        drive: Some(Box::new(drive)),
    }])?;
    let base = h.base(node)?;
    assert!(base.is_none(), "a base from content alone: {base:?}");
    Ok(())
}

/// A download put in place, its commit lost, recovered: the base the commit writes, against
/// the version it acted on, and the record goes.
#[test]
fn recovery_writes_the_commits_base() -> R {
    let mut h = Harness::new()?;
    let (folder, node) = (NodeId::new(), NodeId::new());
    let fetched = h.content(b"A");
    let at = some_version();
    h.local_folder("d", folder)?;
    h.local_file("d/f", NodeId::new(), b"A")?;
    let record = Placing {
        base: BaseRow::new(node, NodeState::file(folder, "f", fetched), at),
        target: RelPath::new("d/f")?,
        placed_tags: Some(Tags::default()),
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        held_ms: None,
    };
    h.db.write(|s| s.folder().put_placing(&record))?;
    recover_placements(&mut h.db, &h.folder)?;
    let base = h.base(node)?.ok_or("no base")?;
    assert_eq!(base.state.content, Some(fetched));
    assert_eq!(base.remote, at);
    assert!(h.db.read(|s| s.all_placing())?.is_empty());
    Ok(())
}

/// A download put in place with a tag, its commit lost; the user removes the tag before the
/// next sync. Recovery reads the item as it was put in place: the base holds the tag (both
/// sides had it then), so its removal is the user's change and goes to the drive, instead of
/// the drive's copy bringing it back.
#[test]
fn recovery_reads_the_tags_the_item_was_placed_with() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let work = Tags::new(vec![hbsync_types::Tag::new("Work", 0)]);
    h.local_file("f", NodeId::new(), b"A")?;
    let path = RelPath::new("f")?;
    h.db.write(|s| {
        let Some(row) = s.get_local(&path)? else {
            return Ok(());
        };
        // The tag removed: the scan reads none.
        s.folder().put_local(&LocalRow {
            tags: Some(Tags::default()),
            ..row
        })
    })?;
    let drive = NodeState {
        tags: work.clone(),
        ..NodeState::file(NodeId::ROOT, "f", h.content(b"A"))
    };
    let record = Placing {
        base: BaseRow::new(node, drive, some_version()),
        target: path,
        placed_tags: Some(work.clone()),
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        held_ms: None,
    };
    h.db.write(|s| s.folder().put_placing(&record))?;
    recover_placements(
        &mut h.db,
        &FolderCtx {
            os: Os::Linux,
            ..h.folder.clone()
        },
    )?;
    let base = h.base(node)?.ok_or("no base")?;
    assert_eq!(base.state.tags, work);
    Ok(())
}

/// A download put in place, its commit lost, where the item can't be read as a node (its
/// name grows past the drive's limit in NFC: U+0958 is excluded from composition). The commit
/// writes no base then and keeps its record; recovery does the same instead of dropping the
/// record (a first download without a base comes back as a conflict of two new files).
#[test]
fn recovery_keeps_a_record_it_cannot_settle() -> R {
    let (mut h, record) = a_record_it_cannot_settle(false)?;
    let node = record.base.node;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.base(node)?, None);
    assert_eq!(h.db.read(|s| s.all_placing())?, [record]);
    // Taken over: the next recovery finds it as the node's.
    let row = h.db.read(|s| s.local_by_node(node))?;
    assert_eq!(row.map(|r| r.path.to_string()), Some("\u{958}".repeat(85)));
    Ok(())
}

/// The same, its node gone on the drive since: held as a file not hashed yet is, then dropped
/// (kept for good, it held its node, a deleted one, for ever).
#[test]
fn recovery_holds_a_record_of_a_deleted_node_it_cannot_settle() -> R {
    let (mut h, record) = a_record_it_cannot_settle(true)?;
    let node = record.base.node;
    recover_placements(&mut h.db, &h.folder)?;
    let held = h.db.read(|s| s.get_placing(node))?.expect("held");
    assert!(held.held_ms.is_some(), "{held:?}");
    assert_eq!(h.base(node)?, None);
    // Held long enough: it goes.
    let since = hbsync_types::unix_now_ms() - UNHASHED_HOLD.as_millis() as i64 - 1;
    h.db.write(|s| {
        s.folder().put_placing(&Placing {
            held_ms: Some(since),
            ..held
        })
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    Ok(())
}

/// A download put in place, its commit lost, where the item can't be read as a node (its
/// name grows past the drive's limit in NFC: U+0958 is excluded from composition); its node
/// still on the drive, or `gone`.
fn a_record_it_cannot_settle(gone: bool) -> Result<(Harness, Placing), Box<dyn std::error::Error>> {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let name = "\u{958}".repeat(85);
    assert!(hbsync_namemap::to_remote(&name, h.folder.os).is_err());
    let fetched = h.content(b"A");
    h.local_file(&name, NodeId::new(), b"A")?;
    let at = some_version();
    let record = Placing {
        base: BaseRow::new(node, NodeState::file(NodeId::ROOT, "f", fetched), at),
        target: RelPath::new(&name)?,
        placed_tags: Some(Tags::default()),
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        held_ms: None,
    };
    h.db.write(|s| {
        if !gone {
            s.account().put_mirror(&MirrorRow::from_content(
                at,
                node,
                NodeId::ROOT,
                NodeKind::File,
                &NodeContent::file("f", 0, 1, false),
                Some(fetched.hash),
            ))?;
        }
        s.folder().put_placing(&record)
    })?;
    Ok((h, record))
}

/// The plan for `nodes` from what the DB holds now.
fn plan_of(h: &mut Harness, nodes: &[NodeId]) -> Result<crate::plan::Plan, StateError> {
    let folder = FolderCtx {
        scope: Arc::new(Scope::of(NodeId::ROOT)),
        ..h.folder.clone()
    };
    let input = h.db.read(|s| view::build_input(s, nodes, &folder))?;
    Ok(crate::reconcile::reconcile(
        &input,
        &crate::plan::Options::default(),
        folder.masking(),
    ))
}

/// A folder synced before, tagged on the drive, made again here: a crash after it is made and
/// before its tags are written leaves it without them. Its record claims no tags (none written
/// yet): recovery reads the folder's tags as not known, the drive's, so the next plan removes
/// none from the drive. A record that claimed them up front took the empty folder for agreed,
/// and the next plan removed them.
#[test]
fn a_folder_made_before_its_tags_keeps_them_on_the_drive() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let at = some_version();
    let state = NodeState {
        tags: work.clone(),
        ..NodeState::folder(NodeId::ROOT, "d")
    };
    let mut content = NodeContent::folder("d");
    content.set_tags(&work);
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        // Synced before (made again after a delete here lost): it gets a record.
        s.folder().put_base(&BaseRow::new(node, state.clone(), at))
    })?;
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    let crashed = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: work.clone(),
        drive: Some(Box::new(DriveVersion {
            state,
            at,
            base_at: Some(at.version),
        })),
    }]);
    assert!(crashed.is_err(), "the crash point wasn't reached");
    let records = h.records()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].placed_tags, None, "it claims tags never written");
    // The scan finds the folder without tags.
    h.local_folder("d", NodeId::new())?;
    h.scanned_with("d", Tags::default())?;
    recover_placements(&mut h.db, &h.folder)?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteTags { .. } | Op::RemoteUpdate { .. })),
        "the drive's tags removed: {:?}",
        plan.ops
    );
    // Where tags are held here, the folder gets the drive's; elsewhere it reads as the drive's.
    assert!(
        !tags_kept(&h.folder.root)
            || plan
                .ops
                .iter()
                .any(|op| matches!(op, Op::LocalTags { node: n, .. } if *n == node)),
        "the folder doesn't get its tags: {:?}",
        plan.ops
    );
    Ok(())
}

/// A folder synced before, tagged Work, deleted here and on the drive (in Recently deleted,
/// Work there too), made again here as it comes back for a child another device wrote into it
/// (its RemoteCreate to follow): no drive version in the plan, none in the mirror. A crash after
/// it is made and before its tags are written: its record (on the version in Recently deleted)
/// lets recovery take the folder for the node, holding no tags, not known. Another device
/// restores it, with Work: the next plan writes Work here, removes nothing from the drive, and
/// never deletes the node there. Without a record, the scan found a new folder, which adoption
/// doesn't pair with a node synced before: the node read as deleted here and went from the
/// drive, its tags with it (sim seed e7972f95's family).
#[test]
fn a_folder_made_from_recently_deleted_keeps_its_record_through_a_crash() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let (synced, removed_at) = (some_version(), some_version());
    let state = NodeState {
        tags: work.clone(),
        ..NodeState::folder(NodeId::ROOT, "d")
    };
    let mut content = NodeContent::folder("d");
    content.set_tags(&work);
    let row =
        |at| MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::Folder, &content, None);
    let in_rd = row(RemoteRef {
        file_id: synced.file_id,
        ..removed_at
    });
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: in_rd.clone(),
            removed_at: Some(1),
        })?;
        s.folder()
            .put_base(&BaseRow::new(node, state.clone(), synced))
    })?;
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    let crashed = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: work.clone(),
        drive: None,
    }]);
    assert!(crashed.is_err(), "the crash point wasn't reached");
    let records = h.records()?;
    assert_eq!(records.len(), 1, "a record before the folder is made");
    assert_eq!(
        records[0].base.remote,
        in_rd.remote_ref(),
        "on the version it is made of"
    );
    assert_eq!(records[0].placed_tags, None, "it claims tags never written");
    // The scan finds the folder, new, without tags; recovery takes it for the node.
    let found = NodeId::new();
    h.local_folder("d", found)?;
    h.scanned_with("d", Tags::default())?;
    recover_placements(&mut h.db, &h.folder)?;
    assert!(h.records()?.is_empty(), "settled");
    let local = h.db.read(|s| s.local_by_node(node))?;
    assert!(local.is_some(), "the folder is the node's");
    let base = h.base(node)?.ok_or("its base")?;
    assert_eq!(
        base.remote,
        in_rd.remote_ref(),
        "settled where it was made from"
    );
    // What the folder and that version both hold: none; where the volume keeps no tags, the
    // folder reads as the drive's.
    if tags_kept(&h.folder.root) {
        assert_eq!(base.state.tags, Tags::default(), "the folder holds none");
    }
    // Another device restores it, with Work.
    let restored = row(RemoteRef {
        file_id: synced.file_id,
        version: some_version().version,
    });
    h.db.write(|s| {
        s.account().delete_removed(node)?;
        s.account().put_mirror(&restored)
    })?;
    let plan = plan_of(&mut h, &[node, found])?;
    assert!(
        !plan.ops.iter().any(|op| matches!(
            op,
            Op::RemoteTags { .. } | Op::RemoteUpdate { .. } | Op::RemoteDelete { .. }
        )),
        "the drive's node or its tags removed: {:?}",
        plan.ops
    );
    assert!(
        !tags_kept(&h.folder.root)
            || plan
                .ops
                .iter()
                .any(|op| matches!(op, Op::LocalTags { node: n, .. } if *n == node)),
        "the folder doesn't get its tags: {:?}",
        plan.ops
    );
    Ok(())
}

/// The same, from a record written before records said which base their plan saw (v0.6.0 and
/// earlier): recovery settles it against the base as it is. Read as "the plan saw no base",
/// the folder's base looked moved: the record went unsettled, and the next plan took the
/// folder's missing tags for the user's and removed them from the drive.
#[test]
fn a_folder_from_an_older_record_keeps_its_tags_on_the_drive() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let at = some_version();
    let state = NodeState {
        tags: work.clone(),
        ..NodeState::folder(NodeId::ROOT, "d")
    };
    let mut content = NodeContent::folder("d");
    content.set_tags(&work);
    let target = RelPath::new("d")?;
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.folder()
            .put_base(&BaseRow::new(node, state.clone(), at))?;
        s.folder().put_placing(&Placing {
            base: BaseRow::new(node, state, at),
            target,
            placed_tags: None,
            prior_tags: None,
            base_at: PlannedBase::Unchecked,
            held_ms: None,
        })
    })?;
    // The scan finds the folder, made without tags.
    h.local_folder("d", NodeId::new())?;
    h.scanned_with("d", Tags::default())?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteTags { .. } | Op::RemoteUpdate { .. })),
        "the drive's tags removed: {:?}",
        plan.ops
    );
    // Where tags are held here, the folder gets the drive's; elsewhere it reads as the drive's.
    assert!(
        !tags_kept(&h.folder.root)
            || plan
                .ops
                .iter()
                .any(|op| matches!(op, Op::LocalTags { node: n, .. } if *n == node)),
        "the folder doesn't get its tags: {:?}",
        plan.ops
    );
    Ok(())
}

/// Where the volume keeps no tags (Windows, say), a placement claims none written (not known),
/// and settles as main does: a folder made again or a file downloaded, tagged on the drive,
/// committed or recovered after a crash, never takes a tag off the drive. A folder claimed
/// none written there, as if it held none.
#[test]
fn no_placement_takes_tags_off_the_drive_where_none_are_kept() -> R {
    for crash in [false, true] {
        for folder in [false, true] {
            let mut h = Harness::new()?;
            h.folder.os = Os::Windows;
            let _ = h.folder.tags_kept.set(false);
            let work = Tags::new(vec![Tag::new("Work", 0)]);
            let (node, at) = match folder {
                true => (NodeId::new(), some_version()),
                false => h.seed("x", b"A")?,
            };
            let mut content = match folder {
                true => NodeContent::folder("x"),
                false => NodeContent::file("x", 0, 1, false),
            };
            content.set_tags(&work);
            let state = NodeState {
                tags: work.clone(),
                ..match folder {
                    true => NodeState::folder(NodeId::ROOT, "x"),
                    false => NodeState::file(NodeId::ROOT, "x", h.content(b"A")),
                }
            };
            let kind = if folder {
                NodeKind::Folder
            } else {
                NodeKind::File
            };
            let hash = (!folder).then(|| h.content(b"A").hash);
            h.db.write(|s| {
                s.account().put_mirror(&MirrorRow::from_content(
                    at,
                    node,
                    NodeId::ROOT,
                    kind,
                    &content,
                    hash,
                ))?;
                // Synced before, tagged: a folder made again gets a record.
                s.folder().put_base(&BaseRow::new(node, state.clone(), at))
            })?;
            let drive = Some(Box::new(DriveVersion {
                state,
                at,
                base_at: Some(at.version),
            }));
            let op = match folder {
                true => Op::LocalMkdir {
                    node,
                    parent: NodeId::ROOT,
                    name: "x".into(),
                    tags: work.clone(),
                    drive,
                },
                false => Op::LocalDownload {
                    node,
                    parent: NodeId::ROOT,
                    name: "x".into(),
                    replace: false,
                    tags: work.clone(),
                    drive,
                },
            };
            if crash {
                h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
                assert!(h.run(vec![op]).is_err(), "the crash point wasn't reached");
                // A folder claims nothing written. (A download claims what reads back: on
                // Windows nothing, the volume unsupported; this test's volume keeps tags.)
                assert!(
                    !folder || h.records()?.iter().all(|r| r.placed_tags.is_none()),
                    "a record claims tags written where none are kept: {:?}",
                    h.records()?
                );
                // The scan finds the item, its tags not known.
                match folder {
                    true => h.local_folder("x", NodeId::new())?,
                    false => h.local_file("x", NodeId::new(), b"A")?,
                }
                recover_placements(&mut h.db, &h.folder)?;
            } else {
                h.run(vec![op])?;
            }
            assert!(h.base(node)?.is_some(), "crash {crash}, folder {folder}");
            let plan = plan_of(&mut h, &[node])?;
            assert!(
                !plan.ops.iter().any(|op| match op {
                    Op::RemoteTags { tags, .. } => tags.get("Work").is_none(),
                    Op::RemoteUpdate { tags, .. } => tags.get("Work").is_none(),
                    _ => false,
                }),
                "crash {crash}, folder {folder}: the drive's tag removed: {:?}",
                plan.ops
            );
        }
    }
    Ok(())
}

/// An untagged file put in place, its commit lost (its record claims no tags); the user tags it
/// before the next sync. Recovery takes the tag for the user's, and it goes to the drive.
/// Read as not known (the drive's tags), it was lost.
#[test]
fn a_tag_added_before_recovery_goes_up() -> R {
    let mut h = Harness::new()?;
    record_of_a_crashed_download(&mut h, &Tags::default())?;
    let node = h.records()?[0].base.node;
    let red = Tags::new(vec![Tag::new("Red", 0)]);
    h.local_file("f", NodeId::new(), b"A")?;
    h.scanned_with("f", red.clone())?;
    recover_placements(
        &mut h.db,
        &FolderCtx {
            os: Os::Linux,
            ..h.folder.clone()
        },
    )?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::RemoteTags { node: n, tags, .. } if *n == node && *tags == red)
        ),
        "the user's tag doesn't go up: {:?}",
        plan.ops
    );
    Ok(())
}

/// Folders are made in place: nothing in the client's temporary folder (on another volume, or
/// not even a folder, as here) stands in the way.
#[test]
fn a_folder_is_made_in_place() -> R {
    let mut h = Harness::new()?;
    let tmp = layout::tmp_dir(&h.folder.root);
    if let Some(dir) = tmp.parent() {
        fs::create_dir_all(dir)?;
    }
    fs::write(&tmp, b"not a folder")?;
    let node = NodeId::new();
    let drive = DriveVersion {
        state: NodeState::folder(NodeId::ROOT, "d"),
        at: some_version(),
        base_at: None,
    };
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: Some(Box::new(drive)),
    }])?;
    assert!(h.folder.root.join("d").is_dir(), "{report:?}");
    assert!(h.base(node)?.is_some());
    Ok(())
}

/// A download put in place, its commit lost (`placed`: what it is tagged with on the drive):
/// what its record says of its tags.
fn record_of_a_crashed_download(
    h: &mut Harness,
    placed: &Tags,
) -> Result<Option<Tags>, Box<dyn std::error::Error>> {
    let (node, at) = h.seed("f", b"A")?;
    let state = NodeState {
        tags: placed.clone(),
        ..NodeState::file(NodeId::ROOT, "f", h.content(b"A"))
    };
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    let crashed = h.run(vec![Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: false,
        tags: placed.clone(),
        drive: Some(Box::new(DriveVersion {
            state,
            at,
            base_at: None,
        })),
    }]);
    assert!(crashed.is_err(), "the crash point wasn't reached");
    assert_eq!(fs::read(h.folder.root.join("f"))?, b"A");
    let records = h.records()?;
    assert_eq!(records.len(), 1);
    Ok(records[0].placed_tags.clone())
}

/// A file edited on the drive and tagged there (work), tagged red here, and an operation of the
/// run settling its base at the drive's version before the download commits (as the drive's
/// write of red would): the download's commit, and recovery of it after a crash, leave the
/// same base. Neither takes the run's own settling for the version the plan saw (recovery
/// can't know it): the base keeps its bytes and version, and its tags agree with the version
/// the download acted on.
fn base_after_a_download(crash: bool) -> Result<Option<BaseRow>, Box<dyn std::error::Error>> {
    let mut h = Harness::new()?;
    let (node, at) = h.seed("f", b"B")?;
    let (work, both) = (
        Tags::new(vec![Tag::new("Work", 0)]),
        Tags::new(vec![Tag::new("Red", 0), Tag::new("Work", 0)]),
    );
    let mut content = NodeContent::file("f", 0, 1, false);
    content.set_tags(&work);
    let hash = h.content(b"B").hash;
    h.local_file("f", node, b"A")?;
    h.scanned_with("f", Tags::new(vec![Tag::new("Red", 0)]))?;
    let old = h.content(b"A");
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(old.hash),
            ..BaseRow::new(
                node,
                NodeState::file(NodeId::ROOT, "f", old),
                some_version(),
            )
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(hash),
        ))
    })?;
    let planned = h.base(node)?.map(|b| b.remote.version);
    let download = Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: true,
        tags: both,
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                tags: work,
                ..NodeState::file(NodeId::ROOT, "f", h.content(b"B"))
            },
            at,
            base_at: planned,
        })),
    };
    if crash {
        h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    }
    let ran = h.run(vec![Op::Settle { node }, download]);
    assert_eq!(ran.is_err(), crash, "{ran:?}");
    if crash {
        // The scan finds the replaced file, with what it holds of tags.
        let read = tags::read(&h.folder.root.join("f"));
        h.local_file("f", node, b"B")?;
        h.scanned_with("f", read.unwrap_or_default())?;
        recover_placements(&mut h.db, &h.folder)?;
        assert_eq!(h.records()?, []);
    }
    // Each harness has its own node: the rest is compared.
    Ok(h.base(node)?.map(|b| BaseRow {
        node: NodeId::ROOT,
        ..b
    }))
}

#[test]
fn a_download_commits_as_it_is_recovered() -> R {
    let committed = base_after_a_download(false)?;
    assert!(committed.is_some());
    assert_eq!(committed, base_after_a_download(true)?);
    Ok(())
}

/// A first download of a name the name map wouldn't give back from this device's spelling
/// (decomposed; on Windows, an escape's own code point) records the spelling, committed or
/// recovered: the item reads as the drive's name, not as renamed.
#[test]
fn a_first_download_records_the_local_spelling() -> R {
    for (os, name) in [
        (Os::Linux, "e\u{301}"),
        (Os::MacOs, "e\u{301}"),
        (Os::Windows, "a\u{F022}b"),
    ] {
        for crash in [false, true] {
            let mut h = Harness::new()?;
            h.folder.os = os;
            let (node, at) = h.seed(name, b"A")?;
            if crash {
                h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
            }
            let ran = h.run(vec![Op::LocalDownload {
                node,
                parent: NodeId::ROOT,
                name: name.into(),
                replace: false,
                tags: Tags::default(),
                drive: Some(Box::new(DriveVersion {
                    state: NodeState::file(NodeId::ROOT, name, h.content(b"A")),
                    at,
                    base_at: None,
                })),
            }]);
            assert_eq!(ran.is_err(), crash, "{ran:?}");
            if crash {
                h.local_file(name, node, b"A")?;
                recover_placements(&mut h.db, &h.folder)?;
                assert_eq!(h.records()?, []);
            }
            let base = h.base(node)?.ok_or("a base")?;
            assert_eq!(
                base.local_name.as_deref(),
                Some(name),
                "{os:?}, crash {crash}"
            );
            let read = h.db.read(|s| view::local_node(s, node, os, None))?;
            assert_eq!(
                read.and_then(|l| l.state).map(|l| l.name).as_deref(),
                Some(name),
                "{os:?}, crash {crash}"
            );
        }
    }
    Ok(())
}

/// A download's record claims the tags it writes, the empty set too, as they read back: an
/// untagged file is known to hold none. (Claiming none for it read as not known, and a tag the
/// user had removed came back.) Where the volume keeps no tags, it claims none.
#[test]
fn a_record_claims_the_tags_it_writes() -> R {
    let mut h = Harness::new()?;
    let kept = tags_kept(&h.folder.root);
    let none = record_of_a_crashed_download(&mut h, &Tags::default())?;
    assert_eq!(none, kept.then(Tags::default));
    let mut h = Harness::new()?;
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let expected = kept.then(|| work.held_by(h.folder.os));
    assert_eq!(record_of_a_crashed_download(&mut h, &work)?, expected);
    Ok(())
}

/// A file synced with a tag, the tag removed here, the file edited on the drive: the download
/// writes the merged tags (none), its commit lost. Recovery takes the record's word that the
/// file holds none: the base keeps the tag, so the next plan takes it off the drive instead of
/// writing it back.
#[test]
fn a_removed_tag_stays_removed_through_recovery() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // no tag can be removed here
    }
    let (node, at) = h
        .remote
        .server()
        .seed_file(NodeId::ROOT, "f", b"B", h.folder.algorithm)?;
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut content = NodeContent::file("f", 0, 1, false);
    content.set_tags(&work);
    let earlier = NodeState {
        tags: work.clone(),
        ..NodeState::file(NodeId::ROOT, "f", h.content(b"A"))
    };
    let fetched = NodeState {
        tags: work.clone(),
        ..NodeState::file(NodeId::ROOT, "f", h.content(b"B"))
    };
    let hash = h.content(b"B").hash;
    let base = some_version();
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(hash),
        ))?;
        s.folder().put_base(&BaseRow::new(node, earlier, base))?;
        s.folder().put_placing(&Placing {
            base: BaseRow::new(node, fetched, at),
            target: RelPath::new("f").expect("a path"),
            placed_tags: Some(Tags::default()),
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(base.version),
            held_ms: None,
        })
    })?;
    // The scan finds the new bytes, untagged.
    h.local_file("f", NodeId::new(), b"B")?;
    h.scanned_with("f", Tags::default())?;
    recover_placements(
        &mut h.db,
        &FolderCtx {
            os: Os::Linux,
            ..h.folder.clone()
        },
    )?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan.ops.iter().any(|op| matches!(op, Op::LocalTags { .. })),
        "the removed tag comes back: {:?}",
        plan.ops
    );
    assert!(
        plan.ops.iter().any(|op| matches!(
            op,
            Op::RemoteTags { node: n, tags, .. } if *n == node && tags.is_empty()
        )),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// A folder made again with no tags holds none as soon as it is made: its record claims the
/// empty set from the start (a crash right after it is made leaves that true). Where tags
/// aren't kept, it claims nothing (not known).
#[test]
fn an_untagged_folder_claims_no_tags() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let at = some_version();
    let state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| s.folder().put_base(&BaseRow::new(node, state.clone(), at)))?;
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    let crashed = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state,
            at,
            base_at: Some(at.version),
        })),
    }]);
    assert!(crashed.is_err(), "the crash point wasn't reached");
    let expected = tags::CAN_TAG.then(Tags::default);
    assert_eq!(h.records()?[0].placed_tags, expected);
    Ok(())
}

/// Tags that can't be written (a value too large for the file system's attributes) leave the
/// file without them, and its record claims none: recovery reads the item as not known (the
/// drive's tags standing), as a commit does whose tags write failed.
#[test]
fn a_tag_write_that_fails_is_never_claimed() -> R {
    let mut h = Harness::new()?;
    let huge = Tags::new(vec![Tag::new("x".repeat(70_000), 0)]);
    let probe = h.folder.root.join(".huge");
    fs::write(&probe, b"")?;
    if !tags_kept(&h.folder.root) || tags::write(&probe, &huge).is_ok() {
        return Ok(()); // no tags here, or this file system takes it
    }
    assert_eq!(record_of_a_crashed_download(&mut h, &huge)?, None);
    // The scan finds the file; recovery writes what the commit would have: the drive's tags.
    let node = h.db.read(|s| s.all_placing())?[0].base.node;
    h.local_file("f", NodeId::new(), b"A")?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.base(node)?.ok_or("no base")?.state.tags, huge);
    Ok(())
}

/// A download that fails after its batch's records are written (the file gone from the drive)
/// leaves no record: recovery can never take an item the user puts at that place for it.
#[test]
fn a_failed_download_leaves_no_record() -> R {
    let mut h = Harness::new()?;
    let (node, at) = h.seed("f", b"A")?;
    h.remote.server().delete(at.file_id, at.version)?;
    let drive = DriveVersion {
        state: NodeState::file(NodeId::ROOT, "f", h.content(b"A")),
        at,
        base_at: None,
    };
    h.run(vec![Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: false,
        tags: Tags::default(),
        drive: Some(Box::new(drive)),
    }])?;
    assert!(!h.folder.root.join("f").exists());
    assert_eq!(h.records()?, []);
    Ok(())
}

/// A folder made, its commit lost, recovered: as its commit, what is in it on the drive is
/// marked dirty (it may have been put aside unseen), not only the folder.
#[test]
fn recovering_a_folder_marks_what_is_in_it() -> R {
    let mut h = Harness::new()?;
    let (folder, child) = (NodeId::new(), NodeId::new());
    let at = some_version();
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.account().put_mirror(&MirrorRow::from_content(
            some_version(),
            child,
            folder,
            NodeKind::Folder,
            &NodeContent::folder("e"),
            None,
        ))?;
        s.folder().put_placing(&Placing {
            base: BaseRow::new(folder, NodeState::folder(NodeId::ROOT, "d"), at),
            target: RelPath::new("d").expect("a path"),
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::Absent,
            held_ms: None,
        })?;
        // Pulled before: nothing marked now.
        s.folder().clear_dirty(child)
    })?;
    h.local_folder("d", NodeId::new())?;
    recover_placements(&mut h.db, &h.folder)?;
    assert!(h.base(folder)?.is_some());
    assert!(
        h.db.read(|s| s.is_dirty(child))?,
        "what is in it isn't looked at"
    );
    Ok(())
}

/// The mirror holds another version than the plan saw (another folder's engine pulled it, or
/// this run wrote it; the same bytes even): the download is stale for the next plan, and its
/// record never mixes one version's place and tags with another's.
#[test]
fn a_download_runs_only_on_the_version_the_plan_saw() -> R {
    let mut h = Harness::new()?;
    let (node, _) = h.seed("f", b"A")?;
    let planned = DriveVersion {
        state: NodeState::file(NodeId::ROOT, "f", h.content(b"A")),
        at: some_version(),
        base_at: None,
    };
    let report = h.run(vec![Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: false,
        tags: Tags::default(),
        drive: Some(Box::new(planned)),
    }])?;
    assert!(!h.folder.root.join("f").exists(), "fetched another version");
    assert_eq!(h.base(node)?, None);
    assert_eq!(report.skipped.len(), 1, "{report:?}");
    Ok(())
}

/// A remote that runs `hook` once, when a payload is first read (a download in flight).
struct ReadHook {
    inner: FakeRemote,
    hook: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Remote for ReadHook {
    fn changes(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::ChangesPage> {
        self.inner.changes(cursor, limit)
    }

    fn change_refs(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::RefsPage> {
        self.inner.change_refs(cursor, limit)
    }

    fn header(
        &self,
        file_id: hbsync_types::RemoteFileId,
    ) -> hbsync_remote::Result<hbsync_remote::RemoteHeader> {
        self.inner.header(file_id)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn holding(
        &self,
        folders: &[hbsync_types::NodeId],
        holds: hbsync_remote::Holds<'_>,
    ) -> hbsync_remote::Result<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn delete_for_good(
        &self,
        files: &[hbsync_types::Doomed],
    ) -> hbsync_remote::Result<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Written>> {
        self.inner.set_removed(files, removed)
    }

    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<Written> {
        self.inner.create(node, payload)
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<Written> {
        self.inner.update(target, meta, payload)
    }

    fn delete(
        &self,
        files: &[hbsync_types::Doomed],
    ) -> hbsync_remote::Result<hbsync_remote::Deletes> {
        self.inner.delete(files)
    }

    fn read(
        &self,
        file_id: hbsync_types::RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn io::Write,
    ) -> hbsync_remote::Result<u64> {
        let hook = self.hook.lock().ok().and_then(|mut h| h.take());
        if let Some(hook) = hook {
            hook();
        }
        self.inner.read(file_id, range, sink)
    }
}

/// The file is deleted on the drive while its download is in flight: the feed (another
/// engine's pull) removes its mirror row before the commit, and here marks nothing (a folder
/// that holds no row of it yet). The commit still settles against the version it fetched, and
/// the node stays dirty: the next plan takes the delete here. Without a base, it read the
/// fetched file as new and put it back on the drive; not dirty, nothing looked at it again.
#[test]
fn a_delete_while_the_download_is_in_flight_is_taken() -> R {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.db");
    let root = dir.path().join("root");
    fs::create_dir_all(&root)?;
    let (ctx, cfg) = (FolderCtx::new(&root), ExecConfig::default());
    let mut db = StateDb::open(&path)?;
    let fake = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let (node, at) = fake
        .server()
        .seed_file(NodeId::ROOT, "f", b"A", ctx.algorithm)?;
    let content = FileContent {
        hash: hash_bytes(b"A", ctx.algorithm),
        size: 1,
        mtime_ms: 0,
        executable: false,
    };
    db.write(|s| {
        // Planned because it was dirty: nothing clears that until a plan finds it clean.
        s.folder().mark_dirty(node)?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(content.hash),
        ))
    })?;
    let hook_path = path.clone();
    let remote = ReadHook {
        inner: fake,
        hook: std::sync::Mutex::new(Some(Box::new(move || {
            let gone = StateDb::open(&hook_path).and_then(|mut other| {
                other.write(|s| s.account().delete_mirror(at.file_id).map(drop))
            });
            assert!(gone.is_ok(), "{gone:?}");
        }))),
    };
    let drive = DriveVersion {
        state: NodeState::file(NodeId::ROOT, "f", content),
        at,
        base_at: None,
    };
    Executor::new(&mut db, &remote, &ctx, &cfg).run(vec![Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: false,
        tags: Tags::default(),
        drive: Some(Box::new(drive)),
    }])?;
    assert_eq!(fs::read(root.join("f"))?, b"A");
    assert!(db.read(|s| s.get_mirror(node))?.is_none(), "the hook ran");
    assert!(db.read(|s| s.is_dirty(node))?, "nothing looks at it again");
    let ctx = FolderCtx {
        scope: Arc::new(Scope::of(NodeId::ROOT)),
        ..ctx
    };
    let input = db.read(|s| view::build_input(s, &[node], &ctx))?;
    let plan = crate::reconcile::reconcile(&input, &crate::plan::Options::default(), ctx.masking());
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalTrash { node: n, .. } if *n == node)),
        "the delete reaches here: {:?}",
        plan.ops
    );
    Ok(())
}

/// A download in folder `d` put in place, and its commit failing after (the folder's row gone
/// meanwhile, as another engine could have removed it): its record stays, for recovery to
/// settle what is in place. Only one that failed before its item was in place goes.
#[test]
fn a_record_stays_when_the_commit_fails_after_placing() -> R {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.db");
    let root = dir.path().join("root");
    fs::create_dir_all(root.join("d"))?;
    let (ctx, cfg) = (FolderCtx::new(&root), ExecConfig::default());
    let mut db = StateDb::open(&path)?;
    let fake = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let folder = NodeId::new();
    let (node, at) = fake.server().seed_file(folder, "f", b"A", ctx.algorithm)?;
    let content = FileContent {
        hash: hash_bytes(b"A", ctx.algorithm),
        size: 1,
        mtime_ms: 0,
        executable: false,
    };
    db.write(|s| {
        s.folder().put_local(&LocalRow {
            path: RelPath::new("d").expect("a path"),
            node: folder,
            kind: NodeKind::Folder,
            fingerprint: hbsync_platform::stat(&root.join("d"))
                .expect("the folder")
                .fingerprint(),
            executable: None,
            hash: None,
            tags: None,
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            folder,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(content.hash),
        ))
    })?;
    let hook_path = path.clone();
    let remote = ReadHook {
        inner: fake,
        hook: std::sync::Mutex::new(Some(Box::new(move || {
            let gone = StateDb::open(&hook_path)
                .and_then(|mut other| other.write(|s| s.folder().delete_local(folder)));
            assert!(gone.is_ok(), "{gone:?}");
        }))),
    };
    let drive = DriveVersion {
        state: NodeState::file(folder, "f", content),
        at,
        base_at: None,
    };
    let _ = Executor::new(&mut db, &remote, &ctx, &cfg).run(vec![Op::LocalDownload {
        node,
        parent: folder,
        name: "f".into(),
        replace: false,
        tags: Tags::default(),
        drive: Some(Box::new(drive)),
    }]);
    assert_eq!(fs::read(root.join("d/f"))?, b"A", "put in place");
    assert_eq!(
        db.read(|s| s.local_by_node(node))?,
        None,
        "the commit failed"
    );
    assert_eq!(db.read(|s| s.all_placing())?.len(), 1, "the record went");
    Ok(())
}

/// A batch whose run is asked to stop while its first download is in flight: the one done
/// commits, the other never starts and leaves no record (one that runs later writes its own).
#[test]
fn a_stopped_batch_leaves_no_records() -> R {
    let mut h = Harness::new()?;
    h.cfg.transfer.workers = 1;
    let mut ops = Vec::new();
    for name in ["f", "g"] {
        let (node, at) = h.seed(name, b"A")?;
        ops.push(Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: name.into(),
            replace: false,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, name, h.content(b"A")),
                at,
                base_at: None,
            })),
        });
    }
    let stop = Arc::new(AtomicBool::new(false));
    let asked = Arc::clone(&stop);
    let remote = ReadHook {
        inner: h.remote.clone(),
        hook: std::sync::Mutex::new(Some(Box::new(move || {
            asked.store(true, Ordering::Relaxed);
        }))),
    };
    Executor::new(&mut h.db, &remote, &h.folder, &h.cfg)
        .with_stop(stop)
        .run(ops)?;
    let placed = ["f", "g"]
        .iter()
        .filter(|n| h.folder.root.join(n).exists())
        .count();
    assert_eq!(placed, 1, "one download ran");
    assert_eq!(h.records()?, []);
    Ok(())
}

/// An operation can come for a node that wasn't dirty (a sibling renamed for a clash, say).
/// Where the drive moved on from the version it acted on meanwhile, the node is dirty after:
/// the next plan looks at it.
#[test]
fn a_node_whose_drive_moved_on_stays_dirty() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let (acted, now) = (some_version(), some_version());
    let file = NodeState::file(NodeId::ROOT, "a", h.content(b"A"));
    h.local_file("a", node, b"A")?;
    h.mirror(node, "a", now, b"A")?;
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(h_content_hash(b"A")),
            ..BaseRow::new(node, file.clone(), acted)
        })?;
        s.folder().clear_dirty(node)
    })?;
    h.run(vec![Op::LocalMove {
        node,
        parent: NodeId::ROOT,
        name: "b".into(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                name: "b".into(),
                ..file
            },
            at: acted,
            base_at: Some(acted.version),
        })),
    }])?;
    assert!(h.folder.root.join("b").exists());
    assert!(
        h.db.read(|s| s.is_dirty(node))?,
        "nothing looks at it again"
    );
    Ok(())
}

fn h_content_hash(data: &[u8]) -> hbsync_types::ContentHash {
    hash_bytes(data, hbsync_fileio::HashAlgorithm::Blake3)
}

/// The drive tags a file red (and renames it, if `renamed`) while it is edited here (if
/// `edited`): one plan's writes on the node end in tagging it red here, each after the ones
/// before settled the base (the move at the drive's version, the upload at its own). Then the
/// user removes red: the next plan takes red off the drive. (A later write checked the base
/// against the plan's version only, failed, and left the tag unagreed: red came back.)
fn red_stays_removed_after(renamed: bool, edited: bool) -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, before) = h.seed("a", b"A")?;
    let red = Tags::new(vec![Tag::new("Red", 0)]);
    h.local_file("a", node, if edited { b"B" } else { b"A" })?;
    h.scanned_with("a", Tags::default())?;
    let name = if renamed { "b" } else { "a" };
    let mut content = NodeContent::file(name, 0, 1, false);
    content.set_tags(&red);
    let meta = NodeMeta {
        node,
        parent: NodeId::ROOT,
        kind: NodeKind::File,
        content: content.clone(),
        removed: false,
    };
    let after = h
        .remote
        .server()
        .update(before.file_id, before.version, &meta, None)?;
    let hash = h.content(b"A").hash;
    let synced = NodeState::file(NodeId::ROOT, "a", h.content(b"A"));
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(hash),
            ..BaseRow::new(node, synced, before)
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            after,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(hash),
        ))
    })?;
    let mut ops = plan_of(&mut h, &[node])?.ops;
    // The tag write last, as it runs when retried after the upload.
    ops.sort_by_key(|op| matches!(op, Op::LocalTags { .. }));
    let kinds: Vec<&str> = ops.iter().map(Op::kind).collect();
    let mut want = Vec::new();
    if renamed {
        want.push("LocalMove");
    }
    if edited {
        want.push("RemoteUpdate");
    }
    want.push("LocalTags");
    assert_eq!(kinds, want, "{ops:?}");
    let report = h.run(ops)?;
    assert_eq!(report.done, want.len(), "{report:?}");
    assert_eq!(tags::read(&h.folder.root.join(name)), Some(red.clone()));
    // The user removes red.
    tags::write(&h.folder.root.join(name), &Tags::default())?;
    h.scanned_with(name, Tags::default())?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::RemoteTags { node: n, tags, .. } if *n == node && tags.is_empty())
        ),
        "red comes back: {:?}",
        plan.ops
    );
    Ok(())
}

#[test]
fn a_tag_removed_after_a_rename_and_tag_stays_removed() -> R {
    red_stays_removed_after(true, false)
}

#[test]
fn a_tag_removed_after_an_upload_and_tag_stays_removed() -> R {
    red_stays_removed_after(false, true)
}

#[test]
fn a_tag_removed_after_a_rename_upload_and_tag_stays_removed() -> R {
    red_stays_removed_after(true, true)
}

/// A record an earlier run left (its item couldn't be read as a node yet, so recovery kept it)
/// holds off this run's placements of the node, a download and a folder made again: they
/// would write their own record over it, or remove it. The node stays dirty, nothing is put in
/// place, and the record waits for recovery: only the operation that wrote a record removes it.
#[test]
fn an_earlier_runs_record_holds_off_a_placement() -> R {
    let mut h = Harness::new()?;
    let (file, at) = h.seed("f", b"A")?;
    let file_state = NodeState::file(NodeId::ROOT, "f", h.content(b"A"));
    let (folder, folder_at) = (NodeId::new(), some_version());
    let folder_state = NodeState::folder(NodeId::ROOT, "d");
    let records = [
        Placing {
            base: BaseRow::new(file, file_state.clone(), at),
            target: RelPath::new("f")?,
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::Absent,
            held_ms: None,
        },
        Placing {
            base: BaseRow::new(folder, folder_state.clone(), folder_at),
            target: RelPath::new("d")?,
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(folder_at.version),
            held_ms: None,
        },
    ];
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.folder()
            .put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))?;
        records.iter().try_for_each(|r| s.folder().put_placing(r))
    })?;
    let report = h.run(vec![
        Op::LocalDownload {
            node: file,
            parent: NodeId::ROOT,
            name: "f".into(),
            replace: false,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: file_state,
                at,
                base_at: None,
            })),
        },
        Op::LocalMkdir {
            node: folder,
            parent: NodeId::ROOT,
            name: "d".into(),
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: folder_state,
                at: folder_at,
                base_at: Some(folder_at.version),
            })),
        },
    ])?;
    assert_eq!(report.skipped.len(), 2, "{report:?}");
    assert!(!h.folder.root.join("f").exists() && !h.folder.root.join("d").exists());
    let mut left = h.records()?;
    left.sort_by_key(|r| r.base.node);
    let mut want = records.to_vec();
    want.sort_by_key(|r| r.base.node);
    assert_eq!(left, want);
    assert!(h.db.read(|s| Ok(s.is_dirty(file)? && s.is_dirty(folder)?))?);
    Ok(())
}

/// A round leaves a folder made and its record (its run cut short after the folder was made);
/// the scan hasn't seen the folder yet. The next round's recovery reads the disk and settles
/// the folder there as the node: one folder, the node's, no record left, nothing left for the
/// round's folder op to do. Without the recovery, the record held the node off until the next
/// sync.
#[test]
fn a_folder_a_round_left_is_taken_by_the_next() -> R {
    let mut h = Harness::new()?;
    let (node, at) = (NodeId::new(), some_version());
    let state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.folder().put_base(&BaseRow::new(node, state.clone(), at))
    })?;
    let mkdir = || Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: state.clone(),
            at,
            base_at: Some(at.version),
        })),
    };
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    assert!(h.run(vec![mkdir()]).is_err());
    assert_eq!(h.records()?.len(), 1);
    h.cfg.hooks.crash_at = None;
    // The next round.
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    assert!(h.base(node)?.is_some());
    let report = h.run(vec![mkdir()])?;
    assert!(report.failed.is_empty(), "{report:?}");
    let here: Vec<_> = fs::read_dir(&h.folder.root)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with('.'))
        .collect();
    assert_eq!(here, ["d"]);
    let row = h.db.read(|s| s.local_by_node(node))?;
    assert_eq!(row.map(|r| r.path), Some(RelPath::new("d")?));
    Ok(())
}

/// The drive tags a file y (V1) and a plan writes y here; before it commits, something else
/// settles the base at V2 and the drive removes y (V3). The write's compare and set fails, but
/// its tag still agrees with V1, the version it acted on: the next plan reads V3 as the drive's
/// removal of y and takes it off here. Left out of the base, y read as added here and went back
/// up.
#[test]
fn a_tag_the_drive_removed_after_a_write_goes() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let y = Tags::new(vec![Tag::new("y", 0)]);
    h.local_file("f", node, b"A")?;
    h.scanned_with("f", Tags::default())?;
    let c = h.content(b"A");
    let file = NodeState::file(NodeId::ROOT, "f", c);
    let (v0, v1, v2, v3) = (
        some_version(),
        some_version(),
        some_version(),
        some_version(),
    );
    let mirror = |at: RemoteRef, tags: &Tags| {
        let mut content = NodeContent::file("f", 0, 1, false);
        content.set_tags(tags);
        MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(c.hash),
        )
    };
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(c.hash),
            ..BaseRow::new(node, file.clone(), v0)
        })?;
        s.account().put_mirror(&mirror(v1, &y))
    })?;
    let ops = plan_of(&mut h, &[node])?.ops;
    assert!(matches!(ops.as_slice(), [Op::LocalTags { .. }]), "{ops:?}");
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(c.hash),
            ..BaseRow::new(node, file.clone(), v2)
        })?;
        s.account().put_mirror(&mirror(v3, &Tags::default()))
    })?;
    h.run(ops)?;
    assert_eq!(tags::read(&h.folder.root.join("f")), Some(y.clone()));
    h.scanned_with("f", y)?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalTags { tags, .. } if tags.is_empty())),
        "y doesn't go: {:?}",
        plan.ops
    );
    Ok(())
}

/// The drive renames a file (V1) and a plan moves it here; before the move commits, something
/// else settles the base at V2, where the drive holds `drive` and the base `base`, and the item
/// holds `item` by then (the user changed its tags). The move writes no tags: the base keeps
/// its own, whatever V1 held (`acted`). The next plan, as the item stands.
fn plan_after_a_move(base: &Tags, item: &Tags, drive: &Tags, acted: &Tags) -> PlanResult {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(None); // nothing to tag with here
    }
    let node = NodeId::new();
    h.local_file("a", node, b"A")?;
    h.scanned_with("a", item.clone())?;
    let c = h.content(b"A");
    let (v1, v2) = (some_version(), some_version());
    let mut content = NodeContent::file("b", 0, 1, false);
    content.set_tags(drive);
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(c.hash),
            ..BaseRow::new(
                node,
                NodeState {
                    tags: base.clone(),
                    ..NodeState::file(NodeId::ROOT, "a", c)
                },
                v2,
            )
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            v2,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(c.hash),
        ))
    })?;
    let report = h.run(vec![Op::LocalMove {
        node,
        parent: NodeId::ROOT,
        name: "b".into(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                tags: acted.clone(),
                ..NodeState::file(NodeId::ROOT, "b", c)
            },
            at: v1,
            // The plan saw the base before V2.
            base_at: Some(some_version().version),
        })),
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    Ok(Some((node, plan_of(&mut h, &[node])?)))
}

type PlanResult = Result<Option<(NodeId, crate::plan::Plan)>, Box<dyn std::error::Error>>;

/// V2 tagged z, on the drive and here (agreed); the user removes z just as a move the plan
/// made against V1 (no z) commits. z stays removed: the next plan takes it off the drive.
/// (Settled against V1, the move's base dropped z, and the next plan read z as the drive's
/// addition and put it back.)
#[test]
fn a_tag_removed_while_a_move_commits_stays_removed() -> R {
    let (none, z) = (Tags::default(), Tags::new(vec![Tag::new("z", 0)]));
    let Some((node, plan)) = plan_after_a_move(&z, &none, &z, &none)? else {
        return Ok(());
    };
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::RemoteTags { node: n, tags, .. } if *n == node && tags.is_empty())
        ),
        "z comes back: {:?}",
        plan.ops
    );
    Ok(())
}

/// V1 tagged t; V2 removed it, and the base agreed (none). The user adds t again here, and a
/// move the plan made against V1 commits. The user's t goes up. (Settled against V1, which
/// holds t, the base took t for agreed, and the next plan read V2 as the drive removing it.)
#[test]
fn a_tag_added_again_while_a_move_commits_goes_up() -> R {
    let (none, t) = (Tags::default(), Tags::new(vec![Tag::new("t", 0)]));
    let Some((node, plan)) = plan_after_a_move(&none, &t, &none, &t)? else {
        return Ok(());
    };
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::RemoteTags { node: n, tags, .. } if *n == node && *tags == t)
        ),
        "t doesn't go up: {:?}",
        plan.ops
    );
    Ok(())
}

/// One run places a file, then a second download of it (to another name, which an item
/// unknown to the sync holds) fails before putting anything in place: its record goes. The
/// first one's success says nothing of the second's item: a placed mark per node kept the
/// record, and recovery could have taken the unknown item for the node.
#[test]
fn a_later_download_that_fails_leaves_no_record() -> R {
    let mut h = Harness::new()?;
    let (node, at) = h.seed("f", b"A")?;
    fs::write(h.folder.root.join("g"), b"not ours")?;
    let state = NodeState::file(NodeId::ROOT, "f", h.content(b"A"));
    let download = |name: &str, replace: bool| Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: name.into(),
        replace,
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                name: name.into(),
                ..state.clone()
            },
            at,
            base_at: None,
        })),
    };
    let report = h.run(vec![download("f", false), download("g", true)])?;
    assert_eq!(report.done, 1, "{report:?}");
    assert_eq!(report.failed.len(), 1, "{report:?}");
    assert_eq!(h.records()?, []);
    assert_eq!(fs::read(h.folder.root.join("g"))?, b"not ours");
    Ok(())
}

/// A download and a folder made put their item in place, then their commit fails (a commit
/// write, a folder's stat): the operation ends there, not tried again (nothing placed twice,
/// nothing "taken" by its own item), and recovery settles it right away, its folder rescanned:
/// one way to settle what is placed, within the run as main's retry did.
#[test]
fn a_placement_whose_commit_fails_is_settled_by_recovery() -> R {
    let mut h = Harness::new()?;
    let (file, at) = h.seed("f", b"A")?;
    let (folder, folder_at) = (NodeId::new(), some_version());
    let folder_state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        // Synced before: it gets a record.
        s.folder()
            .put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))
    })?;
    let ops = vec![
        Op::LocalDownload {
            node: file,
            parent: NodeId::ROOT,
            name: "f".into(),
            replace: false,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, "f", h.content(b"A")),
                at,
                base_at: None,
            })),
        },
        Op::LocalMkdir {
            node: folder,
            parent: NodeId::ROOT,
            name: "d".into(),
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: folder_state,
                at: folder_at,
                base_at: Some(folder_at.version),
            })),
        },
    ];
    h.cfg.hooks.faults.fail(FailPoint::PlacedRead, 2);
    let report = h.run(ops)?;
    assert_eq!(report.done, 2, "{report:?}");
    assert!(
        report.failed.is_empty() && report.skipped.is_empty(),
        "{report:?}"
    );
    assert_eq!(h.records()?, []);
    let mut here: Vec<_> = fs::read_dir(&h.folder.root)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with('.'))
        .collect();
    here.sort();
    assert_eq!(here, ["d", "f"]);
    for node in [file, folder] {
        assert!(h.base(node)?.is_some());
        assert!(h.db.read(|s| s.local_by_node(node))?.is_some());
    }
    Ok(())
}

/// A folder made again fails its commit (its stat) once in place; the files the same plan
/// downloads into it go in: recovery settled the folder right away. Left for a later round,
/// they failed for want of their folder.
#[test]
fn files_go_into_a_folder_whose_commit_failed() -> R {
    let mut h = Harness::new()?;
    let (folder, folder_at) = (NodeId::new(), some_version());
    let folder_state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.folder()
            .put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))
    })?;
    let mut ops = vec![Op::LocalMkdir {
        node: folder,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: folder_state,
            at: folder_at,
            base_at: Some(folder_at.version),
        })),
    }];
    for name in ["x", "y"] {
        let (node, at) =
            h.remote
                .server()
                .seed_file(folder, name, name.as_bytes(), h.folder.algorithm)?;
        let c = h.content(name.as_bytes());
        let content = NodeContent::file(name, 0, 1, false);
        h.db.write(|s| {
            s.account().put_mirror(&MirrorRow::from_content(
                at,
                node,
                folder,
                NodeKind::File,
                &content,
                Some(c.hash),
            ))
        })?;
        ops.push(Op::LocalDownload {
            node,
            parent: folder,
            name: name.into(),
            replace: false,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(folder, name, c),
                at,
                base_at: None,
            })),
        });
    }
    h.cfg.hooks.faults.fail(FailPoint::PlacedRead, 1);
    let report = h.run(ops)?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(report.done, 3, "{report:?}");
    assert_eq!(fs::read(h.folder.root.join("d/x"))?, b"x");
    assert_eq!(fs::read(h.folder.root.join("d/y"))?, b"y");
    Ok(())
}

/// A download's commit fails once its file is in place, and recovery settles it in the run;
/// meanwhile the user edits a file beside it, after the plan. A later download over that file
/// finds the edit (its guard still has the row the plan saw) and leaves it. Recovery that
/// rescanned the folder refreshed that row, and the download wrote over the edit.
#[test]
fn recovery_in_the_run_leaves_a_sibling_edit_alone() -> R {
    let mut h = Harness::new()?;
    let (a, a_at) = h.seed("a", b"A")?;
    // b, synced at B0; the drive has B1 since.
    let (b, b_at) = h
        .remote
        .server()
        .seed_file(NodeId::ROOT, "b", b"B1", h.folder.algorithm)?;
    h.local_file("b", b, b"B0")?;
    let b0 = h.content(b"B0");
    let before = some_version();
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(b0.hash),
            ..BaseRow::new(b, NodeState::file(NodeId::ROOT, "b", b0), before)
        })
    })?;
    h.mirror(b, "b", b_at, b"B1")?;
    // The user edits b after the plan.
    fs::write(h.folder.root.join("b"), b"user's edit")?;
    let ops = vec![
        Op::LocalDownload {
            node: a,
            parent: NodeId::ROOT,
            name: "a".into(),
            replace: false,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, "a", h.content(b"A")),
                at: a_at,
                base_at: None,
            })),
        },
        // Not a transfer: what comes after runs in the next batch.
        Op::LocalMkdir {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            name: "z".into(),
            tags: Tags::default(),
            drive: None,
        },
        Op::LocalDownload {
            node: b,
            parent: NodeId::ROOT,
            name: "b".into(),
            replace: true,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, "b", h.content(b"B1")),
                at: b_at,
                base_at: Some(before.version),
            })),
        },
    ];
    h.cfg.hooks.faults.fail(FailPoint::PlacedRead, 1);
    h.run(ops)?;
    assert_eq!(fs::read(h.folder.root.join("b"))?, b"user's edit");
    assert!(h.base(a)?.is_some(), "a isn't settled");
    Ok(())
}

/// A folder new here (no record) whose commit fails once it is made: its retry takes over the
/// folder it made, as main does, in the same run.
#[test]
fn a_new_folder_whose_commit_failed_is_taken_by_its_retry() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    h.cfg.hooks.faults.fail(FailPoint::PlacedRead, 1);
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: None,
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    assert!(
        report.failed.is_empty() && report.skipped.is_empty(),
        "{report:?}"
    );
    let row = h.db.read(|s| s.local_by_node(node))?;
    assert_eq!(row.map(|r| r.path), Some(RelPath::new("d")?));
    Ok(())
}

/// What recovery reads at a placement's target: nothing only when verifiably nothing can be
/// there (not found, a file where a folder would be, a loop of links; or what the scan skips,
/// a link), and "can't tell" when its metadata can't be read (a folder denying access): the
/// record stays then. Read as nothing, the record went, and the item became a new one.
#[cfg(unix)]
#[test]
fn recovery_tells_an_unreadable_item_from_none() -> R {
    use super::recover::{OnDisk, look};
    use std::os::unix::fs::PermissionsExt;
    let h = Harness::new()?;
    let root = h.folder.root.clone();
    fs::write(root.join("f"), b"A")?;
    std::os::unix::fs::symlink(root.join("f"), root.join("link"))?;
    std::os::unix::fs::symlink(root.join("l2"), root.join("l1"))?;
    std::os::unix::fs::symlink(root.join("l1"), root.join("l2"))?;
    fs::create_dir(root.join("p"))?;
    fs::write(root.join("p/f"), b"A")?;
    let read = |rel: &str| look(&root, &RelPath::new(rel).expect("a path"));
    assert!(matches!(read("f"), OnDisk::There(_)));
    assert!(matches!(read("gone"), OnDisk::Absent));
    assert!(matches!(read("link"), OnDisk::Absent));
    // A file where a folder would be, a loop of links: nothing can be there.
    assert!(matches!(read("f/x"), OnDisk::Absent));
    assert!(matches!(read("l1/x"), OnDisk::Absent));
    fs::set_permissions(root.join("p"), fs::Permissions::from_mode(0o000))?;
    let (denied, readable) = (read("p/f"), fs::metadata(root.join("p/f")).is_ok());
    fs::set_permissions(root.join("p"), fs::Permissions::from_mode(0o755))?;
    if !readable {
        assert!(matches!(denied, OnDisk::Unreadable));
    } // else root, who reads it anyway
    Ok(())
}

/// Recovery in the run never writes the item's row over another node's row at its target:
/// the record stays, for the recovery between rounds to judge after its rescan. Overwritten,
/// that node lost its place here.
#[test]
fn recovery_in_the_run_leaves_another_nodes_row() -> R {
    use super::recover::{Applied, Judged, apply};
    let mut h = Harness::new()?;
    let (node, other) = (NodeId::new(), NodeId::new());
    h.local_file("f", other, b"other")?;
    let record = Placing {
        base: BaseRow::new(
            node,
            NodeState::file(NodeId::ROOT, "f", h.content(b"A")),
            some_version(),
        ),
        target: RelPath::new("f")?,
        placed_tags: None,
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        held_ms: None,
    };
    h.db.write(|s| s.folder().put_placing(&record))?;
    let row = LocalRow {
        path: RelPath::new("f")?,
        node,
        kind: NodeKind::File,
        fingerprint: hbsync_platform::stat(&h.folder.root.join("f"))?.fingerprint(),
        executable: Some(false),
        hash: Some(h.content(b"A").hash),
        tags: None,
    };
    let folder = &h.folder;
    let applied =
        h.db.write(|s| apply(s.folder(), &record, Judged::Settle(row), folder, 0))?;
    assert!(matches!(applied, Applied::Kept));
    assert_eq!(h.records()?, [record]);
    let path = RelPath::new("f")?;
    assert_eq!(
        h.db.read(|s| s.get_local(&path))?.map(|r| r.node),
        Some(other)
    );
    Ok(())
}

/// Recovery between rounds reads the disk as recovery in the run does: a target in a folder
/// that denies access may hold the item, so its record stays (the scan has no row there
/// either). Taken for nothing there, the record went, and the item became a new one.
#[cfg(unix)]
#[test]
fn a_record_in_an_unreadable_folder_stays() -> R {
    use std::os::unix::fs::PermissionsExt;
    let mut h = Harness::new()?;
    let node = NodeId::new();
    fs::create_dir(h.folder.root.join("p"))?;
    fs::write(h.folder.root.join("p/f"), b"A")?;
    let record = Placing {
        base: BaseRow::new(
            node,
            NodeState::file(NodeId::ROOT, "f", h.content(b"A")),
            some_version(),
        ),
        target: RelPath::new("p/f")?,
        placed_tags: None,
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        held_ms: None,
    };
    h.db.write(|s| s.folder().put_placing(&record))?;
    let p = h.folder.root.join("p");
    fs::set_permissions(&p, fs::Permissions::from_mode(0o000))?;
    let readable = fs::metadata(p.join("f")).is_ok();
    let recovered = recover_placements(&mut h.db, &h.folder);
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755))?;
    recovered?;
    if readable {
        return Ok(()); // root reads it anyway
    }
    assert_eq!(h.records()?.len(), 1);
    Ok(())
}

/// A record whose file stays unhashed holds its node only so long ([`UNHASHED_HOLD`]), from
/// when recovery first finds it so: however long ago the record was written (a long download,
/// then a crash), the first finding holds it; past the hold, recovery drops it as main does
/// (the file is then the scan's new item), rather than hold the node off for good.
#[test]
fn a_record_whose_file_stays_unhashed_goes_in_time() -> R {
    let mut h = Harness::new()?;
    record_of_a_crashed_download(&mut h, &Tags::default())?;
    let node = h.records()?[0].base.node;
    assert_eq!(h.records()?[0].held_ms, None);
    h.local_file("f", NodeId::new(), b"A")?;
    let path = RelPath::new("f")?;
    h.db.write(|s| {
        let row = s.get_local(&path)?.expect("a row");
        s.folder().put_local(&LocalRow { hash: None, ..row })
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    let held = h.records()?.remove(0);
    let since = held.held_ms.expect("held from the first finding") - 5_000;
    h.db.write(|s| {
        s.folder().put_placing(&Placing {
            held_ms: Some(since),
            ..held.clone()
        })
    })?;
    // A later finding doesn't hold it again.
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?[0].held_ms, Some(since));
    let old = hbsync_types::unix_now_ms() - UNHASHED_HOLD.as_millis() as i64 - 1;
    h.db.write(|s| {
        s.folder().put_placing(&Placing {
            held_ms: Some(old),
            ..held
        })
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    assert!(h.base(node)?.is_none());
    Ok(())
}

/// A download put in place, its commit lost; the scan finds the file but can't hash it yet
/// (locked): recovery keeps the record for a later one, which settles it once hashed. Taken
/// for not there, the record went, and the file was a new item later.
#[test]
fn a_record_whose_file_cant_be_hashed_yet_stays() -> R {
    let mut h = Harness::new()?;
    record_of_a_crashed_download(&mut h, &Tags::default())?;
    let node = h.records()?[0].base.node;
    h.local_file("f", NodeId::new(), b"A")?;
    let path = RelPath::new("f")?;
    h.db.write(|s| {
        let row = s.get_local(&path)?.expect("a row");
        s.folder().put_local(&LocalRow { hash: None, ..row })
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?.len(), 1);
    assert!(h.base(node)?.is_none());
    // Hashed now.
    let hash = h.content(b"A").hash;
    h.db.write(|s| {
        let row = s.get_local(&path)?.expect("a row");
        s.folder().put_local(&LocalRow {
            hash: Some(hash),
            ..row
        })
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    assert!(h.base(node)?.is_some());
    Ok(())
}

/// A folder made again takes over one there unknown to the sync, tagged t, and its tag write
/// fails (the folder read-only): it claims nothing written (not known), so the base's tags
/// start afresh, t agreed with the drive. Claimed written as none, t read as removed by the
/// write against a drive that holds it, and the base kept its own (none).
#[cfg(unix)]
#[test]
fn a_folder_taken_over_whose_tag_write_fails_claims_nothing() -> R {
    use std::os::unix::fs::PermissionsExt;
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, at) = (NodeId::new(), some_version());
    let t = Tags::new(vec![Tag::new("t", 0)]);
    let d = h.folder.root.join("d");
    fs::create_dir(&d)?;
    tags::write(&d, &t)?;
    fs::set_permissions(&d, fs::Permissions::from_mode(0o555))?;
    let refused = tags::write(&d, &Tags::default()).is_err();
    if !refused {
        fs::set_permissions(&d, fs::Permissions::from_mode(0o755))?;
        return Ok(()); // a read-only folder still takes tags here (root)
    }
    let mut content = NodeContent::folder("d");
    content.set_tags(&t);
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.folder().put_base(&BaseRow::new(
            node,
            NodeState::folder(NodeId::ROOT, "d"),
            at,
        ))
    })?;
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                tags: t.clone(),
                ..NodeState::folder(NodeId::ROOT, "d")
            },
            at,
            base_at: Some(at.version),
        })),
    }]);
    fs::set_permissions(&d, fs::Permissions::from_mode(0o755))?;
    assert_eq!(report?.done, 1);
    assert_eq!(h.base(node)?.map(|b| b.state.tags), Some(t));
    Ok(())
}

/// A folder made again finds one there unknown to the sync, tagged t, and takes it: what it
/// held before the write is t, not nothing. Its write leaves t (the drive holds t too): t didn't
/// change, so the base keeps its own (none) rather than take t for the write's; the next plan
/// finds both sides holding it.
#[test]
fn a_folder_taken_over_holds_its_tags_before_the_write() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, at) = (NodeId::new(), some_version());
    let t = Tags::new(vec![Tag::new("t", 0)]);
    let state = NodeState {
        tags: t.clone(),
        ..NodeState::folder(NodeId::ROOT, "d")
    };
    let mut content = NodeContent::folder("d");
    content.set_tags(&t);
    fs::create_dir(h.folder.root.join("d"))?;
    tags::write(&h.folder.root.join("d"), &t)?;
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.folder().put_base(&BaseRow::new(
            node,
            NodeState::folder(NodeId::ROOT, "d"),
            at,
        ))
    })?;
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: t.clone(),
        drive: Some(Box::new(DriveVersion {
            state,
            at,
            base_at: Some(at.version),
        })),
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    assert_eq!(h.base(node)?.map(|b| b.state.tags), Some(Tags::default()));
    Ok(())
}

/// A download or a folder made, tagged work on the drive, whose tag write fails: the item
/// holds none. Its row and the base hold what it holds, so the next plan writes work again,
/// and never takes it off the drive. Read as not known, the item looked as if holding the
/// drive's tags, the base took work, and the next scan's none read as the user removing it.
/// `unread`: what it holds can't be read back either: made here, it holds none all the same,
/// and its row says so (not known, it read as the drive's, the base took work, through a later
/// write in the run on the node too: a file's tags sent up).
fn a_failed_tag_write_removes_nothing(folder: bool, unread: bool) -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut content = match folder {
        true => NodeContent::folder("x"),
        false => NodeContent::file("x", 0, 1, false),
    };
    content.set_tags(&work);
    let (node, at) = match folder {
        true => (NodeId::new(), some_version()),
        false => {
            // On the drive, tagged.
            let (node, seeded) = h.seed("x", b"A")?;
            let meta = NodeMeta {
                node,
                parent: NodeId::ROOT,
                kind: NodeKind::File,
                content: content.clone(),
                removed: false,
            };
            let at = h
                .remote
                .server()
                .update(seeded.file_id, seeded.version, &meta, None)?;
            (node, at)
        }
    };
    let state = NodeState {
        tags: work.clone(),
        ..match folder {
            true => NodeState::folder(NodeId::ROOT, "x"),
            false => NodeState::file(NodeId::ROOT, "x", h.content(b"A")),
        }
    };
    let kind = if folder {
        NodeKind::Folder
    } else {
        NodeKind::File
    };
    let hash = (!folder).then(|| h.content(b"A").hash);
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            kind,
            &content,
            hash,
        ))?;
        // A folder synced before, deleted here: made again, with a record.
        if folder {
            s.folder()
                .put_base(&BaseRow::new(node, state.clone(), at))?;
        }
        Ok(())
    })?;
    let base_at = folder.then_some(at.version);
    let drive = Some(Box::new(DriveVersion { state, at, base_at }));
    let op = match folder {
        true => Op::LocalMkdir {
            node,
            parent: NodeId::ROOT,
            name: "x".into(),
            tags: work.clone(),
            drive,
        },
        false => Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: "x".into(),
            replace: false,
            tags: work.clone(),
            drive,
        },
    };
    let mut ops = vec![op];
    if unread && !folder {
        ops.push(Op::RemoteTags {
            node,
            file_id: at.file_id,
            expected: at.version,
            tags: work.clone(),
        });
    }
    let count = ops.len();
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 1);
    if unread {
        h.cfg.hooks.faults.fail(FailPoint::TagRead, 1);
    }
    let report = h.run(ops)?;
    assert_eq!(report.done, count, "{report:?}");
    let row = h.db.read(|s| s.local_by_node(node))?.ok_or("no row")?;
    assert_eq!(row.tags, Some(Tags::default()), "{row:?}");
    // The next scan reads what the item holds.
    let path = h.folder.root.join("x");
    assert_eq!(tags::read(&path), Some(Tags::default()));
    h.scanned_with("x", Tags::default())?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan.ops.iter().any(|op| match op {
            Op::RemoteTags { tags, .. } | Op::RemoteUpdate { tags, .. } =>
                tags.get("Work").is_none(),
            _ => false,
        }),
        "folder {folder}: work taken off the drive: {:?}",
        plan.ops
    );
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::LocalTags { node: n, tags, .. } if *n == node && *tags == work)
        ),
        "folder {folder}: work isn't written again: {:?}",
        plan.ops
    );
    Ok(())
}

#[test]
fn a_failed_tag_write_on_a_download_removes_nothing() -> R {
    a_failed_tag_write_removes_nothing(false, false)
}

#[test]
fn a_failed_tag_write_on_a_folder_made_removes_nothing() -> R {
    a_failed_tag_write_removes_nothing(true, false)
}

#[test]
fn a_failed_tag_write_left_unread_on_a_download_removes_nothing() -> R {
    a_failed_tag_write_removes_nothing(false, true)
}

#[test]
fn a_failed_tag_write_left_unread_on_a_folder_made_removes_nothing() -> R {
    a_failed_tag_write_removes_nothing(true, true)
}

/// A file at `p1/f` this device synced, moved to `p2` and edited on the drive: the download
/// that brings it there moves the old version aside. Returns the node and the download.
fn a_download_from_another_folder(
    h: &mut Harness,
) -> Result<(NodeId, Op), Box<dyn std::error::Error>> {
    let (p1, p2) = (NodeId::new(), NodeId::new());
    h.local_folder("p1", p1)?;
    h.local_folder("p2", p2)?;
    let (node, at) = h
        .remote
        .server()
        .seed_file(p2, "f", b"B", h.folder.algorithm)?;
    h.local_file("p1/f", node, b"A")?;
    let (a, b) = (h.content(b"A"), h.content(b"B"));
    let before = some_version();
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(a.hash),
            ..BaseRow::new(node, NodeState::file(p1, "f", a), before)
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            p2,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(b.hash),
        ))
    })?;
    let op = Op::LocalDownload {
        node,
        parent: p2,
        name: "f".into(),
        replace: true,
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState::file(p2, "f", b),
            at,
            base_at: Some(before.version),
        })),
    };
    Ok((node, op))
}

/// What is aside: its entries' names.
fn aside_entries(h: &Harness) -> Vec<String> {
    hbsync_local::layout::aside_entries(&h.folder.root)
        .map(|(p, _)| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect::<Option<_>>()
        .unwrap_or_default()
}

/// The roots the old-version tests run under: a plain one and, where it differs (Windows:
/// `\\?\`), the canonical one.
fn canonical_harnesses() -> Result<Vec<Harness>, Box<dyn std::error::Error>> {
    let plain = Harness::new()?;
    let mut canonical = Harness::new()?;
    canonical.folder.root = fs::canonicalize(&canonical.folder.root)?;
    Ok(vec![plain, canonical])
}

/// The download moves the old version aside, puts the new one in place and, once committed,
/// unlinks the old one: nothing is kept.
#[test]
fn an_old_version_goes_once_the_new_one_is_in() -> R {
    for mut h in canonical_harnesses()? {
        let (node, op) = a_download_from_another_folder(&mut h)?;
        h.run(vec![op])?;
        assert_eq!(fs::read(h.at("p2/f")?)?, b"B");
        assert!(!h.at("p1/f")?.exists());
        assert_eq!(aside_entries(&h), Vec::<String>::new());
        assert!(!hbsync_local::layout::trash_dir(&h.folder.root).exists());
        let row = h.db.read(|s| s.local_by_node(node))?;
        assert_eq!(row.map(|r| r.path.to_string()), Some("p2/f".into()));
    }
    Ok(())
}

/// The download moves the old version aside, then can't put the new one in place: the old
/// version comes back at once. Gone, the next plan took its absence for the user deleting it
/// (against the drive's edit: a conflict, and the download again).
#[test]
fn an_old_version_comes_back_when_the_new_one_fails() -> R {
    for mut h in canonical_harnesses()? {
        let (node, op) = a_download_from_another_folder(&mut h)?;
        h.cfg.hooks.faults.fail(FailPoint::PlaceNew, 1);
        h.run(vec![op])?;
        assert_eq!(fs::read(h.at("p1/f")?)?, b"A");
        assert_eq!(aside_entries(&h), Vec::<String>::new());
        assert_eq!(h.records()?, []);
        let row = h.db.read(|s| s.local_by_node(node))?;
        assert_eq!(row.map(|r| r.path.to_string()), Some("p1/f".into()));
        let plan = plan_of(&mut h, &[node])?;
        assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    }
    Ok(())
}

/// The same, putting it back failing too: it stays aside, for the next sync's recovery. That
/// finds it the synced version (what the drive's newer one replaced) and finishes the delete,
/// its base with it; the next plan brings the new version.
#[test]
fn an_old_version_that_cant_come_back_stays_aside() -> R {
    let mut h = Harness::new()?;
    let (node, op) = a_download_from_another_folder(&mut h)?;
    h.cfg.hooks.faults.fail(FailPoint::PlaceNew, 1);
    h.cfg.hooks.faults.fail(FailPoint::PutBack, 1);
    h.run(vec![op])?;
    assert!(!h.at("p1/f")?.exists());
    assert_eq!(aside_entries(&h).len(), 1);
    assert_eq!(h.records()?, []);
    crate::exec::recover_aside(&mut h.db, &h.folder)?;
    assert_eq!(aside_entries(&h), Vec::<String>::new());
    assert!(h.db.read(|s| s.get_base(node))?.is_none(), "its base went");
    let plan = plan_of(&mut h, &[node])?;
    h.run(plan.ops)?;
    assert_eq!(fs::read(h.at("p2/f")?)?, b"B");
    Ok(())
}

/// A crash after the new version is in place leaves the old one aside: the next sync's
/// recovery finds its node holding other bytes now (the new version committed), and unlinks it
/// once its bytes read as the old version's; one that doesn't (written to aside) goes back beside
/// the new one, a new item there.
#[test]
fn an_old_version_left_aside_by_a_crash_goes() -> R {
    for written in [false, true] {
        let mut h = Harness::new()?;
        let (node, op) = a_download_from_another_folder(&mut h)?;
        let aside = hbsync_local::layout::aside_dir(&h.folder.root);
        fs::create_dir_all(&aside)?;
        // As a crash after the commit left it: the old version aside, the new one in place.
        let a = h.content(b"A").hash;
        h.run(vec![op])?;
        let entry = aside.join(hbsync_local::layout::AsideEntry::File(node, a).name());
        fs::write(
            &entry,
            if written {
                &b"A, written aside"[..]
            } else {
                b"A"
            },
        )?;
        crate::exec::recover_aside(&mut h.db, &h.folder)?;
        assert!(!entry.exists());
        assert_eq!(h.at("p2/f (conflict 1)")?.exists(), written, "{written}");
        assert_eq!(fs::read(h.at("p2/f")?)?, b"B");
    }
    Ok(())
}

/// The download from another folder unlinks the old version; the drive then deletes the node,
/// and the file is deleted outright (sync semantics §7): aside, checked, unlinked, nothing kept.
/// (A node trashed twice within a second once collided on its entry, seed
/// 8474444713664359667: deletes no longer make one.)
#[test]
fn a_node_replaced_then_deleted_goes() -> R {
    let mut h = Harness::new()?;
    let (node, op) = a_download_from_another_folder(&mut h)?;
    h.run(vec![op])?;
    let report = h.run(vec![Op::LocalTrash {
        node,
        subtree: vec![node],
    }])?;
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert!(!h.at("p2/f")?.exists());
    assert_eq!(
        aside_entries(&h),
        Vec::<String>::new(),
        "nothing left aside"
    );
    assert!(h.db.read(|s| s.get_base(node))?.is_none(), "its base went");
    Ok(())
}

/// A download replacing a synced file, the run settling the node's base at the drive's version
/// first (as its own write of a tag would), then crashing once the file is in place; meanwhile
/// the drive deleted the node. Recovery finds the fetched file there and settles it as the
/// node with the record's base, so the drive's delete reaches here: the next plan deletes
/// it. Settled against the base the plan saw, which had moved, the base kept the old
/// bytes, the fetched file read as an edit, and the edit won over the delete: the node was
/// uploaded again.
#[test]
fn a_record_doesnt_bring_back_a_node_the_drive_deleted() -> R {
    // A name the drive holds decomposed, which this device would compose: its spelling is
    // recorded, or the item reads as renamed (an edit, winning over the delete).
    for (removed, local) in [(false, "f"), (true, "f"), (false, "e\u{301}")] {
        let remote = local.to_owned();
        let spelled = hbsync_namemap::to_remote(local, Os::Linux)? != remote;
        let mut h = Harness::new()?;
        let (node, at) = h.seed(&remote, b"B")?;
        let content = NodeContent::file(&remote, 0, 1, false);
        h.local_file(local, node, b"A")?;
        let (old, new) = (h.content(b"A"), h.content(b"B"));
        h.db.write(|s| {
            s.folder().put_base(&BaseRow {
                local_hash: Some(old.hash),
                local_name: spelled.then(|| local.to_owned()),
                ..BaseRow::new(
                    node,
                    NodeState::file(NodeId::ROOT, &remote, old),
                    some_version(),
                )
            })?;
            s.account().put_mirror(&MirrorRow::from_content(
                at,
                node,
                NodeId::ROOT,
                NodeKind::File,
                &content,
                Some(new.hash),
            ))
        })?;
        let planned = h.base(node)?.map(|b| b.remote.version);
        let download = Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: remote.clone(),
            replace: true,
            tags: Tags::default(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, &remote, new),
                at,
                base_at: planned,
            })),
        };
        h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
        assert!(h.run(vec![Op::Settle { node }, download]).is_err());
        // The drive deletes it: gone, or in Recently deleted.
        h.db.write(|s| {
            let row = s.account().delete_mirror(at.file_id)?.expect("its row");
            match removed {
                true => s.account().put_removed(&RemovedRow {
                    row,
                    removed_at: Some(1),
                }),
                false => Ok(()),
            }
        })?;
        h.local_file(local, node, b"B")?;
        recover_placements(&mut h.db, &h.folder)?;
        assert_eq!(h.records()?, []);
        let plan = plan_of(&mut h, &[node])?;
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, Op::RemoteCreate { .. } | Op::RemoteUpdate { .. })),
            "removed {removed}, {local:?}: the node comes back on the drive: {:?}",
            plan.ops
        );
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, Op::LocalTrash { node: n, .. } if *n == node)),
            "removed {removed}, {local:?}: {:?}",
            plan.ops
        );
    }
    Ok(())
}

/// The same, the node taken along by its folder going to Recently deleted: recovery reads it
/// as gone, as the plan does.
#[test]
fn a_record_doesnt_bring_back_a_node_taken_along() -> R {
    let mut h = Harness::new()?;
    let folder = NodeId::new();
    h.local_folder("d", folder)?;
    let (node, at) = h
        .remote
        .server()
        .seed_file(folder, "f", b"B", h.folder.algorithm)?;
    h.local_file("d/f", node, b"A")?;
    let (old, new) = (h.content(b"A"), h.content(b"B"));
    let folder_at = some_version();
    h.db.write(|s| {
        s.folder().put_base(&BaseRow::new(
            folder,
            NodeState::folder(NodeId::ROOT, "d"),
            folder_at,
        ))?;
        s.folder().put_base(&BaseRow {
            local_hash: Some(old.hash),
            ..BaseRow::new(node, NodeState::file(folder, "f", old), some_version())
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            folder,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(new.hash),
        ))
    })?;
    let planned = h.base(node)?.map(|b| b.remote.version);
    let download = Op::LocalDownload {
        node,
        parent: folder,
        name: "f".into(),
        replace: true,
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState::file(folder, "f", new),
            at,
            base_at: planned,
        })),
    };
    h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    assert!(h.run(vec![Op::Settle { node }, download]).is_err());
    // The folder goes to Recently deleted, after the file's version; the file is still live.
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: MirrorRow::from_content(
                folder_at,
                folder,
                NodeId::ROOT,
                NodeKind::Folder,
                &NodeContent::folder("d"),
                None,
            ),
            removed_at: Some(i64::MAX / 2),
        })
    })?;
    h.local_file("d/f", node, b"B")?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    let plan = plan_of(&mut h, &[folder, node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { .. } | Op::RemoteUpdate { .. })),
        "the node comes back on the drive: {:?}",
        plan.ops
    );
    Ok(())
}

/// Tags written but too large to sync read back as nothing to go by: that says nothing of the
/// volume, which still keeps tags. Only an unsupported volume marks the root as keeping none.
#[test]
fn tags_too_large_dont_mark_the_volume_tagless() -> R {
    let mut h = Harness::new()?;
    let large = Tags::new(vec![Tag::new("x".repeat(3000), 0)]);
    let probe = h.folder.root.join(".large");
    fs::write(&probe, b"")?;
    if !tags_kept(&h.folder.root) || tags::write(&probe, &large)?.is_some() {
        return Ok(()); // no tags here, or this file system won't take it
    }
    let (node, at) = h.seed("f", b"A")?;
    h.run(vec![Op::LocalDownload {
        node,
        parent: NodeId::ROOT,
        name: "f".into(),
        replace: false,
        tags: large.clone(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                tags: large,
                ..NodeState::file(NodeId::ROOT, "f", h.content(b"A"))
            },
            at,
            base_at: None,
        })),
    }])?;
    assert!(h.folder.root.join("f").exists());
    assert_ne!(h.folder.tags_kept.get(), Some(&false));
    Ok(())
}

/// Recovery checks the base against the version the record's plan saw, as the commit does: the
/// crashed run settled the base since (another write on the node), so recovery leaves it alone
/// and the node dirty, instead of taking it back to the record's version.
#[test]
fn recovery_checks_the_base_the_record_saw() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let (planned, since, at) = (some_version(), some_version(), some_version());
    let file = NodeState::file(NodeId::ROOT, "f", h.content(b"A"));
    let base = BaseRow {
        local_hash: Some(h.content(b"A").hash),
        ..BaseRow::new(node, file.clone(), since)
    };
    h.db.write(|s| {
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(h_content_hash(b"A")),
        ))?;
        s.folder().put_base(&base)?;
        s.folder().put_placing(&Placing {
            base: BaseRow::new(node, file, at),
            target: RelPath::new("f").expect("a path"),
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(planned.version),
            held_ms: None,
        })?;
        s.folder().clear_dirty(node)
    })?;
    h.local_file("f", NodeId::new(), b"A")?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.base(node)?, Some(base));
    assert!(h.db.read(|s| s.is_dirty(node))?);
    Ok(())
}

/// A file edited on the drive and tagged red here: the plan writes red to the drive and
/// downloads the edit. The download acts on the version the tag write left (the bytes it
/// fetches are the same, the base is there), so the run leaves nothing for another: the next
/// plan has nothing to do, after a crash and recovery too. Acting only on the version the plan
/// saw, the download went stale behind the run's own write, and the next plan downloaded it
/// again. Returns the base it leaves.
fn a_download_after_the_runs_own_tag_write(
    crash: bool,
) -> Result<Option<BaseRow>, Box<dyn std::error::Error>> {
    let mut h = Harness::new()?;
    let (node, _) = h.seed("f", b"B")?;
    let red = Tags::new(vec![Tag::new("Red", 0)]);
    h.local_file("f", node, b"A")?;
    tags::write_on(&h.folder.root.join("f"), &red)?;
    // Read again as the scan would, tags and all.
    h.local_file("f", node, b"A")?;
    h.scanned_with("f", red)?;
    let old = h.content(b"A");
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(old.hash),
            ..BaseRow::new(
                node,
                NodeState::file(NodeId::ROOT, "f", old),
                some_version(),
            )
        })
    })?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        matches!(
            plan.ops[..],
            [Op::RemoteTags { .. }, Op::LocalDownload { .. }]
        ),
        "{:?}",
        plan.ops
    );
    if crash {
        h.cfg.hooks.crash_at = Some(CrashPoint::AfterPlaced);
    }
    let ran = h.run(plan.ops);
    if crash {
        assert!(ran.is_err(), "the crash point wasn't reached");
        let read = tags::read(&h.folder.root.join("f"));
        h.local_file("f", node, b"B")?;
        h.scanned_with("f", read.unwrap_or_default())?;
        recover_placements(&mut h.db, &h.folder)?;
        assert_eq!(h.records()?, []);
    } else {
        let report = ran?;
        assert_eq!(report.done, 2, "{report:?}");
    }
    assert_eq!(fs::read(h.folder.root.join("f"))?, b"B");
    let again = plan_of(&mut h, &[node])?;
    assert!(again.ops.is_empty(), "crash {crash}: {:?}", again.ops);
    Ok(h.base(node)?.map(|b| BaseRow {
        node: NodeId::ROOT,
        ..b
    }))
}

#[test]
fn a_download_follows_the_runs_own_tag_write() -> R {
    if !tags_kept(&Harness::new()?.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let committed = a_download_after_the_runs_own_tag_write(false)?;
    assert!(committed.is_some());
    assert_eq!(committed, a_download_after_the_runs_own_tag_write(true)?);
    Ok(())
}

/// A folder in Recently deleted, tagged work, made here as its restore begins (its RemoteCreate
/// to follow); another device restores it first, work removed. The folder made here has the
/// removed version as its base, so the next plan takes work off it. With no base, work read
/// as added here, and the plan wrote it back to the drive.
#[test]
fn a_restored_folder_made_here_keeps_a_removal_from_the_drive() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut tagged = NodeContent::folder("a");
    tagged.set_tags(&work);
    let at = some_version();
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::Folder, &tagged, None),
            removed_at: Some(1),
        })
    })?;
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a".into(),
        tags: work.clone(),
        drive: None,
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    // Restored elsewhere first, without work.
    h.db.write(|s| {
        s.account().delete_removed(node)?;
        s.account().put_mirror(&MirrorRow::from_content(
            RemoteRef {
                file_id: at.file_id,
                version: some_version().version,
            },
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("a"),
            None,
        ))
    })?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteTags { .. } | Op::RemoteUpdate { .. })),
        "work comes back: {:?}",
        plan.ops
    );
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::LocalTags { node: n, tags, .. } if *n == node && tags.is_empty())
        ),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// A folder in Recently deleted, tagged work, to be made here as its restore begins, under
/// another name (a file of this device's holds "a"). Another device restores it first, removes
/// work, and maybe deletes it again (`again`): the version there no longer holds work, so the
/// folder waits for the next plan, which makes it from the drive as it is now. Made with work
/// and settled against the drive's version without it, work read as added here, and went back
/// up over the removal. Still there as planned (`as_planned`), it is made.
fn a_folder_to_make_from_recently_deleted(again: bool, as_planned: bool) -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut tagged = NodeContent::folder("a");
    tagged.set_tags(&work);
    let content = if as_planned {
        tagged
    } else {
        NodeContent::folder("a")
    };
    let row = MirrorRow::from_content(
        some_version(),
        node,
        NodeId::ROOT,
        NodeKind::Folder,
        &content,
        None,
    );
    h.db.write(|s| match again || as_planned {
        true => s.account().put_removed(&RemovedRow {
            row,
            removed_at: Some(1),
        }),
        false => s.account().put_mirror(&row),
    })?;
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a (conflict 1)".into(),
        tags: work,
        drive: None,
    }])?;
    let made = h.folder.root.join("a (conflict 1)").exists();
    assert_eq!(made, as_planned, "{report:?}");
    assert_eq!(report.skipped.len(), usize::from(!as_planned), "{report:?}");
    assert_eq!(h.base(node)?.is_some(), as_planned);
    Ok(())
}

#[test]
fn a_folder_to_make_from_recently_deleted_restored_since_waits() -> R {
    a_folder_to_make_from_recently_deleted(false, false)
}

#[test]
fn a_folder_to_make_from_recently_deleted_changed_there_since_waits() -> R {
    a_folder_to_make_from_recently_deleted(true, false)
}

#[test]
fn a_folder_to_make_from_recently_deleted_as_planned_is_made() -> R {
    a_folder_to_make_from_recently_deleted(true, true)
}

/// A folder in Recently deleted, tagged Work, made here as its restore begins: the check finds
/// Work there and the folder is made with it. Before its commit, another device's restore
/// without Work is pulled (another thread). The commit settles against the version the check
/// accepted (its base holds Work), so the next plan reads Work as removed on the drive and
/// removes it here. Settled against the restored version, Work read as this device adding it,
/// and went back up over the removal (review 15 K1).
#[test]
fn a_folder_made_from_recently_deleted_settles_against_the_version_it_checked() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut tagged = NodeContent::folder("a");
    tagged.set_tags(&work);
    let removed_at = some_version();
    let row = |at, content: &NodeContent| {
        MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::Folder, content, None)
    };
    let in_rd = row(removed_at, &tagged);
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: in_rd.clone(),
            removed_at: Some(1),
        })
    })?;
    let restored = row(
        RemoteRef {
            version: some_version().version,
            ..removed_at
        },
        &NodeContent::folder("a"),
    );
    h.cfg.hooks.before_commit = Some(Meanwhile(std::sync::Arc::new(move |db| {
        db.write(|s| {
            s.account().delete_removed(node)?;
            s.account().put_mirror(&restored)
        })
        .expect("the pull meanwhile");
    })));
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a".into(),
        tags: work,
        drive: None,
    }])?;
    assert!(h.folder.root.join("a").is_dir(), "{report:?}");
    let base = h.base(node)?.ok_or("no base")?;
    assert_eq!(
        base.remote, removed_at,
        "settled against what it was made of"
    );
    assert!(base.state.tags.get("Work").is_some(), "{base:?}");
    // The next plan removes Work here, as removed on the drive; it never sends it up.
    let plan = plan_of(&mut h, &[node])?;
    let tags_of = |op: &Op| match op {
        Op::LocalTags { node: n, tags, .. } if *n == node => Some(tags.clone()),
        Op::RemoteTags { node: n, .. } if *n == node => panic!("sent up: {op:?}"),
        _ => None,
    };
    let local: Vec<Tags> = plan.ops.iter().filter_map(tags_of).collect();
    assert_eq!(local, [Tags::default()], "{plan:?}");
    Ok(())
}

/// A folder synced here tagged Work, deleted here and on the drive (its last version there
/// tagged `last`), made again here as it comes back for a child another device wrote into it
/// (its RemoteCreate to follow), whose tag write fails: it holds none. Returns its base after,
/// and the plan once the drive has it back as `restored`.
fn a_folder_made_back_whose_tag_write_fails(
    last: &Tags,
    restored: &Tags,
) -> Result<Option<(Tags, crate::plan::Plan)>, Box<dyn std::error::Error>> {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(None); // nothing to tag with here
    }
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let content = |tags: &Tags| {
        let mut c = NodeContent::folder("a");
        c.set_tags(tags);
        c
    };
    let at = some_version();
    h.db.write(|s| {
        s.folder().put_base(&BaseRow::new(
            node,
            NodeState {
                tags: work.clone(),
                ..NodeState::folder(NodeId::ROOT, "a")
            },
            at,
        ))?;
        s.account().put_removed(&RemovedRow {
            row: MirrorRow::from_content(
                at,
                node,
                NodeId::ROOT,
                NodeKind::Folder,
                &content(last),
                None,
            ),
            removed_at: Some(1),
        })
    })?;
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 1);
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a".into(),
        tags: work,
        drive: None,
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    let row = h.db.read(|s| s.local_by_node(node))?.expect("its row");
    assert_eq!(row.tags, Some(Tags::default()));
    let base = h.base(node)?.expect("its base").state.tags;
    h.db.write(|s| {
        s.account().delete_removed(node)?;
        s.account().put_mirror(&MirrorRow::from_content(
            RemoteRef {
                file_id: at.file_id,
                version: some_version().version,
            },
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content(restored),
            None,
        ))
    })?;
    Ok(Some((base, plan_of(&mut h, &[node])?)))
}

/// Its last version on the drive tagged Work: the base can't claim Work, which the folder never
/// got. Once the folder is back with Work, the next plan writes Work here again. With the base
/// claiming it, the folder without it read as the user removing it, and the plan removed it on
/// the drive.
#[test]
fn a_folder_made_back_whose_tag_write_fails_keeps_its_tags_on_the_drive() -> R {
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let Some((base, plan)) = a_folder_made_back_whose_tag_write_fails(&work, &work)? else {
        return Ok(());
    };
    assert_eq!(base, Tags::default());
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteTags { .. } | Op::RemoteUpdate { .. })),
        "Work is removed on the drive: {:?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalTags { tags, .. } if *tags == work)),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// Its last version on the drive without Work (another device removed it, then deleted the
/// folder): the base keeps Work, so the restore that follows merges the drive's removal in
/// rather than reading Work, the plan's, as added here.
#[test]
fn a_folder_made_back_whose_tag_write_fails_keeps_a_removal_from_the_drive() -> R {
    let none = Tags::default();
    let Some((base, plan)) = a_folder_made_back_whose_tag_write_fails(&none, &none)? else {
        return Ok(());
    };
    assert_eq!(base, Tags::new(vec![Tag::new("Work", 0)]));
    assert!(
        !plan.ops.iter().any(|op| match op {
            Op::RemoteTags { tags, .. }
            | Op::RemoteUpdate { tags, .. }
            | Op::LocalTags { tags, .. } => tags.get("Work").is_some(),
            _ => false,
        }),
        "Work comes back: {:?}",
        plan.ops
    );
    Ok(())
}

/// A folder synced before made again here as the drive had it when the plan was made (`a`),
/// its base moved since (`b`), and the drive deleted it meanwhile: the commit settles it at
/// the version it acted on, as recovery does, so the delete reaches it and the next plan
/// deletes it. Against the base that moved, it read as renamed, an edit winning over the
/// delete, and the plan made the node again on the drive.
#[test]
fn a_commit_doesnt_bring_back_a_node_the_drive_deleted() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let file_id = some_version().file_id;
    let at = |v: RemoteRef| RemoteRef {
        file_id,
        version: v.version,
    };
    let (planned, since, acted) = (at(some_version()), at(some_version()), at(some_version()));
    h.db.write(|s| {
        s.folder().put_base(&BaseRow::new(
            node,
            NodeState::folder(NodeId::ROOT, "b"),
            since,
        ))
    })?;
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a".into(),
        tags: Tags::default(),
        drive: Some(Box::new(DriveVersion {
            state: NodeState::folder(NodeId::ROOT, "a"),
            at: acted,
            base_at: Some(planned.version),
        })),
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    assert_eq!(h.records()?, []);
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { .. } | Op::RemoteUpdate { .. })),
        "the node comes back on the drive: {:?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalTrash { node: n, .. } if *n == node)),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// A folder synced before, made again here, its commit lost; the drive deleted it meanwhile,
/// a file in it still there. Recovery settles the folder at its record's version and, as the
/// commit does, marks what is in it on the drive dirty.
#[test]
fn recovery_of_a_deleted_folder_marks_what_is_in_it() -> R {
    let mut h = Harness::new()?;
    let folder = NodeId::new();
    let at = some_version();
    let (child, child_at) = h
        .remote
        .server()
        .seed_file(folder, "f", b"A", h.folder.algorithm)?;
    h.local_folder("d", folder)?;
    let record = Placing {
        base: BaseRow::new(folder, NodeState::folder(NodeId::ROOT, "d"), at),
        target: RelPath::new("d")?,
        placed_tags: Some(Tags::default()),
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::At(some_version().version),
        held_ms: None,
    };
    let content = h.content(b"A");
    h.db.write(|s| {
        s.folder().put_base(&BaseRow::new(
            folder,
            NodeState::folder(NodeId::ROOT, "d"),
            some_version(),
        ))?;
        s.account().put_mirror(&MirrorRow::from_content(
            child_at,
            child,
            folder,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(content.hash),
        ))?;
        s.folder().put_placing(&record)?;
        s.folder().clear_dirty(child)
    })?;
    recover_placements(&mut h.db, &h.folder)?;
    assert_eq!(h.records()?, []);
    assert_eq!(h.base(folder)?.map(|b| b.remote), Some(at));
    assert!(h.db.read(|s| s.is_dirty(child))?);
    Ok(())
}

/// A file tagged blue here, work on the drive: the plan writes work here, and the write fails.
/// The file keeps blue, and its row says so, so the next plan still sends blue up. Read as not
/// known, the file looked as if holding the drive's tags (work), and blue never went up.
/// `unread`: what it holds can't be read back either: its row keeps what the scan read.
fn a_failed_tag_write_keeps_what_the_item_holds(unread: bool) -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, at) = h.seed("x", b"A")?;
    let (blue, work) = (
        Tags::new(vec![Tag::new("Blue", 0)]),
        Tags::new(vec![Tag::new("Work", 0)]),
    );
    h.local_file("x", node, b"A")?;
    tags::write_on(&h.folder.root.join("x"), &blue)?;
    // Read again as the scan would, tags and all.
    h.local_file("x", node, b"A")?;
    h.scanned_with("x", blue.clone())?;
    let a = h.content(b"A");
    let mut content = NodeContent::file("x", 0, 1, false);
    content.set_tags(&work);
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(a.hash),
            ..BaseRow::new(node, NodeState::file(NodeId::ROOT, "x", a), at)
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(a.hash),
        ))
    })?;
    let both = Tags::new(vec![Tag::new("Blue", 0), Tag::new("Work", 0)]);
    let scanned = h.db.read(|s| s.local_by_node(node))?.ok_or("no row")?;
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 1);
    if unread {
        h.cfg.hooks.faults.fail(FailPoint::TagRead, 1);
    }
    h.run(vec![Op::LocalTags {
        node,
        tags: both,
        drive: Some(Box::new(DriveVersion {
            state: NodeState {
                tags: work,
                ..NodeState::file(NodeId::ROOT, "x", a)
            },
            at,
            base_at: Some(at.version),
        })),
    }])?;
    let row = h.db.read(|s| s.local_by_node(node))?.expect("its row");
    assert_eq!(row.tags, Some(blue));
    // Unread, the row stays as the scan read it, its change time too: the scan reads whatever
    // changed since.
    if unread {
        assert_eq!(row, scanned);
    }
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        plan.ops.iter().any(|op| match op {
            Op::RemoteTags { tags, .. } | Op::RemoteUpdate { tags, .. } =>
                tags.get("Blue").is_some(),
            _ => false,
        }),
        "blue doesn't go up: {:?}",
        plan.ops
    );
    Ok(())
}

#[test]
fn a_failed_tag_write_keeps_what_the_item_holds_as_read() -> R {
    a_failed_tag_write_keeps_what_the_item_holds(false)
}

#[test]
fn a_failed_tag_write_left_unread_keeps_what_the_scan_read() -> R {
    a_failed_tag_write_keeps_what_the_item_holds(true)
}

/// A folder made whose tag write fails, and what it holds can't be read either: made here (from
/// Recently deleted), it holds none (a failed write changes nothing), and its row says so: never
/// not known, which reads as the drive's tags.
#[test]
fn a_folder_made_whose_tags_cant_be_read_back_holds_none() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let mut tagged = NodeContent::folder("d");
    tagged.set_tags(&Tags::new(vec![Tag::new("Work", 0)]));
    let row = MirrorRow::from_content(
        some_version(),
        node,
        NodeId::ROOT,
        NodeKind::Folder,
        &tagged,
        None,
    );
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row,
            removed_at: Some(1),
        })
    })?;
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 1);
    h.cfg.hooks.faults.fail(FailPoint::TagRead, 1);
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "d".into(),
        tags: Tags::new(vec![Tag::new("Work", 0)]),
        drive: None,
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    let row = h.db.read(|s| s.local_by_node(node))?.expect("its row");
    assert_eq!(row.tags, Some(Tags::default()));
    Ok(())
}

/// A file tagged blue here (not sent up yet), or with red recoloured here, edited on the drive:
/// the download's tag write fails, which would leave the new version without the tag as it is
/// here, and it nowhere. It isn't put in place: the file keeps it, and the next plan still
/// sends it up.
#[test]
fn a_download_whose_tag_write_fails_keeps_a_tag_not_sent_up() -> R {
    let (blue, red, recoloured) = (Tag::new("Blue", 0), Tag::new("Red", 0), Tag::new("Red", 3));
    for (held, drive) in [(blue.clone(), None), (recoloured.clone(), Some(red))] {
        let mut h = Harness::new()?;
        if !tags_kept(&h.folder.root) {
            return Ok(()); // nothing to tag with here
        }
        // A colour only where this OS keeps one (macOS).
        if tags::express(&held, h.folder.os).as_ref() != Some(&held) {
            continue;
        }
        let here = Tags::new(vec![held.clone()]);
        let there = Tags::new(drive.clone());
        // On the drive, edited, with what the base holds of the tag (red, its colour unchanged
        // there).
        let mut content = NodeContent::file("x", 0, 1, false);
        content.set_tags(&there);
        let (node, seeded) = h.seed("x", b"B")?;
        let meta = NodeMeta {
            node,
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: content.clone(),
            removed: false,
        };
        let at = h
            .remote
            .server()
            .update(seeded.file_id, seeded.version, &meta, None)?;
        h.local_file("x", node, b"A")?;
        tags::write_on(&h.at("x")?, &here)?;
        // Read again as the scan would, tags and all.
        h.local_file("x", node, b"A")?;
        h.scanned_with("x", here.clone())?;
        let (a, b) = (h.content(b"A"), h.content(b"B"));
        let before = some_version();
        h.db.write(|s| {
            s.folder().put_base(&BaseRow {
                local_hash: Some(a.hash),
                ..BaseRow::new(
                    node,
                    NodeState {
                        tags: there.clone(),
                        ..NodeState::file(NodeId::ROOT, "x", a)
                    },
                    before,
                )
            })?;
            s.account().put_mirror(&MirrorRow::from_content(
                at,
                node,
                NodeId::ROOT,
                NodeKind::File,
                &content,
                Some(b.hash),
            ))
        })?;
        h.cfg.hooks.faults.fail(FailPoint::TagWrite, 10);
        h.run(vec![Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: "x".into(),
            replace: true,
            tags: here.clone(),
            drive: Some(Box::new(DriveVersion {
                state: NodeState {
                    tags: there,
                    ..NodeState::file(NodeId::ROOT, "x", b)
                },
                at,
                base_at: Some(before.version),
            })),
        }])?;
        assert_eq!(fs::read(h.at("x")?)?, b"A", "{held:?}");
        assert_eq!(tags::read(&h.at("x")?), Some(here));
        assert_eq!(h.records()?, []);
        let plan = plan_of(&mut h, &[node])?;
        assert!(
            plan.ops.iter().any(|op| match op {
                Op::RemoteTags { tags, .. } | Op::RemoteUpdate { tags, .. } =>
                    tags.get(&held.name) == Some(&held),
                _ => false,
            }),
            "{held:?} doesn't go up: {:?}",
            plan.ops
        );
    }
    Ok(())
}

/// A tag a failed download write loses: one the item holds as the download was to write it,
/// that the drive doesn't hold so (a colour changed here counts).
#[test]
fn a_recoloured_tag_counts_as_not_sent_up() {
    let tags = |t: &[(&str, u8)]| Tags::new(t.iter().map(|(n, c)| Tag::new(*n, *c)));
    let (red3, red0, none) = (tags(&[("Red", 3)]), tags(&[("Red", 0)]), tags(&[]));
    assert!(super::transfer_ops::tags_lost(&red3, &red3, &red0));
    assert!(super::transfer_ops::tags_lost(&red3, &red3, &none));
    assert!(!super::transfer_ops::tags_lost(&red3, &red3, &red3));
    // Not to be written so (the drive's colour won): nothing lost.
    assert!(!super::transfer_ops::tags_lost(&red3, &red0, &red0));
}

/// The same, its tag write failing: the folder holds none. It settles against the removed
/// version as any placement does (the tags both sides agree on), so its base doesn't claim work,
/// which the next plan would read as this device taking it off.
#[test]
fn a_restored_folder_made_here_settles_as_a_placement() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let node = NodeId::new();
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let mut tagged = NodeContent::folder("a");
    tagged.set_tags(&work);
    let at = some_version();
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::Folder, &tagged, None),
            removed_at: Some(1),
        })
    })?;
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 1);
    let report = h.run(vec![Op::LocalMkdir {
        node,
        parent: NodeId::ROOT,
        name: "a".into(),
        tags: work,
        drive: None,
    }])?;
    assert_eq!(report.done, 1, "{report:?}");
    let base = h.base(node)?.expect("a base");
    assert_eq!(base.remote, at);
    assert_eq!(base.state.tags, Tags::default());
    Ok(())
}

/// A file tagged blue here, edited on the drive, and a drive that won't take writes (read-only):
/// sending blue up fails for good this run, so the download whose tag write fails doesn't wait
/// for it: it is put in place, as ever.
#[test]
fn a_download_doesnt_wait_for_a_tag_the_drive_refused() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, at) = h.seed("x", b"B")?;
    let blue = Tags::new(vec![Tag::new("Blue", 0)]);
    h.local_file("x", node, b"A")?;
    tags::write_on(&h.at("x")?, &blue)?;
    h.local_file("x", node, b"A")?;
    h.scanned_with("x", blue.clone())?;
    let (a, b) = (h.content(b"A"), h.content(b"B"));
    let before = some_version();
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(a.hash),
            ..BaseRow::new(node, NodeState::file(NodeId::ROOT, "x", a), before)
        })
    })?;
    h.remote.faults().refuse_writes = true;
    h.cfg.hooks.faults.fail(FailPoint::TagWrite, 10);
    h.run(vec![
        Op::RemoteTags {
            node,
            file_id: at.file_id,
            expected: at.version,
            tags: blue.clone(),
        },
        Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: "x".into(),
            replace: true,
            tags: blue,
            drive: Some(Box::new(DriveVersion {
                state: NodeState::file(NodeId::ROOT, "x", b),
                at,
                base_at: Some(before.version),
            })),
        },
    ])?;
    assert_eq!(fs::read(h.at("x")?)?, b"B");
    Ok(())
}

/// A node in a role folder in Recently deleted, as the main folder reads it: that folder isn't
/// the main folder's to bring back (§6.3), so the node isn't taken along from here, as the plan
/// reads it too.
#[test]
fn a_removed_folder_outside_this_part_takes_nothing_along() -> R {
    let mut h = Harness::new()?;
    let scope = Scope::main();
    let role = *scope.outside.iter().next().expect("a role folder");
    let (node, at) = (NodeId::new(), some_version());
    let hash = h.content(b"A").hash;
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: MirrorRow::from_content(
                some_version(),
                role,
                NodeId::ROOT,
                NodeKind::Folder,
                &NodeContent::folder("Documents"),
                None,
            ),
            removed_at: Some(i64::MAX / 2),
        })?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            role,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(hash),
        ))
    })?;
    let gone = h.db.read(|s| view::gone_on_drive(s, node, &scope))?;
    assert!(!gone);
    Ok(())
}

/// A folder the mirror has (restored), its row in Recently deleted not gone yet: it is live, so
/// what is in it isn't taken along, as the plan reads it (it looks only at folders the mirror
/// lacks).
#[test]
fn a_live_folder_with_a_leftover_removal_takes_nothing_along() -> R {
    let mut h = Harness::new()?;
    let (folder, node, at) = (NodeId::new(), NodeId::new(), some_version());
    let row = MirrorRow::from_content(
        some_version(),
        folder,
        NodeId::ROOT,
        NodeKind::Folder,
        &NodeContent::folder("d"),
        None,
    );
    let hash = h.content(b"A").hash;
    h.db.write(|s| {
        s.account().put_removed(&RemovedRow {
            row: row.clone(),
            removed_at: Some(i64::MAX / 2),
        })?;
        s.account().put_mirror(&row)?;
        s.account().put_mirror(&MirrorRow::from_content(
            at,
            node,
            folder,
            NodeKind::File,
            &NodeContent::file("f", 0, 1, false),
            Some(hash),
        ))
    })?;
    let gone =
        h.db.read(|s| view::gone_on_drive(s, node, &Scope::main()))?;
    assert!(!gone);
    Ok(())
}

/// Removals go to the drive together, in one request, each file at the version its plan saw:
/// one written since stays as it is (never forced) and is planned again, the others go.
#[test]
fn removals_go_in_one_request_and_a_changed_file_stays() -> R {
    let mut h = Harness::new()?;
    let (a, at_a) = h.seed("a", b"A")?;
    let (b, at_b) = h.seed("b", b"B")?;
    let (c, at_c) = h.seed("c", b"C")?;
    // Another device renames b after this plan was made.
    let renamed = hbsync_remote::NodeMeta {
        node: b,
        parent: NodeId::ROOT,
        kind: NodeKind::File,
        content: NodeContent::file("b2", 0, 1, false),
        removed: false,
    };
    h.remote
        .server()
        .update(at_b.file_id, at_b.version, &renamed, None)?;
    let delete = |node, at: RemoteRef| Op::RemoteDelete {
        node,
        parent: NodeId::ROOT,
        file_id: at.file_id,
        expected: at.version,
    };
    let before = h.remote.faults().requests;
    let report = h.run(vec![delete(a, at_a), delete(b, at_b), delete(c, at_c)])?;
    assert_eq!(h.remote.faults().requests - before, 1, "one request");
    assert!(report.news.behind, "{report:?}");
    let server = h.remote.server();
    let removed: Vec<_> = server.removed().map(|r| r.file_id).collect();
    assert_eq!(removed.len(), 2, "{removed:?}");
    assert!(removed.contains(&at_a.file_id) && removed.contains(&at_c.file_id));
    assert_eq!(server.live_names(), ["b2"]);
    drop(server);
    assert!(h.db.read(|s| s.removed_by_node(b))?.is_none());
    assert!(h.db.read(|s| s.removed_by_node(a))?.is_some());
    Ok(())
}

/// A folder with no base here at "b (conflict 1)" (a file has b), the drive's at b tagged Red:
/// the plan renames it on the drive and writes Red here. The rename loses to another device's,
/// which removes Red too, and the tag write runs first (as after a move put back for a name
/// taken): no base records what it writes, so it waits, and the next plan sends no Red up.
/// Written, Red read as this device's add, and went back up.
#[test]
fn a_tag_written_where_no_base_settles_is_not_an_add() -> R {
    let mut h = Harness::new()?;
    if !tags_kept(&h.folder.root) {
        return Ok(()); // nothing to tag with here
    }
    // The higher id: the file keeps the name b (§6.1).
    let node = NodeId(hbsync_types::Uuid::max());
    let red = Tags::new(vec![Tag::new("Red", 6)]);
    h.local_folder("b (conflict 1)", node)?;
    h.scanned_with("b (conflict 1)", Tags::default())?;
    let folder = |name: &str, tags: &Tags, at| {
        let mut c = NodeContent::folder(name);
        c.set_tags(tags);
        MirrorRow::from_content(at, node, NodeId::ROOT, NodeKind::Folder, &c, None)
    };
    let (file, _) = h.seed("b", b"B")?;
    let v1 = some_version();
    h.db.write(|s| s.account().put_mirror(&folder("b", &red, v1)))?;
    let ops = plan_of(&mut h, &[node, file])?.ops;
    assert!(
        ops.iter()
            .any(|op| matches!(op, Op::RemoteUpdate { tags, .. } if *tags == red)),
        "{ops:?}"
    );
    let here: Vec<Op> = ops
        .into_iter()
        .filter(|op| matches!(op, Op::LocalTags { .. }))
        .collect();
    assert_eq!(here.len(), 1);
    let report = h.run(here)?;
    assert!(
        matches!(report.skipped.as_slice(), [(_, Skip::Waiting(_))]),
        "it waits: {report:?}"
    );
    assert_eq!(tags::read(&h.at("b (conflict 1)")?), Some(Tags::default()));
    let v2 = RemoteRef {
        version: some_version().version,
        ..v1
    };
    h.db.write(|s| {
        s.account()
            .put_mirror(&folder("b (conflict 1)", &Tags::default(), v2))
    })?;
    let ops = plan_of(&mut h, &[node])?.ops;
    let sends_red = ops.iter().any(|op| match op {
        Op::RemoteTags { tags, .. } | Op::RemoteUpdate { tags, .. } => tags.get("Red").is_some(),
        _ => false,
    });
    assert!(!sends_red, "Red went back up: {ops:?}");
    let report = h.run(ops)?;
    assert!(report.failed.is_empty(), "{report:?}");
    let base = h.base(node)?.expect("settled");
    assert_eq!(base.state.tags, Tags::default());
    assert_eq!(base.state.name, "b (conflict 1)");
    Ok(())
}

/// Every error goes through the one policy: a file another program locks or a full disk is
/// left for a later sync (paced by the runner, told when it keeps coming back), not retried
/// at once; a failing state DB ends the run, as at a crash; a server that says when to come
/// back stops the run until then.
#[test]
fn errors_go_through_the_policy() {
    // EBUSY and ENOSPC, on Linux and macOS alike.
    #[cfg(unix)]
    for code in [16, 28] {
        let outcome = io_outcome(io::Error::from_raw_os_error(code));
        assert!(
            matches!(outcome, Outcome::Skip(Skip::Stale(_))),
            "{outcome:?}"
        );
    }
    let denied = io_outcome(io::Error::from(io::ErrorKind::PermissionDenied));
    assert!(
        matches!(denied, Outcome::Skip(Skip::Unsyncable(_))),
        "{denied:?}"
    );
    let corrupt = StateError::Corrupt("test".into());
    assert!(matches!(Outcome::from(corrupt), Outcome::Fatal(_)));
    let later = remote_outcome(RemoteError::Later {
        why: "paused".into(),
        retry_after: Duration::from_secs(600),
    });
    assert!(matches!(later, Outcome::Stop(Stopped::Later(_, d)) if d == Duration::from_secs(600)));
    let full = remote_outcome(RemoteError::Full {
        why: "quota".into(),
        retry_after: Some(Duration::from_secs(60)),
    });
    assert!(matches!(full, Outcome::Full(Some(_))));
}

/// The drive removes a file and a plan here trashes it; before the trash runs, the drive brings
/// it back (a restore, the same file at a new version) and another engine's pull puts that in
/// the mirror. The trash acted on the removal: the next plan brings the file back here, and
/// never deletes it again (the user's restore stands).
#[test]
fn a_restore_while_the_trash_runs_stands() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let removed = some_version();
    let back = RemoteRef {
        version: some_version().version,
        ..removed
    };
    let file = NodeState::file(NodeId::ROOT, "a", h.content(b"A"));
    h.local_file("a", node, b"A")?;
    h.db.write(|s| {
        s.folder().put_base(&BaseRow {
            local_hash: Some(h_content_hash(b"A")),
            ..BaseRow::new(node, file, removed)
        })
    })?;
    // The pull: back in the mirror, at its new version.
    h.mirror(node, "a", back, b"A")?;
    h.run(vec![Op::LocalTrash {
        node,
        subtree: vec![node],
    }])?;
    assert!(!h.folder.root.join("a").exists(), "trashed");
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { .. })),
        "the restore is undone: {:?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { node: n, .. } if *n == node)),
        "the file comes back here: {:?}",
        plan.ops
    );
    Ok(())
}

/// Where the live folder a removed folder is in sits, for [`removed_in_a_live_folder`].
#[derive(Clone, Copy, PartialEq)]
enum Under {
    /// At the top.
    Top,
    /// In a folder at the top, synced here.
    Folder,
    /// In a folder at the top in Recently deleted, which took it along: written after, the
    /// new node keeps both.
    Removed,
    /// In the role folder, which the main folder leaves out.
    Role,
}

/// A folder written into a folder another device removed (the folder in Recently deleted, in a
/// live one `under` where it says), the new node alone dirty: the plan from narrow views and
/// from whole folders. Returns the narrow one, and the ids of the live folder, the removed one
/// and the new one.
fn removed_in_a_live_folder(
    under: Under,
) -> Result<(crate::plan::Plan, [NodeId; 3]), Box<dyn std::error::Error>> {
    let mut h = Harness::new()?;
    let (top, live, removed, new) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
    let folder_row = |node, parent, name: &str, at| {
        MirrorRow::from_content(
            at,
            node,
            parent,
            NodeKind::Folder,
            &NodeContent::folder(name),
            None,
        )
    };
    let top = match under {
        Under::Top => NodeId::ROOT,
        Under::Role => hbsync_types::Role::Documents.node(),
        Under::Folder | Under::Removed => top,
    };
    let (at_top, at_live) = (some_version(), some_version());
    if under == Under::Folder {
        h.local_folder("x", top)?;
        h.local_folder("x/a", live)?;
    } else if under == Under::Top {
        h.local_folder("a", live)?;
    }
    h.db.write(|s| {
        let synced =
            |parent, name: &str, node, at| BaseRow::new(node, NodeState::folder(parent, name), at);
        match under {
            Under::Top => s.folder().put_base(&synced(top, "a", live, at_live))?,
            Under::Folder => {
                s.folder()
                    .put_base(&synced(NodeId::ROOT, "x", top, at_top))?;
                s.folder().put_base(&synced(top, "a", live, at_live))?;
            }
            Under::Removed | Under::Role => {}
        }
        if !top.is_root() {
            let row = folder_row(top, NodeId::ROOT, "x", at_top);
            match under {
                Under::Removed => s.account().put_removed(&RemovedRow {
                    row,
                    removed_at: Some(50),
                })?,
                _ => s.account().put_mirror(&row)?,
            }
        }
        let mut row = folder_row(live, top, "a", at_live);
        row.changed_at = Some(10);
        s.account().put_mirror(&row)?;
        s.account().put_removed(&RemovedRow {
            row: folder_row(removed, live, "b", some_version()),
            removed_at: Some(100),
        })?;
        let mut row = folder_row(new, removed, "c", some_version());
        row.changed_at = Some(200); // written after the removal: it keeps its folder
        s.account().put_mirror(&row)?;
        // The new node alone dirty (the rest planned before: another page's otherwise).
        for node in [top, live, removed] {
            s.folder().clear_dirty(node)?;
        }
        s.folder().mark_dirty(new)
    })?;
    let ctx = FolderCtx {
        scope: Arc::new(Scope::of(NodeId::ROOT)),
        ..h.folder.clone()
    };
    let opts = crate::plan::Options::default();
    let reconcile = |input: &crate::reconcile::Input, _: &mut StateDb| {
        crate::reconcile::reconcile(input, &opts, ctx.masking())
    };
    let narrow = view::plan_narrow(&mut h.db, &[new], &ctx, reconcile)?;
    let whole = h.db.read(|s| view::build_input(s, &[new], &ctx))?;
    let whole = crate::reconcile::reconcile(&whole, &opts, ctx.masking());
    assert_eq!(narrow, whole, "the narrow view plans as whole folders");
    assert!(
        !narrow
            .conflicts
            .iter()
            .any(|c| matches!(c, crate::plan::Conflict::Orphan { .. })),
        "nothing orphaned: {:?}",
        narrow.ops
    );
    Ok((narrow, [live, removed, new]))
}

/// The removed folder comes back where it was, in its live folder: not read as gone for good,
/// what was written into it since put into Recovered files.
#[test]
fn a_narrow_view_holds_the_live_folder_a_removed_one_is_in() -> R {
    for under in [Under::Top, Under::Folder] {
        let (plan, [live, removed, _]) = removed_in_a_live_folder(under)?;
        assert!(
            plan.ops.iter().any(|op| matches!(op,
                Op::RemoteCreate { node, parent, .. } if *node == removed && *parent == live)),
            "{:?}",
            plan.ops
        );
    }
    Ok(())
}

/// The live folder in a folder in Recently deleted: that one is looked up too, and the new
/// node keeps them all (written after their removals).
#[test]
fn a_narrow_view_follows_removed_folders_above_the_live_one() -> R {
    let (plan, [live, removed, _]) = removed_in_a_live_folder(Under::Removed)?;
    assert!(
        plan.ops.iter().any(|op| matches!(op,
            Op::RemoteCreate { node, parent, .. } if *node == removed && *parent == live)),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// In the role folder, which the main folder leaves out: neither view brings it in.
#[test]
fn a_narrow_view_leaves_a_removed_folder_in_the_role_folder_alone() -> R {
    let (plan, [_, removed, new]) = removed_in_a_live_folder(Under::Role)?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| op.node() == removed || op.node() == new),
        "{:?}",
        plan.ops
    );
    Ok(())
}

/// A node another writer put in a file is an orphan for Recovered files (§6.3), where a synced
/// item already holds its name: both views look up what is in Recovered files by that name
/// from the first (the whole view missed it and planned the clash; the narrow one took a
/// second round, listing the top and Recovered files whole), and give the node another name.
#[test]
fn a_node_in_a_file_meets_what_is_in_recovered_files() -> R {
    let mut h = Harness::new()?;
    let (file, kept, new) = (NodeId::new(), NodeId::new(), NodeId::new());
    let recovered = hbsync_types::RECOVERED_NAME;
    h.local_folder(recovered, NodeId::RECOVERED)?;
    h.local_file(&format!("{recovered}/c"), kept, b"kept")?;
    h.local_file("f", file, b"f")?;
    let (kept_c, file_c) = (h.content(b"kept"), h.content(b"f"));
    let synced = [
        (
            NodeId::RECOVERED,
            NodeState::folder(NodeId::ROOT, recovered),
        ),
        (kept, NodeState::file(NodeId::RECOVERED, "c", kept_c)),
        (file, NodeState::file(NodeId::ROOT, "f", file_c)),
    ];
    h.db.write(|s| {
        for (node, state) in &synced {
            let at = some_version();
            s.folder()
                .put_base(&BaseRow::new(*node, state.clone(), at))?;
            let content = match state.content {
                Some(c) => NodeContent::file(&state.name, 0, c.size, false),
                None => NodeContent::folder(&state.name),
            };
            let hash = state.content.map(|c| c.hash);
            let row = MirrorRow::from_content(at, *node, state.parent, state.kind, &content, hash);
            s.account().put_mirror(&row)?;
        }
        let row = MirrorRow::from_content(
            some_version(),
            new,
            file,
            NodeKind::File,
            &NodeContent::file("c", 0, 3, false),
            Some(hash_bytes(b"new", h.folder.algorithm)),
        );
        s.account().put_mirror(&row)?;
        // Synced: only the new node is dirty.
        for (node, _) in &synced {
            s.folder().clear_dirty(*node)?;
        }
        s.folder().mark_dirty(new)
    })?;
    let ctx = FolderCtx {
        scope: Arc::new(Scope::of(NodeId::ROOT)),
        ..h.folder.clone()
    };
    let opts = crate::plan::Options::default();
    let reconcile = |input: &crate::reconcile::Input, _: &mut StateDb| {
        crate::reconcile::reconcile(input, &opts, ctx.masking())
    };
    let whole = h.db.read(|s| view::build_input(s, &[new], &ctx))?;
    let whole = crate::reconcile::reconcile(&whole, &opts, ctx.masking());
    let narrow = view::plan_narrow(&mut h.db, &[new], &ctx, reconcile)?;
    assert_eq!(narrow, whole, "the narrow view plans as the whole one");
    let placed = whole.ops.iter().find_map(|op| match op {
        Op::LocalDownload {
            node, parent, name, ..
        } if *node == new => Some((*parent, name)),
        _ => None,
    });
    let (parent, name) = placed.ok_or_else(|| format!("{:?}", whole.ops))?;
    assert_eq!(parent, NodeId::RECOVERED);
    assert_ne!(name, "c", "not over what Recovered files holds");
    Ok(())
}

/// A folder removed on the drive at a time not known yet (an open removal) took along the
/// synced file still live in it (`along::taken_along`: an open removal is the newest): the
/// guard counts it with what is gone remotely, as the reconciler does, and a confirmed trash
/// of the folder covers it (sync semantics §7).
#[test]
fn an_open_removal_counts_what_it_took_along() -> R {
    let mut h = Harness::new()?;
    let (folder, file) = (NodeId::new(), NodeId::new());
    h.local_folder("d", folder)?;
    h.local_file("d/f", file, b"f")?;
    let f_c = h.content(b"f");
    h.db.write(|s| {
        s.folder().put_base(&BaseRow::new(
            folder,
            NodeState::folder(NodeId::ROOT, "d"),
            some_version(),
        ))?;
        s.folder().put_base(&BaseRow::new(
            file,
            NodeState::file(folder, "f", f_c),
            some_version(),
        ))?;
        let row = |node, parent, kind, name: &str| {
            let content = match kind {
                NodeKind::File => NodeContent::file(name, 0, f_c.size, false),
                _ => NodeContent::folder(name),
            };
            let hash = (kind == NodeKind::File).then_some(f_c.hash);
            MirrorRow::from_content(some_version(), node, parent, kind, &content, hash)
        };
        s.account().put_removed(&RemovedRow {
            row: row(folder, NodeId::ROOT, NodeKind::Folder, "d"),
            removed_at: None,
        })?;
        let mut live = row(file, folder, NodeKind::File, "f");
        live.changed_at = Some(5);
        s.account().put_mirror(&live)?;
        s.folder().clear_dirty(file)?;
        s.folder().mark_dirty(folder)
    })?;
    let (pending, confirmed) =
        h.db.read(|s| Ok((s.pending_file_deletes()?, s.confirmed_file_deletes(folder)?)))?;
    assert_eq!(
        pending,
        (0, 1),
        "the file taken along counts as gone remotely"
    );
    assert_eq!(
        confirmed,
        Some((false, vec![file])),
        "the folder's trash covers it"
    );
    Ok(())
}
