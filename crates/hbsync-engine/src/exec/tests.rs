//! The executor on a hand-made state: operations and recovery where the plan, the mirror and
//! the disk disagree in ways a sync of two clients rarely reaches.

use super::recover::recover_placements;
use super::*;
use crate::engine::EngineConfig;
use hbsync_fileio::hash_bytes;
use hbsync_platform::tags;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::MirrorFile;
use hbsync_types::schema::NodeContent;
use hbsync_types::{FileContent, RemoteRef, Tag};

type R = Result<(), Box<dyn std::error::Error>>;

struct Harness {
    _dir: tempfile::TempDir,
    db: StateDb,
    remote: FakeRemote,
    cfg: ExecConfig,
}

impl Harness {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let cfg = EngineConfig::new(dir.path(), "t").exec;
        Ok(Harness {
            _dir: dir,
            db: StateDb::open_in_memory()?,
            remote: FakeRemote::new(FakeServer::new(ServerConfig::default())),
            cfg,
        })
    }

    /// Writes `data` at `rel` and records it as the scan would: `node`, hashed.
    fn local_file(&mut self, rel: &str, node: NodeId, data: &[u8]) -> R {
        let path = self.cfg.root.join(rel);
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
            hash: Some(hash_bytes(data, self.cfg.algorithm)),
            tags: None,
        };
        self.db.write(|s| s.put_local(&row))?;
        Ok(())
    }

    /// Records the folder at `rel` as the scan would.
    fn local_folder(&mut self, rel: &str, node: NodeId) -> R {
        let path = self.cfg.root.join(rel);
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
        self.db.write(|s| s.put_local(&row))?;
        Ok(())
    }

    fn content(&self, data: &[u8]) -> FileContent {
        FileContent {
            hash: hash_bytes(data, self.cfg.algorithm),
            size: data.len() as u64,
            mtime_ms: 0,
            executable: false,
        }
    }

    fn run(&mut self, ops: Vec<Op>) -> Result<ExecReport, StateError> {
        Executor::new(&mut self.db, &self.remote, &self.cfg).run(ops)
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
                .seed_file(NodeId::ROOT, name, data, self.cfg.algorithm)?;
        self.mirror(node, name, at, data)?;
        Ok((node, at))
    }

    /// The mirror's row of `node`: a file `name` at the top holding `data`, at `at`.
    fn mirror(&mut self, node: NodeId, name: &str, at: RemoteRef, data: &[u8]) -> R {
        let c = self.content(data);
        self.db.write(|s| {
            s.put_mirror(&MirrorRow {
                file_id: at.file_id,
                version: at.version,
                node,
                parent: NodeId::ROOT,
                name: name.into(),
                kind: NodeKind::File,
                file: Some(MirrorFile {
                    hash: c.hash,
                    size: c.size,
                    mtime_ms: 0,
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
            s.put_local(&LocalRow {
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
        placed_ms: 0,
    };
    h.db.write(|s| s.put_placing(&record))?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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
        s.put_local(&LocalRow {
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
        placed_ms: 0,
    };
    h.db.write(|s| s.put_placing(&record))?;
    recover_placements(&mut h.db, &h.cfg.root, Os::Linux)?;
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
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let name = "\u{958}".repeat(85);
    assert!(hbsync_namemap::to_remote(&name, h.cfg.os).is_err());
    let fetched = h.content(b"A");
    h.local_file(&name, NodeId::new(), b"A")?;
    let record = Placing {
        base: BaseRow::new(
            node,
            NodeState::file(NodeId::ROOT, "f", fetched),
            some_version(),
        ),
        target: RelPath::new(&name)?,
        placed_tags: Some(Tags::default()),
        prior_tags: Some(Tags::default()),
        base_at: PlannedBase::Absent,
        placed_ms: 0,
    };
    h.db.write(|s| s.put_placing(&record))?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    assert_eq!(h.base(node)?, None);
    assert_eq!(h.db.read(|s| s.all_placing())?, [record]);
    // Taken over: the next recovery finds it as the node's.
    let row = h.db.read(|s| s.local_by_node(node))?;
    assert_eq!(row.map(|r| r.path.to_string()), Some(name));
    Ok(())
}

/// The plan for `nodes` from what the DB holds now.
fn plan_of(h: &mut Harness, nodes: &[NodeId]) -> Result<crate::plan::Plan, StateError> {
    let (os, filter) = (h.cfg.os, Arc::clone(&h.cfg.filter));
    let input = h.db.read(|s| {
        view::build_input(
            s,
            nodes,
            os,
            &filter,
            &crate::scope::Scope::of(NodeId::ROOT),
        )
    })?;
    Ok(crate::reconcile::reconcile(
        &input,
        &crate::plan::Options::default(),
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
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        // Synced before (made again after a delete here lost): it gets a record.
        s.put_base(&BaseRow::new(node, state.clone(), at))
    })?;
    h.cfg.crash_at = Some(CrashPoint::AfterPlaced);
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
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    let plan = plan_of(&mut h, &[node])?;
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteTags { .. } | Op::RemoteUpdate { .. })),
        "the drive's tags removed: {:?}",
        plan.ops
    );
    assert!(
        plan.ops
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
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.put_base(&BaseRow::new(node, state.clone(), at))?;
        s.put_placing(&Placing {
            base: BaseRow::new(node, state, at),
            target,
            placed_tags: None,
            prior_tags: None,
            base_at: PlannedBase::Unchecked,
            placed_ms: 0,
        })
    })?;
    // The scan finds the folder, made without tags.
    h.local_folder("d", NodeId::new())?;
    h.scanned_with("d", Tags::default())?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalTags { node: n, .. } if *n == node)),
        "the folder doesn't get its tags: {:?}",
        plan.ops
    );
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
    recover_placements(&mut h.db, &h.cfg.root, Os::Linux)?;
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
    let tmp = layout::tmp_dir(&h.cfg.root);
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
    assert!(h.cfg.root.join("d").is_dir(), "{report:?}");
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
    h.cfg.crash_at = Some(CrashPoint::AfterPlaced);
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
    assert_eq!(fs::read(h.cfg.root.join("f"))?, b"A");
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
        s.put_base(&BaseRow {
            local_hash: Some(old.hash),
            ..BaseRow::new(
                node,
                NodeState::file(NodeId::ROOT, "f", old),
                some_version(),
            )
        })?;
        s.put_mirror(&MirrorRow::from_content(
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
        h.cfg.crash_at = Some(CrashPoint::AfterPlaced);
    }
    let ran = h.run(vec![Op::Settle { node }, download]);
    assert_eq!(ran.is_err(), crash, "{ran:?}");
    if crash {
        // The scan finds the replaced file, with what it holds of tags.
        let read = tags::read(&h.cfg.root.join("f"));
        h.local_file("f", node, b"B")?;
        h.scanned_with("f", read.unwrap_or_default())?;
        recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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

/// A download's record claims the tags it writes, the empty set too, as they read back: an
/// untagged file is known to hold none. (Claiming none for it read as not known, and a tag the
/// user had removed came back.) Where the volume keeps no tags, it claims none.
#[test]
fn a_record_claims_the_tags_it_writes() -> R {
    let mut h = Harness::new()?;
    let kept = tags_kept(&h.cfg.root);
    let none = record_of_a_crashed_download(&mut h, &Tags::default())?;
    assert_eq!(none, kept.then(Tags::default));
    let mut h = Harness::new()?;
    let work = Tags::new(vec![Tag::new("Work", 0)]);
    let expected = kept.then(|| work.held_by(h.cfg.os));
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
    let (node, at) = h
        .remote
        .server()
        .seed_file(NodeId::ROOT, "f", b"B", h.cfg.algorithm)?;
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
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::File,
            &content,
            Some(hash),
        ))?;
        s.put_base(&BaseRow::new(node, earlier, base))?;
        s.put_placing(&Placing {
            base: BaseRow::new(node, fetched, at),
            target: RelPath::new("f").expect("a path"),
            placed_tags: Some(Tags::default()),
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(base.version),
            placed_ms: 0,
        })
    })?;
    // The scan finds the new bytes, untagged.
    h.local_file("f", NodeId::new(), b"B")?;
    h.scanned_with("f", Tags::default())?;
    recover_placements(&mut h.db, &h.cfg.root, Os::Linux)?;
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
/// empty set from the start (a crash right after it is made leaves that true).
#[test]
fn an_untagged_folder_claims_no_tags() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    let at = some_version();
    let state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| s.put_base(&BaseRow::new(node, state.clone(), at)))?;
    h.cfg.crash_at = Some(CrashPoint::AfterPlaced);
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
    assert_eq!(h.records()?[0].placed_tags, Some(Tags::default()));
    Ok(())
}

/// Tags that can't be written (a value too large for the file system's attributes) leave the
/// file without them, and its record claims none: recovery reads the item as not known (the
/// drive's tags standing), as a commit does whose tags write failed.
#[test]
fn a_tag_write_that_fails_is_never_claimed() -> R {
    let mut h = Harness::new()?;
    let huge = Tags::new(vec![Tag::new("x".repeat(70_000), 0)]);
    let probe = h.cfg.root.join(".huge");
    fs::write(&probe, b"")?;
    if !tags_kept(&h.cfg.root) || tags::write(&probe, &huge).is_ok() {
        return Ok(()); // no tags here, or this file system takes it
    }
    assert_eq!(record_of_a_crashed_download(&mut h, &huge)?, None);
    // The scan finds the file; recovery writes what the commit would have: the drive's tags.
    let node = h.db.read(|s| s.all_placing())?[0].base.node;
    h.local_file("f", NodeId::new(), b"A")?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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
    assert!(!h.cfg.root.join("f").exists());
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
        s.put_mirror(&MirrorRow::from_content(
            at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.put_mirror(&MirrorRow::from_content(
            some_version(),
            child,
            folder,
            NodeKind::Folder,
            &NodeContent::folder("e"),
            None,
        ))?;
        s.put_placing(&Placing {
            base: BaseRow::new(folder, NodeState::folder(NodeId::ROOT, "d"), at),
            target: RelPath::new("d").expect("a path"),
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::Absent,
            placed_ms: 0,
        })?;
        // Pulled before: nothing marked now.
        s.clear_dirty(child)
    })?;
    h.local_folder("d", NodeId::new())?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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
    assert!(!h.cfg.root.join("f").exists(), "fetched another version");
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

    fn create(
        &self,
        node: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<Target> {
        self.inner.create(node, payload)
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<Target> {
        self.inner.update(target, meta, payload)
    }

    fn delete(
        &self,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> hbsync_remote::Result<()> {
        self.inner.delete(file_id, expected)
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
    let cfg = EngineConfig::new(&root, "t").exec;
    let mut db = StateDb::open(&path)?;
    let fake = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let (node, at) = fake
        .server()
        .seed_file(NodeId::ROOT, "f", b"A", cfg.algorithm)?;
    let content = FileContent {
        hash: hash_bytes(b"A", cfg.algorithm),
        size: 1,
        mtime_ms: 0,
        executable: false,
    };
    db.write(|s| {
        // Planned because it was dirty: nothing clears that until a plan finds it clean.
        s.mark_dirty(node)?;
        s.put_mirror(&MirrorRow::from_content(
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
            let gone = StateDb::open(&hook_path)
                .and_then(|mut other| other.write(|s| s.delete_mirror(at.file_id).map(drop)));
            assert!(gone.is_ok(), "{gone:?}");
        }))),
    };
    let drive = DriveVersion {
        state: NodeState::file(NodeId::ROOT, "f", content),
        at,
        base_at: None,
    };
    Executor::new(&mut db, &remote, &cfg).run(vec![Op::LocalDownload {
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
    let input = db.read(|s| {
        view::build_input(
            s,
            &[node],
            cfg.os,
            &cfg.filter,
            &crate::scope::Scope::of(NodeId::ROOT),
        )
    })?;
    let plan = crate::reconcile::reconcile(&input, &crate::plan::Options::default());
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
    let cfg = EngineConfig::new(&root, "t").exec;
    let mut db = StateDb::open(&path)?;
    let fake = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let folder = NodeId::new();
    let (node, at) = fake.server().seed_file(folder, "f", b"A", cfg.algorithm)?;
    let content = FileContent {
        hash: hash_bytes(b"A", cfg.algorithm),
        size: 1,
        mtime_ms: 0,
        executable: false,
    };
    db.write(|s| {
        s.put_local(&LocalRow {
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
        s.put_mirror(&MirrorRow::from_content(
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
                .and_then(|mut other| other.write(|s| s.delete_local(folder)));
            assert!(gone.is_ok(), "{gone:?}");
        }))),
    };
    let drive = DriveVersion {
        state: NodeState::file(folder, "f", content),
        at,
        base_at: None,
    };
    let _ = Executor::new(&mut db, &remote, &cfg).run(vec![Op::LocalDownload {
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
    Executor::new(&mut h.db, &remote, &h.cfg)
        .with_stop(stop)
        .run(ops)?;
    let placed = ["f", "g"]
        .iter()
        .filter(|n| h.cfg.root.join(n).exists())
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
        s.put_base(&BaseRow {
            local_hash: Some(h_content_hash(b"A")),
            ..BaseRow::new(node, file.clone(), acted)
        })?;
        s.clear_dirty(node)
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
    assert!(h.cfg.root.join("b").exists());
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
    if !tags_kept(&h.cfg.root) {
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
        s.put_base(&BaseRow {
            local_hash: Some(hash),
            ..BaseRow::new(node, synced, before)
        })?;
        s.put_mirror(&MirrorRow::from_content(
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
    assert_eq!(tags::read(&h.cfg.root.join(name)), Some(red.clone()));
    // The user removes red.
    tags::write(&h.cfg.root.join(name), &Tags::default())?;
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
            placed_ms: 0,
        },
        Placing {
            base: BaseRow::new(folder, folder_state.clone(), folder_at),
            target: RelPath::new("d")?,
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(folder_at.version),
            placed_ms: 0,
        },
    ];
    h.db.write(|s| {
        s.put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))?;
        records.iter().try_for_each(|r| s.put_placing(r))
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
    assert!(!h.cfg.root.join("f").exists() && !h.cfg.root.join("d").exists());
    let mut left = h.records()?;
    left.sort_by_key(|r| r.base.node);
    let mut want = records.to_vec();
    want.sort_by_key(|r| r.base.node);
    assert_eq!(left, want);
    assert!(h.db.read(|s| Ok(s.is_dirty(file)? && s.is_dirty(folder)?))?);
    Ok(())
}

/// A round leaves a folder made and its record (its run cut short after the folder was made);
/// the scan hasn't seen the folder yet. The next round's recovery drops the record (nothing it
/// knows is at its place), and that round's folder op takes the folder that is there: one
/// folder, the node's, no record left. Without the recovery, the record held the node off
/// until the next sync.
#[test]
fn a_folder_a_round_left_is_taken_by_the_next() -> R {
    let mut h = Harness::new()?;
    let (node, at) = (NodeId::new(), some_version());
    let state = NodeState::folder(NodeId::ROOT, "d");
    h.db.write(|s| {
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.put_base(&BaseRow::new(node, state.clone(), at))
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
    h.cfg.crash_at = Some(CrashPoint::AfterPlaced);
    assert!(h.run(vec![mkdir()]).is_err());
    assert_eq!(h.records()?.len(), 1);
    h.cfg.crash_at = None;
    // The next round.
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    let report = h.run(vec![mkdir()])?;
    assert!(report.skipped.is_empty(), "{report:?}");
    assert_eq!(h.records()?, []);
    let here: Vec<_> = fs::read_dir(&h.cfg.root)?
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
    if !tags_kept(&h.cfg.root) {
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
        s.put_base(&BaseRow {
            local_hash: Some(c.hash),
            ..BaseRow::new(node, file.clone(), v0)
        })?;
        s.put_mirror(&mirror(v1, &y))
    })?;
    let ops = plan_of(&mut h, &[node])?.ops;
    assert!(matches!(ops.as_slice(), [Op::LocalTags { .. }]), "{ops:?}");
    h.db.write(|s| {
        s.put_base(&BaseRow {
            local_hash: Some(c.hash),
            ..BaseRow::new(node, file.clone(), v2)
        })?;
        s.put_mirror(&mirror(v3, &Tags::default()))
    })?;
    h.run(ops)?;
    assert_eq!(tags::read(&h.cfg.root.join("f")), Some(y.clone()));
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
    if !tags_kept(&h.cfg.root) {
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
        s.put_base(&BaseRow {
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
        s.put_mirror(&MirrorRow::from_content(
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
    fs::write(h.cfg.root.join("g"), b"not ours")?;
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
    assert_eq!(fs::read(h.cfg.root.join("g"))?, b"not ours");
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
        s.put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        // Synced before: it gets a record.
        s.put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))
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
    h.cfg
        .fail_commits
        .store(2, std::sync::atomic::Ordering::Relaxed);
    let report = h.run(ops)?;
    assert_eq!(report.done, 2, "{report:?}");
    assert!(
        report.failed.is_empty() && report.skipped.is_empty(),
        "{report:?}"
    );
    assert_eq!(h.records()?, []);
    let mut here: Vec<_> = fs::read_dir(&h.cfg.root)?
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
        s.put_mirror(&MirrorRow::from_content(
            folder_at,
            folder,
            NodeId::ROOT,
            NodeKind::Folder,
            &NodeContent::folder("d"),
            None,
        ))?;
        s.put_base(&BaseRow::new(folder, folder_state.clone(), folder_at))
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
                .seed_file(folder, name, name.as_bytes(), h.cfg.algorithm)?;
        let c = h.content(name.as_bytes());
        let content = NodeContent::file(name, 0, 1, false);
        h.db.write(|s| {
            s.put_mirror(&MirrorRow::from_content(
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
    h.cfg
        .fail_commits
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let report = h.run(ops)?;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(report.done, 3, "{report:?}");
    assert_eq!(fs::read(h.cfg.root.join("d/x"))?, b"x");
    assert_eq!(fs::read(h.cfg.root.join("d/y"))?, b"y");
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
        .seed_file(NodeId::ROOT, "b", b"B1", h.cfg.algorithm)?;
    h.local_file("b", b, b"B0")?;
    let b0 = h.content(b"B0");
    let before = some_version();
    h.db.write(|s| {
        s.put_base(&BaseRow {
            local_hash: Some(b0.hash),
            ..BaseRow::new(b, NodeState::file(NodeId::ROOT, "b", b0), before)
        })
    })?;
    h.mirror(b, "b", b_at, b"B1")?;
    // The user edits b after the plan.
    fs::write(h.cfg.root.join("b"), b"user's edit")?;
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
    h.cfg
        .fail_commits
        .store(1, std::sync::atomic::Ordering::Relaxed);
    h.run(ops)?;
    assert_eq!(fs::read(h.cfg.root.join("b"))?, b"user's edit");
    assert!(h.base(a)?.is_some(), "a isn't settled");
    Ok(())
}

/// A folder new here (no record) whose commit fails once it is made: its retry takes over the
/// folder it made, as main does, in the same run.
#[test]
fn a_new_folder_whose_commit_failed_is_taken_by_its_retry() -> R {
    let mut h = Harness::new()?;
    let node = NodeId::new();
    h.cfg
        .fail_commits
        .store(1, std::sync::atomic::Ordering::Relaxed);
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

/// What recovery in the run reads at a placement's target: nothing only when verifiably
/// nothing is there (not found; or what the scan skips, a link), and "can't tell" when its
/// metadata can't be read (a folder denying access): the record stays then. Read as nothing,
/// the record went, and the item became a new one.
#[cfg(unix)]
#[test]
fn recovery_tells_an_unreadable_item_from_none() -> R {
    use super::recover::{OnDisk, read_item};
    use std::os::unix::fs::PermissionsExt;
    let h = Harness::new()?;
    let (root, alg, node) = (h.cfg.root.clone(), h.cfg.algorithm, NodeId::new());
    fs::write(root.join("f"), b"A")?;
    std::os::unix::fs::symlink(root.join("f"), root.join("link"))?;
    fs::create_dir(root.join("p"))?;
    fs::write(root.join("p/f"), b"A")?;
    let read = |rel: &str| read_item(&root, &RelPath::new(rel).expect("a path"), node, alg);
    assert!(matches!(read("f"), OnDisk::Item(r) if r.node == node && r.hash.is_some()));
    assert!(matches!(read("gone"), OnDisk::Absent));
    assert!(matches!(read("link"), OnDisk::Absent));
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
    use super::recover::{Judged, apply};
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
        placed_ms: 0,
    };
    h.db.write(|s| s.put_placing(&record))?;
    let row = LocalRow {
        path: RelPath::new("f")?,
        node,
        kind: NodeKind::File,
        fingerprint: hbsync_platform::stat(&h.cfg.root.join("f"))?.fingerprint(),
        executable: Some(false),
        hash: Some(h.content(b"A").hash),
        tags: None,
    };
    let os = h.cfg.os;
    let at = h.db.write(|s| apply(s, &record, Judged::Settle(row), os))?;
    assert_eq!(at, None);
    assert_eq!(h.records()?, [record]);
    let path = RelPath::new("f")?;
    assert_eq!(
        h.db.read(|s| s.get_local(&path))?.map(|r| r.node),
        Some(other)
    );
    Ok(())
}

/// A record whose file stays unhashed holds its node only so long ([`UNHASHED_HOLD`]): past
/// it, recovery drops it as main does (the file is then the scan's new item), rather than hold
/// the node off for good.
#[test]
fn a_record_whose_file_stays_unhashed_goes_in_time() -> R {
    let mut h = Harness::new()?;
    record_of_a_crashed_download(&mut h, &Tags::default())?;
    let record = h.records()?.remove(0);
    let old = hbsync_types::unix_now_ms() - UNHASHED_HOLD.as_millis() as i64 - 1;
    h.db.write(|s| {
        s.put_placing(&Placing {
            placed_ms: old,
            ..record.clone()
        })
    })?;
    h.local_file("f", NodeId::new(), b"A")?;
    let path = RelPath::new("f")?;
    h.db.write(|s| {
        let row = s.get_local(&path)?.expect("a row");
        s.put_local(&LocalRow { hash: None, ..row })
    })?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    assert_eq!(h.records()?, []);
    assert!(h.base(record.base.node)?.is_none());
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
        s.put_local(&LocalRow { hash: None, ..row })
    })?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    assert_eq!(h.records()?.len(), 1);
    assert!(h.base(node)?.is_none());
    // Hashed now.
    let hash = h.content(b"A").hash;
    h.db.write(|s| {
        let row = s.get_local(&path)?.expect("a row");
        s.put_local(&LocalRow {
            hash: Some(hash),
            ..row
        })
    })?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
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
    if !tags_kept(&h.cfg.root) {
        return Ok(()); // nothing to tag with here
    }
    let (node, at) = (NodeId::new(), some_version());
    let t = Tags::new(vec![Tag::new("t", 0)]);
    let d = h.cfg.root.join("d");
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
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.put_base(&BaseRow::new(
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
    if !tags_kept(&h.cfg.root) {
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
    fs::create_dir(h.cfg.root.join("d"))?;
    tags::write(&h.cfg.root.join("d"), &t)?;
    h.db.write(|s| {
        s.put_mirror(&MirrorRow::from_content(
            at,
            node,
            NodeId::ROOT,
            NodeKind::Folder,
            &content,
            None,
        ))?;
        s.put_base(&BaseRow::new(
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

/// Tags written but too large to sync read back as nothing to go by: that says nothing of the
/// volume, which still keeps tags. Only an unsupported volume marks the root as keeping none.
#[test]
fn tags_too_large_dont_mark_the_volume_tagless() -> R {
    let mut h = Harness::new()?;
    let large = Tags::new(vec![Tag::new("x".repeat(3000), 0)]);
    let probe = h.cfg.root.join(".large");
    fs::write(&probe, b"")?;
    if !tags_kept(&h.cfg.root) || tags::write(&probe, &large)?.is_some() {
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
    assert!(h.cfg.root.join("f").exists());
    assert_ne!(h.cfg.tags_kept.get(), Some(&false));
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
        s.put_base(&base)?;
        s.put_placing(&Placing {
            base: BaseRow::new(node, file, at),
            target: RelPath::new("f").expect("a path"),
            placed_tags: None,
            prior_tags: Some(Tags::default()),
            base_at: PlannedBase::At(planned.version),
            placed_ms: 0,
        })?;
        s.clear_dirty(node)
    })?;
    h.local_file("f", NodeId::new(), b"A")?;
    recover_placements(&mut h.db, &h.cfg.root, h.cfg.os)?;
    assert_eq!(h.base(node)?, Some(base));
    assert!(h.db.read(|s| s.is_dirty(node))?);
    Ok(())
}
