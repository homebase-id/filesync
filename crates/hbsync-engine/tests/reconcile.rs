//! One test per rule of `docs/spec/sync-semantics.md`.

use hbsync_engine::{BaseExtra, Conflict, DriveVersion, Input, Listed, Op, Options, reconcile};
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_types::{
    ContentHash, ContentState, FileContent, NodeId, NodeKind, NodeState, RECOVERED_NAME,
    RemoteFileId, RemoteRef, Tag, Tags, Uuid, VersionTag,
};

fn h(data: &str) -> ContentHash {
    hash_bytes(data.as_bytes(), HashAlgorithm::Blake3)
}

fn file_state(parent: NodeId, name: &str, hash: ContentHash, size: u64) -> NodeState {
    NodeState::file(
        parent,
        name,
        FileContent {
            hash,
            size,
            mtime_ms: 0,
            executable: false,
        },
    )
}

fn file(parent: NodeId, name: &str, data: &str) -> NodeState {
    file_state(parent, name, h(data), data.len() as u64)
}

fn folder(parent: NodeId, name: &str) -> NodeState {
    NodeState::folder(parent, name)
}

fn ids(n: usize) -> Vec<NodeId> {
    let mut v: Vec<NodeId> = (0..n).map(|_| NodeId::new()).collect();
    v.sort();
    v
}

const ROOT: NodeId = NodeId::ROOT;

#[derive(Default)]
struct World {
    input: Input,
    opts: Options,
}

impl World {
    /// A node both sides and the base agree on.
    fn synced(&mut self, node: NodeId, state: NodeState) -> &mut Self {
        let local_hash = state.content.map(|c| c.hash);
        self.input.base.insert(node, state.clone());
        self.input.local.insert(node, state.clone());
        self.input.remote.insert(node, state);
        self.input.base_extra.insert(
            node,
            BaseExtra {
                local_hash,
                content_state: ContentState::Local,
                version: None,
            },
        );
        self.input.remote_refs.insert(node, remote_ref());
        if self
            .input
            .base
            .get(node)
            .is_some_and(|s| s.kind == NodeKind::File)
        {
            self.opts.synced_files += 1;
        }
        self
    }

    fn local(&mut self, node: NodeId, state: Option<NodeState>) -> &mut Self {
        match state {
            Some(s) => self.input.local.insert(node, s),
            None => self.input.local.remove(node),
        };
        self.input.dirty.insert(node);
        self
    }

    fn remote(&mut self, node: NodeId, state: Option<NodeState>) -> &mut Self {
        match state {
            Some(s) => {
                self.input
                    .remote_refs
                    .entry(node)
                    .or_insert_with(remote_ref);
                self.input.remote.insert(node, s)
            }
            None => {
                self.input.remote_refs.remove(&node);
                self.input.remote.remove(node)
            }
        };
        self.input.dirty.insert(node);
        self
    }

    fn plan(&self) -> hbsync_engine::Plan {
        reconcile(&self.input, &self.opts)
    }

    fn version(&self, node: NodeId) -> VersionTag {
        self.input.remote_refs[&node].version
    }
}

fn remote_ref() -> RemoteRef {
    RemoteRef {
        file_id: RemoteFileId(Uuid::new_v4()),
        version: VersionTag(Uuid::new_v4()),
    }
}

fn kinds(ops: &[Op]) -> Vec<&'static str> {
    ops.iter()
        .map(|op| match op {
            Op::LocalFork { .. } => "fork",
            Op::LocalMkdir { .. } => "mkdir",
            Op::LocalMove { .. } => "move",
            Op::LocalDownload { replace: false, .. } => "download",
            Op::LocalDownload { replace: true, .. } => "download-replace",
            Op::LocalTags { .. } => "local-tags",
            Op::LocalTrash { .. } => "trash",
            Op::RemoteCreate { .. } => "create",
            Op::RemoteUpdate { upload: true, .. } => "upload",
            Op::RemoteUpdate { upload: false, .. } => "rename",
            Op::RemoteTags { .. } => "remote-tags",
            Op::RemoteDelete { .. } => "delete",
            Op::Settle { .. } => "settle",
            Op::LocalDetach { .. } => "detach",
            Op::Forget { subtree, .. } if subtree.is_empty() => "forget",
            Op::Forget { .. } => "forget+trash",
        })
        .collect()
}

#[test]
fn local_creates_go_up_parents_first() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.local(f, Some(file(d, "a.txt", "a")))
        .local(d, Some(folder(ROOT, "Docs")));
    let plan = w.plan();
    assert_eq!(plan.ops.iter().map(Op::node).collect::<Vec<_>>(), [d, f]);
    assert_eq!(kinds(&plan.ops), ["create", "create"]);
}

/// All folders first, then the files, which the executor uploads together on its pool.
#[test]
fn local_creates_make_every_folder_before_any_file() {
    let [top, a, b, f, g] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.local(a, Some(folder(ROOT, "A")))
        .local(top, Some(file(ROOT, "top.txt", "t")))
        .local(b, Some(folder(a, "B")))
        .local(f, Some(file(a, "f.txt", "f")))
        .local(g, Some(file(b, "g.txt", "g")));
    let plan = w.plan();
    assert_eq!(
        plan.ops.iter().map(Op::node).collect::<Vec<_>>(),
        [a, b, top, f, g]
    );
}

#[test]
fn remote_creates_come_down() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.remote(f, Some(file(d, "a.txt", "a")))
        .remote(d, Some(folder(ROOT, "Docs")));
    assert_eq!(kinds(&w.plan().ops), ["mkdir", "download"]);
}

#[test]
fn edits_and_moves_propagate() {
    let [a, b, c, d] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(a, file(ROOT, "a", "1"))
        .synced(b, file(ROOT, "b", "1"))
        .synced(c, file(ROOT, "c", "1"))
        .synced(d, file(ROOT, "d", "1"));
    w.local(a, Some(file(ROOT, "a", "local edit")));
    w.remote(b, Some(file(ROOT, "b", "remote edit")));
    w.local(c, Some(file(ROOT, "c-renamed", "1")));
    w.remote(d, Some(file(ROOT, "d-renamed", "1")));
    let plan = w.plan();
    assert!(plan.ops.contains(&Op::RemoteUpdate {
        node: a,
        file_id: w.input.remote_refs[&a].file_id,
        expected: w.version(a),
        parent: ROOT,
        name: "a".into(),
        upload: true,
        tags: Tags::default(),
    }));
    assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
        node: b,
        parent: ROOT,
        name: "b".into(),
        replace: true,
        tags: Tags::default(),
        drive: None,
    }));
    assert!(plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, upload: false, name, .. } if *node == c && name == "c-renamed")));
    assert!(without_drive(&plan.ops).contains(&Op::LocalMove {
        node: d,
        parent: ROOT,
        name: "d-renamed".into(),
        drive: None,
    }));
    assert!(plan.conflicts.is_empty());
}

/// A folder deleted here leaves the drive before what is in it (node schema, Recently deleted).
#[test]
fn local_folder_delete_goes_top_down() {
    let [d, f, g] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"))
        .synced(f, file(d, "f", "1"))
        .synced(g, folder(d, "g"));
    w.local(d, None).local(f, None).local(g, None);
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["delete", "delete", "delete"]);
    assert_eq!(
        plan.ops.first().map(Op::node),
        Some(d),
        "the folder goes first"
    );
}

#[test]
fn remote_folder_delete_trashes_only_the_top() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d")).synced(f, file(d, "f", "1"));
    w.remote(f, None).remote(d, None);
    let plan = w.plan();
    let mut subtree = vec![d, f];
    subtree.sort();
    assert_eq!(plan.ops, [Op::LocalTrash { node: d, subtree }]);
}

#[test]
fn edit_edit_keeps_both_versions() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = World::default();
    w.opts.device = "laptop".into();
    w.opts.date = "2026-10-08".into();
    w.synced(f, file(ROOT, "report.txt", "v1"));
    w.local(f, Some(file(ROOT, "report.txt", "local v2")));
    w.remote(f, Some(file(ROOT, "report.txt", "remote v2")));
    let plan = w.plan();
    let Some(Op::LocalFork { new_node, name, .. }) = plan.ops.first() else {
        panic!("{plan:?}")
    };
    assert_eq!(name, "report (conflict laptop 2026-10-08).txt");
    assert_eq!(kinds(&plan.ops), ["fork", "create", "download"]);
    assert!(plan.ops.contains(&Op::RemoteCreate {
        node: *new_node,
        parent: ROOT,
        name: name.clone(),
        kind: NodeKind::File,
        tags: Tags::default(),
    }));
    assert_eq!(
        plan.conflicts,
        [Conflict::EditEdit {
            node: f,
            copy: *new_node
        }]
    );
}

/// `f` edited here to `mine` against the drive's other version, planned on `computer`: the
/// conflict copy's id.
fn copy_id(f: NodeId, mine: FileContent, computer: &str) -> NodeId {
    let mut w = World::default();
    w.opts.computer = computer.into();
    w.local(f, Some(NodeState::file(ROOT, "report.txt", mine)));
    w.remote(f, Some(file(ROOT, "report.txt", "theirs")));
    let plan = w.plan();
    match plan.ops.first() {
        Some(Op::LocalFork { new_node, name, .. }) if !name.is_empty() => *new_node,
        _ => panic!("no fork: {plan:?}"),
    }
}

/// The conflict copy's id is fixed for the conflict on a computer: planned again (after a
/// crash, say), it is the same copy. Another computer makes its own copy of the same conflict,
/// with the same bytes, under another id; other bytes (the executable bit too) another copy.
#[test]
fn a_conflict_copy_id_is_the_same_on_a_replan_and_its_computers_own() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mine = FileContent {
        hash: h("mine"),
        size: 4,
        mtime_ms: 0,
        executable: false,
    };
    let laptop = copy_id(f, mine, "laptop's db id");
    assert_eq!(copy_id(f, mine, "laptop's db id"), laptop);
    assert_ne!(copy_id(f, mine, "desk's db id"), laptop);
    let executable = FileContent {
        executable: true,
        ..mine
    };
    assert_ne!(copy_id(f, executable, "laptop's db id"), laptop);
}

/// A fork whose copy has no folder yet (the file's folder here waits for its own parent: the
/// repairs take the copy out) waits: no copy, never one named "", and nothing done to the
/// node. Its conflicts (the edits, the moves) aren't reported yet: that it waits is.
#[test]
fn a_fork_whose_copy_has_to_wait_waits() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    // The local folder of the file is nowhere in the input.
    w.local(f, Some(file(d, "report.txt", "mine")));
    w.remote(f, Some(file(ROOT, "report.txt", "theirs")));
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .all(|op| op.node() != f
                && !matches!(op, Op::LocalFork { .. } | Op::RemoteCreate { .. })),
        "{plan:?}"
    );
    assert!(plan.deferred.contains(&f), "{plan:?}");
    assert_eq!(plan.conflicts, [Conflict::CopyWaits { node: f }]);
}

#[test]
fn same_edit_on_both_sides_just_settles() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(f, file(ROOT, "f", "v1"));
    w.local(f, Some(file(ROOT, "f", "v2")))
        .remote(f, Some(file(ROOT, "f", "v2")));
    let plan = w.plan();
    assert_eq!(plan.ops, [Op::Settle { node: f }]);
    assert!(plan.conflicts.is_empty());
}

#[test]
fn move_move_remote_wins() {
    let [a, b, f] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(a, folder(ROOT, "a"))
        .synced(b, folder(ROOT, "b"))
        .synced(f, file(ROOT, "f", "1"));
    w.local(f, Some(file(a, "f", "1")))
        .remote(f, Some(file(b, "f", "1")));
    let plan = w.plan();
    assert_eq!(
        plan.ops,
        [Op::LocalMove {
            node: f,
            parent: b,
            name: "f".into(),
            drive: Some(Box::new(DriveVersion {
                state: file(b, "f", "1"),
                at: w.input.remote_refs[&f],
                base_at: None,
            })),
        }]
    );
    assert_eq!(plan.conflicts, [Conflict::MoveMove { node: f }]);
}

#[test]
fn edit_beats_delete_both_ways() {
    let [x, y] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(x, file(ROOT, "x", "1"))
        .synced(y, file(ROOT, "y", "1"));
    // Local deleted x, remote edited it: restore locally.
    w.local(x, None)
        .remote(x, Some(file(ROOT, "x", "remote edit")));
    // Local edited y, remote deleted it: re-create remotely under the same node id.
    w.local(y, Some(file(ROOT, "y", "local edit")))
        .remote(y, None);
    let plan = w.plan();
    assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
        node: x,
        parent: ROOT,
        name: "x".into(),
        replace: false,
        tags: Tags::default(),
        drive: None,
    }));
    assert!(plan.ops.contains(&Op::RemoteCreate {
        node: y,
        parent: ROOT,
        name: "y".into(),
        kind: NodeKind::File,
        tags: Tags::default(),
    }));
    assert!(!plan.ops.iter().any(Op::is_delete));
}

#[test]
fn live_child_resurrects_a_deleted_folder() {
    let [d, old, new] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"))
        .synced(old, file(d, "old", "1"));
    // Locally the folder was deleted; remotely a file was added to it meanwhile.
    w.local(d, None).local(old, None);
    w.remote(new, Some(file(d, "new", "2")));
    let plan = w.plan();
    assert!(without_drive(&plan.ops).contains(&Op::LocalMkdir {
        node: d,
        parent: ROOT,
        name: "d".into(),
        tags: Tags::default(),
        drive: None,
    }));
    assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
        node: new,
        parent: d,
        name: "new".into(),
        replace: false,
        tags: Tags::default(),
        drive: None,
    }));
    // The other child's delete still goes through.
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { node, .. } if *node == old))
    );
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { node, .. } if *node == d))
    );
}

#[test]
fn remote_deleted_folder_with_new_local_child_is_recreated() {
    let [d, new] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"));
    w.remote(d, None);
    w.local(new, Some(file(d, "new", "2")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["create", "create"]);
    assert_eq!(plan.ops.iter().map(Op::node).collect::<Vec<_>>(), [d, new]);
}

#[test]
fn crossing_moves_are_not_a_cycle() {
    let [a, b] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(a, folder(ROOT, "a")).synced(b, folder(ROOT, "b"));
    w.local(a, Some(folder(b, "a")))
        .remote(b, Some(folder(a, "b")));
    let plan = w.plan();
    // The local move (a into b) is undone; the remote move (b into a) is applied.
    assert!(without_drive(&plan.ops).contains(&Op::LocalMove {
        node: a,
        parent: ROOT,
        name: "a".into(),
        drive: None,
    }));
    assert!(without_drive(&plan.ops).contains(&Op::LocalMove {
        node: b,
        parent: a,
        name: "b".into(),
        drive: None,
    }));
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { .. }))
    );
    assert_eq!(plan.conflicts, [Conflict::Cycle { node: a }]);
}

#[test]
fn name_clash_lower_id_keeps_the_name() {
    let [low, high] = ids(2)[..] else {
        unreachable!()
    };
    for (local_node, remote_node) in [(low, high), (high, low)] {
        let mut w = World::default();
        w.local(local_node, Some(file(ROOT, "x.txt", "local")));
        w.remote(remote_node, Some(file(ROOT, "X.TXT", "remote")));
        let plan = w.plan();
        assert_eq!(plan.conflicts, [Conflict::NameClash { node: high }]);
        if high == remote_node {
            assert!(plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, name, .. } if *node == high && name == "X (conflict 1).TXT")));
            assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
                node: high,
                parent: ROOT,
                name: "X (conflict 1).TXT".into(),
                replace: false,
                tags: Tags::default(),
                drive: None,
            }));
        } else {
            assert!(plan.ops.contains(&Op::RemoteCreate {
                node: high,
                parent: ROOT,
                name: "x (conflict 1).txt".into(),
                kind: NodeKind::File,
                tags: Tags::default(),
            }));
            assert!(without_drive(&plan.ops).contains(&Op::LocalMove {
                node: high,
                parent: ROOT,
                name: "x (conflict 1).txt".into(),
                drive: None,
            }));
        }
    }
}

/// A plan from part of a folder's children ([`Input::listed`]) names the folders it judged
/// without all it needs: a clash (a numbered name must be free among all of them), a name
/// whose clashes weren't read (an orphan moved to the root).
#[test]
fn names_judged_in_part_of_a_folder_ask_for_all_of_it() {
    let [low, high, orphan, gone] = ids(4)[..] else {
        unreachable!()
    };
    let docs = NodeId::new();
    let named = |names: &[&str]| Listed {
        named: [(
            docs,
            names
                .iter()
                .map(|n| hbsync_namemap::sibling_hash(n))
                .collect(),
        )]
        .into(),
        ..Listed::default()
    };
    // No clash among the names read: nothing more needed.
    let mut w = World::default();
    w.synced(docs, folder(ROOT, "Docs"));
    w.local(low, Some(file(docs, "x.txt", "local")));
    w.input.listed = Some(named(&["x.txt"]));
    assert!(w.plan().unlisted.is_empty());
    // A clash: the loser's numbered name is judged against the whole folder.
    w.remote(high, Some(file(docs, "X.TXT", "remote")));
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::NameClash { node: high }]);
    assert_eq!(plan.unlisted, [docs].into());
    // Listed whole: the plan stands.
    w.input.listed = Some(Listed {
        full: [docs].into(),
        ..Listed::default()
    });
    assert!(w.plan().unlisted.is_empty());
    // An orphan the server kept goes into Recovered files, made at the root: neither was read.
    let mut w = World::default();
    w.remote(orphan, Some(file(gone, "kept.txt", "data")));
    w.input.remote_complete = true;
    w.input.listed = Some(Listed::default());
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: orphan }]);
    assert_eq!(plan.unlisted, [ROOT, NodeId::RECOVERED].into());
}

#[test]
fn mass_delete_is_held() {
    let nodes = ids(80);
    let mut w = World::default();
    for (i, n) in nodes.iter().enumerate() {
        w.synced(*n, file(ROOT, &format!("f{i}"), "x"));
    }
    let keep = NodeId::new();
    w.local(keep, Some(file(ROOT, "new", "n")));
    for n in &nodes {
        w.local(*n, None);
    }
    let plan = w.plan();
    assert_eq!(plan.held.len(), 80);
    assert_eq!(
        kinds(&plan.ops),
        ["create"],
        "the rest of the plan proceeds"
    );

    w.opts.allow_mass_delete = true;
    assert_eq!(w.plan().ops.iter().filter(|op| op.is_delete()).count(), 80);
}

#[test]
fn online_only_files_are_not_deleted_or_downloaded() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(f, file(ROOT, "big.iso", "1"));
    if let Some(e) = w.input.base_extra.get_mut(&f) {
        e.content_state = ContentState::OnlineOnly;
        e.local_hash = None;
    }
    w.local(f, None);
    assert!(w.plan().ops.is_empty(), "no local bytes is not a delete");
    w.remote(f, Some(file(ROOT, "big.iso", "2")));
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { .. })),
        "{plan:?}"
    );
}

#[test]
fn unhashed_and_unchanged_nodes() {
    let [busy, quiet] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(quiet, file(ROOT, "q", "1"));
    w.input.dirty.insert(quiet);
    w.local(busy, Some(file(ROOT, "being-written", "?")));
    w.input.local_unhashed.insert(busy);
    let plan = w.plan();
    assert!(plan.ops.is_empty());
    assert_eq!(plan.deferred, [busy]);
    assert_eq!(plan.clean, [quiet]);
}

#[test]
fn sha256_remote_node_moved_locally_is_not_re_uploaded() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = World::default();
    let sha = file_state(ROOT, "f", hash_bytes(b"1", HashAlgorithm::Sha256), 1);
    let local = file(ROOT, "f", "1");
    w.input.base.insert(f, sha.clone());
    w.input.remote.insert(f, sha);
    w.input.local.insert(f, local.clone());
    w.input.base_extra.insert(
        f,
        BaseExtra {
            local_hash: local.content.map(|c| c.hash),
            content_state: ContentState::Local,
            version: None,
        },
    );
    w.input.remote_refs.insert(f, remote_ref());
    w.local(f, Some(file(ROOT, "f-renamed", "1")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["rename"], "{plan:?}");
}

#[test]
fn file_moved_into_remotely_deleted_folder_keeps_the_folder() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    for (folder_id, file_id) in [(d, f), (f, d)] {
        let mut w = World::default();
        w.synced(file_id, file(ROOT, "c.txt", "2"))
            .synced(folder_id, folder(ROOT, "c.txt"));
        // Locally the file moved into the folder; remotely the folder was deleted.
        w.local(file_id, Some(file(folder_id, "a", "2")));
        w.remote(folder_id, None);
        let plan = w.plan();
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, Op::LocalTrash { node, .. } if *node == folder_id)),
            "{plan:#?}"
        );
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == folder_id)),
            "{plan:#?}"
        );
    }
}

#[test]
fn orphan_with_unknown_parent_is_recovered_when_mirror_is_complete() {
    let [gone, child] = ids(2)[..] else {
        unreachable!()
    };
    let recovered = NodeId::RECOVERED;
    let mut w = World::default();
    w.remote(child, Some(folder(gone, "kept")));
    assert_eq!(w.plan().deferred, [child], "incomplete mirror: wait");
    w.input.remote_complete = true;
    let plan = w.plan();
    // Recovered files is made on both sides, before the orphan moves into it.
    let ops = without_drive(&plan.ops);
    let at = |want: &Op| ops.iter().position(|op| op == want).expect("{want:?}");
    let made = at(&Op::RemoteCreate {
        node: recovered,
        parent: ROOT,
        name: RECOVERED_NAME.into(),
        kind: NodeKind::Folder,
        tags: Tags::default(),
    });
    at(&Op::LocalMkdir {
        node: recovered,
        parent: ROOT,
        name: RECOVERED_NAME.into(),
        tags: Tags::default(),
        drive: None,
    });
    let moved = ops
        .iter()
        .position(|op| matches!(op, Op::RemoteUpdate { node, parent, name, .. } if *node == child && *parent == recovered && name == "kept"))
        .expect("moved on the drive");
    assert!(made < moved);
    at(&Op::LocalMkdir {
        node: child,
        parent: recovered,
        name: "kept".into(),
        tags: Tags::default(),
        drive: None,
    });
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: child }]);

    // Recovered files there already, live (renamed): the orphan joins it, under a free name
    // where it clashes with what is there (a higher id, which keeps its name all the same).
    let other = NodeId::new();
    w.synced(recovered, folder(ROOT, "Renamed by the user"));
    w.synced(other, folder(recovered, "kept"));
    let plan = w.plan();
    assert!(
        plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, name, .. } if *node == child && *parent == recovered && name == "kept (conflict 1)")),
        "{:#?}", plan.ops
    );
    assert!(!plan.ops.iter().any(|op| op.node() == recovered));
}

/// Recovered files in Recently deleted comes back (alone) for a new orphan, where it was; one
/// this device filters takes none: the orphan goes to the top instead.
#[test]
fn a_deleted_or_filtered_recovered_files_and_a_new_orphan() {
    let [gone, child] = ids(2)[..] else {
        unreachable!()
    };
    let recovered = NodeId::RECOVERED;
    let mut w = World::default();
    w.input.remote_complete = true;
    w.remote(child, Some(file(gone, "kept.txt", "data")));
    w.input.removed.insert(
        recovered,
        hbsync_engine::reconcile::Removed {
            at: 1,
            state: folder(ROOT, "Found"),
        },
    );
    let plan = w.plan();
    assert!(
        without_drive(&plan.ops).contains(&Op::RemoteCreate {
            node: recovered,
            parent: ROOT,
            name: "Found".into(),
            kind: NodeKind::Folder,
            tags: Tags::default(),
        }),
        "{:#?}",
        plan.ops
    );
    assert!(plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. } if *node == child && *parent == recovered)));

    let mut w = World {
        opts: ignoring(&format!("/{RECOVERED_NAME}/\n")),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.remote(child, Some(file(gone, "kept.txt", "data")));
    let plan = w.plan();
    assert!(plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. } if *node == child && *parent == ROOT)));
    assert!(
        !plan.ops.iter().any(|op| op.node() == recovered),
        "{:#?}",
        plan.ops
    );
}

#[test]
fn a_file_still_being_written_keeps_its_remotely_deleted_folder() {
    let [d, new] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"));
    w.remote(d, None);
    // A new local file the scan couldn't hash yet (locked, still being written).
    w.local(
        new,
        Some(NodeState {
            content: None,
            ..file(d, "new", "?")
        }),
    );
    w.input.local_unhashed.insert(new);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalTrash { .. })),
        "{plan:#?}"
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == d)),
        "{plan:#?}"
    );
    assert_eq!(plan.deferred, [new]);
}

#[test]
fn a_file_still_being_written_keeps_its_name() {
    let [first, writing] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.remote(first, Some(file(ROOT, "Report.txt", "theirs")));
    w.local(
        writing,
        Some(NodeState {
            content: None,
            ..file(ROOT, "report.txt", "?")
        }),
    );
    w.input.local_unhashed.insert(writing);
    let plan = w.plan();
    // The remote file comes down under another name; the local one isn't touched.
    assert!(plan.ops.iter().all(|op| op.node() != writing), "{plan:#?}");
    assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
        node: first,
        parent: ROOT,
        name: "Report (conflict 1).txt".into(),
        replace: false,
        tags: Tags::default(),
        drive: None,
    }));
}

#[test]
fn children_of_a_withdrawn_orphan_are_withdrawn_too() {
    // Ids ascend: the child is looked at before its parent.
    let [gone, parent, child] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.remote(parent, Some(folder(gone, "p")));
    w.remote(child, Some(file(parent, "c", "1")));
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{plan:#?}");
    let mut deferred = plan.deferred.clone();
    deferred.sort();
    assert_eq!(deferred, [parent, child]);
}

// ---- §10 filters ----

fn ignoring(rules: &str) -> Options {
    Options {
        filter: std::sync::Arc::new(
            hbsync_local::Filter::from_rules(rules, false).expect("valid rules"),
        ),
        ..Options::default()
    }
}

#[test]
fn filtered_remote_nodes_are_never_downloaded() {
    let [junk, b, inside, log] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("build/\n*.log\n"),
        ..World::default()
    };
    w.remote(junk, Some(file(ROOT, ".DS_Store", "x")))
        .remote(b, Some(folder(ROOT, "build")))
        .remote(inside, Some(file(b, "out.o", "o")))
        .remote(log, Some(file(ROOT, "run.log", "l")));
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{plan:?}");
    assert_eq!(plan.clean.len(), 4);
}

#[test]
fn a_remote_move_into_an_ignored_folder_takes_the_file_away_here() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let mut w = World {
        opts: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(f, file(ROOT, "a.txt", "v1"));
    w.remote(b, Some(folder(ROOT, "build")))
        .remote(f, Some(file(b, "a.txt", "v1")));
    // The copy here goes to the trash and the base with it, in one step: the remote is never
    // touched.
    let plan = w.plan();
    assert_eq!(
        plan.ops,
        [Op::Forget {
            node: f,
            subtree: vec![f]
        }],
        "{plan:?}"
    );
    // Gone here already: only the base goes.
    w.local(f, None);
    assert_eq!(
        w.plan().ops,
        [Op::Forget {
            node: f,
            subtree: vec![]
        }]
    );
}

#[test]
fn a_local_edit_of_a_file_moved_into_an_ignored_folder_stays_as_a_new_file() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let mut w = World {
        opts: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(f, file(ROOT, "a.txt", "v1"));
    w.local(f, Some(file(ROOT, "a.txt", "local v2")));
    w.remote(b, Some(folder(ROOT, "build")))
        .remote(f, Some(file(b, "a.txt", "v1")));
    let plan = w.plan();
    let Some(&Op::LocalDetach { node, new_node }) = plan.ops.first() else {
        panic!("{plan:?}")
    };
    assert_eq!(node, f);
    // The detach drops the old base itself.
    assert_eq!(kinds(&plan.ops), ["detach", "create"], "{plan:?}");
    assert!(plan.ops.contains(&Op::RemoteCreate {
        node: new_node,
        parent: ROOT,
        name: "a.txt".into(),
        kind: NodeKind::File,
        tags: Tags::default(),
    }));
}

/// A detached item's new id is its computer's: the same on a replan, another on another
/// computer, even one with the same device name.
#[test]
fn a_detached_items_id_is_its_computers() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let detached = |computer: &str| {
        let mut w = World {
            opts: ignoring("build/\n"),
            ..World::default()
        };
        w.opts.device = "laptop".into();
        w.opts.computer = computer.into();
        w.synced(f, file(ROOT, "a.txt", "v1"));
        w.local(f, Some(file(ROOT, "a.txt", "local v2")));
        w.remote(b, Some(folder(ROOT, "build")))
            .remote(f, Some(file(b, "a.txt", "v1")));
        let plan = w.plan();
        match plan.ops.first() {
            Some(&Op::LocalDetach { new_node, .. }) => new_node,
            _ => panic!("{plan:?}"),
        }
    };
    let one = detached("one computer's db id");
    assert_eq!(detached("one computer's db id"), one);
    assert_ne!(detached("another computer's db id"), one);
}

#[test]
fn a_folder_moved_into_an_ignored_folder_goes_whole_unless_it_holds_news() {
    let [b, d, f, n] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(d, folder(ROOT, "proj"))
        .synced(f, file(d, "f.txt", "v1"));
    w.remote(b, Some(folder(ROOT, "build")))
        .remote(d, Some(folder(b, "proj")));
    let plan = w.plan();
    assert_eq!(
        plan.ops,
        [Op::Forget {
            node: d,
            subtree: {
                let mut v = vec![d, f];
                v.sort();
                v
            },
        }],
        "{plan:?}"
    );

    // A file created here meanwhile keeps the folder, as a new one; the rest goes.
    w.local(n, Some(file(d, "new.txt", "n")));
    let plan = w.plan();
    assert_eq!(
        kinds(&plan.ops),
        ["detach", "create", "create", "forget+trash"],
        "{plan:?}"
    );
    let Some(&Op::LocalDetach { node, new_node }) = plan.ops.first() else {
        panic!("{plan:?}")
    };
    assert_eq!(node, d);
    assert!(plan.ops.contains(&Op::RemoteCreate {
        node: n,
        parent: new_node,
        name: "new.txt".into(),
        kind: NodeKind::File,
        tags: Tags::default(),
    }));
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::Forget { node, .. } if *node == f))
    );
}

#[test]
fn a_folder_with_children_this_device_ignores_survives_a_local_delete() {
    let [d, f, log] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("*.log\n"),
        ..World::default()
    };
    w.synced(d, folder(ROOT, "proj"))
        .synced(f, file(d, "f.txt", "v1"));
    w.remote(log, Some(file(d, "run.log", "l")));
    w.local(d, None).local(f, None);
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["mkdir", "delete"], "{plan:?}");
    assert!(matches!(plan.ops[1], Op::RemoteDelete { node, .. } if node == f));
}

#[test]
fn what_a_new_rule_hides_leaves_the_drive_unless_another_device_added_to_it() {
    let [d, f, g] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(d, folder(ROOT, "build"))
        .synced(f, file(d, "out.o", "o"));
    // The scanner no longer reports them: kept here only, so they leave the drive.
    w.local(d, None).local(f, None);
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["delete", "delete"], "{plan:?}");
    assert_eq!(plan.ops[0].node(), d);

    // Another device added to the folder meanwhile: that stays, and so does the folder; only
    // what this device synced leaves, and the folder's base goes.
    w.remote(g, Some(file(d, "new.o", "n")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["delete", "forget"], "{plan:?}");
    assert_eq!(plan.ops[0].node(), f);
    assert_eq!(plan.ops[1].node(), d);
}

#[test]
fn a_folder_deleted_here_stays_deleted_when_what_it_held_leaves_the_drive() {
    let [p, b, f] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(p, folder(ROOT, "proj"))
        .synced(b, folder(p, "build"))
        .synced(f, file(b, "out.o", "o"));
    // The user adds the rule and deletes the project: what the rule hides leaves the drive, so
    // it doesn't keep the project alive.
    w.local(p, None).local(b, None).local(f, None);
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["delete", "delete", "delete"], "{plan:?}");
    assert_eq!(plan.ops[0].node(), p);
}

#[test]
fn a_file_brought_back_into_view_keeps_its_place_over_a_remote_move() {
    let [logs, f] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World {
        opts: ignoring("*.o\n"),
        ..World::default()
    };
    w.synced(logs, folder(ROOT, "logs"))
        .synced(f, file(ROOT, "out.o", "o"));
    // A new rule hides it, so the user renames it to keep it synced, while another device
    // moves it to another place the rule hides: following that would take it off the drive.
    w.local(f, Some(file(ROOT, "out.bin", "o")))
        .remote(f, Some(file(logs, "out.o", "o")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["rename"], "{plan:?}");
    assert!(plan.conflicts.is_empty(), "{plan:?}");
}

/// The plan's ops without the drive version their local writes carry (compared by value).
fn without_drive(ops: &[Op]) -> Vec<Op> {
    ops.iter()
        .cloned()
        .map(|mut op| {
            if let Op::LocalMkdir { drive, .. }
            | Op::LocalMove { drive, .. }
            | Op::LocalDownload { drive, .. }
            | Op::LocalTags { drive, .. } = &mut op
            {
                *drive = None;
            }
            op
        })
        .collect()
}

/// A local write carries the drive's version the plan saw (its base settles against that
/// version, not a later one): a folder made, a file fetched, a move and a tag change.
#[test]
fn local_writes_carry_the_drive_version_the_plan_saw() {
    let [d, f, moved, tagged] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.remote(d, Some(folder(ROOT, "d")))
        .remote(f, Some(file(d, "f", "1")));
    w.synced(moved, file(ROOT, "m", "2"))
        .remote(moved, Some(file(d, "m", "2")));
    let red = NodeState {
        tags: Tags::new(vec![Tag::new("Red", 6)]),
        ..file(ROOT, "t", "3")
    };
    w.synced(tagged, file(ROOT, "t", "3"))
        .remote(tagged, Some(red.clone()));
    let plan = w.plan();
    let drive = |node| {
        plan.ops
            .iter()
            .find(|op| op.node() == node)
            .and_then(Op::drive)
    };
    for (node, op) in [
        (d, "mkdir"),
        (f, "download"),
        (moved, "move"),
        (tagged, "local-tags"),
    ] {
        let seen = DriveVersion {
            state: w.input.remote.get(node).cloned().expect("on the drive"),
            at: w.input.remote_refs[&node],
            base_at: None,
        };
        assert_eq!(drive(node), Some(&seen), "{op} carries what the plan saw");
    }
    assert_eq!(drive(tagged).map(|d| &d.state), Some(&red));
}

/// A fork whose copy has to wait makes its node wait before the names are judged: the node
/// keeps the name it holds on the drive, and a new file wanting that name yields to it, the
/// same whichever id is lower. (Judged as if the node went ahead, it lost to a lower id: the
/// plan renamed it, but it waited, and the other file went up under the name it still held.)
#[test]
fn a_waiting_fork_keeps_its_name_and_a_new_file_yields() {
    let [d, a, b] = ids(3)[..] else {
        unreachable!()
    };
    for (f, x) in [(a, b), (b, a)] {
        for x_on_drive in [false, true] {
            let mut w = World::default();
            w.synced(f, file(ROOT, "a.txt", "v1"));
            // Here: edited, in a folder that waits for its own; on the drive: edited, moved.
            w.local(f, Some(file(d, "a.txt", "mine")));
            w.remote(f, Some(file(ROOT, "x.txt", "theirs")));
            let other = Some(file(ROOT, "x.txt", "other"));
            if x_on_drive {
                w.remote(x, other);
            } else {
                w.local(x, other);
            }
            let plan = w.plan();
            assert!(plan.ops.iter().all(|op| op.node() != f), "{plan:?}");
            assert!(plan.deferred.contains(&f), "{plan:?}");
            let mut conflicts = plan.conflicts.clone();
            conflicts.sort_by_key(|c| c.node() == x);
            assert_eq!(
                conflicts,
                [
                    Conflict::CopyWaits { node: f },
                    Conflict::NameClash { node: x }
                ]
            );
            let renamed = |op: &Op| match op {
                Op::RemoteCreate { node, name, .. } | Op::RemoteUpdate { node, name, .. } => {
                    *node == x && name == "x (conflict 1).txt"
                }
                _ => false,
            };
            assert!(plan.ops.iter().any(renamed), "{plan:?}");
        }
    }
}
