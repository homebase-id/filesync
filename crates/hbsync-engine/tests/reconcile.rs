//! One test per rule of `docs/spec/sync-semantics.md`.

use hbsync_engine::scope::Scope;
use hbsync_engine::{
    BaseExtra, Conflict, Coverage, DriveVersion, Input, Masking, Op, Options, reconcile,
};
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
    /// This device's ignore rules (§10).
    filter: hbsync_local::Filter,
    /// The folder is read-only (§13).
    read_only: bool,
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
            self.opts.guarding.synced += 1;
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
        let scope = Scope::main();
        let masking = Masking {
            filter: &self.filter,
            scope: &scope,
            read_only: self.read_only,
        };
        reconcile(&self.input, &self.opts, masking)
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
            Op::LocalDelete { .. } => "delete",
            Op::RemoteCreate { .. } => "create",
            Op::RemoteUpdate { upload: true, .. } => "upload",
            Op::RemoteUpdate { upload: false, .. } => "rename",
            Op::RemoteTags { .. } => "remote-tags",
            Op::RemoteDelete { .. } => "delete",
            Op::Settle { .. } => "settle",
            Op::LocalDetach { .. } => "detach",
            Op::LocalEvict { .. } => "evict",
            Op::Forget { subtree, .. } if subtree.is_empty() => "forget",
            Op::Forget { .. } => "forget+delete",
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
fn remote_folder_delete_deletes_only_the_top() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d")).synced(f, file(d, "f", "1"));
    w.remote(f, None).remote(d, None);
    let plan = w.plan();
    let mut subtree = vec![d, f];
    subtree.sort();
    assert_eq!(plan.ops, [Op::LocalDelete { node: d, subtree }]);
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

/// A cycle with no local move to undo (§6.2): two devices each moved one folder into the
/// other, and the drive took both (its mirror holds `a` in `b` and `b` in `a`). The lowest id
/// is undone: its drive place would close the cycle again, so it goes to the top, under its
/// name; the other stays in it. Told as a cycle. (The scan of 2026-10-23, G9.)
#[test]
fn a_cycle_with_no_local_move_puts_the_lowest_at_the_top() {
    let [a, b] = ids(2)[..] else { unreachable!() };
    let (low, high) = (a.min(b), a.max(b));
    let mut w = World::default();
    w.synced(a, folder(ROOT, "a")).synced(b, folder(ROOT, "b"));
    w.remote(a, Some(folder(b, "a")))
        .remote(b, Some(folder(a, "b")));
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::Cycle { node: low }], "{plan:?}");
    let name = |n: NodeId| if n == a { "a" } else { "b" };
    // The drive gets the lowest back at the top; here, the other goes into it.
    assert!(
        plan.ops.iter().any(|op| matches!(op,
            Op::RemoteUpdate { node, parent, name: n, .. }
                if *node == low && *parent == ROOT && n == name(low))),
        "{:?}",
        plan.ops
    );
    assert!(without_drive(&plan.ops).contains(&Op::LocalMove {
        node: high,
        parent: low,
        name: name(high).into(),
        drive: None,
    }));
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

/// A plan from part of a folder's children ([`Input::coverage`]) names the folders it judged
/// without all it needs: a clash (a numbered name must be free among all of them), a name
/// whose clashes weren't read (an orphan moved to the root).
#[test]
fn names_judged_in_part_of_a_folder_ask_for_all_of_it() {
    let [low, high, orphan, gone] = ids(4)[..] else {
        unreachable!()
    };
    let docs = NodeId::new();
    let named = |names: &[&str]| Coverage {
        named: [(
            docs,
            names
                .iter()
                .map(|n| hbsync_namemap::sibling_hash(n))
                .collect(),
        )]
        .into(),
        ..Coverage::default()
    };
    // No clash among the names read: nothing more needed.
    let mut w = World::default();
    w.synced(docs, folder(ROOT, "Docs"));
    w.local(low, Some(file(docs, "x.txt", "local")));
    w.input.coverage = Some(named(&["x.txt"]));
    assert!(w.plan().uncovered.is_empty());
    // A clash: the loser's numbered name is judged against the whole folder.
    w.remote(high, Some(file(docs, "X.TXT", "remote")));
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::NameClash { node: high }]);
    assert_eq!(plan.uncovered, [docs].into());
    // Covered whole: the plan stands.
    w.input.coverage = Some(Coverage {
        full: [docs].into(),
        ..Coverage::default()
    });
    assert!(w.plan().uncovered.is_empty());
    // An orphan the server kept goes into Recovered files, made at the root: neither was read.
    let mut w = World::default();
    w.remote(orphan, Some(file(gone, "kept.txt", "data")));
    w.input.remote_complete = true;
    w.input.coverage = Some(Coverage::default());
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: orphan }]);
    assert_eq!(plan.uncovered, [ROOT, NodeId::RECOVERED].into());
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

    w.opts.guarding.settings.allow = true;
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
                .any(|op| matches!(op, Op::LocalDelete { node, .. } if *node == folder_id)),
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

/// Another writer moved a node into a file (§6.3): it is repaired as an orphan is, into
/// Recovered files on the drive, whatever the mirror's completeness.
#[test]
fn a_node_whose_folder_is_a_file_is_recovered() {
    let [f, child] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(f, file(ROOT, "f", "x"));
    w.synced(child, folder(ROOT, "d"));
    w.remote(child, Some(folder(f, "d")));
    let plan = w.plan();
    assert!(
        plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. } if *node == child && *parent == NodeId::RECOVERED)),
        "{:#?}",
        plan.ops
    );
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: child }]);
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
    let at = |want: &Op| {
        ops.iter()
            .position(|op| op == want)
            .unwrap_or_else(|| panic!("{want:?}"))
    };
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

/// Recovered files made beside a plain folder of its name, whose id sorts above its own: the
/// plain folder keeps the name all the same.
#[test]
fn recovered_files_loses_a_name_clash_at_the_top() {
    let [gone, child] = ids(2)[..] else {
        unreachable!()
    };
    let plain = NodeId(Uuid::from_u128(u128::MAX));
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(plain, folder(ROOT, RECOVERED_NAME));
    w.remote(child, Some(file(gone, "kept.txt", "data")));
    let plan = w.plan();
    assert!(
        plan.ops.iter().any(|op| matches!(op, Op::RemoteCreate { node, name, .. } if *node == NodeId::RECOVERED && *name == format!("{RECOVERED_NAME} (conflict 1)"))),
        "{:#?}",
        plan.ops
    );
    assert!(
        !plan.ops.iter().any(|op| op.node() == plain),
        "{:#?}",
        plan.ops
    );
}

/// The user moved Recovered files into o, whose folder is then deleted for good: o, the
/// orphan, goes to the top, never into what it holds.
#[test]
fn an_orphan_holding_recovered_files_goes_to_the_top() {
    let [gone, o] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.remote(o, Some(folder(gone, "o")));
    w.remote(NodeId::RECOVERED, Some(folder(o, RECOVERED_NAME)));
    let plan = w.plan();
    assert!(
        plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. } if *node == o && *parent == ROOT)),
        "{:#?}",
        plan.ops
    );
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: o }]);
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
        filter: ignoring(&format!("/{RECOVERED_NAME}/\n")),
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

/// A file being written in a folder the drive deleted: the folder's delete waits for its hash,
/// neither done here nor undone on the drive (§6.3); once hashed, the new file keeps the
/// folder alive and it comes back.
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
            .any(|op| matches!(op, Op::LocalDelete { .. })),
        "{plan:#?}"
    );
    assert!(plan.ops.is_empty(), "the delete waits: {plan:#?}");
    let mut deferred = plan.deferred.clone();
    deferred.sort();
    assert_eq!(deferred, [d, new]);
    // Hashed.
    w.input.local_unhashed.remove(&new);
    w.local(new, Some(file(d, "new", "!")));
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == d)),
        "{plan:#?}"
    );
}

#[test]
fn a_file_still_being_written_keeps_its_name() {
    let [first, writing] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.remote(first, Some(file(ROOT, "Report.txt", "theirs")));
    w.writing(writing, "report.txt");
    let plan = w.plan();
    // The remote file waits for the name; the local one isn't touched (§6.3).
    assert!(plan.ops.is_empty(), "{plan:#?}");
    assert!(plan.deferred.contains(&first), "{plan:#?}");
    // Hashed: a name clash as any, both kept (the lower id keeps the name).
    w.input.local_unhashed.remove(&writing);
    w.local(writing, Some(file(ROOT, "report.txt", "mine")));
    let plan = w.plan();
    assert!(without_drive(&plan.ops).contains(&Op::LocalDownload {
        node: first,
        parent: ROOT,
        name: "Report.txt".into(),
        replace: false,
        tags: Tags::default(),
        drive: None,
    }));
    assert!(
        plan.conflicts
            .contains(&Conflict::NameClash { node: writing }),
        "{plan:#?}"
    );
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

fn ignoring(rules: &str) -> hbsync_local::Filter {
    hbsync_local::Filter::from_rules(rules, false).expect("valid rules")
}

#[test]
fn filtered_remote_nodes_are_never_downloaded() {
    let [junk, b, inside, log] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("build/\n*.log\n"),
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
        filter: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(f, file(ROOT, "a.txt", "v1"));
    w.remote(b, Some(folder(ROOT, "build")))
        .remote(f, Some(file(b, "a.txt", "v1")));
    // The copy here is deleted and the base with it, in one step: the remote is never touched.
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
        filter: ignoring("build/\n"),
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
            filter: ignoring("build/\n"),
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
        filter: ignoring("build/\n"),
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
        ["detach", "create", "create", "forget+delete"],
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

impl World {
    /// This device ignoring `build/`.
    fn ignoring_build() -> World {
        World {
            filter: ignoring("build/\n"),
            ..World::default()
        }
    }

    /// The drive moved folder `d` (`name`) into `build` (`b`), at the top.
    fn moved_into_build(&mut self, b: NodeId, d: NodeId, name: &str) -> &mut Self {
        self.remote(b, Some(folder(ROOT, "build")))
            .remote(d, Some(folder(b, name)))
    }

    /// A file `name` at the top here still being written: not hashed yet (§6.3).
    fn writing(&mut self, node: NodeId, name: &str) -> &mut Self {
        self.local(
            node,
            Some(NodeState {
                content: None,
                ..file(ROOT, name, "?")
            }),
        );
        self.input.local_unhashed.insert(node);
        self
    }
}

/// The items a plan detaches (§10).
fn detached(plan: &hbsync_engine::Plan) -> Vec<NodeId> {
    plan.ops
        .iter()
        .filter_map(|op| match op {
            Op::LocalDetach { node, .. } => Some(*node),
            _ => None,
        })
        .collect()
}

/// Whether a plan moves `node`'s item here to the top: a move, or a download of the drive's
/// version there.
fn moves_to_top(plan: &hbsync_engine::Plan, node: NodeId) -> bool {
    plan.ops.iter().any(|op| match op {
        Op::LocalMove {
            node: n, parent, ..
        }
        | Op::LocalDownload {
            node: n, parent, ..
        } => *n == node && *parent == ROOT,
        _ => false,
    })
}

/// The forgets and deletes of a plan, by node, with what each expects to take here.
fn deletes_here(plan: &hbsync_engine::Plan) -> Vec<(&'static str, NodeId, Vec<NodeId>)> {
    plan.ops
        .iter()
        .filter_map(|op| match op {
            Op::Forget { node, subtree } => Some(("forget", *node, subtree.clone())),
            Op::LocalDelete { node, subtree } => Some(("delete", *node, subtree.clone())),
            _ => None,
        })
        .collect()
}

/// Another device moved `f.txt` out of `proj` to the top, then `proj` into `build/`, which this
/// device ignores: the file moves out here, unchanged, edited on the drive, edited here (not
/// forked: the drive's bytes are as synced) or renamed in `proj` (a move/move: the drive's place
/// wins). The folder goes whole, never detached and brought back empty (#356); its forget
/// expects only what stays in it till then.
#[test]
fn a_folder_moved_into_an_ignored_folder_after_its_file_moved_out_goes_whole() {
    let cases = [
        ("unchanged", ("f.txt", "v1"), "v1", "move"),
        (
            "edited on the drive",
            ("f.txt", "v1"),
            "v2",
            "download-replace",
        ),
        ("edited here", ("f.txt", "mine"), "v1", "move"),
        ("renamed here", ("renamed.txt", "v1"), "v1", "move"),
    ];
    for (case, (here, bytes), drive, moving) in cases {
        let [b, d, f, g] = ids(4)[..] else {
            unreachable!()
        };
        let mut w = World::ignoring_build();
        w.synced(d, folder(ROOT, "proj"))
            .synced(f, file(d, "f.txt", "v1"))
            .synced(g, file(d, "g.txt", "g"));
        w.local(f, Some(file(d, here, bytes)));
        w.moved_into_build(b, d, "proj")
            .remote(f, Some(file(ROOT, "f.txt", drive)));
        let plan = w.plan();
        assert!(detached(&plan).is_empty(), "{case}: {plan:?}");
        assert!(moves_to_top(&plan, f), "{case}: {plan:?}");
        assert!(kinds(&plan.ops).contains(&moving), "{case}: {plan:?}");
        assert_eq!(deletes_here(&plan), [("forget", d, vec![d, g])], "{case}");
    }
}

/// As above, but the file was edited on both sides: its conflict copy stays where the file is,
/// so the folder is kept for it, detached.
#[test]
fn a_folder_moved_into_an_ignored_folder_keeps_a_conflict_copy_of_what_moved_out() {
    let [b, d, f] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::ignoring_build();
    w.synced(d, folder(ROOT, "proj"))
        .synced(f, file(d, "f.txt", "v1"));
    w.local(f, Some(file(d, "f.txt", "mine")));
    w.moved_into_build(b, d, "proj")
        .remote(f, Some(file(ROOT, "f.txt", "theirs")));
    let plan = w.plan();
    let Some(&Op::LocalDetach { node, new_node }) = plan.ops.first() else {
        panic!("{plan:?}")
    };
    assert_eq!(node, d);
    assert!(
        plan.ops.iter().any(|op| matches!(
            op,
            Op::RemoteCreate { node, parent: ROOT, .. } if *node == new_node
        )),
        "{plan:?}"
    );
}

/// Nested folders out of view: `proj/sub/f.txt` moved to the top by another device, then `proj`
/// into `build/`. Both folders go whole: `f` needs neither.
#[test]
fn nested_folders_moved_into_an_ignored_folder_go_whole_after_their_file_moved_out() {
    let [b, d, sub, f, g] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = World::ignoring_build();
    w.synced(d, folder(ROOT, "proj"))
        .synced(sub, folder(d, "sub"))
        .synced(f, file(sub, "f.txt", "v1"))
        .synced(g, file(sub, "g.txt", "g"));
    w.moved_into_build(b, d, "proj")
        .remote(f, Some(file(ROOT, "f.txt", "v1")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["move", "forget+delete"], "{plan:?}");
    assert_eq!(deletes_here(&plan), [("forget", d, vec![d, sub, g])]);
}

/// A folder moved out of one now out of view takes along what is in it: a file the drive moved
/// into `build/` meanwhile, and one it deleted. Their own forget and delete stay (the folder's
/// forget no longer covers them), each of what it holds here.
#[test]
fn what_a_folder_moved_out_of_one_out_of_view_holds_is_planned_on_its_own() {
    let [b, d, e, x, y, g] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = World::ignoring_build();
    w.synced(d, folder(ROOT, "proj"))
        .synced(e, folder(d, "e"))
        .synced(x, file(e, "x.txt", "x"))
        .synced(y, file(e, "y.txt", "y"))
        .synced(g, file(d, "g.txt", "g"));
    w.moved_into_build(b, d, "proj")
        .remote(x, Some(file(b, "x.txt", "x")))
        .remote(e, Some(folder(ROOT, "e")))
        .remote(y, None);
    let plan = w.plan();
    assert!(detached(&plan).is_empty(), "{plan:?}");
    assert!(moves_to_top(&plan, e), "{plan:?}");
    let mut got = deletes_here(&plan);
    got.sort();
    let want = [
        ("delete", y, vec![y]),
        ("forget", d, vec![d, g]),
        ("forget", x, vec![x]),
    ];
    assert_eq!(got, want, "{plan:?}");
}

/// A folder moved out of one now out of view holds a file the drive moved into `build/` that
/// was edited here: the file is detached where it is (in the folder that moves out), and that
/// takes no masked folder above it along: `proj` goes whole.
#[test]
fn an_edited_file_in_a_folder_moved_out_keeps_no_folder_above_it() {
    let [b, d, e, x, g] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = World::ignoring_build();
    w.synced(d, folder(ROOT, "proj"))
        .synced(e, folder(d, "e"))
        .synced(x, file(e, "x.txt", "x"))
        .synced(g, file(d, "g.txt", "g"));
    w.local(x, Some(file(e, "x.txt", "mine")));
    w.moved_into_build(b, d, "proj")
        .remote(x, Some(file(b, "x.txt", "x")))
        .remote(e, Some(folder(ROOT, "e")));
    let plan = w.plan();
    assert_eq!(detached(&plan), [x], "{plan:?}");
    assert!(moves_to_top(&plan, e), "{plan:?}");
    assert!(
        plan.ops.iter().any(|op| matches!(
            op,
            Op::RemoteCreate { parent, .. } if *parent == e
        )),
        "{plan:?}"
    );
    assert_eq!(deletes_here(&plan), [("forget", d, vec![d, g])]);
}

/// Two folders out of view, a file moved out of each; one file can't move in this plan (a file
/// still being written holds its name at the top, §6.3): its folder is kept for it, detached,
/// and its forget never takes it; only the other folder goes whole.
#[test]
fn a_file_that_waits_to_move_out_keeps_only_its_own_folder() {
    let [b, d1, f1, d2, f2, writing] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = World::ignoring_build();
    w.synced(d1, folder(ROOT, "one"))
        .synced(f1, file(d1, "f1.txt", "1"))
        .synced(d2, folder(ROOT, "two"))
        .synced(f2, file(d2, "f2.txt", "2"));
    w.moved_into_build(b, d1, "one")
        .remote(d2, Some(folder(b, "two")))
        .remote(f1, Some(file(ROOT, "f1.txt", "1")))
        .remote(f2, Some(file(ROOT, "f2.txt", "2")))
        .writing(writing, "f1.txt");
    let plan = w.plan();
    assert_eq!(detached(&plan), [d1], "{plan:?}");
    assert_eq!(deletes_here(&plan), [("forget", d2, vec![d2])], "{plan:?}");
}

#[test]
fn a_folder_with_children_this_device_ignores_survives_a_local_delete() {
    let [d, f, log] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("*.log\n"),
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
        filter: ignoring("build/\n"),
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

/// Whether a plan makes the folder `node`, here or on the drive.
fn makes(plan: &hbsync_engine::Plan, node: NodeId) -> bool {
    let made = |op: &&Op| matches!(op, Op::LocalMkdir { .. } | Op::RemoteCreate { .. });
    plan.ops.iter().filter(made).any(|op| op.node() == node)
}

/// The folders `folders` (each in the one before it, the first at the top) synced here and gone
/// since on both sides (deleted for good on the drive; here the scanner no longer reports them,
/// or they were deleted), with a file another device added (`g`) live in the last.
fn kept_of_folders_gone(mut w: World, folders: &[(NodeId, &str)], g: NodeId) -> World {
    w.input.remote_complete = true;
    let mut parent = ROOT;
    for &(id, name) in folders {
        w.synced(id, folder(parent, name));
        w.local(id, None).remote(id, None);
        parent = id;
    }
    w.remote(g, Some(file(parent, "new.o", "n")));
    w
}

/// What recovers the orphan `g` into a new Recovered files and forgets the folders it was in.
const RECOVERS: [&str; 4] = ["mkdir", "create", "rename", "download"];

/// A folder a new rule hides, deleted for good on the drive with a file another device added
/// still in it (the server kept that): this device knows the folder from before only, and
/// doesn't make it again at a place it filters, over the item it keeps to itself (which the
/// scanner no longer reports). It forgets the folder, and the file is an orphan like any other
/// (§6.3): into Recovered files.
#[test]
fn a_folder_a_new_rule_hides_isnt_made_again_for_what_the_drive_kept_of_it() {
    let [b, g] = ids(2)[..] else { unreachable!() };
    let world = |w| kept_of_folders_gone(w, &[(b, "build")], g);
    let plan = world(World::ignoring_build()).plan();
    assert_eq!(kinds(&plan.ops), [&RECOVERS[..], &["settle"]].concat());
    assert!(!makes(&plan, b), "{plan:?}");
    assert_eq!(plan.ops[4].node(), b);
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: g }]);
    // Without the rule, the file keeps its folder: made again.
    let plan = world(World::default()).plan();
    assert!(makes(&plan, b), "{plan:?}");
    assert!(plan.conflicts.is_empty(), "{plan:?}");
}

/// As above, with the folder below one that went with it, both known from before only: the
/// place is judged by where both were.
#[test]
fn a_folder_below_one_a_new_rule_hides_isnt_made_again_either() {
    let [p, b, g] = ids(3)[..] else {
        unreachable!()
    };
    let world = |w| kept_of_folders_gone(w, &[(p, "build"), (b, "obj")], g);
    let plan = world(World::ignoring_build()).plan();
    assert_eq!(
        kinds(&plan.ops),
        [&RECOVERS[..], &["settle", "settle"]].concat()
    );
    assert!(!makes(&plan, p) && !makes(&plan, b), "{plan:?}");
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: g }]);
    let plan = world(World::default()).plan();
    assert!(makes(&plan, p) && makes(&plan, b), "{plan:?}");
}

/// A file this device synced in `build` before a rule hid it, which another device changed
/// since: the folder is gone on both sides (for good on the drive; no longer reported here),
/// the file no longer reported here, and edited on the drive, which kept it. This device has
/// a base of it and nothing else.
fn held_from_before(mut w: World, b: NodeId, f: NodeId) -> World {
    w.input.remote_complete = true;
    w.synced(b, folder(ROOT, "build"));
    w.synced(f, file(b, "out.o", "o"));
    w.local(b, None).remote(b, None).local(f, None);
    w.remote(f, Some(file(b, "out.o", "o2")));
    w
}

/// Whether `plan` leaves `node` as it is, here and on the drive: nothing done to it but its
/// base dropped (a forget that takes nothing here with it).
fn leaves(plan: &hbsync_engine::Plan, node: NodeId) -> bool {
    let its: Vec<&Op> = plan.ops.iter().filter(|op| op.node() == node).collect();
    matches!(its[..], [Op::Forget { subtree, .. }] if subtree.is_empty())
}

/// As above, for a file this device synced there before the rule (it has a base of it; its
/// item is its own now, no longer reported), which another device changed since: a base
/// alone is no hold, so the folder isn't made again for it either.
/// Made again, its name is taken by the item the device keeps: tried at every sync, for good
/// (#427). The file is an orphan like any other: into Recovered files, in its version on the
/// drive.
#[test]
fn nor_is_it_made_again_for_a_file_synced_there_before_the_rule() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let plan = held_from_before(World::ignoring_build(), b, f).plan();
    assert!(!makes(&plan, b), "{plan:?}");
    assert!(makes(&plan, NodeId::RECOVERED), "{plan:?}");
    let moved = plan.ops.iter().find_map(|op| match op {
        Op::RemoteUpdate { node, parent, .. } if *node == f => Some(*parent),
        _ => None,
    });
    assert_eq!(moved, Some(NodeId::RECOVERED), "{plan:?}");
    let kinds = kinds(&plan.ops);
    assert!(kinds.contains(&"download"), "{plan:?}");
    assert!(
        !kinds.contains(&"delete"),
        "nothing leaves the drive: {plan:?}"
    );
    // (The other device's edit wins over what reads as this device's delete, by the merge's
    // own rule, said as such.)
    let conflicts = [
        Conflict::EditDelete { node: f },
        Conflict::Orphan { node: f },
    ];
    assert_eq!(plan.conflicts, conflicts, "{plan:?}");
    // Without the rule, the file keeps its folder: made again.
    let plan = held_from_before(World::default(), b, f).plan();
    assert!(makes(&plan, b), "{plan:?}");
    assert_eq!(
        plan.conflicts,
        [Conflict::EditDelete { node: f }],
        "{plan:?}"
    );
}

/// With a second rule that hides the file wherever it is (`*.o`): Recovered files is no place
/// this device syncs it at either, so the file is left as it is, on the drive, for a device
/// that syncs it (§6.3, "Only into a place the device syncs"), and the base of it goes (no
/// base outlives what it tells of). Taken for held, it went into Recovered files and came
/// down there, unreported from then on: an item the device stopped syncing, which left the
/// drive, the other device's edit with it.
#[test]
fn a_file_synced_before_the_rule_isnt_recovered_to_where_a_rule_hides_it() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let w = World {
        filter: ignoring("build/\n*.o\n"),
        ..World::default()
    };
    let plan = held_from_before(w, b, f).plan();
    assert!(
        !makes(&plan, b) && !makes(&plan, NodeId::RECOVERED),
        "{plan:?}"
    );
    assert!(leaves(&plan, f), "{plan:?}");
}

/// So too where Recovered files itself is known from before only, in the folder the rule
/// hides (the user had moved it there): where it would come back to isn't known down from
/// the top, so there is no place to recover to. A file taken for held was sent into it,
/// found no folder there (none is made again at a place the device filters), and was
/// recovered into it again: a plan that never ended.
#[test]
fn nor_into_recovered_files_where_the_rule_hides_that_too() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let recovered = NodeId::RECOVERED;
    let mut w = held_from_before(World::ignoring_build(), b, f);
    w.synced(recovered, folder(b, "Recovered files"));
    w.local(recovered, None).remote(recovered, None);
    let plan = w.plan();
    assert!(!makes(&plan, b) && !makes(&plan, recovered), "{plan:?}");
    assert!(leaves(&plan, f), "{plan:?}");
}

/// And where a rule hides Recovered files itself, known from before at the top and gone
/// since: no place this device syncs, the file left.
#[test]
fn nor_into_recovered_files_where_a_rule_hides_it_at_the_top() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let recovered = NodeId::RECOVERED;
    let w = World {
        filter: ignoring("build/\nRecovered files/\n"),
        ..World::default()
    };
    let mut w = held_from_before(w, b, f);
    w.synced(recovered, folder(ROOT, "Recovered files"));
    w.local(recovered, None).remote(recovered, None);
    let plan = w.plan();
    assert!(!makes(&plan, b) && !makes(&plan, recovered), "{plan:?}");
    assert!(leaves(&plan, f), "{plan:?}");
}

/// And where the folder is in Recently deleted (masked, where this device filters it), the
/// file written since its removal: left for the devices that sync that place, which bring the
/// folder back with it (§6.3, "Nor is what is in a folder in Recently deleted …"). Taken for
/// held, it was moved into Recovered files, and the folder stayed deleted for the others.
#[test]
fn nor_is_it_recovered_from_a_removed_folder_the_rule_hides() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let mut w = held_from_before(World::ignoring_build(), b, f);
    let removed = hbsync_engine::reconcile::Removed {
        at: 1,
        state: folder(ROOT, "build"),
    };
    w.input.removed.insert(b, removed);
    // (Written after the removal: not taken along by it.)
    w.input.changed_at.insert(f, 10);
    let plan = w.plan();
    assert!(
        !makes(&plan, b) && !makes(&plan, NodeId::RECOVERED),
        "{plan:?}"
    );
    assert!(leaves(&plan, f), "{plan:?}");
}

/// The file of [`held_from_before`] once its folder's base is gone (settled away by an
/// earlier plan, the file's still there: its delete was held, or skipped), under the two
/// rules; with an item of it here (`here`) or without.
fn folder_base_gone(b: NodeId, f: NodeId, here: bool) -> World {
    let mut w = World {
        filter: ignoring("build/\n*.o\n"),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.synced(f, file(b, "out.o", "o"));
    if !here {
        w.local(f, None);
    }
    w.remote(f, Some(file(b, "out.o", "o2")));
    w
}

/// Whether `plan` moves `node` into Recovered files on the drive.
fn recovers(plan: &hbsync_engine::Plan, node: NodeId) -> bool {
    let into = |op: &Op| matches!(op, Op::RemoteUpdate { node: n, parent, .. } if *n == node && *parent == NodeId::RECOVERED);
    plan.ops.iter().any(into)
}

/// And once the folder's base is gone, the file's still there: the file is no more held for
/// that, and is left still. (Nothing is told from where a base is: the bases above it go in
/// any order.)
#[test]
fn nor_once_its_folders_base_is_gone() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let plan = folder_base_gone(b, f, false).plan();
    assert!(
        !makes(&plan, b) && !makes(&plan, NodeId::RECOVERED),
        "{plan:?}"
    );
    assert!(leaves(&plan, f), "{plan:?}");
}

/// What a device holds is an item here: with one (an input the scanner wouldn't give under
/// these rules: it pins the definition), the same file is held, and recovered as before.
#[test]
fn an_item_here_is_a_hold_where_a_base_alone_is_none() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let plan = folder_base_gone(b, f, true).plan();
    assert!(recovers(&plan, f), "{plan:?}");
    assert_eq!(plan.conflicts, [Conflict::Orphan { node: f }], "{plan:?}");
}

/// The file of [`held_from_before`] in a folder `sub` of `build` that the drive still has
/// (the delete of `build` stopped there), as it synced here: under `rules`.
fn in_a_folder_still_there(rules: &str, b: NodeId, sub: NodeId, f: NodeId) -> World {
    let mut w = World {
        filter: ignoring(rules),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.synced(b, folder(ROOT, "build"));
    w.synced(sub, folder(b, "sub"));
    w.synced(f, file(sub, "out.o", "o"));
    w.local(b, None).remote(b, None);
    w.local(sub, None).local(f, None);
    w.remote(f, Some(file(sub, "out.o", "o2")));
    w
}

/// Whether `plan` deletes `node` from the drive.
fn deletes_on_drive(plan: &hbsync_engine::Plan, node: NodeId) -> bool {
    let deleted = |op: &Op| matches!(op, Op::RemoteDelete { node: n, .. } if *n == node);
    plan.ops.iter().any(deleted)
}

/// A folder the drive still has, between the folder that is gone and the file: it isn't
/// removed as one this device stopped syncing in the plan that moves the other device's file
/// out of it (cut short between the two, the removal took the file along: the other device's
/// edit, gone with it). The file is recovered; the folder's removal waits for the plan after.
#[test]
fn a_folder_still_on_the_drive_isnt_removed_over_another_devices_file() {
    let [b, sub, f] = ids(3)[..] else {
        unreachable!()
    };
    let plan = in_a_folder_still_there("build/\n", b, sub, f).plan();
    assert!(
        !deletes_on_drive(&plan, sub) && !deletes_on_drive(&plan, f),
        "{plan:?}"
    );
    assert!(!makes(&plan, b) && !makes(&plan, sub), "{plan:?}");
    assert!(recovers(&plan, f), "{plan:?}");
    assert!(plan.deferred.contains(&sub), "{plan:?}");
}

/// With a second rule that hides the file's name everywhere (`*.o`), the file is left where
/// it is, and the folder stays on the drive with it (a folder that holds what isn't leaving
/// isn't leaving, §10 rule 2): the bases of both go. Removed, the folder took the file
/// along.
#[test]
fn nor_where_a_second_rule_hides_the_file_in_it() {
    let [b, sub, f] = ids(3)[..] else {
        unreachable!()
    };
    let plan = in_a_folder_still_there("build/\n*.o\n", b, sub, f).plan();
    assert!(
        !deletes_on_drive(&plan, sub) && !deletes_on_drive(&plan, f),
        "{plan:?}"
    );
    assert!(!makes(&plan, NodeId::RECOVERED), "{plan:?}");
    assert!(leaves(&plan, sub) && leaves(&plan, f), "{plan:?}");
}

/// Whether `plan` makes or fills anything here in the folder `folder`: a place this device
/// filters, in these tests.
fn brings_into(plan: &hbsync_engine::Plan, folder: NodeId) -> bool {
    let into = |op: &Op| match op {
        Op::LocalMkdir { node, parent, .. } => *node == folder || *parent == folder,
        Op::LocalDownload { parent, .. } | Op::LocalMove { parent, .. } => *parent == folder,
        _ => false,
    };
    plan.ops.iter().any(into)
}

/// A folder the drive has at a place (its folder is in the plan) that this device filters
/// since it renamed that folder. Nothing is made or
/// brought down into it here (unreported from then on, it left the drive at the next sync:
/// another device's new file, deleted). The file is recovered, as before; and the folder
/// isn't removed in the plan that moves the file out of it (cut short between the two, the
/// removal took the file along): that waits for the plan after.
#[test]
fn a_folder_at_a_place_this_device_filters_isnt_filled_here() {
    let [x, cache, c1, f] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("a/cache/\n"),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.synced(x, folder(ROOT, "x"));
    w.synced(cache, folder(x, "cache"));
    w.synced(c1, file(cache, "c1", "1"));
    // Renamed here: `cache` is hidden now, with what is in it.
    w.local(x, Some(folder(ROOT, "a")));
    w.local(cache, None).local(c1, None);
    // Another device added a file to it.
    w.remote(f, Some(file(cache, "f", "new")));
    let plan = w.plan();
    assert!(!brings_into(&plan, cache), "{plan:?}");
    assert!(recovers(&plan, f), "{plan:?}");
    assert!(!deletes_on_drive(&plan, cache), "{plan:?}");
    assert!(!deletes_on_drive(&plan, f), "{plan:?}");
    assert!(plan.deferred.contains(&cache), "{plan:?}");
}

/// So too where the folder above it is gone for good on the drive, and still here, in view:
/// that one isn't made again for a folder this device hides in it (made again, the hidden
/// folder was made here and filled: what came down left the drive at the next sync). The
/// file is recovered, and the hidden folder's removal waits.
#[test]
fn nor_is_the_folder_above_made_again_for_a_hidden_one() {
    let [x, cache, c1, f] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("cache/\n"),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.synced(x, folder(ROOT, "x"));
    w.synced(cache, folder(x, "cache"));
    w.synced(c1, file(cache, "c1", "1"));
    w.remote(x, None);
    w.local(cache, None).local(c1, None);
    w.remote(f, Some(file(cache, "f", "new")));
    let plan = w.plan();
    assert!(!makes(&plan, x), "{plan:?}");
    assert!(!brings_into(&plan, cache), "{plan:?}");
    assert!(recovers(&plan, f), "{plan:?}");
    assert!(
        !deletes_on_drive(&plan, cache) && !deletes_on_drive(&plan, f),
        "{plan:?}"
    );
    assert!(plan.deferred.contains(&cache), "{plan:?}");
}

/// A folder `cache` in the folder still on the drive, hidden by a rule of its own wherever it
/// is, with another device's file in it and one beside it: nothing is made or brought down
/// into `cache`, nor is `cache` or the folder it is in removed over a file. Whichever of the
/// two files the plan comes to first (`first`: the one beside it). The plan, and the folder
/// still there, `cache`, the file beside it and the file in it.
fn hidden_folder_in_the_one_still_there(first: bool) -> (hbsync_engine::Plan, [NodeId; 4]) {
    let [b, sub, cache, lo, hi] = ids(5)[..] else {
        unreachable!()
    };
    // (The plan takes the nodes from the last id down.)
    let (keep, x) = if first { (hi, lo) } else { (lo, hi) };
    let mut w = World {
        filter: ignoring("build/\ncache/\n"),
        ..World::default()
    };
    w.input.remote_complete = true;
    w.synced(b, folder(ROOT, "build"));
    w.synced(sub, folder(b, "sub"));
    w.synced(cache, folder(sub, "cache"));
    w.synced(keep, file(sub, "keep.txt", "k"));
    w.synced(x, file(cache, "x.txt", "x"));
    w.local(b, None).remote(b, None);
    for hidden in [sub, cache, keep, x] {
        w.local(hidden, None);
    }
    w.remote(keep, Some(file(sub, "keep.txt", "k2")));
    w.remote(x, Some(file(cache, "x.txt", "x2")));
    (w.plan(), [sub, cache, keep, x])
}

#[test]
fn a_hidden_folder_in_the_one_still_there_isnt_filled_or_removed() {
    for first in [true, false] {
        let (plan, [sub, cache, keep, x]) = hidden_folder_in_the_one_still_there(first);
        // Each file is recovered by itself: never brought down into the hidden folder, nor
        // removed, nor a folder over it (their removal waits).
        assert!(!brings_into(&plan, cache), "{first}: {plan:?}");
        for node in [sub, cache, keep, x] {
            assert!(!deletes_on_drive(&plan, node), "{first}: {plan:?}");
        }
        assert!(
            recovers(&plan, keep) && recovers(&plan, x),
            "{first}: {plan:?}"
        );
        let waits = |folder| plan.deferred.contains(&folder);
        assert!(waits(sub) && waits(cache), "{first}: {plan:?}");
    }
}

/// With nothing changed in it, the folder still leaves the drive with what is in it, as any
/// folder this device stops syncing (§10, rule 1).
#[test]
fn a_folder_still_on_the_drive_with_nothing_changed_in_it_still_leaves() {
    let [b, sub, f] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = in_a_folder_still_there("build/\n", b, sub, f);
    w.remote(f, Some(file(sub, "out.o", "o")));
    let plan = w.plan();
    assert!(deletes_on_drive(&plan, sub), "{plan:?}");
    assert!(!makes(&plan, NodeId::RECOVERED), "{plan:?}");
}

/// A file synced there before the rule that the drive has as it synced leaves the drive, as
/// any item this device stops syncing (§10, rule 1): unchanged by the above.
#[test]
fn a_file_synced_before_the_rule_and_unchanged_since_still_leaves_the_drive() {
    let [b, f] = ids(2)[..] else { unreachable!() };
    let mut w = held_from_before(World::ignoring_build(), b, f);
    w.remote(f, Some(file(b, "out.o", "o")));
    let plan = w.plan();
    assert!(!makes(&plan, b), "{plan:?}");
    let deleted = |op: &Op| matches!(op, Op::RemoteDelete { node, .. } if *node == f);
    assert!(plan.ops.iter().any(deleted), "{plan:?}");
}

#[test]
fn a_folder_deleted_here_stays_deleted_when_what_it_held_leaves_the_drive() {
    let [p, b, f] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("build/\n"),
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
        filter: ignoring("*.o\n"),
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

/// An item placed but not hashed yet (a download a crash left unsettled, here with no base),
/// tagged Red by the user, whose node another writer put in a file: its merge is deferred, and
/// the orphan repair leaves it be. It adopted it from the drive's state, writing the drive's
/// tags over the item and sending them up as decided: Red was lost (seed d856d284). The rest
/// of the folder plans as ever: another orphan in the file goes into Recovered files, a new
/// file comes down. Once hashed, the node is adopted with its tags.
#[test]
fn an_unhashed_orphan_waits_and_keeps_its_tags() {
    let [f, item, other, new] = ids(4)[..] else {
        unreachable!()
    };
    let red = hbsync_types::Tags::new([hbsync_types::Tag::new("Red", 6)]);
    let tagged = |state: NodeState| NodeState {
        tags: red.clone(),
        ..state
    };
    let mut w = World::default();
    w.synced(f, file(ROOT, "f", "x"));
    w.synced(other, file(ROOT, "o", "o"));
    w.remote(other, Some(file(f, "o", "o")));
    w.remote(new, Some(file(ROOT, "n", "n")));
    w.remote(item, Some(file(f, "b", "b")));
    w.local(
        item,
        Some(tagged(NodeState {
            content: None,
            ..file(ROOT, "b", "b")
        })),
    );
    w.input.local_unhashed.insert(item);
    let plan = w.plan();
    assert!(
        !plan.ops.iter().any(|op| op.node() == item),
        "nothing done to the item till it is hashed: {:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&item), "{:?}", plan.deferred);
    assert!(
        plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. } if *node == other && *parent == NodeId::RECOVERED)),
        "the other orphan is recovered: {:#?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { node, .. } if *node == new)),
        "the new file comes down: {:#?}",
        plan.ops
    );

    // Hashed: adopted into Recovered files, with Red, on the drive too.
    w.input.local_unhashed.remove(&item);
    w.local(item, Some(tagged(file(ROOT, "b", "b"))));
    let plan = w.plan();
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::RemoteUpdate { node, parent, tags, .. }
            if *node == item && *parent == NodeId::RECOVERED && *tags == red)
        ),
        "{:#?}",
        plan.ops
    );
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalTags { node, .. } if *node == item)),
        "{:#?}",
        plan.ops
    );
}

/// A file being written here (not hashed yet: its merge deferred) in a folder the drive
/// deleted: the folder's delete waits for its hash (neither done here nor undone), and once
/// hashed its edit keeps the folder alive, which comes back on the drive.
#[test]
fn a_file_not_hashed_yet_keeps_its_deleted_folder() {
    let [p, d] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(p, folder(ROOT, "p"));
    w.synced(d, file(p, "d", "1"));
    w.remote(p, None).remote(d, None);
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(p, "d", "2")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { .. })),
        "nothing deleted: {:#?}",
        plan.ops
    );
    assert!(plan.ops.is_empty(), "the delete waits: {:#?}", plan.ops);
    assert!(plan.deferred.contains(&d), "{:?}", plan.deferred);
    // Hashed: the edit wins, and the folder comes back.
    w.input.local_unhashed.remove(&d);
    w.local(d, Some(file(p, "d", "2")));
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == p)),
        "the folder comes back: {:#?}",
        plan.ops
    );
}

/// A file not hashed yet keeps its name here: a new file the drive has under that name, in
/// that folder, waits for its hash (it was planned onto the name, and refused there every
/// round while the file stayed unhashed).
#[test]
fn a_file_not_hashed_yet_keeps_its_name() {
    let [f, d, new] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(f, file(ROOT, "f", "x"));
    w.synced(d, file(ROOT, "b", "1"));
    w.remote(d, Some(file(f, "b", "1"))); // another writer put it in a file
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(ROOT, "b", "2")
        }),
    );
    w.input.local_unhashed.insert(d);
    w.remote(new, Some(file(ROOT, "b", "n")));
    let plan = w.plan();
    assert!(
        !plan.ops.iter().any(|op| op.node() == new || op.node() == d),
        "the new file waits for the name: {:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&new), "{:?}", plan.deferred);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
}

/// What another writer put into a file not hashed yet here (a file: no folder) is an orphan
/// for Recovered files, as in any file; the file itself waits.
#[test]
fn what_is_in_a_file_not_hashed_yet_is_recovered() {
    let [d, y] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, file(ROOT, "d", "1"));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(ROOT, "d", "2")
        }),
    );
    w.input.local_unhashed.insert(d);
    w.remote(y, Some(file(d, "y", "y")));
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, parent, .. }
            if *node == y && *parent == NodeId::RECOVERED)),
        "{:#?}",
        plan.ops
    );
    assert!(!plan.ops.iter().any(|op| op.node() == d), "{:#?}", plan.ops);
}

/// `node` moves into Recovered files on the drive, and nothing deletes it.
fn assert_recovered_not_deleted(plan: &hbsync_engine::plan::Plan, node: NodeId) {
    let ops = &plan.ops;
    assert!(
        ops.iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node: n, parent, .. }
            if *n == node && *parent == NodeId::RECOVERED)),
        "{ops:#?}"
    );
    assert!(
        !ops.iter()
            .any(|op| op.node() == node && op.deletes().is_some()),
        "{ops:#?}"
    );
}

/// Another writer put `y` in file `f`, then `f` went to Recently deleted: a removed file takes
/// nothing along, so `y` is an orphan for Recovered files, as in a live file (§6.3). Taken
/// along, it was deleted here and left live on the drive, where nothing listed it.
#[test]
fn what_is_in_a_removed_file_is_recovered_not_taken_along() {
    let [f, y] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(f, file(ROOT, "f", "1"));
    w.synced(y, file(ROOT, "y", "y"));
    w.remote(f, None);
    w.input.removed.insert(
        f,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: file(ROOT, "f", "1"),
        },
    );
    w.remote(y, Some(file(f, "y", "y")));
    w.input.changed_at.insert(y, 10);
    assert_recovered_not_deleted(&w.plan(), y);
}

/// Folder `d` went to Recently deleted with file `f` in it, and another writer had put `y` in
/// `f`: `d`'s removal took `f` along but not `y` (a file takes nothing along), so `y` is an
/// orphan for Recovered files (§6.3) and `f` goes here.
#[test]
fn what_is_in_a_file_a_removal_took_along_is_recovered() {
    let [d, f, y] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(d, folder(ROOT, "d"));
    w.synced(f, file(d, "f", "1"));
    w.synced(y, file(ROOT, "y", "y"));
    w.remote(d, None);
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    w.input.changed_at.insert(f, 10);
    w.remote(y, Some(file(f, "y", "y")));
    w.input.changed_at.insert(y, 10);
    assert_recovered_not_deleted(&w.plan(), y);
}

/// The drive renames and moves a file while it is being written here: nothing is done to it
/// till it is hashed, then the move comes down and the edit goes up, never lost.
#[test]
fn a_move_on_the_drive_waits_for_a_file_not_hashed_yet() {
    let [p, q, d] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(p, folder(ROOT, "p"));
    w.synced(q, folder(ROOT, "q"));
    w.synced(d, file(p, "d", "1"));
    w.remote(d, Some(file(q, "e", "1")));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(p, "d", "2")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(!plan.ops.iter().any(|op| op.node() == d), "{:#?}", plan.ops);
    // Hashed.
    w.input.local_unhashed.remove(&d);
    w.local(d, Some(file(p, "d", "2")));
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalMove { node, parent, name, .. }
            if *node == d && *parent == q && name == "e")),
        "{:#?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, upload: true, .. } if *node == d)),
        "{:#?}",
        plan.ops
    );
}

/// A file being written here at the top, unhashed: another device moves it into folder z and
/// edits it, and this device's user deletes z. The delete of z waits (it took the drive's
/// node along, and the other device's edit was lost once hashed); hashed, the edits fork and z
/// stays.
#[test]
fn a_folder_deleted_here_waits_for_a_file_moved_into_it_on_the_drive() {
    let [z, d] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(z, folder(ROOT, "z"));
    w.synced(d, file(ROOT, "d", "1"));
    w.remote(d, Some(file(z, "d", "theirs")));
    w.local(z, None);
    let writing = NodeState {
        content: None,
        ..file(ROOT, "d", "mine")
    };
    w.local(d, Some(writing));
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.deferred.contains(&z), "{:?}", plan.deferred);

    w.input.local_unhashed.remove(&d);
    w.local(d, Some(file(ROOT, "d", "mine")));
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { .. })),
        "{:#?}",
        plan.ops
    );
    assert!(
        plan.conflicts
            .iter()
            .any(|c| matches!(c, Conflict::EditEdit { node, .. } if *node == d)),
        "{:?}",
        plan.conflicts
    );
}

/// Another device moves a file from p to q and deletes p, while it is being written here in
/// p: p's delete waits, and isn't undone on the drive (it was made again, empty, for good);
/// hashed, the file follows its move and p goes.
#[test]
fn a_folder_deleted_on_the_drive_isnt_restored_for_a_file_not_hashed_yet() {
    let [p, q, d] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(p, folder(ROOT, "p"));
    w.synced(q, folder(ROOT, "q"));
    w.synced(d, file(p, "d", "1"));
    w.remote(p, None);
    w.remote(d, Some(file(q, "d", "1")));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(p, "d", "1")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);

    w.input.local_unhashed.remove(&d);
    w.local(d, Some(file(p, "d", "1")));
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { .. })),
        "{:#?}",
        plan.ops
    );
    assert!(
        plan.ops.iter().any(
            |op| matches!(op, Op::LocalMove { node, parent, .. } if *node == d && *parent == q)
        ),
        "{:#?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { node, .. } if *node == p)),
        "{:#?}",
        plan.ops
    );
}

/// Another device swaps two names in a folder (b to c, x to b) while b is being written here:
/// x waits for the name (it was renamed for good, "b (conflict 1)"), and once b is hashed both
/// follow the drive, with no conflict.
#[test]
fn a_rename_onto_the_name_of_a_file_not_hashed_yet_waits() {
    let [d, m] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, file(ROOT, "b", "1"));
    w.synced(m, file(ROOT, "x", "2"));
    w.remote(d, Some(file(ROOT, "c", "1")));
    w.remote(m, Some(file(ROOT, "b", "2")));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(ROOT, "b", "1")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    assert!(plan.deferred.contains(&m), "{:?}", plan.deferred);

    w.input.local_unhashed.remove(&d);
    w.local(d, Some(file(ROOT, "b", "1")));
    let plan = w.plan();
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    for (node, to) in [(d, "c"), (m, "b")] {
        assert!(
            plan.ops.iter().any(
                |op| matches!(op, Op::LocalMove { node: n, name, .. } if *n == node && name == to)
            ),
            "{:#?}",
            plan.ops
        );
    }
}

/// A file being written here, renamed on the drive (b to c): its drive name is taken for it
/// too. A file this device's user renames to c meanwhile waits (it went up as a second c).
#[test]
fn the_drive_name_of_a_file_not_hashed_yet_is_taken() {
    let [d, x] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, file(ROOT, "b", "1"));
    w.synced(x, file(ROOT, "x", "2"));
    w.remote(d, Some(file(ROOT, "c", "1")));
    w.local(x, Some(file(ROOT, "c", "2")));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(ROOT, "b", "1")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.deferred.contains(&x), "{:?}", plan.deferred);
}

/// Not in the page planned (context), a file being written still holds what it is in: the
/// drive deleted its folder (dirty), which isn't deleted with it; and one whose node another
/// writer put in a file isn't adopted from the drive's state.
#[test]
fn a_file_not_hashed_yet_holds_as_context_too() {
    let [p, d, f, e] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(p, folder(ROOT, "p"));
    w.synced(d, file(p, "d", "1"));
    w.synced(f, file(ROOT, "f", "f"));
    w.synced(e, file(ROOT, "e", "1"));
    w.remote(p, None).remote(d, None);
    w.remote(e, Some(file(f, "e", "1")));
    for node in [d, e] {
        let state = w
            .input
            .local
            .get(node)
            .cloned()
            .map(|s| NodeState { content: None, ..s });
        w.local(node, state);
        w.input.local_unhashed.insert(node);
        w.input.dirty.remove(&node);
    }
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
}

/// A new folder with a file being written in it still goes up: what waits is only a delete,
/// a restore or a move.
#[test]
fn a_new_folder_goes_up_while_a_file_in_it_is_written() {
    let [n, d] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.local(n, Some(folder(ROOT, "n")));
    w.local(
        d,
        Some(NodeState {
            content: None,
            ..file(n, "d", "1")
        }),
    );
    w.input.local_unhashed.insert(d);
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == n)),
        "{:#?}",
        plan.ops
    );
    assert_eq!(plan.deferred, [d]);
}

/// A file being written in folder b, which the user moved here out of r into x, and r deleted
/// here: b's move waits (b holds the file), and so does r's delete, b's folder on the drive. r
/// deleted on the drive took b and what else is in it along (f1, never deleted by anyone).
#[test]
fn a_folder_that_holds_a_waiting_folder_on_the_drive_waits_too() {
    let [r, b, f1, x, wf] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(r, folder(ROOT, "r"))
        .synced(b, folder(r, "b"))
        .synced(f1, file(b, "f1", "1"))
        .synced(x, folder(ROOT, "x"))
        .synced(wf, file(ROOT, "w", "w"));
    w.local(b, Some(folder(x, "b")));
    w.local(r, None);
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(b, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::RemoteDelete { .. } | Op::LocalDelete { .. })),
        "{:#?}",
        plan.ops
    );
}

/// The drive moved folder b into c, a file being written here into b, and deleted b's old
/// folder d; here a file in b has an edit. b waits where it is here (it holds the file), and
/// so does d: never deleted here with b and the edit in it.
#[test]
fn a_folder_that_holds_a_waiting_folder_here_waits_too() {
    let [d, b, e, a, c, wf] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"))
        .synced(b, folder(d, "b"))
        .synced(e, file(b, "e", "1"))
        .synced(a, folder(ROOT, "a"))
        .synced(c, folder(ROOT, "c"))
        .synced(wf, file(a, "w", "w"));
    w.remote(b, Some(folder(c, "b")));
    w.remote(wf, Some(file(b, "w", "w")));
    w.remote(d, None);
    w.local(e, Some(file(b, "e", "2")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(a, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { .. } | Op::RemoteDelete { .. })),
        "{:#?}",
        plan.ops
    );
}

/// The drive moved n out of g to the top as x.txt and deleted g; here n has an edit, and a new
/// x.txt at the top is being written. n waits for the name where it is, and g, which holds it,
/// waits too: never deleted with n's edit in it.
#[test]
fn a_node_waiting_for_a_name_keeps_its_folder_here() {
    let [g, n, hh] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(g, folder(ROOT, "g"))
        .synced(n, file(g, "x.txt", "1"));
    w.remote(n, Some(file(ROOT, "x.txt", "1")));
    w.remote(g, None);
    w.local(n, Some(file(g, "x.txt", "2")));
    w.local(
        hh,
        Some(NodeState {
            content: None,
            ..file(ROOT, "x.txt", "h")
        }),
    );
    w.input.local_unhashed.insert(hh);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { .. } | Op::RemoteDelete { .. })),
        "{:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&n), "{:?}", plan.deferred);
    assert_eq!(plan.waiting, [hh]);
}

/// Here n moved out of r to the top as x.txt, with an edit, and r deleted; the drive renamed h,
/// being written here, to x.txt. n waits for the drive's name, and r, its folder on the drive,
/// waits too: its delete took n along there.
#[test]
fn a_node_waiting_for_a_name_keeps_its_folder_on_the_drive() {
    let [r, n, hh] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(r, folder(ROOT, "r"))
        .synced(n, file(r, "n", "1"))
        .synced(hh, file(ROOT, "h", "h"));
    w.remote(hh, Some(file(ROOT, "x.txt", "h")));
    w.local(n, Some(file(ROOT, "x.txt", "2")));
    w.local(r, None);
    w.local(
        hh,
        Some(NodeState {
            content: None,
            ..file(ROOT, "h", "h2")
        }),
    );
    w.input.local_unhashed.insert(hh);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { .. } | Op::RemoteDelete { .. })),
        "{:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&n), "{:?}", plan.deferred);
}

/// A chain of name waits five long: n1 would take the name of a file being written, n2 n1's,
/// and so on, and the drive deleted n5's folder, where n5 has an edit here. Each plan again
/// finds the next waiter (the folders judged before the top: ids below the root's), till none
/// is new: n5's folder waits, never deleted with its edit (a cap on the plans did).
#[test]
fn a_chain_of_name_waits_holds_its_last_folder() {
    let id = |n: u128| NodeId(Uuid::from_u128(n));
    let [g1, g2, g3, g4, g5] =
        [5, 4, 3, 2, 1].map(|k| id(0x1000_0000_0000_4000_8000_0000_0000_0000 + k));
    let [n1, n2, n3, n4, n5, hh] =
        [1, 2, 3, 4, 5, 6].map(|k| id(0xf000_0000_0000_4000_8000_0000_0000_0000 + k));
    let mut w = World::default();
    for (g, name) in [(g1, "g1"), (g2, "g2"), (g3, "g3"), (g4, "g4"), (g5, "g5")] {
        w.synced(g, folder(ROOT, name));
    }
    let names = [
        (n1, g1, "a1"),
        (n2, g2, "b2"),
        (n3, g3, "c3"),
        (n4, g4, "d4"),
        (n5, g5, "e5"),
    ];
    for (n, g, name) in names {
        w.synced(n, file(g, name, "1"));
    }
    // Each onto the place of the one before; n1 onto x0 at the top.
    w.remote(n1, Some(file(ROOT, "x0", "1")));
    for pair in names.windows(2) {
        let [(_, g, name), (n, _, _)] = pair else {
            unreachable!()
        };
        w.remote(*n, Some(file(*g, name, "1")));
    }
    w.remote(g5, None);
    w.local(n5, Some(file(g5, "e5", "2")));
    w.local(
        hh,
        Some(NodeState {
            content: None,
            ..file(ROOT, "x0", "h")
        }),
    );
    w.input.local_unhashed.insert(hh);
    let plan = w.plan();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDelete { .. } | Op::RemoteDelete { .. })),
        "{:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&n5), "{:?}", plan.deferred);
    assert_eq!(plan.waiting, [hh]);
}

/// A folder holding a file being written, planned all the same (the drive tagged it: it stays
/// put), keeps its name: a new file here whose name clashes with it waits, and so does the
/// folder's tag, never the folder renamed on both sides with the file in it.
#[test]
fn a_waiting_folder_planned_keeps_its_name() {
    let [x, g, wf] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(g, folder(ROOT, "g")).synced(wf, file(g, "w", "w"));
    let red = Tags::new([Tag::new("Red", 6)]);
    w.remote(
        g,
        Some(NodeState {
            tags: red,
            ..folder(ROOT, "g")
        }),
    );
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(g, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    w.local(x, Some(file(ROOT, "G", "x")));
    let plan = w.plan();
    assert!(
        !plan.ops.iter().any(|op| matches!(op,
            Op::LocalMove { node, .. } | Op::RemoteUpdate { node, .. } if *node == g)),
        "{:#?}",
        plan.ops
    );
    assert!(plan.deferred.contains(&x), "{:?}", plan.deferred);
    assert!(
        plan.ops.is_empty(),
        "the clash waits whole: {:#?}",
        plan.ops
    );
}

/// A new folder here holding a file being written, and a new folder of the same name on the
/// drive: neither goes, never a second "Photos" on the drive (nor the drive's renamed for
/// good); once the file is hashed, a name clash as any.
#[test]
fn a_new_waiting_folder_and_a_new_drive_folder_of_its_name_wait() {
    let [mine, theirs, wf] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.local(mine, Some(folder(ROOT, "Photos")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(mine, "w", "1")
        }),
    );
    w.input.local_unhashed.insert(wf);
    w.remote(theirs, Some(folder(ROOT, "Photos")));
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
}

/// A file edited on both sides, and a file being written here under the name its conflict
/// copy would take: the fork waits (its copy would land on the taken name), and isn't made
/// on a later plan either.
#[test]
fn a_fork_onto_the_name_of_a_file_not_hashed_yet_waits() {
    let [x, u] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(x, file(ROOT, "x.txt", "1"));
    w.remote(x, Some(file(ROOT, "x.txt", "theirs")));
    w.local(x, Some(file(ROOT, "x.txt", "mine")));
    let copy = hbsync_engine::tree::conflict_copy_name("x.txt", &w.opts.device, &w.opts.date);
    w.local(
        u,
        Some(NodeState {
            content: None,
            ..file(ROOT, &copy, "u")
        }),
    );
    w.input.local_unhashed.insert(u);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.deferred.contains(&x), "{:?}", plan.deferred);
}

/// A confirmed folder that holds a file being written, and isn't deleted (its delete undone),
/// confirms nothing in it: deletes here of what is in it are held for the user, as any. Only
/// what was in a confirmed folder whose own delete waits goes.
#[test]
fn a_confirmed_folder_that_stays_confirms_nothing_in_it() {
    let v = ids(8);
    let (f, wf, files) = (v[0], v[1], &v[2..]);
    let mut w = World::default();
    w.opts.guarding.settings.min = 2;
    w.synced(f, folder(ROOT, "f"));
    for (i, n) in files.iter().enumerate() {
        w.synced(*n, file(f, &format!("x{i}"), "1"));
    }
    w.synced(wf, file(f, "w", "w"));
    for n in files {
        w.local(*n, None);
    }
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(f, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    w.opts.guarding.confirmed = std::sync::Arc::new(std::collections::HashSet::from([f]));
    let plan = w.plan();
    assert_eq!(plan.held.len(), files.len(), "{plan:#?}");
    assert!(plan.delete_waits.is_empty(), "{:?}", plan.delete_waits);

    // The folder deleted here too: its delete waits, confirmed, and what was in it goes.
    w.local(f, None);
    let plan = w.plan();
    assert!(plan.held.is_empty(), "{plan:#?}");
    assert_eq!(plan.delete_waits, [f]);
}

/// A folder holding a file being written, renamed in case only on the drive ("photos" to
/// "Photos"): it holds no name against itself, and an edit in it goes up as ever.
#[test]
fn a_waiting_folder_renamed_in_case_holds_up_nothing_in_it() {
    let [h, wf, a, other] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(h, folder(ROOT, "photos"))
        .synced(wf, file(h, "w", "w"))
        .synced(a, file(h, "a.txt", "1"))
        .synced(other, file(ROOT, "o", "1"));
    w.remote(h, Some(folder(ROOT, "Photos")));
    w.remote(other, Some(file(ROOT, "o", "2")));
    w.local(a, Some(file(h, "a.txt", "2")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(h, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, upload: true, .. } if *node == a)),
        "{:#?}",
        plan.ops
    );
    assert!(
        !plan.ops.iter().any(|op| op.node() == h),
        "the rename waits: {:#?}",
        plan.ops
    );
}

/// A folder holding a file being written clashes with a new drive folder of its name: that
/// one waits, and an edit inside the waiting folder goes up all the same.
#[test]
fn a_clash_with_a_waiting_folder_holds_up_only_the_clash() {
    let [h, wf, a, theirs] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(h, folder(ROOT, "photos"))
        .synced(wf, file(h, "w", "w"))
        .synced(a, file(h, "a.txt", "1"));
    w.remote(theirs, Some(folder(ROOT, "Photos")));
    w.local(a, Some(file(h, "a.txt", "2")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(h, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, upload: true, .. } if *node == a)),
        "{:#?}",
        plan.ops
    );
    assert!(
        !plan.ops.iter().any(|op| op.node() == theirs),
        "{:#?}",
        plan.ops
    );
}

/// Edited on both sides, waiting for its conflict copy's name (a file being written has it): the
/// user is told the copy waits.
#[test]
fn a_fork_waiting_for_a_taken_name_is_told() {
    let [x, u] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(x, file(ROOT, "x.txt", "1"));
    w.remote(x, Some(file(ROOT, "x.txt", "theirs")));
    w.local(x, Some(file(ROOT, "x.txt", "mine")));
    let copy = hbsync_engine::tree::conflict_copy_name("x.txt", &w.opts.device, &w.opts.date);
    w.local(
        u,
        Some(NodeState {
            content: None,
            ..file(ROOT, &copy, "u")
        }),
    );
    w.input.local_unhashed.insert(u);
    let plan = w.plan();
    assert_eq!(plan.conflicts, [Conflict::CopyWaits { node: x }]);
}

/// Here a file being written moved from f0 to zz, and each f_k renamed to f_(k-1): a chain of
/// waits through names on the drive. All of it waits, nothing renamed, in few plans.
#[test]
fn a_chain_through_drive_names_waits_whole() {
    let n = 30;
    let v = ids(n + 1);
    let (l, files) = (v[0], &v[1..]);
    let mut w = World::default();
    w.synced(l, file(ROOT, "f00", "l"));
    for (k, f) in files.iter().enumerate() {
        w.synced(*f, file(ROOT, &format!("f{:02}", k + 1), "1"));
    }
    w.local(
        l,
        Some(NodeState {
            content: None,
            ..file(ROOT, "zz", "l2")
        }),
    );
    w.input.local_unhashed.insert(l);
    for (k, f) in files.iter().enumerate() {
        w.local(*f, Some(file(ROOT, &format!("f{k:02}"), "1")));
    }
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    assert_eq!(plan.deferred.len(), n + 1, "{:?}", plan.deferred);
}

/// A folder holding a file being written, alone under a name a node this device doesn't sync
/// holds on the drive (§10): never renamed with its file in it.
#[cfg(target_os = "linux")]
#[test]
fn a_waiting_folder_on_a_masked_name_is_never_renamed() {
    let [b, wf, masked, other] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("/Build\n"),
        ..World::default()
    };
    w.synced(b, folder(ROOT, "build"))
        .synced(wf, file(b, "w", "w"))
        .synced(other, file(ROOT, "o", "1"));
    w.remote(masked, Some(file(ROOT, "Build", "x")));
    w.remote(other, Some(file(ROOT, "o", "2")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(b, "w", "w2")
        }),
    );
    w.input.local_unhashed.insert(wf);
    let plan = w.plan();
    assert!(!plan.ops.iter().any(|op| op.node() == b), "{:#?}", plan.ops);
}

/// File `f` was `d`; here it is renamed `L.txt` and tagged Red, and a folder `d` another
/// device made, which this device ignores, takes the name `d` on the drive (§10: its holder
/// keeps it). Planned in a page without `f` (another page plans it, §6.5), nothing renames `f`
/// for that clash: its own page plans it with its change, as one page does (the rename and the
/// tag go up). Before, the clash renamed it `d (conflict 1)` from the drive's version, and the
/// user's rename and tag were lost.
#[cfg(target_os = "linux")]
#[test]
fn a_clash_never_renames_what_another_page_plans() {
    let [f, d, n] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("/d/\n"),
        ..World::default()
    };
    w.synced(f, file(ROOT, "d", "1"));
    let mut renamed = file(ROOT, "L.txt", "1");
    renamed.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(f, Some(renamed));
    w.remote(d, Some(folder(ROOT, "d")));
    w.local(n, Some(file(ROOT, "n", "n")));
    // Whole: the rename and the tag go up.
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, name, .. }
            if *node == f && name == "L.txt")),
        "{:#?}",
        plan.ops
    );
    // In a page of `n` alone: `f` is left to its page.
    w.input.dirty = [n].into();
    w.input.dirty_elsewhere.insert(f);
    let plan = w.plan();
    assert!(!plan.ops.iter().any(|op| op.node() == f), "{:#?}", plan.ops);
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    assert!(plan.uncovered.is_empty(), "{:?}", plan.uncovered);
    // Only tagged, in its own page: still on `d`, it is renamed for the clash there.
    let mut tagged = file(ROOT, "d", "1");
    tagged.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(f, Some(tagged));
    w.input.dirty = [f].into();
    w.input.dirty_elsewhere = [n].into();
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteUpdate { node, name, .. }
            if *node == f && name == "d (conflict 1)")),
        "{:#?}",
        plan.ops
    );
}

/// A new folder here holding a file being written, alone under a name a node this device
/// doesn't sync holds on the drive (§10): it waits, never made beside it under that name.
#[cfg(target_os = "linux")]
#[test]
fn a_new_waiting_folder_on_a_masked_name_waits() {
    let [b, wf, masked] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("/Build\n"),
        ..World::default()
    };
    w.local(b, Some(folder(ROOT, "build")));
    w.local(
        wf,
        Some(NodeState {
            content: None,
            ..file(b, "w", "1")
        }),
    );
    w.input.local_unhashed.insert(wf);
    w.remote(masked, Some(file(ROOT, "Build", "x")));
    let plan = w.plan();
    assert!(!plan.ops.iter().any(|op| op.node() == b), "{:#?}", plan.ops);
}

/// A file inside a folder that waits for a taken name, edited alike on both sides: no conflict
/// copy waits, none is told (only a fork's copy that waited is).
#[test]
fn the_same_edit_in_a_waiting_folder_tells_no_conflict() {
    let [u, wfold, x] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(u, file(ROOT, "u", "1"))
        .synced(wfold, folder(ROOT, "w"))
        .synced(x, file(wfold, "x", "1"));
    // u renamed here to v, not hashed yet: its drive name u is taken; w renamed here to u.
    w.local(
        u,
        Some(NodeState {
            content: None,
            ..file(ROOT, "v", "1")
        }),
    );
    w.input.local_unhashed.insert(u);
    w.local(wfold, Some(folder(ROOT, "u")));
    w.local(x, Some(file(wfold, "x", "2")));
    w.remote(x, Some(file(wfold, "x", "2")));
    let plan = w.plan();
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
}

/// A file being written here in folder p, which the drive moved into a folder this device
/// ignores, and then deleted p: the item here is detached (a new id, §10) and holds p, and
/// what waits for it is told by the id the store knows it by, so it is read early.
#[test]
fn a_detached_file_waits_by_its_stored_id() {
    let [p, b, x] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("build/\n"),
        ..World::default()
    };
    w.synced(p, folder(ROOT, "p")).synced(x, file(p, "x", "1"));
    w.remote(b, Some(folder(ROOT, "build")));
    w.remote(x, Some(file(b, "x", "1")));
    w.remote(p, None);
    w.local(
        x,
        Some(NodeState {
            content: None,
            ..file(p, "x", "2")
        }),
    );
    w.input.local_unhashed.insert(x);
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalDetach { node, .. } if *node == x)),
        "{plan:#?}"
    );
    assert!(
        !plan.ops.iter().any(|op| op.node() == p),
        "p's delete waits: {plan:#?}"
    );
    assert_eq!(plan.waiting, [x]);
}

/// A file in a folder that waits for a taken name, edited unlike on both sides: its conflict
/// copy waits with it, and the user is told so.
#[test]
fn a_fork_in_a_waiting_folder_is_told() {
    let [u, wfold, x] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(u, file(ROOT, "u", "1"))
        .synced(wfold, folder(ROOT, "w"))
        .synced(x, file(wfold, "x", "1"));
    w.local(
        u,
        Some(NodeState {
            content: None,
            ..file(ROOT, "v", "1")
        }),
    );
    w.input.local_unhashed.insert(u);
    w.local(wfold, Some(folder(ROOT, "u")));
    w.local(x, Some(file(wfold, "x", "mine")));
    w.remote(x, Some(file(wfold, "x", "theirs")));
    let plan = w.plan();
    assert_eq!(
        plan.conflicts,
        [Conflict::CopyWaits { node: x }],
        "{plan:#?}"
    );
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
}

/// Another device removed `d` and stopped: `x` and `y` in it are taken along, live. Here `x`
/// was edited, which keeps `d` alive: restored, and restoring a folder restores what was
/// deleted with it (node schema, Recently deleted), so `y`, unchanged, stays as it is, also
/// when it is in the plan (dirty: marked, a page of its own). Before, a `y` in the plan read
/// as taken along and was deleted while `d` came back; one out of the plan stayed.
#[test]
fn a_folder_an_edit_restores_keeps_what_its_delete_took_along() {
    let [d, x, y] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(d, folder(ROOT, "d"))
        .synced(x, file(d, "x", "x"))
        .synced(y, file(d, "y", "y"));
    w.remote(d, None);
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    for n in [x, y] {
        w.input.changed_at.insert(n, 10);
    }
    w.local(x, Some(file(d, "x", "edited")));
    for y_dirty in [false, true] {
        if y_dirty {
            w.input.dirty.insert(y);
        }
        let plan = w.plan();
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == d)),
            "y dirty: {y_dirty}: {:#?}",
            plan.ops
        );
        assert!(
            !plan.ops.iter().any(|op| op.node() == y),
            "y dirty: {y_dirty}: {:#?}",
            plan.ops
        );
    }
}

/// As [`a_folder_an_edit_restores_keeps_what_its_delete_took_along`], `d` restored by its own
/// change here (renamed): what its removal took along, unchanged, stays with it too, in the
/// plan (`y`, dirty) or not (`h`).
#[test]
fn a_folder_restored_by_its_own_change_keeps_what_its_delete_took_along() {
    let [d, y, h] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(d, folder(ROOT, "d"))
        .synced(y, file(d, "y", "y"))
        .synced(h, folder(d, "h"));
    w.remote(d, None);
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    for n in [y, h] {
        w.input.changed_at.insert(n, 10);
    }
    w.local(d, Some(folder(ROOT, "e")));
    w.input.dirty.insert(y);
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == d)),
        "{:#?}",
        plan.ops
    );
    assert!(
        !plan.ops.iter().any(|op| [y, h].contains(&op.node())),
        "{:#?}",
        plan.ops
    );
}

/// As [`a_folder_restored_by_its_own_change_keeps_what_its_delete_took_along`], `y` dirty
/// (tagged here: no change that keeps it on its own) and planned in a page without `d`, a
/// piece of `d`'s too big for one (`Options::piece`): `y` waits for `d`'s plan, which keeps
/// it. Planned apart, `y` went with the removal while `d` came back (§6.5).
#[test]
fn what_a_removal_took_along_waits_for_its_folder_in_another_page() {
    let [d, y] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(d, folder(ROOT, "d")).synced(y, file(d, "y", "y"));
    w.remote(d, None);
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    w.input.changed_at.insert(y, 10);
    w.local(d, Some(folder(ROOT, "e")));
    let mut tagged = file(d, "y", "y");
    tagged.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(y, Some(tagged));
    // Planned whole: `d` comes back, and `y` with it, tagged.
    let plan = w.plan();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == d)),
        "{:#?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .all(|op| op.node() != y || matches!(op, Op::RemoteTags { .. })),
        "{:#?}",
        plan.ops
    );
    // A piece without `d`, which another page plans: `y` waits, nothing done.
    w.input.dirty.remove(&d);
    w.input.dirty_elsewhere.insert(d);
    w.input.going_above.insert(d);
    w.opts.piece = true;
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    assert!(plan.deferred.contains(&y), "{plan:#?}");
    // Moved here, `y` plans the same whatever `d`'s plan: it doesn't wait.
    let mut moved = file(ROOT, "y", "y");
    moved.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(y, Some(moved));
    let plan = w.plan();
    assert!(!plan.deferred.contains(&y), "{plan:#?}");
}

/// Another device removed `p` and stopped after `d` in it: `y` in `d` is taken along, live.
/// Here `d` is renamed (it comes back, into `p`) and `y` tagged, in a page with them both
/// but not `p`, which another page plans (a piece of `p`'s too big for one). `d`'s restore
/// waits for `p`'s page (it goes into a folder the drive doesn't hold yet), and so does `y`,
/// which stays only if `d` does: all three planned together next, as in one page. Before, `y`
/// went with the removal (into Recently deleted apart from its folder, its tag lost) while
/// `d` waited, and came back after (§6.5).
#[test]
fn what_a_removal_took_along_waits_with_its_folder_placed_with_another_page() {
    let [p, d, y] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(p, folder(ROOT, "p"))
        .synced(d, folder(p, "d"))
        .synced(y, file(d, "y", "y"));
    w.remote(p, None).remote(d, None);
    for (n, at, state) in [(p, 100, folder(ROOT, "p")), (d, 101, folder(p, "d"))] {
        w.input
            .removed
            .insert(n, hbsync_engine::reconcile::Removed { at, state });
    }
    w.input.changed_at.insert(y, 10);
    w.local(d, Some(folder(p, "e")));
    let mut tagged = file(d, "y", "y");
    tagged.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(y, Some(tagged));
    // Planned whole: `p` and `d` come back, and `y` with them, tagged.
    let plan = w.plan();
    for folder in [p, d] {
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, Op::RemoteCreate { node, .. } if *node == folder)),
            "{:#?}",
            plan.ops
        );
    }
    let of_y: Vec<&Op> = plan.ops.iter().filter(|op| op.node() == y).collect();
    assert!(
        matches!(of_y[..], [Op::RemoteTags { .. }]),
        "{:#?}",
        plan.ops
    );
    // A piece with `d` and `y`, without `p`: they wait for `p`, nothing done.
    w.input.dirty.remove(&p);
    w.input.dirty_elsewhere.insert(p);
    w.input.going_above.insert(p);
    w.opts.piece = true;
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:#?}", plan.ops);
    let mut together = vec![p, d, y];
    together.sort();
    assert_eq!(plan.waits_together, [together], "{plan:#?}");
}

/// Another device removed `d` and stopped: `y` in it is taken along, live, and tagged here.
/// Here `d` moved into `q` and `r` into `d`; another device moved `q` into `r`, and another
/// page plans `q`: a cycle across pages, so `d` and `r` wait where the drive has them, to be
/// planned with `q` (§6.5). `y` stays only if `d` does, which that page decides: it waits
/// with them. Before, it went with the removal while `d` came back next.
#[test]
fn what_a_removal_took_along_waits_with_its_folder_in_a_cycle_across_pages() {
    let [d, q, r, y] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.input.remote_complete = true;
    w.synced(d, folder(ROOT, "d"))
        .synced(q, folder(ROOT, "q"))
        .synced(r, folder(ROOT, "r"))
        .synced(y, file(d, "y", "y"));
    w.remote(d, None).remote(q, Some(folder(r, "q")));
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    w.input.changed_at.insert(y, 10);
    w.local(d, Some(folder(q, "d")))
        .local(r, Some(folder(d, "r")));
    let mut tagged = file(d, "y", "y");
    tagged.tags = Tags::new([Tag::new("Red", 6)]);
    w.local(y, Some(tagged));
    // Planned whole: `d` comes back, and `y` with it, tagged.
    let plan = w.plan();
    let of_y: Vec<&Op> = plan.ops.iter().filter(|op| op.node() == y).collect();
    assert!(
        matches!(of_y[..], [Op::RemoteTags { .. }]),
        "{:#?}",
        plan.ops
    );
    // `q` in another page: `y` waits with `d`, nothing done to it.
    w.input.dirty.remove(&q);
    w.input.dirty_elsewhere.insert(q);
    let plan = w.plan();
    assert!(!plan.ops.iter().any(|op| op.node() == y), "{:#?}", plan.ops);
    assert!(
        plan.waits_together
            .iter()
            .any(|g| g.contains(&d) && g.contains(&y)),
        "{plan:#?}"
    );
}

/// §7, §8: a folder removed on the drive; out of it first, to the top, a file moved and
/// changed there (downloaded to its new folder: a move and an update in one), and a folder
/// moved with a file in it removed. The plan moves them out, deletes that file in the folder
/// moved out (a delete of its own, after the move), and deletes the folder, which expects only
/// what goes with it.
#[test]
fn a_folder_delete_expects_what_the_plan_moves_out_of_it_gone() {
    let [d, a, b, g, x, k] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"))
        .synced(a, file(d, "a", "a"))
        .synced(b, file(d, "b", "b"))
        .synced(g, folder(d, "g"))
        .synced(x, file(g, "x", "x"))
        .synced(k, file(g, "k", "k"));
    w.remote(d, None)
        .remote(a, None)
        .remote(x, None)
        .remote(b, Some(file(ROOT, "b", "changed")))
        .remote(g, Some(folder(ROOT, "g")));
    let plan = w.plan();
    assert_eq!(
        kinds(&plan.ops),
        ["move", "download-replace", "delete", "delete"],
        "{:?}",
        plan.ops
    );
    assert_eq!(
        plan.ops[2..],
        [
            Op::LocalDelete {
                node: x,
                subtree: vec![x]
            },
            Op::LocalDelete {
                node: d,
                subtree: vec![d, a]
            }
        ]
    );
    assert!(plan.moves_out_of.contains(&d));
}

/// §7: a delete held as every synced file (`wiped`) of a folder a file moves out of first:
/// held, it stays in the dirty set while that move is planned (`held::out_of_dirty`, as when
/// the delete expected the file).
#[test]
fn a_held_delete_covers_what_the_plan_moves_out_of_it() {
    let [d, a, b] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d"))
        .synced(a, file(d, "a", "a"))
        .synced(b, file(d, "b", "b"));
    w.remote(d, None)
        .remote(a, None)
        .remote(b, Some(file(ROOT, "b", "b")));
    w.opts.guarding.wiped = std::sync::Arc::new([d].into_iter().collect());
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["move"]);
    assert_eq!(
        plan.held,
        [Op::LocalDelete {
            node: d,
            subtree: vec![d, a]
        }]
    );
    assert!(plan.moves_out_of.contains(&d));
    let (out, nodes) = hbsync_engine::held::out_of_dirty(&plan, &[d, a, b]);
    assert!(out.is_empty() && nodes.is_empty(), "{out:?} {nodes:?}");
}

/// §7: a removal elsewhere in a folder held as every synced file (`wiped`) stays held till
/// answered, whatever the plan moves meanwhile, in it or out of it. Each shape: folder W held,
/// folder M in it with X in it removed on the drive, and M (or W) moved there:
/// - M moved out of W (W removed): let through, and M's move failed or went stale, X's delete
///   would go under the held W, unasked (the plan after such a failed move is this one again:
///   nothing changed here or on the drive);
/// - M moved within W, from W/a to W/b;
/// - W itself moved (restored elsewhere, say), M with it.
#[test]
fn a_delete_in_a_held_folder_stays_held_whatever_moves() {
    #[derive(Debug)]
    enum Shape {
        OutOfHeld,
        WithinHeld,
        HeldMoves,
    }
    let mut let_through = Vec::new();
    for shape in [Shape::OutOfHeld, Shape::WithinHeld, Shape::HeldMoves] {
        let [p, wf, a, b, m, x, k] = ids(7)[..] else {
            unreachable!()
        };
        let mut w = World::default();
        w.synced(p, folder(ROOT, "P"))
            .synced(wf, folder(ROOT, "W"))
            .synced(a, folder(wf, "a"))
            .synced(b, folder(wf, "b"))
            .synced(m, folder(a, "M"))
            .synced(x, file(m, "X", "x"))
            .synced(k, file(m, "k", "k"));
        w.remote(x, None);
        match shape {
            Shape::OutOfHeld => w
                .remote(wf, None)
                .remote(a, None)
                .remote(b, None)
                .remote(m, Some(folder(ROOT, "M"))),
            Shape::WithinHeld => w.remote(m, Some(folder(b, "M"))),
            Shape::HeldMoves => w.remote(wf, Some(folder(p, "W"))),
        };
        w.opts.guarding.wiped = std::sync::Arc::new([wf].into_iter().collect());
        let plan = w.plan();
        // X's delete, its own after M's move, and where W goes too, W's, which doesn't expect
        // what M takes out of it.
        let x_delete = Op::LocalDelete {
            node: x,
            subtree: vec![x],
        };
        let held = match shape {
            Shape::OutOfHeld => vec![
                x_delete,
                Op::LocalDelete {
                    node: wf,
                    subtree: vec![wf, a, b],
                },
            ],
            Shape::WithinHeld | Shape::HeldMoves => vec![x_delete],
        };
        if plan.held != held || kinds(&plan.ops) != ["move"] {
            let_through.push(format!("{shape:?}: {:?} held {:?}", plan.ops, plan.held));
        }
    }
    assert!(let_through.is_empty(), "{let_through:#?}");
}

// ---- §13 Read-only folders ----

/// Each of `plan`'s ops by its kind and node, sorted.
fn sorted_ops(plan: &hbsync_engine::Plan) -> Vec<(&'static str, NodeId)> {
    let mut ops: Vec<_> = plan.ops.iter().map(|op| (op.kind(), op.node())).collect();
    ops.sort();
    ops
}

/// A read-only world: the drive decides (§13).
fn read_only() -> World {
    World {
        read_only: true,
        ..World::default()
    }
}

/// A file edited here moves out first, kept; nothing goes up, and the node stays to plan
/// again, for the drive's version to come back.
#[test]
fn read_only_an_edit_here_moves_out() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(f, file(ROOT, "a.txt", "a"))
        .local(f, Some(file(ROOT, "a.txt", "mine")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["evict"]);
    assert_eq!(
        plan.ops[0],
        Op::LocalEvict {
            node: f,
            subtree: vec![f]
        }
    );
    assert!(!plan.clean.contains(&f));
    assert!(plan.conflicts.is_empty());
}

/// Read-only is asked node by node: a file this identity may change on the drive syncs both
/// ways, so its edit and its tag here go up and a rename there comes down; the file beside
/// it follows the drive, its edit moving out. Nothing is written of any other node.
#[test]
fn read_only_a_node_that_may_be_changed_syncs_both_ways() {
    let [mine, renamed, theirs] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    let mut tagged = file(ROOT, "r.txt", "r");
    tagged.tags = Tags::new(vec![Tag::new("Red", 0)]);
    w.synced(mine, file(ROOT, "a.txt", "a"))
        .synced(renamed, file(ROOT, "r.txt", "r"))
        .synced(theirs, file(ROOT, "g.txt", "g"))
        .local(mine, Some(file(ROOT, "a.txt", "mine")))
        .local(renamed, Some(tagged))
        .remote(renamed, Some(file(ROOT, "r2.txt", "r")))
        .local(theirs, Some(file(ROOT, "g.txt", "mine too")));
    w.input.writable.extend([mine, renamed]);
    let plan = w.plan();
    let mut got: Vec<(&str, NodeId)> = kinds(&plan.ops)
        .into_iter()
        .zip(plan.ops.iter().map(Op::node))
        .collect();
    got.sort();
    let mut want = vec![
        ("evict", theirs),
        ("move", renamed),
        ("remote-tags", renamed),
        ("upload", mine),
    ];
    want.sort();
    assert_eq!(got, want);
    assert!(plan.conflicts.is_empty());

    // Not said of them, every one follows the drive.
    w.input.writable.clear();
    let plan = w.plan();
    assert!(!plan.writes_drive());
    // (The rename there comes down, and the tag set here is undone.)
    let mut want = vec![
        ("LocalEvict", mine),
        ("LocalEvict", theirs),
        ("LocalMove", renamed),
        ("LocalTags", renamed),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
}

/// A world where every item of the drive may be changed by this identity (§13).
fn editing(w: &mut World) {
    let all: Vec<NodeId> = w.input.remote.iter().map(|(n, _)| n).collect();
    w.input.writable.extend(all);
}

/// An item that may be changed, gone there, follows the drive: it goes here, and an edit of
/// it here moves out (it isn't made again there); taken along by its folder's removal
/// there, it isn't removed there by this device. Deleted here, it is removed there, to its
/// owner's Recently deleted; but not where the drive's changed since it synced here: that
/// one comes back.
#[test]
fn read_only_an_item_that_may_be_changed_gone_on_one_side() {
    let [deleted, deleted_changed, gone, edited_gone, d, along] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.input.remote_complete = true;
    w.synced(deleted, file(ROOT, "a.txt", "a"))
        .synced(deleted_changed, file(ROOT, "c.txt", "c"))
        .synced(gone, file(ROOT, "g.txt", "g"))
        .synced(edited_gone, file(ROOT, "e.txt", "e"))
        .synced(d, folder(ROOT, "d"))
        .synced(along, file(d, "x.txt", "x"));
    editing(&mut w);
    w.local(deleted, None)
        .local(deleted_changed, None)
        .remote(deleted_changed, Some(file(ROOT, "c.txt", "theirs")))
        .remote(gone, None)
        .remote(edited_gone, None)
        .local(edited_gone, Some(file(ROOT, "e.txt", "mine")))
        .remote(d, None);
    w.input.removed.insert(
        d,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: folder(ROOT, "d"),
        },
    );
    w.input.changed_at.insert(along, 10);
    let plan = w.plan();
    let mut want = vec![
        ("LocalDelete", d),
        ("LocalDelete", gone),
        ("LocalDownload", deleted_changed),
        ("LocalEvict", edited_gone),
        ("RemoteDelete", deleted),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);

    // The same bytes in the same place, at another version than it synced at (its owner
    // removed and restored it since, say): no delete made before that removes it.
    let mut w = read_only();
    w.synced(deleted, file(ROOT, "a.txt", "a"));
    editing(&mut w);
    w.local(deleted, None);
    let synced_at = VersionTag(Uuid::new_v4());
    w.input
        .base_extra
        .get_mut(&deleted)
        .expect("synced")
        .version = Some(synced_at);
    let plan = w.plan();
    assert_eq!(sorted_ops(&plan), [("LocalDownload", deleted)]);
    // At that version, it is removed.
    w.input
        .remote_refs
        .get_mut(&deleted)
        .expect("there")
        .version = synced_at;
    assert_eq!(sorted_ops(&w.plan()), [("RemoteDelete", deleted)]);
}

/// A folder deleted here is removed there with all that was in it only where all the drive
/// has in it, as this device reads it, may be changed: with one item in it that may only be
/// read, or one the drive changed or made since, the delete is undone, whole: the folder
/// comes back with all that was in it, and nothing is removed there.
#[test]
fn read_only_a_folder_deleted_here_goes_whole_or_comes_back_whole() {
    let build = |all_may_be_changed: bool, changed_there: bool| {
        let [d, sub, f, g, new_there] = ids(5)[..] else {
            unreachable!()
        };
        let mut w = read_only();
        // (The mass-delete guard off: it would hold a delete of every synced file, §7.)
        w.opts.guarding.settings.allow = true;
        w.synced(d, folder(ROOT, "d"))
            .synced(sub, folder(d, "sub"))
            .synced(f, file(d, "f.txt", "f"))
            .synced(g, file(sub, "g.txt", "g"));
        editing(&mut w);
        if !all_may_be_changed {
            w.input.writable.remove(&g);
        }
        for n in [d, sub, f, g] {
            w.local(n, None);
        }
        if changed_there {
            w.remote(new_there, Some(file(sub, "new.txt", "n")));
            w.input.writable.insert(new_there);
        }
        (w, [d, sub, f, g])
    };
    let (w, all) = build(true, false);
    let plan = w.plan();
    let mut want: Vec<(&str, NodeId)> = all.iter().map(|n| ("RemoteDelete", *n)).collect();
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
    // The folder before what is in it.
    let at = |n: NodeId| plan.ops.iter().position(|op| op.node() == n);
    assert!(
        at(all[0]) < at(all[1]) && at(all[1]) < at(all[3]),
        "{:?}",
        plan.ops
    );

    for (all_may, changed) in [(false, false), (true, true)] {
        let (w, all) = build(all_may, changed);
        let plan = w.plan();
        assert!(!plan.writes_drive(), "{:?}", plan.ops);
        for n in all {
            assert!(
                plan.ops.iter().any(|op| op.node() == n
                    && matches!(op, Op::LocalMkdir { .. } | Op::LocalDownload { .. })),
                "{n:?} doesn't come back: {:?}",
                plan.ops
            );
        }
    }
}

/// A file that may be changed, edited on both sides: the drive's wins and this device's
/// moves out, as of any file of the folder. No copy is made (it would be a new item there).
#[test]
fn read_only_a_file_that_may_be_changed_edited_on_both_sides_moves_out() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(f, file(ROOT, "a.txt", "a"));
    editing(&mut w);
    w.local(f, Some(file(ROOT, "a.txt", "mine")))
        .remote(f, Some(file(ROOT, "a.txt", "theirs")));
    let plan = w.plan();
    assert_eq!(sorted_ops(&plan), [("LocalEvict", f)]);
    assert!(plan.conflicts.is_empty());
    assert!(!plan.clean.contains(&f));
}

/// An item that may be changed is renamed where it is, never moved to another folder: a
/// move here is undone, whole, and an edit made with it still goes up, under the drive's
/// name in the drive's folder. A rename in its folder goes up.
#[test]
fn read_only_a_move_here_of_an_item_that_may_be_changed_is_undone() {
    let [d, moved, moved_edited, renamed] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(d, folder(ROOT, "d"))
        .synced(moved, file(ROOT, "m.txt", "m"))
        .synced(moved_edited, file(ROOT, "e.txt", "e"))
        .synced(renamed, file(d, "r.txt", "r"));
    editing(&mut w);
    w.local(moved, Some(file(d, "m2.txt", "m")))
        .local(moved_edited, Some(file(d, "e.txt", "mine")))
        .local(renamed, Some(file(d, "r2.txt", "r")));
    let plan = w.plan();
    let mut want = vec![
        ("LocalMove", moved),
        ("LocalMove", moved_edited),
        ("RemoteUpdate", moved_edited),
        ("RemoteUpdate", renamed),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
    for op in &plan.ops {
        match op {
            Op::RemoteUpdate {
                node,
                parent,
                name,
                upload,
                ..
            } if *node == moved_edited => {
                assert_eq!((*parent, name.as_str(), *upload), (ROOT, "e.txt", true));
            }
            Op::RemoteUpdate {
                node,
                parent,
                name,
                upload,
                ..
            } if *node == renamed => {
                assert_eq!((*parent, name.as_str(), *upload), (d, "r2.txt", false));
            }
            Op::LocalMove {
                node, parent, name, ..
            } if *node == moved => {
                assert_eq!((*parent, name.as_str()), (ROOT, "m.txt"));
            }
            _ => {}
        }
    }
}

/// A rename here of an item that may be changed, onto a name another item of the drive
/// holds, isn't sent: two of one name there would be the owner's to mend. The rename is
/// undone here, and an edit made with it goes up under the drive's name. So onto a name this
/// device leaves out of the drive (§10).
#[test]
fn read_only_a_rename_onto_a_taken_name_is_undone() {
    let [w1, w2, newcomer, w3, ignored] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("*.log\n"),
        ..read_only()
    };
    w.synced(w1, file(ROOT, "a.txt", "a"))
        .synced(w2, file(ROOT, "c.txt", "c"))
        .synced(w3, file(ROOT, "n.txt", "n"))
        .remote(newcomer, Some(file(ROOT, "b.txt", "b")))
        .remote(ignored, Some(file(ROOT, "x.log", "x")));
    editing(&mut w);
    w.local(w1, Some(file(ROOT, "b.txt", "a")))
        .local(w2, Some(file(ROOT, "B.TXT", "mine")))
        .local(w3, Some(file(ROOT, "x.log", "n")));
    let plan = w.plan();
    // Each rename undone here; the edit made with one goes up under the drive's name; the
    // newcomer comes down onto its name once that is free here.
    let mut want = vec![
        ("LocalDownload", newcomer),
        ("LocalMove", w1),
        ("LocalMove", w2),
        ("LocalMove", w3),
        ("RemoteUpdate", w2),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want, "{:?}", plan.conflicts);
    let sent = plan.ops.iter().find(|op| op.writes_remote());
    assert!(
        matches!(sent, Some(Op::RemoteUpdate { name, upload: true, .. }) if name == "c.txt"),
        "{sent:?}"
    );
}

/// A rename here to its own name spelled otherwise (case) goes up, also where another item
/// holds that name with it (two shared without their folders, both at the top; or a clash
/// the drive has already): the rename makes none.
#[test]
fn read_only_a_rename_in_case_only_goes_up_beside_another_of_the_name() {
    let [mine, other] = ids(2)[..] else {
        unreachable!()
    };
    // (The lower id holds the name here: the other was left out.)
    let (mine, other) = (mine.min(other), mine.max(other));
    let mut w = read_only();
    w.synced(mine, file(ROOT, "a", "1"))
        .remote(other, Some(file(ROOT, "a", "2")));
    editing(&mut w);
    w.local(mine, Some(file(ROOT, "A", "1")));
    let plan = w.plan();
    let sent: Vec<&Op> = plan.ops.iter().filter(|op| op.writes_remote()).collect();
    assert!(
        matches!(sent[..], [Op::RemoteUpdate { node, name, upload: false, .. }] if *node == mine && name == "A"),
        "{:?}",
        plan.ops
    );
}

/// What is new here in a folder this identity may change on the drive is added there, a new
/// folder with what is in it. What is new at the top, or in a folder it may only read, moves
/// out as ever.
#[test]
fn read_only_what_is_new_in_a_folder_that_may_be_changed_is_added() {
    let [to_edit, to_read, file_in, dir_in, below, at_top, in_read] = ids(7)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(to_edit, folder(ROOT, "edit"))
        .synced(to_read, folder(ROOT, "read"));
    w.input.writable.insert(to_edit);
    w.local(file_in, Some(file(to_edit, "new.txt", "n")))
        .local(dir_in, Some(folder(to_edit, "New")))
        .local(below, Some(file(dir_in, "below.txt", "b")))
        .local(at_top, Some(file(ROOT, "top.txt", "t")))
        .local(in_read, Some(file(to_read, "r.txt", "r")));
    let plan = w.plan();
    let mut want = vec![
        ("LocalEvict", at_top),
        ("LocalEvict", in_read),
        ("RemoteCreate", below),
        ("RemoteCreate", dir_in),
        ("RemoteCreate", file_in),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
    // What the drive has in Recently deleted isn't new here: it isn't made again there.
    let mut gone = read_only();
    gone.synced(to_edit, folder(ROOT, "edit"));
    gone.input.writable.insert(to_edit);
    gone.local(file_in, Some(file(to_edit, "new.txt", "n")));
    gone.input.removed.insert(
        file_in,
        hbsync_engine::reconcile::Removed {
            at: 100,
            state: file(to_edit, "new.txt", "old"),
        },
    );
    assert!(!gone.plan().writes_drive(), "{:?}", gone.plan().ops);

    // The folder before what is in it.
    let made: Vec<NodeId> = (plan.ops.iter().filter(|op| op.writes_remote()))
        .map(Op::node)
        .collect();
    let at = |n: NodeId| made.iter().position(|m| *m == n);
    assert!(at(dir_in) < at(below), "{:?}", plan.ops);
}

/// A delete here takes no edit. `inner` may be changed and was edited here, in `dir`, whose
/// share is withdrawn (gone from what this identity reads) while `inner`'s stays: the drive
/// has it at the top now, where `top` holds its name, so it is left out and its edit doesn't
/// go up. `dir`'s delete here doesn't take the edit: it moves out first. Not edited, or
/// edited to the bytes the drive has, it goes with its folder (the drive has it).
#[test]
fn read_only_a_delete_here_takes_no_edit_of_what_was_left_out() {
    for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
        let id = ids(3);
        let [top, dir, inner] = order.map(|i| id[i]);
        for (here, there, moves_out) in [("mine", "x", true), ("x", "x", false), ("y", "y", false)]
        {
            let mut w = read_only();
            w.synced(top, file(ROOT, "a.txt", "t"))
                .synced(dir, folder(ROOT, "d"))
                .synced(inner, file(dir, "a.txt", "x"));
            w.remote(dir, None)
                .remote(inner, Some(file(ROOT, "a.txt", there)));
            editing(&mut w);
            w.local(inner, Some(file(dir, "a.txt", here)));
            let plan = w.plan();
            assert!(!plan.writes_drive(), "{order:?}: {:?}", plan.ops);
            let mut want = vec![("LocalDelete", dir)];
            if moves_out {
                want.push(("LocalEvict", inner));
            }
            want.sort();
            assert_eq!(sorted_ops(&plan), want, "{order:?} {here}");
            let mut taken = (plan.ops.iter())
                .find_map(|op| match op {
                    Op::LocalDelete { subtree, .. } => Some(subtree.clone()),
                    _ => None,
                })
                .expect("the folder goes here");
            taken.sort();
            let mut all = match moves_out {
                true => vec![dir],
                false => vec![dir, inner],
            };
            all.sort();
            assert_eq!(taken, all, "{order:?} {here}: {:?}", plan.ops);
        }
    }
}

/// The same a level down: a folder `sub` of `dir` is the one left out (shared still, its name
/// at the top taken), and holds the edited file, and one new here (it would be added there,
/// `sub` its to add to, were `sub` planned). The two files alone move out; `sub` goes with
/// `dir`. And the plan after it ran (the file's base forgotten as it moved out, `dir` and
/// `sub` gone here): nothing to do, and no removal there of the file that moved out.
#[test]
fn read_only_a_delete_here_takes_no_edit_below_a_folder_left_out() {
    let [top, dir, sub, inner, fresh] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(top, folder(ROOT, "s"))
        .synced(dir, folder(ROOT, "d"))
        .synced(sub, folder(dir, "s"))
        .synced(inner, file(sub, "a.txt", "x"));
    w.remote(dir, None).remote(sub, Some(folder(ROOT, "s")));
    editing(&mut w);
    w.local(inner, Some(file(sub, "a.txt", "mine")))
        .local(fresh, Some(file(sub, "new.txt", "n")));
    let plan = w.plan();
    assert!(!plan.writes_drive(), "{:?}", plan.ops);
    let mut want = vec![
        ("LocalDelete", dir),
        ("LocalEvict", fresh),
        ("LocalEvict", inner),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
    let subtree = |op: &Op| match op {
        Op::LocalDelete { subtree, .. } | Op::LocalEvict { subtree, .. } => subtree.clone(),
        _ => Vec::new(),
    };
    let mut taken: Vec<Vec<NodeId>> = plan.ops.iter().map(subtree).collect();
    taken.iter_mut().for_each(|t| t.sort());
    taken.sort();
    let mut all = vec![vec![fresh], vec![inner], {
        let mut d = vec![dir, sub];
        d.sort();
        d
    }];
    all.sort();
    assert_eq!(taken, all, "{:?}", plan.ops);

    let mut after = read_only();
    after
        .synced(top, folder(ROOT, "s"))
        .remote(sub, Some(folder(ROOT, "s")))
        .remote(inner, Some(file(sub, "a.txt", "x")));
    editing(&mut after);
    let plan = after.plan();
    assert!(plan.ops.is_empty(), "{:?}", plan.ops);
    // For its name still: `sub` is left out, told so, and planned again.
    assert!(
        matches!(plan.conflicts[..], [Conflict::NameTaken { .. }]),
        "{:?}",
        plan.conflicts
    );
    assert_eq!(plan.conflicts[0].node(), sub);
    assert!(plan.deferred.contains(&sub));
}

/// A file edited here whose folder on the drive this device can't read (a newer writer's,
/// say) waits, out of the plan, its move out with it. Its folder here goes (its share
/// withdrawn): the edit moves out first, whether the folder writes there or only reads.
#[test]
fn read_only_a_file_that_waits_for_a_folder_it_cant_read_moves_its_edit_out() {
    let [b, f, q] = ids(3)[..] else {
        unreachable!()
    };
    for writes in [false, true] {
        let mut w = read_only();
        w.synced(b, folder(ROOT, "b"))
            .synced(f, file(b, "f.txt", "x"));
        w.remote(b, None).remote(f, Some(file(q, "f.txt", "x")));
        w.input.unreadable.insert(q);
        if writes {
            editing(&mut w);
        }
        w.local(f, Some(file(b, "f.txt", "mine")));
        let plan = w.plan();
        assert!(!plan.writes_drive(), "{writes}: {:?}", plan.ops);
        let mut want = vec![("LocalDelete", b), ("LocalEvict", f)];
        want.sort();
        assert_eq!(sorted_ops(&plan), want, "{writes}: {:?}", plan.ops);
        let taken = (plan.ops.iter())
            .find_map(|op| match op {
                Op::LocalDelete { subtree, .. } => Some(subtree.clone()),
                _ => None,
            })
            .expect("the folder goes here");
        assert_eq!(taken, [b], "{writes}: {:?}", plan.ops);
    }
}

/// A file edited on both sides, in a folder this identity may add to: both are kept, as on
/// its own drive. The drive's version keeps the file, and this device's becomes a copy
/// beside it, added there.
#[test]
fn read_only_a_file_edited_on_both_sides_where_it_may_add_keeps_both() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(d, folder(ROOT, "d"))
        .synced(f, file(d, "a.txt", "a"));
    editing(&mut w);
    w.local(f, Some(file(d, "a.txt", "mine")))
        .remote(f, Some(file(d, "a.txt", "theirs")));
    let plan = w.plan();
    let [Conflict::EditEdit { node, copy }] = plan.conflicts[..] else {
        panic!("{:?}", plan.conflicts);
    };
    assert_eq!(node, f);
    let mut want = vec![
        ("LocalDownload", f),
        ("LocalFork", f),
        ("RemoteCreate", copy),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
}

/// An item that may be changed moves between two folders this identity may add to; a move
/// out of one into a folder it may only read, or to the top, is undone.
#[test]
fn read_only_a_move_between_folders_that_may_be_changed_goes_up() {
    let [from, to, to_read, moved, into_read, to_top] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(from, folder(ROOT, "from"))
        .synced(to, folder(ROOT, "to"))
        .synced(to_read, folder(ROOT, "read"))
        .synced(moved, file(from, "m.txt", "m"))
        .synced(into_read, file(from, "r.txt", "r"))
        .synced(to_top, file(from, "t.txt", "t"));
    editing(&mut w);
    w.input.writable.remove(&to_read);
    w.local(moved, Some(file(to, "m2.txt", "m")))
        .local(into_read, Some(file(to_read, "r.txt", "r")))
        .local(to_top, Some(file(ROOT, "t.txt", "t")));
    let plan = w.plan();
    let mut want = vec![
        ("LocalMove", into_read),
        ("LocalMove", to_top),
        ("RemoteUpdate", moved),
    ];
    want.sort();
    assert_eq!(sorted_ops(&plan), want);
    let sent = plan.ops.iter().find(|op| op.writes_remote());
    assert!(
        matches!(sent, Some(Op::RemoteUpdate { parent, name, upload: false, .. })
            if *parent == to && name == "m2.txt"),
        "{sent:?}"
    );
}

/// What is added to a folder of the drive under a name another item holds there goes under
/// a numbered name: no two of one name are made on the owner's drive. A move onto a taken
/// name is undone.
#[test]
fn read_only_what_is_added_onto_a_taken_name_gets_a_numbered_one() {
    let [d, e, newcomer, added, moved] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(d, folder(ROOT, "d"))
        .synced(e, folder(ROOT, "e"))
        .synced(moved, file(e, "m.txt", "m"))
        .remote(newcomer, Some(file(d, "b.txt", "theirs")));
    editing(&mut w);
    w.local(added, Some(file(d, "b.txt", "mine")))
        .local(moved, Some(file(d, "B.TXT", "m")));
    let plan = w.plan();
    for op in &plan.ops {
        match op {
            Op::RemoteCreate {
                node, parent, name, ..
            } => {
                assert_eq!((*node, *parent), (added, d), "{op:?}");
                assert_ne!(name.to_lowercase(), "b.txt", "{op:?}");
            }
            op => assert!(!op.writes_remote(), "{op:?}"),
        }
    }
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::RemoteCreate { .. })),
        "{:?}",
        plan.ops
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, Op::LocalMove { node, parent, name, .. }
            if *node == moved && *parent == e && name == "m.txt")),
        "{:?}",
        plan.ops
    );
    assert!(
        plan.conflicts
            .contains(&Conflict::NameClash { node: added }),
        "{:?}",
        plan.conflicts
    );
}

/// A new folder here moves out as one, with what is in it; a new file at the top too.
#[test]
fn read_only_new_items_move_out_whole() {
    let [d, g, n] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.local(d, Some(folder(ROOT, "New")))
        .local(g, Some(file(d, "g.txt", "g")))
        .local(n, Some(file(ROOT, "n.txt", "n")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    let mut evicts: Vec<(NodeId, Vec<NodeId>)> = plan
        .ops
        .iter()
        .filter_map(|op| match op {
            Op::LocalEvict { node, subtree } => Some((*node, subtree.clone())),
            _ => None,
        })
        .collect();
    evicts.sort();
    let mut whole = vec![d, g];
    whole.sort();
    let mut want = vec![(d, whole), (n, vec![n])];
    want.sort();
    assert_eq!(evicts, want);
}

/// A rename and a tag here are undone, a delete here comes back: the drive's place, tags and
/// item, nothing moved out.
#[test]
fn read_only_moves_tags_and_deletes_here_are_undone() {
    let [f, g] = ids(2)[..] else { unreachable!() };
    let mut w = read_only();
    let mut tagged = file(ROOT, "b.txt", "a");
    tagged.tags = Tags::new(vec![Tag::new("Red", 0)]);
    w.synced(f, file(ROOT, "a.txt", "a"))
        .synced(g, file(ROOT, "g.txt", "g"))
        .local(f, Some(tagged))
        .local(g, None);
    let plan = w.plan();
    assert!(!plan.writes_drive());
    let mut got = kinds(&plan.ops);
    got.sort();
    assert_eq!(got, ["download", "local-tags", "move"]);
}

/// A file the drive's owner edited while this device only renamed it: the drive's bytes come
/// down to the drive's place, the rename undone with them.
#[test]
fn read_only_a_rename_here_loses_to_the_drive() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(f, file(ROOT, "a.txt", "a"))
        .local(f, Some(file(ROOT, "mine.txt", "a")))
        .remote(f, Some(file(ROOT, "a.txt", "theirs")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["download-replace"]);
    assert!(matches!(&plan.ops[0], Op::LocalDownload { name, .. } if name == "a.txt"));
}

/// A synced file moved into a folder made here: the file moves back first, then the folder
/// moves out without it.
#[test]
fn read_only_a_new_folder_moves_out_after_what_moves_back_out_of_it() {
    let [f, n] = ids(2)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(f, file(ROOT, "a.txt", "a"))
        .local(n, Some(folder(ROOT, "New")))
        .local(f, Some(file(n, "a.txt", "a")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["move", "evict"]);
    assert_eq!(
        plan.ops[1],
        Op::LocalEvict {
            node: n,
            subtree: vec![n]
        }
    );
}

/// The drive's owner deleted a folder this device added a file to: the file moves out first,
/// then the folder's delete here takes the rest, not it.
#[test]
fn read_only_a_new_file_in_a_folder_deleted_there_moves_out_before_the_delete() {
    let [d, f, g] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(d, folder(ROOT, "Docs"))
        .synced(f, file(d, "a.txt", "a"))
        .local(g, Some(file(d, "g.txt", "g")))
        .remote(d, None)
        .remote(f, None);
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["evict", "delete"]);
    assert_eq!(
        plan.ops[0],
        Op::LocalEvict {
            node: g,
            subtree: vec![g]
        }
    );
    let mut rest = vec![d, f];
    rest.sort();
    assert_eq!(
        plan.ops[1],
        Op::LocalDelete {
            node: d,
            subtree: rest
        }
    );
}

/// The same item made on both sides (adopted: same id, same bytes) isn't moved out.
#[test]
fn read_only_the_drives_own_bytes_here_stay() {
    let [f] = ids(1)[..] else { unreachable!() };
    let mut w = read_only();
    w.local(f, Some(file(ROOT, "a.txt", "a")))
        .remote(f, Some(file(ROOT, "a.txt", "a")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["settle"]);
}

/// An orphan (its folder gone for good from the drive) goes nowhere on the drive: what is here
/// of it goes, unchanged, or moves out, changed; never into Recovered files.
#[test]
fn read_only_orphans_are_let_go_of() {
    let [f, g, gone] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.input.remote_complete = true;
    w.synced(f, file(ROOT, "a.txt", "a"))
        .synced(g, file(ROOT, "g.txt", "g"))
        .remote(f, Some(file(gone, "a.txt", "a")))
        .remote(g, Some(file(gone, "g.txt", "g")))
        .local(g, Some(file(ROOT, "g.txt", "mine")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(plan.conflicts.is_empty());
    let got = sorted_ops(&plan);
    let mut want = vec![("LocalDelete", f), ("LocalEvict", g)];
    want.sort();
    assert_eq!(got, want);
}

/// What mends the drive is its owner's, also of nodes this identity may change there: an
/// orphan is let go of (never removed there, never moved into Recovered files), a name
/// clash's loser is left out (never renamed there), and a cycle there isn't undone there.
#[test]
fn read_only_no_repair_writes_the_drive_whatever_may_be_changed() {
    // Orphans.
    let [f, g, gone] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.input.remote_complete = true;
    w.synced(f, file(ROOT, "a.txt", "a"))
        .synced(g, file(ROOT, "g.txt", "g"))
        .remote(f, Some(file(gone, "a.txt", "a")))
        .remote(g, Some(file(gone, "g.txt", "g")))
        .local(g, Some(file(ROOT, "g.txt", "mine")));
    w.input.writable.extend([f, g]);
    let plan = w.plan();
    assert!(!plan.writes_drive(), "{:?}", plan.ops);
    let got = sorted_ops(&plan);
    let mut want = vec![("LocalDelete", f), ("LocalEvict", g)];
    want.sort();
    assert_eq!(got, want);

    // A clash of names.
    let [old, new] = ids(2)[..] else {
        unreachable!()
    };
    let (old, new) = (new, old);
    let mut w = read_only();
    w.synced(old, file(ROOT, "a.txt", "a"))
        .remote(new, Some(file(ROOT, "A.TXT", "b")));
    w.input.writable.extend([old, new]);
    let plan = w.plan();
    assert!(!plan.writes_drive(), "{:?}", plan.ops);
    assert_eq!(plan.conflicts, [Conflict::NameTaken { node: new }]);

    // A cycle on the drive (another writer's).
    let [a, b] = ids(2)[..] else { unreachable!() };
    let mut w = read_only();
    w.synced(a, folder(ROOT, "a"))
        .synced(b, folder(ROOT, "b"))
        .remote(a, Some(folder(b, "a")))
        .remote(b, Some(folder(a, "b")));
    w.input.writable.extend([a, b]);
    let plan = w.plan();
    assert!(!plan.writes_drive(), "{:?}", plan.ops);

    // The folder whose move the repair undoes here follows the drive in its tags too: one
    // set here is undone, not left as if it were synced.
    let red = |s: NodeState| NodeState {
        tags: Tags::new(vec![Tag::new("Red", 0)]),
        ..s
    };
    w.local(a, Some(red(folder(ROOT, "a"))))
        .local(b, Some(red(folder(ROOT, "b"))));
    let plan = w.plan();
    let [Conflict::Cycle { node: victim }] = plan.conflicts[..] else {
        panic!("{:?}", plan.conflicts);
    };
    let its: Vec<&str> = (sorted_ops(&plan).into_iter())
        .filter(|(_, n)| *n == victim)
        .map(|(kind, _)| kind)
        .collect();
    assert!(its.contains(&"LocalTags"), "{:?}", plan.ops);
    assert!(
        plan.ops
            .iter()
            .all(|op| !op.writes_remote() || op.node() != victim),
        "{:?}",
        plan.ops
    );
}

/// Two of the drive's items whose names clash here: the one held here keeps its name, the other
/// isn't synced here (it can't be renamed on the drive), and says why.
#[test]
fn read_only_a_name_clash_leaves_the_newcomer_out() {
    let [old, new] = ids(2)[..] else {
        unreachable!()
    };
    // The newcomer has the lower id: it would win the name both ways.
    let (old, new) = (new, old);
    let mut w = read_only();
    w.synced(old, file(ROOT, "a.txt", "a"))
        .remote(new, Some(file(ROOT, "A.TXT", "b")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(plan.ops.is_empty(), "{:?}", plan.ops);
    assert_eq!(plan.conflicts, [Conflict::NameTaken { node: new }]);
    assert!(plan.deferred.contains(&new));
}

/// Two of the drive's items meet on a name that a third, synced here, holds on disk, and one
/// of the two is the folder that third is in on the drive now: the folder takes the name (it
/// is made, and the item here moves into it), whatever the ids say. With the name given to
/// the other, its download would wait for the name the item here holds, which would wait for
/// its folder, which waited for the name: for good (filesync #419).
#[test]
fn read_only_the_folder_the_holder_of_a_name_moves_into_takes_it() {
    let [newcomer, held, dir] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(held, file(ROOT, "a", "x"))
        .remote(held, Some(file(dir, "a", "x")))
        .remote(dir, Some(folder(ROOT, "a")))
        .remote(newcomer, Some(file(ROOT, "a", "y")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["mkdir", "move"], "{:?}", plan.ops);
    assert_eq!(plan.conflicts, [Conflict::NameTaken { node: newcomer }]);
    assert!(plan.deferred.contains(&newcomer));
}

/// A folder left out for its name keeps what is in it where it is here, and what that holds
/// in ANOTHER folder here keeps its name there. `inner` synced at the top (shared without its
/// folder); its folder `dir` comes, to a name `other` holds here, and is left out; a newcomer
/// of `inner`'s name at the top is left out too. It was downloaded onto `inner`'s name before
/// (the top was judged without `inner`, which the plan had in `dir`): refused by the disk,
/// at every sync.
#[test]
fn read_only_what_a_folder_left_out_holds_elsewhere_keeps_its_name() {
    for order in [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2]] {
        let id = ids(4);
        let [newcomer, inner, dir, other] = order.map(|i| id[i]);
        let mut w = read_only();
        w.synced(other, folder(ROOT, "a"))
            .synced(inner, file(ROOT, "c.txt", "x"))
            .remote(inner, Some(file(dir, "c.txt", "x")))
            .remote(dir, Some(folder(ROOT, "a")))
            .remote(newcomer, Some(file(ROOT, "C.txt", "y")));
        let plan = w.plan();
        assert!(!plan.writes_drive());
        assert!(plan.ops.is_empty(), "{order:?}: {:?}", plan.ops);
        let mut left: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
        left.sort();
        let mut want = vec![dir, newcomer];
        want.sort();
        assert_eq!(left, want, "{order:?}: {:?}", plan.conflicts);
        assert!(
            (plan.conflicts.iter()).all(|c| matches!(c, Conflict::NameTaken { .. })),
            "{:?}",
            plan.conflicts
        );
        assert!(plan.deferred.contains(&newcomer), "{order:?}");
    }
}

/// The same with the item held in a folder judged BEFORE the one the names meet in (folders
/// are judged in the order of their ids; the top is last): `inner` is synced in `held_in`,
/// the drive has it in `dir` now, which loses its name in `clash_in`, a later folder. The
/// newcomer of `inner`'s name in `held_in` is left out, that folder judged again.
#[test]
fn read_only_what_a_folder_left_out_holds_in_a_folder_judged_before_keeps_its_name() {
    let [held_in, newcomer, inner, dir, other, clash_in] = ids(6)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(held_in, folder(ROOT, "h"))
        .synced(clash_in, folder(ROOT, "q"))
        .synced(other, folder(clash_in, "a"))
        .synced(inner, file(held_in, "c.txt", "x"))
        .remote(inner, Some(file(dir, "c.txt", "x")))
        .remote(dir, Some(folder(clash_in, "a")))
        .remote(newcomer, Some(file(held_in, "c.txt", "y")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(plan.ops.is_empty(), "{:?}", plan.ops);
    let mut left: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    left.sort();
    let mut want = vec![dir, newcomer];
    want.sort();
    assert_eq!(left, want, "{:?}", plan.conflicts);
    assert!(plan.deferred.contains(&newcomer));
}

/// The item synced here keeps its name though this device renamed it meanwhile: the rename is
/// undone, and the newcomer (the lower id) is the one left out.
#[test]
fn read_only_the_synced_item_keeps_its_name_through_a_rename_here() {
    let [newcomer, synced] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(synced, file(ROOT, "a.txt", "a"))
        .local(synced, Some(file(ROOT, "b.txt", "a")))
        .remote(newcomer, Some(file(ROOT, "A.txt", "new")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["move"]);
    assert_eq!(plan.conflicts, [Conflict::NameTaken { node: newcomer }]);
}

/// A file edited here that the drive moved onto a name taken here, out of a folder it deleted:
/// the edit still moves out (only its syncing waits for the clash), and the folder's delete
/// here doesn't take it.
#[test]
fn read_only_an_edited_clash_loser_moves_out_before_its_folder_goes() {
    let [d1, d2, edited, held] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(d1, folder(ROOT, "D1"))
        .synced(d2, folder(ROOT, "D2"))
        .synced(held, file(d2, "a.txt", "a"))
        .synced(edited, file(d1, "x", "x"))
        .local(edited, Some(file(d1, "x", "mine")))
        .remote(edited, Some(file(d2, "A.TXT", "x")))
        .remote(d1, None);
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["evict", "delete"]);
    assert_eq!(
        plan.ops[0],
        Op::LocalEvict {
            node: edited,
            subtree: vec![edited]
        }
    );
    assert_eq!(
        plan.ops[1],
        Op::LocalDelete {
            node: d1,
            subtree: vec![d1]
        }
    );
    assert_eq!(plan.conflicts, [Conflict::NameTaken { node: edited }]);
}

/// `f` as executable as `x`.
fn executable(mut s: NodeState, x: bool) -> NodeState {
    if let Some(c) = s.content.as_mut() {
        c.executable = x;
    }
    s
}

/// The executable bit of a node that may be changed is as in a folder that syncs both ways:
/// set here, it goes up; of the file beside it, which follows the drive, it stays as it is.
/// A file this device ignores since it synced follows the drive whatever it says: an ignore
/// rule here removes nothing from another identity's drive, and a bit set on such a file is
/// no change to keep.
#[test]
fn read_only_an_executable_bit_of_a_node_that_may_be_changed_goes_up() {
    let [mine, theirs, log, set_log] = ids(4)[..] else {
        unreachable!()
    };
    let mut w = World {
        filter: ignoring("*.log\n"),
        ..read_only()
    };
    for (node, name, set) in [
        (mine, "m.sh", true),
        (theirs, "t.sh", true),
        (log, "a.log", false),
        (set_log, "b.log", true),
    ] {
        w.synced(node, file(ROOT, name, "x"))
            .local(node, Some(executable(file(ROOT, name, "x"), set)));
    }
    w.input.writable.extend([mine, log, set_log]);
    let plan = w.plan();
    let got = sorted_ops(&plan);
    let mut want = vec![("Forget", log), ("Forget", set_log), ("RemoteUpdate", mine)];
    want.sort();
    assert_eq!(got, want);
}

/// An executable bit changed here stays as it is: nothing moves out, nothing comes down; the
/// drive's change of the bit comes down; and an edit there of a file whose bit only changed
/// here comes down over it, nothing moved out (its bytes are the ones synced).
#[test]
fn read_only_executable_bits_follow_the_drive_where_it_changed_them() {
    let [here, there, both] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(here, file(ROOT, "here.sh", "h"))
        .synced(there, file(ROOT, "there.sh", "t"))
        .synced(both, file(ROOT, "both.sh", "b"))
        .local(here, Some(executable(file(ROOT, "here.sh", "h"), true)))
        .remote(there, Some(executable(file(ROOT, "there.sh", "t"), true)))
        .local(both, Some(executable(file(ROOT, "both.sh", "b"), true)))
        .remote(both, Some(file(ROOT, "both.sh", "edited")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    let mut got: Vec<(&str, NodeId)> = plan
        .ops
        .iter()
        .filter(|op| !matches!(op, Op::Settle { .. }))
        .map(|op| (op.kind(), op.node()))
        .collect();
    got.sort();
    let mut want = vec![("LocalDownload", there), ("LocalDownload", both)];
    want.sort();
    assert_eq!(got, want);
}

/// A clash loser left where it is here holds its name there: what the drive puts on that name
/// is left out too, never planned onto it (it waited forever for the name).
#[test]
fn read_only_a_left_out_loser_holds_the_name_it_has_here() {
    let [kept, loser, newcomer] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(kept, file(ROOT, "a", "a"))
        .synced(loser, file(ROOT, "b", "b"))
        .remote(loser, Some(file(ROOT, "A", "b")))
        .remote(newcomer, Some(file(ROOT, "b", "new")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { .. })),
        "{:?}",
        plan.ops
    );
    let mut told: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    told.sort();
    let mut want = vec![loser, newcomer];
    want.sort();
    assert_eq!(told, want);
}

/// [`read_only_a_left_out_loser_holds_the_name_it_has_here`] where that name is in another
/// folder, one judged before the clash (its id the lower): judged again with it.
#[test]
fn read_only_a_left_out_loser_holds_its_name_in_a_folder_judged_before() {
    let [q, p, kept, loser, newcomer] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(q, folder(ROOT, "Q"))
        .synced(p, folder(ROOT, "P"))
        .synced(kept, file(p, "a", "a"))
        .synced(loser, file(q, "b", "b"))
        .remote(loser, Some(file(p, "A", "b")))
        .remote(newcomer, Some(file(q, "b", "new")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { .. })),
        "{:?}",
        plan.ops
    );
    let mut told: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    told.sort();
    told.dedup();
    let mut want = vec![loser, newcomer];
    want.sort();
    assert_eq!(told, want);
}

/// [`read_only_a_left_out_loser_holds_the_name_it_has_here`] where the loser changed here: it
/// moves out first, so it holds no name, and what the drive puts there comes down at once.
#[test]
fn read_only_a_left_out_loser_that_moves_out_holds_no_name() {
    let [kept, loser, newcomer] = ids(3)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(kept, file(ROOT, "a", "a"))
        .synced(loser, file(ROOT, "b", "b"))
        .local(loser, Some(file(ROOT, "b", "mine")))
        .remote(loser, Some(file(ROOT, "A", "b")))
        .remote(newcomer, Some(file(ROOT, "b", "new")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["evict", "download"], "{:?}", plan.ops);
    let told: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    assert_eq!(told, [loser]);
}

/// Read-only, a drive-side clash of names one apart in case: what is on the name here keeps
/// it, ahead of what was synced there and moved out (its id the lower), which is left out
/// instead of planned onto a name it can't take.
#[test]
fn read_only_what_is_on_a_name_here_keeps_it_from_what_moved_out() {
    let [moved, held] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(moved, file(ROOT, "Foo", "a"))
        .local(moved, None)
        .synced(held, file(ROOT, "foo", "b"));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, Op::LocalDownload { .. })),
        "{:?}",
        plan.ops
    );
    let told: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    assert_eq!(told, [moved]);
}

/// Read-only, two of the drive's items on one name, both synced here: the one on the name on
/// disk keeps it and follows the drive, and the one parked under a temporary name for it
/// (reported at the place it belongs) is left out, though its id is the lower. The same where
/// this device masks something (`masked`: the plan is made from the masks' view).
#[test]
fn read_only_what_is_on_a_name_here_keeps_it_from_a_parked_item() {
    let [parked, held, build] = ids(3)[..] else {
        unreachable!()
    };
    for masked in [false, true] {
        let mut w = World {
            filter: ignoring("build/\n"),
            ..read_only()
        };
        w.synced(parked, file(ROOT, "a", "1"))
            .synced(held, file(ROOT, "a", "2"))
            .remote(held, Some(file(ROOT, "a", "3")));
        if masked {
            w.remote(build, Some(folder(ROOT, "build")));
        }
        w.input.parked.insert(parked);
        let plan = w.plan();
        assert!(!plan.writes_drive());
        let got: Vec<(&str, NodeId)> = (kinds(&plan.ops).into_iter())
            .zip(plan.ops.iter().map(Op::node))
            .collect();
        assert_eq!(got, [("download-replace", held)], "masked: {masked}");
        assert_eq!(plan.conflicts, [Conflict::NameTaken { node: parked }]);
    }
}

/// Read-only, a new folder here holding a file still being written waits for it, and so does
/// what else is new in it, a folder below included: they move out with it, as one, once it is
/// read (not one by one beside where it goes).
#[test]
fn read_only_what_is_in_a_new_folder_that_waits_waits_with_it() {
    let [n, u, h, sub, deep] = ids(5)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.local(n, Some(folder(ROOT, "New")))
        .local(
            u,
            Some(NodeState {
                content: None,
                ..file(n, "u", "?")
            }),
        )
        .local(h, Some(file(n, "h", "h")))
        .local(sub, Some(folder(n, "Sub")))
        .local(deep, Some(file(sub, "d", "d")));
    w.input.local_unhashed.insert(u);
    let plan = w.plan();
    assert!(plan.ops.is_empty(), "{:?}", plan.ops);
    assert!(!plan.writes_drive());
    // Once it is read, the folder moves out whole.
    w.input.local_unhashed.remove(&u);
    w.local(u, Some(file(n, "u", "u")));
    let plan = w.plan();
    let mut whole = vec![n, u, h, sub, deep];
    whole.sort();
    assert_eq!(
        plan.ops,
        [Op::LocalEvict {
            node: n,
            subtree: whole
        }]
    );
}

/// Read-only, an item synced here that is edited here keeps its name from one the drive has a
/// name one apart in case, left out till now: it moves out, and its drive version comes back
/// to that name (one edit doesn't hand the name over).
#[test]
fn read_only_an_edit_here_doesnt_hand_its_name_over() {
    let [synced, other] = ids(2)[..] else {
        unreachable!()
    };
    let mut w = read_only();
    w.synced(synced, file(ROOT, "a.txt", "a"))
        .local(synced, Some(file(ROOT, "a.txt", "mine")))
        .remote(other, Some(file(ROOT, "A.txt", "b")));
    let plan = w.plan();
    assert!(!plan.writes_drive());
    assert_eq!(kinds(&plan.ops), ["evict"], "{:?}", plan.ops);
    let told: Vec<NodeId> = plan.conflicts.iter().map(Conflict::node).collect();
    assert_eq!(told, [other]);
}
