//! One test per rule of `docs/spec/sync-semantics.md`.

use hbsync_engine::reconcile::file_state;
use hbsync_engine::{BaseExtra, Conflict, Input, Op, Options, RemoteRef, reconcile};
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_types::{
    ContentHash, ContentState, NodeId, NodeKind, NodeState, RemoteFileId, Uuid, VersionTag,
};

fn h(data: &str) -> ContentHash {
    hash_bytes(data.as_bytes(), HashAlgorithm::Blake3)
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
            Op::LocalTrash { .. } => "trash",
            Op::RemoteCreate { .. } => "create",
            Op::RemoteUpdate { upload: true, .. } => "upload",
            Op::RemoteUpdate { upload: false, .. } => "rename",
            Op::RemoteDelete { .. } => "delete",
            Op::Settle { .. } => "settle",
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
        upload: true
    }));
    assert!(plan.ops.contains(&Op::LocalDownload {
        node: b,
        parent: ROOT,
        name: "b".into(),
        replace: true
    }));
    assert!(plan.ops.iter().any(|op| matches!(op, Op::RemoteUpdate { node, upload: false, name, .. } if *node == c && name == "c-renamed")));
    assert!(plan.ops.contains(&Op::LocalMove {
        node: d,
        parent: ROOT,
        name: "d-renamed".into()
    }));
    assert!(plan.conflicts.is_empty());
}

#[test]
fn local_folder_delete_goes_bottom_up() {
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
        plan.ops.last().map(Op::node),
        Some(d),
        "the folder goes last"
    );
}

#[test]
fn remote_folder_delete_trashes_only_the_top() {
    let [d, f] = ids(2)[..] else { unreachable!() };
    let mut w = World::default();
    w.synced(d, folder(ROOT, "d")).synced(f, file(d, "f", "1"));
    w.remote(f, None).remote(d, None);
    let plan = w.plan();
    assert_eq!(plan.ops, [Op::LocalTrash { node: d }]);
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
        kind: NodeKind::File
    }));
    assert_eq!(
        plan.conflicts,
        [Conflict::EditEdit {
            node: f,
            copy: *new_node
        }]
    );
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
            name: "f".into()
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
    assert!(plan.ops.contains(&Op::LocalDownload {
        node: x,
        parent: ROOT,
        name: "x".into(),
        replace: false
    }));
    assert!(plan.ops.contains(&Op::RemoteCreate {
        node: y,
        parent: ROOT,
        name: "y".into(),
        kind: NodeKind::File
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
    assert!(plan.ops.contains(&Op::LocalMkdir {
        node: d,
        parent: ROOT,
        name: "d".into()
    }));
    assert!(plan.ops.contains(&Op::LocalDownload {
        node: new,
        parent: d,
        name: "new".into(),
        replace: false
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
    assert!(plan.ops.contains(&Op::LocalMove {
        node: a,
        parent: ROOT,
        name: "a".into()
    }));
    assert!(plan.ops.contains(&Op::LocalMove {
        node: b,
        parent: a,
        name: "b".into()
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
            assert!(plan.ops.contains(&Op::LocalDownload {
                node: high,
                parent: ROOT,
                name: "X (conflict 1).TXT".into(),
                replace: false
            }));
        } else {
            assert!(plan.ops.contains(&Op::RemoteCreate {
                node: high,
                parent: ROOT,
                name: "x (conflict 1).txt".into(),
                kind: NodeKind::File
            }));
            assert!(plan.ops.contains(&Op::LocalMove {
                node: high,
                parent: ROOT,
                name: "x (conflict 1).txt".into()
            }));
        }
    }
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
        },
    );
    w.input.remote_refs.insert(f, remote_ref());
    w.local(f, Some(file(ROOT, "f-renamed", "1")));
    let plan = w.plan();
    assert_eq!(kinds(&plan.ops), ["rename"], "{plan:?}");
}
