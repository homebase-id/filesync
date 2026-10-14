//! A node tree keyed by node id, with the invariants of sync-semantics §6.
//!
//! The same type holds the base, local, remote and merged views. Parent pointers are node ids;
//! the root ([`NodeId::ROOT`]) is implicit and never stored.

use std::collections::{BTreeSet, HashMap};

use hbsync_platform::name::{MAX_NAME_BYTES, name_key, split_extension};
use hbsync_types::{NodeId, NodeKind, NodeState};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tree {
    nodes: HashMap<NodeId, NodeState>,
    children: HashMap<NodeId, BTreeSet<NodeId>>,
}

/// A broken invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Several nodes in one folder share a name under [`name_key`].
    DuplicateName { parent: NodeId, nodes: Vec<NodeId> },
    /// Following parents from `node` loops.
    Cycle { node: NodeId },
    /// The parent isn't in the tree (and isn't the root).
    Orphan { node: NodeId, parent: NodeId },
    /// The parent is a file.
    ParentNotFolder { node: NodeId, parent: NodeId },
}

impl Tree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn get(&self, node: NodeId) -> Option<&NodeState> {
        self.nodes.get(&node)
    }

    pub fn contains(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node)
    }

    /// `true` for the root and for every node in the tree.
    pub fn exists(&self, node: NodeId) -> bool {
        node.is_root() || self.contains(node)
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &NodeState)> {
        self.nodes.iter().map(|(id, s)| (*id, s))
    }

    pub fn ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.keys().copied()
    }

    /// Inserts or replaces a node, keeping the child index current.
    pub fn insert(&mut self, node: NodeId, state: NodeState) -> Option<NodeState> {
        let old = self.remove(node);
        self.children.entry(state.parent).or_default().insert(node);
        self.nodes.insert(node, state);
        old
    }

    pub fn remove(&mut self, node: NodeId) -> Option<NodeState> {
        let old = self.nodes.remove(&node)?;
        if let Some(siblings) = self.children.get_mut(&old.parent) {
            siblings.remove(&node);
            if siblings.is_empty() {
                self.children.remove(&old.parent);
            }
        }
        Some(old)
    }

    /// Direct children of `parent` (which may be the root), in node-id order.
    pub fn children(&self, parent: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.children.get(&parent).into_iter().flatten().copied()
    }

    /// The child of `parent` whose name has the same key as `name`, other than `except`.
    pub fn child_named(
        &self,
        parent: NodeId,
        name: &str,
        except: Option<NodeId>,
    ) -> Option<NodeId> {
        let key = name_key(name);
        self.children(parent)
            .filter(|c| Some(*c) != except)
            .find(|c| self.nodes.get(c).is_some_and(|s| name_key(&s.name) == key))
    }

    /// `node`'s parents up to (excluding) the root, nearest first. Stops at a missing node and
    /// never loops: a cycle ends the walk.
    pub fn ancestors(&self, node: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut seen = BTreeSet::from([node]);
        let mut current = self.nodes.get(&node).map(|s| s.parent);
        while let Some(parent) = current {
            if parent.is_root() || !seen.insert(parent) {
                break;
            }
            out.push(parent);
            current = self.nodes.get(&parent).map(|s| s.parent);
        }
        out
    }

    /// The names from the root down to `node`, or `None` if `node` isn't connected to the root
    /// (missing node on the way, or a cycle).
    pub fn path(&self, node: NodeId) -> Option<Vec<&str>> {
        let mut names = Vec::new();
        let mut current = node;
        let mut steps = 0usize;
        while !current.is_root() {
            let state = self.nodes.get(&current)?;
            names.push(state.name.as_str());
            current = state.parent;
            steps += 1;
            if steps > self.nodes.len() {
                return None; // cycle
            }
        }
        names.reverse();
        Some(names)
    }

    /// `node` and everything below it, parents before children.
    pub fn subtree(&self, node: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut stack = vec![node];
        while let Some(n) = stack.pop() {
            if !n.is_root() {
                out.push(n);
            }
            let mut kids: Vec<NodeId> = self.children(n).collect();
            kids.reverse();
            stack.extend(kids);
        }
        out
    }

    /// Whether moving `node` under `new_parent` would create a cycle.
    pub fn would_cycle(&self, node: NodeId, new_parent: NodeId) -> bool {
        !new_parent.is_root() && (new_parent == node || self.ancestors(new_parent).contains(&node))
    }

    /// Every broken invariant, in a deterministic order.
    pub fn violations(&self) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut ids: Vec<NodeId> = self.ids().collect();
        ids.sort();
        for &node in &ids {
            let parent = self.nodes[&node].parent;
            if parent.is_root() {
                continue;
            }
            match self.nodes.get(&parent) {
                None => out.push(Violation::Orphan { node, parent }),
                Some(p) if p.kind != NodeKind::Folder => {
                    out.push(Violation::ParentNotFolder { node, parent })
                }
                Some(_) => {}
            }
        }
        for &node in &ids {
            if self.path(node).is_none() && self.nodes.contains_key(&self.nodes[&node].parent) {
                // Connected parents but no path to the root: a cycle (report each member once).
                let cycle_member_min = self.cycle_members(node).into_iter().min();
                if cycle_member_min == Some(node) {
                    out.push(Violation::Cycle { node });
                }
            }
        }
        let mut parents: Vec<NodeId> = self.children.keys().copied().collect();
        parents.sort();
        for parent in parents {
            let mut by_key: HashMap<String, Vec<NodeId>> = HashMap::new();
            for child in self.children(parent) {
                by_key
                    .entry(name_key(&self.nodes[&child].name))
                    .or_default()
                    .push(child);
            }
            let mut groups: Vec<Vec<NodeId>> =
                by_key.into_values().filter(|g| g.len() > 1).collect();
            for g in &mut groups {
                g.sort();
            }
            groups.sort();
            out.extend(
                groups
                    .into_iter()
                    .map(|nodes| Violation::DuplicateName { parent, nodes }),
            );
        }
        out
    }

    /// The nodes on the cycle reachable from `node` (empty if none).
    pub fn cycle_members(&self, node: NodeId) -> Vec<NodeId> {
        let mut order = Vec::new();
        let mut current = node;
        loop {
            if current.is_root() {
                return Vec::new();
            }
            if let Some(i) = order.iter().position(|n| *n == current) {
                return order[i..].to_vec();
            }
            order.push(current);
            match self.nodes.get(&current) {
                Some(s) => current = s.parent,
                None => return Vec::new(),
            }
        }
    }
}

/// The first of `candidates` that `is_free`, preferring one `syncs` says this device syncs
/// (§10); `None` if none is free. How the executor and the simulator pick a numbered name.
pub fn first_free<E>(
    candidates: impl IntoIterator<Item = String>,
    mut is_free: impl FnMut(&str) -> Result<bool, E>,
    mut syncs: impl FnMut(&str) -> bool,
) -> Result<Option<String>, E> {
    let mut filtered = None;
    for candidate in candidates {
        if is_free(&candidate)? {
            if syncs(&candidate) {
                return Ok(Some(candidate));
            }
            filtered.get_or_insert(candidate);
        }
    }
    Ok(filtered)
}

/// The numbered variants of `name` tried for a free one, in order: a hundred at most.
pub fn numbered_names(name: &str) -> impl Iterator<Item = String> + '_ {
    (1..=100).map(move |n| numbered_conflict_name(name, n))
}

/// `"report (conflict 2).pdf"`: the name a node gets when another node keeps its name.
pub fn numbered_conflict_name(name: &str, n: usize) -> String {
    with_tag(name, &format!(" (conflict {n})"))
}

/// The names a restored item tries, in order: its own, then the restored variants; a hundred
/// at most.
pub fn restored_names(name: &str) -> impl Iterator<Item = String> + '_ {
    std::iter::once(name.to_owned()).chain((1..100).map(move |n| restored_name(name, n)))
}

/// `"report (restored).pdf"`, then `"report (restored 2).pdf"`: a restored item whose name
/// was taken meanwhile.
pub fn restored_name(name: &str, n: usize) -> String {
    match n {
        0 | 1 => with_tag(name, " (restored)"),
        n => with_tag(name, &format!(" (restored {n})")),
    }
}

/// `"report (conflict laptop 2026-10-08).pdf"`: the name of a conflict copy of an edit.
pub fn conflict_copy_name(name: &str, device: &str, date: &str) -> String {
    // The device name is configured; keep it from crowding out the file's own name.
    let device = truncate_bytes(device, 64);
    with_tag(name, &format!(" (conflict {device} {date})"))
}

/// Inserts `tag` before the extension, shortening the stem (or, if the extension is what makes
/// the name long, the whole name) so the result stays within [`MAX_NAME_BYTES`].
fn with_tag(name: &str, tag: &str) -> String {
    let (stem, ext) = split_extension(name);
    let (stem, ext) = if tag.len() + ext.len() < MAX_NAME_BYTES {
        (stem, ext)
    } else {
        (name, "")
    };
    let budget = MAX_NAME_BYTES.saturating_sub(tag.len() + ext.len());
    let stem = truncate_bytes(stem, budget);
    format!("{stem}{tag}{ext}")
}

/// The longest prefix of `s` of at most `max` bytes that ends on a character boundary.
fn truncate_bytes(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folder(parent: NodeId, name: &str) -> NodeState {
        NodeState::folder(parent, name)
    }

    fn ids(n: usize) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = (0..n).map(|_| NodeId::new()).collect();
        v.sort();
        v
    }

    #[test]
    fn paths_children_and_subtrees() {
        let [a, b, c] = ids(3)[..] else {
            unreachable!()
        };
        let mut t = Tree::new();
        t.insert(a, folder(NodeId::ROOT, "a"));
        t.insert(b, folder(a, "b"));
        t.insert(c, folder(b, "c"));
        assert_eq!(t.path(c), Some(vec!["a", "b", "c"]));
        assert_eq!(t.ancestors(c), vec![b, a]);
        assert_eq!(t.children(a).collect::<Vec<_>>(), [b]);
        assert_eq!(t.subtree(a), [a, b, c]);
        assert!(t.would_cycle(a, c));
        assert!(!t.would_cycle(c, a));
        assert_eq!(t.child_named(NodeId::ROOT, "A", None), Some(a));

        // Moving re-indexes children.
        t.insert(c, folder(NodeId::ROOT, "c"));
        assert_eq!(t.children(b).count(), 0);
        assert_eq!(t.path(c), Some(vec!["c"]));
        assert!(t.violations().is_empty());
    }

    #[test]
    fn violations_are_found() {
        let [a, b, c, d, e] = ids(5)[..] else {
            unreachable!()
        };
        let missing = NodeId::new();
        let mut t = Tree::new();
        t.insert(a, folder(NodeId::ROOT, "Same"));
        t.insert(b, folder(NodeId::ROOT, "same"));
        t.insert(c, folder(missing, "orphan"));
        // d and e point at each other.
        t.insert(d, folder(e, "d"));
        t.insert(e, folder(d, "e"));
        let v = t.violations();
        assert!(v.contains(&Violation::DuplicateName {
            parent: NodeId::ROOT,
            nodes: vec![a, b]
        }));
        assert!(v.contains(&Violation::Orphan {
            node: c,
            parent: missing
        }));
        assert!(v.contains(&Violation::Cycle { node: d.min(e) }));
        assert_eq!(
            v.iter()
                .filter(|x| matches!(x, Violation::Cycle { .. }))
                .count(),
            1
        );
        assert_eq!(t.path(d), None);
        assert_eq!(t.ancestors(d), vec![e]);
    }

    #[test]
    fn file_parent_is_a_violation() {
        let [f, x] = ids(2)[..] else { unreachable!() };
        let content = hbsync_types::FileContent {
            hash: hbsync_fileio::hash_bytes(b"", hbsync_fileio::HashAlgorithm::Blake3),
            size: 0,
            mtime_ms: 0,
            executable: false,
        };
        let mut t = Tree::new();
        t.insert(f, NodeState::file(NodeId::ROOT, "f", content));
        t.insert(x, folder(f, "x"));
        assert_eq!(
            t.violations(),
            [Violation::ParentNotFolder { node: x, parent: f }]
        );
    }

    proptest::proptest! {
        #[test]
        fn child_index_matches_parent_pointers(
            ops in proptest::collection::vec((0usize..8, 0usize..9, proptest::bool::ANY), 1..60)
        ) {
            let nodes = ids(8);
            let mut t = Tree::new();
            for (i, (who, parent, remove)) in ops.into_iter().enumerate() {
                let parent = if parent == 8 { NodeId::ROOT } else { nodes[parent] };
                if remove {
                    t.remove(nodes[who]);
                } else {
                    t.insert(nodes[who], folder(parent, &format!("n{i}")));
                }
                for (id, state) in t.iter() {
                    proptest::prop_assert!(t.children(state.parent).any(|c| c == id));
                }
                let indexed: usize = std::iter::once(NodeId::ROOT)
                    .chain(nodes.iter().copied())
                    .map(|p| t.children(p).count())
                    .sum();
                proptest::prop_assert_eq!(indexed, t.len());
            }
        }
    }

    #[test]
    fn the_first_free_name_one_that_syncs_if_any() {
        let names = || ["a", "b", "c", "d"].map(String::from);
        let free = |n: &str| Ok::<_, ()>(n != "a");
        assert_eq!(first_free(names(), free, |_| true), Ok(Some("b".into())));
        assert_eq!(
            first_free(names(), free, |n| n == "c"),
            Ok(Some("c".into()))
        );
        assert_eq!(first_free(names(), free, |_| false), Ok(Some("b".into())));
        assert_eq!(
            first_free(names(), |_| Ok::<_, ()>(false), |_| true),
            Ok(None)
        );
        assert_eq!(first_free(names(), |_| Err("db"), |_| true), Err("db"));
    }

    #[test]
    fn conflict_names() {
        assert_eq!(
            numbered_conflict_name("a.tar.gz", 2),
            "a.tar (conflict 2).gz"
        );
        assert_eq!(numbered_conflict_name(".bashrc", 1), ".bashrc (conflict 1)");
        assert_eq!(
            conflict_copy_name("Report.pdf", "laptop", "2026-10-08"),
            "Report (conflict laptop 2026-10-08).pdf"
        );
    }

    #[test]
    fn conflict_names_stay_within_the_name_limit() {
        use hbsync_platform::name::check_remote_name;
        let long_stem = format!("{}.txt", "é".repeat(125)); // 254 bytes
        let long_ext = format!("a.{}", "x".repeat(253));
        for name in [long_stem.as_str(), long_ext.as_str(), &"x".repeat(255)] {
            let numbered = numbered_conflict_name(name, 12);
            let copy = conflict_copy_name(name, &"d".repeat(300), "2026-10-08");
            for n in [&numbered, &copy] {
                assert!(n.len() <= MAX_NAME_BYTES, "{} bytes: {n}", n.len());
                assert_eq!(check_remote_name(n), Ok(()));
            }
            assert!(numbered.contains(" (conflict 12)"), "{numbered}");
            assert!(copy.contains(" 2026-10-08)"), "{copy}");
        }
        assert!(numbered_conflict_name(&long_stem, 1).ends_with(" (conflict 1).txt"));
        assert_ne!(
            numbered_conflict_name(&long_stem, 1),
            numbered_conflict_name(&long_stem, 2)
        );
    }
}
