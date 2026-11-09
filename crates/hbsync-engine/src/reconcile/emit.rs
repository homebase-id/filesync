//! Step 4: the merged tree against each side, as operations in an order that can run (§8).

use super::*;

impl Reconciler<'_> {
    // ---- 4: operations ----

    pub(super) fn emit(&mut self) {
        let i = self.input;
        let mut forks = Vec::new();
        let mut creates = Vec::new(); // (depth, op)
        let mut moves = Vec::new();
        let mut content = Vec::new();
        let mut deletes = Vec::new(); // (depth, op)
        // The nodes whose item here a delete (or a forget) takes: their subtrees are filled in
        // once only the topmost of them is left.
        let mut gone_here: HashSet<NodeId> = HashSet::new();
        let mut settles = Vec::new();

        let scope: Vec<NodeId> = self.scope.iter().copied().collect();
        let fork_copies: HashSet<NodeId> = self.forks.values().copied().collect();
        for node in scope {
            let target = self.merged.get(node).cloned();
            let forked = self.forks.get(&node).copied();
            let local = if forked.is_some() {
                None
            } else {
                self.local_state(node)
            };
            let local_real = i.local.get(node);
            let remote = i.remote.get(node);
            let depth = |tree: &Tree| tree.ancestors(node).len();

            if let Some(copy) = forked {
                // The repairs made the fork wait where they took the copy out: one without its
                // state here is never made under the name "".
                let Some(name) = self.merged.get(copy).map(|s| s.name.clone()) else {
                    self.fork_waits(node);
                    continue;
                };
                forks.push(Op::LocalFork {
                    node,
                    new_node: copy,
                    name,
                });
            }

            // Remote side.
            let mut remote_ops = false;
            match (&target, remote) {
                (None, Some(now)) => {
                    if let Some(r) = i.remote_refs.get(&node) {
                        deletes.push((
                            depth(&i.remote),
                            Op::RemoteDelete {
                                node,
                                parent: now.parent,
                                file_id: r.file_id,
                                expected: r.version,
                            },
                        ));
                        remote_ops = true;
                    }
                }
                (Some(t), None) => {
                    let has_bytes = t.kind == NodeKind::Folder
                        || fork_copies.contains(&node)
                        || local.as_ref().is_some_and(|l| l.same_content(t));
                    if has_bytes {
                        creates.push((
                            self.merged.ancestors(node).len(),
                            Op::RemoteCreate {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                kind: t.kind,
                                tags: t.tags.clone(),
                            },
                        ));
                        remote_ops = true;
                    } else {
                        self.deferred.insert(node);
                    }
                }
                (Some(t), Some(r)) => {
                    let upload = self.origin.get(&node) == Some(&Origin::Local)
                        && t.kind == NodeKind::File
                        && !self.remote_has(r, t);
                    let retag = t.tags != r.tags;
                    if let Some(rr) = i.remote_refs.get(&node) {
                        if !t.same_place(r) || upload {
                            let op = Op::RemoteUpdate {
                                node,
                                file_id: rr.file_id,
                                expected: rr.version,
                                parent: t.parent,
                                name: t.name.clone(),
                                upload,
                                tags: t.tags.clone(),
                            };
                            if upload {
                                content.push(op)
                            } else {
                                moves.push(op)
                            }
                            remote_ops = true;
                        } else if retag {
                            content.push(Op::RemoteTags {
                                node,
                                file_id: rr.file_id,
                                expected: rr.version,
                                tags: t.tags.clone(),
                            });
                            remote_ops = true;
                        }
                    }
                }
                (None, None) => {}
            }

            // Local side (a fork copy already exists locally). A local write carries the
            // drive's version the plan saw: the base settles against it.
            let drive = || {
                Some(Box::new(DriveVersion {
                    state: i.remote.get(node)?.clone(),
                    at: *i.remote_refs.get(&node)?,
                    base_at: i.base_extra.get(&node).and_then(|e| e.version),
                }))
            };
            let mut local_ops = false;
            if !fork_copies.contains(&node) {
                match (&target, local_real.filter(|_| forked.is_none())) {
                    (None, Some(_)) => {
                        let subtree = Vec::new(); // filled in below, for the topmost
                        let op = if self.masks.masked.contains(&node) {
                            Op::Forget { node, subtree }
                        } else {
                            Op::LocalDelete { node, subtree }
                        };
                        gone_here.insert(node);
                        deletes.push((depth(&i.local), op));
                        local_ops = true;
                    }
                    (Some(t), None) => {
                        if t.kind == NodeKind::Folder {
                            creates.push((
                                self.merged.ancestors(node).len(),
                                Op::LocalMkdir {
                                    node,
                                    parent: t.parent,
                                    name: t.name.clone(),
                                    tags: t.tags.clone(),
                                    drive: drive(),
                                },
                            ));
                            local_ops = true;
                        } else if self.content_state(node) != ContentState::OnlineOnly {
                            content.push(Op::LocalDownload {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                replace: false,
                                tags: t.tags.clone(),
                                drive: drive(),
                            });
                            local_ops = true;
                        }
                    }
                    (Some(t), Some(l)) => {
                        let download = t.kind == NodeKind::File
                            && self.origin.get(&node) == Some(&Origin::Remote)
                            && !self.local_matches_remote(node, l, t);
                        if download {
                            // Downloading to the new place moves and updates in one step.
                            content.push(Op::LocalDownload {
                                node,
                                parent: t.parent,
                                name: t.name.clone(),
                                replace: true,
                                tags: t.tags.clone(),
                                drive: drive(),
                            });
                            local_ops = true;
                        } else {
                            if !t.same_place(l) {
                                moves.push(Op::LocalMove {
                                    node,
                                    parent: t.parent,
                                    name: t.name.clone(),
                                    drive: drive(),
                                });
                                local_ops = true;
                            }
                            if t.tags != l.tags {
                                content.push(Op::LocalTags {
                                    node,
                                    tags: t.tags.clone(),
                                    drive: drive(),
                                });
                                local_ops = true;
                            }
                        }
                    }
                    (None, None) => {}
                }
            }

            if !remote_ops && !local_ops && !self.deferred.contains(&node) {
                let needs_settle = match (&target, i.base.get(node)) {
                    (Some(t), Some(b)) => t != b || self.base_local_hash_stale(node, t),
                    (Some(_), None) | (None, Some(_)) => true,
                    (None, None) => false,
                };
                if needs_settle && target.is_none() && self.masks.masked.contains(&node) {
                    // A detached node's base goes with the detach.
                    if !self.masks.detached.contains_key(&node) {
                        let op = Op::Forget {
                            node,
                            subtree: Vec::new(),
                        };
                        deletes.push((depth(&i.base), op));
                    }
                } else if needs_settle {
                    settles.push(Op::Settle { node });
                } else if i.dirty.contains(&node) {
                    self.plan.clean.push(node);
                }
            }
        }

        // Parents before children for creates, all folders before the files; the local
        // operations first: the executor runs the others together on its pool, and each local
        // one between them would end a batch. Deletes: on the drive first, parents before
        // children (what a folder's delete takes along is removed no earlier than the folder:
        // node schema, Recently deleted); then here, children before parents (a folder's base is
        // forgotten only after its children's deletes, on either side; only the topmost local
        // delete of a subtree is needed).
        creates.sort_by_key(|(d, op)| (op.is_transfer(), op.pooled(), *d, op.node()));
        moves.sort_by_key(Op::pooled);
        content.sort_by_key(Op::pooled);
        // What this plan moves out of its folder here (a move, a download to another folder:
        // they run before the deletes) leaves a folder it deletes with what is below it.
        let moving = match gone_here.is_empty() {
            true => HashSet::new(),
            false => moving_out(moves.iter().chain(&content), &i.local),
        };
        // Only the topmost delete here of a subtree is needed.
        deletes.retain(|(_, op)| match op {
            Op::LocalDelete { node, .. } | Op::Forget { node, .. } if gone_here.contains(node) => {
                !below_another(&i.local, *node, &gone_here, &moving)
            }
            _ => true,
        });
        deletes.sort_by_key(|(d, op)| {
            let depth = *d as i64;
            (
                !op.pooled(),
                if op.pooled() { depth } else { -depth },
                op.node(),
            )
        });
        let mut deletes: Vec<Op> = deletes.into_iter().map(|(_, op)| op).collect();
        // What each takes here. A folder's delete expects what goes: not what the plan moves
        // out of it first, with what is below that. Where such a move doesn't run, the folder
        // holds what its delete doesn't expect: it goes stale and takes nothing. A forget, out
        // of view, takes it all (a folder there never holds such a node: one that holds an
        // item this device syncs is detached, `mask.rs`).
        for op in &mut deletes {
            match op {
                Op::LocalDelete { node, subtree } => {
                    let mut trimmed = false;
                    *subtree = i.local.subtree_where(*node, |n| {
                        let stays = !moving.contains(&n);
                        trimmed |= !stays;
                        stays
                    });
                    subtree.sort();
                    if trimmed {
                        self.plan.moves_out_of.insert(*node);
                    }
                }
                Op::Forget { node, subtree } if gone_here.contains(node) => {
                    *subtree = i.local.subtree(*node);
                    subtree.sort();
                }
                _ => {}
            }
        }

        self.plan.ops.extend(
            self.masks
                .detached
                .iter()
                .map(|(&node, &new_node)| Op::LocalDetach { node, new_node }),
        );
        self.plan.ops.extend(forks);
        self.plan.ops.extend(creates.into_iter().map(|(_, op)| op));
        self.plan.ops.extend(moves);
        self.plan.ops.extend(content);
        self.plan.ops.extend(deletes);
        self.plan.ops.extend(settles);
    }

    /// Whether the remote already holds the target content (same algorithm, same hash).
    pub(super) fn remote_has(&self, r: &NodeState, t: &NodeState) -> bool {
        match (&r.content, &t.content) {
            (Some(rc), Some(tc)) => rc.hash == tc.hash && rc.executable == tc.executable,
            _ => true,
        }
    }

    /// Whether the local file already holds the target content (no download needed).
    pub(super) fn local_matches_remote(&self, node: NodeId, l: &NodeState, t: &NodeState) -> bool {
        let (Some(lc), Some(tc)) = (&l.content, &t.content) else {
            return true;
        };
        if lc.executable != tc.executable {
            return false;
        }
        if lc.hash.algorithm == tc.hash.algorithm {
            return lc.hash == tc.hash;
        }
        // Different algorithms: the local bytes are the target if they are the base's bytes
        // and the base already had the target's remote hash.
        let base = self.input.base.get(node).and_then(|b| b.content);
        let base_local = self.input.base_extra.get(&node).and_then(|e| e.local_hash);
        base.is_some_and(|b| b.hash == tc.hash) && base_local == Some(lc.hash)
    }

    pub(super) fn base_local_hash_stale(&self, node: NodeId, t: &NodeState) -> bool {
        let (Some(l), Some(_)) = (
            self.input.local.get(node).and_then(|l| l.content),
            t.content,
        ) else {
            return false;
        };
        self.input.base_extra.get(&node).and_then(|e| e.local_hash) != Some(l.hash)
    }
}

/// The nodes `ops` move out of their folder here: a move, or a download to another folder
/// (`replace`: the drive's version in place of the item here). They run before the deletes,
/// so a folder's delete here doesn't take them, nor what is below them.
fn moving_out<'o>(ops: impl IntoIterator<Item = &'o Op>, local: &Tree) -> HashSet<NodeId> {
    ops.into_iter()
        .filter_map(|op| match op {
            Op::LocalMove { node, parent, .. }
            | Op::LocalDownload {
                node,
                parent,
                replace: true,
                ..
            } if local.get(*node).is_some_and(|l| l.parent != *parent) => Some(*node),
            _ => None,
        })
        .collect()
}

/// Whether `node`, gone here, is in another node gone here (`gone`), whose delete takes it: not
/// where the plan moves a folder between them out of that one first (`moving`), as its delete
/// is one of its own there then. Nearest first; a cycle ends the walk after one lap, and
/// `node` is never its own.
fn below_another(
    local: &Tree,
    node: NodeId,
    gone: &HashSet<NodeId>,
    moving: &HashSet<NodeId>,
) -> bool {
    local
        .ancestors_iter(node)
        .find(|a| gone.contains(a) || moving.contains(a))
        .is_some_and(|a| gone.contains(&a))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Another writer's moves made a cycle here, D in B and B in D, and D is gone: D's delete
    /// is its own (the walk up ends back at D, which isn't its own folder). Where B is gone
    /// too, each is in the other (both dropped, as before).
    #[test]
    fn a_delete_round_a_cycle_isnt_its_own_folders() {
        let (d, b) = (NodeId::new(), NodeId::new());
        let mut local = Tree::new();
        local.insert(d, NodeState::folder(b, "D"));
        local.insert(b, NodeState::folder(d, "B"));
        let none = HashSet::new();
        assert!(!below_another(&local, d, &HashSet::from([d]), &none));
        let both = HashSet::from([d, b]);
        assert!(below_another(&local, d, &both, &none));
        assert!(below_another(&local, b, &both, &none));
    }

    /// Files gone in a folder that sits in a cycle (C1 in C2, C2 in C1): each walk up ends
    /// after a lap, not a walk the size of the tree per file.
    #[test]
    fn walks_up_round_a_cycle_end_after_a_lap() {
        let (c1, c2, f) = (NodeId::new(), NodeId::new(), NodeId::new());
        let mut local = Tree::new();
        local.insert(c1, NodeState::folder(c2, "c1"));
        local.insert(c2, NodeState::folder(c1, "c2"));
        local.insert(f, NodeState::folder(c1, "f"));
        let files: Vec<NodeId> = (0..100_000).map(|_| NodeId::new()).collect();
        for (i, n) in files.iter().enumerate() {
            local.insert(*n, NodeState::folder(f, format!("{i}")));
        }
        let gone: HashSet<NodeId> = files.iter().copied().collect();
        let start = std::time::Instant::now();
        assert!(
            !files
                .iter()
                .any(|n| below_another(&local, *n, &gone, &HashSet::new()))
        );
        // A few steps each: well under a second even unoptimized (a walk the size of the tree
        // each would be 10^10 steps).
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "{:?}",
            start.elapsed()
        );
    }
}
