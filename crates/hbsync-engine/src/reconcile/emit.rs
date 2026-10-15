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
                let name = self
                    .merged
                    .get(copy)
                    .map(|s| s.name.clone())
                    .unwrap_or_default();
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

            // Local side (a fork copy already exists locally).
            let mut local_ops = false;
            if !fork_copies.contains(&node) {
                match (&target, local_real.filter(|_| forked.is_none())) {
                    (None, Some(_)) => {
                        let mut subtree = i.local.subtree(node);
                        subtree.sort();
                        let op = if self.masks.masked.contains(&node) {
                            Op::Forget { node, subtree }
                        } else {
                            Op::LocalTrash { node, subtree }
                        };
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
                            });
                            local_ops = true;
                        } else {
                            if !t.same_place(l) {
                                moves.push(Op::LocalMove {
                                    node,
                                    parent: t.parent,
                                    name: t.name.clone(),
                                });
                                local_ops = true;
                            }
                            if t.tags != l.tags {
                                content.push(Op::LocalTags {
                                    node,
                                    tags: t.tags.clone(),
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
        // trash of a subtree is needed).
        creates.sort_by_key(|(d, op)| (op.is_transfer(), op.pooled(), *d, op.node()));
        deletes.sort_by_key(|(d, op)| {
            let depth = *d as i64;
            match op.pooled() {
                true => (false, depth, op.node()),
                false => (true, -depth, op.node()),
            }
        });
        moves.sort_by_key(Op::pooled);
        content.sort_by_key(Op::pooled);
        let trashed: HashSet<NodeId> = deletes.iter().filter_map(|(_, op)| op.trashes()).collect();
        let deletes: Vec<Op> = deletes
            .into_iter()
            .map(|(_, op)| op)
            .filter(|op| match op.trashes() {
                Some(node) => !i.local.ancestors(node).iter().any(|a| trashed.contains(a)),
                None => true,
            })
            .collect();

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
