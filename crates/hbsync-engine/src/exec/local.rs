//! Local operations: folders, moves, tags, conflict copies, the trash and detaching.

use super::*;

impl Executor<'_> {
    // ---- local operations ----

    pub(super) fn fork(&mut self, node: NodeId, new_node: NodeId, name: &str) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::stale("gone"));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let parent = row.path.parent().unwrap_or_default();
        let target = spell(&parent, name, self.cfg.os).map_err(|e| {
            Outcome::unsyncable(format!("the conflict copy's name can't be used here: {e}"))
        })?;
        // Its id is fixed by the node and the bytes: the next plan picks it again.
        if self.db.read(|s| s.local_by_node(new_node))?.is_some() {
            return Ok(Outcome::unsyncable("its conflict copy's id is in use here"));
        }
        // The name may be held by something that stays: the copy can exist remotely already,
        // made by another device with the same bytes, named to fit its folder there. Then a
        // numbered name here; a later plan moves it where it belongs.
        let target = if self.free_target(&target, None)? {
            target
        } else {
            match numbered_place(self.db, self.cfg, &target, false)? {
                Some(t) => t,
                None => return Ok(Outcome::Retry("conflict name taken".into())),
            }
        };
        // Write-ahead: if we crash after the rename, recovery completes the fork instead of a
        // scan mistaking it for the user renaming the original.
        self.write_ahead(|s| s.put_fork(node, new_node, &target))?;
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            self.db.write(|s| s.delete_fork(node))?;
            return Ok(io_outcome(e));
        }
        self.mark_emptied(&row.path);
        let moved = LocalRow {
            node: new_node,
            ..self.renamed(&row, &target).unwrap_or_else(|| LocalRow {
                path: target.clone(),
                ..row.clone()
            })
        };
        if self.crash_here(CrashPoint::AfterForkRename) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        self.finish(&[(new_node, false), (node, false)], |s| {
            complete_fork(s, node, &moved)
        })
    }

    pub(super) fn mkdir(&mut self, node: NodeId, parent: NodeId, name: &str, tags: &Tags) -> Step {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::stale("already local"));
        }
        let target = self.target_path(parent, name)?;
        let abs = self.abs(&target);
        let created_ms = if ops::CAN_SET_CREATED {
            self.db
                .read(|s| s.get_mirror(node))?
                .as_ref()
                .and_then(MirrorRow::created_ms)
        } else {
            None
        };
        // Idempotent: a folder left by an interrupted run, unknown to the DB (under any case),
        // is adopted. One the DB tracks for another node is never taken over.
        let adopt = self.occupant(&target)?.is_none()
            && fs::symlink_metadata(&abs).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink());
        if !adopt {
            if !self.free_target(&target, None)? {
                return Ok(Outcome::Retry("name taken".into()));
            }
            self.filling(&target)?;
            // Write-ahead for a folder this device synced before: with the commit lost, the
            // scan would find it as a new folder, which adoption doesn't pair with a node that
            // has a base (`recover_placements` does, by this record).
            let record = self.db.read(|s| {
                if s.get_base(node)?.is_none() {
                    return Ok(None);
                }
                Ok(s.get_mirror(node)?.and_then(|m| {
                    Some(Placing {
                        base: BaseRow {
                            node,
                            state: m.state()?,
                            remote: m.remote_ref(),
                            content_state: ContentState::Local,
                            local_name: None,
                            local_hash: None,
                        },
                        target: target.clone(),
                    })
                }))
            })?;
            if let Some(record) = record {
                self.write_ahead(|s| s.put_placing(&record))?;
            }
            if let Err(e) = fs::create_dir(&abs) {
                return Ok(io_outcome(e));
            }
        }
        let tags = write_tags(&abs, tags);
        // Only shown, never synced on: a file system that won't take it is no failure.
        if let Some(ct) = created_ms {
            let _ = ops::set_created(&abs, ct);
        }
        let meta = match hbsync_platform::stat(&abs) {
            Ok(m) => m,
            Err(e) => return Ok(io_outcome(e)),
        };
        self.finish(&[(node, false)], |s| {
            s.delete_placing(node)?;
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::Folder,
                fingerprint: meta.fingerprint(),
                executable: None,
                hash: None,
                tags,
            })?;
            // What's in it may have been put aside unseen (in a folder this device ignores,
            // which this one was moved out of): it comes along, a level per folder made.
            s.mirror_children(node)?
                .iter()
                .try_for_each(|child| s.mark_dirty(child.node))
        })
    }

    /// Gives the local item of `node` these tags, if it is still what the plan saw.
    pub(super) fn local_tags(&mut self, node: NodeId, tags: &Tags) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::stale("gone"));
        };
        let abs = self.abs(&row.path);
        // A file's guard takes in the change time a tag write moves; a folder's is its file id
        // only, so its tags must still be what was read.
        let folder_retagged =
            row.kind == NodeKind::Folder && row.tags.is_some() && tags::read(&abs) != row.tags;
        if !self.on_disk_unchanged(&row) || folder_retagged {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let read = write_tags(&abs, tags);
        // Writing tags moves the change time only (a folder's mtime moves with its children).
        // If anything else moved, the item changed meanwhile: the old row stays, and the
        // rescan looks at it.
        let meta = match hbsync_platform::stat(&abs) {
            Ok(m) => m,
            Err(e) => return Ok(io_outcome(e)),
        };
        let fp = row.fingerprint;
        let same = meta.file_id == fp.file_id
            && (row.kind == NodeKind::Folder || fp.same_after_rename(&meta));
        if !same {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let row = LocalRow {
            fingerprint: meta.fingerprint(),
            tags: read,
            ..row
        };
        self.finish(&[(node, false)], |s| s.put_local(&row))
    }

    pub(super) fn local_move(&mut self, node: NodeId, parent: NodeId, name: &str) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::stale("gone"));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        if target == row.path {
            self.db.write(|s| s.delete_parked(node))?;
            return Ok(Outcome::Done);
        }
        if target.starts_with(&row.path) {
            return Ok(Outcome::Retry(
                "would move a folder into itself for now".into(),
            ));
        }
        if !self.free_target(&target, Some(node))? {
            return Ok(Outcome::Retry("name taken".into()));
        }
        self.filling(&target)?;
        if let Err(e) = ops::rename_noreplace(&self.abs(&row.path), &self.abs(&target)) {
            return Ok(io_outcome(e));
        }
        self.mark_emptied(&row.path);
        let refreshed = self.renamed(&row, &target);
        self.finish(&[(node, false)], |s| {
            s.move_local_subtree(&row.path, &target)?;
            s.delete_parked(node)?;
            refreshed.as_ref().map_or(Ok(()), |r| s.put_local(r))
        })
    }

    /// See [`renamed_row`].
    pub(super) fn renamed(&self, old: &LocalRow, path: &RelPath) -> Option<LocalRow> {
        renamed_row(&self.cfg.root, old, path)
    }

    /// Moves the local item of `node` to the trash; with `forget`, the bases of `node` and
    /// `expected` go too (§10).
    pub(super) fn trash(&mut self, node: NodeId, expected: &[NodeId], forget: bool) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            if forget {
                return self.forget_base(node);
            }
            return Ok(Outcome::Done); // already gone
        };
        let rows = self.db.read(|s| s.local_subtree(&row.path))?;
        let mut nodes: Vec<NodeId> = rows.iter().map(|r| r.node).collect();
        nodes.sort();
        if nodes != expected {
            return Ok(Outcome::stale(
                "the folder holds something the plan didn't expect",
            ));
        }
        // Everything on disk must be known and unchanged; otherwise rescan first.
        let Some(holding) = self.subtree_check(&row, &rows) else {
            return Ok(rescan(&mut self.report, &row.path));
        };
        if holding.is_empty() {
            if let Err(e) = self.move_to_trash(&row.path, node) {
                return Ok(io_outcome(e));
            }
        } else {
            // Ignored items are kept here, and so are the folders they are in (§10): the rest
            // goes, and those folders are released, for the scan to find as new ones.
            let holders: HashSet<RelPath> = holding
                .into_iter()
                .flat_map(|folder| {
                    std::iter::successors(Some(folder), |p| p.parent())
                        .take_while(|p| p.starts_with(&row.path))
                })
                .collect();
            for r in &rows {
                let in_holder = r.path.parent().is_some_and(|p| holders.contains(&p));
                if in_holder
                    && !holders.contains(&r.path)
                    && let Err(e) = self.move_to_trash(&r.path, r.node)
                {
                    self.report.rescan.push(row.path.clone());
                    return Ok(io_outcome(e));
                }
            }
            // A released folder can't keep a temporary name (it carries the old id): it gets
            // a real one. Deepest first, so the paths above stay valid.
            let mut parked: Vec<(&LocalRow, RelPath)> = Vec::new();
            for r in rows.iter().filter(|r| holders.contains(&r.path)) {
                if let Some(original) = self.db.read(|s| view::parked_place(s, r))? {
                    parked.push((r, original));
                }
            }
            parked.sort_by_key(|(r, _)| std::cmp::Reverse(r.path.depth()));
            for (r, original) in parked {
                if let Some(to) = release_place(self.db, self.cfg, &original, true)? {
                    self.filling(&to)?;
                    // Best effort: if it fails, the item keeps its name and the scan finds it there.
                    let _ = ops::rename_noreplace(&self.abs(&r.path), &self.abs(&to));
                }
            }
            self.report
                .rescan
                .push(row.path.parent().unwrap_or_default());
        }
        self.finish(&[(node, false)], |s| {
            for r in s.delete_local_subtree(&row.path)? {
                s.delete_parked(r.node)?;
                if r.node != node {
                    s.mark_dirty(r.node)?;
                }
                if forget {
                    s.delete_base(r.node)?;
                }
            }
            Ok(())
        })
    }

    /// Makes the local item of `node` the new node `new_node`, in place (§10). Everything
    /// below it is planned again: it sits in a new folder now.
    pub(super) fn detach(&mut self, node: NodeId, new_node: NodeId) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(if self.db.read(|s| s.local_by_node(new_node))?.is_some() {
                Outcome::Settled // an earlier try did it
            } else {
                Outcome::stale("gone locally")
            });
        };
        // A parked item's temporary name carries its old id: it gets a real one first, its
        // original if that is free. After a crash in between, the scan finds it there.
        let row = match self.db.read(|s| view::parked_place(s, &row))? {
            None => row,
            Some(original) => {
                if !self.on_disk_unchanged(&row) {
                    return Ok(rescan(&mut self.report, &row.path));
                }
                let is_dir = row.kind == NodeKind::Folder;
                let Some(to) = release_place(self.db, self.cfg, &original, is_dir)? else {
                    return Ok(Outcome::unsyncable("no free name to leave the parked one"));
                };
                self.filling(&to)?;
                match restore_parked(self.db, &self.cfg.root, &row, &to)? {
                    Ok(row) => row,
                    Err(e) => return Ok(io_outcome(e)),
                }
            }
        };
        let detached = self.finish(&[], |s| {
            let below = s.local_subtree(&row.path)?;
            s.rekey_local(node, new_node)?;
            // The old node isn't synced here any more; a base left behind would read as a
            // local delete once it is back in view.
            s.delete_base(node)?;
            s.mark_dirty(node)?;
            s.mark_dirty(new_node)?;
            below
                .iter()
                .filter(|r| r.node != node)
                .try_for_each(|r| s.mark_dirty(r.node))
        });
        // The new id goes out with the item in this run: durable first (a power cut must not
        // make the next plan pick another).
        if detached.is_ok() {
            self.flush_db()?;
        }
        detached
    }

    /// Drops the base of a node this device no longer syncs and holds nothing of (§10).
    pub(super) fn forget_base(&mut self, node: NodeId) -> Step {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::waiting("its local item goes first"));
        }
        // The base stays a tree: a child's base may still be needed (its delete held or failed),
        // unless the child is forgotten in this run too (children go first; in a cycle of
        // bases, as a remote cycle leaves them, one must go before the others).
        let children = self.db.read(|s| s.base_children(node))?;
        if children.iter().any(|c| !self.forgetting.contains(&c.node)) {
            return Ok(Outcome::waiting("its children first"));
        }
        self.finish(&[], |s| s.delete_base(node).map(drop))
    }

    /// Moves a local item into `.hbsync/trash/<time>-<node>/`.
    pub(super) fn move_to_trash(&mut self, path: &RelPath, node: NodeId) -> io::Result<()> {
        let stamp = hbsync_types::unix_now();
        let dir = layout::trash_dir(&self.cfg.root).join(format!("{stamp}-{}", node.0.simple()));
        fs::create_dir_all(&dir)?;
        let name = path.name().unwrap_or("item");
        ops::rename_noreplace(&self.abs(path), &dir.join(name))?;
        self.mark_emptied(path);
        self.unsynced_dirs.insert(dir);
        Ok(())
    }
}
