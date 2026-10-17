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
        self.write_durable(|s| s.put_fork(node, new_node, &target))?;
        if let Err(e) = self.rename(&row.path, &target)? {
            self.db.write(|s| s.delete_fork(node))?;
            return Ok(io_outcome(e));
        }
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
        self.finish(&[Settle::node(new_node), Settle::node(node)], |s| {
            complete_fork(s, node, &moved)
        })
    }

    pub(super) fn mkdir(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        tags: &Tags,
        drive: Option<&DriveVersion>,
    ) -> Step {
        if self.db.read(|s| s.local_by_node(node))?.is_some() {
            return Ok(Outcome::stale("already local"));
        }
        if self.placing_earlier(node)? {
            return Ok(Outcome::stale("an earlier placement isn't settled yet"));
        }
        let target = self.target_path(parent, name)?;
        let abs = self.abs(&target);
        let (mirror, base_at) = self.db.read(|s| {
            Ok((
                s.get_mirror(node)?,
                s.get_base(node)?.map(|b| b.remote.version),
            ))
        })?;
        let synced_before = base_at.is_some();
        let created_ms = if ops::CAN_SET_CREATED {
            mirror.as_ref().and_then(MirrorRow::created_ms)
        } else {
            None
        };
        // What it acts on: the drive's version the plan saw (for one synced before, the
        // mirror's if the plan saw none, for the record). The base settles against it.
        let acted = drive.cloned().or_else(|| {
            let m = mirror.as_ref().filter(|_| synced_before)?;
            Some(DriveVersion {
                state: m.state()?,
                at: m.remote_ref(),
                base_at,
            })
        });
        // Its write-ahead record, kept for a folder this device synced before: with the commit
        // lost, the scan would find it as a new folder, which adoption doesn't pair with a node
        // that has a base (`recover_placements` does, by this record). Its base is the version
        // it acts on. Made without tags, it holds none as soon as it is made: so it claims, if
        // it is to hold none; tags to write are not known until written.
        //
        // Idempotent: a folder left by an interrupted run, unknown to the DB (under any case),
        // is adopted, with the tags it holds (what was there before the write). One the DB
        // tracks for another node is never taken over.
        let adopt = self.occupant(&target)?.is_none()
            && fs::symlink_metadata(&abs).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink());
        let prior = if adopt {
            tags::read(&abs)
        } else {
            Some(Tags::default())
        };
        // Where the volume keeps no tags (this OS none), what it holds of them is never known:
        // it claims none, as a download does.
        let kept_tags = tags::CAN_TAG && self.cfg.tags_kept.get() != Some(&false);
        let none = tags.held_by(self.cfg.os).is_empty();
        let mut record = acted.as_ref().map(|a| Placing {
            base: BaseRow::new(node, a.state.clone(), a.at),
            target: target.clone(),
            placed_tags: (none && kept_tags).then(Tags::default),
            prior_tags: prior,
            base_at: a.base_at.into(),
            held_ms: None,
            trashed: None,
        });
        let kept = !adopt && synced_before;
        if !adopt {
            if !self.free_target(&target, None)? {
                return Ok(Outcome::Retry("name taken".into()));
            }
            if kept && let Some(r) = &record {
                self.write_durable(|s| s.put_placing(r))?;
                self.records.insert(node);
            }
            if let Err(e) = self.put(&target, |at| fs::create_dir(at))? {
                return Ok(io_outcome(e));
            }
        }
        self.placed(node);
        if !adopt && self.crash_here(CrashPoint::AfterPlaced) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        let written = match self.write_item_tags(&abs, tags) {
            Ok(tags::Written::Done(read)) => Some(read),
            Ok(tags::Written::Unsupported) => Some(None),
            Err(_) => None,
        };
        let tags = written.clone().flatten();
        // Tags written: the record claims what the folder holds now, before its commit. A
        // folder taken over held its own: a failed write left those, not known (none claimed).
        if kept_tags
            && (adopt || !none)
            && let Some(r) = &mut record
        {
            r.placed_tags = tags.clone();
            if kept && tags.is_some() {
                self.write_durable(|s| s.put_placing(r))?;
            }
        }
        // Only shown, never synced on: a file system that won't take it is no failure.
        if let Some(ct) = created_ms {
            let _ = ops::set_created(&abs, ct);
        }
        let stat = match self.read_fails() {
            true => Err(io::Error::other("its read failed (a test)")),
            false => hbsync_platform::stat(&abs),
        };
        let meta = match stat {
            Ok(m) => m,
            Err(e) => {
                let outcome = io_outcome(e);
                return match outcome {
                    Outcome::Skip(Skip::Unsyncable(_)) => Ok(outcome),
                    _ if kept => self.placed_not_committed(node),
                    _ => Ok(outcome),
                };
            }
        };
        let row = LocalRow {
            path: target.clone(),
            node,
            kind: NodeKind::Folder,
            fingerprint: meta.fingerprint(),
            executable: None,
            hash: None,
            tags: self.tags_after_write(&abs, written),
        };
        let os = self.cfg.os;
        let fails = self.commit_fails();
        let committed = self.db.write(|s| {
            if fails {
                return Err(StateError::Conflict("the commit failed (a test)".into()));
            }
            s.put_local(&row)?;
            match &record {
                Some(r) => settle_placement(s, r, os),
                None => {
                    let at = settle(s, Settle::placed(node, None, TagWrite::Agreed), os)?;
                    // Made from Recently deleted (its folder restored, its RemoteCreate to
                    // follow): the version there is what it was made of, the base until the
                    // restore settles. Without one, a tag removed on the drive meanwhile read
                    // as this device adding it, and came back.
                    if s.get_base(node)?.is_none()
                        && let Some(removed) = s.removed_by_node(node)?
                        && let Some(state) = removed.row.state()
                    {
                        s.put_base(&BaseRow::new(node, state, removed.row.remote_ref()))?;
                    }
                    mark_children(s, node)?;
                    Ok(at)
                }
            }
        });
        // The DB failing stops the run; its record (one synced before has one) is the next
        // sync's recovery's, and a new folder's next plan takes it over.
        let at = match committed {
            Ok(at) => at,
            Err(e) => return Ok(Outcome::Fatal(e)),
        };
        self.note([(node, at)]);
        Ok(Outcome::Settled)
    }

    /// Gives the local item of `node` these tags, if it is still what the plan saw.
    pub(super) fn local_tags(
        &mut self,
        node: NodeId,
        tags: &Tags,
        drive: Option<&DriveVersion>,
    ) -> Step {
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
        let written = match self.write_item_tags(&abs, tags) {
            Ok(tags::Written::Done(read)) => Some(read),
            Ok(tags::Written::Unsupported) => Some(None),
            Err(_) => None,
        };
        // A failed write leaves the item what it held: read so, not as not known (which reads
        // as the drive's tags, and a tag only the item has would never go up).
        let read = self.tags_after_write(&abs, written);
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
        let before = row.tags.clone();
        let row = LocalRow {
            fingerprint: meta.fingerprint(),
            tags: read,
            ..row
        };
        // What it changed, where both are known; nothing claimed otherwise.
        let wrote = match (&before, &row.tags) {
            (Some(before), Some(written)) => TagWrite::Wrote { before, written },
            _ => TagWrite::Nothing,
        };
        // Tags put nothing in place: no base from the drive's place on content alone.
        self.finish(&[Settle::acted(node, drive, wrote)], |s| s.put_local(&row))
    }

    pub(super) fn local_move(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        drive: Option<&DriveVersion>,
    ) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            return Ok(Outcome::stale("gone"));
        };
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        if target == row.path {
            // Already there: nothing was put in place.
            return self.finish(&[Settle::acted(node, drive, TagWrite::Nothing)], |s| {
                s.delete_parked(node)
            });
        }
        if target.starts_with(&row.path) {
            return Ok(Outcome::Retry(
                "would move a folder into itself for now".into(),
            ));
        }
        if !self.free_target(&target, Some(node))? {
            return Ok(Outcome::Retry("name taken".into()));
        }
        if let Err(e) = self.rename(&row.path, &target)? {
            return Ok(io_outcome(e));
        }
        let refreshed = self.renamed(&row, &target);
        // A move takes the item's tags along: it writes none.
        self.finish(&[Settle::placed(node, drive, TagWrite::Nothing)], |s| {
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
            if let Err(e) = self.move_to_trash(&row.path, node)? {
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
                    && let Err(e) = self.move_to_trash(&r.path, r.node)?
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
                    // Best effort: if it fails, the item keeps its name and the scan finds it there.
                    let _ = self.rename(&r.path, &to)?;
                }
            }
            self.report
                .rescan
                .push(row.path.parent().unwrap_or_default());
        }
        self.finish(&[Settle::node(node)], |s| {
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
                match restore_parked(self.db, &self.cfg.root, &row, &to)? {
                    Ok(row) => row,
                    Err(e) => return Ok(io_outcome(e)),
                }
            }
        };
        // The new id goes out with the item in this run: durable at once (a power cut must not
        // make the next plan pick another).
        self.write_durable(|s| {
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
        })?;
        Ok(Outcome::Settled)
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
    pub(super) fn move_to_trash(
        &mut self,
        path: &RelPath,
        node: NodeId,
    ) -> Result<io::Result<()>, StateError> {
        let entry = trash_entry(path, node);
        self.move_to_trash_as(path, &entry)
    }

    /// Moves the item at `path` into the trash as `entry` ([`trash_entry`]).
    pub(super) fn move_to_trash_as(
        &mut self,
        path: &RelPath,
        entry: &str,
    ) -> Result<io::Result<()>, StateError> {
        let trash = layout::trash_dir(&self.cfg.root);
        let to = trash.join(entry);
        // The trash folder too, made with the first item in it.
        self.db.changed(&trash);
        if let Some(dir) = to.parent()
            && let Err(e) = put_ordered(self.db, dir, |dir| fs::create_dir_all(dir))?
        {
            return Ok(Err(e));
        }
        rename_ordered(self.db, &self.abs(path), &to)
    }

    /// Brings the old version a download put in the trash as `entry` back to `path`, and
    /// removes its entry's folder, empty then.
    pub(super) fn restore_from_trash(
        &mut self,
        entry: &str,
        path: &RelPath,
    ) -> Result<io::Result<()>, StateError> {
        if Self::take_one(&self.cfg.fail_restores) {
            return Ok(Err(io::Error::other("bringing it back failed (a test)")));
        }
        let from = layout::trash_dir(&self.cfg.root).join(entry);
        let back = rename_ordered(self.db, &from, &self.abs(path))?;
        if back.is_ok() {
            remove_entry_dir(&from);
        }
        Ok(back)
    }
}

/// The trash entry (relative to the trash folder) an item at `path` of `node` goes to now: a
/// folder of its own, named by when and by the node.
pub(super) fn trash_entry(path: &RelPath, node: NodeId) -> String {
    format!(
        "{}-{}/{}",
        hbsync_types::unix_now(),
        node.0.simple(),
        path.name().unwrap_or("item")
    )
}

/// Removes the folder of trash entry `item`, emptied by bringing the item back. One that can't
/// be removed (not empty after all) stays, for the daily purge.
pub(super) fn remove_entry_dir(item: &Path) {
    if let Some(dir) = item.parent() {
        drop(fs::remove_dir(dir));
    }
}
