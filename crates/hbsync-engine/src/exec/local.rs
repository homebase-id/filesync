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
        let target = spell(&parent, name, self.folder.os).map_err(|e| {
            Outcome::unsyncable(format!("the conflict copy's name can't be used here: {e}"))
        })?;
        // Its id is fixed by the node, the bytes, the base version and this computer: the next
        // plan picks it again.
        if self.db.read(|s| s.local_by_node(new_node))?.is_some() {
            return Ok(Outcome::unsyncable("its conflict copy's id is in use here"));
        }
        // The name may be held by something that stays (a file the user named so, or another
        // computer's copy of the same conflict made the same day under a device name like
        // this one's). Then a numbered name here; a later plan moves it where it belongs.
        let target = if self.free_target(&target, None)? {
            target
        } else {
            match numbered_place(self.db, self.folder, &target, false)? {
                Some(t) => t,
                None => return Ok(Outcome::Retry("conflict name taken".into())),
            }
        };
        // Write-ahead: if we crash after the rename, recovery completes the fork instead of a
        // scan mistaking it for the user renaming the original.
        self.disk
            .write_durable(self.db, |s| s.put_fork(node, new_node, &target))?;
        if let Err(e) = self.disk.rename(self.db, &row.path, &target)? {
            self.db.write(|s| s.folder().delete_fork(node))?;
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
            complete_fork(s.folder(), node, &moved)
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
        let abs = self.disk.abs(&target);
        // Never synced here and no version of the drive live when planned: made from Recently
        // deleted (its folder restored, its RemoteCreate to follow).
        let (mirror, base_at, removed) = self.db.read(|s| {
            let base_at = s.get_base(node)?.map(|b| b.remote.version);
            let removed = match drive.is_none() && base_at.is_none() {
                true => s.removed_by_node(node)?,
                false => None,
            };
            Ok((s.get_mirror(node)?, base_at, removed))
        })?;
        let synced_before = base_at.is_some();
        let created_ms = if ops::CAN_SET_CREATED {
            mirror.as_ref().and_then(MirrorRow::created_ms)
        } else {
            None
        };
        // What it acts on: the drive's version the plan saw (for one synced before, the
        // mirror's if the plan saw none, for the record). The base settles against it.
        let acted = drive
            .cloned()
            .or_else(|| DriveVersion::of(mirror.as_ref().filter(|_| synced_before)?, base_at));
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
        // What it holds before the write, for a write that fails unread: none where that can't
        // be read either (claiming less than it holds only sends what it holds up).
        let held_before = prior.clone().unwrap_or_default();
        // Where the volume keeps no tags (this OS none), what it holds of them is never known:
        // it claims none, as a download does.
        let kept_tags = self.disk.tags_kept();
        let none = tags.held_by(self.folder.os).is_empty();
        // Made from Recently deleted, it settles against the version there (the commit's), what
        // the tags it writes come from. Restored, changed or purged since the plan, that version
        // may no longer hold them: it waits for the next plan, which decides on the drive as it
        // is now. Settled against a version without a tag it wrote, the tag read as this device
        // adding it, and a tag removed on the drive since came back. One with none to write
        // (Recovered files, made here), or a folder left by an interrupted run (taken over as
        // before), goes ahead.
        if acted.is_none() && !synced_before && !adopt && kept_tags && !none {
            let version = removed.and_then(|r| r.row.state());
            let held = tags.held_by(self.folder.os);
            let holds =
                |v: &hbsync_types::NodeState| held.iter().all(|t| v.tags.get(&t.name) == Some(t));
            if !version.as_ref().is_some_and(holds) {
                return Ok(Outcome::stale("not as planned in Recently deleted"));
            }
        }
        let mut record = acted.as_ref().map(|a| Placing {
            base: BaseRow::new(node, a.state.clone(), a.at),
            target: target.clone(),
            placed_tags: (none && kept_tags).then(Tags::default),
            prior_tags: prior,
            base_at: a.base_at.into(),
            held_ms: None,
        });
        let kept = !adopt && synced_before;
        if !adopt {
            if !self.free_target(&target, None)? {
                return Ok(Outcome::Retry("name taken".into()));
            }
            if kept && let Some(r) = &record {
                self.disk.write_durable(self.db, |s| s.put_placing(r))?;
                self.records.insert(node);
            }
            if let Err(e) = self.disk.put(self.db, &target, |at| fs::create_dir(at))? {
                return Ok(io_outcome(e));
            }
        }
        self.placed(node);
        if !adopt && self.crash_here(CrashPoint::AfterPlaced) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        let left = self.disk.write_tags(&abs, tags);
        let tags = left.claimed.clone();
        // Tags written: the record claims what the folder holds now, before its commit. A
        // folder taken over held its own: a failed write left those, not known (none claimed).
        let claims = kept_tags && (adopt || !none);
        // What it holds isn't what it was to hold: the write failed, or read back as nothing.
        let unknown = claims && tags.is_none();
        if claims && let Some(r) = &mut record {
            r.placed_tags = tags.clone();
            if kept && tags.is_some() {
                self.disk.write_durable(self.db, |s| s.put_placing(r))?;
            }
        }
        if let Some(ct) = created_ms {
            self.disk.set_created(&abs, ct);
        }
        let stat = match self.fails(FailPoint::PlacedRead) {
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
            tags: self.disk.held_after(left, held_before),
        };
        let (os, folder) = (self.folder.os, self.folder);
        let fails = self.fails(FailPoint::Commit);
        let committed = self.db.write(|s| {
            if fails {
                return Err(StateError::Conflict("the commit failed (a test)".into()));
            }
            s.folder().put_local(&row)?;
            match &record {
                Some(r) => settle_placement(s.folder(), r, folder).map(Placed::at),
                None => {
                    // Made from Recently deleted (its folder restored, its RemoteCreate to
                    // follow): it settles against the version there, what it was made of, as
                    // any placement does: the base until the restore settles. Without one, a
                    // tag removed on the drive meanwhile read as this device adding it, and
                    // came back.
                    //
                    // One synced here before keeps its base, but for a tag its failed write
                    // didn't put on it that the drive's last version holds: kept, the folder
                    // without it read as the user removing it, and the plan removed it on the
                    // drive.
                    let base = s.get_base(node)?;
                    let synced = base.is_some();
                    let removed = match !synced || unknown {
                        true => s
                            .removed_by_node(node)?
                            .and_then(|r| DriveVersion::of(&r.row, None)),
                        false => None,
                    };
                    if unknown
                        && let Some(mut b) = base
                        && let Some(held) = &row.tags
                    {
                        let last = removed.as_ref().map_or(&b.state.tags, |d| &d.state.tags);
                        let tags = not_reached(&b.state.tags, held, last);
                        if tags != b.state.tags {
                            b.state.tags = tags;
                            s.folder().put_base(&b)?;
                        }
                    }
                    let removed = removed.filter(|_| !synced);
                    let at = settle(
                        s.folder(),
                        Settle::placed(node, removed.as_ref(), TagWrite::Agreed),
                        os,
                    )?;
                    mark_children(s.folder(), node)?;
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
        // No base to record what it writes (the plan's move or remote write founds one): the
        // drive's tags would read as this device adding them. They wait for it.
        let os = self.folder.os;
        let (row, recorded) = self
            .db
            .read(|s| Ok((s.local_by_node(node)?, tags_recorded(s, node, drive, os)?)))?;
        let Some(row) = row else {
            return Ok(Outcome::stale("gone"));
        };
        let abs = self.disk.abs(&row.path);
        // A file's guard takes in the change time a tag write moves; a folder's is its file id
        // only, so its tags must still be what was read.
        let folder_retagged =
            row.kind == NodeKind::Folder && row.tags.is_some() && tags::read(&abs) != row.tags;
        if !self.on_disk_unchanged(&row) || folder_retagged {
            return Ok(rescan(&mut self.report, &row.path));
        }
        if !recorded {
            return Ok(Outcome::waiting("no base to record the tags in yet"));
        }
        // A failed write leaves the item what it held: read so, not as not known (which reads
        // as the drive's tags, and a tag only the item has would never go up).
        let left = self.disk.write_tags(&abs, tags);
        let unread = self.disk.unread(&left);
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
        // Not read back either: the row stays as the scan read it, its change time too, so the
        // scan reads whatever changed since (a tag the user added meanwhile).
        if unread {
            return self.finish(&[Settle::acted(node, drive, TagWrite::Nothing)], |_| Ok(()));
        }
        let before = row.tags.clone();
        let row = LocalRow {
            fingerprint: meta.fingerprint(),
            tags: left.held,
            ..row
        };
        // What it changed, where both are known; nothing claimed otherwise.
        let wrote = match (&before, &row.tags) {
            (Some(before), Some(written)) => TagWrite::Wrote { before, written },
            _ => TagWrite::Nothing,
        };
        // Tags put nothing in place: no base from the drive's place on content alone.
        self.finish(&[Settle::acted(node, drive, wrote)], |s| {
            s.folder().put_local(&row)
        })
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
                s.folder().delete_parked(node)
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
        if let Err(e) = self.disk.rename(self.db, &row.path, &target)? {
            return Ok(io_outcome(e));
        }
        let refreshed = self.renamed(&row, &target);
        // A move takes the item's tags along: it writes none.
        self.finish(&[Settle::placed(node, drive, TagWrite::Nothing)], |s| {
            s.folder().move_local_subtree(&row.path, &target)?;
            s.folder().delete_parked(node)?;
            refreshed
                .as_ref()
                .map_or(Ok(()), |r| s.folder().put_local(r))
        })
    }

    /// See [`renamed_row`].
    pub(super) fn renamed(&self, old: &LocalRow, path: &RelPath) -> Option<LocalRow> {
        renamed_row(self.disk.root(), old, path)
    }

    /// Moves the local item of `node` to the trash, with the bases of what goes; with `forget`
    /// (§10), `node`'s base goes also where nothing of it is here.
    pub(super) fn trash(&mut self, node: NodeId, expected: &[NodeId], forget: bool) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            if forget {
                return self.forget_base(node);
            }
            // Already gone: a trash earlier in this run took it along with its folder (and its
            // base, [`trashed`]); the executor holds the DB while it runs, so no scan did.
            return Ok(Outcome::Done);
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
            if let Err(e) = self.disk.move_to_trash(self.db, &row.path, node)? {
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
            let mut moved: Vec<&RelPath> = Vec::new();
            for r in &rows {
                let in_holder = r.path.parent().is_some_and(|p| holders.contains(&p));
                if !in_holder || holders.contains(&r.path) {
                    continue;
                }
                if let Err(e) = self.disk.move_to_trash(self.db, &r.path, r.node)? {
                    self.report.rescan.push(row.path.clone());
                    // What did go is in the trash: its rows and bases go now, as below.
                    self.finish(&[], |s| {
                        moved.iter().try_for_each(|path| trashed(s, path, node))
                    })?;
                    return Ok(io_outcome(e));
                }
                moved.push(&r.path);
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
                if let Some(to) = release_place(self.db, self.folder, &original, true)? {
                    // Best effort: if it fails, the item keeps its name and the scan finds it there.
                    let _ = self.disk.rename(self.db, &r.path, &to)?;
                }
            }
            self.report
                .rescan
                .push(row.path.parent().unwrap_or_default());
        }
        if self.crash_here(CrashPoint::AfterTrash) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        // Nothing to settle: what went has neither base nor item here now ([`trashed`]), and the
        // folders released stay for the scan to find as new ones.
        self.finish(&[], |s| trashed(s, &row.path, node))
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
                let Some(to) = release_place(self.db, self.folder, &original, is_dir)? else {
                    return Ok(Outcome::unsyncable("no free name to leave the parked one"));
                };
                match restore_parked(self.db, &self.disk, &row, &to)? {
                    Ok(row) => row,
                    Err(e) => return Ok(io_outcome(e)),
                }
            }
        };
        // The new id goes out with the item in this run: durable at once (a power cut must not
        // make the next plan pick another).
        self.disk.write_durable(self.db, |s| {
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
        if children.iter().any(|c| !self.schedule.forgetting(c.node)) {
            return Ok(Outcome::waiting("its children first"));
        }
        self.finish(&[], |s| s.folder().delete_base(node).map(drop))
    }
}

/// The item at `path` went to the trash, `node`'s trash: its rows go, and so do their bases,
/// whatever the mirror holds by now. What went isn't synced here any more, and a base left
/// behind would read as a local delete once the node is back in view: a restore (or another
/// engine's pull of it) made meanwhile, or the folder that took it along brought back, would be
/// removed again from here. What is in it that the drive still holds elsewhere is planned again.
fn trashed(s: &Store<'_>, path: &RelPath, node: NodeId) -> Result<(), StateError> {
    for r in s.folder().delete_local_subtree(path)? {
        s.folder().delete_parked(r.node)?;
        if r.node != node && s.get_mirror(r.node)?.is_some() {
            s.folder().mark_dirty(r.node)?;
        }
        s.folder().delete_base(r.node)?;
    }
    Ok(())
}
