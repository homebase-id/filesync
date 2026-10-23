//! Local operations: folders, moves, tags, conflict copies, deletes and detaching.

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
        // No version of the drive live when planned: its version in Recently deleted, if any
        // (never synced here: made from there, its folder restored, its RemoteCreate to follow).
        // Read once, before the write: what the check and the commit go by.
        let (mirror, base_at, removed) = self.db.read(|s| {
            let removed = match drive {
                None => s
                    .removed_by_node(node)?
                    .and_then(|r| DriveVersion::of(&r.row, None)),
                Some(_) => None,
            };
            let base_at = s.get_base(node)?.map(|b| b.remote.version);
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
        // that has a base (`recover_placements` does, by this record), and the node read as
        // deleted here. Its base is the version it acts on; one made again from Recently
        // deleted (nowhere live: its RemoteCreate to follow), the version there, for recovery
        // only (its commit keeps its base, below). Made without tags, it holds none as soon as
        // it is made: so it claims, if it is to hold none; tags to write are not known until
        // written.
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
        let on_item_before = prior.clone().unwrap_or_default();
        // Where the volume keeps no tags (this OS none), what it holds of them is never known:
        // it claims none, as a download does.
        let kept_tags = self.disk.tags_kept();
        let none = tags.held_by(self.folder.os).is_empty();
        // Made from Recently deleted, it settles against the version there (as read before the
        // write: a pull landing before the commit reads as one landing after it), what the tags
        // it writes come from. Restored, changed or purged since the plan, that version
        // may no longer hold them: it waits for the next plan, which decides on the drive as it
        // is now. Settled against a version without a tag it wrote, the tag read as this device
        // adding it, and a tag removed on the drive since came back. One with none to write
        // (Recovered files, made here), or a folder left by an interrupted run (taken over as
        // before), goes ahead.
        if acted.is_none() && !synced_before && !adopt && kept_tags && !none {
            let held = tags.held_by(self.folder.os);
            let holds =
                |v: &hbsync_types::NodeState| held.iter().all(|t| v.tags.get(&t.name) == Some(t));
            if !removed.as_ref().is_some_and(|d| holds(&d.state)) {
                return Ok(Outcome::stale("not as planned in Recently deleted"));
            }
        }
        let recorded_on = acted.clone().or_else(|| {
            let from = removed.clone().filter(|_| synced_before)?;
            Some(DriveVersion { base_at, ..from })
        });
        let mut record = recorded_on.as_ref().map(|a| Placing {
            base: BaseRow::new(node, a.state.clone(), a.at),
            target: target.clone(),
            placed_tags: (none && kept_tags).then(Tags::default),
            prior_tags: prior,
            base_at: a.base_at.into(),
            waiting_since_ms: None,
        });
        let kept = !adopt && synced_before;
        // Its record is for recovery only: written ahead, gone with the commit.
        let recovery_only = kept && acted.is_none() && record.is_some();
        if !adopt {
            if !self.free_target(&target, None)? {
                return Ok(Outcome::Retry("name taken".into()));
            }
            if kept && let Some(r) = &record {
                self.disk.write_durable(self.db, |s| s.put_placing(r))?;
                self.settling.placing.insert(node);
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
                // Made again from Recently deleted, its record waits for the next round's
                // recovery and plan: the RemoteCreate planned with it (on the base as it was)
                // finds no folder of the node here, and doesn't run.
                return match outcome {
                    Outcome::Skip(Skip::Unsyncable(_)) => Ok(outcome),
                    _ if kept && !recovery_only => self.placed_not_committed(node),
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
            tags: self.disk.on_item_after(left, on_item_before),
        };
        let (os, folder) = (self.folder.os, self.folder);
        self.cfg.hooks.before_commit(self.db);
        let fails = self.fails(FailPoint::Commit);
        let committed = self.db.write(|s| {
            if fails {
                return Err(StateError::Conflict("the commit failed (a test)".into()));
            }
            s.folder().put_local(&row)?;
            match &record {
                Some(r) if !recovery_only => {
                    settle_placement(s.folder(), r, folder).map(Placed::at)
                }
                _ => {
                    if recovery_only {
                        s.folder().delete_placing(node)?;
                    }
                    // Made from Recently deleted (its folder restored, its RemoteCreate to
                    // follow): it settles against the version there, what it was made of, as
                    // any placement does: the base until the restore settles. Without one, a
                    // tag removed on the drive meanwhile read as this device adding it, and
                    // came back. That is the version the check above accepted, read before the
                    // write, not again here: a pull since (another device restored it without a
                    // tag) would settle it against a version it wasn't made of, the same way.
                    //
                    // One synced here before keeps its base, but for a tag its failed write
                    // didn't put on it that the drive's last version holds: kept, the folder
                    // without it read as the user removing it, and the plan removed it on the
                    // drive.
                    let base = s.get_base(node)?;
                    let synced = base.is_some();
                    if unknown
                        && let Some(mut b) = base
                        && let Some(on_item) = &row.tags
                    {
                        let last = removed.as_ref().map_or(&b.state.tags, |d| &d.state.tags);
                        let tags = not_reached(&b.state.tags, on_item, last);
                        if tags != b.state.tags {
                            b.state.tags = tags;
                            s.folder().put_base(&b)?;
                        }
                    }
                    let removed = removed.as_ref().filter(|_| !synced);
                    let at = settle(
                        s.folder(),
                        Settle::placed(node, removed, TagWrite::Agreed),
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
            tags: left.on_item,
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

    /// Deletes the local item of `node` (§7: aside, checked, unlinked), with the bases of what
    /// goes; with `forget` (§10), `node`'s base goes also where nothing of it is here.
    pub(super) fn delete_here(&mut self, node: NodeId, expected: &[NodeId], forget: bool) -> Step {
        let Some(row) = self.db.read(|s| s.local_by_node(node))? else {
            if forget {
                return self.drop_unsynced_base(node);
            }
            // Already gone: a delete earlier in this run took it along with its folder (and its
            // base, [`deleted_here`]); the executor holds the DB while it runs, so no scan did.
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
        // The item itself must be as its row has it (a folder's contents are checked aside).
        if !self.on_disk_unchanged(&row) {
            return Ok(rescan(&mut self.report, &row.path));
        }
        match self.delete_item(&row, &rows, true)? {
            Err(outcome) => return Ok(outcome),
            Ok(Deleted::All) => {}
            Ok(Deleted::Holding(holding)) => {
                return self.delete_holding(node, &row, &rows, holding);
            }
            Ok(Deleted::Returned { gone }) => return self.went_back(&gone, node),
        }
        if self.crash_here(CrashPoint::AfterUnlink) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        // Nothing to settle: what went has neither base nor item here now ([`deleted_here`]).
        self.finish(&[], |s| deleted_here(s, &row.path, node))
    }

    /// A delete found its item not as the plan saw it and put it back, after `gone` went: their
    /// rows and bases go, and the item is planned again.
    fn went_back(&mut self, gone: &[NodeId], node: NodeId) -> Step {
        if !gone.is_empty() {
            self.finish(&[], |s| went(s, gone.iter().copied(), node))?;
        }
        Ok(Outcome::stale("not as the plan saw it: returned"))
    }

    /// Deletes the folder of `row` that holds ignored items (§10), in the folders `holding`,
    /// which stay here, and so do the folders they are in: everything else goes, item by item
    /// (each aside, checked, unlinked), and those folders are released, for the scan to find as
    /// new ones.
    fn delete_holding(
        &mut self,
        node: NodeId,
        row: &LocalRow,
        rows: &[LocalRow],
        holding: Vec<RelPath>,
    ) -> Step {
        let holders: HashSet<RelPath> = holding
            .into_iter()
            .flat_map(|folder| {
                std::iter::successors(Some(folder), |p| p.parent())
                    .take_while(|p| p.starts_with(&row.path))
            })
            .collect();
        let mut gone: Vec<NodeId> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            let in_holder = r.path.parent().is_some_and(|p| holders.contains(&p));
            if !in_holder || holders.contains(&r.path) {
                continue;
            }
            // Its subtree: the rows after it below it (walk order).
            let below = &rows[i..][..1 + rows[i + 1..]
                .iter()
                .take_while(|b| b.path.starts_with(&r.path))
                .count()];
            let stopped = match self.delete_item(r, below, false)? {
                Ok(Deleted::All) => {
                    gone.extend(below.iter().map(|b| b.node));
                    None
                }
                Ok(Deleted::Returned { gone: some }) => {
                    gone.extend(some);
                    Some(Outcome::stale("not as the plan saw it: returned"))
                }
                Ok(Deleted::Holding(_)) => Some(Outcome::stale("it holds ignored items")),
                Err(outcome) => Some(outcome),
            };
            if let Some(outcome) = stopped {
                self.report.rescan.push(row.path.clone());
                // What did go: its rows and bases go now, as below.
                self.went_back(&gone, node)?;
                return Ok(outcome);
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
            if let Some(to) = release_place(self.db, self.folder, &original, true)? {
                // Best effort: if it fails, the item keeps its name and the scan finds it there.
                let _ = self.disk.rename(self.db, &r.path, &to)?;
            }
        }
        self.report
            .rescan
            .push(row.path.parent().unwrap_or_default());
        if self.crash_here(CrashPoint::AfterUnlink) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        // What went, and the folders released (new ones to the scan): no rows, no bases.
        self.finish(&[], |s| deleted_here(s, &row.path, node))
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
            s.forget_base(node)?;
            s.mark_dirty(new_node)?;
            below
                .iter()
                .filter(|r| r.node != node)
                .try_for_each(|r| s.mark_dirty(r.node))
        })?;
        Ok(Outcome::Settled)
    }

    /// Drops the base of a node this device no longer syncs and holds nothing of (§10).
    pub(super) fn drop_unsynced_base(&mut self, node: NodeId) -> Step {
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

/// What was at `path` went from here, deleted with `node` (the item at the top of what went):
/// its rows go, and so do their bases ([`forget`]).
fn deleted_here(s: &Store<'_>, path: &RelPath, node: NodeId) -> Result<(), StateError> {
    for r in s.folder().delete_local_subtree(path)? {
        forget(s, r.node, node)?;
    }
    Ok(())
}

/// `nodes` went from here, deleted with `node`: their rows go, and so do their bases
/// ([`forget`]).
pub(super) fn went(
    s: &Store<'_>,
    nodes: impl IntoIterator<Item = NodeId>,
    node: NodeId,
) -> Result<(), StateError> {
    for n in nodes {
        s.folder().delete_local(n)?;
        forget(s, n, node)?;
    }
    Ok(())
}

/// `n` went from here, deleted with `node`, its row gone: its base goes too, whatever the
/// mirror holds by now. What went isn't synced here any more, and a base left behind would read
/// as a local delete once the node is back in view: a restore (or another engine's pull of it)
/// made meanwhile, or the folder that took it along brought back, would be removed again from
/// here. What is in it that the drive still holds elsewhere is planned again.
fn forget(s: &Store<'_>, n: NodeId, node: NodeId) -> Result<(), StateError> {
    s.folder().delete_parked(n)?;
    if n != node && s.get_mirror(n)?.is_some() {
        s.folder().mark_dirty(n)?;
    }
    s.folder().delete_base(n)?;
    Ok(())
}
