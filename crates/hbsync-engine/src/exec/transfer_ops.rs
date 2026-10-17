//! Transfers: their guards before the worker pool, placing and recording what it brought (and
//! what the remote writes of `remote.rs` did).

use super::*;
use hbsync_state::Trashed;

impl Executor<'_> {
    // ---- transfers: prepare (guards) and commit (placement, DB) around the worker pool ----

    pub(super) fn prepare(&mut self, op: &Op) -> Step<(Job, Ctx)> {
        match op {
            Op::LocalDownload {
                node,
                parent,
                name,
                replace,
                tags,
                drive,
            } => self.prepare_download(*node, *parent, name, *replace, tags, drive.as_deref()),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind: NodeKind::Folder,
                tags,
            } => self.prepare_folder(*node, *parent, name, tags),
            Op::RemoteCreate {
                node,
                parent,
                name,
                kind,
                tags,
            } => self.prepare_upload(*node, None, *parent, name, *kind, tags),
            Op::RemoteUpdate {
                node,
                file_id,
                expected,
                parent,
                name,
                upload: false,
                tags,
            } => self.prepare_rename(*node, *file_id, *expected, Some((*parent, name)), tags),
            Op::RemoteTags {
                node,
                file_id,
                expected,
                tags,
            } => self.prepare_rename(*node, *file_id, *expected, None, tags),
            Op::RemoteDelete {
                node,
                file_id,
                expected,
                ..
            } => self.prepare_remove(*node, *file_id, *expected),
            Op::RemoteUpdate {
                node,
                file_id,
                expected,
                parent,
                name,
                tags,
                ..
            } => self.prepare_upload(
                *node,
                Some((*file_id, *expected)),
                *parent,
                name,
                NodeKind::File,
                tags,
            ),
            _ => Err(Outcome::unsyncable(
                "not a transfer (a fault of this client)",
            )),
        }
    }

    pub(super) fn prepare_download(
        &mut self,
        node: NodeId,
        parent: NodeId,
        name: &str,
        replace: bool,
        tags: &Tags,
        drive: Option<&DriveVersion>,
    ) -> Step<(Job, Ctx)> {
        let (mirror, current, base_at) = self.db.read(|s| {
            Ok((
                s.get_mirror(node)?,
                s.local_by_node(node)?,
                s.get_base(node)?.map(|b| b.remote.version),
            ))
        })?;
        let Some(mirror) = mirror else {
            return Err(Outcome::stale("gone remotely"));
        };
        if self.placing_earlier(node)? {
            return Err(Outcome::stale("an earlier placement isn't settled yet"));
        }
        let Some(file) = mirror.file else {
            return Err(Outcome::stale("not a file"));
        };
        let expected = file.hash;
        if replace != current.is_some() {
            return Err(Outcome::stale("local presence changed"));
        }
        if let Some(row) = &current
            && !self.on_disk_unchanged(row)
        {
            return Err(rescan(&mut self.report, &row.path));
        }
        let target = self.target_path(parent, name)?;
        let created_ms = mirror.created_ms();
        // What it acts on: the drive's version the plan saw, only while the mirror still holds
        // it, or the one this run's own write made of it since, the bytes the same (a tag
        // write, a move: the base is there). Where the drive moved on otherwise (another
        // folder's engine pulled the next version), the next plan decides on that. Its commit
        // settles against it; kept until then, its tags not known until written.
        let ours = self.settled.get(&node).copied();
        let acted = match drive {
            Some(d) if d.at != mirror.remote_ref() => {
                let same_bytes =
                    |m: &NodeState| m.content.map(|c| c.hash) == d.state.content.map(|c| c.hash);
                match mirror.state() {
                    Some(state)
                        if ours == Some(mirror.remote_ref())
                            && base_at == ours.map(|o| o.version)
                            && same_bytes(&state) =>
                    {
                        DriveVersion {
                            state,
                            at: mirror.remote_ref(),
                            base_at,
                        }
                    }
                    _ => return Err(Outcome::stale("changed on the drive since the plan")),
                }
            }
            Some(d) => d.clone(),
            None => DriveVersion {
                state: mirror.state().ok_or_else(|| Outcome::stale("not a file"))?,
                at: mirror.remote_ref(),
                base_at,
            },
        };
        // It claims the tags it is to write (the empty set too), as they read back once
        // written: where the volume keeps none, not known.
        let kept = tags::CAN_TAG && self.cfg.tags_kept.get() != Some(&false);
        let record = Placing {
            base: BaseRow::new(node, acted.state, acted.at),
            target,
            placed_tags: kept.then(|| tags.held_by(self.cfg.os)),
            // What the file it replaces held; nothing where there was none.
            prior_tags: match &current {
                Some(row) => row.tags.clone(),
                None => Some(Tags::default()),
            },
            base_at: acted.base_at.into(),
            held_ms: None,
            trashed: None,
        };
        let job = Job::Download(Download {
            node,
            file_id: mirror.file_id,
            expected,
            size: file.size,
            mtime_ms: file.mtime_ms,
            executable: (self.cfg.os != Os::Windows).then_some(file.executable),
            tmp_dir: layout::tmp_dir(&self.cfg.root),
            key: download_key(node, &expected),
            local_algorithm: self.cfg.algorithm,
        });
        Ok((
            job,
            Ctx::Download(Box::new(Placement {
                record,
                current,
                created_ms,
                tags: tags.clone(),
            })),
        ))
    }

    pub(super) fn prepare_upload(
        &mut self,
        node: NodeId,
        existing: Option<(hbsync_types::RemoteFileId, hbsync_types::VersionTag)>,
        parent: NodeId,
        name: &str,
        kind: NodeKind,
        tags: &Tags,
    ) -> Step<(Job, Ctx)> {
        // Nothing more goes up once the drive is full; the run's other work goes on.
        if self.full && kind == NodeKind::File {
            return Err(Outcome::waiting("the drive is out of storage"));
        }
        let (row, mirror, base) = self.db.read(|s| {
            Ok((
                s.local_by_node(node)?,
                s.get_mirror(node)?,
                s.get_base(node)?,
            ))
        })?;
        let Some(row) = row else {
            return Err(Outcome::stale("gone locally"));
        };
        if !self.on_disk_unchanged(&row) {
            return Err(rescan(&mut self.report, &row.path));
        }
        let Some(file) = view::local_content(&row, base.as_ref()) else {
            return Err(Outcome::stale("not hashed yet"));
        };
        let mut content = NodeContent::file(name, file.mtime_ms, file.size, file.executable);
        let (hash, size) = (file.hash, file.size);
        let path = self.abs(&row.path);
        let removed = match existing {
            None => self.db.read(|s| s.removed_by_node(node))?,
            Some(_) => None,
        };
        // The version written over has bytes this device never had: edited elsewhere, then
        // deleted (in Recently deleted, or taken along by a folder's delete, node schema):
        // writing over it would lose it. It stays, and this version goes up as a new item.
        let over = match existing {
            None => removed.as_ref().map(|r| &r.row),
            Some((file_id, _)) => mirror.as_ref().filter(|m| m.file_id == file_id),
        };
        if let Some(r) = over
            && r.file.map(|f| f.hash) != base.as_ref().and_then(|b| b.state.content).map(|c| c.hash)
        {
            return Err(self.detach(node, NodeId::new()).unwrap_or_else(|o| o));
        }
        let mut tags = tags.clone();
        let target = match existing {
            // In Recently deleted: restored with these bytes (an edit wins over the delete).
            None if let Some(r) = &removed => {
                content.unknown = r.row.carried_keys();
                tags = merged_with_drive(base.as_ref(), &tags, &r.row.tags());
                Some(r.row.target())
            }
            // A new node: the file's own creation time, kept from then on.
            None => {
                content.set_created_ms(ops::created_ms(&path));
                None
            }
            Some((file_id, version)) => {
                let Some(mirror) = mirror.as_ref().filter(|m| m.file_id == file_id) else {
                    return Err(Outcome::stale("gone remotely"));
                };
                content.unknown = mirror.carried_keys();
                tags = merged_with_drive(base.as_ref(), &tags, &mirror.tags());
                // A node from before creation times, or from a client that sends none.
                if content.created_ms().is_none() {
                    content.set_created_ms(ops::created_ms(&path));
                }
                Some(mirror.target_at(version))
            }
        };
        content.set_tags(&tags);
        let job = Job::Upload {
            meta: NodeMeta {
                node,
                parent,
                kind,
                content,
                removed: false,
            },
            target,
            path,
            hash,
            size,
        };
        Ok((
            job,
            Ctx::Upload {
                hash,
                from: existing.and(mirror.map(|m| m.parent)),
            },
        ))
    }

    /// Records what a job did: `job` is the one that ran, given back by the pool.
    pub(super) fn commit(&mut self, ctx: Ctx, job: Job, result: JobResult) -> Step {
        match (ctx, job, result) {
            (Ctx::Meta { hash, after, .. }, Job::Meta { meta, .. }, JobResult::Written(w)) => {
                self.commit_meta(meta, hash, after, Some(w))
            }
            // Deleted for good meanwhile: out of the tree all the same.
            (
                Ctx::Meta {
                    hash,
                    after: after @ remote::After::Removed { .. },
                    ..
                },
                Job::Meta { meta, .. },
                JobResult::Failed(JobError::Remote(RemoteError::NotFound)),
            ) => self.commit_meta(meta, hash, after, None),
            // An earlier try landed, with whatever the file held then: the bytes count as
            // agreed only where the drive's hash is the local one (an edit since goes up).
            (_, job, JobResult::Failed(JobError::Remote(RemoteError::AlreadyExists))) => {
                self.finish(&[Settle::node(job.node())], |_| Ok(()))
            }
            (_, _, JobResult::Failed(e)) => Ok(match e {
                JobError::Remote(r) => remote_outcome(r),
                // Local file errors (locked, changed mid-read, short) are worth another try.
                e => Outcome::Retry(e.to_string()),
            }),
            (Ctx::Download(d), _, JobResult::Downloaded { file, local_hash }) => {
                self.place_download(*d, file, local_hash)
            }
            (Ctx::Upload { hash, .. }, Job::Upload { meta, .. }, JobResult::Written(written)) => {
                let node = meta.node;
                let row = written_row(written, &meta, Some(hash));
                // The bytes were just sent: equal even if the hashes use other algorithms.
                let how = Settle {
                    same_bytes: true,
                    ..Settle::node(node)
                };
                self.finish(&[how], |s| {
                    s.delete_removed(node)?;
                    s.put_mirror(&row)
                })
            }
            _ => Ok(Outcome::unsyncable(
                "a transfer result for another operation (a fault of this client)",
            )),
        }
    }

    /// Puts a verified download into place: over the old version (if it's still what the plan
    /// saw) or as a new file; an old version at another path goes to the trash first.
    fn place_download(
        &mut self,
        placement: Placement,
        done: hbsync_fileio::CompletedFile,
        local_hash: hbsync_types::ContentHash,
    ) -> Step {
        let Placement {
            mut record,
            current,
            created_ms,
            tags: to_write,
        } = placement;
        let (node, target) = (record.base.node, record.target.clone());
        let target = &target;
        if let Some(row) = &current
            && self.db.read(|s| s.local_by_node(node))?.as_ref() != Some(row)
        {
            done.keep_for_retry();
            return Ok(Outcome::stale("local item changed during the transfer"));
        }
        // The tags go on before the file is in place: a scan in between would read none, and
        // take that for the user removing them. Where they turn out other than its record
        // claims (the write failed, or the volume keeps none), the record says so before the
        // file can be seen there: only that costs a commit.
        // Only a volume that keeps no tags says so for the rest: a set that fails or reads
        // back as nothing to go by doesn't.
        let write = self.write_item_tags(done.path(), &to_write);
        let failed = write.is_err();
        // Failed, it would lose a tag the item there holds and the drive doesn't (the user's,
        // not sent up yet): not put in place then, for a retry, the plan sending it up
        // meanwhile.
        let drive_tags = &record.base.state.tags;
        if failed
            && let Some(held) = current.as_ref().and_then(|r| r.tags.as_ref())
            && held
                .iter()
                .any(|t| to_write.get(&t.name).is_some() && drive_tags.get(&t.name).is_none())
        {
            done.keep_for_retry();
            return Ok(Outcome::Retry("its tags weren't written".into()));
        }
        let tags = match write {
            Ok(tags::Written::Unsupported) => {
                let _ = self.cfg.tags_kept.set(false);
                None
            }
            Ok(tags::Written::Done(read)) => {
                let _ = self.cfg.tags_kept.set(true);
                read
            }
            Err(_) => None,
        };
        if tags != record.placed_tags {
            record.placed_tags = tags.clone();
            self.write_durable(|s| s.put_placing(&record))?;
        }
        // The record claims only what read back; the row holds what the file holds.
        let tags = self.tags_after_write(done.path(), (!failed).then_some(tags));
        let placed = match &current {
            Some(row) if &row.path == target => self.put(target, |at| {
                done.persist_over(at, &row.fingerprint, created_ms)
            })?,
            Some(row) => {
                if !self.on_disk_unchanged(row) || !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("old version changed or name taken".into()));
                }
                // Where the old version goes in the trash, noted before it goes: should the new
                // one not get in place, it comes back (in the run, or by recovery), where the
                // next plan would take its absence for the user deleting it.
                let entry = trash_entry(&row.path, node);
                record.trashed = Some(Trashed {
                    from: row.path.clone(),
                    to: entry.clone(),
                });
                self.write_durable(|s| s.put_placing(&record))?;
                if let Err(e) = self.move_to_trash_as(&row.path, &entry)? {
                    done.keep_for_retry();
                    return Ok(io_outcome(e));
                }
                if self.crash_here(CrashPoint::AfterTrashed) {
                    return Ok(Outcome::Retry("crash point reached".into()));
                }
                let placed = match Self::take_one(&self.cfg.fail_after_trash) {
                    true => Err(FileIoError::Io(io::Error::other(
                        "not put in place (a test)",
                    ))),
                    false => self.put(target, |at| done.persist_new(at, created_ms))?,
                };
                if matches!(&placed, Err(e) if !matches!(e, FileIoError::PlacedUnread(_)))
                    && let Err(e) = self.restore_from_trash(&entry, &row.path)?
                {
                    // Still in the trash: the record stays, for recovery to bring it back.
                    self.placed(node);
                    return Ok(io_outcome(e));
                }
                placed
            }
            None => {
                if !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("name taken".into()));
                }
                self.put(target, |at| done.persist_new(at, created_ms))?
            }
        };
        let placed = match placed {
            Ok(_) if self.read_fails() => Err(FileIoError::PlacedUnread(io::Error::other(
                "its read failed (a test)",
            ))),
            placed => placed,
        };
        let meta = match placed {
            Ok(m) => m,
            Err(FileIoError::TargetChanged) => return Ok(rescan(&mut self.report, target)),
            // In place, its metadata unread: recovery's.
            Err(FileIoError::PlacedUnread(_)) => {
                self.placed(node);
                return self.placed_not_committed(node);
            }
            Err(FileIoError::Io(e)) => return Ok(io_outcome(e)),
            Err(e) => return Ok(Outcome::Retry(e.to_string())),
        };
        self.placed(node);
        if self.crash_here(CrashPoint::AfterPlaced) {
            return Ok(Outcome::Retry("crash point reached".into()));
        }
        let target = target.clone();
        let os = self.cfg.os;
        let fails = self.commit_fails();
        let committed = self.db.write(|s| {
            if fails {
                return Err(StateError::Conflict("the commit failed (a test)".into()));
            }
            if let Some(row) = &current {
                s.delete_local(row.node)?;
            }
            s.delete_parked(node)?;
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::File,
                fingerprint: meta.fingerprint(),
                executable: meta.executable,
                hash: Some(local_hash),
                tags,
            })?;
            // The bytes were just fetched: equal even if the hashes use other algorithms.
            settle_placement(s, &record, os)
        });
        // The DB failing stops the run; the record is the next sync's recovery's.
        let at = match committed {
            Ok(at) => at,
            Err(e) => return Ok(Outcome::Fatal(e)),
        };
        self.note([(node, at)]);
        Ok(Outcome::Settled)
    }
}
