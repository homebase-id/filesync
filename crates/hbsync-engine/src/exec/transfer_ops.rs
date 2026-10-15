//! Transfers: their guards before the worker pool, placing and recording what it brought (and
//! what the remote writes of `remote.rs` did).

use super::*;

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
            } => self.prepare_download(*node, *parent, name, *replace, tags),
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
    ) -> Step<(Job, Ctx)> {
        let (mirror, current) = self
            .db
            .read(|s| Ok((s.get_mirror(node)?, s.local_by_node(node)?)))?;
        let Some(mirror) = mirror else {
            return Err(Outcome::stale("gone remotely"));
        };
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
        // What its commit records, as the drive has it now: kept until then.
        let record = mirror.state().map(|state| {
            Box::new(Placing {
                base: BaseRow {
                    node,
                    state,
                    remote: mirror.remote_ref(),
                    content_state: ContentState::Local,
                    local_name: None,
                    local_hash: None,
                },
                target: target.clone(),
            })
        });
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
            Ctx::Download {
                node,
                target,
                current,
                created_ms,
                tags: tags.clone(),
                record,
            },
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
        // Recently deleted holds a version this device never had (edited elsewhere, then
        // deleted): restoring over it would lose it. It stays there, and this version goes up
        // as a new item.
        if let Some(r) = &removed
            && r.row.file.map(|f| f.hash)
                != base.as_ref().and_then(|b| b.state.content).map(|c| c.hash)
        {
            return Err(self.detach(node, NodeId::new()).unwrap_or_else(|o| o));
        }
        let mut tags = tags.clone();
        let target = match existing {
            // In Recently deleted: restored with these bytes (an edit wins over the delete).
            None if let Some(r) = &removed => {
                content.unknown = r.row.carried_keys();
                tags = restored_tags(base.as_ref(), &tags, r);
                Some(r.row.remote_ref())
            }
            // A new node: the file's own creation time, kept from then on.
            None => {
                content.set_created_ms(ops::created_ms(&path));
                None
            }
            Some((file_id, version)) => {
                let Some(mirror) = &mirror else {
                    return Err(Outcome::stale("gone remotely"));
                };
                content.unknown = mirror.carried_keys();
                // A node from before creation times, or from a client that sends none.
                if content.created_ms().is_none() {
                    content.set_created_ms(ops::created_ms(&path));
                }
                Some(RemoteRef { file_id, version })
            }
        };
        content.set_tags(&tags);
        let job = Job::Upload {
            meta: NodeMeta {
                node,
                parent,
                kind,
                content,
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
            (_, _, JobResult::Failed(e)) => Ok(match e {
                JobError::Remote(RemoteError::AlreadyExists) => Outcome::Done, // an earlier try landed
                JobError::Remote(r) => remote_outcome(r),
                // Local file errors (locked, changed mid-read, short) are worth another try.
                e => Outcome::Retry(e.to_string()),
            }),
            (
                Ctx::Download {
                    node,
                    target,
                    current,
                    created_ms,
                    tags,
                    ..
                },
                _,
                JobResult::Downloaded { file, local_hash },
            ) => self.place_download(node, &target, current, created_ms, &tags, file, local_hash),
            (Ctx::Upload { hash, .. }, Job::Upload { meta, .. }, JobResult::Written(written)) => {
                let node = meta.node;
                let row = written_row(written, &meta, Some(hash));
                // The bytes were just sent: equal even if the hashes use other algorithms.
                self.finish(&[(node, true)], |s| {
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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn place_download(
        &mut self,
        node: NodeId,
        target: &RelPath,
        current: Option<LocalRow>,
        created_ms: Option<i64>,
        tags: &Tags,
        done: hbsync_fileio::CompletedFile,
        local_hash: hbsync_types::ContentHash,
    ) -> Step {
        if let Some(row) = &current
            && self.db.read(|s| s.local_by_node(node))?.as_ref() != Some(row)
        {
            done.keep_for_retry();
            return Ok(Outcome::stale("local item changed during the transfer"));
        }
        // The tags go on before the file is in place: a scan in between would read none, and
        // take that for the user removing them.
        let tags = write_tags(done.path(), tags);
        let placed = match &current {
            Some(row) if &row.path == target => {
                done.persist_over(&self.abs(target), &row.fingerprint, created_ms)
            }
            Some(row) => {
                if !self.on_disk_unchanged(row) || !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("old version changed or name taken".into()));
                }
                match self.move_to_trash(&row.path, node) {
                    Ok(()) => done.persist_new(&self.abs(target), created_ms),
                    Err(e) => {
                        done.keep_for_retry();
                        return Ok(io_outcome(e));
                    }
                }
            }
            None => {
                if !self.free_target(target, Some(node))? {
                    done.keep_for_retry();
                    return Ok(Outcome::Retry("name taken".into()));
                }
                done.persist_new(&self.abs(target), created_ms)
            }
        };
        let meta = match placed {
            Ok(m) => m,
            Err(FileIoError::TargetChanged) => return Ok(rescan(&mut self.report, target)),
            Err(FileIoError::Io(e)) => return Ok(io_outcome(e)),
            Err(e) => return Ok(Outcome::Retry(e.to_string())),
        };
        let target = target.clone();
        // The bytes were just fetched: equal even if the hashes use other algorithms.
        self.finish(&[(node, true)], |s| {
            if let Some(row) = &current {
                s.delete_local(row.node)?;
            }
            s.delete_parked(node)?;
            s.delete_placing(node)?;
            s.put_local(&LocalRow {
                path: target.clone(),
                node,
                kind: NodeKind::File,
                fingerprint: meta.fingerprint(),
                executable: meta.executable,
                hash: Some(local_hash),
                tags,
            })
        })
    }
}
