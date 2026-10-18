//! Settling a node: recording what both sides agree on as its base.

use super::*;
use crate::scope::Scope;

/// A node to settle after an operation, and what against.
#[derive(Debug, Clone, Copy)]
pub struct Settle<'a> {
    pub node: NodeId,
    /// The operation just made the local and remote bytes equal (a transfer), even if their
    /// hashes use other algorithms.
    pub same_bytes: bool,
    pub against: Against<'a>,
    /// What the operation did to the item's tags: the base records that ([`TagWrite`]).
    pub tags: TagWrite<'a>,
    /// Where this run's own operations settled the node's base: a local write's compare and set
    /// takes the base there as unmoved (the plan's next operation on a node follows its earlier
    /// one; only another engine, the feed or recovery moves it otherwise).
    pub ours: Option<RemoteRef>,
}

impl<'a> Settle<'a> {
    /// `node`, against the mirror.
    pub fn node(node: NodeId) -> Self {
        Settle {
            node,
            same_bytes: false,
            against: Against::Mirror,
            tags: TagWrite::Agreed,
            ours: None,
        }
    }

    /// `node` after a local write that acted on `drive` without putting the item in place, and
    /// did `tags` to its tags.
    pub fn acted(node: NodeId, drive: Option<&'a DriveVersion>, tags: TagWrite<'a>) -> Self {
        Settle {
            against: Against::acted(drive),
            tags,
            ..Settle::node(node)
        }
    }

    /// `node` after a local write that put the item in place from `drive`, and did `tags` to
    /// its tags.
    pub fn placed(node: NodeId, drive: Option<&'a DriveVersion>, tags: TagWrite<'a>) -> Self {
        Settle {
            against: Against::placed(drive),
            tags,
            ..Settle::node(node)
        }
    }
}

/// What a placement's commit writes once its item's row is in, and what recovery writes for
/// one whose commit was lost (the same): the base, settled against the version the record acts
/// on, its tags as the record says the write changed them; and for a folder, what is in it on the drive
/// marked dirty (it may have been put aside unseen, in a folder this device ignores which this
/// one was moved out of: it comes along, a level per folder made). Both check the base is still
/// at the version the record's plan saw, and nothing else (not what the run settled it at:
/// recovery can't know that), so they settle the same record the same way; one kept from before
/// records said which is settled against the base as it is.
pub(super) fn settle_placement(
    s: &Store<'_>,
    record: &Placing,
    scope: &Scope,
    os: Os,
) -> Result<Placed, StateError> {
    let node = record.base.node;
    // Gone on the drive since (deleted, in Recently deleted, or taken along by a folder there):
    // settled at the record's version whatever the plan saw, so the removal reaches the item.
    // Against a base that moved meanwhile, the item read as an edit, which won over the delete
    // and brought the node back. Only an item that reads as the node (its name one the drive
    // takes): the record stays otherwise.
    let gone = view::gone_on_drive(s, node, scope)?;
    if gone && !view::local_node(s, node, os, None)?.is_some_and(|l| l.state.is_some()) {
        return Ok(Placed::Kept);
    }
    let planned = if gone {
        PlannedBase::Unchecked
    } else {
        record.base_at
    };
    let base_at = match planned {
        PlannedBase::Unchecked => s.get_base(node)?.map(|b| b.remote.version),
        PlannedBase::Absent => None,
        PlannedBase::At(v) => Some(v),
    };
    let acted = DriveVersion {
        state: record.base.state.clone(),
        at: record.base.remote,
        base_at,
    };
    let tags = match (&record.prior_tags, &record.placed_tags) {
        (Some(before), Some(written)) => TagWrite::Wrote { before, written },
        _ => TagWrite::Unknown,
    };
    let how = Settle {
        same_bytes: true,
        ..Settle::placed(node, Some(&acted), tags)
    };
    let at = settle(s, how, os)?;
    if record.base.state.kind == NodeKind::Folder {
        mark_children(s, node)?;
    }
    Ok(Placed::Settled(at))
}

/// What [`settle_placement`] did.
pub(super) enum Placed {
    /// Settled: where it put the base, if it did (the record goes then).
    Settled(Option<RemoteRef>),
    /// Its node gone on the drive, its item not one the drive takes: the record stays.
    Kept,
}

impl Placed {
    /// Where it put the base, if it did.
    pub(super) fn at(self) -> Option<RemoteRef> {
        match self {
            Placed::Settled(at) => at,
            Placed::Kept => None,
        }
    }
}

/// Marks what is in folder `node` on the drive dirty.
pub(super) fn mark_children(s: &Store<'_>, node: NodeId) -> Result<(), StateError> {
    s.mirror_children(node)?
        .iter()
        .try_for_each(|child| s.mark_dirty(child.node))
}

/// Records in the base whatever both sides now agree on (sync-semantics: the base only takes
/// a value when both sides hold it). A local write settles against the version it acted on,
/// whatever the mirror holds by then: another folder's engine may have pulled the drive's next
/// version, or its removal, into it meanwhile. A placement's write-ahead record goes once its
/// base is written; until then (its item can't be read yet), recovery settles it later.
///
/// A local write's tags record what it wrote, tag by tag ([`TagWrite`]), whatever moved since:
/// a tag it changed takes the written value where the version it acted on holds it, the rest
/// keep the base's (a later version that removed a written tag reads as the drive's removal; a
/// change the user made after the write, as theirs). The rest (content, place, and the version
/// the base is at) settles only if the base is still at the version its plan saw, or where this
/// run's own operations settled it since (compare and set): where anything else moved it, those
/// stay as they are. Where this run's own remote write moved the node past the version it
/// acted on, they settle against that write's version while the mirror holds it. Either way,
/// where the drive moved on from the version they settle against (or they don't), the node
/// stays dirty, for the next plan to decide (an operation may come for a node that wasn't
/// dirty). Returns the version it put the base at, if it did.
pub fn settle(s: &Store<'_>, how: Settle<'_>, os: Os) -> Result<Option<RemoteRef>, StateError> {
    let node = how.node;
    let mirror = s.get_mirror(node)?;
    let base = s.get_base(node)?;
    let at = base.as_ref().map(|b| b.remote);
    let ours = how.ours.filter(|o| at == Some(*o));
    let drive = how.against.drive();
    // A local write's version of the drive its content, place and version settle against
    // (`None` in it: the base moved), with the tags of the version it acted on.
    let acted = drive.map(|d| {
        let state = match ours {
            _ if at.map(|r| r.version) == d.base_at => Some((d.state.clone(), d.at)),
            Some(o) if o == d.at => Some((d.state.clone(), d.at)),
            Some(o) => mirror
                .as_ref()
                .filter(|m| m.remote_ref() == o)
                .and_then(MirrorRow::state)
                .map(|r| (r, o)),
            None => None,
        };
        state.map(|(r, at)| {
            let tags = d.state.tags.clone();
            (NodeState { tags, ..r }, at)
        })
    });
    // What the local tags read as, against the tags they're judged against.
    let reference = match drive {
        Some(d) => Some(d.state.tags.clone()),
        None => mirror.as_ref().map(|m| m.tags()),
    };
    let Some(local) = view::local_node(s, node, os, reference.as_ref())? else {
        match mirror {
            None => {
                s.delete_base(node)?;
            }
            // No local bytes by design: the base follows the remote.
            Some(m)
                if base
                    .as_ref()
                    .is_some_and(|b| b.content_state == ContentState::OnlineOnly) =>
            {
                if let (Some(mut b), Some(r)) = (base, m.state()) {
                    b.state = r;
                    b.remote = m.remote_ref();
                    s.put_base(&b)?;
                    return Ok(Some(b.remote));
                }
            }
            Some(_) => {}
        }
        return Ok(None);
    };
    // What the write left, as the item reads (what this device can't hold is the drive's), and
    // what was there, read alike, but for a tag this device can't hold at all: the item never
    // held that, so the write leaves the drive's value of it, as the base records.
    let reference = reference.unwrap_or_default();
    let holdable = Tags::new(
        reference
            .iter()
            .filter(|t| tags::express(t, os).is_some())
            .cloned(),
    );
    let filled = match how.tags {
        TagWrite::Wrote { before, written } => Some((
            Tags::fill(Some(before), &holdable, os),
            Tags::fill(Some(written), &reference, os),
        )),
        _ => None,
    };
    let how = Settle {
        tags: match &filled {
            Some((before, written)) => TagWrite::Wrote { before, written },
            None => how.tags,
        },
        ..how
    };
    let newest = mirror.as_ref().map(MirrorRow::remote_ref);
    let acted = match acted {
        Some(None) => {
            s.mark_dirty(node)?;
            if let (Some(mut b), Some(l), Some(d)) = (base, &local.state, drive) {
                let tags = how.tags.settle(&b.state.tags, &l.tags, &d.state.tags);
                if tags != b.state.tags {
                    b.state.tags = tags;
                    s.put_base(&b)?;
                }
            }
            if how.against.placed_from() {
                s.delete_placing(node)?;
            }
            return Ok(None);
        }
        Some(Some((r, at))) => {
            if newest != Some(at) {
                s.mark_dirty(node)?;
            }
            Some((r, at))
        }
        None => None,
    };
    let row = match (acted, mirror) {
        (Some((r, at)), _) => settled(base, &local, &r, at, how, os),
        (None, Some(m)) => m
            .state()
            .and_then(|r| settled(base, &local, &r, m.remote_ref(), how, os)),
        (None, None) => None,
    };
    let Some(row) = row else {
        return Ok(None);
    };
    s.put_base(&row)?;
    if how.against.placed_from() {
        s.delete_placing(node)?;
    }
    Ok(Some(row.remote))
}

/// The base once `local` settles against the drive's `remote` (at `at`), `None` where they
/// don't agree enough for one.
fn settled(
    base: Option<BaseRow>,
    local: &view::LocalNode,
    remote: &NodeState,
    at: hbsync_types::RemoteRef,
    how: Settle<'_>,
    os: Os,
) -> Option<BaseRow> {
    let l = local.state.as_ref()?;
    let mut agreed = Agreement::of(l, remote, how.same_bytes);
    // An item at this device's spelling of the drive's name is at that name, though the name
    // map wouldn't give it back (decomposed, an escape's own code point): the spelling is
    // recorded below. Else it reads as renamed from the first placement on.
    agreed.place |= l.parent == remote.parent
        && l.name != remote.name
        && spells(&local.row.path, &remote.name, os);
    let prior = base
        .as_ref()
        .map(|b| b.state.tags.clone())
        .unwrap_or_default();
    let mut state = agreed.settle(
        base.as_ref().map(|b| b.state.clone()),
        l,
        remote,
        how.against.placed_from(),
    )?;
    state.tags = how.tags.settle(&prior, &l.tags, &remote.tags);
    let mut row = base.unwrap_or_else(|| BaseRow::new(how.node, remote.clone(), at));
    row.state = state;
    row.remote = at;
    if agreed.place {
        row.local_name = recorded_name(&local.row.path, &remote.name, os);
    }
    if agreed.content {
        row.local_hash = l.content.map(|c| c.hash);
    }
    Some(row)
}

/// Whether `path` is where this device spells the drive's `name` in its folder (where the
/// executor puts it), whether or not the name map gives `name` back from it.
pub(crate) fn spells(path: &RelPath, name: &str, os: Os) -> bool {
    spell(&path.parent().unwrap_or_default(), name, os).is_ok_and(|p| p == *path)
}

/// The local spelling of `path`'s name, recorded in the base when the name map wouldn't give it
/// back from the remote `name`.
pub(super) fn recorded_name(path: &RelPath, name: &str, os: Os) -> Option<String> {
    path.name()
        .filter(|ln| to_remote(ln, os).as_deref() != Ok(name))
        .map(str::to_owned)
}

/// A metadata-only update of a node to `meta` if it's still at `target` (its payload, of
/// content `hash`, stays); its mirror row after.
pub(crate) fn update_meta(
    remote: &dyn Remote,
    target: &Target,
    meta: &NodeMeta,
    hash: Option<hbsync_types::ContentHash>,
) -> Result<MirrorRow, RemoteError> {
    let written = remote.update(target, meta, None)?;
    Ok(MirrorRow::written(
        written,
        meta.node,
        meta.parent,
        meta.kind,
        &meta.content,
        hash,
    ))
}

/// A mirror row's node as written back: at `parent` under `name`, removed or not.
pub(crate) fn meta_at(row: &MirrorRow, parent: NodeId, name: &str, removed: bool) -> NodeMeta {
    NodeMeta {
        node: row.node,
        parent,
        kind: row.kind,
        content: row.content(name),
        removed,
    }
}

/// [`meta_at`] where and as the row has it.
pub(crate) fn meta(row: &MirrorRow, removed: bool) -> NodeMeta {
    meta_at(row, row.parent, &row.name, removed)
}

/// A removal written (`row`, its answer; `None`: the drive no longer had the file): out of the
/// mirror, into Recently deleted at the answer's time (open if it gave none, until the feed
/// brings it).
pub(crate) fn record_removed(
    s: &Store<'_>,
    file_id: hbsync_types::RemoteFileId,
    row: Option<MirrorRow>,
) -> Result<(), StateError> {
    s.delete_mirror(file_id)?;
    match row {
        Some(row) => s.put_removed(&RemovedRow {
            removed_at: row.changed_at,
            row,
        }),
        None => Ok(()),
    }
}

/// The tags a write over a drive version sends: `ours` merged with that version's, which may
/// hold tags this device never saw (§11): a no-op where the plan merged them already, the
/// drive's tags kept where it didn't (a node restored from Recently deleted, or taken along).
pub(super) fn merged_with_drive(base: Option<&BaseRow>, ours: &Tags, drive: &Tags) -> Tags {
    let base = base.map(|b| b.state.tags.clone()).unwrap_or_default();
    Tags::merge(&base, ours, drive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_platform::{FileId, Fingerprint};
    use hbsync_state::MirrorFile;
    use hbsync_types::{FileContent, RemoteFileId, RemoteRef, Uuid, VersionTag};

    fn content(data: &str) -> FileContent {
        FileContent {
            hash: hbsync_fileio::hash_bytes(data.as_bytes(), HashAlgorithm::Blake3),
            size: data.len() as u64,
            mtime_ms: 0,
            executable: false,
        }
    }

    fn version() -> RemoteRef {
        RemoteRef {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
        }
    }

    /// A download of a node that fetched `fetched` at `at`, committed with the mirror holding
    /// `mirror` by then (none: another engine pulled its removal): the base it leaves.
    /// The rows of `node`, a file `f` at the top: local holding `local`, the mirror's at
    /// `mirror` if any.
    fn rows(
        s: &Store<'_>,
        node: NodeId,
        local: FileContent,
        mirror: Option<(FileContent, RemoteRef)>,
    ) -> Result<(), StateError> {
        if let Some((c, at)) = mirror {
            s.put_mirror(&MirrorRow {
                file_id: at.file_id,
                version: at.version,
                node,
                parent: NodeId::ROOT,
                name: "f".into(),
                kind: NodeKind::File,
                file: Some(MirrorFile {
                    hash: c.hash,
                    size: c.size,
                    mtime_ms: 0,
                    executable: false,
                }),
                content_extra: Default::default(),
                token: None,
                changed_at: None,
            })?;
        }
        s.put_local(&LocalRow {
            path: RelPath::new("f").expect("a path"),
            node,
            kind: NodeKind::File,
            fingerprint: Fingerprint {
                size: local.size,
                mtime_ns: 0,
                ctime_ns: 0,
                file_id: FileId { volume: 1, file: 1 },
            },
            executable: Some(false),
            hash: Some(local.hash),
            tags: None,
        })
    }

    fn download_commit(
        fetched: FileContent,
        at: RemoteRef,
        mirror: Option<(FileContent, RemoteRef)>,
    ) -> Result<Option<BaseRow>, StateError> {
        let node = NodeId::new();
        let mut db = StateDb::open_in_memory()?;
        db.write(|s| {
            rows(s, node, fetched, mirror)?;
            let acted = DriveVersion {
                state: NodeState::file(NodeId::ROOT, "f", fetched),
                at,
                base_at: None,
            };
            let how = Settle {
                same_bytes: true,
                ..Settle::placed(node, Some(&acted), TagWrite::Nothing)
            };
            settle(s, how, Os::Linux)?;
            s.get_base(node)
        })
    }

    /// A download settles against the version it fetched, not the mirror's newer one: another
    /// folder's engine can pull the drive's next version into the shared mirror meanwhile, and
    /// a base claiming those bytes would never fetch them (and upload over them on an edit).
    /// The base is of that version, as recovery writes it (fork ids derive from it).
    #[test]
    fn a_download_settles_against_the_bytes_it_fetched() -> Result<(), StateError> {
        let (fetched, at) = (content("two"), version());
        let base = download_commit(fetched, at, Some((content("three"), version())))?;
        let base = base.expect("a base");
        assert_eq!(base.state.content, Some(fetched));
        assert_eq!(base.remote, at);
        Ok(())
    }

    /// The node's removal pulled into the mirror meanwhile: the download still settles against
    /// what it fetched, as recovery would. Without a base the next plan would read the bytes
    /// as a local edit and put the file back on the drive; with it, the removal reaches here.
    #[test]
    fn a_download_settles_with_the_mirror_row_gone() -> Result<(), StateError> {
        let (fetched, at) = (content("two"), version());
        let base = download_commit(fetched, at, None)?.expect("a base");
        assert_eq!(base.state.content, Some(fetched));
        assert_eq!(base.remote, at);
        Ok(())
    }

    fn tags(names: &[&str]) -> Tags {
        Tags::new(names.iter().map(|n| hbsync_types::Tag::new(*n, 0)))
    }

    /// The local item of `node` holds `tags`, as the scan read them.
    fn holds(s: &Store<'_>, node: NodeId, tags: Tags) -> Result<(), StateError> {
        let row = s.local_by_node(node)?.expect("a local row");
        s.put_local(&LocalRow {
            tags: Some(tags),
            ..row
        })
    }

    /// This run wrote the drive's next version since the plan (an upload, say: the base at it),
    /// and another engine pulled the one after into the mirror; a tag write the plan made
    /// against the version before both then commits. It would settle against this run's
    /// version, but the mirror no longer holds that: the base's content and version stay, the
    /// node dirty, for the next plan to decide on what is there now. Its tags record what it
    /// wrote: the tag it wrote is agreed, not taken for the user's.
    #[test]
    fn a_write_on_an_older_version_records_only_its_tags() -> Result<(), StateError> {
        let node = NodeId::new();
        let (c, older, newer, newest) = (content("one"), version(), version(), version());
        let mut db = StateDb::open_in_memory()?;
        db.write(|s| {
            rows(s, node, c, Some((c, newest)))?;
            holds(s, node, tags(&["Work"]))?;
            let file = NodeState::file(NodeId::ROOT, "f", c);
            let base = BaseRow {
                local_hash: Some(c.hash),
                ..BaseRow::new(node, file.clone(), newer)
            };
            s.put_base(&base)?;
            let acted = DriveVersion {
                state: NodeState {
                    tags: tags(&["Work"]),
                    ..file.clone()
                },
                at: older,
                // The plan saw the base at the older version.
                base_at: Some(older.version),
            };
            let (none, work) = (Tags::default(), tags(&["Work"]));
            let wrote = TagWrite::Wrote {
                before: &none,
                written: &work,
            };
            let how = Settle {
                ours: Some(newer),
                ..Settle::acted(node, Some(&acted), wrote)
            };
            assert_eq!(settle(s, how, Os::Linux)?, None);
            let want = BaseRow {
                state: NodeState {
                    tags: tags(&["Work"]),
                    ..file
                },
                ..base
            };
            assert_eq!(s.get_base(node)?, Some(want));
            assert!(s.is_dirty(node)?);
            Ok(())
        })
    }

    /// Something else (another folder's engine, the feed, recovery) moved the base since the
    /// plan: a download settling against the version it acted on leaves its content, place and
    /// version alone. Its tags record what it wrote, tag by tag: a tag it added that the version
    /// holds is in, one it removed that the version lacks is out, and one it didn't change
    /// stays as the base has it (red: the base lacks it, though the item and version disagree).
    #[test]
    fn a_base_moved_by_another_keeps_all_but_the_written_tags() -> Result<(), StateError> {
        let node = NodeId::new();
        let (c, older, other) = (content("one"), version(), version());
        let mut db = StateDb::open_in_memory()?;
        db.write(|s| {
            rows(s, node, c, Some((c, older)))?;
            holds(s, node, tags(&["Red", "Work"]))?;
            // Elsewhere, with other bytes, at another version, tagged blue.
            let base = BaseRow {
                local_hash: Some(content("zero").hash),
                ..BaseRow::new(
                    node,
                    NodeState {
                        tags: tags(&["Blue"]),
                        ..NodeState::file(NodeId::ROOT, "e", content("zero"))
                    },
                    other,
                )
            };
            s.put_base(&base)?;
            // What the item and the drive agree on: here, these bytes.
            let acted = DriveVersion {
                state: NodeState {
                    tags: tags(&["Work"]),
                    ..NodeState::file(NodeId::ROOT, "f", c)
                },
                at: older,
                base_at: Some(older.version),
            };
            let (before, written) = (tags(&["Blue", "Red"]), tags(&["Red", "Work"]));
            let how = Settle {
                same_bytes: true,
                ..Settle::placed(
                    node,
                    Some(&acted),
                    TagWrite::Wrote {
                        before: &before,
                        written: &written,
                    },
                )
            };
            assert_eq!(settle(s, how, Os::Linux)?, None);
            let want = BaseRow {
                state: NodeState {
                    tags: tags(&["Work"]),
                    ..base.state.clone()
                },
                ..base
            };
            assert_eq!(s.get_base(node)?, Some(want));
            assert!(s.is_dirty(node)?);
            Ok(())
        })
    }

    /// A tag this device can't hold at all (Linux: a name with a comma) the item never held: a
    /// write over it leaves the drive's value, which the base records as agreed. Read from the
    /// drive before the write too, it looked unchanged, the base never took it, and once the
    /// drive's copy went, the item's next upload dropped it.
    #[test]
    fn a_tag_this_device_cant_hold_is_the_drives() -> Result<(), StateError> {
        let node = NodeId::new();
        let (c, at) = (content("one"), version());
        let mut db = StateDb::open_in_memory()?;
        db.write(|s| {
            rows(s, node, c, Some((c, at)))?;
            holds(s, node, Tags::default())?;
            let file = NodeState::file(NodeId::ROOT, "f", c);
            s.put_base(&BaseRow {
                local_hash: Some(c.hash),
                ..BaseRow::new(node, file.clone(), at)
            })?;
            let acted = DriveVersion {
                state: NodeState {
                    tags: tags(&["a,b"]),
                    ..file
                },
                at,
                base_at: Some(at.version),
            };
            let none = Tags::default();
            let wrote = TagWrite::Wrote {
                before: &none,
                written: &none,
            };
            let how = Settle {
                same_bytes: true,
                ..Settle::placed(node, Some(&acted), wrote)
            };
            settle(s, how, Os::Linux)?;
            let base = s.get_base(node)?.map(|b| b.state.tags);
            assert_eq!(base, Some(tags(&["a,b"])));
            Ok(())
        })
    }

    /// A write's tags go into the base as written, tag by tag: what it changed that the version
    /// it acted on holds so (work added, blue removed); not what it changed that the version
    /// holds otherwise (red, in another colour), nor what it didn't change (old stays as the
    /// base has it, though the version dropped it).
    #[test]
    fn the_base_records_what_a_write_changed() {
        let tag = |n: &str, c: u8| hbsync_types::Tag::new(n, c);
        let base = Tags::new([tag("Blue", 0), tag("Old", 0)]);
        let before = Tags::new([tag("Blue", 0), tag("Old", 0)]);
        let written = Tags::new([tag("Old", 0), tag("Red", 1), tag("Work", 0)]);
        let acted = Tags::new([tag("Red", 2), tag("Work", 0)]);
        let wrote = TagWrite::Wrote {
            before: &before,
            written: &written,
        };
        assert_eq!(
            wrote.settle(&base, &written, &acted),
            Tags::new([tag("Old", 0), tag("Work", 0)])
        );
        assert_eq!(TagWrite::Nothing.settle(&base, &written, &acted), base);
    }
}
