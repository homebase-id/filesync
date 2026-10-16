//! Settling a node: recording what both sides agree on as its base.

use super::*;

/// Records in the base whatever both sides now agree on (sync-semantics: the base only takes
/// a value when both sides hold it). `content_hint`: the operation just made the local and
/// remote bytes equal (a transfer), even if their hashes use other algorithms.
pub fn settle(s: &Store<'_>, node: NodeId, os: Os, content_hint: bool) -> Result<(), StateError> {
    let mirror = s.get_mirror(node)?;
    let local = view::local_node(s, node, os, mirror.as_ref().map(|m| m.tags()).as_ref())?;
    let base = s.get_base(node)?;
    let online_only = base
        .as_ref()
        .is_some_and(|b| b.content_state == ContentState::OnlineOnly);
    let (local, mirror) = match (local, mirror) {
        (None, None) => {
            s.delete_base(node)?;
            return Ok(());
        }
        (Some(l), Some(m)) => (l, m),
        (None, Some(m)) if online_only => {
            // No local bytes by design: the base follows the remote.
            if let (Some(mut b), Some(r)) = (base, m.state()) {
                b.state = r;
                b.remote = m.remote_ref();
                s.put_base(&b)?;
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    let (Some(l), Some(r)) = (local.state.clone(), mirror.state()) else {
        return Ok(());
    };
    let agreed = Agreement::of(&l, &r, content_hint);
    let Some(state) = agreed.settle(base.as_ref().map(|b| b.state.clone()), &l, &r) else {
        return Ok(());
    };
    let mut row = base.unwrap_or_else(|| BaseRow::new(node, r.clone(), mirror.remote_ref()));
    row.state = state;
    row.remote = mirror.remote_ref();
    if agreed.place {
        row.local_name = recorded_name(&local.row.path, &r.name, os);
    }
    if agreed.content {
        row.local_hash = l.content.map(|c| c.hash);
    }
    s.put_base(&row)
}

/// The local spelling of `path`'s name, recorded in the base when the name map wouldn't give it
/// back from the remote `name`.
pub(super) fn recorded_name(path: &RelPath, name: &str, os: Os) -> Option<String> {
    path.name()
        .filter(|ln| to_remote(ln, os).as_deref() != Ok(name))
        .map(str::to_owned)
}

/// The mirror row of a node written as `meta` (a file with its content `hash`): the file as
/// written, with its token.
pub(super) fn written_row(
    written: Target,
    meta: &NodeMeta,
    hash: Option<hbsync_types::ContentHash>,
) -> MirrorRow {
    MirrorRow {
        token: written.token,
        ..MirrorRow::from_content(
            written.at,
            meta.node,
            meta.parent,
            meta.kind,
            &meta.content,
            hash,
        )
    }
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
    Ok(written_row(written, meta, hash))
}

/// Gives the item at `path` these tags and returns what it holds now. Tags are no reason to
/// fail an operation: where they can't be written (a read-only item, a full attribute block),
/// they read as not known, and the drive's stand (§11).
pub(super) fn write_tags(path: &Path, tags: &Tags) -> Option<Tags> {
    tags::write(path, tags).ok().flatten()
}

/// The tags of a node this device restores from Recently deleted: `ours` merged with the
/// removed version's, which may hold tags this device never saw (§11).
pub(super) fn restored_tags(base: Option<&BaseRow>, ours: &Tags, removed: &RemovedRow) -> Tags {
    let base = base.map(|b| b.state.tags.clone()).unwrap_or_default();
    Tags::merge(&base, ours, &removed.row.tags())
}
