//! Operations the drive refused for good (sync semantics §8a, refused): a request it won't take
//! as it is (a bad request, a content it won't hold, a hash it won't have, a file it can't
//! give), or a download whose bytes didn't match twice at one version. Such an operation isn't
//! sent again while what it would send or fetch is the same: its key ([`key`]) holds all of
//! that, so any change of the node, here or on the drive, is another operation. A refusal
//! lasts [`REFUSED_FOR`] at most, and until Sync now.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use hbsync_fileio::hash_bytes;
use hbsync_state::{ReadTx, Refused, StateDb, StateError};
use hbsync_types::{FileContent, HashAlgorithm, NodeId, unix_now_ms};

use crate::reconcile::{Op, Requirement};

/// How long a refusal holds its operation back: then it is sent once more.
pub const REFUSED_FOR: Duration = Duration::from_secs(3600);

/// The key of `op`, which sends the local file `content` (its hash, size, time and executable
/// bit) if it uploads: every input of the operation, so another key is another operation.
pub fn key(op: &Op, content: Option<&FileContent>) -> String {
    let mut text = format!("{op:?}");
    if let Some(c) = content {
        // The planner's marker for a file hashed on send isn't its bytes: keyed so, two such
        // files would be one operation.
        assert!(
            !c.hash.is_on_send(),
            "a refusal keyed by the hash-on-send marker"
        );
        text.push_str(&format!(
            "|{}|{}|{}|{}",
            c.hash, c.size, c.mtime_ms, c.executable
        ));
    }
    hash_bytes(text.as_bytes(), HashAlgorithm::Blake3).to_string()
}

/// The kinds of operation the drive can refuse ([`Op::kind`]): `name`'s.
fn kind_named(name: &str) -> Option<&'static str> {
    [
        "RemoteCreate",
        "RemoteUpdate",
        "RemoteTags",
        "RemoteDelete",
        "LocalDownload",
    ]
    .into_iter()
    .find(|k| *k == name)
}

/// [`key`] of `op`, with the bytes its local item holds now if it uploads.
pub(crate) fn key_of(s: &ReadTx<'_>, op: &Op) -> Result<String, StateError> {
    let row = match op.uploads() {
        true => s.local_by_node(op.node())?,
        false => None,
    };
    let Some(row) = row else {
        return Ok(key(op, None));
    };
    let Some(hash) = row.hash else {
        // A file hashed on send has no hash before it is sent: its size, mtime and file id
        // stand for its bytes (a change of them is another operation; a tag's isn't).
        let fp = &row.fingerprint;
        let text = format!(
            "{op:?}|on send|{}|{}|{:?}|{:?}",
            fp.size, fp.mtime_ns, fp.file_id, row.executable
        );
        return Ok(hash_bytes(text.as_bytes(), HashAlgorithm::Blake3).to_string());
    };
    let content = FileContent {
        hash,
        size: row.fingerprint.size,
        mtime_ms: row.fingerprint.mtime_ns / 1_000_000,
        executable: row.executable.unwrap_or(false),
    };
    Ok(key(op, Some(&content)))
}

/// Takes the operations `refused` says the drive refused as they are out of `ops`, and those
/// that wait for one of them, which would only wait again: what goes into a folder whose create
/// is refused, the removals in a folder whose removal is, what goes to a local place the node
/// of a blocked operation would have left, and the delete or forgetting of a local folder a blocked
/// operation's node is in, here, in the base or on the drive (`place`, [`Place`]; names
/// compared as the folder does, `case_insensitive`). Returns `(kept, refused)`; the waiting
/// ones are blocked silently: the refusal they wait for says why.
pub fn take_refused<E>(
    ops: Vec<Op>,
    mut refused: impl FnMut(&Op) -> Result<bool, E>,
    mut place: impl FnMut(NodeId) -> Result<Place, E>,
    case_insensitive: bool,
) -> Result<(Vec<Op>, Vec<Op>), E> {
    let mut blocked = Blocked {
        case_insensitive,
        ..Blocked::default()
    };
    let (mut kept, mut out) = (Vec::new(), Vec::new());
    for op in ops {
        if refused(&op)? {
            blocked.add(&op, &mut place)?;
            out.push(op);
        } else {
            kept.push(op);
        }
    }
    if out.is_empty() {
        return Ok((kept, out));
    }
    // What waits for a blocked operation, and what waits for that, in any order.
    loop {
        let (waiting, rest): (Vec<Op>, Vec<Op>) = std::mem::take(&mut kept)
            .into_iter()
            .partition(|op| blocked.waits(op));
        kept = rest;
        if waiting.is_empty() {
            return Ok((kept, out));
        }
        for op in &waiting {
            blocked.add(op, &mut place)?;
        }
    }
}

/// Where a node is ([`take_refused`]).
#[derive(Debug, Default)]
pub struct Place {
    /// Its local item's folder and name.
    pub local: Option<(NodeId, String)>,
    /// The folders it is in: here, in the base, on the drive.
    pub folders: Vec<NodeId>,
}

/// What blocked operations leave undone: the folders whose create, and whose removal, is blocked,
/// the local places their nodes don't leave, and the local folders their nodes are in.
#[derive(Default)]
struct Blocked {
    creates: HashSet<NodeId>,
    removals: HashSet<NodeId>,
    places: HashSet<(NodeId, String)>,
    folders: HashSet<NodeId>,
    case_insensitive: bool,
}

impl Blocked {
    fn add<E>(
        &mut self,
        op: &Op,
        place: &mut impl FnMut(NodeId) -> Result<Place, E>,
    ) -> Result<(), E> {
        match op {
            Op::RemoteCreate { node, .. } => drop(self.creates.insert(*node)),
            Op::RemoteDelete { node, .. } => drop(self.removals.insert(*node)),
            _ => {}
        }
        let at = place(op.node())?;
        if op.vacates_local_name() == Some(op.node())
            && let Some((parent, name)) = &at.local
        {
            let key = self.key(name);
            self.places.insert((*parent, key));
        }
        self.folders.extend(at.folders);
        Ok(())
    }

    /// The name as the folder compares names.
    fn key(&self, name: &str) -> String {
        match self.case_insensitive {
            true => hbsync_platform::name::name_key(name),
            false => name.to_owned(),
        }
    }

    /// Whether `op` waits for a blocked operation.
    fn waits(&self, op: &Op) -> bool {
        let waits_for_folder = match op.requires() {
            Some(Requirement::RemoteFolder(f)) => self.creates.contains(&f),
            Some(Requirement::FolderFirst(f)) => self.removals.contains(&f),
            None => false,
        };
        let goes_to = match op {
            Op::LocalDownload { parent, name, .. }
            | Op::LocalMkdir { parent, name, .. }
            | Op::LocalMove { parent, name, .. } => Some((*parent, self.key(name))),
            _ => None,
        };
        // A folder isn't deleted or forgotten here while it holds a node a blocked operation
        // would have taken out.
        let empties = matches!(op, Op::LocalDelete { node, .. } | Op::Forget { node, .. }
            if self.folders.contains(node));
        waits_for_folder || empties || goes_to.is_some_and(|p| self.places.contains(&p))
    }
}

/// A folder's refusals as its engine keeps them between runs: whether it has none (found so,
/// and none written since: not read again then), and reading, pruning, applying and clearing
/// them. A run holds the ones that hold ([`Refusals`]).
#[derive(Debug, Default)]
pub(crate) struct FolderRefusals {
    none: bool,
    /// [`FolderRefusals::none`] when the run under way started.
    none_before_run: bool,
}

impl FolderRefusals {
    /// The folder's refusals that still hold ([`REFUSED_FOR`]; older ones are forgotten), and
    /// the operations of `ops` refused as they are taken out, with what waits for them
    /// ([`take_refused`]; names compared as the folder does, `case_insensitive`): the refusals,
    /// for the run, and the refused, with why.
    pub(crate) fn take(
        &mut self,
        db: &mut StateDb,
        ops: &mut Vec<Op>,
        case_insensitive: bool,
    ) -> Result<(Refusals, Vec<(Op, String)>), StateError> {
        let refusals = self.load(db)?;
        if refusals.is_empty() {
            return Ok((refusals, Vec::new()));
        }
        let (kept, out) = db.read(|s| {
            take_refused(
                std::mem::take(ops),
                |op| match refusals.get(op) {
                    Some(r) if r.refused => Ok(key_of(s, op)? == r.key),
                    _ => Ok(false),
                },
                |node| place_of(s, node),
                case_insensitive,
            )
        })?;
        *ops = kept;
        let out = out
            .into_iter()
            .map(|op| {
                let why = refusals
                    .get(&op)
                    .map_or_else(String::new, |r| r.why.clone());
                (op, why)
            })
            .collect();
        Ok((refusals, out))
    }

    /// The folder's refusals that still hold; older ones are forgotten.
    fn load(&mut self, db: &mut StateDb) -> Result<Refusals, StateError> {
        if self.none {
            return Ok(Refusals::default());
        }
        let rows = db.read(|s| s.all_refused())?;
        if rows.is_empty() {
            self.none = true;
            return Ok(Refusals::default());
        }
        let since = unix_now_ms().saturating_sub(REFUSED_FOR.as_millis() as i64);
        let (live, old): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| r.at >= since);
        if !old.is_empty() {
            db.write(|s| s.folder().prune_refused(since).map(drop))?;
        }
        Ok(Refusals::new(live))
    }

    /// Forgets the folder's refusals (Sync now): each refused operation is sent once more.
    pub(crate) fn clear(&mut self, db: &mut StateDb) -> Result<(), StateError> {
        db.write(|s| s.folder().prune_refused(i64::MAX).map(drop))?;
        self.none = true;
        Ok(())
    }

    /// A run starts, which may record refusals: the folder isn't known to have none until it
    /// says it wrote none ([`FolderRefusals::ran`]; a run that fails may have).
    pub(crate) fn running(&mut self) {
        self.none_before_run = std::mem::replace(&mut self.none, false);
    }

    /// The run ended; `wrote`: it recorded a refusal (or a strike).
    pub(crate) fn ran(&mut self, wrote: bool) {
        self.none = self.none_before_run && !wrote;
    }
}

/// Where `node` is ([`Place`]): its local item's folder and name, and the folders it is in
/// here, in the base and on the drive.
fn place_of(s: &ReadTx<'_>, node: NodeId) -> Result<Place, StateError> {
    let mut place = Place::default();
    if let Some(row) = s.local_by_node(node)?
        && let Some(name) = row.path.name()
        && let Some(parent) = s.local_parent(node)?
    {
        place.local = Some((parent, name.to_owned()));
        place.folders.push(parent);
    }
    place
        .folders
        .extend(s.get_base(node)?.map(|b| b.state.parent));
    place.folders.extend(s.get_mirror(node)?.map(|m| m.parent));
    Ok(place)
}

/// The record of `op`'s refusal (`refused`; else its first strike, a download whose bytes
/// didn't match once), with `why`, now.
pub(crate) fn record(
    s: &ReadTx<'_>,
    op: &Op,
    why: &str,
    refused: bool,
) -> Result<Refused, StateError> {
    Ok(Refused {
        node: op.node(),
        op: op.kind().to_owned(),
        key: key_of(s, op)?,
        refused,
        why: why.to_owned(),
        at: unix_now_ms(),
    })
}

/// A run's refusals of the operations planned, by node and operation kind.
#[derive(Debug, Default)]
pub(crate) struct Refusals(HashMap<(NodeId, &'static str), Refused>);

impl Refusals {
    pub(crate) fn new(rows: Vec<Refused>) -> Self {
        // A kind no operation has (a later client's) can't match: left out.
        Refusals(
            rows.into_iter()
                .filter_map(|r| Some(((r.node, kind_named(&r.op)?), r)))
                .collect(),
        )
    }

    pub(crate) fn get(&self, op: &Op) -> Option<&Refused> {
        self.0.get(&(op.node(), op.kind()))
    }

    /// `op` is done: whether it had a refusal (or a strike), which is over now (its row to
    /// delete).
    pub(crate) fn done(&mut self, op: &Op) -> bool {
        self.0.remove(&(op.node(), op.kind())).is_some()
    }

    /// Whether `strike` of `op` (a download's bytes not matching) is its second at the same
    /// key: refused then.
    pub(crate) fn twice(&self, op: &Op, strike: &Refused) -> bool {
        self.get(op)
            .is_some_and(|r| !r.refused && r.key == strike.key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use hbsync_types::{NodeKind, Tags};

    use super::*;

    fn create(node: NodeId, parent: NodeId, kind: NodeKind) -> Op {
        Op::RemoteCreate {
            node,
            parent,
            name: "n".into(),
            kind,
            tags: Tags::default(),
        }
    }

    /// What waits for a refused operation is blocked with it, silently; the rest goes.
    #[test]
    fn what_waits_for_a_refusal_is_blocked_with_it() {
        let (folder, child, grandchild, other) =
            (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
        let ops = vec![
            create(grandchild, child, NodeKind::File),
            create(child, folder, NodeKind::Folder),
            create(folder, NodeId::ROOT, NodeKind::Folder),
            create(other, NodeId::ROOT, NodeKind::File),
            Op::Forget {
                node: folder,
                subtree: Vec::new(),
            },
        ];
        let refused = |op: &Op| {
            Ok::<_, Infallible>(matches!(op, Op::RemoteCreate { node, .. } if *node == folder))
        };
        // The child is in the folder here: the folder isn't forgotten while it waits.
        let place = |node: NodeId| {
            let parent = match node {
                n if n == child => folder,
                n if n == grandchild => child,
                _ => NodeId::ROOT,
            };
            Ok(Place {
                local: None,
                folders: vec![parent],
            })
        };
        let Ok((kept, out)) = take_refused(ops, refused, place, false);
        assert_eq!(out.len(), 1);
        assert_eq!(kept, [create(other, NodeId::ROOT, NodeKind::File)]);
    }

    /// A download into the place a blocked download's item would have left waits; one into
    /// another place doesn't, a name only alike in case on a folder that tells case apart too.
    #[test]
    fn a_place_a_blocked_item_keeps_blocks_what_goes_there() {
        let (blocked, mover, other) = (NodeId::new(), NodeId::new(), NodeId::new());
        let download = |node, name: &str, replace| Op::LocalDownload {
            node,
            parent: NodeId::ROOT,
            name: name.into(),
            replace,
            tags: Tags::default(),
            drive: None,
        };
        let ops = || {
            vec![
                download(blocked, "new", true),
                download(mover, "old", false),
                download(other, "OLD", false),
            ]
        };
        let refused = |op: &Op| Ok::<_, Infallible>(op.node() == blocked);
        let place = |node: NodeId| {
            Ok(Place {
                local: (node == blocked).then(|| (NodeId::ROOT, "old".to_owned())),
                folders: Vec::new(),
            })
        };
        let Ok((kept, _)) = take_refused(ops(), refused, place, false);
        assert_eq!(kept, [download(other, "OLD", false)]);
        let Ok((kept, _)) = take_refused(ops(), refused, place, true);
        assert!(kept.is_empty(), "{kept:?}");
    }
}
