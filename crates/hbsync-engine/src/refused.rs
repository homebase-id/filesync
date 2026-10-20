//! Operations the drive refused for good (sync semantics §8a, refused): a request it won't take
//! as it is (a bad request, a content it won't hold, a hash it won't have, a file it can't
//! give), or a download whose bytes didn't match twice at one version. Such an operation isn't
//! sent again while what it would send or fetch is the same: its key ([`key`]) holds all of
//! that, so any change of the node, here or on the drive, is another operation. A refusal
//! lasts [`REFUSED_FOR`] at most, and until Sync now.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use hbsync_fileio::hash_bytes;
use hbsync_state::{ReadTx, Refused, StateError};
use hbsync_types::{FileContent, HashAlgorithm, NodeId};

use crate::reconcile::{Op, Requirement};

/// How long a refusal holds its operation back: then it is sent once more.
pub const REFUSED_FOR: Duration = Duration::from_secs(3600);

/// The key of `op`, which sends the local file `content` (its hash, size, time and executable
/// bit) if it uploads: every input of the operation, so another key is another operation.
pub fn key(op: &Op, content: Option<&FileContent>) -> String {
    let mut text = format!("{op:?}");
    if let Some(c) = content {
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
    let content = row.and_then(|r| {
        Some(FileContent {
            hash: r.hash?,
            size: r.fingerprint.size,
            mtime_ms: r.fingerprint.mtime_ns / 1_000_000,
            executable: r.executable.unwrap_or(false),
        })
    });
    Ok(key(op, content.as_ref()))
}

/// Takes the operations `refused` says the drive refused as they are out of `ops`, and those
/// that wait for one of them, which would only wait again: what goes into a folder whose create
/// is refused, the removals in a folder whose removal is, what goes to a local place the node
/// of a held operation would have left, and the trash or forgetting of a local folder a held
/// operation's node is in, here, in the base or on the drive (`place`, [`Place`]; names
/// compared as the folder does, `case_insensitive`). Returns `(kept, refused)`; the waiting
/// ones are held silently: the refusal they wait for says why.
pub fn hold_back<E>(
    ops: Vec<Op>,
    mut refused: impl FnMut(&Op) -> Result<bool, E>,
    mut place: impl FnMut(NodeId) -> Result<Place, E>,
    case_insensitive: bool,
) -> Result<(Vec<Op>, Vec<Op>), E> {
    let mut held = Held {
        case_insensitive,
        ..Held::default()
    };
    let (mut kept, mut out) = (Vec::new(), Vec::new());
    for op in ops {
        if refused(&op)? {
            held.add(&op, &mut place)?;
            out.push(op);
        } else {
            kept.push(op);
        }
    }
    if out.is_empty() {
        return Ok((kept, out));
    }
    // What waits for a held operation, and what waits for that, in any order.
    loop {
        let (waiting, rest): (Vec<Op>, Vec<Op>) = std::mem::take(&mut kept)
            .into_iter()
            .partition(|op| held.waits(op));
        kept = rest;
        if waiting.is_empty() {
            return Ok((kept, out));
        }
        for op in &waiting {
            held.add(op, &mut place)?;
        }
    }
}

/// Where a node is ([`hold_back`]).
#[derive(Debug, Default)]
pub struct Place {
    /// Its local item's folder and name.
    pub local: Option<(NodeId, String)>,
    /// The folders it is in: here, in the base, on the drive.
    pub folders: Vec<NodeId>,
}

/// What held operations leave undone: the folders whose create, and whose removal, is held,
/// the local places their nodes don't leave, and the local folders their nodes are in.
#[derive(Default)]
struct Held {
    creates: HashSet<NodeId>,
    removals: HashSet<NodeId>,
    places: HashSet<(NodeId, String)>,
    folders: HashSet<NodeId>,
    case_insensitive: bool,
}

impl Held {
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

    /// Whether `op` waits for an operation held back.
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
        // A folder isn't trashed or forgotten here while it holds a node a held operation
        // would have taken out.
        let empties = matches!(op, Op::LocalTrash { node, .. } | Op::Forget { node, .. }
            if self.folders.contains(node));
        waits_for_folder || empties || goes_to.is_some_and(|p| self.places.contains(&p))
    }
}

/// A folder's refusals, by node and operation kind.
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

    /// Takes `op`'s refusal out.
    pub(crate) fn take(&mut self, op: &Op) -> Option<Refused> {
        self.0.remove(&(op.node(), op.kind()))
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

    /// What waits for a refused operation is held with it, silently; the rest goes.
    #[test]
    fn what_waits_for_a_refusal_is_held_with_it() {
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
        let Ok((kept, out)) = hold_back(ops, refused, place, false);
        assert_eq!(out.len(), 1);
        assert_eq!(kept, [create(other, NodeId::ROOT, NodeKind::File)]);
    }

    /// A download into the place a held download's item would have left waits; one into
    /// another place doesn't, a name only alike in case on a folder that tells case apart too.
    #[test]
    fn a_place_a_held_item_keeps_holds_back_what_goes_there() {
        let (held, mover, other) = (NodeId::new(), NodeId::new(), NodeId::new());
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
                download(held, "new", true),
                download(mover, "old", false),
                download(other, "OLD", false),
            ]
        };
        let refused = |op: &Op| Ok::<_, Infallible>(op.node() == held);
        let place = |node: NodeId| {
            Ok(Place {
                local: (node == held).then(|| (NodeId::ROOT, "old".to_owned())),
                folders: Vec::new(),
            })
        };
        let Ok((kept, _)) = hold_back(ops(), refused, place, false);
        assert_eq!(kept, [download(other, "OLD", false)]);
        let Ok((kept, _)) = hold_back(ops(), refused, place, true);
        assert!(kept.is_empty(), "{kept:?}");
    }
}
