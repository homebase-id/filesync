//! The dirty set planned in pages (sync semantics §6.5): what a page leaves to another, and
//! what it waits for (folders held for it with their dirty subtrees, clashes across pages),
//! planned as pages of their own. The engine's rules, shared with the simulator's small-pages
//! mode.

use std::collections::{BTreeSet, HashSet};

use hbsync_local::Filter;
use hbsync_types::{NodeId, NodeKind};

use crate::plan::Input;
use crate::scope::Scope;
use crate::tree::Tree;

/// What of the dirty set outside a page is still to plan ([`dirty_elsewhere`]): the engine's
/// state DB, or the simulator's model.
pub trait Pending {
    type Error;
    /// Which of `nodes` are still to plan: dirty, or a held delete out of the dirty set.
    fn among(&mut self, nodes: &[NodeId]) -> Result<HashSet<NodeId>, Self::Error>;
    /// Whether something below `folder`, on any side, not in `page`, is dirty: what a page of
    /// its own of `folder` plans (a held delete out of the dirty set is planned again only
    /// when a view reaches it, as in one page).
    fn below(&mut self, folder: NodeId, page: &BTreeSet<NodeId>) -> Result<bool, Self::Error>;
}

/// What `input`'s page leaves to another page (sync semantics §6.5): its view's nodes outside
/// the page still to plan, held as they are ([`Input::dirty_elsewhere`]); its topmost folders
/// that may go with something below them still to plan ([`Input::going`]); and the folders
/// above its nodes that may go and another page plans ([`Input::going_above`]). A query a
/// 500 nodes outside the page, and one a topmost such folder.
pub fn dirty_elsewhere<P: Pending>(
    input: &mut Input,
    filter: &Filter,
    scope: &Scope,
    pending: &mut P,
) -> Result<(), P::Error> {
    let outside: Vec<NodeId> = input
        .base
        .ids()
        .chain(input.local.ids())
        .chain(input.remote.ids())
        .filter(|n| !input.dirty.contains(n))
        .collect::<HashSet<NodeId>>()
        .into_iter()
        .collect();
    let elsewhere = pending.among(&outside)?;
    let hidden = crate::mask::hidden(&input.remote, filter, scope);
    // Synced before, in Recently deleted, or taken along by a folder's removal (still live on
    // the drive, its base gone with the trash): a new folder has no delete to undo.
    let taken = crate::reconcile::merge::taken_along(input);
    let goes = |n: NodeId| {
        (input.base.contains(n) || input.removed.contains_key(&n) || taken.contains(&n))
            && may_go(input, n, &hidden)
    };
    let above = |n: NodeId| crate::reconcile::held_from(input, n);
    let in_page: HashSet<NodeId> = input.dirty.iter().copied().filter(|&n| goes(n)).collect();
    let mut going = HashSet::new();
    for &folder in &in_page {
        if !above(folder).iter().any(|a| in_page.contains(a))
            && pending.below(folder, &input.dirty)?
        {
            going.insert(folder);
        }
    }
    // Walked only where another page plans a folder that may go (hardly ever).
    let candidates: HashSet<NodeId> = elsewhere.iter().copied().filter(|&a| goes(a)).collect();
    let mut going_above = HashSet::new();
    if !candidates.is_empty() {
        for &node in &input.dirty {
            going_above.extend(above(node).into_iter().filter(|a| candidates.contains(a)));
        }
    }
    input.dirty_elsewhere = elsewhere;
    input.going = going;
    input.going_above = going_above;
    Ok(())
}

/// Whether dirty folder `node` may go, on a side or from the folder's view (`hidden`: what its
/// filter or scope leaves out): it isn't a folder here and there, in its place on the drive.
/// What is in it may keep it, so the plan reads all of it.
pub(crate) fn may_go(input: &Input, node: NodeId, hidden: &HashSet<NodeId>) -> bool {
    let is_folder = |t: &Tree| t.get(node).is_some_and(|s| s.kind == NodeKind::Folder);
    let folder = [&input.base, &input.remote, &input.local]
        .into_iter()
        .any(is_folder);
    let stays = is_folder(&input.local)
        && is_folder(&input.remote)
        && input.remote.path(node).is_some()
        && !hidden.contains(&node);
    folder && !stays
}

/// What a page waits for to be planned with, as a page of its own: nodes, and folders with
/// their dirty subtrees.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct Want {
    nodes: Vec<NodeId>,
    folders: Vec<NodeId>,
    /// A piece of a folder too big for a page: it doesn't wait for that folder
    /// ([`crate::plan::Options::own_page`]).
    piece: bool,
}

/// A page of its own ([`OwnPages::next`]): its dirty nodes, and whether it is a piece of a
/// folder too big for a page (`Options::own_page`).
pub type OwnPage<M> = (Vec<M>, bool);

/// Where [`OwnPages`] reads its pages: the engine's state DB, or the simulator's model.
pub trait PageStore {
    /// A dirty node as a page holds it (the engine's `Mark`).
    type Mark: Clone + Eq + std::hash::Hash;
    type Error;
    /// What of `nodes` is dirty.
    fn marks_of(&mut self, nodes: &[NodeId]) -> Result<Vec<Self::Mark>, Self::Error>;
    /// Up to `limit` dirty nodes of `folder`'s subtree on any side, it too, in id order
    /// (`usize::MAX`: all).
    fn marks_under(&mut self, folder: NodeId, limit: usize)
    -> Result<Vec<Self::Mark>, Self::Error>;
    /// The folders directly in `folder`, where a subtree too big for a page splits.
    fn folders_in(&mut self, folder: NodeId) -> Result<Vec<NodeId>, Self::Error>;
}

/// What a page waits for to be planned with, as a page of its own (sync semantics §6.5):
/// folders it held for what in them is dirty in another page ([`Plan::waits_for_pages`]),
/// with their dirty subtrees, a folder's own after its folders' where it is too big; and nodes
/// whose names or places clash across pages, together ([`Plan::waits_together`]). A stack. A
/// page of its own that still waits (a folder there whose name another page's node holds, say)
/// is planned again with what it waits for, one larger page. Each once a sweep: what is held
/// again by its own page (a held delete in it, say) waits for the next.
///
/// [`Plan::waits_for_pages`]: crate::plan::Plan::waits_for_pages
/// [`Plan::waits_together`]: crate::plan::Plan::waits_together
#[derive(Debug, Default)]
pub struct OwnPages {
    /// Each with whether it was split already (too big: its folders first).
    queue: Vec<(Want, bool)>,
    tried: HashSet<Want>,
    /// What the page [`OwnPages::next`] gave last was made of.
    current: Option<Want>,
}

impl OwnPages {
    /// What the last plan waits for ([`Plan::waits_for_pages`], [`Plan::waits_together`]), to
    /// try (each once a sweep); with the page it was, if one of its own.
    ///
    /// [`Plan::waits_for_pages`]: crate::plan::Plan::waits_for_pages
    /// [`Plan::waits_together`]: crate::plan::Plan::waits_together
    pub fn add(&mut self, folders: &[NodeId], together: &[Vec<NodeId>]) {
        let wants: Vec<Want> = match self.current.take() {
            Some(mut want) if !folders.is_empty() || !together.is_empty() => {
                want.nodes.extend(together.iter().flatten());
                want.folders.extend(folders);
                for ids in [&mut want.nodes, &mut want.folders] {
                    ids.sort();
                    ids.dedup();
                }
                vec![want]
            }
            Some(_) => Vec::new(),
            None => together
                .iter()
                .map(|nodes| Want {
                    nodes: nodes.clone(),
                    ..Want::default()
                })
                .chain(folders.iter().map(|&folder| Want {
                    folders: vec![folder],
                    ..Want::default()
                }))
                .collect(),
        };
        for want in wants.into_iter().rev() {
            if self.tried.insert(want.clone()) {
                self.queue.push((want, false));
            }
        }
    }

    /// A new sweep: everything may be tried again. What waits stays queued, the last page's
    /// too, across syncs (one that stopped at its round limit goes on with it), tried once
    /// more this sweep.
    pub fn new_sweep(&mut self) {
        self.tried = self.queue.iter().map(|(want, _)| want.clone()).collect();
        self.current = None;
    }

    /// The next page of its own, from `store`: what waits together, as dirty, with the dirty
    /// subtrees of its folders. A folder's of more than `batch` nodes has its folders'
    /// subtrees come first, then it once more; what is left of it then (more than a page
    /// directly in it) is planned whole, one larger page. So every folder goes as one, or as
    /// its folders and then itself: never item by item. Folders split so go together while
    /// they fit a page: a round each, not one a folder. With whether it is such a piece, which
    /// doesn't wait for the folder above it (`Options::own_page`).
    pub fn next<S: PageStore>(
        &mut self,
        batch: usize,
        store: &mut S,
    ) -> Result<Option<OwnPage<S::Mark>>, S::Error> {
        self.current = None;
        while let Some((want, split)) = self.queue.pop() {
            // One folder alone too big, the first time: its folders first.
            if let [folder] = want.folders[..]
                && want.nodes.is_empty()
                && !split
            {
                let marks = store.marks_under(folder, batch + 1)?;
                if marks.is_empty() {
                    continue;
                }
                if marks.len() <= batch {
                    let piece = want.piece;
                    return self
                        .packed(want, marks, batch, store)
                        .map(|page| Some((page, piece)));
                }
                let folders = store.folders_in(folder)?;
                self.queue.push((want, true));
                self.queue.extend(folders.into_iter().map(|folder| {
                    let want = Want {
                        folders: vec![folder],
                        piece: true,
                        ..Want::default()
                    };
                    (want, false)
                }));
                continue;
            }
            // Else all of it, one page however large.
            let mut page = match want.nodes.is_empty() {
                true => Vec::new(),
                false => store.marks_of(&want.nodes)?,
            };
            let mut seen: HashSet<S::Mark> = page.iter().cloned().collect();
            for &folder in &want.folders {
                let under = store.marks_under(folder, usize::MAX)?;
                page.extend(under.into_iter().filter(|m| seen.insert(m.clone())));
            }
            if page.is_empty() {
                continue;
            }
            let piece = want.piece;
            self.current = Some(want);
            return Ok(Some((page, piece)));
        }
        Ok(None)
    }

    /// `want`'s page, `marks`, with the folders queued after it that wait alone, each whole,
    /// while they fit `batch` with it (one that doesn't goes back, tried next).
    fn packed<S: PageStore>(
        &mut self,
        mut want: Want,
        mut marks: Vec<S::Mark>,
        batch: usize,
        store: &mut S,
    ) -> Result<Vec<S::Mark>, S::Error> {
        let mut seen: HashSet<S::Mark> = marks.iter().cloned().collect();
        while let Some((next, false)) = self.queue.last()
            && next.nodes.is_empty()
            && next.piece == want.piece
            && let [folder] = next.folders[..]
        {
            let under = store.marks_under(folder, batch + 1 - marks.len())?;
            if marks.len() + under.len() > batch {
                break;
            }
            self.queue.pop();
            want.folders.push(folder);
            marks.extend(under.into_iter().filter(|m| seen.insert(m.clone())));
        }
        self.current = Some(want);
        Ok(marks)
    }
}
