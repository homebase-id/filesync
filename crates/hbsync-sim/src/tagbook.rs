//! The oracle's tag bookkeeping ([`TagBook`], in the [`Ledger`]): what users did to tags, in
//! the order they did it, and what became of it, and the rules the drive's tags are checked
//! against in the end ([`TagBook::check`]).
//!
//! [`Ledger`]: crate::oracle::Ledger

use std::collections::{BTreeMap, BTreeSet};

use hbsync_engine::tree::Tree;
use hbsync_types::NodeId;

/// A tag add: the client, the node, the tag's name.
pub(crate) type Add = (usize, NodeId, String);

/// When an add was made, among the users' tag changes, and whether a removal of the tag was
/// under way then that the adding client hadn't seen (another client's not sent up yet, or one
/// on the drive the adding client hadn't pulled): such a removal may win over the add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Made {
    at: u64,
    concurrent: bool,
}

/// What users did to tags, and what became of it. An item that gets a new id on its client
/// takes its tags along.
#[derive(Debug, Default)]
pub(crate) struct TagBook {
    /// The users' tag changes so far: the order adds and removals are compared in.
    seq: u64,
    /// Tags users added (on which client, to which node), with when. A tag added must be on
    /// its node in the end, unless the node was deleted or a removal made after the add (or
    /// under way then) won.
    added: BTreeMap<Add, Made>,
    /// Each client's last removal of each tag of each node: when.
    removed: BTreeMap<Add, u64>,
    /// Tags a client removed from a node its base had them on, and not added again since, with
    /// the server's time then: they stay removed, unless another client added them and the add
    /// wasn't on the drive yet (a merge keeps either side's add; a removal made after the add
    /// was sent up goes up after it, and wins).
    cleared: BTreeMap<Add, i64>,
    /// Tags a client had added to an item it forked off as a conflict copy: the merge may give
    /// them to the original too, as that client's add.
    forked: BTreeSet<Add>,
    /// Tag adds a client's remote write sent up (until the client adds the tag again), with the
    /// server's time of the last such write: they stand once the item goes from it; a restore
    /// keeps an add only if it holds it, or the add is still to go up ([`TagBook::restored`]).
    sent: BTreeMap<Add, i64>,
    /// Tag adds a client's user made to an item they then deleted there before the add was
    /// sent up, while the pending plan may still send it: one that goes up stands with the node,
    /// as any add sent up does ([`TagBook::sent`]).
    unsent: BTreeMap<Add, Made>,
    /// Tags a download wrote on a client's item whose record recovery dropped (the item
    /// changed since: it can't be told from the file there before), that the item still
    /// holds: read against the old base, they are that client's adds, and a removal made
    /// elsewhere meanwhile may come back (sync semantics §8, §11: the known limit).
    unproven: BTreeSet<Add>,
    /// Adds no item of their client's holds any more ([`TagBook::detach`]): they stay with
    /// the node, still that client's (no other client's to its removals), with when they were
    /// sent, if they were.
    detached: BTreeMap<Add, (Made, Option<i64>)>,
    /// Nodes some user deleted (until they are back).
    deleted: BTreeSet<NodeId>,
}

impl TagBook {
    fn next(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// The user of client `c` added tag `name` to `node`; `concurrent`: a removal of it was
    /// under way that the client hadn't seen ([`Made`]).
    pub(crate) fn add(&mut self, c: usize, node: NodeId, name: &str, concurrent: bool) {
        let at = self.next();
        let add = (c, node, name.to_owned());
        self.cleared.remove(&add);
        // Added again: not sent up yet.
        self.sent.remove(&add);
        self.added.insert(add, Made { at, concurrent });
    }

    /// The user of client `c` removed tag `name` from `node`; `synced`: the server's time,
    /// where the client's base had it.
    pub(crate) fn remove(&mut self, c: usize, node: NodeId, name: &str, synced: Option<i64>) {
        let at = self.next();
        self.removed.insert((c, node, name.to_owned()), at);
        if let Some(now) = synced {
            self.cleared.insert((c, node, name.to_owned()), now);
        }
    }

    /// Client `c`'s adds to `node` are client `to`'s now (the item moved to another folder of
    /// the computer, a new id there that its own create sends up).
    pub(crate) fn moved(&mut self, c: usize, to: usize, node: NodeId) {
        for from in mine(&self.added, c, node) {
            let into = (to, from.1, from.2.clone());
            move_key(&mut self.added, &from, into);
        }
    }

    /// `node` left the drive (a delete won): it settles the tag removals made before it.
    pub(crate) fn deleted_now(&mut self, node: NodeId) {
        self.deleted.insert(node);
        self.cleared.retain(|(_, n, _), _| *n != node);
    }

    /// A user deleted `node` (on a device: [`TagBook::left`] says what went with it there).
    pub(crate) fn deleted(&mut self, node: NodeId) {
        self.deleted.insert(node);
    }

    /// Whether a user deleted `node`, and it isn't back since.
    #[cfg(test)]
    pub(crate) fn is_deleted(&self, node: NodeId) -> bool {
        self.deleted.contains(&node)
    }

    /// Client `c`'s item `node` left it (deleted or trashed there): the tag changes made there
    /// went with it, but for an add sent up, which stands with the node ([`TagBook::detach`]:
    /// where a delete loses, the drive's version comes back with it). An item made of the node
    /// here again later is not the one the user tagged, and doesn't take it along.
    pub(crate) fn left(&mut self, c: usize, node: NodeId) {
        self.cleared.retain(|(by, n, _), _| (*by, *n) != (c, node));
        for add in mine(&self.added, c, node) {
            if self.sent.contains_key(&add) {
                self.detach(add);
            } else if let Some(made) = self.added.remove(&add) {
                self.unsent.insert(add, made);
            }
        }
    }

    /// Client `c`'s add `add` no longer belongs to an item of its: it stays with the node (on the
    /// drive, or nowhere), so an item made of the node later doesn't take it along. Its send
    /// time goes along: merged with an add of the client's detached there before, the later
    /// add, sent at the last time, but not sent where either isn't (one not sent excuses every
    /// removal of another client).
    pub(crate) fn detach(&mut self, add: Add) {
        let Some(made) = self.added.remove(&add) else {
            return;
        };
        let sent = self.sent.remove(&add);
        let merged = match self.detached.get(&add) {
            None => (made, sent),
            Some((other, other_sent)) => (
                if made.at >= other.at { made } else { *other },
                sent.zip(*other_sent).map(|(a, b)| a.max(b)),
            ),
        };
        self.detached.insert(add, merged);
    }

    /// `node` is back where a user deleted it (on the drive, or here where the delete lost)
    /// with the tags named `held`: no longer deleted, so what users do to its tags from now on
    /// counts. What they did before is settled by the delete: tag removals (the version back
    /// has what it had), and tags added are there only if it holds them; but for an add
    /// `pending` (a client hasn't sent it up, and its item, synced there, still holds it: that
    /// client never saw the delete, and sends the add up against the version back, as any
    /// add).
    pub(crate) fn restored(
        &mut self,
        node: NodeId,
        held: &BTreeSet<String>,
        pending: impl Fn(&Add) -> bool,
    ) {
        if !self.deleted.remove(&node) {
            return;
        }
        self.cleared.retain(|(_, n, _), _| *n != node);
        self.detached
            .retain(|(_, n, name), _| *n != node || held.contains(name));
        let sent = &self.sent;
        self.added.retain(|add, _| {
            let (_, n, name) = add;
            *n != node || held.contains(name) || (!sent.contains_key(add) && pending(add))
        });
    }

    /// A client put `node` back in place where the drive has it: no longer deleted (a lost
    /// delete settles nothing).
    pub(crate) fn back(&mut self, node: NodeId) {
        self.deleted.remove(&node);
    }

    /// Item `node` of client `c` is `new` from now on: the tags added to it there go along (not
    /// sent under the new id: its own create sends them); removals there count for it after
    /// too.
    pub(crate) fn retrack(&mut self, c: usize, node: NodeId, new: NodeId) {
        for add in mine(&self.added, c, node) {
            move_key(&mut self.added, &add, (c, new, add.2.clone()));
        }
        for add in mine(&self.cleared, c, node) {
            // The item's removal is the new id's now: the old node no longer has it.
            move_key(&mut self.cleared, &add, (c, new, add.2.clone()));
        }
        // Any client's removal on the node counts for the item after (it may have been merged
        // into it before the new id).
        let removed: Vec<(Add, u64)> = self
            .removed
            .iter()
            .filter(|((_, n, _), _)| *n == node)
            .map(|(add, at)| (add.clone(), *at))
            .collect();
        for ((by, _, name), at) in removed {
            self.removed.insert((by, new, name), at);
        }
    }

    /// Client `c` made a plan: it sends what the user does from now on.
    pub(crate) fn planned(&mut self, c: usize) {
        self.unsent.retain(|(at, _, _), _| *at != c);
    }

    /// A remote write of client `c` sent up its item `node` with tag `name`, at the server's
    /// time `now`.
    pub(crate) fn sent(&mut self, c: usize, node: NodeId, name: &str, now: i64) {
        let add = (c, node, name.to_owned());
        if self.added.contains_key(&add) {
            // The last write carrying it: one after a removal sends it again, as this client's
            // add (a node with no base here yet merges both sides').
            self.sent.insert(add, now);
        } else if let Some(made) = self.unsent.remove(&add) {
            // Its item deleted here since, the plan sent it all the same: it stands with the
            // node.
            self.added.insert(add.clone(), made);
            self.sent.insert(add.clone(), now);
            self.detach(add);
        }
    }

    /// Client `c` forked a conflict copy off its item `node`: the tags it added there may go to
    /// the original too.
    pub(crate) fn forked(&mut self, c: usize, node: NodeId) {
        let forked = mine(&self.added, c, node);
        self.forked.extend(forked);
    }

    /// A download wrote tag `name` on client `c`'s item `node`, which its record recovery
    /// dropped ([`TagBook::unproven`]).
    pub(crate) fn unproven(&mut self, c: usize, node: NodeId, name: &str) {
        self.unproven.insert((c, node, name.to_owned()));
    }

    /// The drive's tags (`server`) against what users did: an add stands unless the node was
    /// deleted (and the removed version didn't hold the tag: `held_at_removal`), or a removal
    /// made after it, or under way then, won; a synced tag a client removed stays removed,
    /// unless another client added it and the add wasn't on the drive yet when it was removed
    /// (not sent up then: the removal, sent up after an add on the drive, wins), or holds it
    /// from a download whose record recovery dropped (the known limit). One removed before a
    /// delete was settled by it. Nodes in `refused` are left as they are.
    pub(crate) fn check(
        &self,
        server: &Tree,
        refused: &BTreeSet<NodeId>,
        held_at_removal: impl Fn(NodeId, &str) -> bool,
    ) -> Result<(), String> {
        // A tag change loses to a delete (§11), unless the removed version held the tag: then
        // what brings it back keeps it.
        let adds = self
            .added
            .iter()
            .map(|(add, made)| (add, made, self.sent.get(add)));
        let detached = self
            .detached
            .iter()
            .map(|(add, (made, sent))| (add, made, sent.as_ref()));
        let adds: Vec<_> = adds.chain(detached).collect();
        for ((_, node, name), made, _) in &adds {
            let removed_since = self
                .removed
                .iter()
                .any(|((_, n, t), at)| n == node && t == name && *at > made.at);
            if let Some(s) = server.get(*node)
                && !refused.contains(node)
                && !made.concurrent
                && !removed_since
                && (!self.deleted.contains(node) || held_at_removal(*node, name))
                && s.tags.get(name).is_none()
            {
                return Err(format!("lost tag {name:?} of {node:?}: {:?}", s.tags));
            }
        }
        for ((by, node, name), removed) in &self.cleared {
            let elsewhere = |(c, n, t): &Add| c != by && n == node && t == name;
            let added_elsewhere = adds
                .iter()
                .any(|(add, _, sent)| elsewhere(add) && sent.is_none_or(|sent| sent > removed))
                || self.forked.iter().chain(&self.unproven).any(elsewhere);
            if let Some(s) = server.get(*node)
                && !added_elsewhere
                && !refused.contains(node)
                && s.tags.get(name).is_some()
            {
                return Err(format!(
                    "removed tag {name:?} of {node:?} came back: {:?}",
                    s.tags
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn has_added(&self, add: &Add) -> bool {
        self.added.contains_key(add)
    }

    #[cfg(test)]
    pub(crate) fn has_detached(&self, add: &Add) -> bool {
        self.detached.contains_key(add)
    }

    #[cfg(test)]
    pub(crate) fn has_cleared(&self, add: &Add) -> bool {
        self.cleared.contains_key(add)
    }
}

/// The keys of `map` of client `c`'s item `node`.
fn mine<V>(map: &BTreeMap<Add, V>, c: usize, node: NodeId) -> Vec<Add> {
    map.keys()
        .filter(|(at, n, _)| (*at, *n) == (c, node))
        .cloned()
        .collect()
}

/// Moves `from`'s entry in `map`, if it has one, to `into`.
fn move_key<V>(map: &mut BTreeMap<Add, V>, from: &Add, into: Add) {
    if let Some(v) = map.remove(from) {
        map.insert(into, v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_types::{NodeState, Tag, Tags};

    fn drive(node: NodeId, tags: &[&str]) -> Tree {
        let mut tree = Tree::new();
        let tags = Tags::new(tags.iter().map(|t| Tag::new(*t, 0)));
        tree.insert(
            node,
            NodeState {
                tags,
                ..NodeState::folder(NodeId::ROOT, "a")
            },
        );
        tree
    }

    fn check(book: &TagBook, server: &Tree) -> Result<(), String> {
        book.check(server, &BTreeSet::new(), |_, _| false)
    }

    /// F10: a removal made before an add doesn't excuse losing it; one made after does.
    #[test]
    fn only_a_later_removal_excuses_a_lost_add() {
        let node = NodeId(hbsync_types::Uuid::from_u128(1));
        let mut book = TagBook::default();
        book.add(0, node, "Work", false);
        book.remove(0, node, "Work", Some(1));
        book.add(1, node, "Work", false);
        assert!(check(&book, &drive(node, &[])).is_err(), "c1's add lost");
        assert!(check(&book, &drive(node, &["Work"])).is_ok());
        book.remove(0, node, "Work", None);
        assert!(check(&book, &drive(node, &[])).is_ok(), "removed after");
    }

    /// An add made while a removal was under way it hadn't seen may lose to it.
    #[test]
    fn an_add_racing_a_removal_may_lose() {
        let node = NodeId(hbsync_types::Uuid::from_u128(1));
        let mut book = TagBook::default();
        book.remove(1, node, "Work", Some(1));
        book.add(0, node, "Work", true);
        assert!(check(&book, &drive(node, &[])).is_ok());
    }

    /// A client's own add, detached from its item, is no other client's add to it: unsent, it
    /// doesn't excuse that client's removal coming back.
    #[test]
    fn a_detached_add_is_not_another_clients() {
        let node = NodeId(hbsync_types::Uuid::from_u128(1));
        let mut book = TagBook::default();
        book.add(0, node, "Red", false);
        book.detach((0, node, "Red".into())); // a download refused it: not sent
        book.remove(0, node, "Red", Some(5));
        assert!(
            check(&book, &drive(node, &["Red"])).is_err(),
            "c0's removal came back"
        );
        book.add(1, node, "Red", false);
        book.detach((1, node, "Red".into()));
        assert!(check(&book, &drive(node, &["Red"])).is_ok(), "c1 added it");
    }
}
