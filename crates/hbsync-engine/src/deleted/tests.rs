use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use hbsync_platform::name::name_key;
use hbsync_remote::{NodeMeta, Remote};
use hbsync_state::{MirrorRow, ReadTx, RemovedRow, StateDb, StateError, Store};
use hbsync_types::activity::Deleted;
use hbsync_types::{NodeId, NodeKind, RemoteFileId, Uuid, VersionTag};

use super::listing::{RemovedRows, all, deleted_by_item, find, named};
use super::restore::plan_restore;
use super::rules::{RestoreWave, restore_wave, select, take_restored_name, taken_in};
use super::*;

/// The account's DB as the drive holds it, read and written by a test as by the drive.
struct Db(Mutex<StateDb>);

impl Db {
    fn new() -> Result<Db, StateError> {
        Ok(Db(Mutex::new(StateDb::open_in_memory()?)))
    }

    fn read<T>(
        &self,
        f: impl FnOnce(&ReadTx<'_>) -> Result<T, StateError>,
    ) -> Result<T, StateError> {
        locked(&self.0).read(f)
    }

    fn write<T>(
        &self,
        f: impl FnOnce(&Store<'_>) -> Result<T, StateError>,
    ) -> Result<T, StateError> {
        locked(&self.0).write(f)
    }
}

impl std::ops::Deref for Db {
    type Target = Mutex<StateDb>;

    fn deref(&self) -> &Mutex<StateDb> {
        &self.0
    }
}

/// Recently deleted's operations on `db` and `remote`.
fn ops<'a>(db: &'a Db, remote: &'a dyn Remote) -> AccountOps<'a> {
    AccountOps::new(db, remote)
}

fn removed(node: NodeId, parent: NodeId, removed_at: i64) -> RemovedRow {
    RemovedRow {
        row: MirrorRow {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            node,
            parent,
            name: node.0.to_string(),
            kind: NodeKind::Folder,
            file: None,
            content_extra: None,
            token: None,
            changed_at: None,
        },
        removed_at: Some(removed_at),
    }
}

/// Removed folders in a cycle (a remote one left them so) are restored once each, and the
/// plan ends; so does the walk to the item that took them along.
#[test]
fn a_cycle_in_recently_deleted_ends() -> Result<(), StateError> {
    let db = Db::new()?;
    let (a, b) = (NodeId::new(), NodeId::new());
    db.write(|s| {
        s.account().put_removed(&removed(a, b, 1))?;
        s.account().put_removed(&removed(b, a, 1))
    })?;
    let plan = db.read(|s| plan_restore(s, a))?.map(|p| p.rows.len());
    assert_eq!(plan, Some(2));
    // One of them is the item, the same one whichever row a walk starts from: the listing
    // and a lookup of either row agree on it and on its count.
    let item = a.min(b);
    let listed: Vec<(NodeId, NodeId, usize)> = db
        .read(all)?
        .into_iter()
        .map(|(d, n)| (d.node, d.root, n))
        .collect();
    assert_eq!(listed.len(), 2);
    assert!(
        listed.iter().all(|(_, root, _)| *root == item),
        "{listed:?}"
    );
    for node in [a, b] {
        let found = db.read(|s| find(s, &[node]))?;
        let listed = listed
            .iter()
            .find(|(n, _, _)| *n == node)
            .map(|l| (l.1, l.2));
        assert_eq!(Some((found[0].0.root, found[0].1)), listed, "{node:?}");
    }
    Ok(())
}

fn fake() -> hbsync_remote::fake::FakeRemote {
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
    // The drive has none of the files: each delete for good finds it not there, which is done
    // once the identity said so twice ([`twice`]).
    FakeRemote::new(FakeServer::new(ServerConfig::default()))
}

/// Runs `empty` (a delete for good of files the drive doesn't have), and again a minute on:
/// the first "not there" of each is a suspicion only (sync semantics §8b), the second makes it
/// gone. Returns what the second took.
fn twice(
    db: &Db,
    mut empty: impl FnMut() -> Result<usize, EngineError>,
) -> Result<usize, Box<dyn std::error::Error>> {
    assert_eq!(empty()?, 0, "once is a suspicion");
    crate::testkit::a_minute_on(&mut locked(db))?;
    Ok(empty()?)
}

/// Runs `empty` a minute apart until it takes nothing more (a folder whose members go first
/// waits a round for each: each goes once said not there twice). Returns what it took.
fn until_done(
    db: &Db,
    mut empty: impl FnMut() -> Result<usize, EngineError>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut took = 0;
    for _ in 0..8 {
        took += empty()?;
        if crate::testkit::a_minute_on(&mut locked(db))? == 0 {
            return Ok(took);
        }
    }
    Err("never done".into())
}

fn listed(db: &Db) -> Result<Listed, StateError> {
    Ok(db.read(|s| deletes(s, 0))?.1.listed)
}

fn left(db: &Db) -> Result<Vec<NodeId>, StateError> {
    let mut left: Vec<NodeId> = db.read(all)?.into_iter().map(|(d, _)| d.node).collect();
    left.sort();
    Ok(left)
}

fn sorted(mut nodes: Vec<NodeId>) -> Vec<NodeId> {
    nodes.sort();
    nodes
}

/// Emptying as listed deletes for good exactly what the listing saw: an open removal too,
/// what a delete took along by then; not what was recorded after it (a removal that arrives
/// later with an older server time, an item a listed folder's delete took along that came
/// later, which keeps that folder), nor a node restored and deleted again since.
#[test]
fn emptying_takes_what_was_listed_and_nothing_since() -> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    let db = Db::new()?;
    let [f, with, own, lone, late, later_with] = [(); 6].map(|_| NodeId::new());
    db.write(|s| {
        s.account().put_removed(&removed(f, NodeId::ROOT, 10))?;
        s.account().put_removed(&removed(with, f, 11))?;
        s.account().put_removed(&RemovedRow {
            removed_at: None, // this device's own, its server time not back yet
            ..removed(own, NodeId::ROOT, 0)
        })?;
        s.account().put_removed(&removed(lone, NodeId::ROOT, 20))
    })?;
    let (deletes, summary) = db.read(|s| deletes(s, 10))?;
    assert_eq!((deletes.len(), summary.items), (3, 4));

    db.write(|s| {
        // Recorded after the listing, though removed before it on the server.
        s.account().put_removed(&removed(late, NodeId::ROOT, 5))?;
        s.account().put_removed(&removed(later_with, f, 12))?;
        // Restored, then deleted again.
        s.account().delete_removed(lone)?;
        s.account().put_removed(&removed(lone, NodeId::ROOT, 30))?;
        // Its own removal confirmed: the same removal, still listed.
        match s.removed_by_node(own)? {
            Some(r) => s.account().confirm_removed(r.row.remote_ref(), 25),
            None => Err(StateError::Conflict("own isn't listed".into())),
        }
    })?;
    assert_eq!(
        twice(&db, || ops(&db, &remote).empty_listed(&summary.listed))?,
        2
    );
    assert_eq!(left(&db)?, sorted(vec![f, late, later_with, lone]));
    Ok(())
}

/// Emptying deletes for good only removed rows, never a live node: a folder holding one
/// (its delete took it along, it was written since, or its time isn't known) stays, and so
/// do the folders it is in, emptied of the rest. Emptying as listed, emptying the deletes
/// and the purge agree, and the listing counts what they delete.
#[test]
fn a_live_node_is_never_deleted_for_good() -> Result<(), Box<dyn std::error::Error>> {
    let [outer, f, gone, other, g, h] = [(); 6].map(|_| NodeId::new());
    let [taken, taken_deep, later, unknown, h_child] = [(); 5].map(|_| NodeId::new());
    let live = |node, parent, changed_at| MirrorRow {
        changed_at,
        ..removed(node, parent, 0).row
    };
    for how in ["listed", "deletes", "purge"] {
        let remote = fake();
        let db = Db::new()?;
        db.write(|s| {
            for (node, parent) in [
                (outer, NodeId::ROOT),
                (f, outer),
                (gone, f),
                (other, NodeId::ROOT),
                (g, NodeId::ROOT),
                (h, NodeId::ROOT),
            ] {
                s.account().put_removed(&removed(node, parent, 10))?;
            }
            s.account().put_mirror(&live(taken, f, Some(5)))?;
            s.account().put_mirror(&live(taken_deep, taken, Some(6)))?;
            s.account().put_mirror(&live(later, f, Some(20)))?;
            s.account().put_mirror(&live(unknown, g, None))?;
            s.account().put_mirror(&live(h_child, h, Some(10)))
        })?;
        let along: HashMap<NodeId, usize> = db
            .read(all)?
            .into_iter()
            .map(|(d, n)| (d.node, n))
            .collect();
        // Each delete counts its members (its restore restores them), whatever emptying
        // can take now; a pick counts the same.
        assert_eq!((along[&outer], along[&g], along[&h]), (2, 0, 0));
        let found = db.read(|s| find(s, &[outer]))?;
        assert_eq!(found[0].1, 2, "a pick counts the same");
        let went = match how {
            "listed" => {
                // The count it shows is what emptying takes.
                let (_, summary) = db.read(|s| deletes(s, 0))?;
                assert_eq!((summary.deletes, summary.items), (4, 2));
                twice(&db, || ops(&db, &remote).empty_listed(&summary.listed))?
            }
            "deletes" => {
                let listing = listed(&db)?;
                twice(&db, || {
                    ops(&db, &remote).empty(&[outer, other, g, h], &listing)
                })?
            }
            _ => twice(&db, || ops(&db, &remote).expire(11, None))?,
        };
        assert_eq!(went, 2, "{how}");
        assert_eq!(left(&db)?, sorted(vec![outer, f, g, h]), "{how}");
        for n in [taken, taken_deep, later, unknown, h_child] {
            assert!(db.read(|s| s.get_mirror(n))?.is_some(), "{how}: {n:?}");
        }
    }
    Ok(())
}

/// A folder whose contents didn't all go (one changed meanwhile, its delete refused) stays
/// with them: each folder is looked at again just before its delete.
#[test]
fn a_folder_stays_when_a_delete_in_it_fails() -> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    let db = Db::new()?;
    let (f, child) = (NodeId::new(), NodeId::new());
    let mut row = removed(child, f, 10);
    // On the drive at a version other than the one recorded: its delete is refused.
    let name = row.row.name.clone();
    let target = remote.create(
        &NodeMeta {
            node: child,
            parent: f,
            kind: NodeKind::Folder,
            content: row.row.content(&name),
            removed: true,
        },
        None,
    )?;
    row.row.file_id = target.at().file_id;
    db.write(|s| {
        s.account().put_removed(&removed(f, NodeId::ROOT, 10))?;
        s.account().put_removed(&row)
    })?;
    let listing = listed(&db)?;
    assert_eq!(ops(&db, &remote).empty_listed(&listing)?, 0);
    assert_eq!(left(&db)?, sorted(vec![f, child]));
    Ok(())
}

/// A listing of another DB of the same identity (made again since), or one this DB never
/// gave, empties nothing; one that saw nothing empties nothing.
#[test]
fn emptying_an_unknown_listing_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    let db = Db::new()?;
    let x = NodeId::new();
    db.write(|s| s.account().put_removed(&removed(x, NodeId::ROOT, 1)))?;
    let listing = listed(&db)?;
    let ahead = Listed {
        seq: listing.seq + 1,
        ..listing.clone()
    };
    assert!(ops(&db, &remote).empty_listed(&ahead).is_err());
    // Another DB, its counter as far.
    let again = Db::new()?;
    let elsewhere = listed(&again)?;
    assert_ne!(elsewhere.db, listing.db);
    let other = Listed {
        db: elsewhere.db,
        ..listing.clone()
    };
    assert!(ops(&db, &remote).empty_listed(&other).is_err());
    let nothing = Listed {
        seq: 0,
        ..listing.clone()
    };
    assert_eq!(ops(&db, &remote).empty_listed(&nothing)?, 0);
    assert_eq!(left(&db)?, [x], "nothing went");
    assert_eq!(twice(&db, || ops(&db, &remote).empty_listed(&listing))?, 1);
    Ok(())
}

/// Removed folders in a cycle are listed, so emptying as listed deletes them for good, and
/// so does the purge once they expire.
#[test]
fn a_cycle_in_recently_deleted_is_emptied() -> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    for purge in [false, true] {
        let db = Db::new()?;
        let (a, b) = (NodeId::new(), NodeId::new());
        db.write(|s| {
            s.account().put_removed(&removed(a, b, 1))?;
            s.account().put_removed(&removed(b, a, 1))
        })?;
        let gone = match purge {
            false => {
                let listing = listed(&db)?;
                until_done(&db, || ops(&db, &remote).empty_listed(&listing))?
            }
            true => until_done(&db, || ops(&db, &remote).expire(2, None))?,
        };
        assert_eq!(gone, 2, "purge: {purge}");
        assert_eq!(left(&db)?, [], "purge: {purge}");
    }
    Ok(())
}

/// A chain of removed folders deeper than any path may be isn't a cycle: its item is the
/// last row walked, as at the top, remembered for the chain; that row is its own item, so the
/// listing lists it as a delete.
#[test]
fn a_chain_too_deep_ends_at_the_last_row_walked() -> Result<(), StateError> {
    let db = Db::new()?;
    let chain: Vec<NodeId> = (0..hbsync_state::MAX_DEPTH + 2)
        .map(|_| NodeId::new())
        .collect();
    db.write(|s| {
        for w in chain.windows(2) {
            s.account().put_removed(&removed(w[0], w[1], 1))?;
        }
        Ok(())
    })?;
    let deep = hbsync_state::MAX_DEPTH;
    let items = db.read(|s| {
        let mut rows = RemovedRows::new(s, Vec::new());
        let mut known = HashMap::new();
        let mut items = Vec::new();
        for n in [chain[0], chain[1], chain[deep - 1]] {
            items.push(deleted_by_item(&mut rows, n, &mut known)?);
        }
        Ok(items)
    })?;
    assert_eq!(items, [chain[deep - 1]; 3]);
    Ok(())
}

/// Emptying orders and keeps by the links among what it empties alone, however deep: each
/// folder after the folders in it, and a folder holding something that stays keeps every
/// folder it is in.
#[test]
fn emptying_a_chain_deeper_than_any_path() {
    let chain: Vec<NodeId> = (0..hbsync_state::MAX_DEPTH + 10)
        .map(|_| NodeId::new())
        .collect();
    let folder = |w: &[NodeId]| Chosen {
        node: w[0],
        parent: w.get(1).copied().unwrap_or(NodeId::ROOT),
        folder: true,
    };
    let mut chosen: Vec<Chosen> = (0..chain.len()).map(|i| folder(&chain[i..])).collect();
    let file = NodeId::new();
    chosen.push(Chosen {
        node: file,
        parent: chain[0],
        folder: false,
    });
    let mut holdings = Holdings::default();
    for c in &chosen {
        *holdings.removed.entry(c.parent).or_default() += 1;
    }
    let mut expected = vec![file];
    expected.extend(&chain);
    assert_eq!(select(&chosen, &holdings), expected);
    holdings.live.insert(chain[0]);
    assert_eq!(select(&chosen, &holdings), [file]);
}

/// Items picked by name are those rows alone, described and counted as the listing does
/// them: a delete with its members, an item taken along with none.
#[test]
fn items_are_found_by_name() -> Result<(), StateError> {
    let mut db = StateDb::open_in_memory()?;
    let [run, data, file, other, other_data] = [(); 5].map(|_| NodeId::new());
    let named = |node, parent, name: &str, at| RemovedRow {
        row: MirrorRow {
            name: name.into(),
            ..removed(node, parent, at).row
        },
        ..removed(node, parent, at)
    };
    db.write(|s| {
        s.account()
            .put_removed(&named(run, NodeId::ROOT, "run", 10))?;
        s.account().put_removed(&named(data, run, "data", 10))?;
        s.account().put_removed(&named(file, data, "f.bin", 10))?;
        s.account()
            .put_removed(&named(other, NodeId::ROOT, "other", 20))?;
        s.account()
            .put_removed(&named(other_data, other, "data", 5))
    })?;
    let mut found: Vec<(NodeId, NodeId, usize)> = db
        .read(named_items)?
        .into_iter()
        .map(|(d, n)| (d.node, d.root, n))
        .collect();
    found.sort();
    let listed: HashMap<NodeId, (NodeId, usize)> = db
        .read(all)?
        .into_iter()
        .map(|(d, n)| (d.node, (d.root, n)))
        .collect();
    let mut expected = vec![
        (data, listed[&data].0, listed[&data].1),
        (other_data, listed[&other_data].0, listed[&other_data].1),
    ];
    expected.sort();
    assert_eq!(found, expected);
    assert_eq!(listed[&data], (run, 1), "taken along, holding f.bin");
    assert_eq!(
        listed[&other_data],
        (other_data, 0),
        "deleted on its own, before"
    );
    Ok(())
}

fn named_items(s: &ReadTx<'_>) -> Result<Vec<(Deleted, usize)>, StateError> {
    named(s, &["data".to_owned()])
}

/// A folder's restore brings back what was removed with it or after it, not what was removed
/// from it before; each such item is one of its own in Recently deleted.
#[test]
fn what_went_before_its_folder_stays() -> Result<(), StateError> {
    let db = Db::new()?;
    let (f, early, with, deeper) = (NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new());
    db.write(|s| {
        s.account().put_removed(&removed(early, f, 5))?;
        s.account().put_removed(&removed(f, NodeId::ROOT, 10))?;
        s.account().put_removed(&removed(with, f, 11))?;
        s.account().put_removed(&removed(deeper, with, 12))
    })?;
    let plan: Option<Vec<NodeId>> = db
        .read(|s| plan_restore(s, f))?
        .map(|p| p.rows.iter().map(|r| r.row.node).collect());
    assert_eq!(plan, Some(vec![f, with, deeper]));
    let tops: HashMap<NodeId, NodeId> = db
        .read(all)?
        .into_iter()
        .map(|(d, _)| (d.node, d.root))
        .collect();
    assert_eq!(tops[&early], early, "deleted on its own");
    assert_eq!(
        (tops[&with], tops[&deeper]),
        (f, f),
        "deleted with the folder"
    );
    Ok(())
}

/// The purge removes what a delete that stopped half way left live, a folder before what is
/// in it, at the versions this device knows: one written since (its version here an old
/// one) stays, and so does what is in it; so does one written after the removal, one whose
/// time isn't known, what a folder restored on the drive since holds, and what the delete
/// of an item not kept long enough left.
#[test]
fn the_purge_removes_only_what_a_delete_took_along() -> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    let db = Db::new()?;
    let meta = |node: NodeId, parent: NodeId| NodeMeta {
        node,
        parent,
        kind: NodeKind::Folder,
        content: hbsync_types::schema::NodeContent::folder(node.0.to_string()),
        removed: false,
    };
    let [f, a, b, stale, in_stale, late, unknown] = [(); 7].map(|_| NodeId::new());
    let [back, in_back, young, in_young] = [(); 4].map(|_| NodeId::new());
    let mut rows: HashMap<NodeId, MirrorRow> = HashMap::new();
    let row = |node, parent, w: hbsync_types::Written| {
        let m = meta(node, parent);
        MirrorRow::written(w, node, parent, m.kind, &m.content, None)
    };
    for (node, parent) in [
        (f, NodeId::ROOT),
        (a, f),
        (b, a),
        (stale, f),
        (in_stale, stale),
        (late, f),
        (unknown, f),
        (back, NodeId::ROOT),
        (in_back, back),
        (young, NodeId::ROOT),
        (in_young, young),
    ] {
        rows.insert(
            node,
            row(node, parent, remote.create(&meta(node, parent), None)?),
        );
    }
    let set_removed = |row: &MirrorRow,
                       removed: bool|
     -> Result<hbsync_types::Written, Box<dyn std::error::Error>> {
        let answers = remote
            .set_removed(&[row.target()], removed)?
            .into_answers()?;
        Ok(answers.into_iter().next().ok_or("no answer")??)
    };
    // The deletes: the folders alone (each stopped there); young's not kept long enough.
    for node in [f, back] {
        rows.insert(
            node,
            row(node, NodeId::ROOT, set_removed(&rows[&node], true)?),
        );
    }
    remote.server().advance(1000);
    let before = rows[&f].changed_at.ok_or("no time")? + 500;
    rows.insert(
        young,
        row(young, NodeId::ROOT, set_removed(&rows[&young], true)?),
    );
    // Written since, on another device: this one has the old version.
    remote.update(&rows[&stale].target(), &meta(stale, f), None)?;
    // Written after the removal, as this device knows.
    let w = remote.update(&rows[&late].target(), &meta(late, f), None)?;
    rows.insert(late, row(late, f, w));
    // Restored on the drive since: this device still has it removed.
    set_removed(&rows[&back], false)?;
    db.write(|s| {
        for (node, r) in &rows {
            match *node {
                n if [f, back, young].contains(&n) => s.account().put_removed(&RemovedRow {
                    removed_at: r.changed_at,
                    row: r.clone(),
                })?,
                n if n == unknown => s.account().put_mirror(&MirrorRow {
                    changed_at: None,
                    ..r.clone()
                })?,
                _ => s.account().put_mirror(r)?,
            }
        }
        Ok(())
    })?;
    let mut purge = ops(&db, &remote);
    assert_eq!(purge.remove_taken_along(before)?, 2);
    assert!(purge.behind, "the drive had what this device didn't");
    for n in [a, b] {
        assert!(db.read(|s| s.removed_by_node(n))?.is_some(), "{n:?}");
        assert!(db.read(|s| s.get_mirror(n))?.is_none(), "{n:?}");
    }
    let removed_on_drive: HashSet<NodeId> =
        remote.server().removed().filter_map(|h| h.node()).collect();
    assert_eq!(removed_on_drive, HashSet::from([f, a, b, young]));
    for n in [stale, in_stale, late, unknown, in_back, in_young] {
        assert!(db.read(|s| s.get_mirror(n))?.is_some(), "{n:?}");
    }
    // Its folder kept that long, its own removal not: it waits for its own 30 days.
    assert_eq!(ops(&db, &remote).expire(before, None)?, 0);
    Ok(())
}

/// A folder goes for good only with everything removed below it: a removed node on the
/// drive this device doesn't know (removed since its last read, or one it doesn't read)
/// keeps its folder, which stays in Recently deleted. The rest goes, a cycle of removed
/// folders too (each holds the other, still to go). Emptying as listed, emptying the
/// deletes and the purge agree.
#[test]
fn a_folder_holding_a_removal_this_device_doesnt_know_stays()
-> Result<(), Box<dyn std::error::Error>> {
    for how in ["listed", "deletes", "purge"] {
        let remote = fake();
        let db = Db::new()?;
        let [f, unknown, other, in_other, a, b] = [(); 6].map(|_| NodeId::new());
        for (node, parent) in [
            (f, NodeId::ROOT),
            (unknown, f),
            (other, NodeId::ROOT),
            (in_other, other),
            (a, b),
            (b, a),
        ] {
            let name = node.0.to_string();
            let meta = NodeMeta {
                node,
                parent,
                kind: NodeKind::Folder,
                content: hbsync_types::schema::NodeContent::folder(name),
                removed: true,
            };
            let w = remote.create(&meta, None)?;
            if node != unknown {
                let row = MirrorRow::written(w, node, parent, meta.kind, &meta.content, None);
                db.write(|s| {
                    s.account().put_removed(&RemovedRow {
                        row,
                        removed_at: Some(10),
                    })
                })?;
            }
        }
        let went = match how {
            "listed" => {
                let listing = listed(&db)?;
                ops(&db, &remote).empty_listed(&listing)?
            }
            "deletes" => ops(&db, &remote).empty(&[f, other, a], &listed(&db)?)?,
            _ => ops(&db, &remote).expire(11, None)?,
        };
        assert_eq!(went, 4, "{how}");
        assert_eq!(left(&db)?, [f], "{how}");
        let on_drive: HashSet<NodeId> =
            remote.server().removed().filter_map(|h| h.node()).collect();
        assert_eq!(on_drive, HashSet::from([f, unknown]), "{how}");
    }
    Ok(())
}

/// A live folder the purge removes becomes the nearest removed folder of what is in it: a
/// node in it this device didn't know of (written since its last read, or one it doesn't
/// read) would count as taken along by it. The drive is asked after, and the folder put
/// back, with the ones above it the purge removed; what went with them stays removed.
#[test]
fn a_folder_holding_what_this_device_didnt_know_is_put_back()
-> Result<(), Box<dyn std::error::Error>> {
    let remote = fake();
    let db = Db::new()?;
    let folder = |node: NodeId, parent: NodeId| NodeMeta {
        node,
        parent,
        kind: NodeKind::Folder,
        content: hbsync_types::schema::NodeContent::folder(node.0.to_string()),
        removed: false,
    };
    let [f, outer, a, z, y, b] = [(); 6].map(|_| NodeId::new());
    let mut rows: HashMap<NodeId, MirrorRow> = HashMap::new();
    for (node, parent) in [
        (f, NodeId::ROOT),
        (outer, f),
        (a, outer),
        (z, a),
        (y, outer),
        (b, f),
    ] {
        let m = folder(node, parent);
        let w = remote.create(&m, None)?;
        rows.insert(
            node,
            MirrorRow::written(w, node, parent, m.kind, &m.content, None),
        );
    }
    let w = remote
        .set_removed(&[rows[&f].target()], true)?
        .into_answers()?;
    let w = w.into_iter().next().ok_or("no answer")??;
    let removal = w.changed_at.ok_or("no time")?;
    rows.insert(
        f,
        MirrorRow {
            version: w.at().version,
            changed_at: w.changed_at,
            ..rows[&f].clone()
        },
    );
    // Written since this device's last read: z in a, and in b a node it doesn't read.
    remote.update(&rows[&z].target(), &folder(z, a), None)?;
    let (_, unread_at) =
        remote
            .server()
            .seed_file(b, "unread.bin", b"u", hbsync_fileio::HashAlgorithm::Blake3)?;
    remote.server().unlist(unread_at.file_id);
    db.write(|s| {
        for (node, r) in &rows {
            if *node == f {
                s.account().put_removed(&RemovedRow {
                    removed_at: Some(removal),
                    row: r.clone(),
                })?;
            } else {
                s.account().put_mirror(r)?;
            }
        }
        Ok(())
    })?;
    remote.server().advance(1000);
    let mut purge = ops(&db, &remote);
    // a, outer above it, and b came back; y stays removed.
    assert_eq!(purge.remove_taken_along(removal + 500)?, 1);
    assert!(purge.behind);
    let live: HashSet<NodeId> = remote.server().live().filter_map(|h| h.node()).collect();
    assert_eq!(live.len(), 5, "{live:?}");
    assert!(
        [outer, a, z, b].iter().all(|n| live.contains(n)),
        "{live:?}"
    );
    assert!(db.read(|s| s.removed_by_node(y))?.is_some());
    for n in [outer, a, b] {
        assert!(
            db.read(|s| s.get_mirror(n))?.is_some(),
            "{n:?} back here too"
        );
        assert!(db.read(|s| s.removed_by_node(n))?.is_none(), "{n:?}");
    }
    Ok(())
}

/// A restore's waves: a row waits for its folder while that is still to come back; one whose
/// folder went for good comes back as an orphan under its own name; one whose name is taken in
/// its folder comes back under another, and the names asked for are taken.
#[test]
fn a_restore_comes_back_folder_first_and_by_free_names() {
    let row = |node, parent, name: &str| MirrorRow {
        name: name.to_owned(),
        ..removed(node, parent, 1).row
    };
    let [f, gone] = [(); 2].map(|_| NodeId::new());
    let folder = row(f, NodeId::ROOT, "f");
    let inside = row(NodeId::new(), f, "in f");
    let orphan = row(NodeId::new(), gone, "orphan");
    // Folders gone for good take no names: two orphans of one name both keep it.
    let twin = row(NodeId::new(), gone, "orphan");
    let clash = row(NodeId::new(), NodeId::ROOT, "Taken");
    let live = HashSet::from([NodeId::ROOT]);
    let in_plan = HashSet::from([f, inside.node, orphan.node, twin.node, clash.node]);
    let mut taken = HashMap::from([(NodeId::ROOT, HashSet::from([name_key("taken")]))]);
    let left = vec![&inside, &folder, &orphan, &twin, &clash];

    let wave = restore_wave(left, |r| (r.parent, &r.name), &live, &in_plan, &mut taken);
    assert_eq!(
        wave,
        Some(RestoreWave {
            asked: vec![&folder, &orphan, &twin],
            rest: vec![&clash],
            later: vec![&inside],
        })
    );
    assert!(taken[&NodeId::ROOT].contains(&name_key("f")));
    assert!(!taken.contains_key(&gone));
    // Nothing can come back while its folder isn't.
    assert_eq!(
        restore_wave(
            vec![&inside],
            |r| (r.parent, &r.name),
            &live,
            &in_plan,
            &mut taken
        ),
        None
    );
}

/// At the top of a sync root (the drive's, a role folder's) the state folder's name is taken in
/// any case: each computer's own `.hbsync` is there. Elsewhere it is a name as any.
#[test]
fn the_state_folder_name_is_taken_at_a_sync_roots_top() {
    let docs = hbsync_types::Role::Documents.node();
    let folder = NodeId::new();
    for top in [NodeId::ROOT, docs] {
        let mut taken = HashMap::from([(top, taken_in(top, ["a"]))]);
        assert_eq!(
            take_restored_name(&mut taken, top, ".HBSYNC"),
            Some(".HBSYNC (restored)".to_owned())
        );
    }
    let mut taken = HashMap::from([(folder, taken_in(folder, ["a"]))]);
    assert_eq!(
        take_restored_name(&mut taken, folder, ".hbsync"),
        Some(".hbsync".to_owned())
    );
    // In a role folder gone for good, which may be made again under its id: still taken, and
    // asked for under its own name, it comes back under another.
    let row = MirrorRow {
        name: ".Hbsync".to_owned(),
        ..removed(NodeId::new(), docs, 1).row
    };
    let (live, in_plan) = (HashSet::from([NodeId::ROOT]), HashSet::from([row.node]));
    let mut taken = HashMap::new();
    let wave = restore_wave(
        vec![&row],
        |r| (r.parent, &r.name),
        &live,
        &in_plan,
        &mut taken,
    );
    assert_eq!(
        wave,
        Some(RestoreWave {
            asked: vec![],
            rest: vec![&row],
            later: vec![],
        })
    );
    assert_eq!(
        take_restored_name(&mut taken, docs, ".Hbsync"),
        Some(".Hbsync (restored)".to_owned())
    );
}
