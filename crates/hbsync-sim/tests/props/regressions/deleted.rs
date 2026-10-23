//! Regressions: deletes, Recently deleted, restores, emptying and the purge.

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

fn emptying_after_a_removal_since_the_listing(finisher: usize) {
    use Action::*;
    let (a, c_txt) = (0, 3); // NAMES: "a", "c.txt"
    let act = |client, action| Event::Act { client, action };
    let sync = |client| {
        let mut events = vec![Event::Plan { client }];
        events.extend([Event::Step { client }; 12]);
        events
    };
    for item in [false, true] {
        let mut sim = Sim::new(2);
        let root = sim.pick(0, hbsync_types::NodeId::ROOT, true).expect("root");
        sim.run(&[act(
            0,
            CreateFolder {
                parent: root,
                name: a,
            },
        )]);
        let node_a = sim.item(0, "a").expect("a");
        let parent = sim.pick(0, node_a, true).expect("a");
        sim.run(&[act(
            0,
            CreateFile {
                parent,
                name: c_txt,
            },
        )]);
        assert!(sim.quiesce(30), "no convergence");
        let node_c = sim.item(0, "a/c.txt").expect("a/c.txt");
        let node = sim.pick(0, node_a, false).expect("a");
        sim.run(&[act(0, Delete { node })]);
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
        assert!(
            sim.is_removed(node_a) && !sim.is_removed(node_c),
            "the folder went first"
        );
        sim.run(&[act(0, ListDeleted)]);
        sim.run(&sync(finisher));
        assert!(
            sim.is_removed(node_c),
            "client {finisher} marked the file removed"
        );
        sim.run(&[act(
            0,
            if item {
                EmptyItem { pick: 0 }
            } else {
                EmptyListed
            },
        )]);
        assert!(
            sim.is_removed(node_a),
            "item {item}, by {finisher}: `a` holds the file: it stays"
        );
        assert!(
            sim.is_removed(node_c),
            "item {item}, by {finisher}: the file wasn't listed"
        );
        assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            panic!("item {item}, by {finisher}: {e}");
        }
    }
}

#[test]
fn regression_create_under_concurrently_deleted_folder() {
    use Action::*;
    let events = [
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Event::Plan { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 6 },
        },
        Event::Act {
            client: 1,
            action: CreateFolder {
                parent: 14,
                name: 0,
            },
        },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 3 },
        },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFile {
                parent: 13,
                name: 0,
            },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 1 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 2, name: 0 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 1,
            action: Delete { node: 6 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A device confirms deleting a folder another device moved into a name it ignores. The drive
/// keeps the folder, so the confirmation must not be dropped as "nothing to delete any more"
/// before the delete ran: it would stay held for good.
#[test]
fn regression_confirmed_delete_of_a_folder_moved_into_an_ignored_name() {
    use Action::*;
    use Event::*;
    let act = |client, action| Act { client, action };
    let events = [
        act(1, ToggleIgnore),
        act(0, CreateFolder { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 0 }),
        act(0, ToggleIgnore),
        act(0, CreateJunk { parent: 0 }),
        act(0, ToggleIgnore),
        act(0, ToggleIgnore),
        Plan { client: 0 },
        Step { client: 0 },
        act(0, CreateJunk { parent: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
        Plan { client: 1 },
        act(0, CreateFile { parent: 4, name: 0 }),
        Step { client: 1 },
        act(
            1,
            Move {
                node: 0,
                parent: 1,
                name: 13,
            },
        ),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
        panic!("{e}");
    }
}

/// One client edits a file and deletes it; the other edits the version before. The edit
/// wins, but over a version Recently deleted holds that the editor never had: that one stays
/// there, and the edit becomes a new item.
#[test]
fn regression_edit_over_a_version_deleted_elsewhere() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(c1(), CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(c0(), Edit { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(c0(), Delete { node: 0 }),
        act(c1(), Edit { file: 0 }),
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Deleted on one client, restored (onto a name taken meanwhile), then purged: the clients
/// converge at each step.
#[test]
fn restore_and_purge_converge() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let sync = |client| {
        [
            Event::Plan { client },
            Event::Step { client },
            Event::Step { client },
        ]
    };
    let mut events = vec![act(0, CreateFile { parent: 0, name: 0 })];
    events.extend(sync(0));
    events.extend(sync(1));
    events.push(act(1, Delete { node: 0 }));
    events.extend(sync(1));
    events.push(act(0, CreateFile { parent: 0, name: 0 })); // the name, taken again
    events.extend(sync(0));
    events.push(act(0, Restore { node: 0 }));
    events.extend(sync(0));
    events.extend(sync(1));
    events.push(act(1, Delete { node: 0 }));
    events.extend(sync(1));
    // Daily purges, until what was deleted is old enough.
    events.extend([act(1, Purge), act(1, Purge), act(1, Purge)]);
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Emptying Recently deleted as listed while other devices work: after the listing a listed
/// folder comes back, and (or not) the phone saves an edit of a file a listed folder's delete
/// took along (its own removal still on its way). What was listed goes (or, purged, all of
/// Recently deleted), but never a live node: the folder holding the file stays, and comes back
/// with the edit; nothing written is lost, and the clients converge.
#[test]
fn emptying_as_listed_spares_what_came_after_the_listing() {
    use Action::*;
    use hbsync_types::NodeId;
    let (a, b, c_txt) = (0, 1, 3); // NAMES: "a", "b", "c.txt"
    let act = |client, action| Event::Act { client, action };
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = match path {
            "" => NodeId::ROOT,
            _ => sim.item(0, path).expect(path),
        };
        sim.pick(0, node, true).expect(path)
    };
    let create = |sim: &mut Sim, parent: &str, name, file: bool| {
        let parent = folder(sim, parent);
        let action = match file {
            true => CreateFile { parent, name },
            false => CreateFolder { parent, name },
        };
        sim.run(&[act(0, action)]);
    };
    let mut sync0 = vec![Event::Plan { client: 0 }];
    sync0.extend([Event::Step { client: 0 }; 12]);
    for (edited, purge) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut sim = Sim::new(2);
        create(&mut sim, "", a, false);
        create(&mut sim, "", b, false);
        create(&mut sim, "a", c_txt, true);
        create(&mut sim, "b", c_txt, true);
        assert!(sim.quiesce(30), "no convergence");
        let (node_a, node_b) = (sim.item(0, "a").expect("a"), sim.item(0, "b").expect("b"));
        // Client 0 deletes `b`, and `a` as far as the folder (the file in it, still live, was
        // taken along); then lists Recently deleted.
        let node = sim.pick(0, node_b, false).expect("b");
        sim.run(&[act(0, Delete { node })]);
        sim.run(&sync0);
        let node = sim.pick(0, node_a, false).expect("a");
        sim.run(&[act(0, Delete { node })]);
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
        assert!(sim.is_removed(node_a), "the folder went first");
        sim.run(&[act(0, ListDeleted)]);
        // Since the listing: `b` comes back, and the phone may edit the file in `a`. (No device
        // syncs here: its sync would mark the file removed, a removal recorded after the
        // listing, which keeps `a`.)
        let node = sim.removed_pick(node_b).expect("b removed");
        sim.run(&[act(0, Restore { node })]);
        if edited {
            sim.run(&[act(0, EditTaken { pick: 0 })]);
        }
        sim.run(&[act(0, if purge { Purge } else { EmptyListed })]);
        assert!(sim.is_removed(node_a), "`a` holds a live node: it stays");
        assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            panic!("edited {edited}, purge {purge}: {e}");
        }
        let mut expected = vec!["b", "b/c.txt"];
        if edited {
            expected.splice(0..0, ["a", "a/c.txt"]);
        }
        assert_eq!(
            sim.server_paths(),
            expected,
            "edited {edited}, purge {purge}"
        );
    }
}

/// Emptying as listed by a device that doesn't know a removal made since the listing: client 0
/// deletes `a` and stops after the folder (the file in it still live, taken along), lists
/// Recently deleted, and client 1's sync then marks the file removed; client 0 empties (as
/// listed, or the item) knowing only what it listed. `a` holds that removed file, which isn't
/// going: the drive, asked before `a` goes, says so, and `a` stays with it (a folder goes for
/// good only with everything removed below it). The file stays restorable, and nothing is lost.
/// The same when client 0 itself finishes its delete after the listing, and so knows the
/// removal: emptying the item takes what was in it as listed, nothing recorded since
/// (`deleted::AccountOps::empty` with the listing, as `empty_listed`; before, the item took all
/// of it).
#[test]
fn regression_emptying_spares_a_folder_holding_a_removal_it_didnt_know() {
    for finisher in [1, 0] {
        emptying_after_a_removal_since_the_listing(finisher);
    }
}

/// A folder moved into another after that one's removal (by a device that didn't know of the
/// delete yet) is a change of its own: the purge takes nothing in it along, however old, and
/// it keeps the folder it went into.
#[test]
fn regression_a_folder_moved_into_a_removed_one_keeps_what_is_in_it() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let pick = |sim: &Sim, c: usize, path: &str, as_parent: bool| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, as_parent).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    // NAMES: "a", "b", "c.txt" (client 1 ignores "d").
    for name in [0, 1] {
        let parent = pick(&sim, 0, "", true);
        sim.run(&[act(0, CreateFolder { parent, name })]);
    }
    let parent = pick(&sim, 0, "b", true);
    sim.run(&[act(0, CreateFile { parent, name: 3 })]);
    assert!(sim.quiesce(30), "no convergence");
    // Client 1 moves b into a, planned before client 0's delete of a lands.
    let (node, parent) = (pick(&sim, 1, "b", false), pick(&sim, 1, "a", true));
    sim.run(&[
        act(
            1,
            Move {
                node,
                parent,
                name: 1,
            },
        ),
        Event::Plan { client: 1 },
    ]);
    let node = pick(&sim, 0, "a", false);
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
        // Daily purges, until a's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert!(
        sim.server_paths().iter().any(|p| p == "a/b/c.txt"),
        "b and c.txt stay, in a: {:?}",
        sim.server_paths()
    );
}

/// Purges after a folder's delete stopped half way (the folder removed, the file in it not
/// marked yet): the purge, once the folder is kept long enough, removes the file (taken along)
/// and never deletes it for good at once; the delete carried on finds it removed already, and
/// the client that saw it and the one away since before it converge, nothing lost or back.
#[test]
fn regression_a_purge_mid_delete_leaves_what_the_folder_took_along() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    let (d, a) = (5, 0); // NAMES: "d", "a"
    let parent = folder(&sim, "");
    sim.run(&[act(0, CreateFolder { parent, name: d })]);
    let parent = folder(&sim, "d");
    sim.run(&[act(0, CreateFile { parent, name: a })]);
    assert!(sim.quiesce(30), "no convergence");
    let node = sim.pick(0, sim.item(0, "d").expect("d"), false).expect("d");
    // The folder goes first; the purge comes before the file's own removal, and client 1, away
    // meanwhile, syncs before client 0 goes on.
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        // Daily purges, until the folder's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_paths(),
        Vec::<String>::new(),
        "nothing came back"
    );
}

/// Another device deletes folder b and stops right after the folder, never to come back; no
/// device syncs meanwhile. Once b is kept long enough, the purge removes what it took along, a
/// folder before what is in it, to Recently deleted; once their own removal is kept as long,
/// all of it goes for good. The devices converge, nothing lost.
#[test]
fn regression_the_purge_finishes_a_delete_stopped_half_way() {
    use Action::*;
    let (mut sim, [b, a, c, ac]) = stopped_delete_setup();
    let pick = sim.stopped_pick(b).expect("b");
    let act = |action| Event::Act { client: 0, action };
    sim.run(&[act(DeleteStopped { pick, done: 0 })]);
    assert!(sim.is_removed(b));
    assert!(
        ![a, c, ac].iter().any(|n| sim.is_removed(*n)),
        "taken along, live"
    );
    // Daily purges, until b's removal is old enough.
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(
        [b, a, c, ac].iter().all(|n| sim.is_removed(*n)),
        "removed now"
    );
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(!sim.is_removed(b), "gone for good");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), Vec::<String>::new());
}

/// A folder moved into another here while that one was moved into it elsewhere, its old parent
/// deleted here: undoing the local move puts it back into the deleted folder, which then comes
/// back for it (§6.3) instead of being deleted on the drive under a live child, a plan no
/// executor can carry out (found by `main_folders_leave_role_folders_alone`).
#[test]
fn regression_an_undone_move_brings_back_the_folder_deleted_here() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, CreateJunk, Delete, Move, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(0, CreateFile { parent: 0, name: 2 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(2, CreateFolder { parent: 0, name: 2 }),
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(0, ToggleIgnore),
        act(2, CreateFolder { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 6 }),
        act(0, CreateFile { parent: 0, name: 8 }),
        act(
            0,
            CreateFolder {
                parent: 14,
                name: 1,
            },
        ),
        act(2, CreateFolder { parent: 1, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateJunk { parent: 0 }),
        plan(0),
        step(0),
        plan(2),
        act(0, CreateJunk { parent: 0 }),
        step(2),
        step(2),
        act(2, CreateFolder { parent: 2, name: 0 }),
        step(2),
        step(0),
        step(2),
        act(
            2,
            Move {
                node: 13,
                parent: 2,
                name: 1,
            },
        ),
        plan(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        act(0, CreateFile { parent: 1, name: 0 }),
        act(2, Delete { node: 0 }),
        act(0, CreateFile { parent: 1, name: 0 }),
        step(0),
        step(0),
        act(
            0,
            Move {
                node: 3,
                parent: 4,
                name: 6,
            },
        ),
        act(0, CreateFile { parent: 2, name: 0 }),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_folder, &events) {
        panic!("{e}");
    }
}

/// A device's own folder removal has its time from the write's answer (odin-core #1971): the
/// plan made right after it, before any feed read, sees that time, so what is in the folder
/// went with it, and a restore of the folder brings it all back.
#[test]
fn regression_an_own_removal_time_takes_along_what_is_in_the_folder() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let run = |sim: &mut Sim, events: &[Event]| sim.run(events);
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let (d, a, b) = (5, 0, 1); // NAMES: "d", "a", "b"
    let parent = folder(&sim, "");
    run(
        &mut sim,
        &[Event::Act {
            client: 0,
            action: CreateFolder { parent, name: d },
        }],
    );
    let parent = folder(&sim, "d");
    for name in [a, b] {
        run(
            &mut sim,
            &[Event::Act {
                client: 0,
                action: CreateFile { parent, name },
            }],
        );
    }
    assert!(sim.quiesce(30), "no convergence");
    let photos = sim.item(0, "d").expect("d");
    let node = sim.pick(0, photos, false).expect("d");
    // The folder goes first; the next plan comes before any feed read.
    run(
        &mut sim,
        &[
            Event::Act {
                client: 0,
                action: Delete { node },
            },
            Event::Plan { client: 0 },
            Event::Step { client: 0 },
            Event::Plan { client: 0 },
        ],
    );
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), Vec::<String>::new());
    // All of it is deleted with the folder: restoring it brings it all back.
    let node = sim.removed_pick(photos).expect("d removed");
    run(
        &mut sim,
        &[Event::Act {
            client: 1,
            action: Restore { node },
        }],
    );
    assert!(sim.quiesce(30), "no convergence");
    assert_eq!(sim.server_paths(), ["d", "d/a", "d/b"]);
}

/// The Photos example: on Monday `Photos/a.jpg` is deleted on its own; on Tuesday the folder
/// `Photos`, still holding `b.jpg` and a subfolder with a file. Restoring `Photos` brings back
/// `b.jpg` and the subfolder's file, not `a.jpg`: that stays in Recently deleted, and comes back
/// on its own, into `Photos`. (In the simulator's names: `Photos` is `d`, `a.jpg` `a`, `b.jpg`
/// `b`, the subfolder `d/d` and its file `d/d/c.txt`.)
#[test]
fn regression_photos_restored_without_what_was_deleted_from_it_before() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let act = |sim: &mut Sim, client, action| sim.run(&[Event::Act { client, action }]);
    let settle = |sim: &mut Sim| assert!(sim.quiesce(30), "no convergence");
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let (d, a, b, c_txt) = (5, 0, 1, 3); // NAMES: "d", "a", "b", "c.txt"
    let parent = folder(&sim, "");
    act(&mut sim, 0, CreateFolder { parent, name: d });
    let parent = folder(&sim, "d");
    act(&mut sim, 0, CreateFile { parent, name: a });
    act(&mut sim, 0, CreateFile { parent, name: b });
    act(&mut sim, 0, CreateFolder { parent, name: d });
    let parent = folder(&sim, "d/d");
    act(
        &mut sim,
        0,
        CreateFile {
            parent,
            name: c_txt,
        },
    );
    settle(&mut sim);
    let (photos, a_jpg) = (
        sim.item(0, "d").expect("d"),
        sim.item(0, "d/a").expect("d/a"),
    );

    // Monday: a.jpg on its own. Tuesday: the folder.
    let node = sim.pick(0, a_jpg, false).expect("a");
    act(&mut sim, 0, Delete { node });
    settle(&mut sim);
    let node = sim.pick(0, photos, false).expect("d");
    act(&mut sim, 0, Delete { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), Vec::<String>::new());

    // The other device restores the folder: b.jpg and the subfolder come back, a.jpg doesn't.
    let node = sim.removed_pick(photos).expect("d removed");
    act(&mut sim, 1, Restore { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["d", "d/b", "d/d", "d/d/c.txt"]);
    assert!(sim.is_removed(a_jpg), "a.jpg stays in Recently deleted");

    // a.jpg on its own, into Photos.
    let node = sim.removed_pick(a_jpg).expect("a removed");
    act(&mut sim, 1, Restore { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["d", "d/a", "d/b", "d/d", "d/d/c.txt"]);
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// The computer deleting a folder stops syncing a file in it before it removed the file: the
/// folder's removal took the file along (written before it), so it is removed, though no
/// device marks it so.
#[test]
fn regression_a_file_taken_along_and_no_longer_synced_is_removed() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(3, CreateFolder { parent: 0, name: 5 }),
        plan(3),
        act(0, Phone { op: 0, pick: 0 }),
        act(3, CreateFile { parent: 6, name: 0 }),
        step(3),
        plan(3),
        step(3),
        act(3, ToggleIgnore),
        plan(3),
        step(3),
        step(3),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// One client edits a file and deletes the folder it moved it into; the other, which never
/// had that edit, only renamed the file meanwhile. The folder's removal took the edited version
/// along: the rename doesn't write over it (it stays, with the folder, in Recently deleted), the
/// renamed file goes up as a new item.
#[test]
fn regression_a_rename_doesnt_write_over_a_version_taken_along() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateJunk { parent: 0 }),
        act(
            1,
            Move {
                node: 0,
                parent: 0,
                name: 3,
            },
        ),
        plan(1),
        step(1),
        act(1, CreateFolder { parent: 0, name: 0 }),
        act(1, Edit { file: 0 }),
        plan(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        act(
            1,
            Move {
                node: 6,
                parent: 0,
                name: 0,
            },
        ),
        plan(1),
        act(0, CreateJunk { parent: 0 }),
        step(1),
        step(1),
        act(1, Delete { node: 1 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        plan(1),
        step(1),
        act(
            0,
            Move {
                node: 0,
                parent: 0,
                name: 4,
            },
        ),
        step(1),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
        panic!("{e}");
    }
}

/// Scripted races around a folder's delete (node schema, Recently deleted): the server's time
/// decides what went with the folder.
mod folder_delete_races {
    use super::*;
    use Action::*;
    use hbsync_types::NodeId;

    // Not "d": the odd clients ignore it.
    const D: usize = 1; // "b"
    const A: usize = 0; // "a"
    const B: usize = 3; // "c.txt"

    fn act(sim: &mut Sim, client: usize, action: Action) {
        sim.run(&[Event::Act { client, action }]);
    }

    fn plan(sim: &mut Sim, client: usize) {
        sim.run(&[Event::Plan { client }]);
    }

    fn steps(sim: &mut Sim, client: usize, n: usize) {
        sim.run(&vec![Event::Step { client }; n]);
    }

    fn settle(sim: &mut Sim) {
        assert!(sim.quiesce(30), "no convergence");
        sim.check().unwrap_or_else(|e| panic!("{e}"));
    }

    /// What a create's `parent` is to pick folder `path` on client `c`.
    fn folder(sim: &Sim, c: usize, path: &str) -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, true).expect(path)
    }

    /// What a `Delete` or `Edit` picks for `path` on client `c`.
    fn item(sim: &Sim, c: usize, path: &str) -> usize {
        let node = sim.item(c, path).expect(path);
        sim.pick(c, node, false).expect(path)
    }

    /// The index `Edit` picks for file `path` on client `c` (files only, by id).
    fn pick_file(sim: &Sim, c: usize, path: &str) -> usize {
        sim.file_pick(c, sim.item(c, path).expect(path))
            .expect(path)
    }

    /// Folder `b` with `b/a` in it, synced on both clients.
    fn synced_folder() -> Sim {
        let mut sim = Sim::new(2);
        let root = folder(&sim, 0, "");
        act(
            &mut sim,
            0,
            CreateFolder {
                parent: root,
                name: D,
            },
        );
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: A });
        settle(&mut sim);
        sim
    }

    /// One client adds `b/c.txt` and deletes `b` while the other hasn't downloaded `b/c.txt`
    /// yet, and plans in the middle of the delete: `b/c.txt` was written before the folder's
    /// removal, so it went with it, and the folder doesn't come back.
    #[test]
    fn a_file_not_downloaded_yet_doesnt_bring_its_folder_back() {
        let mut sim = synced_folder();
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: B });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 5);
        let photos = sim.item(0, "b").expect("b");
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1); // the folder first
        assert!(sim.is_removed(photos));
        plan(&mut sim, 1);
        steps(&mut sim, 1, 10);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
    }

    /// One client edits `b/a` and uploads it before the other, not knowing, deletes `b`: the
    /// edit came before the delete, so it went with the folder, into Recently deleted with its
    /// content.
    #[test]
    fn an_edit_before_the_delete_goes_with_the_folder() {
        let mut sim = synced_folder();
        let a = sim.item(1, "b/a").expect("b/a");
        let file = pick_file(&sim, 1, "b/a");
        act(&mut sim, 1, Edit { file });
        plan(&mut sim, 1);
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0); // before the edit is on the drive
        steps(&mut sim, 1, 5);
        steps(&mut sim, 0, 5);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
        assert!(sim.is_removed(a), "the edit is in Recently deleted");
    }

    /// One client deletes `b`; the other, not knowing, edits `b/a` after that: the edit came
    /// after the delete, so the file stays, and brings its folder back.
    #[test]
    fn an_edit_after_the_delete_keeps_the_file_and_its_folder() {
        let mut sim = synced_folder();
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1); // the folder first
        let file = pick_file(&sim, 1, "b/a");
        act(&mut sim, 1, Edit { file });
        plan(&mut sim, 1);
        steps(&mut sim, 1, 5);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), ["b", "b/a"]);
    }

    /// A client that never saw `b` plans in the middle of its delete: `b/c.txt`, written before
    /// the removal, went with it. It isn't moved to the top, nor is `b` brought back.
    #[test]
    fn a_folder_never_seen_doesnt_leave_its_files_at_the_top() {
        let mut sim = Sim::new(2);
        let root = folder(&sim, 0, "");
        act(
            &mut sim,
            0,
            CreateFolder {
                parent: root,
                name: D,
            },
        );
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: B });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 5);
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1);
        plan(&mut sim, 1);
        steps(&mut sim, 1, 10);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
    }
}

/// Documents removes a folder, then a file in it; in between the role folder is deleted and
/// the folder restored, and the file's removal decided in Documents still runs. (Found when a
/// restore put the folder at the top; a restore now brings the role folder back first.)
#[test]
fn regression_a_restore_during_a_folder_delete_isnt_a_write_outside() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[
        act(3, CreateFile { parent: 0, name: 0 }),
        act(4, CreateFolder { parent: 0, name: 0 }),
        act(4, CreateFile { parent: 2, name: 0 }),
    ]);
    let file = sim.item(4, "a/a").expect("Documents' a/a");
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 1 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        plan(4),
        act(4, CreateJunk { parent: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(4),
        step(4),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(4, Delete { node: 3 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        plan(4),
        step(4),
        act(0, DeleteRoleFolder),
        act(0, Restore { node: 0 }),
        step(4),
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    // The role folder (named "a" here) came back first, then the folder in it.
    let paths = sim.server_paths();
    assert!(
        paths.iter().any(|p| p == "a/a"),
        "the restored folder: {paths:?}"
    );
    assert!(sim.is_removed(file), "the file is in Recently deleted");
}

/// Client 2 deletes a folder (an ignore rule takes it off the drive) while clients 0 and 1 tag
/// it red, neither synced yet. Client 0 brings it back (a create of the removed node, with its
/// red) and then removes red. Client 1 never saw the delete: its red goes up against the
/// version back, as any add, and red is back by right. The checker took the restore for
/// settling every add the removed version didn't hold, client 1's unsent one too, and read
/// red as client 0's removal coming back (`cc 6b8411dc…`).
#[test]
fn regression_an_add_not_sent_up_outlives_a_restore() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: Move {
                node: 0,
                parent: 13,
                name: 13,
            },
        },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Plan { client: 2 },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 6 },
        },
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Plan { client: 0 },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: ToggleIgnore,
        },
        Step { client: 1 },
        Plan { client: 2 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 4 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 3, tag: 12 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Step { client: 2 },
        Step { client: 2 },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 3, tag: 4 },
        },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Seed 4e17c897: the computer syncing Documents plans "make a/a, then a/a/a" and makes a/a;
/// meanwhile the phone deletes Documents, Recently deleted is emptied (a goes for good, a/a
/// was made in it after), another device's stopped delete removes a/a, and the stale plan
/// makes a/a/a in it. Written after a/a's removal, the file brings a/a back, but a is nowhere:
/// the main folders put a/a, with the file, into Recovered files (it was shown nowhere).
#[test]
fn regression_a_file_made_in_a_folder_gone_for_good_is_recovered() {
    use hbsync_sim::Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(3, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 3 },
        act(3, CreateFolder { parent: 6, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, DeleteStopped { pick: 0, done: 0 }),
        act(3, CreateFile { parent: 4, name: 0 }),
        Event::Step { client: 3 },
        Event::Plan { client: 3 },
        act(0, DeleteRoleFolder),
        act(0, ListDeleted),
        act(0, EmptyListed),
        Event::Step { client: 3 },
        act(0, DeleteStopped { pick: 0, done: 0 }),
        Event::Step { client: 3 },
    ];
    let mut sim = Sim::with_role_device(Sim::new(3));
    sim.run(&events);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    let paths = sim.server_paths();
    assert!(
        // a/a is named a, and so is the file in it.
        paths.contains(&"Recovered files/a/a".to_owned()),
        "{paths:?}"
    );
}

/// c1 makes folder a and syncs it. c0 gets a, untagged, and tags it Work, writing files into
/// it. c1 tags a Work too, syncs, removes Work and syncs that: the drive's a has none. c0, which
/// never saw c1's Work, sends its own add up over that version (an add either side made is in,
/// §11). c1 writes a file into a, and c0 deletes a before it sees that file: the delete loses,
/// and a comes back to c0 with the drive's Work. Right: c0's add stands, as a lost delete
/// settles nothing. The oracle dropped c0's add with its delete, sent up or not, and so took
/// Work for c1's removal coming back (`cc 35f5b92e…`).
#[test]
fn regression_an_add_sent_up_outlives_its_own_delete_that_lost() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Phone, Tag};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateFolder { parent: 0, name: 8 }),
        plan(1),
        step(1),
        act(1, Tag { node: 0, tag: 13 }),
        plan(0),
        step(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, Tag { node: 6, tag: 1 }),
        act(0, CreateFile { parent: 0, name: 1 }),
        plan(1),
        step(1),
        act(1, Tag { node: 0, tag: 5 }),
        plan(1),
        act(0, CreateFile { parent: 0, name: 3 }),
        step(1),
        plan(0),
        act(1, CreateFile { parent: 6, name: 0 }),
        step(0),
        act(0, Phone { op: 3, pick: 0 }),
        step(0),
        plan(1),
        step(0),
        step(0),
        plan(0),
        step(0),
        act(0, Delete { node: 5 }),
        step(1),
    ];
    run_with(3, None, Sim::with_role_folder, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
    // The story: c0's delete of a lost to c1's file, and a came back with c0's Work.
    let mut sim = Sim::with_role_folder(Sim::new(3));
    sim.run(&events);
    assert!(sim.quiesce(30), "no convergence");
    assert!(
        sim.item(0, "a/a (conflict 1)").is_some(),
        "c1's file kept a"
    );
    for c in 0..2 {
        // (c2 is on Windows: it holds no tags.)
        assert_eq!(sim.local_tags(c, "a"), Some(vec!["Work:0".to_owned()]));
    }
}

/// The phone deletes a file, and a plan here deletes it; before the delete runs, the user
/// restores it on the drive. The delete acted on the removal: the file comes back here, and
/// stays on the drive. A base the delete left behind read as this device deleting it: the plan
/// after removed it again, and the purge, 30 days on, for good.
#[test]
fn regression_a_restore_while_the_delete_runs_stands() {
    use Action::*;
    let mut sim = Sim::new(2);
    let act = |client, action| Event::Act { client, action };
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(30), "no convergence");
    let file = sim.item(0, "a").expect("a");
    let node = sim.pick(1, file, false).expect("a");
    sim.run(&[
        act(1, Delete { node }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        // Client 0 plans the delete; the user restores the file before it runs.
        Event::Plan { client: 0 },
    ]);
    assert_eq!(sim.planned(0), ["LocalDelete"]);
    let restore = sim.removed_pick(file).expect("in Recently deleted");
    sim.run(&[act(0, Restore { node: restore }), Event::Step { client: 0 }]);
    assert!(sim.item(0, "a").is_none(), "deleted");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), ["a"], "the restore stands");
    assert!(sim.item(0, "a").is_some(), "back here");
}

/// Client 0 deletes folder b with b/a in it; before its plan runs, client 1 renames b. The
/// folder's removal is stale, so it ends without removing b, and b/a's removal waits for the
/// next plan (nothing is removed before its folder): b/a is still live once the plan ran. The
/// next plans settle the rename against the delete.
#[test]
fn regression_a_skipped_folder_removal_holds_back_what_is_in_it() {
    use Action::*;
    let mut sim = Sim::new(2);
    let act = |client, action| Event::Act { client, action };
    sim.run(&[act(0, CreateFolder { parent: 0, name: 1 })]); // NAMES: "b"
    let b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, b, true).expect("b");
    sim.run(&[act(0, CreateFile { parent, name: 0 })]); // NAMES: "a"
    assert!(sim.quiesce(30), "no convergence");
    let node = sim.pick(0, b, false).expect("b");
    sim.run(&[act(0, Delete { node }), Event::Plan { client: 0 }]);
    assert_eq!(sim.planned(0), ["RemoteDelete", "RemoteDelete"]);
    let node = sim.pick(1, b, false).expect("b");
    let parent = sim.pick(1, NodeId::ROOT, true).expect("the top");
    sim.run(&[
        act(
            1,
            Move {
                node,
                parent,
                name: 3,
            },
        ), // NAMES: "c.txt"
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
    ]);
    assert!(sim.planned(0).is_empty());
    assert_eq!(sim.server_paths(), ["c.txt", "c.txt/a"], "b/a waits for b");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
}

/// A known limit (the user's call: not now): c0 removes Red, its plan sends the removal, and
/// the user adds Red back before the write lands. The base settles on what both sides hold
/// after the write (Red kept), so the next plan reads the drive's lack of Red as a removal
/// there, and takes Red off the item. The oracle names it rather than passing it silently.
#[test]
fn regression_an_add_while_its_own_removal_lands_is_a_known_limit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, Tag { node: 0, tag: 0 }), // Red added
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(0, Tag { node: 0, tag: 0 }), // Red removed
        Event::Plan { client: 0 },
        act(0, Tag { node: 0, tag: 0 }), // Red added again, the removal still to land
        Event::Step { client: 0 },
    ];
    let limits = known_limits_of(3, &events);
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("OwnRemovalInFlight") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}

/// Seed dc9776d2: client 0 makes folder `c.txt`, tagged Red, with a folder in it, and another
/// device deletes `c.txt` as soon as it is up. Client 1 plans to make it back from Recently
/// deleted (for the folder in it), under a conflict name (its own file holds `c.txt`); client 0
/// restores it first and then removes Red. Client 1's folder, made with the Red of the version
/// in Recently deleted, settled against the drive's newer one: no base (the names differ), so
/// its next plan read Red as its own add and sent it back over the removal. The version it is
/// made of gone, the folder now waits for the next plan.
#[test]
fn regression_a_folder_made_from_recently_deleted_after_its_restore_waits() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(
            1,
            CreateFile {
                parent: 15,
                name: 9,
            },
        ),
        act(
            0,
            CreateFolder {
                parent: 15,
                name: 11,
            },
        ),
        act(
            1,
            CreateFile {
                parent: 6,
                name: 13,
            },
        ),
        act(0, Tag { node: 10, tag: 0 }),
        act(1, Delete { node: 13 }),
        act(0, Meanwhile { op: 0 }),
        act(
            0,
            CreateFolder {
                parent: 10,
                name: 11,
            },
        ),
        Event::Plan { client: 0 },
        act(
            1,
            Move {
                node: 7,
                parent: 3,
                name: 11,
            },
        ),
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
        act(0, Tag { node: 4, tag: 0 }),
    ];
    let limits = known_limits_of(2, &events);
    assert!(limits.is_empty(), "{limits:?}");
}

/// Client 1 deletes file `a` (or, `folder`, folder `b` holding `b/c.txt`); client 0 moves it
/// aside to delete it, then `between` runs (a crash there lets `unlinked` items go first) before
/// client 0's next step, and every client settles. Returns the drive's paths once the check
/// passed.
fn delete_elsewhere_with(folder: bool, unlinked: usize, between: &[Event]) -> Vec<String> {
    use Action::*;
    let mut sim = Sim::new(2);
    let act = |client, action| Event::Act { client, action };
    let (a, b, c_txt) = (0, 1, 3); // NAMES: "a", "b", "c.txt"
    let top = if folder {
        sim.run(&[act(0, CreateFolder { parent: 0, name: b })]);
        let node_b = sim.item(0, "b").expect("b");
        let parent = sim.pick(0, node_b, true).expect("b");
        sim.run(&[act(
            0,
            CreateFile {
                parent,
                name: c_txt,
            },
        )]);
        "b"
    } else {
        sim.run(&[act(0, CreateFile { parent: 0, name: a })]);
        "a"
    };
    assert!(sim.quiesce(30), "no convergence");
    let item = sim.item(0, top).expect(top);
    let node = sim.pick(1, item, false).expect(top);
    sim.run(&[
        act(1, Delete { node }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
    ]);
    assert_eq!(sim.planned(0), ["LocalDelete"]);
    sim.run(&[Event::Step { client: 0 }]);
    assert!(sim.item(0, top).is_none(), "aside");
    sim.crash_after_unlinks(unlinked);
    sim.run(between);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    sim.server_paths()
}

/// An app that had `a` open writes to it after client 0 moved it aside to delete it: the check
/// finds it changed and puts it back, and the edit wins: `a` is restored on the drive with its
/// new bytes. Unlinked, the edit was lost.
#[test]
fn an_edit_of_a_file_aside_is_kept() {
    let edit = Event::Act {
        client: 0,
        action: Action::EditAside { pick: 0 },
    };
    let paths = delete_elsewhere_with(false, 0, &[edit, Event::Step { client: 0 }]);
    assert_eq!(paths, ["a"], "the edit restored it");
}

/// An app saves a new file into folder `b` after client 0 moved it aside to delete it: the
/// check finds it there and puts the folder back; the new file keeps the folder alive, restored,
/// and what was synced in it goes.
#[test]
fn a_file_saved_into_a_folder_aside_is_kept() {
    let save = Event::Act {
        client: 0,
        action: Action::CreateInAside { pick: 0, name: 0 },
    };
    let paths = delete_elsewhere_with(true, 0, &[save, Event::Step { client: 0 }]);
    assert_eq!(paths, ["b", "b/a"], "the new file kept its folder");
}

/// Client 0 dies with folder `b` aside: before anything of it is unlinked, after `b/c.txt`
/// is, and after both are (the commit lost: the bases stay). What is left aside goes back at the
/// next plan, and the delete is made again; nothing is lost.
#[test]
fn a_crash_with_a_delete_aside_loses_nothing() {
    for unlinked in 0..3 {
        let paths = delete_elsewhere_with(true, unlinked, &[Event::Crash { client: 0 }]);
        assert!(paths.is_empty(), "{paths:?}");
    }
}

/// Client 1 keeps an ignored file `b/d` (it ignores `d`) in folder `b`, which client 0
/// deletes: client 1 moves only `b/c.txt` aside, the folder staying for what it keeps (sync
/// semantics §10). An app writes to `b/c.txt` there: the check puts it back, the edit wins, and
/// `b` comes back on the drive with it; `b/d` stays on client 1 only.
#[test]
fn an_edit_aside_beside_an_ignored_item_is_kept() {
    use Action::*;
    let mut sim = Sim::new(2);
    let act = |client, action| Event::Act { client, action };
    let (b, c_txt, d) = (1, 3, 5); // NAMES: "b", "c.txt", "d"
    sim.run(&[act(0, CreateFolder { parent: 0, name: b })]);
    let node_b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, node_b, true).expect("b");
    sim.run(&[act(
        0,
        CreateFile {
            parent,
            name: c_txt,
        },
    )]);
    assert!(sim.quiesce(30), "no convergence");
    let parent = sim.pick(1, node_b, true).expect("b");
    sim.run(&[act(1, CreateFile { parent, name: d })]);
    let node = sim.pick(0, node_b, false).expect("b");
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
    ]);
    assert_eq!(sim.planned(1), ["LocalDelete"]);
    sim.run(&[Event::Step { client: 1 }]);
    assert!(sim.item(1, "b/c.txt").is_none(), "aside");
    assert!(sim.item(1, "b/d").is_some(), "kept");
    sim.run(&[act(1, EditAside { pick: 0 }), Event::Step { client: 1 }]);
    assert!(sim.item(1, "b/c.txt").is_some(), "put back");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_paths(),
        ["b", "b/c.txt"],
        "the edit kept its folder"
    );
    assert!(sim.item(1, "b/d").is_some(), "kept");
}

/// Another device removes `b` and stops: what is in it is taken along, live. c1 edited
/// `b/c.txt` meanwhile, which keeps `b`: restored, and restoring a folder restores what was
/// deleted with it (node schema, Recently deleted), so `b/a` and `b/a/c.txt`, unchanged, come
/// back with it, planned with it (every node dirty) or not (in pages, as the engine marks
/// them). Before, planned with it, they were deleted while `b` came back; out of the plan,
/// they stayed.
#[test]
fn regression_a_folder_an_edit_restores_keeps_what_its_delete_took_along() {
    use Action::*;
    for pages in [None, Some(1)] {
        let (mut sim, [b, _, c, _]) = stopped_delete_setup();
        if let Some(size) = pages {
            sim = sim.with_small_pages(size);
        }
        let file = sim.file_pick(1, c).expect("b/c.txt on 1");
        sim.run(&[Event::Act {
            client: 1,
            action: Edit { file },
        }]);
        let pick = sim.stopped_pick(b).expect("b");
        sim.run(&[Event::Act {
            client: 0,
            action: DeleteStopped { pick, done: 0 },
        }]);
        // c1 first, before c0 (unchanged there) finishes the delete: then `b` comes back
        // alone (a restore by a write brings back the folder only).
        // In pages, a page at a time.
        let mut first = Vec::new();
        for _ in 0..6 {
            first.push(Event::Plan { client: 1 });
            first.extend([Event::Step { client: 1 }; 6]);
        }
        sim.run(&first);
        assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
        if let Err(e) = sim.check() {
            panic!("pages {pages:?}: {e}");
        }
        assert_eq!(
            sim.server_paths(),
            ["b", "b/a", "b/a/c.txt", "b/c.txt"],
            "pages: {pages:?}"
        );
    }
}

/// Client 0 moves file `a` to `b` and edits it; client 1's download brings the new version to
/// `b`, its old version aside first, and dies: before the new one is in place (`earlier` 2), or
/// once it is committed (`earlier` 0). An app then writes to the old version aside. Recovery
/// keeps that write (as the node where the new one never came, else as a new item beside it),
/// and nothing is lost; unwritten, the old version simply goes.
#[test]
fn an_old_version_aside_after_a_crash_keeps_a_write() {
    use Action::*;
    for (earlier, write) in [(0, false), (0, true), (2, false), (2, true)] {
        let mut sim = Sim::new(2).with_split_downloads();
        let act = |client, action| Event::Act { client, action };
        sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]); // NAMES: "a"
        assert!(sim.quiesce(30), "no convergence");
        let a = sim.item(0, "a").expect("a");
        let node = sim.pick(0, a, false).expect("a");
        let file = sim.file_pick(0, a).expect("a");
        sim.run(&[
            act(
                0,
                Move {
                    node,
                    parent: 0,
                    name: 1,
                },
            ), // NAMES: "b"
            act(0, Edit { file }),
        ]);
        // Client 0 syncs alone.
        sim.run(&[Event::Plan { client: 0 }]);
        for _ in 0..10 {
            sim.run(&[Event::Step { client: 0 }]);
        }
        sim.run(&[Event::Plan { client: 1 }, Event::Step { client: 1 }]);
        assert_eq!(sim.planned(1), ["LocalDownload"]);
        // Client 0's crashes, with nothing to run, only count: client 1's comes next.
        let mut events = vec![Event::Crash { client: 0 }; earlier];
        events.push(Event::Crash { client: 1 });
        if write {
            events.push(act(1, EditAside { pick: 0 }));
        }
        sim.run(&events);
        assert!(sim.quiesce(30), "no convergence ({earlier}, {write})");
        if let Err(e) = sim.check() {
            panic!("({earlier}, {write}): {e}");
        }
    }
}

/// c0 moves folder `a` into `c.txt` and tags `a/a` while another device removes `a` and stops:
/// `a/a` is taken along, live. The move keeps `a`, and with it what its removal took along
/// (`a/a`, tagged only). In pages of one node, `a`'s subtree is too big for a page, so `a/a` is
/// planned first, without `a`: it waits for `a`'s plan, which keeps it. Before, it went with
/// the removal while `a` came back empty (seed 523cb1ea).
#[test]
fn regression_what_a_removal_took_along_waits_for_its_folder_in_pages() {
    use Action::*;
    let acts = [
        (0, CreateFolder { parent: 0, name: 0 }, false),
        (0, Edit { file: 0 }, true),
        (1, CreateFolder { parent: 1, name: 3 }, false),
        (
            1,
            CreateFolder {
                parent: 15,
                name: 0,
            },
            false,
        ),
        (0, Edit { file: 0 }, true),
        (0, Tag { node: 2, tag: 0 }, false),
        (
            0,
            Move {
                node: 3,
                parent: 1,
                name: 0,
            },
            false,
        ),
        (0, DeleteStopped { pick: 3, done: 2 }, false),
    ];
    for pages in [None, Some(1)] {
        let mut sim = Sim::new(3).without_ignore_rules();
        if let Some(size) = pages {
            sim = sim.with_small_pages(size);
        }
        for (client, action, sync) in acts {
            sim.run(&[Event::Act { client, action }]);
            if sync {
                assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
            }
        }
        assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
        if let Err(e) = sim.check() {
            panic!("pages {pages:?}: {e}");
        }
        assert_eq!(
            sim.server_paths(),
            ["c.txt", "c.txt/a", "c.txt/a/a"],
            "pages: {pages:?}"
        );
        assert_eq!(sim.server_counts(), (3, 0), "pages: {pages:?}");
    }
}

/// c1 deletes a folder while c0 moves a node out of it into a new folder whose name another
/// page's node takes (c1's new folder of the same name). In pages of one node, the moved
/// node waits for that page, which keeps the folder: what else c1 deleted in it waits with
/// it, so the folder goes as one, with it, as planned whole. Before, it went first and
/// Recently deleted listed it apart from its folder (seed 6743e2e6).
#[test]
fn regression_what_a_folder_waiting_for_another_page_deletes_waits_with_it() {
    use Action::*;
    let acts = [
        (
            2,
            CreateFolder {
                parent: 12,
                name: 11,
            },
            false,
        ),
        (0, Phone { op: 6, pick: 4 }, true),
        (
            2,
            CreateFile {
                parent: 2,
                name: 15,
            },
            false,
        ),
        (0, CreateFolder { parent: 7, name: 0 }, false),
        (
            1,
            CreateFile {
                parent: 12,
                name: 7,
            },
            true,
        ),
        (1, DeleteStopped { pick: 7, done: 11 }, false),
        (2, Tag { node: 8, tag: 3 }, true),
        (1, Delete { node: 6 }, false),
        (0, CreateFolder { parent: 5, name: 8 }, false),
        (
            0,
            Move {
                node: 2,
                parent: 7,
                name: 5,
            },
            false,
        ),
        (
            1,
            CreateFolder {
                parent: 7,
                name: 10,
            },
            false,
        ),
    ];
    let end = |pages: Option<usize>| {
        let mut sim = Sim::new(3).without_ignore_rules();
        if let Some(size) = pages {
            sim = sim.with_small_pages(size);
        }
        for (client, action, sync) in acts {
            sim.run(&[Event::Act { client, action }]);
            if sync {
                assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
            }
        }
        assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
        if let Err(e) = sim.check() {
            panic!("pages {pages:?}: {e}");
        }
        (sim.server_paths(), sim.removed_files_apart())
    };
    let whole = end(None);
    assert_eq!(whole.1, 0, "{whole:?}");
    assert_eq!(end(Some(1)), whole, "in pages of one");
}

/// A folder's removal elsewhere held as every synced file, confirmed; its files, held apart
/// after (one of them still being written), held so too; then the confirmation lost (new
/// ignore rules make a new engine). The folder's delete takes those held files along, so it is
/// held with them (§7), though the folder alone is short of every synced file now: the oracle
/// counted it as a removal held by the count.
#[test]
fn a_delete_taking_held_files_along_is_held_with_them() {
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(1, Action::CreateJunk { parent: 0 }),
        act(1, Action::Meanwhile { op: 0 }),
        act(
            1,
            Action::Move {
                node: 3,
                parent: 7,
                name: 1,
            },
        ),
        act(1, Action::CreateFile { parent: 2, name: 6 }),
        Event::Confirm { client: 1 },
        act(1, Action::CreateJunk { parent: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Confirm { client: 1 },
        act(1, Action::Delete { node: 10 }),
        Event::Step { client: 1 },
        act(1, Action::ToggleIgnore),
        Event::Confirm { client: 0 },
    ];
    if let Err(e) = run_with(2, Some(WIPED_MIN), with_a_folder_wiped, &events) {
        panic!("{e}");
    }
}

/// Another device removes a folder on the drive and stops; c1 moves a node into it (which
/// brings it back) out of a folder it deletes. In pages of one node, the moved node waits for
/// that folder's page of its own and keeps the deleted folder meanwhile: what else c1 deleted
/// in it waits with it, so it goes as one, as planned whole. Before, it went first, listed
/// apart from its folder in Recently deleted (seed 8f96db87).
#[test]
fn regression_what_a_folder_waiting_for_a_folders_page_deletes_waits_with_it() {
    use Action::*;
    let acts = [
        (0, CreateFile { parent: 0, name: 0 }, false),
        (0, CreateFolder { parent: 0, name: 8 }, false),
        (0, Edit { file: 0 }, false),
        (0, Purge, false),
        (0, CreateFolder { parent: 0, name: 0 }, false),
        (0, CreateFolder { parent: 0, name: 5 }, false),
        (0, CreateFolder { parent: 1, name: 3 }, false),
        (0, CreateFile { parent: 3, name: 0 }, false),
        (0, CreateFile { parent: 0, name: 1 }, false),
        (0, Purge, true),
        (0, DeleteStopped { pick: 1, done: 0 }, false),
        (
            1,
            Move {
                node: 3,
                parent: 10,
                name: 0,
            },
            false,
        ),
        (1, Delete { node: 1 }, false),
    ];
    let end = |pages: Option<usize>| {
        let mut sim = Sim::new(3).without_ignore_rules();
        if let Some(size) = pages {
            sim = sim.with_small_pages(size);
        }
        for (client, action, sync) in acts {
            sim.run(&[Event::Act { client, action }]);
            if sync {
                assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
            }
        }
        assert!(sim.quiesce(30), "no convergence (pages: {pages:?})");
        if let Err(e) = sim.check() {
            panic!("pages {pages:?}: {e}");
        }
        (sim.server_paths(), sim.removed_files_apart())
    };
    let whole = end(None);
    assert_eq!(whole.1, 0, "{whole:?}");
    assert_eq!(end(Some(1)), whole, "in pages of one");
}

/// c2 makes d/c.txt; the phone deletes d and stops after the folder (c.txt live, taken along),
/// then edits c.txt there: a live child, which keeps d (§6.3). c1 ignores everything named d
/// (§10): what is in d isn't its. It planned c.txt as an orphan (d in Recently deleted, at a
/// place it filters) and moved another device's file into Recovered files on the drive; d
/// stayed deleted for the others. Now it leaves it, and the computers that sync d bring it
/// back with the file.
#[test]
fn regression_a_computer_leaves_what_is_written_into_a_removed_folder_it_ignores() {
    use Action::*;
    let mut sim = Sim::new(3).with_narrow_check();
    let act = |client, action| Event::Act { client, action };
    let (c_txt, d) = (3, 5); // NAMES: "c.txt", "d"
    let root = sim.pick(2, NodeId::ROOT, true).expect("root");
    sim.run(&[act(
        2,
        CreateFolder {
            parent: root,
            name: d,
        },
    )]);
    let folder = sim.item(2, "d").expect("d");
    let parent = sim.pick(2, folder, true).expect("d");
    sim.run(&[act(
        2,
        CreateFile {
            parent,
            name: c_txt,
        },
    )]);
    assert!(sim.quiesce(30), "no convergence");
    let pick = sim.stopped_pick(folder).expect("d on the drive");
    sim.run(&[
        act(0, DeleteStopped { pick, done: 0 }),
        act(0, EditTaken { pick: 0 }),
    ]);
    // c1 plans first, alone.
    sim.run(&[Event::Plan { client: 1 }]);
    for _ in 0..4 {
        sim.run(&[Event::Step { client: 1 }]);
    }
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), ["d", "d/c.txt"]);
}

/// In pages of one with the guard at one file: a file made in a folder another device removed
/// meanwhile is left on the drive, its folder gone (detached), and c0 deletes it there, a file
/// not synced here; another page has a synced file gone here (a conflict's fork). The guard
/// weighs the page's delete in its plan, and the other pages' files gone here as its pending
/// count: neither is over the limit, so it goes on (found by
/// `guarded_clients_in_pages_hold_the_whole_delete`, whose oracle added the two).
#[test]
fn regression_a_paged_delete_of_a_file_not_synced_here_is_weighed_in_its_plan() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFile { parent: 0, name: 5 }),
        act(0, Meanwhile { op: 1 }),
        Event::Plan { client: 0 },
        act(1, Meanwhile { op: 0 }),
        act(1, CreateJunk { parent: 0 }),
        Event::Step { client: 0 },
        act(0, Meanwhile { op: 0 }),
        act(1, CreateFile { parent: 0, name: 5 }),
        act(1, CreateFolder { parent: 0, name: 6 }),
        act(
            1,
            Move {
                node: 0,
                parent: 6,
                name: 0,
            },
        ),
        Event::Plan { client: 1 },
        Event::Plan { client: 1 },
        act(0, Edit { file: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim.with_small_pages(1), &events) {
        panic!("{e}");
    }
}
