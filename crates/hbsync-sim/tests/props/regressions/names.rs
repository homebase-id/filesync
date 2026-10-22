//! Regressions: names, moves, conflicts, filters, cycles and orphans.

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

#[test]
fn concurrent_edit_produces_a_conflict_copy() {
    let mut sim = Sim::new(2);
    let create = Action::CreateFile { parent: 0, name: 0 };
    sim.run(&[Event::Act {
        client: 0,
        action: create,
    }]);
    assert!(sim.quiesce(10));
    sim.run(&[
        Event::Act {
            client: 0,
            action: Action::Edit { file: 0 },
        },
        Event::Act {
            client: 1,
            action: Action::Edit { file: 0 },
        },
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(sim.check(), Ok(()));
    assert!(sim.conflicts > 0);
}

#[test]
fn regression_three_clients_case_clash() {
    use Action::*;
    let events = [
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 3 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 1 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 5 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 2,
            action: CreateFolder {
                parent: 0,
                name: 13,
            },
        },
        Event::Plan { client: 2 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 3 },
        },
        Event::Step { client: 2 },
        Event::Plan { client: 0 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 15 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 7 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_user_move_into_clashing_folder() {
    use Action::*;
    let events = [
        Event::Act {
            client: 0,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: Move {
                node: 0,
                parent: 1,
                name: 9,
            },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: CreateFile {
                parent: 0,
                name: 14,
            },
        },
        Event::Act {
            client: 1,
            action: CreateFile {
                parent: 0,
                name: 15,
            },
        },
        Event::Plan { client: 1 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Step { client: 1 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Plan { client: 0 },
        Event::Plan { client: 0 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 2 },
        },
        Event::Plan { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 8, name: 0 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 1,
            action: Move {
                node: 1,
                parent: 9,
                name: 0,
            },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A folder moved remotely into a place one client ignores, while that client holds a new
/// item in it (detached), then more moves around it.
#[test]
fn regression_detached_folder() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        act(0, CreateFile { parent: 5, name: 0 }),
        act(0, CreateJunk { parent: 0 }),
        act(
            0,
            CreateFolder {
                parent: 1,
                name: 13,
            },
        ),
        act(0, CreateFile { parent: 1, name: 0 }),
        act(0, CreateFolder { parent: 3, name: 8 }),
        Event::Step { client: 0 },
        act(0, CreateJunk { parent: 0 }),
        Event::Plan { client: 1 },
        act(
            1,
            CreateFile {
                parent: 0,
                name: 13,
            },
        ),
        Event::Step { client: 1 },
        act(0, CreateFile { parent: 0, name: 5 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        act(1, CreateFolder { parent: 6, name: 0 }),
        Event::Step { client: 0 },
        act(0, CreateFolder { parent: 0, name: 3 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(
            0,
            Move {
                node: 0,
                parent: 11,
                name: 1,
            },
        ),
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Three clients, junk and a client that ignores `d` among concurrent creates, moves and
/// edits.
#[test]
fn regression_three_clients_with_filters() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(0, CreateFile { parent: 6, name: 0 }),
        act(2, CreateFile { parent: 0, name: 6 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(
            2,
            CreateFile {
                parent: 0,
                name: 13,
            },
        ),
        act(0, Delete { node: 0 }),
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Step { client: 1 },
        act(0, Edit { file: 0 }),
        act(1, CreateJunk { parent: 0 }),
        act(1, CreateFile { parent: 2, name: 0 }),
        act(
            0,
            Move {
                node: 0,
                parent: 1,
                name: 13,
            },
        ),
        act(0, CreateFile { parent: 0, name: 1 }),
        Event::Plan { client: 1 },
        act(0, CreateFile { parent: 0, name: 1 }),
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(
            0,
            Move {
                node: 0,
                parent: 1,
                name: 2,
            },
        ),
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_fork_onto_a_name_that_stays_taken() {
    use Action::*;
    use Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 6 },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 14,
                name: 10,
            },
        },
        Step { client: 0 },
        Step { client: 1 },
        Plan { client: 0 },
        Plan { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 7 },
        },
        Plan { client: 2 },
        Act {
            client: 1,
            action: Edit { file: 13 },
        },
        Step { client: 0 },
        Step { client: 0 },
        Step { client: 1 },
        Step { client: 2 },
        Plan { client: 2 },
        Step { client: 1 },
        Act {
            client: 2,
            action: Move {
                node: 3,
                parent: 12,
                name: 3,
            },
        },
        Plan { client: 0 },
        Step { client: 1 },
        Plan { client: 1 },
        Plan { client: 2 },
        Act {
            client: 0,
            action: CreateFile {
                parent: 13,
                name: 13,
            },
        },
        Plan { client: 0 },
        Step { client: 2 },
        Act {
            client: 2,
            action: Delete { node: 4 },
        },
        Act {
            client: 1,
            action: Move {
                node: 7,
                parent: 10,
                name: 3,
            },
        },
        Step { client: 0 },
        Step { client: 2 },
        Act {
            client: 2,
            action: CreateFile { parent: 0, name: 7 },
        },
        Step { client: 0 },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Step { client: 0 },
        Act {
            client: 2,
            action: Edit { file: 4 },
        },
        Act {
            client: 1,
            action: Edit { file: 4 },
        },
        Plan { client: 2 },
        Step { client: 0 },
        Act {
            client: 1,
            action: Edit { file: 15 },
        },
        Step { client: 2 },
        Act {
            client: 0,
            action: Edit { file: 15 },
        },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 4 },
        },
        Act {
            client: 0,
            action: Edit { file: 11 },
        },
        Act {
            client: 0,
            action: Edit { file: 13 },
        },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Move {
                node: 5,
                parent: 12,
                name: 9,
            },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 13,
                name: 2,
            },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 6, name: 5 },
        },
        Act {
            client: 1,
            action: Move {
                node: 6,
                parent: 6,
                name: 7,
            },
        },
        Step { client: 0 },
        Step { client: 0 },
        Step { client: 0 },
        Plan { client: 2 },
        Plan { client: 0 },
        Step { client: 1 },
        Act {
            client: 2,
            action: CreateFile {
                parent: 15,
                name: 9,
            },
        },
        Plan { client: 2 },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Act {
            client: 1,
            action: CreateFolder {
                parent: 12,
                name: 12,
            },
        },
        Step { client: 0 },
        Step { client: 2 },
        Act {
            client: 0,
            action: Move {
                node: 12,
                parent: 1,
                name: 5,
            },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 10,
                name: 11,
            },
        },
        Plan { client: 1 },
        Plan { client: 2 },
        Act {
            client: 1,
            action: Move {
                node: 12,
                parent: 14,
                name: 1,
            },
        },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Act {
            client: 0,
            action: Move {
                node: 11,
                parent: 4,
                name: 8,
            },
        },
        Act {
            client: 2,
            action: Edit { file: 14 },
        },
        Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_forget_of_a_cycle_of_bases() {
    use Action::*;
    use Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateJunk { parent: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder {
                parent: 10,
                name: 0,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Act {
            client: 2,
            action: CreateJunk { parent: 1 },
        },
        Act {
            client: 2,
            action: Move {
                node: 3,
                parent: 3,
                name: 1,
            },
        },
        Act {
            client: 2,
            action: Move {
                node: 0,
                parent: 6,
                name: 5,
            },
        },
        Act {
            client: 2,
            action: Move {
                node: 13,
                parent: 3,
                name: 13,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Act {
            client: 2,
            action: ToggleIgnore,
        },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Found by a 150,000-case run: client 1 starts ignoring `d` between planning its download and
/// running it, and stops again before its next plan. A rule change counts from the next run
/// (the executor holds one filter for its run): the download settles, and a tag removed on the
/// drive later stays removed. Settled with the new rule, `d` had no base, and its next plan
/// merged the tags as if both sides had made the file.
#[test]
fn regression_a_rule_change_counts_from_the_next_plan() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(c1(), ToggleIgnore), // client 1 syncs `d`
        act(c0(), CreateFile { parent: 0, name: 5 }),
        act(c0(), Tag { node: 0, tag: 1 }), // Work
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(c1(), ToggleIgnore),
        Event::Step { client: 1 },
        act(c1(), ToggleIgnore),
        act(c0(), Tag { node: 0, tag: 1 }), // Work removed
        act(c0(), Edit { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Found by a 150,000-case run (2026-10-16): two devices hold the same file without a base,
/// both edited against the drive, and fork it. The copy id was the same for both (`fork_id`:
/// same node, same bytes, no base version), and the first device's copy was on the drive
/// (detached) when the second planned its fork: the copy's merged state was the drive's node,
/// not the fork's, so the fork took no name and the copy was created on the drive as "". Each
/// computer now makes its own copy, under its own id.
#[test]
fn regression_two_devices_fork_their_own_copies() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 1 },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 2, name: 0 },
        },
        Act {
            client: 1,
            action: CreateFile {
                parent: 0,
                name: 11,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Plan { client: 0 },
        Act {
            client: 2,
            action: Move {
                node: 1,
                parent: 1,
                name: 8,
            },
        },
        Act {
            client: 1,
            action: CreateFile { parent: 0, name: 9 },
        },
        Plan { client: 1 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Act {
            client: 2,
            action: CreateFile { parent: 1, name: 5 },
        },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFile { parent: 1, name: 0 },
        },
        Step { client: 1 },
        Act {
            client: 2,
            action: Delete { node: 15 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Act {
            client: 2,
            action: Edit { file: 8 },
        },
        Step { client: 0 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 1 },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Plan { client: 1 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// c0 detaches its edited copy of the held role folder; the phone moves the file to the top,
/// c0 downloads it there, then ignores its new name: detached again, it got the id of its first
/// detached copy, and the download overwrote the edit.
#[test]
fn regression_detached_again_while_the_first_copy_lives() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Edit, Move, Phone, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(2, CreateFolder { parent: 0, name: 0 }),
        act(2, ToggleIgnore),
        act(2, CreateFile { parent: 2, name: 0 }),
        act(0, Phone { op: 4, pick: 5 }),
        act(2, Delete { node: 0 }),
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(0, Edit { file: 0 }),
        act(0, Phone { op: 0, pick: 0 }),
        plan(0),
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        act(0, Phone { op: 11, pick: 8 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        plan(2),
        step(2),
        act(
            2,
            Move {
                node: 0,
                parent: 0,
                name: 13,
            },
        ),
        act(2, CreateFile { parent: 0, name: 0 }),
        step(0),
        plan(2),
        step(0),
        step(2),
        plan(0),
        step(0),
        step(0),
        step(2),
        step(0),
        step(0),
        step(0),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_folder_held, &events) {
        panic!("{e}");
    }
}

/// Another app deletes folder b outright, which c2 made with a file in it; the other two
/// computers never had it. Both find the file with no folder and plan, before either runs:
/// both make Recovered files and move the file into it. One folder, one copy of the file, no
/// conflict copies, and the computer that made b lets go of it.
#[test]
fn regression_two_computers_recover_the_same_orphan_at_once() {
    use Action::*;
    let mut sim = Sim::new(3);
    let act = |client, action| Event::Act { client, action };
    let (b, c) = (1, 3); // NAMES: "b", "c.txt"
    let root = sim.pick(2, NodeId::ROOT, true).expect("root");
    sim.run(&[act(
        2,
        CreateFolder {
            parent: root,
            name: b,
        },
    )]);
    let folder = sim.item(2, "b").expect("b");
    let parent = sim.pick(2, folder, true).expect("b");
    sim.run(&[
        act(2, CreateFile { parent, name: c }),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
    ]);
    let pick = sim.stopped_pick(folder).expect("b on the drive");
    sim.run(&[act(0, DeleteOutright { pick })]);
    assert!(
        !sim.server_paths().iter().any(|p| p == "b"),
        "gone for good"
    );
    sim.run(&[Event::Plan { client: 0 }, Event::Plan { client: 1 }]);
    for _ in 0..6 {
        sim.run(&[Event::Step { client: 0 }, Event::Step { client: 1 }]);
    }
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_paths(),
        ["Recovered files", "Recovered files/c.txt"],
    );
    for client in 0..3 {
        assert!(
            sim.item(client, "Recovered files/c.txt").is_some(),
            "c{client}"
        );
    }
}

/// c1 (Linux) tags its new folder "x\ny" and sends it up; then deletes the folder, which c2
/// renames meanwhile, so the delete loses and c1 makes the folder again. c1 dies before its
/// commit, and the user renames the folder: recovery takes it for a new item (a new id).
/// The add was on the drive's node, and stays there; the new item doesn't hold the tag. The
/// oracle moved the add to the new id all the same, and reported the tag lost there
/// (`cc 67c5f487…`).
#[test]
fn regression_a_sent_add_stays_with_its_node_when_the_item_gets_a_new_id() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Move, Tag};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let crash = |client| Event::Crash { client };
    let events = [
        act(1, CreateFolder { parent: 0, name: 6 }),
        act(
            1,
            CreateFile {
                parent: 1,
                name: 10,
            },
        ),
        act(1, Tag { node: 0, tag: 3 }),
        plan(1),
        step(1),
        act(1, Delete { node: 6 }),
        plan(2),
        act(1, CreateFile { parent: 0, name: 3 }),
        step(2),
        act(
            2,
            Move {
                node: 0,
                parent: 5,
                name: 2,
            },
        ),
        plan(2),
        act(1, Delete { node: 0 }),
        step(2),
        plan(1),
        crash(1),
        act(
            1,
            Move {
                node: 2,
                parent: 5,
                name: 6,
            },
        ),
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Another app moved c.txt into the file d, which every main client ignores (c0 since just
/// now, d synced before): d isn't leaving the drive while it holds c.txt (a file is judged as
/// a folder is), and c.txt stays in d, where no client syncs it (`cc` of
/// `orphans_are_recovered_without_loss`).
#[test]
fn regression_a_node_in_an_ignored_file_stays_with_it() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, Phone { op: 3, pick: 0 }),
        act(
            3,
            CreateFile {
                parent: 0,
                name: 13,
            },
        ),
        act(0, Restore { node: 0 }),
        act(2, ToggleIgnore),
        act(0, Edit { file: 0 }),
        act(3, MoveAcross { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(0, IntoFile { pick: 1, file: 6 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, ToggleIgnore),
        act(0, CreateFile { parent: 0, name: 0 }),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_device, &events) {
        panic!("{e}");
    }
}

/// Another app moves the folder b into the file c.txt: every main client moves it into
/// Recovered files on the drive, with what is in it, and they converge.
#[test]
fn regression_a_folder_moved_into_a_file_is_recovered() {
    use Action::*;
    let mut sim = Sim::new(3);
    let act = |client, action| Event::Act { client, action };
    let (b, c, d) = (1, 3, 5); // NAMES: "b", "c.txt", "d"
    let root = sim.pick(0, NodeId::ROOT, true).expect("root");
    sim.run(&[act(
        0,
        CreateFolder {
            parent: root,
            name: b,
        },
    )]);
    let root = sim.pick(0, NodeId::ROOT, true).expect("root");
    sim.run(&[act(
        0,
        CreateFile {
            parent: root,
            name: c,
        },
    )]);
    let folder = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, folder, true).expect("b");
    sim.run(&[act(0, CreateFile { parent, name: d })]);
    assert!(sim.quiesce(30), "no convergence");
    let file = sim.item(0, "c.txt").expect("c.txt");
    let (pick, file) = sim.file_move_picks(folder, file).expect("on the drive");
    sim.run(&[act(1, IntoFile { pick, file })]);
    assert!(
        sim.server_paths().iter().any(|p| p == "c.txt/b"),
        "{:?}",
        sim.server_paths()
    );
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    let paths = sim.server_paths();
    assert!(
        paths.iter().any(|p| p == "Recovered files/b/d"),
        "{paths:?}"
    );
}

/// A write's answer and another device's change that a pull recorded before it ([`Action::Meanwhile`]):
/// the change stands, everywhere. A rename's answer and a removal after it: the file goes to
/// Recently deleted on every client. A removal's answer and a restore after it: the file is back
/// on every client.
#[test]
fn regression_a_change_pulled_before_an_answer_stands() {
    use Action::*;
    let mut sim = Sim::new(2);
    sim.run(&[
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 1 },
        },
    ]);
    assert!(sim.quiesce(10));
    let file = sim.item(0, "a").expect("a");
    let pick = sim.pick(0, file, false).expect("a pick");
    // Renamed to "c.txt", removed meanwhile by another device.
    sim.run(&[
        Event::Act {
            client: 0,
            action: Move {
                node: pick,
                parent: 0,
                name: 3,
            },
        },
        Event::Act {
            client: 0,
            action: Meanwhile { op: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ]);
    assert!(sim.is_removed(file), "{:?}", sim.server_paths());
    assert!(sim.quiesce(10));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.item(0, "c.txt"), None, "the removal stands");
    assert_eq!(sim.item(1, "c.txt"), None);

    // Deleted here, restored meanwhile by another device.
    let other = sim.item(0, "b").expect("b");
    let pick = sim.pick(0, other, false).expect("b pick");
    sim.run(&[
        Event::Act {
            client: 0,
            action: Delete { node: pick },
        },
        Event::Act {
            client: 0,
            action: Meanwhile { op: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ]);
    assert!(!sim.is_removed(other), "restored: {:?}", sim.server_paths());
    assert!(sim.quiesce(10));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.item(0, "b"), Some(other), "the restore stands");
    assert_eq!(sim.item(1, "b"), Some(other));
}

/// Seed bf84d30f, a known limit (the in-flight family, the user's call: not now): c0's write
/// carrying its Red lands, and another device removes the folder before the answer is
/// recorded; c0's base never takes Red as on the drive, so once the folder is back c0 sends
/// Red up again, over c1's later removal. The oracle names it.
#[test]
fn regression_an_add_whose_answer_was_superseded_is_a_known_limit() {
    use Action::*;
    use Event::*;
    let limits = known_limits_of(
        3,
        &[
            Act {
                client: 1,
                action: CreateJunk { parent: 0 },
            },
            Act {
                client: 1,
                action: CreateFolder {
                    parent: 0,
                    name: 11,
                },
            },
            Act {
                client: 1,
                action: CreateFile { parent: 0, name: 1 },
            },
            Act {
                client: 0,
                action: CreateFolder { parent: 0, name: 3 },
            },
            Plan { client: 1 },
            Act {
                client: 0,
                action: CreateFile { parent: 0, name: 0 },
            },
            Step { client: 1 },
            Act {
                client: 0,
                action: Meanwhile { op: 0 },
            },
            Act {
                client: 0,
                action: Tag { node: 2, tag: 0 },
            },
            Plan { client: 0 },
            Step { client: 0 },
            Step { client: 0 },
            Act {
                client: 0,
                action: ListDeleted,
            },
            Act {
                client: 0,
                action: EmptyListed,
            },
            Plan { client: 0 },
            Step { client: 0 },
            Plan { client: 1 },
            Act {
                client: 0,
                action: CreateFile { parent: 0, name: 0 },
            },
            Act {
                client: 0,
                action: CreateFile { parent: 0, name: 0 },
            },
            Act {
                client: 1,
                action: CreateFile { parent: 0, name: 2 },
            },
            Act {
                client: 0,
                action: CreateFile { parent: 0, name: 0 },
            },
            Step { client: 1 },
            Act {
                client: 1,
                action: Tag { node: 13, tag: 12 },
            },
        ],
    );
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("AnswerSuperseded") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}

/// c2 makes b/d/c.txt; another app deletes folder b for good, its folder d left live: an
/// orphan. c1 ignores everything named d (§10), so Recovered files/d is no place it syncs: it
/// leaves the orphan to the computers that do. Brought in, d would have read as an item c1
/// stopped syncing and left the drive, another device's file with it. The others recover it;
/// nothing goes to Recently deleted.
#[test]
fn regression_an_orphan_at_a_place_a_computer_ignores_stays_on_the_drive() {
    use Action::*;
    let mut sim = Sim::new(3).with_narrow_check();
    let act = |client, action| Event::Act { client, action };
    let (b, c, d) = (1, 3, 5); // NAMES: "b", "c.txt", "d"
    // c1, an odd one, ignores everything named d.
    let root = sim.pick(2, NodeId::ROOT, true).expect("root");
    sim.run(&[act(
        2,
        CreateFolder {
            parent: root,
            name: b,
        },
    )]);
    let folder = sim.item(2, "b").expect("b");
    let parent = sim.pick(2, folder, true).expect("b");
    sim.run(&[act(2, CreateFolder { parent, name: d })]);
    let inner = sim.item(2, "b/d").expect("b/d");
    let parent = sim.pick(2, inner, true).expect("b/d");
    sim.run(&[
        act(2, CreateFile { parent, name: c }),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
    ]);
    let pick = sim.stopped_pick(folder).expect("b on the drive");
    sim.run(&[act(0, DeleteOutright { pick })]);
    // c1 plans first, alone.
    sim.run(&[Event::Plan { client: 1 }]);
    for _ in 0..4 {
        sim.run(&[Event::Step { client: 1 }]);
    }
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_paths(),
        [
            "Recovered files",
            "Recovered files/d",
            "Recovered files/d/c.txt"
        ],
    );
    assert!(sim.item(1, "Recovered files/d").is_none(), "c1 ignores d");
    for client in [0, 2] {
        assert!(
            sim.item(client, "Recovered files/d/c.txt").is_some(),
            "c{client}"
        );
    }
}
