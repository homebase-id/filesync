//! Regressions: crashes, failed commits, placements and their records.

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};

/// An item parked under a temporary name whose original name a rule change then filters,
/// while another item this client ignores holds that name.
#[test]
fn regression_parked_under_a_name_that_became_ignored() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let file = |client| act(client, CreateFile { parent: 0, name: 0 });
    let events = [
        file(2),
        file(1),
        Event::Plan { client: 1 },
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(1, CreateFolder { parent: 0, name: 5 }),
        act(
            2,
            CreateFolder {
                parent: 0,
                name: 13,
            },
        ),
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Plan { client: 0 },
        file(0),
        file(0),
        file(0),
        act(1, ToggleIgnore),
        act(
            2,
            Move {
                node: 2,
                parent: 2,
                name: 0,
            },
        ),
        file(0),
        file(0),
        file(0),
        Event::Plan { client: 2 },
        file(0),
        file(0),
        Event::Plan { client: 1 },
        Event::Step { client: 2 },
        Event::Step { client: 1 },
        act(1, Delete { node: 1 }),
        Event::Plan { client: 2 },
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        Event::Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The file is deleted on the drive between a download's fetch and its commit: the commit
/// settles against the version it fetched, so the delete reaches the client. Settled against
/// the drive (nothing there), the file had no base, and the client put it back.
#[test]
fn regression_a_delete_during_a_download_is_taken() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2).with_split_downloads();
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ]);
    let file = sim.item(0, "a").expect("the file");
    sim.run(&[
        Event::Plan { client: 1 },
        Event::Step { client: 1 }, // fetched
        act(0, Delete { node: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 }, // removed on the drive
        Event::Step { client: 1 }, // in place, committed
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert!(sim.is_removed(file), "the file came back on the drive");
    assert_eq!(sim.item(1, "a"), None);
}

/// A download put in place, its commit lost, and the next sync's scan doesn't hash the file yet
/// (the first crash leaves it so): recovery keeps the record and the plan defers the node; the
/// sync after settles it. A record kept holds its node off placements, never for good.
#[test]
fn a_record_kept_past_recovery_is_settled_later() {
    use Action::*;
    let mut sim = Sim::new(2).with_split_downloads();
    sim.run(&[
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },  // fetched
        Event::Crash { client: 1 }, // put in place, its commit lost
        Event::Plan { client: 1 },  // the record kept
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// Client 1's plan downloads both files a changed; the user edits b there after the plan, and
/// the download of a fails its commit once in place. Recovery settles a by its own item alone:
/// b's download still finds the edit, which isn't lost (the engine's recovery rescanned the
/// folder, and b's next guard passed on the edit).
#[test]
fn a_sibling_edit_survives_a_failed_commit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(10));
    let (a, b) = (sim.item(0, "a").expect("a"), sim.item(0, "b").expect("b"));
    let (a0, b0) = (
        sim.file_pick(0, a).expect("a on 0"),
        sim.file_pick(0, b).expect("b on 0"),
    );
    sim.run(&[
        act(0, Edit { file: a0 }),
        act(0, Edit { file: b0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
    ]);
    let b1 = sim.file_pick(1, b).expect("b on 1");
    sim.run(&[act(1, Edit { file: b1 })]);
    sim.fail_next_commit();
    sim.run(&[Event::Step { client: 1 }, Event::Step { client: 1 }]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// As [`regression_the_purge_finishes_a_delete_stopped_half_way`], but the phone edits b/a/c.txt
/// after the delete: written after the removal, it isn't taken along. The purge leaves it live,
/// and b/a above it, and b holding them; it removes b/c.txt only. The devices bring b back
/// with them, nothing lost.
#[test]
fn regression_the_purge_leaves_what_was_written_after_a_stopped_delete() {
    use Action::*;
    let (mut sim, [b, a, c, ac]) = stopped_delete_setup();
    let pick = sim.stopped_pick(b).expect("b");
    let act = |action| Event::Act { client: 0, action };
    sim.run(&[act(DeleteStopped { pick, done: 0 })]);
    // EditTaken picks among the files taken along, in id order.
    let pick = usize::from(ac > c);
    sim.run(&[act(EditTaken { pick })]);
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(sim.is_removed(c), "taken along: removed");
    assert!(
        !sim.is_removed(a) && !sim.is_removed(ac),
        "written after: live"
    );
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(sim.is_removed(b), "b holds them");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), ["b", "b/a", "b/a/c.txt"]);
}

/// A file put into a folder after another device removed it (with what was in it) isn't in
/// that delete: the purge leaves the folder while the file is live in it, so the file keeps its
/// folder, which comes back, and nothing is lost.
#[test]
fn regression_a_file_written_into_a_removed_folder_holds_it_through_the_purge() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let folder = |sim: &Sim, c: usize, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, true).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    // NAMES: "a", "c.txt", "b" (client 1 ignores "d").
    let (folder_name, c, b) = (0, 3, 1);
    let parent = folder(&sim, 0, "");
    sim.run(&[act(
        0,
        CreateFolder {
            parent,
            name: folder_name,
        },
    )]);
    let parent = folder(&sim, 0, "a");
    sim.run(&[act(0, CreateFile { parent, name: c })]);
    assert!(sim.quiesce(30), "no convergence");
    // Client 1 plans its new file before client 0's delete of the folder lands; the upload
    // goes into the removed folder.
    let parent = folder(&sim, 1, "a");
    sim.run(&[
        act(1, CreateFile { parent, name: b }),
        Event::Plan { client: 1 },
    ]);
    let node = sim.pick(0, sim.item(0, "a").expect("a"), false).expect("a");
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        // The folder, then the file in it.
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
    ]);
    sim.run(&[
        // Daily purges, until the folder's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    // The file brings its folder back (not left at the top, the folder gone for good).
    assert_eq!(sim.server_paths(), ["a", "a/b"]);
}

/// A folder is written into another after every client began to ignore that one, and after it
/// was removed: written after the removal, it stays; its folder, ignored everywhere, isn't any
/// client's to bring back, so it goes to the top, where it stays visible.
#[test]
fn regression_a_folder_written_into_an_ignored_removed_one_stays_visible() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(
            2,
            CreateFolder {
                parent: 0,
                name: 13,
            },
        ),
        plan(2),
        act(2, CreateFolder { parent: 0, name: 0 }),
        step(2),
        plan(0),
        act(0, Edit { file: 0 }),
        plan(2),
        step(0),
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        plan(0),
        step(0),
        step(2),
    ];
    if let Err(e) = run(3, &events) {
        panic!("{e}");
    }
    if let Err(e) = run_with(3, None, Sim::with_role_folder, &events) {
        panic!("{e}");
    }
}

/// Client 0 deletes a tagged folder whose drive copy holds a file it never had: the delete loses,
/// and the folder is made again, but the crash comes before its tags are written and its commit.
/// The user then renames it: away from the record's place, the scan finds it as a new folder
/// (the engine's write-ahead record pairs only what is at its place), not the node with no tags.
/// The simulator kept it as the node, its missing tag read as a removal (`cc 03016076…`).
#[test]
fn regression_a_folder_made_again_and_moved_before_recovery_is_a_new_folder() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 0, tag: 8 },
        },
        Act {
            client: 1,
            action: CreateFile { parent: 6, name: 0 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 1 },
        Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Plan { client: 0 },
        Crash { client: 0 },
        Act {
            client: 0,
            action: Move {
                node: 0,
                parent: 7,
                name: 1,
            },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// c1 makes folder a, which clashes with the role folder a: its plan parks it under a temporary
/// name to rename it. The user tags it Work; another device deletes the role folder outright,
/// so the next plan creates c1's folder on the drive as a, with Work, while it is still parked.
/// c0 removes Work. The executor reads a parked node at its original place, so the create
/// settles a base with Work and the removal comes down. The simulator settled it at its parked
/// name: no base, and c1 sent Work up again as its add (`cc b961b04c…`).
#[test]
fn regression_a_parked_folder_created_on_the_drive_settles_its_base() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, DeleteOutright, Tag, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(2, CreateFolder { parent: 0, name: 2 }),
        plan(2),
        act(1, CreateFolder { parent: 0, name: 8 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(2),
        plan(1),
        step(1),
        act(0, CreateFolder { parent: 0, name: 0 }),
        act(1, Tag { node: 1, tag: 5 }),
        act(0, DeleteOutright { pick: 0 }),
        plan(1),
        act(0, DeleteOutright { pick: 0 }),
        step(1),
        plan(0),
        act(0, DeleteOutright { pick: 0 }),
        plan(1),
        act(0, ToggleIgnore),
        step(1),
        step(0),
        act(0, Tag { node: 14, tag: 13 }),
    ];
    run_with(3, None, Sim::with_role_device, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The same as the regression above with a file's rename: c0 tags its new folder c.txt Red and
/// sends it up; c2 renames a file to c.txt, so the folder becomes "c (conflict 1).txt", and c0's
/// delete of the folder loses. c0 makes it again, dies before its commit, and the user moves
/// it: recovery takes it for a new item. The add stayed with the node; the oracle moved it to
/// the new id, which holds no Red (`cc c229b7e4…`).
#[test]
fn regression_a_sent_add_stays_with_its_node_after_a_lost_delete_and_a_crash() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Move, Tag};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let crash = |client| Event::Crash { client };
    let events = [
        act(2, CreateFile { parent: 0, name: 0 }),
        act(
            2,
            Move {
                node: 0,
                parent: 0,
                name: 11,
            },
        ),
        act(0, CreateFolder { parent: 0, name: 3 }),
        act(0, Tag { node: 0, tag: 0 }),
        plan(0),
        step(0),
        plan(2),
        step(2),
        act(0, Delete { node: 0 }),
        step(2),
        step(2),
        plan(0),
        crash(0),
        act(
            0,
            Move {
                node: 0,
                parent: 1,
                name: 0,
            },
        ),
        act(0, CreateFile { parent: 0, name: 0 }),
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Seed 58230c9c (it failed on main): c2 deletes the folder a it was making again, a crash
/// loses the commit of a, made again since, and the user moves it into another folder before
/// the next sync. Recovery takes the moved item for a new one; the drive's a, no longer held
/// here, is removed from there as the user's doing (the oracle took it for a removal no one
/// asked for).
#[test]
fn regression_a_folder_moved_after_a_lost_commit_leaves_as_the_users() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        act(2, CreateFolder { parent: 0, name: 0 }),
        Event::Step { client: 1 },
        act(1, CreateFolder { parent: 8, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        act(2, Delete { node: 8 }),
        Event::Plan { client: 2 },
        Event::Crash { client: 2 },
        act(
            1,
            Move {
                node: 5,
                parent: 8,
                name: 3,
            },
        ),
        act(
            2,
            Move {
                node: 8,
                parent: 4,
                name: 0,
            },
        ),
    ];
    if let Err(e) = run_with(3, None, Sim::with_split_downloads, &events) {
        panic!("{e}");
    }
}

/// A file being written (not hashed at the next plan) in a folder another app deletes
/// outright: the folder isn't trashed with it, the file's edit comes up, and the folder comes
/// back with it (its edits win). It was trashed here, and the edit lost.
#[test]
fn regression_a_file_being_written_keeps_its_folder() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(2, CreateFolder { parent: 0, name: 0 }),
        act(2, CreateFile { parent: 6, name: 0 }),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
        act(2, EditWriting { file: 0 }),
        act(0, DeleteOutright { pick: 0 }),
    ];
    if let Err(e) = run(3, &events) {
        panic!("{e}");
    }
}
