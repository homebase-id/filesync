//! Regressions: tags across devices and file systems, and the known limits of tag merging.

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

/// The narrow property's setup (split downloads, plans checked against the engine's path):
/// the tag adds `events` lose to a known limit, as [`Sim::tags_lost_to_known_limits`] names
/// them.
fn known_limits_narrow(events: &[Event]) -> Vec<String> {
    let mut sim = Sim::with_split_downloads(Sim::new(3)).with_narrow_check();
    sim.run(events);
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    sim.tags_lost_to_known_limits()
}

/// The drive renames a file and tags it red (client 0): client 1's plan moves it and tags it,
/// two writes on the node, the second checking the base at the version the first settled it to.
/// Then client 1's user removes red: it stays removed. (Checked against the plan's version, the
/// second write's base was left alone, red unagreed, and the next plan wrote red back.)
#[test]
fn regression_a_tag_removed_after_a_rename_and_tag_stays_removed() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(10));
    let file = sim.item(0, "a").expect("the file");
    let root = sim.pick(0, NodeId::ROOT, true).expect("the root");
    let it = sim.pick(0, file, false).expect("on client 0");
    sim.run(&[
        act(
            0,
            Move {
                node: it,
                parent: root,
                name: 1,
            },
        ), // b
        act(0, Tag { node: it, tag: 0 }), // Red
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ]);
    assert_eq!(sim.local_tags(1, "b"), Some(vec!["Red:0".to_owned()]));
    let it = sim.pick(1, file, false).expect("on client 1");
    sim.run(&[act(1, Tag { node: it, tag: 0 })]); // Red removed
    assert!(sim.quiesce(10));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.server_tags("b"), Some(Vec::new()));
}

/// A tagged folder is deleted here while another device adds to it: the delete loses, and the
/// folder is made again, with its record (synced before). The client dies once it is made,
/// before its tags are written: the record claims none, so recovery takes only what the folder
/// and the drive both have, and the next plan writes the drive's tags on the folder. A record
/// that claimed them up front took the bare folder for agreed: the next plan removed them from
/// the drive.
#[test]
fn regression_a_folder_made_again_keeps_its_tags() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(c0(), CreateFolder { parent: 0, name: 0 }),
        act(c0(), Tag { node: 0, tag: 0 }), // Red
    ]);
    assert!(sim.quiesce(10));
    let folder = sim.item(0, "a").expect("the folder");
    let into = sim.pick(1, folder, true).expect("on client 1");
    let it = sim.pick(0, folder, false).expect("on client 0");
    sim.run(&[
        act(
            c1(),
            CreateFile {
                parent: into,
                name: 1,
            },
        ),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(c0(), Delete { node: it }),
        Event::Plan { client: 0 },
        Event::Crash { client: 0 },
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.server_tags("a"), Some(vec!["Red:6".to_owned()]));
}

/// Found by a 150,000-case run (2026-10-16), failing on main: a tag added on client 1 was lost
/// ("lost tag \"Red\"") with clients that sync only when asked and the guard at 1. Passes
/// with sim-role-fixes' tag fixes.
#[test]
fn regression_a_tag_lost_when_syncing_only_when_asked() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 8 },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 6, name: 6 },
        },
        Act {
            client: 0,
            action: CreateFolder { parent: 0, name: 3 },
        },
        Plan { client: 0 },
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 6 },
        },
        Plan { client: 2 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 1,
            action: Edit { file: 0 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Act {
            client: 0,
            action: Delete { node: 5 },
        },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 0 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Act {
            client: 1,
            action: Move {
                node: 12,
                parent: 1,
                name: 0,
            },
        },
        Step { client: 0 },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 8 },
        },
        Act {
            client: 1,
            action: Tag { node: 2, tag: 4 },
        },
        Step { client: 0 },
        Plan { client: 1 },
        Step { client: 1 },
        Step { client: 0 },
        Step { client: 0 },
    ];
    run_settled(
        3,
        Some(1),
        |sim| sim,
        |sim| sim.quiesce_announced(300),
        &events,
    )
    .map_err(|e| e.to_string())
    .unwrap_or_else(|e| panic!("{e}"));
}

/// Tags meet on every device: a Mac's tag and colour change, a Linux tag (Linux keeps no
/// colours, and keeps the Mac's), and a Windows device, which has no tags, keeps them through
/// its own edit and rename. A tag removed anywhere goes everywhere.
#[test]
fn tags_meet_across_macos_linux_and_windows() {
    use Action::*;
    let mut sim = Sim::new(3); // c0 macOS, c1 Linux, c2 Windows
    let act = |client, action| Event::Act { client, action };
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(10));
    sim.run(&[
        act(0, Tag { node: 0, tag: 0 }), // Red, on the Mac
        act(1, Tag { node: 0, tag: 1 }), // Work, on Linux at the same time
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(
        sim.server_tags("a"),
        Some(vec!["Red:6".into(), "Work:0".into()])
    );
    sim.run(&[
        act(0, Colour { node: 0, tag: 0 }),
        act(2, Edit { file: 0 }),
        act(
            2,
            Move {
                node: 0,
                parent: 0,
                name: 1,
            },
        ),
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(
        sim.server_tags("b"),
        Some(vec!["Red:7".into(), "Work:0".into()])
    );
    sim.run(&[act(1, Tag { node: 0, tag: 0 })]); // Linux removes Red
    assert!(sim.quiesce(10));
    assert_eq!(sim.server_tags("b"), Some(vec!["Work:0".into()]));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// A tag change is not an edit: against a delete elsewhere the delete wins (the item is in
/// Recently deleted), and against an edit or a move both go through.
#[test]
fn a_tag_change_loses_to_a_delete_and_meets_an_edit_or_move() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let sync = |client| {
        [
            Event::Plan { client },
            Event::Step { client },
            Event::Step { client },
        ]
    };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(10));
    // `a`: tagged here, deleted there first. `b`: tagged here, edited and renamed there.
    sim.run(&[
        act(0, Tag { node: 0, tag: 0 }),
        act(0, Tag { node: 1, tag: 1 }),
        act(1, Delete { node: 0 }),
        act(1, Edit { file: 1 }),
        act(
            1,
            Move {
                node: 1,
                parent: 0,
                name: 3,
            },
        ),
    ]);
    sim.run(&sync(1));
    assert!(sim.quiesce(10));
    assert_eq!(sim.server_tags("a"), None);
    assert_eq!(sim.server_tags("c.txt"), Some(vec!["Work:0".into()]));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// c2 starts ignoring `d`, which it synced, after c0 tagged it: `d` was taken off the drive as
/// unchanged since c2 synced it (only place and bytes were compared). It stays, with its tag.
#[test]
fn regression_a_folder_tagged_elsewhere_doesnt_leave() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Purge, Tag, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(0, CreateFolder { parent: 0, name: 5 }),
        plan(0),
        step(0),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 7, name: 0 }),
        act(0, Tag { node: 2, tag: 0 }),
        act(1, ToggleIgnore),
        plan(2),
        plan(1),
        plan(0),
        step(1),
        step(2),
        act(2, ToggleIgnore),
        step(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        plan(2),
        step(2),
        step(2),
        act(1, CreateFile { parent: 6, name: 0 }),
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        plan(1),
        step(1),
    ];
    for setup in [(|sim| sim) as fn(Sim) -> Sim, Sim::with_role_folder] {
        let mut sim = setup(Sim::new(3));
        sim.run(&events);
        assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            panic!("{e}");
        }
        assert_eq!(sim.server_tags("d"), Some(vec!["Red:6".to_owned()]));
    }
}

/// One client removes a folder while the other, which synced a folder made in it, tags that
/// folder and makes one in it. The removal would take the synced folder along (§6.3), but the
/// new one keeps it alive: merged after all, it keeps its tag.
#[test]
fn regression_a_folder_kept_alive_in_a_removed_one_keeps_its_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        plan(1),
        step(1),
        act(1, Delete { node: 0 }),
        plan(0),
        step(0),
        plan(1),
        act(0, CreateFolder { parent: 2, name: 0 }),
        plan(0),
        step(0),
        act(0, Tag { node: 1, tag: 4 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(1),
        act(
            0,
            CreateFolder {
                parent: 10,
                name: 0,
            },
        ),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
        panic!("{e}");
    }
}

/// A synced folder is moved into another and tagged; a client that had it at its old place
/// only by a download that landed after that, removes the other folder: unchanged here, the
/// folder goes with it, into Recently deleted with its tag (the drive's), on no computer.
#[test]
fn regression_a_folder_taken_along_goes_with_its_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[act(1, CreateFolder { parent: 0, name: 6 })]);
    let folder = sim.item_named(1, ".txt").expect("the folder");
    sim.run(&[
        act(0, CreateFolder { parent: 0, name: 8 }),
        plan(0),
        step(0),
        plan(1),
        act(0, Delete { node: 0 }),
        step(1),
        step(1),
        act(
            1,
            Move {
                node: 2,
                parent: 1,
                name: 0,
            },
        ),
        act(1, Tag { node: 8, tag: 0 }),
        plan(1),
        plan(0),
        step(1),
        step(0),
        step(0),
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert!(sim.is_removed(folder), "{:?}", sim.server_paths());
    assert!(
        sim.removed_tags(folder)
            .iter()
            .any(|t| t.starts_with("Red")),
        "Recently deleted keeps Red"
    );
}

/// A synced folder is moved into another and tagged on one computer; the other, which hadn't
/// seen that yet, removes the other folder, taking it along, and renames its copy: the rename
/// brings it back with the tag the drive had, which this computer never saw (§11).
#[test]
fn regression_a_folder_brought_back_from_a_removal_keeps_the_drives_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[
        act(1, CreateFolder { parent: 0, name: 6 }),
        act(0, CreateFolder { parent: 0, name: 8 }),
    ]);
    assert!(sim.quiesce(10));
    let folder = sim.item_named(1, ".txt").expect("the folder");
    let other = sim.item(0, "a").expect("the other folder");
    let pick = |sim: &Sim, c, n, parent| sim.pick(c, n, parent).expect("picked");
    let (f1, a1, a0) = (
        pick(&sim, 1, folder, false),
        pick(&sim, 1, other, true),
        pick(&sim, 0, other, false),
    );
    sim.run(&[
        act(
            1,
            Move {
                node: f1,
                parent: a1,
                name: 1,
            },
        ),
        act(1, Tag { node: f1, tag: 0 }),
        act(0, Delete { node: a0 }),
        plan(1),
        plan(0),
        step(1),
        step(0),
    ]);
    // Client 0 renames its copy, which it still has at the top.
    let (f0, top) = (
        pick(&sim, 0, folder, false),
        pick(&sim, 0, NodeId::ROOT, true),
    );
    sim.run(&[act(
        0,
        Move {
            node: f0,
            parent: top,
            name: 1,
        },
    )]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    let red =
        |tags: Option<Vec<String>>| tags.is_some_and(|t| t.iter().any(|t| t.starts_with("Red")));
    assert!(
        red(sim.server_tags("b")),
        "the drive keeps Red: {:?}",
        sim.server_paths()
    );
    // The Mac and the Linux computer; Windows holds no tags (the drive keeps them, §11).
    for c in 0..2 {
        assert!(red(sim.local_tags(c, "b")), "computer {c} keeps Red");
    }
}

/// A folder restored from Recently deleted, made here before its RemoteCreate runs: another
/// device restores it first, with a tag removed. The folder made here has the removed version
/// as its base, so the removal wins (with no base, the tag read as added here, and came back).
#[test]
fn regression_a_tag_removed_while_a_restored_folder_has_no_base() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 0, tag: 9 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 1, name: 5 },
        },
        Plan { client: 0 },
        Act {
            client: 1,
            action: CreateFolder { parent: 7, name: 0 },
        },
        Step { client: 0 },
        Act {
            client: 1,
            action: Move {
                node: 3,
                parent: 13,
                name: 3,
            },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 2, name: 0 },
        },
        Plan { client: 1 },
        Plan { client: 0 },
        Step { client: 1 },
        Step { client: 0 },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 0 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 5 },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Client 1 downloads a version client 0 tagged Work; the crash comes once it is in place,
/// before its commit, and the user edits it. Client 0 removes Work meanwhile. Recovery can't
/// tell the edited item from the file there before (its bytes are neither), so it drops the
/// record and the old base stands: the Work the download wrote reads as client 1 adding it,
/// and comes back (the known limit, sync semantics §8, §11; `cc 86011d5e…`). Nothing is lost:
/// the edit goes to a conflict copy.
#[test]
fn regression_a_tag_a_crashed_download_wrote_may_come_back_after_an_edit() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 1 },
        },
        Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 9 },
        },
        Step { client: 1 },
        Plan { client: 1 },
        Step { client: 1 },
        Crash { client: 1 },
        Act {
            client: 1,
            action: Edit { file: 0 },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A Linux client tags a folder and writes a file into it as a macOS client deletes it: the
/// file brings the folder back with the tag, and the Windows client's plan, made while the
/// folder was deleted, restores it too. The macOS client deletes it again before that restore
/// is sent: the restore merges with the version deleted, and a tag neither of the two clients
/// can hold comes back with it, never removed by them (§11).
#[test]
fn regression_a_late_restore_keeps_a_tag_its_device_cant_hold() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Tag};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        plan(1),
        step(1),
        plan(0),
        step(0),
        act(1, Tag { node: 0, tag: 3 }),
        act(
            1,
            CreateFile {
                parent: 14,
                name: 0,
            },
        ),
        plan(1),
        act(0, Delete { node: 0 }),
        plan(0),
        step(0),
        step(1),
        plan(2),
        plan(1),
        step(1),
        step(2),
        plan(0),
        step(0),
        step(0),
        act(0, Delete { node: 0 }),
        plan(0),
        step(0),
        step(2),
    ];
    if let Err(e) = run(3, &events) {
        panic!("{e}");
    }
}

/// A Documents folder restores a folder deleted elsewhere, tagged and renamed here meanwhile,
/// as the phone deletes Documents and the user restores that folder at the drive's top: the
/// restore finds it there, written by another, and sends nothing; Documents lets go of it, the
/// tag not sent up with it (§12).
#[test]
fn regression_a_restore_found_done_elsewhere_sends_no_tag() {
    use hbsync_sim::Action::{CreateFolder, Delete, DeleteRoleFolder, Move, Restore, Tag};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(4, CreateFolder { parent: 0, name: 2 }),
        plan(4),
        step(4),
        plan(3),
        act(4, Delete { node: 0 }),
        plan(4),
        step(4),
        step(3),
        act(
            3,
            Move {
                node: 0,
                parent: 1,
                name: 0,
            },
        ),
        act(3, Tag { node: 0, tag: 4 }),
        plan(3),
        act(0, DeleteRoleFolder),
        act(0, Restore { node: 2 }),
        step(3),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// Client 1 makes folder c.txt tagged red, and it is synced. Client 2 deletes it as client 1
/// writes a file into it; client 0 deletes it too. Client 0's next plan makes it again for that
/// file (its RemoteCreate to follow), and its tag write fails: it holds no red. Its base kept
/// red, so the next plan read the folder without it as the user's removal, and removed red on
/// the drive for everyone. The failed write settles as not known: against the base, as no
/// version of the drive is there, red goes, and the next plan writes it here again
/// (`cc b63b7063…`, CI run 39377359513).
#[test]
fn regression_a_folder_made_back_whose_tag_write_fails_keeps_its_tag() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Edit, Move, Tag, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        act(
            1,
            Move {
                node: 0,
                parent: 13,
                name: 3,
            },
        ),
        act(1, Tag { node: 0, tag: 8 }),
        act(1, CreateFolder { parent: 7, name: 9 }),
        act(1, CreateFile { parent: 1, name: 0 }),
        plan(1),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(1),
        plan(2),
        step(2),
        act(2, Delete { node: 0 }),
        plan(0),
        step(1),
        step(0),
        act(0, ToggleIgnore),
        step(1),
        act(0, Edit { file: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        plan(2),
        step(2),
        step(2),
        act(1, CreateFile { parent: 3, name: 0 }),
        plan(0),
        plan(1),
        step(0),
        step(0),
        step(2),
        step(0),
        act(0, Delete { node: 12 }),
        step(0),
        step(1),
    ];
    run_with(3, None, Sim::with_role_folder, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The other side of the regression above: c0 (macOS) syncs folder a tagged Work. c1 removes
/// Work and then deletes a, while c2, which never saw either, writes a file into it. c0 deleted
/// a too, and makes it again for c2's file, its tag write failing: it holds no Work. The drive's
/// last version lacks Work, so c0's base keeps it, and the restore that follows merges c1's
/// removal in. Settled against c0's base alone, Work read as added there, and came back.
#[test]
fn regression_a_folder_made_back_whose_tag_write_fails_keeps_a_removal() {
    use Action::{CreateFile, CreateFolder, Delete, Tag};
    let (a, work) = (0, 1); // NAMES: "a"; TAGS: "Work"
    let mut sim = Sim::new(3);
    let sync = |sim: &mut Sim, client| {
        sim.run(&[Event::Plan { client }]);
        for _ in 0..8 {
            sim.run(&[Event::Step { client }]);
        }
    };
    let act = |sim: &mut Sim, client, action| sim.run(&[Event::Act { client, action }]);
    let root = sim.pick(0, NodeId::ROOT, true).expect("root");
    act(
        &mut sim,
        0,
        CreateFolder {
            parent: root,
            name: a,
        },
    );
    let folder = sim.item(0, "a").expect("a");
    let node = sim.pick(0, folder, false).expect("a");
    act(&mut sim, 0, Tag { node, tag: work });
    for c in 0..3 {
        sync(&mut sim, c);
    }
    assert_eq!(sim.server_tags("a"), Some(vec!["Work:0".to_owned()]));
    // c2 writes into a, planned before the delete reaches the drive.
    let parent = sim.pick(2, folder, true).expect("a on c2");
    act(&mut sim, 2, CreateFile { parent, name: a });
    sim.run(&[Event::Plan { client: 2 }]);
    let node = sim.pick(1, folder, false).expect("a on c1");
    act(&mut sim, 1, Tag { node, tag: work });
    sync(&mut sim, 1);
    let node = sim.pick(1, folder, false).expect("a on c1");
    act(&mut sim, 1, Delete { node });
    sync(&mut sim, 1);
    let node = sim.pick(0, folder, false).expect("a on c0");
    act(&mut sim, 0, Delete { node });
    for _ in 0..8 {
        sim.run(&[Event::Step { client: 2 }]);
    }
    assert!(!sim.server_paths().iter().any(|p| p == "a"), "a is deleted");
    sim.fail_next_tag_write();
    sync(&mut sim, 0);
    assert_eq!(sim.local_tags(0, "a"), Some(vec![]), "the write failed");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_tags("a"),
        Some(vec![]),
        "{:?}",
        sim.server_paths()
    );
}

/// c0 makes folder b tagged Red; c2 a file b, so the folder is "b (conflict 1)" on the drive.
/// c1 (Linux) plans to make it under that name, and dies once it is made, before its commit:
/// the folder has no base. Its next plan renames it on the drive and writes the drive's Red
/// here; the rename loses to c0's own, and c0 removes Red. The folder still had no base (its
/// name and the version the write acted on, still b, disagreed), so the Red written here read
/// as c1 adding it, and went back up. Now a tag write waits for a base: the drive's tags come
/// down once one is settled (`cc cfaf6f6d…`).
#[test]
fn regression_a_tag_written_before_a_base_isnt_an_add() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Edit, Tag};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let crash = |client| Event::Crash { client };
    let events = [
        act(2, CreateFile { parent: 0, name: 2 }),
        act(2, CreateFile { parent: 0, name: 1 }),
        act(0, CreateFolder { parent: 0, name: 1 }),
        act(0, Edit { file: 0 }),
        act(0, Tag { node: 0, tag: 12 }),
        plan(2),
        act(1, Edit { file: 0 }),
        plan(0),
        step(0),
        step(2),
        step(2),
        plan(0),
        plan(1),
        crash(1),
        act(1, CreateFile { parent: 0, name: 0 }),
        step(0),
        plan(1),
        step(1),
        step(0),
        step(1),
        step(1),
        act(0, Tag { node: 0, tag: 0 }),
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The same as the regression above, from the other side: c1's folder a, tagged Red, clashes
/// with c0's file a, and c0 (macOS) makes it as "a (conflict 1)", dying before its commit. c1
/// removes Red. c0's next plan wrote the drive's Red on its folder with no base to record it,
/// and its rename took Red back up (`cc ab37e2a6…`).
#[test]
fn regression_a_tag_written_before_a_base_isnt_an_add_on_the_renaming_side() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Tag};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let crash = |client| Event::Crash { client };
    let events = [
        act(0, CreateFile { parent: 0, name: 8 }),
        act(1, CreateFolder { parent: 0, name: 8 }),
        act(1, Tag { node: 0, tag: 12 }),
        act(0, CreateFile { parent: 0, name: 1 }),
        act(0, CreateFile { parent: 0, name: 3 }),
        plan(1),
        act(1, CreateFile { parent: 0, name: 3 }),
        step(1),
        act(1, CreateFolder { parent: 0, name: 6 }),
        plan(0),
        act(1, Tag { node: 15, tag: 4 }),
        plan(1),
        crash(0),
        plan(0),
        step(0),
        step(0),
        step(0),
        step(1),
        plan(0),
        step(0),
        step(1),
        step(1),
        step(0),
        step(0),
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Seed 0a9df7c6, a known limit (the user's call: not now): c0's edit loses to c1's, which
/// also removed Red, so c0 forks its item into a conflict copy and downloads c1's version as
/// a new item. Its base keeps Red, which neither the new item nor the drive holds, and when c1
/// adds Red again, c0 reads the item's lack of it as its own removal and sends it over the
/// add. The oracle names it rather than passing it silently.
#[test]
fn regression_a_new_item_keeping_a_base_tag_is_a_known_limit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFile { parent: 0, name: 6 }),
        act(0, Tag { node: 0, tag: 12 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(1, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
        act(1, Edit { file: 0 }),
        act(
            1,
            CreateFile {
                parent: 1,
                name: 11,
            },
        ),
        act(0, CreateFile { parent: 0, name: 8 }),
        act(1, Delete { node: 4 }),
        act(0, Edit { file: 8 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(
            0,
            Move {
                node: 5,
                parent: 0,
                name: 3,
            },
        ),
        act(1, Tag { node: 2, tag: 0 }),
        act(0, CreateFile { parent: 0, name: 8 }),
        Event::Plan { client: 1 },
        act(0, Delete { node: 5 }),
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 1 },
        Event::Step { client: 0 },
        act(1, Tag { node: 0, tag: 12 }),
    ];
    let limits = known_limits_of(2, &events);
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("NewItemKeepsBaseTag") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}

/// Seed f9ca4128, a known limit (the in-flight family, the user's call: not now): c1's plan
/// writes Red (c0's add) onto its folder and up; before it runs, c1's user adds Red and
/// removes it again. The plan's writes, made before, land over the removal, and Red is back.
/// The oracle names it rather than passing it silently.
#[test]
fn regression_a_removal_under_its_own_tag_write_is_a_known_limit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(0, Tag { node: 0, tag: 0 }),
        act(
            1,
            Move {
                node: 0,
                parent: 1,
                name: 1,
            },
        ),
        act(1, CreateJunk { parent: 14 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(1, Delete { node: 7 }),
        act(1, Tag { node: 0, tag: 4 }),
        Event::Step { client: 1 },
        act(1, Tag { node: 0, tag: 0 }),
        Event::Step { client: 1 },
    ];
    let mut sim = Sim::new(2);
    sim.guard = Some(1);
    sim.run(&events);
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    let limits = sim.tags_lost_to_known_limits();
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("OwnTagWriteInFlight") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}

/// Seeds d856d284 and fc58ff8f: a file here not hashed yet (a placement a crash left unread)
/// that the user tagged Red, whose node another writer put in a file, waits for its hash, then
/// goes into Recovered files with Red. It was adopted from the drive's state, the drive's tags
/// written over it (a known limit till what waits for a hash waited).
#[test]
fn regression_an_unhashed_orphan_keeps_its_tags() {
    use Action::*;
    use Event::*;
    for events in [
        vec![
            Act {
                client: 4,
                action: CreateFile {
                    parent: 10,
                    name: 1,
                },
            },
            Step { client: 4 },
            Act {
                client: 1,
                action: Edit { file: 6 },
            },
            Act {
                client: 3,
                action: DeleteOutright { pick: 8 },
            },
            Act {
                client: 1,
                action: CreateFile {
                    parent: 4,
                    name: 13,
                },
            },
            Act {
                client: 1,
                action: Move {
                    node: 5,
                    parent: 8,
                    name: 3,
                },
            },
            Act {
                client: 4,
                action: NewerFolder { name: 0 },
            },
            Plan { client: 2 },
            Step { client: 1 },
            Plan { client: 4 },
            Act {
                client: 0,
                action: DeleteStopped { pick: 11, done: 10 },
            },
            Act {
                client: 3,
                action: Edit { file: 5 },
            },
            Step { client: 0 },
            Plan { client: 4 },
            Step { client: 2 },
            Act {
                client: 4,
                action: Purge,
            },
            Confirm { client: 1 },
            Step { client: 2 },
            Step { client: 1 },
            Confirm { client: 4 },
            Step { client: 1 },
            Plan { client: 2 },
            Act {
                client: 0,
                action: EmptyItem { pick: 4 },
            },
            Step { client: 0 },
            Step { client: 3 },
            Act {
                client: 1,
                action: DeleteRoleFolder,
            },
            Act {
                client: 4,
                action: IntoFile { pick: 15, file: 13 },
            },
            Plan { client: 3 },
            Act {
                client: 0,
                action: CreateFile { parent: 1, name: 2 },
            },
            Confirm { client: 0 },
            Act {
                client: 0,
                action: Purge,
            },
            Step { client: 4 },
            Step { client: 2 },
            Step { client: 3 },
            Step { client: 2 },
            Act {
                client: 2,
                action: Tag { node: 1, tag: 0 },
            },
            Step { client: 2 },
            Plan { client: 4 },
            Act {
                client: 0,
                action: Tag { node: 7, tag: 13 },
            },
            Crash { client: 3 },
            Step { client: 0 },
            Confirm { client: 4 },
            Step { client: 4 },
            Step { client: 2 },
            Confirm { client: 2 },
            Act {
                client: 2,
                action: DeleteOutright { pick: 3 },
            },
            Crash { client: 2 },
            Step { client: 4 },
            Plan { client: 4 },
            Act {
                client: 1,
                action: DeleteOutright { pick: 8 },
            },
            Step { client: 2 },
            Act {
                client: 3,
                action: Tag { node: 12, tag: 12 },
            },
            Step { client: 1 },
            Act {
                client: 4,
                action: DeleteStopped { pick: 14, done: 10 },
            },
            Act {
                client: 1,
                action: DeleteRoleFolder,
            },
            Step { client: 4 },
            Act {
                client: 0,
                action: Purge,
            },
            Confirm { client: 1 },
            Plan { client: 4 },
            Act {
                client: 0,
                action: EmptyItem { pick: 5 },
            },
            Step { client: 4 },
            Act {
                client: 0,
                action: Move {
                    node: 7,
                    parent: 15,
                    name: 8,
                },
            },
            Crash { client: 1 },
            Step { client: 1 },
            Act {
                client: 1,
                action: IntoFile { pick: 2, file: 7 },
            },
            Act {
                client: 3,
                action: NewerFolder { name: 5 },
            },
            Act {
                client: 0,
                action: Move {
                    node: 3,
                    parent: 10,
                    name: 14,
                },
            },
            Act {
                client: 4,
                action: IntoFile { pick: 5, file: 6 },
            },
        ],
        vec![
            Crash { client: 0 },
            Act {
                client: 4,
                action: CreateFile { parent: 0, name: 3 },
            },
            Act {
                client: 4,
                action: CreateFile { parent: 0, name: 6 },
            },
            Crash { client: 0 },
            Act {
                client: 3,
                action: CreateFile { parent: 0, name: 5 },
            },
            Plan { client: 1 },
            Step { client: 4 },
            Plan { client: 3 },
            Step { client: 0 },
            Step { client: 0 },
            Crash { client: 0 },
            Step { client: 1 },
            Act {
                client: 0,
                action: IntoFile { pick: 15, file: 1 },
            },
            Act {
                client: 0,
                action: Tag { node: 2, tag: 4 },
            },
        ],
    ] {
        let limits = known_limits_narrow(&events);
        assert!(limits.is_empty(), "{limits:?}");
    }
}

/// Seed 353c4a5d, a known limit (the revert-to-base family, the user's call: not now): c1's
/// tag write lands, another device removes the folder meanwhile, and the folder is made again
/// without the tag; c1's user had removed Red and added it back, to what its base holds, so
/// nothing went up, and the drive's state comes down over it. The oracle names it.
#[test]
fn regression_a_tag_added_back_to_the_base_is_a_known_limit() {
    use Action::*;
    use Event::*;
    let limits = known_limits_of(
        2,
        &[
            Act {
                client: 1,
                action: CreateFolder { parent: 0, name: 0 },
            },
            Plan { client: 1 },
            Step { client: 1 },
            Act {
                client: 1,
                action: Meanwhile { op: 0 },
            },
            Act {
                client: 1,
                action: Tag { node: 0, tag: 4 },
            },
            Plan { client: 1 },
            Plan { client: 0 },
            Step { client: 1 },
            Act {
                client: 0,
                action: ListDeleted,
            },
            Act {
                client: 0,
                action: EmptyListed,
            },
            Act {
                client: 1,
                action: Tag { node: 0, tag: 4 },
            },
            Step { client: 0 },
            Act {
                client: 1,
                action: Tag { node: 0, tag: 12 },
            },
            Act {
                client: 0,
                action: CreateFolder { parent: 2, name: 0 },
            },
        ],
    );
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("AddBackToBase") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}

/// Seed 4c8098b5, the known limit of seed 0a9df7c6 (the user's call: not now) met through a
/// lost commit: client 1 forks its edited `a` into a conflict copy and downloads the drive's
/// version (Red removed) as a new item, and the download's commit fails: recovery settles its
/// record, and the base keeps Red, which neither the new item nor the drive holds. Client 0's
/// add of Red, made before, then reads on client 1 as its removal, and goes. The oracle names
/// the limit on recovery's path too, as on the commit's.
#[test]
fn regression_a_new_item_keeping_a_base_tag_through_recovery_is_a_known_limit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(
            1,
            CreateFile {
                parent: 12,
                name: 9,
            },
        ),
        act(0, Meanwhile { op: 11 }),
        act(1, Tag { node: 13, tag: 12 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(0, Tag { node: 7, tag: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(1, Edit { file: 14 }),
        Event::Plan { client: 0 },
        Event::Plan { client: 2 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(0, Tag { node: 9, tag: 4 }),
        Event::Step { client: 1 },
        Event::Plan { client: 1 },
        Event::Step { client: 2 },
        Event::Step { client: 1 },
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 1 },
    ];
    let limits = known_limits_of(3, &events);
    // That case alone.
    assert_eq!(limits.len(), 1, "{limits:?}");
    assert!(
        limits[0].starts_with("NewItemKeepsBaseTag") && limits[0].ends_with(" Red"),
        "{limits:?}"
    );
}
