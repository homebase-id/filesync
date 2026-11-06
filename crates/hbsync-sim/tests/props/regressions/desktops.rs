//! Scripted runs: each computer's Desktop, a folder of its own in Desktops (§12).

use hbsync_sim::{Action, Event, Sim};

/// Clients 3 and 4 are the Desktops of client 0's and client 1's computers.
fn desktops() -> Sim {
    Sim::new(3).with_desktop_devices()
}

fn act(client: usize, action: Action) -> Event {
    Event::Act { client, action }
}

/// Each computer's Desktop is a folder of its own in Desktops, named as the computer; each main
/// folder shows the other computer's, never its own; the third computer shows both.
#[test]
fn each_main_folder_shows_the_other_computers_desktops() {
    let mut sim = desktops();
    sim.run(&[
        act(3, Action::CreateFile { parent: 0, name: 0 }),
        act(4, Action::CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(30), "converges");
    sim.check().expect("check");
    let paths = sim.server_paths();
    for path in ["Desktops", "Desktops/c0", "Desktops/c0/a", "Desktops/c1/b"] {
        assert!(paths.iter().any(|p| p == path), "{path}: {paths:?}");
    }
    assert_eq!(sim.local_paths(3), ["a"]);
    assert_eq!(sim.local_paths(4), ["b"]);
    assert_eq!(
        sim.local_paths(0),
        ["Desktops", "Desktops/c1", "Desktops/c1/b"]
    );
    assert_eq!(
        sim.local_paths(1),
        ["Desktops", "Desktops/c0", "Desktops/c0/a"]
    );
    let all = [
        "Desktops",
        "Desktops/c0",
        "Desktops/c0/a",
        "Desktops/c1",
        "Desktops/c1/b",
    ];
    assert_eq!(sim.local_paths(2), all);
}

/// A Desktop deleted on another computer (its folder, or Desktops with every one in it) comes
/// back at its computer's next sync, with what was in it; nothing goes from that Desktop.
#[test]
fn a_desktop_deleted_elsewhere_comes_back() {
    for gone in ["Desktops/c0", "Desktops"] {
        let mut sim = desktops();
        sim.run(&[act(3, Action::CreateFile { parent: 0, name: 0 })]);
        assert!(sim.quiesce(30), "converges");
        let node = sim.item(2, gone).expect("on c2");
        let pick = sim.pick(2, node, false).expect("pick");
        sim.run(&[
            act(2, Action::Delete { node: pick }),
            Event::Plan { client: 2 },
        ]);
        assert!(sim.quiesce(30), "{gone}: converges");
        sim.check().expect("check");
        assert_eq!(sim.local_paths(3), ["a"], "{gone}");
        assert!(
            sim.server_paths().iter().any(|p| p == "Desktops/c0/a"),
            "{gone}"
        );
    }
}

/// A computer renamed: its Desktop's folder takes the name at its next sync.
#[test]
fn a_desktop_follows_its_computer_s_name() {
    let mut sim = desktops();
    sim.run(&[act(3, Action::CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(30), "converges");
    sim.run(&[act(3, Action::RenameComputer { name: 3 })]);
    assert!(sim.quiesce(30), "converges");
    sim.check().expect("check");
    let paths = sim.server_paths();
    assert!(paths.iter().any(|p| p == "Desktops/c.txt/a"), "{paths:?}");
    assert!(sim.local_paths(1).iter().any(|p| p == "Desktops/c.txt/a"));
}

/// Two computers syncing one Desktop share it: what each holds is on both, and neither main
/// folder shows it.
#[test]
fn a_shared_desktop_is_on_both_computers() {
    let mut sim = Sim::new(3).with_shared_desktop();
    sim.run(&[
        act(3, Action::CreateFile { parent: 0, name: 0 }),
        act(4, Action::CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(30), "converges");
    sim.check().expect("check");
    assert_eq!(sim.local_paths(3), ["a", "b"]);
    assert_eq!(sim.local_paths(4), ["a", "b"]);
    assert_eq!(sim.local_paths(0), ["Desktops"]);
    assert_eq!(sim.local_paths(1), ["Desktops"]);
}

/// Seed a7c289f1: c1 moves Desktops into a new folder, which another device deletes as it
/// lands; then the Desktop's computer gives its folder in Desktops its name (a write after that
/// delete). The owner's main folder, which leaves its own Desktop out, took Desktops as gone
/// with the folder (written before the delete) and removed it from the drive: what this folder
/// leaves out kept no folder above it from a delete. A node written after the delete keeps its
/// folders, whichever folder of a computer syncs it.
#[test]
fn regression_a_desktop_written_after_a_delete_keeps_desktops() {
    use hbsync_sim::Action::{CreateFolder, Meanwhile, Move};
    let events = [
        act(1, Meanwhile { op: 0 }),
        Event::Plan { client: 3 },
        act(1, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(
            1,
            Move {
                node: 1,
                parent: 3,
                name: 0,
            },
        ),
        act(2, Meanwhile { op: 0 }),
    ];
    if let Err(e) = crate::common::run_with(3, None, Sim::with_shared_desktop, &events) {
        panic!("{e}");
    }
}

/// Seed c7b8d080: c1 tags Desktops, another device deletes it as the tag lands, and c0 empties
/// Recently deleted: Desktops goes for good, and the Desktops' computers make it again, a new
/// folder of the same id. The delete settled the tag: it isn't owed on the new one.
#[test]
fn regression_desktops_made_again_owes_no_tag_of_the_old_one() {
    use hbsync_sim::Action::{CreateFile, EmptyListed, ListDeleted, Meanwhile, Tag};
    let events = [
        act(0, CreateFile { parent: 0, name: 0 }),
        act(1, Meanwhile { op: 0 }),
        Event::Plan { client: 4 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(1, Tag { node: 0, tag: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        act(0, ListDeleted),
        act(0, EmptyListed),
    ];
    if let Err(e) = crate::common::run_with(3, None, Sim::with_desktop_devices, &events) {
        panic!("{e}");
    }
}
