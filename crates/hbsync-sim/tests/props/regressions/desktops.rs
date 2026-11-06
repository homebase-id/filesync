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
