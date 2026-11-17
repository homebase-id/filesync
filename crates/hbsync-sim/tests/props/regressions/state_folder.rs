//! A state folder (`.hbsync`) below the top of a sync folder, another sync folder's moved or
//! copied in (sync semantics §10): left out at any depth, with all in it.

use hbsync_sim::{Action, Sim};

use super::{act, settle, top};

/// Client 0 has the folder `a` holding `b` and a state folder; client 1 has synced.
fn holding_one() -> Sim {
    use Action::*;
    let mut sim = Sim::new(2).without_ignore_rules().with_narrow_check();
    sim.run(&[act(0, CreateFolder { parent: 0, name: 0 })]);
    let a = sim.item(0, "a").expect("the folder");
    let parent = sim.pick(0, a, true).expect("its pick");
    sim.run(&[
        act(0, CreateFile { parent, name: 1 }),
        act(0, CreateStateFolder { parent }),
    ]);
    settle(&mut sim);
    sim
}

/// Nothing of it reaches the drive or another device, at the top or below it.
#[test]
fn a_state_folder_stays_on_its_device_at_any_depth() {
    let mut sim = holding_one();
    let here = ["a", "a/.hbsync", "a/.hbsync/x", "a/b"];
    assert_eq!(sim.local_paths(0), here);
    assert_eq!(sim.server_paths(), ["a", "a/b"]);
    assert_eq!(sim.local_paths(1), ["a", "a/b"]);

    let parent = top(&sim, 1);
    sim.run(&[act(1, Action::CreateStateFolder { parent })]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "a/b"]);
    assert_eq!(sim.local_paths(0), here);
}

/// It is no junk: the folder it is in, deleted on another device, stays for it where it is,
/// without what was synced, and is a new folder from then on.
#[test]
fn a_state_folder_keeps_its_folder_from_a_delete() {
    let mut sim = holding_one();
    let a = sim.item(1, "a").expect("the folder");
    let node = sim.pick(1, a, false).expect("its pick");
    sim.run(&[act(1, Action::Delete { node })]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(0), ["a", "a/.hbsync", "a/.hbsync/x"]);
    assert_ne!(sim.item(0, "a"), Some(a), "released");
    assert_eq!(sim.server_paths(), ["a"]);
    assert_eq!(sim.local_paths(1), ["a"]);
}
