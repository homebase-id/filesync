//! Read-only folders (sync semantics §13): the drive decides, nothing is written to it, and
//! what changed there that going back would lose moves out, kept.

use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

/// What an action's `parent` is to pick the top on client `c`.
fn top(sim: &Sim, c: usize) -> usize {
    sim.pick(c, NodeId::ROOT, true).expect("the top")
}

/// Client 0 syncs both ways; client 1 is read-only. Client 0 makes `a` and the folder `b`
/// holding `c.txt`; both settle.
fn shared() -> Sim {
    use Action::*;
    let mut sim = Sim::new(2).with_read_only(1);
    sim.run(&[Event::Act {
        client: 0,
        action: CreateFile { parent: 0, name: 0 },
    }]);
    sim.run(&[Event::Act {
        client: 0,
        action: CreateFolder { parent: 0, name: 1 },
    }]);
    let b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, b, true).expect("b's pick");
    sim.run(&[Event::Act {
        client: 0,
        action: CreateFile { parent, name: 3 },
    }]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt"]);
    sim
}

/// Lets every client settle, then checks the run (convergence, no loss, no write to the
/// drive from the read-only one).
fn settle(sim: &mut Sim) {
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// An edit there moves out, kept; the drive's version comes back.
#[test]
fn an_edit_in_a_read_only_folder_moves_out_and_the_drives_comes_back() {
    let mut sim = shared();
    let drive = sim.server_paths();
    let a = sim.item(1, "a").expect("a");
    let file = sim.file_pick(1, a).expect("a's pick");
    sim.run(&[Event::Act {
        client: 1,
        action: Action::Edit { file },
    }]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), drive);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt"]);
    assert_eq!(sim.moved_out(1), ["a"]);
}

/// A new folder there moves out with what is in it, as one; a new file too. (Client 1 ignores
/// `d`: names other than that.)
#[test]
fn new_items_in_a_read_only_folder_move_out() {
    use Action::*;
    let mut sim = shared();
    let drive = sim.server_paths();
    let parent = top(&sim, 1);
    sim.run(&[Event::Act {
        client: 1,
        action: CreateFolder { parent, name: 3 },
    }]);
    let d = sim.item(1, "c.txt").expect("c.txt");
    let (parent, root) = (sim.pick(1, d, true).expect("c.txt's pick"), top(&sim, 1));
    sim.run(&[
        Event::Act {
            client: 1,
            action: CreateFile { parent, name: 0 },
        },
        Event::Act {
            client: 1,
            action: CreateFile {
                parent: root,
                name: 6,
            },
        },
    ]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), drive);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt"]);
    let mut out = sim.moved_out(1);
    out.sort();
    assert_eq!(out.len(), 2);
    assert_eq!(out[1], "c.txt");
}

/// A rename, a move and a delete there are undone: nothing moves out, nothing is lost.
#[test]
fn moves_and_deletes_in_a_read_only_folder_are_undone() {
    use Action::*;
    let mut sim = shared();
    let drive = sim.server_paths();
    let a = sim.item(1, "a").expect("a");
    let c = sim.item(1, "b/c.txt").expect("c.txt");
    let (node, root) = (sim.pick(1, a, false).expect("a's pick"), top(&sim, 1));
    sim.run(&[Event::Act {
        client: 1,
        action: Move {
            node,
            parent: root,
            name: 3,
        },
    }]);
    let node = sim.pick(1, c, false).expect("c's pick");
    sim.run(&[Event::Act {
        client: 1,
        action: Delete { node },
    }]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), drive);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt"]);
    assert!(sim.moved_out(1).is_empty());
}

/// A file edited there whose folder the drive's owner deletes meanwhile: the edit moves out,
/// the folder goes.
#[test]
fn an_edit_in_a_folder_deleted_elsewhere_moves_out() {
    let mut sim = shared();
    let c = sim.item(1, "b/c.txt").expect("c.txt");
    let file = sim.file_pick(1, c).expect("c's pick");
    let b = sim.item(0, "b").expect("b");
    let node = sim.pick(0, b, false).expect("b's pick");
    sim.run(&[
        Event::Act {
            client: 1,
            action: Action::Edit { file },
        },
        Event::Act {
            client: 0,
            action: Action::Delete { node },
        },
    ]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a"]);
    assert_eq!(sim.local_paths(1), ["a"]);
    assert_eq!(sim.moved_out(1), ["c.txt"]);
}

/// An item ignored there since it synced (§10) stays on the drive: kept there only, the
/// drive's one isn't synced there any more.
#[test]
fn an_item_ignored_in_a_read_only_folder_stays_on_the_drive() {
    use Action::*;
    let mut sim = shared();
    // Client 1 (odd) starts out ignoring `d`: it syncs it once it doesn't.
    sim.run(&[Event::Act {
        client: 1,
        action: ToggleIgnore,
    }]);
    let parent = top(&sim, 0);
    sim.run(&[Event::Act {
        client: 0,
        action: CreateFile { parent, name: 5 },
    }]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt", "d"]);
    let drive = sim.server_paths();
    sim.run(&[Event::Act {
        client: 1,
        action: ToggleIgnore,
    }]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), drive);
    assert_eq!(sim.local_paths(1), ["a", "b", "b/c.txt", "d"]);
    assert!(sim.moved_out(1).is_empty());
}
