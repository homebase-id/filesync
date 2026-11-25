//! A member that writes to what is shared with it for editing (sync semantics §13, "What may
//! be changed is told per item"): its change of such an item, where the drive has it, reaches
//! the drive; nothing else of its folder does.

use hbsync_sim::{Action, Event, Sim};

use super::{act, settle, share, top};

/// The owner turns the share of `path` to editing (or back to reading).
fn turn(sim: &mut Sim, path: &str) {
    let node = sim.item(0, path).expect("on the owner's device");
    let pick = sim.share_pick(node).expect("on the drive");
    sim.run(&[act(0, Action::Editable { pick, below: false })]);
}

/// Client 0, the owner, has `a` and the folder `b` holding another `a`; client 2 is a member
/// that writes, of each of `shared` for editing. Each plan is checked against the engine's own
/// view.
fn owned(shared: &[&str]) -> Sim {
    use Action::*;
    let mut sim = Sim::new(3).with_writing_member(2).with_narrow_check();
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 1 }),
    ]);
    let b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, b, true).expect("b's pick");
    sim.run(&[act(0, CreateFile { parent, name: 0 })]);
    settle(&mut sim);
    for path in shared {
        share(&mut sim, path);
        turn(&mut sim, path);
    }
    settle(&mut sim);
    sim
}

/// The member edits its file at `path`.
fn edit(sim: &mut Sim, path: &str) {
    let node = sim.item(2, path).expect("on the member's device");
    let file = sim.file_pick(2, node).expect("a file");
    sim.run(&[act(2, Action::Edit { file })]);
}

/// The member moves its item at `path` into `folder` (`""`: the top) as the name `name` picks.
fn rename(sim: &mut Sim, path: &str, folder: &str, name: usize) {
    let node = sim.item(2, path).expect("on the member's device");
    let node = sim.pick(2, node, false).expect("its pick");
    let parent = match folder {
        "" => top(sim, 2),
        folder => {
            let folder = sim.item(2, folder).expect("the folder");
            sim.pick(2, folder, true).expect("the folder's pick")
        }
    };
    sim.run(&[act(2, Action::Move { node, parent, name })]);
}

/// An edit and a rename of a file shared for editing reach the drive, and the owner's
/// devices: nothing moves out.
#[test]
fn an_edit_and_a_rename_reach_the_drive() {
    let mut sim = owned(&["a"]);
    assert_eq!(sim.local_paths(2), ["a"]);
    edit(&mut sim, "a");
    settle(&mut sim);
    assert!(sim.moved_out(2).is_empty(), "{:?}", sim.moved_out(2));
    rename(&mut sim, "a", "", 3);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["b", "b/a", "c.txt"]);
    assert_eq!(sim.local_paths(0), ["b", "b/a", "c.txt"]);
    assert_eq!(sim.local_paths(2), ["c.txt"]);
    assert!(sim.moved_out(2).is_empty());
}

/// A file shared without its folder is at the member's top: renamed and edited there, it
/// stays in its folder on the drive.
#[test]
fn a_file_at_the_top_stays_in_its_folder_on_the_drive() {
    let mut sim = owned(&["b/a"]);
    assert_eq!(sim.local_paths(2), ["a"]);
    edit(&mut sim, "a");
    rename(&mut sim, "a", "", 3);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/c.txt"]);
    assert_eq!(sim.local_paths(2), ["c.txt"]);
    assert!(sim.moved_out(2).is_empty());
}

/// A move to another folder is undone here: it doesn't reach the drive. An edit made with
/// the move still does. A delete here does: the file goes to the drive's Recently deleted,
/// and off the owner's devices.
#[test]
fn a_move_is_undone_and_a_delete_is_a_removal() {
    let mut sim = owned(&["a", "b", "b/a"]);
    let drive = sim.server_paths();
    edit(&mut sim, "a");
    rename(&mut sim, "a", "b", 3);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), drive);
    assert_eq!(sim.local_paths(2), drive);
    assert!(sim.moved_out(2).is_empty(), "the edit went up");
    let node = sim.item(2, "b/a").expect("b/a");
    let node = sim.pick(2, node, false).expect("its pick");
    sim.run(&[act(2, Action::Delete { node })]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b"]);
    assert_eq!(sim.local_paths(0), ["a", "b"]);
    assert_eq!(sim.local_paths(2), ["a", "b"]);
}

/// A file the member may only read, edited here: the edit moves out, and the file is gone
/// from the folder until the drive's version is back. Its share turned to editing in
/// between, that is no delete of the member's: nothing is removed on the drive, and the
/// file comes back.
#[test]
fn what_moved_out_is_not_a_delete_once_it_may_be_changed() {
    let mut sim = owned(&[]);
    share(&mut sim, "a");
    settle(&mut sim);
    edit(&mut sim, "a");
    // The move out, and no more of the member's sync yet.
    sim.run(&[Event::Plan { client: 2 }, Event::Step { client: 2 }]);
    assert_eq!(sim.moved_out(2), ["a"]);
    assert!(sim.local_paths(2).is_empty());
    turn(&mut sim, "a");
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a"]);
    assert_eq!(sim.local_paths(2), ["a"]);
}

/// A share turned back to reading after the member planned its write: the write doesn't run
/// (the plan's operations are stepped as planned, before any new plan), and the edit moves
/// out at the next plan.
#[test]
fn a_share_turned_back_before_the_write_runs_stops_it() {
    let mut sim = owned(&["a"]);
    edit(&mut sim, "a");
    sim.run(&[Event::Plan { client: 2 }]);
    assert_eq!(sim.planned(2), ["RemoteUpdate"]);
    turn(&mut sim, "a");
    sim.run(&[Event::Step { client: 2 }]);
    assert!(sim.planned(2).is_empty(), "{:?}", sim.planned(2));
    // (A write that ran all the same is a breach, which `settle` checks.)
    settle(&mut sim);
    assert_eq!(sim.moved_out(2), ["a"]);
    assert_eq!(sim.local_paths(2), ["a"]);
}

/// A rename onto the name of a file the owner just shared isn't sent: the drive never holds
/// two of one name by the member's doing. The rename is undone, and both files are here.
#[test]
fn a_rename_onto_a_taken_name_is_undone() {
    let mut sim = owned(&["a"]);
    // The owner makes `c.txt` and shares it; the member hasn't read it yet.
    let parent = top(&sim, 0);
    sim.run(&[act(0, Action::CreateFile { parent, name: 3 })]);
    for _ in 0..4 {
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
    }
    share(&mut sim, "c.txt");
    rename(&mut sim, "a", "", 3);
    assert_eq!(sim.local_paths(2), ["c.txt"]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a", "c.txt"]);
    assert_eq!(sim.local_paths(2), ["a", "c.txt"]);
}

/// The member makes its file named by `name` in `folder` (`""`: the top).
fn make(sim: &mut Sim, folder: &str, name: usize) {
    let parent = match folder {
        "" => top(sim, 2),
        folder => {
            let folder = sim.item(2, folder).expect("the folder");
            sim.pick(2, folder, true).expect("the folder's pick")
        }
    };
    sim.run(&[act(2, Action::CreateFile { parent, name })]);
}

/// What the member makes in a folder shared for editing is added to the drive, and is the
/// member's to change from then on; what it makes at the top, or in a folder shared for
/// reading, moves out.
#[test]
fn what_is_made_in_a_folder_shared_for_editing_reaches_the_drive() {
    let mut sim = owned(&["b"]);
    assert_eq!(sim.local_paths(2), ["b"]);
    make(&mut sim, "b", 3);
    make(&mut sim, "", 5);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a", "b/c.txt"]);
    assert_eq!(sim.local_paths(0), ["a", "b", "b/a", "b/c.txt"]);
    assert_eq!(sim.local_paths(2), ["b", "b/c.txt"]);
    assert_eq!(sim.moved_out(2), ["d"]);
    // Shared as its folder is: an edit of it goes up.
    edit(&mut sim, "b/c.txt");
    settle(&mut sim);
    assert_eq!(sim.moved_out(2), ["d"]);
    // The folder turned back to reading: what is made in it now moves out.
    turn(&mut sim, "b");
    make(&mut sim, "b", 5);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a", "b/c.txt"]);
    assert_eq!(sim.moved_out(2), ["d", "d"]);
}

/// A file moves between two folders shared for editing; into a folder shared for reading,
/// the move is undone.
#[test]
fn a_move_between_folders_shared_for_editing_reaches_the_drive() {
    let mut sim = owned(&[]);
    // Two more folders of the owner's: `d`, and one named `c.txt`.
    for name in [5, 3] {
        let parent = top(&sim, 0);
        sim.run(&[act(0, Action::CreateFolder { parent, name })]);
    }
    settle(&mut sim);
    for path in ["b", "b/a", "d"] {
        share(&mut sim, path);
        turn(&mut sim, path);
    }
    share(&mut sim, "c.txt");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["b", "b/a", "c.txt", "d"]);
    rename(&mut sim, "b/a", "d", 1);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "c.txt", "d", "d/b"]);
    assert_eq!(sim.local_paths(2), ["b", "c.txt", "d", "d/b"]);
    rename(&mut sim, "d/b", "c.txt", 1);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "c.txt", "d", "d/b"]);
    assert_eq!(sim.local_paths(2), ["b", "c.txt", "d", "d/b"]);
    assert!(sim.moved_out(2).is_empty());
}

/// A file shared for editing that its owner moves into a folder of theirs the member isn't
/// shared: for the member it is at its top now, where another of its name is, so it is left
/// out there and stays where the member has it. Edited on both sides besides, its conflict
/// copy isn't made while it is left out: the member plans nothing for it (it planned the
/// copy up at every sync, never made), keeps its edit where it is, and nothing moves out.
#[test]
fn a_file_left_out_for_its_name_makes_no_conflict_copy_meanwhile() {
    let mut sim = owned(&["a", "b", "b/a"]);
    let theirs = sim.item(0, "b/a").expect("b/a");
    let b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, b, true).expect("b's pick");
    sim.run(&[act(0, Action::CreateFolder { parent, name: 5 })]);
    let d = sim.item(0, "b/d").expect("b/d");
    let (node, parent) = (
        sim.pick(0, theirs, false).expect("its pick"),
        sim.pick(0, d, true).expect("b/d's pick"),
    );
    let name = 0;
    sim.run(&[act(0, Action::Move { node, parent, name })]);
    let file = sim.file_pick(0, theirs).expect("a file");
    sim.run(&[act(0, Action::Edit { file })]);
    edit(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/d", "b/d/a"]);
    assert_eq!(sim.local_paths(2), ["a", "b", "b/a"]);
    assert!(sim.planned(2).is_empty(), "{:?}", sim.planned(2));
    assert!(sim.moved_out(2).is_empty(), "{:?}", sim.moved_out(2));
    assert_ne!(sim.local_hash(2, theirs), sim.drive_hash(theirs));
    assert_eq!(sim.local_hash(0, theirs), sim.drive_hash(theirs));
}
