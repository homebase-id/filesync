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

/// The member's sync: planned and run till its plan is done (what waits is left for the
/// next).
fn sync_member(sim: &mut Sim) {
    sim.run(&[Event::Plan { client: 2 }]);
    while !sim.planned(2).is_empty() {
        sim.run(&[Event::Step { client: 2 }]);
    }
}

/// The sharer's server takes every write the member's server holds on its way, in order.
fn deliver_all(sim: &mut Sim) {
    while sim.writes_on_their_way() > 0 {
        sim.run(&[Event::Deliver { pick: 0 }]);
    }
}

/// An edit on its way (the member's server holds it) waits: asked again, it is on its way
/// still, nothing sent twice. Delivered, it lands once, and the member settles on it:
/// nothing moves out.
#[test]
fn an_edit_on_its_way_lands_once() {
    let mut sim = owned(&["a"]);
    sim.hold_member_writes(true);
    let a = sim.item(2, "a").expect("a");
    let before = sim.drive_hash(a);
    edit(&mut sim, "a");
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1);
    assert_eq!(sim.drive_hash(a), before, "not on the drive yet");
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1, "asked again, not sent twice");
    deliver_all(&mut sim);
    assert_eq!(sim.drive_hash(a), sim.local_hash(2, a), "landed");
    sync_member(&mut sim);
    settle(&mut sim);
    assert_eq!(sim.drive_hash(a), sim.local_hash(2, a));
    assert_eq!(sim.local_hash(0, a), sim.local_hash(2, a));
    assert!(sim.moved_out(2).is_empty(), "{:?}", sim.moved_out(2));
    assert_eq!(sim.local_paths(2), ["a"]);
}

/// An edit on its way, and the sharer edits the file meanwhile: delivered, the member's
/// write isn't taken (the file is at another version), which is no breach. The member ends
/// with the sharer's version, its own edit moved out (the file is at its top, where it may
/// add nothing: no conflict copy there); the oracle checks the edit's bytes are kept.
#[test]
fn an_edit_on_its_way_over_the_sharers_edit_is_not_taken() {
    let mut sim = owned(&["a"]);
    sim.hold_member_writes(true);
    let a = sim.item(2, "a").expect("a");
    edit(&mut sim, "a");
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1);
    let file = sim.file_pick(0, a).expect("on the owner's device");
    sim.run(&[act(0, Action::Edit { file })]);
    for _ in 0..4 {
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
    }
    let theirs = sim.local_hash(0, a);
    assert_eq!(sim.drive_hash(a), theirs);
    deliver_all(&mut sim);
    assert_eq!(sim.drive_hash(a), theirs, "not taken");
    settle(&mut sim);
    assert_eq!(sim.drive_hash(a), theirs);
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.local_hash(2, a), theirs);
    assert_eq!(sim.moved_out(2), ["a"], "the member's edit kept");
}

/// A new folder with a file in it, both the member's: the folder's write is on its way, and
/// the file's waits for it unsent (its folder is on its way to the drive). Delivered, the
/// folder is answered as written, the file goes, and both land.
#[test]
fn a_file_in_a_new_folder_on_its_way_waits_for_it() {
    let mut sim = owned(&["b"]);
    sim.hold_member_writes(true);
    let b = sim.item(2, "b").expect("b");
    let parent = sim.pick(2, b, true).expect("b's pick");
    sim.run(&[act(2, Action::CreateFolder { parent, name: 5 })]);
    make(&mut sim, "b/d", 3);
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1, "the folder's alone");
    deliver_all(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a", "b/d"]);
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1, "the file's, sent now");
    deliver_all(&mut sim);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b", "b/a", "b/d", "b/d/c.txt"]);
    assert_eq!(sim.local_paths(0), ["a", "b", "b/a", "b/d", "b/d/c.txt"]);
    assert_eq!(sim.local_paths(2), ["b", "b/d", "b/d/c.txt"]);
    assert!(sim.moved_out(2).is_empty());
}

/// The member's process dies while its edit is on its way: started again, it doesn't
/// remember the send, and asks again: the edit waits behind the one held, unsent. Delivered,
/// the edit has landed once; the member, refused on the version it planned over, settles on
/// the drive's (its own edit): nothing is copied or moved out.
#[test]
fn an_edit_on_its_way_through_a_crash_lands_once() {
    let mut sim = owned(&["a"]);
    sim.hold_member_writes(true);
    let a = sim.item(2, "a").expect("a");
    edit(&mut sim, "a");
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1);
    sim.run(&[Event::Crash { client: 2 }]);
    sync_member(&mut sim);
    assert_eq!(
        sim.writes_on_their_way(),
        1,
        "waits behind the one held, unsent"
    );
    deliver_all(&mut sim);
    assert_eq!(sim.drive_hash(a), sim.local_hash(2, a));
    settle(&mut sim);
    assert_eq!(sim.drive_hash(a), sim.local_hash(2, a));
    assert_eq!(sim.local_paths(2), ["a"]);
    assert!(sim.moved_out(2).is_empty());
}

/// The member's delete on its way, and the sharer edits the file meanwhile: delivered, the
/// removal isn't taken; the edit wins, nothing goes to Recently deleted, and the member has
/// the sharer's version back.
#[test]
fn a_removal_on_its_way_loses_to_the_sharers_edit() {
    let mut sim = owned(&["a"]);
    sim.hold_member_writes(true);
    let a = sim.item(2, "a").expect("a");
    let node = sim.pick(2, a, false).expect("its pick");
    sim.run(&[act(2, Action::Delete { node })]);
    sync_member(&mut sim);
    assert_eq!(sim.writes_on_their_way(), 1);
    let file = sim.file_pick(0, a).expect("on the owner's device");
    sim.run(&[act(0, Action::Edit { file })]);
    for _ in 0..4 {
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
    }
    let theirs = sim.local_hash(0, a);
    deliver_all(&mut sim);
    assert!(!sim.is_removed(a), "not taken");
    settle(&mut sim);
    assert!(!sim.is_removed(a));
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.local_hash(2, a), theirs);
}

/// The member deletes a folder and the file in it, both its to change, and its folder's
/// removal is sent; the owner withdraws the folder's share before the file's removal runs.
/// The member reads the file at the top from then on, where the drive has it in the folder
/// still: no move there, and the removal the member's user asked for stands (the oracle read
/// the file's place from the member's view, and took the removal for one no one made).
#[test]
fn a_delete_stands_though_the_files_folder_is_no_longer_shared() {
    let mut sim = crate::common::with_all_shared_to_edit(Sim::new(3));
    let folder = sim.item(2, "a").expect("the folder");
    let node = sim.pick(2, folder, false).expect("its pick");
    sim.run(&[
        act(2, Action::Delete { node }),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
    ]);
    assert_eq!(sim.planned(2), ["RemoteDelete"], "the file's removal waits");
    let pick = sim.share_pick(folder).expect("on the drive");
    sim.run(&[
        act(0, Action::Share { pick, below: false }),
        Event::Step { client: 2 },
    ]);
    settle(&mut sim);
    assert!(sim.server_paths().iter().all(|p| !p.starts_with('a')));
}

/// The other way round: the member reads a file at the top (shared without its folder) and
/// deletes it; the owner shares the folder before the removal runs. The member reads the
/// file in the folder from then on, and the drive had it there all along: the removal stands
/// (the oracle had recorded the delete at the place the member read the file, the top).
#[test]
fn a_delete_stands_though_the_files_folder_is_shared_since() {
    let mut sim = owned(&["b/a"]);
    assert_eq!(sim.local_paths(2), ["a"]);
    let file = sim.item(2, "a").expect("the file");
    let node = sim.pick(2, file, false).expect("its pick");
    sim.run(&[act(2, Action::Delete { node }), Event::Plan { client: 2 }]);
    assert_eq!(sim.planned(2), ["RemoteDelete"]);
    share(&mut sim, "b");
    sim.run(&[Event::Step { client: 2 }]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "b"]);
    assert_eq!(sim.local_paths(2), ["b"]);
}

/// The member makes a folder in a folder shared for editing and moves a file of that folder
/// into it, in one sync: the new folder is added and the move goes up (the new folder is
/// shared as the one it is in, as the move runs). The plan's check took the move for one to
/// a folder the member may not add to: the new folder isn't on the drive as it plans.
#[test]
fn a_move_into_a_folder_just_made_in_one_shared_for_editing_reaches_the_drive() {
    let mut sim = owned(&["b", "b/a"]);
    let b = sim.item(2, "b").expect("b");
    let parent = sim.pick(2, b, true).expect("b's pick");
    sim.run(&[act(2, Action::CreateFolder { parent, name: 3 })]);
    rename(&mut sim, "b/a", "b/c.txt", 0);
    settle(&mut sim);
    let all = ["a", "b", "b/c.txt", "b/c.txt/a"];
    assert_eq!(sim.server_paths(), all);
    assert_eq!(sim.local_paths(0), all);
    assert_eq!(sim.local_paths(2), ["b", "b/c.txt", "b/c.txt/a"]);
    assert!(sim.moved_out(2).is_empty());
}

/// The owner withdraws the share of the folder `b`; the file `a` in it stays shared for
/// editing, so the member reads it at the top from then on, where the other `a` holds its
/// name: it is left out, and stays in `b` here. The member edited it meanwhile. The edit
/// doesn't go up, and `b`'s delete here doesn't take it: it moves out first. (It went with
/// the folder: found by the simulator, the owner's folder shared for editing from the start.)
#[test]
fn an_edit_of_a_file_left_out_moves_out_before_its_folder_goes_here() {
    let mut sim = owned(&["a", "b", "b/a"]);
    assert_eq!(sim.local_paths(2), ["a", "b", "b/a"]);
    share(&mut sim, "b");
    edit(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.moved_out(2).len(), 1, "{:?}", sim.moved_out(2));
    assert_eq!(sim.server_paths(), ["a", "b", "b/a"]);
}

/// The guard on: the member's delete of all it may change is held, nothing removed on the
/// drive. Put back, it is undone here without a write there (the items come down again).
/// The owner's removal of it all is held on the member too, and is not the member's to put
/// back (a restore there): it stays held till confirmed.
#[test]
fn held_deletes_of_the_members_are_put_back_and_the_owners_removals_are_not() {
    let mut sim = owned(&["a", "b", "b/a"]);
    sim.guard = Some(1);
    let drive = sim.server_paths();
    for path in ["a", "b"] {
        let node = sim.item(2, path).expect(path);
        let node = sim.pick(2, node, false).expect("its pick");
        sim.run(&[act(2, Action::Delete { node })]);
    }
    sim.run(&[Event::Plan { client: 2 }]);
    assert!(!sim.held(2).is_empty(), "the member's deletes are held");
    assert!(sim.local_paths(2).is_empty());
    sim.run(&[Event::PutBack { client: 2 }]);
    settle(&mut sim);
    assert!(sim.held(2).is_empty(), "{:?}", sim.held(2));
    assert_eq!(sim.server_paths(), drive, "nothing removed there");
    assert_eq!(sim.local_paths(2), drive, "back here");
    assert!(sim.moved_out(2).is_empty());

    // The owner removes it all (confirmed there).
    for path in ["a", "b"] {
        let node = sim.item(0, path).expect(path);
        let node = sim.pick(0, node, false).expect("its pick");
        sim.run(&[act(0, Action::Delete { node })]);
    }
    for _ in 0..4 {
        sim.run(&[
            Event::Plan { client: 0 },
            Event::Confirm { client: 0 },
            Event::Plan { client: 0 },
        ]);
        while !sim.planned(0).is_empty() {
            sim.run(&[Event::Step { client: 0 }]);
        }
    }
    assert!(sim.server_paths().is_empty(), "{:?}", sim.server_paths());
    sim.run(&[Event::Plan { client: 2 }]);
    assert!(!sim.held(2).is_empty(), "the owner's removal is held here");
    sim.run(&[Event::PutBack { client: 2 }, Event::Plan { client: 2 }]);
    assert!(!sim.held(2).is_empty(), "not the member's to put back");
    assert_eq!(sim.local_paths(2), drive, "held: still here");
    assert!(sim.server_paths().is_empty(), "nothing restored there");
    sim.run(&[Event::Confirm { client: 2 }]);
    settle(&mut sim);
    assert!(sim.local_paths(2).is_empty(), "{:?}", sim.local_paths(2));
}
