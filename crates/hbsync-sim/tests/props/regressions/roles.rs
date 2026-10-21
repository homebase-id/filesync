//! Regressions: role folders and the main folder around them.

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};

/// c0 detaches its older copy of the role folder (it made a file in it), c2 renames the
/// detached folder to `d`, which c0 then ignores: c0 detaches it again. That second id was the
/// role folder's own (the detach id undid itself), so c0 let go of a file it had never
/// uploaded.
#[test]
fn regression_detached_twice_back_into_the_role_folder() {
    use hbsync_sim::Action::{CreateFile, Move, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFile { parent: 2, name: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        act(
            2,
            Move {
                node: 0,
                parent: 7,
                name: 5,
            },
        ),
        act(0, ToggleIgnore),
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 2 },
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Step { client: 2 },
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_folder_held, &events) {
        panic!("{e}");
    }
}

/// c0 deletes a folder while c1 puts a file in it: the file is left without its folder on the
/// drive. Every folder looks at it (its place isn't known), and the role folder of c0's
/// computer repaired it as the main folder does, moving it to its own top: into the role
/// folder. Only what reaches its top is a role folder's; the main folder repairs the rest.
#[test]
fn regression_a_role_folder_leaves_a_lost_file_to_the_main_folder() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete};
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(0, Delete { node: 0 }),
        Event::Step { client: 1 },
        act(1, CreateFile { parent: 0, name: 0 }),
        act(1, CreateFolder { parent: 6, name: 0 }),
        Event::Plan { client: 0 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 0 },
        Event::Plan { client: 3 },
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Step { client: 3 },
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_device, &events) {
        panic!("{e}");
    }
}

/// In the role folder, the user moves a folder below what was its child: the plan's moves
/// make a cycle on the drive for a moment, which reaches neither the role folder nor the top.
/// That is no write outside the role folder.
#[test]
fn regression_a_role_folder_untangles_a_cycle_of_its_own() {
    use hbsync_sim::Action::{CreateFolder, Move};
    let act = |action| Event::Act { client: 3, action };
    let events = [
        act(CreateFolder { parent: 0, name: 3 }),
        act(CreateFolder { parent: 8, name: 0 }),
        Event::Plan { client: 3 },
        Event::Step { client: 3 },
        Event::Step { client: 3 },
        Event::Step { client: 3 },
        Event::Step { client: 3 },
        act(Move {
            node: 4,
            parent: 2,
            name: 0,
        }),
        act(CreateFolder { parent: 4, name: 0 }),
        act(Move {
            node: 4,
            parent: 2,
            name: 0,
        }),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_device, &events) {
        panic!("{e}");
    }
}

/// A file goes up into Documents as the phone deletes Documents (the upload was planned
/// before): it sits in a deleted role folder. The main folders leave it alone; the device
/// syncing Documents brings the folder back, the file in it.
#[test]
fn regression_a_file_put_into_a_deleted_role_folder_stays_its() {
    use hbsync_sim::Action::{CreateFile, DeleteRoleFolder, Purge};
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(4, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 4 },
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, DeleteRoleFolder),
        Event::Step { client: 4 },
        Event::Plan { client: 0 },
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// A computer that synced the role folder writes a file into a folder in it after the phone
/// deleted the role folder: written after the removal, the file brings its folders back
/// (§6.3), in that computer's part of the drive. The main folder, which doesn't sync the role
/// folder, leaves it alone: it never brings back what is outside its part.
#[test]
fn regression_a_file_written_into_a_removed_role_folder_stays_out_of_main() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(4, CreateJunk { parent: 0 }),
        act(4, CreateFolder { parent: 0, name: 6 }),
        act(
            4,
            Move {
                node: 6,
                parent: 2,
                name: 0,
            },
        ),
        plan(4),
        step(4),
        act(0, DeleteRoleFolder),
        step(4),
        plan(1),
        step(1),
        step(1),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// One computer stops syncing a folder in the role folder and removes it; another writes a file
/// into it after that. The file stays and brings the folder back on the computer syncing the
/// role folder: through the folder in Recently deleted, the file is in its part of the drive.
#[test]
fn regression_a_file_written_into_a_removed_folder_of_the_role_folder_brings_it_back() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(4, CreateFolder { parent: 0, name: 0 }),
        plan(4),
        act(4, ToggleIgnore),
        act(0, Phone { op: 0, pick: 2 }),
        step(4),
        act(
            4,
            Move {
                node: 0,
                parent: 7,
                name: 5,
            },
        ),
        act(4, CreateFile { parent: 5, name: 1 }),
        plan(4),
        step(4),
        step(4),
        plan(3),
        step(3),
        act(
            3,
            CreateFile {
                parent: 14,
                name: 0,
            },
        ),
        plan(3),
        step(4),
        act(0, Tag { node: 0, tag: 0 }),
        step(3),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// The phone deletes Documents with its file, the user restores the file alone (at the drive's
/// top, its folder gone) and deletes it again there. The device syncing Documents brings the
/// folder back with what was deleted with it: not that file, deleted on its own since.
#[test]
fn regression_a_role_folder_comes_back_without_what_was_restored_from_it() {
    use hbsync_sim::Action::{Delete, DeleteRoleFolder, Restore};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(0, DeleteRoleFolder),
        act(0, Restore { node: 7 }),
        plan(0),
        step(0),
        act(0, Delete { node: 0 }),
        plan(0),
        step(0),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_device, &events) {
        panic!("{e}");
    }
}
