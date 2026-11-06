//! Regressions: executable bits (sync semantics §1).

use crate::common::*;
use hbsync_sim::{Action, Event, Sim};

/// Client 4 edits a file while a Mac (client 0) marks it executable; client 4's plan forks its
/// edit into a conflict copy and is to download the node again, but the Mac takes the mark off
/// before that download runs, so it goes stale. The drive is then as the node's base again,
/// with no item of the node here (the fork took it). The fork left the base as it was, and the
/// next plan read that as a delete here: the file went to Recently deleted, though no one
/// deleted it (on main too; an edit undone elsewhere did the same). The fork marks the base
/// wanted now (§5): the node's absence isn't a delete, and the drive's version comes down.
#[test]
fn regression_a_conflict_copy_whose_download_goes_stale_keeps_the_file() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateJunk { parent: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 4 },
        act(0, Chmod { file: 0 }),
        Event::Step { client: 4 },
        act(4, Edit { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 4 },
        Event::Step { client: 4 },
        Event::Plan { client: 3 },
        Event::Step { client: 3 },
        act(3, Chmod { file: 0 }),
    ];
    if let Err(e) = run_with(5, None, |sim| sim.with_narrow_check(), &events) {
        panic!("{e}");
    }
}

/// Seed b81d0596: a Linux client holds a launcher its user made executable, with no base for
/// it (crashes), and a tag to write on it, which waits for a base it founds against the drive's
/// version. The simulator read the item's bit raw (executable) against the drive's (not), so
/// it founded none and the write waited forever; the executor reads the item as its view
/// does, the bit it doesn't hold the drive's (§1).
#[test]
fn regression_a_launcher_s_tag_write_founds_its_base() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(4, CreateFile { parent: 0, name: 0 }),
        act(4, Launcher { node: 0, name: 0 }),
        act(4, Tag { node: 0, tag: 12 }),
        act(4, Chmod { file: 0 }),
        Event::Crash { client: 0 },
        Event::Plan { client: 4 },
        act(4, CreateFile { parent: 0, name: 0 }),
        Event::Crash { client: 4 },
        act(4, Delete { node: 13 }),
        act(4, Tag { node: 0, tag: 0 }),
    ];
    if let Err(e) = run(5, &events) {
        panic!("{e}");
    }
}

/// Found by `executable_bits_plan_alike_on_the_engines_path` (seed e7841a1a, a Desktop setup),
/// and on main as well with plain folders: c1 (Linux) makes `b/a/c.txt` executable, another
/// device removes `b` as c2's rename of it lands, and c3 (macOS) edits the file: c3's upload
/// (with `b` brought back) sends its own bit, false, over c1's, though no user changed it on
/// c3. Open, in the executable-bit flow (not the Desktops'): pinned for a fix of its own.
#[test]
#[ignore = "open: an edit's upload under a folder brought back sends an unchanged bit"]
fn regression_an_edit_under_a_folder_brought_back_keeps_the_drive_s_bit() {
    use hbsync_sim::Action::*;
    let mut sim = Sim::new(5);
    let root = sim.pick(4, hbsync_types::NodeId::ROOT, true).expect("root");
    sim.run(&[Event::Act {
        client: 4,
        action: CreateFolder {
            parent: root,
            name: 1,
        },
    }]);
    let a = sim.item(4, "b").expect("picked");
    let pa = sim.pick(4, a, true).expect("picked");
    sim.run(&[Event::Act {
        client: 4,
        action: CreateFolder {
            parent: pa,
            name: 0,
        },
    }]);
    let b = sim.item(4, "b/a").expect("picked");
    let pb = sim.pick(4, b, true).expect("picked");
    sim.run(&[Event::Act {
        client: 4,
        action: CreateFile {
            parent: pb,
            name: 3,
        },
    }]);
    assert!(sim.quiesce(30), "synced");
    let f = sim.item(1, "b/a/c.txt").expect("picked");
    let f1 = sim.file_pick(1, f).expect("picked");
    let f3 = sim.file_pick(3, f).expect("picked");
    let a2 = sim.pick(2, a, false).expect("picked");
    sim.run(&[
        Event::Act {
            client: 2,
            action: Meanwhile { op: 0 },
        },
        Event::Act {
            client: 1,
            action: Chmod { file: f1 },
        },
        Event::Act {
            client: 2,
            action: Launcher { node: a2, name: 0 },
        },
        Event::Plan { client: 3 },
        Event::Act {
            client: 3,
            action: Edit { file: f3 },
        },
    ]);
    assert!(sim.quiesce(30), "converges");
    sim.check().expect("check");
}
