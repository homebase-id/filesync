//! Regressions: executable bits (sync semantics §1).

use super::{act, settle};
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
/// (with `b` brought back) sent its own bit, false, over c1's, though no user changed it on
/// c3. The version taken along has a bit c3 never had: it stays, and c3's edit goes up as a new
/// item (§4).
#[test]
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

/// The same through Recently deleted: c1 (Linux) makes `c.txt` executable, then deletes it,
/// while c0 (macOS) edits its plain copy. c0's edit restores the file (an edit wins over the
/// delete), but the version in Recently deleted has a bit c0 never had, and c0 sends its own:
/// that version stays, and c0's edit goes up as a new item (§4).
#[test]
fn regression_a_restore_keeps_the_bit_in_recently_deleted() {
    use hbsync_sim::Action::*;
    let mut sim = Sim::new(2);
    let root = sim.pick(1, hbsync_types::NodeId::ROOT, true).expect("root");
    sim.run(&[Event::Act {
        client: 1,
        action: CreateFile {
            parent: root,
            name: 3,
        },
    }]);
    assert!(sim.quiesce(30), "synced");
    let f = sim.item(1, "c.txt").expect("picked");
    let (f0, f1) = (
        sim.file_pick(0, f).expect("picked"),
        sim.file_pick(1, f).expect("picked"),
    );
    let n1 = sim.pick(1, f, false).expect("picked");
    let step = Event::Step { client: 1 };
    sim.run(&[
        Event::Act {
            client: 0,
            action: Edit { file: f0 },
        },
        Event::Act {
            client: 1,
            action: Chmod { file: f1 },
        },
        Event::Plan { client: 1 },
        step,
        step,
        Event::Act {
            client: 1,
            action: Delete { node: n1 },
        },
        Event::Plan { client: 1 },
        step,
        step,
    ]);
    assert!(sim.is_removed(f), "in Recently deleted before c0 plans");
    assert!(sim.quiesce(30), "converges");
    sim.check().expect("check");
    assert!(sim.is_removed(f), "the version with the bit stays");
}

/// A file deleted for good and made again by a device that never saw its executable bit
/// (#450). Client 0 (a Mac) makes the file and marks it executable; client 1 (Linux) takes the
/// bit; client 3 (a Mac) had the file from before the mark. The file is deleted and Recently
/// deleted emptied while 1 and 3 are away; 3 edits its copy, which makes the node again, as
/// another file, without the bit. Client 1 still holds the bit: read against its base of the
/// old file, the new file's lack of it was "cleared on the drive", and took the bit off its
/// copy, and then off every device's. The bit is agreed only while the node's file has it:
/// client 1 reads it as set here, and sends it up.
#[test]
fn regression_the_executable_bit_stands_where_the_file_is_made_again_without_it() {
    use Action::*;
    use Event::*;
    let synced = |sim: &mut Sim, client: usize| {
        sim.run(&[Plan { client }]);
        for _ in 0..6 {
            sim.run(&[Step { client }]);
        }
    };
    let mut sim = Sim::new(4);
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    for client in [0, 1, 3] {
        synced(&mut sim, client);
    }
    let file = sim.item(0, "a").expect("the file");
    sim.run(&[act(0, Chmod { file: 0 })]);
    synced(&mut sim, 0);
    synced(&mut sim, 1);
    assert_eq!(
        sim.local_executable(1, file),
        Some(true),
        "taken by client 1"
    );
    assert_eq!(
        sim.local_executable(3, file),
        Some(false),
        "not seen by client 3"
    );

    // Deleted, and for good, while 1 and 3 are away.
    let node = sim.pick(0, file, false).expect("its pick");
    sim.run(&[act(0, Delete { node })]);
    synced(&mut sim, 0);
    sim.run(&[act(0, ListDeleted), act(0, EmptyListed)]);
    // Made again by client 3's edit, without the bit.
    let edited = sim.file_pick(3, file).expect("client 3's file");
    sim.run(&[act(3, Edit { file: edited })]);
    synced(&mut sim, 3);
    assert_eq!(
        sim.drive_executable(file),
        Some(false),
        "made again without it"
    );

    synced(&mut sim, 1);
    settle(&mut sim);
    assert_eq!(
        sim.local_executable(1, file),
        Some(true),
        "kept where it was held"
    );
    assert_eq!(sim.drive_executable(file), Some(true), "and sent up");
    assert_eq!(sim.local_executable(3, file), Some(true));
}

/// A file's bit set on one device while another edits it (§1): no conflict. Client 1 (Linux)
/// marks the file executable; client 0's edit reaches the drive first. Client 1 takes the new
/// bytes, keeps its bit and sends it up: one file, edited and executable, on every device
/// that holds the bit. (The planner took bytes and bit as one content: the two changes were
/// a conflict, and client 1 kept its old bytes as a conflict copy.)
#[test]
fn regression_a_bit_set_here_and_an_edit_there_are_both_kept() {
    use Action::*;
    use Event::*;
    let mut sim = Sim::new(3);
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(30), "synced");
    let file = sim.item(0, "a").expect("the file");
    let (f0, f1) = (
        sim.file_pick(0, file).expect("picked"),
        sim.file_pick(1, file).expect("picked"),
    );
    sim.run(&[
        act(1, Chmod { file: f1 }),
        act(0, Edit { file: f0 }),
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 0 },
        Plan { client: 1 },
    ]);
    assert_eq!(sim.planned(1), ["LocalDownload"]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a"], "no conflict copy");
    assert_eq!(sim.local_hash(1, file), sim.drive_hash(file), "the edit");
    for client in [0, 1] {
        assert_eq!(sim.local_executable(client, file), Some(true));
    }
    assert_eq!(sim.drive_executable(file), Some(true));
}

/// A device that doesn't hold the bit (client 2, Windows) reads the drive's as its own (§1):
/// where the drive changed both a file's bit and its bytes since it synced, that read as "the
/// bit changed here, the bytes there", a conflict, and it kept its old bytes as a conflict
/// copy, though nothing changed on it. A bit changed here alone is never one.
#[test]
fn regression_a_change_of_bit_and_bytes_makes_no_copy_where_the_bit_isnt_held() {
    use Action::*;
    use Event::*;
    let mut sim = Sim::new(3);
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(30), "synced");
    let file = sim.item(0, "a").expect("the file");
    let f0 = sim.file_pick(0, file).expect("picked");
    sim.run(&[
        act(0, Chmod { file: f0 }),
        act(0, Edit { file: f0 }),
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 0 },
        Plan { client: 2 },
    ]);
    assert_eq!(sim.planned(2), ["LocalDownload"]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a"], "no conflict copy");
    assert_eq!(sim.drive_executable(file), Some(true));
}
