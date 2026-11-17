//! The folder at a device's top that holds what other identities share with the account (sync
//! semantics §10): the device's own, left out with all in it, its name held against the
//! drive's.

use hbsync_sim::{Action, Sim};
use hbsync_types::NodeId;

use super::{act, settle};

/// `d` in [`Action`]s' names, and `b`.
const D: usize = 5;
const B: usize = 1;

/// Client 0 has `a`, and the folder of shares `d` holding another `a`; client 1 has no folder
/// of shares. Each plan is checked against the engine's own view.
fn sharing() -> Sim {
    use Action::*;
    let mut sim = Sim::new(2)
        .without_ignore_rules()
        .with_shared_folder_on(0, "d")
        .with_narrow_check();
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: D }),
    ]);
    add_to(&mut sim, 0, "d", 0);
    settle(&mut sim);
    sim
}

/// Client `c` makes the file named by `name` in its folder `folder`.
fn add_to(sim: &mut Sim, c: usize, folder: &str, name: usize) {
    let folder = sim.item(c, folder).expect("the folder");
    let parent = sim.pick(c, folder, true).expect("the folder's pick");
    sim.run(&[act(c, Action::CreateFile { parent, name })]);
}

/// Nothing of the folder of shares reaches the drive or another device.
#[test]
fn a_folder_of_shares_stays_on_its_device() {
    let sim = sharing();
    assert_eq!(sim.local_paths(0), ["a", "d", "d/a"]);
    assert_eq!(sim.server_paths(), ["a"]);
    assert_eq!(sim.local_paths(1), ["a"]);
}

/// Another device makes a folder of the same name: it stays out of the device that holds its
/// own there (nothing merged into the folder of shares), and comes once that one yields the
/// name, renamed with its marker; the folder of shares stays the device's own under its new
/// name.
#[test]
fn a_folder_of_the_same_name_on_the_drive_comes_once_the_folder_of_shares_yields() {
    use Action::*;
    let mut sim = sharing();
    sim.run(&[act(1, CreateFolder { parent: 0, name: D })]);
    add_to(&mut sim, 1, "d", B);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "d", "d/b"]);
    assert_eq!(sim.local_paths(0), ["a", "d", "d/a"]);
    assert_eq!(sim.local_paths(1), ["a", "d", "d/b"]);

    let shares = sim.item(0, "d").expect("the folder of shares");
    let node = sim.pick(0, shares, false).expect("its pick");
    let top = sim.pick(0, NodeId::ROOT, true).expect("the top's pick");
    sim.run(&[act(
        0,
        Move {
            node,
            parent: top,
            name: B,
        },
    )]);
    let mut sim = sim.with_shared_folder_on(0, "b");
    settle(&mut sim);
    assert_eq!(sim.item(0, "b"), Some(shares));
    assert_eq!(sim.local_paths(0), ["a", "b", "b/a", "d", "d/b"]);
    assert_eq!(sim.server_paths(), ["a", "d", "d/b"]);
    assert_eq!(sim.local_paths(1), ["a", "d", "d/b"]);
}

/// The name is held whatever would take it: a file of the folder's name on the drive stays
/// out too.
#[test]
fn a_file_of_the_same_name_on_the_drive_stays_out() {
    use Action::*;
    let mut sim = sharing();
    sim.run(&[act(1, CreateFile { parent: 0, name: D })]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["a", "d"]);
    assert_eq!(sim.local_paths(0), ["a", "d", "d/a"]);
}

/// A folder of shares that loses its marker is a plain folder: synced, beside the drive's of
/// its name. One that gets a marker (as the first gets its own back) leaves the drive and the
/// other devices, kept where the marker is.
#[test]
fn a_marker_lost_makes_the_folder_plain_and_one_gained_takes_it_off_the_drive() {
    use Action::*;
    let mut sim = sharing();
    sim.run(&[act(1, CreateFolder { parent: 0, name: D })]);
    add_to(&mut sim, 1, "d", B);
    settle(&mut sim);

    let shares = sim.item(0, "d");
    sim.run(&[act(0, ToggleIgnore)]);
    settle(&mut sim);
    let both = ["a", "d", "d (conflict 1)", "d (conflict 1)/b", "d/a"];
    assert_eq!(sim.server_paths(), both);
    assert_eq!(sim.local_paths(0), both);
    assert_eq!(sim.local_paths(1), both);
    assert_eq!(sim.item(1, "d"), shares);

    sim.run(&[act(0, ToggleIgnore)]);
    settle(&mut sim);
    let left = ["a", "d (conflict 1)", "d (conflict 1)/b"];
    assert_eq!(sim.server_paths(), left);
    assert_eq!(sim.local_paths(1), left);
    assert_eq!(sim.local_paths(0), both);
}
