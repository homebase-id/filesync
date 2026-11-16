//! A member of another identity's drive (sync semantics §13): it has what is shared with it,
//! what is shared without its folder at the top, and writes nothing.

use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

use super::{act, settle};

/// The owner (client 0) shares what `pick` picks with the member, or withdraws its share:
/// alone, or (`below`) with all the drive has in it.
fn shares(pick: usize, below: bool) -> Event {
    act(0, Action::Share { pick, below })
}

/// The owner shares `path`, as it has it, with the member, or withdraws its share.
fn share(sim: &mut Sim, path: &str) {
    let node = sim.item(0, path).expect("on the owner's device");
    let pick = sim.share_pick(node).expect("on the drive");
    sim.run(&[shares(pick, false)]);
}

/// Client 0, the owner, has `a` and the folder `b` holding another `a`; client 2 is a member
/// of what it shares: nothing yet. Each plan is checked against the engine's own view.
fn owned() -> Sim {
    use Action::*;
    let mut sim = Sim::new(3).with_member(2).with_narrow_check();
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 1 }),
    ]);
    let b = sim.item(0, "b").expect("b");
    let parent = sim.pick(0, b, true).expect("b's pick");
    sim.run(&[act(0, CreateFile { parent, name: 0 })]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(0), ["a", "b", "b/a"]);
    assert!(sim.local_paths(2).is_empty());
    sim
}

/// A file shared without its folder is at the top, in its folder once that is shared, and at
/// the top again when the folder's share is withdrawn.
#[test]
fn a_file_shared_without_its_folder_is_at_the_top() {
    let mut sim = owned();
    share(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    share(&mut sim, "b");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["b", "b/a"]);
    share(&mut sim, "b");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    share(&mut sim, "b/a");
    settle(&mut sim);
    assert!(sim.local_paths(2).is_empty());
}

/// Two shared files of one name at the top, one for want of its folder: the member has the
/// one it had, the other left out while the name is taken, and there once it is free. The
/// one synced isn't planned again, so the newcomer's plan finds it by its name at the top.
#[test]
fn a_name_taken_at_the_top_by_a_file_without_its_folder_leaves_the_newcomer_out() {
    let mut sim = owned();
    let (top, inner) = (
        sim.item(0, "a").expect("a"),
        sim.item(0, "b/a").expect("b/a"),
    );
    share(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.item(2, "a"), Some(inner));
    share(&mut sim, "a");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.item(2, "a"), Some(inner));
    share(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.item(2, "a"), Some(top));
}

/// The same with the one at the top there first: the one without its folder is left out.
#[test]
fn a_file_without_its_folder_is_left_out_while_its_name_is_taken_at_the_top() {
    let mut sim = owned();
    let (top, inner) = (
        sim.item(0, "a").expect("a"),
        sim.item(0, "b/a").expect("b/a"),
    );
    share(&mut sim, "a");
    settle(&mut sim);
    share(&mut sim, "b/a");
    settle(&mut sim);
    assert_eq!(sim.item(2, "a"), Some(top));
    // Its folder shared: it has its place.
    share(&mut sim, "b");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a", "b", "b/a"]);
    assert_eq!(sim.item(2, "b/a"), Some(inner));
}

/// A file the drive has in a file (another client's mistake) that isn't shared with the
/// member is at the top there, as any whose folder isn't shared; no client that writes syncs
/// it (one ignores it, the other leaves what is in a file), so the drive keeps it where it is.
#[test]
fn what_the_drive_has_in_a_file_not_shared_is_at_the_top_for_a_member() {
    use Action::*;
    let mut sim = Sim::new(3).with_member(2).with_narrow_check();
    sim.run(&[
        act(1, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 3 }),
        act(0, CreateFile { parent: 0, name: 5 }),
        Event::Plan { client: 0 },
        act(0, ToggleIgnore),
        Event::Step { client: 0 },
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        act(0, NewerFolder { name: 3 }),
        act(0, Delete { node: 7 }),
        Event::Step { client: 0 },
        act(2, Restore { node: 0 }),
        Event::Step { client: 0 },
        Event::Step { client: 1 },
        shares(8, false),
        act(0, Restore { node: 0 }),
        act(0, Restore { node: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, IntoFile { pick: 11, file: 3 }),
    ]);
    settle(&mut sim);
    assert!(sim.server_paths().contains(&"a/d".to_string()));
    assert_eq!(sim.local_paths(2), ["d"]);
}

/// What is in a folder no client reads (a newer writer's) waits for it on the owner's
/// devices; shared, it is at the top for the member, the folder shared with it or not: a
/// folder it can't read isn't one it has.
#[test]
fn what_is_in_a_folder_no_client_reads_is_at_the_top_for_a_member() {
    use Action::*;
    let mut sim = Sim::new(3).with_member(2).with_narrow_check();
    // The folder is the drive's first node, its file the second.
    sim.run(&[act(0, NewerFolder { name: 3 }), shares(1, false)]);
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["c.txt", "c.txt/a"]);
    assert!(sim.local_paths(0).is_empty());
    assert_eq!(sim.local_paths(2), ["a"]);
    sim.run(&[shares(0, false)]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
}

/// A known limit: two shared files of one name at the top, each for want of its folder. The
/// member has the first; it is deleted for good on the drive as the second is shared, so the
/// second's download parks the first (its delete planned) and takes the name; the member dies
/// there, and the first is put back on the drive. The second keeps the name, on disk under
/// it; the first waits under its parked name, hidden, while the name is taken, and has it
/// once the second's share is withdrawn.
#[test]
fn a_copy_parked_for_a_download_waits_parked_while_its_name_is_taken() {
    use Action::*;
    let mut sim = Sim::new(3)
        .with_member(2)
        .with_small_pages(3)
        .with_split_downloads();
    sim.run(&[
        act(0, NewerFolder { name: 0 }),
        act(0, NewerFolder { name: 1 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        shares(2, true),
        Event::Crash { client: 0 },
        shares(9, true),
        shares(0, true),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, DeleteStopped { pick: 1, done: 2 }),
        act(0, Purge),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        shares(11, false),
        Event::Crash { client: 2 },
        act(0, Tag { node: 0, tag: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, Tag { node: 0, tag: 0 }),
        act(0, Purge),
        act(2, CreateFile { parent: 0, name: 3 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, Purge),
        Event::Plan { client: 2 },
        Event::Step { client: 2 },
        Event::Step { client: 2 },
        Event::Crash { client: 2 },
        act(0, Restore { node: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Crash { client: 0 },
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
    ]);
    settle(&mut sim);
    let second = sim.item(2, "a").expect("a");
    let [parked, _] = &sim.local_paths(2)[..] else {
        panic!("{:?}", sim.local_paths(2));
    };
    assert!(parked.starts_with(".hbsync-move-"), "{parked}");
    let first = sim.item(2, parked).expect("parked");
    let pick = sim.share_pick(second).expect("on the drive");
    sim.run(&[shares(pick, false)]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a"]);
    assert_eq!(sim.item(2, "a"), Some(first));
}

/// A file renamed on the drive to a name taken at the member's top waits there under its old
/// name, and a new file of that old name waits for it in turn: each has its name once the
/// first one's is free.
#[test]
fn a_new_file_waits_for_the_name_a_waiting_one_holds() {
    use Action::*;
    let mut sim = owned();
    let (top, inner) = (
        sim.item(0, "a").expect("a"),
        sim.item(0, "b/a").expect("b/a"),
    );
    let parent = sim.pick(0, NodeId::ROOT, true).expect("the top");
    sim.run(&[act(0, CreateFile { parent, name: 3 })]);
    settle(&mut sim);
    let old = sim.item(0, "c.txt").expect("c.txt");
    share(&mut sim, "b/a");
    share(&mut sim, "c.txt");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a", "c.txt"]);
    // The owner's own `a` gone, it renames `c.txt` to `a`: the member's `a` keeps the name.
    let pick = |sim: &Sim, node| sim.pick(0, node, false).expect("on the owner's device");
    let node = pick(&sim, top);
    sim.run(&[act(0, Delete { node })]);
    let node = pick(&sim, old);
    sim.run(&[act(
        0,
        Move {
            node,
            parent,
            name: 0,
        },
    )]);
    settle(&mut sim);
    assert_eq!(sim.local_paths(0), ["a", "b", "b/a"]);
    assert_eq!(sim.item(2, "c.txt"), Some(old));
    // A new `c.txt` waits for the name the old one holds here.
    sim.run(&[act(0, CreateFile { parent, name: 3 })]);
    settle(&mut sim);
    let new = sim.item(0, "c.txt").expect("the new c.txt");
    share(&mut sim, "c.txt");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a", "c.txt"]);
    assert_eq!(sim.item(2, "a"), Some(inner));
    assert_eq!(sim.item(2, "c.txt"), Some(old));
    // The folder shared, its file leaves the top: each has its name.
    share(&mut sim, "b");
    settle(&mut sim);
    assert_eq!(sim.local_paths(2), ["a", "b", "b/a", "c.txt"]);
    assert_eq!(sim.item(2, "a"), Some(old));
    assert_eq!(sim.item(2, "c.txt"), Some(new));
}
