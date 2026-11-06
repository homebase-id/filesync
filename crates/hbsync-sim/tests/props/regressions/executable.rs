//! Regressions: executable bits (sync semantics §1).

use crate::common::*;
use hbsync_sim::{Action, Event};

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
