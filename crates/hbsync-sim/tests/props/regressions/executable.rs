//! Regressions: executable bits (sync semantics §1).

use crate::common::*;
use hbsync_sim::{Action, Event};

/// A known engine flaw, not of the bit's rule (on main too): client 4 edits a file while a Mac
/// (client 0) marks it executable; client 4's plan forks its edit into a conflict copy and is
/// to download the node again, but the Mac takes the mark off before that download runs, so
/// it goes stale. The drive is then as the node's base again, with no item of the node here
/// (the fork took it): the next plan reads that as a delete here and removes the file from the
/// drive, to Recently deleted, though no one deleted it. A fork leaves the node's base as it
/// was; an edit undone elsewhere (the same bytes again) would do the same.
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
