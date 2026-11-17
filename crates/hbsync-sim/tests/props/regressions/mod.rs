//! Scripted runs that once failed, or pin a case, by topic.

mod crash;
mod deleted;
mod desktops;
mod executable;
mod member;
mod names;
mod read_only;
mod roles;
mod shared_folder;
mod state_folder;
mod tags;

use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;

fn act(client: usize, action: Action) -> Event {
    Event::Act { client, action }
}

/// What an action's `parent` is to pick the top on client `c`.
fn top(sim: &Sim, c: usize) -> usize {
    sim.pick(c, NodeId::ROOT, true).expect("the top")
}

/// Client `c` makes the file named by `name` in its folder `folder`.
fn add_to(sim: &mut Sim, c: usize, folder: &str, name: usize) {
    let folder = sim.item(c, folder).expect("the folder");
    let parent = sim.pick(c, folder, true).expect("the folder's pick");
    sim.run(&[act(c, Action::CreateFile { parent, name })]);
}

/// Lets every client settle, then checks the run: all the simulator's rules (convergence, no
/// loss, no write to the drive from a client that only reads it).
fn settle(sim: &mut Sim) {
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}
