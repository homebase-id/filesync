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
mod tags;

use hbsync_sim::{Action, Event, Sim};

fn act(client: usize, action: Action) -> Event {
    Event::Act { client, action }
}

/// Lets every client settle, then checks the run: all the simulator's rules (convergence, no
/// loss, no write to the drive from a client that only reads it).
fn settle(sim: &mut Sim) {
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}
